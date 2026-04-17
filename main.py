import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# --- ENVIRONMENT VARIABLES ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://jstar21k.github.io/Vid-play-site/")

# Logging
logging.basicConfig(level=logging.INFO)

# --- DATABASE ---
client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_db']
files_col = db['files']

async def generate_token():
    return secrets.token_urlsafe(8)[:10]

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args

    if args:
        token = args[0]
        file_data = await files_col.find_one({"token": token})
        if file_data:
            await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=file_data['storage_msg_id'],
                caption=f"🎥 **File:** {file_data['file_name']}\n🚀 **Delivered by JSTAR Bot**",
                parse_mode="Markdown"
            )
            return
    await update.message.reply_text("Welcome to JSTAR Admin Bot.")

async def handle_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID: return

    msg = update.message
    # Check if it's a forward from the storage channel
    if not msg.forward_origin or not hasattr(msg.forward_origin, 'chat') or msg.forward_origin.chat.id != STORAGE_CHANNEL_ID:
        await msg.reply_text("❌ Error: Forward the file from the Storage Channel.")
        return

    attachment = msg.effective_attachment
    if not attachment or isinstance(attachment, list): return

    file_name = getattr(attachment, 'file_name', 'Video_File')
    token = await generate_token()

    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": msg.forward_from_message_id if hasattr(msg, 'forward_from_message_id') else msg.message_id,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0
    })

    link = f"{GATEWAY_URL}?token={token}"
    await msg.reply_text(f"✅ **File Saved!**\n\nLink: {link}")

if __name__ == '__main__':
    # Initialize Application
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.FORWARDED & (filters.Document.ALL | filters.VIDEO), handle_forward))
    
    print("Bot is starting...")
    application.run_polling()
