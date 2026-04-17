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

# --- CONFIG ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://jstar21k.github.io/Vid-play-site/")

logging.basicConfig(level=logging.INFO)

client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_db']
files_col = db['files']

async def generate_token():
    return secrets.token_urlsafe(8)[:10]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if context.args:
        token = context.args[0]
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
    
    # Secure check for forward origin
    origin = msg.forward_origin
    if not origin or not hasattr(origin, 'chat') or origin.chat.id != STORAGE_CHANNEL_ID:
        await msg.reply_text("❌ Error: Forward the file from your Storage Channel.")
        return

    # For channels, the message ID is stored in message_id of the origin if available
    # or we use the message_id from the forwarded message metadata.
    storage_id = getattr(origin, 'message_id', msg.forward_from_message_id if hasattr(msg, 'forward_from_message_id') else None)
    
    if not storage_id:
        await msg.reply_text("❌ Could not detect original Message ID. Try forwarding again.")
        return

    attachment = msg.effective_attachment
    if not attachment or isinstance(attachment, list): return

    file_name = getattr(attachment, 'file_name', 'Video_File')
    token = await generate_token()

    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": storage_id,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0
    })

    await msg.reply_text(f"✅ **File Saved!**\n\nLink: {GATEWAY_URL}?token={token}")

if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.FORWARDED & (filters.Document.ALL | filters.VIDEO), handle_forward))
    application.run_polling()
