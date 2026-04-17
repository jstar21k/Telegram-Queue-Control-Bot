import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
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

# --- KEYBOARDS ---
def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
        [InlineKeyboardButton("📂 Recent Files", callback_data="recent_files")],
        [InlineKeyboardButton("🔄 Refresh Menu", callback_data="refresh")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # User coming from Gateway link
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

    # Admin Menu
    if user_id == ADMIN_USER_ID:
        await update.message.reply_text(
            "👋 **Welcome Boss!**\nUse the buttons below to manage your bot.",
            reply_markup=get_admin_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text("Welcome to JSTAR Video Bot.")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        total_files = await files_col.count_documents({})
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        result = await cursor.to_list(length=1)
        downloads = result[0]['total'] if result else 0
        
        text = f"📊 **Bot Stats**\n\n📁 Files: `{total_files}`\n📥 Total Downloads: `{downloads}`"
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif query.data == "recent_files":
        cursor = files_col.find().sort("created_at", -1).limit(5)
        files = await cursor.to_list(length=5)
        if not files:
            await query.edit_message_text("No files yet.", reply_markup=get_admin_keyboard())
            return
        
        text = "📂 **Last 5 Files:**\n\n"
        for f in files:
            text += f"• `{f['file_name']}` (📥 {f['total_downloads']})\n"
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")
    
    elif query.data == "refresh":
        await query.edit_message_text("Admin Panel Refreshed ✅", reply_markup=get_admin_keyboard())

async def handle_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID: return

    msg = update.message
    origin = msg.forward_origin
    
    if not origin or not hasattr(origin, 'chat') or origin.chat.id != STORAGE_CHANNEL_ID:
        await msg.reply_text("❌ Error: Forward from Storage Channel!")
        return

    # Extract ID correctly
    storage_id = getattr(origin, 'message_id', msg.forward_from_message_id if hasattr(msg, 'forward_from_message_id') else msg.message_id)

    attachment = msg.effective_attachment
    file_name = getattr(attachment, 'file_name', 'Video_File')
    token = await generate_token()

    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": storage_id,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0
    })

    await msg.reply_text(
        f"✅ **Saved Successfully**\n\n"
        f"🔗 **Link:** `{GATEWAY_URL}?token={token}`",
        parse_mode="Markdown"
    )

if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_button))
    application.add_handler(MessageHandler(filters.FORWARDED & (filters.Document.ALL | filters.VIDEO), handle_forward))
    application.run_polling()
