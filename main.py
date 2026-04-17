import os
import secrets
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
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
SHARING_BOT_TOKEN = os.environ.get("SHARING_BOT_TOKEN") 
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))

# --- NEW URL UPDATED ---
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://vidplays.in/")

logging.basicConfig(level=logging.INFO)

# --- DATABASE ---
client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']
logs_col = db['downloads']

async def generate_token():
    return secrets.token_urlsafe(8)[:10]

# --- AUTO DELETER FUNCTION ---
async def delete_files_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id, file_msg_id, warn_msg_id = job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=file_msg_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warn_msg_id)
    except Exception as e:
        logging.error(f"Auto-delete failed: {e}")

# --- KEYBOARDS ---
def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Full Statistics", callback_data="stats")],
        [InlineKeyboardButton("🔌 Bot & DB Status", callback_data="status_check")],
        [InlineKeyboardButton("🔄 Refresh Menu", callback_data="refresh")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await users_col.update_one({"user_id": user_id}, {"$set": {"last_seen": datetime.now(timezone.utc)}}, upsert=True)

    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})
        if file_data:
            await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
            await logs_col.insert_one({"token": token, "time": datetime.now(timezone.utc)})
            
            # 1. Send the file
            file_msg = await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=file_data['storage_msg_id'],
                caption=f"🎥 **File:** {file_data['file_name']}\n🚀 **Delivered by @link69_viral**",
                parse_mode="Markdown"
            )
            
            # 2. Send the warning message
            warn_msg = await update.message.reply_text(
                "⚠️ **IMPORTANT:** Save this file to your 'Saved Messages' now. It will be automatically deleted in **10 minutes** for security.",
                parse_mode="Markdown"
            )

            # 3. Schedule the deletion (600 seconds = 10 minutes)
            context.job_queue.run_once(
                delete_files_job, 
                when=600, 
                data=[user_id, file_msg.message_id, warn_msg.message_id],
                chat_id=user_id
            )
            return

    if user_id == ADMIN_USER_ID:
        await update.message.reply_text("💎 **JSTAR PRO ADMIN PANEL**", reply_markup=get_admin_keyboard(), parse_mode="Markdown")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        res = await cursor.to_list(length=1)
        total_dl = res[0]['total'] if res else 0
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_dl = await logs_col.count_documents({"time": {"$gte": today_start}})

        text = (f"📊 **BOT ANALYTICS**\n\n"
                f"👥 Total Users: `{total_users}`\n"
                f"🔗 Total Links: `{total_links}`\n"
                f"📥 Total Downloads: `{total_dl}`\n"
                f"📅 Downloads Today: `{today_dl}`")
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif query.data == "status_check":
        try:
            await client.admin.command('ping')
            db_status = "✅ Connected"

        text = (f"🔌 **SYSTEM STATUS**\n\n"
                f"🗄 MongoDB: `{db_status}`\n"
                f"🛰 Admin Bot: `✅ Running`")
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif query.data == "refresh":
        await query.edit_message_text("Panel Refreshed ✅", reply_markup=get_admin_keyboard())

async def auto_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel_post = update.channel_post
    if not channel_post or channel_post.chat.id != STORAGE_CHANNEL_ID: return

    attachment = channel_post.effective_attachment
    if not attachment or isinstance(attachment, list): return

    file_name = getattr(attachment, 'file_name', 'New_Upload')
    token = await generate_token()

    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": channel_post.message_id,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0
    })

    link = f"{GATEWAY_URL}?token={token}"
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"🚀 **Auto-Link Generated!**\n\n📁 File: `{file_name}`\n🔗 Link: `{link}`",
            parse_mode="Markdown"
        )
    except:
        logging.error("Could not send link to admin.")

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_button))
    app.add_handler(MessageHandler(filters.Chat(STORAGE_CHANNEL_ID) & (filters.VIDEO | filters.Document.ALL), auto_post_handler))
    
    print("JSTAR Bot Started on vidplays.in...")
    app.run_polling()
