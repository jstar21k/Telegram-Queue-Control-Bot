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
SHARING_BOT_TOKEN = os.environ.get("SHARING_BOT_TOKEN") # Bot 1 Token
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://jstar21k.github.io/Vid-play-site/")

logging.basicConfig(level=logging.INFO)

# --- DATABASE ---
client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']      # Tracks total unique users
logs_col = db['downloads']   # Tracks every download with timestamp

async def generate_token():
    return secrets.token_urlsafe(8)[:10]

# --- KEYBOARDS ---
def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Full Statistics", callback_data="stats")],
        [InlineKeyboardButton("📂 Recent Files", callback_data="recent_files")],
        [InlineKeyboardButton("🔌 Bot & DB Status", callback_data="status_check")],
        [InlineKeyboardButton("🔄 Refresh Menu", callback_data="refresh")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- HANDLERS ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Save User to DB (Total Users Count)
    await users_col.update_one({"user_id": user_id}, {"$set": {"last_seen": datetime.now(timezone.utc)}}, upsert=True)

    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})
        if file_data:
            # Increment Total Downloads
            await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
            # Log this download for "Today" stats
            await logs_col.insert_one({"token": token, "time": datetime.now(timezone.utc)})
            
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=file_data['storage_msg_id'],
                caption=f"🎥 **File:** {file_data['file_name']}\n🚀 **Delivered by JSTAR**",
                parse_mode="Markdown"
            )
            return

    if user_id == ADMIN_USER_ID:
        await update.message.reply_text("💎 **JSTAR PRO ADMIN PANEL**", reply_markup=get_admin_keyboard(), parse_mode="Markdown")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        # 1. Total Files/Links
        total_links = await files_col.count_documents({})
        # 2. Total Users
        total_users = await users_col.count_documents({})
        # 3. Total Downloads (All time)
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        res = await cursor.to_list(length=1)
        total_dl = res[0]['total'] if res else 0
        # 4. Today's Downloads
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_dl = await logs_col.count_documents({"time": {"$gte": today_start}})

        text = (f"📊 **BOT ANALYTICS**\n\n"
                f"👥 Total Users: `{total_users}`\n"
                f"🔗 Total Links: `{total_links}`\n"
                f"📥 Total Downloads: `{total_dl}`\n"
                f"📅 Downloads Today: `{today_dl}`")
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif query.data == "status_check":
        # Check MongoDB
        try:
            await client.admin.command('ping')
            db_status = "✅ Connected"
        except:
            db_status = "❌ Disconnected"

        # Check Sharing Bot (Bot 1)
        bot1_status = "❌ Offline (No Token)"
        if SHARING_BOT_TOKEN:
            try:
                temp_bot = Bot(SHARING_BOT_TOKEN)
                me = await temp_bot.get_me()
                bot1_status = f"✅ Online (@{me.username})"
            except:
                bot1_status = "❌ Token Invalid"

        text = (f"🔌 **SYSTEM STATUS**\n\n"
                f"🗄 MongoDB: `{db_status}`\n"
                f"🤖 Sharing Bot: `{bot1_status}`\n"
                f"🛰 Admin Bot: `✅ Running`")
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="Markdown")

    elif query.data == "refresh":
        await query.edit_message_text("Panel Refreshed ✅", reply_markup=get_admin_keyboard())

# --- AUTO-LINK GENERATION FROM CHANNEL ---
async def auto_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This triggers when you upload a file to the storage channel
    channel_post = update.channel_post
    if not channel_post or channel_post.chat.id != STORAGE_CHANNEL_ID: return

    # Check for video or document
    attachment = channel_post.effective_attachment
    if not attachment or isinstance(attachment, list): return

    file_name = getattr(attachment, 'file_name', 'New_Upload')
    token = await generate_token()

    # Save to Database
    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": channel_post.message_id,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0
    })

    # Send link to Admin's Private Chat
    link = f"{GATEWAY_URL}?token={token}"
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"🚀 **Auto-Link Generated!**\n\n📁 File: `{file_name}`\n🔗 Link: `{link}`",
            parse_mode="Markdown"
        )
    except:
        logging.error("Could not send link to admin. Did you /start the bot?")

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_button))
    
    # Listen to the Storage Channel
    app.add_handler(MessageHandler(filters.Chat(STORAGE_CHANNEL_ID) & (filters.VIDEO | filters.Document.ALL), auto_post_handler))
    
    print("JSTAR Pro Bot Started...")
    app.run_polling()
