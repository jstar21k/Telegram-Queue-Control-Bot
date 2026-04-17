import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler, 
    ContextTypes, 
    filters
)

# --- CONFIG ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
SHARING_BOT_TOKEN = os.environ.get("SHARING_BOT_TOKEN") 
ADMIN_ID = int(os.environ.get("ADMIN_USER_ID", 0))
STORAGE_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
GATEWAY = os.environ.get("GATEWAY_URL", "https://jstar21k.github.io/Vid-play-site/")

logging.basicConfig(level=logging.INFO)

client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']
logs_col = db['downloads']

def get_admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Analytics", callback_data="stats")],
        [InlineKeyboardButton("🔌 System Status", callback_data="status_check")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Track Total Users (For Analytics)
    await users_col.update_one(
        {"user_id": user_id}, 
        {"$set": {"last_seen": datetime.now(timezone.utc)}}, 
        upsert=True
    )

    # --- PART 1: FILE SHARING LOGIC (For Users) ---
    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})
        
        if file_data:
            # 1. Update Stats
            await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
            await logs_col.insert_one({"token": token, "time": datetime.now(timezone.utc)})
            
            # 2. Send File
            try:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=STORAGE_ID,
                    message_id=int(file_data['storage_msg_id']),
                    caption=f"🎥 **File:** {file_data.get('file_name', 'Video')}\n🚀 **Delivered by @mms_link_hub**",
                    parse_mode="Markdown"
                )
            except Exception as e:
                await update.message.reply_text("❌ Error: Bot must be Admin in the Storage Channel.")
            return
        else:
            await update.message.reply_text("❌ Invalid or Expired Link.")
            return

    # --- PART 2: ADMIN PANEL LOGIC (For You Only) ---
    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "💎 **JSTAR PRO ADMIN PANEL**", 
            reply_markup=get_admin_kb(), 
            parse_mode="Markdown"
        )
    else:
        # Default message for regular users who start the bot without a link
        await update.message.reply_text("👋 **Welcome!**\nUse links from our channel @link69_viral to access files.")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        res = await cursor.to_list(1)
        total_dl = res[0]['total'] if res else 0
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_dl = await logs_col.count_documents({"time": {"$gte": today_start}})

        text = (f"📊 **PRO ANALYTICS**\n\n"
                f"👥 Total Users: `{total_users}`\n"
                f"🔗 Total Links: `{total_links}`\n"
                f"📥 Total Downloads: `{total_dl}`\n"
                f"📅 Downloads Today: `{today_dl}`")
        await query.edit_message_text(text, reply_markup=get_admin_kb(), parse_mode="Markdown")

    elif query.data == "status_check":
        db_s = "✅ Connected"
        bot1_s = "❌ Offline"
        if SHARING_BOT_TOKEN:
            try:
                async with Bot(SHARING_BOT_TOKEN) as b1:
                    me = await b1.get_me()
                    bot1_s = f"✅ Online (@{me.username})"
            except: bot1_s = "❌ Invalid Token"

        await query.edit_message_text(
            f"🔌 **SYSTEM STATUS**\n\n🗄 DB: `{db_s}`\n🤖 Sharing Bot: `{bot1_s}`\n🛰 Admin Bot: `✅ Online`", 
            reply_markup=get_admin_kb(), 
            parse_mode="Markdown"
        )

    elif query.data == "refresh":
        await query.edit_message_text("Admin Panel Refreshed ✅", reply_markup=get_admin_kb())

async def auto_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.channel_post.chat.id != STORAGE_ID: return
    token = secrets.token_urlsafe(8)[:10]
    
    # Save to MongoDB
    await files_col.insert_one({
        "token": token, 
        "storage_msg_id": update.channel_post.message_id,
        "file_name": update.channel_post.effective_attachment.file_name if hasattr(update.channel_post.effective_attachment, 'file_name') else "Video",
        "total_downloads": 0, 
        "created_at": datetime.now(timezone.utc)
    })
    
    await context.bot.send_message(ADMIN_ID, f"🚀 **Link Generated!**\n\n`{GATEWAY}?token={token}`")

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Adding Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_button))
    app.add_handler(MessageHandler(filters.Chat(STORAGE_ID) & (filters.VIDEO | filters.Document.ALL), auto_post))
    
    print("Admin Bot is Running...")
    app.run_polling()
