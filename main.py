import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

BOT_TOKEN = os.environ.get("BOT_TOKEN")
SHARING_BOT_TOKEN = os.environ.get("SHARING_BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_USER_ID"))
STORAGE_ID = int(os.environ.get("STORAGE_CHANNEL_ID"))
MONGODB_URI = os.environ.get("MONGODB_URI")
GATEWAY = "https://jstar21k.github.io/Vid-play-site/"

client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="stats")],
        [InlineKeyboardButton("🔌 System Status", callback_data="status")]
    ])
    await update.message.reply_text("💎 **JSTAR ADMIN PANEL**", reply_markup=kb, parse_mode="Markdown")

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "stats":
        total_links = await files_col.count_documents({})
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        res = await cursor.to_list(1)
        total_dl = res[0]['total'] if res else 0
        await query.edit_message_text(f"📊 **Stats**\n\nLinks: `{total_links}`\nDownloads: `{total_dl}`", parse_mode="Markdown")
    
    elif query.data == "status":
        bot1_status = "❌ Offline"
        try:
            bot1 = await Bot(SHARING_BOT_TOKEN).get_me()
            bot1_status = f"✅ Online (@{bot1.username})"
        except: pass
        await query.edit_message_text(f"🔌 **Status**\n\nDB: ✅ Connected\nSharing Bot: `{bot1_status}`", parse_mode="Markdown")

async def auto_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.channel_post.chat.id != STORAGE_ID: return
    token = secrets.token_urlsafe(8)[:10]
    await files_col.insert_one({
        "token": token,
        "storage_msg_id": update.channel_post.message_id,
        "total_downloads": 0,
        "created_at": datetime.now(timezone.utc)
    })
    await context.bot.send_message(ADMIN_ID, f"🚀 **Link Generated!**\n\n`{GATEWAY}?token={token}`")

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.Chat(STORAGE_ID) & (filters.VIDEO | filters.Document.ALL), auto_post))
    app.run_polling()
