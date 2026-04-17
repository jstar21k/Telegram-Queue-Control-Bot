# -*- coding: utf-8 -*-
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
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://vidplays.in/")

logging.basicConfig(level=logging.INFO)

client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']
logs_col = db['downloads']

async def generate_token():
    return secrets.token_urlsafe(8)[:10]

async def delete_files_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id, file_msg_id, warn_msg_id = job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=file_msg_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warn_msg_id)
    except Exception as e:
        logging.error(f"Auto-delete failed: {e}")

def get_admin_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Full Statistics", callback_data="stats")],
        [InlineKeyboardButton("🔌 Bot & DB Status", callback_data="status_check")],
        [InlineKeyboardButton("🔄 Refresh Menu", callback_data="refresh")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await users_col.update_one({"user_id": user_id}, {"$set": {"last_seen": datetime.now(timezone.utc)}}, upsert=True)

    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})
        if file_data:
            try:
                # FIXED: Markdown ki jagah HTML use kiya hai taaki filename bot ko crash na kare
                fname = file_data.get('file_name', 'Video')
                caption_text = f"🎥 <b>File:</b> {fname}\n🚀 <b>Delivered by @link69_viral</b>"
                
                # 1. Send File
                file_msg = await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=int(file_data['storage_msg_id']),
                    caption=caption_text,
                    parse_mode="HTML"
                )
                
                # Update Stats
                await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
                await logs_col.insert_one({"token": token, "time": datetime.now(timezone.utc)})

                # 2. Warning Message
                warn_msg = await update.message.reply_text(
                    "⚠️ <b>IMPORTANT:</b> Save this file to your 'Saved Messages' now. It will be deleted in <b>10 minutes</b>.",
                    parse_mode="HTML"
                )

                # 3. Deletion Job (10 Minutes)
                context.job_queue.run_once(delete_files_job, 600, [user_id, file_msg.message_id, warn_msg.message_id], chat_id=user_id)
                
            except Exception as e:
                await update.message.reply_text(f"❌ <b>Error:</b> Bot file bhej nahi paa raha.\n\n<b>Reason:</b> {str(e)}", parse_mode="HTML")
                logging.error(f"Delivery Error: {e}")
            return
        else:
            await update.message.reply_text("❌ Invalid or Expired Link.")
            return

    if user_id == ADMIN_USER_ID:
        await update.message.reply_text("💎 <b>JSTAR PRO ADMIN PANEL</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        cursor = files_col.aggregate([{"$group": {"_id": None, "total": {"$sum": "$total_downloads"}}}])
        res = await cursor.to_list(length=1)
        total_dl = res[0]['total'] if res else 0
        today_dl = await logs_col.count_documents({"time": {"$gte": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)}})

        text = (f"📊 <b>BOT ANALYTICS</b>\n\n👥 Total Users: <code>{total_users}</code>\n🔗 Total Links: <code>{total_links}</code>\n📥 Total Downloads: <code>{total_dl}</code>\n📅 Downloads Today: <code>{today_dl}</code>")
        await query.edit_message_text(text, reply_markup=get_admin_keyboard(), parse_mode="HTML")

    elif query.data == "status_check":
        try:
            await client.admin.command('ping')
            db_status = "✅ Connected"
        except:
            db_status = "❌ Disconnected"
        await query.edit_message_text(f"🔌 <b>SYSTEM STATUS</b>\n\n🗄 MongoDB: <code>{db_status}</code>\n🛰 Admin Bot: <code>✅ Running</code>", reply_markup=get_admin_keyboard(), parse_mode="HTML")

    elif query.data == "refresh":
        await query.edit_message_text("Admin Panel Refreshed ✅", reply_markup=get_admin_keyboard())

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
        await context.bot.send_message(chat_id=ADMIN_USER_ID, text=f"🚀 <b>Auto-Link Generated!</b>\n\n📁 File: <code>{file_name}</code>\n🔗 Link: <code>{link}</code>", parse_mode="HTML")
    except:
        logging.error("Admin message fail.")

if __name__ == '__main__':
    # JobQueue ke liye specific build
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_button))
    app.add_handler(MessageHandler(filters.Chat(STORAGE_CHANNEL_ID) & (filters.VIDEO | filters.Document.ALL), auto_post_handler))
    print("Bot is Live...")
    app.run_polling()
