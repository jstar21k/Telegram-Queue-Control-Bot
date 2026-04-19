# -*- coding: utf-8 -*-
import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeDefault
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler, ContextTypes, filters
)
from telegram.constants import ChatMemberStatus

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://vidplays.in/")
FORCE_JOIN_CHANNEL = "link69_viral"  # without @

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ━━━ DATABASE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
client = AsyncIOMotorClient(MONGODB_URI)
db = client['tg_bot_pro_db']
files_col = db['files']
users_col = db['users']
logs_col = db['downloads']

# ━━━ PRELOADED CAPTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CAPTIONS = [
    "🔥 This one is gonna make you sweat... watch it alone 🫣",
    "⚠️ Not safe for public places... but you'll still watch it 😈",
    "🤭 She didn't know the camera was on... full video inside",
    "💦 Hot new clip just dropped — you know you want to click",
    "🫣 This was supposed to be private... oops 👀",
    "😈 The ending will shock you... don't skip to the last part",
    "🔞 18+ content — open at your own risk 🥵",
    "🔥 Everyone is searching for this video right now",
    "🤫 Leaked clip — watch before it gets taken down",
    "💦 She thought nobody was watching... surprise surprise",
    "👁️ This video broke the internet last night — see why",
    "😈 2 minutes in... that's when things get wild 🔥",
    "🫦 Trending for all the wrong reasons... and you love it",
    "🔞 Can't believe this is free — premium quality right here",
    "🔥 Her reaction at the end... you'll replay it 10 times",
    "🤭 You'll need headphones for this one... trust me",
    "💦 The most viewed clip this week — find out why",
    "😈 They tried to delete this... but we saved it 😏",
    "🔥 Your browser history called... it wants this video back",
    "🫣 Someone's in big trouble after this leak 💀",
]

# ━━━ HELPERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_token():
    return secrets.token_urlsafe(8)[:10]


def format_duration(seconds):
    if not seconds:
        return "N/A"
    total = int(seconds)
    if total >= 3600:
        h, rem = divmod(total, 3600)
        m, s = divmod(rem, 60)
        return f"{h}:{m:02d}:{s:02d}"
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
        [InlineKeyboardButton("🔌 System Status", callback_data="status")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
    ])


async def is_joined(bot: Bot, user_id: int) -> bool:
    """Check if user has joined the force-join channel."""
    try:
        member = await bot.get_chat_member(chat_id=f"@{FORCE_JOIN_CHANNEL}", user_id=user_id)
        return member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
    except Exception:
        return False


# ━━━ AUTO-DELETE JOB ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def auto_delete(context: ContextTypes.DEFAULT_TYPE):
    chat_id, file_msg_id, warn_msg_id = context.job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=file_msg_id)
        await context.bot.delete_message(chat_id=chat_id, message_id=warn_msg_id)
    except Exception as e:
        logging.warning(f"Auto-delete skipped: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COMMAND: /start
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {"last_seen": datetime.now(timezone.utc), "name": user.full_name}},
        upsert=True,
    )

    # ── /start token? → deliver file with force-join check ──
    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})

        if not file_data:
            await update.message.reply_text("❌ Invalid or expired link.")
            return

        # Force-join check
        joined = await is_joined(context.bot, user.id)
        if not joined:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")
            ]])
            await update.message.reply_text(
                "🔒 <b>Access Denied!</b>\n\n"
                "You must join our channel to get the file.\n"
                "Join below, then click the link again 👇",
                reply_markup=kb,
                parse_mode="HTML"
            )
            # Save pending token so we can check again
            context.user_data['pending_token'] = token
            return

        # Deliver file
        await deliver_file(update, context, file_data)
        return

    # ── Normal start (no token) ──
    if user.id == ADMIN_USER_ID:
        await update.message.reply_text(
            "💎 <b>JSTAR PRO ADMIN PANEL</b>\n\n"
            "/post — Create a channel post\n"
            "/recent — View last 5 uploads",
            reply_markup=admin_kb(),
            parse_mode="HTML",
        )
        return

    # Regular user — show force-join
    joined = await is_joined(context.bot, user.id)
    if joined:
        await update.message.reply_text(
            "👋 Welcome back!\n\n"
            "Send me a link to get your file.",
            parse_mode="HTML",
        )
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")
        ]])
        await update.message.reply_text(
            "👋 Welcome!\n\n"
            "🔒 <b>Join our channel first</b> to access files.\n"
            "Join below, then tap /start again 👇",
            reply_markup=kb,
            parse_mode="HTML",
        )


async def deliver_file(update: Update, context: ContextTypes.DEFAULT_TYPE, file_data: dict):
    """Send file to user, update stats, schedule auto-delete."""
    user_id = update.effective_user.id
    token = file_data.get('token')

    try:
        fname = file_data.get('file_name', 'Video')
        caption = f"🎥 <b>File:</b> {fname}\n🚀 <b>Delivered by @{FORCE_JOIN_CHANNEL}</b>"

        file_msg = await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=STORAGE_CHANNEL_ID,
            message_id=int(file_data['storage_msg_id']),
            caption=caption,
            parse_mode="HTML",
        )

        # Stats
        await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
        await logs_col.insert_one({"token": token, "time": datetime.now(timezone.utc)})

        # Warning + auto-delete
        warn_msg = await update.message.reply_text(
            "⚠️ <b>Save to Saved Messages now!</b> "
            "This file will be deleted in <b>10 minutes</b>.",
            parse_mode="HTML",
        )
        context.job_queue.run_once(
            auto_delete, 600,
            [user_id, file_msg.message_id, warn_msg.message_id],
            chat_id=user_id,
        )

    except Exception as e:
        await update.message.reply_text(
            f"❌ <b>Error:</b> {str(e)}", parse_mode="HTML"
        )
        logging.error(f"Delivery failed: {e}")


# ━━━ COMMAND: /recent ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def recent_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        return

    files = await files_col.find().sort("created_at", -1).limit(5).to_list(5)
    if not files:
        await update.message.reply_text("📂 No files yet.", parse_mode="HTML")
        return

    text = "📂 <b>Last 5 Uploads:</b>\n\n"
    for i, f in enumerate(files, 1):
        name = f.get('file_name', '?')[:40]
        dur = format_duration(f.get('video_duration'))
        dl = f.get('total_downloads', 0)
        link = f"{GATEWAY_URL}?token={f['token']}"
        text += f"<b>{i}.</b> <code>{name}</code>\n   ⏱ {dur} │ 📥 {dl}\n   <code>{link}</code>\n\n"

    await update.message.reply_text(text, parse_mode="HTML")


# ━━━ ADMIN CALLBACK BUTTONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        total_links = await files_col.count_documents({})
        total_users = await users_col.count_documents({})
        agg = await files_col.aggregate([{"$group": {"_id": None, "dl": {"$sum": "$total_downloads"}}}]).to_list(1)
        total_dl = agg[0]['dl'] if agg else 0
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_dl = await logs_col.count_documents({"time": {"$gte": today}})

        await query.edit_message_text(
            f"📊 <b>ANALYTICS</b>\n\n"
            f"👥 Users: <code>{total_users}</code>\n"
            f"🔗 Links: <code>{total_links}</code>\n"
            f"📥 Downloads: <code>{total_dl}</code>\n"
            f"📅 Today: <code>{today_dl}</code>",
            reply_markup=admin_kb(), parse_mode="HTML",
        )

    elif query.data == "status":
        try:
            await client.admin.command('ping')
            db_st = "✅ Connected"
        except Exception:
            db_st = "❌ Disconnected"
        await query.edit_message_text(
            f"🔌 <b>SYSTEM STATUS</b>\n\n"
            f"🗄 MongoDB: <code>{db_st}</code>\n"
            f"🛰 Bot: <code>✅ Running</code>",
            reply_markup=admin_kb(), parse_mode="HTML",
        )

    elif query.data == "refresh":
        await query.edit_message_text("✅ Refreshed!", reply_markup=admin_kb())


# ━━━ AUTO-LINK ON STORAGE UPLOAD ━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def on_storage_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post = update.channel_post
    if not post or post.chat_id != STORAGE_CHANNEL_ID:
        return

    att = post.effective_attachment
    if not att or isinstance(att, list):
        return

    file_name = getattr(att, 'file_name', 'New_Upload')
    video_duration = 0
    if post.video:
        video_duration = post.video.duration or 0
    elif post.document:
        video_duration = getattr(post.document, 'duration', None) or 0

    token = generate_token()
    await files_col.insert_one({
        "file_name": file_name,
        "token": token,
        "storage_msg_id": post.message_id,
        "video_duration": video_duration,
        "created_at": datetime.now(timezone.utc),
        "total_downloads": 0,
    })

    link = f"{GATEWAY_URL}?token={token}"
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                f"🚀 <b>Auto-Link!</b>\n\n"
                f"📁 <code>{file_name}</code>\n"
                f"⏱ <code>{format_duration(video_duration)}</code>\n"
                f"🔗 <code>{link}</code>\n\n"
                f"💡 /post to create channel post"
            ),
            parse_mode="HTML",
        )
    except Exception:
        logging.error("Failed to notify admin.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  /post CONVERSATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SEL, THUMB, CONFIRM = range(3)


async def post_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        return ConversationHandler.END

    files = await files_col.find().sort("created_at", -1).limit(5).to_list(5)
    if not files:
        await update.message.reply_text("❌ No files found. Upload to storage first.")
        return ConversationHandler.END

    kb = []
    for f in files:
        name = f.get('file_name', '?')[:30]
        dur = format_duration(f.get('video_duration'))
        kb.append([InlineKeyboardButton(f"{name} ({dur})", callback_data=f"ps_{f['token']}")])
    kb.append([InlineKeyboardButton("❌ Cancel", callback_data="pc_cancel")])

    await update.message.reply_text(
        "📝 <b>POST — Step 1/3</b>\n\n📂 Select file:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="HTML",
    )
    return SEL


async def post_sel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "pc_cancel":
        await q.edit_message_text("❌ Cancelled.", parse_mode="HTML")
        return ConversationHandler.END

    token = q.data[3:]  # remove "ps_"
    fd = await files_col.find_one({"token": token})
    if not fd:
        await q.edit_message_text("❌ File not found.", parse_mode="HTML")
        return ConversationHandler.END

    context.user_data['_post'] = {
        'name': fd['file_name'],
        'token': fd['token'],
        'duration': format_duration(fd.get('video_duration')),
    }

    await q.edit_message_text(
        "📝 <b>POST — Step 2/3</b>\n\n"
        f"📂 <code>{fd['file_name'][:40]}</code>\n"
        f"⏱ <code>{context.user_data['_post']['duration']}</code>\n\n"
        "📸 Send me a <b>thumbnail</b>.",
        parse_mode="HTML",
    )
    return THUMB


async def post_thumb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pd = context.user_data.get('_post')
    if not pd:
        await update.message.reply_text("❌ Session expired. /post again")
        return ConversationHandler.END

    if not update.message.photo:
        await update.message.reply_text("❌ Send a <b>photo</b> as thumbnail.", parse_mode="HTML")
        return THUMB

    pd['thumb'] = update.message.photo[-1].file_id
    pd['caption'] = secrets.choice(CAPTIONS)

    link = f"{GATEWAY_URL}?token={pd['token']}"
    cap = f"{pd['caption']}\n\n⏱ Duration: {pd['duration']}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Send Now", callback_data="pc_send"),
         InlineKeyboardButton("🔄 New Caption", callback_data="pc_rot")],
        [InlineKeyboardButton("🖼 New Thumbnail", callback_data="pc_rethumb"),
         InlineKeyboardButton("❌ Cancel", callback_data="pc_cancel")],
    ])

    await update.message.reply_text("👀 <b>Preview:</b>", parse_mode="HTML")
    await update.message.reply_photo(
        photo=pd['thumb'], caption=cap, parse_mode="HTML", reply_markup=kb
    )
    return CONFIRM


async def post_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pd = context.user_data.get('_post')
    if not pd:
        return ConversationHandler.END

    if q.data == "pc_send":
        link = f"{GATEWAY_URL}?token={pd['token']}"
        cap = f"{pd['caption']}\n\n⏱ Duration: {pd['duration']}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Watch Now", url=link)]])

        await context.bot.send_photo(
            chat_id=update.effective_user.id,
            photo=pd['thumb'], caption=cap, parse_mode="HTML", reply_markup=kb,
        )
        await q.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=update.effective_user.id,
            text="✅ <b>Done!</b> Forward above to your channel.",
            parse_mode="HTML",
        )
        context.user_data.pop('_post', None)
        return ConversationHandler.END

    elif q.data == "pc_rot":
        pd['caption'] = secrets.choice([c for c in CAPTIONS if c != pd['caption']])
        cap = f"{pd['caption']}\n\n⏱ Duration: {pd['duration']}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Send Now", callback_data="pc_send"),
             InlineKeyboardButton("🔄 New Caption", callback_data="pc_rot")],
            [InlineKeyboardButton("🖼 New Thumbnail", callback_data="pc_rethumb"),
             InlineKeyboardButton("❌ Cancel", callback_data="pc_cancel")],
        ])
        try:
            await q.edit_message_caption(cap, parse_mode="HTML", reply_markup=kb)
        except Exception:
            pass
        return CONFIRM

    elif q.data == "pc_rethumb":
        await q.edit_message_reply_markup(reply_markup=None)
        await q.message.reply_text("🖼 Send a <b>new thumbnail</b>:", parse_mode="HTML")
        return THUMB

    elif q.data == "pc_cancel":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("❌ Cancelled.", parse_mode="HTML")
        context.user_data.pop('_post', None)
        return ConversationHandler.END


async def post_cancel_fb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_USER_ID and context.user_data.get('_post'):
        context.user_data.pop('_post', None)
        await update.message.reply_text("❌ Cancelled. /post to start again.", parse_mode="HTML")
    return ConversationHandler.END


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FORCE JOIN CHECK — callback button from inline
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def force_join_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """After user clicks 'I've Joined' button, verify and deliver."""
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    pending_token = context.user_data.get('pending_token')

    joined = await is_joined(context.bot, user_id)
    if not joined:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}"),
            InlineKeyboardButton("✅ I've Joined", callback_data="check_join"),
        ]])
        await q.edit_message_text(
            "❌ <b>You haven't joined yet!</b>\n\n"
            "Join the channel first, then click the button below:",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    # User joined — deliver file
    if pending_token:
        file_data = await files_col.find_one({"token": pending_token})
        if file_data:
            await q.edit_message_text("✅ <b>Verified!</b> Delivering your file...", parse_mode="HTML")
            await deliver_file(update, context, file_data)
            context.user_data.pop('pending_token', None)
            return

    await q.edit_message_text(
        "✅ <b>Welcome!</b>\n\nNow send me your link to get the file.",
        parse_mode="HTML",
    )


# ━━━ FORCE JOIN BUTTON UPDATE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Override the start's force-join keyboard to include "I've Joined"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await users_col.update_one(
        {"user_id": user.id},
        {"$set": {"last_seen": datetime.now(timezone.utc), "name": user.full_name}},
        upsert=True,
    )

    # ── /start token → deliver file ──
    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})

        if not file_data:
            await update.message.reply_text("❌ Invalid or expired link.")
            return

        # Force-join check
        joined = await is_joined(context.bot, user.id)
        if not joined:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
            ])
            await update.message.reply_text(
                "🔒 <b>Access Denied!</b>\n\n"
                "You must join our channel to get the file.\n"
                "Join below, then tap <b>I've Joined</b> 👇",
                reply_markup=kb,
                parse_mode="HTML",
            )
            context.user_data['pending_token'] = token
            return

        await deliver_file(update, context, file_data)
        return

    # ── Normal /start ──
    if user.id == ADMIN_USER_ID:
        await update.message.reply_text(
            "💎 <b>JSTAR PRO ADMIN PANEL</b>\n\n"
            "/post — Create channel post\n"
            "/recent — View last 5 uploads",
            reply_markup=admin_kb(),
            parse_mode="HTML",
        )
        return

    joined = await is_joined(context.bot, user.id)
    if joined:
        await update.message.reply_text(
            "👋 Welcome back!\n\nSend me a link to get your file.",
            parse_mode="HTML",
        )
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
            [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
        ])
        await update.message.reply_text(
            "👋 Welcome!\n\n"
            "🔒 <b>Join our channel first</b> to access files.\n"
            "Join below, then tap <b>I've Joined</b> 👇",
            reply_markup=kb,
            parse_mode="HTML",
        )


# ━━━ MAIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("recent", recent_cmd))

    # Admin callbacks
    app.add_handler(CallbackQueryHandler(admin_buttons, pattern="^(stats|status|refresh)$"))

    # Force-join verify callback
    app.add_handler(CallbackQueryHandler(force_join_check, pattern="^check_join$"))

    # /post conversation
    app.add_handler(ConversationHandler(
        entry_points=[CommandHandler("post", post_start)],
        states={
            SEL: [CallbackQueryHandler(post_sel_cb, pattern=r"^ps_|^pc_cancel$")],
            THUMB: [MessageHandler(filters.PHOTO, post_thumb)],
            CONFIRM: [CallbackQueryHandler(post_confirm, pattern=r"^pc_")],
        },
        fallbacks=[CommandHandler("cancel", post_cancel_fb)],
        allow_reentry=True,
    ))

    # Storage channel auto-link
    app.add_handler(MessageHandler(
        filters.Chat(STORAGE_CHANNEL_ID) & (filters.VIDEO | filters.Document.ALL),
        on_storage_upload,
    ))

    print("🚀 JSTAR PRO Bot is Live...")
    app.run_polling()
