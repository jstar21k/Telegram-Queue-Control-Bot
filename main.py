# -*- coding: utf-8 -*-
import asyncio
import os
import secrets
import logging
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import (
    Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, BotCommandScopeDefault
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.constants import ChatMemberStatus

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", 0))
MONGODB_URI = os.environ.get("MONGODB_URI")
STORAGE_CHANNEL_ID = int(os.environ.get("STORAGE_CHANNEL_ID", 0))
POST_CHANNEL_ID = int(os.environ.get("POST_CHANNEL_ID", 0))  # channel where bot posts thumbnails
THUMBNAIL_CHANNEL_ID = int(os.environ.get("THUMBNAIL_CHANNEL_ID", 0))
GATEWAY_URL = os.environ.get("GATEWAY_URL", "https://vidplays.in/")
FORCE_JOIN_CHANNEL = "link69_viral"  # without @
HOW_TO_OPEN_LINK = "https://t.me/c/2047194577/41"  # Instructions for opening links

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
sync_col = db['bot_sync']

# ━━━ PRELOADED CAPTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CAPTIONS = [
"Seedha dil pe lagega! 💘🔥 Dekh ke batao kaisa laga?",
"Raat ki neend uddane wali clip 😈💦 Full HD quality!",
"Ye dekh ke phone side mein rakh dena... control nahi hoga! 🥵",
"Bina sound ke mat dekhna! 🎧 Awaz mein jaadu hai 😏",
"Itna bold content TG pe? 😲 Screenshot mat lena, bas dekho!",
"Subah uthte hi ye dekh liya toh din ban jayega! ☀️😈",
"Aankhein phati ki phati reh jayengi! 👀🔥 End miss mat karna.",
"Ye wali movement next level hai! 🌊💃 Rate karo 1-10?",
"Private collection se nikala hai... special for you! 🤫💎",
"Dil ki dhadkan tez kar dega! ❤️‍🔥 Headphones recommended.",
"Ye video save kar lo, baad mein delete ho jayega! ⏳🏃‍♂️",
"Log pooch rahe hain 'Ye kaun hai?' 😏 Comment mein guess karo!",
"Galti se forward mat karna family group mein! 🙈🚫",
"Isse zyada hot aur kya ho sakta hai? 🤯🔥 Challenge accepted?",
"Sirf close friends ke liye... par tumhare liye public kiya! 😈💌",
"Kal raat viral hua tha, ab yahan available! 🔥📲",
"Agar ye pasand aaya toh '🔥' react karo! Let's see power! 👇",
"Ye angle kisi ne nahi dekha hoga! 📸😲 Unique clip!",
"Thoda sa naughty, thoda sa crazy! 😜💦 Perfect combo.",
"Apne best friend ko bhejo jo single hai! 😂👇 Tag him!",
]

# ━━━ PENDING POST STATE (in-memory) ━━━━━━━━━━━━━━━━━━━━━━━━━
# When admin uploads to storage, bot auto-asks for thumbnail.
# This dict holds the pending post info until flow completes.
_pending_post = {}  # user_id -> {token, name, duration, thumb, caption, preview_msg_id}
POST_FLOW_LOCK = asyncio.Lock()


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


def preview_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Send Now", callback_data="pc_send"),
         InlineKeyboardButton("🔄 New Caption", callback_data="pc_rot")],
        [InlineKeyboardButton("🖼 New Thumb", callback_data="pc_rethumb"),
         InlineKeyboardButton("❌ Cancel", callback_data="pc_cancel")],
    ])


def get_channel_kb(link: str):
    """Get keyboard for channel posts with Watch Now + How to Open Link buttons."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Watch Now", url=link)],
        [InlineKeyboardButton("📖 How to Open Link", url=HOW_TO_OPEN_LINK)],
    ])


async def save_pending_post_state(pending: dict):
    data = dict(pending)
    data["updated_at"] = datetime.now(timezone.utc)
    await sync_col.update_one(
        {"_id": "pending_post"},
        {"$set": data},
        upsert=True,
    )


async def load_pending_post_state():
    pending = await sync_col.find_one({"_id": "pending_post"})
    if pending:
        pending.pop("_id", None)
    return pending


async def clear_pending_post_state():
    await sync_col.delete_one({"_id": "pending_post"})


async def save_latest_thumbnail(post):
    if post.photo:
        file_id = post.photo[-1].file_id
    elif post.document and (post.document.mime_type or "").startswith("image/"):
        file_id = post.document.file_id
    else:
        return None

    thumb_data = {
        "channel_id": post.chat_id,
        "message_id": post.message_id,
        "file_id": file_id,
        "updated_at": datetime.now(timezone.utc),
    }
    await sync_col.update_one(
        {"_id": "latest_thumbnail"},
        {"$set": thumb_data},
        upsert=True,
    )
    return thumb_data


async def load_latest_thumbnail():
    thumb = await sync_col.find_one({"_id": "latest_thumbnail"})
    if thumb:
        thumb.pop("_id", None)
    return thumb


async def send_post_confirmations(context: ContextTypes.DEFAULT_TYPE, pending: dict, thumb_data: dict, posted_msg_id):
    confirmation_text = (
        "✅ post done\n"
        f"🎬 {pending['name']}\n"
        f"🔑 token: {pending['token']}\n"
        f"🗄 storage_msg_id: {pending['storage_msg_id']}\n"
        f"🖼 thumb_msg_id: {thumb_data.get('message_id', 'n/a')}\n"
        f"📣 post_msg_id: {posted_msg_id if posted_msg_id else 'n/a'}"
    )

    target_channels = []
    if STORAGE_CHANNEL_ID:
        target_channels.append(STORAGE_CHANNEL_ID)
    if THUMBNAIL_CHANNEL_ID and THUMBNAIL_CHANNEL_ID != STORAGE_CHANNEL_ID:
        target_channels.append(THUMBNAIL_CHANNEL_ID)

    for channel_id in target_channels:
        try:
            await context.bot.send_message(chat_id=channel_id, text=confirmation_text)
        except Exception as e:
            logging.warning(f"Failed to send confirmation to {channel_id}: {e}")


async def publish_pending_post(context: ContextTypes.DEFAULT_TYPE, pending: dict, thumb_data: dict):
    link = f"{GATEWAY_URL}?token={pending['token']}"
    caption_text = pending.get('caption') or secrets.choice(CAPTIONS)
    caption = f"{caption_text}\n\nâ± Duration: {pending['duration']}"

    if POST_CHANNEL_ID:
        posted_message = await context.bot.send_photo(
            chat_id=POST_CHANNEL_ID,
            photo=thumb_data['file_id'],
            caption=caption,
            parse_mode="HTML",
            reply_markup=get_channel_kb(link),
        )
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text="âœ… Auto-post complete using latest thumbnail channel image.",
            parse_mode="HTML",
        )
    else:
        posted_message = await context.bot.send_photo(
            chat_id=ADMIN_USER_ID,
            photo=thumb_data['file_id'],
            caption=caption,
            parse_mode="HTML",
            reply_markup=get_channel_kb(link),
        )
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text="âœ… Auto-post prepared and sent to admin because POST_CHANNEL_ID is not set.",
            parse_mode="HTML",
        )

    await sync_col.update_one(
        {"_id": f"post:{pending['token']}"},
        {"$set": {
            "status": "posted",
            "token": pending["token"],
            "file_name": pending["name"],
            "storage_msg_id": pending["storage_msg_id"],
            "thumb_msg_id": thumb_data.get("message_id"),
            "thumb_channel_id": thumb_data.get("channel_id"),
            "thumb_file_id": thumb_data["file_id"],
            "post_channel_id": POST_CHANNEL_ID or ADMIN_USER_ID,
            "post_msg_id": posted_message.message_id,
            "created_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )

    await send_post_confirmations(context, pending, thumb_data, posted_message.message_id)
    _pending_post.pop(ADMIN_USER_ID, None)
    await clear_pending_post_state()
    return posted_message


async def maybe_auto_post_pending(context: ContextTypes.DEFAULT_TYPE):
    async with POST_FLOW_LOCK:
        pending = _pending_post.get(ADMIN_USER_ID)
        if not pending:
            pending = await load_pending_post_state()
            if pending:
                _pending_post[ADMIN_USER_ID] = pending

        if not pending:
            return False

        thumb_data = await load_latest_thumbnail()
        if not thumb_data or not thumb_data.get("file_id"):
            return False

        pending["thumb"] = thumb_data["file_id"]
        pending["thumb_msg_id"] = thumb_data.get("message_id")
        pending["thumb_channel_id"] = thumb_data.get("channel_id")
        pending["caption"] = pending.get("caption") or secrets.choice(CAPTIONS)
        _pending_post[ADMIN_USER_ID] = pending
        await save_pending_post_state(pending)

        try:
            await publish_pending_post(context, pending, thumb_data)
            return True
        except Exception as e:
            logging.exception("Auto-post failed")
            await context.bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=f"âŒ Auto-post failed: {e}",
                parse_mode="HTML",
            )
            return False


async def is_joined(bot: Bot, user_id: int) -> bool:
    """Smart join check: API first, DB fallback if API fails.
    Once a user is verified as joined, save to DB so they
    never get asked again (even if API acts up).

    IMPORTANT: If API check fails (e.g., bot not admin), we trust
    the user's claim to have joined to avoid blocking real users.
    """
    # Step 1: Try Telegram API
    try:
        member = await bot.get_chat_member(
            chat_id=f"@{FORCE_JOIN_CHANNEL}", user_id=user_id
        )
        joined = member.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
        # Save result to DB (cache for next time)
        await users_col.update_one(
            {"user_id": user_id},
            {"$set": {"channel_joined": joined}},
            upsert=True,
        )
        return joined
    except Exception as e:
        logging.warning(f"get_chat_member failed: {e}")
        # API check failed - this usually means:
        # 1. Bot is not admin of the channel
        # 2. Privacy restrictions
        # 3. Rate limiting
        # Don't block user - check DB cache only
        pass

    # Step 2: API failed → check DB cache
    user = await users_col.find_one({"user_id": user_id})
    if user and user.get("channel_joined"):
        return True  # Was verified before, trust the cache

    # API failed AND not in cache - user claims they joined, so trust them
    # This prevents blocking users when bot verification doesn't work
    logging.info(f"User {user_id}: API check failed, trusting user's claim of joining")
    return True


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

    # ── /start <token> → deliver file with force-join check ──
    if context.args:
        token = context.args[0]
        file_data = await files_col.find_one({"token": token})

        if not file_data:
            await update.message.reply_text("❌ Invalid or expired link.")
            return

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

        # Save joined status in DB so we don't ask again
        await users_col.update_one(
            {"user_id": user.id},
            {"$set": {"channel_joined": True}},
            upsert=True,
        )
        await deliver_file(update, context, file_data)
        return

    # ── Normal /start (no token) ──
    if user.id == ADMIN_USER_ID:
        await update.message.reply_text(
            "💎 <b>JSTAR PRO ADMIN PANEL</b>\n\n"
            "📊 Use buttons below or just upload\n"
            "a file to storage to auto-post.",
            reply_markup=admin_kb(),
            parse_mode="HTML",
        )
        return

    # Check DB cache first (instant, no API call)
    user_data = await users_col.find_one({"user_id": user.id})
    if user_data and user_data.get("channel_joined"):
        await update.message.reply_text(
            "👋 Welcome back!\n\nSend me a link to get your file.",
            parse_mode="HTML",
        )
        return

    # Not cached → do full API check
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


# ━━━ DELIVER FILE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

        # Stats - track download with user_id (exclude admin from counting)
        await files_col.update_one({"token": token}, {"$inc": {"total_downloads": 1}})
        await logs_col.insert_one({
            "token": token,
            "user_id": user_id,  # Track user for analytics
            "is_admin": user_id == ADMIN_USER_ID,  # Mark admin downloads
            "time": datetime.now(timezone.utc)
        })

        # Warning + auto-delete after 10 min (send directly to user, not reply)
        warn_msg = await context.bot.send_message(
            chat_id=user_id,
            text="⚠️ <b>Save to Saved Messages now!</b>\n"
                 "This file will be deleted in <b>10 minutes</b>.",
            parse_mode="HTML",
        )
        context.job_queue.run_once(
            auto_delete, 600,
            [user_id, file_msg.message_id, warn_msg.message_id],
            chat_id=user_id,
        )

    except Exception as e:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"❌ <b>Error:</b> {str(e)}",
            parse_mode="HTML"
        )
        logging.error(f"Delivery failed: {e}")


# ━━━ FORCE JOIN CHECK ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def force_join_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """After user clicks 'I've Joined', verify and deliver file."""
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    pending_token = context.user_data.get('pending_token')

    joined = await is_joined(context.bot, user_id)
    if not joined:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Join Channel", url=f"https://t.me/{FORCE_JOIN_CHANNEL}")],
            [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
        ])
        await q.edit_message_text(
            "❌ <b>You haven't joined yet!</b>\n\n"
            "Join the channel first, then click below:",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    # User is joined → save to DB so never asked again
    await users_col.update_one(
        {"user_id": user_id},
        {"$set": {"channel_joined": True}},
        upsert=True,
    )

    if pending_token:
        file_data = await files_col.find_one({"token": pending_token})
        if file_data:
            await q.edit_message_text(
                "✅ <b>Verified!</b> Delivering your file...",
                parse_mode="HTML",
            )
            await deliver_file(update, context, file_data)
            context.user_data.pop('pending_token', None)
            return

    await q.edit_message_text(
        "✅ <b>Welcome!</b>\n\nNow send me your link to get the file.",
        parse_mode="HTML",
    )


# ━━━ ADMIN CALLBACK BUTTONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def admin_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stats":
        # Get current time for today calculations
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # Total links count
        total_links = await files_col.count_documents({})

        # Total users count
        total_users = await users_col.count_documents({})

        # New users today
        new_users_today = await users_col.count_documents({
            "last_seen": {"$gte": today_start}
        })

        # Total downloads (all)
        agg_all = await logs_col.aggregate(
            [{"$group": {"_id": None, "dl": {"$sum": 1}}}]
        ).to_list(1)
        total_dl_all = agg_all[0]['dl'] if agg_all else 0

        # User downloads only (exclude admin)
        agg_users = await logs_col.aggregate([
            {"$match": {"is_admin": {"$ne": True}}},  # Exclude admin
            {"$group": {"_id": None, "dl": {"$sum": 1}}}
        ]).to_list(1)
        total_dl_users = agg_users[0]['dl'] if agg_users else 0

        # Downloads today (user only, exclude admin)
        today_dl = await logs_col.count_documents({
            "time": {"$gte": today_start},
            "is_admin": {"$ne": True}  # Exclude admin
        })

        # New users who joined today (based on last_seen >= today)
        # This is same as new_users_today

        await query.edit_message_text(
            f"📊 <b>DETAILED ANALYTICS</b>\n\n"
            f"👥 Total Users: <code>{total_users}</code>\n"
            f"👥 New Today: <code>{new_users_today}</code>\n"
            f"🔗 Total Links: <code>{total_links}</code>\n"
            f"📥 Downloads (Users): <code>{total_dl_users}</code>\n"
            f"📥 Downloads (All incl. Admin): <code>{total_dl_all}</code>\n"
            f"📅 Downloads Today: <code>{today_dl}</code>",
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STORAGE UPLOAD → AUTO-LINK + ASK THUMBNAIL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def on_storage_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """File uploaded to storage channel → save to DB, send link, ask for thumbnail."""
    post = update.channel_post
    if not post or post.chat_id != STORAGE_CHANNEL_ID:
        return

    att = post.effective_attachment
    if not att or isinstance(att, list):
        return

    # ── Extract file name (fix: video objects may not have file_name) ──
    if post.video:
        file_name = getattr(post.video, 'file_name', None) or "New_Video"
        video_duration = post.video.duration or 0
    elif post.document:
        file_name = getattr(post.document, 'file_name', None) or "New_File"
        video_duration = getattr(post.document, 'duration', None) or 0
    elif post.audio:
        file_name = getattr(post.audio, 'file_name', None) or "New_Audio"
        video_duration = getattr(post.audio, 'duration', None) or 0
    else:
        file_name = "New_Upload"
        video_duration = 0

    # ── Save to DB ──
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
    pending = {
        'token': token,
        'name': file_name,
        'duration': format_duration(video_duration),
        'storage_msg_id': post.message_id,
    }
    _pending_post[ADMIN_USER_ID] = pending
    await save_pending_post_state(pending)

    # ── Send link to admin ──
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                f"🚀 <b>Auto-Link Created!</b>\n\n"
                f"📁 <code>{file_name}</code>\n"
                f"⏱ <code>{format_duration(video_duration)}</code>\n"
                f"🔗 <code>{link}</code>\n\n"
                f"📸 <b>Now send me a thumbnail</b> to create the post!\n"
                f"(or send /skip to post without thumbnail)"
            ),
            parse_mode="HTML",
        )
    except Exception:
        logging.error("Failed to notify admin.")
        return

    # ── Set pending post state — waiting for thumbnail ──
    await context.bot.send_message(
        chat_id=ADMIN_USER_ID,
        text="No need to send a private thumbnail now. I will use the latest image from the thumbnail channel.",
    )

    if not await maybe_auto_post_pending(context):
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=(
                "No thumbnail is available in the thumbnail channel yet.\n"
                "As soon as a new image is posted there, I will auto-post this video.\n"
                "You can still use /skip if you want to post without a thumbnail."
            ),
        )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN SENDS THUMBNAIL (photo in private chat)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def on_thumbnail_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save the latest thumbnail from the dedicated channel and auto-complete any waiting post."""
    post = update.channel_post
    if not post or post.chat_id != THUMBNAIL_CHANNEL_ID:
        return

    thumb_data = await save_latest_thumbnail(post)
    if not thumb_data:
        return

    await sync_col.update_one(
        {"_id": "thumbnail_channel_status"},
        {"$set": {
            "last_thumb_msg_id": thumb_data["message_id"],
            "last_thumb_file_id": thumb_data["file_id"],
            "channel_id": thumb_data["channel_id"],
            "updated_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )

    await maybe_auto_post_pending(context)


async def on_admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin sent a photo — check if there's a pending post waiting for thumbnail."""
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        pending = await load_pending_post_state()
        if pending:
            _pending_post[user_id] = pending
    if not pending:
        return  # No pending post, ignore

    if not update.message.photo:
        return  # Not a photo

    # ── Save thumbnail, generate caption, show preview ──
    pending['thumb'] = update.message.photo[-1].file_id
    pending['caption'] = secrets.choice(CAPTIONS)

    link = f"{GATEWAY_URL}?token={pending['token']}"
    cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"

    preview_msg = await update.message.reply_photo(
        photo=pending['thumb'],
        caption=cap,
        parse_mode="HTML",
        reply_markup=preview_kb(),
    )
    pending['preview_msg_id'] = preview_msg.message_id
    pending['preview_chat_id'] = preview_msg.chat_id


# ━━━ /skip COMMAND — skip thumbnail, post with caption only ━━━

async def skip_thumb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_USER_ID:
        return

    pending = _pending_post.get(user_id)
    if not pending:
        pending = await load_pending_post_state()
        if pending:
            _pending_post[user_id] = pending
    if not pending:
        await update.message.reply_text("❌ No pending post to skip.")
        return

    link = f"{GATEWAY_URL}?token={pending['token']}"
    cap = f"{secrets.choice(CAPTIONS)}\n\n⏱ Duration: {pending['duration']}"

    # Post directly to channel with BOTH buttons
    if POST_CHANNEL_ID:
        try:
            await context.bot.send_message(
                chat_id=POST_CHANNEL_ID,
                text=cap,
                reply_markup=get_channel_kb(link),
                parse_mode="HTML",
            )
            await update.message.reply_text(
                "✅ <b>Posted to channel!</b>",
                parse_mode="HTML",
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID & bot admin rights.",
                parse_mode="HTML",
            )
    else:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"📝 <b>Post:</b>\n\n{cap}",
            reply_markup=get_channel_kb(link),
            parse_mode="HTML",
        )
        await update.message.reply_text(
            "✅ <b>Done!</b> POST_CHANNEL_ID not set — sent to you.\nSet it in Railway to auto-post.",
            parse_mode="HTML",
        )
    await send_post_confirmations(context, pending, {}, None)
    _pending_post.pop(user_id, None)
    await clear_pending_post_state()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  POST PREVIEW CALLBACKS (Send Now / Rotate / New Thumb / Cancel)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def post_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    pending = _pending_post.get(user_id)
    if not pending:
        await q.answer("❌ Session expired.", show_alert=True)
        return

    # ── SEND NOW → post directly to channel ──
    if q.data == "pc_send":
        link = f"{GATEWAY_URL}?token={pending['token']}"
        cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"

        # Remove buttons from preview
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        # Post directly to channel with BOTH buttons
        if POST_CHANNEL_ID:
            try:
                await context.bot.send_photo(
                    chat_id=POST_CHANNEL_ID,
                    photo=pending['thumb'],
                    caption=cap,
                    parse_mode="HTML",
                    reply_markup=get_channel_kb(link),
                )
                await q.message.reply_text(
                    "✅ <b>Posted to channel!</b>",
                    parse_mode="HTML",
                )
            except Exception as e:
                await q.message.reply_text(
                    f"❌ Failed to post: {e}\nCheck POST_CHANNEL_ID & bot admin rights.",
                    parse_mode="HTML",
                )
        else:
            # Fallback: send to admin if POST_CHANNEL_ID not set
            await context.bot.send_photo(
                chat_id=ADMIN_USER_ID,
                photo=pending['thumb'],
                caption=cap,
                parse_mode="HTML",
                reply_markup=get_channel_kb(link),
            )
            await q.message.reply_text(
                "✅ <b>Done!</b> POST_CHANNEL_ID not set — sent to you.\nSet it in Railway to auto-post.",
                parse_mode="HTML",
            )
        await send_post_confirmations(
            context,
            pending,
            {
                "message_id": pending.get("thumb_msg_id"),
                "channel_id": pending.get("thumb_channel_id"),
            },
            None,
        )
        _pending_post.pop(user_id, None)
        await clear_pending_post_state()

    # ── NEW CAPTION ──
    elif q.data == "pc_rot":
        pending['caption'] = secrets.choice(
            [c for c in CAPTIONS if c != pending['caption']]
        )
        cap = f"{pending['caption']}\n\n⏱ Duration: {pending['duration']}"
        try:
            await q.edit_message_caption(
                caption=cap,
                parse_mode="HTML",
                reply_markup=preview_kb(),
            )
        except Exception:
            pass

    # ── NEW THUMBNAIL ──
    elif q.data == "pc_rethumb":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text(
            "🖼 Send me a <b>new thumbnail</b>:\n(or /skip to post without)",
            parse_mode="HTML",
        )

    # ── CANCEL ──
    elif q.data == "pc_cancel":
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await q.message.reply_text("❌ Post cancelled.", parse_mode="HTML")
        _pending_post.pop(user_id, None)
        await clear_pending_post_state()


# ━━━ MAIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("skip", skip_thumb))

    # Force-join verify callback
    app.add_handler(CallbackQueryHandler(force_join_check, pattern="^check_join$"))

    # Admin panel buttons (stats/status/refresh)
    app.add_handler(CallbackQueryHandler(admin_buttons, pattern="^(stats|status|refresh)$"))

    # Post preview buttons (send/rotate/rethumb/cancel)
    app.add_handler(CallbackQueryHandler(post_callback, pattern="^pc_"))

    # Admin sends photo → check if pending post needs thumbnail
    app.add_handler(MessageHandler(
        filters.Chat(THUMBNAIL_CHANNEL_ID) & (filters.PHOTO | filters.Document.IMAGE),
        on_thumbnail_channel_post,
    ))

    # Storage channel upload → auto-link + ask thumbnail
    app.add_handler(MessageHandler(
        filters.Chat(STORAGE_CHANNEL_ID) & (filters.VIDEO | filters.Document.ALL | filters.AUDIO),
        on_storage_upload,
    ))

    print("🚀 JSTAR PRO Bot is Live...")
    app.run_polling()
