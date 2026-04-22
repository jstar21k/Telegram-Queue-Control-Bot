import logging

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from .config import Settings
from .db import QueueStore
from .intake import detect_intake_media, extract_post_id_from_message
from .telegram_service import TelegramQueueSender


LOGGER = logging.getLogger(__name__)
PUBLISHED_CLEANUP_INTERVAL_SECONDS = 3600


class QueueControllerBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = QueueStore(settings.mongodb_uri, settings.mongo_db_name)
        self.sender = TelegramQueueSender(
            settings.storage_channel_id,
            settings.image_source_channel_id,
        )

    def is_admin(self, user_id: int) -> bool:
        return self.settings.admin_user_id == 0 or user_id == self.settings.admin_user_id

    async def post_init(self, application):
        await self.store.ensure_indexes()
        await self.store.recover_state()
        application.job_queue.run_repeating(
            self.dispatch_tick,
            interval=self.settings.dispatch_interval_seconds,
            first=1,
            name="queue-dispatch-loop",
        )
        application.job_queue.run_repeating(
            self.cleanup_tick,
            interval=PUBLISHED_CLEANUP_INTERVAL_SECONDS,
            first=60,
            name="queue-published-cleanup",
        )
        await self.cleanup_published_records()
        await self.dispatch_next(application)

    async def dispatch_tick(self, context: ContextTypes.DEFAULT_TYPE):
        await self.dispatch_next(context.application)

    async def cleanup_tick(self, context: ContextTypes.DEFAULT_TYPE):
        await self.cleanup_published_records()

    async def cleanup_published_records(self):
        removed = await self.store.cleanup_published_posts()
        if removed:
            LOGGER.info("Cleaned up published queue posts | removed=%s", removed)

    async def dispatch_next(self, application):
        post = await self.store.claim_next_post_for_dispatch()
        if not post:
            return False

        try:
            sent = await self.sender.send_post_to_channels(application.bot, post)
            await self.store.mark_dispatched(
                post["postId"],
                sent["storage_video"],
                sent["image_source"],
            )
            LOGGER.info("Active post dispatched | postId=%s", post["postId"])
            return True
        except Exception as exc:
            LOGGER.exception("Dispatch failed | postId=%s", post["postId"])
            await self.store.mark_failed(post["postId"], str(exc))
            return False

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or not update.message:
            return

        active_post_id = await self.store.get_active_post_id()
        current_collecting = await self.store.get_current_collecting_post_id()

        if self.is_admin(update.effective_user.id):
            await update.message.reply_text(
                "Queue Controller Bot is active.\n\n"
                f"Intake Channel: {self.settings.intake_channel_id}\n"
                f"Storage Channel: {self.settings.storage_channel_id}\n"
                f"Image Source Channel: {self.settings.image_source_channel_id}\n"
                f"Current Collecting: {current_collecting or 'none'}\n"
                f"Active Post: {active_post_id or 'none'}"
            )
            return

        await update.message.reply_text("Queue Controller Bot is running.")

    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or not update.message:
            return
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("You are not authorized to use this command.")
            return

        counts = await self.store.get_status_counts()
        active_post_id = await self.store.get_active_post_id()
        current_collecting = await self.store.get_current_collecting_post_id()
        await update.message.reply_text(
            "Queue Status\n\n"
            f"Collecting: {counts.get('collecting', 0)}\n"
            f"Queued: {counts.get('queued', 0)}\n"
            f"Dispatching: {counts.get('dispatching', 0)}\n"
            f"Dispatched: {counts.get('dispatched', 0)}\n"
            f"Ready To Publish: {counts.get('ready_to_publish', 0)}\n"
            f"Publishing: {counts.get('publishing', 0)}\n"
            f"Published: {counts.get('published', 0)}\n"
            f"Failed: {counts.get('failed', 0)}\n"
            f"Current Collecting: {current_collecting or 'none'}\n"
            f"Active Post: {active_post_id or 'none'}"
        )

    async def queue_clear_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user or not update.message:
            return
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("You are not authorized to use this command.")
            return

        removed = await self.store.clear_queue()
        await update.message.reply_text(
            f"Queue reset complete. Removed {removed} non-published posts."
        )

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        LOGGER.exception("Unhandled Telegram update error", exc_info=context.error)

    async def intake_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.channel_post
        if not message or message.chat_id != self.settings.intake_channel_id:
            return

        post_id = extract_post_id_from_message(message)
        media = detect_intake_media(message)

        if post_id and not media:
            LOGGER.info("postId label received | postId=%s | messageId=%s", post_id, message.message_id)
            await self.store.upsert_post_label(
                post_id,
                message.message_id,
                message.chat_id,
                getattr(message, "media_group_id", None),
            )
            await self.dispatch_next(context.application)
            return

        if not media:
            return

        target_post_id = post_id
        if not target_post_id and media.media_group_id:
            existing = await self.store.find_post_by_media_group(media.media_group_id)
            if existing:
                target_post_id = existing["postId"]
        if not target_post_id:
            target_post_id = await self.store.get_current_collecting_post_id()

        if not target_post_id:
            LOGGER.warning(
                "Media ignored because no postId is available | kind=%s | messageId=%s",
                media.kind,
                media.source_message_id,
            )
            return

        if post_id:
            await self.store.upsert_post_label(
                target_post_id,
                None,
                message.chat_id,
                media.media_group_id,
            )

        LOGGER.info(
            "Attaching media to post | postId=%s | kind=%s | messageId=%s",
            target_post_id,
            media.kind,
            media.source_message_id,
        )
        document = await self.store.attach_media(target_post_id, message.chat_id, media)
        if not document:
            LOGGER.warning(
                "Media ignored because target post does not exist | postId=%s | kind=%s",
                target_post_id,
                media.kind,
            )
            return

        post, became_ready = await self.store.mark_queued_if_complete(target_post_id)
        if became_ready:
            LOGGER.info("Post is fully collected and queued | postId=%s", target_post_id)
        elif post:
            if not (post.get("intake") or {}).get("video"):
                LOGGER.info("Waiting for video | postId=%s", target_post_id)
            if not (post.get("intake") or {}).get("image"):
                LOGGER.info("Waiting for image | postId=%s", target_post_id)

        await self.dispatch_next(context.application)


def build_application(settings: Settings):
    controller = QueueControllerBot(settings)
    application = ApplicationBuilder().token(settings.bot_token).post_init(controller.post_init).build()

    application.add_handler(CommandHandler("start", controller.start_command))
    application.add_handler(CommandHandler("status", controller.status_command))
    application.add_handler(CommandHandler("queueclear", controller.queue_clear_command))

    intake_filter = filters.Chat(settings.intake_channel_id) & (
        filters.TEXT | filters.PHOTO | filters.VIDEO | filters.Document.ALL
    )
    application.add_handler(MessageHandler(intake_filter, controller.intake_handler))
    application.add_error_handler(controller.error_handler)

    return application
