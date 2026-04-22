import logging

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from .config import Settings
from .db import QueueStore
from .intake import (
    detect_intake_media,
    extract_confirmation_details,
    extract_post_id_from_message,
)
from .telegram_service import TelegramQueueSender


LOGGER = logging.getLogger(__name__)


class QueueControllerBot:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = QueueStore(settings.mongodb_uri, settings.mongo_db_name)
        self.sender = TelegramQueueSender(
            settings.video_storage_channel_id,
            settings.image_channel_id,
        )

    def is_admin(self, user_id: int) -> bool:
        return self.settings.admin_user_id == 0 or user_id == self.settings.admin_user_id

    async def post_init(self, application):
        await self.store.ensure_indexes()
        await self.store.recover_state()
        await self.dispatch_next(application)

    async def dispatch_next(self, application):
        post = await self.store.claim_next_pending_post()
        if not post:
            return False

        try:
            sent = await self.sender.send_post_to_storage(application.bot, post)
            await self.store.save_storage_message_ids(
                post["postId"],
                sent["video_message_id"],
                sent["image_message_id"],
            )
            LOGGER.info("Post sent to storage | postId=%s", post["postId"])
            return True
        except Exception as exc:
            LOGGER.exception("Telegram API send failure | postId=%s", post["postId"])
            await self.store.mark_failed(post["postId"], str(exc))
            return False

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.effective_user:
            return
        if not update.message:
            return

        if self.is_admin(update.effective_user.id):
            active_post_id = await self.store.get_active_post_id()
            await update.message.reply_text(
                "Queue Controller Bot is active.\n\n"
                f"Intake Channel: {self.settings.intake_channel_id}\n"
                f"Video Storage Channel: {self.settings.video_storage_channel_id}\n"
                f"Image Channel: {self.settings.image_channel_id}\n"
                f"Video Confirmation Chat: {self.settings.video_confirmation_chat_id}\n"
                f"Image Confirmation Chat: {self.settings.image_confirmation_chat_id}\n"
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
            f"Pending: {counts.get('pending', 0)}\n"
            f"Waiting Confirmation: {counts.get('waiting_confirmation', 0)}\n"
            f"Confirmed: {counts.get('confirmed', 0)}\n"
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
            f"Queue reset complete. Removed {removed} non-confirmed posts."
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
            LOGGER.info(
                "postId received | postId=%s | textMessageId=%s",
                post_id,
                message.message_id,
            )
            await self.store.upsert_post_label(post_id, message.message_id, message.chat_id)
            await self.dispatch_next(context.application)
            return

        if not media:
            return

        target_post_id = post_id or await self.store.get_current_collecting_post_id()
        if not target_post_id:
            LOGGER.warning(
                "Media ignored because no postId is active | kind=%s | messageId=%s",
                media.kind,
                message.message_id,
            )
            return

        if post_id:
            await self.store.upsert_post_label(target_post_id, None, message.chat_id)

        if media.media_group_id:
            LOGGER.info(
                "media group detected | postId=%s | mediaGroupId=%s | kind=%s",
                target_post_id,
                media.media_group_id,
                media.kind,
            )

        if media.kind == "video" and media.mime_type and media.mime_type.startswith("video/") and message.document:
            LOGGER.info(
                "document-as-video received | postId=%s | messageId=%s | mimeType=%s",
                target_post_id,
                media.source_message_id,
                media.mime_type,
            )
        elif media.kind == "video":
            LOGGER.info("video received | postId=%s | messageId=%s", target_post_id, media.source_message_id)
        else:
            LOGGER.info("image received | postId=%s | messageId=%s", target_post_id, media.source_message_id)

        document = await self.store.attach_media(target_post_id, message.chat_id, media)
        if not document:
            LOGGER.warning(
                "Failed to attach media because post does not exist | postId=%s | kind=%s",
                target_post_id,
                media.kind,
            )
            return

        post, became_ready = await self.store.mark_pending_if_complete(target_post_id)
        if became_ready:
            LOGGER.info("post marked complete | postId=%s", target_post_id)
        elif post:
            if not post.get("videoFileId"):
                LOGGER.info("missing video | postId=%s", target_post_id)
            if not post.get("imageFileId"):
                LOGGER.info("missing image | postId=%s", target_post_id)

        await self.dispatch_next(context.application)

    async def confirmation_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.channel_post
        if not message or message.chat_id not in {
            self.settings.video_confirmation_chat_id,
            self.settings.image_confirmation_chat_id,
        } or not message.text:
            return

        post_id, explicit_kind = extract_confirmation_details(message.text)
        if not post_id:
            LOGGER.warning("invalid confirmation message ignored | text=%s", message.text[:200])
            return

        confirmation_kind = explicit_kind
        if not confirmation_kind:
            if message.chat_id == self.settings.video_confirmation_chat_id:
                confirmation_kind = "video"
            elif message.chat_id == self.settings.image_confirmation_chat_id:
                confirmation_kind = "image"

        LOGGER.info(
            "confirmation received | postId=%s | kind=%s | messageId=%s",
            post_id,
            confirmation_kind,
            message.message_id,
        )
        post = await self.store.confirm_post(post_id, confirmation_kind)
        if not post:
            LOGGER.warning("Confirmation ignored because post is not waiting | postId=%s", post_id)
            return

        if post.get("status") == "confirmed":
            LOGGER.info("post fully confirmed | postId=%s", post_id)
        else:
            if not post.get("videoConfirmed"):
                LOGGER.info("waiting for video confirmation | postId=%s", post_id)
            if not post.get("imageConfirmed"):
                LOGGER.info("waiting for image confirmation | postId=%s", post_id)

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

    confirmation_filter = filters.Chat(
        [settings.video_confirmation_chat_id, settings.image_confirmation_chat_id]
    ) & filters.TEXT
    application.add_handler(MessageHandler(confirmation_filter, controller.confirmation_handler))
    application.add_error_handler(controller.error_handler)

    return application
