import io
import logging

from .intake import build_transport_caption


LOGGER = logging.getLogger(__name__)


def _extract_media_payload(message) -> dict:
    if getattr(message, "video", None):
        return {
            "channel_id": message.chat_id,
            "message_id": message.message_id,
            "file_id": message.video.file_id,
            "mime_type": "video/mp4",
            "file_name": getattr(message.video, "file_name", None),
            "duration": getattr(message.video, "duration", None),
            "transport_type": "video",
        }

    if getattr(message, "photo", None):
        return {
            "channel_id": message.chat_id,
            "message_id": message.message_id,
            "file_id": message.photo[-1].file_id,
            "mime_type": "image/jpeg",
            "file_name": None,
            "duration": None,
            "transport_type": "photo",
        }

    if getattr(message, "document", None):
        return {
            "channel_id": message.chat_id,
            "message_id": message.message_id,
            "file_id": message.document.file_id,
            "mime_type": message.document.mime_type,
            "file_name": getattr(message.document, "file_name", None),
            "duration": getattr(message.document, "duration", None),
            "transport_type": "document",
        }

    return {
        "channel_id": message.chat_id,
        "message_id": message.message_id,
        "file_id": None,
        "mime_type": None,
        "file_name": None,
        "duration": None,
        "transport_type": "unknown",
    }


class TelegramQueueSender:
    def __init__(self, storage_channel_id: int, image_source_channel_id: int):
        self.storage_channel_id = storage_channel_id
        self.image_source_channel_id = image_source_channel_id

    async def _download_media_bytes(self, bot, file_id: str) -> bytes | None:
        telegram_file = await bot.get_file(file_id)
        buffer = io.BytesIO()
        await telegram_file.download_to_memory(buffer)
        return buffer.getvalue()

    async def _send_media(self, bot, target_chat_id: int, media: dict, caption: str):
        send_method = media.get("send_method", "send_document")
        file_id = media["file_id"]

        if send_method == "send_video":
            return await bot.send_video(chat_id=target_chat_id, video=file_id, caption=caption)

        if send_method == "send_photo":
            return await bot.send_photo(chat_id=target_chat_id, photo=file_id, caption=caption)

        return await bot.send_document(chat_id=target_chat_id, document=file_id, caption=caption)

    async def send_post_to_channels(self, bot, post: dict) -> dict:
        post_id = post["postId"]
        intake = post["intake"]

        LOGGER.info(
            "Dispatching queued post | postId=%s | storage=%s | image_source=%s",
            post_id,
            self.storage_channel_id,
            self.image_source_channel_id,
        )

        video_message = await self._send_media(
            bot,
            self.storage_channel_id,
            intake["video"],
            build_transport_caption(post_id, "video"),
        )
        raw_image_message = await self._send_media(
            bot,
            self.image_source_channel_id,
            intake["image"],
            build_transport_caption(post_id, "raw_image"),
        )
        raw_image_payload = _extract_media_payload(raw_image_message)
        try:
            raw_image_payload["raw_bytes"] = await self._download_media_bytes(
                bot,
                intake["image"]["file_id"],
            )
            raw_image_payload["raw_mime_type"] = intake["image"].get("mime_type")
            raw_image_payload["raw_file_name"] = intake["image"].get("file_name")
        except Exception:
            LOGGER.exception("Failed to download raw image bytes for workflow handoff | postId=%s", post_id)
            raw_image_payload["raw_bytes"] = None
            raw_image_payload["raw_mime_type"] = intake["image"].get("mime_type")
            raw_image_payload["raw_file_name"] = intake["image"].get("file_name")

        LOGGER.info(
            "Queued post dispatched | postId=%s | storage_video_message_id=%s | image_source_message_id=%s",
            post_id,
            video_message.message_id,
            raw_image_message.message_id,
        )
        return {
            "storage_video": _extract_media_payload(video_message),
            "image_source": raw_image_payload,
        }
