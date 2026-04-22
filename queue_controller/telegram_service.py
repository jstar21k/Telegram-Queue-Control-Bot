import logging


LOGGER = logging.getLogger(__name__)


class TelegramQueueSender:
    def __init__(self, video_storage_channel_id: int, image_channel_id: int):
        self.video_storage_channel_id = video_storage_channel_id
        self.image_channel_id = image_channel_id

    async def send_post_to_storage(self, bot, post: dict) -> dict:
        post_id = post["postId"]

        LOGGER.info(
            "Sending video to storage | postId=%s | fileId=%s | storageChat=%s",
            post_id,
            post["videoFileId"],
            self.video_storage_channel_id,
        )
        video_message = await bot.send_video(
            chat_id=self.video_storage_channel_id,
            video=post["videoFileId"],
            caption=post_id,
        )

        LOGGER.info(
            "Sending image to image channel | postId=%s | fileId=%s | imageChat=%s",
            post_id,
            post["imageFileId"],
            self.image_channel_id,
        )
        image_message = await bot.send_photo(
            chat_id=self.image_channel_id,
            photo=post["imageFileId"],
            caption=post_id,
        )

        LOGGER.info(
            "Post sent to destinations | postId=%s | videoMessageId=%s | imageMessageId=%s",
            post_id,
            video_message.message_id,
            image_message.message_id,
        )
        return {
            "video_message_id": video_message.message_id,
            "image_message_id": image_message.message_id,
        }
