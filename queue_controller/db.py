import logging
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError

from .intake import IntakeMedia


LOGGER = logging.getLogger(__name__)
STATE_DOC_ID = "queue_state"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class QueueStore:
    def __init__(self, mongodb_uri: str, db_name: str):
        self._client = AsyncIOMotorClient(mongodb_uri)
        self._db = self._client[db_name]
        self.queue_posts = self._db["queue_posts"]
        self.queue_state = self._db["queue_state"]

    async def ensure_indexes(self):
        await self.queue_posts.create_index(
            [("postId", ASCENDING)],
            unique=True,
            name="queue_post_id_unique",
        )
        await self.queue_posts.create_index(
            [("status", ASCENDING), ("createdAt", ASCENDING)],
            name="queue_status_created_at",
        )
        await self.queue_posts.create_index(
            [("mediaGroupId", ASCENDING)],
            name="queue_media_group",
            sparse=True,
        )

    async def recover_state(self):
        waiting_post = await self.queue_posts.find_one(
            {"status": "waiting_confirmation"},
            sort=[("sentAt", ASCENDING), ("createdAt", ASCENDING)],
        )
        active_post_id = waiting_post["postId"] if waiting_post else None

        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$set": {
                    "activePostId": active_post_id,
                    "updatedAt": utcnow(),
                },
                "$setOnInsert": {
                    "currentCollectingPostId": None,
                    "createdAt": utcnow(),
                },
            },
            upsert=True,
        )

        if waiting_post:
            LOGGER.info("Recovered active post from MongoDB | postId=%s", active_post_id)

    async def get_current_collecting_post_id(self) -> str | None:
        state = await self.queue_state.find_one({"_id": STATE_DOC_ID}, {"currentCollectingPostId": 1})
        if not state:
            return None
        return state.get("currentCollectingPostId")

    async def set_current_collecting_post_id(self, post_id: str | None):
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$set": {
                    "currentCollectingPostId": post_id,
                    "updatedAt": utcnow(),
                },
                "$setOnInsert": {
                    "createdAt": utcnow(),
                    "activePostId": None,
                },
            },
            upsert=True,
        )

    async def upsert_post_label(self, post_id: str, text_message_id: int | None, source_chat_id: int):
        now = utcnow()
        existing = await self.queue_posts.find_one({"postId": post_id})
        if existing and existing.get("status") not in {"collecting", "pending"}:
            LOGGER.warning(
                "Duplicate postId ignored | postId=%s | status=%s",
                post_id,
                existing.get("status"),
            )
            return existing, False

        document = await self.queue_posts.find_one_and_update(
            {"postId": post_id},
            {
                "$setOnInsert": {
                    "postId": post_id,
                    "status": "collecting",
                    "createdAt": now,
                    "videoFileId": None,
                    "imageFileId": None,
                    "videoMessageId": None,
                    "imageMessageId": None,
                    "mediaGroupId": None,
                    "sentAt": None,
                    "confirmedAt": None,
                },
                "$set": {
                    "sourceChatId": source_chat_id,
                    "updatedAt": now,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if text_message_id is not None:
            await self.queue_posts.update_one(
                {"postId": post_id},
                {"$set": {"textMessageId": text_message_id, "updatedAt": now}},
            )
            document["textMessageId"] = text_message_id
        await self.set_current_collecting_post_id(post_id)
        created = existing is None
        return document, created

    async def attach_media(self, post_id: str, source_chat_id: int, media: IntakeMedia):
        field_map = {
            "video": ("videoFileId", "videoMessageId"),
            "image": ("imageFileId", "imageMessageId"),
        }
        file_field, msg_field = field_map[media.kind]

        document = await self.queue_posts.find_one_and_update(
            {"postId": post_id},
            {
                "$set": {
                    file_field: media.file_id,
                    msg_field: media.source_message_id,
                    "mediaGroupId": media.media_group_id,
                    "sourceChatId": source_chat_id,
                    "updatedAt": utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return document

    async def mark_pending_if_complete(self, post_id: str):
        post = await self.queue_posts.find_one({"postId": post_id})
        if not post:
            return None, False

        is_complete = bool(
            post.get("textMessageId")
            and post.get("videoFileId")
            and post.get("imageFileId")
        )
        if not is_complete:
            return post, False

        if post.get("status") == "collecting":
            post = await self.queue_posts.find_one_and_update(
                {"postId": post_id, "status": "collecting"},
                {"$set": {"status": "pending", "updatedAt": utcnow()}},
                return_document=ReturnDocument.AFTER,
            )
            current_collecting = await self.get_current_collecting_post_id()
            if current_collecting == post_id:
                await self.set_current_collecting_post_id(None)
            return post, True

        return post, post.get("status") == "pending"

    async def claim_next_pending_post(self):
        now = utcnow()
        state = await self.queue_state.find_one_and_update(
            {
                "_id": STATE_DOC_ID,
                "$or": [
                    {"activePostId": None},
                    {"activePostId": {"$exists": False}},
                ],
            },
            {
                "$set": {
                    "activePostId": "__dispatching__",
                    "updatedAt": now,
                },
                "$setOnInsert": {
                    "createdAt": now,
                    "currentCollectingPostId": None,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

        if not state or state.get("activePostId") != "__dispatching__":
            return None

        post = await self.queue_posts.find_one_and_update(
            {
                "status": "pending",
                "textMessageId": {"$ne": None},
                "videoFileId": {"$ne": None},
                "imageFileId": {"$ne": None},
            },
            {
                "$set": {
                    "status": "waiting_confirmation",
                    "sentAt": now,
                    "updatedAt": now,
                }
            },
            sort=[("createdAt", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

        if not post:
            await self.queue_state.update_one(
                {"_id": STATE_DOC_ID},
                {"$set": {"activePostId": None, "updatedAt": utcnow()}},
                upsert=True,
            )
            return None

        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {"$set": {"activePostId": post["postId"], "updatedAt": utcnow()}},
            upsert=True,
        )
        return post

    async def save_storage_message_ids(self, post_id: str, video_message_id: int, image_message_id: int):
        await self.queue_posts.update_one(
            {"postId": post_id},
            {
                "$set": {
                    "storageVideoMessageId": video_message_id,
                    "storageImageMessageId": image_message_id,
                    "updatedAt": utcnow(),
                }
            },
        )

    async def mark_failed(self, post_id: str, reason: str):
        await self.queue_posts.update_one(
            {"postId": post_id},
            {
                "$set": {
                    "status": "failed",
                    "lastError": reason[:1000],
                    "updatedAt": utcnow(),
                }
            },
        )
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID, "activePostId": post_id},
            {"$set": {"activePostId": None, "updatedAt": utcnow()}},
            upsert=True,
        )

    async def confirm_post(self, post_id: str):
        post = await self.queue_posts.find_one({"postId": post_id})
        if not post:
            return None

        if post.get("status") == "confirmed":
            await self.queue_state.update_one(
                {"_id": STATE_DOC_ID, "activePostId": post_id},
                {"$set": {"activePostId": None, "updatedAt": utcnow()}},
                upsert=True,
            )
            return post

        if post.get("status") != "waiting_confirmation":
            return None

        confirmed_post = await self.queue_posts.find_one_and_update(
            {"postId": post_id, "status": "waiting_confirmation"},
            {
                "$set": {
                    "status": "confirmed",
                    "confirmedAt": utcnow(),
                    "updatedAt": utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID, "activePostId": post_id},
            {"$set": {"activePostId": None, "updatedAt": utcnow()}},
            upsert=True,
        )
        return confirmed_post

    async def clear_queue(self):
        result = await self.queue_posts.delete_many({"status": {"$ne": "confirmed"}})
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$set": {
                    "activePostId": None,
                    "currentCollectingPostId": None,
                    "updatedAt": utcnow(),
                },
                "$setOnInsert": {"createdAt": utcnow()},
            },
            upsert=True,
        )
        return result.deleted_count

    async def get_status_counts(self):
        counts = {}
        cursor = self.queue_posts.aggregate(
            [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
        )
        async for row in cursor:
            counts[row["_id"]] = row["count"]
        return counts

    async def get_active_post_id(self):
        state = await self.queue_state.find_one({"_id": STATE_DOC_ID}, {"activePostId": 1})
        if not state:
            return None
        return state.get("activePostId")
