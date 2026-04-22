import logging
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, ReturnDocument


LOGGER = logging.getLogger(__name__)
STATE_DOC_ID = "queue_state"
ACTIVE_STATUSES = {"dispatching", "dispatched", "ready_to_publish", "publishing"}
TERMINAL_STATUSES = {"published", "failed"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class QueueStore:
    def __init__(self, mongodb_uri: str, db_name: str):
        self._client = AsyncIOMotorClient(mongodb_uri)
        self._db = self._client[db_name]
        self.queue_posts = self._db["queue_posts"]
        self.queue_state = self._db["queue_state"]

    async def _ensure_state_document(self):
        now = utcnow()
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$setOnInsert": {
                    "createdAt": now,
                    "updatedAt": now,
                    "activePostId": None,
                    "currentCollectingPostId": None,
                }
            },
            upsert=True,
        )

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
            [("intake.media_group_id", ASCENDING)],
            name="queue_media_group_lookup",
            sparse=True,
        )
        await self.queue_posts.create_index(
            [("transport.image_source.message_id", ASCENDING)],
            name="queue_image_source_message_id",
            sparse=True,
        )
        await self.queue_posts.create_index(
            [("transport.storage_video.message_id", ASCENDING)],
            name="queue_storage_video_message_id",
            sparse=True,
        )
        await self.queue_posts.create_index(
            [("transport.edited_image.message_id", ASCENDING)],
            name="queue_edited_image_message_id",
            sparse=True,
        )
        await self._ensure_state_document()

    async def recover_state(self):
        await self._ensure_state_document()
        state = await self.queue_state.find_one({"_id": STATE_DOC_ID})
        active_post_id = (state or {}).get("activePostId")

        if active_post_id and active_post_id != "__dispatching__":
            active_post = await self.queue_posts.find_one({"postId": active_post_id})
            if active_post and active_post.get("status") in ACTIVE_STATUSES:
                return

        recovered = await self.queue_posts.find_one(
            {"status": {"$in": list(ACTIVE_STATUSES)}},
            sort=[("createdAt", ASCENDING)],
        )

        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$set": {
                    "activePostId": recovered["postId"] if recovered else None,
                    "updatedAt": utcnow(),
                }
            },
            upsert=True,
        )

        if recovered:
            LOGGER.info("Recovered active post from MongoDB | postId=%s", recovered["postId"])

    async def get_current_collecting_post_id(self) -> str | None:
        await self._ensure_state_document()
        state = await self.queue_state.find_one({"_id": STATE_DOC_ID}, {"currentCollectingPostId": 1})
        if not state:
            return None
        return state.get("currentCollectingPostId")

    async def set_current_collecting_post_id(self, post_id: str | None):
        await self._ensure_state_document()
        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {
                "$set": {
                    "currentCollectingPostId": post_id,
                    "updatedAt": utcnow(),
                },
                "$setOnInsert": {"createdAt": utcnow(), "activePostId": None},
            },
            upsert=True,
        )

    async def get_active_post_id(self) -> str | None:
        await self._ensure_state_document()
        state = await self.queue_state.find_one({"_id": STATE_DOC_ID}, {"activePostId": 1})
        if not state:
            return None
        return state.get("activePostId")

    async def get_post(self, post_id: str):
        return await self.queue_posts.find_one({"postId": post_id})

    async def find_post_by_media_group(self, media_group_id: str | None):
        if not media_group_id:
            return None
        return await self.queue_posts.find_one({"intake.media_group_id": media_group_id})

    async def upsert_post_label(
        self,
        post_id: str,
        text_message_id: int | None,
        source_chat_id: int,
        media_group_id: str | None = None,
    ):
        now = utcnow()
        existing = await self.queue_posts.find_one({"postId": post_id})
        if existing and existing.get("status") not in {"collecting", "queued"}:
            LOGGER.warning(
                "Duplicate postId ignored because record is already active or finished | postId=%s | status=%s",
                post_id,
                existing.get("status"),
            )
            return existing, False

        set_fields = {
            "sourceChatId": source_chat_id,
            "updatedAt": now,
        }
        if text_message_id is not None:
            set_fields["intake.text_message_id"] = text_message_id
        if media_group_id:
            set_fields["intake.media_group_id"] = media_group_id

        document = await self.queue_posts.find_one_and_update(
            {"postId": post_id},
            {
                "$setOnInsert": {
                    "postId": post_id,
                    "status": "collecting",
                    "createdAt": now,
                    "intake": {
                        "text_message_id": text_message_id,
                        "media_group_id": media_group_id,
                        "video": None,
                        "image": None,
                    },
                    "transport": {
                        "storage_video": None,
                        "image_source": None,
                        "edited_image": None,
                    },
                    "publish": {},
                    "lastError": None,
                },
                "$set": set_fields,
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        await self.set_current_collecting_post_id(post_id)
        return document, existing is None

    async def attach_media(self, post_id: str, source_chat_id: int, media):
        update_fields = {
            f"intake.{media.kind}": {
                "file_id": media.file_id,
                "message_id": media.source_message_id,
                "media_group_id": media.media_group_id,
                "mime_type": media.mime_type,
                "file_name": media.file_name,
                "duration": media.duration,
                "send_method": media.send_method,
            },
            "sourceChatId": source_chat_id,
            "updatedAt": utcnow(),
        }
        if media.media_group_id:
            update_fields["intake.media_group_id"] = media.media_group_id

        return await self.queue_posts.find_one_and_update(
            {"postId": post_id},
            {"$set": update_fields},
            return_document=ReturnDocument.AFTER,
        )

    async def mark_queued_if_complete(self, post_id: str):
        post = await self.get_post(post_id)
        if not post:
            return None, False

        intake = post.get("intake") or {}
        is_complete = bool(intake.get("video") and intake.get("image"))
        if not is_complete:
            return post, False

        if post.get("status") == "collecting":
            post = await self.queue_posts.find_one_and_update(
                {"postId": post_id, "status": "collecting"},
                {"$set": {"status": "queued", "updatedAt": utcnow()}},
                return_document=ReturnDocument.AFTER,
            )
            current_collecting = await self.get_current_collecting_post_id()
            if current_collecting == post_id:
                await self.set_current_collecting_post_id(None)
            return post, True

        return post, post.get("status") == "queued"

    async def _release_active_if_terminal(self):
        active_post_id = await self.get_active_post_id()
        if not active_post_id or active_post_id == "__dispatching__":
            return

        active_post = await self.get_post(active_post_id)
        if active_post and active_post.get("status") not in TERMINAL_STATUSES:
            return

        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID, "activePostId": active_post_id},
            {"$set": {"activePostId": None, "updatedAt": utcnow()}},
        )

    async def claim_next_post_for_dispatch(self):
        await self._ensure_state_document()
        await self._release_active_if_terminal()

        state = await self.queue_state.find_one_and_update(
            {"_id": STATE_DOC_ID, "activePostId": None},
            {"$set": {"activePostId": "__dispatching__", "updatedAt": utcnow()}},
            return_document=ReturnDocument.AFTER,
        )
        if not state or state.get("activePostId") != "__dispatching__":
            return None

        post = await self.queue_posts.find_one_and_update(
            {"status": "queued", "intake.video": {"$ne": None}, "intake.image": {"$ne": None}},
            {"$set": {"status": "dispatching", "dispatchClaimedAt": utcnow(), "updatedAt": utcnow()}},
            sort=[("createdAt", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )
        if not post:
            await self.queue_state.update_one(
                {"_id": STATE_DOC_ID},
                {"$set": {"activePostId": None, "updatedAt": utcnow()}},
            )
            return None

        await self.queue_state.update_one(
            {"_id": STATE_DOC_ID},
            {"$set": {"activePostId": post["postId"], "updatedAt": utcnow()}},
        )
        return post

    async def mark_dispatched(self, post_id: str, storage_video: dict, image_source: dict):
        await self.queue_posts.update_one(
            {"postId": post_id},
            {
                "$set": {
                    "status": "dispatched",
                    "transport.storage_video": storage_video,
                    "transport.image_source": image_source,
                    "lastError": None,
                    "dispatchedAt": utcnow(),
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

    async def clear_queue(self):
        result = await self.queue_posts.delete_many({"status": {"$nin": ["published"]}})
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
