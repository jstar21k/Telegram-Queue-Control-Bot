from dataclasses import dataclass
import re


EXPLICIT_POST_ID_RE = re.compile(
    r"\bpost[_\s-]*id\s*[:=]\s*([A-Za-z0-9_-]+)\b",
    re.IGNORECASE,
)
GENERIC_POST_ID_RE = re.compile(r"\b(post[_-]?[A-Za-z0-9_-]+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class IntakeMedia:
    kind: str
    file_id: str
    source_message_id: int
    media_group_id: str | None
    mime_type: str | None = None
    file_name: str | None = None
    duration: int | None = None
    send_method: str = "send_document"


def canonicalize_post_id(value: str) -> str:
    cleaned = re.sub(r"\s+", "_", value.strip())
    if not cleaned:
        return cleaned
    if not cleaned.upper().startswith("POST"):
        cleaned = f"POST_{cleaned}"
    return cleaned.upper()


def normalize_post_id(raw_text: str | None) -> str | None:
    if not raw_text:
        return None

    explicit_match = EXPLICIT_POST_ID_RE.search(raw_text)
    if explicit_match:
        return canonicalize_post_id(explicit_match.group(1))

    generic_match = GENERIC_POST_ID_RE.search(raw_text)
    if generic_match:
        return canonicalize_post_id(generic_match.group(1))

    return None


def extract_post_id_from_message(message) -> str | None:
    candidates = [
        message.text or "",
        message.caption or "",
        getattr(message.video, "file_name", "") or "",
        getattr(message.document, "file_name", "") or "",
    ]
    for candidate in candidates:
        post_id = normalize_post_id(candidate)
        if post_id:
            return post_id
    return None


def build_transport_caption(post_id: str, asset_type: str) -> str:
    return f"postId: {post_id}\nasset: {asset_type}"


def detect_intake_media(message) -> IntakeMedia | None:
    if message.video:
        return IntakeMedia(
            kind="video",
            file_id=message.video.file_id,
            source_message_id=message.message_id,
            media_group_id=message.media_group_id,
            mime_type="video/mp4",
            file_name=getattr(message.video, "file_name", None),
            duration=getattr(message.video, "duration", None),
            send_method="send_video",
        )

    if message.photo:
        return IntakeMedia(
            kind="image",
            file_id=message.photo[-1].file_id,
            source_message_id=message.message_id,
            media_group_id=message.media_group_id,
            mime_type="image/jpeg",
            file_name=None,
            duration=None,
            send_method="send_photo",
        )

    if message.document:
        mime_type = (message.document.mime_type or "").lower()
        if mime_type.startswith("video/"):
            return IntakeMedia(
                kind="video",
                file_id=message.document.file_id,
                source_message_id=message.message_id,
                media_group_id=message.media_group_id,
                mime_type=mime_type,
                file_name=getattr(message.document, "file_name", None),
                duration=getattr(message.document, "duration", None),
                send_method="send_document",
            )

        if mime_type.startswith("image/"):
            return IntakeMedia(
                kind="image",
                file_id=message.document.file_id,
                source_message_id=message.message_id,
                media_group_id=message.media_group_id,
                mime_type=mime_type,
                file_name=getattr(message.document, "file_name", None),
                duration=None,
                send_method="send_document",
            )

    return None
