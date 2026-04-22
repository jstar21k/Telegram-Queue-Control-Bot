from dataclasses import dataclass
import re


POST_ID_RE = re.compile(r"\bpost[\s_-]*0*(\d+)\b", re.IGNORECASE)
CONFIRMATION_RE = re.compile(
    r"^\s*(post[\s_-]*\d+)(?:\s+(video|image))?\s+done\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class IntakeMedia:
    kind: str
    file_id: str
    source_message_id: int
    media_group_id: str | None
    mime_type: str | None = None


def normalize_post_id(raw_text: str | None) -> str | None:
    if not raw_text:
        return None

    match = POST_ID_RE.search(raw_text)
    if not match:
        return None
    return f"POST_{int(match.group(1)):03d}"


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


def extract_confirmation_post_id(text: str | None) -> str | None:
    if not text:
        return None

    match = CONFIRMATION_RE.search(text)
    if not match:
        return None
    return normalize_post_id(match.group(1))


def extract_confirmation_details(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None

    match = CONFIRMATION_RE.search(text)
    if not match:
        return None, None

    post_id = normalize_post_id(match.group(1))
    confirmation_kind = (match.group(2) or "").lower() or None
    return post_id, confirmation_kind


def detect_intake_media(message) -> IntakeMedia | None:
    if message.video:
        return IntakeMedia(
            kind="video",
            file_id=message.video.file_id,
            source_message_id=message.message_id,
            media_group_id=message.media_group_id,
            mime_type="video/mp4",
        )

    if message.photo:
        return IntakeMedia(
            kind="image",
            file_id=message.photo[-1].file_id,
            source_message_id=message.message_id,
            media_group_id=message.media_group_id,
            mime_type="image/jpeg",
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
            )

    return None
