from dataclasses import dataclass
import os


def get_env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def get_int_env(*names: str, default: int = 0) -> int:
    raw = get_env(*names)
    if raw is None:
        return default
    return int(raw)


@dataclass(frozen=True)
class Settings:
    bot_token: str
    mongodb_uri: str
    intake_channel_id: int
    video_storage_channel_id: int
    image_channel_id: int
    video_confirmation_chat_id: int
    image_confirmation_chat_id: int
    admin_user_id: int
    health_port: int
    mongo_db_name: str


def load_settings() -> Settings:
    video_storage_channel_id = get_int_env(
        "VIDEO_STORAGE_CHANNEL_ID",
        "STORAGE_CHANNEL_ID",
        "STORAGE_CHAT_ID",
        default=0,
    )
    image_channel_id = get_int_env(
        "IMAGE_CHANNEL_ID",
        "IMAGE_DEST_CHANNEL_ID",
        "THUMBNAIL_CHANNEL_ID",
        "THUMBNAIL_CHAT_ID",
        "THUMBNAIL_SOURCE_CHANNEL_ID",
        default=0,
    )

    return Settings(
        bot_token=get_env("BOT_TOKEN", "TELEGRAM_BOT_TOKEN", default="") or "",
        mongodb_uri=get_env("MONGODB_URI", default="") or "",
        intake_channel_id=get_int_env("INTAKE_CHANNEL_ID", "INTAKE_CHAT_ID", default=0),
        video_storage_channel_id=video_storage_channel_id,
        image_channel_id=image_channel_id,
        video_confirmation_chat_id=get_int_env(
            "VIDEO_CONFIRMATION_CHAT_ID",
            "CONFIRMATION_CHAT_ID",
            "CONTROL_CHAT_ID",
            default=video_storage_channel_id,
        ),
        image_confirmation_chat_id=get_int_env(
            "IMAGE_CONFIRMATION_CHAT_ID",
            default=image_channel_id,
        ),
        admin_user_id=get_int_env("ADMIN_USER_ID", default=0),
        health_port=get_int_env("PORT", default=10000),
        mongo_db_name=get_env("MONGO_DB_NAME", default="tg_bot_pro_db") or "tg_bot_pro_db",
    )
