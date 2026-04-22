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
    mongo_db_name: str
    intake_channel_id: int
    storage_channel_id: int
    image_source_channel_id: int
    admin_user_id: int
    dispatch_interval_seconds: int
    health_port: int


def load_settings() -> Settings:
    storage_channel_id = get_int_env(
        "STORAGE_CHANNEL_ID",
        "VIDEO_STORAGE_CHANNEL_ID",
        "STORAGE_CHAT_ID",
        default=0,
    )
    image_source_channel_id = get_int_env(
        "IMAGE_SOURCE_CHANNEL_ID",
        "SOURCE_IMAGE_CHANNEL_ID",
        "IMAGE_CHANNEL_ID",
        "IMAGE_DEST_CHANNEL_ID",
        default=0,
    )

    return Settings(
        bot_token=get_env("BOT_TOKEN", "TELEGRAM_BOT_TOKEN", default="") or "",
        mongodb_uri=get_env("MONGODB_URI", default="") or "",
        mongo_db_name=get_env("MONGO_DB_NAME", default="tg_bot_pro_db") or "tg_bot_pro_db",
        intake_channel_id=get_int_env("INTAKE_CHANNEL_ID", "INTAKE_CHAT_ID", default=0),
        storage_channel_id=storage_channel_id,
        image_source_channel_id=image_source_channel_id,
        admin_user_id=get_int_env("ADMIN_USER_ID", default=0),
        dispatch_interval_seconds=get_int_env("DISPATCH_INTERVAL_SECONDS", default=10),
        health_port=get_int_env("PORT", default=10000),
    )
