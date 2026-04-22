# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from queue_controller.app import build_application
from queue_controller.config import load_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

LOGGER = logging.getLogger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payload = json.dumps({"status": "ok"}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        return


def start_health_server(port: int):
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    LOGGER.info("Health server listening on port %s", port)
    return server


def validate_settings(settings):
    missing = []
    if not settings.bot_token:
        missing.append("BOT_TOKEN / TELEGRAM_BOT_TOKEN")
    if not settings.mongodb_uri:
        missing.append("MONGODB_URI")
    if not settings.intake_channel_id:
        missing.append("INTAKE_CHANNEL_ID / INTAKE_CHAT_ID")
    if not settings.video_storage_channel_id:
        missing.append("VIDEO_STORAGE_CHANNEL_ID / STORAGE_CHANNEL_ID / STORAGE_CHAT_ID")
    if not settings.image_channel_id:
        missing.append("IMAGE_CHANNEL_ID / IMAGE_DEST_CHANNEL_ID / THUMBNAIL_CHANNEL_ID")
    if not settings.video_confirmation_chat_id:
        missing.append("VIDEO_CONFIRMATION_CHAT_ID / CONFIRMATION_CHAT_ID / CONTROL_CHAT_ID")
    if not settings.image_confirmation_chat_id:
        missing.append("IMAGE_CONFIRMATION_CHAT_ID")

    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))


def main():
    settings = load_settings()
    validate_settings(settings)

    asyncio.set_event_loop(asyncio.new_event_loop())
    start_health_server(settings.health_port)

    LOGGER.info(
        "Startup config loaded | intake=%s | video_storage=%s | image_channel=%s | video_confirmation=%s | image_confirmation=%s",
        settings.intake_channel_id,
        settings.video_storage_channel_id,
        settings.image_channel_id,
        settings.video_confirmation_chat_id,
        settings.image_confirmation_chat_id,
    )

    application = build_application(settings)
    LOGGER.info("Queue Controller Bot is starting...")
    application.run_polling()


if __name__ == "__main__":
    main()
