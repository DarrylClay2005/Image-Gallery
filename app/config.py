import os
import socket
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")
if os.getenv("GALLERY_LOAD_SHARED_MUSIC_ENV", "0").lower() in {"1", "true", "yes", "on"}:
    load_dotenv(ROOT_DIR.parent / "Music" / ".env", override=False)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_csv(name: str) -> list[str]:
    return [item.strip().rstrip("/") for item in _env(name).split(",") if item.strip()]


def _db_host() -> str:
    explicit = _env("GALLERY_DB_HOST")
    if explicit:
        if explicit == "host.docker.internal":
            try:
                socket.gethostbyname(explicit)
            except OSError:
                return "127.0.0.1"
        return explicit
    inherited = _env("DB_HOST") or _env("MYSQL_HOST")
    if inherited and inherited != "host.docker.internal":
        return inherited
    return "127.0.0.1"


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_schema: str
    session_secret: str
    api_token_ttl_seconds: int
    cors_allowed_origins: list[str]
    pages_public_url: str
    uploads_dir: Path
    max_upload_bytes: int
    required_db_packet_bytes: int
    db_blob_chunk_bytes: int
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_from_email: str
    smtp_use_tls: bool
    ai_enabled: bool
    ai_api_key: str
    ai_base_url: str
    ai_model: str
    ai_timeout_seconds: int


def load_settings() -> Settings:
    pages_url = _env("GALLERY_PAGES_PUBLIC_URL", "https://heavenlyxenusvr.github.io/Image-Gallery/")
    ai_api_key = _env("GALLERY_AI_API_KEY") or _env("OPENAI_API_KEY")
    ai_enabled_raw = _env("GALLERY_AI_ENABLED", "true" if ai_api_key else "false")
    return Settings(
        db_host=_db_host(),
        db_port=int(_env("GALLERY_DB_PORT", "3306")),
        db_user=_env("GALLERY_DB_USER") or _env("DB_USER") or _env("MYSQL_USER") or "botuser",
        db_password=_env("GALLERY_DB_PASSWORD") or _env("DB_PASSWORD") or _env("MYSQL_PASSWORD") or "bot_logins",
        db_schema=_env("GALLERY_DB_SCHEMA", "image_gallery"),
        session_secret=_env("GALLERY_SESSION_SECRET", "change-this-gallery-secret"),
        api_token_ttl_seconds=int(_env("GALLERY_API_TOKEN_TTL_SECONDS", "1209600")),
        cors_allowed_origins=_env_csv("GALLERY_CORS_ALLOWED_ORIGINS"),
        pages_public_url=pages_url,
        uploads_dir=Path(_env("GALLERY_UPLOADS_DIR", str(ROOT_DIR / "uploads"))),
        max_upload_bytes=int(_env("GALLERY_MAX_UPLOAD_BYTES", str(500 * 1024 * 1024))),
        required_db_packet_bytes=int(_env("GALLERY_REQUIRED_DB_PACKET_BYTES", str(512 * 1024 * 1024))),
        db_blob_chunk_bytes=int(_env("GALLERY_DB_BLOB_CHUNK_BYTES", str(8 * 1024 * 1024))),
        smtp_host=_env("GALLERY_SMTP_HOST") or _env("SMTP_HOST"),
        smtp_port=int(_env("GALLERY_SMTP_PORT") or _env("SMTP_PORT") or "587"),
        smtp_username=_env("GALLERY_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_password=_env("GALLERY_SMTP_PASSWORD") or _env("SMTP_PASSWORD"),
        smtp_from_email=_env("GALLERY_SMTP_FROM_EMAIL") or _env("SMTP_FROM_EMAIL") or _env("GALLERY_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_use_tls=(_env("GALLERY_SMTP_USE_TLS") or _env("SMTP_USE_TLS") or "true").lower() not in {"0", "false", "no", "off"},
        ai_enabled=ai_enabled_raw.lower() not in {"0", "false", "no", "off"},
        ai_api_key=ai_api_key,
        ai_base_url=(_env("GALLERY_AI_BASE_URL") or _env("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/"),
        ai_model=_env("GALLERY_AI_MODEL", "gpt-5.4-nano"),
        ai_timeout_seconds=max(10, int(_env("GALLERY_AI_TIMEOUT_SECONDS", "45"))),
    )
