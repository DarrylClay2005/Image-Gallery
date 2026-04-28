import os
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
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str
    smtp_from_email: str
    smtp_use_tls: bool


def load_settings() -> Settings:
    pages_url = _env("GALLERY_PAGES_PUBLIC_URL", "https://darrylclay2005.github.io/Image-Gallery/")
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
        max_upload_bytes=int(_env("GALLERY_MAX_UPLOAD_BYTES", str(250 * 1024 * 1024))),
        smtp_host=_env("GALLERY_SMTP_HOST") or _env("SMTP_HOST"),
        smtp_port=int(_env("GALLERY_SMTP_PORT") or _env("SMTP_PORT") or "587"),
        smtp_username=_env("GALLERY_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_password=_env("GALLERY_SMTP_PASSWORD") or _env("SMTP_PASSWORD"),
        smtp_from_email=_env("GALLERY_SMTP_FROM_EMAIL") or _env("SMTP_FROM_EMAIL") or _env("GALLERY_SMTP_USERNAME") or _env("SMTP_USERNAME"),
        smtp_use_tls=(_env("GALLERY_SMTP_USE_TLS") or _env("SMTP_USE_TLS") or "true").lower() not in {"0", "false", "no", "off"},
    )
