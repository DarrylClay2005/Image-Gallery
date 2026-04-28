import mimetypes
import html
import os
import re
import secrets
import hashlib
import hmac
import time
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .auth import extract_bearer_token, issue_token, require_auth, verify_token
from .config import ROOT_DIR, load_settings
from .database import GalleryDatabase
from .emailer import send_verification_email


settings = load_settings()
db = GalleryDatabase(settings)
IMAGE_MIME_PREFIXES = ("image/",)
VIDEO_MIME_PREFIXES = ("video/",)
SAFE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp",
    ".mp4", ".webm", ".mov", ".m4v", ".ogg",
}
ADULT_KEYWORDS = {
    "18plus", "18+", "adult", "nsfw", "not safe for work", "nude", "nudity",
    "explicit", "porn", "porno", "sex", "sexual", "hentai", "ecchi", "lewd",
    "erotic", "fetish", "onlyfans", "camgirl", "cam boy", "xxx",
}


class RegisterRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    display_name: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class PasswordChangeRequest(BaseModel):
    old_password: str
    new_password: str


class AccountDeleteRequest(BaseModel):
    password: str


class FollowRequest(BaseModel):
    following: bool = True


class EmailUpdateRequest(BaseModel):
    email: str | None = None


class EmailCodeRequest(BaseModel):
    code: str


class CategoryRequest(BaseModel):
    name: str
    media_kind: str = "mixed"


class LikeRequest(BaseModel):
    liked: bool = True


class CommentRequest(BaseModel):
    body: str


class MediaUpdateRequest(BaseModel):
    title: str
    description: str | None = None
    category_id: int
    tags: list[str] = []
    is_adult: bool = False
    visibility: str = "public"
    comments_enabled: bool = True
    downloads_enabled: bool = True
    pinned: bool = False


class MediaControlRequest(BaseModel):
    visibility: str | None = None
    comments_enabled: bool | None = None
    downloads_enabled: bool | None = None
    pinned: bool | None = None



class ProfileUpdateRequest(BaseModel):
    display_name: str
    bio: str | None = None
    website_url: str | None = None
    location_label: str | None = None
    profile_color: str = "#37c9a7"
    public_profile: bool = True
    show_liked_count: bool = True


class SettingsUpdateRequest(BaseModel):
    theme_mode: str | None = None
    accent_color: str | None = None
    grid_density: str | None = None
    default_sort: str | None = None
    items_per_page: int | None = None
    autoplay_previews: bool | None = None
    muted_previews: bool | None = None
    reduce_motion: bool | None = None
    open_original_in_new_tab: bool | None = None
    blur_video_previews: bool | None = None


class AgeVerifyRequest(BaseModel):
    birthdate: str
    confirm_over_18: bool = False


class BookmarkRequest(BaseModel):
    bookmarked: bool = True


class CollectionRequest(BaseModel):
    name: str
    description: str | None = None
    is_public: bool = True


class CollectionItemRequest(BaseModel):
    media_id: int
    saved: bool = True


class ReportRequest(BaseModel):
    reason: str
    details: str | None = None


def _jsonable(value: Any) -> Any:
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _auth_optional(request: Request) -> dict[str, Any] | None:
    return verify_token(extract_bearer_token(request), settings.session_secret, settings.api_token_ttl_seconds)


def _user_id(auth: dict[str, Any] | None) -> int | None:
    if not auth:
        return None
    try:
        return int(auth.get("id"))
    except (TypeError, ValueError):
        return None


def _is_age_verified(user: dict[str, Any] | None) -> bool:
    return bool(user and user.get("age_verified_at") and user.get("adult_content_consent"))


async def _viewer_can_open_adult(request: Request) -> bool:
    viewer_id = _user_id(_auth_optional(request))
    if not viewer_id:
        return False
    return _is_age_verified(await db.get_user(viewer_id))


def _age_from_birthdate(birthdate: date) -> int:
    today = date.today()
    years = today.year - birthdate.year
    if (today.month, today.day) < (birthdate.month, birthdate.day):
        years -= 1
    return years


def _public_url(request: Request, storage_path: str, media_id: int | None = None) -> str:
    if media_id is not None:
        return str(request.url_for("serve_media_file", media_id=media_id))
    return str(request.url_for("serve_legacy_upload", path=storage_path))


def _media_access_token(media_id: int) -> str:
    msg = str(int(media_id)).encode("utf-8")
    return hmac.new(settings.session_secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _valid_media_access_token(media_id: int, token: str | None) -> bool:
    return bool(token) and hmac.compare_digest(str(token), _media_access_token(media_id))


def _append_query(url: str, key: str, value: str) -> str:
    return f"{url}{'&' if '?' in url else '?'}{key}={value}"


def _legacy_upload_path(storage_path: str | None) -> Path | None:
    if not storage_path or str(storage_path).startswith(("db://", "avatar-db://")):
        return None
    raw = str(storage_path).replace("\\", "/").lstrip("/")
    if ".." in Path(raw).parts:
        return None
    path = (settings.uploads_dir / raw).resolve()
    try:
        path.relative_to(settings.uploads_dir.resolve())
    except ValueError:
        return None
    return path if path.is_file() else None


def _fallback_avatar_svg(user_id: int) -> str:
    initials = f"U{int(user_id)}"[:3]
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
        '<rect width="128" height="128" rx="64" fill="#202832"/>'
        '<text x="64" y="74" text-anchor="middle" font-family="Inter,Arial,sans-serif" '
        'font-size="34" font-weight="800" fill="#9ba8b7">' + html.escape(initials) + '</text></svg>'
    )


def _verification_url(request: Request, token: str) -> str:
    return str(request.url_for("verify_email")).replace("http://", "https://") + f"?token={token}"


def _verification_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _wants_json(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "application/json" in accept and "text/html" not in accept


def _verification_page(title: str, message: str, *, ok: bool) -> HTMLResponse:
    color = "#37c9a7" if ok else "#ff6b6b"
    return HTMLResponse(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        f"<title>{html.escape(title)}</title>"
        "<style>body{margin:0;min-height:100vh;display:grid;place-items:center;"
        "font-family:Inter,system-ui,sans-serif;background:#10151f;color:#edf4ff}"
        "main{width:min(520px,calc(100vw - 32px));padding:28px;border:1px solid #273244;"
        "background:#151d2b;border-radius:8px}h1{margin:0 0 10px;font-size:1.5rem}"
        "p{color:#aab6c8;line-height:1.5}.badge{display:inline-block;margin-bottom:16px;"
        f"color:{color};font-weight:700}}a{{color:#7dd3fc}}</style></head><body><main>"
        f"<span class=\"badge\">{'Verified' if ok else 'Needs Attention'}</span>"
        f"<h1>{html.escape(title)}</h1><p>{html.escape(message)}</p>"
        f"<p><a href=\"{html.escape(settings.pages_public_url)}\">Return to Image Gallery</a></p>"
        "</main></body></html>"
    )


def _with_urls(request: Request, item: dict[str, Any] | None, adult_allowed: bool = False) -> dict[str, Any] | None:
    if not item:
        return None
    clone = dict(item)
    locked = bool(clone.get("is_adult")) and not adult_allowed
    clone["locked"] = locked
    clone["viewer_can_open_adult"] = adult_allowed
    clone["requires_adult_blur"] = bool(clone.get("is_adult")) and adult_allowed
    if locked:
        clone.pop("storage_path", None)
        clone["url"] = None
        clone["download_url"] = None
    else:
        media_id = int(clone["id"])
        clone["url"] = _public_url(request, clone.get("storage_path", ""), media_id)
        clone["download_url"] = str(request.url_for("download_media", media_id=media_id))
        if clone.get("is_adult") and adult_allowed:
            token = _media_access_token(media_id)
            clone["url"] = _append_query(clone["url"], "access", token)
            clone["download_url"] = _append_query(clone["download_url"], "access", token)
    if clone.get("user_avatar_path"):
        clone["user_avatar_url"] = str(request.url_for("serve_user_avatar", user_id=clone.get("user_id") or clone.get("id")))
    return _jsonable(clone)


def _with_user_urls(request: Request, user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not user:
        return None
    clone = dict(user)
    if clone.get("avatar_path"):
        clone["avatar_url"] = str(request.url_for("serve_user_avatar", user_id=clone["id"]))
    return _jsonable(clone)


def _with_collection_urls(request: Request, collection: dict[str, Any] | None, adult_allowed: bool = False) -> dict[str, Any] | None:
    if not collection:
        return None
    clone = dict(collection)
    if clone.get("cover_path") and (adult_allowed or not clone.get("cover_is_adult")):
        clone["cover_url"] = _public_url(request, clone["cover_path"], int(clone["cover_media_id"]) if clone.get("cover_media_id") else None)
    elif clone.get("cover_is_adult"):
        clone.pop("cover_path", None)
        clone["cover_url"] = None
        clone["cover_locked"] = True
    if clone.get("user_avatar_path"):
        clone["user_avatar_url"] = str(request.url_for("serve_user_avatar", user_id=clone.get("user_id") or clone.get("id")))
    return _jsonable(clone)


def _parse_tags(value: str | None) -> list[str]:
    tags = []
    for raw in re.split(r"[,#]", value or ""):
        tag = re.sub(r"[^A-Za-z0-9_.-]+", "", raw.strip())[:32]
        if tag and tag.lower() not in {existing.lower() for existing in tags}:
            tags.append(tag)
    return tags[:12]


def _moderate_upload(
    *,
    title: str,
    description: str | None,
    tags: list[str],
    filename: str,
    mime_type: str,
    user_marked_adult: bool,
) -> dict[str, Any]:
    combined = " ".join([title, description or "", " ".join(tags), filename, mime_type]).lower()
    normalized = re.sub(r"[^a-z0-9+]+", " ", combined)
    hits = sorted({word for word in ADULT_KEYWORDS if word in normalized or word in combined})
    adult_by_ai = bool(hits)
    is_adult = bool(user_marked_adult or adult_by_ai)
    reason_parts = []
    if user_marked_adult:
        reason_parts.append("Uploader marked this post as 18+.")
    if hits:
        reason_parts.append(f"Automatic moderation matched: {', '.join(hits[:5])}.")
    return {
        "is_adult": is_adult,
        "adult_marked_by_user": bool(user_marked_adult),
        "adult_marked_by_ai": adult_by_ai,
        "moderation_status": "adult" if is_adult else "clear",
        "moderation_score": 0.96 if adult_by_ai else (0.75 if user_marked_adult else 0),
        "moderation_reason": " ".join(reason_parts)[:300] or None,
    }


MAGIC_SIGNATURES = (
    (b"\xff\xd8\xff", "image/jpeg", "image"),
    (b"\x89PNG\r\n\x1a\n", "image/png", "image"),
    (b"GIF87a", "image/gif", "image"),
    (b"GIF89a", "image/gif", "image"),
    (b"\x1aE\xdf\xa3", "video/webm", "video"),
    (b"OggS", "video/ogg", "video"),
)
RATE_BUCKETS: dict[str, list[float]] = {}


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    return forwarded or (request.client.host if request.client else "unknown")


def _rate_limit(key: str, *, limit: int, window_seconds: int) -> None:
    now = time.time()
    bucket = [t for t in RATE_BUCKETS.get(key, []) if now - t < window_seconds]
    if len(bucket) >= limit:
        raise HTTPException(status_code=429, detail="Too many attempts. Try again later.")
    bucket.append(now)
    RATE_BUCKETS[key] = bucket


def _sniff_magic(data: bytes) -> tuple[str, str]:
    head = data[:32]
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return "image/webp", "image"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return "video/mp4", "video"
    for prefix, mime, kind in MAGIC_SIGNATURES:
        if head.startswith(prefix):
            return mime, kind
    raise HTTPException(status_code=400, detail="Unsupported or invalid file bytes.")


def _detect_media_kind(upload: UploadFile) -> str:
    mime = (upload.content_type or mimetypes.guess_type(upload.filename or "")[0] or "").lower()
    if mime.startswith(IMAGE_MIME_PREFIXES):
        return "image"
    if mime.startswith(VIDEO_MIME_PREFIXES):
        return "video"
    raise HTTPException(status_code=400, detail="Only images, GIFs, and videos are allowed.")


def _safe_extension(filename: str, mime_type: str) -> str:
    ext = Path(filename or "").suffix.lower()
    guessed = mimetypes.guess_extension(mime_type or "") or ""
    ext = ext if ext in SAFE_EXTENSIONS else guessed.lower()
    if ext not in SAFE_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported file extension.")
    return ".jpg" if ext == ".jpe" else ext


async def _read_validated_upload(upload: UploadFile, max_bytes: int, *, image_only: bool = False) -> dict[str, Any]:
    content = await upload.read()
    if not content:
        raise HTTPException(status_code=400, detail="Upload is empty.")
    if len(content) > max_bytes:
        raise HTTPException(status_code=413, detail=f"Uploads must be {max_bytes // (1024 * 1024)}MB or smaller.")
    sniffed_mime, media_kind = _sniff_magic(content)
    if image_only and media_kind != "image":
        raise HTTPException(status_code=400, detail="Profile pictures must be images.")
    claimed = (upload.content_type or "").lower()
    if claimed and not claimed.startswith(("image/", "video/", "application/octet-stream")):
        raise HTTPException(status_code=400, detail="Invalid declared content type.")
    original_filename = Path(upload.filename or "upload").name[:255]
    _safe_extension(original_filename, sniffed_mime)
    sha256 = hashlib.sha256(content).hexdigest()
    return {
        "content": content,
        "sha256": sha256,
        "mime_type": sniffed_mime,
        "media_kind": media_kind,
        "original_filename": original_filename,
        "file_size": len(content),
    }


async def lifespan(app: FastAPI):
    settings.uploads_dir.mkdir(parents=True, exist_ok=True)
    await db.connect()
    yield
    await db.close()


app = FastAPI(title="Image Gallery", lifespan=lifespan)
settings.uploads_dir.mkdir(parents=True, exist_ok=True)
allowed_origins = settings.cors_allowed_origins or [
    settings.pages_public_url.rstrip("/"),
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    "http://127.0.0.1:8788",
    "http://localhost:8788",
]
cors_origin_regex = os.getenv("GALLERY_CORS_ALLOW_ORIGIN_REGEX", r"https://[a-z0-9-]+\.trycloudflare\.com")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=cors_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=ROOT_DIR / "app" / "static"), name="static")
app.mount("/app/static", StaticFiles(directory=ROOT_DIR / "app" / "static"), name="app_static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(ROOT_DIR / "index.html")


@app.get("/api/uploads/{path:path}", name="serve_legacy_upload")
async def serve_legacy_upload(path: str) -> FileResponse:
    legacy = _legacy_upload_path(path)
    if not legacy:
        raise HTTPException(status_code=404, detail="Legacy upload not found.")
    media_type = mimetypes.guess_type(str(legacy))[0] or "application/octet-stream"
    return FileResponse(legacy, media_type=media_type, headers={"Cache-Control": "public, max-age=86400"})


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "schema": settings.db_schema, "max_upload_bytes": settings.max_upload_bytes, "server_time": datetime.utcnow().isoformat() + "Z"}


@app.get("/api/live/checks")
async def live_checks(request: Request) -> dict[str, Any]:
    auth = _auth_optional(request)
    checks: list[dict[str, Any]] = []
    try:
        snapshot = await db.site_checks()
        checks.append({"id": "api", "label": "API reachable", "ok": True, "detail": "Backend responded."})
        checks.append({"id": "db", "label": "Database reachable", "ok": True, "detail": f"Schema {settings.db_schema} responded."})
        missing = int(snapshot.get("missing_db_files") or 0)
        checks.append({
            "id": "file_store",
            "label": "DB file store coverage",
            "ok": missing == 0,
            "severity": "warn" if missing else "ok",
            "detail": "All active posts are linked to DB file blobs." if missing == 0 else f"{missing} active post(s) still need DB blob migration or legacy disk fallback.",
        })
        reports = int(snapshot.get("open_reports") or 0)
        checks.append({
            "id": "reports",
            "label": "Open reports",
            "ok": reports == 0,
            "severity": "warn" if reports else "ok",
            "detail": "No open user reports." if reports == 0 else f"{reports} report(s) need review.",
        })
        if auth:
            user = await db.get_user(int(auth["id"]))
            checks.append({"id": "session", "label": "Login session", "ok": bool(user), "detail": "Signed in." if user else "Token is invalid or account is gone."})
            if user and user.get("email"):
                checks.append({
                    "id": "email",
                    "label": "Email verification",
                    "ok": bool(user.get("email_verified_at")),
                    "severity": "warn" if not user.get("email_verified_at") else "ok",
                    "detail": "Email verified." if user.get("email_verified_at") else "Email verification is still pending.",
                })
        status = "ok" if all(c.get("ok") or c.get("severity") == "warn" for c in checks) else "attention"
        return {"ok": True, "status": status, "checks": checks, "snapshot": _jsonable(snapshot), "server_time": datetime.utcnow().isoformat() + "Z"}
    except Exception as exc:
        return {
            "ok": False,
            "status": "offline",
            "checks": [{"id": "db", "label": "Database reachable", "ok": False, "severity": "error", "detail": str(exc)[:240]}],
            "snapshot": {},
            "server_time": datetime.utcnow().isoformat() + "Z",
        }


@app.post("/api/auth/register")
async def register(payload: RegisterRequest, request: Request) -> dict[str, Any]:
    email_token = _verification_code() if payload.email else None
    try:
        user = await db.register_user(payload.username, payload.password, payload.display_name, payload.email, email_token)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except Exception as exc:
        if "Duplicate" in str(exc):
            raise HTTPException(status_code=409, detail="That username or email is already taken.") from None
        raise
    verification_sent = False
    if user.get("email") and email_token:
        verification_sent = send_verification_email(settings, user["email"], _verification_url(request, email_token), email_token)
    return {"user": _jsonable(user), "token": issue_token(settings.session_secret, user), "email_verification_sent": verification_sent}


@app.get("/api/auth/verify-email", name="verify_email")
async def verify_email(request: Request, token: str):
    user = await db.verify_email_by_token(token)
    if not user:
        if _wants_json(request):
            raise HTTPException(status_code=400, detail="Invalid or expired verification link.")
        return _verification_page("Verification Link Expired", "That Image Gallery verification link is invalid or has already been used. Sign in and resend verification from your account.", ok=False)
    if _wants_json(request):
        return {"ok": True, "user": _jsonable(user)}
    return _verification_page("Email Verified", f"{user.get('email') or 'Your email address'} is now verified for Image Gallery.", ok=True)


@app.post("/api/auth/resend-verification")
async def resend_verification(request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    user = await db.get_user(int(auth["id"]))
    if not user:
        raise HTTPException(status_code=404, detail="Account not found.")
    if not user.get("email"):
        raise HTTPException(status_code=400, detail="This account does not have an email address.")
    if user.get("email_verified_at"):
        return {"ok": True, "email_verification_sent": False, "already_verified": True}
    email_token = _verification_code()
    user = await db.issue_email_verification_token(int(auth["id"]), email_token)
    verification_sent = bool(user and send_verification_email(settings, user["email"], _verification_url(request, email_token), email_token))
    return {"ok": verification_sent, "email_verification_sent": verification_sent, "already_verified": False}


@app.post("/api/me/email")
async def update_email(payload: EmailUpdateRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        user = await db.update_user_email(int(auth["id"]), payload.email)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    verification_sent = False
    if user and user.get("email"):
        email_token = _verification_code()
        user = await db.issue_email_verification_token(int(auth["id"]), email_token)
        verification_sent = bool(user and send_verification_email(settings, user["email"], _verification_url(request, email_token), email_token))
    return {"ok": True, "user": _jsonable(user), "email_verification_sent": verification_sent}


@app.post("/api/me/email/verify")
async def verify_email_code(payload: EmailCodeRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    code = re.sub(r"\D+", "", str(payload.code or ""))[:12]
    if not code:
        raise HTTPException(status_code=400, detail="Enter the verification code from your email.")
    user = await db.verify_email_code(int(auth["id"]), code)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid verification code.")
    return {"ok": True, "user": _jsonable(user)}


@app.post("/api/auth/login")
async def login(payload: LoginRequest, request: Request) -> dict[str, Any]:
    ip = _client_ip(request)
    username = (payload.username or "").strip()[:80]
    if await db.count_recent_failed_auth(username, ip) >= 8:
        raise HTTPException(status_code=429, detail="Too many failed login attempts. Try again later.")
    try:
        user = await db.authenticate_user(payload.username, payload.password)
    except ValueError as exc:
        await db.record_auth_attempt(username, ip, False)
        raise HTTPException(status_code=400, detail=str(exc)) from None
    await db.record_auth_attempt(username, ip, bool(user))
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    return {"user": _jsonable(user), "token": issue_token(settings.session_secret, user)}


@app.get("/api/me")
async def me(request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    user = await db.get_user(int(auth["id"]))
    if not user:
        raise HTTPException(status_code=401, detail="Account no longer exists.")
    return {"user": _with_user_urls(request, user)}


@app.patch("/api/me/profile")
async def update_profile(payload: ProfileUpdateRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        user = await db.update_user_profile(int(auth["id"]), payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"user": _with_user_urls(request, user)}


@app.patch("/api/me/settings")
async def update_settings(payload: SettingsUpdateRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        user = await db.update_user_settings(
            int(auth["id"]),
            {key: value for key, value in payload.model_dump().items() if value is not None},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"user": _with_user_urls(request, user)}


@app.post("/api/me/avatar")
async def update_avatar(request: Request, file: UploadFile = File(...)) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    _rate_limit(f"avatar:{auth['id']}", limit=20, window_seconds=3600)
    uploaded = await _read_validated_upload(file, 5 * 1024 * 1024, image_only=True)
    user = await db.save_avatar_file(int(auth["id"]), **uploaded)
    return {"user": _with_user_urls(request, user)}


@app.post("/api/me/age-verification")
async def verify_age(payload: AgeVerifyRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    if not payload.confirm_over_18:
        raise HTTPException(status_code=400, detail="Confirm that you are 18 or older to continue.")
    try:
        birthdate = datetime.strptime(payload.birthdate, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Birthdate must use YYYY-MM-DD.") from None
    if birthdate > date.today():
        raise HTTPException(status_code=400, detail="Birthdate cannot be in the future.")
    if _age_from_birthdate(birthdate) < 18:
        raise HTTPException(status_code=403, detail="You must be 18 or older to view 18+ posts.")
    user = await db.verify_user_age(int(auth["id"]), birthdate)
    return {"user": _with_user_urls(request, user)}


@app.get("/api/me/bookmarks")
async def my_bookmarks(request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    items = await db.list_bookmarks(int(auth["id"]))
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": [_with_urls(request, item, adult_allowed) for item in items]}


@app.get("/api/me/media")
async def my_media(request: Request, include_deleted: bool = True) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    items = await db.list_user_media(int(auth["id"]), include_deleted=include_deleted)
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": [_with_urls(request, item, adult_allowed) for item in items]}




@app.post("/api/me/password")
async def change_password(payload: PasswordChangeRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    ok = await db.change_password(int(auth["id"]), payload.old_password, payload.new_password)
    if not ok:
        raise HTTPException(status_code=401, detail="Current password is incorrect.")
    return {"ok": True}


@app.delete("/api/me")
async def delete_account(payload: AccountDeleteRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    ok = await db.delete_account(int(auth["id"]), payload.password)
    if not ok:
        raise HTTPException(status_code=401, detail="Password is incorrect.")
    return {"deleted": True}


@app.get("/api/users/{username}")
async def public_profile(username: str, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    profile = await db.get_public_profile(username, viewer_id)
    if not profile:
        raise HTTPException(status_code=404, detail="User not found.")
    return {"user": _with_user_urls(request, profile)}


@app.post("/api/users/{user_id}/follow")
async def follow_user(user_id: int, payload: FollowRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        result = await db.set_follow(int(auth["id"]), user_id, payload.following)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if not result:
        raise HTTPException(status_code=404, detail="User not found.")
    return result


@app.get("/api/feed/following")
async def following_feed(request: Request, limit: int = 60, offset: int = 0) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    adult_allowed = await _viewer_can_open_adult(request)
    items = await db.following_feed(int(auth["id"]), limit=limit, offset=offset)
    return {"media": [_with_urls(request, item, adult_allowed) for item in items], "limit": limit, "offset": offset}


@app.get("/api/me/likes")
async def my_likes(request: Request, limit: int = 80, offset: int = 0) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    adult_allowed = await _viewer_can_open_adult(request)
    items = await db.list_liked_media(int(auth["id"]), limit=limit, offset=offset)
    return {"media": [_with_urls(request, item, adult_allowed) for item in items], "limit": limit, "offset": offset}


@app.get("/api/users/{user_id}/followers")
async def user_followers(user_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    users = await db.list_user_follows(user_id, mode="followers", viewer_id=viewer_id)
    return {"users": [_with_user_urls(request, user) for user in users]}


@app.get("/api/users/{user_id}/following")
async def user_following(user_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    users = await db.list_user_follows(user_id, mode="following", viewer_id=viewer_id)
    return {"users": [_with_user_urls(request, user) for user in users]}

@app.get("/api/categories")
async def categories() -> dict[str, Any]:
    return {"categories": _jsonable(await db.list_categories())}


@app.post("/api/categories")
async def create_category(payload: CategoryRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        category = await db.create_category(payload.name, payload.media_kind, int(auth["id"]))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"category": _jsonable(category)}


@app.get("/api/media")
async def media(
    request: Request,
    media_kind: str | None = None,
    category_id: int | None = None,
    q: str | None = None,
    sort: str = "new",
    limit: int = 60,
    offset: int = 0,
) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    adult_allowed = await _viewer_can_open_adult(request)
    items = await db.list_media(
        viewer_id=viewer_id,
        media_kind=media_kind,
        category_id=category_id,
        query=(q or "").strip()[:80] or None,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    return {"media": [_with_urls(request, item, adult_allowed) for item in items]}


@app.get("/api/media/random")
async def random_media(request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    adult_allowed = await _viewer_can_open_adult(request)
    item = await db.random_media(viewer_id)
    if not item:
        raise HTTPException(status_code=404, detail="No media has been uploaded yet.")
    return {"media": _with_urls(request, item, adult_allowed)}


@app.get("/api/tags")
async def tags() -> dict[str, Any]:
    return {"tags": _jsonable(await db.tag_cloud())}


@app.get("/api/collections")
async def collections(request: Request, mine: bool = False) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    adult_allowed = await _viewer_can_open_adult(request)
    if mine and not viewer_id:
        raise HTTPException(status_code=401, detail="Login required")
    rows = await db.list_collections(viewer_id=viewer_id, mine=mine)
    return {"collections": [_with_collection_urls(request, row, adult_allowed) for row in rows]}


@app.post("/api/collections")
async def create_collection(payload: CollectionRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        collection = await db.create_collection(int(auth["id"]), payload.name, payload.description, payload.is_public)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    adult_allowed = await _viewer_can_open_adult(request)
    return {"collection": _with_collection_urls(request, collection, adult_allowed)}


@app.get("/api/collections/{collection_id}")
async def collection_detail(collection_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    adult_allowed = await _viewer_can_open_adult(request)
    collection = await db.get_collection(collection_id, viewer_id)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found.")
    items = await db.list_collection_media(collection_id, viewer_id)
    return {
        "collection": _with_collection_urls(request, collection, adult_allowed),
        "media": [_with_urls(request, item, adult_allowed) for item in items],
    }


@app.post("/api/collections/{collection_id}/items")
async def save_collection_item(collection_id: int, payload: CollectionItemRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    collection = await db.set_collection_item(collection_id, payload.media_id, int(auth["id"]), payload.saved)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"collection": _with_collection_urls(request, collection, adult_allowed)}


@app.post("/api/media")
async def upload_media(
    request: Request,
    file: UploadFile = File(...),
    title: str = Form(...),
    description: str = Form(""),
    category_id: int | None = Form(None),
    category_name: str = Form(""),
    category_kind: str = Form("mixed"),
    tags: str = Form(""),
    is_adult: bool = Form(False),
    visibility: str = Form("public"),
    comments_enabled: bool = Form(True),
    downloads_enabled: bool = Form(True),
    pinned: bool = Form(False),
) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    media_kind = _detect_media_kind(file)
    visibility = str(visibility or "public").lower()
    if visibility not in {"public", "unlisted", "private"}:
        raise HTTPException(status_code=400, detail="Visibility must be public, unlisted, or private.")
    title = " ".join(title.strip().split())[:160]
    if not title:
        raise HTTPException(status_code=400, detail="Title is required.")
    if not category_id:
        category = await db.create_category(category_name, category_kind, int(auth["id"]))
        category_id = int(category["id"])
    parsed_tags = _parse_tags(tags)
    original_filename = Path(file.filename or "upload").name[:255]
    moderation = _moderate_upload(
        title=title,
        description=description.strip()[:2000] or None,
        tags=parsed_tags,
        filename=original_filename,
        mime_type=file.content_type or "application/octet-stream",
        user_marked_adult=is_adult,
    )
    _rate_limit(f"upload:{auth['id']}", limit=60, window_seconds=3600)
    uploaded = await _read_validated_upload(file, settings.max_upload_bytes)
    media_kind = uploaded["media_kind"]
    media_file = await db.save_media_file(user_id=int(auth["id"]), **uploaded)
    if media_file.get("duplicate"):
        raise HTTPException(status_code=409, detail="That exact file is already stored in the gallery database.")
    if media_file["sha256"] != uploaded["sha256"]:
        raise HTTPException(status_code=500, detail="Stored file hash verification failed.")
    item = await db.add_media(
        {
            "user_id": int(auth["id"]),
            "category_id": category_id,
            "title": title,
            "description": description.strip()[:2000] or None,
            "tags": parsed_tags,
            "media_kind": media_kind,
            "mime_type": uploaded["mime_type"],
            "original_filename": uploaded["original_filename"],
            "storage_path": f"db://media/{media_file['id']}",
            "media_file_id": int(media_file["id"]),
            "content_sha256": uploaded["sha256"],
            "file_size": uploaded["file_size"],
            "visibility": visibility,
            "comments_enabled": comments_enabled,
            "downloads_enabled": downloads_enabled,
            "pinned": pinned,
            **moderation,
        }
    )
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.get("/api/media/{media_id}")
async def media_detail(media_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    adult_allowed = await _viewer_can_open_adult(request)
    item = await db.get_media(media_id, viewer_id)
    if not item or item.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Media not found.")
    if item.get("visibility") == "private" and int(item.get("user_id")) != int(viewer_id or 0):
        raise HTTPException(status_code=403, detail="This post is private.")
    if item.get("is_adult") and not adult_allowed:
        raise HTTPException(status_code=403, detail="Age verification required for this 18+ post.")
    await db.increment_counter(media_id, "views")
    comments = await db.list_comments(media_id)
    return {"media": _with_urls(request, item, adult_allowed), "comments": _jsonable(comments)}


@app.patch("/api/media/{media_id}")
async def edit_media(media_id: int, payload: MediaUpdateRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        item = await db.update_media(media_id, int(auth["id"]), payload.model_dump())
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.patch("/api/media/{media_id}/controls")
async def edit_media_controls(media_id: int, payload: MediaControlRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        item = await db.set_media_controls(media_id, int(auth["id"]), payload.model_dump(exclude_none=True))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.post("/api/media/{media_id}/restore")
async def restore_media(media_id: int, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        item = await db.restore_media(media_id, int(auth["id"]))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.post("/api/media/{media_id}/like")
async def like_media(media_id: int, payload: LikeRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    item = await db.set_like(media_id, int(auth["id"]), payload.liked)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.post("/api/media/{media_id}/bookmark")
async def bookmark_media(media_id: int, payload: BookmarkRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    item = await db.set_bookmark(media_id, int(auth["id"]), payload.bookmarked)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    adult_allowed = await _viewer_can_open_adult(request)
    return {"media": _with_urls(request, item, adult_allowed)}


@app.post("/api/media/{media_id}/comments")
async def add_comment(media_id: int, payload: CommentRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    if not await db.get_media(media_id, int(auth["id"])):
        raise HTTPException(status_code=404, detail="Media not found.")
    try:
        comment = await db.add_comment(media_id, int(auth["id"]), payload.body)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"comment": _jsonable(comment)}


@app.delete("/api/comments/{comment_id}")
async def delete_comment(comment_id: int, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        deleted = await db.delete_comment(comment_id, int(auth["id"]))
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from None
    if not deleted:
        raise HTTPException(status_code=404, detail="Comment not found.")
    return {"deleted": True}


@app.post("/api/media/{media_id}/report")
async def report_media(media_id: int, payload: ReportRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    if not await db.get_media(media_id, int(auth["id"])):
        raise HTTPException(status_code=404, detail="Media not found.")
    try:
        report = await db.report_media(media_id, int(auth["id"]), payload.reason, payload.details)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"report": _jsonable(report)}


@app.delete("/api/media/{media_id}")
async def delete_media(media_id: int, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    item = await db.delete_media(media_id, int(auth["id"]))
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    return {"deleted": True}


async def _adult_file_allowed(request: Request, media_id: int, access: str | None) -> bool:
    # Browser <img>/<video> requests do not carry Authorization headers.
    # Age-verified API responses include this signed token in adult media URLs.
    return _valid_media_access_token(media_id, access) or await _viewer_can_open_adult(request)


async def _serve_media_content(media_id: int, request: Request, *, access: str | None, as_download: bool) -> Response:
    viewer_id = _user_id(_auth_optional(request))
    item = await db.get_media(media_id, viewer_id)
    if not item or item.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Media not found.")
    owner = int(item.get("user_id")) == int(viewer_id or 0)
    if item.get("visibility") == "private" and not owner:
        raise HTTPException(status_code=403, detail="This post is private.")
    if as_download and not item.get("downloads_enabled", True) and not owner:
        raise HTTPException(status_code=403, detail="Downloads are disabled for this post.")
    if item.get("is_adult") and not await _adult_file_allowed(request, media_id, access):
        raise HTTPException(status_code=403, detail="Age verification required for this 18+ post.")

    file_row = await db.get_media_file(media_id)
    if file_row:
        digest = hashlib.sha256(file_row["content"]).hexdigest()
        if digest != file_row["sha256"]:
            raise HTTPException(status_code=500, detail="Stored file failed hash verification.")
        headers = {"X-Content-SHA256": digest}
        if as_download:
            await db.increment_counter(media_id, "downloads")
            headers["Content-Disposition"] = f'attachment; filename="{file_row["original_filename"]}"'
        else:
            headers["Cache-Control"] = "public, max-age=86400"
        return Response(content=file_row["content"], media_type=file_row["mime_type"], headers=headers)

    legacy = _legacy_upload_path(item.get("storage_path"))
    if legacy:
        if as_download:
            await db.increment_counter(media_id, "downloads")
        return FileResponse(
            legacy,
            media_type=item.get("mime_type") or mimetypes.guess_type(str(legacy))[0] or "application/octet-stream",
            filename=item.get("original_filename") if as_download else None,
            headers={"Cache-Control": "public, max-age=86400"} if not as_download else None,
        )

    raise HTTPException(
        status_code=404,
        detail="File is missing. Re-upload this post once so it can be saved into the new DB-backed file store.",
    )


@app.get("/api/media/{media_id}/file", name="serve_media_file")
async def serve_media_file(media_id: int, request: Request, access: str | None = None) -> Response:
    return await _serve_media_content(media_id, request, access=access, as_download=False)


@app.get("/api/users/{user_id}/avatar", name="serve_user_avatar")
async def serve_user_avatar(user_id: int) -> Response:
    file_row = await db.get_avatar_file(user_id)
    if file_row:
        digest = hashlib.sha256(file_row["content"]).hexdigest()
        if digest != file_row["sha256"]:
            raise HTTPException(status_code=500, detail="Stored avatar failed hash verification.")
        return Response(content=file_row["content"], media_type=file_row["mime_type"], headers={"Cache-Control": "public, max-age=86400", "X-Content-SHA256": digest})

    user = await db.get_user(user_id)
    legacy = _legacy_upload_path(user.get("avatar_path") if user else None)
    if legacy:
        return FileResponse(
            legacy,
            media_type=(user.get("avatar_mime_type") if user else None) or mimetypes.guess_type(str(legacy))[0] or "image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    # Avoid repeated browser 404 spam for accounts that have no uploaded avatar yet.
    return Response(content=_fallback_avatar_svg(user_id), media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/media/{media_id}/download", name="download_media")
async def download_media(media_id: int, request: Request, access: str | None = None) -> Response:
    return await _serve_media_content(media_id, request, access=access, as_download=True)


@app.get("/api/stats")
async def stats() -> dict[str, Any]:
    return {"stats": _jsonable(await db.stats())}
