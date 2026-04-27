import mimetypes
import os
import re
import shutil
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .auth import extract_bearer_token, issue_token, require_auth, verify_token
from .config import ROOT_DIR, load_settings
from .database import GalleryDatabase


settings = load_settings()
db = GalleryDatabase(settings)
IMAGE_MIME_PREFIXES = ("image/",)
VIDEO_MIME_PREFIXES = ("video/",)
SAFE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif", ".bmp",
    ".mp4", ".webm", ".mov", ".m4v", ".ogg",
}


class RegisterRequest(BaseModel):
    username: str
    password: str
    display_name: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class CategoryRequest(BaseModel):
    name: str
    media_kind: str = "mixed"


class LikeRequest(BaseModel):
    liked: bool = True


class CommentRequest(BaseModel):
    body: str


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


def _public_url(request: Request, storage_path: str) -> str:
    return str(request.url_for("uploads", path=storage_path))


def _with_urls(request: Request, item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not item:
        return None
    clone = dict(item)
    clone["url"] = _public_url(request, clone["storage_path"])
    clone["download_url"] = str(request.url_for("download_media", media_id=clone["id"]))
    if clone.get("user_avatar_path"):
        clone["user_avatar_url"] = _public_url(request, clone["user_avatar_path"])
    return _jsonable(clone)


def _with_user_urls(request: Request, user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not user:
        return None
    clone = dict(user)
    if clone.get("avatar_path"):
        clone["avatar_url"] = _public_url(request, clone["avatar_path"])
    return _jsonable(clone)


def _with_collection_urls(request: Request, collection: dict[str, Any] | None) -> dict[str, Any] | None:
    if not collection:
        return None
    clone = dict(collection)
    if clone.get("cover_path"):
        clone["cover_url"] = _public_url(request, clone["cover_path"])
    if clone.get("user_avatar_path"):
        clone["user_avatar_url"] = _public_url(request, clone["user_avatar_path"])
    return _jsonable(clone)


def _parse_tags(value: str | None) -> list[str]:
    tags = []
    for raw in re.split(r"[,#]", value or ""):
        tag = re.sub(r"[^A-Za-z0-9_.-]+", "", raw.strip())[:32]
        if tag and tag.lower() not in {existing.lower() for existing in tags}:
            tags.append(tag)
    return tags[:12]


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


async def _save_upload(upload: UploadFile, media_kind: str, max_bytes: int) -> tuple[str, int]:
    mime_type = upload.content_type or mimetypes.guess_type(upload.filename or "")[0] or "application/octet-stream"
    ext = _safe_extension(upload.filename or "", mime_type)
    rel_dir = Path(media_kind) / uuid.uuid4().hex[:2]
    abs_dir = settings.uploads_dir / rel_dir
    abs_dir.mkdir(parents=True, exist_ok=True)
    rel_path = rel_dir / f"{uuid.uuid4().hex}{ext}"
    abs_path = settings.uploads_dir / rel_path
    written = 0
    try:
        with abs_path.open("wb") as out:
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise HTTPException(status_code=413, detail="Uploads must be 250MB or smaller.")
                out.write(chunk)
    except Exception:
        abs_path.unlink(missing_ok=True)
        raise
    return rel_path.as_posix(), written


async def _save_avatar(upload: UploadFile) -> tuple[str, int]:
    if _detect_media_kind(upload) != "image":
        raise HTTPException(status_code=400, detail="Profile pictures must be images.")
    return await _save_upload(upload, "avatars", 5 * 1024 * 1024)


@asynccontextmanager
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
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=ROOT_DIR / "app" / "static"), name="static")
app.mount("/app/static", StaticFiles(directory=ROOT_DIR / "app" / "static"), name="app_static")
app.mount("/uploads", StaticFiles(directory=settings.uploads_dir), name="uploads")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(ROOT_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "schema": settings.db_schema, "max_upload_bytes": settings.max_upload_bytes}


@app.post("/api/auth/register")
async def register(payload: RegisterRequest) -> dict[str, Any]:
    try:
        user = await db.register_user(payload.username, payload.password, payload.display_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    except Exception as exc:
        if "Duplicate" in str(exc):
            raise HTTPException(status_code=409, detail="That username is already taken.") from None
        raise
    return {"user": _jsonable(user), "token": issue_token(settings.session_secret, user)}


@app.post("/api/auth/login")
async def login(payload: LoginRequest) -> dict[str, Any]:
    try:
        user = await db.authenticate_user(payload.username, payload.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
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
    storage_path, _file_size = await _save_avatar(file)
    user = await db.update_user_avatar(
        int(auth["id"]),
        storage_path,
        file.content_type or "application/octet-stream",
        Path(file.filename or "avatar").name,
    )
    return {"user": _with_user_urls(request, user)}


@app.get("/api/me/bookmarks")
async def my_bookmarks(request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    items = await db.list_bookmarks(int(auth["id"]))
    return {"media": [_with_urls(request, item) for item in items]}


@app.get("/api/me/media")
async def my_media(request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    items = await db.list_user_media(int(auth["id"]))
    return {"media": [_with_urls(request, item) for item in items]}


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
    items = await db.list_media(
        viewer_id=viewer_id,
        media_kind=media_kind,
        category_id=category_id,
        query=(q or "").strip()[:80] or None,
        sort=sort,
        limit=limit,
        offset=offset,
    )
    return {"media": [_with_urls(request, item) for item in items]}


@app.get("/api/media/random")
async def random_media(request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    item = await db.random_media(viewer_id)
    if not item:
        raise HTTPException(status_code=404, detail="No media has been uploaded yet.")
    return {"media": _with_urls(request, item)}


@app.get("/api/tags")
async def tags() -> dict[str, Any]:
    return {"tags": _jsonable(await db.tag_cloud())}


@app.get("/api/collections")
async def collections(request: Request, mine: bool = False) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    if mine and not viewer_id:
        raise HTTPException(status_code=401, detail="Login required")
    rows = await db.list_collections(viewer_id=viewer_id, mine=mine)
    return {"collections": [_with_collection_urls(request, row) for row in rows]}


@app.post("/api/collections")
async def create_collection(payload: CollectionRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    try:
        collection = await db.create_collection(int(auth["id"]), payload.name, payload.description, payload.is_public)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"collection": _with_collection_urls(request, collection)}


@app.get("/api/collections/{collection_id}")
async def collection_detail(collection_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    collection = await db.get_collection(collection_id, viewer_id)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found.")
    items = await db.list_collection_media(collection_id, viewer_id)
    return {
        "collection": _with_collection_urls(request, collection),
        "media": [_with_urls(request, item) for item in items],
    }


@app.post("/api/collections/{collection_id}/items")
async def save_collection_item(collection_id: int, payload: CollectionItemRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    collection = await db.set_collection_item(collection_id, payload.media_id, int(auth["id"]), payload.saved)
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found.")
    return {"collection": _with_collection_urls(request, collection)}


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
) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    media_kind = _detect_media_kind(file)
    title = " ".join(title.strip().split())[:160]
    if not title:
        raise HTTPException(status_code=400, detail="Title is required.")
    if not category_id:
        category = await db.create_category(category_name, category_kind, int(auth["id"]))
        category_id = int(category["id"])
    storage_path, file_size = await _save_upload(file, media_kind, settings.max_upload_bytes)
    item = await db.add_media(
        {
            "user_id": int(auth["id"]),
            "category_id": category_id,
            "title": title,
            "description": description.strip()[:2000] or None,
            "tags": _parse_tags(tags),
            "media_kind": media_kind,
            "mime_type": file.content_type or "application/octet-stream",
            "original_filename": Path(file.filename or "upload").name[:255],
            "storage_path": storage_path,
            "file_size": file_size,
        }
    )
    return {"media": _with_urls(request, item)}


@app.get("/api/media/{media_id}")
async def media_detail(media_id: int, request: Request) -> dict[str, Any]:
    viewer_id = _user_id(_auth_optional(request))
    item = await db.get_media(media_id, viewer_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    await db.increment_counter(media_id, "views")
    comments = await db.list_comments(media_id)
    return {"media": _with_urls(request, item), "comments": _jsonable(comments)}


@app.post("/api/media/{media_id}/like")
async def like_media(media_id: int, payload: LikeRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    item = await db.set_like(media_id, int(auth["id"]), payload.liked)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    return {"media": _with_urls(request, item)}


@app.post("/api/media/{media_id}/bookmark")
async def bookmark_media(media_id: int, payload: BookmarkRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    item = await db.set_bookmark(media_id, int(auth["id"]), payload.bookmarked)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    return {"media": _with_urls(request, item)}


@app.post("/api/media/{media_id}/comments")
async def add_comment(media_id: int, payload: CommentRequest, request: Request) -> dict[str, Any]:
    auth = require_auth(request, settings.session_secret, settings.api_token_ttl_seconds)
    if not await db.get_media(media_id, int(auth["id"])):
        raise HTTPException(status_code=404, detail="Media not found.")
    try:
        comment = await db.add_comment(media_id, int(auth["id"]), payload.body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return {"comment": _jsonable(comment)}


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
    path = (settings.uploads_dir / item["storage_path"]).resolve()
    if str(path).startswith(str(settings.uploads_dir.resolve())):
        path.unlink(missing_ok=True)
    return {"deleted": True}


@app.get("/api/media/{media_id}/download", name="download_media")
async def download_media(media_id: int) -> FileResponse:
    item = await db.get_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="Media not found.")
    await db.increment_counter(media_id, "downloads")
    path = (settings.uploads_dir / item["storage_path"]).resolve()
    if not str(path).startswith(str(settings.uploads_dir.resolve())) or not path.exists():
        raise HTTPException(status_code=404, detail="File missing.")
    return FileResponse(path, media_type=item["mime_type"], filename=item["original_filename"])


@app.get("/api/stats")
async def stats() -> dict[str, Any]:
    return {"stats": _jsonable(await db.stats())}
