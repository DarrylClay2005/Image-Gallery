from __future__ import annotations

import base64
import io
import json
import mimetypes
import os
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .classification import canonical_category_pair, clean_tokens, infer_category_pair


TOKEN_RE = re.compile(r"[a-z0-9]{2,}")
STOP_TAGS = {
    "the", "and", "for", "with", "from", "fullview", "generated", "image",
    "standard", "lite", "upscayl", "wallpaper", "desktop", "phone",
    "background", "version", "text", "movie", "poster", "upload",
}
LOW_SIGNAL_RE = re.compile(
    r"^(?:img|dsc|pxl|mvimg|screenshot|image|photo|video|scan|untitled|temp|"
    r"whatsapp image|snapchat|signal)[ _-]*\d*$",
    re.IGNORECASE,
)
HEXISH_RE = re.compile(r"^[0-9a-f]{8,}$", re.IGNORECASE)
KNOWN_CATEGORIES = [
    "My Little Pony",
    "FNAF",
    "GIFs",
    "KPOP Demon Hunters",
    "Videos",
    "Crossovers",
    "Hyperdimension Neptunia",
    "Profile Pictures",
    "Cartoon",
    "Memes",
    "Resident Evil",
    "Xenoblade",
    "Sonic",
    "Desktop Backgrounds",
    "Phone Backgrounds",
    "Wallpapers",
]


@dataclass
class SmartMediaAnalysis:
    title: str
    suggested_filename: str
    tags: list[str]
    category_name: str | None
    subcategory_name: str | None
    is_adult: bool
    source: str
    confidence: float
    size: tuple[int, int] | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "suggested_filename": self.suggested_filename,
            "tags": list(self.tags),
            "category_name": self.category_name,
            "subcategory_name": self.subcategory_name,
            "is_adult": bool(self.is_adult),
            "source": self.source,
            "confidence": round(float(self.confidence), 3),
            "size": list(self.size) if self.size else None,
            "reason": self.reason,
        }


def analyze_media_path(
    path: Path,
    *,
    mime_type: str,
    media_kind: str,
    title_hint: str = "",
    description_hint: str = "",
    tags_hint: list[str] | None = None,
    ai_enabled: bool | None = None,
    ai_api_key: str | None = None,
    ai_base_url: str | None = None,
    ai_model: str | None = None,
    ai_timeout_seconds: int | None = None,
) -> SmartMediaAnalysis:
    content = path.read_bytes()
    return analyze_media_bytes(
        content=content,
        filename=path.name,
        mime_type=mime_type,
        media_kind=media_kind,
        title_hint=title_hint,
        description_hint=description_hint,
        tags_hint=tags_hint,
        ai_enabled=ai_enabled,
        ai_api_key=ai_api_key,
        ai_base_url=ai_base_url,
        ai_model=ai_model,
        ai_timeout_seconds=ai_timeout_seconds,
    )


def analyze_media_bytes(
    *,
    content: bytes,
    filename: str,
    mime_type: str,
    media_kind: str,
    title_hint: str = "",
    description_hint: str = "",
    tags_hint: list[str] | None = None,
    ai_enabled: bool | None = None,
    ai_api_key: str | None = None,
    ai_base_url: str | None = None,
    ai_model: str | None = None,
    ai_timeout_seconds: int | None = None,
) -> SmartMediaAnalysis:
    size = _media_size(content, filename, mime_type, media_kind)
    fallback = _heuristic_analysis(
        filename=filename,
        mime_type=mime_type,
        media_kind=media_kind,
        title_hint=title_hint,
        description_hint=description_hint,
        tags_hint=tags_hint or [],
        size=size,
    )
    enabled = _resolve_bool(ai_enabled, env_name="GALLERY_AI_ENABLED", default=bool(ai_api_key or os.getenv("GALLERY_AI_API_KEY") or os.getenv("OPENAI_API_KEY")))
    api_key = str(ai_api_key or os.getenv("GALLERY_AI_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if not enabled or not api_key:
        return fallback
    preview_url = _preview_data_url(content, filename, mime_type, media_kind)
    if not preview_url:
        return fallback
    try:
        ai_result = _openai_vision_analysis(
            preview_url=preview_url,
            filename=filename,
            mime_type=mime_type,
            media_kind=media_kind,
            title_hint=title_hint,
            description_hint=description_hint,
            tags_hint=tags_hint or [],
            fallback=fallback,
            api_key=api_key,
            base_url=str(ai_base_url or os.getenv("GALLERY_AI_BASE_URL") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com/v1").rstrip("/"),
            model=str(ai_model or os.getenv("GALLERY_AI_MODEL") or "gpt-5.4-nano").strip(),
            timeout_seconds=max(10, int(ai_timeout_seconds or os.getenv("GALLERY_AI_TIMEOUT_SECONDS") or 45)),
        )
    except Exception as exc:
        fallback.reason = f"AI suggestion unavailable, using local analyzer: {exc}"
        return fallback
    return _merge_analysis(
        ai_result=ai_result,
        fallback=fallback,
        filename=filename,
        mime_type=mime_type,
        media_kind=media_kind,
    )


def is_low_signal_filename(filename: str) -> bool:
    return _looks_low_signal_name(filename)


def _resolve_bool(value: bool | None, *, env_name: str, default: bool) -> bool:
    if value is not None:
        return bool(value)
    raw = str(os.getenv(env_name, "")).strip().lower()
    if raw:
        return raw not in {"0", "false", "no", "off"}
    return default


def _heuristic_analysis(
    *,
    filename: str,
    mime_type: str,
    media_kind: str,
    title_hint: str,
    description_hint: str,
    tags_hint: list[str],
    size: tuple[int, int] | None,
) -> SmartMediaAnalysis:
    clean_hint_title = _clean_title(title_hint)
    category_name, subcategory_name = canonical_category_pair(
        *infer_category_pair(
            filename=filename,
            media_kind=media_kind,
            title=clean_hint_title or filename,
            size=size,
        )
    )
    title = clean_hint_title or _build_title(filename, category_name, subcategory_name)
    tags = _build_tags(
        filename=filename,
        title=title,
        description=description_hint,
        category_name=category_name,
        subcategory_name=subcategory_name,
        media_kind=media_kind,
        size=size,
        tags_hint=tags_hint,
    )
    suggested_filename = _suggest_filename(
        title=title,
        source_filename=filename,
        mime_type=mime_type,
        category_name=category_name,
        subcategory_name=subcategory_name,
    )
    return SmartMediaAnalysis(
        title=title,
        suggested_filename=suggested_filename,
        tags=tags,
        category_name=category_name,
        subcategory_name=subcategory_name,
        is_adult=_looks_adult(title, description_hint, tags, filename, mime_type),
        source="heuristic",
        confidence=0.45,
        size=size,
    )


def _merge_analysis(
    *,
    ai_result: dict[str, Any],
    fallback: SmartMediaAnalysis,
    filename: str,
    mime_type: str,
    media_kind: str,
) -> SmartMediaAnalysis:
    confidence = _clamp_float(ai_result.get("confidence"), 0.0, 1.0, fallback.confidence)
    ai_title = _clean_title(ai_result.get("title"))
    ai_category = _clean_label(ai_result.get("category_name"))
    ai_subcategory = _clean_label(ai_result.get("subcategory_name"))
    category_name, subcategory_name = canonical_category_pair(
        ai_category or fallback.category_name,
        ai_subcategory or fallback.subcategory_name,
    )
    if confidence < 0.45:
        category_name = fallback.category_name
        subcategory_name = fallback.subcategory_name
    title = ai_title or fallback.title
    tags = _merge_tags(_normalize_tags(ai_result.get("tags")), fallback.tags)
    if not tags:
        tags = list(fallback.tags)
    suggested_filename = _suggest_filename(
        title=title,
        source_filename=filename,
        mime_type=mime_type,
        category_name=category_name,
        subcategory_name=subcategory_name,
        suggested_base=_clean_filename_base(ai_result.get("suggested_filename_base")),
    )
    return SmartMediaAnalysis(
        title=title,
        suggested_filename=suggested_filename,
        tags=tags,
        category_name=category_name,
        subcategory_name=subcategory_name,
        is_adult=bool(ai_result.get("is_adult")) or fallback.is_adult,
        source="openai" if confidence >= 0.45 else fallback.source,
        confidence=max(confidence, fallback.confidence if confidence < 0.45 else 0.0),
        size=fallback.size,
        reason=_clean_title(ai_result.get("reason")) or fallback.reason,
    )


def _openai_vision_analysis(
    *,
    preview_url: str,
    filename: str,
    mime_type: str,
    media_kind: str,
    title_hint: str,
    description_hint: str,
    tags_hint: list[str],
    fallback: SmartMediaAnalysis,
    api_key: str,
    base_url: str,
    model: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    instructions = (
        "You analyze media uploads for a personal gallery. "
        "Return structured JSON only. "
        "Make the title natural and concise. "
        "Create up to 12 short lowercase tags. "
        "Choose a broad category when uncertain. "
        "Only give a specific character or subcategory if the visual evidence is clear. "
        "Prefer these main categories when they fit: "
        + ", ".join(KNOWN_CATEGORIES)
        + ". "
        "If nothing specific fits, use Wallpapers, Desktop Backgrounds, Phone Backgrounds, Videos, or Profile Pictures. "
        "Do not invent lore if the image is ambiguous."
    )
    user_text = (
        f"Filename: {filename}\n"
        f"MIME type: {mime_type}\n"
        f"Media kind: {media_kind}\n"
        f"Existing title hint: {_clean_title(title_hint) or '(none)'}\n"
        f"Existing description hint: {_clean_title(description_hint) or '(none)'}\n"
        f"Existing tags hint: {', '.join(_normalize_tags(tags_hint)) or '(none)'}\n"
        f"Local analyzer fallback title: {fallback.title}\n"
        f"Local analyzer fallback category: {fallback.category_name or 'Wallpapers'}\n"
        f"Local analyzer fallback subcategory: {fallback.subcategory_name or '(none)'}"
    )
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "title": {"type": "string"},
            "suggested_filename_base": {"type": "string"},
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 12,
            },
            "category_name": {"type": "string"},
            "subcategory_name": {"type": "string"},
            "is_adult": {"type": "boolean"},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
        "required": [
            "title",
            "suggested_filename_base",
            "tags",
            "category_name",
            "subcategory_name",
            "is_adult",
            "confidence",
            "reason",
        ],
    }
    payload = {
        "model": model,
        "store": False,
        "temperature": 0.2,
        "reasoning": {"effort": "low"},
        "text": {
            "format": {
                "type": "json_schema",
                "name": "gallery_media_analysis",
                "strict": True,
                "schema": schema,
            }
        },
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": instructions}]},
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": user_text},
                    {"type": "input_image", "image_url": preview_url, "detail": "low"},
                ],
            },
        ],
    }
    request = urllib.request.Request(
        f"{base_url}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error {exc.code}: {body[:240]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI API network error: {exc.reason}") from exc
    data = json.loads(raw or "{}")
    text = _response_text(data)
    if not text:
        raise RuntimeError("OpenAI response did not include structured text.")
    return json.loads(text)


def _response_text(payload: dict[str, Any]) -> str:
    output_text = str(payload.get("output_text") or "").strip()
    if output_text:
        return output_text
    for item in payload.get("output") or []:
        for content in item.get("content") or []:
            text = str(content.get("text") or "").strip()
            if text:
                return text
    return ""


def _media_size(content: bytes, filename: str, mime_type: str, media_kind: str) -> tuple[int, int] | None:
    if media_kind == "image":
        try:
            from PIL import Image

            Image.MAX_IMAGE_PIXELS = None
            with Image.open(io.BytesIO(content)) as image:
                return tuple(int(part) for part in image.size)
        except Exception:
            return None
    return _video_size(content, filename, mime_type) if media_kind == "video" else None


def _video_size(content: bytes, filename: str, mime_type: str) -> tuple[int, int] | None:
    suffix = Path(filename or "video").suffix or mimetypes.guess_extension(mime_type or "") or ".mp4"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as handle:
        handle.write(content)
        handle.flush()
        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height",
                    "-of", "json",
                    handle.name,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(result.stdout or "{}")
            stream = (payload.get("streams") or [{}])[0]
            width = int(stream.get("width") or 0)
            height = int(stream.get("height") or 0)
            return (width, height) if width and height else None
        except Exception:
            return None


def _preview_data_url(content: bytes, filename: str, mime_type: str, media_kind: str) -> str | None:
    if media_kind == "image":
        return _image_preview_data_url(content, filename, mime_type)
    if media_kind == "video":
        return _video_preview_data_url(content, filename, mime_type)
    return None


def _image_preview_data_url(content: bytes, filename: str, mime_type: str) -> str | None:
    try:
        from PIL import Image, ImageSequence

        Image.MAX_IMAGE_PIXELS = None
        with Image.open(io.BytesIO(content)) as image:
            frame = next(ImageSequence.Iterator(image), image)
            preview = frame.convert("RGB")
            preview.thumbnail((1400, 1400))
            output = io.BytesIO()
            preview.save(output, format="JPEG", quality=84, optimize=True)
            encoded = base64.b64encode(output.getvalue()).decode("ascii")
            return f"data:image/jpeg;base64,{encoded}"
    except Exception:
        try:
            encoded = base64.b64encode(content).decode("ascii")
            return f"data:{mime_type or 'application/octet-stream'};base64,{encoded}"
        except Exception:
            return None


def _video_preview_data_url(content: bytes, filename: str, mime_type: str) -> str | None:
    suffix = Path(filename or "video").suffix or mimetypes.guess_extension(mime_type or "") or ".mp4"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as handle:
        handle.write(content)
        handle.flush()
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-v", "error",
                    "-i", handle.name,
                    "-frames:v", "1",
                    "-vf", "scale='min(1400,iw)':-2",
                    "-f", "image2pipe",
                    "-vcodec", "mjpeg",
                    "pipe:1",
                ],
                capture_output=True,
                check=True,
            )
            encoded = base64.b64encode(result.stdout).decode("ascii")
            return f"data:image/jpeg;base64,{encoded}"
        except Exception:
            return None


def _build_title(filename: str, category_name: str | None, subcategory_name: str | None) -> str:
    stem = Path(filename).stem.strip()
    if _looks_low_signal_name(filename):
        label = subcategory_name or category_name or ("Imported Video" if Path(filename).suffix.lower() in {".mp4", ".mov", ".m4v", ".webm", ".ogg"} else "Imported Media")
        suffix = re.sub(r"[^0-9A-Za-z]+", "", stem)[:10]
        return f"{label} {suffix}".strip()[:160]
    normalized = re.sub(r"[_-]+", " ", stem)
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return str(subcategory_name or category_name or "Imported Media")[:160]
    return " ".join(_title_case_token(token) for token in tokens)[:160]


def _title_case_token(token: str) -> str:
    special = {
        "mlp": "MLP",
        "fnaf": "FNAF",
        "sfm": "SFM",
        "eqg": "EQG",
        "ai": "AI",
        "pmv": "PMV",
        "kpop": "KPOP",
        "vr": "VR",
        "3d": "3D",
        "4k": "4K",
        "1080p": "1080p",
        "1440p": "1440p",
    }
    lower = token.lower()
    if lower in special:
        return special[lower]
    if lower.isdigit():
        return lower
    return lower.capitalize()


def _build_tags(
    *,
    filename: str,
    title: str,
    description: str,
    category_name: str | None,
    subcategory_name: str | None,
    media_kind: str,
    size: tuple[int, int] | None,
    tags_hint: list[str],
) -> list[str]:
    tags: list[str] = []
    if category_name:
        tags.extend(_normalize_tokens(category_name))
    if subcategory_name:
        tags.extend(_normalize_tokens(subcategory_name))
    if media_kind == "video":
        tags.append("video")
    if Path(filename).suffix.lower() == ".gif":
        tags.append("gif")
    if size:
        width, height = size
        if width > height:
            tags.append("landscape")
        elif height > width:
            tags.append("portrait")
        else:
            tags.append("square")
        if width >= 3840 or height >= 2160:
            tags.append("4k")
        elif width >= 2560 or height >= 1440:
            tags.append("1440p")
        elif width >= 1920 or height >= 1080:
            tags.append("1080p")
    for value in [title, description, Path(filename).stem]:
        tags.extend(_normalize_tokens(value))
    tags.extend(_normalize_tags(tags_hint))
    return _merge_tags(tags, [])


def _normalize_tokens(value: str | None) -> list[str]:
    results: list[str] = []
    for token in clean_tokens(value):
        if token in STOP_TAGS or len(token) < 3:
            continue
        results.append(token)
    return results


def _merge_tags(primary: list[str], secondary: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in list(primary) + list(secondary):
        for tag in _normalize_tags([raw]):
            if tag in seen:
                continue
            seen.add(tag)
            merged.append(tag)
            if len(merged) >= 12:
                return merged
    return merged


def _normalize_tags(values: Any) -> list[str]:
    if isinstance(values, str):
        candidates = re.split(r"[,#\s]+", values)
    else:
        candidates = list(values or [])
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        tag = re.sub(r"[^a-z0-9_.-]+", "", str(raw or "").strip().lower())[:32]
        if not tag or tag in STOP_TAGS or len(tag) < 2 or tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    return normalized[:12]


def _clean_title(value: Any) -> str:
    return " ".join(str(value or "").strip().split())[:160]


def _clean_label(value: Any) -> str | None:
    cleaned = " ".join(str(value or "").strip().split())[:80]
    return cleaned or None


def _clean_filename_base(value: Any) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", str(value or "").strip().lower())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned[:90]


def _suggest_filename(
    *,
    title: str,
    source_filename: str,
    mime_type: str,
    category_name: str | None,
    subcategory_name: str | None,
    suggested_base: str = "",
) -> str:
    suffix = Path(source_filename or "upload").suffix.lower()
    if not suffix:
        suffix = mimetypes.guess_extension(mime_type or "") or ""
    if suffix == ".jpe":
        suffix = ".jpg"
    base = suggested_base
    if not base:
        seed = title or subcategory_name or category_name or Path(source_filename).stem or "media"
        base = re.sub(r"[^a-z0-9]+", "-", seed.lower()).strip("-")
    if not base:
        base = "media"
    if _looks_low_signal_name(source_filename) and base in {"imported-media", "media", "upload"}:
        base = f"{base}-{Path(source_filename).stem[:8].lower()}".strip("-")
    return f"{base[:90] or 'media'}{suffix}"


def _looks_low_signal_name(filename: str) -> bool:
    stem = Path(filename or "").stem.strip()
    if not stem:
        return True
    lowered = stem.lower()
    compact = re.sub(r"[^a-z0-9]+", "", lowered)
    if LOW_SIGNAL_RE.match(lowered):
        return True
    if HEXISH_RE.fullmatch(compact):
        return True
    if compact.isdigit() and len(compact) >= 6:
        return True
    if re.fullmatch(r"[0-9a-f-]{16,}", lowered):
        return True
    return False


def _looks_adult(title: str, description: str, tags: list[str], filename: str, mime_type: str) -> bool:
    combined = " ".join([title, description, " ".join(tags), filename, mime_type]).lower()
    normalized = re.sub(r"[^a-z0-9+]+", " ", combined)
    adult_keywords = {
        "18plus", "18+", "adult", "nsfw", "not safe for work", "nude", "nudity",
        "explicit", "porn", "porno", "sex", "sexual", "hentai", "ecchi", "lewd",
        "erotic", "fetish", "onlyfans", "xxx",
    }
    return any(word in normalized or word in combined for word in adult_keywords)


def _clamp_float(value: Any, minimum: float, maximum: float, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, number))
