import json
import re
from decimal import Decimal
from typing import Any

import aiomysql

from .auth import hash_password, verify_password
from .config import Settings


SLUG_RE = re.compile(r"[^a-z0-9]+")
USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,40}$")
MEDIA_KINDS = {"image", "video", "mixed"}
DEFAULT_USER_SETTINGS = {
    "theme_mode": "system",
    "accent_color": "#37c9a7",
    "grid_density": "comfortable",
    "default_sort": "new",
    "items_per_page": 60,
    "autoplay_previews": False,
    "muted_previews": True,
    "reduce_motion": False,
    "open_original_in_new_tab": False,
    "blur_video_previews": False,
}
USER_COLUMNS = (
    ("bio", "VARCHAR(500) NULL"),
    ("website_url", "VARCHAR(300) NULL"),
    ("location_label", "VARCHAR(80) NULL"),
    ("profile_color", "VARCHAR(20) NOT NULL DEFAULT '#37c9a7'"),
    ("avatar_path", "VARCHAR(500) NULL"),
    ("avatar_mime_type", "VARCHAR(120) NULL"),
    ("avatar_original_filename", "VARCHAR(255) NULL"),
    ("public_profile", "TINYINT(1) NOT NULL DEFAULT 1"),
    ("show_liked_count", "TINYINT(1) NOT NULL DEFAULT 1"),
    ("user_settings", "JSON NULL"),
    ("updated_at", "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
)


def slugify(value: str) -> str:
    slug = SLUG_RE.sub("-", value.lower()).strip("-")
    return slug[:80] or "category"


def normalize_username(username: str) -> str:
    username = str(username or "").strip()
    if not USERNAME_RE.fullmatch(username):
        raise ValueError("Username must be 3-40 characters using letters, numbers, dots, dashes, or underscores.")
    return username


class GalleryDatabase:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        await self._ensure_schema()
        self.pool = await aiomysql.create_pool(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            db=self.settings.db_schema,
            autocommit=True,
            minsize=1,
            maxsize=10,
        )
        await self.ensure_tables()

    async def close(self) -> None:
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _ensure_schema(self) -> None:
        conn = await aiomysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            user=self.settings.db_user,
            password=self.settings.db_password,
            autocommit=True,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.settings.db_schema}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            conn.close()

    async def ensure_tables(self) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      username VARCHAR(40) NOT NULL UNIQUE,
                      display_name VARCHAR(80) NOT NULL,
                      password_hash VARCHAR(255) NOT NULL,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      last_login_at TIMESTAMP NULL DEFAULT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS categories (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      name VARCHAR(80) NOT NULL UNIQUE,
                      slug VARCHAR(90) NOT NULL UNIQUE,
                      media_kind ENUM('image','video','mixed') NOT NULL DEFAULT 'mixed',
                      created_by BIGINT UNSIGNED NULL,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      CONSTRAINT fk_categories_user FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_items (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      user_id BIGINT UNSIGNED NOT NULL,
                      category_id BIGINT UNSIGNED NOT NULL,
                      title VARCHAR(160) NOT NULL,
                      description TEXT NULL,
                      tags JSON NULL,
                      media_kind ENUM('image','video') NOT NULL,
                      mime_type VARCHAR(120) NOT NULL,
                      original_filename VARCHAR(255) NOT NULL,
                      storage_path VARCHAR(500) NOT NULL,
                      file_size BIGINT UNSIGNED NOT NULL,
                      views BIGINT UNSIGNED NOT NULL DEFAULT 0,
                      downloads BIGINT UNSIGNED NOT NULL DEFAULT 0,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      FULLTEXT KEY ft_media_text (title, description),
                      KEY idx_media_created (created_at),
                      KEY idx_media_kind (media_kind),
                      KEY idx_media_category (category_id),
                      CONSTRAINT fk_media_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                      CONSTRAINT fk_media_category FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE RESTRICT
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_likes (
                      user_id BIGINT UNSIGNED NOT NULL,
                      media_id BIGINT UNSIGNED NOT NULL,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      PRIMARY KEY (user_id, media_id),
                      CONSTRAINT fk_likes_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                      CONSTRAINT fk_likes_media FOREIGN KEY (media_id) REFERENCES media_items(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_comments (
                      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      media_id BIGINT UNSIGNED NOT NULL,
                      user_id BIGINT UNSIGNED NOT NULL,
                      body VARCHAR(500) NOT NULL,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      KEY idx_comments_media (media_id, created_at),
                      CONSTRAINT fk_comments_media FOREIGN KEY (media_id) REFERENCES media_items(id) ON DELETE CASCADE,
                      CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_bookmarks (
                      user_id BIGINT UNSIGNED NOT NULL,
                      media_id BIGINT UNSIGNED NOT NULL,
                      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                      PRIMARY KEY (user_id, media_id),
                      CONSTRAINT fk_bookmarks_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                      CONSTRAINT fk_bookmarks_media FOREIGN KEY (media_id) REFERENCES media_items(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
        await self.ensure_user_columns()
        await self.seed_default_categories()

    async def ensure_user_columns(self) -> None:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME='users'
                    """,
                    (self.settings.db_schema,),
                )
                existing = {row["COLUMN_NAME"] for row in await cur.fetchall()}
                for name, definition in USER_COLUMNS:
                    if name not in existing:
                        await cur.execute(f"ALTER TABLE users ADD COLUMN {name} {definition}")
                await cur.execute("UPDATE users SET user_settings=%s WHERE user_settings IS NULL", (json.dumps(DEFAULT_USER_SETTINGS),))

    async def seed_default_categories(self) -> None:
        defaults = [
            ("Wallpapers", "image"),
            ("Profile Pictures", "image"),
            ("Memes", "mixed"),
            ("GIFs", "image"),
            ("Videos", "video"),
            ("Reaction Images", "image"),
            ("Phone Backgrounds", "image"),
            ("Desktop Backgrounds", "image"),
        ]
        for name, kind in defaults:
            await self.create_category(name, kind, None)

    async def register_user(self, username: str, password: str, display_name: str | None = None) -> dict[str, Any]:
        username = normalize_username(username)
        if len(password or "") < 8:
            raise ValueError("Password must be at least 8 characters.")
        display_name = (display_name or username).strip()[:80] or username
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "INSERT INTO users (username, display_name, password_hash) VALUES (%s, %s, %s)",
                    (username, display_name, hash_password(password)),
                )
                return await self.get_user(cur.lastrowid)

    async def authenticate_user(self, username: str, password: str) -> dict[str, Any] | None:
        username = normalize_username(username)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM users WHERE username=%s", (username,))
                user = await cur.fetchone()
                if not user or not verify_password(password, user["password_hash"]):
                    return None
                await cur.execute("UPDATE users SET last_login_at=CURRENT_TIMESTAMP WHERE id=%s", (user["id"],))
                return await self.get_user(user["id"])

    async def get_user(self, user_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT id, username, display_name, bio, website_url, location_label, profile_color,
                           avatar_path, avatar_mime_type, avatar_original_filename, public_profile,
                           show_liked_count, user_settings, created_at, updated_at
                    FROM users WHERE id=%s
                    """,
                    (user_id,),
                )
                user = await cur.fetchone()
                return self._decode_user(user) if user else None

    async def update_user_profile(self, user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        fields = {
            "display_name": self._clean_text(payload.get("display_name"), 80, required=True),
            "bio": self._clean_text(payload.get("bio"), 500),
            "website_url": self._clean_text(payload.get("website_url"), 300),
            "location_label": self._clean_text(payload.get("location_label"), 80),
            "profile_color": self._clean_color(payload.get("profile_color")),
            "public_profile": 1 if payload.get("public_profile", True) else 0,
            "show_liked_count": 1 if payload.get("show_liked_count", True) else 0,
        }
        if fields["website_url"] and not fields["website_url"].startswith(("http://", "https://")):
            raise ValueError("Website must start with http:// or https://.")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE users
                    SET display_name=%s, bio=%s, website_url=%s, location_label=%s,
                        profile_color=%s, public_profile=%s, show_liked_count=%s
                    WHERE id=%s
                    """,
                    (
                        fields["display_name"],
                        fields["bio"],
                        fields["website_url"],
                        fields["location_label"],
                        fields["profile_color"],
                        fields["public_profile"],
                        fields["show_liked_count"],
                        user_id,
                    ),
                )
        return await self.get_user(user_id)

    async def update_user_settings(self, user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        user = await self.get_user(user_id)
        if not user:
            raise ValueError("Account not found.")
        settings = dict(DEFAULT_USER_SETTINGS)
        settings.update(user.get("user_settings") or {})
        allowed_choices = {
            "theme_mode": {"system", "dark", "light"},
            "grid_density": {"compact", "comfortable", "wide"},
            "default_sort": {"new", "popular", "downloads", "old"},
        }
        for key in DEFAULT_USER_SETTINGS:
            if key not in payload:
                continue
            value = payload[key]
            if key in allowed_choices:
                if value not in allowed_choices[key]:
                    raise ValueError(f"Invalid {key}.")
                settings[key] = value
            elif key == "accent_color":
                settings[key] = self._clean_color(value)
            elif key == "items_per_page":
                settings[key] = max(12, min(int(value or 60), 100))
            else:
                settings[key] = bool(value)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE users SET user_settings=%s WHERE id=%s", (json.dumps(settings), user_id))
        return await self.get_user(user_id)

    async def update_user_avatar(self, user_id: int, storage_path: str, mime_type: str, original_filename: str) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE users
                    SET avatar_path=%s, avatar_mime_type=%s, avatar_original_filename=%s
                    WHERE id=%s
                    """,
                    (storage_path, mime_type[:120], original_filename[:255], user_id),
                )
        return await self.get_user(user_id)


    async def create_category(self, name: str, media_kind: str, user_id: int | None) -> dict[str, Any]:
        name = " ".join(str(name or "").strip().split())[:80]
        if not name:
            raise ValueError("Category name is required.")
        if media_kind not in MEDIA_KINDS:
            raise ValueError("Category type must be image, video, or mixed.")
        base_slug = slugify(name)
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT * FROM categories WHERE name=%s OR slug=%s", (name, base_slug))
                existing = await cur.fetchone()
                if existing:
                    return existing
                await cur.execute(
                    "INSERT INTO categories (name, slug, media_kind, created_by) VALUES (%s, %s, %s, %s)",
                    (name, base_slug, media_kind, user_id),
                )
                await cur.execute("SELECT * FROM categories WHERE id=%s", (cur.lastrowid,))
                return await cur.fetchone()

    async def list_categories(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT c.*, COUNT(m.id) AS media_count
                    FROM categories c
                    LEFT JOIN media_items m ON m.category_id = c.id
                    GROUP BY c.id
                    ORDER BY c.name
                    """
                )
                return list(await cur.fetchall())

    async def add_media(self, item: dict[str, Any]) -> dict[str, Any]:
        tags_json = json.dumps(item.get("tags") or [])
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    INSERT INTO media_items
                      (user_id, category_id, title, description, tags, media_kind, mime_type, original_filename, storage_path, file_size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        item["user_id"],
                        item["category_id"],
                        item["title"],
                        item.get("description"),
                        tags_json,
                        item["media_kind"],
                        item["mime_type"],
                        item["original_filename"],
                        item["storage_path"],
                        item["file_size"],
                    ),
                )
                return await self.get_media(cur.lastrowid, item["user_id"])

    async def list_media(
        self,
        *,
        viewer_id: int | None,
        media_kind: str | None = None,
        category_id: int | None = None,
        query: str | None = None,
        sort: str = "new",
        limit: int = 60,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if media_kind in {"image", "video"}:
            clauses.append("m.media_kind=%s")
            params.append(media_kind)
        if category_id:
            clauses.append("m.category_id=%s")
            params.append(category_id)
        if query:
            clauses.append("(m.title LIKE %s OR m.description LIKE %s OR m.tags LIKE %s)")
            needle = f"%{query}%"
            params.extend([needle, needle, needle])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = {
            "popular": "like_count DESC, m.views DESC, m.created_at DESC",
            "downloads": "m.downloads DESC, m.created_at DESC",
            "old": "m.created_at ASC",
        }.get(sort, "m.created_at DESC")
        viewer = viewer_id or 0
        sql_params = [viewer, viewer, viewer, viewer, viewer, viewer, *params, max(1, min(limit, 100)), max(0, offset)]
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    f"""
                    SELECT m.*, c.name AS category_name, c.slug AS category_slug,
                           u.username,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.display_name ELSE u.username END AS display_name,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.bio ELSE NULL END AS user_bio,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.website_url ELSE NULL END AS user_website_url,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.avatar_path ELSE NULL END AS user_avatar_path,
                           u.profile_color, u.public_profile,
                           COUNT(DISTINCT l.user_id) AS like_count,
                           COUNT(DISTINCT cm.id) AS comment_count,
                           MAX(CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END) AS bookmarked_by_me,
                           MAX(CASE WHEN l2.user_id IS NULL THEN 0 ELSE 1 END) AS liked_by_me
                    FROM media_items m
                    JOIN categories c ON c.id = m.category_id
                    JOIN users u ON u.id = m.user_id
                    LEFT JOIN media_likes l ON l.media_id = m.id
                    LEFT JOIN media_likes l2 ON l2.media_id = m.id AND l2.user_id = %s
                    LEFT JOIN media_bookmarks b ON b.media_id = m.id AND b.user_id = %s
                    LEFT JOIN media_comments cm ON cm.media_id = m.id
                    {where}
                    GROUP BY m.id
                    ORDER BY {order}
                    LIMIT %s OFFSET %s
                    """,
                    tuple(sql_params),
                )
                return [self._decode_media(row) for row in await cur.fetchall()]

    async def get_media(self, media_id: int, viewer_id: int | None = None) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT m.*, c.name AS category_name, c.slug AS category_slug,
                           u.username,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.display_name ELSE u.username END AS display_name,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.bio ELSE NULL END AS user_bio,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.website_url ELSE NULL END AS user_website_url,
                           CASE WHEN u.public_profile=1 OR u.id=%s THEN u.avatar_path ELSE NULL END AS user_avatar_path,
                           u.profile_color, u.public_profile,
                           COUNT(DISTINCT l.user_id) AS like_count,
                           COUNT(DISTINCT cm.id) AS comment_count,
                           MAX(CASE WHEN b.user_id IS NULL THEN 0 ELSE 1 END) AS bookmarked_by_me,
                           MAX(CASE WHEN l2.user_id IS NULL THEN 0 ELSE 1 END) AS liked_by_me
                    FROM media_items m
                    JOIN categories c ON c.id = m.category_id
                    JOIN users u ON u.id = m.user_id
                    LEFT JOIN media_likes l ON l.media_id = m.id
                    LEFT JOIN media_likes l2 ON l2.media_id = m.id AND l2.user_id = %s
                    LEFT JOIN media_bookmarks b ON b.media_id = m.id AND b.user_id = %s
                    LEFT JOIN media_comments cm ON cm.media_id = m.id
                    WHERE m.id=%s
                    GROUP BY m.id
                    """,
                    (viewer_id or 0, viewer_id or 0, viewer_id or 0, viewer_id or 0, viewer_id or 0, viewer_id or 0, media_id),
                )
                row = await cur.fetchone()
                return self._decode_media(row) if row else None

    async def increment_counter(self, media_id: int, column: str) -> None:
        if column not in {"views", "downloads"}:
            raise ValueError("Invalid counter")
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"UPDATE media_items SET {column}={column}+1 WHERE id=%s", (media_id,))

    async def set_like(self, media_id: int, user_id: int, liked: bool) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                if liked:
                    await cur.execute("INSERT IGNORE INTO media_likes (user_id, media_id) VALUES (%s, %s)", (user_id, media_id))
                else:
                    await cur.execute("DELETE FROM media_likes WHERE user_id=%s AND media_id=%s", (user_id, media_id))
        return await self.get_media(media_id, user_id)

    async def add_comment(self, media_id: int, user_id: int, body: str) -> dict[str, Any]:
        body = " ".join(str(body or "").strip().split())[:500]
        if not body:
            raise ValueError("Comment cannot be empty.")
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    "INSERT INTO media_comments (media_id, user_id, body) VALUES (%s, %s, %s)",
                    (media_id, user_id, body),
                )
                await cur.execute(
                    """
                    SELECT cm.*, u.username,
                           CASE WHEN u.public_profile=1 THEN u.display_name ELSE u.username END AS display_name,
                           CASE WHEN u.public_profile=1 THEN u.avatar_path ELSE NULL END AS user_avatar_path
                    FROM media_comments cm JOIN users u ON u.id = cm.user_id
                    WHERE cm.id=%s
                    """,
                    (cur.lastrowid,),
                )
                return await cur.fetchone()

    async def list_comments(self, media_id: int) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT cm.*, u.username,
                           CASE WHEN u.public_profile=1 THEN u.display_name ELSE u.username END AS display_name,
                           CASE WHEN u.public_profile=1 THEN u.avatar_path ELSE NULL END AS user_avatar_path
                    FROM media_comments cm JOIN users u ON u.id = cm.user_id
                    WHERE cm.media_id=%s
                    ORDER BY cm.created_at DESC
                    LIMIT 80
                    """,
                    (media_id,),
                )
                return list(await cur.fetchall())

    async def stats(self) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute("SELECT COUNT(*) AS users FROM users")
                users = (await cur.fetchone())["users"]
                await cur.execute("SELECT COUNT(*) AS categories FROM categories")
                categories = (await cur.fetchone())["categories"]
                await cur.execute("SELECT COUNT(*) AS media, COALESCE(SUM(file_size), 0) AS bytes FROM media_items")
                media = await cur.fetchone()
                await cur.execute("SELECT COUNT(*) AS likes FROM media_likes")
                likes = (await cur.fetchone())["likes"]
                return {"users": users, "categories": categories, "media": media["media"], "bytes": media["bytes"], "likes": likes}

    async def set_bookmark(self, media_id: int, user_id: int, bookmarked: bool) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                if bookmarked:
                    await cur.execute("INSERT IGNORE INTO media_bookmarks (user_id, media_id) VALUES (%s, %s)", (user_id, media_id))
                else:
                    await cur.execute("DELETE FROM media_bookmarks WHERE user_id=%s AND media_id=%s", (user_id, media_id))
        return await self.get_media(media_id, user_id)

    async def list_bookmarks(self, user_id: int, limit: int = 80) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(
                    """
                    SELECT m.*, c.name AS category_name, c.slug AS category_slug,
                           u.username, u.display_name, u.bio AS user_bio, u.website_url AS user_website_url,
                           u.avatar_path AS user_avatar_path, u.profile_color, u.public_profile,
                           COUNT(DISTINCT l.user_id) AS like_count,
                           COUNT(DISTINCT cm.id) AS comment_count,
                           1 AS bookmarked_by_me,
                           MAX(CASE WHEN l2.user_id IS NULL THEN 0 ELSE 1 END) AS liked_by_me
                    FROM media_bookmarks bm
                    JOIN media_items m ON m.id = bm.media_id
                    JOIN categories c ON c.id = m.category_id
                    JOIN users u ON u.id = m.user_id
                    LEFT JOIN media_likes l ON l.media_id = m.id
                    LEFT JOIN media_likes l2 ON l2.media_id = m.id AND l2.user_id = %s
                    LEFT JOIN media_comments cm ON cm.media_id = m.id
                    WHERE bm.user_id=%s
                    GROUP BY m.id, bm.created_at
                    ORDER BY bm.created_at DESC
                    LIMIT %s
                    """,
                    (user_id, user_id, max(1, min(limit, 100))),
                )
                return [self._decode_media(row) for row in await cur.fetchall()]

    def _decode_media(self, row: dict[str, Any]) -> dict[str, Any]:
        tags = row.get("tags")
        if isinstance(tags, str):
            try:
                row["tags"] = json.loads(tags)
            except json.JSONDecodeError:
                row["tags"] = []
        elif tags is None:
            row["tags"] = []
        row["liked_by_me"] = bool(row.get("liked_by_me"))
        row["bookmarked_by_me"] = bool(row.get("bookmarked_by_me"))
        for key in ("like_count", "comment_count", "views", "downloads", "file_size"):
            if isinstance(row.get(key), Decimal):
                row[key] = int(row[key])
        return row

    def _decode_user(self, user: dict[str, Any]) -> dict[str, Any]:
        raw_settings = user.get("user_settings")
        settings = dict(DEFAULT_USER_SETTINGS)
        if isinstance(raw_settings, str):
            try:
                settings.update(json.loads(raw_settings) or {})
            except json.JSONDecodeError:
                pass
        elif isinstance(raw_settings, dict):
            settings.update(raw_settings)
        user["user_settings"] = settings
        user["public_profile"] = bool(user.get("public_profile"))
        user["show_liked_count"] = bool(user.get("show_liked_count"))
        return user

    def _clean_text(self, value: Any, max_length: int, required: bool = False) -> str | None:
        text = " ".join(str(value or "").strip().split())
        if not text:
            if required:
                raise ValueError("Display name is required.")
            return None
        return text[:max_length]

    def _clean_color(self, value: Any) -> str:
        color = str(value or "#37c9a7").strip()
        if not re.fullmatch(r"#[0-9A-Fa-f]{6}", color):
            raise ValueError("Color must be a hex value like #37c9a7.")
        return color.lower()
