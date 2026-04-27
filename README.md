# Image Gallery

FastAPI and MySQL gallery for wallpapers, profile pictures, memes, GIFs, and videos up to 250MB.

## Features
- User registration and login with token auth.
- Upload images, GIFs, and videos.
- Pick an existing category or create a new category while uploading.
- Browse by media type, category, search text, newest, likes, or downloads.
- Like posts, comment on posts, copy direct media addresses, and download files.
- Own MySQL schema and tables using the shared bot database login defaults.
- Static GitHub Pages frontend that reads `live-config.json` to find the current public backend tunnel.

## Database
Defaults match the requested shared login:

- User: `botuser`
- Password: `botlogins`
- Schema: `image_gallery`

The backend creates the schema and tables automatically on startup.

## Local Run
```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8788
```

Open `http://127.0.0.1:8788`.

## Live GitHub Pages Backend
```bash
scripts/start_live_backend.sh 8788
```

The script starts the backend, opens a Cloudflare quick tunnel, writes the tunnel URL to `live-config.json`, commits it, and pushes it to `main` when a Git remote is configured.

## Auto Start
```bash
scripts/install_live_backend_service.sh
```

Stop and remove it with:

```bash
scripts/uninstall_live_backend_service.sh
```
