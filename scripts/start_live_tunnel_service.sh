#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8788}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
BIN_DIR="${ROOT_DIR}/.bin"
CONFIG_FILE="${ROOT_DIR}/live-config.json"
LOG_DIR="${ROOT_DIR}/.runtime"
TUNNEL_LOG="${LOG_DIR}/cloudflared-service.log"
UVICORN_LOG="${LOG_DIR}/uvicorn-service-fallback.log"
AUTO_PUSH_CONFIG="${GALLERY_AUTO_PUSH_CONFIG:-1}"
ALLOW_FALLBACK_BACKEND="${GALLERY_SERVICE_START_BACKEND_IF_MISSING:-1}"
PAGES_ORIGIN="${GALLERY_PAGES_ORIGIN:-https://darrylclay2005.github.io}"
PAGES_URL="${GALLERY_PAGES_PUBLIC_URL:-https://darrylclay2005.github.io/Image-Gallery/}"

mkdir -p "${BIN_DIR}" "${LOG_DIR}"

write_config() {
  local gallery_url="$1"
  cat > "${CONFIG_FILE}" <<JSON
{
  "gallery_url": "${gallery_url}",
  "updated_at": "$(date -Is)"
}
JSON
}

write_offline_config() {
  cat > "${CONFIG_FILE}" <<JSON
{
  "gallery_url": "",
  "updated_at": "$(date -Is)"
}
JSON
}

run_git() {
  if command -v flatpak-spawn >/dev/null 2>&1; then
    flatpak-spawn --host git -C "${ROOT_DIR}" "$@"
  else
    git -C "${ROOT_DIR}" "$@"
  fi
}

publish_config() {
  if [[ "${AUTO_PUSH_CONFIG}" != "1" ]]; then
    echo "GALLERY_AUTO_PUSH_CONFIG is disabled; live-config.json was updated locally only."
    return
  fi
  if ! command -v git >/dev/null 2>&1 && ! command -v flatpak-spawn >/dev/null 2>&1; then
    echo "Skipping live-config push because git is unavailable." >&2
    return
  fi
  if ! run_git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Skipping live-config push because ${ROOT_DIR} is not a git work tree." >&2
    return
  fi
  if run_git diff --quiet -- live-config.json; then
    echo "live-config.json already matches the current tunnel URL."
    return
  fi
  echo "Publishing updated live-config.json to GitHub Pages..."
  run_git add live-config.json
  run_git commit -m "Update live backend URL" -- live-config.json || true
  run_git push origin main || echo "Could not push live-config.json automatically. Push it manually when convenient." >&2
}

install_python_deps() {
  if [[ -x "${VENV_DIR}/bin/python" ]] && ! "${VENV_DIR}/bin/python" -m pip --version >/dev/null 2>&1; then
    rm -rf "${VENV_DIR}"
  fi
  if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    python3 -m venv "${VENV_DIR}"
  fi
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
  "${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements.txt"
}

cloudflared_bin() {
  if command -v cloudflared >/dev/null 2>&1; then
    command -v cloudflared
    return
  fi
  local local_bin="${BIN_DIR}/cloudflared"
  if [[ -x "${local_bin}" ]]; then
    echo "${local_bin}"
    return
  fi
  local machine arch url
  machine="$(uname -m)"
  arch="amd64"
  if [[ "${machine}" == "aarch64" || "${machine}" == "arm64" ]]; then
    arch="arm64"
  fi
  url="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${arch}"
  echo "Downloading cloudflared (${arch})..." >&2
  if command -v curl >/dev/null 2>&1; then
    curl -L --fail "${url}" -o "${local_bin}"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "${local_bin}" "${url}"
  else
    echo "Need curl or wget to download cloudflared." >&2
    exit 1
  fi
  chmod +x "${local_bin}"
  echo "${local_bin}"
}

backend_ready() {
  curl -fsS --max-time 5 "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1
}

start_fallback_backend() {
  if [[ "${ALLOW_FALLBACK_BACKEND}" != "1" ]]; then
    return 1
  fi
  echo "No backend responded on http://127.0.0.1:${PORT}; starting local fallback backend."
  install_python_deps
  export GALLERY_PAGES_PUBLIC_URL="${PAGES_URL}"
  export GALLERY_CORS_ALLOWED_ORIGINS="${GALLERY_CORS_ALLOWED_ORIGINS:-${PAGES_ORIGIN},${PAGES_URL%/},http://127.0.0.1:${PORT},http://localhost:${PORT}}"
  export GALLERY_DB_HOST="${GALLERY_DB_HOST:-127.0.0.1}"
  export GALLERY_DB_USER="${GALLERY_DB_USER:-${DB_USER:-${MYSQL_USER:-botuser}}}"
  cd "${ROOT_DIR}"
  "${VENV_DIR}/bin/python" -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" >"${UVICORN_LOG}" 2>&1 &
  UVICORN_PID="$!"
}

cleanup() {
  if [[ -n "${PUBLISHED_GALLERY_URL:-}" ]] && grep -Fq "\"gallery_url\": \"${PUBLISHED_GALLERY_URL}\"" "${CONFIG_FILE}" 2>/dev/null; then
    write_offline_config
    publish_config
  fi
  if [[ -n "${TUNNEL_PID:-}" ]]; then kill "${TUNNEL_PID}" >/dev/null 2>&1 || true; fi
  if [[ -n "${UVICORN_PID:-}" ]]; then kill "${UVICORN_PID}" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT INT TERM

CLOUDFLARED="$(cloudflared_bin)"

echo "Waiting for Image Gallery backend on http://127.0.0.1:${PORT}"
for _ in {1..45}; do
  if backend_ready; then break; fi
  sleep 2
done

if ! backend_ready; then
  start_fallback_backend || true
  for _ in {1..60}; do
    if backend_ready; then break; fi
    if [[ -n "${UVICORN_PID:-}" ]] && ! kill -0 "${UVICORN_PID}" >/dev/null 2>&1; then
      echo "Fallback backend exited early. Last log lines:" >&2
      tail -80 "${UVICORN_LOG}" >&2 || true
      exit 1
    fi
    sleep 1
  done
fi

if ! backend_ready; then
  echo "Image Gallery backend did not become reachable on port ${PORT}." >&2
  echo "Start Docker or run scripts/start_live_backend.sh ${PORT}." >&2
  exit 1
fi

echo "Opening Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
"${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
TUNNEL_PID="$!"

GALLERY_URL=""
for _ in {1..120}; do
  if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
    echo "Tunnel exited early. Last log lines:" >&2
    tail -80 "${TUNNEL_LOG}" >&2 || true
    exit 1
  fi
  GALLERY_URL="$(grep -Eo 'https://[-a-zA-Z0-9.]+trycloudflare\.com' "${TUNNEL_LOG}" | tail -1 || true)"
  if [[ -z "${GALLERY_URL}" ]]; then
    sleep 1
    continue
  fi
  echo "Waiting for ${GALLERY_URL} to answer through Cloudflare..."
  for _ in {1..40}; do
    if curl -fsS --max-time 10 "${GALLERY_URL}/api/health" >/dev/null 2>&1; then
      write_config "${GALLERY_URL}"
      PUBLISHED_GALLERY_URL="${GALLERY_URL}"
      publish_config
      echo "Live backend URL: ${GALLERY_URL}"
      echo "GitHub Pages frontend: ${PAGES_URL}"
      wait "${TUNNEL_PID}"
      exit $?
    fi
    sleep 1
  done
  echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  exit 1
done

echo "Timed out waiting for Cloudflare tunnel URL." >&2
tail -80 "${TUNNEL_LOG}" >&2 || true
exit 1
