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
TUNNEL_PROVIDER="${GALLERY_TUNNEL_PROVIDER:-auto}"
PAGES_ORIGIN="${GALLERY_PAGES_ORIGIN:-https://darrylclay2005.github.io}"
PAGES_URL="${GALLERY_PAGES_PUBLIC_URL:-https://darrylclay2005.github.io/Image-Gallery/}"
MAX_TUNNEL_START_ATTEMPTS="${GALLERY_MAX_TUNNEL_START_ATTEMPTS:-12}"
TUNNEL_READY_ATTEMPTS="${GALLERY_TUNNEL_READY_ATTEMPTS:-180}"

mkdir -p "${BIN_DIR}" "${LOG_DIR}"

local_urls_json() {
  PORT="${PORT}" python3 <<'PY'
import json
import os
import socket

port = os.environ["PORT"]
urls = [f"http://127.0.0.1:{port}", f"http://localhost:{port}"]

try:
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.connect(("1.1.1.1", 80))
    urls.append(f"http://{udp.getsockname()[0]}:{port}")
    udp.close()
except Exception:
    pass

try:
    hostname = socket.gethostname()
    for family, *_rest, sockaddr in socket.getaddrinfo(hostname, None, family=socket.AF_INET, type=socket.SOCK_STREAM):
        ip = sockaddr[0]
        if ip.startswith("127."):
            continue
        urls.append(f"http://{ip}:{port}")
except Exception:
    pass

seen = set()
deduped = []
for url in urls:
    if url in seen:
        continue
    seen.add(url)
    deduped.append(url)

print(json.dumps(deduped))
PY
}

write_config() {
  local gallery_url="$1"
  local local_urls
  local_urls="$(local_urls_json)"
  cat > "${CONFIG_FILE}" <<JSON
{
  "gallery_url": "${gallery_url}",
  "status": "live",
  "local_urls": ${local_urls},
  "updated_at": "$(date -Is)"
}
JSON
}

write_offline_config() {
  local local_urls
  local_urls="$(local_urls_json)"
  cat > "${CONFIG_FILE}" <<JSON
{
  "gallery_url": "",
  "status": "offline",
  "local_urls": ${local_urls},
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
  if ! GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false run_git ls-remote --exit-code origin HEAD >/dev/null 2>&1; then
    echo "Skipping live-config auto-push because GitHub auth is unavailable; local file was still updated."
    return
  fi
  echo "Publishing updated live-config.json to GitHub Pages..."
  run_git add live-config.json
  run_git commit -m "Update live backend URL" -- live-config.json || true
  GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false run_git push origin main || echo "Could not push live-config.json automatically. Push it manually when convenient." >&2
}

publish_offline_config() {
  write_offline_config
  publish_config
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

port_in_use() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :${PORT}" | grep -q ":${PORT}"
    return
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1
    return
  fi
  return 1
}

kill_stale_port() {
  if [[ "${GALLERY_KILL_STALE_PORT:-1}" != "1" ]]; then
    return 0
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${PORT}/tcp" >/dev/null 2>&1 || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"${PORT}" | xargs -r kill -TERM || true
  fi
}

start_fallback_backend() {
  if [[ "${ALLOW_FALLBACK_BACKEND}" != "1" ]]; then
    return 1
  fi
  if port_in_use && ! backend_ready; then
    echo "Port ${PORT} is busy but the Image Gallery backend is not responding; attempting cleanup."
    kill_stale_port
    sleep 1
  fi
  if port_in_use && ! backend_ready; then
    echo "Port ${PORT} is still busy; cannot start fallback backend." >&2
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

tunnel_retry_delay() {
  local attempt="$1"
  if (( attempt <= 3 )); then
    echo 15
  elif (( attempt <= 6 )); then
    echo 30
  else
    echo 60
  fi
}

log_tunnel_failure_details() {
  echo "Tunnel exited early. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  if grep -Fq 'status_code="429 Too Many Requests"' "${TUNNEL_LOG}" 2>/dev/null; then
    echo "Cloudflare quick tunnel creation is being rate-limited. Waiting before retrying." >&2
  fi
}

publish_live_url() {
  local gallery_url="$1"
  write_config "${gallery_url}"
  PUBLISHED_GALLERY_URL="${gallery_url}"
  publish_config
  echo "Live backend URL: ${gallery_url}"
  echo "GitHub Pages frontend: ${PAGES_URL}"
  wait "${TUNNEL_PID}"
  exit $?
}

start_pinggy_tunnel() {
  echo "Opening Pinggy tunnel to http://127.0.0.1:${PORT}"
  install_python_deps
  : > "${TUNNEL_LOG}"
  "${VENV_DIR}/bin/python" "${ROOT_DIR}/scripts/start_pinggy_tunnel.py" --port "${PORT}" >"${TUNNEL_LOG}" 2>&1 &
  TUNNEL_PID="$!"
  GALLERY_URL=""
  for _ in {1..60}; do
    if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
      echo "Pinggy tunnel exited early. Last log lines:" >&2
      tail -80 "${TUNNEL_LOG}" >&2 || true
      return 1
    fi
    GALLERY_URL="$(grep -Eo 'https://[^[:space:]]+\\.pinggy-free\\.link' "${TUNNEL_LOG}" | tail -1 || true)"
    if [[ -n "${GALLERY_URL}" ]]; then
      echo "Waiting for ${GALLERY_URL} to answer through Pinggy..."
      for ((ready_attempt=1; ready_attempt<=TUNNEL_READY_ATTEMPTS; ready_attempt++)); do
        if curl -fsS --max-time 10 "${GALLERY_URL}/api/health" >/dev/null 2>&1; then
          publish_live_url "${GALLERY_URL}"
        fi
        sleep 1
      done
      echo "Pinggy URL was created but never became reachable. Last log lines:" >&2
      tail -80 "${TUNNEL_LOG}" >&2 || true
      kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
      return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for Pinggy tunnel URL. Last log lines:" >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
  kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
  return 1
}

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
  publish_offline_config
  exit 1
fi

publish_offline_config

if [[ "${TUNNEL_PROVIDER}" == "cloudflare" || "${TUNNEL_PROVIDER}" == "auto" ]]; then
  for ((attempt=1; attempt<=MAX_TUNNEL_START_ATTEMPTS; attempt++)); do
    echo "Opening Cloudflare quick tunnel to http://127.0.0.1:${PORT} (attempt ${attempt}/${MAX_TUNNEL_START_ATTEMPTS})"
    : > "${TUNNEL_LOG}"
    "${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" >"${TUNNEL_LOG}" 2>&1 &
    TUNNEL_PID="$!"

    GALLERY_URL=""
    for _ in {1..120}; do
      if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
        log_tunnel_failure_details
        break
      fi
      GALLERY_URL="$(grep -Eo 'https://[-a-zA-Z0-9.]+trycloudflare\.com' "${TUNNEL_LOG}" | tail -1 || true)"
      if [[ -z "${GALLERY_URL}" ]]; then
        sleep 1
        continue
      fi
      echo "Waiting for ${GALLERY_URL} to answer through Cloudflare..."
      for ((ready_attempt=1; ready_attempt<=TUNNEL_READY_ATTEMPTS; ready_attempt++)); do
        if curl -fsS --max-time 10 "${GALLERY_URL}/api/health" >/dev/null 2>&1; then
          publish_live_url "${GALLERY_URL}"
        fi
        if ! kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
          log_tunnel_failure_details
          break
        fi
        sleep 1
      done
      if [[ -n "${GALLERY_URL}" ]] && kill -0 "${TUNNEL_PID}" >/dev/null 2>&1; then
        echo "Tunnel URL was created but never became reachable. Last log lines:" >&2
        tail -80 "${TUNNEL_LOG}" >&2 || true
        kill "${TUNNEL_PID}" >/dev/null 2>&1 || true
      fi
      break
    done

    if (( attempt < MAX_TUNNEL_START_ATTEMPTS )); then
      retry_delay="$(tunnel_retry_delay "${attempt}")"
      echo "Retrying Cloudflare quick tunnel startup in ${retry_delay}s..."
      sleep "${retry_delay}"
    fi
  done

  echo "Exceeded Cloudflare quick tunnel startup retries." >&2
  tail -80 "${TUNNEL_LOG}" >&2 || true
fi

if [[ "${TUNNEL_PROVIDER}" == "pinggy" || "${TUNNEL_PROVIDER}" == "auto" ]]; then
  start_pinggy_tunnel
fi

publish_offline_config
exit 1
