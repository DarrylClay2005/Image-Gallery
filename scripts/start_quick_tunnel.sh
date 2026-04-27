#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8788}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT_DIR}/.bin"
mkdir -p "${BIN_DIR}"

CLOUDFLARED="${BIN_DIR}/cloudflared"
if [[ ! -x "${CLOUDFLARED}" ]]; then
  echo "Run scripts/start_live_backend.sh once to download cloudflared, or install cloudflared on PATH." >&2
  exit 1
fi

echo "Opening a public Cloudflare quick tunnel to http://127.0.0.1:${PORT}"
"${CLOUDFLARED}" tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}"
