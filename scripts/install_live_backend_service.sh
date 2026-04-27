#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SERVICE_DIR}/image-gallery-live-backend.service"

run_systemctl() {
  if command -v systemctl >/dev/null 2>&1; then
    systemctl "$@"
  elif command -v flatpak-spawn >/dev/null 2>&1; then
    flatpak-spawn --host systemctl "$@"
  else
    echo "systemctl is required to install the live backend service." >&2
    exit 1
  fi
}

mkdir -p "${SERVICE_DIR}"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Image Gallery live backend and GitHub Pages tunnel
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/env bash -lc "cd '${ROOT_DIR}' && exec scripts/start_live_backend.sh 8788"
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF

run_systemctl --user daemon-reload
run_systemctl --user enable --now image-gallery-live-backend.service

echo "Installed ${SERVICE_FILE}"
run_systemctl --user --no-pager status image-gallery-live-backend.service
