#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DIR="${HOME}/.config/systemd/user"
SERVICE_FILE="${SERVICE_DIR}/image-gallery-live-backend.service"

mkdir -p "${SERVICE_DIR}"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Image Gallery live backend and GitHub Pages tunnel
After=network-online.target

[Service]
Type=simple
WorkingDirectory=${ROOT_DIR}
ExecStart=${ROOT_DIR}/scripts/start_live_backend.sh 8788
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now image-gallery-live-backend.service

echo "Installed ${SERVICE_FILE}"
systemctl --user --no-pager status image-gallery-live-backend.service
