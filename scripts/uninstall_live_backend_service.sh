#!/usr/bin/env bash
set -euo pipefail

SERVICE_FILE="${HOME}/.config/systemd/user/image-gallery-live-backend.service"

systemctl --user disable --now image-gallery-live-backend.service >/dev/null 2>&1 || true
rm -f "${SERVICE_FILE}"
systemctl --user daemon-reload

echo "Removed image-gallery-live-backend.service."
