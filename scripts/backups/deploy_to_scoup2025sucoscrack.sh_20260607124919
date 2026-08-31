#!/usr/bin/env bash
set -euo pipefail

# One-command deploy for the Spicer stack.
# Defaults target host to the requested server, but everything is overrideable.
SERVER_HOST="${SERVER_HOST:-scoup2025sucoscrack}"
SERVER_USER="${SERVER_USER:-$USER}"
SERVER_PATH="${SERVER_PATH:-/opt/spicer}"
SSH_TARGET="${SERVER_USER}@${SERVER_HOST}"
DEPLOY_SYSTEMD="${DEPLOY_SYSTEMD:-0}"

echo "==> Deploy target: ${SSH_TARGET}:${SERVER_PATH}"

# Ensure destination exists.
ssh "${SSH_TARGET}" "mkdir -p '${SERVER_PATH}'"

# Sync project files (keep secrets and local runtime state out of transfer).
rsync -avz --delete \
  --exclude ".git" \
  --exclude ".venv" \
  --exclude "__pycache__" \
  --exclude "*.pyc" \
  --exclude ".env" \
  --exclude "pending_comments.db" \
  --exclude "cc_webhook_dedupe.db" \
  --exclude ".marketsharp-profile*" \
  --exclude "logs/*" \
  "$(cd "$(dirname "$0")/.." && pwd)/" \
  "${SSH_TARGET}:${SERVER_PATH}/"

# Remote bootstrap.
ssh "${SSH_TARGET}" "SERVER_PATH='${SERVER_PATH}' DEPLOY_SYSTEMD='${DEPLOY_SYSTEMD}' bash -s" <<'REMOTE'
set -euo pipefail

cd "${SERVER_PATH}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required on remote host." >&2
  exit 1
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium

mkdir -p logs
chmod +x scripts/start_webhook.sh scripts/start_worker.sh

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "NOTICE: .env was created from .env.example. Edit ${SERVER_PATH}/.env before production use."
fi

if [[ "${DEPLOY_SYSTEMD}" == "1" ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    USER_NAME="$(whoami)"

    cat > /tmp/spicer-webhook.service <<EOF
[Unit]
Description=Spicer Webhook (Gunicorn)
After=network.target

[Service]
User=${USER_NAME}
WorkingDirectory=${SERVER_PATH}
EnvironmentFile=${SERVER_PATH}/.env
ExecStart=${SERVER_PATH}/.venv/bin/gunicorn -c ${SERVER_PATH}/gunicorn.conf.py app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    cat > /tmp/spicer-worker.service <<EOF
[Unit]
Description=Spicer UI Worker
After=network.target

[Service]
User=${USER_NAME}
WorkingDirectory=${SERVER_PATH}
EnvironmentFile=${SERVER_PATH}/.env
ExecStart=${SERVER_PATH}/.venv/bin/python ${SERVER_PATH}/queue_ui_poster.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    if command -v sudo >/dev/null 2>&1; then
      sudo mv /tmp/spicer-webhook.service /etc/systemd/system/spicer-webhook.service
      sudo mv /tmp/spicer-worker.service /etc/systemd/system/spicer-worker.service
      sudo systemctl daemon-reload
      sudo systemctl enable --now spicer-webhook
      sudo systemctl enable --now spicer-worker
      echo "==> systemd services enabled and started"
    else
      echo "NOTICE: DEPLOY_SYSTEMD=1 set, but sudo is unavailable. Skipping systemd install."
      rm -f /tmp/spicer-webhook.service /tmp/spicer-worker.service
    fi
  else
    echo "NOTICE: DEPLOY_SYSTEMD=1 set, but systemctl not available. Skipping systemd install."
  fi
fi

echo "==> Remote bootstrap complete"
REMOTE

echo "==> Deploy complete"
echo "Next: ssh ${SSH_TARGET} and edit ${SERVER_PATH}/.env if needed"
