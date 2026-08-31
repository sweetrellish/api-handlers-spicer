#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
BACKUP_DIR="${BACKUP_DIR:-$ROOT/backups}"
SYNC_SCRIPT="$ROOT/deploy/linux/sync_companycam_webhook.sh"

WORKER_SERVICES=(
  marketsharp_queue_worker.service
  marketsharp_comment_worker.service
  spicer-flask-api.service
  true_fail_checker.service
)

usage() {
  cat <<'EOF'
Usage:
  rotate_worker_credentials.sh [options]

Interactive mode (default):
  Prompts for username/password updates and post-change actions.

Non-interactive mode:
  rotate_worker_credentials.sh --non-interactive \
    [--username <value>] \
    [--password <value>] \
    [--run-sync yes|no]

Environment variable alternatives:
  ROTATE_NON_INTERACTIVE=1
  ROTATE_MARKETSHARP_UI_LOGIN_USERNAME=<value>
  ROTATE_MARKETSHARP_UI_LOGIN_PASSWORD=<value>
  ROTATE_RUN_WEBHOOK_SYNC=yes|no

Notes:
  - At least one of username or password must be provided in non-interactive mode.
  - ENV_FILE and BACKUP_DIR can be overridden via environment variables.
EOF
}

to_lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

trim() {
  local v="$1"
  v="${v#"${v%%[![:space:]]*}"}"
  v="${v%"${v##*[![:space:]]}"}"
  printf '%s' "$v"
}

is_yes() {
  local v
  v="$(to_lower "$1")"
  [[ "$v" == "y" || "$v" == "yes" || "$v" == "1" || "$v" == "true" ]]
}

is_no() {
  local v
  v="$(to_lower "$1")"
  [[ "$v" == "n" || "$v" == "no" || "$v" == "0" || "$v" == "false" ]]
}

NON_INTERACTIVE="${ROTATE_NON_INTERACTIVE:-0}"
CLI_USER="${ROTATE_MARKETSHARP_UI_LOGIN_USERNAME:-}"
CLI_PASSWORD="${ROTATE_MARKETSHARP_UI_LOGIN_PASSWORD:-}"
RUN_SYNC_OVERRIDE="${ROTATE_RUN_WEBHOOK_SYNC:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --non-interactive)
      NON_INTERACTIVE="1"
      shift
      ;;
    --username)
      [[ $# -ge 2 ]] || { echo "Error: --username requires a value" >&2; exit 2; }
      CLI_USER="$2"
      shift 2
      ;;
    --password)
      [[ $# -ge 2 ]] || { echo "Error: --password requires a value" >&2; exit 2; }
      CLI_PASSWORD="$2"
      shift 2
      ;;
    --run-sync)
      [[ $# -ge 2 ]] || { echo "Error: --run-sync requires yes|no" >&2; exit 2; }
      RUN_SYNC_OVERRIDE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown option '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: env file not found at $ENV_FILE" >&2
  exit 1
fi

read_env() {
  local key="$1"
  local value
  value="$(grep -m1 "^${key}=" "$ENV_FILE" | cut -d'=' -f2- || true)"
  value="${value//$'\r'/}"
  value="$(trim "$value")"
  printf '%s' "$value"
}

upsert_env_key() {
  local key="$1"
  local value="$2"
  python3 - "$ENV_FILE" "$key" "$value" <<'PY'
from pathlib import Path
import sys

env_file = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]

lines = env_file.read_text(encoding='utf-8', errors='replace').splitlines()
needle = key + "="
replaced = False
out = []
for line in lines:
    if not replaced and line.startswith(needle):
        out.append(f"{key}={value}")
        replaced = True
    else:
        out.append(line)
if not replaced:
    out.append(f"{key}={value}")
env_file.write_text("\n".join(out) + "\n", encoding='utf-8')
PY
}

echo ""
echo "Worker Credential Rotation Assistant"
echo "------------------------------------"
echo "Repo root : $ROOT"
echo "Env file  : $ENV_FILE"
echo ""

current_user="$(read_env MARKETSHARP_UI_LOGIN_USERNAME)"
current_pw="$(read_env MARKETSHARP_UI_LOGIN_PASSWORD)"

if [[ -n "$current_user" ]]; then
  echo "Current MARKETSHARP_UI_LOGIN_USERNAME is set (len=${#current_user})"
else
  echo "Current MARKETSHARP_UI_LOGIN_USERNAME is missing"
fi
if [[ -n "$current_pw" ]]; then
  echo "Current MARKETSHARP_UI_LOGIN_PASSWORD is set (len=${#current_pw})"
else
  echo "Current MARKETSHARP_UI_LOGIN_PASSWORD is missing"
fi

echo ""
new_user=""
new_pw=""

if is_yes "$NON_INTERACTIVE"; then
  new_user="$(trim "$CLI_USER")"
  new_pw="$CLI_PASSWORD"

  if [[ -z "$new_user" && -z "$new_pw" ]]; then
    echo "Error: non-interactive mode requires --username and/or --password (or ROTATE_* env vars)." >&2
    exit 1
  fi

  if [[ -n "$CLI_USER" && -z "$new_user" ]]; then
    echo "Error: username cannot be whitespace-only." >&2
    exit 1
  fi

  echo "Non-interactive mode enabled."
  if [[ -n "$new_user" ]]; then
    echo "Username update requested (len=${#new_user})."
  fi
  if [[ -n "$new_pw" ]]; then
    echo "Password update requested (len=${#new_pw})."
  fi
else
  read -r -p "Update username? [y/N]: " update_user
  update_user="$(to_lower "$update_user")"
  if is_yes "$update_user"; then
    read -r -p "New MARKETSHARP_UI_LOGIN_USERNAME: " new_user
    new_user="$(trim "$new_user")"
    if [[ -z "$new_user" ]]; then
      echo "Error: username cannot be empty when updating." >&2
      exit 1
    fi
  fi

  read -r -p "Update password? [y/N]: " update_pw
  update_pw="$(to_lower "$update_pw")"
  if is_yes "$update_pw"; then
    read -r -s -p "New MARKETSHARP_UI_LOGIN_PASSWORD: " new_pw
    echo ""
    read -r -s -p "Confirm new password: " confirm_pw
    echo ""
    if [[ "$new_pw" != "$confirm_pw" ]]; then
      echo "Error: password confirmation does not match." >&2
      exit 1
    fi
    if [[ -z "$new_pw" ]]; then
      echo "Error: password cannot be empty when updating." >&2
      exit 1
    fi
  fi
fi

if [[ -z "$new_user" && -z "$new_pw" ]]; then
  echo "No credential changes requested."
  exit 0
fi

mkdir -p "$BACKUP_DIR"
stamp="$(date +%Y%m%d%H%M%S)"
backup_file="$BACKUP_DIR/.env_${stamp}"
cp "$ENV_FILE" "$backup_file"
echo "Backed up env file -> $backup_file"

if [[ -n "$new_user" ]]; then
  upsert_env_key "MARKETSHARP_UI_LOGIN_USERNAME" "$new_user"
fi
if [[ -n "$new_pw" ]]; then
  upsert_env_key "MARKETSHARP_UI_LOGIN_PASSWORD" "$new_pw"
fi

echo "Credentials updated in $ENV_FILE"

echo ""
do_sync=""
if is_yes "$NON_INTERACTIVE"; then
  do_sync="$(to_lower "${RUN_SYNC_OVERRIDE:-yes}")"
else
  read -r -p "Run webhook sync now? [Y/n]: " do_sync
  do_sync="$(to_lower "$do_sync")"
fi

if [[ -z "$do_sync" ]] || is_yes "$do_sync"; then
  if [[ -f "$SYNC_SCRIPT" ]]; then
    bash "$SYNC_SCRIPT"
  else
    echo "Warning: sync script not found at $SYNC_SCRIPT"
  fi
elif is_no "$do_sync"; then
  echo "Skipping webhook sync by request."
else
  echo "Warning: unrecognized --run-sync value '$do_sync'; skipping webhook sync."
fi

echo ""
echo "Post-change checks"
echo "------------------"
python3 "$ROOT/spicer_ops_menu.py" --status || true

port="$(read_env FLASK_PORT)"
if [[ -z "$port" ]]; then
  port="5001"
fi
health_url="http://127.0.0.1:${port}/health"
if command -v curl >/dev/null 2>&1; then
  echo ""
  echo "Health check: $health_url"
  curl -fsS "$health_url" || echo "Health endpoint not ready"
fi

if command -v systemctl >/dev/null 2>&1; then
  echo ""
  echo "Service status snapshot:"
  for svc in "${WORKER_SERVICES[@]}"; do
    status="$(systemctl is-active "$svc" 2>/dev/null || true)"
    printf "  %-42s %s\n" "$svc" "${status:-unknown}"
  done
  echo ""
  echo "If any service is not active, run this on the server:"
  echo "  sudo systemctl restart ${WORKER_SERVICES[*]}"
fi

echo ""
echo "Rotation assistant complete."
