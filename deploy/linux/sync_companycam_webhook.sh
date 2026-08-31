#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/home/rellis/spicer/.env}"
CLOUDFLARED_UNIT="${CLOUDFLARED_UNIT:-}"
WEBHOOK_ENDPOINT_PATH="/webhook/companycam"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ ! -f "$ENV_FILE" ]]; then
  echo "env file not found: $ENV_FILE" >&2
  exit 1
fi

read_env() {
  local key="$1"
  local value
  value="$(grep -m1 "^${key}=" "$ENV_FILE" | cut -d'=' -f2- || true)"
  value="${value//$'\r'/}"
  # Trim surrounding whitespace so accidental spaces in .env don't corrupt URLs/tokens.
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

trim_quotes() {
  local s="$1"
  s="${s%\"}"
  s="${s#\"}"
  s="${s%\'}"
  s="${s#\'}"
  printf '%s' "$s"
}

normalize_base_url() {
  local raw
  raw="$(trim_quotes "$1")"
  raw="${raw%/}"

  # Accept either a base URL or a full webhook URL in WEBHOOK_URL.
  if [[ "$raw" == *"${WEBHOOK_ENDPOINT_PATH}" ]]; then
    raw="${raw%${WEBHOOK_ENDPOINT_PATH}}"
  fi

  raw="${raw%/}"
  printf '%s' "$raw"
}

api_call() {
  local method="$1"
  local url="$2"
  local data="${3:-}"
  local body_file="$TMP_DIR/body.json"
  local status

  if [[ -n "$data" ]]; then
    status="$(curl -sS -o "$body_file" -w "%{http_code}" --request "$method" \
      --url "$url" \
      --header "accept: application/json" \
      --header "content-type: application/json" \
      --header "authorization: Bearer $COMPANYCAM_WEBHOOK_TOKEN" \
      --data "$data")"
  else
    status="$(curl -sS -o "$body_file" -w "%{http_code}" --request "$method" \
      --url "$url" \
      --header "accept: application/json" \
      --header "authorization: Bearer $COMPANYCAM_WEBHOOK_TOKEN")"
  fi

  if [[ ! "$status" =~ ^2 ]]; then
    echo "CompanyCam API $method $url failed with HTTP $status" >&2
    cat "$body_file" >&2 || true
    return 1
  fi

  cat "$body_file"
}

COMPANYCAM_WEBHOOK_TOKEN="$(read_env COMPANYCAM_WEBHOOK_TOKEN)"
COMPANYCAM_WEBHOOK_SECRET="$(read_env COMPANYCAM_WEBHOOK_SECRET)"
COMPANYCAM_BASE_URL="$(read_env COMPANYCAM_BASE_URL)"

if [[ -z "${COMPANYCAM_WEBHOOK_TOKEN:-}" ]]; then
  echo "COMPANYCAM_WEBHOOK_TOKEN missing" >&2
  exit 1
fi

if [[ -z "${COMPANYCAM_WEBHOOK_SECRET:-}" ]]; then
  echo "COMPANYCAM_WEBHOOK_SECRET missing" >&2
  exit 1
fi

if [[ -z "${COMPANYCAM_BASE_URL:-}" ]]; then
  COMPANYCAM_BASE_URL="https://api.companycam.com"
fi

resolve_cloudflared_unit() {
  if [[ -n "${CLOUDFLARED_UNIT:-}" ]]; then
    printf '%s' "$CLOUDFLARED_UNIT"
    return 0
  fi

  local candidate
  for candidate in spicer-cloudflared cloudflared; do
    if systemctl status "${candidate}.service" >/dev/null 2>&1; then
      printf '%s' "$candidate"
      return 0
    fi
  done

  printf 'spicer-cloudflared'
  return 0
}

CLOUDFLARED_UNIT="$(resolve_cloudflared_unit)"
#try the journalctl command to get the live tunnel URL from the cloudflared logs first but if that fails, fall back to the env file value
live_base_url="$(journalctl -u "$CLOUDFLARED_UNIT" --no-pager -n 120 | grep -Eo 'https://[-a-z0-9]+\.trycloudflare\.com' | tail -1 || true)"

if [[ -z "$live_base_url" ]]; then
  live_base_url="$(read_env WEBHOOK_URL)"
  if [[ -z "$live_base_url" ]]; then
    echo "could not determine live tunnel URL from $CLOUDFLARED_UNIT logs or env file" >&2
    exit 1
  fi
fi

live_base_url="$(normalize_base_url "$live_base_url")"
if [[ -z "$live_base_url" ]]; then
  echo "resolved base URL is empty after normalization" >&2
  exit 1
fi

target_url="${live_base_url}${WEBHOOK_ENDPOINT_PATH}"

webhooks_json="$(api_call GET "$COMPANYCAM_BASE_URL/v2/webhooks")"

readarray -t parsed < <(python3 - "$target_url" "$webhooks_json" <<'PY'
import json,sys
target = sys.argv[1]
raw = sys.argv[2] if len(sys.argv) > 2 else '[]'
data = json.loads(raw or '[]')
if not isinstance(data, list):
    print('[]')
    print('')
    raise SystemExit(0)
def normalize(url: str) -> str:
    if not isinstance(url, str):
        return ''
    return url.rstrip('/')

enabled = [w for w in data if isinstance(w, dict) and w.get('enabled')]
t = normalize(target)
matching = [
    w for w in enabled
    if normalize(w.get('url', '')) == t and 'comment.*' in (w.get('scopes') or [])
]
if matching:
    print('[]')
    print('ok')
else:
    # Only delete hooks that point at the same target URL but have wrong scope/state.
    stale_ids = [
        w.get('id','') for w in enabled
        if normalize(w.get('url', '')) == t and w.get('id')
    ]
    print(json.dumps(stale_ids))
    print('update')
PY
)

stale_ids_json="${parsed[0]:-[]}"
mode="${parsed[1]:-update}"

if [[ "$mode" == "ok" ]]; then
  echo "webhook already correct: $target_url"
  exit 0
fi

while IFS= read -r stale_id; do
  [[ -z "$stale_id" ]] && continue
  api_call DELETE "$COMPANYCAM_BASE_URL/v2/webhooks/$stale_id" >/dev/null
done < <(python3 - "$stale_ids_json" <<'PY'
import json,sys
raw = sys.argv[1] if len(sys.argv) > 1 else '[]'
try:
    ids = json.loads(raw)
except Exception:
    ids = []
if not isinstance(ids, list):
    ids = []
for i in ids:
    if isinstance(i, str) and i:
        print(i)
PY
)

create_resp="$(api_call POST "$COMPANYCAM_BASE_URL/v2/webhooks" "{\"url\":\"$target_url\",\"scopes\":[\"comment.*\"],\"enabled\":true,\"token\":\"$COMPANYCAM_WEBHOOK_SECRET\"}")"

new_id="$(python3 - "$create_resp" <<'PY'
import json,sys
try:
  raw = sys.argv[1] if len(sys.argv) > 1 else '{}'
  data = json.loads(raw or '{}')
except json.JSONDecodeError:
    data = {}
print(data.get('id',''))
PY
)"

echo "webhook synced to $target_url (id=${new_id:-unknown})"