#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-/home/rellis/spicer/.env}"
CLOUDFLARED_UNIT="${CLOUDFLARED_UNIT:-spicer-cloudflared}"
WEBHOOK_ENDPOINT_PATH="/webhook/companycam"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "env file not found: $ENV_FILE" >&2
  exit 1
fi

read_env() {
  local key="$1"
  local value
  value="$(grep -m1 "^${key}=" "$ENV_FILE" | cut -d'=' -f2- || true)"
  printf '%s' "${value//$'\r'/}"
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

live_base_url="$(journalctl -u "$CLOUDFLARED_UNIT" --no-pager -n 120 | grep -Eo 'https://[-a-z0-9]+\.trycloudflare\.com' | tail -1 || true)"

if [[ -z "$live_base_url" ]]; then
  echo "could not determine live tunnel URL from $CLOUDFLARED_UNIT logs" >&2
  exit 1
fi

target_url="${live_base_url}${WEBHOOK_ENDPOINT_PATH}"

webhooks_json="$(curl --max-time 10 -sS --request GET \
  --url "$COMPANYCAM_BASE_URL/v2/webhooks" \
  --header "accept: application/json" \
  --header "authorization: Bearer $COMPANYCAM_WEBHOOK_TOKEN")"

readarray -t parsed < <(python3 - "$target_url" "$webhooks_json" <<'PY'
import json,sys
target = sys.argv[1]
raw = sys.argv[2] if len(sys.argv) > 2 else '[]'
data = json.loads(raw or '[]')
if not isinstance(data, list):
    print('')
    print('')
    raise SystemExit(0)
enabled = [w for w in data if isinstance(w, dict) and w.get('enabled')]
matching = [w for w in enabled if w.get('url') == target and 'comment.*' in (w.get('scopes') or [])]
if matching:
    print(matching[0].get('id',''))
    print('ok')
else:
    # choose first enabled webhook id as stale candidate
    print(enabled[0].get('id','') if enabled else '')
    print('update')
PY
)

stale_id="${parsed[0]:-}"
mode="${parsed[1]:-update}"

if [[ "$mode" == "ok" ]]; then
  echo "webhook already correct: $target_url"
  exit 0
fi

if [[ -n "$stale_id" ]]; then
  curl --max-time 10 -sS --request DELETE \
    --url "$COMPANYCAM_BASE_URL/v2/webhooks/$stale_id" \
    --header "accept: application/json" \
    --header "authorization: Bearer $COMPANYCAM_WEBHOOK_TOKEN" >/dev/null
fi

create_resp="$(curl --max-time 10 -sS --request POST \
  --url "$COMPANYCAM_BASE_URL/v2/webhooks" \
  --header "accept: application/json" \
  --header "content-type: application/json" \
  --header "authorization: Bearer $COMPANYCAM_WEBHOOK_TOKEN" \
  --data "{\"url\":\"$target_url\",\"scopes\":[\"comment.*\"],\"enabled\":true,\"token\":\"$COMPANYCAM_WEBHOOK_SECRET\"}")"

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
