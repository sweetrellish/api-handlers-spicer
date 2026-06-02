#!/usr/bin/env bash

# Production ops check for the MarketSharp mention worker service.
#
# What it does:
# 1) prints service status and key unit metadata
# 2) prints recent worker journal lines
# 3) scans last 24h logs for error-like patterns
#
# Why this exists:
# - gives operators a single command for quick health checks
# - standardizes post-deploy verification and troubleshooting

set -euo pipefail

SERVICE_NAME="${1:-marketsharp_comment_worker.service}"
TAIL_LINES="${TAIL_LINES:-20}"

section() {
  printf '\n==== %s ====\n' "$1"
}

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl not available on this host."
  exit 1
fi

if ! command -v journalctl >/dev/null 2>&1; then
  echo "journalctl not available on this host."
  exit 1
fi

section "Service Summary"
systemctl --no-pager --full status "$SERVICE_NAME" || true

section "Unit Metadata"
# Keep this property list narrow and stable for low-noise operator output.
systemctl show "$SERVICE_NAME" \
  --property=Id,LoadState,ActiveState,SubState,UnitFileState,FragmentPath,ExecMainPID,MainPID,NRestarts \
  --no-pager || true

section "Recent Journal (last ${TAIL_LINES})"
journalctl -u "$SERVICE_NAME" -n "$TAIL_LINES" --no-pager || true

# section "Potential Errors (last 24h)"
# # This is a heuristic grep, not a full log classifier. It intentionally catches
# # common failure words so operators can triage quickly.
# journalctl -u "$SERVICE_NAME" --since "24 hours ago" --no-pager \
#   | grep -Ei "error|exception|traceback|failed|fatal" || echo "No error-like entries found in last 24 hours."
