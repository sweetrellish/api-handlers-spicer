#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

pick_python() {
  local candidates=(
    "$ROOT_DIR/.venv/bin/python"
    "$ROOT_DIR/tagger/.venv/bin/python"
    "$(command -v python3 || true)"
  )
  local p
  for p in "${candidates[@]}"; do
    if [[ -n "$p" && -x "$p" ]]; then
      printf '%s\n' "$p"
      return 0
    fi
  done
  return 1
}

PYTHON_BIN="$(pick_python || true)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "No usable Python interpreter found for queue worker." >&2
  exit 1
fi

export PYTHONPATH="$ROOT_DIR/scripts:$ROOT_DIR/src:$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"
# Default to MarketSharp-safe canonical mentions unless explicitly overridden.
export MARKETSHARP_NOTE_MENTION_STYLE="${MARKETSHARP_NOTE_MENTION_STYLE:-plain}"

exec "$PYTHON_BIN" -u "$SCRIPT_DIR/queue_ui_poster.py"
