#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
REQ_FILE="$SCRIPT_DIR/requirements.txt"
REQ_HASH_FILE="$SCRIPT_DIR/.requirements.sha256"

pick_python() {
  local candidates=(
    "$VENV_DIR/bin/python"
    "$SCRIPT_DIR/../.venv/bin/python"
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

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  rm -rf "$VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

PYTHON_BIN="$(pick_python || true)"
if [[ -z "$PYTHON_BIN" ]]; then
  echo "No usable Python interpreter found for comment worker." >&2
  exit 1
fi

current_hash="$(shasum -a 256 "$REQ_FILE" | awk '{print $1}')"
cached_hash=""
if [[ -f "$REQ_HASH_FILE" ]]; then
  cached_hash="$(cat "$REQ_HASH_FILE")"
fi

if [[ "$current_hash" != "$cached_hash" ]]; then
  "$PYTHON_BIN" -m pip install -q -r "$REQ_FILE"
  printf '%s\n' "$current_hash" > "$REQ_HASH_FILE"
fi

export PYTHONPATH="$SCRIPT_DIR/../src:$SCRIPT_DIR/..${PYTHONPATH:+:$PYTHONPATH}"

exec "$PYTHON_BIN" -u "$SCRIPT_DIR/comment_worker.py" "$@"