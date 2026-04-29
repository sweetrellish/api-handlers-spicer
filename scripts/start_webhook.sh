#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)"
#python src/app.py
cd "$(dirname "$0")/.."
source .venv/bin/activate
exec gunicorn -c gunicorn.conf.py src.app:app
