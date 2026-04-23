#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
source .venv/bin/activate
exec gunicorn -c gunicorn.conf.py app:app
