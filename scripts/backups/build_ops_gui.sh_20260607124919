#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GUI_DIR="$ROOT_DIR/API Handler Interactive GUI"

if [[ ! -d "$GUI_DIR" ]]; then
  echo "Ops GUI directory not found: $GUI_DIR"
  exit 1
fi

cd "$GUI_DIR"

if [[ ! -d node_modules ]]; then
  echo "Installing GUI dependencies..."
  npm install
fi

echo "Building ops GUI for /ops-gui hosting..."
npm run build:ops-gui

echo "Build complete. Flask can now serve the app at /ops-gui"
