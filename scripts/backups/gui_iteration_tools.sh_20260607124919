#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/home/rellis/spicer/API Handler Interactive GUI}"
BKP_ROOT="${BKP_ROOT:-/home/rellis/spicer/backups/gui}"
SERVICE_URL="${SERVICE_URL:-http://127.0.0.1:5001}"

usage() {
  cat <<'EOF'
Usage:
  gui_iteration_tools.sh backup <iteration_name>
  gui_iteration_tools.sh build
  gui_iteration_tools.sh verify
  gui_iteration_tools.sh rollback <backup_dir_name>
  gui_iteration_tools.sh list

Examples:
  gui_iteration_tools.sh backup iter04_timeline_cards
  gui_iteration_tools.sh build
  gui_iteration_tools.sh verify
  gui_iteration_tools.sh rollback iter04_timeline_cards_20260607_133000

Environment overrides:
  APP_DIR=/home/rellis/spicer/API Handler Interactive GUI
  BKP_ROOT=/home/rellis/spicer/backups/gui
  SERVICE_URL=http://127.0.0.1:5001
EOF
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

backup_iter() {
  local iter="$1"
  local ts
  ts="$(date +%Y%m%d_%H%M%S)"
  local out_dir="$BKP_ROOT/${iter}_${ts}"

  mkdir -p "$out_dir"

  cp -a "$APP_DIR/src/app/App.tsx" "$out_dir/App.tsx.before"

  if [[ -f "$APP_DIR/public/ascii-art/ascii-art-text.png" ]]; then
    cp -a "$APP_DIR/public/ascii-art/ascii-art-text.png" "$out_dir/ascii-art-text.png.before"
  fi

  if [[ -f "$APP_DIR/public/ascii-art/ansi-shadow.txt" ]]; then
    cp -a "$APP_DIR/public/ascii-art/ansi-shadow.txt" "$out_dir/ansi-shadow.txt.before"
  fi

  if [[ -d "$APP_DIR/dist" ]]; then
    tar -C "$APP_DIR" -czf "$out_dir/dist.before.tgz" dist
  fi

  echo "Backup created: $out_dir"
}

build_gui() {
  cd "$APP_DIR"
  npm run build:ops-gui --silent
  echo "Build complete with /ops-gui base path"
}

verify_gui() {
  local html
  local js_path
  local css_path

  echo "Health:"
  curl -sS -i "$SERVICE_URL/health" | sed -n '1,12p'

  echo
  echo "GUI HTML:"
  html="$(curl -sS "$SERVICE_URL/ops-gui")"
  printf "%s\n" "$html" | sed -n '1,40p'

  js_path="$(printf "%s" "$html" | sed -n 's/.*src="\([^"]*assets\/index-[^"]*\.js\)".*/\1/p' | head -n 1)"
  css_path="$(printf "%s" "$html" | sed -n 's/.*href="\([^"]*assets\/index-[^"]*\.css\)".*/\1/p' | head -n 1)"

  echo
  echo "Resolved assets:"
  echo "JS:  ${js_path:-missing}"
  echo "CSS: ${css_path:-missing}"

  if [[ -n "$js_path" ]]; then
    curl -sS -i "$SERVICE_URL$js_path" | sed -n '1,12p'
  fi

  if [[ -n "$css_path" ]]; then
    curl -sS -i "$SERVICE_URL$css_path" | sed -n '1,12p'
  fi
}

rollback_iter() {
  local backup_name="$1"
  local src_dir="$BKP_ROOT/$backup_name"

  [[ -d "$src_dir" ]] || {
    echo "Backup not found: $src_dir" >&2
    exit 1
  }

  cp -a "$src_dir/App.tsx.before" "$APP_DIR/src/app/App.tsx"

  if [[ -f "$src_dir/ascii-art-text.png.before" ]]; then
    mkdir -p "$APP_DIR/public/ascii-art"
    cp -a "$src_dir/ascii-art-text.png.before" "$APP_DIR/public/ascii-art/ascii-art-text.png"
  fi

  if [[ -f "$src_dir/ansi-shadow.txt.before" ]]; then
    mkdir -p "$APP_DIR/public/ascii-art"
    cp -a "$src_dir/ansi-shadow.txt.before" "$APP_DIR/public/ascii-art/ansi-shadow.txt"
  fi

  if [[ -f "$src_dir/dist.before.tgz" ]]; then
    rm -rf "$APP_DIR/dist"
    tar -C "$APP_DIR" -xzf "$src_dir/dist.before.tgz"
    echo "Rolled back dist from tarball"
  else
    echo "No dist backup; rebuilding"
    build_gui
  fi

  echo "Rollback complete from: $src_dir"
}

list_backups() {
  mkdir -p "$BKP_ROOT"
  ls -1dt "$BKP_ROOT"/* 2>/dev/null || true
}

main() {
  need_cmd curl
  need_cmd npm
  need_cmd sed
  need_cmd tar

  local cmd="${1:-}"
  case "$cmd" in
    backup)
      [[ $# -eq 2 ]] || { usage; exit 1; }
      backup_iter "$2"
      ;;
    build)
      build_gui
      ;;
    verify)
      verify_gui
      ;;
    rollback)
      [[ $# -eq 2 ]] || { usage; exit 1; }
      rollback_iter "$2"
      ;;
    list)
      list_backups
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
