#!/usr/bin/env bash
# Detect drift between server expectations and reality:
#   1. Files referenced by spicer_ops_menu.py SCRIPTS_DIR that are missing
#   2. ExecStart paths in spicer-related systemd units that don't exist
#   3. Shadow .py files in scripts/ that collide with src/ modules (PYTHONPATH risk)
#   4. Files in ship-files.default.txt that are missing from disk
set -u
ROOT="${SPICER_ROOT:-/home/rellis/spicer}"
cd "$ROOT" || { echo "ERROR: cannot cd to $ROOT"; exit 2; }
FAIL=0

echo "== 1. Ops menu script references =="
while IFS= read -r line; do
  fname="$(echo "$line" | grep -oE '"[^"]+\.(py|sh)"' | tr -d '"')"
  [[ -z "$fname" ]] && continue
  if [[ ! -f "scripts/$fname" ]]; then
    echo "  MISSING: scripts/$fname"
    FAIL=1
  fi
done < <(grep 'SCRIPTS_DIR /' spicer_ops_menu.py | grep -v '^[[:space:]]*#')

echo "== 2. systemd ExecStart paths =="
for unit in /etc/systemd/system/spicer-*.service /etc/systemd/system/marketsharp_*.service; do
  [[ -f "$unit" ]] || continue
  while IFS= read -r path; do
    if [[ ! -e "$path" ]]; then
      echo "  MISSING: $path  (referenced by $(basename "$unit"))"
      FAIL=1
    fi
  done < <(grep -E '^ExecStart=' "$unit" | sed -E 's/^ExecStart=//; s/^[[:space:]]*[^ ]+[[:space:]]+//' | awk '{print $1}' | grep -E '^/')
done

echo "== 3. Shadow modules (scripts/ vs src/) =="
for f in scripts/*.py; do
  [[ -f "$f" ]] || continue
  base="$(basename "$f")"
  if [[ -f "src/$base" ]] && [[ "$base" != "__init__.py" ]]; then
    echo "  SHADOW: scripts/$base collides with src/$base"
    FAIL=1
  fi
done

echo "== 4. Ship list coverage =="
SHIP="scripts/ship-files.default.txt"
if [[ -f "$SHIP" ]]; then
  while IFS= read -r rel; do
    [[ -z "$rel" || "$rel" =~ ^# ]] && continue
    if [[ ! -e "$rel" ]]; then
      echo "  MISSING: $rel  (listed in $SHIP)"
      FAIL=1
    fi
  done < "$SHIP"
else
  echo "  WARN: $SHIP not found"
fi

if [[ $FAIL -eq 0 ]]; then
  echo
  echo "OK: no drift detected."
else
  echo
  echo "DRIFT detected. See findings above."
fi
exit $FAIL
