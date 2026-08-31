#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/rollout_from_stash.sh [options]

Restores selected files from a stash, stages changed files, commits, pushes,
and optionally verifies key services.

Options:
  --stash <ref>         Stash ref to restore from (default: stash@{0})
  --files <path>        Path to file list (default: scripts/ship-files.default.txt)
  --message <text>      Commit message (default: ops: targeted rollout)
  --no-push             Do not push after commit
  --no-service-check    Skip systemctl is-active checks
  -h, --help            Show this help message
USAGE
}

STASH_REF='stash@{0}'
FILES_PATH='scripts/ship-files.default.txt'
COMMIT_MSG='ops: targeted rollout'
DO_PUSH=1
DO_SERVICE_CHECK=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stash)
      STASH_REF="$2"
      shift 2
      ;;
    --files)
      FILES_PATH="$2"
      shift 2
      ;;
    --message)
      COMMIT_MSG="$2"
      shift 2
      ;;
    --no-push)
      DO_PUSH=0
      shift
      ;;
    --no-service-check)
      DO_SERVICE_CHECK=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f "$FILES_PATH" ]]; then
  echo "Files list not found: $FILES_PATH" >&2
  exit 1
fi

# Validate stash ref before attempting restores.
if ! git rev-parse --verify --quiet "$STASH_REF" >/dev/null; then
  echo "Stash ref not found: $STASH_REF" >&2
  echo "Tip: run 'git stash list' to view available refs." >&2
  exit 1
fi

echo "Using stash: $STASH_REF"
echo "Using file list: $FILES_PATH"

declare -a RESTORED_FILES=()
while IFS= read -r f; do
  [[ -z "$f" ]] && continue
  [[ "$f" =~ ^# ]] && continue

  if git checkout "$STASH_REF" -- "$f" 2>/dev/null; then
    echo "restored $f"
    RESTORED_FILES+=("$f")
  else
    echo "skip $f"
  fi
done < "$FILES_PATH"

if [[ ${#RESTORED_FILES[@]} -eq 0 ]]; then
  echo "No files restored from $STASH_REF; nothing to do."
  exit 0
fi

for f in "${RESTORED_FILES[@]}"; do
  if ! git diff --quiet -- "$f"; then
    git add -- "$f"
  fi
done

if git diff --cached --quiet; then
  echo "No staged changes after restore; nothing to commit."
  exit 0
fi

echo "Staged changes:"
git diff --cached --name-status

git commit -m "$COMMIT_MSG"

if [[ "$DO_PUSH" -eq 1 ]]; then
  git push origin main
fi

if [[ "$DO_SERVICE_CHECK" -eq 1 ]]; then
  echo "Service status check:"
  systemctl is-active spicer-flask-api.service marketsharp_queue_worker.service marketsharp_comment_worker.service
fi

echo "Done."
