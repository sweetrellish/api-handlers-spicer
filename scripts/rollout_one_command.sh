#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/rollout_one_command.sh [options]

One-command safe rollout:
1) Stash current drift
2) Restore selected files from that stash
3) Stage changed files
4) Commit + push
5) Verify key services are active

Options:
  --files <path>        Path to ship list (default: scripts/ship-files.default.txt)
  --message <text>      Commit message (default: ops: targeted rollout)
  --stash-message <t>   Stash message (default: wip-park-YYYYmmdd-HHMMSS)
  --no-push             Do not push after commit
  --no-service-check    Skip systemctl is-active checks
  --drop-stash          Drop the created stash on success
  -h, --help            Show this help message
USAGE
}

FILES_PATH='scripts/ship-files.default.txt'
COMMIT_MSG='ops: targeted rollout'
STASH_MSG="wip-park-$(date +%Y%m%d-%H%M%S)"
DO_PUSH=1
DO_SERVICE_CHECK=1
DROP_STASH=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --files)
      FILES_PATH="$2"
      shift 2
      ;;
    --message)
      COMMIT_MSG="$2"
      shift 2
      ;;
    --stash-message)
      STASH_MSG="$2"
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
    --drop-stash)
      DROP_STASH=1
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

if [[ -z "$(git status --porcelain --untracked-files=all)" ]]; then
  echo "Working tree is clean; nothing to stash or roll out."
  echo "If you want to use an existing stash, run scripts/rollout_from_stash.sh directly."
  exit 0
fi

echo "Stashing current drift: $STASH_MSG"
git stash push -u -m "$STASH_MSG" >/dev/null
NEW_STASH='stash@{0}'

echo "Running rollout_from_stash using $NEW_STASH"
ROLLOUT_ARGS=(
  --stash "$NEW_STASH"
  --files "$FILES_PATH"
  --message "$COMMIT_MSG"
)
[[ "$DO_PUSH" -eq 0 ]] && ROLLOUT_ARGS+=(--no-push)
[[ "$DO_SERVICE_CHECK" -eq 0 ]] && ROLLOUT_ARGS+=(--no-service-check)

"$REPO_ROOT/scripts/rollout_from_stash.sh" "${ROLLOUT_ARGS[@]}"

if [[ "$DROP_STASH" -eq 1 ]]; then
  git stash drop "$NEW_STASH" >/dev/null || true
  echo "Dropped created stash: $NEW_STASH"
else
  echo "Created stash kept: $NEW_STASH"
fi

echo "One-command rollout completed."
