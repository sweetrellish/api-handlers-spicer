# Spicer Agent Rules

## Target Selection

- Treat the server and the dev machine as different execution targets.
- If the task touches a live service on `10.8.0.1`, assume production rules apply.
- If the task is sandbox work, keep it local and explicitly disabled behind dry-run flags.

## Canonical Server Contract

- The server repository lives at `/home/rellis/spicer`.
- Do not rearrange server paths to match the dev machine.
- Use the repo's environment resolution first; use ad hoc paths only as temporary compatibility fallbacks.

## Git Workflow Contract

- Run git operations from the server repository at `/home/rellis/spicer`, not from the dev machine copy.
- Treat the server clone as the canonical git working tree for operational commits/tags.
- Before committing, confirm the current host and working directory to avoid cross-machine drift.
- If local/dev changes are needed, sync deliberately and do not assume git state is shared across machines.

## Database Contract

- The tagger has durable state in SQLite, not in memory.
- Canonical queue DB: `data/pending_comments.db`.
- Canonical idempotency DB: `data/cc_webhook_dedupe.db`.
- Canonical audit DB: `posted_comments_audit.db`.
- If a DB is missing, restore from snapshot before introducing new structure changes.

## Recovery Rules

- Prefer restore from `backups/` snapshots first.
- If the queue DB is gone, recover it before trying to replay tags.
- If the audit DB is gone, use backups or reconstruction from the queue/logs; do not assume it can be recreated from the API alone.

## Deployment Rules

- For server changes, back up the live file, deploy once, restart the service, then verify status/logs.
- For sandbox changes, do not touch server files unless the user approves a production step.
- Keep sandbox features explicitly disabled unless the user asks to turn them on.

## Agent Discipline

- Before editing, identify whether the change belongs to server deployment, dev sandboxing, or data recovery.
- Do not rewrite server layout to match local layout.
- Prefer configuration and path resolution over hard-coded machine-specific paths.
- When in doubt, stop at the boundary and ask for approval before a live-server mutation.
