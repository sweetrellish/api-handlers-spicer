# Spicer Agent Rules

## Target Selection

- Treat the server and the dev machine as different execution targets.
- If the task touches a live service on `10.8.0.1`, assume production rules apply.
- If the task is sandbox work, keep it local and explicitly disabled behind dry-run flags.

## Canonical Server Contract

- The server repository lives at `/home/rellis/spicer`.
- Do not rearrange server paths to match the dev machine.
- Use ad hoc paths only when the repo's environment resolution explicitly fails; flag the fallback in a comment and do not commit it without user approval.

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
- If the queue DB and all backups are absent, halt and ask the user for a recovery strategy before making any structural changes.
- If the audit DB is gone, use backups or reconstruction from the queue/logs; do not assume it can be recreated from the API alone.

## Deployment Rules

- For server changes, back up the live file, deploy once, restart the service, then verify status/logs.
- For sandbox changes, do not touch server files unless the user approves a production step.
- Keep sandbox features explicitly disabled unless the user asks to turn them on.

## Agent Discipline

- Before editing, use this order: 1. Determine target (production IP vs. local). 2. Classify change type (deployment / sandbox / recovery). 3. Apply the matching section's rules. If the task spans multiple types, treat the highest-risk type's rules as authoritative.
- Do not rewrite server layout to match local layout.
- Prefer configuration and path resolution over hard-coded machine-specific paths.
- When in doubt, stop at the boundary and ask for approval before a live-server mutation.
