# Mention Safety Runbook

Last updated: 2026-05-25

## Purpose

Prevent unnecessary email notifications by enforcing explicit mention-only routing and recipient caps.

## Safety Model

1. Only explicit `@username` tokens are eligible recipients.
2. Plain text words (for example `ALL`, `team`, or names without `@`) do not trigger emails.
3. A max recipient cap blocks broad fan-out on a single comment.

## Required Runtime Settings

Set these in runtime env (for example `tagger/.env`):

```dotenv
COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS=true
COMMENT_WORKER_MAX_RECIPIENTS_PER_COMMENT=15
MARKETSHARP_NOTE_REQUIRE_EXPLICIT_MENTIONS=true
MARKETSHARP_NOTE_MENTION_STYLE=plain
```

Optional explicit-mention ambiguity control:

```dotenv
COMMENT_WORKER_AMBIGUOUS_ALIAS_MODE=all
```

## Validate Current Service Configuration

```bash
sudo systemctl status marketsharp_comment_worker.service --no-pager --full
journalctl -u marketsharp_comment_worker.service -n 60 --no-pager
```

Expected startup lines include:

1. `Mention safety: require_explicit_mentions=yes, max_recipients_per_comment=15`
2. `Mention alias index: <n> aliases, 0 ambiguous` (or expected explicit ambiguity)

## Server-Side Non-Sending Test

Run this directly on the server to confirm safe behavior without sending emails:

```bash
cd /home/rellis/spicer/tagger
/home/rellis/spicer/tagger/.venv/bin/python - <<'PY'
import importlib.util
spec = importlib.util.spec_from_file_location("cw", "comment_worker.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
w = mod.CommentWorker(source_override="marketsharp_notes", bootstrap_process_existing_override=True)

plain = "[Test] Ross Abbott Stephen Harrison ALL team status"
explicit = "[Test] @rabbott @sharrison @kbreasure status"

print("plain:", w.extract_mentions(plain))
print("explicit:", w.extract_mentions(explicit))

w.process_comment_text(plain, send_email=False, source="runbook")
w.process_comment_text(explicit, send_email=False, source="runbook")
PY
```

Expected:

1. Plain message resolves to `[]` and logs `No @mentions found`.
2. Explicit message resolves only tagged users and dry-run logs exactly those recipients.

## Deployment and Restart

If code changes are made locally:

```bash
cd /Users/ryanellis/Dev Repo/spicer
bash ./deploy/linux/deploy_marketsharp_comment_worker.sh
```

On server (interactive sudo):

```bash
sudo systemctl restart marketsharp_comment_worker.service
sudo systemctl status marketsharp_comment_worker.service --no-pager --full
```

## Incident Triage Checklist

If unexpected recipients report emails:

1. Capture exact comment text and timestamp.
2. Run server-side non-sending test against that exact text.
3. Check startup logs for mention safety settings.
4. Confirm mapping file contains only intended explicit aliases.
5. Review recent journal lines for recipient expansion patterns.

## Notes on Full-Name Matching

If you want names like `Ross Abbott` to resolve:

1. Add explicit alias keys in mapping input workflow.
2. Keep explicit mention requirement enabled.
3. Require `@` in note text for notification routing.
