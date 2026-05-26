# Production Git Deployment Handoff

Last updated: 2026-05-24

## Scope

This handoff covers the MarketSharp mention-notification production package, including:

1. tagger/comment_worker.py
2. tagger/run_comment_worker.sh
3. deploy/linux/marketsharp_comment_worker.service
4. deploy/linux/deploy_marketsharp_comment_worker.sh
5. scripts/marketsharp_comment_worker_ops_check.sh
6. marketsharp_comment_worker_ops_check.sh
7. docs/MENTION_SAFETY_RUNBOOK.md
7. spicer_ops_menu.py
8. src/queue_ui_poster.py

## Pre-Commit Verification

Run from repository root:

```bash
python3 spicer_ops_menu.py --status
bash ./marketsharp_comment_worker_ops_check.sh marketsharp_comment_worker.service || true
bash ./deploy/linux/deploy_marketsharp_comment_worker.sh
```

Notes:

1. The ops-check script expects Linux systemd and will report unavailable tools on macOS.
2. The deploy script performs remote env checks and a one-shot worker bootstrap.

## Production-Critical Environment Variables

Comment worker:

1. EMAIL_API_URL
2. EMAIL_API_QUERY_TOKEN or EMAIL_API_KEY
3. MARKETSHARP_COMPANY_ID
4. MARKETSHARP_USER_KEY
5. MARKETSHARP_SECRET_KEY
6. MARKETSHARP_ODATA_URL

Queue UI poster formatting:

1. MARKETSHARP_NOTE_MENTION_STYLE=plain
2. MARKETSHARP_NOTE_REQUIRE_EXPLICIT_MENTIONS=true

Mention safety settings:

1. COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS=true
2. COMMENT_WORKER_MAX_RECIPIENTS_PER_COMMENT=15

## Git Release Steps

```bash
git status
git add -A
git commit -m "prod: marketsharp mention worker ops hardening and poster mention-style integration"
git push origin <release-branch>
```

If using tags:

```bash
git tag -a v2026.05.24-mention-worker-prod -m "MarketSharp mention worker production package"
git push origin v2026.05.24-mention-worker-prod
```

## Server Rollout Steps

```bash
cd /home/rellis/spicer
bash ./deploy/linux/deploy_marketsharp_comment_worker.sh
sudo cp /home/rellis/spicer/deploy/linux/marketsharp_comment_worker.service /etc/systemd/system/marketsharp_comment_worker.service
sudo systemctl daemon-reload
sudo systemctl restart marketsharp_comment_worker.service
journalctl -u marketsharp_comment_worker.service -n 80 --no-pager
```

## Acceptance Criteria

1. Service is active and stable under systemd.
2. Startup logs show mapping load and marketsharp_notes source.
3. Startup logs show mention safety guard rails enabled.
	- require_explicit_mentions=yes
	- max_recipients_per_comment=15
4. Ops-check script reports healthy status and no critical error patterns.
5. New MarketSharp note mention sends relay notification successfully.
6. Concurrent queue UI poster still posts notes and preserves mention parsing behavior.

## Rollback

```bash
git checkout <last-known-good-tag>
bash ./deploy/linux/deploy_marketsharp_comment_worker.sh
sudo systemctl restart marketsharp_comment_worker.service
```

Document:

1. rollback trigger
2. rollback operator
3. incident timestamp
4. follow-up corrective action
