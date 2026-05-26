# Spicer Submodule Deployment Plan

Last updated: 2026-05-24

## Purpose

This plan defines how to package the tagger/email-notification capability as a Spicer-admin-maintained deployable submodule and roll it out safely across environments.

## Scope

- Mention parsing worker in tagger/comment_worker.py
- MarketSharp username-to-email mapping in tagger/marketsharp_user-email.json
- Relay-based email delivery path via Google Apps Script web app
- Runtime config in tagger/.env
- Direct MarketSharp note polling path in tagger/comment_worker.py (`--source marketsharp_notes`)
- Ops health-check script in scripts/marketsharp_comment_worker_ops_check.sh
- Concurrent UI poster mention styling in src/queue_ui_poster.py

## Target Outcome

- Admin package can be versioned and updated independently.
- Operations team can deploy, validate, and rollback with a repeatable checklist.
- Documentation is aligned between repository README and Confluence handoff.
- Mention safety operations are documented in docs/MENTION_SAFETY_RUNBOOK.md.

## Stage 1: Branch and Packaging Structure

1. Create a release branch for the maintenance package.
2. Decide submodule path in the parent admin repository, for example vendor/spicer-comment-worker.
3. Ensure these package artifacts exist and are current:
   - tagger/comment_worker.py
   - tagger/requirements.txt
   - tagger/marketsharp_user-email.json
   - tagger/.env template values (no secrets committed)
4. Tag a release candidate once smoke tests pass.

## Stage 2: Configuration and Secret Model

1. In runtime .env, set:
   - EMAIL_API_URL to the Apps Script web app exec URL
   - EMAIL_API_QUERY_TOKEN to the shared secret token
   - API_URL only for listen mode polling
2. Keep secrets outside git in deployment secret storage.
3. Confirm the Apps Script deployment is:
   - Execute as: Me
   - Access: Anyone (or equivalent service-access setting)
4. Validate token parity between Apps Script script property and deployed runtime env.

## Stage 3: Verification Gates

### A. Test mode gate

1. Run a dry run:
   - python3 comment_worker.py --tag rellis --message "@rellis relay test"
2. Run relay send test:
   - python3 comment_worker.py --send-test-emails --tag rellis --message "@rellis relay test"
3. Expected results:
   - Relay JSON response includes ok true
   - Email delivered to mapped recipient

### B. Listen mode gate

1. Ensure API_URL is configured and reachable.
2. Start worker in listen mode.
3. Post a known comment containing @username and confirm notification path.

### C. Service and observability gate

1. Ensure systemd unit is installed and active:
   - marketsharp_comment_worker.service
2. Run packaged ops check:
   - bash ./scripts/marketsharp_comment_worker_ops_check.sh marketsharp_comment_worker.service
3. Confirm check output includes:
   - active service state
   - recent journal entries
   - no error-pattern hits in last 24h (or documented known exceptions)

### D. Concurrent poster formatting gate

1. Set poster env option (if desired):
   - MARKETSHARP_NOTE_MENTION_STYLE=plain
   - MARKETSHARP_NOTE_REQUIRE_EXPLICIT_MENTIONS=true
2. Set worker safety options:
   - COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS=true
   - COMMENT_WORKER_MAX_RECIPIENTS_PER_COMMENT=15
3. Validate one queue-posted note in MarketSharp UI.
4. Confirm mentions remain parseable by the worker (`@username` present in note text).

## Stage 4: Submodule Integration in Parent Admin Repo

1. Add this repository as submodule in parent admin package repository.
2. Pin to a release tag, not a floating branch, for predictable maintenance releases.
3. Add parent-level deployment job steps:
   - submodule sync/update
   - install dependencies from tagger/requirements.txt
   - inject environment secrets
   - run test-mode verification command
   - start service process

## Stage 5: Release and Rollback

### Release

1. Publish release tag.
2. Update parent repo submodule pointer to new tag/commit.
3. Deploy to staging.
4. Run smoke tests.
5. Promote to production.

### Rollback

1. Revert parent repo submodule pointer to previous known-good tag.
2. Redeploy service.
3. Re-run smoke test command.

## Operational Runbook

### Health checks

- Mapping file load count printed at startup
- Email config status printed at startup
- Relay response logs include status/content-type/final-url on failures

### Common failures

1. Missing token or URL in env
   - Symptom: validation error before send
2. HTML error page from Apps Script
   - Symptom: non-JSON relay response
   - Fix: deployment access/redeploy URL and token
3. Unknown username mention
   - Symptom: mapped-email skip log
   - Fix: refresh mapping file from MarketSharp extractor

## Audit and Traceability

Capture for every deployment:

- Date/time
- Release tag or commit
- Operator
- Env target
- Smoke test output
- Rollback needed yes/no

## Maintenance Cadence

- Weekly mapping refresh or on user roster change
- Monthly token rotation for relay
- Quarterly runbook verification
- Weekly ops-check run using scripts/marketsharp_comment_worker_ops_check.sh
