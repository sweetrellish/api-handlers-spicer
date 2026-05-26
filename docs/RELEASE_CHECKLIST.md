# Release Checklist

Last updated: 2026-05-24

## Pre-Release

1. Confirm mapping file is refreshed:
   - tagger/marketsharp_user-email.json
2. Confirm relay settings exist in runtime env:
   - EMAIL_API_URL
   - EMAIL_API_QUERY_TOKEN or EMAIL_API_KEY
3. Confirm mention worker starts without config validation errors.
4. Confirm docs are up to date:
   - docs/SPICER_SUBMODULE_DEPLOYMENT.md
   - docs/CONFLUENCE_HANDOFF_REPORT.md
   - docs/MENTION_SAFETY_RUNBOOK.md
5. Confirm ops-check script is present and executable:
   - scripts/marketsharp_comment_worker_ops_check.sh
6. Confirm queue poster mention formatting mode is set as intended:
   - MARKETSHARP_NOTE_MENTION_STYLE=plain
   - MARKETSHARP_NOTE_REQUIRE_EXPLICIT_MENTIONS=true
7. Confirm worker safety options are set as intended:
   - COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS=true
   - COMMENT_WORKER_MAX_RECIPIENTS_PER_COMMENT=15

## Staging Validation

1. Dry run single-user test:
   - python3 tagger/comment_worker.py --tag rellis --message "@rellis staging smoke"
2. Dry run plain-text non-tag test (should notify nobody):
   - python3 tagger/comment_worker.py --message "Stephen Harrison Ross Abbott ALL hands status" 
3. Send test email to one known account:
   - python3 tagger/comment_worker.py --send-test-emails --tag rellis --message "@rellis staging send"
4. Verify relay output includes ok true.
5. Verify recipient inbox receives message.
6. Validate explicit-only behavior:
   - python3 - <<'PY'
import importlib.util
spec = importlib.util.spec_from_file_location('cw', 'tagger/comment_worker.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
w = m.CommentWorker(source_override='marketsharp_notes', bootstrap_process_existing_override=True)
print(w.extract_mentions('Stephen Harrison ALL status'))
print(w.extract_mentions('@sharrison @rabbott'))
PY

## Production Cutover

1. Pin submodule to release tag in parent admin repository.
2. Deploy staging-approved commit hash to production.
3. Run production smoke test:
   - python3 tagger/comment_worker.py --send-test-emails --tag rellis --message "@rellis production smoke"
4. Start listen mode service process.

## Post-Deploy

1. Validate worker health logs for first 15 minutes.
2. Confirm no relay non-JSON errors.
3. Confirm no repeated config warnings.
4. Run ops-check script:
   - bash ./scripts/marketsharp_comment_worker_ops_check.sh marketsharp_comment_worker.service
5. Capture deployment metadata in operations log:
   - Timestamp
   - Operator
   - Commit/tag
   - Smoke test results

## Rollback

1. Revert submodule pointer to previous known-good tag.
2. Redeploy runtime config and service.
3. Re-run single-user smoke test.
4. Record rollback cause and fix plan.
