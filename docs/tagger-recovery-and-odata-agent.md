# Tagger Recovery And OData Agent

## Purpose

This runbook covers daily operator use of the Tagger Recovery Workbench and the
OData Agent added to the Spicer ops menu.

Primary goals:

- Recover missed MarketSharp note tag notifications safely.
- Inspect and edit recovery candidates before sending.
- Review posted comment and recovery activity by timeframe.
- Explore MarketSharp OData backend data with safe read-first controls.

## Menu Paths

- Tagger Recovery Workbench:
  - Main Menu -> MarketSharp Tagging API -> Tagger Recovery Workbench
- OData Agent:
  - Main Menu -> OData Agent

## Daily Tagger Recovery Procedure

1. Open the ops menu.
2. Go to Tagger Recovery Workbench.
3. Run `Discover candidates`.
4. Use a lookback window that covers the incident or workday.
5. Run `List recovery queue`.
6. Focus on rows with `pending` status.
7. Use `Inspect queue item` for any candidate you intend to process.
8. If the note text needs correction, use `Edit queue item text`.
9. Use `Requeue queue item` after editing if needed.
10. Run `Apply recovery` in dry-run mode first.
11. Confirm the resolved recipients and note text are correct.
12. Re-run `Apply recovery` in real apply mode only when confirmed.

## Recovery Status Meanings

- `pending`: candidate is ready for review or apply.
- `processing`: apply flow is active.
- `sent`: this note revision has already been applied.
- `skipped`: no valid mentions were found for the working text.
- `true_fail`: candidate could not be sent successfully.

## Edited Note Recovery Procedure

Use this when a MarketSharp note was edited after the live worker first saw it.

1. Run `Discover candidates` for a window that includes the original note time.
2. Find the candidate by note id or customer.
3. Inspect the row and verify the working text contains explicit `@username` mentions.
4. If needed, edit the working text to match the intended recovery content.
5. Dry-run apply.
6. If the dry-run recipients are correct, run real apply.
7. If you accidentally rerun the same note revision, the workbench should skip it as already sent.

## Timeframe Visibility Procedure

Use this when you need to answer what was posted or recovered during a given window.

1. Run `Timeline view`.
2. Provide `since` and `until` values in ISO8601 UTC format.
3. Review:
   - `posted_audit` entries from the posted comments audit database.
   - `recovery_audit` entries from the recovery queue lifecycle.
4. Use `Timeline export` when you need a handoff artifact.

## OData Agent Daily Procedure

1. Open OData Agent.
2. Run `Connection check` first.
3. Use `Endpoint catalog` to confirm the right entity set.
4. Use `Browse entity` for common work:
   - `Notes`
   - `Contacts()`
   - `Activities`
5. Use `Custom query` only when the browse flow is insufficient.
6. Use `Save preset` for repeated operational queries.
7. Use `Export last result` when you need to hand results to another operator.

## OData Safety Rules

- Treat OData Agent as read-first.
- Keep result sizes small with `top` limits.
- Start with `top=25` unless you have a reason to widen scope.
- Avoid broad custom queries during active incidents unless necessary.
- Export results instead of copy-pasting large JSON blobs into chat or tickets.

## Escalation Checks

Escalate before apply if any of the following is true:

- The candidate text does not clearly show the intended `@username` mentions.
- Resolved recipients do not match expected staff.
- The same note appears to have multiple conflicting revisions.
- The row enters `true_fail` and the error is not obviously transient.
- OData queries return unexpected 400/401/403 responses.

Escalate before deployment if any of the following is true:

- The server copy differs materially from the local tested behavior.
- The target server files have unexpected manual changes.
- A backup cannot be created cleanly before secure copy.

## Direct CLI Examples

Local examples:

```bash
./.venv/bin/python3 scripts/tagger_recovery_workbench.py discover --hours 4
./.venv/bin/python3 scripts/tagger_recovery_workbench.py list --status pending --limit 20
./.venv/bin/python3 scripts/tagger_recovery_workbench.py inspect --id 41
./.venv/bin/python3 scripts/tagger_recovery_workbench.py apply --id 41
./.venv/bin/python3 scripts/tagger_recovery_workbench.py apply --id 41 --apply --yes
./.venv/bin/python3 scripts/tagger_recovery_workbench.py timeline --since 2026-08-04T13:55:00Z --until 2026-08-04T14:45:00Z
./.venv/bin/python3 scripts/odata_agent.py check
./.venv/bin/python3 scripts/odata_agent.py browse --entity Notes --top 10 --orderby 'dateTime desc'
```

## Deployment Checklist

Before secure copy to server:

1. Back up the current server copy of any file that will be replaced.
2. Confirm destination paths under `/home/rellis/spicer`.
3. Copy files once.
4. Re-run the local menu path on server.
5. Restart only the affected service if deployment changes runtime behavior.
6. Verify logs after restart.