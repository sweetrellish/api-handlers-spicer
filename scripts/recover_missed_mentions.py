#!/usr/bin/env python3
"""Recover missed @mention email notifications from MarketSharp Notes.

Re-runs the tagger mention pipeline (extract @username -> resolve recipients ->
send email) over the most recent MarketSharp notes within a lookback window,
skipping any note already processed by the live worker or by a previous
catch-up run.

Dry-run by default. Pass --apply to actually send emails.

Examples:
  # Preview last 24h (no emails sent):
  python3 scripts/recover_missed_mentions.py

  # Send emails for last 24h:
  python3 scripts/recover_missed_mentions.py --apply

  # Preview last 6 hours, fetching up to 100 notes:
  python3 scripts/recover_missed_mentions.py --hours 6 --top 100

  # Send for everything since a specific ISO timestamp:
  python3 scripts/recover_missed_mentions.py --since 2026-06-01T08:00:00Z --apply
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
for _p in (str(ROOT), str(ROOT / "src"), str(ROOT / "tagger"), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

LIVE_STATE_FILE = ROOT / "tagger" / "comment_worker_state.json"
CATCHUP_STATE_FILE = ROOT / "tagger" / "comment_worker_catchup_state.json"


def parse_args():
    p = argparse.ArgumentParser(
        description="Re-run tagger mention pipeline over recent MarketSharp notes (dry-run by default).",
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--hours", type=float, default=24.0,
                   help="Lookback window in hours (default 24).")
    g.add_argument("--since", type=str,
                   help="ISO8601 timestamp lower bound (e.g. 2026-06-01T00:00:00Z); overrides --hours.")
    p.add_argument("--top", type=int, default=50,
                   help="OData $top limit per fetch (default 50; MarketSharp typically caps at 50).")
    p.add_argument("--apply", action="store_true",
                   help="Actually send emails. Without this flag the run only previews what would be sent.")
    p.add_argument("--include-live-processed", action="store_true",
                   help="Do NOT skip notes already in live worker's processed set. Use only if you suspect the live worker recorded notes it never actually emailed.")
    p.add_argument("--state-file", type=str, default=str(CATCHUP_STATE_FILE),
                   help=f"Catch-up state file (default isolates from live worker: {CATCHUP_STATE_FILE}).")
    return p.parse_args()


def parse_since(args):
    if args.since:
        s = args.since.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            raise SystemExit(f"Could not parse --since={args.since!r} as ISO8601")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return datetime.now(timezone.utc) - timedelta(hours=args.hours)


def load_live_processed_ids():
    if not LIVE_STATE_FILE.exists():
        return set()
    try:
        data = json.loads(LIVE_STATE_FILE.read_text(encoding="utf-8"))
        return {str(x) for x in data.get("processed_note_ids", []) if x}
    except Exception as exc:
        print(f"[warn] could not read live state {LIVE_STATE_FILE}: {exc}")
        return set()


def _parse_iso(ts_raw):
    if not ts_raw:
        return None
    try:
        dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def main():
    args = parse_args()
    since_dt = parse_since(args)
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Redirect CommentWorker state to a separate catch-up file so we never race
    # with the live systemd worker.
    os.environ["COMMENT_WORKER_STATE_FILE"] = args.state_file
    os.environ["COMMENT_WORKER_SOURCE"] = "marketsharp_notes"
    os.environ.setdefault("COMMENT_WORKER_BOOTSTRAP_PROCESS_EXISTING", "false")

    from comment_worker import CommentWorker  # noqa: E402

    worker = CommentWorker(source_override="marketsharp_notes")
    worker.validate_listen_config()
    if args.apply:
        worker.validate_email_config()

    live_processed = set() if args.include_live_processed else load_live_processed_ids()
    catchup_processed = {str(x) for x in worker.state.get("processed_note_ids", []) if x}

    print(f"[catchup] lookback since:    {since_iso}")
    print(f"[catchup] apply emails:      {args.apply}")
    print(f"[catchup] catchup state:     {args.state_file}")
    print(f"[catchup] live processed:    {len(live_processed)} note ids (skipped unless --include-live-processed)")
    print(f"[catchup] catchup processed: {len(catchup_processed)} note ids (always skipped)")

    notes = worker.fetch_marketsharp_notes()
    print(f"[catchup] fetched {len(notes)} notes from MarketSharp OData (top={args.top})")

    eligible = []
    skipped_old = 0
    skipped_live = 0
    skipped_catchup = 0
    for note in notes:
        comment = worker.build_comment_from_note(note)
        if not comment:
            continue
        note_id = str(comment.get("id") or "")
        ts_dt = _parse_iso(comment.get("timestamp"))
        if ts_dt is None or ts_dt < since_dt:
            skipped_old += 1
            continue
        if note_id and note_id in live_processed:
            skipped_live += 1
            continue
        if note_id and note_id in catchup_processed:
            skipped_catchup += 1
            continue
        eligible.append(comment)

    eligible.sort(key=lambda c: c.get("timestamp", ""))
    print(
        f"[catchup] window filter: {len(eligible)} eligible, "
        f"skipped {skipped_old} outside window, "
        f"{skipped_live} already in live, "
        f"{skipped_catchup} already in catchup"
    )

    if not eligible:
        print("[catchup] nothing to do.")
        return 0

    processed = 0
    errors = 0
    for comment in eligible:
        note_id = comment.get("id")
        ts_disp = worker._format_timestamp_display(comment.get("timestamp", ""))
        text_one_line = (comment.get("text") or "").strip().replace("\n", " ")
        if len(text_one_line) > 200:
            text_one_line = text_one_line[:197] + "..."

        action = "APPLY" if args.apply else "DRY-RUN"
        print(f"\n[catchup][{action}] note={note_id}  at {ts_disp}")
        print(f"    text: {text_one_line}")

        try:
            worker.process_comment_text(
                comment.get("text", ""),
                job_info=comment.get("job_info"),
                send_email=args.apply,
                source="catchup",
            )
            if args.apply and note_id:
                # Persist into the catch-up state so re-runs stay idempotent.
                worker.mark_comment_processed(comment)
            processed += 1
        except Exception as exc:  # noqa: BLE001
            errors += 1
            print(f"    [error] pipeline raised: {exc}")

    print()
    if args.apply:
        print(f"[catchup] APPLIED: {processed} comments processed, {errors} errors.")
        print(f"[catchup] catchup state written to: {args.state_file}")
    else:
        print(f"[catchup] DRY-RUN: {processed} comments previewed, {errors} errors.")
        print("[catchup] Re-run with --apply to actually send emails.")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
