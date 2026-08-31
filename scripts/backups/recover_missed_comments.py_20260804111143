"""
Recovery script: Fetches the 50 most recent CompanyCam projects and their comments, cross-references with the audit log, and requeues only missed comments before the cutoff.
"""

import os
import sys
import sqlite3
import re
import argparse
from datetime import UTC, datetime

# Bootstrap module resolution when run directly or via the admin menu from ROOT.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRIPT_DIR)
for _p in (_ROOT, os.path.join(_ROOT, "src"), _SCRIPT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from companycam_service import CompanyCamService  # noqa: E402
from pending_queue import PendingCommentQueue  # noqa: E402
from config import Config  # noqa: E402
from posted_comments_audit import ensure_audit_table  # noqa: E402

AUDIT_DB = Config.AUDIT_DB_PATH
QUEUE_DB = Config.PENDING_QUEUE_DB_PATH


def get_last_posted_timestamp():
    ensure_audit_table()
    if not os.path.exists(AUDIT_DB):
        print(f"[FATAL] Audit log not found at {AUDIT_DB}.")
        sys.exit(1)
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute('SELECT MAX(posted_at) FROM posted_comments_audit')
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row and row[0] else None


def get_all_audit_event_ids():
    ensure_audit_table()
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute('SELECT event_id FROM posted_comments_audit')
    event_ids = set(row[0] for row in cur.fetchall() if row[0])
    conn.close()
    return event_ids


def get_all_queue_event_ids(queue):
    """Return all event ids currently present in pending_comments, any status."""
    event_ids = set()
    for item in queue.get_all_items():
        event_id = item.get('event_id')
        if event_id:
            event_ids.add(str(event_id))
    return event_ids


def extract_comment_author(comment):
    """Extract best-available CompanyCam author display name from known shapes."""
    if not isinstance(comment, dict):
        return ''

    creator = comment.get('creator')
    if not isinstance(creator, dict):
        creator = {}
    user = comment.get('user')
    if not isinstance(user, dict):
        user = {}
    author = comment.get('author')
    if not isinstance(author, dict):
        author = {}

    candidates = [
        comment.get('creator_name'),
        creator.get('name'),
        comment.get('user_name'),
        user.get('name'),
        comment.get('author_name'),
        author.get('name'),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ''


def normalize_project_name(name):
    """Remove trailing project-type suffixes like "(EXTERIOR)" from CC project names."""
    text = (name or '').strip()
    if not text:
        return ''

    # Strip one or more trailing parenthetical suffixes: "Name (EXTERIOR)".
    # Keep the base customer name used for MarketSharp matching.
    normalized = re.sub(r'\s*\([^)]*\)\s*$', '', text).strip()
    return normalized or text


def parse_args():
    parser = argparse.ArgumentParser(description='Recover missed CompanyCam comments into pending queue.')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview selected comments and exit without writing to pending_comments.',
    )
    parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Run with provided flags instead of prompting for input.',
    )
    parser.add_argument(
        '--backfill-audit',
        choices=['y', 'n'],
        default='n',
        help='When non-interactive, whether to backfill missing posted queue items into audit DB.',
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=3,
        help='When non-interactive, look back this many days for missed comments.',
    )
    parser.add_argument(
        '--selection',
        default='a',
        help='When non-interactive, selection value: a, n, or comma/range list like 1,3-5.',
    )
    return parser.parse_args()


def preview_comments(comments):
    print(f"[DRY-RUN] Would queue {len(comments)} missed comments:")
    for idx, comment in enumerate(comments, 1):
        author = comment.get('author') or '<no author>'
        content = (comment.get('content') or '').strip().replace('\n', ' ')
        if len(content) > 180:
            content = content[:177] + '...'
        print(
            f"  [{idx}] event_id={comment.get('event_id')} | project={comment.get('project')} "
            f"| author={author} | created_at={comment.get('created_at_str')}\n"
            f"      {content}"
        )


def main():
    args = parse_args()
    ensure_audit_table()
    # --- Historical Audit Checker ---
    def audit_queue_for_historical_posts():
        print("[HISTORICAL CHECK] Scanning queue for posted items not in audit DB...")
        queue = PendingCommentQueue(QUEUE_DB)
        audit_event_ids = get_all_audit_event_ids()
        items = queue.get_all_items()
        to_audit = []
        for item in items:
            # Only consider items marked as posted
            if item.get('status') == 'posted' and item.get('event_id') and item['event_id'] not in audit_event_ids:
                to_audit.append(item)
        if not to_audit:
            print("[HISTORICAL CHECK] No missing posted items found in queue.")
            return
        print(f"[HISTORICAL CHECK] Found {len(to_audit)} posted items missing from audit DB.")
        from scripts.posted_comments_audit import log_posted_comment
        for item in to_audit:
            log_posted_comment(
                event_id=item.get('event_id'),
                customer_id=None,
                customer_name=item.get('customer_name', ''),
                author_name=item.get('author_name', ''),
                comment_text=item.get('comment_text', ''),
                extra_json=item.get('payload_json', '')
            )
        print(f"[HISTORICAL CHECK] Added {len(to_audit)} missing posted items to audit DB.")

    # Prompt user to run historical checker (or use flags in non-interactive mode)
    if args.non_interactive:
        resp = args.backfill_audit
    else:
        resp = input("Do you want to scan the queue and backfill the audit log with any posted items not in the audit DB? (y/N): ").strip().lower()
    if resp == 'y':
        audit_queue_for_historical_posts()
    # ...existing code...
    from datetime import timedelta
    print("[INFO] Starting missed comment recovery...")
    queue = PendingCommentQueue(QUEUE_DB)
    cc = CompanyCamService()
    queue_event_ids = get_all_queue_event_ids(queue)
    # Prompt for days to go back (or use flag in non-interactive mode)
    if args.non_interactive:
        days_back = max(1, int(args.days_back))
    else:
        while True:
            try:
                days_back = int(input("How many days back should we look for missed comments? (e.g. 3): "))
                if days_back < 1:
                    print("Please enter a positive integer.")
                    continue
                break
            except ValueError:
                print("Invalid input. Please enter a number.")

    cutoff_ts = int((datetime.now(UTC) - timedelta(days=days_back)).timestamp())
    print(f"[INFO] Will look for comments created after {datetime.fromtimestamp(cutoff_ts, UTC)} UTC.")

    projects = cc.list_recent_projects(limit=50)
    print(f"[INFO] Fetched {len(projects)} recent projects from CompanyCam.")
    audit_event_ids = get_all_audit_event_ids()
    seen_event_ids = set(audit_event_ids) | set(queue_event_ids)
    missed_comments = []
    for proj in projects:
        pid = proj.get('id')
        if not pid:
            continue
        comments = cc.list_project_comments(pid)
        for comment in comments:
            event_id = str(comment.get('id'))
            if not event_id or event_id in seen_event_ids:
                continue
            created_at = comment.get('created_at')
            # CompanyCam API may return ISO8601 or timestamp; handle both
            if isinstance(created_at, str):
                try:
                    created_ts = int(datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp())
                except Exception:
                    created_ts = 0
            else:
                created_ts = int(created_at) if created_at else 0
            if created_ts < cutoff_ts:
                continue
            missed_comments.append({
                'event_id': event_id,
                'project': normalize_project_name(proj.get('name', 'Unknown')),
                'author': extract_comment_author(comment),
                'content': comment.get('content') or comment.get('text') or comment.get('body') or '',
                'created_at': created_ts,
                'created_at_str': datetime.fromtimestamp(created_ts, UTC).strftime('%Y-%m-%d %H:%M:%S'),
                'raw': {'project': proj, 'comment': comment}
            })

    if not missed_comments:
        print("[DONE] No missed comments found in the selected window.")
        return

    # Display menu for user selection
    print("\nMissed comments found:")
    for idx, c in enumerate(missed_comments, 1):
        print(f"[{idx}] Project: {c['project']} | Author: {c['author']} | Date: {c['created_at_str']}\n    {c['content']}")

    print("\nSelect comments to queue:")
    print("  a) All\n  n) None\n  Or enter comma-separated numbers (e.g. 1,3,5-7):")
    if args.non_interactive:
        selection = str(args.selection or 'a').strip().lower()
    else:
        selection = input("Your choice: ").strip().lower()

    to_queue = []
    if selection == 'a':
        to_queue = missed_comments
    elif selection == 'n' or not selection:
        print("[INFO] No comments selected. Exiting.")
        return
    else:
        chosen = set()
        for part in selection.split(','):
            part = part.strip()
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    chosen.update(range(start, end+1))
                except Exception:
                    pass
            else:
                try:
                    chosen.add(int(part))
                except Exception:
                    pass
        to_queue = [missed_comments[i-1] for i in sorted(chosen) if 1 <= i <= len(missed_comments)]
    if not to_queue:
        print("[INFO] No valid comments selected. Exiting.")
        return

    dry_run = args.dry_run
    if not dry_run:
        dry_run_resp = input("Dry run only (preview without queue writes)? (y/N): ").strip().lower()
        dry_run = dry_run_resp == 'y'

    if dry_run:
        preview_comments(to_queue)
        print("[DONE] Dry run complete. No queue records were written.")
        return

    print(f"[INFO] Queuing {len(to_queue)} missed comments...")
    queued = []
    for c in to_queue:
        result = queue.enqueue(
            event_id=c['event_id'],
            customer_name=c['project'],
            comment_text=c['content'],
            author_name=c['author'],
            payload=c['raw'],
            last_error='Recovered missed comment (menu)'
        )
        if not result.get('already_queued'):
            queued.append(c['event_id'])
    print(f"[DONE] Queued {len(queued)} missed comments.")
    if queued:
        print("Event IDs:", ', '.join(queued))
    else:
        print("No new comments were queued.")

if __name__ == "__main__":
    main()
