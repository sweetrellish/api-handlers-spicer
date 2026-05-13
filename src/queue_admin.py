#!/usr/bin/env python3
"""queue_admin.py — CLI tool for inspecting and managing the pending_comments queue.

Usage:
    python queue_admin.py status
    python queue_admin.py list-fails [--limit N]
    python queue_admin.py requeue-fails [ID ID ...] [--all]
    python queue_admin.py requeue-unmatched
    python queue_admin.py show ID [ID ...]
    python queue_admin.py test-touch   # verify touch_processing heartbeat works

Run on server:
    cd /home/rellis/spicer/src
    python queue_admin.py status
"""

import argparse
import datetime
import json
import os
import sqlite3
import sys
import time

# Allow running from the project root or from src/
sys.path.insert(0, os.path.dirname(__file__))
from pending_queue import PendingCommentQueue

DB_PATH = os.getenv('QUEUE_DB_PATH', os.path.join(os.path.dirname(__file__), '..', 'pending_comments.db'))
DB_PATH = os.path.abspath(DB_PATH)


def fmt_ts(ts):
    if not ts:
        return 'n/a'
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def cmd_status(queue, _args):
    counts = queue.get_counts()
    total = sum(counts.values())
    print(f"Queue DB: {DB_PATH}")
    print(f"{'Status':<15} {'Count':>8}")
    print('-' * 25)
    for status, count in sorted(counts.items()):
        print(f"  {status:<13} {count:>8}")
    print('-' * 25)
    print(f"  {'TOTAL':<13} {total:>8}")


def cmd_list_fails(queue, args):
    limit = getattr(args, 'limit', 50)
    items = queue.get_true_fail_items(limit=limit)
    if not items:
        print("No true_fail items.")
        return
    print(f"{'ID':>6}  {'Customer':<35}  {'Updated':<19}  Last Error")
    print('-' * 110)
    for item in items:
        err = (item.get('last_error') or '')[:70]
        print(f"  {item['id']:>4}  {item['customer_name']:<35}  {fmt_ts(item['updated_at'])}  {err}")
    print(f"\n({len(items)} items shown)")


def cmd_show(queue, args):
    with queue._connect() as conn:
        conn.row_factory = sqlite3.Row
        placeholders = ','.join('?' * len(args.ids))
        rows = conn.execute(
            f"SELECT * FROM pending_comments WHERE id IN ({placeholders})",
            [int(i) for i in args.ids],
        ).fetchall()
    if not rows:
        print("No matching items found.")
        return
    for row in rows:
        d = dict(row)
        try:
            payload = json.loads(d.get('payload_json') or '{}')
        except Exception:
            payload = d.get('payload_json')
        print(f"\n--- ID {d['id']} ---")
        print(f"  Status:        {d['status']}")
        print(f"  Customer:      {d['customer_name']}")
        print(f"  Author:        {d['author_name']}")
        print(f"  Event ID:      {d['event_id']}")
        print(f"  Retry count:   {d['retry_count']}")
        print(f"  Created:       {fmt_ts(d['created_at'])}")
        print(f"  Updated:       {fmt_ts(d['updated_at'])}")
        print(f"  Last error:    {d['last_error']}")
        print(f"  Comment:       {d['comment_text'][:200]}")
        # Show address fields from payload if present
        if isinstance(payload, dict):
            project = payload.get('project') or payload
            addr = project.get('address') or {}
            if isinstance(addr, dict) and any(addr.values()):
                print(f"  Address fields: {addr}")


def cmd_requeue_fails(queue, args):
    if getattr(args, 'all', False) or not getattr(args, 'ids', None):
        n = queue.requeue_true_fail()
        print(f"Requeued all true_fail items → {n} row(s) set to pending.")
    else:
        ids = [int(i) for i in args.ids]
        n = queue.requeue_true_fail(queue_ids=ids)
        print(f"Requeued {n} row(s): {ids}")


def cmd_list_unmatched(queue, args):
    limit = getattr(args, 'limit', 50)
    with queue._connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, customer_name, retry_count, updated_at, last_error "
            "FROM pending_comments WHERE status='unmatched' ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    if not rows:
        print("No unmatched items.")
        return
    print(f"{'ID':>6}  {'Retry':>5}  {'Customer':<35}  {'Updated':<19}  Last Error")
    print('-' * 120)
    for row in rows:
        err = (row['last_error'] or '')[:60]
        print(f"  {row['id']:>4}  {row['retry_count']:>5}  {row['customer_name']:<35}  {fmt_ts(row['updated_at'])}  {err}")
    print(f"\n({len(rows)} items shown)")


def cmd_requeue_unmatched(queue, _args):
    n = queue.requeue_all_unmatched()
    print(f"Requeued all unmatched items → {n} row(s) set to pending.")


def cmd_test_touch(queue, _args):
    """Create a temporary processing row, verify touch_processing refreshes updated_at."""
    import tempfile, os
    tmp = tempfile.mktemp(suffix='.db')
    q = PendingCommentQueue(tmp)
    try:
        q.enqueue('test-evt-1', 'Test Customer', 'Test comment', 'Test Author')
        rows = q.claim_pending_batch(limit=1)
        assert rows, "claim_pending_batch returned nothing"
        row = rows[0]
        qid = row['id']
        ts_before = row['updated_at']

        time.sleep(2)
        q.touch_processing(qid)

        with q._connect() as conn:
            ts_after = conn.execute(
                "SELECT updated_at FROM pending_comments WHERE id=?", (qid,)
            ).fetchone()[0]

        assert ts_after > ts_before, f"updated_at not refreshed: before={ts_before} after={ts_after}"

        # Verify stale recovery does NOT requeue a freshly-touched item
        recovered = q.requeue_stale_processing(max_age_seconds=1)  # 1s threshold
        time.sleep(0)  # touch was just now, so cutoff at 1s ago won't catch it
        with q._connect() as conn:
            status = conn.execute(
                "SELECT status FROM pending_comments WHERE id=?", (qid,)
            ).fetchone()[0]
        assert status == 'processing', f"Item was incorrectly requeued! status={status}"

        # Now let it go stale and verify recovery DOES work
        time.sleep(3)
        recovered2 = q.requeue_stale_processing(max_age_seconds=2)
        with q._connect() as conn:
            status2 = conn.execute(
                "SELECT status FROM pending_comments WHERE id=?", (qid,)
            ).fetchone()[0]
        assert status2 == 'pending', f"Stale item should have been requeued but status={status2}"
        assert recovered2 >= 1, f"requeue_stale_processing should have returned >=1 but got {recovered2}"

        print("PASS: touch_processing heartbeat test")
        print(f"  updated_at before touch: {ts_before}")
        print(f"  updated_at after touch:  {ts_after}")
        print(f"  Item NOT requeued while fresh (status={status})")
        print(f"  Item requeued after going stale (status={status2}, recovered={recovered2})")
    finally:
        os.unlink(tmp)


def main():
    parser = argparse.ArgumentParser(
        description='Queue admin tool for pending_comments.db',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    sub.add_parser('status', help='Show queue counts by status')

    p_fails = sub.add_parser('list-fails', help='List true_fail items')
    p_fails.add_argument('--limit', type=int, default=50, help='Max rows to show (default 50)')

    p_um = sub.add_parser('list-unmatched', help='List unmatched items')
    p_um.add_argument('--limit', type=int, default=50, help='Max rows to show (default 50)')

    p_show = sub.add_parser('show', help='Show full details for one or more queue IDs')
    p_show.add_argument('ids', nargs='+', help='Queue item IDs')

    p_rf = sub.add_parser('requeue-fails', help='Reset true_fail items back to pending')
    p_rf.add_argument('ids', nargs='*', help='Specific IDs to requeue (omit for all)')
    p_rf.add_argument('--all', action='store_true', help='Requeue all true_fail items')

    sub.add_parser('requeue-unmatched', help='Reset all unmatched items back to pending')
    sub.add_parser('test-touch', help='Run heartbeat unit test (uses a temp DB)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    queue = PendingCommentQueue(DB_PATH)

    commands = {
        'status': cmd_status,
        'list-fails': cmd_list_fails,
        'list-unmatched': cmd_list_unmatched,
        'show': cmd_show,
        'requeue-fails': cmd_requeue_fails,
        'requeue-unmatched': cmd_requeue_unmatched,
        'test-touch': cmd_test_touch,
    }
    commands[args.command](queue, args)


if __name__ == '__main__':
    main()
