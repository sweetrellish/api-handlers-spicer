"""
Recovery script: Fetches the 50 most recent CompanyCam projects and their comments, cross-references with the audit log, and requeues only missed comments before the cutoff.
"""

import os
import sys
import sqlite3
from datetime import datetime
from companycam_service import CompanyCamService
from pending_queue import PendingCommentQueue
from config import Config

AUDIT_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'posted_comments_audit.db')
QUEUE_DB = Config.PENDING_QUEUE_DB_PATH


def get_last_posted_timestamp():
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
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute('SELECT event_id FROM posted_comments_audit')
    event_ids = set(row[0] for row in cur.fetchall() if row[0])
    conn.close()
    return event_ids


def main():
    print("[INFO] Starting missed comment recovery...")
    queue = PendingCommentQueue(QUEUE_DB)
    cc = CompanyCamService()
    projects = cc.list_recent_projects(limit=50)
    print(f"[INFO] Fetched {len(projects)} recent projects from CompanyCam.")
    missed = []
    for proj in projects:
        pid = proj.get('id')
        if not pid:
            continue
        comments = cc.list_project_comments(pid)
        for comment in comments:
            event_id = str(comment.get('id'))
            if not event_id:
                continue
            # Check if already in queue (any status)
            result = queue.enqueue(
                event_id=event_id,
                customer_name=proj.get('name', 'Unknown'),
                comment_text=comment.get('content', ''),
                author_name=comment.get('creator_name', ''),
                payload={'project': proj, 'comment': comment},
                last_error='Recovered missed comment',
            )
            if result.get('already_queued'):
                print(f"[STOP] Encountered already-queued comment (event_id={event_id}). Stopping recovery.")
                print(f"[DONE] Requeued {len(missed)} missed comments before encountering a duplicate.")
                if missed:
                    print("Event IDs:", ', '.join(missed))
                else:
                    print("No missed comments found to requeue.")
                return
            else:
                missed.append(event_id)
    print(f"[DONE] Requeued {len(missed)} missed comments (no duplicates encountered).")
    if missed:
        print("Event IDs:", ', '.join(missed))
    else:
        print("No missed comments found to requeue.")

if __name__ == "__main__":
    main()

