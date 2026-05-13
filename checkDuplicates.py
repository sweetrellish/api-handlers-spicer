#!/usr/bin/env python3
"""
Check for duplicate posted comments in the MarketSharp audit log or queue DB. Usage: python check_duplicates.py [--db path/to/pending_comments --audit path/to/logs]
"""

import argparse
import sqlite3
import json
from collections import Counter, defaultdict

parser = argparse.ArgumentParser(description="Check for duplicate posted comments.")
parser.add_argument('--db', type=str, default='data/pending_comments.db', help='Path to pending_comments.db')
parser.add_argument('--audit', type=str, default='unmatched_comments.jsonl', help='Path to audit log (jsonl)')
args = parser.parse_args()

def check_queue_duplicates(db_path):
    print(f"\nChecking queue DB for duplicate event_id and comment_text: {db_path}")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Check for duplicate event_id
    c.execute("SELECT event_id, COUNT(*) FROM pending_comments GROUP BY event_id HAVING COUNT(*) > 1")
    rows = c.fetchall()
    if rows:
        print("Duplicate event_id(s):")
        for eid, count in rows:
            print(f"  {eid}: {count} times")
    else:
        print("No duplicate event_id found.")
    # Check for duplicate comment_text
    c.execute("SELECT comment_text, COUNT(*) FROM pending_comments GROUP BY comment_text HAVING COUNT(*) > 1")
    rows = c.fetchall()
    if rows:
        print("Duplicate comment_text(s):")
        for text, count in rows:
            print(f"  {text[:60]}...: {count} times")
    else:
        print("No duplicate comment_text found.")
    conn.close()

def check_audit_duplicates(audit_path):
    print(f"\nChecking audit log for duplicate comments: {audit_path}")
    seen = defaultdict(list)
    event_id_count = Counter()
    comment_text_count = Counter()
    try:
        with open(audit_path, 'r') as f:
            for i, line in enumerate(f, 1):
                try:
                    obj = json.loads(line)
                    eid = obj.get('event_id')
                    text = obj.get('comment_text')
                    key = (eid, text)
                    seen[key].append(i)
                    event_id_count[eid] += 1
                    comment_text_count[text] += 1
                except Exception:
                    continue
        dups = {k: v for k, v in seen.items() if len(v) > 1}
        if dups:
            print("Duplicate entries in audit log:")
            for (eid, text), lines in dups.items():
                print(f"  event_id={eid}, comment_text={str(text)[:60]}...: {len(lines)} times (lines {lines})")
        else:
            print("No duplicate entries found in audit log.")
        print("\nSummary of event_id posting counts in audit log:")
        for eid, count in event_id_count.items():
            flag = " <== DUPLICATE" if count > 1 else ""
            print(f"  event_id={eid}: {count} times{flag}")
        print("\nSummary of comment_text posting counts in audit log:")
        for text, count in comment_text_count.items():
            flag = " <== DUPLICATE" if count > 1 else ""
            print(f"  comment_text={str(text)[:60]}...: {count} times{flag}")
    except FileNotFoundError:
        print("Audit log file not found.")

def cross_reference_queue_and_audit(db_path, audit_path):
    print("\nCross-referencing queue and audit log for event_id and comment_text:")
    # Load all event_ids and comment_texts from queue
    queue_event_ids = set()
    queue_comment_texts = set()
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT event_id, comment_text FROM pending_comments")
        for eid, text in c.fetchall():
            queue_event_ids.add(eid)
            queue_comment_texts.add(text)
        conn.close()
    except Exception as e:
        print(f"  Could not read queue DB: {e}")
        return
    # Load all event_ids and comment_texts from audit log
    audit_event_ids = set()
    audit_comment_texts = set()
    try:
        with open(audit_path, 'r') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    audit_event_ids.add(obj.get('event_id'))
                    audit_comment_texts.add(obj.get('comment_text'))
                except Exception:
                    continue
    except Exception as e:
        print(f"  Could not read audit log: {e}")
        return
    only_in_queue = queue_event_ids - audit_event_ids
    only_in_audit = audit_event_ids - queue_event_ids
    print(f"  Event IDs only in queue: {sorted(only_in_queue)}")
    print(f"  Event IDs only in audit log: {sorted(only_in_audit)}")
    # Optionally, do the same for comment_texts
    only_text_in_queue = queue_comment_texts - audit_comment_texts
    only_text_in_audit = audit_comment_texts - queue_comment_texts
    print(f"  Comment texts only in queue: {sorted(list(only_text_in_queue))[:3]} ...")
    print(f"  Comment texts only in audit log: {sorted(list(only_text_in_audit))[:3]} ...")

if __name__ == "__main__":
    check_queue_duplicates(args.db)
    check_audit_duplicates(args.audit)
    cross_reference_queue_and_audit(args.db, args.audit)

