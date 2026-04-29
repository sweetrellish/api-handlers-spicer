"""Test script for PendingCommentQueue and true_fail logic (local/dev use)."""

import sys
import os

# Adds the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from pending_queue import PendingCommentQueue
import time
import random
import sqlite3

# Use a local test DB
TEST_DB = '/tmp/test_pending_queue.db'

# Remove old test DB if present
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

queue = PendingCommentQueue(TEST_DB)

# Enqueue some test items
for i in range(5):
    queue.enqueue(
        event_id=f"evt_{i}",
        customer_name=f"Customer {i}",
        comment_text=f"Test comment {i}",
        author_name=f"Author {i}",
        payload={"foo": i},
        last_error=None
    )

print("Initial counts:", queue.get_counts())


# Mark some as unmatched
pending = queue.get_pending_batch(limit=5)
for item in pending[:2]:
    queue.mark_unmatched(item['id'], "No match found")

# Fetch unmatched items directly from DB
def get_unmatched_items(q, limit=10):
    with q._connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM pending_comments WHERE status = 'unmatched' ORDER BY updated_at DESC LIMIT ?
            """,
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]

unmatched = get_unmatched_items(queue)
if unmatched:
    queue.mark_true_fail(unmatched[0]['id'], "Manual review: permanent fail")

print("Counts after marking unmatched and true_fail:", queue.get_counts())

# List true_fail items with human-readable timestamps
import datetime
true_fails = queue.get_true_fail_items()
print(f"\nTrue fail items: {len(true_fails)}")
for item in true_fails:
    updated = datetime.datetime.fromtimestamp(item['updated_at']).strftime('%Y-%m-%d %H:%M:%S')
    created = datetime.datetime.fromtimestamp(item['created_at']).strftime('%Y-%m-%d %H:%M:%S')
    print(f"ID: {item['id']}, Event: {item['event_id']}, Last error: {item['last_error']}")
    print(f"  Customer: {item['customer_name']}, Author: {item['author_name']}")
    print(f"  Created: {created}, Updated: {updated}")
    print(f"  Comment: {item['comment_text']}")
    print(f"  Payload: {item['payload_json']}")
    print("---")


# Requeue true_fail item (commented out to allow manual review)
# if true_fails:
#     queue.mark_failed(true_fails[0]['id'], "Manual requeue for test")
#     print(f"Requeued true_fail item ID {true_fails[0]['id']}")

print("Final counts:", queue.get_counts())

