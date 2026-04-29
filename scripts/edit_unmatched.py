"""
Edit the payload_json and/or customer_name of an unmatched queue item and set it back to pending.
Now interactive: shows unmatched queue items, prompts for ID and new name.
"""
import sys
import json
import sqlite3

DB_PATH = "../pending_comments.db"  # Change if your DB is elsewhere

def list_unmatched(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, customer_name, created_at, comment_text FROM pending_comments WHERE status='unmatched' ORDER BY created_at ASC")
    rows = cur.fetchall()
    if not rows:
        print("No unmatched queue items found.")
        return []
    print("\nUnmatched Queue Items:")
    print("ID | Customer Name | Created At | Comment Text (truncated)")
    print("-" * 80)
    for row in rows:
        print(f"{row[0]:<4}| {row[1]:<25}| {row[2]:<12}| {str(row[3])[:40]}")
    print()
    return [r[0] for r in rows]

def main():
    conn = sqlite3.connect(DB_PATH)
    unmatched_ids = list_unmatched(conn)
    if not unmatched_ids:
        return

    try:
        queue_id = int(input("Enter the ID of the queue item to edit: ").strip())
    except Exception:
        print("Invalid input. Exiting.")
        return
    if queue_id not in unmatched_ids:
        print(f"ID {queue_id} is not in the list of unmatched items.")
        return

    cur = conn.cursor()
    cur.execute("SELECT payload_json, customer_name FROM pending_comments WHERE id=? AND status='unmatched'", (queue_id,))
    row = cur.fetchone()
    if not row:
        print(f"No unmatched queue item found with id={queue_id}")
        return
    payload_json, old_customer_name = row
    try:
        payload = json.loads(payload_json)
    except Exception as e:
        print(f"Failed to parse payload_json: {e}")
        return

    print(f"Current customer_name: {old_customer_name}")
    new_customer_name = input("Enter the new customer_name: ").strip()
    if not new_customer_name:
        print("No new name entered. Exiting.")
        return

    # Update the customer_name in the payload (edit other fields as needed)
    payload['customer_name'] = new_customer_name
    new_payload_json = json.dumps(payload)

    # Update the DB row: payload_json, customer_name, status, last_error
    cur.execute(
        "UPDATE pending_comments SET payload_json=?, customer_name=?, status='pending', last_error=NULL WHERE id=?",
        (new_payload_json, new_customer_name, queue_id)
    )
    conn.commit()
    print(f"Updated queue item {queue_id}: set customer_name to '{new_customer_name}' and status to 'pending'.")
    conn.close()

if __name__ == "__main__":
    main()

