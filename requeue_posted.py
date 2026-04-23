"""Requeue all posted comments for re-push by setting status='pending'."""

import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'spicer', 'pending_comments.db')

def main():
    if not os.path.exists(DB_FILE):
        print(f"[FATAL] pending_comments.db not found at {DB_FILE}.")
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pending_comments WHERE status = 'posted'")
    count = cur.fetchone()[0]
    print(f"Found {count} posted comments to requeue.")
    if count == 0:
        print("Nothing to requeue.")
        return
    confirm = input("Proceed with requeuing all posted comments? (y/N): ").strip().lower()
    if confirm != 'y':
        print("Aborted. No changes made.")
        return
    cur.execute("""
        UPDATE pending_comments
        SET status = 'pending', last_error = 'Manual requeue for author correction', updated_at = strftime('%s','now')
        WHERE status = 'posted'
    """)
    conn.commit()
    print(f"Requeued {cur.rowcount} comments. They will be re-pushed by the queue worker.")
    conn.close()

if __name__ == "__main__":
    main()

