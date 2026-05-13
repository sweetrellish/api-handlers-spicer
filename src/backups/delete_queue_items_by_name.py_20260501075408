import sqlite3
import sys
import os
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pending_comments.db'))
#DB_PATH=sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
#DB_PATH = 'pending_comments.db'  # Change this if your DB file is named differently or in another location


def list_queue_items():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, customer_name, comment_text, status, created_at FROM pending_comments ORDER BY created_at ASC")
    rows = cur.fetchall()
    conn.close()
    print("\nCurrent Queue Items:")
    print("ID | Customer Name | Status | Created At | Comment Text (truncated)")
    print("-" * 80)
    for row in rows:
        print(f"{row[0]:<4} | {row[1]:<20} | {row[3]:<10} | {row[4]} | {row[2][:40]}")
    print(f"\nTotal: {len(rows)} item(s)\n")
    return rows


def delete_queue_item_by_id(queue_id):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_comments WHERE id = ?", (queue_id,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Deleted {deleted} queue item(s) with id = {queue_id}")


def main():
    while True:
        rows = list_queue_items()
        if not rows:
            print("No items to delete.")
            break
        try:
            queue_id = input("Enter the ID of the queue item to delete (or 0 to exit): ")
            if queue_id.strip() == '0':
                print("Exiting.")
                break
            queue_id = int(queue_id)
        except ValueError:
            print("Invalid input. Please enter a valid ID or 0 to exit.")
            continue
        delete_queue_item_by_id(queue_id)
        print()


if __name__ == "__main__":
    main()

