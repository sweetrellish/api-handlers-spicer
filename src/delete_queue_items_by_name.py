"""
This script allows you to view and delete items from the pending_comments queue in the SQLite database. It lists all current queue items with their ID, customer name, status, creation time, and a truncated version of the comment text. You can enter the ID of the item you wish to delete or enter 0 to exit the program.
"""
import sqlite3
import sys
import os

# Adjust the path to your database file if necessary
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pending_comments.db'))

# Ensure the database file exists
if not os.path.exists(DB_PATH):
    print(f"Database file not found at {DB_PATH}. Please check the path and try again.")
    sys.exit(1)

# Function to list all queue items
def list_queue_items():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Fetch all queue items ordered by creation time
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

# Function to delete a queue item by its ID or selected range of IDs
def delete_queue_item_by_id(queue_id, delete_range=False):
    first_id, last_id = None, None
    if delete_range:
        try:
            first_id, last_id = map(int, queue_id.split('-'))
            if first_id > last_id:
                print("Invalid range. First ID should be less than or equal to Last ID.")
                return
        except ValueError:
            print("Invalid input for range. Please enter in the format 'first_id-last_id'.")
            return
    # Connect to the database and delete the specified queue item(s)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if delete_range:
        cur.execute("DELETE FROM pending_comments WHERE id BETWEEN ? AND ?", (first_id, last_id))
    else:
        # Delete the queue item with the specified ID
        cur.execute("DELETE FROM pending_comments WHERE id = ?", (queue_id,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    # Print the number of deleted items
    print(f"Deleted {deleted} queue item(s) with id = {queue_id}" if not delete_range else f"Deleted {deleted} queue item(s) with ids between {first_id} and {last_id}")

def main():
    while True:
        rows = list_queue_items()
        if not rows:
            print("No items to delete.")
            break
        try:
            queue_id = input("Enter the ID of the queue item to delete, or a range (e.g., 1-5 or 0 to exit): ")
            if queue_id.strip() == '0':
                print("Exiting.")
                break
            if '-' in queue_id:
                delete_queue_item_by_id(queue_id, delete_range=True)
                continue
            queue_id = int(queue_id)
        except ValueError:
            print("Invalid input. Please enter a valid ID or 0 to exit.")
            continue
        delete_queue_item_by_id(queue_id)
        print()


if __name__ == "__main__":
    main()
