"""
Interactive queue review and management tool for MarketSharp/CompanyCam integration.

Features:
- Visual menu for each queue item
- [P]ost to audit log (if not already)
- [R]equeue for retry (after editing in CC/MS)
- [D]elete (remove from queue)
- [S]kip (move to next)
- Show full details for each item
- Robust, user-friendly CLI
"""

import sys
import json
from pending_queue import PendingCommentQueue
from scripts.posted_comments_audit import log_posted_comment
from config import Config

def print_item_details(item):
    print("\n--- Queue Item ---")
    for k in ["id", "event_id", "customer_id", "customer_name", "author_name", "status", "created_at", "comment_text"]:
        print(f"{k}: {item.get(k)}")
    payload = item.get("payload_json")
    if payload:
        try:
            payload_obj = json.loads(payload)
            print("payload_json (truncated):", json.dumps(payload_obj, indent=2)[:500])
        except Exception:
            print("payload_json:", payload[:200])
    print("------------------\n")

def main():
    queue = PendingCommentQueue(Config.PENDING_QUEUE_DB_PATH)
    items = queue.get_all_items()
    print(f"Loaded {len(items)} queue items.\n")
    for item in items:
        print_item_details(item)
        while True:
            print("Choose action: [P]ost to audit log, [R]equeue, [D]elete, [S]kip, [Q]uit")
            choice = input("> ").strip().lower()
            if choice == "p":
                log_posted_comment(
                    event_id=item.get("event_id"),
                    customer_id=item.get("customer_id"),
                    customer_name=item.get("customer_name"),
                    author_name=item.get("author_name"),
                    comment_text=item.get("comment_text"),
                    extra_json=item.get("payload_json"),
                )
                print("[OK] Posted to audit log.")
                break
            elif choice == "r":
                queue.requeue(item["id"])
                print("[OK] Requeued for retry.")
                break
            elif choice == "d":
                queue.delete(item["id"])
                print("[OK] Deleted from queue.")
                break
            elif choice == "s":
                print("[SKIP] Moving to next item.")
                break
            elif choice == "q":
                print("Exiting.")
                sys.exit(0)
            else:
                print("Invalid choice. Please enter P, R, D, S, or Q.")

if __name__ == "__main__":
    main()

