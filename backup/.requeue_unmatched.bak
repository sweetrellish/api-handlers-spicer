"""Manually move unmatched queue rows back to pending for immediate retry."""

from pending_queue import PendingCommentQueue
from config import Config


def main():
    queue = PendingCommentQueue(Config.PENDING_QUEUE_DB_PATH)
    moved = queue.requeue_all_unmatched()
    counts = queue.get_counts()
    print(f"requeued_unmatched={moved}")
    print(f"queue_counts={counts}")


if __name__ == '__main__':
    main()
