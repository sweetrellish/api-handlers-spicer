# This script is for testing the PendingCommentQueue by enqueuing multiple test comments.
from pending_queue import PendingCommentQueue

queue = PendingCommentQueue("../pending_comments.db")  # Adjust path if needed

for i in range(10):
    result = queue.enqueue(
        event_id=f"test-event-ryan-{i}",
        customer_name="Ryan Ellis (Test Account)",  # Only target the test account
        comment_text=f"This is test comment #{{i}} for Ryan Ellis test account.",
        author_name=f"Test User {i}",
        payload={"test": True, "index": i},
        last_error=None
    )
    print(f"Enqueue result for item {i}:", result)

