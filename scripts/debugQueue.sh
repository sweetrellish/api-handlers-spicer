#!/bin/bash
# Debug script: List all pending queue items and their last error/status

QUEUE_DB="/home/rellis/spicer/pending_comments.db"

if [ ! -f "$QUEUE_DB" ]; then
  echo "Pending queue database not found at $QUEUE_DB"
  exit 1
fi

echo "== Pending Queue Items =="
sqlite3 "$QUEUE_DB" "SELECT id, event_id, customer_name, comment_text, status, last_error, created_at, updated_at FROM pending_comments WHERE status='pending' ORDER BY created_at ASC;"

echo -e "\nTotal pending: $(sqlite3 "$QUEUE_DB" "SELECT COUNT(*) FROM pending_comments WHERE status='pending';")"

