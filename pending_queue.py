"""Durable queue for comments that cannot yet be written to MarketSharp."""

import json
import sqlite3
import threading
import time



class PendingCommentQueue:
    """SQLite-backed queue of comments waiting for MarketSharp write support."""

    def get_all_items(self):
        """Return all queue items regardless of status, ordered by created_at."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, event_id, customer_name, comment_text, author_name, payload_json,
                       status, last_error, created_at, updated_at
                FROM pending_comments
                ORDER BY created_at ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def __init__(self, db_path):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._ensure_table()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _ensure_table(self):
        # Ensure the pending_comments table exists and includes retry_count for robust retry tracking.
        # The retry_count column is added for automatic escalation after repeated unmatched cycles.
        # Migration is attempted on startup for safety if the column is missing.
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE,
                    customer_name TEXT NOT NULL,
                    comment_text TEXT NOT NULL,
                    author_name TEXT,
                    payload_json TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # Try to add retry_count if missing (for migration safety)
            try:
                conn.execute("ALTER TABLE pending_comments ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;")
            except Exception:
                pass
            conn.commit()
    def mark_true_fail(self, queue_id, error_message):
        """Mark an item as a true failure after all retries/manual review."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_comments
                    SET status = 'true_fail', last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (error_message[:1000], now, queue_id),
                )
                conn.commit()

    def get_true_fail_items(self, limit=20):
        """Return true_fail rows for review or manual fix."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, event_id, customer_name, comment_text, author_name, payload_json,
                       status, last_error, created_at, updated_at
                FROM pending_comments
                WHERE status = 'true_fail'
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def enqueue(self, event_id, customer_name, comment_text, author_name=None, payload=None, last_error=None):
        """Insert a pending comment if unseen; return its queue metadata."""
        now = int(time.time())
        payload_json = json.dumps(payload or {})

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT id, status, created_at, updated_at FROM pending_comments WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if row:
                    return {
                        'queue_id': row[0],
                        'status': row[1],
                        'created_at': row[2],
                        'updated_at': row[3],
                        'already_queued': True,
                    }

                cur = conn.execute(
                    """
                    INSERT INTO pending_comments (
                        event_id,
                        customer_name,
                        comment_text,
                        author_name,
                        payload_json,
                        status,
                        last_error,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        event_id,
                        customer_name,
                        comment_text,
                        author_name,
                        payload_json,
                        last_error,
                        now,
                        now,
                    ),
                )
                conn.commit()
                return {
                    'queue_id': cur.lastrowid,
                    'status': 'pending',
                    'created_at': now,
                    'updated_at': now,
                    'already_queued': False,
                }

    def get_pending_batch(self, limit=10):
        """Return oldest pending rows for processing."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, event_id, customer_name, comment_text, author_name, payload_json,
                       status, last_error, created_at, updated_at
                FROM pending_comments
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_processing(self, queue_id):
        """Mark an item as actively being processed by a worker."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_comments
                    SET status = 'processing', updated_at = ?
                    WHERE id = ?
                    """,
                    (now, queue_id),
                )
                conn.commit()

    def mark_posted(self, queue_id):
        """Mark an item as posted successfully and reset retry_count."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_comments
                    SET status = 'posted', last_error = NULL, updated_at = ?, retry_count = 0
                    WHERE id = ?
                    """,
                    (now, queue_id),
                )
                conn.commit()

    def mark_failed(self, queue_id, error_message):
        """Record a posting failure and return item to pending for retry."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_comments
                    SET status = 'pending', last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (error_message[:1000], now, queue_id),
                )
                conn.commit()

    def mark_unmatched(self, queue_id, error_message):
        """Mark an item as unmatched and increment retry_count."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE pending_comments
                    SET status = 'unmatched', last_error = ?, updated_at = ?, retry_count = retry_count + 1
                    WHERE id = ?
                    """,
                    (error_message[:1000], now, queue_id),
                )
                conn.commit()

    def requeue_stale_processing(self, max_age_seconds):
        """Return stale processing rows to pending after worker interruption."""
        now = int(time.time())
        cutoff = now - int(max_age_seconds)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE pending_comments
                    SET
                        status = 'pending',
                        last_error = CASE
                            WHEN last_error IS NULL OR last_error = ''
                                THEN 'Recovered stale processing item after worker restart'
                            ELSE last_error
                        END,
                        updated_at = ?
                    WHERE status = 'processing' AND updated_at < ?
                    """,
                    (now, cutoff),
                )
                conn.commit()
                return cur.rowcount or 0

    def requeue_stale_unmatched(self, max_age_seconds):
        """Requeue unmatched rows on a schedule for periodic re-checking."""
        now = int(time.time())
        cutoff = now - int(max_age_seconds)
        with self._lock:
            with self._connect() as conn:
                # Increment retry_count for all requeued unmatched
                cur = conn.execute(
                    """
                    UPDATE pending_comments
                    SET
                        status = 'pending',
                        last_error = CASE
                            WHEN last_error IS NULL OR last_error = ''
                                THEN 'Scheduled retry for previously unmatched customer'
                            ELSE last_error
                        END,
                        updated_at = ?,
                        retry_count = retry_count + 1
                    WHERE status = 'unmatched' AND updated_at < ?
                    """,
                    (now, cutoff),
                )
                conn.commit()
                return cur.rowcount or 0

    def requeue_all_unmatched(self):
        """Immediately requeue all unmatched rows for manual retry."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE pending_comments
                    SET
                        status = 'pending',
                        last_error = CASE
                            WHEN last_error IS NULL OR last_error = ''
                                THEN 'Manual retry requested for unmatched customer'
                            ELSE last_error
                        END,
                        updated_at = ?
                    WHERE status = 'unmatched'
                    """,
                    (now,),
                )
                conn.commit()
                return cur.rowcount or 0

    def get_counts(self):
        """Return queue counts by status for simple health checks."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) as count
                FROM pending_comments
                GROUP BY status
                """
            ).fetchall()
            counts = {'pending': 0, 'processing': 0, 'posted': 0, 'unmatched': 0, 'true_fail': 0}
            for status, count in rows:
                counts[status] = count
            return counts

