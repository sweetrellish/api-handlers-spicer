import hashlib
import hmac
import base64
import sqlite3
import threading
import time

from config.config import Config


class IdempotencyStore:
    """Simple SQLite store to prevent duplicate webhook processing."""

    def __init__(self, db_path):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._ensure_table()

    def _connect(self):
        # Keep connections short-lived so this works reliably under process restarts.
        return sqlite3.connect(self.db_path)

    def _ensure_table(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    received_at INTEGER NOT NULL
                )
                """
            )
            conn.commit()

    def seen_or_store(self, event_id):
        """Return True if event already exists, otherwise store and return False."""
        now = int(time.time())
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT event_id FROM processed_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if row:
                    return True

                conn.execute(
                    "INSERT INTO processed_events (event_id, received_at) VALUES (?, ?)",
                    (event_id, now),
                )
                conn.commit()
                return False

    def prune_older_than(self, max_age_seconds):
        """Remove stale dedupe rows older than max_age_seconds."""
        cutoff = int(time.time()) - max_age_seconds
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM processed_events WHERE received_at < ?",
                    (cutoff,),
                )
                conn.commit()


def extract_event_id(event_data, raw_body):
    """Build a stable event id from known fields, falling back to payload hash."""
    event_type = str(event_data.get('type', 'unknown'))

    for key in ('event_id', 'id', 'uuid'):
        value = event_data.get(key)
        if value:
            return f"{event_type}:{value}"

    data = event_data.get('data', {}) if isinstance(event_data.get('data', {}), dict) else {}
    data_id = data.get('id')
    if data_id:
        # Most CompanyCam comment events include a stable comment id here.
        return f"{event_type}:{data_id}"

    payload_hash = hashlib.sha256(raw_body).hexdigest()
    return f"{event_type}:{payload_hash}"


def verify_webhook_auth(event_data, raw_body, headers):
    """
    Verify webhook authenticity.

    Supports either:
    - shared token in payload (token/webhook_token or configured token field)
    - HMAC SHA-256 signature in configured header
    """
    if not Config.WEBHOOK_AUTH_REQUIRED:
        return True, 'Webhook auth disabled by WEBHOOK_AUTH_REQUIRED=False'

    secret = Config.COMPANYCAM_WEBHOOK_SECRET.strip()
    if not secret:
        # Allow local development when secret is intentionally omitted.
        return True, 'Webhook secret not configured; verification skipped'

    data = event_data.get('data', {}) if isinstance(event_data.get('data', {}), dict) else {}
    token_keys = [Config.WEBHOOK_TOKEN_FIELD, 'token', 'webhook_token']

    for key in token_keys:
        for container in (event_data, data):
            token = container.get(key) if isinstance(container, dict) else None
            if token and hmac.compare_digest(str(token), secret):
                return True, f'Webhook token validated using field: {key}'

    # Some senders place shared tokens in headers rather than payload fields.
    header_token_keys = [
        'X-Webhook-Token',
        'X-CompanyCam-Token',
        'X-Companycam-Token',
        'Webhook-Token',
    ]
    for header_name in header_token_keys:
        header_token = headers.get(header_name, '') or headers.get(header_name.lower(), '')
        if header_token and hmac.compare_digest(str(header_token), secret):
            return True, f'Webhook token validated using header: {header_name}'

    signature = headers.get(Config.WEBHOOK_SIGNATURE_HEADER, '') or headers.get(
        Config.WEBHOOK_SIGNATURE_HEADER.lower(), ''
    )
    if signature:
        sig = signature.strip()
        if sig.startswith('sha256='):
            sig = sig.split('=', 1)[1]

        # SHA-256 variants
        digest256 = hmac.new(secret.encode('utf-8'), raw_body, hashlib.sha256).digest()
        computed_hex = digest256.hex()
        computed_b64 = base64.b64encode(digest256).decode('utf-8')
        computed_b64url = base64.urlsafe_b64encode(digest256).decode('utf-8').rstrip('=')

        if (
            hmac.compare_digest(sig, computed_hex)
            or hmac.compare_digest(sig, computed_b64)
            or hmac.compare_digest(sig, computed_b64url)
        ):
            return True, 'Webhook signature validated (SHA-256)'

        # CompanyCam uses HMAC-SHA1 base64 per their documentation
        digest1 = hmac.new(secret.encode('utf-8'), raw_body, hashlib.sha1).digest()
        computed_sha1_b64 = base64.b64encode(digest1).decode('utf-8')
        if hmac.compare_digest(sig, computed_sha1_b64):
            return True, 'Webhook signature validated (SHA-1)'

    return False, 'Missing or invalid webhook token/signature'
