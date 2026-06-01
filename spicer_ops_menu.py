#!/usr/bin/env python3
"""
spicer_ops_menu.py — Unified admin console for the CompanyCam → MarketSharp integration.

Provides live queue status, queue management, contact mapping, audit history,
worker/service control, and webhook testing in a single interactive terminal UI.

Usage:
    python spicer_ops_menu.py            # interactive menu
    python spicer_ops_menu.py --status   # print queue counts and exit
"""

import argparse
import csv
import datetime
import json
import os
import sqlite3
import shutil
import subprocess
import sys
import time
import hashlib
from pathlib import Path

# ── path bootstrap ────────────────────────────────────────────────────────────
# Set up paths so this can be run from the project root or from src/
ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT / "scripts"
SRC_DIR = ROOT / "src"

# Add repo root so internal modules resolve when run from any cwd
for _p in (str(ROOT), str(SRC_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── optional imports (graceful degradation) ───────────────────────────────────
# Load environment variables from the repo-root .env file.
from env_bootstrap import load_repo_env

load_repo_env(ROOT, override=False)
# Requests is used for webhook testing but not required for other menu functions
try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

# ── runtime config ─────────────────────────────────────────────────────────────
# Allow overriding DB paths and other settings via environment variables
def _resolve_db_path(env_key, default_candidates):
    """Resolve a DB path with sane fallbacks across legacy/new locations."""
    explicit = os.getenv(env_key, "").strip()
    if explicit:
        explicit_path = os.path.abspath(explicit)
        if os.path.exists(explicit_path):
            return explicit_path

        # If an explicit path is configured but missing, prefer an existing
        # known candidate so dashboard/status views point at live data.
        for candidate in default_candidates:
            path = os.path.abspath(str(candidate))
            if os.path.exists(path):
                return path

        # No fallback exists yet; preserve explicit target for fresh-bootstrap.
        return explicit_path

    for candidate in default_candidates:
        path = os.path.abspath(str(candidate))
        if os.path.exists(path):
            return path

    return os.path.abspath(str(default_candidates[0]))


def _resolve_mapping_file_path():
    """Resolve mapping file path with fallback to data/ when root file is absent."""
    explicit = os.getenv("MARKETSHARP_UI_CONTACT_URL_MAP_FILE", "").strip()
    if explicit:
        explicit_path = os.path.abspath(explicit)
        if os.path.exists(explicit_path):
            return explicit_path

    candidates = [
        ROOT / "marketsharp_contact_mappings.json",
        ROOT / "data" / "marketsharp_contact_mappings.json",
    ]
    for candidate in candidates:
        path = os.path.abspath(str(candidate))
        if os.path.exists(path):
            return path

    # Default to legacy root location when no file exists yet.
    return os.path.abspath(str(candidates[0]))


DB_PATH = _resolve_db_path(
    "PENDING_QUEUE_DB_PATH",
    [
        ROOT / "data" / "pending_comments.db",
        ROOT / "pending_comments.db",
        ROOT / "src" / "pending_comments.db",
    ],
)
# Audit DB is separate from the main queue DB to allow it to be more persistent and less volatile.
AUDIT_DB = _resolve_db_path(
    "AUDIT_DB_PATH",
    [
        ROOT / "posted_comments_audit.db",
        ROOT / "data" / "posted_comments_audit.db",
    ],
)
# Contact mapping file for manual URL overrides (project ID or customer name → MarketSharp contact URL)
MAPPING_FILE = _resolve_mapping_file_path()
# Local URL for testing the webhook receiver (must match the URL configured in CompanyCam)
WEBHOOK_LOCAL_URL = f"http://127.0.0.1:{os.getenv('FLASK_PORT', '5001')}/webhook/companycam"
HEALTH_URL = f"http://127.0.0.1:{os.getenv('FLASK_PORT', '5001')}/health"

CANONICAL_QUEUE_DB = os.path.abspath(str(ROOT / "data" / "pending_comments.db"))
CANONICAL_IDEMPOTENCY_DB = os.path.abspath(str(ROOT / "data" / "cc_webhook_dedupe.db"))
CANONICAL_AUDIT_DB = os.path.abspath(str(ROOT / "posted_comments_audit.db"))
BACKUP_DIR = ROOT / "backups"

QUEUE_DB_CANDIDATES = [
    CANONICAL_QUEUE_DB,
    os.path.abspath(str(ROOT / "pending_comments.db")),
    os.path.abspath(str(ROOT / "src" / "pending_comments.db")),
]
IDEMPOTENCY_DB_CANDIDATES = [
    CANONICAL_IDEMPOTENCY_DB,
    os.path.abspath(str(ROOT / "cc_webhook_dedupe.db")),
]
AUDIT_DB_CANDIDATES = [
    CANONICAL_AUDIT_DB,
    os.path.abspath(str(ROOT / "data" / "posted_comments_audit.db")),
]

# Worker services to check/control; these are the core queue processors that should be running.
WORKER_SERVICES = [
    "marketsharp_queue_worker.service",
    "marketsharp_queue_worker_event.service",
]
# ALL_SERVICES includes the workers plus the Flask API service and the true_fail checker.
ALL_SERVICES = WORKER_SERVICES + [
    "spicer-flask-api.service",
    "true_fail_checker.service",
]

# ── terminal colors ────────────────────────────────────────────────────────────
# Simple ANSI color codes for terminal output; no external dependencies needed.
def _c(text, code): return f"\033[{code}m{text}\033[0m"
def red(t):     return _c(t, "31")
def green(t):   return _c(t, "32")
def yellow(t):  return _c(t, "33")
def blue(t):    return _c(t, "34")
def magenta(t): return _c(t, "35")
def cyan(t):    return _c(t, "36")
def gray(t):    return _c(t, "90")
def orange(t):  return _c(t, "38;5;208m")  # Bright orange for emphasis
# Text styles
# Bold for emphasis (e.g. customer names, counts)
def bold(t):    return f"\033[1m{t}\033[0m"
# Dimmed text for less important info or placeholders
def dim(t):     return f"\033[2m{t}\033[0m"
# Underline for section headers or important notes
def ul(t):      return f"\033[4m{t}\033[0m"

# Status colors for queue items; default to no color if status is unrecognized
STATUS_COLOR = {
    "pending":    yellow,
    "processing": cyan,
    "posted":     green,
    "unmatched":  magenta,
    "true_fail":  red,
}

# ── utility functions ─────────────────────────────────────────────────────────
# Progress bar for long-running operations; call with current and total counts to update in place.
def universalProgressBar(current, total, bar_length=30):
    percent = float(current) / total
    arrow = '█' * int(round(percent * bar_length))
    spaces = '░' * (bar_length - len(arrow))
    sys.stdout.write(f"\rProgress: [{arrow}{spaces}] {int(percent * 100)}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        print()  # New line on completion

# Colorize status text based on predefined STATUS_COLOR mapping; defaults to plain text if status is unknown.
def clr_status(s):
   fn = STATUS_COLOR.get(s, str)
   return fn(s)

# Format a timestamp (in seconds) as a human-readable string, or 'n/a' if falsy.
def fmt_ts(ts):
    if not ts:
        return dim("n/a")
    try:
        return datetime.datetime.fromtimestamp(int(ts)).strftime("%m/%d %H:%M")
    except Exception:
        return str(ts)
    
# Clear the terminal screen (cross-platform)
def clear():
    os.system("clear" if os.name == "posix" else "cls")

# Pause and wait for user input, with an optional message.
def pause(msg="Press Enter to continue..."):
    input(dim(f"\n{msg}"))

# Horizontal rule for separating sections in the menu; customizable character and width.
def hr(char="─", width=72):
    print(dim(char * width))

# Print a section header with a title, surrounded by horizontal rules for emphasis.
def section(title):
    hr()
    print(bold(cyan(f"  {title}")))
    hr()


def render_menu_options(options, indent="  ", key_style=cyan):
    """Render menu options with optional dimmed gray descriptions.

    Each option can be:
    - (key, label)
    - (key, label, description)
    """
    for opt in options:
        key = opt[0]
        label = opt[1]
        desc = opt[2] if len(opt) > 2 else ""
        suffix = f" {dim(gray('— ' + desc))}" if desc else ""
        print(f"{indent}[{key_style(key)}] {label}{suffix}")

# ── splash ─────────────────────────────────────────────────────────────────────
SPLASH = r"""
   _____            
  / ===_|      @                        ////////  ////////  ////////  ////////////////////
 | (___   ___  _  ___   ____     ___    ///  ///  ///  ///    ///     //////  ///  /////// 
  \___ \ / _ \| |/ __\ / __ \|^^//^\\   ///  ///  ///  ///    ///     //////////////////// 
  ____) | |_| | | (___|  ^__/|  /       ////////  ////////    ///     ///  /////////  ////
 |_____/|  __/\__,___/ \____/|__|       ///  ///  ///         ///     //// //////// //////
        | |                             ///  ///  ///         ///     /////________///////
        |_|                             ///  ///  ///       ///////   //////////////////// 
Spicer Bros. Admin Console                                            written by Ryan Ellis
"""

# ── splash and status display ─────────────────────────────────────────────────
# Display the splash screen and optionally show queue counts in a status line. This is called at the start of the menu and after certain actions to refresh the display.
def print_splash(counts=None):
    clear()
    print(dim(yellow(SPLASH)))
    if counts:
        parts = []
        for s in ("pending", "processing", "unmatched", "true_fail", "posted"):
            n = counts.get(s, 0)
            if n or s in ("pending", "posted"):
                parts.append(f"{clr_status(s)}: {bold(str(n))}")
        print("  Queue → " + "  │  ".join(parts))
    print()

# ── DB helpers ─────────────────────────────────────────────────────────────────
# These functions abstract the database access for the pending comments queue and the audit log.

# Connect to the SQLite database at the given path (or default DB_PATH) and return a connection object with row factory set to sqlite3.Row for dict-like access.
def db_connect(path=None):
    conn = sqlite3.connect(path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _discover_existing_db_paths(candidates):
    seen = set()
    out = []
    for path in candidates:
        p = os.path.abspath(str(path))
        if p in seen:
            continue
        seen.add(p)
        if os.path.exists(p):
            out.append(p)
    return out


def _sqlite_table_names(path):
    try:
        with sqlite3.connect(path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _queue_counts_for_path(path):
    try:
        with sqlite3.connect(path) as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM pending_comments GROUP BY status"
            ).fetchall()
        return {status: count for status, count in rows}
    except Exception:
        return None


def _sqlite_integrity(path):
    try:
        with sqlite3.connect(path) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        if not row:
            return "unknown"
        return str(row[0])
    except Exception as e:
        return f"error: {e}"


def _file_sha256(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _queue_db_fingerprint(path):
    """Return stable queue DB fingerprint for duplicate detection.

    Fingerprint combines file hash and row count where available.
    """
    if not os.path.exists(path):
        return "missing"
    digest = _file_sha256(path)
    counts = _queue_counts_for_path(path)
    if counts is None:
        return f"{digest}|no-pending-table"
    return f"{digest}|{sorted(counts.items())}"


def _backup_file(path):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    src = Path(path)
    dest = BACKUP_DIR / f"{src.name}_{stamp}"
    shutil.copy2(src, dest)
    return dest


def _ensure_pending_comments_schema(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE,
                customer_name TEXT,
                comment_text TEXT,
                author_name TEXT,
                payload_json TEXT,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                created_at INTEGER,
                updated_at INTEGER
            )
            """
        )
        conn.commit()


def _read_pending_rows(path):
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT event_id, customer_name, comment_text, author_name, payload_json,
                   status, retry_count, last_error, created_at, updated_at
            FROM pending_comments
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def _row_key(row):
    event_id = (row.get("event_id") or "").strip()
    if event_id:
        return ("event", event_id)
    return (
        "fallback",
        row.get("customer_name") or "",
        row.get("comment_text") or "",
        row.get("author_name") or "",
        row.get("created_at") or 0,
    )


def _consolidate_queue_dbs(destination, sources):
    _ensure_pending_comments_schema(destination)
    existing_keys = set()
    for row in _read_pending_rows(destination):
        existing_keys.add(_row_key(row))

    inserted = 0
    with sqlite3.connect(destination) as conn:
        for source in sources:
            if not os.path.exists(source):
                continue
            if "pending_comments" not in _sqlite_table_names(source):
                continue
            for row in _read_pending_rows(source):
                key = _row_key(row)
                if key in existing_keys:
                    continue
                existing_keys.add(key)
                conn.execute(
                    """
                    INSERT INTO pending_comments (
                        event_id, customer_name, comment_text, author_name, payload_json,
                        status, retry_count, last_error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row.get("event_id"),
                        row.get("customer_name"),
                        row.get("comment_text"),
                        row.get("author_name"),
                        row.get("payload_json"),
                        row.get("status") or "pending",
                        int(row.get("retry_count") or 0),
                        row.get("last_error"),
                        int(row.get("created_at") or int(time.time())),
                        int(row.get("updated_at") or int(time.time())),
                    ),
                )
                inserted += 1
        conn.commit()
    return inserted

# Queue operations: fetching counts, fetching items by status, updating item status, etc.
def queue_counts():
    # Return a dict of counts by status, e.g. {"pending": 5, "posted": 20}, or an empty dict if DB is missing or an error occurs.
    if not os.path.exists(DB_PATH):
        return {}
    try:
        with db_connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM pending_comments GROUP BY status"
            ).fetchall()
        return {r["status"]: r["n"] for r in rows}
    except Exception:
        return {}


def queue_db_preflight_warning():
    """Return warning text when multiple queue DB candidates coexist."""
    existing = _discover_existing_db_paths(QUEUE_DB_CANDIDATES)
    if len(existing) <= 1:
        return ""

    fingerprints = {_queue_db_fingerprint(path) for path in existing}
    if len(fingerprints) == 1:
        # Multiple files exist but content is aligned; no active data risk.
        return ""

    return (
        f"Multiple queue DB files detected ({len(existing)}). "
        "Use [d] Database Administration -> Consolidate queue DBs to canonical path."
    )


def print_predeploy_checks_noninteractive():
    """Run DB-focused predeploy checks and return process exit code."""
    errors = []
    warnings = []

    queue_existing = _discover_existing_db_paths(QUEUE_DB_CANDIDATES)
    if len(queue_existing) > 1:
        fingerprints = {_queue_db_fingerprint(path) for path in queue_existing}
        if len(fingerprints) > 1:
            warnings.append(
                "Queue DB candidates diverge in content; consolidate before deploy."
            )

    db_targets = []
    for path in [DB_PATH, AUDIT_DB] + QUEUE_DB_CANDIDATES + IDEMPOTENCY_DB_CANDIDATES + AUDIT_DB_CANDIDATES:
        p = os.path.abspath(path)
        if os.path.exists(p) and p not in db_targets:
            db_targets.append(p)

    for path in db_targets:
        integrity = _sqlite_integrity(path)
        if integrity != "ok":
            errors.append(f"Integrity check failed for {path}: {integrity}")

    print("Predeploy DB checks")
    print("-------------------")
    print(f"Queue DB in use: {DB_PATH}")
    print(f"Audit DB in use: {AUDIT_DB}")
    print(f"DB files checked: {len(db_targets)}")

    if warnings:
        print("Warnings:")
        for message in warnings:
            print(f"- {message}")
    else:
        print("Warnings: none")

    if errors:
        print("Errors:")
        for message in errors:
            print(f"- {message}")
        return 1

    print("Errors: none")
    print("Result: PASS")
    return 0

# Fetch queue items by status (e.g. ["pending", "unmatched"]) with an optional limit; returns a list of dicts.
def fetch_queue(statuses=None, limit=200):
    if not os.path.exists(DB_PATH):
        return []
    clause = ""
    params = []
    if statuses:
        placeholders = ",".join("?" * len(statuses))
        clause = f"WHERE status IN ({placeholders})"
        params = list(statuses)
    with db_connect() as conn:
        rows = conn.execute(
            f"""SELECT id, event_id, customer_name, author_name, comment_text,
                       status, retry_count, last_error, created_at, updated_at
                FROM pending_comments {clause}
                ORDER BY updated_at DESC LIMIT ?""",
            params + [limit],
        ).fetchall()
    return [dict(r) for r in rows]

# Update the status of a queue item, optionally setting the last error message.
def queue_set_status(item_id, status, last_error=None):
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "UPDATE pending_comments SET status=?, last_error=?, updated_at=?, retry_count=0 WHERE id=?",
            (status, last_error, now, item_id),
        )
        conn.commit()

# Update the customer name of a queue item and reset it to pending status, optionally setting a last error message.
def queue_update_name(item_id, new_name):
    now = int(time.time())
    with db_connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM pending_comments WHERE id=?",
            (item_id,),
        ).fetchone()
        payload_json = row[0] if row else None
        new_payload_json = payload_json
        # Keep payload_json in sync with customer_name when possible.
        if payload_json:
            try:
                payload = json.loads(payload_json)
                if isinstance(payload, dict):
                    payload["customer_name"] = new_name
                    new_payload_json = json.dumps(payload)
            except Exception:
                new_payload_json = payload_json
        conn.execute(
            "UPDATE pending_comments SET payload_json=?, customer_name=?, status='pending', retry_count=0, updated_at=? WHERE id=?",
            (new_payload_json, new_name, now, item_id),
        )
        conn.commit()

# ── queue display helpers ──────────────────────────────────────────────────────
# Functions for displaying queue items in the terminal, including color-coding and formatting for better readability.

# Print a single queue item in a concise format for list views, showing ID, status, customer name, retry count, and last error if present.
def print_queue_row(item, idx=None):
    prefix = f"  {dim(str(idx) + '.')} " if idx is not None else "  "
    status = clr_status(item["status"])
    name = bold(item["customer_name"] or dim("(no name)"))
    rc = item.get("retry_count", 0)
    rc_str = f" {red('x' + str(rc))}" if rc else ""
    ts = fmt_ts(item.get("updated_at"))
    print(f"{prefix}[{bold(str(item['id']))}] {status}{rc_str}  {name}  {dim(ts)}")
    if item.get("last_error"):
        print(f"       {dim(item['last_error'][:90])}")

# Detailed view of a queue item, showing all fields and an excerpt of the comment and payload.
def print_item_detail(item):
    section(f"Queue Item #{item['id']}")
    # Define the fields to display with their labels and values, applying color and formatting as needed.
    fields = [
        ("Status",    clr_status(item["status"])),
        ("Customer",  bold(item.get("customer_name", ""))),
        ("Author",    item.get("author_name", "")),
        ("Retries",   str(item.get("retry_count", 0))),
        ("Created",   fmt_ts(item.get("created_at"))),
        ("Updated",   fmt_ts(item.get("updated_at"))),
        ("Event ID",  dim(item.get("event_id", ""))),
        ("Last Error",item.get("last_error") or dim("none")),
    ]
    # Print each field with a label and value, applying color and formatting as defined in the fields list.
    for label, val in fields:
        print(f"  {bold(label + ':'):<22} {val}")
    print()
    text = item.get("comment_text", "")
    if text:
        print(f"  {bold('Comment:')}")
        for line in text.splitlines()[:6]:
            print(f"    {line}")
    try:
        payload = json.loads(item.get("payload_json") or "{}")
        proj = (payload.get("data") or {}).get("payload", {})
        if proj:
            print(f"\n  {bold('Payload excerpt:')}")
            print(f"    {json.dumps(proj, indent=2)[:300]}")
    except Exception:
        pass

# ── queue management menu ──────────────────────────────────────────────────────
def menu_queue_status(counts):
    section("Queue Status")
    total = sum(counts.values())
    for s in ("pending", "processing", "unmatched", "true_fail", "posted"):
        n = counts.get(s, 0)
        bar = green("█" * min(n, 30)) if n else dim("░")
        print(f"  {clr_status(s):<30} {bold(str(n)):>6}  {bar}")
    hr()
    print(f"  {'TOTAL':<30} {bold(str(total)):>6}")
    pause()

def menu_browse_queue(counts):
    statuses = ["pending", "processing", "unmatched", "true_fail"]
    while True:
        section("Browse Queue")
        items = fetch_queue(statuses)
        if not items:
            print(green("  Queue is empty (no pending/unmatched/true_fail items)."))
            pause()
            return
        for i, item in enumerate(items, 1):
            print_queue_row(item, i)
        print()
        render_menu_options([
            ("#", "Inspect item by number", "Open details and actions for one queue item"),
            ("b", "Back", "Return to the previous menu"),
        ])
        choice = input("  > ").strip().lower()
        if choice == "b":
            return
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(items):
                _item_detail_loop(items[idx])

def _item_detail_loop(item):
    while True:
        print_item_detail(item)
        print(bold("  Actions:"))
        render_menu_options([
            ("r", "Requeue", "Set status to pending and retry processing"),
            ("e", "Edit Name", "Rename customer and requeue item"),
            ("d", "Delete", "Permanently remove this queue item"),
            ("s", "Skip / Back", "Return to browse without changes"),
        ], indent="    ")
        act = input("  > ").strip().lower()
        if act == "r":
            queue_set_status(item["id"], "pending", last_error="Manual requeue")
            print(green(f"  ✓ Item #{item['id']} requeued."))
            pause()
            return
        elif act == "e":
            new_name = input(f"  New customer name [{item['customer_name']}]: ").strip()
            if new_name:
                queue_update_name(item["id"], new_name)
                print(green(f"  ✓ Renamed → '{new_name}' and requeued."))
            pause()
            return
        elif act == "d":
            confirm = input(red(f"  Delete item #{item['id']}? (yes/N): ")).strip().lower()
            if confirm == "yes":
                with db_connect() as conn:
                    conn.execute("DELETE FROM pending_comments WHERE id=?", (item["id"],))
                    conn.commit()
                print(red(f"  ✗ Item #{item['id']} deleted."))
            pause()
            return
        elif act in ("s", "b", ""):
            return

def menu_requeue_all_unmatched():
    section("Requeue All Unmatched")
    items = fetch_queue(["unmatched"])
    if not items:
        print(green("  No unmatched items to requeue."))
        pause()
        return
    print(f"  Found {yellow(str(len(items)))} unmatched items:")
    for item in items[:10]:
        print_queue_row(item)
    if len(items) > 10:
        print(dim(f"  ... and {len(items) - 10} more"))
    confirm = input(f"\n  Requeue all {len(items)} unmatched items? (y/N): ").strip().lower()
    if confirm == "y":
        now = int(time.time())
        with db_connect() as conn:
            conn.execute(
                "UPDATE pending_comments SET status='pending', retry_count=0, updated_at=? WHERE status='unmatched'",
                (now,),
            )
            conn.commit()
        print(green(f"  ✓ {len(items)} items requeued."))
    pause()

def menu_requeue_true_fails():
    section("Review True-Fail Items")
    items = fetch_queue(["true_fail"])
    if not items:
        print(green("  No true_fail items."))
        pause()
        return
    for i, item in enumerate(items, 1):
        print_queue_row(item, i)
    print()
    print(dim("  Tip: type e to rename a true_fail item and requeue it."))
    raw = input("  Enter IDs to requeue (comma-separated), [a]ll, [e]dit name, or [b]ack: ").strip().lower()
    if raw == "b" or not raw:
        return

    if raw == "e":
        try:
            selected = int(input("  Enter ID to rename + requeue: ").strip())
        except ValueError:
            print(red("  Invalid ID."))
            pause()
            return
        target = next((item for item in items if item["id"] == selected), None)
        if not target:
            print(red("  ID not found in true_fail list."))
            pause()
            return
        current_name = target.get("customer_name") or ""
        new_name = input(f"  New customer name [{current_name}]: ").strip()
        if not new_name:
            print(yellow("  No name entered; nothing changed."))
            pause()
            return
        queue_update_name(selected, new_name)
        print(green(f"  ✓ Renamed item #{selected} to '{new_name}' and requeued."))
        pause()
        return

    now = int(time.time())
    if raw == "a":
        ids = [item["id"] for item in items]
    else:
        try:
            ids = [int(x.strip()) for x in raw.split(",")]
        except ValueError:
            print(red("  Invalid input."))
            pause()
            return
    with db_connect() as conn:
        for iid in ids:
            conn.execute(
                "UPDATE pending_comments SET status='pending', retry_count=0, updated_at=? WHERE id=?",
                (now, iid),
            )
        conn.commit()
    print(green(f"  ✓ {len(ids)} item(s) requeued."))
    pause()

def menu_requeue_posted():
    section("Re-push Posted Comments")
    items = fetch_queue(["posted"])
    if not items:
        print(dim("  No posted items."))
        pause()
        return
    for item in items:
        print_queue_row(item)
    print()
    raw = input(f"  Requeue which? [a]ll {len(items)}, comma-sep IDs, or [b]ack: ").strip().lower()
    if raw == "b" or not raw:
        return
    now = int(time.time())
    if raw == "a":
        ids = [item["id"] for item in items]
    else:
        try:
            ids = [int(x.strip()) for x in raw.split(",")]
        except ValueError:
            print(red("  Invalid input."))
            pause()
            return
    with db_connect() as conn:
        for iid in ids:
            conn.execute(
                "UPDATE pending_comments SET status='pending', retry_count=0, last_error='Manual re-push', updated_at=? WHERE id=?",
                (now, iid),
            )
        conn.commit()
    print(green(f"  ✓ {len(ids)} item(s) requeued."))
    pause()

def menu_check_duplicates():
    section("Duplicate Check")
    if not os.path.exists(DB_PATH):
        print(red("  DB not found."))
        pause()
        return
    with db_connect() as conn:
        dup_event = conn.execute(
            "SELECT event_id, COUNT(*) AS n FROM pending_comments GROUP BY event_id HAVING n > 1"
        ).fetchall()
        dup_text = conn.execute(
            "SELECT comment_text, COUNT(*) AS n FROM pending_comments GROUP BY comment_text HAVING n > 1"
        ).fetchall()
    if not dup_event and not dup_text:
        print(green("  No duplicates found in queue DB."))
        pause()
        return
    if dup_event:
        print(yellow(f"  Duplicate event_ids:"))
        for row in dup_event:
            print(yellow(f"    {row['n']}x  {row['event_id']}"))
            with db_connect() as conn:
                dupes = conn.execute(
                    "SELECT id, customer_name, status, created_at FROM pending_comments WHERE event_id=? ORDER BY id",
                    (row["event_id"],)
                ).fetchall()
            for d in dupes:
                print(f"       #{d['id']} [{d['status']}] {d['customer_name']}  {fmt_ts(d['created_at'])}")
        print()
    if dup_text:
        print(yellow(f"  Duplicate comment texts:"))
        for row in dup_text:
            snippet = (row["comment_text"] or "")[:60]
            print(yellow(f"    {row['n']}x: {snippet}…"))
            with db_connect() as conn:
                dupes = conn.execute(
                    "SELECT id, customer_name, status, created_at FROM pending_comments WHERE comment_text=? ORDER BY id",
                    (row["comment_text"],)
                ).fetchall()
            for d in dupes:
                print(f"       #{d['id']} [{d['status']}] {d['customer_name']}  {fmt_ts(d['created_at'])}")
        print()
    opts = [
        ("1", "Delete older duplicate event_ids", "Keep the newest item per duplicate event_id, delete older ones"),
        ("2", "Delete older duplicate comment texts", "Keep the newest item per duplicate comment text, delete older ones"),
        ("b", "Back", "Return without changes"),
    ]
    render_menu_options(opts)
    choice = input("\n  > ").strip().lower()
    if choice == "1":
        deleted = 0
        with db_connect() as conn:
            for row in dup_event:
                max_id = conn.execute(
                    "SELECT MAX(id) FROM pending_comments WHERE event_id=?", (row["event_id"],)
                ).fetchone()[0]
                result = conn.execute(
                    "DELETE FROM pending_comments WHERE event_id=? AND id != ?", (row["event_id"], max_id)
                )
                deleted += result.rowcount
            conn.commit()
        print(green(f"  ✓ Deleted {deleted} older duplicate entries (by event_id)."))
        pause()
    elif choice == "2":
        deleted = 0
        with db_connect() as conn:
            for row in dup_text:
                max_id = conn.execute(
                    "SELECT MAX(id) FROM pending_comments WHERE comment_text=?", (row["comment_text"],)
                ).fetchone()[0]
                result = conn.execute(
                    "DELETE FROM pending_comments WHERE comment_text=? AND id != ?", (row["comment_text"], max_id)
                )
                deleted += result.rowcount
            conn.commit()
        print(green(f"  ✓ Deleted {deleted} older duplicate entries (by comment text)."))
        pause()

# ── contact mapping menu ───────────────────────────────────────────────────────
def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        return {}
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_mapping(data):
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def menu_contact_mapping():
    while True:
        section("Contact Mapping")
        mapping = load_mapping()
        print(f"  Mapping file: {dim(MAPPING_FILE)}")
        print(f"  Entries: {bold(str(len(mapping)))}\n")
        opts = [
            ("1", "List all mappings", "Show every project/contact override currently configured"),
            ("2", "Add / update a mapping", "Create or overwrite one mapping entry"),
            ("3", "Delete a mapping", "Remove one mapping key from the file"),
            ("4", "List unresolved queue items (no mapping)", "Show queue rows lacking a direct mapping"),
            ("5", "Import Google group CSVs -> mention groups JSON", "Build mention group aliases from Google exports"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _list_mappings(mapping)
        elif choice == "2":
            _upsert_mapping(mapping)
        elif choice == "3":
            _delete_mapping(mapping)
        elif choice == "4":
            _list_unresolved(mapping)
        elif choice == "5":
            _import_google_group_mentions()
        elif choice == "b":
            return


def _import_google_group_mentions():
    section("Import Google Groups to Mention Map")
    script_path = SCRIPTS_DIR / "import_google_groups_to_mentions.py"
    if not script_path.exists():
        print(red(f"  Missing importer script: {script_path}"))
        pause()
        return

    input_dir = input("  Directory containing group CSV files: ").strip()
    if not input_dir:
        print(yellow("  Input directory is required."))
        pause()
        return

    if not os.path.isdir(input_dir):
        print(red(f"  Directory not found: {input_dir}"))
        pause()
        return

    glob_pattern = input("  File glob [*.csv]: ").strip() or "*.csv"
    default_domain = input("  Default domain [spicerbros.com]: ").strip() or "spicerbros.com"
    output_json_default = str(ROOT / "tagger" / "marketsharp_group_mentions.json")
    output_json = input(f"  Output JSON [{output_json_default}]: ").strip() or output_json_default

    cmd = [
        sys.executable,
        str(script_path),
        "--input-dir",
        input_dir,
        "--glob",
        glob_pattern,
        "--default-domain",
        default_domain,
        "--output-json",
        output_json,
    ]

    print(dim("\n  Running importer..."))
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        output = ((r.stdout or "") + ("\n" + r.stderr if r.stderr else "")).strip()
        if output:
            print(output)
        if r.returncode != 0:
            print(red(f"\n  Import failed (exit {r.returncode})."))
            pause()
            return
    except Exception as e:
        print(red(f"  Import failed: {e}"))
        pause()
        return

    print(green("\n  ✓ Mention group map import completed."))
    print(dim(f"  Output: {output_json}"))
    print(dim("  Restart marketsharp_comment_worker.service to load new group map."))
    pause()

def _list_mappings(mapping):
    section("All Contact Mappings")
    if not mapping:
        print(dim("  (empty)"))
        pause()
        return
    for i, (k, v) in enumerate(sorted(mapping.items()), 1):
        key_col = cyan(k) if k.startswith("project:") else magenta(k)
        print(f"  {dim(str(i) + '.')} {key_col}")
        print(f"       {dim(v)}")
    pause()

def _upsert_mapping(mapping):
    section("Add / Update Mapping")
    print("  Key formats:  project:12345678   or   name:john smith")
    key = input("  Key: ").strip()
    if not key:
        return
    url = input("  MarketSharp contact URL: ").strip()
    if not url:
        return
    mapping[key] = url
    save_mapping(mapping)
    print(green(f"  ✓ Saved: {key} → {url[:60]}…"))
    pause()

def _delete_mapping(mapping):
    section("Delete Mapping")
    key = input("  Key to delete: ").strip()
    if key in mapping:
        del mapping[key]
        save_mapping(mapping)
        print(green(f"  ✓ Deleted: {key}"))
    else:
        print(yellow(f"  Key not found: {key}"))
    pause()

def _list_unresolved(mapping):
    section("Queue Items Without a Contact Mapping")
    items = fetch_queue(["pending", "processing", "unmatched"])
    unresolved = []
    for item in items:
        try:
            payload = json.loads(item.get("payload_json") or "{}")
            proj_id = (payload.get("data") or {}).get("project_id") or \
                      (payload.get("data") or {}).get("location_id")
        except Exception:
            proj_id = None
        proj_key = f"project:{proj_id}" if proj_id else None
        name_key = f"name:{(item.get('customer_name') or '').lower()}"
        if not (proj_key in mapping or name_key in mapping):
            unresolved.append((item, proj_id))
    if not unresolved:
        print(green("  All active queue items have a mapping or will use OData search."))
    else:
        for item, proj_id in unresolved[:30]:
            print_queue_row(item)
            if proj_id:
                print(f"       {dim('project id: ' + str(proj_id))}")
    pause()

# ── audit log menu ─────────────────────────────────────────────────────────────
def ensure_audit_table():
    with db_connect(AUDIT_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS posted_comments_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT, customer_id TEXT, customer_name TEXT,
                author_name TEXT, comment_text TEXT,
                posted_at INTEGER, posted_at_iso TEXT, extra_json TEXT
            )
        """)
        conn.commit()

def menu_audit_log():
    while True:
        section("Audit Log")
        opts = [
            ("1", "Show recent posted comments (last 50)", "View newest successful posts in the audit DB"),
            ("2", "Search audit log by customer name", "Filter recent audit history by customer"),
            ("3", "Export audit log to CSV", "Write full posted audit history to a CSV file"),
            ("4", "Check for posted items missing from audit", "Cross-check queue posted rows vs audit entries"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _audit_recent()
        elif choice == "2":
            _audit_search()
        elif choice == "3":
            _audit_export_csv()
        elif choice == "4":
            _audit_crosscheck()
        elif choice == "b":
            return

def _audit_recent():
    section("Recent Posted Comments")
    if not os.path.exists(AUDIT_DB):
        print(dim("  No audit DB found yet."))
        pause()
        return
    ensure_audit_table()
    with db_connect(AUDIT_DB) as conn:
        rows = conn.execute(
            "SELECT * FROM posted_comments_audit ORDER BY posted_at DESC LIMIT 50"
        ).fetchall()
    if not rows:
        print(dim("  Audit log is empty."))
        pause()
        return
    print(f"  {'ID':>5}  {'Posted':>14}  {'Customer':<30}  Comment excerpt")
    hr("─", 80)
    for r in rows:
        snippet = (r["comment_text"] or "")[:40]
        print(f"  {r['id']:>5}  {fmt_ts(r['posted_at']):>14}  {(r['customer_name'] or ''):<30}  {dim(snippet)}")
    pause()

def _audit_search():
    term = input("  Search customer name: ").strip()
    if not term:
        return
    ensure_audit_table()
    with db_connect(AUDIT_DB) as conn:
        rows = conn.execute(
            "SELECT * FROM posted_comments_audit WHERE customer_name LIKE ? ORDER BY posted_at DESC LIMIT 50",
            (f"%{term}%",),
        ).fetchall()
    section(f"Audit results for '{term}'")
    if not rows:
        print(dim("  No results."))
    for r in rows:
        print(f"  [{r['id']}] {fmt_ts(r['posted_at'])}  {r['customer_name']}  — {dim((r['comment_text'] or '')[:60])}")
    pause()

def _audit_export_csv():
    out_path = ROOT / f"audit_export_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    ensure_audit_table()
    with db_connect(AUDIT_DB) as conn:
        rows = conn.execute("SELECT * FROM posted_comments_audit ORDER BY posted_at ASC").fetchall()
    if not rows:
        print(dim("  Audit log is empty."))
        pause()
        return
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(rows[0].keys())
        writer.writerows(rows)
    print(green(f"  ✓ Exported {len(rows)} rows → {out_path}"))
    pause()

def _audit_crosscheck():
    section("Posted Items Missing from Audit")
    ensure_audit_table()
    with db_connect(AUDIT_DB) as conn:
        audit_ids = {r[0] for r in conn.execute("SELECT event_id FROM posted_comments_audit").fetchall()}
    posted = fetch_queue(["posted"])
    missing = [i for i in posted if i.get("event_id") not in audit_ids]
    if not missing:
        print(green("  All posted queue items are in the audit log."))
    else:
        print(yellow(f"  {len(missing)} posted item(s) not in audit:"))
        for item in missing[:20]:
            print_queue_row(item)
    pause()


# ── database administration menu ─────────────────────────────────────────────
def menu_database_admin():
    while True:
        section("Database Administration")
        opts = [
            ("1", "Show DB paths and row counts", "Display active DB files and current row totals"),
            ("2", "Backup active DB files now", "Create timestamped backups of live DB files"),
            ("3", "Run SQLite integrity checks", "Run PRAGMA integrity_check on discovered DB files"),
            ("4", "Consolidate queue DBs to canonical path", "Merge duplicate queue DBs into data/pending_comments.db"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _db_admin_show_paths()
        elif choice == "2":
            _db_admin_backup_now()
        elif choice == "3":
            _db_admin_integrity_check()
        elif choice == "4":
            _db_admin_consolidate_queue()
        elif choice == "b":
            return


def _db_admin_show_paths():
    section("DB Paths and Status")

    print(f"  Queue DB in use:      {bold(DB_PATH)}")
    print(f"  Queue canonical path: {dim(CANONICAL_QUEUE_DB)}")
    print(f"  Audit DB in use:      {bold(AUDIT_DB)}")
    print(f"  Audit canonical path: {dim(CANONICAL_AUDIT_DB)}")
    print(f"  Idempotency canonical:{dim(CANONICAL_IDEMPOTENCY_DB)}")
    print()

    def _show_group(title, candidates):
        print(bold(f"  {title}:"))
        existing = _discover_existing_db_paths(candidates)
        if not existing:
            print(dim("    (no files found)"))
            return
        for path in existing:
            size = os.path.getsize(path)
            tables = _sqlite_table_names(path)
            queue_counts = _queue_counts_for_path(path)
            print(f"    {path}  {dim(str(size) + ' bytes')}")
            if queue_counts is not None:
                print(f"      pending_comments: {queue_counts}")
            else:
                print(f"      tables: {tables}")

    _show_group("Queue DB candidates", QUEUE_DB_CANDIDATES)
    _show_group("Idempotency DB candidates", IDEMPOTENCY_DB_CANDIDATES)
    _show_group("Audit DB candidates", AUDIT_DB_CANDIDATES)
    pause()


def _db_admin_backup_now():
    section("Backup Active DB Files")
    targets = []
    for path in [DB_PATH, AUDIT_DB] + IDEMPOTENCY_DB_CANDIDATES:
        p = os.path.abspath(path)
        if os.path.exists(p) and p not in targets:
            targets.append(p)

    if not targets:
        print(yellow("  No database files found to back up."))
        pause()
        return

    print("  Will back up:")
    for p in targets:
        print(f"   - {p}")
    confirm = input("\n  Continue? (y/N): ").strip().lower()
    if confirm != "y":
        return

    for p in targets:
        try:
            dest = _backup_file(p)
            print(green(f"  ✓ {p} -> {dest}"))
        except Exception as e:
            print(red(f"  ✗ Backup failed for {p}: {e}"))
    pause()


def _db_admin_integrity_check():
    section("SQLite Integrity Check")
    targets = []
    for path in QUEUE_DB_CANDIDATES + IDEMPOTENCY_DB_CANDIDATES + AUDIT_DB_CANDIDATES:
        p = os.path.abspath(path)
        if os.path.exists(p) and p not in targets:
            targets.append(p)
    if not targets:
        print(yellow("  No database files found."))
        pause()
        return

    for p in targets:
        result = _sqlite_integrity(p)
        color = green if result == "ok" else yellow
        print(f"  {p}\n    -> {color(result)}")
    pause()


def _db_admin_consolidate_queue():
    section("Consolidate Queue DBs")
    existing = _discover_existing_db_paths(QUEUE_DB_CANDIDATES)
    if not existing:
        print(yellow("  No queue DB files found."))
        pause()
        return

    print(f"  Destination canonical DB: {bold(CANONICAL_QUEUE_DB)}")
    print("  Source DB files detected:")
    for p in existing:
        print(f"   - {p}")

    print()
    print(dim("  This operation merges missing queue rows into canonical DB and backs up non-canonical DB files."))
    confirm = input("  Proceed with consolidation? (yes/N): ").strip().lower()
    if confirm != "yes":
        return

    backups = []
    for p in existing:
        if os.path.abspath(p) == os.path.abspath(CANONICAL_QUEUE_DB):
            continue
        try:
            backups.append((p, _backup_file(p)))
        except Exception as e:
            print(red(f"  ✗ Backup failed for {p}: {e}"))
            pause()
            return

    inserted = _consolidate_queue_dbs(CANONICAL_QUEUE_DB, existing)
    print(green(f"  ✓ Consolidation complete. Inserted {inserted} missing row(s)."))
    for src, backup_path in backups:
        print(dim(f"  Backup kept for {src} at {backup_path}"))

    counts = _queue_counts_for_path(CANONICAL_QUEUE_DB) or {}
    print(f"  Canonical queue counts: {counts}")

    # Offer to remove stale non-canonical files so the split-DB warning clears.
    stale = [p for p, _ in backups]
    if stale:
        print()
        print(dim("  Non-canonical DB files have been backed up and merged."))
        print(dim("  Remove them now to clear the split-DB warning?"))
        for p in stale:
            print(dim(f"   - {p}"))
        if input("  Delete stale files? (yes/N): ").strip().lower() == "yes":
            for p in stale:
                try:
                    os.remove(p)
                    print(green(f"  ✓ Removed {p}"))
                except Exception as e:
                    print(red(f"  ✗ Could not remove {p}: {e}"))
    pause()


def print_db_status_noninteractive():
    """Print DB path/status summary for automation and quick diagnostics."""
    print(f"DB_PATH={DB_PATH}")
    print(f"AUDIT_DB={AUDIT_DB}")
    print(f"CANONICAL_QUEUE_DB={CANONICAL_QUEUE_DB}")
    print(f"CANONICAL_AUDIT_DB={CANONICAL_AUDIT_DB}")
    print(f"CANONICAL_IDEMPOTENCY_DB={CANONICAL_IDEMPOTENCY_DB}")

    groups = [
        ("queue", QUEUE_DB_CANDIDATES),
        ("idempotency", IDEMPOTENCY_DB_CANDIDATES),
        ("audit", AUDIT_DB_CANDIDATES),
    ]
    for name, candidates in groups:
        for path in _discover_existing_db_paths(candidates):
            size = os.path.getsize(path)
            integrity = _sqlite_integrity(path)
            counts = _queue_counts_for_path(path)
            print(f"{name}: {path} size={size} integrity={integrity}")
            if counts is not None:
                print(f"{name}:pending_counts={counts}")

# ── diagnostics & service control ─────────────────────────────────────────────
def _systemctl(action, service):
    try:
        r = subprocess.run(
            ["sudo", "systemctl", action, service],
            capture_output=True, text=True, timeout=15,
        )
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    except FileNotFoundError:
        return False, "systemctl not available (macOS dev machine?)"
    except Exception as e:
        return False, str(e)

def _service_status(service):
    try:
        r = subprocess.run(
            ["systemctl", "is-active", service],
            capture_output=True, text=True, timeout=5,
        )
        return r.stdout.strip()
    except Exception:
        return "unknown"

def menu_diagnostics():
    while True:
        section("Diagnostics & Service Control")
        print(f"  DB path: {dim(DB_PATH)}  exists={green('yes') if os.path.exists(DB_PATH) else red('no')}")
        print(f"  Mapping: {dim(MAPPING_FILE)}  entries={bold(str(len(load_mapping())))}\n")
        print(f"  {'Service':<48} {'Status'}")
        hr("─", 70)
        for svc in ALL_SERVICES:
            status = _service_status(svc)
            col = green if status == "active" else (yellow if status in ("activating", "deactivating") else red)
            print(f"  {svc:<48} {col(status)}")
        print()
        opts = [
            ("1", "Restart queue workers", "Restart queue and event worker services"),
            ("2", "Restart all services", "Restart workers plus API and true-fail checker"),
            ("3", "View worker journal (last 40 lines)", "Show recent systemd logs for queue workers"),
            ("4", "Check local health endpoint", "Call local /health and print status"),
            ("5", "Show env config summary", "Print key runtime environment settings"),
            ("6", "MarketSharp mention worker ops check", "Run packaged mention-worker health script"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _restart_workers()
        elif choice == "2":
            _restart_all()
        elif choice == "3":
            _view_journal()
        elif choice == "4":
            _check_health()
        elif choice == "5":
            _show_env()
        elif choice == "6":
            _marketsharp_comment_worker_ops_check()
        elif choice == "b":
            return

def _marketsharp_comment_worker_ops_check():
    section("MarketSharp Mention Worker Ops Check")
    script_path = SCRIPTS_DIR / "marketsharp_comment_worker_ops_check.sh"
    if not script_path.exists():
        print(red(f"  Missing script: {script_path}"))
        pause()
        return

    try:
        r = subprocess.run(
            ["bash", str(script_path), "marketsharp_comment_worker.service"],
            capture_output=True,
            text=True,
            timeout=90,
        )
        output = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
        print(output.strip() or dim("(no output)"))
        if r.returncode != 0:
            print(yellow(f"\n  Script exited with status {r.returncode}."))
    except Exception as e:
        print(red(f"  Ops check failed: {e}"))
    pause()

def _restart_workers():
    for svc in WORKER_SERVICES:
        ok, msg = _systemctl("restart", svc)
        icon = green("✓") if ok else red("✗")
        print(f"  {icon} {svc}: {dim(msg[:80]) if msg else ''}")
    pause()

def _restart_all():
    for svc in ALL_SERVICES:
        ok, msg = _systemctl("restart", svc)
        icon = green("✓") if ok else red("✗")
        print(f"  {icon} {svc}: {dim(msg[:80]) if msg else ''}")
    pause()

def _view_journal():
    section("Worker Journal")
    # Playwright emits thousands of debug lines per minute; filter them out so
    # the 40 displayed lines are meaningful.  "--since" bounds the scan window
    # to avoid lock-contention on the rapidly-growing journal file.
    _PLAYWRIGHT_NOISE = {
        "retrying click action", "scrolling into view", "done scrolling",
        "waiting for element to be visible", "element is visible, enabled and stable",
        "subtree intercepts pointer events", "waiting 20ms", "waiting 100ms",
        "waiting 500ms",
    }
    try:
        r = subprocess.run(
            ["journalctl", "-u", "marketsharp_queue_worker.service",
             "--since", "30 min ago",
             "--no-pager", "-n", "200", "--output=cat"],
            capture_output=True, text=True, timeout=10,
        )
        raw = r.stdout or r.stderr or ""
        lines = [
            l for l in raw.splitlines()
            if not any(noise in l for noise in _PLAYWRIGHT_NOISE)
        ]
        output = "\n".join(lines[-40:]) or dim("(no output in last 30 min)")
    except FileNotFoundError:
        output = yellow("journalctl not available on this machine.")
    except Exception as e:
        output = red(str(e))
    print(output)
    pause()

def _check_health():
    section("Local Health Check")
    if not _HAS_REQUESTS:
        print(yellow("  'requests' not installed — run: pip install requests"))
        pause()
        return
    for url in [HEALTH_URL]:
        try:
            resp = _requests.get(url, timeout=4)
            col = green if resp.status_code == 200 else yellow
            print(f"  {col(str(resp.status_code))} {url}")
        except Exception as e:
            print(f"  {red('ERR')} {url}  {dim(str(e))}")
    pause()

def _show_env():
    section("Environment Config Summary")
    keys = [
        "MARKETSHARP_MODE", "MARKETSHARP_COMPANY_ID", "MARKETSHARP_ODATA_URL",
        "FLASK_PORT", "PENDING_QUEUE_DB_PATH",
        "MARKETSHARP_UI_BASE_URL", "MARKETSHARP_UI_SEARCH_SELECTOR",
        "COMPANYCAM_WEBHOOK_ID",
    ]
    for k in keys:
        v = os.getenv(k, dim("(not set)"))
        if any(s in k for s in ("KEY", "SECRET", "PASSWORD", "TOKEN")):
            v = green("(set)") if os.getenv(k) else red("(not set)")
        print(f"  {k:<42} {v}")
    pause()

# ── webhook testing menu ───────────────────────────────────────────────────────
_SAMPLE_PAYLOAD = {
    "event_type": "comment.created",
    "token": "",
    "data": {
        "id": "test-admin-001",
        "content": "Admin console test comment",
        "created_at": 0,
        "creator": {"name": "Admin Console"},
        "subject": {"type": "Project", "id": "00000000", "name": "Test Project"},
    },
}

def menu_webhook_testing():
    while True:
        section("Webhook & Integration Testing")
        opts = [
            ("1", "Send test comment to local webhook", "POST a sample CompanyCam comment payload locally"),
            ("2", "Enqueue a manual test item", "Insert a test queue row for UI poster processing"),
            ("3", "Verify CompanyCam webhook registration", "Validate remote webhook endpoint configuration"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _send_test_webhook()
        elif choice == "2":
            _enqueue_test_item()
        elif choice == "3":
            _verify_cc_webhook()
        elif choice == "b":
            return


def menu_missed_comment_catchup():
    section("Missed Comment Catch-up")
    script_path = SCRIPTS_DIR / "recover_missed_comments.py"
    if not script_path.exists():
        print(red(f"  Missing script: {script_path}"))
        pause()
        return

    print(dim("  This will run the interactive recovery flow for missed comments."))
    print(dim("  It can backfill audit history and requeue missed CompanyCam comments."))
    confirm = input("\n  Launch recovery script now? (y/N): ").strip().lower()
    if confirm != "y":
        return

    print()
    try:
        # Run interactively so prompts/selections are handled directly by the operator.
        # Prefer the project venv interpreter (has all deps); fall back to sys.executable.
        _venv_python = ROOT / ".venv" / "bin" / "python3"
        _python = str(_venv_python) if _venv_python.exists() else sys.executable
        # Also set PYTHONPATH so src/ and root modules resolve correctly from ROOT.
        env = os.environ.copy()
        pythonpath_parts = [str(SRC_DIR), str(ROOT), str(SCRIPTS_DIR)]
        existing = env.get("PYTHONPATH", "")
        if existing:
            pythonpath_parts.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        print(dim(f"  Runtime interpreter: {_python}"))
        print(dim(f"  Runtime PYTHONPATH: {env.get('PYTHONPATH', '')}"))
        code = subprocess.run(
            [_python, str(script_path)],
            cwd=str(ROOT),
            env=env,
            check=False,
        ).returncode
        print()
        if code == 0:
            print(green("  ✓ Missed comment recovery completed."))
        else:
            print(yellow(f"  Recovery script exited with status {code}."))
    except Exception as e:
        print(red(f"  Failed to run recovery script: {e}"))
    pause()


def menu_recover_missed_mentions():
    """Run the tagger @mention catch-up script over recent MarketSharp notes."""
    section("Tagger Mention Recovery (MarketSharp Notes)")
    script_path = SCRIPTS_DIR / "recover_missed_mentions.py"
    if not script_path.exists():
        print(red(f"  Missing script: {script_path}"))
        pause()
        return

    print(dim("  Re-runs the tagger @mention pipeline over recent MarketSharp notes."))
    print(dim("  Dry-run is the default — emails are only sent if you choose 'apply'."))
    print(dim("  Uses an isolated state file so the live worker is not disturbed."))
    print()

    raw_hours = input("  Lookback window in hours [24]: ").strip() or "24"
    try:
        hours = float(raw_hours)
        if hours <= 0:
            raise ValueError
    except ValueError:
        print(yellow("  Invalid hours value; aborting."))
        pause()
        return

    apply_choice = input("  Send emails? Type 'apply' to actually send (default dry-run): ").strip().lower()
    apply_flag = apply_choice == "apply"

    cmd_args = ["--hours", str(hours)]
    if apply_flag:
        cmd_args.append("--apply")

    # Tagger has its own venv. Fall back to main venv, then sys.executable.
    _tagger_python = ROOT / "tagger" / ".venv" / "bin" / "python"
    _main_python = ROOT / ".venv" / "bin" / "python3"
    if _tagger_python.exists():
        _python = str(_tagger_python)
    elif _main_python.exists():
        _python = str(_main_python)
    else:
        _python = sys.executable

    env = os.environ.copy()
    pythonpath_parts = [str(ROOT), str(ROOT / "src"), str(ROOT / "tagger"), str(SCRIPTS_DIR)]
    existing = env.get("PYTHONPATH", "")
    if existing:
        pythonpath_parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    print()
    print(dim(f"  Interpreter: {_python}"))
    print(dim(f"  Mode: {'APPLY (will send emails)' if apply_flag else 'DRY-RUN (preview only)'}"))
    print()

    try:
        code = subprocess.run(
            [_python, str(script_path), *cmd_args],
            cwd=str(ROOT),
            env=env,
            check=False,
        ).returncode
        print()
        if code == 0:
            print(green("  ✓ Tagger mention recovery completed."))
        else:
            print(yellow(f"  Recovery script exited with status {code}."))
    except Exception as e:
        print(red(f"  Failed to run mention recovery: {e}"))
    pause()


def _send_test_webhook():
    section("Send Test Webhook")
    if not _HAS_REQUESTS:
        print(yellow("  Install 'requests': pip install requests"))
        pause()
        return
    name = input("  Customer name [Test Customer]: ").strip() or "Test Customer"
    comment = input("  Comment text [Admin console test]: ").strip() or "Admin console test"
    payload = json.loads(json.dumps(_SAMPLE_PAYLOAD))
    payload["data"]["content"] = comment
    payload["data"]["id"] = f"test-admin-{int(time.time())}"
    payload["data"]["subject"]["name"] = name
    payload["token"] = os.getenv("COMPANYCAM_WEBHOOK_SECRET", "")
    url = WEBHOOK_LOCAL_URL
    print(f"\n  POST {url}")
    try:
        resp = _requests.post(url, json=payload, timeout=10)
        col = green if resp.status_code == 200 else yellow
        print(f"  {col(str(resp.status_code))} {resp.text[:200]}")
    except Exception as e:
        print(red(f"  Request failed: {e}"))
    pause()

def _enqueue_test_item():
    section("Enqueue Manual Test Item")
    name = input("  Customer name: ").strip()
    if not name:
        print(yellow("  Name required."))
        pause()
        return
    comment = input("  Comment text: ").strip() or "Manually queued by admin console"
    now = int(time.time())
    event_id = f"manual-admin-{now}"
    with db_connect() as conn:
        try:
            conn.execute(
                """INSERT INTO pending_comments
                   (event_id, customer_name, comment_text, author_name, payload_json,
                    status, retry_count, last_error, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (event_id, name, comment, "Admin Console", "{}", "pending", 0, None, now, now),
            )
            conn.commit()
            print(green(f"  ✓ Enqueued event_id={event_id}"))
        except sqlite3.IntegrityError:
            print(yellow("  Duplicate event_id — already in queue."))
    pause()

def _verify_cc_webhook():
    section("CompanyCam Webhook Registration")
    if not _HAS_REQUESTS:
        print(yellow("  Install 'requests': pip install requests"))
        pause()
        return
    token = os.getenv("COMPANYCAM_WEBHOOK_TOKEN", "")
    if not token:
        print(red("  COMPANYCAM_WEBHOOK_TOKEN not set."))
        pause()
        return
    try:
        resp = _requests.get(
            "https://api.companycam.com/v2/webhooks",
            headers={"accept": "application/json", "authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code != 200:
            print(yellow(f"  API returned {resp.status_code}: {resp.text[:200]}"))
        else:
            body = resp.json()
            hooks = body if isinstance(body, list) else body.get("webhooks", [])
            if not hooks:
                print(yellow("  No webhooks registered."))
            for h in hooks:
                status = green("enabled") if h.get("enabled") else red("disabled")
                print(f"  [{status}] {h.get('url', '')}  scopes={h.get('scopes', [])}")
    except Exception as e:
        print(red(f"  Request failed: {e}"))
    pause()

# ── GCLID / conversion report menu ────────────────────────────────────────────
def menu_gclid_report():
    """GCLID & Google Ads Conversion Report sub-menu."""
    import importlib
    import importlib.util
    import sys as _sys
    import subprocess as _sp
    from datetime import datetime as _dt, timedelta as _td

    # Lazy-load gclid-ms module (file is named with a dash — use importlib)
    _gclid_mod = None
    _gclid_dir = os.path.join(os.path.dirname(__file__), "gclid")

    def _run_py_script(args, cwd=None):
        try:
            result = _sp.run(
                [_sys.executable] + args,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=3600,
            )
            return result.returncode, result.stdout, result.stderr
        except Exception as e:
            return 1, "", str(e)

    def _human_size(num_bytes: int) -> str:
        size = float(max(0, num_bytes))
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024.0 or unit == "GB":
                return f"{size:.1f}{unit}"
            size /= 1024.0
        return f"{num_bytes}B"

    def _discover_csv_files() -> list[dict]:
        out_dir = os.getenv("GCLID_REPORT_OUT_DIR", os.path.join(os.path.dirname(__file__), "data"))
        roots = [out_dir, _gclid_dir]
        files: list[dict] = []
        seen: set[str] = set()

        for root in roots:
            if not root or not os.path.isdir(root):
                continue
            try:
                names = os.listdir(root)
            except Exception:
                continue
            for name in names:
                if not name.lower().endswith(".csv"):
                    continue
                path = os.path.join(root, name)
                if path in seen or not os.path.isfile(path):
                    continue
                seen.add(path)
                try:
                    st = os.stat(path)
                    files.append({
                        "path": path,
                        "name": name,
                        "mtime": st.st_mtime,
                        "size": st.st_size,
                    })
                except Exception:
                    continue

        files.sort(key=lambda f: f.get("mtime", 0), reverse=True)
        return files

    def _preview_csv(path: str, max_lines: int = 50):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for idx, line in enumerate(f, start=1):
                    if idx > max_lines:
                        print(dim(f"... truncated at {max_lines} lines ..."))
                        break
                    print(line.rstrip("\n"))
        except Exception as e:
            print(red(f"  Failed to read CSV: {e}"))

    def _open_csv_external(path: str):
        import shutil as _shutil

        try:
            if _sys.platform == "darwin":
                _sp.run(["open", path], check=False)
                print(green("  Opened with system default app."))
                return
            opener = _shutil.which("xdg-open")
            if opener:
                _sp.run([opener, path], check=False)
                print(green("  Opened with system default app."))
                return
            print(yellow("  No GUI opener available. Use the full path shown below."))
        except Exception as e:
            print(red(f"  Open failed: {e}"))

    def _csv_file_manager():
        while True:
            section("CSV File Manager")
            files = _discover_csv_files()
            if not files:
                print(yellow("  No CSV files found in gclid/data output locations."))
                pause()
                return

            print("  Generated CSV files (newest first):")
            for i, f in enumerate(files, start=1):
                ts = _dt.fromtimestamp(f["mtime"]).strftime("%Y-%m-%d %H:%M")
                print(f"   {i:>2}. {f['name']}  {dim(ts)}  {dim(_human_size(f['size']))}")

            render_menu_options([
                ("v", "View CSV in terminal", "Preview file content in this terminal session"),
                ("o", "Open CSV with system app", "Launch file with OS default opener"),
                ("d", "Delete CSV(s) by number", "Remove one or more files by index"),
                ("p", "Purge old CSVs (keep newest N)", "Delete older files while retaining newest N"),
                ("b", "Back", "Return to GCLID report menu"),
            ])
            act = input("\n  Choice: ").strip().lower()

            if act == "b":
                return
            if act == "v":
                raw = input("  File number to view: ").strip()
                if not raw.isdigit() or int(raw) < 1 or int(raw) > len(files):
                    print(yellow("  Invalid selection."))
                    continue
                n_raw = input("  Max lines [50]: ").strip() or "50"
                max_lines = 50
                try:
                    max_lines = max(1, int(n_raw))
                except ValueError:
                    pass
                print()
                _preview_csv(files[int(raw) - 1]["path"], max_lines=max_lines)
                print()
                pause()
                continue
            if act == "o":
                raw = input("  File number to open: ").strip()
                if not raw.isdigit() or int(raw) < 1 or int(raw) > len(files):
                    print(yellow("  Invalid selection."))
                    continue
                path = files[int(raw) - 1]["path"]
                print(f"  Path: {bold(path)}")
                _open_csv_external(path)
                pause()
                continue
            if act == "d":
                raw = input("  File numbers to delete (comma-separated): ").strip()
                idxs = []
                for part in raw.split(","):
                    part = part.strip()
                    if part.isdigit():
                        n = int(part)
                        if 1 <= n <= len(files):
                            idxs.append(n)
                idxs = sorted(set(idxs), reverse=True)
                if not idxs:
                    print(yellow("  No valid file numbers provided."))
                    continue
                confirm = input(f"  Delete {len(idxs)} file(s)? [y/N] ").strip().lower()
                if confirm != "y":
                    continue
                deleted = 0
                for n in idxs:
                    path = files[n - 1]["path"]
                    try:
                        os.remove(path)
                        deleted += 1
                    except Exception as e:
                        print(yellow(f"  Could not delete {path}: {e}"))
                print(green(f"  Deleted {deleted} file(s)."))
                continue
            if act == "p":
                keep_raw = input("  Keep newest how many CSVs? [10]: ").strip() or "10"
                try:
                    keep_n = max(0, int(keep_raw))
                except ValueError:
                    print(yellow("  Invalid number."))
                    continue
                to_delete = files[keep_n:]
                if not to_delete:
                    print(dim("  Nothing to purge."))
                    continue
                confirm = input(f"  Purge {len(to_delete)} older CSV(s)? [y/N] ").strip().lower()
                if confirm != "y":
                    continue
                deleted = 0
                for f in to_delete:
                    try:
                        os.remove(f["path"])
                        deleted += 1
                    except Exception as e:
                        print(yellow(f"  Could not delete {f['path']}: {e}"))
                print(green(f"  Purged {deleted} CSV file(s)."))
                continue

            print(yellow("  Invalid choice."))


    def _run_eligibility_audit_and_summary():
        section("Eligibility Audit & Executive Summary")
        month = input("  Month (YYYY-MM) [2025-02]: ").strip() or "2025-02"
        limit_raw = input("  Limit contacts (0 = all) [0]: ").strip() or "0"
        try:
            limit = int(limit_raw)
        except ValueError:
            print(red("  Invalid limit; using 0."))
            limit = 0

        audit_name = f"eligibility_audit_{month}.csv"
        summary_name = f"eligibility_summary_{month}.txt"

        audit_script = os.path.join(_gclid_dir, "audit_contact_eligibility.py")
        summary_script = os.path.join(_gclid_dir, "summarize_eligibility_audit.py")

        if not os.path.exists(audit_script):
            print(red(f"  Missing script: {audit_script}"))
            pause()
            return
        if not os.path.exists(summary_script):
            print(red(f"  Missing script: {summary_script}"))
            pause()
            return

        print(dim("\n  Running eligibility audit..."))
        audit_args = [
            audit_script,
            "--month", month,
            "--out", audit_name,
            "--limit", str(max(0, limit)),
        ]
        code, out, err = _run_py_script(audit_args, cwd=_gclid_dir)
        if out:
            print(out)
        if code != 0:
            print(red("  Audit failed."))
            if err:
                print(dim(err.strip()[:1200]))
            pause()
            return

        print(dim("\n  Building executive summary..."))
        summary_args = [
            summary_script,
            "--audit-csv", audit_name,
            "--month", month,
            "--out", summary_name,
        ]
        code, out, err = _run_py_script(summary_args, cwd=_gclid_dir)
        if out:
            print(out)
        if code != 0:
            print(red("  Summary failed."))
            if err:
                print(dim(err.strip()[:1200]))
            pause()
            return

        print(green("  Audit + summary complete."))
        print(f"  Audit CSV: {bold(os.path.join(_gclid_dir, audit_name))}")
        print(f"  Summary:   {bold(os.path.join(_gclid_dir, summary_name))}")
        pause()

    def _run_contact_roster_reconciliation():
        """Backend-only reconciliation: all matched IDs vs exportable IDs for month."""
        import csv

        section("Backend Contact Roster Reconciliation")
        month = input("  Month (YYYY-MM) [2025-02]: ").strip() or "2025-02"
        limit_raw = input("  Limit contacts (0 = all) [0]: ").strip() or "0"
        try:
            limit = int(limit_raw)
        except ValueError:
            print(red("  Invalid limit; using 0."))
            limit = 0

        audit_name = f"eligibility_audit_{month}.csv"
        all_ids_name = f"gclid_matched_contacts_{month}.csv"
        exporting_ids_name = f"gclid_exporting_contacts_{month}.csv"

        audit_script = os.path.join(_gclid_dir, "audit_contact_eligibility.py")
        if not os.path.exists(audit_script):
            print(red(f"  Missing script: {audit_script}"))
            pause()
            return

        print(dim("\n  Running backend eligibility audit (source-of-truth contact set)..."))
        audit_args = [
            audit_script,
            "--month", month,
            "--out", audit_name,
            "--limit", str(max(0, limit)),
        ]
        code, out, err = _run_py_script(audit_args, cwd=_gclid_dir)
        if out:
            print(out)
        if code != 0:
            print(red("  Reconciliation failed during audit."))
            if err:
                print(dim(err.strip()[:1200]))
            pause()
            return

        audit_path = os.path.join(_gclid_dir, audit_name)
        all_ids_path = os.path.join(_gclid_dir, all_ids_name)
        exporting_ids_path = os.path.join(_gclid_dir, exporting_ids_name)

        all_rows = []
        exporting_rows = []
        try:
            with open(audit_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = (row.get("contact_id") or "").strip()
                    if not cid:
                        continue
                    all_rows.append({
                        "contact_id": cid,
                        "gclid": row.get("gclid", ""),
                        "would_export": row.get("would_export", "N"),
                        "exclusion_reason": row.get("exclusion_reason", ""),
                    })
                    if (row.get("would_export") or "").upper() == "Y":
                        exporting_rows.append({
                            "contact_id": cid,
                            "gclid": row.get("gclid", ""),
                        })
        except Exception as e:
            print(red(f"  Failed to read audit CSV: {e}"))
            pause()
            return

        try:
            with open(all_ids_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["contact_id", "gclid", "would_export", "exclusion_reason"])
                w.writeheader()
                w.writerows(all_rows)
            with open(exporting_ids_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["contact_id", "gclid"])
                w.writeheader()
                w.writerows(exporting_rows)
        except Exception as e:
            print(red(f"  Failed to write reconciliation files: {e}"))
            pause()
            return

        print(green("  Reconciliation complete."))
        print(f"  Matched contacts (backend, parseable GCLID): {bold(str(len(all_rows)))}")
        print(f"  Exporting contacts (month window):         {bold(str(len(exporting_rows)))}")
        print(f"  All matched IDs: {bold(all_ids_path)}")
        print(f"  Exporting IDs:   {bold(exporting_ids_path)}")
        pause()

    def _run_backend_source_roster_export(mod):
        """Export full backend source-of-truth roster (no month filtering)."""
        import csv

        section("Backend Source Roster Export")
        stamp = _dt.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"gclid_backend_source_contacts_{stamp}.csv"
        out_path = os.path.join(_gclid_dir, out_name)

        print(dim("\n  Fetching all backend contacts with parseable GCLID..."))
        try:
            rb = mod.ReportBuilder()
            contacts = rb.contacts_with_gclid(contact_ids=None)
        except Exception as e:
            print(red(f"  Source export failed: {e}"))
            pause()
            return

        rows = []
        for c in contacts:
            if not isinstance(c, dict):
                continue
            fields = c.get("fields", {}) or {}
            rows.append({
                "contact_id": c.get("contact_id", ""),
                "gclid": fields.get("gclid", ""),
                "utm_source": fields.get("utm_source", ""),
                "utm_medium": fields.get("utm_medium", ""),
                "utm_campaign": fields.get("utm_campaign", ""),
                "utm_term": fields.get("utm_term", ""),
                "utm_content": fields.get("utm_content", ""),
                "note_date": c.get("note_date", ""),
            })

        try:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=[
                        "contact_id",
                        "gclid",
                        "utm_source",
                        "utm_medium",
                        "utm_campaign",
                        "utm_term",
                        "utm_content",
                        "note_date",
                    ],
                )
                w.writeheader()
                w.writerows(rows)
        except Exception as e:
            print(red(f"  Failed to write source roster: {e}"))
            pause()
            return

        print(green("  Backend source roster export complete."))
        print(f"  Source-of-truth matched contacts: {bold(str(len(rows)))}")
        print(f"  Output CSV: {bold(out_path)}")
        pause()

    def _load_gclid():
        nonlocal _gclid_mod
        if _gclid_mod:
            return _gclid_mod
        spec = importlib.util.spec_from_file_location(
            "gclid_ms",
            os.path.join(os.path.dirname(__file__), "gclid", "gclid_sync.py"),
        )
        if spec is None or spec.loader is None:
            # fallback: root-level gclid-ms.py
            spec = importlib.util.spec_from_file_location(
                "gclid_ms",
                os.path.join(os.path.dirname(__file__), "gclid-ms.py"),
            )
        if spec is None or spec.loader is None:
            print(red("  gclid-ms.py not found — cannot run report"))
            return None
        mod = importlib.util.module_from_spec(spec)
        try:
            loader = spec.loader
            loader.exec_module(mod)
        except Exception as e:
            print(red(f"  Failed to load gclid module: {e}"))
            return None
        _gclid_mod = mod
        return mod

    while True:
        section("GCLID & Google Ads Conversion Report")
        print("  Pulls all MarketSharp contacts whose notes contain a GCLID")
        print("  value (from website lead email triggers) and exports a CSV")
        print("  formatted for Google Ads offline conversion upload.\n")
        opts = [
            ("1", "Run report — this month", "Build and export conversion report for current month"),
            ("2", "Run report — last month", "Build and export conversion report for previous month"),
            ("3", "Run report — custom date range", "Build report from a custom start date"),
            ("4", "Preview report in terminal (no file written)", "Generate rows and print preview only"),
            ("5", "Show last exported CSV", "List most recent conversion export files"),
            ("6", "Run eligibility audit + executive summary", "Create audit and summary artifacts for a month"),
            ("7", "Backend contact roster reconciliation", "Compare exportable contacts vs backend source roster"),
            ("8", "Export backend source roster (all matched contacts)", "Export full backend matched-contact roster CSV"),
            ("9", "CSV file manager (open/view/delete/purge)", "Inspect and manage generated CSV files"),
            ("b", "Back", "Return to the previous menu"),
        ]
        render_menu_options(opts, key_style=bold)
        choice = input("\n  Choice: ").strip().lower()

        if choice == "b":
            return

        mod = _load_gclid()
        if mod is None:
            pause()
            continue

        # Build date ranges
        now = _dt.now()
        if choice == "1":
            since = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            until = None
            label = f"{now.strftime('%B %Y')}"
            file_label = now.strftime('%Y%m')
        elif choice == "2":
            first_this = now.replace(day=1)
            last_month = first_this - _td(days=1)
            since = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            until = first_this.replace(hour=0, minute=0, second=0, microsecond=0)
            label = f"{last_month.strftime('%B %Y')}"
            file_label = last_month.strftime('%Y%m')
        elif choice == "3":
            raw = input("  Start date (YYYY-MM-DD): ").strip()
            try:
                since = _dt.strptime(raw, "%Y-%m-%d")
                label = f"since {raw}"
                file_label = since.strftime('%Y%m')
                until = None
            except ValueError:
                print(red("  Invalid date format."))
                pause()
                continue
        elif choice == "4":
            # Preview only
            print()
            try:
                rb = mod.ReportBuilder()
                rows = rb.build_conversion_rows()
                if not rows:
                    print(yellow("  No GCLID contacts found."))
                else:
                    mod.CSVExporter().preview(rows)
            except Exception as e:
                print(red(f"  Preview failed: {e}"))
            pause()
            continue
        elif choice == "5":
            out_dir = os.getenv("GCLID_REPORT_OUT_DIR", os.path.join(os.path.dirname(__file__), "data"))
            csvs = sorted(
                (f for f in os.listdir(out_dir) if f.startswith("spicer_conversions") and f.endswith(".csv")),
                reverse=True,
            ) if os.path.isdir(out_dir) else []
            if csvs:
                print(f"\n  Latest: {bold(os.path.join(out_dir, csvs[0]))}")
                for f in csvs[:5]:
                    print(f"    {f}")
            else:
                print(yellow("  No exported CSVs found."))
            pause()
            continue
        elif choice == "6":
            _run_eligibility_audit_and_summary()
            continue
        elif choice == "7":
            _run_contact_roster_reconciliation()
            continue
        elif choice == "8":
            _run_backend_source_roster_export(mod)
            continue
        elif choice == "9":
            _csv_file_manager()
            continue
        else:
            continue

        # Run the export
        out_dir = os.getenv("GCLID_REPORT_OUT_DIR", os.path.join(os.path.dirname(__file__), "data"))
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"spicer_conversions_{file_label}.csv")

        print(f"\n  Building report for {bold(label)} …")
        try:
            rb = mod.ReportBuilder()
            rows = rb.build_conversion_rows(since=since, until=until)
            if not rows:
                print(yellow("  No GCLID contacts or conversions found for that range."))
            else:
                written = mod.CSVExporter().export(rows, out_path)
                print(green(f"  Exported {written} rows → {out_path}"))

                # Verify that all exportable conversion rows were written to disk.
                data_lines = 0
                try:
                    with open(out_path, newline="", encoding="utf-8") as f:
                        for i, _ in enumerate(f):
                            # line 1 = Parameters row, line 2 = header row
                            if i >= 2:
                                data_lines += 1
                except Exception as ve:
                    print(yellow(f"  Could not verify CSV row count: {ve}"))
                else:
                    print(f"  Row check: built={len(rows)}  written={written}  csv_data_lines={data_lines}")
                    if data_lines != written:
                        print(yellow("  Warning: on-disk CSV line count does not match written row count."))

                # Offer a quick preview
                if input("  Preview rows in terminal? [y/N] ").strip().lower() == "y":
                    mod.CSVExporter().preview(rows)
        except Exception as e:
            print(red(f"  Report failed: {e}"))
        pause()


# ── category menus ─────────────────────────────────────────────────────────────
def _run_predeploy_checks_from_menu():
    section("Predeploy DB Checks")
    code = print_predeploy_checks_noninteractive()
    if code == 0:
        print(green("\n  ✓ Predeploy checks passed."))
    else:
        print(red(f"\n  ✗ Predeploy checks failed (exit {code})."))
    pause()


def _show_db_status_from_menu():
    section("DB Status Summary")
    print_db_status_noninteractive()
    pause()


def menu_companycam_api():
    while True:
        section("CompanyCam API")
        opts = [
            ("1", "Webhook & Integration Tests", "Send test payloads and verify CompanyCam webhook setup"),
            ("2", "Missed Comment Catch-up", "Run recovery flow for missed webhook comments"),
            ("3", "Verify Webhook Registration", "Check current CompanyCam webhook registration status"),
            ("b", "Back", "Return to the main category menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            menu_webhook_testing()
        elif choice == "2":
            menu_missed_comment_catchup()
        elif choice == "3":
            _verify_cc_webhook()
        elif choice == "b":
            return


def menu_marketsharp_tagging_api():
    while True:
        counts = queue_counts()
        active_n = counts.get("pending", 0) + counts.get("processing", 0)
        unmatched_n = counts.get("unmatched", 0)
        true_fail_n = counts.get("true_fail", 0)
        posted_n = counts.get("posted", 0)

        section("MarketSharp Tagging API")
        print(f"  Queue snapshot: active={cyan(str(active_n))}  unmatched={yellow(str(unmatched_n))}  true_fail={red(str(true_fail_n))}  posted={green(str(posted_n))}")
        print()
        opts = [
            ("1", "Queue Status", "View queue totals by processing state"),
            ("2", "Browse & Manage Queue", "Inspect queue items and requeue/edit/delete"),
            ("3", "Requeue All Unmatched", "Bulk requeue unmatched items"),
            ("4", "Review True-Fail Items", "Requeue or rename permanently failed rows"),
            ("5", "Re-push Posted Comments", "Replay posted comments through queue worker"),
            ("6", "Contact Mapping", "Manage project/contact URL mapping overrides"),
            ("7", "Duplicate Check", "Scan queue DB for duplicate event IDs and texts"),
            ("8", "Tagger Mention Recovery", "Replay tagger @mention emails for missed MarketSharp notes (dry-run by default)"),
            ("b", "Back", "Return to the main category menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            menu_queue_status(counts)
        elif choice == "2":
            menu_browse_queue(counts)
        elif choice == "3":
            menu_requeue_all_unmatched()
        elif choice == "4":
            menu_requeue_true_fails()
        elif choice == "5":
            menu_requeue_posted()
        elif choice == "6":
            menu_contact_mapping()
        elif choice == "7":
            menu_check_duplicates()
        elif choice == "8":
            menu_recover_missed_mentions()
        elif choice == "b":
            return


def menu_google_click_ad_reporting():
    # The detailed reporting submenu lives in menu_gclid_report(); this wrapper
    # previously exposed three options that all routed to the same submenu,
    # which created confusing circular delegation. Call it directly instead.
    menu_gclid_report()


def menu_database_category():
    while True:
        section("Database Administration")
        opts = [
            ("1", "Database Administration", "Backup, integrity checks, and queue DB consolidation"),
            ("2", "Audit Log", "Inspect and export posted-comment audit history"),
            ("3", "DB Status Summary", "Print active DB paths, integrity, and queue counts"),
            ("b", "Back", "Return to the main category menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            menu_database_admin()
        elif choice == "2":
            menu_audit_log()
        elif choice == "3":
            _show_db_status_from_menu()
        elif choice == "b":
            return


def menu_system_maintenance():
    while True:
        section("System Maintenance")
        opts = [
            ("1", "Diagnostics & Services", "Service restart controls, journal, health, env, mention-worker check"),
            ("2", "Predeploy DB Checks", "Run non-interactive DB preflight checks"),
            ("b", "Back", "Return to the main category menu"),
        ]
        render_menu_options(opts)
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            menu_diagnostics()
        elif choice == "2":
            _run_predeploy_checks_from_menu()
        elif choice == "b":
            return


CATEGORY_ITEMS = [
    ("1", "CompanyCam API", "Webhook registration, local webhook tests, and missed-comment recovery", menu_companycam_api),
    ("2", "MarketSharp Tagging API", "Queue operations, mention/tagging flows, and mapping controls", menu_marketsharp_tagging_api),
    ("3", "Google Click Ad Reporting", "GCLID conversion exports and reporting audits", menu_google_click_ad_reporting),
    ("4", "Database Administration", "DB backup, integrity, consolidation, and audit access", menu_database_category),
    ("5", "System Maintenance", "Service diagnostics, health checks, and predeploy validation", menu_system_maintenance),
    ("q", "Quit", "Exit the admin console", None),
]


def main_menu():
    while True:
        counts = queue_counts()
        print_splash(counts)
        preflight_warning = queue_db_preflight_warning()
        if preflight_warning:
            print(red(f"  ⚠ {preflight_warning}"))
            print()

        section("Main Menu — Functional Categories")
        render_menu_options(CATEGORY_ITEMS)
        print()

        choice = input("  Select: ").strip().lower()
        for key, _label, _desc, fn in CATEGORY_ITEMS:
            if choice == key:
                if fn is None:
                    print(green("\n  Program closing ...\n"))
                    return
                fn()
                break
        else:
            print(yellow("  Invalid selection."))
            time.sleep(0.5)

def main():
    parser = argparse.ArgumentParser(description="Spicer API Admin Console")
    parser.add_argument("--status", action="store_true", help="Print queue counts and exit")
    parser.add_argument("--db-status", action="store_true", help="Print DB path and integrity summary")
    parser.add_argument("--predeploy-check", action="store_true", help="Run DB predeploy checks and exit non-zero on errors")
    args = parser.parse_args()

    if args.status:
        counts = queue_counts()
        total = sum(counts.values())
        for s in ("pending", "processing", "unmatched", "true_fail", "posted"):
            print(f"{s:<15} {counts.get(s, 0)}")
        print(f"{'TOTAL':<15} {total}")
        return

    if args.db_status:
        print_db_status_noninteractive()
        return

    if args.predeploy_check:
        raise SystemExit(print_predeploy_checks_noninteractive())

    main_menu()

if __name__ == "__main__":
    main()

