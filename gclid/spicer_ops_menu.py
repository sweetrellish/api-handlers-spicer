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
import subprocess
import sys
import time
from pathlib import Path
from unittest import loader

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
# Load environment variables from .env if available, but don't require it
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass
# Requests is used for webhook testing but not required for other menu functions
try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

# ── runtime config ─────────────────────────────────────────────────────────────
# Allow overriding DB paths and other settings via environment variables
DB_PATH = os.path.abspath(
    os.getenv("PENDING_QUEUE_DB_PATH", str(ROOT / "pending_comments.db"))
)
# Audit DB is separate from the main queue DB to allow it to be more persistent and less volatile.
AUDIT_DB = os.path.abspath(
    os.getenv("AUDIT_DB_PATH", str(ROOT / "posted_comments_audit.db"))
)
# Contact mapping file for manual URL overrides (project ID or customer name → MarketSharp contact URL)
MAPPING_FILE = os.path.abspath(
    os.getenv("MARKETSHARP_UI_CONTACT_URL_MAP_FILE", str(ROOT / "marketsharp_contact_mappings.json"))
)
# Local URL for testing the webhook receiver (must match the URL configured in CompanyCam)
WEBHOOK_LOCAL_URL = f"http://127.0.0.1:{os.getenv('FLASK_PORT', '5001')}/webhook/companycam"
HEALTH_URL = f"http://127.0.0.1:{os.getenv('FLASK_PORT', '5001')}/health"

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
Spicer Bros. Admin Console
"""

# ── splash and status display ─────────────────────────────────────────────────
# Display the splash screen and optionally show queue counts in a status line. This is called at the start of the menu and after certain actions to refresh the display.
def print_splash(counts=None):
    clear()
    print(dim(red(SPLASH)))
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
        conn.execute(
            "UPDATE pending_comments SET customer_name=?, status='pending', retry_count=0, updated_at=? WHERE id=?",
            (new_name, now, item_id),
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
        print(dim("  Enter item number to inspect, or [b] back: "))
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
        print(bold("  Actions: ") + "[r]equeue  [e]dit name  [d]elete  [s]kip/back")
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
    raw = input("  Enter IDs to requeue (comma-separated), [a]ll, or [b]ack: ").strip().lower()
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
                "UPDATE pending_comments SET status='pending', retry_count=0, updated_at=? WHERE id=?",
                (now, iid),
            )
        conn.commit()
    print(green(f"  ✓ {len(ids)} item(s) requeued."))
    pause()

def menu_requeue_posted():
    section("Re-push Posted Comments")
    with db_connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM pending_comments WHERE status='posted'").fetchone()[0]
    if not count:
        print(dim("  No posted items."))
        pause()
        return
    confirm = input(f"  Requeue {yellow(str(count))} posted item(s) for re-push? (y/N): ").strip().lower()
    if confirm == "y":
        now = int(time.time())
        with db_connect() as conn:
            conn.execute(
                "UPDATE pending_comments SET status='pending', retry_count=0, last_error='Manual re-push', updated_at=? WHERE status='posted'",
                (now,),
            )
            conn.commit()
        print(green(f"  ✓ {count} items requeued."))
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
    for row in dup_event:
        print(yellow(f"  Duplicate event_id ({row['n']}x): {row['event_id']}"))
    for row in dup_text:
        snippet = (row["comment_text"] or "")[:60]
        print(yellow(f"  Duplicate text ({row['n']}x): {snippet}…"))
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
            ("1", "List all mappings"),
            ("2", "Add / update a mapping"),
            ("3", "Delete a mapping"),
            ("4", "List unresolved queue items (no mapping)"),
            ("b", "Back"),
        ]
        for key, label in opts:
            print(f"  [{cyan(key)}] {label}")
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _list_mappings(mapping)
        elif choice == "2":
            _upsert_mapping(mapping)
        elif choice == "3":
            _delete_mapping(mapping)
        elif choice == "4":
            _list_unresolved(mapping)
        elif choice == "b":
            return

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
            ("1", "Show recent posted comments (last 50)"),
            ("2", "Search audit log by customer name"),
            ("3", "Export audit log to CSV"),
            ("4", "Check for posted items missing from audit"),
            ("b", "Back"),
        ]
        for key, label in opts:
            print(f"  [{cyan(key)}] {label}")
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
            ("1", "Restart queue workers"),
            ("2", "Restart all services"),
            ("3", "View worker journal (last 40 lines)"),
            ("4", "Check local health endpoint"),
            ("5", "Show env config summary"),
            ("b", "Back"),
        ]
        for key, label in opts:
            print(f"  [{cyan(key)}] {label}")
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
        elif choice == "b":
            return

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
    try:
        r = subprocess.run(
            ["journalctl", "-u", "marketsharp_queue_worker.service",
             "-u", "marketsharp_queue_worker_event.service",
             "--no-pager", "-n", "40", "--output=short"],
            capture_output=True, text=True, timeout=10,
        )
        output = r.stdout or r.stderr or dim("(no output)")
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
            ("1", "Send test comment to local webhook"),
            ("2", "Enqueue a manual test item"),
            ("3", "Verify CompanyCam webhook registration"),
            ("b", "Back"),
        ]
        for key, label in opts:
            print(f"  [{cyan(key)}] {label}")
        choice = input("\n  > ").strip().lower()
        if choice == "1":
            _send_test_webhook()
        elif choice == "2":
            _enqueue_test_item()
        elif choice == "3":
            _verify_cc_webhook()
        elif choice == "b":
            return

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
    import importlib, importlib.util, sys as _sys, subprocess as _sp, calendar as _cal
    from datetime import datetime as _dt, timedelta as _td

    # Lazy-load gclid-ms module (file is named with a dash — use importlib)
    _gclid_mod = None
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
        print(f"  Pulls all MarketSharp contacts whose notes contain a GCLID")
        print(f"  value (from website lead email triggers) and exports a CSV")
        print(f"  formatted for Google Ads offline conversion upload.\n")
        opts = [
            ("1", "Run report — this month"),
            ("2", "Run report — last month"),
            ("3", "Run report — custom date range"),
            ("4", "Preview report in terminal (no file written)"),
            ("5", "Show last exported CSV"),
            ("b", "Back"),
        ]
        for k, label in opts:
            print(f"  [{bold(k)}] {label}")
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


# ── main menu ──────────────────────────────────────────────────────────────────
MENU_ITEMS = [
    ("1", "Queue Status",               lambda c: menu_queue_status(c)),
    ("2", "Browse & Manage Queue",      lambda c: menu_browse_queue(c)),
    ("3", "Requeue All Unmatched",      lambda c: menu_requeue_all_unmatched()),
    ("4", "Review True-Fail Items",     lambda c: menu_requeue_true_fails()),
    ("5", "Re-push Posted Comments",    lambda c: menu_requeue_posted()),
    ("6", "Duplicate Check",            lambda c: menu_check_duplicates()),
    ("7", "Contact Mapping",            lambda c: menu_contact_mapping()),
    ("8", "Audit Log",                  lambda c: menu_audit_log()),
    ("9", "Diagnostics & Services",      lambda c: menu_diagnostics()),
    ("0", "Webhook & Integration Tests", lambda c: menu_webhook_testing()),
    ("g", "GCLID & Conversion Report",   lambda c: menu_gclid_report()),
    ("q", "Quit",                        None),
]

def main_menu():
    while True:
        counts = queue_counts()
        print_splash(counts)
        section("Main Menu — Spicer Bros. API Admin Console")
        for key, label, _ in MENU_ITEMS:
            badge = ""
            if key == "4":
                n = counts.get("true_fail", 0)
                badge = f"  {red('⚠ ' + str(n) + ' need review')}" if n else ""
            elif key == "3":
                n = counts.get("unmatched", 0)
                badge = f"  {yellow(str(n) + ' unmatched')}" if n else ""
            elif key == "2":
                n = counts.get("pending", 0) + counts.get("processing", 0)
                badge = f"  {cyan(str(n) + ' active')}" if n else ""
            print(f"  [{cyan(key)}] {label}{badge}")
        print()
        choice = input("  Select: ").strip().lower()
        for key, label, fn in MENU_ITEMS:
            if choice == key:
                if fn is None:
                    print(green("\n  Goodbye.\n"))
                    return
                fn(counts)
                break
        else:
            print(yellow("  Invalid selection."))
            time.sleep(0.5)

def main():
    parser = argparse.ArgumentParser(description="Spicer API Admin Console")
    parser.add_argument("--status", action="store_true", help="Print queue counts and exit")
    args = parser.parse_args()

    if args.status:
        counts = queue_counts()
        total = sum(counts.values())
        for s in ("pending", "processing", "unmatched", "true_fail", "posted"):
            print(f"{s:<15} {counts.get(s, 0)}")
        print(f"{'TOTAL':<15} {total}")
        return

    main_menu()

if __name__ == "__main__":
    main()

