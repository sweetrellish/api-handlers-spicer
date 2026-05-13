"""
Audit log for all posted comments: records every comment posted to MarketSharp for permanent history.
This module provides a function to log posted comments and a CLI to print the audit log.
"""

import os
import sqlite3
import sys
from datetime import datetime

AUDIT_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'posted_comments_audit.db')

flags = {
    '-r': 'print in reverse chronological order (newest first)',
    '--csv': 'export the audit log to a CSV file instead of printing'
}


def ensure_audit_table():
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS posted_comments_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            customer_id TEXT,
            customer_name TEXT,
            author_name TEXT,
            comment_text TEXT,
            posted_at INTEGER,
            posted_at_iso TEXT,
            extra_json TEXT
        )
    ''')
    conn.commit()
    conn.close()

def log_posted_comment(event_id, customer_id, customer_name, author_name, comment_text, extra_json=None):
    ensure_audit_table()
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    now = int(datetime.utcnow().timestamp())
    now_iso = datetime.utcnow().isoformat()
    cur.execute('''
        INSERT INTO posted_comments_audit (event_id, customer_id, customer_name, author_name, comment_text, posted_at, posted_at_iso, extra_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (event_id, customer_id, customer_name, author_name, comment_text, now, now_iso, extra_json or ''))
    conn.commit()
    conn.close()
    print(f"[AUDIT] Logged posted comment: event_id={event_id}, customer_id={customer_id}, author={author_name}, at={now_iso}")

def print_audit_log():
    if flags.get('-r'):
        order = 'DESC'
    else:        
        order = 'ASC'
    ensure_audit_table()
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute(f'SELECT id, event_id, customer_id, customer_name, author_name, posted_at_iso, comment_text FROM posted_comments_audit ORDER BY posted_at {order}')
    rows = cur.fetchall()
    print("\n=== Posted Comments Audit Log ===")
    for row in rows:
        print(f"ID: {row[0]} | Event: {row[1]} | Customer: {row[2]} ({row[3]}) | Author: {row[4]} | Time: {row[5]}\n  Comment: {row[6][:120]}{'...' if len(row[6]) > 120 else ''}\n")
    print(f"Total: {len(rows)} posted comments in audit log.")
    conn.close()

def export_audit_log_csv(csv_path=None):
    """Export the audit log to a CSV file for reporting or backup."""
    import csv
    ensure_audit_table()
    conn = sqlite3.connect(AUDIT_DB)
    cur = conn.cursor()
    cur.execute('SELECT id, event_id, customer_id, customer_name, author_name, posted_at_iso, comment_text, extra_json FROM posted_comments_audit ORDER BY posted_at DESC')
    rows = cur.fetchall()
    if not csv_path:
        csv_path = os.path.join(os.path.dirname(AUDIT_DB), 'posted_comments_audit.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'event_id', 'customer_id', 'customer_name', 'author_name', 'posted_at_iso', 'comment_text', 'extra_json'])
        for row in rows:
            writer.writerow(row)
    print(f"Exported {len(rows)} rows to {csv_path}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--csv':
        export_audit_log_csv()
    else:
        print_audit_log()

