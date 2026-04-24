"""
Check all found pending_comments.db files for posted comments.
Prints the status counts for each file.
"""

import sqlite3
import os

DB_PATHS = [
    '/home/rellis/pending_comments.db',
    '/home/rellis/spicer/pending_comments.db',
    '/home/rellis/spicer/scripts/pending_comments.db',
]

def check_db(path):
    if not os.path.exists(path):
        print(f"[MISSING] {path}")
        return
    try:
        conn = sqlite3.connect(path)
        cur = conn.cursor()
        cur.execute("SELECT status, COUNT(*) FROM pending_comments GROUP BY status;")
        rows = cur.fetchall()
        if rows:
            print(f"\n{path}:")
            for status, count in rows:
                print(f"  {status}: {count}")
        else:
            print(f"\n{path}: No rows found.")
        conn.close()
    except Exception as e:
        print(f"[ERROR] {path}: {e}")

def main():
    for db in DB_PATHS:
        check_db(db)

if __name__ == "__main__":
    main()

