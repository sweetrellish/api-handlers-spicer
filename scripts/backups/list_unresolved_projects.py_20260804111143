#!/usr/bin/env python3
"""Print unresolved CompanyCam project mappings from the pending queue."""

import json
import sys
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import Config
from src.mapping_registry import load_mapping_env, load_mapping_file, merge_contact_mappings
from src.queue_ui_poster import _extract_project_id_from_payload


def main():
    load_dotenv(override=True)

    mapping_file = REPO_ROOT / (
        __import__('os').getenv('MARKETSHARP_UI_CONTACT_URL_MAP_FILE', 'data/marketsharp_contact_mappings.json').strip()
    )
    file_mappings = load_mapping_file(mapping_file)
    env_mappings = load_mapping_env(__import__('os').getenv('MARKETSHARP_UI_CONTACT_URL_MAP', ''))
    contact_mappings = merge_contact_mappings(file_mappings, env_mappings)

    conn = sqlite3.connect(Config.PENDING_QUEUE_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, status, customer_name, comment_text, payload_json, updated_at, last_error
        FROM pending_comments
        WHERE status IN ('pending', 'processing', 'unmatched')
        ORDER BY updated_at DESC, id DESC
        """
    ).fetchall()
    conn.close()

    grouped = {}
    for row in rows:
        payload_obj = json.loads(row['payload_json'] or '{}')
        project_id = _extract_project_id_from_payload(payload_obj) or 'unknown'
        project_key = f'project:{project_id}' if project_id != 'unknown' else ''
        entry = grouped.setdefault(
            project_id,
            {
                'project_id': project_id,
                'project_key': project_key,
                'customer_name': row['customer_name'],
                'mapped': bool(project_key and contact_mappings.get(project_key)),
                'contact_url': contact_mappings.get(project_key, ''),
                'items': [],
            },
        )
        entry['items'].append(
            {
                'queue_id': row['id'],
                'status': row['status'],
                'comment_text': row['comment_text'],
                'updated_at': row['updated_at'],
                'last_error': row['last_error'],
            }
        )

    print(json.dumps(list(grouped.values()), indent=2, ensure_ascii=True))


if __name__ == '__main__':
    main()
