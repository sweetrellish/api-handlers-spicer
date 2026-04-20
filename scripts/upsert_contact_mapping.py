#!/usr/bin/env python3
"""Upsert a project-keyed MarketSharp contact URL mapping."""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import Config
from mapping_registry import load_mapping_file, save_mapping_file
from queue_ui_poster import _extract_project_id_from_payload


def resolve_project_id(queue_id):
    conn = sqlite3.connect(Config.PENDING_QUEUE_DB_PATH)
    row = conn.execute(
        'SELECT payload_json FROM pending_comments WHERE id = ?',
        (queue_id,),
    ).fetchone()
    conn.close()
    if not row:
        raise ValueError(f'Queue item {queue_id} not found')
    payload_obj = json.loads(row[0] or '{}')
    project_id = _extract_project_id_from_payload(payload_obj)
    if not project_id:
        raise ValueError(f'Could not extract project id from queue item {queue_id}')
    return project_id


def main():
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description='Upsert a MarketSharp contact URL mapping')
    parser.add_argument('--project-id', help='CompanyCam project/location id')
    parser.add_argument('--queue-id', type=int, help='Pending queue row id to extract project id from')
    parser.add_argument('--url', required=True, help='MarketSharp contact detail URL')
    parser.add_argument(
        '--file',
        default=os.getenv('MARKETSHARP_UI_CONTACT_URL_MAP_FILE', 'marketsharp_contact_mappings.json').strip(),
        help='Mapping file path relative to repo root or absolute path',
    )
    args = parser.parse_args()

    if not args.project_id and not args.queue_id:
        raise ValueError('Provide either --project-id or --queue-id')

    project_id = args.project_id or resolve_project_id(args.queue_id)
    mapping_file = Path(args.file)
    if not mapping_file.is_absolute():
        mapping_file = REPO_ROOT / mapping_file

    mappings = load_mapping_file(mapping_file)
    mappings[f'project:{project_id}'] = args.url.strip()
    save_mapping_file(mapping_file, mappings)
    print(f'updated project:{project_id} -> {args.url.strip()} in {mapping_file}')


if __name__ == '__main__':
    main()