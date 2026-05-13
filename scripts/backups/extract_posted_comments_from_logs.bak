"""
Scrape logs/worker.out.log and logs/worker.err.log for successfully posted comments.
Extracts JSON payloads or lines containing comment content and author info.
Outputs a CSV file with event_id, author, content, and timestamp if found.
"""

import os
import re
import json
import csv

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '../logs')
LOG_FILES = [
    os.path.join(LOG_DIR, 'worker.out.log'),
    os.path.join(LOG_DIR, 'worker.err.log'),
]
OUTPUT_CSV = os.path.join(LOG_DIR, 'extracted_posted_comments.csv')

# Regex to match JSON blobs or lines with comment info
JSON_RE = re.compile(r'\{.*?\}')
AUTHOR_RE = re.compile(r"author(?:_name)?['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
CONTENT_RE = re.compile(r"content['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
EVENT_ID_RE = re.compile(r"event_id['\"]?\s*[:=]\s*['\"]?(\d+)")

results = []

for log_file in LOG_FILES:
    if not os.path.exists(log_file):
        continue
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Try to extract JSON
            match = JSON_RE.search(line)
            if match:
                try:
                    data = json.loads(match.group(0))
                    # Try to extract comment info from known structure
                    comment = data.get('payload', {}).get('comment', {})
                    if comment:
                        event_id = comment.get('id') or data.get('event_id')
                        author = comment.get('creator_name') or comment.get('author_name')
                        content = comment.get('content')
                        ts = comment.get('created_at') or data.get('created_at')
                        if event_id and author and content:
                            results.append({
                                'event_id': event_id,
                                'author': author,
                                'content': content,
                                'timestamp': ts,
                                'source': os.path.basename(log_file)
                            })
                except Exception:
                    pass
            # Fallback: try to extract from line if not JSON
            else:
                author = None
                content = None
                event_id = None
                author_match = AUTHOR_RE.search(line)
                content_match = CONTENT_RE.search(line)
                event_id_match = EVENT_ID_RE.search(line)
                if author_match:
                    author = author_match.group(1)
                if content_match:
                    content = content_match.group(1)
                if event_id_match:
                    event_id = event_id_match.group(1)
                if author and content:
                    results.append({
                        'event_id': event_id or '',
                        'author': author,
                        'content': content,
                        'timestamp': '',
                        'source': os.path.basename(log_file)
                    })

if results:
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['event_id', 'author', 'content', 'timestamp', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"Extracted {len(results)} posted comments to {OUTPUT_CSV}")
else:
    print("No posted comments found in logs.")

