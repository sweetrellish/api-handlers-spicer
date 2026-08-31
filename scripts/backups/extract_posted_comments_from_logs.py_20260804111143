"""
Scrape logs/worker.out.log and logs/worker.err.log for successfully posted comments.
Extracts JSON payloads or lines containing comment content and author info.
Outputs a CSV file with event_id, author, content, and timestamp if found.
"""



import os
import re
import json
import csv
import sys
import subprocess


def usage():
    print("Usage: python extract_posted_comments_from_logs.py [logfile1 logfile2 ...] [--output output.csv] [--journal SERVICE]", file=sys.stderr)
    sys.exit(1)


# Parse command-line arguments
args = sys.argv[1:]
output_csv = None
log_files = []
journal_service = None

# Default: if no args, use the worker journal
if not args:
    journal_service = 'marketsharp_queue_worker.service'
else:
    if '--journal' in args:
        idx = args.index('--journal')
        if idx + 1 >= len(args):
            usage()
        journal_service = args[idx + 1]
        # Remove --journal and service from args
        args = args[:idx] + args[idx+2:]

    if '--output' in args:
        idx = args.index('--output')
        if idx + 1 >= len(args):
            usage()
        output_csv = args[idx + 1]
        log_files = args[:idx] + args[idx+2:]
    else:
        log_files = args

# If --journal is set (or defaulted), extract logs from journalctl
if journal_service:
    try:
        print(f"Reading logs from systemd journal for service: {journal_service}")
        result = subprocess.run([
            'journalctl', '-u', journal_service, '--no-pager', '-o', 'cat'
        ], capture_output=True, text=True, check=True)
        # Write to a temp file for processing
        import tempfile
        with tempfile.NamedTemporaryFile('w+', delete=False, encoding='utf-8') as tmpf:
            tmpf.write(result.stdout)
            tmpf.flush()
            log_files = [tmpf.name]
    except Exception as e:
        print(f"Error reading journal for {journal_service}: {e}", file=sys.stderr)
        sys.exit(1)

# If no log files provided, use default
if not log_files:
    LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    log_files = [
        os.path.join(LOG_DIR, 'worker.out.log'),
        os.path.join(LOG_DIR, 'worker.err.log'),
    ]
if not output_csv:
    if log_files:
        output_csv = os.path.join(os.path.dirname(log_files[0]), 'extracted_posted_comments.csv')
    else:
        output_csv = 'extracted_posted_comments.csv'

# Regex to match JSON blobs or lines with comment info
JSON_RE = re.compile(r'\{.*?\}')
AUTHOR_RE = re.compile(r"author(?:_name)?['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
CONTENT_RE = re.compile(r"content['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
EVENT_ID_RE = re.compile(r"event_id['\"]?\s*[:=]\s*['\"]?(\d+)")


results = []

for log_file in log_files:
    if not os.path.exists(log_file):
        print(f"Warning: log file not found: {log_file}", file=sys.stderr)
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
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['event_id', 'author', 'content', 'timestamp', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"Extracted {len(results)} posted comments to {output_csv}")
else:
    print("No posted comments found in logs.")

