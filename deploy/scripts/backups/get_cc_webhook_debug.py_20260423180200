#!/usr/bin/env python3
"""
get_cc_webhook_debug.py
----------------------
GETs the CompanyCam webhook endpoint to check existence and permissions.
"""
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

CC_TOKEN = os.getenv('COMPANYCAM_WEBHOOK_TOKEN', '')
WEBHOOK_ID = os.getenv('COMPANYCAM_WEBHOOK_ID', '')
CC_BASE = 'https://api.companycam.com'

if not CC_TOKEN:
    print('ERROR: COMPANYCAM_WEBHOOK_TOKEN is not set', file=sys.stderr)
    sys.exit(1)
if not WEBHOOK_ID:
    print('ERROR: COMPANYCAM_WEBHOOK_ID is not set', file=sys.stderr)
    sys.exit(1)

endpoint = f'{CC_BASE}/v2/webhooks/{WEBHOOK_ID}'
headers = {
    'Authorization': f'Bearer {CC_TOKEN}',
    'Accept': 'application/json',
}
print(f'GET {endpoint}')
try:
    resp = requests.get(endpoint, headers=headers, timeout=15)
    print('Status:', resp.status_code)
    print('Response:', resp.text[:1200])
    resp.raise_for_status()
except requests.RequestException as exc:
    print(f'ERROR during GET: {exc}', file=sys.stderr)
    if hasattr(exc, 'response') and exc.response is not None:
        print(exc.response.text[:1200], file=sys.stderr)
    sys.exit(1)
