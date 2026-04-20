#!/usr/bin/env python3
"""
update_cc_webhook_url_debug.py
-----------------------------
Debug version: prints full request/response for CompanyCam webhook PATCH.
"""
import os
import re
import subprocess
import sys
import time
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

CC_TOKEN = os.getenv('COMPANYCAM_WEBHOOK_TOKEN', '')
WEBHOOK_ID = os.getenv('COMPANYCAM_WEBHOOK_ID', '')
CC_BASE = 'https://api.companycam.com'

URL_PATTERN = re.compile(r'https://[a-z0-9\-]+\.trycloudflare\.com')

POLL_INTERVAL = 3
MAX_WAIT = 60

def get_tunnel_url() -> str | None:
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        result = subprocess.run(
            ['journalctl', '-u', 'spicer-cloudflared', '--no-pager', '-n', '60', '-l'],
            capture_output=True,
            text=True,
        )
        match = URL_PATTERN.search(result.stdout)
        if match:
            return match.group(0)
        time.sleep(POLL_INTERVAL)
    return None

def update_webhook(new_url: str) -> bool:
    if not CC_TOKEN:
        print('ERROR: COMPANYCAM_WEBHOOK_TOKEN is not set', file=sys.stderr)
        return False
    if not WEBHOOK_ID:
        print('ERROR: COMPANYCAM_WEBHOOK_ID is not set', file=sys.stderr)
        return False
    endpoint = f'{CC_BASE}/v2/webhooks/{WEBHOOK_ID}'
    payload = {'webhook_url': f'{new_url}/webhook'}
    headers = {
        'Authorization': f'Bearer {CC_TOKEN}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    # First, try GET to see if the webhook exists and token is valid
    print(f'GET {endpoint}')
    try:
        get_resp = requests.get(endpoint, headers=headers, timeout=15)
        print('GET Status:', get_resp.status_code)
        print('GET Response:', get_resp.text[:800])
        if get_resp.status_code != 200:
            print('ERROR: GET failed, PATCH will likely fail as well.', file=sys.stderr)
    except requests.RequestException as exc:
        print(f'ERROR during GET: {exc}', file=sys.stderr)
        return False

    print(f'PATCH {endpoint}')
    print('Headers:', headers)
    print('Payload:', payload)
    try:
        resp = requests.patch(endpoint, json=payload, headers=headers, timeout=15)
        print('Status:', resp.status_code)
        print('Response:', resp.text[:800])
        resp.raise_for_status()
        print(f'Webhook {WEBHOOK_ID} updated → {new_url}/webhook')
        return True
    except requests.RequestException as exc:
        print(f'ERROR updating webhook: {exc}', file=sys.stderr)
        if hasattr(exc, 'response') and exc.response is not None:
            print(exc.response.text[:800], file=sys.stderr)
        return False

def main():
    print('Waiting for cloudflared tunnel URL...')
    url = get_tunnel_url()
    if not url:
        print('ERROR: cloudflared URL not found in journal within timeout', file=sys.stderr)
        sys.exit(1)
    print(f'Tunnel URL: {url}')
    success = update_webhook(url)
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
