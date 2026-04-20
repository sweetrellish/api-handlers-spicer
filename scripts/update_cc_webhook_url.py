#!/usr/bin/env python3
"""
update_cc_webhook_url.py
------------------------
Reads the current cloudflared Quick Tunnel URL from the spicer-cloudflared
systemd service journal, then PATCHes the CompanyCam webhook to point at
the new URL.

Run automatically as the spicer-webhook-url-sync.service one-shot unit,
which is triggered after spicer-cloudflared.service starts.

Required env vars (loaded from /home/rellis/spicer/.env):
  COMPANYCAM_WEBHOOK_TOKEN  — CC OAuth / personal access token
  COMPANYCAM_WEBHOOK_ID     — numeric webhook ID (e.g. 221464)
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

POLL_INTERVAL = 3   # seconds between journal checks
MAX_WAIT = 60       # seconds before giving up


def get_tunnel_url() -> str | None:
    """Read the cloudflared journal and extract the Quick Tunnel URL."""
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
    """PATCH the CompanyCam webhook to use new_url/webhook as its endpoint."""
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
    try:
        resp = requests.patch(endpoint, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        print(f'Webhook {WEBHOOK_ID} updated → {new_url}/webhook')
        return True
    except requests.RequestException as exc:
        print(f'ERROR updating webhook: {exc}', file=sys.stderr)
        if hasattr(exc, 'response') and exc.response is not None:
            print(exc.response.text[:400], file=sys.stderr)
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
