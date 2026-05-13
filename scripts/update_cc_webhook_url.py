def delete_duplicate_webhooks(target_url, keep_id):
    """Delete any webhooks (except keep_id) that use the target_url."""
    endpoint = f'{CC_BASE}/v2/webhooks'
    headers = {
        'Authorization': f'Bearer {CC_TOKEN}',
        'Accept': 'application/json',
    }
    try:
        resp = requests.get(endpoint, headers=headers, timeout=15)
        resp.raise_for_status()
        webhooks = resp.json()
        for wh in webhooks:
            wh_id = wh.get('id')
            wh_url = wh.get('url')
            if wh_id != keep_id and wh_url == target_url:
                del_endpoint = f'{CC_BASE}/v2/webhooks/{wh_id}'
                print(f'Deleting duplicate webhook ID {wh_id} using URL {wh_url}')
                del_resp = requests.delete(del_endpoint, headers=headers, timeout=15)
                if del_resp.status_code == 204:
                    print(f'Webhook {wh_id} deleted successfully.')
                else:
                    print(f'Failed to delete webhook {wh_id}: {del_resp.status_code} {del_resp.text}')
    except Exception as e:
        print(f'Error deleting duplicate webhooks: {e}', file=sys.stderr)
def list_all_webhooks():
    """List all webhooks and print their IDs and URLs for duplicate detection."""
    endpoint = f'{CC_BASE}/v2/webhooks'
    headers = {
        'Authorization': f'Bearer {CC_TOKEN}',
        'Accept': 'application/json',
    }
    try:
        resp = requests.get(endpoint, headers=headers, timeout=15)
        resp.raise_for_status()
        webhooks = resp.json()
        print('\n--- Existing CompanyCam Webhooks ---')
        for wh in webhooks:
            wh_id = wh.get('id')
            wh_url = wh.get('url')
            wh_enabled = wh.get('enabled', False)
            print(f'ID: {wh_id} | Enabled: {wh_enabled} | URL: {wh_url}')
        print('--- End Webhook List ---\n')
    except Exception as e:
        print(f'Error listing webhooks: {e}', file=sys.stderr)
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
    webhook_url = f'{new_url}/webhook/companycam'
    # Delete any other webhooks using this URL before updating
    delete_duplicate_webhooks(webhook_url, WEBHOOK_ID)
    payload = {'url': webhook_url, 'enabled': True}
    headers = {
        'Authorization': f'Bearer {CC_TOKEN}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    try:
        # First, GET the current webhook config
        get_resp = requests.get(endpoint, headers=headers, timeout=15)
        get_resp.raise_for_status()
        current = get_resp.json()
        current_url = current.get('url')
        current_enabled = current.get('enabled', False)
        if current_url == webhook_url and current_enabled:
            print(f'Webhook {WEBHOOK_ID} already set to {webhook_url} and enabled. No update needed.')
            return True
        # Only PATCH if something needs to change
        resp = requests.patch(endpoint, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        print(f'Webhook {WEBHOOK_ID} updated → {webhook_url} and enabled')
        return True
    except requests.RequestException as exc:
        print(f'ERROR updating webhook: {exc}', file=sys.stderr)
        if hasattr(exc, 'response') and exc.response is not None:
            print(exc.response.text[:400], file=sys.stderr)
        return False


def main():
        # List all webhooks before updating
    list_all_webhooks()
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

