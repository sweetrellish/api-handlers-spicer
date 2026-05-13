#!/usr/bin/env python3
"""
Health check script for cloudflared tunnel and MarketSharp queue worker.
Checks:
- cloudflared tunnel is running and public URL is reachable
- queue worker (event-driven or polling) is running
- Optionally: webhook endpoint is reachable from the public internet
"""
import subprocess
import requests
import sys
import os
import time

# Configurable
CLOUDFLARED_SERVICES = [
    "spicer-cloudflared.service",
    "cloudflared-watchdog.service"
]
WORKER_SERVICES = [
    "spicer-worker.service",
    "marketsharp_queue_worker_event.service",
    "marketsharp_queue_worker.service"
]
WEBHOOK_URLS = [
    "http://localhost:5000/webhook/companycam",
    "http://localhost:5001/webhook/companycam"
]
TUNNEL_STATUS_URL = os.getenv("CLOUDFLARED_STATUS_URL", "http://localhost:8080/metrics")  # If cloudflared metrics enabled


def check_systemd_service(service_name):
    try:
        result = subprocess.run(["systemctl", "is-active", service_name], capture_output=True, text=True)
        return result.stdout.strip() == "active"
    except Exception as e:
        print(f"[ERROR] Could not check {service_name}: {e}")
        return False

def check_webhook(url):
    try:
        resp = requests.options(url, timeout=5)
        return resp.status_code in (200, 204)
    except Exception as e:
        print(f"[ERROR] Webhook endpoint not reachable at {url}: {e}")
        return False

def check_cloudflared_metrics():
    try:
        resp = requests.get(TUNNEL_STATUS_URL, timeout=5)
        return resp.status_code == 200
    except Exception as e:
        print(f"[WARN] Could not reach cloudflared metrics endpoint: {e}")
        return False

def main():
    print("--- Health Check ---")
    # Check all cloudflared services
    for svc in CLOUDFLARED_SERVICES:
        ok = check_systemd_service(svc)
        print(f"{svc}: {'OK' if ok else 'NOT RUNNING'}")
    # Check all worker services
    for svc in WORKER_SERVICES:
        ok = check_systemd_service(svc)
        print(f"{svc}: {'OK' if ok else 'NOT RUNNING'}")
    # Check all webhook endpoints
    for url in WEBHOOK_URLS:
        ok = check_webhook(url)
        print(f"webhook endpoint {url}: {'OK' if ok else 'NOT REACHABLE'}")
    metrics_ok = check_cloudflared_metrics()
    print(f"cloudflared metrics: {'OK' if metrics_ok else 'NOT REACHABLE'}")
    # Summary
    if (any(check_systemd_service(svc) for svc in CLOUDFLARED_SERVICES)
        and any(check_systemd_service(svc) for svc in WORKER_SERVICES)
        and any(check_webhook(url) for url in WEBHOOK_URLS)):
        print("[HEALTH] At least one of each critical service is running.")
        sys.exit(0)
    else:
        print("[HEALTH] One or more critical services are NOT healthy.")
        sys.exit(1)

if __name__ == "__main__":
    main()

