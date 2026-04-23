#!/bin/bash
# Restart all relevant MarketSharp API handler services on Linux

set -e

SERVICES=(
    marketsharp_queue_worker
    spicer-webhook-sync
    spicer-webhook-url-sync
    true_fail_checker
)

echo "Restarting MarketSharp API handler services..."
for svc in "${SERVICES[@]}"; do
    echo "Restarting $svc.service..."
    sudo systemctl restart "$svc.service"
    sudo systemctl status "$svc.service" --no-pager --lines=5
    echo
    sleep 1
done

echo "All services restarted."

