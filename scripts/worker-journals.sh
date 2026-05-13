#!/bin/bash
echo "=== UI Poster Worker Logs ==="
journalctl -u marketsharp_queue_worker.service -n 100 -e

echo
echo "=== Event Worker Logs ==="
journalctl -u marketsharp_queue_worker_event.service -n 100 -e
