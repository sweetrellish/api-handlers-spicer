#!/bin/bash

# 1. Check systemd service status
echo "==> Checking spicer-flask-api.service status..."
systemctl status spicer-flask-api.service --no-pager

# 2. Test API health endpoint
echo -e "\n==> Testing API / endpoint..."
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5001/

# 3. Test /test endpoint (if available)
echo -e "\n==> Testing API /test endpoint..."
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:5001/test

# 4. Check SQLite DB file existence and permissions
echo -e "\n==> Checking SQLite DB files in ./data/ ..."
for db in cc_webhook_dedupe.db pending_comments.db posted_comments_audit.db; do
    if [ -f "data/$db" ]; then
        echo "Found: data/$db"
        ls -l "data/$db"
    else
        echo "Missing: data/$db"
    fi
done

# 5. Tail last 20 lines of service logs
echo -e "\n==> Last 20 lines of spicer-flask-api.service logs:"
journalctl -u spicer-flask-api.service -n 20 --no-pager

echo -e "\n==> Done. Review output for errors or non-200 responses."
