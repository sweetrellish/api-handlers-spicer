#!/bin/bash

echo "==== Failed systemd services ===="
systemctl --failed --no-legend --plain
echo "==============================="

FAILED_SERVICES=$(systemctl --failed --no-legend --plain | awk '{print $1}' | grep -E '\.service$')

for SERVICE in $FAILED_SERVICES; do
    echo "==============================="
    echo "Service: $SERVICE"
    echo "==============================="
    echo "Status:"
    systemctl status "$SERVICE"
    echo
    echo "Recent logs (last 40 lines):"
    journalctl -u "$SERVICE" -n 40 --no-pager
    echo
    echo "Attempting restart..."
    systemctl restart "$SERVICE"
    sleep 2
    systemctl is-active --quiet "$SERVICE"
    if [ $? -eq 0 ]; then
        echo "[OK] $SERVICE is now active."
    else
        echo "[FAIL] $SERVICE is still not active. See logs above for details."
    fi
    echo "---------------------------------"
    echo
    echo "Resetting failed state for $SERVICE..."
    systemctl reset-failed "$SERVICE"
done

echo
echo "==== Diagnosis complete. Review output above for errors and next steps. ===="
echo
echo "==== Verification: Current Service Status ===="
systemctl --failed --no-legend --plain
