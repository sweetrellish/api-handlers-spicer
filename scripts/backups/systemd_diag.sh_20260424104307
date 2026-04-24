#!/bin/bash
# diagnose_systemd_failures.sh
# Portable script to diagnose and help fix failed systemd services
set -euo pipefail

# List all failed services
# use for spicer services
#FAILED_SERVICES=$(systemctl --failed --no-legend | awk '$1 !~ /^●$/ {print $1}')
#use for all cases
FAILED_SERVICES=$(systemctl list-units --all --state=failed --no-legend | awk '$1 !~ /^●$/ {print $1}')
if [ -z "$FAILED_SERVICES" ]; then
    echo "No failed systemd services detected."
    exit 0
fi

echo "==== Failed systemd services ===="
for svc in $FAILED_SERVICES; do
    echo "- $svc"
done

echo
for svc in $FAILED_SERVICES; do
    echo "==============================="
    echo "Service: $svc"
    echo "==============================="
    echo "Status:"
    systemctl status "$svc" --no-pager || true
    echo
    echo "Recent logs (last 40 lines):"
    journalctl -u "$svc" -n 40 --no-pager || true
    echo
    echo "Attempting restart..."
    sudo systemctl restart "$svc" || true
    sleep 2
    if systemctl is-active --quiet "$svc"; then
        echo "[OK] $svc restarted and is now ACTIVE."
    else
        echo "[FAIL] $svc is still not active. See logs above for details."
    fi
    echo
    echo "---------------------------------"
    echo
    # Optionally, prompt to reset-failed if still failed
    if ! systemctl is-active --quiet "$svc"; then
        echo "Resetting failed state for $svc..."
        sudo systemctl reset-failed "$svc"
    fi
    echo
    sleep 1
    done

echo "==== Diagnosis complete. Review output above for errors and next steps. ===="

echo
echo "==== Verification: Current Service Status ===="
for svc in $FAILED_SERVICES; do
    echo "-----------------------------"
    echo "Status for $svc:"
    systemctl status "$svc" --no-pager || true
done

echo
echo "==== All failed units after attempted fixes ===="
systemctl --failed || true

# Show all failed units (not just services)
echo "==== All failed systemd units (any type) ===="
systemctl list-units --all --state=failed || true

echo
# Show systemctl status summary and highlight degraded state
echo "==== Systemctl Status Summary ===="
systemctl status | head -20

echo
if systemctl status | grep -q 'State: degraded'; then
    echo "[WARNING] System is still degraded. See above for failed units or investigate further."
else
    echo "[OK] System is not degraded."
fi

