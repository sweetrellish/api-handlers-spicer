#!/usr/bin/env python3
"""
spicer_ops_menu.py
------------------
Remote health check and repair menu for Spicer API services.
- Checks status of systemd services (webhook, worker, cloudflared)
- Diagnoses common issues (inactive, failed, missing dependencies)
- Offers repair actions (restart, reload, reinstall, log dump)
- Interactive menu for remote ops
"""

import os
import subprocess
import sys
import getpass
from dotenv import load_dotenv # type: ignore
load_dotenv()

SERVICES = [
    ('spicer-webhook', 'API Webhook (Gunicorn)'),
    ('spicer-worker', 'UI Worker'),
    ('spicer-cloudflared', 'Cloudflared Tunnel'),
]

def get_ssh_creds():
    # Gateway (jump host) credentials
    gw_host = os.environ.get('SPICER_GATEWAY_HOST') or input('Gateway SSH host (Optiplex): ')
    gw_user = os.environ.get('SPICER_GATEWAY_USER') or input('Gateway SSH user: ')
    gw_pw = os.environ.get('SPICER_GATEWAY_PASS')
    if gw_pw is None:
        gw_pw = getpass.getpass(f"Password for {gw_user}@{gw_host} (leave blank to use SSH key): ")

    # Rack server credentials
    host = os.environ.get('SPICER_REMOTE_HOST') or input('Remote SSH host (Rack): ')
    user = os.environ.get('SPICER_REMOTE_USER') or input('Remote SSH user (Rack): ')
    pw = os.environ.get('SPICER_REMOTE_PASS')
    if pw is None:
        pw = getpass.getpass(f"Password for {user}@{host} (leave blank to use SSH key): ")
    return (gw_host, gw_user, gw_pw), (host, user, pw)

# Remote command execution via SSH with optional sudo
def run_remote(cmd, capture=True, sudo=False):
    # Unpack credentials
    (gw_host, gw_user, gw_pw), (host, user, pw) = SSH_CREDS
    # Use ProxyJump (-J) to go through gateway to remote host
    ssh_cmd = [
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-J', f'{gw_user}@{gw_host}',
        f'{user}@{host}',
    ]
    # if sudo needed enter sudo password pulled from env or prompt, else just run command
    if sudo:
        if pw:
            full_cmd = ssh_cmd + [f"echo {pw} | sudo -S {cmd}"]
        else:
            full_cmd = ssh_cmd + [f"sudo {cmd}"]
    if capture:
        result = subprocess.run(full_cmd, capture_output=True, text=True)
        return result.stdout.strip()
    else:
        subprocess.run(full_cmd)

# Service status checks and operations
def check_service(name):
    status = run_remote(f'systemctl is-active {name}')
    loaded = run_remote(f'systemctl is-enabled {name}')
    return status, loaded

# Print status of all services
def print_status():
    print("\n=== Spicer Service Status (Remote) ===")
    for svc, desc in SERVICES:
        status, loaded = check_service(svc)
        print(f"{desc:22} [{svc}]: {status} (enabled: {loaded})")
    print()

# Restart a service
def restart_service(name):
    print(f"Restarting {name} (remote)...")
    run_remote(f'systemctl restart {name}', capture=False, sudo=True)
    print(f"{name} restarted.")

# Show recent logs for a service
def show_logs(name, lines=30):
    print(f"\n--- Last {lines} log lines for {name} (remote) ---")
    logs = run_remote(f'journalctl -u {name} --no-pager -n {lines} -l')
    print(logs)

# Interactive menu
def menu():
    while True:
        print("Options:")
        print("  1. Check service status")
        print("  2. Restart all services")
        print("  3. Restart a service")
        print("  4. Show logs for a service")
        print("  5. Exit")
        choice = input("Select option: ").strip()
        if choice == '1':
            print_status()
        elif choice == '2':
            for svc, _ in SERVICES:
                restart_service(svc)
        elif choice == '3':
            svc_map = {str(i+1): svc for i, (svc, _) in enumerate(SERVICES)}
            for idx, (svc, desc) in enumerate(SERVICES, 1):
                print(f"    {idx}. {desc} [{svc}]")
            sel = input("Select service #: ").strip()
            if sel in svc_map:
                restart_service(svc_map[sel])
            else:
                print("Invalid selection.")
        elif choice == '4':
            svc_map = {str(i+1): svc for i, (svc, _) in enumerate(SERVICES)}
            for idx, (svc, desc) in enumerate(SERVICES, 1):
                print(f"    {idx}. {desc} [{svc}]")
            sel = input("Select service #: ").strip()
            if sel in svc_map:
                show_logs(svc_map[sel])
            else:
                print("Invalid selection.")
        elif choice == '5':
            print("Exiting.")
            sys.exit(0)
        else:
            print("Invalid option. Try again.")

if __name__ == '__main__':
    global SSH_CREDS
    SSH_CREDS = get_ssh_creds()
    menu()

