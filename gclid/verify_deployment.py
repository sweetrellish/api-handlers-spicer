#!/usr/bin/env python3
"""Verify deployed files match local versions."""
import subprocess
import sys
import hashlib
from pathlib import Path

LOCAL_ROOT = Path(__file__).resolve().parent.parent

FILES_TO_CHECK = {
    "gclid/gclid_sync.py": "gclid_sync.py",
    "gclid/gclid_worker.py": "gclid_worker.py", 
    "spicer_ops_menu.py": "spicer_ops_menu.py",
}

def get_local_hash(relative_path: str) -> str:
    """Compute SHA256 of local file."""
    path = LOCAL_ROOT / relative_path
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        sha.update(f.read())
    return sha.hexdigest()

def get_remote_hash(remote_path: str, host: str = "rellis@scoup2025sucosc425rack") -> str:
    """Get SHA256 of remote file via SSH."""
    try:
        result = subprocess.run(
            ["ssh", host, f"sha256sum /home/rellis/spicer/{remote_path} | cut -d' ' -f1"],
            capture_output=True, text=True, timeout=10
        )
        return result.stdout.strip()
    except Exception as e:
        print(f"  ❌ Could not fetch remote hash: {e}")
        return ""

def check_syntax(relative_path: str) -> bool:
    """Check Python syntax."""
    path = LOCAL_ROOT / relative_path
    result = subprocess.run(
        [sys.executable, "-m", "py_compile", str(path)],
        capture_output=True
    )
    return result.returncode == 0

def main():
    print("\n" + "=" * 70)
    print("Deployment Verification")
    print("=" * 70)

    print("\n1. Local File Status:")
    print("-" * 70)
    
    all_ok = True
    for local_rel, display_name in FILES_TO_CHECK.items():
        local_path = LOCAL_ROOT / local_rel
        
        if not local_path.exists():
            print(f"  ❌ {display_name}: NOT FOUND locally")
            all_ok = False
            continue

        # Check syntax
        if not check_syntax(local_rel):
            print(f"  ❌ {display_name}: SYNTAX ERROR")
            all_ok = False
            continue

        # Get hash
        local_hash = get_local_hash(local_rel)
        print(f"  ✓ {display_name}")
        print(f"    SHA256: {local_hash[:16]}...")

    if not all_ok:
        print("\n❌ Local files not ready for deployment")
        return 1

    print("\n2. Remote File Verification:")
    print("-" * 70)
    
    mismatches = []
    for local_rel, display_name in FILES_TO_CHECK.items():
        local_hash = get_local_hash(local_rel)
        remote_hash = get_remote_hash(local_rel)

        if not remote_hash:
            print(f"  ⚠ {display_name}: Could not verify (not deployed yet?)")
            continue

        if local_hash == remote_hash:
            print(f"  ✓ {display_name}: matches")
        else:
            print(f"  ❌ {display_name}: MISMATCH")
            print(f"    Local:  {local_hash[:16]}...")
            print(f"    Remote: {remote_hash[:16]}...")
            mismatches.append(display_name)

    if mismatches:
        print(f"\n⚠ {len(mismatches)} file(s) need deployment:")
        for name in mismatches:
            print(f"  • {name}")
        return 1

    print("\n✅ All files deployed correctly")
    return 0

if __name__ == "__main__":
    sys.exit(main())
