#!/usr/bin/env python3
"""Full export validation suite - run after worker generates CSV."""
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

def run_validator(script_name: str) -> bool:
    """Run a single validator and return success."""
    script = SCRIPT_DIR / script_name
    if not script.exists():
        print(f"❌ Validator not found: {script}")
        return False
    
    try:
        result = subprocess.run([sys.executable, str(script)], capture_output=False)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running {script_name}: {e}")
        return False

def main():
    print("=" * 70)
    print("GCLID CSV Validation Suite")
    print("=" * 70)
    print()

    validators = [
        ("validate_csv_schema.py", "CSV Schema & Format"),
        ("validate_lifecycle.py", "Lifecycle Ordering"),
        ("validate_times.py", "Time Diversity"),
    ]

    results = {}
    for script, label in validators:
        print(f"\n--- {label} ---")
        results[label] = run_validator(script)
        print()

    print("\n" + "=" * 70)
    print("Summary:")
    print("=" * 70)
    
    all_passed = True
    for label, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {label}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("✅ All validations passed - CSV is ready for client upload")
    else:
        print("❌ Some validations failed - see details above")

    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
