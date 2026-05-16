#!/usr/bin/env python3
"""Compare GCLID export coverage across multiple months."""
import argparse
import subprocess
import sys
import csv
from pathlib import Path
from datetime import datetime

GCLID_DIR = Path(__file__).resolve().parent
SPICER_DATA = GCLID_DIR.parent / "data"
DEFAULT_REFERENCE_CANDIDATES = [
    GCLID_DIR / "Contacts (1).csv",
    GCLID_DIR / "ms-report-hasGCLID.csv",
]

def load_reference_contacts(reference_csv: Path | None):
    """Load reference contacts from CSV (ContactId required)."""
    contacts = set()
    if reference_csv is None or not reference_csv.exists():
        return contacts
    
    with open(reference_csv, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("ContactId", "").strip()
            if cid:
                contacts.add(cid)
    return contacts

def resolve_reference_csv(cli_path: str | None) -> Path | None:
    if cli_path:
        p = Path(cli_path)
        return p if p.exists() else None
    for p in DEFAULT_REFERENCE_CANDIDATES:
        if p.exists():
            return p
    return None

def run_report(month_str: str, contacts_csv: str) -> tuple[int, int]:
    """
    Run worker for a specific month, return (num_rows, num_unique_contacts).
    month_str format: "2025-02"
    """
    csv_path = SPICER_DATA / f"spicer_conversions_{month_str.replace('-', '')}.csv"
    
    # Run worker
    print(f"  Generating {month_str}...", end=" ", flush=True)
    result = subprocess.run(
        [sys.executable, "gclid_worker.py", "--month", month_str,
         "--contacts-csv", contacts_csv, "--contacts-mode", "assist"],
        cwd=str(GCLID_DIR),
        capture_output=True,
        text=True,
        timeout=1200
    )

    if result.returncode != 0:
        combined = (result.stdout or "") + "\n" + (result.stderr or "")
        if "No GCLID contacts / conversions found" in combined:
            print("NO_DATA")
            return 0, 0
        print(f"ERROR")
        tail = combined.splitlines()[-5:]
        for line in tail:
            print(f"    {line}")
        return 0, 0
    
    if not csv_path.exists():
        print(f"NO CSV")
        return 0, 0
    
    # Parse CSV
    with open(csv_path, encoding='utf-8') as f:
        lines = [ln.rstrip('\n') for ln in f]
    
    if len(lines) < 3:
        print("EMPTY")
        return 0, 0
    
    reader = csv.DictReader(lines[1:])
    rows = list(reader)
    
    # Count unique contacts (by GCLID)
    unique_gclids = set(r.get("GCLID", "").strip() for r in rows if r.get("GCLID", "").strip())
    
    print(f"OK")
    return len(rows), len(unique_gclids)

def main():
    p = argparse.ArgumentParser(description="Compare export coverage across months")
    p.add_argument("--start", default="2025-02", help="Start month YYYY-MM (default: 2025-02)")
    p.add_argument("--months", type=int, default=3, help="Number of months to test (default: 3)")
    p.add_argument(
        "--contacts-csv",
        default="ms-report-hasGCLID.csv",
        help="Contacts CSV passed to gclid_worker.py (default: ms-report-hasGCLID.csv)",
    )
    p.add_argument(
        "--reference-csv",
        default=None,
        help="Reference CSV with ContactId values for coverage denominator",
    )
    args = p.parse_args()

    print("=" * 70)
    print("GCLID Export Coverage by Month")
    print("=" * 70)
    print()

    reference_csv = resolve_reference_csv(args.reference_csv)
    reference = load_reference_contacts(reference_csv)
    print(f"Reference contacts: {len(reference)}\n")
    if reference_csv is not None:
        print(f"Reference CSV: {reference_csv}\n")
    else:
        print("Reference CSV: not found (coverage denominator unavailable)\n")
    
    # Generate month list
    months = []
    start = datetime.strptime(args.start, "%Y-%m")
    for i in range(max(1, args.months)):
        year = start.year + ((start.month - 1 + i) // 12)
        month_num = ((start.month - 1 + i) % 12) + 1
        month = datetime(year, month_num, 1)
        month_str = month.strftime("%Y-%m")
        months.append(month_str)
    
    results = {}
    for month in months:
        rows, contacts = run_report(month, args.contacts_csv)
        coverage = 100.0 * contacts / len(reference) if reference else 0
        results[month] = {
            "rows": rows,
            "contacts": contacts,
            "coverage": coverage,
        }
    
    # Display table
    print("\n" + "=" * 70)
    print("Results:")
    print("=" * 70)
    print(f"{'Month':<12} {'Rows':<8} {'Contacts':<12} {'Coverage':<12}")
    print("-" * 70)
    
    for month in months:
        r = results[month]
        print(f"{month:<12} {r['rows']:<8} {r['contacts']:<12} {r['coverage']:>10.1f}%")
    
    # Analysis
    print("\n" + "=" * 70)
    print("Analysis:")
    print("=" * 70)
    
    best_month = max(months, key=lambda m: results[m]['coverage'])
    best_coverage = results[best_month]['coverage']
    
    if best_coverage < 5:
        print("⚠ VERY LOW COVERAGE across all months (<5%)")
        print("\nPossible issues:")
        print("  1. Appointment/job queries failing silently")
        print("  2. Query filters too restrictive")
        print("  3. Data doesn't exist in MarketSharp for Feb-Apr 2025")
        print("\nNext steps:")
        print("  - Run with --debug-contacts to see per-contact details")
        print("  - Check appointment/job query results directly")
    elif best_coverage < 30:
        print(f"⚠ Low coverage ({best_coverage:.1f}% in {best_month})")
        print("  Consider checking data quality and query filters")
    else:
        print(f"✓ Reasonable coverage ({best_coverage:.1f}% in {best_month})")
    
    print()
    return 0

if __name__ == "__main__":
    sys.exit(main())
