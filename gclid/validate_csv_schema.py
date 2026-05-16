#!/usr/bin/env python3
"""Validate CSV schema and Parameters line."""
import csv
import sys
from pathlib import Path

CSV_PATH = Path(__file__).parent.parent / "data" / "spicer_conversions_202502.csv"

def main():
    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        return False

    with open(CSV_PATH, encoding='utf-8') as f:
        lines = [ln.rstrip('\n') for ln in f]

    if not lines:
        print("❌ CSV is empty")
        return False

    print(f"✓ CSV found: {CSV_PATH}")
    print(f"✓ Total lines: {len(lines)}")

    # Check Parameters line
    if not lines[0].startswith("Parameters:TimeZone="):
        print(f"❌ Line 1 should start with 'Parameters:TimeZone=' but got: {lines[0][:60]}")
        return False
    print(f"✓ Parameters line: {lines[0]}")

    # Check header
    expected_cols = ["GCLID", "Conversion Name", "Conversion Time", "Conversion Value", "Conversion Currency"]
    actual_cols = lines[1].split(",")
    if actual_cols != expected_cols:
        print(f"❌ Header mismatch")
        print(f"  Expected: {expected_cols}")
        print(f"  Got: {actual_cols}")
        return False
    print(f"✓ Header correct: {actual_cols}")

    # Data rows
    data_lines = len(lines) - 2
    print(f"✓ Data rows: {data_lines}")

    # Parse and validate
    reader = csv.DictReader(lines[1:])
    rows = list(reader)
    print(f"✓ Rows parsed: {len(rows)}")

    # Check all rows have GCLID
    missing_gclid = sum(1 for r in rows if not r.get("GCLID", "").strip())
    if missing_gclid:
        print(f"⚠ {missing_gclid} rows missing GCLID (should be filtered)")
    else:
        print(f"✓ All rows have GCLID")

    print("\n✅ Schema validation passed")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
