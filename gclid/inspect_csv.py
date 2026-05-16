#!/usr/bin/env python3
"""Quick inspection of CSV structure and headers."""
import csv
import sys
from pathlib import Path

CSV_PATH = Path(__file__).parent.parent / "data" / "spicer_conversions_202502.csv"

def main():
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        return 1

    with open(CSV_PATH, encoding='utf-8') as f:
        lines = [ln.rstrip('\n') for ln in f]

    print(f"line1 = {lines[0]}")
    print(f"line2 = {lines[1]}")
    print(f"data_lines = {len(lines) - 2}")

    reader = csv.DictReader(lines[1:])
    print(f"fieldnames = {reader.fieldnames}")
    
    expected = ['GCLID', 'Conversion Name', 'Conversion Time', 'Conversion Value', 'Conversion Currency']
    columns_ok = reader.fieldnames == expected
    print(f"columns_ok = {columns_ok}")

    if not columns_ok:
        print(f"\nExpected: {expected}")
        print(f"Got:      {reader.fieldnames}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())

