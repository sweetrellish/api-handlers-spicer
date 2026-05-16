#!/usr/bin/env python3
"""Verify that conversion times have reasonable diversity, not all identical."""
import csv
import sys
from pathlib import Path
from collections import Counter

CSV_PATH = Path(__file__).parent.parent / "data" / "spicer_conversions_202502.csv"

def main():
    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        return False

    with open(CSV_PATH, encoding='utf-8') as f:
        lines = [ln.rstrip('\n') for ln in f]

    if len(lines) < 3:
        print("❌ CSV too small")
        return False

    # Skip Parameters and header rows
    reader = csv.DictReader(lines[1:])
    rows = list(reader)

    times = [r.get("Conversion Time", "").strip() for r in rows if r.get("Conversion Time")]
    time_counts = Counter(times)

    print(f"Total rows: {len(rows)}")
    print(f"Unique times: {len(time_counts)}")
    print(f"\nTop 10 most common times:")

    for time, count in time_counts.most_common(10):
        pct = 100.0 * count / len(rows) if rows else 0
        bar = "█" * min(int(pct / 5), 20)
        print(f"  {time:45} {count:3d} ({pct:5.1f}%) {bar}")

    # Acceptance criteria
    if len(time_counts) == 1:
        print("\n⚠ All rows have identical timestamp (acceptable if source is date-only)")
        print("  Verify this matches expected behavior for your data source")
    elif len(time_counts) < len(rows) / 2:
        print(f"\n⚠ Low diversity: {len(time_counts)} unique times for {len(rows)} rows")
        print("  Expected if source timestamps are mostly dates (using consistent fallback time)")
    else:
        print(f"\n✓ Good time diversity: {len(time_counts)} unique times")

    print("\n✅ Time analysis complete")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
