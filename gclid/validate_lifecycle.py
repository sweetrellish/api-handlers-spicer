#!/usr/bin/env python3
"""Validate that Sold Jobs always occur after their paired Qualified Leads."""
import csv
import sys
from datetime import datetime
from pathlib import Path

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

    print(f"Checking {len(rows)} rows for lifecycle ordering...")

    # Map GCLID -> first Qualified Lead timestamp
    first_qualified = {}
    fmt = "%m/%d/%Y %I:%M:%S %p"

    for row in rows:
        gclid = row.get("GCLID", "").strip()
        conv_type = row.get("Conversion Name", "").strip()
        conv_time_raw = row.get("Conversion Time", "").strip()

        if not gclid or not conv_type or not conv_time_raw:
            continue

        # Remove timezone suffix
        conv_time = conv_time_raw.replace(" America/New_York", "")

        try:
            dt = datetime.strptime(conv_time, fmt)
            if conv_type == "Qualified Lead":
                if gclid not in first_qualified:
                    first_qualified[gclid] = dt
                else:
                    first_qualified[gclid] = min(first_qualified[gclid], dt)
        except ValueError as e:
            print(f"⚠ Skipping row with unparseable time '{conv_time}': {e}")
            continue

    # Check Sold Job ordering
    violations = []
    for row in rows:
        gclid = row.get("GCLID", "").strip()
        conv_type = row.get("Conversion Name", "").strip()
        conv_time_raw = row.get("Conversion Time", "").strip()

        if conv_type != "Sold Job":
            continue

        conv_time = conv_time_raw.replace(" America/New_York", "")
        try:
            sold_dt = datetime.strptime(conv_time, fmt)
            qual_dt = first_qualified.get(gclid)

            if qual_dt and sold_dt <= qual_dt:
                violations.append({
                    "gclid": gclid,
                    "qualified": qual_dt.isoformat(),
                    "sold": sold_dt.isoformat(),
                })
        except ValueError:
            continue

    if violations:
        print(f"\n❌ Found {len(violations)} lifecycle violations:")
        for v in violations[:5]:
            print(f"  GCLID {v['gclid']}: Sold {v['sold']} <= Qualified {v['qualified']}")
        if len(violations) > 5:
            print(f"  ... and {len(violations) - 5} more")
        return False

    print(f"✓ First Qualified Leads tracked: {len(first_qualified)}")
    print(f"✓ Sold Jobs checked: {sum(1 for r in rows if r.get('Conversion Name') == 'Sold Job')}")
    print(f"✓ No lifecycle violations found")

    print("\n✅ Lifecycle validation passed")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
