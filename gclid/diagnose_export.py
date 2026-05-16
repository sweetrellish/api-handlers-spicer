#!/usr/bin/env python3
"""Diagnostic analysis of GCLID export completeness and coverage."""
import csv
import sys
from pathlib import Path
from collections import defaultdict

CSV_PATH = Path(__file__).parent.parent / "data" / "spicer_conversions_202502.csv"
REFERENCE_CONTACTS = Path(__file__).parent / "Contacts (1).csv"

def load_reference_contacts():
    """Load the MarketSharp reference contact list (304 contacts with GCLID presence)."""
    contacts = {}
    if not REFERENCE_CONTACTS.exists():
        print(f"⚠ Reference file not found: {REFERENCE_CONTACTS}")
        return contacts
    
    with open(REFERENCE_CONTACTS, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("ContactId", "").strip()
            name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
            if cid:
                contacts[cid] = {"name": name}
    return contacts

def main():
    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        return 1

    # Load reference
    reference = load_reference_contacts()
    print(f"Reference: {len(reference)} contacts with GCLID presence in MarketSharp\n")

    # Load export
    with open(CSV_PATH, encoding='utf-8') as f:
        lines = [ln.rstrip('\n') for ln in f]

    if len(lines) < 3:
        print("❌ CSV too small")
        return 1

    reader = csv.DictReader(lines[1:])
    rows = list(reader)

    # Analysis
    contact_rows = defaultdict(list)
    contact_gclids = {}
    by_type = defaultdict(int)

    for row in rows:
        gclid = row.get("GCLID", "").strip()
        conv_type = row.get("Conversion Name", "").strip()
        conv_time = row.get("Conversion Time", "").strip()

        # Try to infer contact ID from GCLID (it's the display value, not actual ID)
        # We'll group by GCLID as proxy for unique contacts
        contact_gclids[gclid] = True
        contact_rows[gclid].append({
            "type": conv_type,
            "time": conv_time,
        })
        by_type[conv_type] += 1

    print(f"Export Analysis:")
    print(f"  Total rows: {len(rows)}")
    print(f"  Unique contacts (by GCLID): {len(contact_gclids)}")
    print(f"  Conversion types:")
    for ctype, count in sorted(by_type.items()):
        print(f"    - {ctype}: {count}")

    print(f"\n--- Per-Contact Breakdown ---")
    for gclid, conv_list in sorted(contact_rows.items()):
        ql_count = sum(1 for c in conv_list if c['type'] == 'Qualified Lead')
        sj_count = sum(1 for c in conv_list if c['type'] == 'Sold Job')
        print(f"  {gclid[:30]:30} : QL={ql_count} SJ={sj_count}")

    # Coverage analysis
    coverage_rate = 100.0 * len(contact_gclids) / len(reference) if reference else 0
    print(f"\n--- Coverage Analysis ---")
    print(f"  Reference contacts: {len(reference)}")
    print(f"  Contacts in export: {len(contact_gclids)}")
    print(f"  Coverage rate: {coverage_rate:.1f}%")

    # Data quality flags
    print(f"\n--- Data Quality Checks ---")
    
    # Check for email/phone (should not be present)
    header = lines[1].split(',')
    has_email = 'Email' in header
    has_phone = 'Phone' in header
    if has_email or has_phone:
        print(f"  ⚠ WARNING: Unexpected columns in header: {[h for h in header if h in ['Email', 'Phone']]}")
    else:
        print(f"  ✓ No Email/Phone columns")

    # Check for empty GCLIDs
    empty_gclids = sum(1 for r in rows if not r.get('GCLID', '').strip())
    if empty_gclids:
        print(f"  ⚠ {empty_gclids} rows with missing/empty GCLID")
    else:
        print(f"  ✓ All rows have GCLID")

    # Check GCLID format validity (rough: Google GCLIDs are usually 30+ chars, start with AW- or similar)
    invalid_gclids = 0
    for gclid in contact_gclids:
        if len(gclid) < 20:
            invalid_gclids += 1
    if invalid_gclids:
        print(f"  ⚠ {invalid_gclids} GCLIDs look suspiciously short (<20 chars)")

    # Timestamp coverage
    times = [r.get("Conversion Time", "").strip() for r in rows]
    time_set = set(times)
    print(f"  Timestamp range: {min(times) if times else 'N/A'} to {max(times) if times else 'N/A'}")
    print(f"  Unique timestamps: {len(time_set)}")

    print(f"\n--- Interpretation ---")
    if coverage_rate < 10:
        print(f"  ⚠ Very low coverage ({coverage_rate:.1f}%)")
        print(f"    This could indicate:")
        print(f"    - Contacts have GCLIDs but no appointments/jobs in this period")
        print(f"    - Query filters are too restrictive")
        print(f"    - Date range doesn't overlap with conversion events")
    elif coverage_rate < 50:
        print(f"  ⚠ Low coverage ({coverage_rate:.1f}%)")
        print(f"    Many reference contacts are not represented in export")
    else:
        print(f"  ✓ Good coverage ({coverage_rate:.1f}%)")

    # Row count sanity check
    avg_rows_per_contact = len(rows) / len(contact_gclids) if contact_gclids else 0
    print(f"  Average rows per contact: {avg_rows_per_contact:.1f}")
    if avg_rows_per_contact < 1:
        print(f"    ⚠ Most contacts have only one row (likely just synthetic QL, no jobs)")
    elif avg_rows_per_contact > 3:
        print(f"    ✓ Good row diversity per contact")

    return 0

if __name__ == "__main__":
    sys.exit(main())
