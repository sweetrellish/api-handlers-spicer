#!/usr/bin/env python3
"""Query MarketSharp directly to see raw appointment/job data for a contact."""
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

# Ensure root and src are importable regardless of current working directory.
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

def main():
    p = argparse.ArgumentParser(description="Inspect raw appointment/job query results by ContactId")
    p.add_argument("contact_ids", nargs="+", help="One or more ContactId values")
    args = p.parse_args()

    contact_ids = [c.strip() for c in args.contact_ids if c.strip()]
    if not contact_ids:
        print("No contact IDs provided.")
        return 1

    try:
        from gclid.gclid_sync import ReportBuilder
    except Exception as e:
        print(f"Error importing ReportBuilder: {e}")
        return 1

    try:
        builder = ReportBuilder(auto_discover=False)
    except TypeError:
        builder = ReportBuilder()
    
    print("=" * 70)
    print("Raw MarketSharp Query Results")
    print("=" * 70)
    print()
    
    for cid in contact_ids:
        print(f"\n--- Contact: {cid} ---\n")
        
        # Appointments
        print("APPOINTMENTS:")
        try:
            appts = builder.appointments_for_contact(cid)
            print(f"  Found: {len(appts)}")
            for i, a in enumerate(appts[:3]):
                print(f"\n  [{i+1}]")
                print(f"    setDate: {a.get('setDate') or a.get('SetDate') or a.get('appointmentDate')}")
                print(f"    dateTime: {a.get('dateTime')}")
                print(f"    startDate: {a.get('startDate')}")
                print(f"    Full keys: {list(a.keys())}")
            if len(appts) > 3:
                print(f"\n  ... and {len(appts) - 3} more")
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Jobs
        print("\n\nJOBS:")
        try:
            jobs = builder.jobs_for_contact(cid)
            print(f"  Found: {len(jobs)}")
            for i, j in enumerate(jobs[:3]):
                print(f"\n  [{i+1}]")
                print(f"    saleDate: {j.get('saleDate') or j.get('SaleDate')}")
                print(f"    soldDate: {j.get('soldDate') or j.get('SoldDate')}")
                print(f"    contractDate: {j.get('contractDate') or j.get('ContractDate')}")
                print(f"    Full keys: {list(j.keys())}")
            if len(jobs) > 3:
                print(f"\n  ... and {len(jobs) - 3} more")
        except Exception as e:
            print(f"  ERROR: {e}")
        
        # Contact record
        print("\n\nCONTACT RECORD:")
        try:
            contact = builder._fetch_contact_record(cid)
            if contact:
                print(f"  firstName: {contact.get('firstName') or contact.get('FirstName')}")
                print(f"  lastName: {contact.get('lastName') or contact.get('LastName')}")
                print(f"  email: {contact.get('email')}")
                print(f"  phone: {contact.get('phone')}")
                print(f"  Full keys: {list(contact.keys())[:10]}...")
            else:
                print("  Not found")
        except Exception as e:
            print(f"  ERROR: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
