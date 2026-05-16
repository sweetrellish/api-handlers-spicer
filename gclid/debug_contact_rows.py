#!/usr/bin/env python3
"""Debug per-contact GCLID data extraction and row building."""
import argparse
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

def main():
    p = argparse.ArgumentParser(description="Debug per-contact row generation")
    p.add_argument("contact_ids", nargs="+", help="One or more ContactId values")
    p.add_argument("--month", default="2025-02", help="Month window YYYY-MM (default: 2025-02)")
    args = p.parse_args()

    if not args.contact_ids:
        print("Usage: python debug_contact_rows.py <contact_id> [contact_id2 ...]")
        print("\nExample:")
        print("  python debug_contact_rows.py 767aa2a7-ec31-4c08-8876-3a4c8911319e")
        print("\nThis will show:")
        print("  - Contact details (name, email, phone, GCLID, UTM fields)")
        print("  - Appointments found and dates")
        print("  - Jobs found and dates")
        print("  - Rows that would be generated")
        print("  - Why rows might be filtered or skipped")
        return 1
    
    contact_ids = [c.strip() for c in args.contact_ids if c.strip()]

    # Month range for testing
    since = datetime.strptime(args.month, "%Y-%m")
    if since.month == 12:
        until = since.replace(year=since.year + 1, month=1)
    else:
        until = since.replace(month=since.month + 1)

    try:
        from gclid.gclid_sync import ReportBuilder
    except Exception as e:
        print(f"Error importing ReportBuilder: {e}")
        return 1
    
    builder = ReportBuilder()
    
    print("=" * 80)
    print("GCLID Contact Row Generation Debug")
    print("=" * 80)
    print(f"Date range: {since.date()} to {until.date()}\n")
    
    for cid in contact_ids:
        print(f"\n--- Contact: {cid} ---\n")
        
        # Get contact with GCLID
        try:
            all_contacts = builder.contacts_with_gclid(contact_ids=[cid])
            if not all_contacts:
                print("  NOT FOUND in contacts_with_gclid")
                print("\nINQUIRY FALLBACK DIAGNOSTICS:")
                try:
                    inquiries = builder.inquiries_for_contact(cid)
                except Exception as ex:
                    inquiries = []
                    print(f"  inquiry fetch error: {type(ex).__name__}: {ex}")
                print(f"  inquiries_found: {len(inquiries)}")
                for i, inq in enumerate(inquiries[:5]):
                    text = (
                        inq.get("note")
                        or inq.get("Note")
                        or inq.get("description")
                        or inq.get("Description")
                        or inq.get("comments")
                        or inq.get("Comments")
                        or inq.get("activity")
                        or inq.get("Activity")
                        or ""
                    )
                    dt = (
                        inq.get("dateTime")
                        or inq.get("DateTime")
                        or inq.get("createdDate")
                        or inq.get("CreatedDate")
                        or inq.get("enteredDate")
                        or inq.get("EnteredDate")
                        or ""
                    )
                    text_preview = " ".join(str(text).split())[:200]
                    has_gclid_hint = ("gclid=" in str(text).lower()) or ("[gclid]" in str(text).lower())
                    print(f"    [{i+1}] date={dt!r} has_gclid_hint={has_gclid_hint} text={text_preview!r}")
                continue
            
            entry = all_contacts[0]
            print("CONTACT INFO:")
            print(f"  contact_id: {entry.get('contact_id')}")
            print(f"  contact_name: {entry.get('contact_name')}")
            
            fields = entry.get('fields', {})
            print(f"\nFIELDS:")
            print(f"  GCLID: {fields.get('gclid')}")
            print(f"  utm_source: {fields.get('utm_source')}")
            print(f"  utm_medium: {fields.get('utm_medium')}")
            print(f"  utm_campaign: {fields.get('utm_campaign')}")
            
            # Appointments
            print(f"\nAPPOINTMENTS:")
            appts = builder.appointments_for_contact(cid)
            print(f"  Total found: {len(appts)}")
            for i, a in enumerate(appts[:5]):
                appt_date = (
                    a.get("setDate") or a.get("SetDate") or 
                    a.get("appointmentDate") or a.get("AppointmentDate") or
                    a.get("scheduledDate") or a.get("dateTime") or ""
                )
                print(f"    [{i+1}] date={appt_date}")
            if len(appts) > 5:
                print(f"    ... and {len(appts) - 5} more")
            
            # Jobs
            print(f"\nJOBS:")
            jobs = builder.jobs_for_contact(cid)
            print(f"  Total found: {len(jobs)}")
            for i, j in enumerate(jobs[:5]):
                sold_date = (
                    j.get('saleDate') or j.get('SaleDate') or
                    j.get('contractDate') or j.get('ContractDate') or
                    j.get('soldDate') or j.get('SoldDate') or ""
                )
                print(f"    [{i+1}] saleDate={sold_date}")
            if len(jobs) > 5:
                print(f"    ... and {len(jobs) - 5} more")
            
            # Generate rows
            print(f"\nGENERATED ROWS:")
            rows = builder._build_rows_for_contact(entry, since=since, until=until)
            print(f"  Total: {len(rows)}")
            for i, row in enumerate(rows):
                print(f"    [{i+1}] {row.get('conversion_type'):20} {row.get('conversion_date'):35} value={row.get('revenue')}")
            
            if not rows:
                print("\n  ⚠ No rows generated. Possible reasons:")
                if not appts and not jobs:
                    print("    - No appointments or jobs found in MarketSharp")
                else:
                    print("    - Appointments/jobs exist but fall outside date range")
                    print(f"    - Date range: {since.date()} to {until.date()}")
                    if appts:
                        dates = []
                        for a in appts:
                            d = a.get("setDate") or a.get("appointmentDate") or ""
                            if d:
                                dates.append(d)
                        if dates:
                            print(f"    - Appointment dates: {dates[:3]}")
                    if jobs:
                        dates = []
                        for j in jobs:
                            d = j.get('saleDate') or j.get('contractDate') or ""
                            if d:
                                dates.append(d)
                        if dates:
                            print(f"    - Job dates: {dates[:3]}")
        
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
