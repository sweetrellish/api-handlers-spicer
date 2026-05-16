#!/usr/bin/env python3
"""Per-contact eligibility audit for GCLID export.

Outputs one row per ContactId with:
- parseable_gclid
- appointments/jobs found
- first/last event dates
- in-month eligible flag
- would_export flag (based on current row builder logic)
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit per-contact export eligibility")
    p.add_argument("--contacts-csv", default="Contacts (1).csv", help="CSV with ContactId column")
    p.add_argument("--month", default="2025-02", help="Month window YYYY-MM")
    p.add_argument("--out", default="eligibility_audit.csv", help="Output CSV path")
    p.add_argument("--limit", type=int, default=0, help="Optional max contacts to audit (0 = all)")
    return p.parse_args()


def month_window(month_str: str) -> tuple[datetime, datetime]:
    since = datetime.strptime(month_str, "%Y-%m")
    if since.month == 12:
        until = since.replace(year=since.year + 1, month=1)
    else:
        until = since.replace(month=since.month + 1)
    return since, until


def load_contacts(path: Path, limit: int = 0) -> list[dict]:
    contacts: list[dict] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = (row.get("ContactId") or row.get("contactId") or "").strip()
            if not cid:
                continue
            contacts.append({
                "contact_id": cid,
                "first_name": (row.get("First Name") or "").strip(),
                "last_name": (row.get("Last Name") or "").strip(),
            })
            if limit > 0 and len(contacts) >= limit:
                break
    return contacts


def to_clean_str(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_odataish(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None

    # OData format /Date(1741237200000)/
    if s.startswith("/Date(") and s.endswith(")/"):
        inner = s[6:-2]
        ms_txt = ""
        for ch in inner:
            if ch.isdigit() or (ch == "-" and not ms_txt):
                ms_txt += ch
            else:
                break
        if ms_txt:
            try:
                return datetime.fromtimestamp(int(ms_txt) / 1000, tz=timezone.utc).replace(tzinfo=None)
            except Exception:
                return None

    # ISO-like strings
    try:
        iso = s.replace("Z", "+00:00")
        return datetime.fromisoformat(iso).replace(tzinfo=None)
    except Exception:
        pass

    # Common fallback formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue

    return None


def event_date_candidates(record: dict) -> list[datetime]:
    keys = (
        "setDate", "SetDate", "appointmentDate", "AppointmentDate", "scheduledDate", "dateTime", "startDate",
        "saleDate", "SaleDate", "contractDate", "ContractDate", "soldDate", "SoldDate", "closeDate", "CloseDate",
        "completedDate", "CompletedDate", "createdDate", "CreatedDate", "enteredDate", "EnteredDate",
    )
    vals: list[datetime] = []
    for k in keys:
        if k in record:
            dt = parse_odataish(str(record.get(k) or ""))
            if dt is not None:
                vals.append(dt)
    return vals


def make_builder():
    from gclid.gclid_sync import ReportBuilder
    try:
        return ReportBuilder(auto_discover=False)
    except TypeError:
        return ReportBuilder()


def main() -> int:
    args = parse_args()
    contacts_csv = Path(args.contacts_csv)
    if not contacts_csv.exists():
        print(f"Contacts CSV not found: {contacts_csv}")
        return 1

    since, until = month_window(args.month)
    contacts = load_contacts(contacts_csv, limit=args.limit)
    if not contacts:
        print("No ContactId rows found.")
        return 1

    print(f"Loaded {len(contacts)} contacts from {contacts_csv}")
    print(f"Audit month window: {since.date()} to {until.date()}")

    builder = make_builder()

    ids = [c["contact_id"] for c in contacts]
    parsed_entries = builder.contacts_with_gclid(contact_ids=ids)
    by_id = {
        (e.get("contact_id") or "").strip(): e
        for e in parsed_entries
        if isinstance(e, dict) and (e.get("contact_id") or "").strip()
    }

    out_rows: list[dict] = []
    reason_counts = {
        "empty_or_unparseable_gclid": 0,
        "no_appointments_or_jobs": 0,
        "events_outside_month": 0,
        "rows_built_zero_despite_events": 0,
        "would_export": 0,
    }
    total = len(contacts)
    for idx, c in enumerate(contacts, start=1):
        cid = c["contact_id"]
        parseable = cid in by_id

        appts = builder.appointments_for_contact(cid)
        jobs = builder.jobs_for_contact(cid)

        event_dts: list[datetime] = []
        for a in appts:
            event_dts.extend(event_date_candidates(a if isinstance(a, dict) else {}))
        for j in jobs:
            event_dts.extend(event_date_candidates(j if isinstance(j, dict) else {}))

        first_event = min(event_dts) if event_dts else None
        last_event = max(event_dts) if event_dts else None

        in_month = any((since <= dt < until) for dt in event_dts)

        rows_built = 0
        if parseable:
            try:
                built = builder._build_rows_for_contact(by_id[cid], since=since, until=until)
                rows_built = len(built)
            except Exception:
                rows_built = -1

        reason = ""
        if not parseable:
            reason = "empty_or_unparseable_gclid"
        elif len(appts) == 0 and len(jobs) == 0:
            reason = "no_appointments_or_jobs"
        elif not in_month:
            reason = "events_outside_month"
        elif rows_built == 0:
            reason = "rows_built_zero_despite_events"
        else:
            reason = "would_export"

        if reason in reason_counts:
            reason_counts[reason] += 1

        out_rows.append({
            "contact_id": cid,
            "first_name": c["first_name"],
            "last_name": c["last_name"],
            "parseable_gclid": "Y" if parseable else "N",
            "gclid": ((by_id.get(cid, {}).get("fields", {}) or {}).get("gclid", "") if parseable else ""),
            "appointments_found": len(appts),
            "jobs_found": len(jobs),
            "first_event_date": to_clean_str(first_event),
            "last_event_date": to_clean_str(last_event),
            "has_event_in_month": "Y" if in_month else "N",
            "rows_built_in_month": rows_built,
            "would_export": "Y" if rows_built > 0 else "N",
            "exclusion_reason": reason,
        })

        if idx % 25 == 0 or idx == total:
            print(f"Processed {idx}/{total} contacts")

    out_path = Path(args.out)
    fields = [
        "contact_id",
        "first_name",
        "last_name",
        "parseable_gclid",
        "gclid",
        "appointments_found",
        "jobs_found",
        "first_event_date",
        "last_event_date",
        "has_event_in_month",
        "rows_built_in_month",
        "would_export",
        "exclusion_reason",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)

    parseable_n = sum(1 for r in out_rows if r["parseable_gclid"] == "Y")
    in_month_n = sum(1 for r in out_rows if r["has_event_in_month"] == "Y")
    would_export_n = sum(1 for r in out_rows if r["would_export"] == "Y")

    print("\nAudit Summary")
    print("=" * 60)
    print(f"Contacts audited:         {len(out_rows)}")
    print(f"Parseable GCLID:         {parseable_n}")
    print(f"Has events in month:     {in_month_n}")
    print(f"Would export rows:       {would_export_n}")
    print("\nExclusion / outcome breakdown:")
    print(f"  empty_or_unparseable_gclid:   {reason_counts['empty_or_unparseable_gclid']}")
    print(f"  no_appointments_or_jobs:      {reason_counts['no_appointments_or_jobs']}")
    print(f"  events_outside_month:         {reason_counts['events_outside_month']}")
    print(f"  rows_built_zero_despite_events:{reason_counts['rows_built_zero_despite_events']}")
    print(f"  would_export:                 {reason_counts['would_export']}")
    print(f"Audit CSV:               {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
