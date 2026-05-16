#!/usr/bin/env python3
"""Find which ContactId values have parseable GCLID notes."""
import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
for p in (str(ROOT), str(SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)


def load_contact_ids(path: Path) -> list[str]:
    ids: list[str] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = (row.get("ContactId") or row.get("contactId") or "").strip()
            if cid:
                ids.append(cid)
    return ids


def main() -> int:
    p = argparse.ArgumentParser(description="Find contacts with parseable GCLID notes")
    p.add_argument("--contacts-csv", default="Contacts (1).csv", help="CSV with ContactId column")
    p.add_argument("--limit", type=int, default=20, help="How many matching IDs to print")
    args = p.parse_args()

    contacts_csv = Path(args.contacts_csv)
    if not contacts_csv.exists():
        print(f"CSV not found: {contacts_csv}")
        return 1

    try:
        from gclid.gclid_sync import ReportBuilder
    except Exception as e:
        print(f"Error importing ReportBuilder: {e}")
        return 1

    def _make_builder():
        try:
            return ReportBuilder(auto_discover=False)
        except TypeError:
            return ReportBuilder()

    ids = load_contact_ids(contacts_csv)
    print(f"Loaded {len(ids)} contact IDs from {contacts_csv}")

    rb = _make_builder()
    matched = rb.contacts_with_gclid(contact_ids=ids)

    matched_ids = [m.get("contact_id", "").strip() for m in matched if isinstance(m, dict)]
    matched_ids = [m for m in matched_ids if m]

    print(f"Parseable GCLID contacts: {len(matched_ids)}/{len(ids)}")
    print("\nFirst matches:")
    for cid in matched_ids[: max(1, args.limit)]:
        print(cid)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
