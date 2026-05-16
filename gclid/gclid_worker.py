#!/usr/bin/env python3
"""gclid_worker.py — Monthly GCLID / Google Ads offline-conversion report runner.

Runs as a systemd oneshot (triggered by a .timer on the 1st of every month) or
can be invoked directly.  Queries MarketSharp for contacts whose notes contain a
GCLID value (written by website email-trigger automation), builds a CSV in the
Google Ads offline-conversion upload format, and saves it to GCLID_REPORT_OUT_DIR.

Usage
-----
    # One-shot: generate last month's report
    python3 gclid_worker.py

    # Daemon mode: runs indefinitely, wakes on the 1st of each month at 06:00
    python3 gclid_worker.py --daemon

    # Generate for a specific month  (YYYY-MM)
    python3 gclid_worker.py --month 2026-04

Environment variables (set in .env or systemd EnvironmentFile)
---------------------------------------------------------------
    GCLID_REPORT_OUT_DIR   Directory for CSV output  (default: ./data)
    GCLID_REPORT_DAY       Day of month to run       (default: 1)
    GCLID_REPORT_HOUR      Hour (24 h) to run        (default: 6)
    MARKETSHARP_GCLID_WRITE_MODE  note | customfield  (default: note)
    SPICER_CURRENCY        Currency code              (default: USD)
"""

import argparse
import csv
import importlib.util
import logging
import os
import sys
import time
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Bootstrap: load .env and configure logging
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gclid_worker] %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gclid_worker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUT_DIR    = os.getenv("GCLID_REPORT_OUT_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
REPORT_DAY = int(os.getenv("GCLID_REPORT_DAY",  "1"))
REPORT_HR  = int(os.getenv("GCLID_REPORT_HOUR", "6"))


# ---------------------------------------------------------------------------
# Load gclid_sync module (handles the dash in "gclid-ms.py" filename)
# ---------------------------------------------------------------------------
def _load_gclid_module():
    """Locate and load gclid_sync.py (preferred) or gclid-ms.py (fallback)."""
    candidates = [
        os.path.join(os.path.dirname(__file__), "gclid_sync.py"),
        os.path.join(os.path.dirname(__file__), "..", "gclid-ms.py"),
    ]
    for path in candidates:
        if os.path.exists(path):
            spec = importlib.util.spec_from_file_location("gclid_ms", path)
            if spec is None or spec.loader is None:
                log.error("Failed to load gclid module from %s: spec or loader is None", path)
                continue
            mod  = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            log.debug("Loaded gclid module from %s", path)
            return mod
    raise FileNotFoundError("gclid_sync.py / gclid-ms.py not found — check install")


# ---------------------------------------------------------------------------
# Report runner
# ---------------------------------------------------------------------------
def run_report(since: datetime | None = None, month_label: str | None = None,
               until: datetime | None = None,
               contact_ids: list | None = None,
               csv_contacts: list[dict] | None = None,
               contacts_mode: str = "auto",
               diagnostic_contact_fields: bool = False,
               diagnostic_limit: int = 10) -> str | None:
    """Build the conversion CSV for *since* → *until* (or now if until is None).

        contacts_mode:
            - auto:      standalone engine (query inquiry notes directly)
            - csv-only:  only use provided ContactId values
            - assist:    auto-discovered IDs union CSV IDs, then fetch by ID

    Returns the output file path on success, None on failure.
    """
    mod = _load_gclid_module()
    os.makedirs(OUT_DIR, exist_ok=True)

    now   = datetime.now()
    label = month_label or (since.strftime("%Y-%m") if since else now.strftime("%Y-%m"))
    out_path = os.path.join(OUT_DIR, f"spicer_conversions_{label.replace('-', '')}.csv")

    log.info("Building GCLID conversion report  since=%s  out=%s",
             since.isoformat() if since else "all-time", out_path)
    if contacts_mode == "csv-only":
        log.info("Contact discovery mode: csv-only")
    elif contacts_mode == "assist":
        log.info("Contact discovery mode: assist")
    else:
        log.info("Contact discovery mode: auto")

    try:
        rb = mod.ReportBuilder()

        if diagnostic_contact_fields:
            ids = list(dict.fromkeys(contact_ids or []))
            log.info("Diagnostic mode: inspecting %d contact(s)", len(ids))
            for idx, cid in enumerate(ids[: max(0, diagnostic_limit)]):
                raw = rb._fetch_contact_record(cid)
                if not isinstance(raw, dict):
                    log.info("DIAG %s: no contact record returned", cid)
                    continue
                email, phone = rb._contact_email_phone(raw)
                keys = sorted(str(k) for k in raw.keys())
                key_preview = ", ".join(keys[:25])
                contact_phone_id = raw.get("contactPhoneId") or raw.get("ContactPhoneId") or ""
                contact_phone = raw.get("ContactPhone") or raw.get("contactPhone") or ""
                contact_phone_keys = ""
                if isinstance(contact_phone, dict):
                    contact_phone_keys = ", ".join(sorted(str(k) for k in contact_phone.keys())[:25])
                log.info(
                    "DIAG %s: email=%r phone=%r contactPhoneId=%r ContactPhoneType=%s ContactPhoneKeys=%s keys=%s",
                    cid,
                    email,
                    phone,
                    contact_phone_id,
                    type(contact_phone).__name__,
                    contact_phone_keys,
                    key_preview,
                )
            return "diagnostic"

        if contacts_mode == "csv-only":
            csv_ids = sorted(set(contact_ids or []))
            log.info("Using CSV-driven contact list: %d contact IDs", len(csv_ids))
            csv_contacts = rb.contacts_with_gclid(contact_ids=csv_ids)
            rows = rb.build_conversion_rows_from_contacts(csv_contacts, since=since, until=until)
        elif contacts_mode == "assist":
            csv_ids = set(contact_ids or [])
            if csv_ids:
                log.info("CSV assist list loaded: %d contact IDs", len(csv_ids))

            csv_contacts = csv_contacts or []
            # Keep the assist path conservative: only CSV contacts that already
            # resolved to a parseable GCLID are treated as eligible seeds.
            csv_with_gclid = [
                c for c in csv_contacts
                if isinstance(c, dict) and (c.get("fields") or {}).get("gclid")
            ]

            auto_contacts = rb.contacts_with_gclid(contact_ids=None)
            auto_by_id = {
                e.get("contact_id", "").strip(): e
                for e in auto_contacts
                if isinstance(e, dict) and e.get("contact_id")
            }

            # Keep assist mode scoped to CSV contacts when CSV is supplied.
            # Auto-discovery acts as a helper source for those same contact IDs.
            final_contacts: list[dict] = []
            seen: set[str] = set()

            for entry in csv_with_gclid:
                cid = (entry.get("contact_id") or "").strip()
                if cid and cid not in seen:
                    final_contacts.append(entry)
                    seen.add(cid)

            for cid in sorted(csv_ids):
                if not cid or cid in seen:
                    continue
                cached = auto_by_id.get(cid)
                if cached:
                    final_contacts.append(cached)
                    seen.add(cid)

            missing_csv_ids = sorted(cid for cid in csv_ids if cid and cid not in seen)
            log.info(
                "Assist merge: auto=%d csv=%d csv_with_gclid=%d resolved_before_fetch=%d still_missing=%d",
                len(auto_by_id), len(csv_ids), len(csv_with_gclid), len(seen), len(missing_csv_ids)
            )

            extra_contacts = []
            if missing_csv_ids:
                log.info("Attempting per-contact fetch for %d missing CSV IDs...", len(missing_csv_ids))
                try:
                    extra_contacts = rb.contacts_with_gclid(contact_ids=missing_csv_ids)
                    for entry in extra_contacts:
                        cid = (entry.get("contact_id") or "").strip()
                        if cid and cid not in seen:
                            final_contacts.append(entry)
                            seen.add(cid)
                    if extra_contacts:
                        log.info("Per-contact fetch succeeded: got %d contacts", len(extra_contacts))
                    else:
                        log.info("Per-contact fetch completed but yielded no contacts; using auto-discovered set")
                except Exception as exc:
                    log.warning("Per-contact fetch failed or timed out: %s; using auto-discovered set", exc)

            # No CSV provided: preserve historical assist behavior.
            if not csv_ids:
                final_contacts = auto_contacts + extra_contacts

            rows = rb.build_conversion_rows_from_contacts(
                final_contacts,
                since=since,
                until=until,
            )
        else:
            # Standalone engine: discovery is driven entirely by inquiry note parsing.
            rows = rb.build_conversion_rows(since=since, until=until, contact_ids=None)

        if not rows:
            log.warning("No GCLID contacts / conversions found for %s", label)
            return None
        written = mod.CSVExporter().export(rows, out_path)
        log.info("Exported %d conversion rows → %s", written, out_path)
        return out_path
    except Exception as exc:
        log.error("Report failed: %s", exc, exc_info=True)
        return None


def _last_month_since() -> tuple[datetime, str]:
    """Return (datetime for 1st of last month, 'YYYY-MM' label)."""
    first_this = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_m     = first_this - timedelta(days=1)
    since      = last_m.replace(day=1)
    return since, last_m.strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Daemon loop
# ---------------------------------------------------------------------------
def _next_run_dt() -> datetime:
    """Return the next datetime when the report should run."""
    now  = datetime.now()
    candidate = now.replace(day=REPORT_DAY, hour=REPORT_HR, minute=0, second=0, microsecond=0)
    if candidate <= now:
        # Advance to next month
        if candidate.month == 12:
            candidate = candidate.replace(year=candidate.year + 1, month=1)
        else:
            candidate = candidate.replace(month=candidate.month + 1)
    return candidate


def daemon_loop():
    log.info("GCLID worker daemon started — will run on day %d at %02d:00 each month",
             REPORT_DAY, REPORT_HR)
    while True:
        next_run = _next_run_dt()
        sleep_s  = max(0, (next_run - datetime.now()).total_seconds())
        log.info("Next report scheduled for %s  (%.1f h from now)",
                 next_run.strftime("%Y-%m-%d %H:%M"), sleep_s / 3600)
        time.sleep(sleep_s)

        since, label = _last_month_since()
        run_report(since=since, month_label=label)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    global OUT_DIR
    p = argparse.ArgumentParser(
        description="Monthly GCLID / Google Ads offline-conversion report runner",
    )
    p.add_argument(
        "--daemon", action="store_true",
        help="Run as a long-lived daemon; wakes monthly to generate the report",
    )
    p.add_argument(
        "--month", metavar="YYYY-MM",
        help="Generate report for a specific month instead of last month",
    )
    p.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Generate report for all conversions on or after this date",
    )
    p.add_argument(
        "--out-dir", metavar="DIR", default=None,
        help=f"Override output directory (default: {OUT_DIR})",
    )
    p.add_argument(
        "--contacts-csv", metavar="FILE", default=None,
        help=(
            "CSV exported from MarketSharp's contact filter (must have a 'ContactId' column). "
            "Used by --contacts-mode csv-only or assist."
        ),
    )
    p.add_argument(
        "--contacts-mode", choices=["auto", "assist", "csv-only"], default="auto",
        help=(
            "Contact discovery strategy: auto (standalone inquiry-note scan), "
            "assist (auto + CSV IDs), csv-only (CSV IDs only). "
            "If --contacts-csv is provided and mode is left as auto, assist is used."
        ),
    )
    p.add_argument(
        "--diagnostic-contact-fields", action="store_true",
        help="Print the raw field keys plus extracted email/phone for each CSV contact and exit.",
    )
    p.add_argument(
        "--diagnostic-limit", type=int, default=10,
        help="Maximum number of CSV contacts to inspect in diagnostic mode.",
    )
    args = p.parse_args()

    if args.out_dir:
        OUT_DIR = args.out_dir

    # Load contact IDs from the MS export CSV if provided
    contact_ids: list | None = None
    csv_contacts: list[dict] | None = None
    if args.contacts_csv:
        contact_ids = []
        csv_contacts = []
        try:
            with open(args.contacts_csv, newline="", encoding="utf-8-sig") as f:
                lines = f.readlines()
            # MarketSharp exports have a report-title row (e.g. "Contacts") before
            # the actual column-header row.  Scan forward to find the real header.
            header_idx = next(
                (i for i, l in enumerate(lines) if "ContactId" in l or "contactId" in l),
                None,
            )
            if header_idx is None:
                p.error(f"--contacts-csv: no ContactId column found in {args.contacts_csv}")
            import io as _io
            reader = csv.DictReader(_io.StringIO("".join(lines[header_idx:])))

            def _first_nonempty(row: dict, keys: list[str]) -> str:
                for k in keys:
                    v = (row.get(k) or "").strip()
                    if v:
                        return v
                return ""

            for row in reader:
                cid = (row.get("ContactId") or row.get("contactId") or
                       row.get("contact_id") or "").strip()
                if cid:
                    contact_ids.append(cid)

                    # Optional enrichment: if CSV already contains GCLID/UTMs,
                    # pass them through directly so note parsing is not required.
                    gclid = _first_nonempty(row, [
                        "GCLID", "gclid", "Google Click ID", "google_click_id"
                    ])
                    utm_source = _first_nonempty(row, ["utm_source", "UTM Source", "Source"])
                    utm_medium = _first_nonempty(row, ["utm_medium", "UTM Medium", "Medium"])
                    utm_campaign = _first_nonempty(row, ["utm_campaign", "UTM Campaign", "Campaign"])
                    utm_term = _first_nonempty(row, ["utm_term", "UTM Term", "Term"])
                    utm_content = _first_nonempty(row, ["utm_content", "UTM Content", "Content"])

                    if gclid:
                        csv_contacts.append({
                            "contact_id": cid,
                            "fields": {
                                "gclid": gclid,
                                "utm_source": utm_source,
                                "utm_medium": utm_medium,
                                "utm_campaign": utm_campaign,
                                "utm_term": utm_term,
                                "utm_content": utm_content,
                            },
                        })
            if not contact_ids:
                p.error(f"--contacts-csv: no ContactId values found in {args.contacts_csv}")
            log.info("Loaded %d contact IDs from %s", len(contact_ids), args.contacts_csv)
            if csv_contacts:
                log.info("CSV includes %d contact rows with direct GCLID values", len(csv_contacts))
        except FileNotFoundError:
            p.error(f"--contacts-csv: file not found: {args.contacts_csv}")

    contacts_mode = args.contacts_mode
    if contacts_mode == "auto" and args.contacts_csv:
        contacts_mode = "assist"
    if contacts_mode in ("assist", "csv-only") and not contact_ids:
        p.error("--contacts-mode assist/csv-only requires --contacts-csv")

    if args.daemon:
        daemon_loop()
        return

    if args.month:
        try:
            since = datetime.strptime(args.month, "%Y-%m")
            label = args.month
            # Upper bound: first moment of the following month
            if since.month == 12:
                until = since.replace(year=since.year + 1, month=1)
            else:
                until = since.replace(month=since.month + 1)
        except ValueError:
            p.error("--month must be YYYY-MM format")
    elif args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d")
            label = since.strftime("%Y-%m")
            until = None
        except ValueError:
            p.error("--since must be YYYY-MM-DD format")
    else:
        since, label = _last_month_since()
        # For default last-month run, also cap at start of current month
        until = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    result = run_report(since=since, month_label=label, until=until,
                        contact_ids=contact_ids, csv_contacts=csv_contacts,
                        contacts_mode=contacts_mode,
                        diagnostic_contact_fields=args.diagnostic_contact_fields,
                        diagnostic_limit=args.diagnostic_limit)
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
