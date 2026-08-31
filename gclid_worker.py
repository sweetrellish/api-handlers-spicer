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
import json
import logging
import os
import subprocess
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
VALIDATE_AFTER_EXPORT_DEFAULT = os.getenv("GCLID_VALIDATE_AFTER_EXPORT", "0").strip().lower() in ("1", "true", "yes", "on")
VALIDATE_STRICT_DEFAULT = os.getenv("GCLID_VALIDATE_STRICT", "1").strip().lower() in ("1", "true", "yes", "on")
MIN_ROWS_DEFAULT = max(0, int(os.getenv("GCLID_MIN_ROWS", "0")))
STRICT_MIN_ROWS_DEFAULT = os.getenv("GCLID_MIN_ROWS_STRICT", "0").strip().lower() in ("1", "true", "yes", "on")


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


def _run_export_validations(csv_path: str) -> bool:
    """Run validation suite against a specific export CSV."""
    script_dir = os.path.dirname(__file__)
    validator = os.path.join(script_dir, "validate_all.py")
    if not os.path.exists(validator):
        log.error("Validation script not found: %s", validator)
        return False

    cmd = [sys.executable, validator, "--csv", csv_path]
    log.info("Running post-export validations: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd, cwd=script_dir)
    except Exception as exc:
        log.error("Failed to run post-export validations: %s", exc)
        return False

    if result.returncode == 0:
        log.info("Post-export validations passed")
        return True

    log.error("Post-export validations failed with exit code %d", result.returncode)
    return False


def _log_run_summary(label: str, out_path: str, rows_written: int, rows_built: int,
                     validation_passed: bool | None, audit_stats: dict | None,
                     run_stats: dict | None) -> None:
    """Emit a compact operator-facing summary for the completed run."""
    validation_text = (
        "skipped" if validation_passed is None
        else ("passed" if validation_passed else "failed")
    )
    audit_text = "audit skipped"
    if audit_stats:
        audit_text = (
            f"audit pages={audit_stats.get('pages', 0)} "
            f"parseable={audit_stats.get('parseable_inquiries_in_range', 0)} "
            f"raw={audit_stats.get('raw_inquiries_in_range', 0)} "
            f"unique_contacts={audit_stats.get('unique_contacts_in_range', 0)}"
        )

    metrics_text = "metrics unavailable"
    if run_stats:
        metrics_text = (
            f"entity_probes={run_stats.get('entity_probes', 0)} "
            f"probe_200={run_stats.get('entity_probe_200', 0)} "
            f"probe_400={run_stats.get('entity_probe_400', 0)} "
            f"probe_404={run_stats.get('entity_probe_404', 0)} "
            f"transient_retries={run_stats.get('transient_retries', 0)} "
            f"retry_errors={run_stats.get('retryable_request_errors', 0)} "
            f"note_fallback_hits={run_stats.get('note_fallback_hits', 0)} "
            f"note_fallback_disabled={'yes' if run_stats.get('note_fallback_disabled') else 'no'}"
        )

    log.info(
        "Run summary: month=%s rows_built=%d rows_written=%d validation=%s %s output=%s",
        label,
        rows_built,
        rows_written,
        validation_text,
        audit_text,
        out_path,
    )
    log.info("Run metrics: %s", metrics_text)


def _write_run_manifest(label: str, out_path: str, rows_written: int, rows_built: int,
                        validation_passed: bool | None, audit_stats: dict | None,
                        run_stats: dict | None) -> str:
    """Write a small JSON manifest beside the export for quick post-run review."""
    manifest_path = out_path.replace(".csv", ".summary.json")
    payload = {
        "month": label,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "output_csv": out_path,
        "rows_built": rows_built,
        "rows_written": rows_written,
        "validation": {
            "passed": validation_passed,
        },
        "audit": audit_stats or {},
        "run_metrics": run_stats or {},
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    return manifest_path


def _write_inquiry_audit(mod, out_dir: str, label: str,
                         since: datetime | None = None,
                         until: datetime | None = None) -> tuple[str, dict]:
    """Write a backend-only inquiry audit CSV for reconciliation."""
    rb = mod.ReportBuilder()
    if not getattr(rb, "_svc", None):
        raise RuntimeError("MarketSharp service unavailable for inquiry audit")

    inq_top = int(getattr(mod, "GCLID_INQUIRY_TOP", 5000))
    url = f"{mod._odata_url()}/Inquiries"
    flt = (
        "(substringof('GCLID:',note) or substringof('gclid:',note) or substringof('GCLID=',note) or "
        "substringof('gclid=',note) or substringof('[GCLID]',note) or substringof('GCLID',note) or "
        "substringof('gclid',note) or substringof('Google Click ID',note) or substringof('google click id',note))"
    )
    params = {"$filter": flt, "$top": str(inq_top)}

    rows: list[dict] = []
    stats = {
        "pages": 0,
        "raw_inquiries_in_range": 0,
        "parseable_inquiries_in_range": 0,
        "empty_or_unparseable_in_range": 0,
        "unique_contacts_in_range": 0,
    }
    unique_contacts: set[str] = set()

    next_url: str | None = url
    page = 0
    while next_url:
        page += 1
        resp = mod._req.get(
            next_url,
            headers=rb._svc._odata_headers(),
            params=params if page == 1 else None,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("d", {})
        items = data.get("results", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            break

        for inq in items:
            dt_raw = mod._inquiry_date(inq)
            dt_clean = mod._clean_date(dt_raw) if dt_raw else ""
            if not dt_clean:
                continue
            try:
                dt = datetime.strptime(dt_clean[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
            if since and dt < since:
                continue
            if until and dt >= until:
                continue

            txt = mod._inquiry_text(inq)
            fields = mod._parse_gclid_note(txt)
            cid = str(inq.get("contactId") or inq.get("ContactId") or "")
            unique_contacts.add(cid)
            stats["raw_inquiries_in_range"] += 1
            rows.append({
                "contact_id": cid,
                "inquiry_date": dt_clean,
                "parseable_gclid": "Y" if fields.get("gclid") else "N",
                "gclid": fields.get("gclid", ""),
                "utm_source": fields.get("utm_source", ""),
                "utm_medium": fields.get("utm_medium", ""),
                "utm_campaign": fields.get("utm_campaign", ""),
                "utm_term": fields.get("utm_term", ""),
                "utm_content": fields.get("utm_content", ""),
                "note_excerpt": txt[:250].replace("\n", " | "),
                "reason": "would_export" if fields.get("gclid") else "empty_or_unparseable_gclid",
            })
            if fields.get("gclid"):
                stats["parseable_inquiries_in_range"] += 1
            else:
                stats["empty_or_unparseable_in_range"] += 1

        next_url = data.get("__next") if isinstance(data, dict) else None

    stats["pages"] = page
    stats["unique_contacts_in_range"] = len({c for c in unique_contacts if c})

    out_path = os.path.join(out_dir, f"inquiry_audit_{label}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "contact_id",
                "inquiry_date",
                "parseable_gclid",
                "gclid",
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "note_excerpt",
                "reason",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return out_path, stats


# ---------------------------------------------------------------------------
# Report runner
# ---------------------------------------------------------------------------
def run_report(since: datetime | None = None, month_label: str | None = None,
               until: datetime | None = None,
               diagnostic_contact_fields: bool = False,
               diagnostic_limit: int = 10,
               validate_output: bool = False,
               validate_strict: bool = True,
               min_rows: int = 0,
               strict_min_rows: bool = False) -> str | None:
    """Build the conversion CSV for *since* → *until* (or now if until is None).

    Returns the output file path on success, None on failure.
    """
    mod = _load_gclid_module()
    os.makedirs(OUT_DIR, exist_ok=True)

    now   = datetime.now()
    label = month_label or (since.strftime("%Y-%m") if since else now.strftime("%Y-%m"))
    out_path = os.path.join(OUT_DIR, f"spicer_conversions_{label.replace('-', '')}.csv")

    log.info("Building GCLID conversion report  since=%s  out=%s",
             since.isoformat() if since else "all-time", out_path)
    log.info("Contact discovery mode: backend auto")

    try:
        rb = mod.ReportBuilder()
        audit_stats: dict | None = None
        validation_passed: bool | None = None

        if diagnostic_contact_fields:
            # Backend-only diagnostic: sample discovered contacts and log raw fields.
            sample_contacts = rb.contacts_with_gclid(contact_ids=None)
            ids = [
                (c.get("contact_id") or "").strip()
                for c in sample_contacts
                if isinstance(c, dict)
            ]
            ids = [cid for cid in ids if cid]
            ids = ids[: max(0, diagnostic_limit)]
            log.info("Diagnostic mode: inspecting %d backend-discovered contact(s)", len(ids))
            for cid in ids:
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

        # Standalone engine: discovery is driven entirely by inquiry note parsing.
        rows = rb.build_conversion_rows(since=since, until=until, contact_ids=None)
        rows_built = len(rows)

        if not rows:
            log.warning("No GCLID contacts / conversions found for %s", label)
            _log_run_summary(label, out_path, 0, 0, None, None, getattr(rb, "_run_stats", None))
            return None
        written = mod.CSVExporter().export(rows, out_path)
        log.info("Exported %d conversion rows → %s", written, out_path)

        if written < max(0, min_rows):
            log.warning("Export row count %d is below configured minimum %d", written, min_rows)
            if strict_min_rows:
                log.error("Minimum-row safeguard failed in strict mode; treating report run as failed")
                return None

        if validate_output:
            validation_passed = _run_export_validations(out_path)
            if not validation_passed and validate_strict:
                log.error("Validation failed in strict mode; treating report run as failed")
                return None

        # Backend-only reconciliation trail: inquiry cohort audit for the same month.
        try:
            audit_path, audit_stats = _write_inquiry_audit(mod, OUT_DIR, label, since=since, until=until)
            log.info(
                "Inquiry audit: pages=%d raw_inquiries=%d parseable=%d empty_or_unparseable=%d unique_contacts=%d",
                audit_stats["pages"],
                audit_stats["raw_inquiries_in_range"],
                audit_stats["parseable_inquiries_in_range"],
                audit_stats["empty_or_unparseable_in_range"],
                audit_stats["unique_contacts_in_range"],
            )
            log.info("Exported inquiry audit → %s", audit_path)
        except Exception as audit_exc:
            log.warning("Inquiry audit export failed: %s", audit_exc)

        manifest_path = _write_run_manifest(
            label,
            out_path,
            written,
            rows_built,
            validation_passed,
            audit_stats,
            getattr(rb, "_run_stats", None),
        )
        log.info("Exported run manifest → %s", manifest_path)
        _log_run_summary(label, out_path, written, rows_built, validation_passed, audit_stats, getattr(rb, "_run_stats", None))
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
        "--diagnostic-contact-fields", action="store_true",
        help="Print raw field keys plus extracted email/phone for sampled backend contacts and exit.",
    )
    p.add_argument(
        "--diagnostic-limit", type=int, default=10,
        help="Maximum number of backend contacts to inspect in diagnostic mode.",
    )
    p.add_argument(
        "--validate-output", action="store_true", default=VALIDATE_AFTER_EXPORT_DEFAULT,
        help="Run validation suite against the generated CSV after export.",
    )
    p.add_argument(
        "--non-strict-validation", action="store_true",
        help="Log validation failures without failing the report command.",
    )
    p.add_argument(
        "--min-rows", type=int, default=MIN_ROWS_DEFAULT,
        help="Warn if exported conversion rows fall below this minimum.",
    )
    p.add_argument(
        "--strict-min-rows", action="store_true", default=STRICT_MIN_ROWS_DEFAULT,
        help="Treat min-row safeguard failures as command failures.",
    )
    args = p.parse_args()

    if args.out_dir:
        OUT_DIR = args.out_dir

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
                        diagnostic_contact_fields=args.diagnostic_contact_fields,
                        diagnostic_limit=args.diagnostic_limit,
                        validate_output=args.validate_output,
                        validate_strict=not args.non_strict_validation,
                        min_rows=args.min_rows,
                        strict_min_rows=args.strict_min_rows)
    sys.exit(0 if result else 1)


if __name__ == "__main__":
    main()
