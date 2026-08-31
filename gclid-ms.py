"""
gclid-ms.py -- GCLID / UTM -> MarketSharp custom field sync
              + monthly Google Ads offline-conversion export

Commands
--------
  write   Push GCLID/UTM fields onto an existing MarketSharp contact
  sync    Resolve contact by name then write fields
  report  Monthly CSV export formatted for Google Ads offline conversions
  fields  Read back stored field values for a contact

Quick start
-----------
  python gclid-ms.py write --contact-id <id> --gclid AW-xxx
  python gclid-ms.py sync  --name "Jane Smith" --gclid AW-xxx --utm-source google
  python gclid-ms.py report --since 2026-04-01 --out may_conversions.csv
  python gclid-ms.py fields --contact-id <id>
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    import requests as _req
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] gclid-ms -- %(message)s",
)
log = logging.getLogger("gclid-ms")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# "note"        -> structured note on the contact (no setup, always works)
# "customfield" -> POST to ContactCustomFields entity (needs FIELD_IDS below)
GCLID_WRITE_MODE = os.getenv("MARKETSHARP_GCLID_WRITE_MODE", "note").lower()

FIELD_IDS: dict = {
    "gclid":        os.getenv("MS_FIELD_ID_GCLID", ""),
    "utm_source":   os.getenv("MS_FIELD_ID_UTM_SOURCE", ""),
    "utm_medium":   os.getenv("MS_FIELD_ID_UTM_MEDIUM", ""),
    "utm_campaign": os.getenv("MS_FIELD_ID_UTM_CAMPAIGN", ""),
    "utm_term":     os.getenv("MS_FIELD_ID_UTM_TERM", ""),
    "utm_content":  os.getenv("MS_FIELD_ID_UTM_CONTENT", ""),
}

ADS_CONV_APPOINTMENT = os.getenv("GADS_CONVERSION_APPOINTMENT", "Set Appointment")
ADS_CONV_SOLD        = os.getenv("GADS_CONVERSION_SOLD",        "Sold Job")

GCLID_NOTE_PREFIX = "[GCLID]"

# ---------------------------------------------------------------------------
# Shared service helpers
# ---------------------------------------------------------------------------

def _odata_url() -> str:
    try:
        from config import Config
        return Config.MARKETSHARP_ODATA_URL.rstrip("/")
    except Exception:
        return os.getenv(
            "MARKETSHARP_ODATA_URL",
            "https://api4.marketsharpm.com/WcfDataService.svc",
        )


def _ms_service():
    """Return a live MarketSharpService, or None if unavailable."""
    try:
        from marketsharp_service import MarketSharpService
        return MarketSharpService()
    except Exception as exc:
        log.warning("MarketSharpService unavailable: %s", exc)
        return None


# ---------------------------------------------------------------------------
# MarketSharpFieldWriter
# ---------------------------------------------------------------------------

class MarketSharpFieldWriter:
    """Writes GCLID / UTM fields to a MarketSharp contact.

    "note" mode (default):
        Appends a structured note that can be parsed back later.
        e.g.  [GCLID] gclid=AW-123 utm_source=google utm_medium=cpc

    "customfield" mode:
        POSTs to ContactCustomFields using the IDs in FIELD_IDS.
        Requires those fields to exist in MarketSharp Admin first.
    """

    def __init__(self, svc=None):
        self._svc = svc or _ms_service()

    def write(self, contact_id: str, fields: dict) -> bool:
        """Persist non-empty fields to contact_id. Returns True on success."""
        if not contact_id:
            log.error("write() called without contact_id")
            return False
        fields = {k: v for k, v in fields.items() if v}
        if not fields:
            log.warning("No non-empty fields for contact %s", contact_id)
            return True
        if GCLID_WRITE_MODE == "customfield":
            return self._write_custom_fields(contact_id, fields)
        return self._write_as_note(contact_id, fields)

    def read(self, contact_id: str) -> dict:
        """Return the stored GCLID/UTM dict for contact_id, or {}."""
        if GCLID_WRITE_MODE == "customfield":
            return self._read_custom_fields(contact_id)
        return self._read_from_notes(contact_id)

    # -- note strategy -------------------------------------------------------

    def _write_as_note(self, contact_id: str, fields: dict) -> bool:
        if not self._svc:
            log.error("MarketSharpService not available")
            return False
        body = GCLID_NOTE_PREFIX + " " + " ".join(f"{k}={v}" for k, v in fields.items())
        result = self._svc.post_comment(contact_id, body, author_name="GCLID Sync")
        if result is None:
            log.error("Note write failed for contact %s", contact_id)
            return False
        log.info("Note written for contact %s: %s", contact_id, body)
        return True

    def _read_from_notes(self, contact_id: str) -> dict:
        """Scan the contact's notes for the most recent [GCLID] note."""
        if not (_HAS_REQUESTS and self._svc):
            return {}
        try:
            resp = _req.get(
                f"{_odata_url()}/Notes",
                headers=self._svc._odata_headers(),
                params={
                    "$filter": (
                        f"contactId eq '{contact_id}'"
                        " and substringof('GCLID',note)"
                    ),
                    "$orderby": "dateTime desc",
                    "$top": "5",
                },
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json().get("d", {})
            items = raw.get("results", raw) if isinstance(raw, dict) else raw
            if not items:
                return {}
            # Merge all matching notes; website lead note wins for GCLID
            merged: dict = {}
            for item in items:
                parsed = _parse_gclid_note(item.get("note", ""))
                if parsed.get("gclid") and not merged.get("gclid"):
                    merged.update(parsed)
                elif not merged:
                    merged.update(parsed)
            return merged
        except Exception as exc:
            log.warning("Failed reading notes for contact %s: %s", contact_id, exc)
            return {}

    # -- custom field strategy -----------------------------------------------

    def _write_custom_fields(self, contact_id: str, fields: dict) -> bool:
        if not (_HAS_REQUESTS and self._svc):
            log.error("requests / MarketSharpService not available")
            return False
        success = True
        for key, value in fields.items():
            field_id = FIELD_IDS.get(key, "")
            if not field_id:
                log.warning("No FIELD_ID configured for '%s' -- skipping", key)
                continue
            try:
                resp = _req.post(
                    f"{_odata_url()}/ContactCustomFields",
                    headers=self._svc._odata_headers(),
                    json={"contactId": contact_id, "customFieldId": field_id, "value": str(value)},
                    timeout=10,
                )
                if resp.status_code not in (200, 201, 204):
                    log.error("CF write failed field=%s status=%s body=%s",
                              key, resp.status_code, resp.text[:200])
                    success = False
                else:
                    log.info("Custom field '%s' written for contact %s", key, contact_id)
            except Exception as exc:
                log.error("Error writing custom field %s: %s", key, exc)
                success = False
        return success

    def _read_custom_fields(self, contact_id: str) -> dict:
        if not (_HAS_REQUESTS and self._svc):
            return {}
        try:
            resp = _req.get(
                f"{_odata_url()}/ContactCustomFields",
                headers=self._svc._odata_headers(),
                params={"$filter": f"contactId eq '{contact_id}'"},
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json().get("d", {})
            items = raw.get("results", raw) if isinstance(raw, dict) else raw
            id_to_key = {v: k for k, v in FIELD_IDS.items() if v}
            return {
                id_to_key.get(str(i.get("customFieldId", "")), str(i.get("customFieldId", ""))): i.get("value", "")
                for i in items
            }
        except Exception as exc:
            log.warning("Failed reading custom fields for contact %s: %s", contact_id, exc)
            return {}


# Map the raw field names from website lead notes to internal names.
_WEBSITE_LEAD_FIELD_MAP = {
    "gclid":        "gclid",
    "medium":       "utm_medium",
    "campaign":     "utm_campaign",
    "src":          "utm_content",
    "heardaboutus": "heard_about_us",
    "interest":     "interest",
}


def _parse_website_lead_note(note_text: str) -> dict:
    """Parse a MarketSharp email-trigger note in Key: Value<br> format.

    Example (from Robert Burns, 2026-05-13):
        Source: Website Leads Lead Capture<br>HeardAboutUs: Internet<br>
        Interest: Roofing<br>Source: google<br>Medium: cpc<br>
        Campaign: iMedia One<br>SRC: Google-Ads<br>GCLID: CjwKCAj...
    """
    import html as _html
    import re as _re
    out: dict = {}
    # Strip wrapping HTML tags
    clean = _re.sub(r"<[^>]+>", lambda m: "\n" if "br" in m.group().lower() else "", note_text)
    clean = _html.unescape(clean)
    source_values: list[str] = []
    for line in clean.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        key_lower = key.lower().replace(" ", "").replace("_", "")
        if key_lower == "source":
            source_values.append(val)
            continue
        internal = _WEBSITE_LEAD_FIELD_MAP.get(key_lower)
        if internal:
            out[internal] = val
    # First Source = lead_source descriptor; last Source = utm_source
    if source_values:
        out["utm_source"] = source_values[-1]
        if len(source_values) > 1:
            out["lead_source"] = source_values[0]
    return out


def _parse_gclid_note(note_text: str) -> dict:
    """Auto-detect note format and parse GCLID/UTM fields.

    Handles two formats:
      1. Website email-trigger notes: Key: Value<br>... (contains 'GCLID:')
      2. Our own sync notes:          [GCLID] gclid=AW-123 utm_source=google
    """
    if not note_text:
        return {}
    if "GCLID:" in note_text:
        return _parse_website_lead_note(note_text)
    if GCLID_NOTE_PREFIX in note_text:
        out: dict = {}
        body = note_text.split(GCLID_NOTE_PREFIX, 1)[1].strip()
        for token in body.split():
            if "=" in token:
                k, _, v = token.partition("=")
                out[k.strip()] = v.strip()
        return out
    return {}


# ---------------------------------------------------------------------------
# GCLIDExtractor  -- parse marketing params from form/webhook payloads
# ---------------------------------------------------------------------------

UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")

class GCLIDExtractor:
    """Extract GCLID and UTM parameters from a variety of payload shapes.

    Handles:
    - Flat dict  {"gclid": "...", "utm_source": "google"}
    - Nested     {"form_fields": {"gclid": "..."}}
    - URL query  "gclid=AW-xxx&utm_source=google"
    - CLI args   passed as kwargs
    """

    @staticmethod
    def from_dict(payload: dict) -> dict:
        """Walk a dict (possibly nested) and return all marketing fields."""
        result = {}
        flat = GCLIDExtractor._flatten(payload)
        if flat.get("gclid"):
            result["gclid"] = flat["gclid"]
        for k in UTM_KEYS:
            if flat.get(k):
                result[k] = flat[k]
        return result

    @staticmethod
    def from_querystring(qs: str) -> dict:
        """Parse a URL query string into marketing fields."""
        try:
            from urllib.parse import parse_qs
            parsed = parse_qs(qs)
            flat = {k: v[0] for k, v in parsed.items() if v}
            return GCLIDExtractor.from_dict(flat)
        except Exception:
            return {}

    @staticmethod
    def from_kwargs(**kwargs) -> dict:
        """Build from explicit CLI/keyword arguments."""
        return GCLIDExtractor.from_dict(kwargs)

    @staticmethod
    def _flatten(obj, prefix="", out=None):
        if out is None:
            out = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                full_key = f"{prefix}.{k}" if prefix else k
                GCLIDExtractor._flatten(v, full_key, out)
                # Also store under the bare leaf key for simple access
                out[k] = v if not isinstance(v, dict) else out.get(k)
        elif isinstance(obj, str):
            out[prefix] = obj
        else:
            out[prefix] = obj
        return out


# ---------------------------------------------------------------------------
# GCLIDSyncer  -- resolve contact by name then write fields
# ---------------------------------------------------------------------------

class GCLIDSyncer:
    """High-level orchestrator: name lookup -> field write."""

    def __init__(self, svc=None):
        self._svc = svc or _ms_service()
        self._writer = MarketSharpFieldWriter(self._svc)

    def sync_by_id(self, contact_id: str, fields: dict) -> bool:
        """Write fields directly when you already have the contact ID."""
        return self._writer.write(contact_id, fields)

    def sync_by_name(self, name: str, fields: dict, address: dict | None = None) -> dict:
        """Resolve contact by name (and optionally address) then write fields.

        Returns:
            {"ok": bool, "contact_id": str | None, "contact_name": str | None}
        """
        if not self._svc:
            log.error("MarketSharpService not available for name lookup")
            return {"ok": False, "contact_id": None, "contact_name": None}

        contact = self._svc.get_customer_by_name(name, project_address=address)
        if not contact:
            log.warning("No MarketSharp contact found for name: %s", name)
            return {"ok": False, "contact_id": None, "contact_name": None}

        contact_id = (
            contact.get("id")
            or contact.get("Id")
            or contact.get("contactId")
            or ""
        )
        contact_name = (
            contact.get("name")
            or contact.get("businessName")
            or contact.get("firstName", "") + " " + contact.get("lastName", "")
        ).strip()

        if not contact_id:
            log.error("Contact found but has no ID: %s", contact)
            return {"ok": False, "contact_id": None, "contact_name": contact_name}

        ok = self._writer.write(contact_id, fields)
        return {"ok": ok, "contact_id": contact_id, "contact_name": contact_name}


# ---------------------------------------------------------------------------
# ReportBuilder  -- query MarketSharp for conversion data
# ---------------------------------------------------------------------------

class ReportBuilder:
    """Query MarketSharp OData for appointments and sold jobs linked to GCLIDs.

    MarketSharp OData entities used:
        Inquiries   -- intake records; contain appointment date fields
        Jobs        -- sold-job records linked to an Inquiry/Contact
        Notes       -- where GCLID data lives when write_mode="note"

    Because MarketSharp's OData schema varies by tenant, this class
    uses a pragmatic fallback chain: try the most common entity name first,
    catch 4xx, try the next.
    """

    _INQUIRY_ENTITIES = ("Inquiries", "Inquiry", "Leads")
    _JOB_ENTITIES     = ("Jobs", "Job", "SoldJobs")

    def __init__(self, svc=None):
        self._svc = svc or _ms_service()

    def contacts_with_gclid(self, since: datetime | None = None) -> list[dict]:
        """Return list of {contact_id, contact_name, fields} for contacts
        whose notes contain a GCLID value (either from website email-trigger
        notes in Key: Value<br> format, or from our own [GCLID] sync notes)."""
        if not (_HAS_REQUESTS and self._svc):
            log.error("Service unavailable for report")
            return []

        # Match both website lead notes ("GCLID:") and our sync notes ("[GCLID]")
        # OData substringof('GCLID', note) covers both since both contain "GCLID"
        params: dict = {
            "$filter": "substringof('GCLID',note)",
            "$orderby": "dateTime desc",
            "$top": "1000",
        }
        if since:
            iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["$filter"] += f" and dateTime ge datetime'{iso}'"

        contacts: dict = {}
        try:
            resp = _req.get(
                f"{_odata_url()}/Notes",
                headers=self._svc._odata_headers(),
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.json().get("d", {})
            items = raw.get("results", raw) if isinstance(raw, dict) else raw
            for note in items:
                cid = str(note.get("contactId") or note.get("ContactId") or "")
                if not cid:
                    continue
                fields = _parse_gclid_note(note.get("note", ""))
                if fields:
                    contacts[cid] = {"contact_id": cid, "fields": fields}
        except Exception as exc:
            log.warning("Notes query failed: %s", exc)

        # Enrich each contact entry with name/email/phone via a batch fetch
        if contacts and _HAS_REQUESTS and self._svc:
            for cid in list(contacts.keys()):
                try:
                    r = _req.get(
                        f"{_odata_url()}/Contacts({cid})",
                        headers=self._svc._odata_headers(),
                        timeout=10,
                    )
                    r.raise_for_status()
                    raw_c = r.json().get("d", {})
                    if isinstance(raw_c, dict):
                        first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                        last  = raw_c.get("lastName")  or raw_c.get("LastName")  or ""
                        contacts[cid]["contact_name"] = f"{first} {last}".strip()
                        contacts[cid]["_raw_contact"]  = raw_c
                except Exception as exc:
                    log.debug("Could not fetch contact %s: %s", cid, exc)

        return list(contacts.values())

    def inquiries_for_contact(self, contact_id: str) -> list[dict]:
        """Return all Inquiry records linked to contact_id."""
        return self._fetch_linked(self._INQUIRY_ENTITIES, contact_id)

    def jobs_for_contact(self, contact_id: str) -> list[dict]:
        """Return all Job records linked to contact_id."""
        return self._fetch_linked(self._JOB_ENTITIES, contact_id)

    def _contact_email_phone(self, contact_record: dict) -> tuple[str, str]:
        """Extract best email and phone from a MarketSharp contact record."""
        email = (
            contact_record.get("email1")
            or contact_record.get("email")
            or contact_record.get("Email1")
            or ""
        )
        phone = (
            contact_record.get("cellPhone")
            or contact_record.get("homePhone")
            or contact_record.get("workPhone")
            or contact_record.get("phone")
            or contact_record.get("CellPhone")
            or contact_record.get("HomePhone")
            or ""
        )
        return email.strip(), phone.strip()

    def build_conversion_rows(self, since: datetime | None = None) -> list[dict]:
        """Main report: one row per appointment or sold job with a GCLID.

        Each row contains:
            contact_id, contact_name, email, phone, gclid, utm_source,
            utm_medium, utm_campaign, utm_term, utm_content,
            conversion_type, conversion_date, revenue
        """
        rows = []
        gclid_contacts = self.contacts_with_gclid(since=since)
        log.info("Found %d contacts with GCLID data", len(gclid_contacts))

        for entry in gclid_contacts:
            cid    = entry["contact_id"]
            fields = entry["fields"]
            gclid  = fields.get("gclid", "")
            cname  = entry.get("contact_name", "")
            email  = ""
            phone  = ""

            # Pull email/phone from the raw contact record if available
            raw = entry.get("_raw_contact")
            if raw:
                email, phone = self._contact_email_phone(raw)

            base = {
                "contact_id":   cid,
                "contact_name": cname,
                "email":        email,
                "phone":        phone,
                "gclid":        gclid,
                "utm_source":   fields.get("utm_source", ""),
                "utm_medium":   fields.get("utm_medium", ""),
                "utm_campaign": fields.get("utm_campaign", ""),
                "utm_term":     fields.get("utm_term", ""),
                "utm_content":  fields.get("utm_content", ""),
            }

            # Appointments
            for inq in self.inquiries_for_contact(cid):
                appt_date = (
                    inq.get("appointmentDate")
                    or inq.get("AppointmentDate")
                    or inq.get("scheduledDate")
                    or ""
                )
                if appt_date:
                    rows.append({**base,
                                 "conversion_type": ADS_CONV_APPOINTMENT,
                                 "conversion_date": _clean_date(appt_date),
                                 "revenue": ""})

            # Sold jobs
            for job in self.jobs_for_contact(cid):
                sold_date = (
                    job.get("soldDate")
                    or job.get("SoldDate")
                    or job.get("closeDate")
                    or ""
                )
                revenue = (
                    job.get("jobCost")
                    or job.get("revenue")
                    or job.get("contractAmount")
                    or ""
                )
                if sold_date:
                    rows.append({**base,
                                 "conversion_type": ADS_CONV_SOLD,
                                 "conversion_date": _clean_date(sold_date),
                                 "revenue": str(revenue)})

        log.info("Built %d conversion rows", len(rows))
        return rows

    # -------------------------------------------------------------------------

    def _fetch_linked(self, entity_names: tuple, contact_id: str) -> list[dict]:
        """Try each entity name until one returns data or all 404."""
        if not (_HAS_REQUESTS and self._svc):
            return []
        for entity in entity_names:
            try:
                resp = _req.get(
                    f"{_odata_url()}/{entity}",
                    headers=self._svc._odata_headers(),
                    params={"$filter": f"contactId eq '{contact_id}'", "$top": "100"},
                    timeout=10,
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                raw = resp.json().get("d", {})
                return raw.get("results", raw) if isinstance(raw, dict) else raw
            except Exception as exc:
                log.debug("Entity %s query failed for contact %s: %s", entity, contact_id, exc)
        return []


def _clean_date(raw: str) -> str:
    """Normalize a date string to YYYY-MM-DD HH:MM:SS."""
    raw = str(raw).strip()
    # Strip OData /Date(ms)/ format
    if raw.startswith("/Date("):
        ms = int(raw[6:raw.index(")")])
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    # Try a few common formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return raw


# ---------------------------------------------------------------------------
# CSVExporter  -- business report CSV
# ---------------------------------------------------------------------------
# CSVExporter  -- business report CSV (appointments + sold jobs with GCLID)
# ---------------------------------------------------------------------------

# Column order matches the offline conversions example format
CSV_COLUMNS = [
    "GCLID",
    "Email",
    "Phone Number",
    "Conversion Name",
    "Conversion Time",
    "Conversion Value",
    "Conversion Currency",
]

# Internal-only columns included in preview but not the upload CSV
_INTERNAL_COLUMNS = ["contact_id", "contact_name", "utm_source",
                     "utm_medium", "utm_campaign", "utm_term", "utm_content"]

CURRENCY = os.getenv("SPICER_CURRENCY", "USD")


class CSVExporter:
    """Write ReportBuilder rows in the Google Ads offline conversions format.

    Output columns (matches example CSV):
        GCLID              Google Click ID
        Email              Contact email (match key fallback)
        Phone Number       Contact phone (match key fallback)
        Conversion Name    "Booked Appt" or "Sold Job"
        Conversion Time    Date of appointment or sale (YYYY-MM-DD)
        Conversion Value   Revenue / job cost (blank for appointments)
        Conversion Currency USD (or SPICER_CURRENCY env var)
    """

    def export(self, rows: list, out_path: str) -> int:
        """Write rows to out_path. Returns number of rows written."""
        written = 0
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for row in rows:
                conv_time = row.get("date") or row.get("conversion_date", "")
                if not conv_time:
                    continue
                gclid = row.get("gclid", "").strip()
                email = row.get("email", "").strip()
                phone = row.get("phone", "").strip()
                # Row must have at least one match key
                if not gclid and not email and not phone:
                    log.debug("Skipping row with no match key: %s", row.get("contact_name"))
                    continue
                writer.writerow({
                    "GCLID":               gclid,
                    "Email":               email,
                    "Phone Number":        phone,
                    "Conversion Name":     row.get("conversion_type", ""),
                    "Conversion Time":     conv_time,
                    "Conversion Value":    row.get("revenue", ""),
                    "Conversion Currency": CURRENCY,
                })
                written += 1
        log.info("Exported %d rows to %s", written, out_path)
        return written

    def preview(self, rows: list, limit: int = 20) -> None:
        """Print a table preview to stdout."""
        fmt = "{:<30} {:<26} {:<18} {:<14} {:<22} {}"
        header = fmt.format("Contact Name", "GCLID", "Email", "Type", "Date", "Value")
        print(header)
        print("-" * len(header))
        for row in rows[:limit]:
            print(fmt.format(
                (row.get("contact_name") or row.get("contact_id", ""))[:29],
                row.get("gclid", "")[:25],
                row.get("email", "")[:17],
                row.get("conversion_type", "")[:13],
                (row.get("date") or row.get("conversion_date", ""))[:21],
                row.get("revenue", ""),
            ))
        if len(rows) > limit:
            print(f"  ... and {len(rows) - limit} more rows")

# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gclid-ms",
        description="GCLID/UTM <-> MarketSharp sync + appointment/job CSV export",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # -- write ----------------------------------------------------------------
    pw = sub.add_parser("write", help="Write fields to a contact by ID")
    pw.add_argument("--contact-id", required=True)
    pw.add_argument("--gclid",        default="")
    pw.add_argument("--utm-source",   default="")
    pw.add_argument("--utm-medium",   default="")
    pw.add_argument("--utm-campaign", default="")
    pw.add_argument("--utm-term",     default="")
    pw.add_argument("--utm-content",  default="")

    # -- sync -----------------------------------------------------------------
    ps = sub.add_parser("sync", help="Resolve contact by name then write fields")
    ps.add_argument("--name", required=True, help="Customer name to look up")
    ps.add_argument("--gclid",        default="")
    ps.add_argument("--utm-source",   default="")
    ps.add_argument("--utm-medium",   default="")
    ps.add_argument("--utm-campaign", default="")
    ps.add_argument("--utm-term",     default="")
    ps.add_argument("--utm-content",  default="")
    ps.add_argument("--address",      default="",
                    help="Optional JSON address object to aid name disambiguation")

    # -- report ---------------------------------------------------------------
    pr = sub.add_parser("report", help="Export appointment/sold-job CSV for contacts with a GCLID")
    pr.add_argument("--since", default="",
                    help="ISO date lower bound, e.g. 2026-01-01")
    pr.add_argument("--out",   default="",
                    help="Output CSV path (default: spicer_conversions_YYYYMM.csv)")
    pr.add_argument("--preview", action="store_true",
                    help="Print first 20 rows to stdout instead of writing file")

    # -- fields ---------------------------------------------------------------
    pf = sub.add_parser("fields", help="Read back stored GCLID/UTM fields for a contact")
    pf.add_argument("--contact-id", required=True)

    return p


def _fields_from_args(args) -> dict:
    return GCLIDExtractor.from_kwargs(
        gclid=getattr(args, "gclid", ""),
        utm_source=getattr(args, "utm_source", ""),
        utm_medium=getattr(args, "utm_medium", ""),
        utm_campaign=getattr(args, "utm_campaign", ""),
        utm_term=getattr(args, "utm_term", ""),
        utm_content=getattr(args, "utm_content", ""),
    )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "write":
        fields = _fields_from_args(args)
        ok = MarketSharpFieldWriter().write(args.contact_id, fields)
        sys.exit(0 if ok else 1)

    elif args.command == "sync":
        fields = _fields_from_args(args)
        address = {}
        if args.address:
            try:
                address = json.loads(args.address)
            except json.JSONDecodeError:
                log.warning("--address is not valid JSON; ignoring")
        result = GCLIDSyncer().sync_by_name(args.name, fields, address=address or None)
        if result["ok"]:
            print(f"OK  contact_id={result['contact_id']}  name={result['contact_name']}")
        else:
            print(f"FAIL  contact not found or write failed for: {args.name}")
            sys.exit(1)

    elif args.command == "report":
        since = None
        if args.since:
            try:
                since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                log.error("--since must be YYYY-MM-DD; got: %s", args.since)
                sys.exit(1)

        rows = ReportBuilder().build_conversion_rows(since=since)
        if not rows:
            print("No conversion rows found.")
            sys.exit(0)

        exporter = CSVExporter()
        if args.preview:
            exporter.preview(rows, limit=20)
        else:
            out = args.out or f"spicer_conversions_{datetime.now().strftime('%Y%m')}.csv"
            n = exporter.export(rows, out)
            print(f"Wrote {n} rows -> {out}")

    elif args.command == "fields":
        stored = MarketSharpFieldWriter().read(args.contact_id)
        if stored:
            for k, v in stored.items():
                print(f"  {k:<20} {v}")
        else:
            print("No GCLID/UTM fields found for this contact.")


if __name__ == "__main__":
    main()
