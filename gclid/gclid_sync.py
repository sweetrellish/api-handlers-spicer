"""
gclid_sync.py -- GCLID / UTM -> MarketSharp custom field sync
              + monthly Google Ads offline-conversion export

Commands
--------
  write   Push GCLID/UTM fields onto an existing MarketSharp contact
  sync    Resolve contact by name then write fields
  report  Monthly CSV export formatted for Google Ads offline conversions
  fields  Read back stored field values for a contact

Quick start
-----------
  python gclid_sync.py write --contact-id <id> --gclid AW-xxx
  python gclid_sync.py sync  --name "Jane Smith" --gclid AW-xxx --utm-source google
  python gclid_sync.py report --since 2026-04-01 --out may_conversions.csv
  python gclid_sync.py fields --contact-id <id>
  python gclid
"""

import argparse
import concurrent.futures as _cf
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

_ROOT        = Path(__file__).resolve().parent   # gclid/
_SPICER_ROOT = _ROOT.parent                      # spicer/
for _p in [str(_ROOT), str(_SPICER_ROOT), str(_SPICER_ROOT / "src")]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import requests as _req
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    from dotenv import load_dotenv
    load_dotenv(_ROOT / ".env")
except ImportError:
    # python-dotenv not installed — parse .env manually so downstream
    # modules (e.g. marketsharp_service) find vars already in os.environ.
    _env_path = _ROOT / ".env"
    if _env_path.exists():
        with open(_env_path) as _ef:
            for _line in _ef:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _, _v = _line.partition("=")
                _v = _v.strip().strip('"').strip("'")
                os.environ.setdefault(_k.strip(), _v)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] gclid_sync -- %(message)s",
)
log = logging.getLogger("gclid_sync")

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

ADS_CONV_LEAD        = os.getenv("GADS_CONVERSION_LEAD",        "Lead")
ADS_CONV_APPOINTMENT = os.getenv("GADS_CONVERSION_APPOINTMENT", "Qualified Lead")
ADS_CONV_SOLD        = os.getenv("GADS_CONVERSION_SOLD",        "Sold Job")
QUALIFIED_LEAD_VALUE = int(os.getenv("GADS_LEAD_VALUE",         "200"))  # nominal $ for appt rows
ADS_TIMEZONE         = os.getenv("GADS_TIMEZONE",               "America/New_York")
GADS_DATE_ONLY_TIME  = os.getenv("GADS_DATE_ONLY_TIME",         "12:00:00")
GADS_QUALIFIED_LEAD_LOOKBACK_DAYS = max(
    0,
    int(os.getenv("GADS_QUALIFIED_LEAD_LOOKBACK_DAYS", "90")),
)
GCLID_MAX_WORKERS    = max(1, int(os.getenv("GCLID_MAX_WORKERS", "8")))
GCLID_ENRICH_WORKERS = max(1, int(os.getenv("GCLID_ENRICH_WORKERS", "12")))
GCLID_EAGER_ENRICH   = os.getenv("GCLID_EAGER_ENRICH", "0").strip().lower() in ("1", "true", "yes", "on")

# Header present in every automated website lead note pushed to MarketSharp.
# Using this as the OData filter catches ALL website leads regardless of how
# the GCLID param happens to be formatted in the note body.
LEAD_NOTE_HEADER = os.getenv("SPICER_LEAD_NOTE_HEADER", "Source: Website Leads Lead Capture")

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
        # marketsharp_service.py may have a bare `from dotenv import load_dotenv`
        # at module level.  If python-dotenv is not installed, stub it out so
        # the import succeeds — the .env vars are already in os.environ from
        # the manual parse above.
        import sys
        if "dotenv" not in sys.modules:
            try:
                import dotenv  # noqa: F401
            except ImportError:
                from types import ModuleType
                _stub = ModuleType("dotenv")
                _stub.load_dotenv = lambda *a, **kw: None  # type: ignore[attr-defined]
                sys.modules["dotenv"] = _stub
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

    def _enrich_contact_records(self, contacts: dict) -> None:
        """Add contact_name/email/phone fields in parallel for known contact IDs."""
        if not contacts or not (_HAS_REQUESTS and self._svc):
            return

        ids = list(contacts.keys())
        worker_count = min(max(1, GCLID_ENRICH_WORKERS), max(1, len(ids)))

        def _fetch_one(cid: str) -> tuple[str, dict | None]:
            if not cid or not self._svc:
                return cid, None
            try:
                r = _req.get(
                    f"{_odata_url()}/Contacts(guid'{cid}')",
                    headers=self._svc._odata_headers(),
                    timeout=10,
                )
                r.raise_for_status()
                raw_c = r.json().get("d", {})
                return cid, raw_c if isinstance(raw_c, dict) else None
            except Exception as exc:
                log.debug("Could not fetch contact %s: %s", cid, exc)
                return cid, None

        if worker_count == 1:
            for cid in ids:
                _, raw_c = _fetch_one(cid)
                if isinstance(raw_c, dict):
                    first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                    last = raw_c.get("lastName") or raw_c.get("LastName") or ""
                    contacts[cid]["contact_name"] = f"{first} {last}".strip()
                    contacts[cid]["_raw_contact"] = raw_c
            return

        with _cf.ThreadPoolExecutor(max_workers=worker_count) as ex:
            futures = [ex.submit(_fetch_one, cid) for cid in ids]
            for fut in _cf.as_completed(futures):
                cid, raw_c = fut.result()
                if isinstance(raw_c, dict):
                    first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                    last = raw_c.get("lastName") or raw_c.get("LastName") or ""
                    contacts[cid]["contact_name"] = f"{first} {last}".strip()
                    contacts[cid]["_raw_contact"] = raw_c

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

        def _extract_best(items) -> dict:
            if not isinstance(items, list) or not items:
                return {}
            merged: dict = {}
            for item in items:
                parsed = _parse_gclid_note(item.get("note", ""))
                if parsed.get("gclid") and not merged.get("gclid"):
                    merged.update(parsed)
                elif not merged:
                    merged.update(parsed)
            return merged

        try:
            resp = _req.get(
                f"{_odata_url()}/Notes",
                headers=self._svc._odata_headers(),
                params={
                    "$filter": (
                        f"contactId eq '{contact_id}'"
                        " and (substringof('gclid=',note) or substringof('[GCLID]',note))"
                    ),
                    "$orderby": "dateTime desc",
                    "$top": "5",
                },
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json().get("d", {})
            items = raw.get("results", raw) if isinstance(raw, dict) else raw
            return _extract_best(items)
        except Exception as exc:
            # Tenant-safe fallback: some schemas reject substring filters or note field names.
            # Retry with broader contact-only queries and parse note text client-side.
            try:
                for flt in (
                    f"contactId eq '{contact_id}'",
                    f"contactId eq guid'{contact_id}'",
                    f"ContactId eq '{contact_id}'",
                    f"ContactId eq guid'{contact_id}'",
                ):
                    resp2 = _req.get(
                        f"{_odata_url()}/Notes",
                        headers=self._svc._odata_headers(),
                        params={
                            "$filter": flt,
                            "$orderby": "dateTime desc",
                            "$top": "100",
                        },
                        timeout=10,
                    )
                    if resp2.status_code == 404:
                        break
                    if resp2.status_code == 400:
                        continue
                    resp2.raise_for_status()
                    raw2 = resp2.json().get("d", {})
                    items2 = raw2.get("results", raw2) if isinstance(raw2, dict) else raw2
                    merged2 = _extract_best(items2)
                    if merged2:
                        return merged2
            except Exception as exc2:
                log.warning("Failed reading notes for contact %s: %s", contact_id, exc2)
                return {}

            # Keep warning from the original path if broad fallbacks didn't yield data.
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

def _inquiry_text(inq: dict) -> str:
    """Return the best text field from an Inquiry OData record."""
    return (
        inq.get("comment") or inq.get("note") or inq.get("noteText")
        or inq.get("inquiryNote") or inq.get("text") or inq.get("content")
        or inq.get("Comment") or inq.get("Note") or inq.get("NoteText")
        or inq.get("InquiryNote") or inq.get("Text") or inq.get("Content")
        or ""
    )


def _inquiry_date(inq: dict) -> str:
    """Return the best date string from an Inquiry OData record."""
    return (
        inq.get("inquiryDate") or inq.get("dateTime") or inq.get("date")
        or inq.get("InquiryDate") or inq.get("DateTime") or inq.get("createDate")
        or ""
    )


def _parse_url_note(note_text: str) -> dict:
    """Parse GCLID and UTM params from a URL embedded in a plain-text note.

    Confirmed live format (MarketSharp email-trigger, 2026-05-13):
        Referrer: https://www.spicerbros.com/?utm_campaign=...&gclid=Cj0K...
    """
    import re as _re
    from urllib.parse import urlparse as _up, parse_qs as _pqs, unquote_plus as _uq
    match = _re.search(r'https?://\S+gclid=[^\s"<]+', note_text, _re.IGNORECASE)
    if not match:
        return {}
    url = match.group(0).rstrip(".,;)")
    params = _pqs(_up(url).query, keep_blank_values=False)
    def _first(k: str) -> str:
        return _uq(params.get(k, [""])[0])
    gclid = _first("gclid")
    if not gclid:
        return {}
    out: dict = {"gclid": gclid}
    for k in ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"):
        v = _first(k)
        if v:
            out[k] = v
    return out


def _parse_gclid_note(note_text: str) -> dict:
    """Auto-detect note format and parse GCLID/UTM fields.

    Priority order:
      1. URL query-string embedded in plain text  (confirmed live format)
         e.g. Referrer: https://...?gclid=Cj0K...&utm_source=google
      2. HTML Key: Value<br> format: GCLID: CjwKCAj...<br>Source: google
      3. Our own sync notes:         [GCLID] gclid=AW-123 utm_source=google
    """
    if not note_text:
        return {}
    # Format 1: URL with gclid= (lowercase — OData substringof is case-sensitive)
    if "gclid=" in note_text.lower():
        result = _parse_url_note(note_text)
        if result.get("gclid"):
            return result
    # Format 2: HTML (or plain) Key: Value with GCLID: label
    if "GCLID:" in note_text:
        import html as _html, re as _re
        out: dict = {}
        # Replace ALL HTML tags with newline so p/div/span-based layouts
        # don't collapse multiple key:value pairs onto one line.
        clean = _re.sub(r"<[^>]+>", "\n", note_text)
        clean = _html.unescape(clean)
        source_values: list = []
        _kmap = {"gclid": "gclid", "medium": "utm_medium", "campaign": "utm_campaign",
                 "src": "utm_content", "heardaboutus": "heard_about_us", "interest": "interest"}
        for line in clean.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            key, _, val = line.partition(":")
            kl = key.strip().lower().replace(" ", "").replace("_", "")
            val = val.strip()
            if kl == "source":
                source_values.append(val)
            elif kl in _kmap:
                out[_kmap[kl]] = val
        if source_values:
            out["utm_source"] = source_values[-1]
            if len(source_values) > 1:
                out["lead_source"] = source_values[0]
        return out
    # Format 3: [GCLID] key=value our own sync notes
    if GCLID_NOTE_PREFIX in note_text:
        out = {}
        body = note_text.split(GCLID_NOTE_PREFIX, 1)[1].strip()
        for token in body.split():
            if "=" in token:
                k, _, v = token.partition("=")
                out[k.strip()] = v.strip()
        return out
    return {}


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

    _INQUIRY_ENTITIES     = ("Inquiries", "Inquiry", "Leads", "WebLeads", "Activities", "Notes")
    _APPOINTMENT_ENTITIES = ("Appointments", "Appointment")
    _JOB_ENTITIES         = ("Jobs", "Job", "SoldJobs")

    def __init__(self, svc=None):
        self._svc = svc or _ms_service()

    def _enrich_contact_records(self, contacts: dict) -> None:
        """Add contact_name/email/phone fields in parallel for known contact IDs."""
        if not contacts or not (_HAS_REQUESTS and self._svc):
            return

        ids = list(contacts.keys())
        worker_count = min(max(1, GCLID_ENRICH_WORKERS), max(1, len(ids)))

        def _fetch_one(cid: str) -> tuple[str, dict | None]:
            if not cid or not self._svc:
                return cid, None
            try:
                r = _req.get(
                    f"{_odata_url()}/Contacts(guid'{cid}')",
                    headers=self._svc._odata_headers(),
                    timeout=10,
                )
                r.raise_for_status()
                raw_c = r.json().get("d", {})
                return cid, raw_c if isinstance(raw_c, dict) else None
            except Exception as exc:
                log.debug("Could not fetch contact %s: %s", cid, exc)
                return cid, None

        if worker_count == 1:
            for cid in ids:
                _, raw_c = _fetch_one(cid)
                if isinstance(raw_c, dict):
                    first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                    last = raw_c.get("lastName") or raw_c.get("LastName") or ""
                    contacts[cid]["contact_name"] = f"{first} {last}".strip()
                    contacts[cid]["_raw_contact"] = raw_c
            return

        with _cf.ThreadPoolExecutor(max_workers=worker_count) as ex:
            futures = [ex.submit(_fetch_one, cid) for cid in ids]
            for fut in _cf.as_completed(futures):
                cid, raw_c = fut.result()
                if isinstance(raw_c, dict):
                    first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                    last = raw_c.get("lastName") or raw_c.get("LastName") or ""
                    contacts[cid]["contact_name"] = f"{first} {last}".strip()
                    contacts[cid]["_raw_contact"] = raw_c

    def contacts_with_gclid(self, contact_ids: list | None = None) -> list[dict]:
        """Return list of {contact_id, contact_name, fields} for ALL contacts
        whose notes contain a GCLID value.  Date filtering is done downstream
        in build_conversion_rows so a 2025 lead whose appointment is in 2026
        still appears in the 2026 report.

        If *contact_ids* is provided the OData Notes substringof query is skipped
        entirely and each contact's notes are fetched individually instead.
        This is more reliable when you have a ground-truth contact list (e.g.
        exported from MarketSharp's "has GCLID" contact filter).
        """
        if not (_HAS_REQUESTS and self._svc):
            log.error("Service unavailable for report")
            return []

        if contact_ids is not None:
            # CSV-driven mode: try per-contact fetch, but with guards against hanging/timeout.
            # If all entities fail repeatedly, fall back to empty (no auto data for CSV list).
            return self._contacts_from_ids(contact_ids)

        # GCLID data lives in Inquiry records (not Contact Notes).
        # Confirmed field name: 'note'. No 'comment' field exists in Inquiries.
        inq_filter = (
            "(substringof('GCLID:',note)"
            " or substringof('gclid:',note)"
            " or substringof('GCLID=',note)"
            " or substringof('gclid=',note)"
            " or substringof('[GCLID]',note))"
        )
        params: dict = {"$filter": inq_filter, "$top": "500"}

        contacts: dict = {}
        for entity in self._INQUIRY_ENTITIES:
            next_url: str | None = f"{_odata_url()}/{entity}"
            page = 0
            failed = False
            try:
                while next_url:
                    page += 1
                    resp = _req.get(
                        next_url,
                        headers=self._svc._odata_headers(),
                        params=params if page == 1 else None,
                        timeout=15,
                    )
                    if resp.status_code == 404:
                        failed = True
                        break
                    resp.raise_for_status()
                    data = resp.json().get("d", {})
                    items = data.get("results", data) if isinstance(data, dict) else data
                    for inq in (items if isinstance(items, list) else []):
                        cid = str(inq.get("contactId") or inq.get("ContactId") or "")
                        if not cid:
                            continue
                        text = _inquiry_text(inq)
                        fields = _parse_gclid_note(text)
                        if fields.get("gclid") and cid not in contacts:
                            inq_dt = _inquiry_date(inq)
                            contacts[cid] = {"contact_id": cid, "fields": fields,
                                             "note_date": _clean_date(inq_dt) if inq_dt else ""}
                        elif fields and not fields.get("gclid"):
                            log.debug("Inquiry matched filter but has no GCLID value: contact=%s", cid)
                    next_url = data.get("__next") if isinstance(data, dict) else None
            except Exception as exc:
                log.warning("%s query failed: %s", entity, exc)
                failed = True
            if not failed:
                log.info("Inquiry query (%s): %d page(s), %d contacts with parseable GCLID",
                         entity, page, len(contacts))
                break  # found a working entity name

        # Optional eager enrichment: disabled by default for performance.
        # Email/phone are fetched lazily for contacts that actually emit rows.
        if GCLID_EAGER_ENRICH and contacts and _HAS_REQUESTS and self._svc:
            self._enrich_contact_records(contacts)

        return list(contacts.values())

    def _contacts_from_ids(self, contact_ids: list) -> list[dict]:
        """Fetch Inquiry records individually for each contact_id and extract GCLIDs.

        The GCLID lives in the Inquiry (lead intake) record, not Contact Notes.
        Used when a ground-truth contact list is available (e.g. from an MS export).
        """
        contacts: dict = {}
        total = len(contact_ids)

        # Probe to find the working entity name before iterating all contacts.
        # Try a no-filter request (just $top=1) to identify which name returns 200.
        # If all entities return 400, skip per-contact fetch (tenant likely has broken OData).
        working_entity: str | None = None
        has_any_400 = False
        if _HAS_REQUESTS and self._svc:
            for entity in self._INQUIRY_ENTITIES:
                try:
                    r = _req.get(
                        f"{_odata_url()}/{entity}",
                        headers=self._svc._odata_headers(),
                        params={"$top": "1"},
                        timeout=10,
                    )
                    log.info("Entity probe: %s -> HTTP %d", entity, r.status_code)
                    if r.status_code == 400:
                        has_any_400 = True
                    if r.status_code == 200:
                        working_entity = entity
                        break
                except Exception as exc:
                    log.debug("Entity probe %s error: %s", entity, exc)
            if working_entity:
                log.info("Using inquiry entity: %s", working_entity)
            elif has_any_400:
                log.warning("All Inquiry entities returned 400 — tenant OData likely broken for complex filters")
            else:
                log.warning("No Inquiry-type entity found. Tried: %s", self._INQUIRY_ENTITIES)

        if working_entity:
            ordered_entities = (working_entity,) + tuple(
                e for e in self._INQUIRY_ENTITIES if e != working_entity
            )
        else:
            ordered_entities = self._INQUIRY_ENTITIES

        # If all global filters returned 400 but we can fetch individual records by key,
        # use direct key lookup (avoids broken filter syntax entirely).
        use_key_lookup = has_any_400

        for cid in contact_ids:
            cid = cid.strip()
            if not cid:
                continue
            try:
                if not (_HAS_REQUESTS and self._svc):
                    log.warning("Service unavailable; skipping contact %s", cid)
                    continue
                inq_list: list = []
                found_entity: str = ""
                
                # Strategy 1: Direct key lookup on Contact record (avoids filters).
                # If tenant filters are broken but direct access works, this succeeds.
                if use_key_lookup:
                    for key_variant in (f"guid'{cid}'", f"'{cid}'"):
                        try:
                            url = f"{_odata_url()}/Contacts({key_variant})"
                            resp = _req.get(
                                url,
                                headers=self._svc._odata_headers(),
                                timeout=5,
                            )
                            log.debug("  Direct key: Contacts(%s)  status=%d", key_variant, resp.status_code)
                            if resp.status_code == 200:
                                data = resp.json().get("d", {})
                                if isinstance(data, dict) and data.get("note"):
                                    note_text = data.get("note", "")
                                    fields = _parse_gclid_note(note_text)
                                    if fields.get("gclid"):
                                        # Also extract email/phone from the direct key response
                                        email, phone = data.get("email") or data.get("Email"), data.get("phone") or data.get("Phone")
                                        if email:
                                            fields["email"] = email
                                        if phone:
                                            fields["phone"] = phone
                                        contacts[cid] = {
                                            "contact_id": cid,
                                            "fields": fields,
                                            "note_date": "",
                                        }
                                        log.debug("  Found GCLID in Contact(%s) note  email=%s phone=%s", cid, email or "(none)", phone or "(none)")
                                        break
                        except Exception as exc:
                            log.debug("  Direct key Contacts(%s) error: %s", key_variant, exc)
                    
                    if cid in contacts:
                        continue  # found via key lookup; skip filter-based search

                # Strategy 2: Filter-based search (if key lookup not enabled or failed).
                for entity in ordered_entities:
                    url = f"{_odata_url()}/{entity}"
                    # Try both filter syntaxes: plain string first, then guid'...' for true GUID props
                    for flt in (
                        f"contactId eq '{cid}'",
                        f"contactId eq guid'{cid}'",
                        f"ContactId eq '{cid}'",
                        f"ContactId eq guid'{cid}'",
                    ):
                        try:
                            resp = _req.get(
                                url,
                                headers=self._svc._odata_headers(),
                                params={
                                    "$filter": flt,
                                    "$top": "50",
                                },
                                timeout=5,
                            )
                            log.debug("  %s  filter=%r  status=%d", entity, flt, resp.status_code)
                            if resp.status_code == 404:
                                break  # entity doesn't exist; skip to next entity name
                            if resp.status_code == 400:
                                log.debug("    400 body: %s", resp.text[:300])
                                continue  # try other filter syntax
                            resp.raise_for_status()
                            data = resp.json().get("d", {})
                            results = data.get("results", data) if isinstance(data, dict) else data
                            if isinstance(results, list):
                                if results:
                                    inq_list = results
                                    found_entity = entity
                                    log.debug("  Found %d record(s) in %s for %s", len(results), entity, cid)
                                    break  # non-empty result — done
                                # empty list but valid response; keep trying other entities
                                found_entity = entity
                        except Exception as exc:
                            log.debug("  %s  filter=%r  error: %s", entity, flt, exc)
                    if inq_list:
                        break  # already have results

                for inq in inq_list:
                    text = _inquiry_text(inq)
                    fields = _parse_gclid_note(text)
                    if fields.get("gclid"):
                        inq_dt = _inquiry_date(inq)
                        contacts[cid] = {
                            "contact_id": cid,
                            "fields": fields,
                            "note_date": _clean_date(inq_dt) if inq_dt else "",
                        }
                        break

                # Fallback for tenant variability: if inquiry text parsing missed,
                # try the standard reader path (notes/custom fields per write mode).
                if cid not in contacts:
                    try:
                        fallback_fields = MarketSharpFieldWriter(self._svc).read(cid)
                    except Exception:
                        fallback_fields = {}
                    if isinstance(fallback_fields, dict) and fallback_fields.get("gclid"):
                        contacts[cid] = {
                            "contact_id": cid,
                            "fields": fallback_fields,
                            "note_date": "",
                        }

                if cid not in contacts:
                    if inq_list:
                        sample = _inquiry_text(inq_list[0])[:200].replace("\n", " | ")
                        log.warning("No GCLID in %d inquiry(s) for %s (%s) — oldest: %r",
                                    len(inq_list), cid, found_entity, sample)
                    elif found_entity:
                        log.warning("No inquiries for contact %s — entity %s responded 200 but returned 0 records",
                                    cid, found_entity)
                    else:
                        log.warning("No inquiries returned for contact %s — all entity names 404'd or errored: %s",
                                    cid, ordered_entities)
            except Exception as exc:
                log.warning("Inquiry fetch failed for contact %s: %s", cid, exc)

        log.info("CSV-driven: %d/%d contacts had a parseable GCLID in their notes",
                 len(contacts), total)

        if self._svc is None:
            log.warning("MarketSharpService not available; skipping contact enrichment")
            return list(contacts.values())

        # Optional eager enrichment: disabled by default for performance.
        if GCLID_EAGER_ENRICH:
            self._enrich_contact_records(contacts)

        return list(contacts.values())

    def inquiries_for_contact(self, contact_id: str) -> list[dict]:
        """Return all Inquiry records linked to contact_id."""
        return self._fetch_linked(self._INQUIRY_ENTITIES, contact_id)

    def appointments_for_contact(self, contact_id: str) -> list[dict]:
        """Return Appointment records for a contact.

        Appointments don't have a contactId field — they're linked via an
        inquiry-like entity (tenant-specific name: Inquiries/Inquiry/Leads/etc.).
        We fetch that entity with $expand=Appointment and extract sub-objects.
        The expand returns a collection: {results: [...]}
        """
        if not (_HAS_REQUESTS and self._svc):
            return []
        for entity in self._INQUIRY_ENTITIES:
            url = f"{_odata_url()}/{entity}"
            for flt in (f"contactId eq '{contact_id}'", f"contactId eq guid'{contact_id}'"):
                try:
                    resp = _req.get(
                        url,
                        headers=self._svc._odata_headers(),
                        params={
                            "$filter": flt,
                            "$expand": "Appointment",
                            "$top": "100",
                        },
                        timeout=10,
                    )
                    if resp.status_code == 404:
                        break  # entity name doesn't exist for this tenant
                    if resp.status_code == 400:
                        continue  # try the alternate filter syntax
                    resp.raise_for_status()

                    data = resp.json().get("d", {})
                    items = data.get("results", data) if isinstance(data, dict) else data
                    if not isinstance(items, list):
                        continue

                    appts: list[dict] = []
                    for inq in items:
                        appt_expand = inq.get("Appointment")
                        # $expand=Appointment may return {results:[...]}, object, or nothing.
                        if isinstance(appt_expand, dict) and "results" in appt_expand:
                            for appt in appt_expand.get("results", []):
                                if appt and (
                                    appt.get("id")
                                    or appt.get("setDate")
                                    or appt.get("appointmentDate")
                                    or appt.get("SetDate")
                                    or appt.get("AppointmentDate")
                                ):
                                    appts.append(appt)
                        elif isinstance(appt_expand, dict):
                            if (
                                appt_expand.get("id")
                                or appt_expand.get("setDate")
                                or appt_expand.get("appointmentDate")
                                or appt_expand.get("SetDate")
                                or appt_expand.get("AppointmentDate")
                            ):
                                appts.append(appt_expand)
                        else:
                            # Some tenants expose appointment-like date fields directly
                            # on the inquiry entity row.
                            if (
                                inq.get("setDate")
                                or inq.get("appointmentDate")
                                or inq.get("SetDate")
                                or inq.get("AppointmentDate")
                                or inq.get("scheduledDate")
                                or inq.get("dateTime")
                                or inq.get("startDate")
                            ):
                                appts.append(inq)

                    if appts:
                        log.debug(
                            "appointments_for_contact %s: entity=%s filter=%r inquiry_rows=%d appts=%d",
                            contact_id,
                            entity,
                            flt,
                            len(items),
                            len(appts),
                        )
                        return appts

                    # Valid response but no appointments; try next entity name.
                    break
                except Exception as exc:
                    log.debug("%s/$expand=Appointment failed for %s (filter=%r): %s",
                              entity, contact_id, flt, exc)

        # Fallback: try fetching via direct Contact key with $expand=Appointments
        # This works even when per-contact filter queries return 400.
        for key_variant in (f"guid'{contact_id}'", f"'{contact_id}'"):
            for expand in ("Appointments", "Appointment"):
                try:
                    r = _req.get(
                        f"{_odata_url()}/Contacts({key_variant})",
                        headers=self._svc._odata_headers(),
                        params={"$expand": expand},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        d = r.json().get("d", {})
                        appt_data = d.get(expand) or d.get(expand.lower())
                        if isinstance(appt_data, dict):
                            results = appt_data.get("results", [])
                            if results:
                                log.debug("appointments_for_contact %s via Contact expand=%s: %d appts", contact_id, expand, len(results))
                                return results
                        elif isinstance(appt_data, list) and appt_data:
                            return appt_data
                except Exception as exc:
                    log.debug("Contact key/$expand=%s failed for %s: %s", expand, contact_id, exc)
            break  # tried both expand names; no need to retry with other key variant if we got a 200
        return []

    def jobs_for_contact(self, contact_id: str) -> list[dict]:
        """Return all Job records linked to contact_id.

        Jobs have a direct contactId field, so we query the Jobs entity.
        We expand the Contract relationship to get amount data (totalContract).
        """
        if not (_HAS_REQUESTS and self._svc):
            return []
        try:
            # Query with Contract expand to get amount data
            resp = _req.get(
                f"{_odata_url()}/Jobs",
                headers=self._svc._odata_headers(),
                params={
                    "$filter": f"contactId eq '{contact_id}'",
                    "$expand": "Contract",
                    "$top": "100",
                },
                timeout=10,
            )
            if resp.status_code == 404:
                return []
            if resp.status_code == 200:
                data = resp.json().get("d", {})
                jobs = data.get("results", data) if isinstance(data, dict) else data
                return jobs if isinstance(jobs, list) else []
            log.debug("Jobs query failed for %s: HTTP %d", contact_id, resp.status_code)
        except Exception as exc:
            log.debug("jobs_for_contact %s failed: %s", contact_id, exc)

        # Fallback: try fetching via direct Contact key with $expand=Jobs
        for key_variant in (f"guid'{contact_id}'", f"'{contact_id}'"):
            for expand in ("Jobs", "Job"):
                try:
                    r = _req.get(
                        f"{_odata_url()}/Contacts({key_variant})",
                        headers=self._svc._odata_headers(),
                        params={"$expand": expand},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        d = r.json().get("d", {})
                        job_data = d.get(expand) or d.get(expand.lower())
                        if isinstance(job_data, dict):
                            results = job_data.get("results", [])
                            if results:
                                log.debug("jobs_for_contact %s via Contact expand=%s: %d jobs", contact_id, expand, len(results))
                                return results
                        elif isinstance(job_data, list) and job_data:
                            return job_data
                except Exception as exc:
                    log.debug("Contact key/$expand=%s failed for %s: %s", expand, contact_id, exc)
            break
        return []

    def _contact_email_phone(self, contact_record: dict) -> tuple[str, str]:
        """Extract best email and phone from a MarketSharp contact record."""
        import re as _re

        def _valid_email(v) -> str:
            s = str(v or "").strip()
            if not s:
                return ""
            return s if ("@" in s and "." in s.split("@")[-1]) else ""

        def _valid_phone(v) -> str:
            s = str(v or "").strip()
            if not s:
                return ""
            # Reject UUID/contact-id style values.
            if _re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s):
                return ""
            # Keep if it looks like a phone after stripping punctuation.
            digits = "".join(ch for ch in s if ch.isdigit())
            if len(digits) < 7 or len(digits) > 15:
                return ""
            return s

        def _iter_values(obj):
            if isinstance(obj, dict):
                for key, val in obj.items():
                    yield key, val
                    if isinstance(val, (dict, list, tuple)):
                        yield from _iter_values(val)
            elif isinstance(obj, (list, tuple)):
                for item in obj:
                    yield from _iter_values(item)

        def _pick_from_keys(obj, wanted: tuple[str, ...], validator) -> str:
            if not isinstance(obj, dict):
                return ""
            for key, val in obj.items():
                key_text = str(key).lower()
                if any(token in key_text for token in wanted):
                    cand = validator(val)
                    if cand:
                        return cand
            return ""

        email = (
            _pick_from_keys(contact_record, ("email", "e-mail"), _valid_email)
            or
            _valid_email(contact_record.get("email1"))
            or _valid_email(contact_record.get("email"))
            or _valid_email(contact_record.get("emailAddress"))
            or _valid_email(contact_record.get("primaryEmail"))
            or _valid_email(contact_record.get("Email1"))
            or _valid_email(contact_record.get("Email"))
            or _valid_email(contact_record.get("EmailAddress"))
            or _valid_email(contact_record.get("PrimaryEmail"))
            or ""
        )
        phone = (
            _pick_from_keys(contact_record, ("phone", "mobile", "cell"), _valid_phone)
            or
            _valid_phone(contact_record.get("cellPhone"))
            or _valid_phone(contact_record.get("homePhone"))
            or _valid_phone(contact_record.get("workPhone"))
            or _valid_phone(contact_record.get("phone"))
            or _valid_phone(contact_record.get("phone1"))
            or _valid_phone(contact_record.get("phone2"))
            or _valid_phone(contact_record.get("mobilePhone"))
            or _valid_phone(contact_record.get("primaryPhone"))
            or _valid_phone(contact_record.get("phoneNumber"))
            or _valid_phone(contact_record.get("CellPhone"))
            or _valid_phone(contact_record.get("HomePhone"))
            or _valid_phone(contact_record.get("WorkPhone"))
            or _valid_phone(contact_record.get("Phone"))
            or _valid_phone(contact_record.get("Phone1"))
            or _valid_phone(contact_record.get("Phone2"))
            or _valid_phone(contact_record.get("MobilePhone"))
            or _valid_phone(contact_record.get("PrimaryPhone"))
            or _valid_phone(contact_record.get("PhoneNumber"))
            or ""
        )

        # Tenant-safe fallback: walk nested dict/list values and pick the first
        # valid email/phone-like field we can find.
        if not email and isinstance(contact_record, dict):
            for key, val in _iter_values(contact_record):
                if "email" not in str(key).lower():
                    continue
                cand = _valid_email(val)
                if cand:
                    email = cand
                    break

        if not phone and isinstance(contact_record, dict):
            for key, val in _iter_values(contact_record):
                if not val:
                    continue
                k = str(key).lower()
                if "fax" in k:
                    continue
                if "phone" in k or k in ("mobile", "cell"):
                    cand = _valid_phone(val)
                    if cand:
                        phone = cand
                        break

        if not phone and isinstance(contact_record, dict):
            contact_phone_id = (
                contact_record.get("contactPhoneId")
                or contact_record.get("ContactPhoneId")
                or contact_record.get("contactphoneid")
                or ""
            )
            if contact_phone_id and _HAS_REQUESTS and self._svc:
                base = _odata_url()
                headers = self._svc._odata_headers()
                contact_id_guess = (
                    contact_record.get("id")
                    or contact_record.get("Id")
                    or contact_record.get("contactId")
                    or contact_record.get("ContactId")
                    or ""
                )
                if not contact_id_guess:
                    contact_id_guess = contact_phone_id
                for key_variant in (f"guid'{contact_id_guess}'", f"'{contact_id_guess}'"):
                    try:
                        r = _req.get(
                            f"{base}/Contacts({key_variant})",
                            headers=headers,
                            params={"$expand": "ContactPhone"},
                            timeout=5,
                        )
                        if r.status_code != 200:
                            continue
                        expanded = r.json().get("d", {})
                        if isinstance(expanded, dict):
                            nested = expanded.get("ContactPhone") or expanded.get("contactPhone")
                            if isinstance(nested, dict):
                                email2, phone2 = self._contact_email_phone(nested)
                                if not email and email2:
                                    email = email2
                                if phone2:
                                    phone = phone2
                                    break
                    except Exception:
                        continue
        return email.strip(), phone.strip()

    def _fetch_contact_record(self, contact_id: str) -> dict | None:
        """Fetch a contact record by ID for lazy email/phone enrichment."""
        if not (_HAS_REQUESTS and self._svc and contact_id):
            return None

        base = _odata_url()
        headers = self._svc._odata_headers()

        # Try direct key-based entity URLs first (tenant-dependent key syntax).
        direct_urls = [
            f"{base}/Contacts(guid'{contact_id}')",
            f"{base}/Contacts('{contact_id}')",
            f"{base}/Contact(guid'{contact_id}')",
            f"{base}/Contact('{contact_id}')",
        ]
        for url in direct_urls:
            try:
                r = _req.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    raw = r.json().get("d", {})
                    if isinstance(raw, dict):
                        return raw
            except Exception:
                pass

        # Fallback: query by filter in case entity key style is different.
        filter_candidates = (
            f"id eq guid'{contact_id}'",
            f"id eq '{contact_id}'",
            f"contactId eq guid'{contact_id}'",
            f"contactId eq '{contact_id}'",
        )
        for entity in ("Contacts", "Contact"):
            for flt in filter_candidates:
                try:
                    r = _req.get(
                        f"{base}/{entity}",
                        headers=headers,
                        params={"$filter": flt, "$top": "1"},
                        timeout=10,
                    )
                    if r.status_code != 200:
                        continue
                    raw = r.json().get("d", {})
                    items = raw.get("results", raw) if isinstance(raw, dict) else raw
                    if isinstance(items, list) and items:
                        if isinstance(items[0], dict):
                            return items[0]
                    elif isinstance(items, dict):
                        return items
                except Exception:
                    continue

        log.debug("Could not lazy-fetch contact %s via direct key or filtered query", contact_id)
        return None

    def _pick_amount(self, obj: dict | None) -> str:
        """Extract best available numeric amount from a dict-like job/contract object."""
        if not isinstance(obj, dict):
            return ""

        preferred = (
            "totalContract", "TotalContract",
            "cashTotal", "CashTotal",
            "contractAmount", "ContractAmount",
            "saleAmount", "SaleAmount",
            "soldAmount", "SoldAmount",
            "jobAmount", "JobAmount",
            "totalAmount", "TotalAmount",
            "netAmount", "NetAmount",
            "revenue", "Revenue",
            "amount", "Amount",
            "value", "Value",
        )
        for key in preferred:
            val = obj.get(key)
            if val not in (None, ""):
                return str(val)

        # Heuristic fallback for unknown amount-like keys
        for key, val in obj.items():
            if val in (None, ""):
                continue
            lkey = str(key).lower()
            if any(tok in lkey for tok in ("contract", "amount", "total", "price", "sale", "revenue", "value")):
                return str(val)
        return ""

    def _to_dt(self, cleaned: str) -> datetime | None:
        if not cleaned:
            return None
        try:
            return datetime.strptime(cleaned[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def _ensure_after(self, candidate_clean: str, floor_dt: datetime | None) -> str:
        """If candidate <= floor_dt, bump by 1 second to preserve lifecycle order."""
        if not candidate_clean or floor_dt is None:
            return candidate_clean
        cand_dt = self._to_dt(candidate_clean)
        if cand_dt is None:
            return candidate_clean
        if cand_dt <= floor_dt:
            return (floor_dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        return candidate_clean

    def _build_rows_for_contact(self, entry: dict,
                                since: datetime | None = None,
                                until: datetime | None = None) -> list[dict]:
        """Build all appointment/sold rows for a single contact entry."""
        rows: list[dict] = []
        cid = entry["contact_id"]
        fields = entry["fields"]
        gclid = fields.get("gclid", "")
        cname = entry.get("contact_name", "")
        email = ""
        phone = ""

        raw = entry.get("_raw_contact")
        if raw:
            email, phone = self._contact_email_phone(raw)

        details_loaded = bool(raw)

        def _ensure_contact_details() -> None:
            nonlocal email, phone, details_loaded
            if details_loaded:
                return
            raw_c = self._fetch_contact_record(cid)
            if isinstance(raw_c, dict):
                email, phone = self._contact_email_phone(raw_c)
                # Keep enriched contact on the entry for any downstream reuse.
                entry["_raw_contact"] = raw_c
                first = raw_c.get("firstName") or raw_c.get("FirstName") or ""
                last = raw_c.get("lastName") or raw_c.get("LastName") or ""
                if not entry.get("contact_name"):
                    entry["contact_name"] = f"{first} {last}".strip()
            details_loaded = True

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

        lead_dates: list[datetime] = []
        lookback_since: datetime | None = None
        if since and GADS_QUALIFIED_LEAD_LOOKBACK_DAYS > 0:
            lookback_since = since - timedelta(days=GADS_QUALIFIED_LEAD_LOOKBACK_DAYS)

        # Appointments -> Qualified Lead rows
        for appt in self.appointments_for_contact(cid):
            appt_date = (
                appt.get("setDate")
                or appt.get("appointmentDate")
                or appt.get("SetDate")
                or appt.get("AppointmentDate")
                or appt.get("scheduledDate")
                or appt.get("dateTime")
                or appt.get("startDate")
                or ""
            )
            if appt_date:
                lead_clean = _clean_date(appt_date)
                if self._in_range(lead_clean, since, until):
                    _ensure_contact_details()
                    rows.append({**base,
                                 "contact_name": entry.get("contact_name", cname),
                                 "email": email,
                                 "phone": phone,
                                 "conversion_type": ADS_CONV_APPOINTMENT,
                                 "conversion_date": lead_clean,
                                 "revenue": str(QUALIFIED_LEAD_VALUE)})
                lead_dt = self._to_dt(lead_clean)
                if lead_dt is not None:
                    lead_dates.append(lead_dt)

        # Fallback lead date source for ordering/synthetic lead creation.
        note_fallback_dt = self._to_dt(entry.get("note_date", ""))
        if note_fallback_dt is not None:
            lead_dates.append(note_fallback_dt)
        first_lead_dt = min(lead_dates) if lead_dates else None

        # Track the LATEST actual appointment row date — sold jobs must come after this.
        appt_row_dts = [
            self._to_dt(r["conversion_date"])
            for r in rows
            if r.get("conversion_type") == ADS_CONV_APPOINTMENT
        ]
        last_appt_dt = max((d for d in appt_row_dts if d is not None), default=None)

        sold_rows_for_contact: list[dict] = []

        # Sold jobs -> Sold Job rows
        for job in self.jobs_for_contact(cid):
            sold_date = (
                job.get("saleDate") or job.get("SaleDate")
                or job.get("contractDate") or job.get("ContractDate")
                or job.get("soldDate") or job.get("SoldDate")
                or job.get("closeDate") or job.get("CloseDate")
                or job.get("completedDate") or job.get("CompletedDate")
                or ""
            )

            raw_rev = ""
            for ckey in ("Contract", "contract", "Contracts", "contracts"):
                cnode = job.get(ckey)
                if not cnode:
                    continue

                if isinstance(cnode, dict) and "results" in cnode:
                    for c in cnode.get("results", []):
                        raw_rev = self._pick_amount(c)
                        if raw_rev:
                            break
                elif isinstance(cnode, list):
                    for c in cnode:
                        raw_rev = self._pick_amount(c)
                        if raw_rev:
                            break
                elif isinstance(cnode, dict):
                    raw_rev = self._pick_amount(cnode)

                if raw_rev:
                    break

            if not raw_rev:
                raw_rev = self._pick_amount(job)

            if sold_date:
                try:
                    rev_str = str(int(float(str(raw_rev)))) if raw_rev else ""
                except (ValueError, TypeError):
                    rev_str = str(raw_rev)

                sold_clean = _clean_date(sold_date)
                # Ensure sold comes after the latest actual QL row (not just first lead signal).
                sold_clean = self._ensure_after(sold_clean, last_appt_dt or first_lead_dt)
                if self._in_range(sold_clean, since, until):
                    _ensure_contact_details()
                    sold_rows_for_contact.append({**base,
                                                  "contact_name": entry.get("contact_name", cname),
                                                  "email": email,
                                                  "phone": phone,
                                                  "conversion_type": ADS_CONV_SOLD,
                                                  "conversion_date": sold_clean,
                                                  "revenue": rev_str})

        # Lifecycle rule: sold lead should also have a qualified lead row.
        has_qualified_row = any(r.get("conversion_type") == ADS_CONV_APPOINTMENT for r in rows)
        if sold_rows_for_contact and not has_qualified_row:
            synthetic_dt = first_lead_dt
            if synthetic_dt is None:
                synthetic_dt = self._to_dt(sold_rows_for_contact[0].get("conversion_date", ""))
                if synthetic_dt is not None:
                    synthetic_dt = synthetic_dt - timedelta(seconds=1)
            synthetic_clean = (
                synthetic_dt.strftime("%Y-%m-%d %H:%M:%S")
                if synthetic_dt is not None else ""
            )

            # Include synthetic/paired qualified lead when:
            # 1) it's in the normal report window, or
            # 2) it's before `since` but inside configured lookback (for cross-period sold rows).
            include_synthetic = self._in_range(synthetic_clean, since, until)
            if (not include_synthetic and synthetic_dt is not None and since is not None
                    and lookback_since is not None and lookback_since <= synthetic_dt < since):
                include_synthetic = True

            if include_synthetic:
                _ensure_contact_details()
                rows.append({**base,
                             "contact_name": entry.get("contact_name", cname),
                             "email": email,
                             "phone": phone,
                             "conversion_type": ADS_CONV_APPOINTMENT,
                             "conversion_date": synthetic_clean,
                             "revenue": str(QUALIFIED_LEAD_VALUE)})

            if synthetic_dt is not None:
                # Floor is the later of synthetic QL date or any real QL rows.
                sold_floor_dt = max(filter(None, [synthetic_dt, last_appt_dt]), default=synthetic_dt)
                sold_rows_for_contact = [
                    {
                        **r,
                        "conversion_date": self._ensure_after(r.get("conversion_date", ""), sold_floor_dt),
                    }
                    for r in sold_rows_for_contact
                ]

        rows.extend(sold_rows_for_contact)
        # Defensive dedupe: some tenants emit repeated appointment/job entities
        # through expanded relationships. Keep stable order, drop exact duplicates.
        deduped: list[dict] = []
        seen: set[tuple] = set()
        for r in rows:
            key = (
                r.get("contact_id", ""),
                r.get("gclid", ""),
                r.get("conversion_type", ""),
                r.get("conversion_date", ""),
                str(r.get("revenue", "")),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(r)
        return deduped

    def build_conversion_rows(self, since: datetime | None = None,
                               until: datetime | None = None,
                               contact_ids: list | None = None) -> list[dict]:
        """Main report: one row per appointment or sold job with a GCLID.

        Each row contains:
            contact_id, contact_name, email, phone, gclid, utm_source,
            utm_medium, utm_campaign, utm_term, utm_content,
            conversion_type, conversion_date, revenue
        """
        rows: list[dict] = []
        gclid_contacts = self.contacts_with_gclid(contact_ids=contact_ids)
        log.info("Found %d contacts with GCLID data", len(gclid_contacts))

        return self.build_conversion_rows_from_contacts(
            gclid_contacts,
            since=since,
            until=until,
            already_logged=True,
        )

    def build_conversion_rows_from_contacts(self, gclid_contacts: list,
                                            since: datetime | None = None,
                                            until: datetime | None = None,
                                            already_logged: bool = False) -> list[dict]:
        """Build conversion rows from pre-fetched contact entries.

        This avoids an extra contact discovery pass when caller already has
        the merged contact set (e.g. assist mode).
        """
        rows: list[dict] = []
        if not already_logged:
            log.info("Found %d contacts with GCLID data", len(gclid_contacts))

        if not gclid_contacts:
            log.info("Built %d conversion rows", len(rows))
            return rows

        # Per-contact work is network-bound (multiple OData GETs), so thread
        # parallelism significantly reduces wall-clock time.
        worker_count = min(max(1, GCLID_MAX_WORKERS), max(1, len(gclid_contacts)))
        if worker_count == 1:
            for entry in gclid_contacts:
                rows.extend(self._build_rows_for_contact(entry, since=since, until=until))
        else:
            log.info("Parallel contact processing enabled: workers=%d", worker_count)
            with _cf.ThreadPoolExecutor(max_workers=worker_count) as ex:
                futures = [ex.submit(self._build_rows_for_contact, e, since, until)
                           for e in gclid_contacts]
                for fut in _cf.as_completed(futures):
                    try:
                        rows.extend(fut.result())
                    except Exception as exc:
                        log.warning("Contact row build task failed: %s", exc)

        log.info("Built %d conversion rows", len(rows))
        return rows

    def _in_range(self, date_str: str, since: datetime | None,
                  until: datetime | None) -> bool:
        """Return True if date_str falls within [since, until).
        Empty date_str is always included (let it through for manual review)."""
        if not date_str:
            return True
        try:
            dt = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return True
        if since and dt < since:
            return False
        if until and dt >= until:
            return False
        return True

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
    """Normalize a date string to local YYYY-MM-DD HH:MM:SS.

    Rules:
      - If source includes a real time, preserve it (converting from UTC when needed).
      - If source is date-only, apply a consistent fallback time (GADS_DATE_ONLY_TIME).
    """

    raw = str(raw or "").strip()
    if not raw:
        return ""

    def _ads_tzinfo():
        if ZoneInfo is None:
            return timezone.utc
        try:
            return ZoneInfo(ADS_TIMEZONE)
        except Exception:
            return timezone.utc

    def _as_local_naive(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(_ads_tzinfo()).replace(tzinfo=None)

    # Strip OData /Date(ms)/ format (milliseconds since epoch UTC)
    if raw.startswith("/Date("):
        try:
            ms = int(raw[6:raw.index(")")])
            dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return _as_local_naive(dt_utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return raw

    # UTC timestamp formats
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return _as_local_naive(dt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # Local timestamp format (no timezone marker)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # Date-only formats: inject a consistent fallback time.
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            d = datetime.strptime(raw, fmt)
            return f"{d.strftime('%Y-%m-%d')} {GADS_DATE_ONLY_TIME}"
        except ValueError:
            pass

    # Last-chance ISO parser (handles offsets like +00:00)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return _as_local_naive(dt).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return raw


def _format_ads_time(dt_str: str) -> str:
    """Format a YYYY-MM-DD HH:MM:SS string into Google Ads offline conversion format.

    Output: M/d/yyyy h:mm:ss AM/PM TZ  (e.g. '5/13/2026 2:30:00 PM America/New_York')
    The timezone is the IANA name appended as a literal suffix, which is what
    Google Ads Offline Conversion Import expects.
    """
    if not dt_str:
        return ""
    try:
        dt = datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S")
        time_part = dt.strftime("%I:%M:%S %p").lstrip("0")  # strip leading zero on hour
        return f"{dt.month}/{dt.day}/{dt.year} {time_part} {ADS_TIMEZONE}"
    except ValueError:
        return dt_str


# ---------------------------------------------------------------------------
# CSVExporter  -- business report CSV
# ---------------------------------------------------------------------------
# CSVExporter  -- business report CSV (appointments + sold jobs with GCLID)
# ---------------------------------------------------------------------------

# Column order matches client-provided offline conversion example
CSV_COLUMNS = [
    "GCLID",
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
        Conversion Name    "Booked Appt" or "Sold Job"
        Conversion Time    Date of appointment or sale (YYYY-MM-DD)
        Conversion Value   Revenue / job cost (blank for appointments)
        Conversion Currency USD (or SPICER_CURRENCY env var)
    """

    def export(self, rows: list, out_path: str) -> int:
        """Write rows to out_path. Returns number of rows written."""
        written = 0
        skipped_no_gclid = 0
        input_rows = len(rows)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            # Google Ads CSV uploads support a leading parameters row.
            f.write(f"Parameters:TimeZone={ADS_TIMEZONE}\n")
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            for row in rows:
                gclid = row.get("gclid", "").strip()
                if not gclid:
                    log.debug("Skipping row with no GCLID: %s", row.get("contact_name"))
                    skipped_no_gclid += 1
                    continue
                conv_time = row.get("date") or row.get("conversion_date", "")
                if not conv_time:
                    conv_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    log.warning("Row for %s has no conversion date — defaulting to today",
                                row.get("contact_name") or row.get("contact_id", ""))
                writer.writerow({
                    "GCLID":             gclid,
                    "Conversion Name":   row.get("conversion_type", ""),
                    "Conversion Time":   _format_ads_time(conv_time),
                    "Conversion Value":  row.get("revenue", ""),
                    "Conversion Currency": CURRENCY,
                })
                written += 1
        if skipped_no_gclid:
            log.warning("Skipped %d/%d conversion row(s) with no GCLID", skipped_no_gclid, input_rows)
        log.info("Exported %d rows to %s", written, out_path)
        return written

    def preview(self, rows: list, limit: int = 20) -> None:
        """Print a table preview to stdout."""
        fmt = "{:<30} {:<26} {:<16} {:<22} {}"
        header = fmt.format("Contact Name", "GCLID", "Type", "Date", "Value")
        print(header)
        print("-" * len(header))
        for row in rows[:limit]:
            print(fmt.format(
                (row.get("contact_name") or row.get("contact_id", ""))[:29],
                row.get("gclid", "")[:25],
                row.get("conversion_type", "")[:15],
                (row.get("date") or row.get("conversion_date", ""))[:21],
                row.get("revenue", ""),
            ))
        if len(rows) > limit:
            print(f"  ... and {len(rows) - limit} more rows")

# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gclid_sync",
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

    pp = sub.add_parser("probe", help="Dump raw OData records to debug entity/field names")
    pp.add_argument("--contact-id", default=None, help="Scope to a specific contact ID")
    pp.add_argument("--limit", type=int, default=3, help="Number of records to fetch (default 3)")
    pp.add_argument("--filter", dest="note_filter", default=None,
                    help="Raw OData $filter string (default: no filter)")
    pp.add_argument("--entity", default=None,
                    help="Specific entity to query (default: probe all candidates)")
    pp.add_argument("--expand", default=None,
                    help="OData $expand parameter (e.g. 'Appointment' to expand related records)")

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

    elif args.command == "probe":
        svc = _ms_service()
        if not svc:
            print("ERROR: MarketSharpService unavailable")
            sys.exit(1)

        base_url = _odata_url()
        candidate_entities = (
            [args.entity] if args.entity
            else ["Notes", "Inquiries", "Inquiry", "Leads", "WebLeads", "Activities"]
        )

        for entity in candidate_entities:
            url = f"{base_url}/{entity}"
            params: dict = {"$top": str(args.limit)}
            if args.expand:
                params["$expand"] = args.expand
            if args.contact_id:
                for flt in (f"contactId eq '{args.contact_id}'",
                            f"contactId eq guid'{args.contact_id}'"):
                    params["$filter"] = flt
                    r = _req.get(url, headers=svc._odata_headers(), params=params, timeout=15)
                    print(f"\n{'='*60}")
                    print(f"GET {entity}  filter={flt!r}  -> HTTP {r.status_code}")
                    if r.status_code == 404:
                        print("  (entity not found)")
                        break
                    if r.status_code == 400:
                        print(f"  400 body: {r.text[:300]}")
                        continue
                    try:
                        data = r.json().get("d", {})
                        items = data.get("results", data) if isinstance(data, dict) else data
                        items = items if isinstance(items, list) else [items]
                        print(f"  {len(items)} record(s)")
                        for i, rec in enumerate(items, 1):
                            txt = _inquiry_text(rec)
                            parsed = _parse_gclid_note(txt) if txt else {}
                            print(f"  --- record {i} ---")
                            print(f"  keys: {sorted(rec.keys())}")
                            print(f"  inquiry_text ({len(txt)} chars): {txt[:400]!r}")
                            if parsed:
                                print(f"  parsed: {parsed}")
                    except Exception as e:
                        print(f"  parse error: {e}")
                        print(f"  raw: {r.text[:500]}")
                    if r.status_code == 200:
                        break  # don't retry with guid syntax if plain string 200'd
            else:
                # No contact filter — just grab first N records to see schema
                if args.note_filter:
                    params["$filter"] = args.note_filter
                r = _req.get(url, headers=svc._odata_headers(), params=params, timeout=15)
                print(f"\n{'='*60}")
                print(f"GET {entity}  -> HTTP {r.status_code}")
                if r.status_code != 200:
                    print(f"  body: {r.text[:300]}")
                    continue
                try:
                    data = r.json().get("d", {})
                    items = data.get("results", data) if isinstance(data, dict) else data
                    items = items if isinstance(items, list) else [items]
                    print(f"  {len(items)} record(s)")
                    if items:
                        print(f"  keys: {sorted(items[0].keys())}")
                        txt = _inquiry_text(items[0])
                        print(f"  inquiry_text ({len(txt)} chars): {txt[:300]!r}")
                except Exception as e:
                    print(f"  parse error: {e}")
                    print(f"  raw: {r.text[:500]}")
        print()


if __name__ == "__main__":
    main()
