# GCLID Integration — Spicer Bros. API Handler

## Overview

This feature connects Google Ads click tracking (GCLID + UTM parameters) to
MarketSharp CRM conversions, producing a monthly CSV that can be uploaded to
Google Ads as offline conversion data.

### Data flow

```text
Google Ads click
    └─▶ Website landing page (UTM + GCLID in URL)
            └─▶ Contact form submission
                    └─▶ MarketSharp email trigger (creates note with GCLID fields)
                            └─▶ gclid_worker.py (runs 1st of month)
                                    └─▶ spicer_conversions_YYYYMM.csv
                                            └─▶ Upload to Google Ads
```

### Note format written by website email triggers

MarketSharp notes from the website lead automation look like:

```text
Source: Website Leads Lead Capture<br>HeardAboutUs: Internet<br>
Interest: Roofing<br>Source: google<br>Medium: cpc<br>
Campaign: iMedia One<br>SRC: Google-Ads<br>
GCLID: CjwKCAjwwpDQBhAuEiwAa-4Wo...
```

The parser (`_parse_website_lead_note`) handles HTML `<td>`/`<br>` tags, maps
field names to internal keys, and deduplicates the two `Source:` values.

---

## Files

| File | Purpose |
| ---- | ------- |
| `gclid/gclid_sync.py` | Core module — parser, `ReportBuilder`, `CSVExporter`, CLI |
| `gclid/gclid_worker.py` | Monthly report runner — one-shot or daemon mode |
| `gclid/__init__.py` | Package exports |
| `deploy/linux/spicer-gclid-report.service` | systemd oneshot service |
| `deploy/linux/spicer-gclid-report.timer` | systemd timer (1st of month, 06:00) |

The root-level `gclid-ms.py` is the canonical development copy; `gclid/gclid_sync.py`
is the deployed version (identical content, snake_case name for importability).

---

## Environment variables

Add these to `/home/rellis/spicer/.env`:

```dotenv
# ── GCLID / Google Ads ──────────────────────────────────────────────────────

# How GCLID fields are written when using the `write` CLI command.
# "note"        → append a [GCLID] key=value note  (default, always works)
# "customfield" → POST to ContactCustomFields (requires MS_FIELD_ID_* vars)
MARKETSHARP_GCLID_WRITE_MODE=note

# Required only when WRITE_MODE=customfield.  Set these to the HTML field IDs
# found in MarketSharp CRM → Custom Fields inspector.
MS_FIELD_ID_GCLID=
MS_FIELD_ID_UTM_SOURCE=
MS_FIELD_ID_UTM_MEDIUM=
MS_FIELD_ID_UTM_CAMPAIGN=
MS_FIELD_ID_UTM_TERM=
MS_FIELD_ID_UTM_CONTENT=

# Output directory for monthly CSV exports
GCLID_REPORT_OUT_DIR=/home/rellis/spicer/data

# Day of month and hour to run the automated report (used by daemon mode)
GCLID_REPORT_DAY=1
GCLID_REPORT_HOUR=6

# Currency code in exported CSV (default: USD)
SPICER_CURRENCY=USD
```

---

## Deployment

### 1 — Copy service files

```bash
sudo cp deploy/linux/spicer-gclid-report.service /etc/systemd/system/
sudo cp deploy/linux/spicer-gclid-report.timer   /etc/systemd/system/
sudo systemctl daemon-reload
```

### 2 — Enable and start the timer

```bash
sudo systemctl enable --now spicer-gclid-report.timer
systemctl list-timers spicer-gclid-report.timer
```

### 3 — Test a manual run

```bash
# Generate last month's report right now
cd /home/rellis/spicer
.venv/bin/python3 gclid/gclid_worker.py

# Generate for a specific month
.venv/bin/python3 gclid/gclid_worker.py --month 2026-04

# Preview contacts with GCLID (no file written)
.venv/bin/python3 gclid-ms.py report --preview
```

### 4 — Access from the admin console

```bash
python3 spicer_ops_menu.py
# → press [G] for GCLID & Conversion Report
```

---

## CSV output format

Output filename: `spicer_conversions_YYYYMM.csv`

Columns match the Google Ads offline conversions upload template:

| Column | Source |
| ------ | ------- |
| `GCLID` | Parsed from MarketSharp note |
| `Email` | Contact email from OData |
| `Phone Number` | Contact phone from OData |
| `Conversion Name` | `Booked Appt` or `Sold Job` |
| `Conversion Time` | Appointment date or sold date (YYYY-MM-DD) |
| `Conversion Value` | Job cost / contract amount (blank for appointments) |
| `Conversion Currency` | `USD` (or `SPICER_CURRENCY`) |

Rows with no match key (no GCLID, email, or phone) are skipped automatically.

---

## `gclid_sync.py` CLI reference

```python
python3 gclid-ms.py --help

Commands:
  write    Push GCLID/UTM fields onto a MarketSharp contact by ID
  sync     Resolve contact by name+address then write fields
  report   Build the conversion CSV
  fields   Show configured custom field IDs from .env

Examples:
  python3 gclid-ms.py write --contact-id 12345 --gclid AW-abc --utm-source google
  python3 gclid-ms.py report --since 2026-05-01 --out /tmp/may.csv --preview
  python3 gclid-ms.py report --preview
```

---

## Note format field mapping

The website email-trigger note format is parsed as follows:

| Raw note field | Internal key | CSV column |
| -------------- | ------------- | ------------ |
| `GCLID` | `gclid` | `GCLID` |
| `Source` (last value) | `utm_source` | *(internal only)* |
| `Source` (first value) | `lead_source` | *(internal only)* |
| `Medium` | `utm_medium` | *(internal only)* |
| `Campaign` | `utm_campaign` | `Conversion Name` context |
| `SRC` | `utm_content` | *(internal only)* |
| `HeardAboutUs` | `heard_about_us` | *(internal only)* |
| `Interest` | `interest` | *(internal only)* |

---

## Contact filter (MarketSharp)

A saved contact filter has been configured in MarketSharp to surface all contacts
whose notes contain `GCLID`. Use this as a manual verification tool to confirm
the automated worker is capturing all leads.

Filter criteria: **Notes contain → "GCLID"**

