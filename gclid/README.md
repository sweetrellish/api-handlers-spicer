# GCLID Reporting Submodule

This directory contains the MarketSharp-to-Google-Ads reporting pipeline used to:

- capture and sync GCLID/UTM values into MarketSharp
- generate monthly offline conversion CSV exports for Google Ads
- audit eligibility so low coverage can be explained and monitored
- summarize audit results for client/stakeholder review
- support monthly production runs and future systemd scheduling

The current conclusion is that the module is production ready for monthly reporting, provided the upstream GCLID capture process and the reporting window policy are agreed with the client.

## What This Submodule Does

The pipeline starts with contacts in MarketSharp, finds contacts whose inquiry notes contain a parseable GCLID, queries appointments and jobs tied to those contacts, and emits a Google Ads offline-conversion CSV. The same pipeline now also includes a contact-level audit that explains why contacts were included or excluded.

### Core outcomes

- Export monthly conversion rows in the Google Ads offline conversion format.
- Distinguish between eligible rows and contacts excluded by upstream data quality.
- Produce an executive summary with coverage percentages and primary bottlenecks.
- Provide a repeatable operating workflow for monthly reporting.

## Repository Layout

- [`gclid_sync.py`](gclid_sync.py): core MarketSharp query engine, row builder, and CSV export logic.
- [`gclid_worker.py`](gclid_worker.py): monthly report runner and CLI entry point.
- [`audit_contact_eligibility.py`](audit_contact_eligibility.py): per-contact audit with exclusion reasons.
- [`summarize_eligibility_audit.py`](summarize_eligibility_audit.py): executive summary generator for audit CSV output.
- [`compare_months.py`](compare_months.py): coverage comparison across multiple months.
- [`check_raw_queries.py`](check_raw_queries.py): raw OData inspection tool for appointments/jobs/contact data.
- [`debug_contact_rows.py`](debug_contact_rows.py): per-contact row tracing and fallback diagnostics.
- [`find_parseable_contacts.py`](find_parseable_contacts.py): identify contacts with parseable GCLID notes.
- [`diagnose_export.py`](diagnose_export.py): broader export diagnostics and sanity checks.
- [`validate_all.py`](validate_all.py): run the validation suite against a generated CSV.
- [`validate_csv_schema.py`](validate_csv_schema.py): schema and format checks.
- [`validate_lifecycle.py`](validate_lifecycle.py): verify Qualified Lead and Sold Job ordering.
- [`validate_times.py`](validate_times.py): inspect timestamp diversity and formatting behavior.
- [`verify_deployment.py`](verify_deployment.py): compare local vs deployed script content.
- [`spicer_ops_menu.py`](../spicer_ops_menu.py): admin menu integration for production use.

## Development History

The module evolved in three stages:

1. **Baseline export pipeline**
   - sync GCLID/UTM data into MarketSharp
   - export appointments and sold jobs to Google Ads CSV

2. **Debugging and refinement**
   - diagnose low coverage on February 2025 exports
   - add row-level tracing and raw query inspection
   - fix duplicate row inflation in the row builder
   - verify that the issue was primarily upstream data quality and timing, not the exporter itself

3. **Production hardening**
   - add a contact-level eligibility audit
   - add an executive summary generator
   - integrate the audit workflow into the operations menu
   - prepare the module for scheduled monthly execution

## How The Pipeline Works

### 1. Contact discovery

The pipeline searches MarketSharp inquiry records and/or a supplied CSV contact list for notes containing a parseable GCLID. The supported note shapes include:

- URL query-string notes with `gclid=`
- HTML or plain-text `GCLID: value` notes
- structured internal notes written by the sync tool

### 2. Event extraction

For contacts with parseable GCLID values, the row builder pulls:

- appointments for the contact
- sold jobs for the contact
- contact details when available
- revenue fields for sold-job rows

### 3. Row shaping

The module emits rows in the Google Ads offline conversion layout:

- `GCLID`
- `Conversion Name`
- `Conversion Time`
- `Conversion Value`
- `Conversion Currency`

### 4. Validation

After export, the validation suite checks:

- CSV schema and header format
- lifecycle ordering
- time diversity and consistency

### 5. Eligibility audit

The audit workflow counts contacts that:

- have parseable GCLID values
- have events in the selected month
- would produce exportable rows
- were excluded, and why

## Local Development Setup

### Requirements

- Python 3.10 or newer
- Access to MarketSharp credentials and OData endpoints
- Optional: `python-dotenv` for `.env` loading
- Optional: `requests` for live API access

### Run a report locally

```bash
cd gclid
python gclid_worker.py --month 2025-02 --contacts-csv ms-report-hasGCLID.csv --contacts-mode assist
```

### Run the audit locally

```bash
cd gclid
python audit_contact_eligibility.py --contacts-csv "Contacts (1).csv" --month 2025-02 --out eligibility_audit_2025-02.csv
python summarize_eligibility_audit.py --audit-csv eligibility_audit_2025-02.csv --month 2025-02 --out eligibility_summary_2025-02.txt
```

### Run validation

```bash
cd gclid
python validate_all.py
```

### Inspect a difficult contact

```bash
cd gclid
python debug_contact_rows.py <contact_id>
python check_raw_queries.py <contact_id>
```

## Production Workflow

### Monthly operator flow

1. Run the monthly export.
2. Run the validation suite.
3. Run the eligibility audit and summary.
4. Review the summary before sending to the client or uploading to Google Ads.
5. Archive the CSV, audit CSV, and summary together.

### Recommended monthly artifacts

- `spicer_conversions_YYYYMM.csv`
- `eligibility_audit_YYYY-MM.csv`
- `eligibility_summary_YYYY-MM.txt`

### What to review in the summary

- `empty_or_unparseable_gclid` rising month-over-month usually means lead-capture formatting drift.
- `events_outside_month` usually means the reporting window should be broadened or a lookback should be approved.
- `rows_built_zero_despite_events` should stay near zero; if not, inspect row-builder logic.

## Systemd Readiness

This module is suitable for a `systemd` timer/oneshot design if the following are true:

- credentials are available on the target host
- the target host can reach MarketSharp APIs
- the monthly contact CSV is available or reproducible on the host
- the client agrees on the attribution policy and reporting window
- the operator can archive the generated outputs after each run

### Suggested service shape

- `gclid-report.service`: run one monthly export and exit
- `gclid-report.timer`: trigger on the desired schedule

### Operational checks before enabling systemd

- verify `python gclid_worker.py --month YYYY-MM ...` succeeds on the server
- verify `validate_all.py` passes for the generated CSV
- verify the audit summary is saved alongside the CSV
- verify logs are retained long enough for failure review

## Current Findings And Decision Guidance

The February 2025 audit showed that low coverage was mostly caused by upstream data quality, not exporter failure.

- Most contacts lacked a parseable GCLID value.
- A smaller group had valid GCLIDs but events outside the target month.
- A small edge-case group needs follow-up, but it does not change the overall conclusion.

This means the best next action depends on business policy:

- If the goal is higher conversion coverage, fix the lead-capture source so GCLID is always populated.
- If the goal is broader attribution, approve a lookback or wider reporting window.
- If the goal is operational safety, keep the audit and validation steps in every monthly run.

## Deployment Notes

For deployed hosts, keep the following aligned:

- `gclid_sync.py`
- `gclid_worker.py`
- audit and summary scripts
- validation scripts
- the operations menu entry that runs the audit flow

A deployment is complete only when the server copy, validation output, and monthly report artifacts all agree.

## Suggested Next Improvements

- Add a small wrapper script for unattended monthly runs.
- Add a systemd unit file and timer definition.
- Add a README-linked checklist for monthly operator handoff.
- Track monthly coverage metrics in a simple history file or dashboard.
- Keep edge-case contacts under review until the row-builder mismatch is fully explained.

## Practical Rule

If the audit says the exporter would only produce a small fraction of contacts, do not treat that as a failure of the export code until the audit explains why.

That distinction is the reason this submodule now has both a report generator and an eligibility auditor.
