#!/usr/bin/env python3
"""Summarize eligibility audit CSV into an executive report."""

import argparse
import csv
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize eligibility audit CSV")
    p.add_argument("--audit-csv", required=True, help="Path to eligibility audit CSV")
    p.add_argument("--month", default="", help="Optional month label (YYYY-MM)")
    p.add_argument("--out", default="", help="Optional output text file")
    return p.parse_args()


def pct(n: int, d: int) -> str:
    if d <= 0:
        return "0.0%"
    return f"{(100.0 * n / d):.1f}%"


def build_summary(rows: list[dict], month_label: str) -> str:
    total = len(rows)
    reasons = Counter(r.get("exclusion_reason", "") for r in rows)

    parseable = sum(1 for r in rows if (r.get("parseable_gclid") or "").upper() == "Y")
    in_month = sum(1 for r in rows if (r.get("has_event_in_month") or "").upper() == "Y")
    exportable = sum(1 for r in rows if (r.get("would_export") or "").upper() == "Y")

    # This metric is intentionally narrower than the raw event count: it only
    # reports contacts that both have a parseable GCLID and have at least one
    # appointment/job record available to the row builder.
    ql_job_present = sum(
        1 for r in rows
        if (int(r.get("appointments_found", 0) or 0) > 0 or int(r.get("jobs_found", 0) or 0) > 0)
        and (r.get("parseable_gclid") or "").upper() == "Y"
    )

    header = "Eligibility Audit Executive Summary"
    if month_label:
        header += f" ({month_label})"

    lines = []
    lines.append("=" * 72)
    lines.append(header)
    lines.append("=" * 72)
    lines.append("")
    lines.append("Topline")
    lines.append(f"- Contacts audited: {total}")
    lines.append(f"- Parseable GCLID: {parseable} ({pct(parseable, total)})")
    lines.append(f"- Any appointments/jobs found: {ql_job_present} ({pct(ql_job_present, total)})")
    lines.append(f"- Has event in target month: {in_month} ({pct(in_month, total)})")
    lines.append(f"- Would export: {exportable} ({pct(exportable, total)})")
    lines.append("")

    lines.append("Exclusion Breakdown")
    ordered = [
        "empty_or_unparseable_gclid",
        "no_appointments_or_jobs",
        "events_outside_month",
        "rows_built_zero_despite_events",
        "would_export",
    ]
    for key in ordered:
        n = reasons.get(key, 0)
        lines.append(f"- {key}: {n} ({pct(n, total)})")
    lines.append("")

    lines.append("Interpretation")
    if total == 0:
        lines.append("- No rows were present in the audit CSV.")
    else:
        top_reason = max((k for k in ordered if k != "would_export"), key=lambda k: reasons.get(k, 0))
        if top_reason == "empty_or_unparseable_gclid":
            lines.append("- Primary bottleneck is missing or unparseable GCLID values in inquiry notes.")
        elif top_reason == "events_outside_month":
            lines.append("- Primary bottleneck is timing: events exist but are outside the selected month.")
        elif top_reason == "no_appointments_or_jobs":
            lines.append("- Primary bottleneck is event availability: contacts lack appointment/job records.")
        else:
            lines.append("- Primary bottleneck is row build eligibility despite event presence.")

        if reasons.get("rows_built_zero_despite_events", 0) == 0:
            lines.append("- Row builder logic appears consistent for contacts with in-month eligible events.")
        else:
            lines.append("- Some contacts had events but still built zero rows; inspect row-build logic for edge cases.")

    lines.append("")
    lines.append("Recommended Actions")
    lines.append("- Standardize lead capture note format so GCLID value is always populated when key is present.")
    lines.append("- Track monthly conversion eligibility with this audit before report export.")
    lines.append("- For low-volume months, broaden reporting window or include lookback analysis where allowed.")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    audit_csv = Path(args.audit_csv)
    if not audit_csv.exists():
        print(f"Audit CSV not found: {audit_csv}")
        return 1

    with open(audit_csv, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    summary = build_summary(rows, args.month)
    print(summary)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(summary, encoding="utf-8")
        print(f"Saved summary: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
