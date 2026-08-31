#!/usr/bin/env python3
"""One-command pipeline: merge Google Admin group CSVs and build mention group JSON.

This wraps:
- scripts/merge_google_group_csvs.py
- scripts/build_group_mentions_from_google_csv.py

Typical use:
  python3 scripts/import_google_groups_to_mentions.py \
      --input-dir /path/to/group-csvs \
      --default-domain spicerbros.com \
      --output-json tagger/marketsharp_group_mentions.json
"""

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from merge_google_group_csvs import merge_csvs  # noqa: E402
from build_group_mentions_from_google_csv import build_mapping  # noqa: E402


def main():
    parser = argparse.ArgumentParser(
        description="Merge Google group CSV exports and build mention-group JSON in one step"
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing per-group CSV exports")
    parser.add_argument("--glob", default="*.csv", help="Glob pattern for CSV files")
    parser.add_argument(
        "--merged-csv",
        default="data/google_groups_members_merged.csv",
        help="Intermediate merged CSV output path",
    )
    parser.add_argument(
        "--output-json",
        default="tagger/marketsharp_group_mentions.json",
        help="Final mention-group JSON output path",
    )
    parser.add_argument(
        "--default-domain",
        default="",
        help="Default domain for deriving group email from filename (example: spicerbros.com)",
    )
    parser.add_argument("--group-column", default="group_email", help="Merged CSV column for group email")
    parser.add_argument("--member-column", default="member_email", help="Merged CSV column for member email")
    parser.add_argument(
        "--group-name-column",
        default="group_name",
        help="Merged CSV column for group display name",
    )
    parser.add_argument("--verbose", action="store_true", help="Print per-file diagnostics")
    args = parser.parse_args()

    files, rows, merged_path = merge_csvs(
        input_dir=args.input_dir,
        glob_pattern=args.glob,
        output_csv=args.merged_csv,
        default_domain=args.default_domain,
        verbose=args.verbose,
    )

    mapping = build_mapping(
        input_csv=str(merged_path),
        group_col=args.group_column,
        member_col=args.member_column,
        group_name_col=args.group_name_column,
    )

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(__import__("json").dumps(mapping, indent=2), encoding="utf-8")

    print(f"Merged {len(files)} CSV file(s).")
    print(f"Merged rows: {len(rows)}")
    print(f"Merged CSV: {merged_path}")
    print(f"Group entries: {len(mapping)}")
    print(f"Mention JSON: {output_json}")


if __name__ == "__main__":
    main()
