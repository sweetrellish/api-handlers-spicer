#!/usr/bin/env python3
"""Read-first OData agent for backend visibility in Spicer.

Provides connectivity checks, endpoint catalog, controlled browsing,
custom queries with safe caps, preset management, and exports.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
for _p in (str(ROOT), str(ROOT / "src"), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from marketsharp_service import MarketSharpService  # noqa: E402

DATA_DIR = ROOT / "data"
PRESET_FILE = DATA_DIR / "odata_agent_presets.json"
LAST_RESULT_FILE = DATA_DIR / "odata_agent_last_result.json"

DEFAULT_TOP = 25
MAX_TOP = 100


CATALOG = [
    ("Notes", "Recent notes and note text payloads"),
    ("Contacts()", "Contact entities with fields for matching and routing"),
    ("Activities", "Scheduled activity entities"),
]


def ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def redact_value(value):
    text = str(value or "")
    if len(text) <= 6:
        return "***"
    return text[:3] + "..." + text[-3:]


def service_client():
    return MarketSharpService()


def normalize_top(value):
    top = int(value or DEFAULT_TOP)
    if top < 1:
        top = DEFAULT_TOP
    if top > MAX_TOP:
        top = MAX_TOP
    return top


def fetch_entity(path, params=None, timeout=20):
    svc = service_client()
    clean_path = (path or "").strip().lstrip("/")
    if not clean_path:
        raise ValueError("entity path is required")

    url = f"{svc.odata_url}/{clean_path}"
    response = requests.get(
        url,
        headers=svc._odata_headers(verbose=True),
        params=params or {},
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()

    rows = []
    if isinstance(payload, dict):
        if isinstance(payload.get("value"), list):
            rows = payload["value"]
        elif isinstance(payload.get("d"), dict) and isinstance(payload["d"].get("results"), list):
            rows = payload["d"]["results"]
        elif isinstance(payload.get("d"), list):
            rows = payload["d"]
        elif isinstance(payload.get("d"), dict):
            rows = [payload["d"]]
        else:
            rows = [payload]
    elif isinstance(payload, list):
        rows = payload

    return {"url": url, "params": params or {}, "rows": rows, "raw": payload}


def save_last_result(result):
    ensure_data_dir()
    with open(LAST_RESULT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)


def load_last_result():
    if not LAST_RESULT_FILE.exists():
        return None
    with open(LAST_RESULT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_presets():
    if not PRESET_FILE.exists():
        return {}
    with open(PRESET_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def save_presets(presets):
    ensure_data_dir()
    with open(PRESET_FILE, "w", encoding="utf-8") as f:
        json.dump(presets, f, indent=2, sort_keys=True)


def cmd_check(_args):
    svc = service_client()
    print("odata_url:", svc.odata_url)
    print("company_id:", redact_value(svc.company_id))
    result = fetch_entity("Contacts()", params={"$top": "1"}, timeout=20)
    print("connection: ok")
    print("rows:", len(result["rows"]))
    save_last_result(result)


def cmd_catalog(_args):
    print("OData catalog")
    for name, desc in CATALOG:
        print(f"- {name}: {desc}")


def cmd_browse(args):
    entity = args.entity
    top = normalize_top(args.top)
    params = {"$top": str(top)}
    if args.filter:
        params["$filter"] = args.filter
    if args.orderby:
        params["$orderby"] = args.orderby
    if args.select:
        params["$select"] = args.select

    result = fetch_entity(entity, params=params, timeout=args.timeout)
    save_last_result(result)

    print("query_url:", result["url"])
    if result["params"]:
        print("query_params:", urlencode(result["params"]))
    print("row_count:", len(result["rows"]))

    preview_n = min(args.preview, len(result["rows"]))
    for i in range(preview_n):
        print(f"[{i+1}] {json.dumps(result['rows'][i], default=str)[:500]}")


def cmd_query(args):
    top = normalize_top(args.top)
    params = {"$top": str(top)}

    for pair in args.param:
        if "=" not in pair:
            raise SystemExit(f"invalid --param value: {pair}")
        key, value = pair.split("=", 1)
        params[key] = value

    result = fetch_entity(args.path, params=params, timeout=args.timeout)
    save_last_result(result)

    print("query_url:", result["url"])
    if result["params"]:
        print("query_params:", urlencode(result["params"]))
    print("row_count:", len(result["rows"]))


def cmd_preset_save(args):
    presets = load_presets()
    top = normalize_top(args.top)
    payload = {
        "path": args.path,
        "top": top,
        "params": {},
    }
    for pair in args.param:
        if "=" not in pair:
            raise SystemExit(f"invalid --param value: {pair}")
        key, value = pair.split("=", 1)
        payload["params"][key] = value
    presets[args.name] = payload
    save_presets(presets)
    print(f"saved preset: {args.name}")


def cmd_preset_run(args):
    presets = load_presets()
    if args.name not in presets:
        raise SystemExit(f"preset not found: {args.name}")
    payload = presets[args.name]
    params = dict(payload.get("params") or {})
    params["$top"] = str(normalize_top(payload.get("top", DEFAULT_TOP)))
    result = fetch_entity(payload["path"], params=params, timeout=args.timeout)
    save_last_result(result)
    print(f"preset: {args.name}")
    print("path:", payload["path"])
    print("row_count:", len(result["rows"]))


def cmd_preset_list(_args):
    presets = load_presets()
    if not presets:
        print("no presets")
        return
    for name in sorted(presets):
        payload = presets[name]
        print(f"{name}: path={payload.get('path')} top={payload.get('top')}")


def cmd_export(args):
    result = load_last_result()
    if not result:
        raise SystemExit("no last result available; run check/browse/query first")

    rows = result.get("rows") or []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output = args.output or str(DATA_DIR / f"odata_agent_export_{stamp}.{args.format}")

    ensure_data_dir()

    if args.format == "json":
        if not output.endswith(".json"):
            output += ".json"
        with open(output, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
    else:
        if not output.endswith(".csv"):
            output += ".csv"
        headers = set()
        for row in rows:
            if isinstance(row, dict):
                headers.update(row.keys())
        fieldnames = sorted(headers)
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row if isinstance(row, dict) else {"value": str(row)})

    print(f"exported {len(rows)} rows -> {output}")


def parse_args():
    parser = argparse.ArgumentParser(description="OData agent")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Run OData connectivity check")
    sub.add_parser("catalog", help="Show endpoint catalog")

    p_browse = sub.add_parser("browse", help="Browse a known entity")
    p_browse.add_argument("--entity", choices=["Notes", "Contacts()", "Activities"], required=True)
    p_browse.add_argument("--filter", default="")
    p_browse.add_argument("--orderby", default="")
    p_browse.add_argument("--select", default="")
    p_browse.add_argument("--top", type=int, default=DEFAULT_TOP)
    p_browse.add_argument("--timeout", type=int, default=20)
    p_browse.add_argument("--preview", type=int, default=5)

    p_query = sub.add_parser("query", help="Run custom OData query path")
    p_query.add_argument("--path", required=True, help="Path such as Notes or Contacts()")
    p_query.add_argument("--param", action="append", default=[], help="Repeated key=value params")
    p_query.add_argument("--top", type=int, default=DEFAULT_TOP)
    p_query.add_argument("--timeout", type=int, default=20)

    p_save = sub.add_parser("preset-save", help="Save named query preset")
    p_save.add_argument("--name", required=True)
    p_save.add_argument("--path", required=True)
    p_save.add_argument("--param", action="append", default=[])
    p_save.add_argument("--top", type=int, default=DEFAULT_TOP)

    p_run = sub.add_parser("preset-run", help="Run named query preset")
    p_run.add_argument("--name", required=True)
    p_run.add_argument("--timeout", type=int, default=20)

    sub.add_parser("preset-list", help="List presets")

    p_export = sub.add_parser("export", help="Export last query result")
    p_export.add_argument("--format", choices=["json", "csv"], default="json")
    p_export.add_argument("--output", default="")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "check":
        cmd_check(args)
    elif args.command == "catalog":
        cmd_catalog(args)
    elif args.command == "browse":
        cmd_browse(args)
    elif args.command == "query":
        cmd_query(args)
    elif args.command == "preset-save":
        cmd_preset_save(args)
    elif args.command == "preset-run":
        cmd_preset_run(args)
    elif args.command == "preset-list":
        cmd_preset_list(args)
    elif args.command == "export":
        cmd_export(args)
    else:
        raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
