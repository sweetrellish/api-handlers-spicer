#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

if [[ -x "$VENV_PYTHON" ]]; then
  PYTHON_BIN="$VENV_PYTHON"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
else
  echo "[preflight] warning: python3 unavailable; skipping comment-worker preflight." >&2
  exit 0
fi

"$PYTHON_BIN" - "$SCRIPT_DIR" <<'PY'
import os
import sys
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

script_dir = sys.argv[1]
env_file = os.path.join(script_dir, ".env")

enabled = os.getenv("COMMENT_WORKER_PREFLIGHT_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
strict = os.getenv("COMMENT_WORKER_PREFLIGHT_STRICT", "false").strip().lower() in ("1", "true", "yes", "on")
timeout_s = float(os.getenv("COMMENT_WORKER_PREFLIGHT_TIMEOUT_SECONDS", "6"))

if not enabled:
    print("[preflight] disabled by COMMENT_WORKER_PREFLIGHT_ENABLED=false")
    raise SystemExit(0)


def load_env_defaults(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            key = k.strip()
            if not key or key in os.environ:
                continue
            val = v.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            os.environ[key] = val


def derive_splash_url(webhook_url: str) -> str:
    if not webhook_url:
        return ""
    parsed = urlsplit(webhook_url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return urlunsplit((parsed.scheme, parsed.netloc, "/assets/email-splash.png", "", ""))


def check_url(url: str, timeout: float) -> tuple[bool, str]:
    if not url:
        return False, "missing URL"
    for method in ("HEAD", "GET"):
        try:
            req = Request(url, method=method)
            with urlopen(req, timeout=timeout) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                if 200 <= int(status) < 400:
                    return True, f"{method} {status}"
                return False, f"{method} {status}"
        except HTTPError as exc:
            if exc.code in (405, 501) and method == "HEAD":
                continue
            return False, f"{method} HTTP {exc.code}"
        except URLError as exc:
            if method == "HEAD":
                continue
            return False, f"{method} url-error: {exc.reason}"
        except Exception as exc:
            if method == "HEAD":
                continue
            return False, f"{method} error: {exc}"
    return False, "request failed"


load_env_defaults(env_file)

webhook_url = (os.getenv("WEBHOOK_URL", "") or "").strip()
splash_mode = (os.getenv("COMMENT_WORKER_SPLASH_MODE", "ascii") or "ascii").strip().lower()
splash_raw = (os.getenv("COMMENT_WORKER_SPLASH_IMAGE_URL", "") or "").strip()
splash_auto = (not splash_raw) or splash_raw.lower() == "auto"
splash_url = derive_splash_url(webhook_url) if splash_auto else splash_raw

warnings: list[str] = []

print("[preflight] comment worker startup checks")
print(f"[preflight] webhook_url={webhook_url or '<missing>'}")
print(f"[preflight] splash_mode={splash_mode}, splash_source={'derived' if splash_auto else 'explicit'}")
print(f"[preflight] splash_url={splash_url or '<missing>'}")

if not webhook_url:
    warnings.append("WEBHOOK_URL is missing; tunnel-derived APIs and splash auto mode cannot track host rotation")
else:
    parsed_webhook = urlsplit(webhook_url)
    if not parsed_webhook.scheme or not parsed_webhook.netloc:
        warnings.append("WEBHOOK_URL is not a valid absolute URL")

if splash_url:
    ok, status = check_url(splash_url, timeout_s)
    if ok:
        print(f"[preflight] splash check: ok ({status})")
    else:
        warnings.append(f"splash URL is not reachable ({status})")

if webhook_url and splash_url:
    webhook_host = urlsplit(webhook_url).netloc
    splash_host = urlsplit(splash_url).netloc
    if webhook_host and splash_host and webhook_host != splash_host:
        warnings.append(
            "splash host differs from webhook host; this can drift after tunnel rotation "
            f"(webhook={webhook_host}, splash={splash_host})"
        )

if warnings:
    for msg in warnings:
        print(f"[preflight] warning: {msg}")
    if strict:
        print("[preflight] strict mode enabled; aborting startup due to warnings")
        raise SystemExit(2)
    print("[preflight] continuing startup (set COMMENT_WORKER_PREFLIGHT_STRICT=true to enforce)")
else:
    print("[preflight] all checks passed")
PY