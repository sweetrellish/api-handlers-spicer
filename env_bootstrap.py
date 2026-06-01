"""Utilities for loading the repo root .env file reliably."""

from __future__ import annotations

import os
from pathlib import Path


def _parse_env_file(env_file: Path, override: bool = False) -> bool:
    loaded = False
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if not override and key in os.environ:
            continue

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ[key] = value
        loaded = True

    return loaded


def load_repo_env(project_root: str | Path, override: bool = False) -> bool:
    """Load the .env file from the repo root, with a manual fallback."""
    env_file = Path(project_root) / ".env"
    if not env_file.exists():
        return False

    try:
        from dotenv import load_dotenv as _load_dotenv  # type: ignore
    except Exception:
        return _parse_env_file(env_file, override=override)

    return bool(_load_dotenv(env_file, override=override))