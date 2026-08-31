"""Helpers for loading and maintaining MarketSharp contact URL mappings."""

import json
from pathlib import Path


def _normalize_mapping_dict(mapping_dict):
    if mapping_dict and not isinstance(mapping_dict, dict):
        raise ValueError('Contact URL mappings must be a JSON object')

    normalized = {}
    for key, value in (mapping_dict or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError('Contact URL mapping keys and values must be strings')
        normalized[key.strip()] = value.strip()
    return normalized


def load_mapping_file(mapping_file_path):
    """Load a JSON mapping file when present."""
    if not mapping_file_path:
        return {}

    path = Path(mapping_file_path)
    if not path.exists():
        return {}

    try:
        mapping_dict = json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid contact mapping file JSON at {path}: {exc}') from exc

    return _normalize_mapping_dict(mapping_dict)


def load_mapping_env(mapping_json_raw):
    """Load JSON mappings from env var content."""
    raw_value = (mapping_json_raw or '').strip()
    if not raw_value:
        return {}

    try:
        mapping_dict = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid MARKETSHARP_UI_CONTACT_URL_MAP JSON: {exc}') from exc

    return _normalize_mapping_dict(mapping_dict)


def merge_contact_mappings(file_mappings, env_mappings):
    """Merge file-backed mappings with env overrides."""
    merged = dict(file_mappings or {})
    merged.update(env_mappings or {})
    return merged


def save_mapping_file(mapping_file_path, mapping_dict):
    """Persist contact mappings in a stable JSON format."""
    path = Path(mapping_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_mapping_dict(mapping_dict)
    path.write_text(json.dumps(normalized, indent=2, sort_keys=True) + '\n', encoding='utf-8')
