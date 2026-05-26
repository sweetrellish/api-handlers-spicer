"""Shared mention styling helpers for MarketSharp note text."""

import json
import os
import re


_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_SRC_DIR)


class VerifiedMentionResolver:
    """Resolve @tokens to canonical usernames using local mapping."""

    def __init__(self):
        self.ambiguous_alias_mode = (
            os.getenv('COMMENT_WORKER_AMBIGUOUS_ALIAS_MODE', 'all').strip().lower() or 'all'
        )
        self.require_explicit_mentions = (
            os.getenv('MARKETSHARP_NOTE_REQUIRE_EXPLICIT_MENTIONS', 'true').strip().lower() == 'true'
        )
        self.user_email_map = self._load_user_email_map()
        self.alias_to_username, self.ambiguous_aliases = self._build_alias_index(self.user_email_map)

    @staticmethod
    def _normalize_token(value):
        token = (value or '').strip().lower()
        if token.startswith('@'):
            token = token[1:]
        return token

    def _load_user_email_map(self):
        map_file = os.getenv(
            'MARKETSHARP_USER_EMAIL_MAP_FILE',
            os.path.join(_ROOT_DIR, 'tagger', 'marketsharp_user-email.json'),
        )
        try:
            with open(map_file, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                return {}
            mapping = {}
            for username, email in payload.items():
                key = (username or '').strip().lower()
                val = (email or '').strip().lower()
                if key and '@' in val:
                    mapping[key] = val
            return mapping
        except Exception:
            return {}

    def _build_alias_index(self, user_email_map):
        # Only explicit dictionary keys are valid aliases here. If human names
        # should resolve, add them as explicit keys in the mapping file.
        alias_candidates = {}
        for username, email in (user_email_map or {}).items():
            key = (username or '').strip().lower()
            if not key:
                continue
            alias_candidates.setdefault(key, set()).add(key)

        alias_to_username = {}
        ambiguous_aliases = {}
        for alias, usernames in alias_candidates.items():
            if len(usernames) == 1:
                alias_to_username[alias] = next(iter(usernames))
            else:
                ambiguous_aliases[alias] = sorted(usernames)
        return alias_to_username, ambiguous_aliases

    def resolve_explicit_at_token(self, token):
        key = self._normalize_token(token)
        if not key:
            return None
        if key in self.user_email_map:
            return key
        if key in self.alias_to_username:
            return self.alias_to_username[key]

        ambiguous = self.ambiguous_aliases.get(key, [])
        if not ambiguous:
            return None
        if self.ambiguous_alias_mode == 'skip':
            return None
        return ambiguous[0]


_verified_mention_resolver = None


def _get_verified_mention_resolver():
    global _verified_mention_resolver
    if _verified_mention_resolver is None:
        _verified_mention_resolver = VerifiedMentionResolver()
    return _verified_mention_resolver


def apply_note_mention_style(note_text, style):
    """Apply mention normalization/styling to note text.

    Base behavior normalizes verified mentions to canonical `@username` text.
    The style argument is retained for compatibility with existing callers.
    """
    full_text = note_text or ''

    # Preserve leading metadata prefixes like:
    #   [Ryan Ellis] hey ryan testing
    #   [Company Cam] [Ryan Ellis] hey ryan testing
    # and apply mention styling only to the note body.
    prefix_match = re.match(r'^((?:\[[^\]]+\]\s*)+)(.*)$', full_text, flags=re.DOTALL)
    if prefix_match:
        lead_prefix = prefix_match.group(1)
        text = prefix_match.group(2)
    else:
        lead_prefix = ''
        text = full_text

    resolver = _get_verified_mention_resolver()

    def _wrap(canonical):
        return f"@{canonical}"

    def _normalize_explicit_if_verified(match):
        token = match.group(1)
        start = match.start(1)
        end = match.end(1)

        if start >= 2 and text[start - 2:start] == '[@' and end < len(text) and text[end] == ']':
            return match.group(0)

        if start >= 3 and text[start - 3:start] == '**@':
            if end + 1 < len(text) and text[end:end + 2] == '**':
                return match.group(0)

        canonical = resolver.resolve_explicit_at_token(token)
        if not canonical:
            return match.group(0)
        return _wrap(canonical)

    text = re.sub(
        r'(?<!\w)@([A-Za-z0-9]+)\b',
        _normalize_explicit_if_verified,
        text,
    )

    if resolver.require_explicit_mentions:
        return f"{lead_prefix}{text}"

    def _normalize_standalone_if_verified(match):
        token = match.group(1)
        start = match.start(1)
        end = match.end(1)

        # Skip tokens that are already part of explicit @mentions.
        if start >= 1 and text[start - 1] == '@':
            return token

        if start >= 2 and text[start - 2:start] == '[@' and end < len(text) and text[end] == ']':
            return token

        if start >= 3 and text[start - 3:start] == '**@':
            if end + 1 < len(text) and text[end:end + 2] == '**':
                return token

        canonical = resolver.resolve_explicit_at_token(token)
        if not canonical:
            return token
        return _wrap(canonical)

    styled = re.sub(
        r'\b([A-Za-z][A-Za-z0-9]{1,63})\b',
        _normalize_standalone_if_verified,
        text,
    )
    return f"{lead_prefix}{styled}"
