"""Mention notification worker for Spicer MarketSharp operations.

This worker supports two input sources:
1. api_comments: read comment payloads from API_URL/comments (local/dev path)
2. marketsharp_notes: poll MarketSharp OData Notes directly (server/prod path)

For each note/comment it extracts @username mentions, resolves recipient email
addresses from tagger/marketsharp_user-email.json, and sends notifications via
the configured relay endpoint.
"""

import os
import sys
import importlib.util
import re
import time
import json
import argparse
import html
from collections import defaultdict
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
import requests
from dotenv import load_dotenv
from config import Config   

# Ensure src imports resolve even when launched outside wrapper scripts.
_TAGGER_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_TAGGER_DIR)
_SRC_DIR = os.path.join(_ROOT_DIR, "src")
for _p in (_ROOT_DIR, _SRC_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

_MS_SERVICE_PATH = os.path.join(_SRC_DIR, "marketsharp_service.py")
_MS_SPEC = importlib.util.spec_from_file_location("spicer_src_marketsharp_service", _MS_SERVICE_PATH)
if _MS_SPEC is None or _MS_SPEC.loader is None:
    raise ImportError(f"Unable to load MarketSharpService module from {_MS_SERVICE_PATH}")
_MS_MODULE = importlib.util.module_from_spec(_MS_SPEC)
_MS_SPEC.loader.exec_module(_MS_MODULE)
MarketSharpService = _MS_MODULE.MarketSharpService

# Load environment variables from .env file
load_dotenv()

# Define the CommentWorker class
class CommentWorker:

    # Initialize the CommentWorker with configuration settings
    def __init__(
        self,
        email_api_url_override=None,
        email_api_key_override=None,
        email_api_query_token_override=None,
        poll_seconds_override=None,
        source_override=None,
        bootstrap_process_existing_override=None,
    ):
        # Load configuration settings from the Config class
        self.config = Config()

        # Set API URLs and keys from the configuration
        self.api_url = os.getenv("API_URL")
        if not self.api_url:
            self.api_url = self._derive_api_url_from_webhook_url(os.getenv("WEBHOOK_URL", ""))
        self.email_api_url = email_api_url_override or os.getenv("EMAIL_API_URL")
        self.email_api_key = email_api_key_override or os.getenv("EMAIL_API_KEY")
        self.email_api_query_token = (
            email_api_query_token_override or os.getenv("EMAIL_API_QUERY_TOKEN")
        )
        self.source = (source_override or os.getenv("COMMENT_WORKER_SOURCE", "api_comments")).strip().lower()
        self.poll_seconds = int(poll_seconds_override or os.getenv("COMMENT_WORKER_POLL_SECONDS", "60"))
        self.http_timeout_seconds = int(os.getenv("COMMENT_WORKER_HTTP_TIMEOUT_SECONDS", "20"))
        self.state_file = os.getenv(
            "COMMENT_WORKER_STATE_FILE",
            os.path.join(os.path.dirname(__file__), "comment_worker_state.json"),
        )
        bootstrap_default = os.getenv("COMMENT_WORKER_BOOTSTRAP_PROCESS_EXISTING", "false").lower() == "true"
        self.bootstrap_process_existing = (
            bootstrap_default
            if bootstrap_process_existing_override is None
            else bootstrap_process_existing_override
        )
        self.ambiguous_alias_mode = (
            os.getenv("COMMENT_WORKER_AMBIGUOUS_ALIAS_MODE", "all").strip().lower() or "all"
        )
        self.require_explicit_mentions = (
            os.getenv("COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS", "true").strip().lower()
            == "true"
        )
        try:
            self.max_recipients_per_comment = int(
                os.getenv("COMMENT_WORKER_MAX_RECIPIENTS_PER_COMMENT", "15")
            )
        except ValueError:
            self.max_recipients_per_comment = 15

        self.user_email_map_file = os.getenv(
            "MARKETSHARP_USER_EMAIL_MAP_FILE",
            os.path.join(os.path.dirname(__file__), "marketsharp_user-email.json"),
        )
        self.user_email_map = self.load_user_email_map()
        self.email_to_usernames = self.build_email_to_usernames(self.user_email_map)
        self.group_map_file = os.getenv(
            "MARKETSHARP_GROUP_MENTION_MAP_FILE",
            os.path.join(os.path.dirname(__file__), "marketsharp_group_mentions.json"),
        )
        self.group_alias_to_recipients = self.load_group_mentions_map()
        self.alias_to_username, self.ambiguous_aliases = self.build_alias_index(self.user_email_map)
        self.seen_comment_ids = set()
        self.state = self.load_state()
        self.ms_service = MarketSharpService() if self.source == "marketsharp_notes" else None
        self.print_email_config_status()

    @staticmethod
    def _normalize_token(value):
        token = (value or "").strip().lower()
        if token.startswith("@"):
            token = token[1:]
        return token

    @staticmethod
    def build_email_to_usernames(user_email_map):
        """Build reverse email lookup to support group members declared by email."""
        reverse = defaultdict(set)
        for username, email in (user_email_map or {}).items():
            key = (username or "").strip().lower()
            mail = (email or "").strip().lower()
            if not key or "@" not in mail:
                continue
            reverse[mail].add(key)
        return {k: sorted(v) for k, v in reverse.items()}

    def _member_to_recipient_tokens(self, member):
        """Resolve a group member spec into internal recipient tokens.

        Returns username tokens (e.g. "rellis") and/or direct email tokens
        (e.g. "email:person@example.com").
        """
        token = self._normalize_token(member)
        if not token:
            return []

        if "@" in token:
            usernames = self.email_to_usernames.get(token, [])
            if usernames:
                return usernames
            return [f"email:{token}"]

        if token in self.user_email_map:
            return [token]
        if token in self.alias_to_username:
            return [self.alias_to_username[token]]
        return []

    def _collect_group_recipients(self, raw_group_data):
        """Collect recipient tokens from group JSON payload.

        Accepts either:
        - ["member1", "member2"]
        - {"members": [...], "aliases": [...]}
        """
        if isinstance(raw_group_data, list):
            members = raw_group_data
            aliases = []
        elif isinstance(raw_group_data, dict):
            members = raw_group_data.get("members", [])
            aliases = raw_group_data.get("aliases", [])
        else:
            return set(), []

        recipients = set()
        for member in members:
            for recipient in self._member_to_recipient_tokens(member):
                recipients.add(recipient)
        alias_tokens = [self._normalize_token(a) for a in aliases if self._normalize_token(a)]
        return recipients, alias_tokens

    def load_group_mentions_map(self):
        """Load mention groups from optional JSON map.

        Always includes default aliases for notifying all mapped users:
        everyone, all, allhands, team.
        """
        all_user_recipients = set((self.user_email_map or {}).keys())
        groups = {
            "everyone": set(all_user_recipients),
            "all": set(all_user_recipients),
            "allhands": set(all_user_recipients),
            "team": set(all_user_recipients),
        }

        try:
            with open(self.group_map_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, dict):
                print(f"Group map file is not an object: {self.group_map_file}")
                return groups

            for group_name, raw_group_data in payload.items():
                group_key = self._normalize_token(group_name)
                if not group_key:
                    continue
                recipients, aliases = self._collect_group_recipients(raw_group_data)
                if not recipients:
                    continue

                groups.setdefault(group_key, set()).update(recipients)
                for alias in aliases:
                    groups.setdefault(alias, set()).update(recipients)

            print(
                f"Loaded group mention map from {self.group_map_file} "
                f"({len(groups)} group aliases)."
            )
            return groups
        except FileNotFoundError:
            # Optional file: not required for default everyone/all behavior.
            return groups
        except Exception as e:
            print(f"Failed to load group mention map {self.group_map_file}: {e}")
            return groups

    @staticmethod
    def _mask_secret(value):
        if not value:
            return "<missing>"
        value = str(value)
        if len(value) <= 6:
            return "***"
        return f"{value[:3]}...{value[-3:]}"

    def print_email_config_status(self):
        has_url = bool(self.email_api_url)
        has_key = bool(self.email_api_key)
        has_query_token = bool(self.email_api_query_token)
        has_url_token = self._has_token_in_url(self.email_api_url)
        print(
            "Email config status: "
            f"url={'set' if has_url else 'missing'}, "
            f"bearer_key={'set' if has_key else 'missing'}, "
            f"query_token={'set' if has_query_token else 'missing'}, "
            f"url_has_token={'yes' if has_url_token else 'no'}"
        )
        if has_query_token:
            print(f"Email query token: {self._mask_secret(self.email_api_query_token)}")
        print(
            f"Worker runtime: poll_seconds={self.poll_seconds}, "
            f"http_timeout_seconds={self.http_timeout_seconds}"
        )
        print(f"Worker API base: {self.api_url or '<missing>'}")
        print(
            f"Worker source: {self.source}, state_file={self.state_file}, "
            f"bootstrap_process_existing={'yes' if self.bootstrap_process_existing else 'no'}"
        )
        print(
            f"Mention safety: require_explicit_mentions={'yes' if self.require_explicit_mentions else 'no'}, "
            f"max_recipients_per_comment={self.max_recipients_per_comment}"
        )
        print(
            f"Mention alias index: {len(self.alias_to_username)} aliases, "
            f"{len(self.ambiguous_aliases)} ambiguous, "
            f"ambiguous_alias_mode={self.ambiguous_alias_mode}"
        )
        print(
            f"Mention group aliases: {len(self.group_alias_to_recipients)} "
            f"(source: {self.group_map_file})"
        )

    @staticmethod
    def _derive_api_url_from_webhook_url(webhook_url):
        """Derive API base URL from WEBHOOK_URL when API_URL is not set."""
        if not webhook_url:
            return ""
        parsed = urlsplit(webhook_url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        return urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")

    def has_email_api_config(self):
        has_url_token = self._has_token_in_url(self.email_api_url)
        has_auth = bool(self.email_api_key or self.email_api_query_token or has_url_token)
        return bool(self.email_api_url and has_auth)

    @staticmethod
    def _has_token_in_url(url):
        if not url:
            return False
        parsed = urlsplit(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        return bool(query.get("token"))

    def validate_listen_config(self):
        missing = []
        if self.source == "api_comments" and not self.api_url:
            missing.append("API_URL (or WEBHOOK_URL for auto-derive)")
        if self.source == "marketsharp_notes":
            if not Config.MARKETSHARP_COMPANY_ID:
                missing.append("MARKETSHARP_COMPANY_ID")
            if not Config.MARKETSHARP_USER_KEY:
                missing.append("MARKETSHARP_USER_KEY")
            if not Config.MARKETSHARP_SECRET_KEY:
                missing.append("MARKETSHARP_SECRET_KEY")
        if missing:
            raise ValueError(
                "Missing required environment variables for listen mode: "
                + ", ".join(missing)
            )

    def validate_email_config(self):
        missing = []
        if not self.email_api_url:
            missing.append("EMAIL_API_URL")
        has_url_token = self._has_token_in_url(self.email_api_url)
        if not self.email_api_key and not self.email_api_query_token and not has_url_token:
            missing.append("EMAIL_API_KEY or EMAIL_API_QUERY_TOKEN")
        if missing:
            raise ValueError(
                "Email sending requires: " + ", ".join(missing)
            )

    def _build_email_endpoint(self):
        """Return email endpoint URL and auth headers.

        Supports either:
        - Authorization Bearer token via EMAIL_API_KEY
        - Query token via EMAIL_API_QUERY_TOKEN (for Apps Script relays)
        """
        url = self.email_api_url or ""
        headers = {}

        if self.email_api_key:
            headers["Authorization"] = f"Bearer {self.email_api_key}"

        if self.email_api_query_token:
            parsed = urlsplit(url)
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            query["token"] = self.email_api_query_token
            url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))

        return url, headers

    def load_user_email_map(self):
        """Load username->email mapping from disk."""
        try:
            with open(self.user_email_map_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                print(f"User map file is not an object: {self.user_email_map_file}")
                return {}

            mapping = {}
            for username, email in raw.items():
                if not isinstance(username, str) or not isinstance(email, str):
                    continue
                key = username.strip().lower()
                value = email.strip().lower()
                if not key or "@" not in value:
                    continue
                mapping[key] = value

            print(f"Loaded {len(mapping)} user email mappings from {self.user_email_map_file}")
            return mapping
        except FileNotFoundError:
            print(f"User map file not found: {self.user_email_map_file}")
            return {}
        except Exception as e:
            print(f"Failed to load user map file {self.user_email_map_file}: {e}")
            return {}

    def build_alias_index(self, user_email_map):
        """Build alias lookup from explicit dictionary keys only.

        Ambiguous aliases are still tracked when multiple explicit dictionary
        keys resolve to the same token, but no inferred name aliases are added.
        """
        alias_candidates = {}
        for username, email in (user_email_map or {}).items():
            key = (username or "").strip().lower()
            if not key:
                continue

            # Username itself is always a valid alias.
            alias_candidates.setdefault(key, set()).add(key)

        alias_to_username = {}
        ambiguous_aliases = {}
        for alias, usernames in alias_candidates.items():
            if len(usernames) == 1:
                alias_to_username[alias] = next(iter(usernames))
            else:
                ambiguous_aliases[alias] = sorted(usernames)

        return alias_to_username, ambiguous_aliases

    def load_state(self):
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return {}
            return raw
        except FileNotFoundError:
            return {}
        except Exception as e:
            print(f"Failed to load worker state {self.state_file}: {e}")
            return {}

    def save_state(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Failed to save worker state {self.state_file}: {e}")

    def fetch_comments(self):   
        """Fetch comments from the Marketsharp API."""
        try:
            # Make a GET request to the comments endpoint of the Marketsharp API
            comments_url = f"{self.api_url}/comments"
            response = requests.get(comments_url, timeout=self.http_timeout_seconds)
            if response.status_code == 404:
                print(
                    f"Error fetching comments: '{comments_url}' returned 404. "
                    "No /comments endpoint is available at the configured API base."
                )
                return []
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                print(f"Error fetching comments: expected list, got {type(payload).__name__}")
                return []
            return payload
        
        # Handle any exceptions that occur during the API request
        except requests.RequestException as e:
            print(f"Error fetching comments: {e}")
            return []

    @staticmethod
    def _extract_note_id(note):
        if not isinstance(note, dict):
            return ""
        for key in ("id", "Id", "noteId", "NoteID"):
            value = note.get(key)
            if value is not None:
                return str(value)
        return ""

    @staticmethod
    def _extract_note_text(note):
        if not isinstance(note, dict):
            return ""
        for key in ("note", "Note", "text", "Text", "body", "Body"):
            value = note.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_note_timestamp(note):
        if not isinstance(note, dict):
            return ""
        for key in ("dateTime", "DateTime", "createdDate", "CreatedDate", "modifiedDate", "ModifiedDate"):
            value = note.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    @staticmethod
    def _extract_note_contact_id(note):
        if not isinstance(note, dict):
            return ""
        for key in ("contactId", "ContactId", "contact_id", "ContactID"):
            value = note.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    @staticmethod
    def _normalize_odata_payload(payload):
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []

        if isinstance(payload.get("value"), list):
            return payload["value"]

        data = payload.get("d")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                return data["results"]
        return []

    def fetch_marketsharp_notes(self):
        if not self.ms_service:
            return []

        try:
            response = requests.get(
                f"{self.ms_service.odata_url}/Notes?$orderby=dateTime desc&$top=50",
                headers=self.ms_service._odata_headers(),
                timeout=self.http_timeout_seconds,
            )
            response.raise_for_status()
            notes = self._normalize_odata_payload(response.json())
            if not isinstance(notes, list):
                return []
            return notes
        except requests.RequestException as e:
            print(f"Error fetching MarketSharp notes: {e}")
            return []

    def build_comment_from_note(self, note):
        text = self._extract_note_text(note)
        note_id = self._extract_note_id(note)
        timestamp = self._extract_note_timestamp(note)
        contact_id = self._extract_note_contact_id(note)
        if not text or not note_id:
            return None
        return {
            "id": note_id,
            "text": text,
            "timestamp": timestamp,
            "contact_id": contact_id,
            "source": "marketsharp_notes",
        }

    def fetch_marketsharp_note_comments(self):
        notes = self.fetch_marketsharp_notes()
        comments = []
        state_note_ids = set(self.state.get("processed_note_ids", []))
        last_seen_timestamp = self.state.get("last_seen_note_timestamp", "")

        for note in notes:
            comment = self.build_comment_from_note(note)
            if not comment:
                continue
            note_id = comment["id"]
            timestamp = comment.get("timestamp", "")
            if note_id in state_note_ids:
                continue
            if last_seen_timestamp and timestamp and timestamp <= last_seen_timestamp:
                continue
            comments.append(comment)

        comments.sort(key=lambda item: item.get("timestamp", ""))

        if not self.state and notes and not self.bootstrap_process_existing:
            newest = self.build_comment_from_note(notes[0])
            if newest:
                self.state["last_seen_note_timestamp"] = newest.get("timestamp", "")
                self.state["processed_note_ids"] = [newest["id"]]
                self.save_state()
                print("Bootstrap complete: seeded MarketSharp notes cursor without processing historical notes.")
            return []

        return comments

    @staticmethod
    def get_comment_id(comment):
        if not isinstance(comment, dict):
            return ""
        for key in ("id", "comment_id", "commentId"):
            value = comment.get(key)
            if value is not None:
                return str(value)
        return ""

    def mark_comment_processed(self, comment):
        comment_id = self.get_comment_id(comment)
        if comment_id:
            self.seen_comment_ids.add(comment_id)

        if self.source == "marketsharp_notes":
            processed = list(self.state.get("processed_note_ids", []))
            if comment_id and comment_id not in processed:
                processed.append(comment_id)
            self.state["processed_note_ids"] = processed[-500:]
            timestamp = comment.get("timestamp") or ""
            if timestamp:
                current = self.state.get("last_seen_note_timestamp", "")
                if not current or timestamp > current:
                    self.state["last_seen_note_timestamp"] = timestamp
            self.save_state()

# Extract mentions from comment text using explicit @tokens by default.
    def extract_mentions(self, comment):

        """Extract recipient usernames from comment text.

        Default behavior processes explicit @mentions only. Optional plain-text
        inference can be enabled with COMMENT_WORKER_REQUIRE_EXPLICIT_MENTIONS=false.
        """
        pattern = r'(?<!\w)@([A-Za-z0-9]+)\b'
        found = re.findall(pattern, comment or "")
        # Normalize and dedupe while preserving mention order.
        seen = set()
        mentions = []
        for raw_token in found:
            token = self._normalize_token(raw_token)
            if not token:
                continue

            # Group keywords only expand when explicitly mentioned with @.
            group_recipients = sorted(self.group_alias_to_recipients.get(token, []))
            if group_recipients:
                for recipient in group_recipients:
                    if recipient in seen:
                        continue
                    seen.add(recipient)
                    mentions.append(recipient)
                continue

            key = token
            if key in seen:
                continue
            seen.add(key)
            mentions.append(key)

        if not self.require_explicit_mentions:
            # Optional fallback mode for environments that intentionally allow
            # plain-text dictionary keys to infer recipients.
            inferred = self.infer_mentions_from_plain_text(comment or "")
            for username in inferred:
                key = username.lower()
                if key in seen:
                    continue
                seen.add(key)
                mentions.append(key)
        return mentions

    def infer_mentions_from_plain_text(self, comment):
        """Infer usernames from plain text using the alias index.

        This path is disabled by default in production safety mode.
        """
        tokens = re.findall(r"\b[A-Za-z0-9]{3,}\b", comment or "")
        inferred = []
        seen = set()
        for token in tokens:
            alias = token.strip().lower()

            username = self.alias_to_username.get(alias)
            if username:
                if username in seen:
                    continue
                seen.add(username)
                inferred.append(username)
                continue

            ambiguous = self.ambiguous_aliases.get(alias, [])
            if not ambiguous:
                continue

            if self.ambiguous_alias_mode == "skip":
                continue

            # In "all" mode, notify all mapped users sharing that alias.
            if self.ambiguous_alias_mode == "all":
                for candidate in ambiguous:
                    if candidate in seen:
                        continue
                    seen.add(candidate)
                    inferred.append(candidate)
                continue

            # Fallback: deterministic first candidate if mode is unknown.
            first = ambiguous[0]
            if first not in seen:
                seen.add(first)
                inferred.append(first)
        return inferred

    def resolve_email_for_username(self, username):
        """Resolve mention username to an email using the mapping file."""
        if isinstance(username, str) and username.startswith("email:"):
            email = username.split(":", 1)[1].strip().lower()
            return email if "@" in email else None
        return self.user_email_map.get((username or "").strip().lower())

    def process_comment_text(self, comment_text, send_email=True, source="api"):
        """Parse @mentions and send/preview notifications for one comment body."""
        if send_email and not self.has_email_api_config():
            print(
                f"[{source}] Email sending disabled: missing EMAIL_API_URL and auth token."
            )
            print(
                f"[{source}] Use --email-api-url with --email-api-key or --email-api-query-token, or set them in .env."
            )
            return

        mentions = self.extract_mentions(comment_text)
        if not mentions:
            print(f"[{source}] No @mentions found.")
            return

        if self.max_recipients_per_comment > 0 and len(mentions) > self.max_recipients_per_comment:
            print(
                f"[{source}] Mention safety block: resolved {len(mentions)} recipients, "
                f"max allowed is {self.max_recipients_per_comment}. Skipping sends."
            )
            print(f"[{source}] Resolved recipients: {mentions}")
            return

        notified_emails = set()
        for username in mentions:
            recipient_email = self.resolve_email_for_username(username)
            display_name = username
            if isinstance(username, str) and username.startswith("email:"):
                display_name = username.split(":", 1)[1]
            if not recipient_email:
                print(f"[{source}] No mapped email for mentioned user '@{display_name}', skipping.")
                continue

            # Multiple usernames can map to the same mailbox (for example when
            # ambiguous aliases fan out). Send one email per recipient inbox.
            recipient_key = recipient_email.strip().lower()
            if recipient_key in notified_emails:
                print(
                    f"[{source}] Recipient {recipient_email} already notified in this comment; "
                    f"skipping duplicate alias '@{display_name}'."
                )
                continue
            notified_emails.add(recipient_key)

            if send_email:
                self.send_email_notification(display_name, recipient_email, comment_text)
            else:
                print(
                    f"[{source}] Dry run: would notify '@{display_name}' at {recipient_email} "
                    f"for comment: {comment_text}"
                )

    @staticmethod
    def build_html_body(comment):
        """Return an HTML email body with @mentions emphasized."""
        escaped = html.escape(comment or "")

        # Bolden @mentions in HTML payload while preserving safe escaping.
        def _highlight(match):
            mention = match.group(0)
            return f"<strong>{mention}</strong>"

        highlighted = re.sub(r"(?<!\w)@[A-Za-z0-9]+\b", _highlight, escaped)
        return (
            "<p>You were mentioned in the following comment:</p>"
            f"<p>{highlighted}</p>"
        )

# Send an email notification to the user mentioned in the comment
    def send_email_notification(self, username, recipient_email, comment):
        """Send an email notification to the user mentioned in the comment."""

        # Prepare the email data with the recipient's email address, subject, and body of the email
        email_data = {
            "to": recipient_email,
            "subject": "You were mentioned in a comment",
            "body": f"You were mentioned in the following comment: {comment}",
            "html_body": self.build_html_body(comment),
            "htmlBody": self.build_html_body(comment),
        }
        try:
            email_api_url, headers = self._build_email_endpoint()
            # Make a POST request to the email API to send the email notification
            response = requests.post(
                email_api_url,
                headers=headers,
                json=email_data,
                timeout=self.http_timeout_seconds,
            )
            # Raise an exception if the request was unsuccessful
            response.raise_for_status()

            # Apps Script may return HTTP 200 even when the payload reports an app-level error.
            relay_json = {}
            try:
                relay_json = response.json()
            except ValueError:
                snippet = (response.text or "")[:300].replace("\n", " ")
                content_type = response.headers.get("content-type", "")
                redirects = len(response.history)
                print(
                    f"Relay response for {recipient_email} was not JSON "
                    f"(status {response.status_code}, content-type '{content_type}', "
                    f"redirects {redirects}, final-url {response.url}). Body preview: {snippet}"
                )
                return

            if relay_json.get("ok") is False:
                print(
                    f"Relay reported failure for {recipient_email}: "
                    f"{relay_json.get('error', 'unknown error')}"
                )
                return

            if relay_json.get("ok") is not True:
                print(
                    f"Relay response missing explicit ok=true for {recipient_email}: {relay_json}"
                )
                return

            print(f"Relay response for {recipient_email}: {relay_json}")
            print(f"Email sent to {username} at {recipient_email}")
            # Handle any exceptions that occur during the API request
        except requests.RequestException as e:
            print(f"Error sending email to {username} at {recipient_email}: {e}")

    def run(self, run_once=False):
        """Run the comment worker to fetch comments and send notifications."""
        # Continuously fetch comments and process them in an infinite loop
        while True:
            if self.source == "marketsharp_notes":
                comments = self.fetch_marketsharp_note_comments()
            else:
                comments = self.fetch_comments()
            for comment in comments:
                comment_id = self.get_comment_id(comment)
                if comment_id and comment_id in self.seen_comment_ids:
                    continue

                comment_text = comment.get('text', '')

                self.process_comment_text(comment_text, send_email=True, source="api")

                self.mark_comment_processed(comment)

            if run_once:
                print("Run-once mode complete.")
                return

            time.sleep(self.poll_seconds)


def build_test_message(base_message, tags):
    """Build a single message string for test mode."""
    parts = []
    existing_mentions = set()

    if base_message:
        normalized_message = base_message.strip()
        parts.append(normalized_message)
        existing_mentions = {
            match.lower() for match in re.findall(r"(?<!\w)@([A-Za-z0-9]+)\b", normalized_message)
        }

    appended_tags = set()
    for tag in tags:
        normalized = (tag or "").strip().lstrip("@").lower()
        if not normalized:
            continue
        if normalized in existing_mentions or normalized in appended_tags:
            continue
        parts.append(f"@{normalized}")
        appended_tags.add(normalized)

    return " ".join(parts).strip()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Comment worker for @username mention notifications.",
    )
    parser.add_argument(
        "--mode",
        choices=["listen", "test"],
        default=None,
        help="Optional explicit mode. If omitted, test mode is inferred when --message, --tag, or --send-test-emails is used.",
    )
    parser.add_argument(
        "--message",
        default="",
        help="Comment text for test mode (can include @mentions).",
    )
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Username to tag in test mode; repeat flag for multiple users. Using this flag implies test mode.",
    )
    parser.add_argument(
        "--tag-all-mapped-users",
        action="store_true",
        help="In test mode, append every username from the loaded mapping as @mentions.",
    )
    parser.add_argument(
        "--send-test-emails",
        action="store_true",
        help="In test mode, actually call the email API. Default is dry-run preview only. Using this flag implies test mode.",
    )
    parser.add_argument(
        "--email-api-url",
        default="",
        help="Optional override for EMAIL_API_URL.",
    )
    parser.add_argument(
        "--email-api-key",
        default="",
        help="Optional override for EMAIL_API_KEY.",
    )
    parser.add_argument(
        "--email-api-query-token",
        default="",
        help="Optional override for EMAIL_API_QUERY_TOKEN (appended as ?token=... on EMAIL_API_URL).",
    )
    parser.add_argument(
        "--poll-seconds",
        default="",
        help="Optional listen-mode poll interval override (seconds).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="In listen mode, fetch/process comments once and exit.",
    )
    parser.add_argument(
        "--source",
        choices=["api_comments", "marketsharp_notes"],
        default="",
        help="Listener source: local/API /comments feed or direct MarketSharp OData Notes polling.",
    )
    parser.add_argument(
        "--bootstrap-process-existing",
        action="store_true",
        help="On first marketsharp_notes run, process existing fetched notes instead of seeding cursor and skipping history.",
    )
    return parser.parse_args()


def determine_mode(args):
    """Infer the execution mode from explicit mode or provided test inputs."""
    if args.mode:
        return args.mode
    if args.message or args.tag or args.send_test_emails:
        return "test"
    return "listen"


def main():
    args = parse_args()
    worker = CommentWorker(
        email_api_url_override=(args.email_api_url or None),
        email_api_key_override=(args.email_api_key or None),
        email_api_query_token_override=(args.email_api_query_token or None),
        poll_seconds_override=(args.poll_seconds or None),
        source_override=(args.source or None),
        bootstrap_process_existing_override=(True if args.bootstrap_process_existing else None),
    )
    mode = determine_mode(args)

    if mode == "listen":
        worker.validate_listen_config()
        worker.run(run_once=args.once)
        return

    test_tags = list(args.tag)
    if args.tag_all_mapped_users:
        test_tags.extend(sorted(worker.user_email_map.keys()))

    message = build_test_message(args.message, test_tags)
    if not message:
        raise ValueError("Test mode requires --message and/or at least one --tag.")

    if args.send_test_emails:
        worker.validate_email_config()

    worker.process_comment_text(
        message,
        send_email=args.send_test_emails,
        source="test",
    )


if __name__ == "__main__":
    main()
    