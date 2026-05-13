"""Background worker that posts queued comments through the MarketSharp web UI.

This is intended for environments where API write access is unavailable but a
human-authenticated browser session can be maintained.
"""

import logging
import os
import time
import json
import re
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from companycam_service import CompanyCamService
from config import Config
from marketsharp_service import MarketSharpService
from mapping_registry import load_mapping_env, load_mapping_file, merge_contact_mappings
from pending_queue import PendingCommentQueue
from posted_comments_audit import log_posted_comment


@dataclass
class UiConfig:
    base_url: str
    user_data_dir: str
    headless: bool
    batch_size: int
    poll_seconds: int
    processing_timeout_seconds: int
    unmatched_retry_seconds: int
    unmatched_dump_path: str
    search_input_selector: str
    first_result_selector: str
    notes_tab_selector: str
    note_button_selector: str
    note_input_selector: str
    note_save_selector: str
    contact_url_map_file: str
    contact_url_map: dict
    contact_type: str
    login_check_selector: str
    login_company_id: str
    login_username: str
    login_password: str


_cc_service_instance = None


def _get_companycam_service():
    """Lazily initialize CompanyCam service for address lookups during queue replay."""
    global _cc_service_instance
    if _cc_service_instance is None:
        _cc_service_instance = CompanyCamService()
    return _cc_service_instance


def build_ui_config():
    """Load UI automation selectors from environment variables."""
    contact_url_map_file = os.getenv(
        'MARKETSHARP_UI_CONTACT_URL_MAP_FILE',
        'marketsharp_contact_mappings.json',
    ).strip()
    file_mappings = load_mapping_file(contact_url_map_file)
    env_mappings = load_mapping_env(os.getenv('MARKETSHARP_UI_CONTACT_URL_MAP', ''))
    merged_contact_url_map = merge_contact_mappings(file_mappings, env_mappings)

    return UiConfig(
        base_url=os.getenv('MARKETSHARP_UI_BASE_URL', '').strip(),
        user_data_dir=os.getenv('MARKETSHARP_UI_USER_DATA_DIR', '.marketsharp-profile').strip(),
        headless=os.getenv('MARKETSHARP_UI_HEADLESS', 'False').lower() == 'true',
        batch_size=int(os.getenv('QUEUE_WORKER_BATCH_SIZE', '5')),
        poll_seconds=1,  # Set poll interval to 1 second for rapid queue cycling
        processing_timeout_seconds=int(os.getenv('QUEUE_PROCESSING_TIMEOUT_SECONDS', '1800')),
        unmatched_retry_seconds=int(os.getenv('QUEUE_UNMATCHED_RETRY_SECONDS', '10')),
        unmatched_dump_path=os.getenv('QUEUE_UNMATCHED_DUMP_PATH', 'unmatched_comments.jsonl').strip(),
        search_input_selector=os.getenv('MARKETSHARP_UI_SEARCH_SELECTOR', '').strip(),
        first_result_selector=os.getenv('MARKETSHARP_UI_FIRST_RESULT_SELECTOR', '').strip(),
        notes_tab_selector=os.getenv('MARKETSHARP_UI_NOTES_TAB_SELECTOR', '').strip(),
        note_button_selector=os.getenv('MARKETSHARP_UI_NOTE_BUTTON_SELECTOR', '').strip(),
        note_input_selector=os.getenv('MARKETSHARP_UI_NOTE_INPUT_SELECTOR', '').strip(),
        note_save_selector=os.getenv('MARKETSHARP_UI_NOTE_SAVE_SELECTOR', '').strip(),
        contact_url_map_file=contact_url_map_file,
        contact_url_map=merged_contact_url_map,
        contact_type=os.getenv('MARKETSHARP_UI_CONTACT_TYPE', '3').strip() or '3',
        login_check_selector=os.getenv('MARKETSHARP_UI_LOGIN_CHECK_SELECTOR', '').strip(),
        login_company_id=os.getenv('MARKETSHARP_UI_LOGIN_COMPANY_ID', '').strip(),
        login_username=os.getenv('MARKETSHARP_UI_LOGIN_USERNAME', '').strip(),
        login_password=os.getenv('MARKETSHARP_UI_LOGIN_PASSWORD', '').strip(),
    )


def validate_ui_config(ui_cfg):
    """Fail fast when UI automation selectors are not configured."""
    required = {
        'MARKETSHARP_UI_BASE_URL': ui_cfg.base_url,
        'MARKETSHARP_UI_SEARCH_SELECTOR': ui_cfg.search_input_selector,
        'MARKETSHARP_UI_FIRST_RESULT_SELECTOR': ui_cfg.first_result_selector,
        'MARKETSHARP_UI_NOTE_BUTTON_SELECTOR': ui_cfg.note_button_selector,
        'MARKETSHARP_UI_NOTE_INPUT_SELECTOR': ui_cfg.note_input_selector,
        'MARKETSHARP_UI_NOTE_SAVE_SELECTOR': ui_cfg.note_save_selector,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f'Missing required UI worker env vars: {", ".join(missing)}')


def wait_for_login(page, ui_cfg):
    """Handle login if not already authenticated."""
    if not ui_cfg.login_check_selector:
        logging.info('No login-check selector configured; waiting 30 seconds for manual login.')
        time.sleep(30)
        return

    # If credentials are configured and we're on the login page, auto-login
    if (ui_cfg.login_username and ui_cfg.login_password
            and page.query_selector('#UsernameTextBox') is not None):
        logging.info('Login form detected; attempting auto-login.')
        if ui_cfg.login_company_id:
            page.fill('#CompanyIDTextBox', ui_cfg.login_company_id)
        page.fill('#UsernameTextBox', ui_cfg.login_username)
        page.fill('#PasswordTextBox', ui_cfg.login_password)
        page.click('#LoginButton')
        page.wait_for_load_state('domcontentloaded', timeout=30000)
        logging.info('Auto-login submitted; waiting for dashboard.')

    logging.info('Waiting for login check selector: %s', ui_cfg.login_check_selector)
    page.wait_for_selector(ui_cfg.login_check_selector, timeout=300000, state='attached')
    logging.info('Login check selector detected; continuing worker startup.')


def pick_visible_locator(page, selectors, timeout_ms=500):
    """Return the first locator whose selector is visible on the page."""
    tried = []
    for selector in selectors:
        if not selector:
            continue
        tried.append(selector)
        locator = page.locator(selector).first
        try:
            locator.wait_for(state='visible', timeout=timeout_ms)
            return locator, selector
        except PlaywrightTimeoutError:
            continue

    raise PlaywrightTimeoutError(f'No visible selector found. Tried: {tried}')


def click_first_visible_result(page, selectors, timeout_ms=700):
    """Click the first visible autocomplete result from any supported selector."""
    deadline = time.time() + (timeout_ms / 1000.0)
    tried = [s for s in selectors if s]

    while time.time() < deadline:
        for selector in tried:
            locator = page.locator(selector)
            count = locator.count()
            if count < 1:
                continue

            first = locator.first
            if first.is_visible():
                first.click(timeout=300)
                return selector

        page.wait_for_timeout(20)

    raise PlaywrightTimeoutError(f'No visible autocomplete result found. Tried: {tried}')


def _normalize_name(value):
    return ' '.join((value or '').strip().lower().split())


def _name_tokens(value):
    normalized = _normalize_name(value)
    cleaned = re.sub(r'[^a-z0-9 ]+', ' ', normalized)
    return [token for token in cleaned.split() if token]


def _name_variants(value):
    normalized = _normalize_name(value)
    tokens = _name_tokens(value)
    variants = {normalized}
    if len(tokens) >= 2:
        variants.add(' '.join(reversed(tokens)))
        variants.add(', '.join([tokens[-1], ' '.join(tokens[:-1])]))
    return {v for v in variants if v}


def _unique_strings(values):
    seen = set()
    ordered = []
    for value in values:
        text = ' '.join((value or '').strip().split())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(text)
    return ordered


def _search_query_variants(customer_name):
    """Generate MarketSharp-friendly search queries for a customer name."""
    base = ' '.join((customer_name or '').strip().split())
    tokens = _name_tokens(base)
    if not tokens:
        return []

    original_given = ''
    if len(tokens) >= 2:
        pattern = re.compile(rf'\b{re.escape(tokens[-1])}\b\s*$', re.IGNORECASE)
        original_given = pattern.sub('', base).strip(' ,')

    variants = [
        base,
        base.replace('&', 'and'),
        base.replace(' and ', ' & '),
        ' '.join(tokens),
    ]

    if len(tokens) >= 2:
        last_name = tokens[-1]
        given_tokens = tokens[:-1]
        given_joined = ' '.join(given_tokens)
        given_original = original_given or given_joined
        variants.extend([
            f'{last_name}, {given_original}',
            f'{last_name}, {given_original.replace("&", "and")}',
            f'{last_name}, {given_joined}',
            f'{last_name} {given_original}',
            f'{last_name} {given_joined}',
            # Last-name-only fallback: surfaces compound/household names like
            # "Bill and Christine Hubbard" that won't match "Bill Hubbard" directly.
            last_name,
        ])

    return _unique_strings(variants)


def _fill_search_query(page, search_candidates, customer_query, timeout_ms=400):
    """Fill the MarketSharp search input with the given query.

    Root cause of the autocomplete problem: at the server's default 1280px viewport
    ``window.matchMedia('(max-width: 1300px)')`` is true, so ``initializeSearch()``
    wires the jQuery UI autocomplete to ``#txtSearchBoxMobile``.  But CSS hides that
    element inside ``.show-in-mobile-only`` at non-phone widths, so focus/click on it
    silently fails and autocomplete AJAX never fires.

    Fix: widen the viewport to 1400px (>1300px breakpoint) and call
    ``initializeSearch()`` — now jQuery UI autocomplete is wired to ``#searchTextBox``
    (the desktop input), which IS visible at 1400px and can be interacted with
    normally.  The function is defined at script-block scope, so it IS globally
    callable from ``page.evaluate``.
    """
    # Step 0: Widen viewport so the desktop search box is visible, then re-wire
    # the autocomplete widget to #searchTextBox via initializeSearch().
    try:
        page.set_viewport_size({'width': 1400, 'height': 900})
        page.evaluate(
            "() => { if (typeof initializeSearch === 'function') initializeSearch(); }"
        )
        page.wait_for_timeout(300)
    except Exception:
        pass  # best-effort; fall through to Pass 1 which still tries ancestor-unhide

    # After widening to 1400px, initializeSearch() wires autocomplete to #searchTextBox
    # (desktop input) → #searchResults.  Typing into #txtSearchBoxMobile (mobile input)
    # would still fire AJAX into #searchResultsMobile, which is display:none at 1400px.
    # Playwright's is_visible() returns False on all items in a hidden container, so
    # click_matching_result would find nothing.  Prioritise the desktop selector.
    desktop_candidates = ['#searchTextBox'] + [
        s for s in search_candidates if s and s not in ('#searchTextBox', '#txtSearchBoxMobile')
    ]

    # Pass 1: Force-unhide CSS-hidden ancestors, then Playwright click + keyboard type.
    #
    # Root cause: #txtSearchBoxMobile lives inside .show-in-mobile-only which is
    # display:none at the Playwright default 1280px viewport. Browsers silently
    # reject el.focus() when any ancestor has display:none, so keystrokes go
    # nowhere. We walk up the DOM, un-hide every display:none ancestor, position
    # the input in a fixed overlay so it is on-screen, then let Playwright click
    # and type into it normally. This fires real keydown/keypress/keyup/input
    # events which trigger MarketSharp's jQuery UI autocomplete AJAX handler.
    for selector in [s for s in desktop_candidates if s]:
        if page.locator(selector).count() < 1:
            continue
        unhid = page.evaluate(
            """
            (selector) => {
                const el = document.querySelector(selector);
                if (!el) return false;
                // Walk up the ancestor chain and un-hide any display:none nodes.
                let node = el.parentElement;
                while (node && node !== document.body) {
                    if (window.getComputedStyle(node).display === 'none') {
                        node.style.setProperty('display', 'block', 'important');
                    }
                    node = node.parentElement;
                }
                // Pin the input itself in a fixed on-screen overlay so Playwright
                // can click it regardless of the parent layout.
                el.style.setProperty('position', 'fixed', 'important');
                el.style.setProperty('top',       '10px',   'important');
                el.style.setProperty('left',      '10px',   'important');
                el.style.setProperty('width',     '300px',  'important');
                el.style.setProperty('height',    '30px',   'important');
                el.style.setProperty('z-index',   '999999', 'important');
                el.style.setProperty('display',   'block',  'important');
                el.style.setProperty('visibility','visible','important');
                el.style.setProperty('opacity',   '1',      'important');
                el.value = '';
                return true;
            }
            """,
            selector,
        )
        if not unhid:
            continue
        try:
            loc = page.locator(selector)
            loc.click(timeout=2000)
            # Type character-by-character — fires real keydown/keypress/keyup/input
            # events that trigger MarketSharp's jQuery UI autocomplete AJAX handler.
            page.keyboard.type(customer_query, delay=40)
            return None, selector
        except Exception:
            continue

    # Pass 2: visible element (genuine desktop-rendered input with no CSS suppression).
    try:
        search_box, search_selector_used = pick_visible_locator(
            page,
            desktop_candidates,
            timeout_ms=timeout_ms,
        )
        search_box.click()
        search_box.fill('')
        search_box.fill(customer_query)
        page.evaluate(
            """
            (selector) => {
                const el = document.querySelector(selector);
                if (el) {
                    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
                }
            }
            """,
            search_selector_used,
        )
        page.wait_for_timeout(0)
        return search_box, search_selector_used
    except PlaywrightTimeoutError:
        raise


def click_matching_result(page, selectors, customer_name, timeout_ms=12000):
    """Click the autocomplete row that best matches the target customer name."""
    deadline = time.time() + (timeout_ms / 1000.0)
    target = _normalize_name(customer_name)
    target_variants = _name_variants(customer_name)
    target_tokens = set(_name_tokens(customer_name))
    tried = [s for s in selectors if s]
    seen_labels = {}

    while time.time() < deadline:
        best_prefix = None
        best_token_overlap = None

        for selector in tried:
            locator = page.locator(selector)
            count = locator.count()
            if count < 1:
                continue

            labels_for_selector = []

            for idx in range(count):
                item = locator.nth(idx)
                if not item.is_visible():
                    continue

                label = _normalize_name(item.inner_text())
                if not label:
                    continue
                if len(labels_for_selector) < 5:
                    labels_for_selector.append(label)

                # Fast-exit: MarketSharp shows "No results. Click to add a new Contact."
                # as a placeholder item when the search returns nothing.  Bail immediately
                # rather than waiting out the full timeout.
                if 'no results' in label or 'add a new contact' in label:
                    raise PlaywrightTimeoutError(
                        f'No matching autocomplete result found for "{customer_name}" '
                        f'(MarketSharp returned "No results" placeholder). '
                        f'Tried selectors: {tried}.'
                    )

                # Exact/variant match is safest; click immediately.
                if label in target_variants:
                    item.click(timeout=10000)
                    return selector, label

                # Keep the closest safe fallback only when result starts with target.
                if label.startswith(target) and best_prefix is None:
                    best_prefix = (item, selector, label)

                label_tokens = set(_name_tokens(label))
                overlap = len(target_tokens.intersection(label_tokens))
                # Keep a token-overlap fallback (e.g. "ellis, ryan") when we have high confidence.
                if overlap >= 2:
                    if best_token_overlap is None or overlap > best_token_overlap[0]:
                        best_token_overlap = (overlap, item, selector, label)

            if labels_for_selector:
                seen_labels[selector] = labels_for_selector

        if best_prefix is not None:
            best_prefix[0].click(timeout=10000)
            return best_prefix[1], best_prefix[2]

        if best_token_overlap is not None:
            best_token_overlap[1].click(timeout=10000)
            return best_token_overlap[2], best_token_overlap[3]

        page.wait_for_timeout(5)

    raise PlaywrightTimeoutError(
        f'No matching autocomplete result found for "{customer_name}". '
        f'Tried selectors: {tried}. Seen labels: {seen_labels}'
    )


def pick_visible_locator_in_frames(page, selectors, timeout_ms=5000):
    """Return first visible locator across main page and child frames."""
    deadline = time.time() + (timeout_ms / 1000.0)
    tried = [s for s in selectors if s]

    while time.time() < deadline:
        for frame in page.frames:
            for selector in tried:
                locator = frame.locator(selector)
                if locator.count() < 1:
                    continue
                first = locator.first
                if first.is_visible():
                    return first, selector, frame.url
                # Element exists but is not yet visible — log once per frame
                logging.debug(
                    'Selector %s found in frame %s but not yet visible; waiting.',
                    selector, frame.url,
                )
        page.wait_for_timeout(250)

    # Final diagnostic pass: report what exists vs. what is visible
    for frame in page.frames:
        for selector in tried:
            count = frame.locator(selector).count()
            if count > 0:
                visible = frame.locator(selector).first.is_visible()
                logging.warning(
                    'Timeout: selector %s found (count=%d) in frame %s but visible=%s',
                    selector, count, frame.url, visible,
                )

    raise PlaywrightTimeoutError(f'No visible selector found across frames. Tried: {tried}')


def _extract_project_id_from_payload(payload_obj):
    """Extract a stable CompanyCam project/location identifier from queued payload JSON."""
    if not isinstance(payload_obj, dict):
        return None

    comment_data = payload_obj.get('data') or payload_obj.get('payload') or payload_obj
    if not isinstance(comment_data, dict):
        comment_data = {}

    nested_payload = comment_data.get('payload', {})
    if not isinstance(nested_payload, dict):
        nested_payload = {}

    comment_obj = comment_data.get('comment', {})
    if not isinstance(comment_obj, dict):
        comment_obj = {}
    if not comment_obj and isinstance(nested_payload.get('comment'), dict):
        comment_obj = nested_payload.get('comment', {})

    project_obj = comment_data.get('project', {})
    if not isinstance(project_obj, dict):
        project_obj = {}
    if not project_obj and isinstance(nested_payload.get('project'), dict):
        project_obj = nested_payload.get('project', {})
        if not isinstance(project_obj, dict):
            project_obj = {}

    project_id = (
        comment_data.get('project_id')
        or comment_data.get('projectId')
        or nested_payload.get('project_id')
        or nested_payload.get('projectId')
        or comment_obj.get('project_id')
        or comment_obj.get('projectId')
        or project_obj.get('id')
    )

    if project_id:
        return str(project_id).strip()

    commentable_type = (
        comment_data.get('commentable_type')
        or nested_payload.get('commentable_type')
        or comment_obj.get('commentable_type')
        or ''
    )
    commentable_id = (
        comment_data.get('commentable_id')
        or nested_payload.get('commentable_id')
        or comment_obj.get('commentable_id')
    )

    if commentable_id and str(commentable_type).lower() in {'', 'project', 'location'}:
        return str(commentable_id).strip()

    return None


def _normalize_address_dict(raw):
    """Normalize an address dict to standard keys (street/city/state/postal).

    Handles CompanyCam field variants like street_address_1, line1, postalCode, etc.
    Returns {} if no usable fields are present.
    """
    if not isinstance(raw, dict):
        return {}
    # Only accept string values — a nested dict (e.g. raw['street'] = {'street_address_1': ...})
    # must be discarded rather than str()'d, which would produce garbage OData queries.
    # Also discard stringified Python dicts (old webhook bug stored str(dict) as the street).
    def _first_str(d, *keys):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v and not v.lstrip().startswith('{'):
                return v
        return ''
    street = _first_str(
        raw,
        'street', 'line1', 'address1',
        'street_address_1', 'streetAddress1', 'street_address',
    )
    city = raw.get('city') or ''
    state = raw.get('state') or raw.get('stateCode') or ''
    postal = raw.get('postal') or raw.get('postalCode') or raw.get('zip') or raw.get('zipCode') or ''
    normalized = {
        'street': str(street).strip(),
        'city': str(city).strip(),
        'state': str(state).strip(),
        'postal': str(postal).strip(),
    }
    if any(normalized.values()):
        return normalized
    return {}


def _extract_project_address_from_payload(payload_obj):
    """Extract project address context captured at webhook ingestion time."""
    if not isinstance(payload_obj, dict):
        return {}

    # _spicer.project_address is the pre-captured address written at enqueue time.
    # Normalize it because older payloads may have used non-standard keys (street_address_1 etc.)
    spicer_meta = payload_obj.get('_spicer')
    if isinstance(spicer_meta, dict):
        spicer_address = spicer_meta.get('project_address')
        if isinstance(spicer_address, dict) and spicer_address:
            normalized = _normalize_address_dict(spicer_address)
            if normalized:
                return normalized

    comment_data = payload_obj.get('data') or payload_obj.get('payload') or payload_obj
    if not isinstance(comment_data, dict):
        comment_data = {}

    nested_payload = comment_data.get('payload', {})
    if not isinstance(nested_payload, dict):
        nested_payload = {}

    project_obj = comment_data.get('project', {})
    if not isinstance(project_obj, dict):
        project_obj = {}
    if not project_obj and isinstance(nested_payload, dict):
        project_obj = nested_payload.get('project', {})
        if not isinstance(project_obj, dict):
            project_obj = {}

    # Merge nested_address and top-level project_obj fields, then normalize
    raw_nested = project_obj.get('address')
    nested_address = raw_nested if isinstance(raw_nested, dict) else {}
    merged = dict(project_obj)
    if isinstance(nested_address, dict):
        for k, v in nested_address.items():
            merged[k] = v  # nested_address fields take precedence
    return _normalize_address_dict(merged)



def _extract_project_address_from_companycam(project_id):
    """Fetch project address directly from CompanyCam when payload omits it."""
    if not project_id:
        return {}

    try:
        project = _get_companycam_service().get_project_by_id(project_id)
    except Exception as exc:
        logging.warning('CompanyCam project lookup failed for %s: %s', project_id, exc)
        return {}

    if not isinstance(project, dict):
        return {}

    # Ensure nested_address is a dict, else use empty dict
    nested_address = project.get('address') if isinstance(project.get('address'), dict) else {}
    address = {
        'street': (
            (nested_address.get('street') if isinstance(nested_address, dict) else '')
            or (nested_address.get('line1') if isinstance(nested_address, dict) else '')
            or (nested_address.get('address1') if isinstance(nested_address, dict) else '')
            or (project.get('address1') if isinstance(project, dict) else '')
            or (project.get('street') if isinstance(project, dict) else '')
            or ''
        ),
        'city': (nested_address.get('city') if isinstance(nested_address, dict) else '') or (project.get('city') if isinstance(project, dict) else ''),
        'state': (
            (nested_address.get('state') if isinstance(nested_address, dict) else '')
            or (nested_address.get('stateCode') if isinstance(nested_address, dict) else '')
            or (project.get('state') if isinstance(project, dict) else '')
            or (project.get('stateCode') if isinstance(project, dict) else '')
            or ''
        ),
        'postal': (
            (nested_address.get('postal') if isinstance(nested_address, dict) else '')
            or (nested_address.get('postalCode') if isinstance(nested_address, dict) else '')
            or (nested_address.get('zip') if isinstance(nested_address, dict) else '')
            or (project.get('postalCode') if isinstance(project, dict) else '')
            or (project.get('zip') if isinstance(project, dict) else '')
            or (project.get('zipCode') if isinstance(project, dict) else '')
            or ''
        ),
    }
    if any(str(value).strip() for value in address.values()):
        return {key: str(value).strip() for key, value in address.items()}
    return {}


def resolve_direct_contact_url(item, ui_cfg):
    """Resolve a direct MarketSharp contact URL for a queued item using stable identifiers first."""
    payload_json = item.get('payload_json') or ''
    payload_obj = {}
    if payload_json:
        try:
            payload_obj = json.loads(payload_json)
        except json.JSONDecodeError:
            logging.warning('Invalid payload_json for queue item id=%s; skipping direct contact lookup.', item.get('id'))

    project_id = _extract_project_id_from_payload(payload_obj)
    project_address = _extract_project_address_from_payload(payload_obj)
    if project_id and not project_address:
        project_address = _extract_project_address_from_companycam(project_id)
    if project_id:
        project_key = f'project:{project_id}'
        direct_contact_url = ui_cfg.contact_url_map.get(project_key)
        if direct_contact_url:
            return direct_contact_url, project_key

    customer_name = item.get('customer_name') or ''
    name_key = f'name:{_normalize_name(customer_name)}'
    direct_contact_url = ui_cfg.contact_url_map.get(name_key)
    if direct_contact_url:
        return direct_contact_url, name_key

    # First, try to match by name (with address as tie-breaker)
    customer = MarketSharpService().get_customer_by_name(
        customer_name,
        project_address=project_address,
    )
    if customer and customer.get('id'):
        contact_oid = customer['id']
        logging.info('OData name-match contactOid=%s for customer %r', contact_oid, customer_name)
        auto_url = (
            f'https://www1.marketsharpm.com/ContactDetail.aspx?contactOid={contact_oid}'
            f'&contactType={ui_cfg.contact_type}'
        )
        return auto_url, 'marketsharp-name-match'

    # Double-check: try to match by address only if name match failed
    if project_address and any(project_address.values()):
        customer_by_address = MarketSharpService().get_customer_by_address(project_address)
        if customer_by_address and customer_by_address.get('id'):
            contact_oid = customer_by_address['id']
            logging.info('OData address-match contactOid=%s for customer %r', contact_oid, customer_name)
            auto_url = (
                f'https://www1.marketsharpm.com/ContactDetail.aspx?contactOid={contact_oid}'
                f'&contactType={ui_cfg.contact_type}'
            )
            return auto_url, 'marketsharp-address-match'

    return None, None


def open_customer_and_add_note(page, ui_cfg, item, note_text, search_override=None, _resolved_direct=None):
    """Search for customer in MarketSharp UI and add a note. Optionally override search query."""
    customer_name = item['customer_name']
    if _resolved_direct is not None:
        direct_contact_url, direct_contact_key = _resolved_direct
    else:
        direct_contact_url, direct_contact_key = resolve_direct_contact_url(item, ui_cfg)
    if direct_contact_url:
        logging.info(
            'Using direct contact URL for customer %s via %s: %s',
            customer_name,
            direct_contact_key,
            direct_contact_url,
        )
        # Adapt the URL's host to match the browser's current session host so that
        # cookies are valid.  The worker's session always lands on www1 after login
        # even though the configured base URL is www2.
        current_netloc = urlparse(page.url).netloc
        if current_netloc:
            parsed_direct = urlparse(direct_contact_url)
            if parsed_direct.netloc != current_netloc:
                logging.info(
                    'Adapting contact URL host %s → %s',
                    parsed_direct.netloc, current_netloc,
                )
                direct_contact_url = urlunparse(parsed_direct._replace(netloc=current_netloc))
        page.goto(direct_contact_url, wait_until='load', timeout=120000)
        # Wait for AJAX/UpdatePanel to settle — WebForms renders tab content after 'load'
        try:
            page.wait_for_load_state('networkidle', timeout=15000)
        except PlaywrightTimeoutError:
            logging.info('networkidle timed out after direct URL navigation; proceeding.')

        # MarketSharp can silently redirect an expired session to the customers list
        # (no login form shown).  Detect both cases and re-auth before retrying.
        def _on_wrong_page(wait_ms=2500):
            url = page.url.lower()
            if 'contactdetail' not in url and 'contactoid' not in url:
                return True
            # MarketSharp shows "Record View" on ContactDetail.aspx when the
            # contactOid is invalid or missing — the URL stays correct but the
            # page content shows the customers list.  Detect via the Quick Find
            # form inputs (placeholder text) which are only visible in Record View.
            # React may need a moment to render, so use wait_for_selector with a
            # short deadline rather than an instant query_selector check.
            # Use a combined CSS selector so a single timed wait covers both.
            try:
                page.wait_for_selector(
                    'input[placeholder="First Name"], input[placeholder="Last Name"]',
                    timeout=wait_ms,
                )
                return True
            except PlaywrightTimeoutError:
                return False

        if page.query_selector('#UsernameTextBox') is not None or _on_wrong_page():
            logging.warning(
                'Direct URL navigation for %s landed on unexpected page (%s); '
                're-authenticating and retrying.',
                customer_name, page.url,
            )
            wait_for_login(page, ui_cfg)
            page.goto(direct_contact_url, wait_until='load', timeout=120000)
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except PlaywrightTimeoutError:
                pass

        if _on_wrong_page(wait_ms=800):
            raise PlaywrightTimeoutError(
                f'Direct URL for {customer_name!r} still landed on wrong page {page.url!r} '
                'after re-auth; cannot locate contact detail.'
            )

        # Detect NoContactPanel: page loaded but wrong contactType — retry with 1 and 2.
        def _no_contact_panel_visible():
            try:
                el = page.query_selector('#ctl00_ctl00_MainContentPlaceHolder_ContentPlaceHolder2_NoContactPanel')
                if el and el.is_visible():
                    return True
            except Exception:
                pass
            return False

        if _no_contact_panel_visible():
            parsed_direct = urlparse(direct_contact_url)
            qs = dict(q.split('=', 1) for q in parsed_direct.query.split('&') if '=' in q)
            contact_oid = qs.get('contactOid', '')
            base_contact_url = f'https://{parsed_direct.netloc}/ContactDetail.aspx'
            found_contact = False
            for try_type in ('1', '2'):
                alt_url = f'{base_contact_url}?contactOid={contact_oid}&contactType={try_type}'
                logging.info(
                    'NoContactPanel detected for contactType=%s; retrying with contactType=%s (%s)',
                    ui_cfg.contact_type, try_type, customer_name,
                )
                page.goto(alt_url, wait_until='load', timeout=120000)
                try:
                    page.wait_for_load_state('networkidle', timeout=15000)
                except PlaywrightTimeoutError:
                    pass
                if not _no_contact_panel_visible():
                    logging.info('Contact loaded successfully with contactType=%s for %s', try_type, customer_name)
                    found_contact = True
                    break
            if not found_contact:
                raise PlaywrightTimeoutError(
                    f'NoContactPanel shown for {customer_name!r} with all contactTypes (1/2/3); '
                    f'contactOid={contact_oid} may not exist.'
                )
    else:
        for attempt in (1, 2, 3):
            try:
                # Use networkidle so Playwright waits through the JS redirect from
                # AppEntryRouting.aspx to the actual dashboard before we check selectors.
                page.goto(ui_cfg.base_url, wait_until='networkidle', timeout=60000)
                break
            except PlaywrightError as exc:
                if ('net::ERR_ABORTED' in str(exc) or 'Timeout' in type(exc).__name__) and attempt < 3:
                    logging.warning('Navigation aborted/timed-out on attempt %s; retrying.', attempt)
                    page.wait_for_timeout(1000)
                    continue
                raise

        search_candidates = list(dict.fromkeys(filter(None, [
            ui_cfg.search_input_selector,
            '#txtSearchBoxMobile',
            '#searchTextBox',
            # Modern React-style MarketSharp dashboard selectors
            'input[type="search"]',
            'input[placeholder*="earch" i]',
            'input[placeholder*="ontact" i]',
            'input[placeholder*="ustomer" i]',
            '[class*="search" i] input',
            '[class*="Search"] input',
            'header input',
            'nav input',
        ])))
        result_candidates = [
            '#searchResults ul.ui-autocomplete li.ui-menu-item a.ui-menu-item-wrapper',
            '#searchResultsMobile ul.ui-autocomplete li.ui-menu-item a.ui-menu-item-wrapper',
            'ul.ui-autocomplete li.ui-menu-item a.ui-menu-item-wrapper',
            '#searchResults ul.ui-autocomplete li.ui-menu-item',
            '#searchResultsMobile ul.ui-autocomplete li.ui-menu-item',
            'ul.ui-autocomplete li.ui-menu-item',
            '#searchResults li',
            '#searchResultsMobile li',
            'ul.ui-autocomplete li',
            'ul.ui-autocomplete li a',
            'ul.ui-autocomplete li div',
            ui_cfg.first_result_selector,
        ]

        last_match_exc = None
        search_fill_exc = None
        customer_query = search_override if search_override is not None else customer_name
        try:
            # Only check for the explicit login form — if it's present, session expired visibly.
            if page.query_selector('#UsernameTextBox') is not None:
                logging.warning('Session expired (login form) after base_url navigation; re-authenticating.')
                wait_for_login(page, ui_cfg)
                page.wait_for_load_state('domcontentloaded', timeout=15000)

            # Note: on www1.marketsharpm.com, AppEntryRouting.aspx IS the dashboard URL and
            # never redirects — do not wait for a URL change. Just go straight to filling the
            # search box. _fill_search_query tries Playwright visibility first, then falls back
            # to JS injection for elements that are in the DOM but not "visible" (e.g. inside
            # a `not-clickable` parent container that suppresses pointer-events).
            logging.info('Filling search box on page: %s', page.url)

            try:
                search_box, search_selector_used = _fill_search_query(
                    page, search_candidates, customer_query, timeout_ms=800,
                )
            except PlaywrightTimeoutError as fill_exc:
                search_fill_exc = fill_exc
                raise

            # Wait for autocomplete AJAX to complete. Typing is async so we need
            # to allow time for the last keyup → debounce → XHR → response.
            page.wait_for_timeout(2500)
            # Diagnostic: log what the autocomplete dropdown contains right now.
            try:
                ac_info = page.evaluate("""
                    () => {
                        const ul = document.querySelector('ul.ui-autocomplete');
                        if (!ul) return {exists: false, visible: false, items: 0};
                        const style = window.getComputedStyle(ul);
                        return {
                            exists: true,
                            visible: style.display !== 'none' && style.visibility !== 'hidden',
                            items: ul.querySelectorAll('li.ui-menu-item').length,
                        };
                    }
                """)
                logging.info(
                    'Autocomplete dropdown after fill: exists=%s visible=%s items=%s (query=%r)',
                    ac_info.get('exists'), ac_info.get('visible'), ac_info.get('items'), customer_query,
                )
            except Exception as diag_exc:
                logging.debug('Autocomplete diagnostic failed: %s', diag_exc)

            # Now interact with the DOM — shorter timeout since AJAX should already be done.
            result_selector, label = click_matching_result(page, result_candidates, customer_query, timeout_ms=4000)
            logging.info('Clicked result selector=%s label=%s', result_selector, label)
        except PlaywrightTimeoutError as exc:
            # Save screenshot + page HTML for diagnosis.
            item_id = item.get('id', 'unknown')
            # Choose message based on whether the fill step succeeded.
            if search_fill_exc is not None:
                fail_reason = f'Search box not found in DOM (url={page.url})'
            else:
                fail_reason = f'Autocomplete AJAX returned no matching results for {customer_query!r} (url={page.url})'
            try:
                shot_path = f'search_box_fail_{item_id}.png'
                page.screenshot(path=shot_path)
                logging.warning(
                    '%s; screenshot saved to %s',
                    fail_reason, shot_path,
                )
            except Exception as shot_exc:
                logging.warning('Could not take search-box-fail screenshot: %s', shot_exc)
            try:
                html_path = f'search_box_fail_{item_id}.html'
                with open(html_path, 'w', encoding='utf-8') as _f:
                    _f.write(page.content())
                # Log all input elements found on the page so we can identify the right selector
                inputs_info = page.evaluate("""
                    () => Array.from(document.querySelectorAll('input, [role="searchbox"], [role="combobox"]'))
                        .map(el => ({
                            tag: el.tagName,
                            id: el.id,
                            name: el.name,
                            type: el.type,
                            placeholder: el.placeholder,
                            className: el.className.slice(0, 80),
                            visible: el.offsetParent !== null,
                        }))
                """)
                logging.warning(
                    'Page HTML dumped to %s. Input elements found on page: %s',
                    html_path, inputs_info,
                )
            except Exception as dump_exc:
                logging.warning('Could not dump page HTML: %s', dump_exc)
            last_match_exc = exc
            raise last_match_exc

    try:
        page.wait_for_load_state('domcontentloaded', timeout=15000)
    except PlaywrightTimeoutError:
        logging.info('Contact page load-state wait timed out; continuing.')

    if ui_cfg.notes_tab_selector:
        notes_tab_selectors = [
            ui_cfg.notes_tab_selector,
            'a[id*="NotesTab"]',
            'a[id*="Notes"]',
            'a:has-text("Notes")',
        ]
        notes_clicked = False
        try:
            notes_tab, _, notes_tab_frame = pick_visible_locator_in_frames(
                page,
                notes_tab_selectors,
                timeout_ms=15000,
            )
            notes_tab.click(timeout=10000)
            page.wait_for_timeout(500)
            logging.info('Clicked notes tab selector: %s (frame=%s)', notes_tab_selectors[0], notes_tab_frame)
            notes_clicked = True
        except PlaywrightTimeoutError:
            pass

        if not notes_clicked:
            # Visibility check failed — try JS click across all frames as a fallback.
            # This works even if the element has display:none or zero dimensions during animation.
            for frame in page.frames:
                for sel in notes_tab_selectors:
                    try:
                        clicked = frame.evaluate(
                            """(selector) => {
                                const el = document.querySelector(selector);
                                if (el) { el.click(); return true; }
                                return false;
                            }""",
                            sel,
                        )
                        if clicked:
                            page.wait_for_timeout(500)
                            logging.info('Clicked notes tab via JS fallback selector=%s frame=%s', sel, frame.url)
                            notes_clicked = True
                            break
                    except Exception:
                        pass
                if notes_clicked:
                    break

        if not notes_clicked:
            # Save a screenshot + HTML dump for diagnosis — path is in the worker's cwd
            item_id = item.get('id', 'unknown')
            try:
                screenshot_path = f'notes_tab_fail_{item_id}.png'
                page.screenshot(path=screenshot_path)
                logging.warning(
                    'Notes tab not found via normal or JS click. Screenshot saved to %s (url=%s). '
                    'Selector: %s',
                    screenshot_path, page.url,
                    ui_cfg.notes_tab_selector,
                )
            except Exception as ss_exc:
                logging.warning(
                    'Notes tab not found and screenshot failed (%s). Selector: %s',
                    ss_exc,
                    ui_cfg.notes_tab_selector,
                )
            try:
                html_path = f'notes_tab_fail_{item_id}.html'
                with open(html_path, 'w', encoding='utf-8') as _f:
                    _f.write(page.content())
                logging.warning('Notes-tab-fail page HTML saved to %s', html_path)
            except Exception as html_exc:
                logging.warning('Could not dump notes-tab-fail HTML: %s', html_exc)

    note_button, note_button_selector_used, note_button_frame = pick_visible_locator_in_frames(
        page,
        [
            ui_cfg.note_button_selector,
            'a[id*="AddNewButton"]',
            'input[id*="AddNewButton"]',
            'button:has-text("Add Note")',
            'a:has-text("Add Note")',
        ],
        timeout_ms=15000,
    )
    logging.info('Using note-button selector: %s (frame=%s)', note_button_selector_used, note_button_frame)
    note_button.click(timeout=15000)

    note_input, note_input_selector_used, note_input_frame = pick_visible_locator_in_frames(
        page,
        [
            ui_cfg.note_input_selector,
            'textarea[id*="noteTextBox"]',
            'textarea[name*="note"]',
        ],
        timeout_ms=20000,
    )
    logging.info('Using note-input selector: %s (frame=%s)', note_input_selector_used, note_input_frame)
    note_input.click()
    note_input.fill(note_text)

    save_button, save_selector_used, save_frame = pick_visible_locator_in_frames(
        page,
        [
            ui_cfg.note_save_selector,
            'input[id*="ContactNoteSaveButton"]',
            'button:has-text("Save")',
            'a:has-text("Save")',
        ],
        timeout_ms=20000,
    )
    logging.info('Using save selector: %s (frame=%s)', save_selector_used, save_frame)
    save_button.click(timeout=15000)


def process_once(page, ui_cfg, queue):
    """Process one pending batch and return number of attempted rows."""

    logging.info(f"[Worker] Claiming up to {ui_cfg.batch_size} pending items from queue DB: {getattr(queue, 'db_path', 'unknown')}")
    # claim_pending_batch atomically transitions rows pending→processing in one statement,
    # preventing a concurrent worker process from picking up the same item.
    pending_items = queue.claim_pending_batch(limit=ui_cfg.batch_size)
    if not pending_items:
        logging.info("[Worker] No pending items found for processing.")
        return 0

    logging.info(f"[Worker] Claimed {len(pending_items)} items: {[item['id'] for item in pending_items]}")

    for item in pending_items:
        queue_id = item['id']
        customer_name = item['customer_name']
        comment_text = item['comment_text']
        author_name = item.get('author_name')
        # Prepend author if present and not already in the text
        if author_name and author_name.strip() and not comment_text.strip().startswith(f'[{author_name.strip()}]'):
            note_text = f'[{author_name.strip()}] {comment_text.strip()}'
        else:
            note_text = comment_text
        retry_count = item.get('retry_count', 0)

        # Extract address info if present
        payload_json = item.get('payload_json') or ''
        address_variants = []
        extracted_address = None
        if payload_json:
            try:
                payload_obj = json.loads(payload_json)
                address_obj = _extract_project_address_from_payload(payload_obj)
                if address_obj and any(address_obj.values()):
                    addr_str = ' '.join([str(address_obj.get(k, '')) for k in ('street', 'city', 'state', 'postal') if address_obj.get(k)])
                    if addr_str.strip():
                        address_variants.append(addr_str.strip())
                        extracted_address = addr_str.strip()
            except Exception as exc:
                logging.warning('Failed to parse address from payload for queue_id=%s: %s', queue_id, exc)
        logging.info('Extracted address for queue_id=%s: %s', queue_id, extracted_address)

        # Build all search queries: name variants, address variants, and combos
        name_variants = list(_search_query_variants(customer_name))
        search_variants = name_variants.copy()
        for addr in address_variants:
            # Try address only
            search_variants.append(addr)
            # Try name + address
            for nv in name_variants:
                search_variants.append(f'{nv} {addr}')

        # Resolve the direct contact URL once per item — avoids 5+ OData calls per search variant.
        resolved_direct = resolve_direct_contact_url(item, ui_cfg)
        direct_contact_url, _ = resolved_direct

        # Item is already in 'processing' state (claimed atomically above).
        posted = False
        last_error = None

        def _attempt_post(label, variant_override=None, use_direct=True):
            """Try open_customer_and_add_note once, return (posted, error_str)."""
            nonlocal last_error
            direct_hint = resolved_direct if use_direct else (None, None)
            for attempt in range(3):
                try:
                    open_customer_and_add_note(
                        page, ui_cfg, item, note_text,
                        search_override=variant_override,
                        _resolved_direct=direct_hint,
                    )
                    queue.mark_posted(queue_id)
                    log_posted_comment(
                        event_id=item.get('event_id'),
                        customer_id=item.get('customer_id'),
                        customer_name=customer_name,
                        author_name=author_name,
                        comment_text=note_text,
                        extra_json=item.get('payload_json'),
                    )
                    logging.info('Posted queued item id=%s customer=%s via %s', queue_id, customer_name, label)
                    logging.info(json.dumps({
                        'event_id': item.get('event_id'),
                        'author': author_name,
                        'content': comment_text,
                        'timestamp': int(time.time()),
                        'status': 'posted',
                        'queue_id': queue_id,
                        'customer_name': customer_name,
                    }))
                    return True, None
                except Exception as exc:
                    if 'Execution context was destroyed' in str(exc) or 'Most likely because of a navigation' in str(exc):
                        logging.warning('Execution context lost (%s attempt %d); reloading.', label, attempt + 1)
                        try:
                            page.reload(wait_until='domcontentloaded', timeout=15000)
                            page.wait_for_load_state('domcontentloaded', timeout=15000)
                        except Exception as reload_exc:
                            logging.error('Page reload failed: %s', reload_exc)
                        continue
                    last_error = str(exc)
                    logging.exception('Error posting queued item id=%s (%s, attempt %d)', queue_id, label, attempt + 1)
                    return False, last_error
            return False, last_error

        if direct_contact_url:
            # Direct URL found: try it first before falling back to search variants.
            queue.touch_processing(queue_id)
            posted, _ = _attempt_post('direct-url')
            if not posted:
                logging.warning(
                    'Direct URL attempt failed for %s; falling back to search variants.',
                    customer_name,
                )

        if not posted:
            # No direct URL or direct URL failed: iterate name/address search variants.
            for idx, variant in enumerate(search_variants):
                logging.info('Attempting search variant %d/%d: "%s"', idx + 1, len(search_variants), variant)
                # Refresh updated_at before each attempt so that the other worker's
                # stale-recovery timer doesn't reset this item while we are still working on it.
                queue.touch_processing(queue_id)
                posted, _ = _attempt_post(f'search-variant={variant!r}', variant_override=variant, use_direct=False)
                if posted:
                    break

        if not posted:
            # If all variants failed, escalate or mark unmatched
            retry_count = item.get('retry_count', 0)
            if retry_count >= 4:
                queue.mark_true_fail(queue_id, f'Exceeded retry limit for "{customer_name}": {last_error}')
                append_unmatched_dump(ui_cfg.unmatched_dump_path, item, last_error)
                logging.warning(
                    'Escalated queued item id=%s to true_fail after 5 tries (customer=%s).',
                    queue_id,
                    customer_name,
                )
            else:
                queue.mark_unmatched(queue_id, f'Unmatched customer: {last_error}')
                append_unmatched_dump(ui_cfg.unmatched_dump_path, item, last_error)
                logging.warning(
                    'Marked queued item id=%s as unmatched (customer=%s).',
                    queue_id,
                    customer_name,
                )

    logging.info(f"[Worker] Finished processing batch of {len(pending_items)} items.")
    return len(pending_items)


def append_unmatched_dump(dump_path, item, error_text):
    """Append unmatched events to a jsonl dump for manual review."""
    event = {
        'timestamp': int(time.time()),
        'queue_id': item.get('id'),
        'event_id': item.get('event_id'),
        'customer_name': item.get('customer_name'),
        'comment_text': item.get('comment_text'),
        'author_name': item.get('author_name'),
        'payload_json': item.get('payload_json'),
        'reason': error_text,
    }

    with open(dump_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, ensure_ascii=True) + '\n')


def main():
    """Run queue worker loop until interrupted."""
    # Override any previously-exported shell vars so .env is source of truth.
    load_dotenv(override=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

    ui_cfg = build_ui_config()
    validate_ui_config(ui_cfg)

    queue = PendingCommentQueue(Config.PENDING_QUEUE_DB_PATH)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=ui_cfg.user_data_dir,
            headless=ui_cfg.headless,
        )
        page = context.pages[0] if context.pages else context.new_page()

        page.goto(ui_cfg.base_url, wait_until='domcontentloaded', timeout=120000)
        wait_for_login(page, ui_cfg)

        logging.info('Queue UI poster started. Polling every %s seconds.', ui_cfg.poll_seconds)
        while True:
            recovered = queue.requeue_stale_processing(ui_cfg.processing_timeout_seconds)
            if recovered:
                logging.warning('Recovered %s stale processing queue items.', recovered)

            requeued_unmatched = queue.requeue_stale_unmatched(ui_cfg.unmatched_retry_seconds)
            if requeued_unmatched:
                logging.info(
                    'Requeued %s unmatched queue items for scheduled retry.',
                    requeued_unmatched,
                )

            attempted = process_once(page, ui_cfg, queue)
            counts = queue.get_counts()
            logging.info(
                'Worker tick attempted=%s queue_counts=%s',
                attempted,
                counts,
            )
            time.sleep(ui_cfg.poll_seconds)


if __name__ == '__main__':
    main()
