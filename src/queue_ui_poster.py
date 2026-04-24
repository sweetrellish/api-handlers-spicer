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

from dotenv import load_dotenv
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from src.companycam_service import CompanyCamService
from config.config import Config
from src.marketsharp_service import MarketSharpService
from src.mapping_registry import load_mapping_env, load_mapping_file, merge_contact_mappings
from src.pending_queue import PendingCommentQueue
from scripts.posted_comments_audit import log_posted_comment


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
        'data/marketsharp_contact_mappings.json',
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
        processing_timeout_seconds=int(os.getenv('QUEUE_PROCESSING_TIMEOUT_SECONDS', '300')),
        unmatched_retry_seconds=int(os.getenv('QUEUE_UNMATCHED_RETRY_SECONDS', '10')),
        unmatched_dump_path=os.getenv('QUEUE_UNMATCHED_DUMP_PATH', 'data/unmatched_comments.jsonl').strip(),
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
    """Fill the MarketSharp search input with the given query using visible or hidden fallback."""
    search_selector_used = None
    search_box = None
    try:
        search_box, search_selector_used = pick_visible_locator(
            page,
            search_candidates,
            timeout_ms=timeout_ms,
        )
        search_box.click()
        search_box.fill('')
        search_box.fill(customer_query)
        # Force UI to update instantly by dispatching Enter keyup event
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
        for selector in [s for s in search_candidates if s]:
            if page.locator(selector).count() < 1:
                continue
            injected = page.evaluate(
                """
                ({selector, value}) => {
                    const el = document.querySelector(selector);
                    if (!el) return false;
                    el.focus();
                    el.value = '';
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }));
                    return true;
                }
                """,
                {'selector': selector, 'value': customer_query},
            )
            if injected:
                return None, selector
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

                # Exact/variant match is safest; click immediately.
                if label in target_variants:
                    item.click(timeout=200)
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
            best_prefix[0].click(timeout=200)
            return best_prefix[1], best_prefix[2]

        if best_token_overlap is not None:
            best_token_overlap[1].click(timeout=200)
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
        page.wait_for_timeout(250)

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


def _extract_project_address_from_payload(payload_obj):
    """Extract project address context captured at webhook ingestion time."""
    if not isinstance(payload_obj, dict):
        return {}

    spicer_meta = payload_obj.get('_spicer') if isinstance(payload_obj.get('_spicer'), dict) else {}
    if isinstance(spicer_meta, dict):
        return spicer_meta.get('project_address')

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

    # Ensure nested_address is a dict, else use empty dict
    nested_address = project_obj.get('address') if isinstance(project_obj.get('address'), dict) else {}
    # Ensure all .get() calls are on dicts only
    address = {
        'street': (
            (nested_address.get('street') if isinstance(nested_address, dict) else '')
            or (nested_address.get('line1') if isinstance(nested_address, dict) else '')
            or (nested_address.get('address1') if isinstance(nested_address, dict) else '')
            or (project_obj.get('address1') if isinstance(project_obj, dict) else '')
            or (project_obj.get('street') if isinstance(project_obj, dict) else '')
            or ''
        ),
        'city': (nested_address.get('city') if isinstance(nested_address, dict) else '') or (project_obj.get('city') if isinstance(project_obj, dict) else ''),
        'state': (
            (nested_address.get('state') if isinstance(nested_address, dict) else '')
            or (nested_address.get('stateCode') if isinstance(nested_address, dict) else '')
            or (project_obj.get('state') if isinstance(project_obj, dict) else '')
            or (project_obj.get('stateCode') if isinstance(project_obj, dict) else '')
            or ''
        ),
        'postal': (
            (nested_address.get('postal') if isinstance(nested_address, dict) else '')
            or (nested_address.get('postalCode') if isinstance(nested_address, dict) else '')
            or (nested_address.get('zip') if isinstance(nested_address, dict) else '')
            or (project_obj.get('postalCode') if isinstance(project_obj, dict) else '')
            or (project_obj.get('zip') if isinstance(project_obj, dict) else '')
            or (project_obj.get('zipCode') if isinstance(project_obj, dict) else '')
            or ''
        ),
    }
    if any(str(value).strip() for value in address.values()):
        return {key: str(value).strip() for key, value in address.items()}
    return {}


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
        auto_url = (
            f'https://www2.marketsharpm.com/ContactDetail.aspx?contactOid={customer["id"]}'
            f'&contactType={ui_cfg.contact_type}'
        )
        return auto_url, 'marketsharp-name-match'

    # Double-check: try to match by address only if name match failed
    if project_address and any(project_address.values()):
        customer_by_address = MarketSharpService().get_customer_by_address(project_address)
        if customer_by_address and customer_by_address.get('id'):
            auto_url = (
                f'https://www2.marketsharpm.com/ContactDetail.aspx?contactOid={customer_by_address["id"]}'
                f'&contactType={ui_cfg.contact_type}'
            )
            return auto_url, 'marketsharp-address-match'

    return None, None


def open_customer_and_add_note(page, ui_cfg, item, note_text, search_override=None):
    """Search for customer in MarketSharp UI and add a note. Optionally override search query."""
    customer_name = item['customer_name']
    direct_contact_url, direct_contact_key = resolve_direct_contact_url(item, ui_cfg)
    if direct_contact_url:
        logging.info(
            'Using direct contact URL for customer %s via %s: %s',
            customer_name,
            direct_contact_key,
            direct_contact_url,
        )
        page.goto(direct_contact_url, wait_until='domcontentloaded', timeout=120000)
        page.wait_for_load_state('domcontentloaded')
    else:
        for attempt in (1, 2, 3):
            try:
                page.goto(ui_cfg.base_url, wait_until='domcontentloaded', timeout=120000)
                page.wait_for_load_state('domcontentloaded')
                break
            except PlaywrightError as exc:
                if 'net::ERR_ABORTED' in str(exc) and attempt < 3:
                    logging.warning('Navigation aborted on attempt %s; retrying.', attempt)
                    page.wait_for_timeout(1000)
                    continue
                raise

        search_candidates = [
            ui_cfg.search_input_selector,
            '#searchTextBox',
            '#txtSearchBoxMobile',
        ]
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
        customer_query = search_override if search_override is not None else customer_name
        try:
            # Ensure page is fully loaded before interacting
            page.wait_for_load_state('domcontentloaded')
            search_box, search_selector_used = pick_visible_locator(page, search_candidates, timeout_ms=500)
            search_box.click()
            search_box.fill(customer_query)
            # Dispatch keyup/Enter to force UI update
            page.keyboard.press('Enter')
            page.wait_for_timeout(5)
            # Robust navigation wait: try to wait for navigation, catch context errors, and retry DOM readiness
            try:
                page.wait_for_load_state('domcontentloaded', timeout=7000)
            except Exception as nav_exc:
                logging.warning('Navigation wait after search triggered an exception: %s', nav_exc)
                # Try to wait for the DOM to be ready again
                try:
                    page.wait_for_load_state('domcontentloaded', timeout=7000)
                except Exception as nav_exc2:
                    logging.error('Second navigation wait also failed: %s', nav_exc2)
                    raise
            # Now interact with the DOM
            result_selector, label = click_matching_result(page, result_candidates, customer_query, timeout_ms=12000)
            logging.info('Clicked result selector=%s label=%s', result_selector, label)
        except PlaywrightTimeoutError as exc:
            last_match_exc = exc
            raise last_match_exc

    try:
        page.wait_for_load_state('domcontentloaded', timeout=15000)
    except PlaywrightTimeoutError:
        logging.info('Contact page load-state wait timed out; continuing.')

    if ui_cfg.notes_tab_selector:
        try:
            notes_tab = page.locator(ui_cfg.notes_tab_selector).first
            notes_tab.wait_for(state='visible', timeout=8000)
            notes_tab.click(timeout=10000)
            logging.info('Clicked notes tab selector: %s', ui_cfg.notes_tab_selector)
        except PlaywrightTimeoutError:
            logging.info('Notes tab not visible; continuing without explicit tab click.')

    note_button, note_button_selector_used = pick_visible_locator(
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
    logging.info('Using note-button selector: %s', note_button_selector_used)
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
    pending_items = queue.get_pending_batch(limit=ui_cfg.batch_size)
    if not pending_items:
        return 0

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

        queue.mark_processing(queue_id)
        posted = False
        last_error = None
        for idx, variant in enumerate(search_variants):
            logging.info('Attempting search variant %d/%d: "%s"', idx + 1, len(search_variants), variant)
            for attempt in range(3):
                try:
                    open_customer_and_add_note(page, ui_cfg, item, note_text, search_override=variant)
                    queue.mark_posted(queue_id)
                    # Log to audit DB
                    log_posted_comment(
                        event_id=item.get('event_id'),
                        customer_id=item.get('customer_id'),
                        customer_name=customer_name,
                        author_name=author_name,
                        comment_text=note_text,
                        extra_json=item.get('payload_json'),
                    )
                    logging.info('Posted queued item id=%s customer=%s using search="%s"', queue_id, customer_name, variant)
                    posted = True
                    break
                except Exception as exc:
                    if 'Execution context was destroyed' in str(exc) or 'Most likely because of a navigation' in str(exc):
                        logging.warning('Execution context lost on search variant "%s" (attempt %d), reloading page and retrying...', variant, attempt + 1)
                        try:
                            page.reload(wait_until='domcontentloaded', timeout=15000)
                            page.wait_for_load_state('domcontentloaded', timeout=15000)
                        except Exception as reload_exc:
                            logging.error('Page reload failed: %s', reload_exc)
                        continue
                    last_error = str(exc)
                    logging.exception('Error posting queued item id=%s (search="%s", attempt %d)', queue_id, variant, attempt + 1)
                    break
                # If posted, break out of retry loop
                if posted:
                    break
            if posted:
                break

        # If all variants failed, and address is available, try address-only search as a final check
        if not posted and address_variants:
            for addr in address_variants:
                logging.info('Final address-only check for queue_id=%s: "%s"', queue_id, addr)
                for attempt in range(3):
                    try:
                        open_customer_and_add_note(page, ui_cfg, item, note_text, search_override=addr)
                        queue.mark_posted(queue_id)
                        # Log to audit DB
                        log_posted_comment(
                            event_id=item.get('event_id'),
                            customer_id=item.get('customer_id'),
                            customer_name=customer_name,
                            author_name=author_name,
                            comment_text=note_text,
                            extra_json=item.get('payload_json'),
                        )
                        logging.info('Posted queued item id=%s customer=%s using address-only search="%s"', queue_id, customer_name, addr)
                        posted = True
                        break
                    except Exception as exc:
                        if 'Execution context was destroyed' in str(exc) or 'Most likely because of a navigation' in str(exc):
                            logging.warning('Execution context lost on address-only search "%s" (attempt %d), reloading page and retrying...', addr, attempt + 1)
                            try:
                                page.reload(wait_until='domcontentloaded', timeout=15000)
                                page.wait_for_load_state('domcontentloaded', timeout=15000)
                            except Exception as reload_exc:
                                logging.error('Page reload failed: %s', reload_exc)
                            continue
                        last_error = str(exc)
                        logging.exception('Error posting queued item id=%s (address-only search="%s", attempt %d)', queue_id, addr, attempt + 1)
                        break
                    if posted:
                        break
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

