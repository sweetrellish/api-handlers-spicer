"""HTTP client wrapper for MarketSharp operations used by this integration."""

import base64
import hashlib
import hmac
import logging
import re
import time
from datetime import datetime, timezone
import requests
from config.config import Config
from scripts.posted_comments_audit import log_posted_comment



class MarketSharpService:
    def __init__(self):
        self.mode = Config.MARKETSHARP_MODE
        self.base_url = Config.MARKETSHARP_BASE_URL.rstrip('/')
        self.api_key = Config.MARKETSHARP_API_KEY
        self.odata_url = Config.MARKETSHARP_ODATA_URL.rstrip('/')
        self.company_id = str(Config.MARKETSHARP_COMPANY_ID)
        self.user_key = Config.MARKETSHARP_USER_KEY
        self.secret_key = Config.MARKETSHARP_SECRET_KEY
        self.note_contact_type = Config.MARKETSHARP_NOTE_CONTACT_TYPE
        self.timeout = 10
        self._odata_entity_cache = {}
        self.rest_headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        self.effective_mode = self._resolve_mode()

    def get_customer_by_address(self, project_address):
        """Return the best customer match for a given address (street/city/state/postal)."""
        if not isinstance(project_address, dict) or not any(project_address.values()):
            return None

        if self.effective_mode == 'rest_write':
            # REST: Try searching by postal, city, or street (if supported by your REST API)
            # This is a fallback; REST API may not support direct address search.
            for field in ('postal', 'city', 'street'):
                value = project_address.get(field)
                if value:
                    try:
                        response = requests.get(
                            f'{self.base_url}/customers',
                            headers=self.rest_headers,
                            params={field: value},
                            timeout=self.timeout
                        )
                        response.raise_for_status()
                        data = response.json()
                        customers = data.get('data', []) if isinstance(data, dict) else data
                        best = None
                        best_score = -100
                        for customer in customers:
                            score = self._address_match_score(project_address, customer)
                            if score > best_score:
                                best = customer
                                best_score = score
                        if best and best_score >= 0:
                            return best
                    except requests.RequestException as e:
                        logging.warning('REST address search failed for %s=%s: %s', field, value, e)
            return None

        # OData: Search by postal, city, or street
        addr = self._normalize_address_obj(project_address)
        search_fields = []
        if addr.get('postal'):
            search_fields.append(('postalCode', addr['postal']))
            search_fields.append(('zipCode', addr['postal']))
        if addr.get('city'):
            search_fields.append(('city', addr['city']))
        if addr.get('street'):
            search_fields.append(('address1', addr['street']))
            search_fields.append(('street', addr['street']))

        contacts_by_id = {}
        for field, value in search_fields:
            escaped_value = value.replace("'", "''")
            filter_query = f"$filter=substringof('{escaped_value}',{field})"
            try:
                contacts = self._odata_fetch_contacts(filter_query, top=50)
                for contact in contacts:
                    cid = contact.get('id')
                    if cid:
                        contacts_by_id[cid] = contact
            except requests.RequestException as exc:
                logging.warning('OData address search failed for %s=%s: %s', field, value, exc)

        if not contacts_by_id:
            return None

        # Score and pick best
        best = None
        best_score = -100
        for contact in contacts_by_id.values():
            score = self._address_match_score(project_address, contact)
            if score > best_score:
                best = contact
                best_score = score
        if best and best_score >= 0:
            return best
        return None

    def _resolve_mode(self):
        """Pick the active integration mode from configured credentials."""
        if self.mode != 'auto':
            return self.mode

        has_odata = all([self.company_id, self.user_key, self.secret_key])
        has_rest = all([self.api_key, self.base_url])

        if has_odata:
            return 'odata_write'
        if has_rest:
            return 'rest_write'
        return 'odata_readonly'

    def supports_write(self):
        """Return whether this service can attempt direct write operations."""
        return self.effective_mode in ['rest_write', 'odata_write']

    def _odata_headers(self, verbose=False):
        """Build headers for OData requests."""
        headers = {
            'Authorization': self._odata_auth_header(),
            'Accept': 'application/json',
        }
        if verbose:
            headers['Content-Type'] = 'application/json;odata=verbose'
            headers['Accept'] = 'application/json;odata=verbose'
        else:
            headers['Content-Type'] = 'application/json'
        return headers

    def _odata_auth_header(self):
        """Build MarketSharp OData custom auth header from API Maintenance credentials."""
        epoch = str(int(time.time()))
        message = f'{self.company_id}{self.user_key}{epoch}'.encode('utf-8')
        secret_bytes = base64.b64decode(self.secret_key)
        digest = hmac.new(secret_bytes, message, hashlib.sha256).digest()
        digest_b64 = base64.b64encode(digest).decode('utf-8')
        return f'{self.company_id}:{self.user_key}:{epoch}:{digest_b64}'

    @staticmethod
    def _normalize_name(value):
        """Normalize customer names for resilient matching across systems."""
        if not isinstance(value, str):
            return ''
        normalized = value.lower().replace('&', ' and ')
        normalized = re.sub(r'[^a-z0-9\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    @staticmethod
    def _normalize_address_text(value):
        if not isinstance(value, str):
            return ''
        normalized = value.lower().replace('&', ' and ')
        normalized = re.sub(r'[^a-z0-9\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized

    def _normalize_address_obj(self, address_obj):
        if not isinstance(address_obj, dict):
            return {
                'street': '',
                'city': '',
                'state': '',
                'postal': '',
                'full': '',
            }

        street = self._normalize_address_text(
            address_obj.get('street')
            or address_obj.get('line1')
            or address_obj.get('address1')
            or address_obj.get('streetAddress')
            or address_obj.get('address')
            or ''
        )
        city = self._normalize_address_text(address_obj.get('city') or '')
        state = self._normalize_address_text(address_obj.get('state') or address_obj.get('stateCode') or '')
        postal = self._normalize_address_text(
            address_obj.get('postal')
            or address_obj.get('postalCode')
            or address_obj.get('zip')
            or address_obj.get('zipCode')
            or ''
        )
        full = ' '.join([part for part in [street, city, state, postal] if part]).strip()
        return {
            'street': street,
            'city': city,
            'state': state,
            'postal': postal,
            'full': full,
        }

    def _coerce_address_dict(self, address_obj):
        if not isinstance(address_obj, dict):
            return {}
        return {
            'street': (
                address_obj.get('street')
                or address_obj.get('line1')
                or address_obj.get('address1')
                or address_obj.get('streetAddress')
                or address_obj.get('address')
                or ''
            ),
            'city': address_obj.get('city') or '',
            'state': address_obj.get('state') or address_obj.get('stateCode') or '',
            'postal': (
                address_obj.get('postal')
                or address_obj.get('postalCode')
                or address_obj.get('zip')
                or address_obj.get('zipCode')
                or ''
            ),
        }

    def _fetch_odata_entity(self, uri):
        if not uri:
            return {}

        normalized_uri = uri.replace('http://', 'https://')
        if normalized_uri in self._odata_entity_cache:
            return self._odata_entity_cache[normalized_uri]

        try:
            response = requests.get(
                normalized_uri,
                headers=self._odata_headers(verbose=True),
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            entity = {}
            if isinstance(data, dict):
                raw = data.get('d', {})
                if isinstance(raw, dict):
                    entity = raw
                elif isinstance(raw, list) and raw:
                    entity = raw[0]
            self._odata_entity_cache[normalized_uri] = entity
            return entity
        except requests.RequestException as exc:
            logging.warning('Failed OData entity fetch %s: %s', normalized_uri, exc)
            self._odata_entity_cache[normalized_uri] = {}
            return {}

    def _extract_contact_address(self, contact):
        if not isinstance(contact, dict):
            return {}

        # Some tenants expose address fields directly on Contact.
        direct = self._coerce_address_dict(contact)
        if any(direct.values()):
            return direct

        address_link = ((contact.get('Address') or {}).get('__deferred') or {}).get('uri')
        if not address_link:
            return {}

        address_entity = self._fetch_odata_entity(address_link)
        return self._coerce_address_dict(address_entity)

    def _address_match_score(self, project_address, contact):
        if not isinstance(project_address, dict):
            return 0

        project = self._normalize_address_obj(project_address)
        if not project.get('full'):
            return 0

        contact_address = self._normalize_address_obj(self._extract_contact_address(contact))
        if not contact_address.get('full'):
            return 0

        score = 0
        project_postal = project.get('postal')
        contact_postal = contact_address.get('postal')
        if project_postal and contact_postal:
            if project_postal == contact_postal:
                score += 4
            else:
                return -6

        project_street = project.get('street')
        contact_street = contact_address.get('street')
        if project_street and contact_street:
            if project_street == contact_street:
                score += 3
            elif project_street in contact_street or contact_street in project_street:
                score += 2

        if project.get('city') and contact_address.get('city') and project['city'] == contact_address['city']:
            score += 2

        if project.get('state') and contact_address.get('state') and project['state'] == contact_address['state']:
            score += 1

        return score

    def get_customer_by_name(self, customer_name, project_address=None):
        """Return the best exact-name match for a MarketSharp customer."""
        if self.effective_mode == 'rest_write':
            return self._get_customer_by_name_rest(customer_name, project_address=project_address)
        return self._get_customer_by_name_odata(customer_name, project_address=project_address)

    def _get_customer_by_name_rest(self, customer_name, project_address=None):
        """Find customer by name using partner REST API."""
        try:
            response = requests.get(
                f'{self.base_url}/customers',
                headers=self.rest_headers,
                params={'name': customer_name},
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            # MarketSharp API typically returns a list
            if isinstance(data, dict) and 'data' in data:
                customers = data['data']
            elif isinstance(data, list):
                customers = data
            else:
                return None

            target = self._normalize_name(customer_name)

            # Prefer exact normalized match.
            for customer in customers:
                if self._normalize_name(customer.get('name', '')) == target:
                    return customer

            # Fallback to containment match for minor formatting differences.
            for customer in customers:
                candidate = self._normalize_name(customer.get('name', ''))
                if candidate and target and (candidate in target or target in candidate):
                    return customer

            return None
        except requests.RequestException as e:
            logging.exception('Error fetching MarketSharp customer %s: %s', customer_name, str(e))
            return None

    def _get_customer_by_name_odata(self, customer_name, project_address=None):
        """Find customer by name using read-only OData endpoint.

        When project_address is available, first attempt a combined name+address
        OData query (last-name AND postal/city).  This resolves household names
        like "Bill and Christine Hubbard" that would never match "Bill Hubbard"
        on name tokens alone.  Falls back to name-only search if the combined
        query returns nothing or errors.
        """
        try:
            # --- address-anchored pass (highest precision) ---
            if project_address:
                address_contacts = self._search_contacts_odata_name_and_address(
                    customer_name, project_address
                )
                if address_contacts:
                    # Pick best candidate; allow fuzzy because compound names won't
                    # be an exact token match.
                    match = self._match_contact_candidates(
                        address_contacts,
                        customer_name,
                        project_address=project_address,
                        exact_only=False,
                    )
                    if match:
                        logging.info(
                            'OData name+address query resolved "%s" -> %s',
                            customer_name,
                            match.get('name'),
                        )
                        return match

            # --- name-only pass (standard path) ---
            contacts = self._search_contacts_odata(customer_name)
            exact_match = self._match_contact_candidates(
                contacts,
                customer_name,
                project_address=project_address,
                exact_only=True,
            )
            if exact_match:
                return exact_match

            fuzzy_match = self._match_contact_candidates(
                contacts,
                customer_name,
                project_address=project_address,
                exact_only=False,
            )
            if fuzzy_match:
                return fuzzy_match

            return None
        except Exception as e:
            logging.exception('Error fetching MarketSharp OData customer %s: %s', customer_name, str(e))
            return None

    def _search_contacts_odata_name_and_address(self, customer_name, project_address):
        """Query OData with (last-name token) AND (postal OR city) combined filter.

        This is the key disambiguation query for households with compound names.
        We try multiple address clauses in priority order and stop at the first
        non-empty result set.
        """
        search_terms = self._odata_contact_search_terms(customer_name)
        if not search_terms:
            return []

        addr = self._normalize_address_obj(project_address) if isinstance(project_address, dict) else {}
        postal = addr.get('postal', '').strip()
        city = self._normalize_address_text(addr.get('city', ''))
        street = self._normalize_address_text(addr.get('street', ''))

        address_clauses = []
        if postal:
            escaped_postal = postal.replace("'", "''")
            address_clauses.append(f"substringof('{escaped_postal}',postalCode) or substringof('{escaped_postal}',zipCode)")
        if city:
            escaped_city = city.replace("'", "''")
            address_clauses.append(f"substringof('{escaped_city}',city)")

        if not address_clauses:
            return []

        contacts_by_id = {}
        # Use only the strongest name term (last token = surname) for the AND query.
        # Title-case the term since MS OData is case-sensitive on lastName.
        primary_term = search_terms[0]
        name_variants = list(dict.fromkeys([primary_term.capitalize(), primary_term.title(), primary_term]))

        for addr_clause in address_clauses:
            for variant in name_variants:
                escaped_term = variant.replace("'", "''")
                name_clause = (
                    f"substringof('{escaped_term}',firstName) or "
                    f"substringof('{escaped_term}',lastName) or "
                    f"substringof('{escaped_term}',businessName)"
                )
                filter_query = f"$filter=({name_clause}) and ({addr_clause})"
                try:
                    contacts = self._odata_fetch_contacts(filter_query, top=25)
                    for contact in contacts:
                        contact_id = contact.get('id')
                        if contact_id:
                            contacts_by_id[contact_id] = contact
                    if contacts_by_id:
                        logging.info(
                            'OData name+address filter found %d candidate(s) for "%s" using variant=%s clause: %s',
                            len(contacts_by_id),
                            customer_name,
                            variant,
                            addr_clause,
                        )
                        break
                except requests.RequestException as exc:
                    logging.warning(
                        'OData name+address query failed for "%s" (variant=%s clause: %s): %s',
                        customer_name,
                        variant,
                        addr_clause,
                        exc,
                    )
            if contacts_by_id:
                break

        return list(contacts_by_id.values())

    def _odata_contact_search_terms(self, customer_name):
        """Generate high-signal name tokens for filtered OData contact lookups."""
        normalized = self._normalize_name(customer_name)
        tokens = [token for token in normalized.split() if token and token != 'and']
        search_terms = []
        for token in reversed(tokens):
            if len(token) >= 3:
                search_terms.append(token)
        return search_terms[:4]

    def _odata_fetch_contacts(self, filter_query, top=50):
        """Fetch contacts from OData using the legacy Contacts() endpoint."""
        url = f'{self.odata_url}/Contacts()?{filter_query}&$top={top}'
        response = requests.get(
            url,
            headers=self._odata_headers(verbose=True),
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            return []
        raw_contacts = data.get('d', [])
        if isinstance(raw_contacts, list):
            return raw_contacts
        if isinstance(raw_contacts, dict):
            return raw_contacts.get('results', []) or []
        return []

    def _search_contacts_odata(self, customer_name):
        """Search OData contacts by significant name tokens and aggregate candidates.

        The MS OData API is case-sensitive on lastName/firstName for substringof.
        We try each token in both its original (lowercase) form and Title-cased
        form to ensure name lookups succeed regardless of stored case.
        """
        search_terms = self._odata_contact_search_terms(customer_name)
        if not search_terms:
            return []

        contacts_by_id = {}
        for term in search_terms:
            lower = term.lower()
            title = term.capitalize()

            # Attempt 1: multi-field OR with lowercase (lastName first — field order is
            # significant for this OData server; firstName-first queries 400).
            escaped_lower = lower.replace("'", "''")
            multi_query = (
                "$filter="
                f"substringof('{escaped_lower}',lastName) or "
                f"substringof('{escaped_lower}',firstName) or "
                f"substringof('{escaped_lower}',businessName)"
            )
            try:
                found = self._odata_fetch_contacts(multi_query, top=75)
                for c in found:
                    cid = c.get('id')
                    if cid:
                        contacts_by_id[cid] = c
            except requests.RequestException as exc:
                logging.warning('OData multi-field search failed for term %s: %s', lower, exc)

            # Attempt 2: lastName-only Title Case (MS OData requires Title Case for lastName
            # substringof; this catches compound/household names like "Bill and Christine Hubbard").
            if title != lower:
                escaped_title = title.replace("'", "''")
                last_name_query = f"$filter=substringof('{escaped_title}',lastName)"
                try:
                    found = self._odata_fetch_contacts(last_name_query, top=75)
                    for c in found:
                        cid = c.get('id')
                        if cid:
                            contacts_by_id[cid] = c
                except requests.RequestException as exc:
                    logging.warning('OData lastName search failed for term %s: %s', title, exc)

        return list(contacts_by_id.values())

    def _match_contact_candidates(self, contacts, customer_name, project_address=None, exact_only=False):
        """Return the best candidate from a filtered OData contact set."""
        target = self._normalize_name(customer_name)
        exact_candidates = []
        for contact in contacts:
            full_name = f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
            candidate_names = [
                full_name,
                contact.get('businessName', ''),
                contact.get('mailMergeName', ''),
                contact.get('qbName', ''),
            ]
            for candidate in candidate_names:
                normalized = self._normalize_name(candidate)
                if normalized and normalized == target:
                    address_score = self._address_match_score(project_address, contact)
                    exact_candidates.append((address_score, {
                        'id': contact.get('id'),
                        'name': candidate or customer_name,
                        'raw': contact,
                    }))

        if exact_candidates:
            if any(score >= 0 for score, _ in exact_candidates):
                exact_candidates.sort(key=lambda pair: pair[0], reverse=True)
                return exact_candidates[0][1]
            return exact_candidates[0][1]

        if exact_only:
            return None

        target_tokens = set(t for t in target.split() if len(t) >= 3)
        fuzzy_candidates = []
        for contact in contacts:
            full_name = f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
            candidate_names = [
                full_name,
                contact.get('businessName', ''),
                contact.get('mailMergeName', ''),
                contact.get('qbName', ''),
            ]
            for candidate in candidate_names:
                normalized = self._normalize_name(candidate)
                if not normalized:
                    continue

                # Substring containment check (original behaviour).
                substring_match = target and (normalized in target or target in normalized)

                # Token-subset check: all significant target tokens appear in the candidate.
                # This catches "Bill Hubbard" → "Bill and Christine Hubbard".
                candidate_tokens = set(t for t in normalized.split() if len(t) >= 3)
                token_subset_match = bool(target_tokens) and target_tokens.issubset(candidate_tokens)

                if substring_match or token_subset_match:
                    address_score = self._address_match_score(project_address, contact)
                    # Require address non-mismatch for token-subset-only matches.
                    # Score 0 = no address data (neutral) → accept. Score < 0 = address conflict → reject.
                    if token_subset_match and not substring_match and address_score < 0:
                        continue
                    fuzzy_candidates.append((address_score, {
                        'id': contact.get('id'),
                        'name': candidate or customer_name,
                        'raw': contact,
                    }))

        if not fuzzy_candidates:
            return None

        if any(score >= 0 for score, _ in fuzzy_candidates):
            fuzzy_candidates.sort(key=lambda pair: pair[0], reverse=True)
            return fuzzy_candidates[0][1]

        return None

    def post_comment(self, customer_id, comment_text, author_name=None):
        """Create a note on the target MarketSharp customer account."""
        if self.effective_mode == 'odata_write':
            return self._post_comment_odata(customer_id, comment_text, author_name)

        if self.effective_mode != 'rest_write':
            logging.info('Skipping direct write: MarketSharp mode is %s', self.effective_mode)
            return None
        try:
            # Prepend author to comment text if present and not already present
            note_text = comment_text
            if author_name and author_name.strip() and not comment_text.strip().startswith(f'[{author_name.strip()}]'):
                note_text = f'[{author_name.strip()}] {comment_text.strip()}'

            payload = {
                'text': note_text,
                # Keep the mapped object type explicit for future API changes.
                'type': 'note'
            }

            if author_name:
                payload['author'] = author_name

            response = requests.post(
                f'{self.base_url}/customers/{customer_id}/notes',
                headers=self.rest_headers,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.exception(
                'Error posting comment to MarketSharp customer %s: %s',
                customer_id,
                str(e),
            )
            return None

    def _post_comment_odata(self, customer_id, comment_text, author_name=None):
        """Create a MarketSharp note through the OData Notes entity."""
        note_body = comment_text
        if author_name and author_name.strip() and not comment_text.strip().startswith(f'[{author_name.strip()}]'):
            note_body = f'[{author_name.strip()}] {comment_text.strip()}'

        now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        base_payload = {
            'contactId': customer_id,
            'contactType': self.note_contact_type,
            'note': note_body,
            'dateTime': now_iso,
            'isActive': True,
        }

        try:
            # Attempt 1: simple JSON payload.
            response = requests.post(
                f'{self.odata_url}/Notes',
                headers=self._odata_headers(),
                json=base_payload,
                timeout=self.timeout,
            )

            if response.status_code in [200, 201, 204]:
                try:
                    return response.json()
                except ValueError:
                    return {'status': response.status_code, 'created': True}

            # Attempt 2: OData verbose payload with metadata type.
            verbose_payload = dict(base_payload)
            verbose_payload['__metadata'] = {'type': 'MSharpModel.Note'}
            response2 = requests.post(
                f'{self.odata_url}/Notes',
                headers=self._odata_headers(verbose=True),
                json=verbose_payload,
                timeout=self.timeout,
            )
            if response2.status_code in [200, 201, 204]:
                try:
                    return response2.json()
                except ValueError:
                    return {'status': response2.status_code, 'created': True}

            logging.error(
                'OData note create failed customer=%s status1=%s body1=%s status2=%s body2=%s',
                customer_id,
                response.status_code,
                (response.text or '')[:300],
                response2.status_code,
                (response2.text or '')[:300],
            )
            return None
        except requests.RequestException as e:
            logging.exception('Error posting OData note to customer %s: %s', customer_id, str(e))
            return None

    # Log to audit DB after successful post
            from scripts.posted_comments_audit import log_posted_comment
            log_posted_comment(
                event_id=None,  # You may pass event_id if available
                customer_id=customer_id,
                customer_name=None,  # You may pass customer_name if available
                author_name=author_name,
                comment_text=note_text,
                extra_json=None,  # You may pass extra context if available
            )

