"""Core business logic for mapping CompanyCam webhook comments into MarketSharp notes."""

import logging
import json
import os
import hashlib
from datetime import datetime
from companycam_service import CompanyCamService
from marketsharp_service import MarketSharpService
from pending_queue import PendingCommentQueue
from config import Config


class WebhookHandler:
    # Path to the user mapping file (edit as needed)
    USER_MAPPING_FILE = os.getenv('COMPANYCAM_TO_MARKETSHARP_USER_MAP', 'companycam_to_marketsharp_user_map.json')

    def _load_user_mapping(self):
        try:
            with open(self.USER_MAPPING_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not load user mapping file: {self.USER_MAPPING_FILE} ({e})")
            return {}
    """Handles CompanyCam webhook events and syncs to MarketSharp"""

    def __init__(self):
        self.cc_service = CompanyCamService()
        self.ms_service = MarketSharpService()
        self.pending_queue = PendingCommentQueue(Config.PENDING_QUEUE_DB_PATH)

    @staticmethod
    def _extract_project_address(project):
        """Extract a normalized project address object from CompanyCam project payloads."""
        if not isinstance(project, dict):
            return {}

        nested_address = project.get('address') or {}
        if not isinstance(nested_address, dict):
            nested_address = {}

        street = (
            nested_address.get('street')
            or nested_address.get('line1')
            or nested_address.get('address1')
            or project.get('address')
            or project.get('address1')
            or project.get('street')
            or ''
        )
        city = nested_address.get('city') or project.get('city') or ''
        state = nested_address.get('state') or nested_address.get('stateCode') or project.get('state') or project.get('stateCode') or ''
        postal = (
            nested_address.get('postal')
            or nested_address.get('postalCode')
            or nested_address.get('zip')
            or project.get('postalCode')
            or project.get('zip')
            or project.get('zipCode')
            or ''
        )

        address_obj = {
            'street': str(street).strip(),
            'city': str(city).strip(),
            'state': str(state).strip(),
            'postal': str(postal).strip(),
        }
        if any(address_obj.values()):
            return address_obj
        return {}

    @staticmethod
    def _enrich_payload_with_project_context(event_data, project_id, project_address):
        """Attach stable project context so queue-side resolvers can reuse it."""
        payload = event_data if isinstance(event_data, dict) else {'raw_event': event_data}
        payload = dict(payload)

        spicer_meta = payload.get('_spicer') 
        if not isinstance(spicer_meta, dict):
            spicer_meta = {}
        spicer_meta['project_id'] = str(project_id)
        if isinstance(project_address, dict) and project_address:
            spicer_meta['project_address'] = project_address
        payload['_spicer'] = spicer_meta
        return payload

    def process_comment_event(self, event_data):
        # Load user mapping (CompanyCam name/email → MarketSharp username/ID)
        user_mapping = self._load_user_mapping()
        """
        Process a CompanyCam comment event and post to MarketSharp
        
        Expected event_data structure:
        {
            'type': 'comment.created' or 'comment.updated',
            'data': {
                'id': 'comment_id',
                'text': 'comment text',
                'project_id': 'project_id',
                'user': {'name': 'author name'}
            }
        }
        """
        try:
            # CompanyCam payload shape can vary by webhook version/context.
            event_type = (
                event_data.get('type')
                or event_data.get('event')
                or event_data.get('event_type')
                or event_data.get('scope')
                or ''
            )

            # Only process comment events
            if not event_type.startswith('comment.'):
                return {
                    'success': False,
                    'message': f'Event type {event_type} is not a comment event'
                }

            comment_data = event_data.get('data') or event_data.get('payload') or {}
            if not isinstance(comment_data, dict):
                comment_data = {}

            # Fallback for flatter payloads.
            if not comment_data:
                comment_data = event_data

            project_obj = comment_data.get('project', {})
            if not isinstance(project_obj, dict):
                project_obj = {}

            # Some payloads use integration payload wrappers.
            payload_obj = comment_data.get('payload', {})
            if not isinstance(payload_obj, dict):
                payload_obj = {}

            if not project_obj and isinstance(payload_obj.get('project'), dict):
                project_obj = payload_obj.get('project', {})

            comment_obj = comment_data.get('comment', {})
            if not isinstance(comment_obj, dict):
                comment_obj = {}
            if not comment_obj and isinstance(payload_obj.get('comment'), dict):
                comment_obj = payload_obj.get('comment', {})

            user_obj = comment_data.get('user', {})
            if not isinstance(user_obj, dict):
                user_obj = {}

            author_obj = comment_data.get('author', {})
            if not isinstance(author_obj, dict):
                author_obj = {}

            project_id = (
                comment_data.get('project_id')
                or comment_data.get('projectId')
                or project_obj.get('id')
                or payload_obj.get('project_id')
                or payload_obj.get('projectId')
                or comment_obj.get('project_id')
                or comment_obj.get('projectId')
            )

            # CompanyCam comment object can reference the parent entity via commentable fields.
            commentable_type = (
                comment_data.get('commentable_type')
                or payload_obj.get('commentable_type')
                or comment_obj.get('commentable_type')
                or ''
            )
            commentable_id = (
                comment_data.get('commentable_id')
                or payload_obj.get('commentable_id')
                or comment_obj.get('commentable_id')
            )

            if not project_id and commentable_id:
                lowered_type = str(commentable_type).lower()
                if lowered_type == 'project':
                    project_id = commentable_id
                elif lowered_type == 'location':
                    # CompanyCam often uses "Location" as the project entity type.
                    project_id = commentable_id
                elif lowered_type == 'photo':
                    photo = self.cc_service.get_photo_by_id(commentable_id)
                    if isinstance(photo, dict):
                        project_id = (
                            photo.get('project_id')
                            or (photo.get('project') or {}).get('id')
                        )
                elif not lowered_type:
                    # If type is missing, treat commentable id as project id first.
                    project_id = commentable_id

            text_candidates = [
                comment_data.get('text'),
                comment_data.get('body'),
                comment_data.get('content'),
                payload_obj.get('text'),
                payload_obj.get('body'),
                payload_obj.get('content'),
                comment_obj.get('text'),
                comment_obj.get('body'),
                comment_obj.get('content'),
            ]
            comment_text = next((value for value in text_candidates if isinstance(value, str) and value.strip()), '')

            # Extract CompanyCam user identifier (name or email)
            cc_user_name = (
                user_obj.get('name')
                or author_obj.get('name')
                or comment_data.get('user_name')
                or comment_data.get('author_name')
                or comment_data.get('creator_name')
                or payload_obj.get('creator_name')
                or comment_obj.get('creator_name')
            )
            cc_user_email = (
                user_obj.get('email')
                or author_obj.get('email')
                or comment_data.get('user_email')
                or comment_data.get('author_email')
                or comment_data.get('creator_email')
                or payload_obj.get('creator_email')
                or comment_obj.get('creator_email')
            )

            # Try to map to MarketSharp user
            ms_author = None
            if cc_user_email and cc_user_email in user_mapping:
                ms_author = user_mapping[cc_user_email]
            elif cc_user_name and cc_user_name in user_mapping:
                ms_author = user_mapping[cc_user_name]
            else:
                ms_author = None

            # Fallback: use CompanyCam name if not mapped
            author_name = ms_author or cc_user_name

            if not project_id or not comment_text:
                logging.info(
                    'Extraction debug: event_type=%s commentable_type=%s project_id=%s text_present=%s comment_keys=%s payload_keys=%s comment_obj_keys=%s',
                    event_type,
                    commentable_type,
                    project_id,
                    bool(comment_text),
                    sorted(comment_data.keys()) if isinstance(comment_data, dict) else [],
                    sorted(payload_obj.keys()) if isinstance(payload_obj, dict) else [],
                    sorted(comment_obj.keys()) if isinstance(comment_obj, dict) else [],
                )

            if not project_id or not comment_text:
                return {
                    'success': False,
                    'message': 'Missing required fields: project_id or comment text'
                }

            # Get project details to find customer name
            project = self.cc_service.get_project_by_id(project_id)
            if not project:
                return {
                    'success': False,
                    'message': f'Could not retrieve project {project_id} from CompanyCam'
                }

            # Prefer project name; fallback for alternate CompanyCam field naming.
            customer_name = project.get('name') or project.get('customer_name')
            if not customer_name:
                return {
                    'success': False,
                    'message': f'Could not find customer name in project {project_id}'
                }

            project_address = self._extract_project_address(project)
            payload_with_context = self._enrich_payload_with_project_context(
                event_data,
                project_id,
                project_address,
            )

            event_id = (
                event_data.get('id')
                or comment_data.get('id')
                or comment_obj.get('id')
                or f"cc-{project_id}-{hashlib.sha256(comment_text.encode('utf-8')).hexdigest()[:16]}"
            )

            # Find customer in MarketSharp by name
            customer = self.ms_service.get_customer_by_name(
                customer_name,
                project_address=project_address,
            )
            if not customer:
                queued = self.pending_queue.enqueue(
                    event_id=event_id,
                    customer_name=customer_name,
                    comment_text=comment_text,
                    author_name=author_name,
                    payload=payload_with_context,
                    last_error=f'Customer "{customer_name}" not found in MarketSharp',
                )
                return {
                    'success': True,
                    'queued': True,
                    'queue_id': queued['queue_id'],
                    'message': f'Queued comment: customer "{customer_name}" not found in MarketSharp yet'
                }

            customer_id = customer.get('id')

            # Standard MarketSharp OData access is read-only; queue comment for later replay.
            if not self.ms_service.supports_write():
                queued = self.pending_queue.enqueue(
                    event_id=event_id,
                    customer_name=customer_name,
                    comment_text=comment_text,
                    author_name=author_name,
                    payload=payload_with_context,
                    last_error='MarketSharp integration is in read-only mode (odata_readonly)',
                )
                return {
                    'success': True,
                    'queued': True,
                    'queue_id': queued['queue_id'],
                    'customer_id': customer_id,
                    'message': f'Queued comment for {customer_name}; MarketSharp write access not enabled ({self.ms_service.effective_mode})'
                }

            # Post comment to MarketSharp
            result = self.ms_service.post_comment(customer_id, comment_text, author_name)

            if result:
                return {
                    'success': True,
                    'message': f'Comment posted to MarketSharp customer: {customer_name}',
                    'customer_id': customer_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
            queued = self.pending_queue.enqueue(
                event_id=event_id,
                customer_name=customer_name,
                comment_text=comment_text,
                author_name=author_name,
                payload=payload_with_context,
                last_error=f'Failed direct post for customer_id={customer_id}',
            )
            return {
                'success': True,
                'queued': True,
                'queue_id': queued['queue_id'],
                'customer_id': customer_id,
                'message': f'Queued comment after MarketSharp post failure for {customer_name}'
            }

        except Exception as e:
            return {
                'success': False,
                'message': f'Error processing webhook event: {str(e)}'
            }

