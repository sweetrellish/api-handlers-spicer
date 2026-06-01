"""Flask entrypoint for receiving CompanyCam webhooks and syncing to MarketSharp."""

from flask import Flask, request, jsonify # type: ignore
import logging
from collections import deque
from webhook_handler import WebhookHandler
from config import Config
from security import IdempotencyStore, extract_event_id, verify_webhook_auth

app = Flask(__name__)
handler = WebhookHandler()
idempotency_store = IdempotencyStore(Config.IDEMPOTENCY_DB_PATH)
recent_comments = deque(maxlen=100)

# Keep logs structured enough for grep/tail when running under systemd or Docker.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)


def _extract_recent_comment(event_data, event_id):
    """Extract a minimal recent-comment record for the /comments dev feed."""
    if not isinstance(event_data, dict):
        return None

    event_type = (
        event_data.get('type')
        or event_data.get('event')
        or event_data.get('event_type')
        or event_data.get('scope')
        or ''
    )
    if not str(event_type).startswith('comment.'):
        return None

    comment_data = event_data.get('data') or event_data.get('payload') or {}
    if not isinstance(comment_data, dict):
        comment_data = {}
    if not comment_data:
        comment_data = event_data

    comment_obj = comment_data.get('comment') or {}
    if not isinstance(comment_obj, dict):
        comment_obj = {}

    text = (
        comment_data.get('text')
        or comment_data.get('body')
        or comment_data.get('content')
        or comment_obj.get('text')
        or comment_obj.get('body')
        or comment_obj.get('content')
        or ''
    )
    if not isinstance(text, str) or not text.strip():
        return None

    comment_id = (
        comment_data.get('id')
        or comment_data.get('comment_id')
        or comment_obj.get('id')
        or event_id
    )
    user_obj = comment_data.get('user') or {}
    if not isinstance(user_obj, dict):
        user_obj = {}

    return {
        'id': str(comment_id),
        'text': text.strip(),
        'event_type': str(event_type),
        'user_name': user_obj.get('name') or comment_data.get('user_name') or '',
        'source': comment_data.get('source') or 'comment_webhook',
        'note_type': comment_data.get('note_type') or '',
    }


def _record_recent_comment(event_data, event_id):
    record = _extract_recent_comment(event_data, event_id)
    if not record:
        return

    for existing in recent_comments:
        if existing.get('id') == record['id']:
            return
    recent_comments.appendleft(record)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


@app.route('/comments', methods=['GET'])
def comments():
    """Development feed of recently received comment webhook payloads."""
    return jsonify(list(recent_comments)), 200


@app.route('/comments/clear', methods=['POST'])
def clear_comments():
    """Clear the in-memory development comments feed."""
    recent_comments.clear()
    return jsonify({'success': True, 'count': 0}), 200

@app.route('/webhook/companycam', methods=['POST'])
def companycam_webhook():
    """
    Webhook endpoint for CompanyCam events
    
    CompanyCam will POST events to this endpoint with comment.* event types
    """
    try:
        # Retain the raw payload for signature checks and stable dedupe hashing.
        raw_body = request.get_data() or b''

        # Parse JSON body without throwing a framework error on invalid input.
        event_data = request.get_json(silent=True)

        if not event_data:
            return jsonify({
                'success': False,
                'message': 'No JSON payload provided'
            }), 400

        logging.info('Webhook payload keys: %s', sorted(event_data.keys()))

        verified, verify_message = verify_webhook_auth(event_data, raw_body, request.headers)
        if not verified:
            logging.warning('Rejected webhook: %s', verify_message)
            logging.info(
                'Auth debug: headers=%s token_fields=%s',
                sorted(list(request.headers.keys())),
                {
                    'top_level_token': bool(event_data.get('token')),
                    'top_level_webhook_token': bool(event_data.get('webhook_token')),
                    'data_token': bool((event_data.get('data') or {}).get('token'))
                    if isinstance(event_data.get('data'), dict)
                    else False,
                    'data_webhook_token': bool((event_data.get('data') or {}).get('webhook_token'))
                    if isinstance(event_data.get('data'), dict)
                    else False,
                },
            )
            return jsonify({
                'success': False,
                'message': 'Unauthorized webhook request'
            }), 401

        # CompanyCam may retry deliveries; dedupe ensures idempotent handling.
        event_id = extract_event_id(event_data, raw_body)
        if idempotency_store.seen_or_store(event_id):
            logging.info('Duplicate webhook ignored: %s', event_id)
            return jsonify({
                'success': True,
                'message': 'Duplicate webhook ignored',
                'event_id': event_id
            }), 200

        # Log the event
        logging.info(
            'Received webhook event: %s (id=%s)',
            event_data.get('type')
            or event_data.get('event')
            or event_data.get('event_type')
            or event_data.get('scope'),
            event_id,
        )

        _record_recent_comment(event_data, event_id)

        # Process the event
        result = handler.process_comment_event(event_data)
        logging.info('Webhook processing result: %s', result)

        # Return 200 so webhook providers do not auto-disable on business-level failures.
        return jsonify(result), 200

    except Exception as e:
        logging.exception('Error processing webhook: %s', str(e))
        return jsonify({
            'success': False,
            'message': f'Internal server error: {str(e)}'
        }), 500

@app.route('/test', methods=['POST'])
def test_webhook():
    """
    Development endpoint to seed the /comments feed with a synthetic comment.

    This bypasses CompanyCam and MarketSharp business processing so the
    comment worker can be validated locally against a predictable input.
    """
    try:
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}

        text = payload.get('text') or 'Local dev seeded comment @rellis'
        comment_id = str(payload.get('id') or f'test-comment-{len(recent_comments) + 1}')
        user_name = payload.get('user_name') or 'Local Test User'
        source = payload.get('source') or 'comment_webhook'
        note_type = payload.get('note_type') or ''

        test_event = {
            'type': 'comment.created',
            'data': {
                'id': comment_id,
                'text': text,
                'project_id': 'local-dev-project',
                'user': {'name': user_name},
                'source': source,
                'note_type': note_type,
            }
        }
        _record_recent_comment(test_event, comment_id)

        return jsonify({
            'success': True,
            'message': 'Seeded development comment feed',
            'comment': _extract_recent_comment(test_event, comment_id),
            'count': len(recent_comments),
        }), 200
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error in test: {str(e)}'
        }), 500


@app.route('/test/marketsharp-note', methods=['POST'])
def test_marketsharp_note():
    """Seed the /comments feed with a MarketSharp-style Contact Note record."""
    try:
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}

        seeded_payload = {
            'text': payload.get('text') or '@rellis local MarketSharp Contact Note test',
            'id': payload.get('id') or f'ms-note-{len(recent_comments) + 1}',
            'user_name': payload.get('user_name') or 'Ryan Ellis',
            'source': 'marketsharp_ui_note',
            'note_type': payload.get('note_type') or 'Contact Note',
        }
        # Reuse the generic local seeding logic.
        with app.test_request_context('/test', method='POST', json=seeded_payload):
            return test_webhook()
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error in MarketSharp note test: {str(e)}'
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Return consistent JSON for unknown routes."""
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    """Return consistent JSON for unhandled internal errors."""
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # This is for local testing. Use Gunicorn in production.
    app.run(
        host='0.0.0.0',
        port=Config.FLASK_PORT,
        debug=Config.FLASK_DEBUG
    )
