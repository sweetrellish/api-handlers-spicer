"""Flask entrypoint for receiving CompanyCam webhooks and syncing to MarketSharp."""

import sys
import os
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_ROOT_DIR = _THIS_DIR.parent
for _candidate in (str(_THIS_DIR), str(_ROOT_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

from flask import Flask, request, jsonify, send_from_directory # type: ignore
import logging
from collections import deque
from webhook_handler import WebhookHandler
from config import Config
from security import IdempotencyStore, extract_event_id, verify_webhook_auth
from ops_api import ops_bp

try:
    from portal_api import portal_bp
    _PORTAL_IMPORT_ERROR = None
except Exception as _portal_exc:
    portal_bp = None
    _PORTAL_IMPORT_ERROR = _portal_exc

app = Flask(__name__)
app.register_blueprint(ops_bp)
if portal_bp is not None:
    app.register_blueprint(portal_bp)
else:
    logging.warning('portal_api unavailable at startup: %s', _PORTAL_IMPORT_ERROR)
handler = WebhookHandler()
idempotency_store = IdempotencyStore(Config.IDEMPOTENCY_DB_PATH)
recent_comments = deque(maxlen=100)
OPS_GUI_DIST = Path(__file__).resolve().parent.parent / 'API Handler Interactive GUI' / 'dist'
CUSTOMER_PORTAL_DIST = Path(__file__).resolve().parent.parent / 'customer-portal' / 'dist'
TAGGER_DIR = Path(__file__).resolve().parent.parent / 'tagger'


def _resolve_email_splash_path():
    """Resolve the local splash image path for email rendering."""
    configured = (os.getenv('COMMENT_WORKER_SPLASH_IMAGE_PATH', '') or '').strip()
    if configured:
        candidate = Path(configured)
        if not candidate.is_absolute():
            candidate = (TAGGER_DIR / configured).resolve()
        return candidate

    fallback_candidates = [
        TAGGER_DIR / 'ascii-art-text(11).png',
        TAGGER_DIR / 'splash_dim_yellow.png',
        TAGGER_DIR / 'splash_dim_yellow_final.png',
    ]
    for candidate in fallback_candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    # Final fallback keeps legacy behavior if none of the expected files exist.
    return TAGGER_DIR / 'splash_dim_yellow.png'

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


@app.route('/ops-gui', methods=['GET'])
@app.route('/ops-gui/', methods=['GET'])
@app.route('/ops-gui/<path:asset_path>', methods=['GET'])
def ops_gui(asset_path=''):
    """Serve the built ops GUI as a static SPA under /ops-gui."""
    if not OPS_GUI_DIST.exists():
        return jsonify({
            'success': False,
            'message': 'Ops GUI build not found. Run npm run build:ops-gui in API Handler Interactive GUI.'
        }), 404

    if asset_path:
        target = OPS_GUI_DIST / asset_path
        if target.exists() and target.is_file():
            return send_from_directory(str(OPS_GUI_DIST), asset_path)

    return send_from_directory(str(OPS_GUI_DIST), 'index.html')


@app.route('/customer-portal', methods=['GET'])
@app.route('/customer-portal/', methods=['GET'])
@app.route('/customer-portal/<path:asset_path>', methods=['GET'])
def customer_portal(asset_path=''):
    """Serve the built customer portal SPA under /customer-portal."""
    if not CUSTOMER_PORTAL_DIST.exists():
        return jsonify({
            'success': False,
            'message': 'Customer portal build not found. Build the customer-portal app first.'
        }), 404

    if asset_path:
        target = CUSTOMER_PORTAL_DIST / asset_path
        if target.exists() and target.is_file():
            return send_from_directory(str(CUSTOMER_PORTAL_DIST), asset_path)

    return send_from_directory(str(CUSTOMER_PORTAL_DIST), 'index.html')


@app.route('/assets/email-splash.png', methods=['GET'])
def email_splash_asset():
    """Serve the email splash image from local disk via HTTP.

    This route exists so email clients can load a stable URL instead of relying
    on data-URI image support, which is inconsistent in mobile inbox apps.
    """
    splash_path = _resolve_email_splash_path()
    if not splash_path.exists() or not splash_path.is_file():
        return jsonify({
            'success': False,
            'message': f'Email splash image not found: {splash_path}'
        }), 404
    return send_from_directory(str(splash_path.parent), splash_path.name)

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
