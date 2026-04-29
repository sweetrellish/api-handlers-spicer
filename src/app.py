"""Flask entrypoint for receiving CompanyCam webhooks and syncing to MarketSharp."""

from flask import Flask, request, jsonify
import logging
from src.webhook_handler import WebhookHandler
from src.config import Config
from src.security import IdempotencyStore, extract_event_id, verify_webhook_auth

app = Flask(__name__)
handler = WebhookHandler()
idempotency_store = IdempotencyStore(Config.IDEMPOTENCY_DB_PATH)

# Keep logs structured enough for grep/tail when running under systemd or Docker.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

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
    Test endpoint to send a sample webhook event
    
    Useful for testing without CompanyCam sending real events
    """
    try:
        # Local-only sanity endpoint; this bypasses webhook auth intentionally.
        test_event = {
            'type': 'comment.created',
            'data': {
                'id': 'test-comment-123',
                'text': 'This is a test comment',
                'project_id': 'test-project-123',
                'user': {'name': 'Test User'}
            }
        }
        
        result = handler.process_comment_event(test_event)
        return jsonify(result), (200 if result['success'] else 400)
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error in test: {str(e)}'
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
