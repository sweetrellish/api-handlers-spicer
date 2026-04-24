"""Centralized environment-backed application settings."""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Runtime configuration loaded from environment variables."""

    # Flask runtime configuration.
    FLASK_PORT = int(os.getenv('FLASK_PORT', 5001))
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'

    # CompanyCam API and webhook configuration.
    COMPANYCAM_WEBHOOK_TOKEN = os.getenv('COMPANYCAM_WEBHOOK_TOKEN')
    COMPANYCAM_WEBHOOK_ID = os.getenv('COMPANYCAM_WEBHOOK_ID', '')
    COMPANYCAM_BASE_URL = 'https://api.companycam.com'
    COMPANYCAM_WEBHOOK_SECRET = os.getenv('COMPANYCAM_WEBHOOK_SECRET', '')
    WEBHOOK_SIGNATURE_HEADER = os.getenv('WEBHOOK_SIGNATURE_HEADER', 'X-CompanyCam-Signature')
    WEBHOOK_TOKEN_FIELD = os.getenv('WEBHOOK_TOKEN_FIELD', 'token')
    WEBHOOK_AUTH_REQUIRED = os.getenv('WEBHOOK_AUTH_REQUIRED', 'True').lower() == 'true'

    # MarketSharp integration mode:
    # - auto: infer mode from provided credentials
    # - odata_readonly: read customer data and queue comments
    # - odata_write: create notes via OData Notes entity
    # - rest_write: attempt direct write API calls (requires partner REST access)
    MARKETSHARP_MODE = os.getenv('MARKETSHARP_MODE', 'auto').lower()

    # MarketSharp OData credentials from API Maintenance page.
    MARKETSHARP_COMPANY_ID = os.getenv('MARKETSHARP_COMPANY_ID', '')
    MARKETSHARP_USER_KEY = os.getenv('MARKETSHARP_USER_KEY', '')
    MARKETSHARP_SECRET_KEY = os.getenv('MARKETSHARP_SECRET_KEY', '')
    MARKETSHARP_ODATA_URL = os.getenv(
        'MARKETSHARP_ODATA_URL',
        'https://api4.marketsharpm.com/WcfDataService.svc',
    )
    MARKETSHARP_NOTE_CONTACT_TYPE = os.getenv('MARKETSHARP_NOTE_CONTACT_TYPE', 'Contact')

    # Optional REST write settings (partner-only).
    MARKETSHARP_API_KEY = os.getenv('MARKETSHARP_API_KEY', '')
    MARKETSHARP_BASE_URL = os.getenv('MARKETSHARP_BASE_URL', '')

    # Idempotency persistence location.
    IDEMPOTENCY_DB_PATH = os.getenv('IDEMPOTENCY_DB_PATH', 'data/cc_webhook_dedupe.db')
    PENDING_QUEUE_DB_PATH = os.getenv('PENDING_QUEUE_DB_PATH', 'data/pending_comments.db')


def validate_config():
    """Fail fast when required credentials are missing."""
    required_vars = ['COMPANYCAM_WEBHOOK_TOKEN']

    if Config.MARKETSHARP_MODE in ['odata_readonly', 'odata_write']:
        required_vars.extend([
            'MARKETSHARP_COMPANY_ID',
            'MARKETSHARP_USER_KEY',
            'MARKETSHARP_SECRET_KEY',
        ])
    elif Config.MARKETSHARP_MODE == 'rest_write':
        required_vars.extend(['MARKETSHARP_API_KEY', 'MARKETSHARP_BASE_URL'])
    elif Config.MARKETSHARP_MODE == 'auto':
        has_odata = all([
            Config.MARKETSHARP_COMPANY_ID,
            Config.MARKETSHARP_USER_KEY,
            Config.MARKETSHARP_SECRET_KEY,
        ])
        has_rest = all([
            Config.MARKETSHARP_API_KEY,
            Config.MARKETSHARP_BASE_URL,
        ])
        if not has_odata and not has_rest:
            required_vars.extend([
                'MARKETSHARP_COMPANY_ID or MARKETSHARP_API_KEY',
                'MARKETSHARP_USER_KEY or MARKETSHARP_BASE_URL',
                'MARKETSHARP_SECRET_KEY or MARKETSHARP_BASE_URL',
            ])
    else:
        raise ValueError(
            'Invalid MARKETSHARP_MODE. Use one of: auto, odata_readonly, odata_write, rest_write'
        )

    missing = [var for var in required_vars if not getattr(Config, var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

validate_config()
