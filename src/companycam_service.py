"""HTTP client wrapper for CompanyCam API operations used by this integration."""

import logging
import requests
from config.config import Config


class CompanyCamService:
    """Service for interacting with CompanyCam API"""

    def __init__(self):
        self.base_url = Config.COMPANYCAM_BASE_URL
        self.access_token = Config.COMPANYCAM_WEBHOOK_TOKEN
        self.timeout = 10
        self.headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_project_by_id(self, project_id):
        """Fetch CompanyCam project details by project id."""
        try:
            response = requests.get(
                f'{self.base_url}/v2/projects/{project_id}',
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.exception('Error fetching CompanyCam project %s: %s', project_id, str(e))
            return None

    def get_comment_details(self, comment_id):
        """Fetch CompanyCam comment details by comment id."""
        try:
            response = requests.get(
                f'{self.base_url}/v2/comments/{comment_id}',
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.exception('Error fetching CompanyCam comment %s: %s', comment_id, str(e))
            return None

    def get_photo_by_id(self, photo_id):
        """Fetch CompanyCam photo details by photo id."""
        try:
            response = requests.get(
                f'{self.base_url}/v2/photos/{photo_id}',
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.exception('Error fetching CompanyCam photo %s: %s', photo_id, str(e))
            return None
