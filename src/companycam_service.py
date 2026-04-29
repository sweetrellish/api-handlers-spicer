"""HTTP client wrapper for CompanyCam API operations used by this integration."""

import logging
import requests
from config import Config


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

    def list_recent_projects(self, limit=50):
        """Fetch the most recently active projects (default: 50)."""
        try:
            params = {'order': 'desc', 'sort': 'updated_at', 'per_page': limit}
            response = requests.get(
                f'{self.base_url}/v2/projects',
                headers=self.headers,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json() if response.json() else []
        except requests.RequestException as e:
            logging.exception('Error fetching recent CompanyCam projects: %s', str(e))
            return []

    def list_project_comments(self, project_id):
        """Fetch all comments for a given project."""
        try:
            comments = []
            page = 1
            while True:
                params = {'page': page, 'per_page': 100}
                response = requests.get(
                    f'{self.base_url}/v2/projects/{project_id}/comments',
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json() or []
                if not data:
                    break
                comments.extend(data)
                if len(data) < 100:
                    break
                page += 1
            return comments
        except requests.RequestException as e:
            logging.exception('Error fetching comments for project %s: %s', project_id, str(e))
            return []

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

