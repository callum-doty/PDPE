"""
Facebook API client.
"""

import requests
from typing import Dict


class FacebookClient:
    """Client for Facebook Graph API."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://graph.facebook.com/v18.0"

    def get_events(self, location: str) -> Dict:
        """Get events near location."""
        url = f"{self.base_url}/search"
        params = {
            "q": location,
            "type": "event",
            "access_token": self.access_token,
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
