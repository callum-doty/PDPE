"""
News API client.
"""

import requests
from typing import Dict


class NewsApiClient:
    """Client for News API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2"

    def get_headlines(self, query: str, location: str = None) -> Dict:
        """Get news headlines."""
        url = f"{self.base_url}/everything"
        params = {
            "q": query,
            "apiKey": self.api_key,
            "sortBy": "publishedAt",
            "language": "en",
        }

        if location:
            params["q"] += f" AND {location}"

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
