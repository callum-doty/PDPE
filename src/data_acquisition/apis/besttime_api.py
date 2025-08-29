"""
BestTime API client for venue popularity data.
"""

import requests
from typing import Dict


class BestTimeClient:
    """Client for BestTime API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://besttime.app/api/v1"
        self.headers = {
            "Content-Type": "application/json",
        }

    def get_venue_popularity(self, venue_id: str) -> Dict:
        """Get venue popularity data."""
        url = f"{self.base_url}/venues/{venue_id}/popularity"
        params = {
            "api_key": self.api_key,
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def search_venues(self, query: str, lat: float, lon: float) -> Dict:
        """Search for venues near coordinates."""
        url = f"{self.base_url}/venues/search"
        params = {
            "api_key": self.api_key,
            "q": query,
            "lat": lat,
            "lng": lon,
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
