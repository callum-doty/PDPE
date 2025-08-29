"""
Google APIs client (Places, etc.).
"""

import requests
from typing import Dict


class GooglePlacesClient:
    """Client for Google Places API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://maps.googleapis.com/maps/api/place"

    def text_search(self, query: str) -> Dict:
        """Search for places using text query."""
        url = f"{self.base_url}/textsearch/json"
        params = {
            "query": query,
            "key": self.api_key,
            "fields": "place_id,name,rating,types,price_level,user_ratings_total",
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
