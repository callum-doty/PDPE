"""
Ticketmaster API client.
"""

import requests
from typing import Dict


class TicketmasterClient:
    """Client for Ticketmaster Discovery API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://app.ticketmaster.com/discovery/v2"

    def search_events(self, city: str, radius: int = 25) -> Dict:
        """Search for events in a city."""
        url = f"{self.base_url}/events.json"
        params = {
            "apikey": self.api_key,
            "city": city,
            "radius": radius,
            "unit": "miles",
            "sort": "date,asc",
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_venues(self, city: str) -> Dict:
        """Get venues in a city."""
        url = f"{self.base_url}/venues.json"
        params = {
            "apikey": self.api_key,
            "city": city,
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
