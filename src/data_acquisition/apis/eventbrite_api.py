"""
Eventbrite API client.
"""

import requests
from typing import Dict
from datetime import datetime, timedelta, timezone


class EventbriteClient:
    """Client for Eventbrite API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.eventbriteapi.com/v3"
        self.headers = {
            "Content-Type": "application/json",
        }

    def search_events(
        self, city: str, within_days: int = 7, radius: str = "15mi"
    ) -> Dict:
        """Search for events in a city within a date range."""
        start_date = datetime.now(timezone.utc)
        end_date = start_date + timedelta(days=within_days)

        url = f"{self.base_url}/events/search/"
        params = {
            "token": self.api_key,  # Use token as query parameter instead of Bearer header
            "location.address": city,
            "location.within": radius,
            "start_date.range_start": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_date.range_end": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expand": "venue,category",
            "sort_by": "date",
            "page_size": 20,
            "status": "live",
        }

        print(f"Making Eventbrite API request to: {url}")
        print(f"Parameters: {params}")

        response = requests.get(url, headers=self.headers, params=params, timeout=15)

        print(f"Response status: {response.status_code}")
        if response.status_code != 200:
            print(f"Response content: {response.text}")

        response.raise_for_status()
        return response.json()

    def _make_request(self, url: str, params: Dict) -> Dict:
        """Make a generic API request."""
        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
