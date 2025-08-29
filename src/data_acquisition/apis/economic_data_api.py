"""
Economic Data API client.
"""

import requests
from typing import Dict


class EconomicDataClient:
    """Client for economic data APIs (FRED, etc.)."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred"

    def get_series_data(self, series_id: str) -> Dict:
        """Get economic time series data."""
        url = f"{self.base_url}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
