"""
Weather API client (National Weather Service).
"""

import requests
from typing import Dict


class WeatherClient:
    """Client for National Weather Service API."""

    def __init__(self):
        self.base_url = "https://api.weather.gov"
        self.headers = {
            "User-Agent": "PsychoDemographicEngine/1.0 (contact@example.com)"
        }

    def get_forecast(self, lat: float, lon: float) -> Dict:
        """Get weather forecast for coordinates."""
        # First, get the forecast office and grid coordinates
        points_url = f"{self.base_url}/points/{lat},{lon}"
        points_response = requests.get(points_url, headers=self.headers, timeout=10)
        points_response.raise_for_status()
        points_data = points_response.json()

        # Get current conditions from the forecast
        forecast_url = points_data["properties"]["forecast"]
        forecast_response = requests.get(forecast_url, headers=self.headers, timeout=10)
        forecast_response.raise_for_status()
        return forecast_response.json()
