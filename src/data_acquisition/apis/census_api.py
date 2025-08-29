"""
Census API client for demographic data.
"""

import requests
from typing import Dict, List


class CensusClient:
    """Client for US Census API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key
        self.base_url = "https://api.census.gov/data"

    def get_demographic_data(
        self, year: int, geography: str, variables: List[str]
    ) -> Dict:
        """Get demographic data for specified geography."""
        url = f"{self.base_url}/{year}/acs/acs5"
        params = {
            "get": ",".join(variables),
            "for": geography,
        }

        if self.api_key:
            params["key"] = self.api_key

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_population_data(self, state: str, county: str = None) -> Dict:
        """Get population data for state/county."""
        geography = f"state:{state}"
        if county:
            geography += f",county:{county}"

        variables = ["B01003_001E"]  # Total population
        return self.get_demographic_data(2021, geography, variables)
