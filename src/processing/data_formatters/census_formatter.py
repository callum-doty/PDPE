"""
census formatter data formatter.
"""

from typing import Dict, List


def format_census_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for census
    return {
        "source": "census",
        "data": raw_data,
        "formatted": True,
    }
