"""
besttime formatter data formatter.
"""

from typing import Dict, List


def format_besttime_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for besttime
    return {
        "source": "besttime",
        "data": raw_data,
        "formatted": True,
    }
