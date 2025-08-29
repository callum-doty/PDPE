"""
twitter formatter data formatter.
"""

from typing import Dict, List


def format_twitter_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for twitter
    return {
        "source": "twitter",
        "data": raw_data,
        "formatted": True,
    }
