"""
facebook formatter data formatter.
"""

from typing import Dict, List


def format_facebook_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for facebook
    return {
        "source": "facebook",
        "data": raw_data,
        "formatted": True,
    }
