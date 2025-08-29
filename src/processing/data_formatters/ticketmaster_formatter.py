"""
ticketmaster formatter data formatter.
"""

from typing import Dict, List


def format_ticketmaster_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for ticketmaster
    return {
        "source": "ticketmaster",
        "data": raw_data,
        "formatted": True,
    }
