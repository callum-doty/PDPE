"""
universities formatter data formatter.
"""

from typing import Dict, List


def format_universities_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for universities
    return {
        "source": "universities",
        "data": raw_data,
        "formatted": True,
    }
