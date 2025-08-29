"""
news formatter data formatter.
"""

from typing import Dict, List


def format_news_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for news
    return {
        "source": "news",
        "data": raw_data,
        "formatted": True,
    }
