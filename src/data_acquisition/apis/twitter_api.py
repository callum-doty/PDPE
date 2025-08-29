"""
Twitter API client.
"""

import requests
from typing import Dict


class TwitterClient:
    """Client for Twitter API v2."""

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token
        self.base_url = "https://api.twitter.com/2"
        self.headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }

    def search_tweets(self, query: str, max_results: int = 10) -> Dict:
        """Search for tweets."""
        url = f"{self.base_url}/tweets/search/recent"
        params = {
            "query": query,
            "max_results": max_results,
            "tweet.fields": "created_at,public_metrics,context_annotations",
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_user_tweets(self, user_id: str) -> Dict:
        """Get tweets from a specific user."""
        url = f"{self.base_url}/users/{user_id}/tweets"
        params = {
            "tweet.fields": "created_at,public_metrics",
        }

        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
