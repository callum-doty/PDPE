"""
Google data formatter.
"""

from typing import Dict, List


def format_google_places_data(raw_data: Dict) -> List[Dict]:
    """Format Google Places API response into standardized format."""
    formatted_places = []

    if not raw_data.get("results"):
        return formatted_places

    for place in raw_data["results"]:
        formatted_place = {
            "source": "google_places",
            "place_id": place.get("place_id"),
            "name": place.get("name"),
            "rating": place.get("rating", 0),
            "types": place.get("types", []),
            "price_level": place.get("price_level"),
            "user_ratings_total": place.get("user_ratings_total", 0),
            "formatted_address": place.get("formatted_address"),
            "geometry": place.get("geometry", {}),
        }
        formatted_places.append(formatted_place)

    return formatted_places
