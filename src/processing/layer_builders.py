"""
Functions to build probability layers from event and demographic data.
"""

from typing import List, Dict
from ..models.database import Event
from config.constants import EVENT_TAG_WEIGHTS, VENUE_CATEGORY_SCORES, WEATHER_IMPACTS
import datetime


def calculate_demographic_score(category: str, google_data: dict = None) -> int:
    """
    Calculate demographic score based on venue category and Google Places data.
    Higher scores indicate better alignment with target demographics.
    """
    base_score = VENUE_CATEGORY_SCORES.get(category.lower(), 1)

    # Enhance score with Google Places data if available
    if google_data:
        # Boost score for highly rated venues (4.0+ rating)
        rating = google_data.get("rating", 0)
        if rating >= 4.5:
            base_score += 2
        elif rating >= 4.0:
            base_score += 1
        elif rating < 3.0 and rating > 0:
            base_score -= 1

        # Consider popularity (number of reviews)
        reviews = google_data.get("user_ratings_total", 0)
        if reviews > 100:
            base_score += 1
        elif reviews > 500:
            base_score += 2

        # Price level consideration (higher price often correlates with professional venues)
        price_level = google_data.get("price_level")
        if price_level == 3 or price_level == 4:  # Expensive/Very Expensive
            base_score += 1

    return max(0, base_score)  # Ensure non-negative score


def calculate_event_score(tags: List[str]) -> int:
    """Calculate event score based on tags."""
    tags_lower = [t.lower() for t in tags]
    score = 0

    for tag in tags_lower:
        if tag in EVENT_TAG_WEIGHTS:
            score += EVENT_TAG_WEIGHTS[tag]

    return score


def calculate_weather_score(weather: dict, event_category: str) -> int:
    """Calculate weather impact score."""
    condition = weather.get("condition", "").lower()
    base_score = WEATHER_IMPACTS.get(condition, 0)

    # Adjust for outdoor events
    if event_category in ("outdoor", "festival") or event_category == "music":
        if condition == "rain":
            return -3
        elif condition == "sunny":
            return 2

    return base_score


def score_event(event: Event, weather_at_event: dict, google_data: dict = None) -> dict:
    """
    Score an event based on demographic, event, and weather factors.
    """
    demo = calculate_demographic_score(event.location.category, google_data)
    ev = calculate_event_score(event.tags)
    w = calculate_weather_score(weather_at_event, event.category)
    total = demo + ev + w

    return {
        "demographic_score": demo,
        "event_score": ev,
        "weather_score": w,
        "total_score": total,
    }


def build_demographic_layer(events: List[Event], google_data_map: Dict = None) -> Dict:
    """
    Build a demographic probability layer from events.
    Returns a dictionary mapping location coordinates to demographic scores.
    """
    demographic_layer = {}

    for event in events:
        coord_key = (event.location.latitude, event.location.longitude)
        google_data = (
            google_data_map.get(event.location.name) if google_data_map else None
        )

        demo_score = calculate_demographic_score(event.location.category, google_data)

        if coord_key in demographic_layer:
            # Average scores for same location
            demographic_layer[coord_key] = (
                demographic_layer[coord_key] + demo_score
            ) / 2
        else:
            demographic_layer[coord_key] = demo_score

    return demographic_layer


def build_event_activity_layer(events: List[Event]) -> Dict:
    """
    Build an event activity layer showing event density and quality.
    Returns a dictionary mapping location coordinates to activity scores.
    """
    activity_layer = {}

    for event in events:
        coord_key = (event.location.latitude, event.location.longitude)
        event_score = calculate_event_score(event.tags)

        if coord_key in activity_layer:
            # Sum event scores for same location (more events = higher activity)
            activity_layer[coord_key] += event_score
        else:
            activity_layer[coord_key] = event_score

    return activity_layer


def build_weather_layer(events: List[Event], weather_data_map: Dict) -> Dict:
    """
    Build a weather impact layer.
    Returns a dictionary mapping location coordinates to weather impact scores.
    """
    weather_layer = {}

    for event in events:
        coord_key = (event.location.latitude, event.location.longitude)
        weather_data = weather_data_map.get(event.external_id, {})

        weather_score = calculate_weather_score(weather_data, event.category)

        if coord_key in weather_layer:
            # Average weather scores for same location
            weather_layer[coord_key] = (weather_layer[coord_key] + weather_score) / 2
        else:
            weather_layer[coord_key] = weather_score

    return weather_layer
