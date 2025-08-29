"""
Eventbrite data formatter.
"""

from typing import List, Dict
from datetime import datetime


def format_eventbrite_events(raw_data: Dict) -> List[Dict]:
    """Format raw Eventbrite API response into standardized event format."""
    formatted_events = []

    if not raw_data.get("events"):
        return formatted_events

    for event in raw_data["events"]:
        try:
            # Extract basic event info
            event_data = {
                "source": "eventbrite",
                "external_id": event.get("id"),
                "name": event.get("name", {}).get("text", ""),
                "description": event.get("description", {}).get("text", ""),
                "url": event.get("url", ""),
                "start_time": event.get("start", {}).get("utc"),
                "end_time": event.get("end", {}).get("utc"),
                "timezone": event.get("start", {}).get("timezone"),
                "is_free": event.get("is_free", False),
                "currency": event.get("currency"),
            }

            # Extract venue information
            venue = event.get("venue")
            if venue:
                event_data["venue"] = {
                    "id": venue.get("id"),
                    "name": venue.get("name"),
                    "address": venue.get("address", {}).get(
                        "localized_address_display"
                    ),
                    "latitude": venue.get("latitude"),
                    "longitude": venue.get("longitude"),
                }

            # Extract category information
            category = event.get("category")
            if category:
                event_data["category"] = {
                    "id": category.get("id"),
                    "name": category.get("name"),
                    "short_name": category.get("short_name"),
                }

            # Extract organizer information
            organizer = event.get("organizer")
            if organizer:
                event_data["organizer"] = {
                    "id": organizer.get("id"),
                    "name": organizer.get("name"),
                    "description": organizer.get("description", {}).get("text", ""),
                }

            formatted_events.append(event_data)

        except Exception as e:
            print(
                f"Error formatting Eventbrite event {event.get('id', 'unknown')}: {e}"
            )
            continue

    return formatted_events


def format_eventbrite_venue(venue_data: Dict) -> Dict:
    """Format Eventbrite venue data."""
    return {
        "id": venue_data.get("id"),
        "name": venue_data.get("name"),
        "address": venue_data.get("address", {}).get("localized_address_display"),
        "city": venue_data.get("address", {}).get("city"),
        "region": venue_data.get("address", {}).get("region"),
        "country": venue_data.get("address", {}).get("country"),
        "postal_code": venue_data.get("address", {}).get("postal_code"),
        "latitude": venue_data.get("latitude"),
        "longitude": venue_data.get("longitude"),
        "capacity": venue_data.get("capacity"),
    }
