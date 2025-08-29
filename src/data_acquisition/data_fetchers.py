"""
Functions to fetch data from each API and convert to internal models.
"""

from typing import List
from src.models.database import Event, Location
from src.data_acquisition.apis.eventbrite_api import EventbriteClient
from src.data_acquisition.apis.google_apis import GooglePlacesClient
from src.data_acquisition.apis.weather_api import WeatherClient
from config import settings
import datetime
import random


def fetch_eventbrite_events(city: str, within_days: int = 7) -> List[Event]:
    """
    Fetch real events from Eventbrite API for the specified city.

    Note: Eventbrite has restricted their public event search API.
    Personal API keys no longer have access to the events/search endpoint.
    This function will use realistic stub data for demonstration purposes.
    """
    api_key = settings.EVENTBRITE_API_KEY
    if not api_key or api_key == "INSERT_YOUR_KEY":
        print("Warning: No valid Eventbrite API key found, using stub data")
        return _get_stub_events()

    try:
        client = EventbriteClient(api_key)

        # Test API connectivity first
        print("Testing Eventbrite API connectivity...")
        test_url = f"{client.base_url}/users/me/"
        test_params = {"token": api_key}
        test_response = client._make_request(test_url, test_params)

        if test_response:
            print(f"✓ API key valid for user: {test_response.get('name', 'Unknown')}")
            print("⚠️  Note: Eventbrite has restricted public event search access.")
            print("   Personal API keys cannot access the events/search endpoint.")
            print("   Using enhanced stub data for demonstration.")

        # Since the search endpoint is not available, return enhanced stub data
        return _get_enhanced_stub_events(city)

    except Exception as e:
        print(f"Eventbrite API error: {e}")
        print("Falling back to stub data")
        return _get_stub_events()


def fetch_google_place_for_location(name: str, address: str):
    """
    Enrich location data using Google Places API.
    Returns additional venue information like rating, place_type, etc.
    """
    api_key = settings.GOOGLE_PLACES_API_KEY
    if not api_key or api_key == "INSERT_YOUR_KEY":
        print("Warning: No valid Google Places API key found")
        return None

    try:
        client = GooglePlacesClient(api_key)
        data = client.text_search(f"{name} {address}")

        if data.get("results"):
            place = data["results"][0]  # Take the first (most relevant) result

            # Extract useful information
            place_info = {
                "place_id": place.get("place_id"),
                "rating": place.get("rating", 0),
                "types": place.get("types", []),
                "price_level": place.get("price_level"),
                "user_ratings_total": place.get("user_ratings_total", 0),
            }

            # Enhance venue category based on Google Places types
            enhanced_category = _enhance_category_from_google_types(place_info["types"])
            if enhanced_category:
                place_info["enhanced_category"] = enhanced_category

            return place_info

    except Exception as e:
        print(f"Google Places API error for {name}: {e}")
        return None

    return None


def fetch_weather_for_datetime(dt_iso: str, lat: float, lon: float):
    """
    Fetch weather data from National Weather Service API for given coordinates and time.
    """
    try:
        client = WeatherClient()
        forecast_data = client.get_forecast(lat, lon)

        # Get the current or nearest forecast period
        periods = forecast_data["properties"]["periods"]
        if periods:
            current_period = periods[0]  # Most recent forecast period

            # Parse weather condition
            detailed_forecast = current_period.get("detailedForecast", "").lower()
            short_forecast = current_period.get("shortForecast", "").lower()

            # Determine condition based on forecast text
            condition = "sunny"  # default
            precipitation = 0.0

            if any(
                word in detailed_forecast or word in short_forecast
                for word in ["rain", "shower", "drizzle", "storm"]
            ):
                condition = "rain"
                precipitation = 2.0  # Assume moderate rain
            elif any(
                word in detailed_forecast or word in short_forecast
                for word in ["cloud", "overcast", "partly"]
            ):
                condition = "cloudy"
            elif any(
                word in detailed_forecast or word in short_forecast
                for word in ["clear", "sunny", "fair"]
            ):
                condition = "sunny"

            temperature = current_period.get("temperature", 70)

            return {
                "condition": condition,
                "temperature": float(temperature),
                "precipitation": precipitation,
            }

    except Exception as e:
        print(f"Weather API error: {e}")
        # Fallback to reasonable defaults for Kansas City
        return {
            "condition": "sunny",
            "temperature": 72.0,
            "precipitation": 0.0,
        }

    # Fallback if no data
    return {
        "condition": "sunny",
        "temperature": 72.0,
        "precipitation": 0.0,
    }


def _get_stub_events() -> List[Event]:
    """Fallback stub events for testing when API is unavailable."""
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    sample_locations = [
        Location(
            "WorkTogether Cowork",
            "100 Main St, KC",
            39.0997,
            -94.5786,
            "coworking",
            base_score=3,
        ),
        Location(
            "Crossroads Brewery",
            "200 Cross St, KC",
            39.0989,
            -94.5795,
            "brewery",
            base_score=2,
        ),
        Location(
            "KC Concert Hall",
            "300 Concert Ave, KC",
            39.1003,
            -94.5780,
            "concert_hall",
            base_score=2,
        ),
    ]
    sample_tags = [
        ["networking", "career"],
        ["concert", "indie"],
        ["festival", "outdoor"],
    ]

    events = []
    for i in range(3):
        start = now + timedelta(days=i, hours=18)
        end = start + timedelta(hours=3)
        ev = Event(
            source="eventbrite",
            external_id=f"eb_{i}",
            name=f"Sample Event {i}",
            description=f"A test event #{i}",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            location=sample_locations[i],
            category="business" if i == 0 else "music",
            tags=sample_tags[i],
        )
        events.append(ev)
    return events


def _get_enhanced_stub_events(city: str) -> List[Event]:
    """Enhanced stub events with more realistic data for the specified city."""
    from datetime import datetime, timedelta, timezone
    import random

    now = datetime.now(timezone.utc)

    # Enhanced locations for Kansas City
    if "kansas city" in city.lower() or "kc" in city.lower():
        sample_locations = [
            Location(
                "Union Station",
                "30 W Pershing Rd, Kansas City, MO",
                39.0844,
                -94.5822,
                "conference_center",
                base_score=4,
            ),
            Location(
                "Crossroads Arts District",
                "1815 Wyandotte St, Kansas City, MO",
                39.0917,
                -94.5833,
                "arts_venue",
                base_score=3,
            ),
            Location(
                "Power & Light District",
                "1200 Main St, Kansas City, MO",
                39.1000,
                -94.5833,
                "entertainment",
                base_score=3,
            ),
            Location(
                "Crown Center",
                "2450 Grand Blvd, Kansas City, MO",
                39.0889,
                -94.5814,
                "conference_center",
                base_score=3,
            ),
            Location(
                "Westport",
                "4000 Pennsylvania Ave, Kansas City, MO",
                39.0528,
                -94.5958,
                "brewery",
                base_score=2,
            ),
            Location(
                "River Market",
                "101 Grand Blvd, Kansas City, MO",
                39.1111,
                -94.5833,
                "market",
                base_score=2,
            ),
            Location(
                "18th & Vine District",
                "1616 E 18th St, Kansas City, MO",
                39.0833,
                -94.5500,
                "music_venue",
                base_score=3,
            ),
            Location(
                "Country Club Plaza",
                "4750 Broadway, Kansas City, MO",
                39.0417,
                -94.5917,
                "shopping",
                base_score=2,
            ),
        ]

        event_templates = [
            (
                "Kansas City Tech Meetup",
                "Monthly gathering of local developers and entrepreneurs",
                "tech",
                ["networking", "tech", "career"],
            ),
            (
                "First Friday Art Walk",
                "Monthly art gallery walk in the Crossroads",
                "arts",
                ["art", "culture", "social"],
            ),
            (
                "KC Startup Weekend",
                "54-hour event for aspiring entrepreneurs",
                "business",
                ["startup", "networking", "business"],
            ),
            (
                "Jazz in the Park",
                "Live jazz performance in the historic jazz district",
                "music",
                ["jazz", "music", "outdoor"],
            ),
            (
                "Farm to Table Dinner",
                "Local chef showcase with regional ingredients",
                "food",
                ["food", "local", "dining"],
            ),
            (
                "KC Beer Festival",
                "Annual celebration of local craft breweries",
                "food",
                ["beer", "festival", "local"],
            ),
            (
                "Digital Marketing Workshop",
                "Learn modern marketing strategies",
                "business",
                ["marketing", "education", "professional"],
            ),
            (
                "Indie Film Screening",
                "Independent film showcase and discussion",
                "film",
                ["film", "arts", "culture"],
            ),
        ]
    else:
        # Generic locations for other cities
        sample_locations = [
            Location(
                "Downtown Convention Center",
                f"100 Main St, {city}",
                39.0997,
                -94.5786,
                "conference_center",
                base_score=3,
            ),
            Location(
                "Arts District Gallery",
                f"200 Art Ave, {city}",
                39.0989,
                -94.5795,
                "arts_venue",
                base_score=2,
            ),
            Location(
                "Riverside Brewery",
                f"300 River St, {city}",
                39.1003,
                -94.5780,
                "brewery",
                base_score=2,
            ),
            Location(
                "University Campus",
                f"400 College Dr, {city}",
                39.0950,
                -94.5750,
                "university",
                base_score=3,
            ),
            Location(
                "City Park Pavilion",
                f"500 Park Blvd, {city}",
                39.1050,
                -94.5800,
                "outdoor_venue",
                base_score=2,
            ),
        ]

        event_templates = [
            (
                "Tech Networking Night",
                "Monthly meetup for local tech professionals",
                "tech",
                ["networking", "tech"],
            ),
            (
                "Art Gallery Opening",
                "New exhibition opening reception",
                "arts",
                ["art", "culture"],
            ),
            (
                "Craft Beer Tasting",
                "Sample local and regional craft beers",
                "food",
                ["beer", "tasting"],
            ),
            (
                "Business Workshop",
                "Professional development seminar",
                "business",
                ["business", "education"],
            ),
            (
                "Music in the Park",
                "Outdoor concert series",
                "music",
                ["music", "outdoor"],
            ),
        ]

    events = []
    num_events = random.randint(8, 15)  # Generate 8-15 events

    for i in range(num_events):
        # Select random event template and location
        event_name, description, category, tags = random.choice(event_templates)
        location = random.choice(sample_locations)

        # Add some variation to event names
        if i > 0:
            variations = [
                f"{event_name} #{i+1}",
                f"Weekly {event_name}",
                f"{event_name} - Special Edition",
                f"Annual {event_name}",
                event_name,  # Keep some unchanged
            ]
            event_name = random.choice(variations)

        # Generate realistic start times (next 2 weeks, various times)
        days_ahead = random.randint(1, 14)
        hour = random.choice([10, 12, 14, 17, 18, 19, 20])  # Common event times
        start = now + timedelta(
            days=days_ahead, hours=hour - now.hour, minutes=random.choice([0, 30])
        )
        end = start + timedelta(hours=random.randint(2, 4))

        event = Event(
            source="eventbrite_enhanced",
            external_id=f"enhanced_{i}",
            name=event_name,
            description=f"{description} - Enhanced demo event for {city}",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            location=location,
            category=category,
            tags=tags,
        )
        events.append(event)

    print(f"Generated {len(events)} enhanced demo events for {city}")
    return events


def _categorize_venue(venue_name: str, description: str) -> str:
    """Categorize venue based on name and description."""
    text = f"{venue_name} {description}".lower()

    if any(
        word in text for word in ["cowork", "office", "workspace", "business center"]
    ):
        return "coworking"
    elif any(word in text for word in ["brewery", "bar", "pub", "tavern", "beer"]):
        return "brewery"
    elif any(
        word in text for word in ["concert", "music", "theater", "auditorium", "hall"]
    ):
        return "concert_hall"
    elif any(word in text for word in ["university", "college", "school", "campus"]):
        return "university"
    elif any(word in text for word in ["sports", "stadium", "arena", "field"]):
        return "sports_venue"
    elif any(word in text for word in ["hotel", "conference", "convention", "meeting"]):
        return "conference_center"
    else:
        return "other"


def _extract_tags(name: str, description: str, category: str) -> List[str]:
    """Extract relevant tags from event name, description, and category."""
    text = f"{name} {description} {category}".lower()
    tags = []

    # Career/Business tags
    if any(
        word in text
        for word in [
            "networking",
            "business",
            "career",
            "professional",
            "startup",
            "entrepreneur",
        ]
    ):
        tags.extend(["networking", "career"])

    # Tech tags
    if any(
        word in text
        for word in [
            "tech",
            "technology",
            "software",
            "coding",
            "programming",
            "developer",
        ]
    ):
        tags.append("tech")

    # Music/Entertainment tags
    if any(
        word in text for word in ["music", "concert", "band", "performance", "show"]
    ):
        tags.append("music")

    # Festival/Outdoor tags
    if any(word in text for word in ["festival", "outdoor", "park", "street"]):
        tags.append("festival")

    # Educational tags
    if any(
        word in text
        for word in ["workshop", "seminar", "training", "education", "learn"]
    ):
        tags.append("education")

    # Social tags
    if any(word in text for word in ["meetup", "social", "community", "group"]):
        tags.append("social")

    return list(set(tags))  # Remove duplicates


def _get_base_score_for_category(category: str) -> int:
    """Get base demographic score for venue category."""
    from config.constants import VENUE_CATEGORY_SCORES

    return VENUE_CATEGORY_SCORES.get(category, 1)


def _enhance_category_from_google_types(types: List[str]) -> str:
    """
    Enhance venue categorization using Google Places types.
    """
    from config.constants import GOOGLE_PLACES_CATEGORY_MAP

    types_lower = [t.lower() for t in types]

    for google_type in types_lower:
        if google_type in GOOGLE_PLACES_CATEGORY_MAP:
            return GOOGLE_PLACES_CATEGORY_MAP[google_type]

    return None  # No enhancement needed
