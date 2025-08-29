# Trait mappings and constants for the whereabouts engine

# Kansas City bounding box (approximate)
KC_BOUNDING_BOX = {
    "north": 39.3209,
    "south": 38.9517,
    "east": -94.3554,
    "west": -94.7435,
}

# Venue category mappings for demographic scoring
VENUE_CATEGORY_SCORES = {
    "coworking": 4,
    "university": 4,
    "conference_center": 4,
    "brewery": 2,
    "concert_hall": 2,
    "sports_venue": 1,
    "other": 1,
}

# Event tag scoring weights
EVENT_TAG_WEIGHTS = {
    "career": 5,
    "networking": 5,
    "business": 5,
    "startup": 5,
    "tech": 5,
    "concert": 3,
    "festival": 3,
    "music": 3,
    "family": -3,
    "kids": -3,
    "teen": -3,
}

# Weather condition impacts
WEATHER_IMPACTS = {
    "sunny": 2,
    "cloudy": 0,
    "rain": -3,
    "snow": -2,
    "storm": -4,
}

# Google Places type mappings
GOOGLE_PLACES_CATEGORY_MAP = {
    "coworking_space": "coworking",
    "office": "coworking",
    "business": "coworking",
    "university": "university",
    "school": "university",
    "library": "university",
    "educational": "university",
    "night_club": "brewery",
    "bar": "brewery",
    "brewery": "brewery",
    "liquor_store": "brewery",
    "concert_hall": "concert_hall",
    "theater": "concert_hall",
    "performing_arts": "concert_hall",
    "music_venue": "concert_hall",
    "stadium": "sports_venue",
    "gym": "sports_venue",
    "sports_complex": "sports_venue",
    "bowling_alley": "sports_venue",
    "convention_center": "conference_center",
    "meeting_room": "conference_center",
    "conference": "conference_center",
}

# Grid settings for spatial analysis
GRID_CELL_SIZE_M = 500  # 500 meter grid cells
MIN_EVENTS_PER_CELL = 1  # Minimum events required for cell to be considered

# Bayesian fusion parameters
PRIOR_PROBABILITY = 0.1  # Base probability for any location
CONFIDENCE_THRESHOLD = 0.7  # Minimum confidence for high-probability areas
