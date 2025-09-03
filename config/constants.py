"""
Psychographic scoring constants and weights for the PPM system.
Based on the refined approach for predicting career-driven, competent, fun individuals.
"""

# Geographic boundaries for Kansas City
KC_BOUNDING_BOX = {
    "north": 39.3209,
    "south": 38.9517,
    "east": -94.3461,
    "west": -94.7417,
}

# Downtown Kansas City coordinates (for distance calculations)
KC_DOWNTOWN = {"lat": 39.0997, "lng": -94.5786}

# Psychographic scoring weights for target demographic
PSYCHOGRAPHIC_WEIGHTS = {
    "career_driven": {
        "venue_categories": {
            "coworking_space": 0.95,
            "business_center": 0.90,
            "networking_venue": 0.85,
            "conference_center": 0.85,
            "upscale_restaurant": 0.75,
            "coffee_shop": 0.70,
            "hotel": 0.65,
            "office_building": 0.60,
            "bank": 0.55,
            "library": 0.50,
        },
        "event_types": {
            "professional_networking": 0.95,
            "business_conference": 0.90,
            "startup_event": 0.85,
            "career_fair": 0.80,
            "workshop": 0.75,
            "seminar": 0.75,
            "trade_show": 0.70,
            "meetup": 0.65,
        },
        "keywords": [
            "career",
            "professional",
            "networking",
            "business",
            "startup",
            "entrepreneur",
            "leadership",
            "management",
            "conference",
            "workshop",
        ],
    },
    "competent": {
        "demographic_indicators": {
            "education_bachelors_plus": 0.90,
            "education_graduate": 0.95,
            "income_above_median": 0.80,
            "professional_occupation": 0.85,
            "management_occupation": 0.90,
        },
        "venue_categories": {
            "university": 0.90,
            "museum": 0.80,
            "library": 0.85,
            "cultural_center": 0.75,
            "art_gallery": 0.70,
        },
        "keywords": [
            "education",
            "skilled",
            "expert",
            "qualified",
            "experienced",
            "certified",
            "degree",
            "training",
            "development",
        ],
    },
    "fun": {
        "venue_categories": {
            "entertainment": 0.90,
            "nightlife": 0.85,
            "restaurant": 0.80,
            "bar": 0.85,
            "recreational": 0.75,
            "sports_venue": 0.70,
            "park": 0.65,
            "shopping": 0.60,
            "movie_theater": 0.75,
            "music_venue": 0.90,
        },
        "event_types": {
            "music": 0.90,
            "food_drink": 0.85,
            "social": 0.80,
            "cultural": 0.75,
            "sports": 0.70,
            "festival": 0.85,
            "party": 0.80,
            "comedy": 0.75,
        },
        "keywords": [
            "fun",
            "entertainment",
            "social",
            "party",
            "music",
            "food",
            "drinks",
            "festival",
            "celebration",
            "nightlife",
            "comedy",
        ],
    },
}

# Event tag importance weights
EVENT_TAG_WEIGHTS = {
    "music": 0.9,
    "food": 0.8,
    "business": 0.7,
    "networking": 0.9,
    "professional": 0.8,
    "social": 0.7,
    "entertainment": 0.8,
    "cultural": 0.6,
    "educational": 0.7,
    "technology": 0.8,
    "startup": 0.9,
    "career": 0.9,
}

# Venue category scoring weights
VENUE_CATEGORY_SCORES = {
    "restaurant": 0.8,
    "bar": 0.9,
    "entertainment": 0.7,
    "retail": 0.6,
    "professional_services": 0.8,
    "coworking_space": 0.9,
    "coffee_shop": 0.7,
    "nightlife": 0.8,
    "cultural": 0.6,
    "recreational": 0.7,
}

# Time-based multipliers for psychographic relevance
TIME_MULTIPLIERS = {
    "career_driven": {
        "weekday_business_hours": 1.2,  # 9 AM - 6 PM weekdays
        "weekday_evening": 1.0,  # 6 PM - 10 PM weekdays
        "weekend_day": 0.7,  # 9 AM - 6 PM weekends
        "weekend_evening": 0.8,  # 6 PM - 10 PM weekends
        "late_night": 0.3,  # 10 PM - 6 AM
    },
    "fun": {
        "weekday_business_hours": 0.6,
        "weekday_evening": 1.1,
        "weekend_day": 1.0,
        "weekend_evening": 1.3,
        "late_night": 1.2,
    },
}

# Weather impact multipliers
WEATHER_MULTIPLIERS = {
    "temperature": {
        "optimal_range": (65, 80),  # Fahrenheit
        "penalty_per_degree": 0.01,  # Penalty for each degree outside optimal
    },
    "rain_probability": {
        "indoor_venues": 0.1,  # Minimal impact on indoor venues
        "outdoor_venues": 0.8,  # High impact on outdoor venues
        "threshold": 0.3,  # Rain probability threshold
    },
    "severe_weather": {"multiplier": 0.3},  # Severe weather reduces all activity
}

# Distance decay parameters
DISTANCE_DECAY = {
    "downtown_reference": KC_DOWNTOWN,
    "max_distance_km": 50,
    "decay_rate": 0.1,  # Exponential decay rate
}

# Feature engineering parameters
FEATURE_PARAMS = {
    "grid_resolution_meters": 500,
    "spatial_buffer_meters": 1000,
    "temporal_window_hours": 24,
    "min_venue_rating": 1.0,
    "max_venue_rating": 5.0,
    "confidence_threshold": 0.7,
}

# Model selection thresholds
MODEL_SELECTION = {
    "small_dataset_threshold": 1000,  # Use XGBoost for < 1k samples
    "large_dataset_threshold": 100000,  # Use Neural Networks for > 100k samples
    "graph_relationship_threshold": 0.3,  # Use GNN if venue-event correlation > 0.3
    "uncertainty_threshold": 0.8,  # Use Bayesian if uncertainty needed
}

# API rate limits and quotas
API_LIMITS = {
    "google_places": {"requests_per_day": 100000, "requests_per_second": 50},
    "eventbrite": {"requests_per_hour": 1000, "requests_per_second": 5},
    "twitter": {"requests_per_15min": 300, "requests_per_second": 1},
    "weather": {"requests_per_day": 1000, "requests_per_second": 10},
}

# Data quality thresholds
DATA_QUALITY = {
    "min_venue_reviews": 5,
    "min_event_attendance": 10,
    "max_missing_features": 0.2,  # 20% missing features allowed
    "min_label_confidence": 0.6,
    "outlier_z_threshold": 3.0,
}

# Caching configuration
CACHE_CONFIG = {
    "feature_cache_hours": 1,
    "prediction_cache_minutes": 15,
    "venue_cache_hours": 24,
    "weather_cache_minutes": 30,
}

# Psychographic layer configurations
CUSTOM_LAYERS = {
    "college_layer": {
        "universities": [
            {
                "name": "University of Missouri-Kansas City",
                "lat": 39.0347,
                "lng": -94.5783,
                "weight": 1.0,
            },
            {
                "name": "Kansas City Art Institute",
                "lat": 39.0438,
                "lng": -94.5889,
                "weight": 0.8,
            },
            {
                "name": "Rockhurst University",
                "lat": 39.0358,
                "lng": -94.5264,
                "weight": 0.9,
            },
            {
                "name": "Avila University",
                "lat": 38.9892,
                "lng": -94.6258,
                "weight": 0.7,
            },
        ],
        "influence_radius_km": 5.0,
        "decay_rate": 0.2,
    },
    "spending_propensity": {
        "high_income_multiplier": 1.2,
        "education_multiplier": 1.1,
        "age_optimal_range": (25, 40),
        "professional_occupation_bonus": 0.15,
    },
}

# Ensemble model weights (will be learned during training)
ENSEMBLE_WEIGHTS = {"xgboost": 0.4, "neural": 0.3, "bayesian": 0.2, "graph": 0.1}

# Performance monitoring thresholds
MONITORING_THRESHOLDS = {
    "min_auc_roc": 0.75,
    "min_precision_at_k": 0.70,
    "max_prediction_latency_ms": 200,
    "max_batch_latency_s": 2.0,
    "min_uptime_percent": 99.9,
}
