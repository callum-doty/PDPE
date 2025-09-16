#!/usr/bin/env python3
"""
Create a comprehensive map that layers ALL data from the database and APIs.
This includes venues, events, weather, traffic, social sentiment, economic data,
demographics, ML predictions, labels, and all scraped venue data.
"""

import sys
import os
import psycopg2
import json
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2.extras

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from src.backend.visualization.interactive_map_builder import InteractiveMapBuilder

    print("✓ Successfully imported InteractiveMapBuilder")
except ImportError as e:
    print(f"✗ Failed to import InteractiveMapBuilder: {e}")
    sys.exit(1)

# Load environment variables
load_dotenv()


def get_db_conn():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not found in environment variables")
        return None
    return psycopg2.connect(db_url)


def fetch_all_venues_data():
    """Fetch ALL venues from database including static and dynamic scraped venues."""
    print("🏢 Fetching all venues data (API + Static + Dynamic)...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get all venues with comprehensive data
    query = """
    SELECT 
        v.venue_id,
        v.external_id,
        v.provider,
        v.name,
        v.category,
        v.subcategory,
        v.price_tier,
        v.avg_rating,
        v.review_count,
        v.lat,
        v.lng,
        v.address,
        v.phone,
        v.website,
        v.hours_json,
        v.amenities,
        v.psychographic_relevance,
        v.created_at,
        v.updated_at,
        -- Get latest foot traffic data
        vt.visitors_count,
        vt.median_dwell_seconds,
        vt.visitors_change_24h,
        vt.visitors_change_7d,
        vt.peak_hour_ratio,
        -- Get latest traffic data
        td.congestion_score,
        td.travel_time_to_downtown,
        td.travel_time_index,
        -- Get latest social sentiment
        ss.mention_count,
        ss.positive_sentiment,
        ss.negative_sentiment,
        ss.neutral_sentiment,
        ss.engagement_score,
        ss.psychographic_keywords
    FROM venues v
    LEFT JOIN LATERAL (
        SELECT visitors_count, median_dwell_seconds, visitors_change_24h, 
               visitors_change_7d, peak_hour_ratio
        FROM venue_traffic 
        WHERE venue_id = v.venue_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) vt ON true
    LEFT JOIN LATERAL (
        SELECT congestion_score, travel_time_to_downtown, travel_time_index
        FROM traffic_data 
        WHERE venue_id = v.venue_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) td ON true
    LEFT JOIN LATERAL (
        SELECT mention_count, positive_sentiment, negative_sentiment,
               neutral_sentiment, engagement_score, psychographic_keywords
        FROM social_sentiment 
        WHERE venue_id = v.venue_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) ss ON true
    WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
    ORDER BY v.psychographic_relevance->>'career_driven' DESC NULLS LAST
    """

    cur.execute(query)
    venues_raw = cur.fetchall()

    venues = []
    for venue in venues_raw:
        venue_data = {
            "venue_id": str(venue[0]),
            "external_id": venue[1],
            "provider": venue[2] or "unknown",
            "name": venue[3] or "Unknown Venue",
            "category": venue[4] or "unknown",
            "subcategory": venue[5],
            "price_tier": venue[6],
            "avg_rating": venue[7],
            "review_count": venue[8],
            "latitude": float(venue[9]),
            "longitude": float(venue[10]),
            "address": venue[11],
            "phone": venue[12],
            "website": venue[13],
            "hours_json": venue[14],
            "amenities": venue[15] or [],
            "psychographic_relevance": venue[16] or {},
            "created_at": venue[17],
            "updated_at": venue[18],
            # Traffic data
            "visitors_count": venue[19],
            "median_dwell_seconds": venue[20],
            "visitors_change_24h": venue[21],
            "visitors_change_7d": venue[22],
            "peak_hour_ratio": venue[23],
            # Congestion data
            "congestion_score": venue[24],
            "travel_time_to_downtown": venue[25],
            "travel_time_index": venue[26],
            # Social sentiment data
            "mention_count": venue[27],
            "positive_sentiment": venue[28],
            "negative_sentiment": venue[29],
            "neutral_sentiment": venue[30],
            "engagement_score": venue[31],
            "psychographic_keywords": venue[32] or [],
            # Calculate comprehensive score
            "total_score": calculate_comprehensive_venue_score(venue),
            # Categorize by data source
            "data_source": categorize_venue_data_source(venue[2]),
        }
        venues.append(venue_data)

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(venues)} venues from all sources")
    return venues


def fetch_all_events_data():
    """Fetch ALL events from database including API and scraped events."""
    print("🎪 Fetching all events data (API + Scraped)...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get all events with comprehensive data
    query = """
    SELECT 
        e.event_id,
        e.external_id,
        e.provider,
        e.name,
        e.description,
        e.category,
        e.subcategory,
        e.tags,
        e.start_time,
        e.end_time,
        e.ticket_price_min,
        e.ticket_price_max,
        e.predicted_attendance,
        e.actual_attendance,
        e.psychographic_relevance,
        e.created_at,
        -- Venue information
        v.name as venue_name,
        v.lat,
        v.lng,
        v.address as venue_address,
        v.category as venue_category,
        -- Social sentiment for events
        ss.mention_count,
        ss.positive_sentiment,
        ss.negative_sentiment,
        ss.engagement_score,
        ss.psychographic_keywords
    FROM events e
    LEFT JOIN venues v ON e.venue_id = v.venue_id
    LEFT JOIN LATERAL (
        SELECT mention_count, positive_sentiment, negative_sentiment,
               engagement_score, psychographic_keywords
        FROM social_sentiment 
        WHERE event_id = e.event_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) ss ON true
    WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
    AND (e.start_time IS NULL OR e.start_time >= NOW() - INTERVAL '30 days')
    ORDER BY e.start_time ASC NULLS LAST
    """

    cur.execute(query)
    events_raw = cur.fetchall()

    events = []
    for event in events_raw:
        event_data = {
            "event_id": str(event[0]),
            "external_id": event[1],
            "provider": event[2] or "unknown",
            "name": event[3] or "Unknown Event",
            "description": event[4],
            "category": event[5] or "unknown",
            "subcategory": event[6],
            "tags": event[7] or [],
            "start_time": event[8],
            "end_time": event[9],
            "ticket_price_min": event[10],
            "ticket_price_max": event[11],
            "predicted_attendance": event[12],
            "actual_attendance": event[13],
            "psychographic_relevance": event[14] or {},
            "created_at": event[15],
            # Venue information
            "venue_name": event[16] or "Unknown Venue",
            "latitude": float(event[17]),
            "longitude": float(event[18]),
            "venue_address": event[19],
            "venue_category": event[20],
            # Social data
            "mention_count": event[21],
            "positive_sentiment": event[22],
            "negative_sentiment": event[23],
            "engagement_score": event[24],
            "psychographic_keywords": event[25] or [],
            # Calculate comprehensive score
            "total_score": calculate_comprehensive_event_score(event),
            # Format date for display
            "date": event[8].strftime("%Y-%m-%d %H:%M") if event[8] else "TBD",
            # Categorize by data source
            "data_source": categorize_event_data_source(event[2]),
        }
        events.append(event_data)

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(events)} events from all sources")
    return events


def fetch_weather_data():
    """Fetch comprehensive weather data."""
    print("🌤️ Fetching weather data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent weather data (last 48 hours)
    query = """
    SELECT 
        lat, lng, temperature_f, feels_like_f, humidity, 
        rain_probability, precipitation_mm, wind_speed_mph, 
        weather_condition, uv_index, ts
    FROM weather_data 
    WHERE ts >= NOW() - INTERVAL '48 hours'
    ORDER BY ts DESC
    LIMIT 100
    """

    cur.execute(query)
    weather_raw = cur.fetchall()

    weather_data = []
    for weather in weather_raw:
        weather_data.append(
            {
                "latitude": float(weather[0]),
                "longitude": float(weather[1]),
                "temperature": weather[2],
                "feels_like": weather[3],
                "humidity": weather[4],
                "rain_probability": weather[5],
                "precipitation": weather[6],
                "wind_speed": weather[7],
                "conditions": weather[8] or "Unknown",
                "uv_index": weather[9],
                "timestamp": weather[10],
                "data_source": "api_weather",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(weather_data)} weather points")
    return weather_data


def fetch_traffic_congestion_data():
    """Fetch traffic congestion data."""
    print("🚗 Fetching traffic congestion data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent traffic data with venue information
    query = """
    SELECT 
        v.lat, v.lng, v.name, td.congestion_score, 
        td.travel_time_to_downtown, td.travel_time_index, td.ts
    FROM traffic_data td
    JOIN venues v ON td.venue_id = v.venue_id
    WHERE td.ts >= NOW() - INTERVAL '24 hours'
    AND v.lat IS NOT NULL AND v.lng IS NOT NULL
    ORDER BY td.ts DESC
    LIMIT 200
    """

    cur.execute(query)
    traffic_raw = cur.fetchall()

    traffic_data = []
    for traffic in traffic_raw:
        traffic_data.append(
            {
                "latitude": float(traffic[0]),
                "longitude": float(traffic[1]),
                "venue_name": traffic[2],
                "congestion_score": traffic[3] or 0,
                "travel_time_downtown": traffic[4] or 0,
                "travel_time_index": traffic[5] or 1.0,
                "timestamp": traffic[6],
                "data_source": "api_traffic",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(traffic_data)} traffic congestion points")
    return traffic_data


def fetch_social_sentiment_data():
    """Fetch social sentiment data."""
    print("📱 Fetching social sentiment data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent social sentiment data
    query = """
    SELECT 
        v.lat, v.lng, v.name, ss.platform, ss.mention_count,
        ss.positive_sentiment, ss.negative_sentiment, ss.neutral_sentiment,
        ss.engagement_score, ss.psychographic_keywords, ss.ts,
        e.name as event_name
    FROM social_sentiment ss
    LEFT JOIN venues v ON ss.venue_id = v.venue_id
    LEFT JOIN events e ON ss.event_id = e.event_id
    WHERE ss.ts >= NOW() - INTERVAL '7 days'
    AND (v.lat IS NOT NULL OR e.event_id IS NOT NULL)
    ORDER BY ss.ts DESC
    LIMIT 300
    """

    cur.execute(query)
    sentiment_raw = cur.fetchall()

    sentiment_data = []
    for sentiment in sentiment_raw:
        sentiment_data.append(
            {
                "latitude": float(sentiment[0]) if sentiment[0] else None,
                "longitude": float(sentiment[1]) if sentiment[1] else None,
                "venue_name": sentiment[2],
                "platform": sentiment[3],
                "mention_count": sentiment[4] or 0,
                "positive_sentiment": sentiment[5] or 0,
                "negative_sentiment": sentiment[6] or 0,
                "neutral_sentiment": sentiment[7] or 0,
                "engagement_score": sentiment[8] or 0,
                "psychographic_keywords": sentiment[9] or [],
                "timestamp": sentiment[10],
                "event_name": sentiment[11],
                "data_source": "api_social",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(sentiment_data)} social sentiment points")
    return sentiment_data


def fetch_economic_data():
    """Fetch economic indicators data."""
    print("💰 Fetching economic data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent economic data
    query = """
    SELECT 
        geographic_area, unemployment_rate, median_household_income,
        business_openings, business_closures, consumer_confidence,
        local_spending_index, ts
    FROM economic_data 
    WHERE ts >= NOW() - INTERVAL '30 days'
    ORDER BY ts DESC
    LIMIT 100
    """

    cur.execute(query)
    economic_raw = cur.fetchall()

    economic_data = []
    for econ in economic_raw:
        economic_data.append(
            {
                "geographic_area": econ[0],
                "unemployment_rate": econ[1],
                "median_household_income": econ[2],
                "business_openings": econ[3],
                "business_closures": econ[4],
                "consumer_confidence": econ[5],
                "local_spending_index": econ[6],
                "timestamp": econ[7],
                "data_source": "api_economic",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(economic_data)} economic data points")
    return economic_data


def fetch_demographics_data():
    """Fetch demographics/census data."""
    print("👥 Fetching demographics data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get demographics data with geometry
    query = """
    SELECT 
        tract_id, ST_AsGeoJSON(geometry) as geometry, median_income, 
        median_income_z, pct_bachelors, pct_graduate, pct_age_20_30,
        pct_age_30_40, pct_age_20_40, population, population_density,
        pct_professional_occupation, pct_management_occupation
    FROM demographics 
    WHERE geometry IS NOT NULL
    ORDER BY median_income DESC
    LIMIT 500
    """

    cur.execute(query)
    demographics_raw = cur.fetchall()

    demographics_data = []
    for demo in demographics_raw:
        demographics_data.append(
            {
                "tract_id": demo[0],
                "geometry": json.loads(demo[1]) if demo[1] else None,
                "median_income": demo[2],
                "median_income_z": demo[3],
                "pct_bachelors": demo[4],
                "pct_graduate": demo[5],
                "pct_age_20_30": demo[6],
                "pct_age_30_40": demo[7],
                "pct_age_20_40": demo[8],
                "population": demo[9],
                "population_density": demo[10],
                "pct_professional_occupation": demo[11],
                "pct_management_occupation": demo[12],
                "data_source": "census_demographics",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(demographics_data)} demographic areas")
    return demographics_data


def fetch_ml_predictions_data():
    """Fetch ML predictions data."""
    print("🤖 Fetching ML predictions data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent ML predictions
    query = """
    SELECT 
        lat, lng, psychographic_density, confidence_lower, confidence_upper,
        model_version, model_ensemble, contributing_factors, ts
    FROM predictions 
    WHERE ts >= NOW() - INTERVAL '7 days'
    AND lat IS NOT NULL AND lng IS NOT NULL
    ORDER BY psychographic_density DESC
    LIMIT 1000
    """

    cur.execute(query)
    predictions_raw = cur.fetchall()

    predictions_data = []
    for pred in predictions_raw:
        predictions_data.append(
            {
                "latitude": float(pred[0]),
                "longitude": float(pred[1]),
                "psychographic_density": pred[2],
                "confidence_lower": pred[3],
                "confidence_upper": pred[4],
                "model_version": pred[5],
                "model_ensemble": pred[6],
                "contributing_factors": pred[7],
                "timestamp": pred[8],
                "data_source": "ml_predictions",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(predictions_data)} ML predictions")
    return predictions_data


def fetch_manual_labels_data():
    """Fetch manual labels/ground truth data."""
    print("✅ Fetching manual labels data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get manual labels
    query = """
    SELECT 
        ml.lat, ml.lng, ml.psychographic_density, ml.labeler_id, ml.confidence,
        ml.notes, ml.validation_status, ml.ts, v.name as venue_name
    FROM manual_labels ml
    LEFT JOIN venues v ON ml.venue_id = v.venue_id
    WHERE ml.lat IS NOT NULL AND ml.lng IS NOT NULL
    ORDER BY ml.ts DESC
    LIMIT 500
    """

    cur.execute(query)
    labels_raw = cur.fetchall()

    labels_data = []
    for label in labels_raw:
        labels_data.append(
            {
                "latitude": float(label[0]),
                "longitude": float(label[1]),
                "psychographic_density": label[2],
                "labeler_id": label[3],
                "confidence": label[4],
                "notes": label[5],
                "validation_status": label[6],
                "timestamp": label[7],
                "venue_name": label[8],
                "data_source": "manual_labels",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(labels_data)} manual labels")
    return labels_data


def fetch_proxy_labels_data():
    """Fetch proxy labels data."""
    print("🔗 Fetching proxy labels data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get proxy labels
    query = """
    SELECT 
        pl.source, pl.psychographic_density, pl.confidence, pl.source_data,
        pl.ts, v.lat, v.lng, v.name as venue_name, e.name as event_name
    FROM proxy_labels pl
    LEFT JOIN venues v ON pl.venue_id = v.venue_id
    LEFT JOIN events e ON pl.event_id = e.event_id
    WHERE (v.lat IS NOT NULL OR e.event_id IS NOT NULL)
    ORDER BY pl.ts DESC
    LIMIT 500
    """

    cur.execute(query)
    proxy_raw = cur.fetchall()

    proxy_data = []
    for proxy in proxy_raw:
        proxy_data.append(
            {
                "source": proxy[0],
                "psychographic_density": proxy[1],
                "confidence": proxy[2],
                "source_data": proxy[3],
                "timestamp": proxy[4],
                "latitude": float(proxy[5]) if proxy[5] else None,
                "longitude": float(proxy[6]) if proxy[6] else None,
                "venue_name": proxy[7],
                "event_name": proxy[8],
                "data_source": "proxy_labels",
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(proxy_data)} proxy labels")
    return proxy_data


def fetch_psychographic_layers():
    """Fetch custom psychographic layer data."""
    print("🧠 Fetching psychographic layers...")

    conn = get_db_conn()
    if not conn:
        return {}

    cur = conn.cursor()

    # Get all psychographic layer data
    query = """
    SELECT layer_name, lat, lng, score, confidence, metadata
    FROM psychographic_layers
    WHERE score > 0.05
    ORDER BY layer_name, score DESC
    """

    cur.execute(query)
    layers_raw = cur.fetchall()

    layers = {}
    for layer in layers_raw:
        layer_name = layer[0]
        if layer_name not in layers:
            layers[layer_name] = {}

        layers[layer_name][(float(layer[1]), float(layer[2]))] = {
            "score": layer[3],
            "confidence": layer[4],
            "metadata": layer[5] or {},
            "data_source": "calculated_psychographic",
        }

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(layers)} psychographic layers")
    for layer_name, layer_data in layers.items():
        print(f"  - {layer_name}: {len(layer_data)} points")

    return layers


def categorize_venue_data_source(provider):
    """Categorize venue by data source type."""
    if not provider:
        return "unknown"

    provider = provider.lower()

    if "google" in provider or "places" in provider:
        return "api_places"
    elif any(
        x in provider
        for x in [
            "tmobile",
            "uptown",
            "kauffman",
            "starlight",
            "midland",
            "knuckleheads",
        ]
    ):
        return "scraped_static"
    elif any(x in provider for x in ["visitkc", "do816", "thepitchkc", "aura"]):
        return "scraped_dynamic"
    else:
        return "scraped_local"


def categorize_event_data_source(provider):
    """Categorize event by data source type."""
    if not provider:
        return "unknown"

    provider = provider.lower()

    if "predicthq" in provider:
        return "api_events"
    elif any(x in provider for x in ["visitkc", "do816", "thepitchkc"]):
        return "scraped_dynamic"
    else:
        return "scraped_local"


def calculate_comprehensive_venue_score(venue_data):
    """Calculate comprehensive venue score from all available data."""
    psychographic = venue_data[16] or {}

    # Base psychographic score
    career_score = psychographic.get("career_driven", 0)
    competent_score = psychographic.get("competent", 0)
    fun_score = psychographic.get("fun", 0)
    base_score = (career_score * 0.4) + (competent_score * 0.3) + (fun_score * 0.3)

    # Social sentiment boost
    positive_sentiment = venue_data[28] or 0
    engagement_score = venue_data[31] or 0
    social_boost = (positive_sentiment * 0.3) + (engagement_score * 0.2)

    # Traffic/popularity boost
    visitors_count = venue_data[19] or 0
    visitors_change = venue_data[21] or 0
    traffic_boost = min(
        0.2, (visitors_count / 1000) * 0.1 + (max(0, visitors_change) * 0.1)
    )

    total_score = min(1.0, base_score + social_boost + traffic_boost)
    return max(0.0, total_score)


def calculate_comprehensive_event_score(event_data):
    """Calculate comprehensive event score from all available data."""
    psychographic = event_data[14] or {}

    # Base psychographic score
    career_score = psychographic.get("career_driven", 0)
    competent_score = psychographic.get("competent", 0)
    fun_score = psychographic.get("fun", 0)
    base_score = (career_score * 0.4) + (competent_score * 0.3) + (fun_score * 0.3)

    # Social sentiment boost
    positive_sentiment = event_data[22] or 0
    engagement_score = event_data[24] or 0
    social_boost = (positive_sentiment * 0.3) + (engagement_score * 0.2)

    # Attendance boost
    predicted_attendance = event_data[12] or 0
    attendance_boost = min(0.2, (predicted_attendance / 500) * 0.1)

    total_score = min(1.0, base_score + social_boost + attendance_boost)
    return max(0.0, total_score)


def create_comprehensive_all_data_map():
    """Create comprehensive map with ALL data from database and APIs."""
    print("🗺️  Creating Comprehensive All-Data Map")
    print("=" * 70)

    # Initialize map builder
    try:
        map_builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))
        print("✓ InteractiveMapBuilder initialized")
    except Exception as e:
        print(f"✗ Failed to initialize InteractiveMapBuilder: {e}")
        return 1

    # Fetch ALL data from database
    print("\n📊 Fetching ALL Data from Database...")

    venues_data = fetch_all_venues_data()
    events_data = fetch_all_events_data()
    weather_data = fetch_weather_data()
    traffic_data = fetch_traffic_congestion_data()
    social_data = fetch_social_sentiment_data()
    economic_data = fetch_economic_data()
    demographics_data = fetch_demographics_data()
    predictions_data = fetch_ml_predictions_data()
    manual_labels_data = fetch_manual_labels_data()
    proxy_labels_data = fetch_proxy_labels_data()
    psychographic_layers = fetch_psychographic_layers()

    # Organize data by source type
    api_layers = {}
    scraped_layers = {}
    calculated_layers = {}
    ground_truth_layers = {}

    # API Data Layers
    if venues_data:
        api_venues = [v for v in venues_data if v["data_source"] == "api_places"]
        if api_venues:
            api_layers["places"] = api_venues
            print(f"  ✓ Added {len(api_venues)} API venues to places layer")

    if events_data:
        api_events = [e for e in events_data if e["data_source"] == "api_events"]
        if api_events:
            api_layers["events"] = api_events
            print(f"  ✓ Added {len(api_events)} API events to events layer")

    if weather_data:
        api_layers["weather"] = weather_data
        print(f"  ✓ Added {len(weather_data)} weather points to weather layer")

    if traffic_data:
        api_layers["traffic_congestion"] = traffic_data
        print(f"  ✓ Added {len(traffic_data)} traffic points to traffic layer")

    if social_data:
        api_layers["social_sentiment"] = social_data
        print(f"  ✓ Added {len(social_data)} social sentiment points")

    if economic_data:
        api_layers["economic_indicators"] = economic_data
        print(f"  ✓ Added {len(economic_data)} economic data points")

    # Scraped Data Layers
    if venues_data:
        static_venues = [v for v in venues_data if v["data_source"] == "scraped_static"]
        dynamic_venues = [
            v for v in venues_data if v["data_source"] == "scraped_dynamic"
        ]
        local_venues = [v for v in venues_data if v["data_source"] == "scraped_local"]

        if static_venues:
            scraped_layers["static_venues"] = static_venues
            print(f"  ✓ Added {len(static_venues)} static scraped venues")
        if dynamic_venues:
            scraped_layers["dynamic_venues"] = dynamic_venues
            print(f"  ✓ Added {len(dynamic_venues)} dynamic scraped venues")
        if local_venues:
            scraped_layers["local_venues"] = local_venues
            print(f"  ✓ Added {len(local_venues)} local scraped venues")

    if events_data:
        scraped_events = [
            e
            for e in events_data
            if e["data_source"] in ["scraped_dynamic", "scraped_local"]
        ]
        if scraped_events:
            scraped_layers["scraped_events"] = scraped_events
            print(f"  ✓ Added {len(scraped_events)} scraped events")

    # Calculated Data Layers
    if demographics_data:
        calculated_layers["demographics"] = demographics_data
        print(f"  ✓ Added {len(demographics_data)} demographic areas")

    if predictions_data:
        calculated_layers["ml_predictions"] = predictions_data
        print(f"  ✓ Added {len(predictions_data)} ML predictions")

    # Add psychographic layers to calculated
    for layer_name, layer_data in psychographic_layers.items():
        if layer_data:
            calculated_layers[layer_name] = layer_data
            print(f"  ✓ Added {len(layer_data)} points to {layer_name} layer")

    # Ground Truth Layers
    if manual_labels_data:
        ground_truth_layers["manual_labels"] = manual_labels_data
        print(f"  ✓ Added {len(manual_labels_data)} manual labels")

    if proxy_labels_data:
        ground_truth_layers["proxy_labels"] = proxy_labels_data
        print(f"  ✓ Added {len(proxy_labels_data)} proxy labels")

    # Create comprehensive layered heatmap
    print(f"\n🎨 Creating Comprehensive All-Data Map...")

    try:
        # Combine all layers for the comprehensive map
        all_layers = {
            "api_layers": api_layers,
            "scraped_layers": scraped_layers,
            "calculated_layers": calculated_layers,
            "ground_truth_layers": ground_truth_layers,
        }

        output_file = map_builder.create_comprehensive_all_data_map(
            all_layers=all_layers,
            output_path="comprehensive_all_data_map.html",
            style="streets",
        )

        if output_file and output_file.exists():
            print(f"✓ Comprehensive all-data map created: {output_file}")

            # Display comprehensive summary
            print(f"\n📊 Comprehensive All-Data Summary:")

            print(f"  📡 API Data Layers:")
            for layer_name, layer_data in api_layers.items():
                print(f"    - {layer_name}: {len(layer_data)} items")

            print(f"  🌐 Scraped Data Layers:")
            for layer_name, layer_data in scraped_layers.items():
                print(f"    - {layer_name}: {len(layer_data)} items")

            print(f"  🧠 Calculated Data Layers:")
            for layer_name, layer_data in calculated_layers.items():
                if isinstance(layer_data, dict):
                    print(f"    - {layer_name}: {len(layer_data)} items")
                else:
                    print(f"    - {layer_name}: {len(layer_data)} items")

            print(f"  ✅ Ground Truth Layers:")
            for layer_name, layer_data in ground_truth_layers.items():
                print(f"    - {layer_name}: {len(layer_data)} items")

            # Calculate totals
            total_venues = len(venues_data)
            total_events = len(events_data)
            total_data_points = (
                len(weather_data)
                + len(traffic_data)
                + len(social_data)
                + len(economic_data)
                + len(demographics_data)
                + len(predictions_data)
                + len(manual_labels_data)
                + len(proxy_labels_data)
                + sum(len(layer_data) for layer_data in psychographic_layers.values())
            )

            print(f"\n🎯 Total Data Summary:")
            print(f"  🏢 Total Venues: {total_venues}")
            print(f"  🎪 Total Events: {total_events}")
            print(f"  📊 Total Data Points: {total_data_points}")
            print(
                f"  🗂️  Total Layers: {len(api_layers) + len(scraped_layers) + len(calculated_layers) + len(ground_truth_layers)}"
            )

            # Open in browser
            try:
                map_builder.open_in_browser(output_file)
                print(f"\n🌐 Comprehensive map opened in browser!")
            except Exception as e:
                print(f"⚠️  Could not auto-open browser: {e}")
                print(f"   Please manually open: {output_file.absolute()}")

            print(f"\n🎉 Comprehensive all-data map generation completed successfully!")
            print(f"📁 Output file: {output_file.absolute()}")

            print(f"\n💡 COMPREHENSIVE MAP FEATURES:")
            print(f"   🏆 Complete Data Coverage - ALL database tables and APIs")
            print(
                f"   📊 Multi-Source Visualization - API, Scraped, Calculated, Ground Truth"
            )
            print(
                f"   🎛️  Advanced Layer Controls - Hierarchical organization by data source"
            )
            print(
                f"   📈 Real-time Data Integration - Weather, traffic, social sentiment"
            )
            print(f"   🤖 ML Predictions & Confidence - Model outputs with uncertainty")
            print(f"   ✅ Ground Truth Validation - Manual and proxy labels")
            print(f"   🌍 Geographic Coverage - Demographics, economic indicators")
            print(f"   🔍 Cross-Layer Analysis - Data correlation and relationships")

            return 0

        else:
            print("✗ Failed to create comprehensive all-data map")
            return 1

    except Exception as e:
        print(f"✗ Error creating comprehensive all-data map: {e}")
        import traceback

        traceback.print_exc()
        return 1


def main():
    """Main function to create comprehensive all-data map."""
    return create_comprehensive_all_data_map()


if __name__ == "__main__":
    sys.exit(main())
