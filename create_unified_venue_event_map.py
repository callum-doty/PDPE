#!/usr/bin/env python3
"""
Create a comprehensive unified map showing all venues and events from all APIs and scraped data sources.
This script aggregates data from the database and creates an interactive map with multiple layers.
"""

import sys
import os
import psycopg2
import json
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

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
    """Fetch all venues from database with their complete information."""
    print("📍 Fetching all venues data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get all venues with their psychographic scores and additional data
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
        -- Get latest traffic data
        td.congestion_score,
        td.travel_time_to_downtown,
        -- Get latest social sentiment
        ss.mention_count,
        ss.positive_sentiment,
        ss.engagement_score
    FROM venues v
    LEFT JOIN LATERAL (
        SELECT visitors_count, median_dwell_seconds, visitors_change_24h, visitors_change_7d
        FROM venue_traffic 
        WHERE venue_id = v.venue_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) vt ON true
    LEFT JOIN LATERAL (
        SELECT congestion_score, travel_time_to_downtown
        FROM traffic_data 
        WHERE venue_id = v.venue_id 
        ORDER BY ts DESC 
        LIMIT 1
    ) td ON true
    LEFT JOIN LATERAL (
        SELECT mention_count, positive_sentiment, engagement_score
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
            # Additional data
            "visitors_count": venue[19],
            "median_dwell_seconds": venue[20],
            "visitors_change_24h": venue[21],
            "visitors_change_7d": venue[22],
            "congestion_score": venue[23],
            "travel_time_to_downtown": venue[24],
            "mention_count": venue[25],
            "positive_sentiment": venue[26],
            "engagement_score": venue[27],
            # Calculate total score for visualization
            "total_score": calculate_venue_total_score(venue[16] or {}),
        }
        venues.append(venue_data)

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(venues)} venues")
    return venues


def fetch_all_events_data():
    """Fetch all events from database with their complete information."""
    print("🎪 Fetching all events data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get all events with venue information
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
        ss.engagement_score
    FROM events e
    LEFT JOIN venues v ON e.venue_id = v.venue_id
    LEFT JOIN LATERAL (
        SELECT mention_count, positive_sentiment, engagement_score
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
            "engagement_score": event[23],
            # Calculate total score for visualization
            "total_score": calculate_event_total_score(event[14] or {}),
            # Format date for display
            "date": event[8].strftime("%Y-%m-%d %H:%M") if event[8] else "TBD",
        }
        events.append(event_data)

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(events)} events")
    return events


def fetch_weather_data():
    """Fetch recent weather data."""
    print("🌤️ Fetching weather data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent weather data (last 24 hours)
    query = """
    SELECT 
        lat, lng, temperature_f, feels_like_f, humidity, 
        rain_probability, wind_speed_mph, weather_condition, 
        uv_index, ts
    FROM weather_data 
    WHERE ts >= NOW() - INTERVAL '24 hours'
    ORDER BY ts DESC
    LIMIT 50
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
                "wind_speed": weather[6],
                "conditions": weather[7] or "Unknown",
                "uv_index": weather[8],
                "timestamp": weather[9],
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(weather_data)} weather points")
    return weather_data


def fetch_foot_traffic_data():
    """Fetch recent foot traffic data."""
    print("🚶 Fetching foot traffic data...")

    conn = get_db_conn()
    if not conn:
        return []

    cur = conn.cursor()

    # Get recent foot traffic data with venue information
    query = """
    SELECT 
        v.lat, v.lng, v.name, vt.visitors_count, 
        vt.median_dwell_seconds, vt.visitors_change_24h, 
        vt.visitors_change_7d, vt.ts
    FROM venue_traffic vt
    JOIN venues v ON vt.venue_id = v.venue_id
    WHERE vt.ts >= NOW() - INTERVAL '7 days'
    AND v.lat IS NOT NULL AND v.lng IS NOT NULL
    ORDER BY vt.ts DESC
    LIMIT 100
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
                "volume": traffic[3] or 0,
                "dwell_time": traffic[4] or 0,
                "change_24h": traffic[5],
                "change_7d": traffic[6],
                "timestamp": traffic[7],
            }
        )

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(traffic_data)} foot traffic points")
    return traffic_data


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
    WHERE score > 0.1
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
        }

    cur.close()
    conn.close()

    print(f"✓ Fetched {len(layers)} psychographic layers")
    for layer_name, layer_data in layers.items():
        print(f"  - {layer_name}: {len(layer_data)} points")

    return layers


def calculate_venue_total_score(psychographic_relevance):
    """Calculate total score for venue based on psychographic relevance."""
    if not psychographic_relevance:
        return 0.0

    career_score = psychographic_relevance.get("career_driven", 0)
    competent_score = psychographic_relevance.get("competent", 0)
    fun_score = psychographic_relevance.get("fun", 0)

    # Weighted average with career_driven having highest weight
    total_score = (career_score * 0.5) + (competent_score * 0.3) + (fun_score * 0.2)
    return min(1.0, max(0.0, total_score))


def calculate_event_total_score(psychographic_relevance):
    """Calculate total score for event based on psychographic relevance."""
    if not psychographic_relevance:
        return 0.0

    career_score = psychographic_relevance.get("career_driven", 0)
    competent_score = psychographic_relevance.get("competent", 0)
    fun_score = psychographic_relevance.get("fun", 0)

    # For events, fun might be weighted higher
    total_score = (career_score * 0.4) + (competent_score * 0.3) + (fun_score * 0.3)
    return min(1.0, max(0.0, total_score))


def create_unified_map():
    """Create comprehensive unified map with all data sources."""
    print("🗺️  Creating Unified Venue & Event Map")
    print("=" * 60)

    # Initialize map builder
    try:
        map_builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))
        print("✓ InteractiveMapBuilder initialized")
    except Exception as e:
        print(f"✗ Failed to initialize InteractiveMapBuilder: {e}")
        return 1

    # Fetch all data from database
    print("\n📊 Fetching Data from Database...")

    venues_data = fetch_all_venues_data()
    events_data = fetch_all_events_data()
    weather_data = fetch_weather_data()
    traffic_data = fetch_foot_traffic_data()
    psychographic_layers = fetch_psychographic_layers()

    # Prepare API layers data structure
    api_layers = {}

    if venues_data:
        api_layers["places"] = venues_data
        print(f"  ✓ Added {len(venues_data)} venues to places layer")

    if events_data:
        api_layers["events"] = events_data
        print(f"  ✓ Added {len(events_data)} events to events layer")

    if weather_data:
        api_layers["weather"] = weather_data
        print(f"  ✓ Added {len(weather_data)} weather points to weather layer")

    if traffic_data:
        api_layers["foot_traffic"] = traffic_data
        print(f"  ✓ Added {len(traffic_data)} foot traffic points to traffic layer")

    # Prepare assumption layers (convert to expected format)
    assumption_layers = {}
    for layer_name, layer_data in psychographic_layers.items():
        # Convert to simple coordinate -> score mapping
        assumption_layers[layer_name] = {
            coord: data["score"] for coord, data in layer_data.items()
        }
        print(f"  ✓ Added {len(layer_data)} points to {layer_name} assumption layer")

    # Create comprehensive layered heatmap
    print(f"\n🎨 Creating Comprehensive Map...")

    try:
        output_file = map_builder.create_layered_heatmap(
            api_layers=api_layers,
            assumption_layers=assumption_layers,
            output_path="unified_venue_event_map.html",
            style="streets",
        )

        if output_file and output_file.exists():
            print(f"✓ Unified map created: {output_file}")

            # Display comprehensive summary
            print(f"\n📊 Comprehensive Data Summary:")
            print(f"  🏢 Venues & Places:")
            print(f"    - Total venues: {len(venues_data)}")
            if venues_data:
                providers = set(v["provider"] for v in venues_data)
                print(f"    - Data providers: {', '.join(providers)}")
                categories = set(v["category"] for v in venues_data)
                print(f"    - Categories: {len(categories)} types")

            print(f"  🎪 Events:")
            print(f"    - Total events: {len(events_data)}")
            if events_data:
                providers = set(e["provider"] for e in events_data)
                print(f"    - Data providers: {', '.join(providers)}")
                upcoming_events = len(
                    [
                        e
                        for e in events_data
                        if e["start_time"] and e["start_time"] > datetime.now()
                    ]
                )
                print(f"    - Upcoming events: {upcoming_events}")

            print(f"  🌤️  Weather Data: {len(weather_data)} recent points")
            print(f"  🚶 Foot Traffic: {len(traffic_data)} venue traffic points")

            print(f"  🧠 Psychographic Layers:")
            for layer_name, layer_data in assumption_layers.items():
                print(f"    - {layer_name}: {len(layer_data)} grid points")

            # Open in browser
            try:
                map_builder.open_in_browser(output_file)
                print(f"\n🌐 Map opened in browser!")
            except Exception as e:
                print(f"⚠️  Could not auto-open browser: {e}")
                print(f"   Please manually open: {output_file.absolute()}")

            print(f"\n🎉 Unified map generation completed successfully!")
            print(f"📁 Output file: {output_file.absolute()}")

            print(f"\n💡 MAP FEATURES:")
            print(f"   🏆 Venue Ranking Sidebar - Browse all venues by score")
            print(f"   📍 Venue & Event Markers - Click for detailed information")
            print(f"   🎛️  Layer Controls - Toggle different data layers")
            print(f"   📊 Comprehensive Legend - Understand all data sources")
            print(f"   🌡️  Real-time Data - Weather, traffic, social sentiment")
            print(f"   🧠 Predictive Layers - Psychographic modeling results")

            return 0

        else:
            print("✗ Failed to create unified map")
            return 1

    except Exception as e:
        print(f"✗ Error creating unified map: {e}")
        import traceback

        traceback.print_exc()
        return 1


def main():
    """Main function to create unified venue and event map."""
    return create_unified_map()


if __name__ == "__main__":
    sys.exit(main())
