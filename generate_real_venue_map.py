#!/usr/bin/env python3
"""
Generate comprehensive venue map using real data from the database.
This creates an interactive map with actual venues and events with psychographic scores.
"""

import sys
import os
from pathlib import Path
import psycopg2
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder

    print("✓ Successfully imported InteractiveMapBuilder")
except ImportError as e:
    print(f"✗ Failed to import InteractiveMapBuilder: {e}")
    sys.exit(1)


def get_db_conn():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:@localhost:5432/ppm")
    return psycopg2.connect(db_url)


def fetch_venues_from_db():
    """Fetch all venues from database with psychographic scores."""
    print("🏪 Fetching venues from database...")

    conn = get_db_conn()
    cur = conn.cursor()

    query = """
    SELECT 
        external_id, provider, name, category, subcategory, 
        price_tier, avg_rating, review_count, lat, lng, 
        address, phone, website, psychographic_relevance
    FROM venues 
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    ORDER BY avg_rating DESC NULLS LAST
    """

    cur.execute(query)
    rows = cur.fetchall()

    venues = []
    for row in rows:
        # Parse psychographic relevance (JSONB returns dict, not string)
        psychographic_relevance = row[13] if row[13] else {}

        venue = {
            "external_id": row[0],
            "provider": row[1],
            "name": row[2],
            "category": row[3],
            "subcategory": row[4],
            "price_tier": row[5],
            "avg_rating": row[6],
            "review_count": row[7],
            "latitude": float(row[8]),
            "longitude": float(row[9]),
            "address": row[10],
            "phone": row[11],
            "website": row[12],
            "psychographic_relevance": psychographic_relevance,
            "total_score": psychographic_relevance.get("career_driven", 0.5),
        }
        venues.append(venue)

    cur.close()
    conn.close()

    print(f"✅ Fetched {len(venues)} venues from database")
    return venues


def fetch_events_from_db():
    """Fetch all events from database with psychographic scores."""
    print("🎉 Fetching events from database...")

    conn = get_db_conn()
    cur = conn.cursor()

    query = """
    SELECT 
        e.external_id, e.provider, e.name, e.description, e.category, 
        e.tags, e.start_time, e.end_time, e.ticket_price_min, e.ticket_price_max,
        e.psychographic_relevance, v.lat, v.lng, v.name as venue_name
    FROM events e
    LEFT JOIN venues v ON e.venue_id = v.venue_id
    WHERE e.start_time >= NOW()
    ORDER BY e.start_time
    """

    cur.execute(query)
    rows = cur.fetchall()

    events = []
    for row in rows:
        # Parse psychographic relevance (JSONB returns dict, not string)
        psychographic_relevance = row[10] if row[10] else {}

        # Use venue location if available, otherwise skip
        if row[11] and row[12]:
            event = {
                "external_id": row[0],
                "provider": row[1],
                "name": row[2],
                "description": row[3],
                "category": row[4],
                "tags": row[5],
                "start_time": row[6],
                "end_time": row[7],
                "ticket_price_min": row[8],
                "ticket_price_max": row[9],
                "psychographic_relevance": psychographic_relevance,
                "latitude": float(row[11]),
                "longitude": float(row[12]),
                "venue_name": row[13] or "Unknown Venue",
                "total_score": psychographic_relevance.get("career_driven", 0.5),
                "date": row[6].strftime("%Y-%m-%d") if row[6] else "Unknown Date",
            }
            events.append(event)

    cur.close()
    conn.close()

    print(f"✅ Fetched {len(events)} events from database")
    return events


def fetch_demographics_from_db():
    """Fetch demographic data from database."""
    print("📊 Fetching demographics from database...")

    conn = get_db_conn()
    cur = conn.cursor()

    query = """
    SELECT 
        tract_id, median_income, median_income_z, pct_bachelors, pct_graduate,
        pct_age_20_30, pct_age_30_40, pct_professional_occupation, 
        pct_management_occupation, population_density,
        ST_Y(ST_Centroid(geometry::geometry)) as lat, ST_X(ST_Centroid(geometry::geometry)) as lng
    FROM demographics
    """

    cur.execute(query)
    rows = cur.fetchall()

    demographics = {}
    for row in rows:
        lat, lng = float(row[10]), float(row[11])

        # Calculate demographic score based on psychographic factors
        demo_score = (
            (row[3] / 100.0) * 0.3  # Bachelor's degree
            + (row[4] / 100.0) * 0.2  # Graduate degree
            + (row[5] / 100.0) * 0.2  # Age 20-30
            + (row[6] / 100.0) * 0.2  # Age 30-40
            + (row[7] / 100.0) * 0.1  # Professional occupation
        )

        demographics[(lat, lng)] = min(1.0, demo_score)

    cur.close()
    conn.close()

    print(f"✅ Fetched {len(demographics)} demographic points from database")
    return demographics


def generate_college_density_layer():
    """Generate college density layer based on known college locations."""
    print("🎓 Generating college density layer...")

    # Known college locations in KC area
    college_locations = [
        (39.0354, -94.5781),  # UMKC
        (39.0189, -94.6708),  # Rockhurst University
        (39.1142, -94.6275),  # Park University
        (39.0458, -94.5833),  # UMKC Downtown
    ]

    college_density = {}

    # Create grid around Kansas City
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461
    grid_size = 40

    lat_step = (lat_max - lat_min) / grid_size
    lng_step = (lng_max - lng_min) / grid_size

    for i in range(grid_size):
        for j in range(grid_size):
            lat = lat_min + (i * lat_step)
            lng = lng_min + (j * lng_step)

            # Calculate density based on distance to colleges
            total_influence = 0
            for college_lat, college_lng in college_locations:
                distance = ((lat - college_lat) ** 2 + (lng - college_lng) ** 2) ** 0.5
                influence = max(0, 1.0 - (distance * 20))
                total_influence += influence

            density = min(1.0, total_influence)
            if density > 0.1:
                college_density[(lat, lng)] = round(density, 3)

    print(f"✅ Generated {len(college_density)} college density points")
    return college_density


def generate_spending_propensity_layer():
    """Generate spending propensity layer based on affluent areas."""
    print("💰 Generating spending propensity layer...")

    # High-income/high-spending areas in KC
    affluent_areas = [
        (39.0458, -94.5833),  # Country Club Plaza
        (39.0739, -94.5861),  # Crossroads
        (39.0997, -94.5786),  # Downtown/Power & Light
        (39.0354, -94.5781),  # Midtown
        (39.1167, -94.6275),  # Northland
    ]

    spending_propensity = {}

    # Create grid around Kansas City
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461
    grid_size = 35

    lat_step = (lat_max - lat_min) / grid_size
    lng_step = (lng_max - lng_min) / grid_size

    for i in range(grid_size):
        for j in range(grid_size):
            lat = lat_min + (i * lat_step)
            lng = lng_min + (j * lng_step)

            # Calculate propensity based on distance to affluent areas
            total_influence = 0
            for area_lat, area_lng in affluent_areas:
                distance = ((lat - area_lat) ** 2 + (lng - area_lng) ** 2) ** 0.5
                influence = max(0, 1.0 - (distance * 15))
                total_influence += influence

            propensity = min(1.0, total_influence * 0.8 + 0.2)  # Base level + proximity
            if propensity > 0.3:
                spending_propensity[(lat, lng)] = round(propensity, 3)

    print(f"✅ Generated {len(spending_propensity)} spending propensity points")
    return spending_propensity


def main():
    """Generate comprehensive venue map with real data."""
    print("🗺️  Generating Real Venue Map with Database Data")
    print("=" * 60)

    try:
        # Initialize map builder
        map_builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))
        print("✓ InteractiveMapBuilder initialized")

        # Fetch real data from database
        venues = fetch_venues_from_db()
        events = fetch_events_from_db()
        demographics = fetch_demographics_from_db()

        # Generate assumption layers
        college_density = generate_college_density_layer()
        spending_propensity = generate_spending_propensity_layer()

        # Prepare API layers (real data from database)
        api_layers = {
            "events": events,
            "places": venues,
            "weather": [],  # Could be populated with real weather data
            "foot_traffic": [],  # Could be populated with real foot traffic data
        }

        # Prepare assumption layers
        assumption_layers = {
            "college_density": college_density,
            "spending_propensity": spending_propensity,
            "demographics": demographics,
        }

        # Create comprehensive layered heatmap with real data
        print("\n🎨 Creating Real Venue Map...")

        output_file = map_builder.create_layered_heatmap(
            api_layers=api_layers,
            assumption_layers=assumption_layers,
            output_path="real_venue_comprehensive_map.html",
            style="streets",
        )

        if output_file and output_file.exists():
            print(f"✅ Real venue map created: {output_file}")

            # Display comprehensive summary
            print(f"\n📊 REAL DATA SUMMARY:")
            print(f"  🏪 Real Venues: {len(venues)} (from Google Places)")
            print(
                f"     - Restaurants: {len([v for v in venues if 'restaurant' in v.get('category', '').lower()])}"
            )
            print(
                f"     - Bars: {len([v for v in venues if 'bar' in v.get('category', '').lower()])}"
            )
            print(
                f"     - Coffee Shops: {len([v for v in venues if 'cafe' in v.get('category', '').lower()])}"
            )
            print(
                f"     - Entertainment: {len([v for v in venues if 'entertainment' in v.get('category', '').lower()])}"
            )
            print(
                f"     - Other: {len([v for v in venues if not any(t in v.get('category', '').lower() for t in ['restaurant', 'bar', 'cafe', 'entertainment'])])}"
            )

            print(f"  🎉 Real Events: {len(events)} (from PredictHQ)")
            print(f"  📊 Demographics: {len(demographics)} census tract points")
            print(f"  🎓 College Density: {len(college_density)} calculated points")
            print(
                f"  💰 Spending Propensity: {len(spending_propensity)} calculated points"
            )

            # Calculate average scores
            if venues:
                avg_venue_score = sum(v.get("total_score", 0) for v in venues) / len(
                    venues
                )
                high_score_venues = len(
                    [v for v in venues if v.get("total_score", 0) >= 0.7]
                )
                print(f"\n🎯 PSYCHOGRAPHIC ANALYSIS:")
                print(f"  Average venue score: {avg_venue_score:.3f}")
                print(f"  High-scoring venues (≥0.7): {high_score_venues}")
                print(
                    f"  Top venue: {max(venues, key=lambda x: x.get('total_score', 0))['name']} ({max(venues, key=lambda x: x.get('total_score', 0))['total_score']:.3f})"
                )

            # Open in browser
            try:
                map_builder.open_in_browser(output_file)
                print(f"\n🌐 Real venue map opened in browser!")
            except Exception as e:
                print(f"⚠️  Could not auto-open browser: {e}")
                print(f"   Please manually open: {output_file.absolute()}")

            print(f"\n🎉 Real venue map generation completed successfully!")
            print(f"📁 Output file: {output_file.absolute()}")
            print(f"\n📋 MAP FEATURES:")
            print(f"  ✅ Interactive layers with real venue data")
            print(f"  ✅ Psychographic scoring for all venues")
            print(f"  ✅ Real events with venue locations")
            print(f"  ✅ Demographic overlays")
            print(f"  ✅ College density and spending propensity layers")
            print(f"  ✅ Comprehensive legend and controls")

            return 0

        else:
            print("✗ Failed to create real venue map")
            return 1

    except Exception as e:
        print(f"\n❌ Error creating real venue map: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
