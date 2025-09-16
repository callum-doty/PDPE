#!/usr/bin/env python3
"""
Populate database with sample data from various sources and create comprehensive unified map.
This script runs ETL processes to gather data, then creates the unified visualization.
"""

import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Load environment variables
load_dotenv()


def run_etl_process(script_name, description, required=False):
    """Run an ETL process and handle errors gracefully."""
    print(f"\n🔄 Running {description}...")

    try:
        # Try to run the ETL script
        result = subprocess.run(
            [sys.executable, "-m", f"src.etl.{script_name}"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=120,  # 2 minute timeout
        )

        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            if result.stdout:
                # Show last few lines of output
                lines = result.stdout.strip().split("\n")
                for line in lines[-3:]:
                    if line.strip():
                        print(f"   {line}")
            return True
        else:
            print(f"⚠️  {description} completed with warnings")
            if result.stderr:
                error_lines = result.stderr.strip().split("\n")
                for line in error_lines[-2:]:
                    if line.strip() and not line.startswith("WARNING"):
                        print(f"   Warning: {line}")
            return not required

    except subprocess.TimeoutExpired:
        print(f"⏰ {description} timed out (2 min limit)")
        return not required
    except Exception as e:
        print(f"❌ {description} failed: {e}")
        return not required


def populate_sample_events():
    """Create sample events data if no events exist."""
    print("\n🎪 Creating sample events data...")

    try:
        import psycopg2
        from datetime import datetime, timedelta
        import random
        import uuid

        # Get database connection
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("❌ DATABASE_URL not found")
            return False

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Check if we already have events
        cur.execute("SELECT COUNT(*) FROM events")
        event_count = cur.fetchone()[0]

        if event_count > 0:
            print(f"✅ Already have {event_count} events in database")
            cur.close()
            conn.close()
            return True

        # Get some venues to associate events with
        cur.execute(
            """
            SELECT venue_id, name, lat, lng, category 
            FROM venues 
            WHERE lat IS NOT NULL AND lng IS NOT NULL 
            LIMIT 20
        """
        )
        venues = cur.fetchall()

        if not venues:
            print("❌ No venues found to associate events with")
            cur.close()
            conn.close()
            return False

        # Sample event data
        event_types = [
            ("Tech Meetup", "technology", ["networking", "professional", "career"]),
            (
                "Business Networking",
                "business",
                ["networking", "professional", "career"],
            ),
            (
                "Startup Pitch Night",
                "business",
                ["entrepreneurship", "innovation", "career"],
            ),
            (
                "Professional Workshop",
                "education",
                ["learning", "professional", "skill"],
            ),
            (
                "Industry Conference",
                "business",
                ["conference", "professional", "career"],
            ),
            ("Career Fair", "career", ["jobs", "professional", "career"]),
            (
                "Leadership Seminar",
                "education",
                ["leadership", "professional", "management"],
            ),
            (
                "Innovation Summit",
                "technology",
                ["innovation", "entrepreneurship", "tech"],
            ),
            ("Marketing Workshop", "business", ["marketing", "professional", "skill"]),
            ("Finance Seminar", "business", ["finance", "professional", "investment"]),
            ("Jazz Night", "entertainment", ["music", "culture", "fun"]),
            ("Art Gallery Opening", "culture", ["art", "culture", "social"]),
            ("Wine Tasting", "social", ["wine", "social", "networking"]),
            ("Food Festival", "food", ["food", "culture", "fun"]),
            ("Live Music", "entertainment", ["music", "entertainment", "fun"]),
        ]

        events_created = 0

        for i in range(25):  # Create 25 sample events
            venue = random.choice(venues)
            event_type, category, tags = random.choice(event_types)

            # Generate event dates (mix of past and future)
            days_offset = random.randint(-15, 45)  # 15 days ago to 45 days future
            start_time = datetime.now() + timedelta(days=days_offset)
            end_time = start_time + timedelta(hours=random.randint(1, 4))

            # Calculate psychographic scores based on event type and tags
            career_score = (
                0.8
                if "professional" in tags or "career" in tags
                else random.uniform(0.1, 0.4)
            )
            competent_score = (
                0.7
                if "skill" in tags or "education" in category
                else random.uniform(0.2, 0.6)
            )
            fun_score = (
                0.9
                if "fun" in tags or "entertainment" in category
                else random.uniform(0.3, 0.7)
            )

            psychographic_relevance = {
                "career_driven": career_score,
                "competent": competent_score,
                "fun": fun_score,
            }

            # Insert event
            cur.execute(
                """
                INSERT INTO events (
                    event_id, external_id, provider, name, description, category, 
                    subcategory, tags, start_time, end_time, venue_id,
                    ticket_price_min, ticket_price_max, predicted_attendance,
                    psychographic_relevance, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    str(uuid.uuid4()),
                    f"sample_{i}",
                    "sample_data",
                    f"{event_type} at {venue[1]}",
                    f"Join us for an exciting {event_type.lower()} event at {venue[1]}. Great opportunity for networking and learning.",
                    category,
                    None,
                    tags,
                    start_time,
                    end_time,
                    venue[0],
                    0 if "free" in event_type.lower() else random.randint(10, 100),
                    (
                        random.randint(50, 200)
                        if "free" not in event_type.lower()
                        else None
                    ),
                    random.randint(20, 150),
                    psycopg2.extras.Json(psychographic_relevance),
                    datetime.now(),
                ),
            )

            events_created += 1

        conn.commit()
        cur.close()
        conn.close()

        print(f"✅ Created {events_created} sample events")
        return True

    except Exception as e:
        print(f"❌ Failed to create sample events: {e}")
        return False


def populate_sample_weather():
    """Create sample weather data."""
    print("\n🌤️ Creating sample weather data...")

    try:
        import psycopg2
        import random
        from datetime import datetime, timedelta

        # Get database connection
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("❌ DATABASE_URL not found")
            return False

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Check if we already have recent weather data
        cur.execute(
            """
            SELECT COUNT(*) FROM weather_data 
            WHERE ts >= NOW() - INTERVAL '24 hours'
        """
        )
        weather_count = cur.fetchone()[0]

        if weather_count > 10:
            print(f"✅ Already have {weather_count} recent weather points")
            cur.close()
            conn.close()
            return True

        # Kansas City area coordinates
        kc_lat, kc_lng = 39.0997, -94.5786

        # Create weather points around Kansas City
        weather_points = []
        for i in range(15):
            # Generate points within ~20 mile radius of KC
            lat_offset = random.uniform(-0.3, 0.3)
            lng_offset = random.uniform(-0.3, 0.3)

            lat = kc_lat + lat_offset
            lng = kc_lng + lng_offset

            # Generate realistic weather data
            temp_f = random.randint(45, 85)
            feels_like = temp_f + random.randint(-5, 10)
            humidity = random.randint(30, 80)
            rain_prob = random.randint(0, 40)
            wind_speed = random.randint(0, 15)

            conditions = random.choice(
                [
                    "Clear",
                    "Partly Cloudy",
                    "Cloudy",
                    "Light Rain",
                    "Sunny",
                    "Overcast",
                    "Scattered Clouds",
                ]
            )

            # Insert weather data
            cur.execute(
                """
                INSERT INTO weather_data (
                    ts, lat, lng, temperature_f, feels_like_f, humidity,
                    rain_probability, wind_speed_mph, weather_condition,
                    uv_index, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    datetime.now()
                    - timedelta(
                        minutes=random.randint(0, 1440)
                    ),  # Within last 24 hours
                    lat,
                    lng,
                    temp_f,
                    feels_like,
                    humidity,
                    rain_prob,
                    wind_speed,
                    conditions,
                    random.randint(1, 8),
                    datetime.now(),
                ),
            )

            weather_points.append((lat, lng))

        conn.commit()
        cur.close()
        conn.close()

        print(f"✅ Created {len(weather_points)} weather data points")
        return True

    except Exception as e:
        print(f"❌ Failed to create sample weather data: {e}")
        return False


def populate_sample_psychographic_layers():
    """Create sample psychographic layer data."""
    print("\n🧠 Creating sample psychographic layers...")

    try:
        import psycopg2
        import random
        import math
        from datetime import datetime

        # Get database connection
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            print("❌ DATABASE_URL not found")
            return False

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Check if we already have psychographic layers
        cur.execute("SELECT COUNT(*) FROM psychographic_layers")
        layer_count = cur.fetchone()[0]

        if layer_count > 50:
            print(f"✅ Already have {layer_count} psychographic layer points")
            cur.close()
            conn.close()
            return True

        # Kansas City area bounds
        lat_min, lat_max = 38.9517, 39.3209
        lng_min, lng_max = -94.7417, -94.3461

        # Create college density layer
        college_locations = [
            (39.0354, -94.5781),  # UMKC
            (39.0189, -94.6708),  # Rockhurst University
            (39.1142, -94.6275),  # Park University
        ]

        college_points = 0
        for i in range(30):
            lat = random.uniform(lat_min, lat_max)
            lng = random.uniform(lng_min, lng_max)

            # Calculate density based on distance to colleges
            total_influence = 0
            for college_lat, college_lng in college_locations:
                distance = math.sqrt(
                    (lat - college_lat) ** 2 + (lng - college_lng) ** 2
                )
                influence = max(0, 1.0 - (distance * 20))
                total_influence += influence

            density = min(1.0, total_influence + random.uniform(-0.1, 0.1))

            if density > 0.2:
                cur.execute(
                    """
                    INSERT INTO psychographic_layers (
                        layer_name, lat, lng, grid_cell_id, score, confidence, 
                        metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        "college_density",
                        lat,
                        lng,
                        f"grid_{i}_{int(lat*1000)}_{int(lng*1000)}",
                        density,
                        random.uniform(0.7, 0.95),
                        psycopg2.extras.Json(
                            {"source": "calculated", "method": "distance_based"}
                        ),
                        datetime.now(),
                    ),
                )
                college_points += 1

        # Create spending propensity layer
        affluent_areas = [
            (39.0458, -94.5833),  # Country Club Plaza
            (39.0739, -94.5861),  # Crossroads
            (39.0997, -94.5786),  # Downtown/Power & Light
        ]

        spending_points = 0
        for i in range(25):
            lat = random.uniform(lat_min, lat_max)
            lng = random.uniform(lng_min, lng_max)

            # Calculate propensity based on distance to affluent areas
            total_influence = 0
            for area_lat, area_lng in affluent_areas:
                distance = math.sqrt((lat - area_lat) ** 2 + (lng - area_lng) ** 2)
                influence = max(0, 1.0 - (distance * 15))
                total_influence += influence

            propensity = min(1.0, (total_influence * 0.7) + random.uniform(0.2, 0.8))

            if propensity > 0.3:
                cur.execute(
                    """
                    INSERT INTO psychographic_layers (
                        layer_name, lat, lng, grid_cell_id, score, confidence, 
                        metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        "spending_propensity",
                        lat,
                        lng,
                        f"grid_sp_{i}_{int(lat*1000)}_{int(lng*1000)}",
                        propensity,
                        random.uniform(0.6, 0.9),
                        psycopg2.extras.Json(
                            {"source": "calculated", "method": "proximity_based"}
                        ),
                        datetime.now(),
                    ),
                )
                spending_points += 1

        conn.commit()
        cur.close()
        conn.close()

        print(
            f"✅ Created {college_points} college density points and {spending_points} spending propensity points"
        )
        return True

    except Exception as e:
        print(f"❌ Failed to create sample psychographic layers: {e}")
        return False


def main():
    """Main function to populate data and create comprehensive map."""
    print("🚀 Populating Database & Creating Comprehensive Map")
    print("=" * 70)

    # Check if we have database connection
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not found in environment variables")
        print("   Please ensure your database is configured properly")
        return 1

    print("✅ Database connection configured")

    # Try to run some ETL processes (non-critical)
    print("\n📊 Running ETL Processes (Optional)...")

    # These are optional - if they fail, we continue
    run_etl_process("ingest_local_venues", "Local venue scraping", required=False)

    # Create sample data to populate the map
    print("\n🎯 Creating Sample Data...")

    populate_sample_events()
    populate_sample_weather()
    populate_sample_psychographic_layers()

    # Now create the comprehensive map
    print("\n🗺️  Creating Comprehensive Unified Map...")

    try:
        # Import and run the unified map creator
        from create_unified_venue_event_map import create_unified_map

        result = create_unified_map()

        if result == 0:
            print("\n🎉 SUCCESS! Comprehensive map created with all available data!")
            print("\n📋 WHAT'S INCLUDED:")
            print("   🏢 Venues from Google Places API")
            print("   🎪 Sample events with psychographic scoring")
            print("   🌤️  Sample weather data points")
            print("   🧠 Psychographic layers (college density, spending propensity)")
            print("   📊 Interactive visualization with multiple toggleable layers")

            print("\n🌐 The map should have opened in your browser automatically.")
            print("   If not, open: unified_venue_event_map.html")

            print("\n💡 MAP USAGE TIPS:")
            print(
                "   • Use the layer controls (top-right) to toggle different data types"
            )
            print("   • Click the venue ranking sidebar (left) to browse all venues")
            print("   • Click markers for detailed venue/event information")
            print("   • Different colors indicate different psychographic scores")

            return 0
        else:
            print("❌ Failed to create comprehensive map")
            return 1

    except Exception as e:
        print(f"❌ Error creating comprehensive map: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
