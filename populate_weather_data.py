#!/usr/bin/env python3
"""
Standalone Weather Data Population Script

This script populates weather data for venue locations to improve data completeness
in the venue-centric architecture.
"""

import sys
import os
from pathlib import Path
import requests
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.etl.utils import get_db_conn

# Weather API configuration
WEATHER_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
WEATHER_API_BASE_URL = "https://api.openweathermap.org/data/2.5"


def safe_request(url, params=None, timeout=10):
    """Make a safe HTTP request with error handling."""
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def fetch_current_weather(lat, lng):
    """Fetch current weather data for a specific location."""
    if not WEATHER_API_KEY:
        print("❌ No weather API key found - cannot fetch real weather data")
        return None

    url = f"{WEATHER_API_BASE_URL}/weather"
    params = {
        "lat": lat,
        "lon": lng,
        "appid": WEATHER_API_KEY,
        "units": "imperial",  # Fahrenheit, mph
    }

    return safe_request(url, params=params)


def process_weather_data(weather_data, lat, lng):
    """Process weather API response into database format."""
    if not weather_data:
        return None

    main = weather_data.get("main", {})
    weather = weather_data.get("weather", [{}])[0]
    wind = weather_data.get("wind", {})

    processed_data = {
        "ts": datetime.utcnow(),
        "lat": lat,
        "lng": lng,
        "temperature_f": main.get("temp"),
        "feels_like_f": main.get("feels_like"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed_mph": wind.get("speed"),
        "wind_direction": wind.get("deg"),
        "weather_condition": weather.get("main", "").lower(),
        "weather_description": weather.get("description", ""),
        "visibility": weather_data.get("visibility"),
        "uv_index": None,
        "rain_probability": None,
        "precipitation_mm": 0,
    }

    # Handle precipitation data if available
    if "rain" in weather_data:
        rain_1h = weather_data["rain"].get("1h", 0)
        processed_data["precipitation_mm"] = rain_1h
    elif "snow" in weather_data:
        snow_1h = weather_data["snow"].get("1h", 0)
        processed_data["precipitation_mm"] = snow_1h

    return processed_data


def create_weather_table_if_not_exists(db_conn):
    """Verify weather_data table exists (it should already exist)."""
    cur = db_conn.cursor()

    try:
        # Check if table exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'weather_data'
            );
        """
        )

        table_exists = cur.fetchone()[0]

        if table_exists:
            print("✅ Weather table exists")
        else:
            print("⚠️  Weather table does not exist - this is unexpected")
            # Don't create a new table, just report the issue

        db_conn.commit()

    except Exception as e:
        db_conn.rollback()
        print(f"❌ Error checking weather table: {e}")
        raise
    finally:
        cur.close()


def upsert_weather_to_db(weather_records, db_conn):
    """Insert or update weather data in the database."""
    if not weather_records:
        return

    cur = db_conn.cursor()

    try:
        for record in weather_records:
            # Convert datetime to string if needed
            ts = record["ts"]
            if isinstance(ts, datetime):
                ts = ts.isoformat()

            # Match existing table schema - only include columns that exist
            cur.execute(
                """
                INSERT INTO weather_data (
                    ts, lat, lng, temperature_f, feels_like_f, humidity,
                    wind_speed_mph, weather_condition, rain_probability, 
                    precipitation_mm, uv_index
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """,
                (
                    ts,
                    record["lat"],
                    record["lng"],
                    record["temperature_f"],
                    record["feels_like_f"],
                    record["humidity"],
                    record["wind_speed_mph"],
                    record["weather_condition"],
                    record["rain_probability"],
                    record["precipitation_mm"],
                    record["uv_index"],
                ),
            )

        db_conn.commit()
        print(f"✅ Successfully upserted {len(weather_records)} weather records")

    except Exception as e:
        db_conn.rollback()
        print(f"❌ Error upserting weather data: {e}")
        raise
    finally:
        cur.close()


def get_venue_locations(db_conn):
    """Get unique venue locations for weather data collection."""
    cur = db_conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute(
            """
            SELECT DISTINCT 
                ROUND(lat::numeric, 4) as lat,
                ROUND(lng::numeric, 4) as lng
            FROM venues 
            WHERE lat IS NOT NULL AND lng IS NOT NULL
            ORDER BY lat, lng
        """
        )

        locations = [(row["lat"], row["lng"]) for row in cur.fetchall()]
        print(f"📍 Found {len(locations)} unique venue locations")
        return locations

    except Exception as e:
        print(f"❌ Error fetching venue locations: {e}")
        return []
    finally:
        cur.close()


def populate_weather_data():
    """Main function to populate weather data for all venue locations."""
    print("🌤️  POPULATING WEATHER DATA FOR VENUES")
    print("=" * 60)

    # Connect to database
    print("1. Connecting to database...")
    db_conn = get_db_conn()
    if not db_conn:
        print("❌ Database connection failed")
        return
    print("✅ Database connected")

    # Create weather table if needed
    print("2. Creating/verifying weather table...")
    create_weather_table_if_not_exists(db_conn)

    # Get venue locations
    print("3. Fetching venue locations...")
    locations = get_venue_locations(db_conn)

    if not locations:
        print("❌ No venue locations found")
        return

    # Fetch weather data for each location
    print("4. Fetching weather data...")
    all_weather_records = []

    for i, (lat, lng) in enumerate(locations, 1):
        try:
            print(
                f"   Fetching weather for location {i}/{len(locations)}: ({lat}, {lng})"
            )

            # Fetch current weather
            weather_data = fetch_current_weather(float(lat), float(lng))
            if weather_data:
                processed_weather = process_weather_data(
                    weather_data, float(lat), float(lng)
                )
                if processed_weather:
                    all_weather_records.append(processed_weather)

            # Rate limiting - don't overwhelm the API
            if WEATHER_API_KEY and i % 10 == 0:
                import time

                time.sleep(1)  # 1 second delay every 10 requests

        except Exception as e:
            print(f"   ❌ Error fetching weather for ({lat}, {lng}): {e}")
            continue

    # Save weather data to database
    if all_weather_records:
        print(f"5. Saving {len(all_weather_records)} weather records to database...")
        upsert_weather_to_db(all_weather_records, db_conn)

        # Show sample data
        sample = all_weather_records[0]
        print(
            f"   Sample: {sample['weather_condition']} at {sample['temperature_f']:.1f}°F"
        )
        print(
            f"   Humidity: {sample['humidity']}%, Wind: {sample['wind_speed_mph']:.1f} mph"
        )
    else:
        print("❌ No weather records to save")

    db_conn.close()

    print(f"\n✅ Weather data population complete!")
    return len(all_weather_records)


if __name__ == "__main__":
    print("WEATHER DATA POPULATION TOOL")
    print("=" * 60)
    print("Populating weather data to improve venue data completeness...")
    print()

    try:
        records_added = populate_weather_data()

        if records_added:
            print(f"\n🎉 SUCCESS! Added {records_added} weather records")
            print("Weather data will now be available in venue-centric maps")
        else:
            print("\n⚠️  No weather records were added")

    except Exception as e:
        print(f"❌ Weather population failed: {e}")
        import traceback

        traceback.print_exc()
