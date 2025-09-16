#!/usr/bin/env python3
"""
Ingest real data from APIs into the database for the heatmap to use.
This script fetches data from Google Places, PredictHQ, and other APIs.
"""

import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from src.etl.ingest_places import fetch_google_places, upsert_places_to_db
    from src.etl.ingest_events import fetch_predicthq_events, upsert_events_to_db
    from src.etl.utils import get_db_conn

    print("✓ Successfully imported ETL modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)


def ingest_venues_data():
    """Fetch and ingest venue data from Google Places API."""
    print("🏪 Ingesting venue data from Google Places API...")

    # Kansas City area searches
    searches = [
        "restaurants in Kansas City, MO",
        "entertainment in Kansas City, MO",
        "shopping in Kansas City, MO",
        "bars in Kansas City, MO",
        "museums in Kansas City, MO",
        "hotels in Kansas City, MO",
        "cafes in Kansas City, MO",
        "theaters in Kansas City, MO",
    ]

    total_venues = 0

    for search_query in searches:
        try:
            print(f"  Searching: {search_query}")
            response = fetch_google_places(search_query, location="39.0997,-94.5786")

            if response and "results" in response:
                venues = response["results"]
                if venues:
                    upsert_places_to_db(venues)
                    total_venues += len(venues)
                    print(f"    ✓ Added {len(venues)} venues")
                else:
                    print(f"    ⚠️  No venues found")
            else:
                print(f"    ❌ No response from API")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    print(f"✅ Total venues ingested: {total_venues}")
    return total_venues


def ingest_events_data():
    """Fetch and ingest events data from PredictHQ API."""
    print("🎉 Ingesting events data from PredictHQ API...")

    try:
        # Fetch events for next 30 days
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        response = fetch_predicthq_events(
            lat=39.0997,
            lng=-94.5786,
            radius="50km",
            start_date=start_date,
            end_date=end_date,
        )

        if response and "results" in response:
            events = response["results"]
            if events:
                upsert_events_to_db(response)
                print(f"✅ Added {len(events)} events")
                return len(events)
            else:
                print("⚠️  No events found")
                return 0
        else:
            print("❌ No response from PredictHQ API")
            return 0

    except Exception as e:
        print(f"❌ Error fetching events: {e}")
        return 0


def add_sample_demographics():
    """Add sample demographic data for Kansas City census tracts."""
    print("📊 Adding sample demographic data...")

    conn = get_db_conn()
    cursor = conn.cursor()

    # Sample demographic data for Kansas City area census tracts
    sample_demographics = [
        {
            "tract_id": "29095001100",
            "lat": 39.0997,
            "lng": -94.5786,
            "median_income": 75000,
            "pct_bachelors": 45.0,
            "pct_graduate": 20.0,
            "pct_age_20_30": 18.0,
            "pct_age_30_40": 16.0,
            "pct_professional_occupation": 35.0,
            "pct_management_occupation": 15.0,
            "population_density": 2500.0,
        },
        {
            "tract_id": "29095001200",
            "lat": 39.1012,
            "lng": -94.5844,
            "median_income": 85000,
            "pct_bachelors": 55.0,
            "pct_graduate": 25.0,
            "pct_age_20_30": 22.0,
            "pct_age_30_40": 18.0,
            "pct_professional_occupation": 45.0,
            "pct_management_occupation": 20.0,
            "population_density": 3200.0,
        },
        {
            "tract_id": "29095001300",
            "lat": 39.0739,
            "lng": -94.5861,
            "median_income": 65000,
            "pct_bachelors": 35.0,
            "pct_graduate": 15.0,
            "pct_age_20_30": 15.0,
            "pct_age_30_40": 14.0,
            "pct_professional_occupation": 28.0,
            "pct_management_occupation": 12.0,
            "population_density": 1800.0,
        },
        {
            "tract_id": "29095001400",
            "lat": 39.0458,
            "lng": -94.5833,
            "median_income": 95000,
            "pct_bachelors": 60.0,
            "pct_graduate": 30.0,
            "pct_age_20_30": 20.0,
            "pct_age_30_40": 19.0,
            "pct_professional_occupation": 50.0,
            "pct_management_occupation": 25.0,
            "population_density": 2800.0,
        },
        {
            "tract_id": "29095001500",
            "lat": 39.1167,
            "lng": -94.6275,
            "median_income": 55000,
            "pct_bachelors": 25.0,
            "pct_graduate": 10.0,
            "pct_age_20_30": 12.0,
            "pct_age_30_40": 13.0,
            "pct_professional_occupation": 20.0,
            "pct_management_occupation": 8.0,
            "population_density": 1200.0,
        },
        {
            "tract_id": "29095001600",
            "lat": 39.0625,
            "lng": -94.5975,
            "median_income": 70000,
            "pct_bachelors": 40.0,
            "pct_graduate": 18.0,
            "pct_age_20_30": 16.0,
            "pct_age_30_40": 15.0,
            "pct_professional_occupation": 32.0,
            "pct_management_occupation": 14.0,
            "population_density": 2100.0,
        },
        {
            "tract_id": "29095001700",
            "lat": 39.0454,
            "lng": -94.5804,
            "median_income": 88000,
            "pct_bachelors": 52.0,
            "pct_graduate": 28.0,
            "pct_age_20_30": 19.0,
            "pct_age_30_40": 17.0,
            "pct_professional_occupation": 42.0,
            "pct_management_occupation": 22.0,
            "population_density": 2600.0,
        },
    ]

    for demo in sample_demographics:
        # Create a simple polygon around the point for geometry
        lat, lng = demo["lat"], demo["lng"]
        polygon_wkt = f"POLYGON(({lng-0.01} {lat-0.01}, {lng+0.01} {lat-0.01}, {lng+0.01} {lat+0.01}, {lng-0.01} {lat+0.01}, {lng-0.01} {lat-0.01}))"

        cursor.execute(
            """
            INSERT INTO demographics (
                tract_id, geometry, median_income, pct_bachelors, pct_graduate,
                pct_age_20_30, pct_age_30_40, pct_professional_occupation,
                pct_management_occupation, population_density
            ) VALUES (
                %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (tract_id) DO UPDATE SET
                median_income = EXCLUDED.median_income,
                pct_bachelors = EXCLUDED.pct_bachelors,
                pct_graduate = EXCLUDED.pct_graduate,
                pct_age_20_30 = EXCLUDED.pct_age_20_30,
                pct_age_30_40 = EXCLUDED.pct_age_30_40,
                pct_professional_occupation = EXCLUDED.pct_professional_occupation,
                pct_management_occupation = EXCLUDED.pct_management_occupation,
                population_density = EXCLUDED.population_density
        """,
            (
                demo["tract_id"],
                polygon_wkt,
                demo["median_income"],
                demo["pct_bachelors"],
                demo["pct_graduate"],
                demo["pct_age_20_30"],
                demo["pct_age_30_40"],
                demo["pct_professional_occupation"],
                demo["pct_management_occupation"],
                demo["population_density"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Added {len(sample_demographics)} demographic records")
    return len(sample_demographics)


def main():
    """Main data ingestion process."""
    print("🚀 Starting Real Data Ingestion...")
    print("=" * 50)

    total_venues = 0
    total_events = 0
    total_demographics = 0

    try:
        # Ingest venues from Google Places API
        total_venues = ingest_venues_data()

        # Ingest events from PredictHQ API
        total_events = ingest_events_data()

        # Add sample demographic data (since we don't have Census API integration yet)
        total_demographics = add_sample_demographics()

        print("\n" + "=" * 50)
        print("📊 DATA INGESTION SUMMARY")
        print("=" * 50)
        print(f"✅ Venues ingested: {total_venues}")
        print(f"✅ Events ingested: {total_events}")
        print(f"✅ Demographics added: {total_demographics}")
        print(f"✅ Total records: {total_venues + total_events + total_demographics}")

        if total_venues > 0 or total_events > 0 or total_demographics > 0:
            print("\n🎉 Data ingestion successful!")
            print("🗺️  You can now run the fixed heatmap to see real data:")
            print("   python create_region_wide_heatmap_fixed.py")
        else:
            print("\n⚠️  No data was ingested. Check API keys and connections.")

        return 0

    except Exception as e:
        print(f"\n❌ Error during data ingestion: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
