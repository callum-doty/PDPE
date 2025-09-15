#!/usr/bin/env python3
"""
Fixed real data ingestion script with proper environment loading and API testing.
"""

import sys
import os
from pathlib import Path
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))


def get_db_conn():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:@localhost:5432/ppm")
    return psycopg2.connect(db_url)


def test_google_places_api():
    """Test Google Places API connectivity."""
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("❌ GOOGLE_PLACES_API_KEY not found")
        return False

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "key": api_key,
        "query": "restaurants in Kansas City, MO",
        "location": "39.0997,-94.5786",
        "radius": 50000,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()

        if response.status_code == 200 and data.get("status") == "OK":
            print(
                f"✅ Google Places API working - found {len(data.get('results', []))} results"
            )
            return data.get("results", [])
        else:
            print(
                f"❌ Google Places API error: {data.get('error_message', data.get('status', 'Unknown error'))}"
            )
            return False
    except Exception as e:
        print(f"❌ Google Places API request failed: {e}")
        return False


def fetch_google_places_venues():
    """Fetch venues from Google Places API."""
    print("🏪 Fetching venues from Google Places API...")

    # Different search queries for comprehensive coverage
    searches = [
        "restaurants Kansas City MO",
        "bars Kansas City MO",
        "coffee shops Kansas City MO",
        "entertainment Kansas City MO",
        "museums Kansas City MO",
        "hotels Kansas City MO",
        "shopping Kansas City MO",
        "gyms Kansas City MO",
        "coworking spaces Kansas City MO",
        "theaters Kansas City MO",
    ]

    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    all_venues = []

    for query in searches:
        print(f"  Searching: {query}")

        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            "key": api_key,
            "query": query,
            "location": "39.0997,-94.5786",
            "radius": 25000,  # 25km radius
        }

        try:
            response = requests.get(url, params=params, timeout=15)
            data = response.json()

            if response.status_code == 200 and data.get("status") == "OK":
                venues = data.get("results", [])
                all_venues.extend(venues)
                print(f"    ✅ Found {len(venues)} venues")
            else:
                print(
                    f"    ❌ Error: {data.get('error_message', data.get('status', 'Unknown'))}"
                )

        except Exception as e:
            print(f"    ❌ Request failed: {e}")

    # Remove duplicates based on place_id
    unique_venues = {}
    for venue in all_venues:
        place_id = venue.get("place_id")
        if place_id and place_id not in unique_venues:
            unique_venues[place_id] = venue

    print(f"✅ Total unique venues found: {len(unique_venues)}")
    return list(unique_venues.values())


def calculate_psychographic_score(venue):
    """Calculate psychographic relevance score for a venue."""
    venue_types = venue.get("types", [])
    rating = venue.get("rating", 0)
    price_level = venue.get("price_level", 0)

    # Psychographic scoring weights
    type_scores = {
        "restaurant": 0.7,
        "bar": 0.6,
        "night_club": 0.8,
        "cafe": 0.7,
        "gym": 0.6,
        "spa": 0.5,
        "shopping_mall": 0.4,
        "movie_theater": 0.6,
        "museum": 0.7,
        "art_gallery": 0.8,
        "library": 0.6,
        "university": 0.9,
        "school": 0.8,
        "hospital": 0.3,
        "bank": 0.4,
        "real_estate_agency": 0.5,
        "lawyer": 0.7,
        "accounting": 0.6,
        "lodging": 0.5,
        "tourist_attraction": 0.6,
        "amusement_park": 0.7,
        "bowling_alley": 0.6,
        "casino": 0.5,
        "establishment": 0.5,
        "food": 0.7,
        "point_of_interest": 0.5,
    }

    # Calculate base score from venue types
    base_score = 0.3  # Default base score
    for venue_type in venue_types:
        if venue_type in type_scores:
            base_score = max(base_score, type_scores[venue_type])

    # Adjust for rating (higher rating = more appealing)
    rating_multiplier = 1.0
    if rating >= 4.5:
        rating_multiplier = 1.2
    elif rating >= 4.0:
        rating_multiplier = 1.1
    elif rating >= 3.5:
        rating_multiplier = 1.0
    elif rating >= 3.0:
        rating_multiplier = 0.9
    else:
        rating_multiplier = 0.8

    # Adjust for price level (moderate prices preferred for target demographic)
    price_multiplier = 1.0
    if price_level == 2 or price_level == 3:  # Moderate to expensive
        price_multiplier = 1.1
    elif price_level == 4:  # Very expensive
        price_multiplier = 0.9
    elif price_level == 1:  # Inexpensive
        price_multiplier = 0.95

    final_score = min(1.0, base_score * rating_multiplier * price_multiplier)
    return round(final_score, 3)


def upsert_venues_to_db(venues):
    """Insert venues into database."""
    if not venues:
        print("⚠️  No venues to insert")
        return 0

    print(f"💾 Inserting {len(venues)} venues into database...")

    conn = get_db_conn()
    cur = conn.cursor()

    rows = []
    for venue in venues:
        # Calculate psychographic relevance
        psychographic_score = calculate_psychographic_score(venue)
        psychographic_relevance = {
            "career_driven": psychographic_score,
            "competent": psychographic_score * 0.9,
            "fun": psychographic_score * 1.1,
        }

        # Extract location
        location = venue.get("geometry", {}).get("location", {})
        lat = location.get("lat")
        lng = location.get("lng")

        if not lat or not lng:
            continue

        row = (
            venue.get("place_id"),
            "google_places",
            venue.get("name", "Unknown"),
            ",".join(venue.get("types", [])),
            venue.get("types", ["unknown"])[0] if venue.get("types") else "unknown",
            venue.get("price_level"),
            venue.get("rating"),
            venue.get("user_ratings_total"),
            lat,
            lng,
            venue.get("formatted_address", ""),
            venue.get("formatted_phone_number", ""),
            venue.get("website", ""),
            json.dumps(psychographic_relevance),
        )
        rows.append(row)

    if not rows:
        print("⚠️  No valid venues to insert")
        return 0

    sql = """
    INSERT INTO venues (
        external_id, provider, name, category, subcategory, price_tier, 
        avg_rating, review_count, lat, lng, address, phone, website, 
        psychographic_relevance, geo, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (external_id) DO UPDATE SET
        name = EXCLUDED.name,
        category = EXCLUDED.category,
        subcategory = EXCLUDED.subcategory,
        price_tier = EXCLUDED.price_tier,
        avg_rating = EXCLUDED.avg_rating,
        review_count = EXCLUDED.review_count,
        lat = EXCLUDED.lat,
        lng = EXCLUDED.lng,
        address = EXCLUDED.address,
        phone = EXCLUDED.phone,
        website = EXCLUDED.website,
        psychographic_relevance = EXCLUDED.psychographic_relevance,
        geo = ST_SetSRID(ST_MakePoint(EXCLUDED.lng, EXCLUDED.lat), 4326),
        updated_at = NOW()
    """

    # Prepare template with geo calculation
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW(), NOW())"

    # Flatten rows to include lng, lat for geo calculation
    flattened_rows = []
    for row in rows:
        flattened_row = row + (row[9], row[8])  # Add lng, lat for geo
        flattened_rows.append(flattened_row)

    execute_values(cur, sql, flattened_rows, template=template)
    conn.commit()

    inserted_count = cur.rowcount
    cur.close()
    conn.close()

    print(f"✅ Successfully inserted/updated {inserted_count} venues")
    return inserted_count


def fetch_predicthq_events():
    """Fetch events from PredictHQ API."""
    print("🎉 Fetching events from PredictHQ API...")

    api_key = os.getenv("PREDICT_HQ_API_KEY")
    if not api_key:
        print("❌ PREDICT_HQ_API_KEY not found")
        return []

    url = "https://api.predicthq.com/v1/events/"
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

    # Get events for next 30 days in Kansas City area
    start_date = datetime.now().strftime("%Y-%m-%d")
    end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

    params = {
        "location.origin": "39.0997,-94.5786",
        "location.scale": "50km",
        "active.gte": start_date,
        "active.lte": end_date,
        "limit": 100,
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        data = response.json()

        if response.status_code == 200:
            events = data.get("results", [])
            print(f"✅ Found {len(events)} events")
            return events
        else:
            print(f"❌ PredictHQ API error: {data.get('message', 'Unknown error')}")
            return []

    except Exception as e:
        print(f"❌ PredictHQ API request failed: {e}")
        return []


def upsert_events_to_db(events):
    """Insert events into database."""
    if not events:
        print("⚠️  No events to insert")
        return 0

    print(f"💾 Inserting {len(events)} events into database...")

    conn = get_db_conn()
    cur = conn.cursor()

    rows = []
    for event in events:
        # Calculate psychographic relevance based on event category
        category = event.get("category", "").lower()
        labels = [label.lower() for label in event.get("labels", [])]

        # Psychographic scoring for events
        base_score = 0.5
        if any(
            keyword in category or any(keyword in label for label in labels)
            for keyword in [
                "business",
                "conference",
                "networking",
                "professional",
                "career",
            ]
        ):
            base_score = 0.9
        elif any(
            keyword in category or any(keyword in label for label in labels)
            for keyword in ["music", "festival", "entertainment", "nightlife", "social"]
        ):
            base_score = 0.8
        elif any(
            keyword in category or any(keyword in label for label in labels)
            for keyword in ["cultural", "art", "museum", "education"]
        ):
            base_score = 0.7

        psychographic_relevance = {
            "career_driven": base_score,
            "competent": base_score * 0.9,
            "fun": base_score * 1.1,
        }

        # Extract location
        location = event.get("location", [])
        if not location or len(location) < 2:
            continue

        lat, lng = location[1], location[0]  # PredictHQ uses [lng, lat] format

        row = (
            event.get("id"),
            "predicthq",
            event.get("title", "Unknown Event"),
            event.get("description", ""),
            event.get("category", "unknown"),
            event.get("labels", []),
            event.get("start"),
            event.get("end"),
            event.get("predicted_event_spend"),
            event.get("predicted_event_spend"),
            json.dumps(psychographic_relevance),
        )
        rows.append(row)

    if not rows:
        print("⚠️  No valid events to insert")
        return 0

    sql = """
    INSERT INTO events (
        external_id, provider, name, description, category, tags, 
        start_time, end_time, ticket_price_min, ticket_price_max,
        psychographic_relevance, created_at, updated_at
    ) VALUES %s
    ON CONFLICT (external_id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        category = EXCLUDED.category,
        tags = EXCLUDED.tags,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time,
        ticket_price_min = EXCLUDED.ticket_price_min,
        ticket_price_max = EXCLUDED.ticket_price_max,
        psychographic_relevance = EXCLUDED.psychographic_relevance,
        updated_at = NOW()
    """

    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
    execute_values(cur, sql, rows, template=template)
    conn.commit()

    inserted_count = cur.rowcount
    cur.close()
    conn.close()

    print(f"✅ Successfully inserted/updated {inserted_count} events")
    return inserted_count


def add_sample_demographics():
    """Add sample demographic data for Kansas City census tracts."""
    print("📊 Adding demographic data...")

    conn = get_db_conn()
    cursor = conn.cursor()

    # Enhanced demographic data for Kansas City area
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
    ]

    for demo in sample_demographics:
        lat, lng = demo["lat"], demo["lng"]
        polygon_wkt = f"POLYGON(({lng-0.01} {lat-0.01}, {lng+0.01} {lat-0.01}, {lng+0.01} {lat+0.01}, {lng-0.01} {lat+0.01}, {lng-0.01} {lat-0.01}))"

        cursor.execute(
            """
            INSERT INTO demographics (
                tract_id, geometry, median_income, median_income_z, pct_bachelors, pct_graduate,
                pct_age_20_30, pct_age_30_40, pct_age_20_40, population, population_density,
                pct_professional_occupation, pct_management_occupation, updated_at
            ) VALUES (
                %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            ) ON CONFLICT (tract_id) DO UPDATE SET
                median_income = EXCLUDED.median_income,
                median_income_z = EXCLUDED.median_income_z,
                pct_bachelors = EXCLUDED.pct_bachelors,
                pct_graduate = EXCLUDED.pct_graduate,
                pct_age_20_30 = EXCLUDED.pct_age_20_30,
                pct_age_30_40 = EXCLUDED.pct_age_30_40,
                pct_age_20_40 = EXCLUDED.pct_age_20_40,
                population = EXCLUDED.population,
                population_density = EXCLUDED.population_density,
                pct_professional_occupation = EXCLUDED.pct_professional_occupation,
                pct_management_occupation = EXCLUDED.pct_management_occupation,
                updated_at = NOW()
        """,
            (
                demo["tract_id"],
                polygon_wkt,
                demo["median_income"],
                (demo["median_income"] - 70000) / 20000,  # Z-score normalization
                demo["pct_bachelors"],
                demo["pct_graduate"],
                demo["pct_age_20_30"],
                demo["pct_age_30_40"],
                demo["pct_age_20_30"]
                + demo["pct_age_30_40"],  # Combined 20-40 age group
                int(demo["population_density"] * 2),  # Estimated population
                demo["population_density"],
                demo["pct_professional_occupation"],
                demo["pct_management_occupation"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Added {len(sample_demographics)} demographic records")
    return len(sample_demographics)


def main():
    """Main data ingestion process."""
    print("🚀 Starting Enhanced Real Data Ingestion...")
    print("=" * 60)

    total_venues = 0
    total_events = 0
    total_demographics = 0

    try:
        # Test API connectivity first
        print("🔍 Testing API connectivity...")
        test_result = test_google_places_api()
        if test_result is False:
            print(
                "⚠️  Google Places API test failed, but continuing with other data sources..."
            )

        # Fetch and ingest venues
        venues = fetch_google_places_venues()
        if venues:
            total_venues = upsert_venues_to_db(venues)

        # Fetch and ingest events
        events = fetch_predicthq_events()
        if events:
            total_events = upsert_events_to_db(events)

        # Add demographic data
        total_demographics = add_sample_demographics()

        print("\n" + "=" * 60)
        print("📊 ENHANCED DATA INGESTION SUMMARY")
        print("=" * 60)
        print(f"✅ Venues ingested: {total_venues}")
        print(f"✅ Events ingested: {total_events}")
        print(f"✅ Demographics added: {total_demographics}")
        print(f"✅ Total records: {total_venues + total_events + total_demographics}")

        if total_venues > 0 or total_events > 0:
            print("\n🎉 Real data ingestion successful!")
            print("🗺️  Ready to generate comprehensive venue map with real data!")
        else:
            print("\n⚠️  Limited data ingested. Check API keys and connections.")

        return 0

    except Exception as e:
        print(f"\n❌ Error during data ingestion: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
