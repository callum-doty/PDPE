#!/usr/bin/env python3
"""
Test script to verify all APIs are working with real data and no synthetic fallbacks.
This script will fail if any API returns synthetic/mock data.
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path for imports
sys.path.append("src")

from etl.ingest_foot_traffic import fetch_foot_traffic
from etl.ingest_weather import fetch_current_weather, fetch_weather_forecast
from etl.ingest_events import fetch_predicthq_events
from etl.ingest_places import fetch_google_places


def test_foot_traffic_api():
    """Test that foot traffic API returns real BestTime data, not synthetic"""
    print("🔍 Testing Foot Traffic API (BestTime)...")

    try:
        # Use a sample Google Place ID for testing (this should be replaced with a real venue ID from your API results)
        test_venue_id = "ChIJl5npr173wIcRolGqauYlhVU"  # Sample Kansas City venue ID
        result = fetch_foot_traffic(test_venue_id, "restaurant")

        if result is None:
            print(
                "✅ PASS: Foot traffic API properly returns None when no data available (no synthetic fallback)"
            )
            return True

        # Check that it's NOT synthetic data
        data_source = result.get("data_source", "")
        if data_source == "mock_generator":
            print("❌ FAIL: Foot traffic API returned synthetic data (mock_generator)")
            return False

        if data_source == "besttime_api":
            print("✅ PASS: Foot traffic API returned real BestTime data")
            return True
        else:
            print(f"⚠️  WARNING: Unexpected data source: {data_source}")
            return False

    except Exception as e:
        print(f"❌ FAIL: Foot traffic API error: {e}")
        return False


def test_weather_api():
    """Test that weather API returns real OpenWeatherMap data"""
    print("🔍 Testing Weather API (OpenWeatherMap)...")

    try:
        # Kansas City coordinates
        lat, lng = 39.0997, -94.5786

        # Test current weather
        current_weather = fetch_current_weather(lat, lng)
        if not current_weather:
            print("❌ FAIL: No current weather data returned")
            return False

        # Check for real weather data fields
        if "main" not in current_weather or "weather" not in current_weather:
            print("❌ FAIL: Weather API response missing expected fields")
            return False

        print("✅ PASS: Weather API returned real OpenWeatherMap data")

        # Test forecast
        forecast = fetch_weather_forecast(lat, lng)
        if not forecast or "list" not in forecast:
            print("❌ FAIL: No weather forecast data returned")
            return False

        print("✅ PASS: Weather forecast API returned real data")
        return True

    except Exception as e:
        print(f"❌ FAIL: Weather API error: {e}")
        return False


def test_events_api():
    """Test that events API returns real PredictHQ data"""
    print("🔍 Testing Events API (PredictHQ)...")

    try:
        events_data = fetch_predicthq_events()

        if not events_data:
            print("❌ FAIL: No events data returned")
            return False

        # Check for PredictHQ response structure
        if "results" not in events_data:
            print("❌ FAIL: Events API response missing 'results' field")
            return False

        results = events_data["results"]
        if len(results) == 0:
            print("⚠️  WARNING: Events API returned 0 events (may be normal)")
            return True
        else:
            print(f"✅ PASS: Events API returned {len(results)} real events")
            return True

    except Exception as e:
        print(f"❌ FAIL: Events API error: {e}")
        return False


def test_places_api():
    """Test that Google Places API returns real data"""
    print("🔍 Testing Google Places API...")

    try:
        # Test with Kansas City search
        places_data = fetch_google_places("restaurants in Kansas City, MO")

        if not places_data:
            print("❌ FAIL: No places data returned")
            return False

        # Check for Google Places response structure
        if "results" not in places_data:
            print("❌ FAIL: Places API response missing 'results' field")
            return False

        results = places_data["results"]
        if len(results) == 0:
            print("❌ FAIL: Places API returned 0 results")
            return False
        else:
            print(f"✅ PASS: Places API returned {len(results)} real venues")
            return True

    except Exception as e:
        print(f"❌ FAIL: Places API error: {e}")
        return False


def test_database_connection():
    """Test database connection"""
    print("🔍 Testing Database Connection...")

    try:
        from etl.utils import get_db_conn

        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result and result[0] == 1:
            print("✅ PASS: Database connection successful")
            return True
        else:
            print("❌ FAIL: Database connection test failed")
            return False

    except Exception as e:
        print(f"❌ FAIL: Database connection error: {e}")
        return False


def main():
    """Run all API tests"""
    print("=" * 60)
    print("🚀 TESTING ALL APIs FOR REAL DATA (NO SYNTHETIC FALLBACKS)")
    print("=" * 60)
    print()

    # Track test results
    tests = [
        ("Database Connection", test_database_connection),
        ("Foot Traffic API", test_foot_traffic_api),
        ("Weather API", test_weather_api),
        ("Events API", test_events_api),
        ("Google Places API", test_places_api),
    ]

    results = {}

    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        results[test_name] = test_func()
        print()

    # Summary
    print("=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(tests)

    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"{test_name:<25} {status}")
        if passed_test:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 SUCCESS: All APIs are working with real data!")
        print("🚫 No synthetic fallbacks detected!")
        return 0
    else:
        print(f"\n💥 FAILURE: {total - passed} API(s) failed or using synthetic data")
        print("🔧 Please fix the failing APIs before proceeding")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
