#!/usr/bin/env python3
"""
Test script to verify the Streamlit app fixes for venue API and data scraping.
This script tests the core functionality without running the full Streamlit interface.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def test_database_connection():
    """Test that database connection returns dictionary-like objects"""
    print("🔍 Testing database connection...")

    try:
        from shared.database.connection import get_database_connection

        with get_database_connection() as db:
            # Test a simple query
            result = db.execute_query("SELECT 1 as test_column")

            if result:
                print(f"✅ Database connection successful")
                print(f"   Result type: {type(result)}")
                print(f"   First row type: {type(result[0])}")
                print(f"   Can access by key: {result[0]['test_column']}")
                return True
            else:
                print("❌ No results returned from test query")
                return False

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def test_venue_collector():
    """Test that venue collector returns proper dataclass objects"""
    print("\n🏢 Testing venue collector...")

    try:
        from features.venues.collectors.venue_collector import VenueCollector

        collector = VenueCollector()

        # Test the collect_all_venues method (returns list)
        results = collector.collect_all_venues()

        print(f"✅ Venue collector executed successfully")
        print(f"   Results type: {type(results)}")

        if isinstance(results, list):
            print(f"   Number of results: {len(results)}")
            if results:
                first_result = results[0]
                print(f"   First result type: {type(first_result)}")
                print(f"   Has success attribute: {hasattr(first_result, 'success')}")
                print(f"   Success value: {first_result.success}")
                print(f"   Venues collected: {first_result.venues_collected}")
                print(f"   Events collected: {first_result.events_collected}")

        # Test the collect_data method (returns single result)
        single_result = collector.collect_data()
        print(f"   Single result type: {type(single_result)}")
        print(f"   Has success attribute: {hasattr(single_result, 'success')}")

        return True

    except Exception as e:
        print(f"❌ Venue collector test failed: {e}")
        return False


def test_venue_data_access():
    """Test accessing venue data from database"""
    print("\n🗺️ Testing venue data access...")

    try:
        from shared.database.connection import get_database_connection

        with get_database_connection() as db:
            # Test venue query similar to what Streamlit uses
            venues = db.execute_query(
                """
                SELECT name, lat, lng, category, avg_rating
                FROM venues 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                LIMIT 5
            """
            )

            print(f"✅ Venue query executed successfully")
            print(f"   Found {len(venues)} venues")

            if venues:
                venue = venues[0]
                print(f"   First venue type: {type(venue)}")
                print(f"   Can access name: {venue['name']}")
                print(f"   Can access lat: {venue['lat']}")
                print(f"   Can access lng: {venue['lng']}")

            return True

    except Exception as e:
        print(f"❌ Venue data access test failed: {e}")
        return False


def test_event_data_access():
    """Test accessing event data from database"""
    print("\n🎭 Testing event data access...")

    try:
        from shared.database.connection import get_database_connection
        from datetime import datetime, timedelta

        with get_database_connection() as db:
            # Test event query similar to what Streamlit uses
            start_date = datetime.now()
            end_date = start_date + timedelta(days=7)

            events = db.execute_query(
                """
                SELECT name, lat, lng, start_time, venue_name
                FROM master_events_data 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                AND start_time BETWEEN ? AND ?
                LIMIT 5
            """,
                (start_date, end_date),
            )

            print(f"✅ Event query executed successfully")
            print(f"   Found {len(events)} events")

            if events:
                event = events[0]
                print(f"   First event type: {type(event)}")
                print(f"   Can access name: {event['name']}")
                print(f"   Can access venue_name: {event['venue_name']}")

            return True

    except Exception as e:
        print(f"❌ Event data access test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 Testing Streamlit App Fixes")
    print("=" * 50)

    tests = [
        test_database_connection,
        test_venue_collector,
        test_venue_data_access,
        test_event_data_access,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")

    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The Streamlit app fixes should work correctly.")
        print("\nYou can now run the Streamlit app with:")
        print("   streamlit run app/main.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
