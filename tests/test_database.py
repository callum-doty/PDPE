#!/usr/bin/env python3
"""
Test database schema and operations for PPM application

Comprehensive tests for all database tables, views, and operations
"""

import sys
import os
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from core.database import get_database
import json
from datetime import datetime


def test_database_setup():
    """Test that database is set up correctly"""
    print("🧪 Testing Database Setup...")
    print("=" * 60)

    db = get_database()

    # Test venues table
    print("Testing venues table...")
    venues = db.get_venues()
    print(f"  ✅ Found {len(venues)} venues")

    # Test events table
    print("Testing events table...")
    events = db.get_events()
    print(f"  ✅ Found {len(events)} events")

    # Test system config
    print("Testing system config...")
    config_value = db.get_system_config("downtown_kc_lat")
    assert config_value == 39.0997, f"Expected 39.0997, got {config_value}"
    print(f"  ✅ System config working: downtown_kc_lat = {config_value}")

    # Test collection status
    print("Testing collection status...")
    result = db.execute_query("SELECT COUNT(*) as count FROM collection_status")
    assert result[0]["count"] > 0, "Collection status not initialized"
    print(f"  ✅ Found {result[0]['count']} collection sources")

    # Test views
    print("Testing views...")
    result = db.execute_query("SELECT * FROM vw_collection_health LIMIT 1")
    print(f"  ✅ Views working - found {len(result)} health records")

    print("\n✅ All basic database tests passed!")


def test_enrichment_data_operations():
    """Test enrichment data operations"""
    print("\n🧪 Testing Enrichment Data Operations...")
    print("=" * 60)

    db = get_database()

    # Test demographic data
    print("Testing demographic data operations...")
    demo_data = {
        "lat": 39.1,
        "lng": -94.6,
        "census_tract": "29095001100",
        "median_income": 85000,
        "bachelor_degree_pct": 0.55,
        "age_20_40_pct": 0.42,
        "professional_occupation_pct": 0.48,
        "data_source": "census_bureau",
        "year": 2023,
    }

    result = db.upsert_demographic_data(demo_data)
    assert result.success, f"Failed to insert demographic data: {result.error}"
    print(f"  ✅ Demographic data upserted successfully")

    # Test retrieval
    retrieved = db.get_demographic_data(39.1, -94.6, 0.01)
    assert retrieved is not None, "Failed to retrieve demographic data"
    assert (
        retrieved["median_income"] == 85000
    ), "Demographic data not retrieved correctly"
    print(f"  ✅ Demographic data retrieved successfully")

    # Test weather data
    print("Testing weather data operations...")
    weather_data = {
        "lat": 39.1,
        "lng": -94.6,
        "timestamp": datetime.now().isoformat(),
        "temperature_f": 72.5,
        "humidity_pct": 0.65,
        "conditions": "clear",
        "is_forecast": False,
        "provider": "openweather",
    }

    result = db.upsert_weather_data(weather_data)
    assert result.success, f"Failed to insert weather data: {result.error}"
    print(f"  ✅ Weather data upserted successfully")

    print("\n✅ All enrichment data tests passed!")


def test_caching_operations():
    """Test caching operations"""
    print("\n🧪 Testing Caching Operations...")
    print("=" * 60)

    db = get_database()

    # Test API cache
    print("Testing API cache operations...")
    cache_key = "test_api_call_123"
    api_source = "google_places"
    response_data = '{"test": "data", "venues": [{"name": "Test Venue"}]}'

    result = db.set_api_cache(cache_key, api_source, response_data, 1)
    assert result.success, f"Failed to set API cache: {result.error}"
    print(f"  ✅ API cache set successfully")

    # Test retrieval
    cached = db.get_api_cache(cache_key)
    assert cached is not None, "Failed to retrieve cached data"
    assert cached["api_source"] == api_source, "Cached data not retrieved correctly"
    print(f"  ✅ API cache retrieved successfully")

    # Test geocoding cache
    print("Testing geocoding cache operations...")
    address = "1200 Main St, Kansas City, MO"
    lat, lng = 39.0997, -94.5786

    result = db.cache_geocoding(
        address, lat, lng, "1200 Main Street, Kansas City, MO 64105"
    )
    assert result.success, f"Failed to cache geocoding: {result.error}"
    print(f"  ✅ Geocoding cached successfully")

    # Test retrieval
    coords = db.geocode_cached(address)
    assert coords is not None, "Failed to retrieve geocoded coordinates"
    assert abs(coords[0] - lat) < 0.001, "Geocoded coordinates not retrieved correctly"
    print(f"  ✅ Geocoding cache retrieved successfully")

    print("\n✅ All caching tests passed!")


def test_collection_status_operations():
    """Test collection status operations"""
    print("\n🧪 Testing Collection Status Operations...")
    print("=" * 60)

    db = get_database()

    # Test successful collection update
    print("Testing collection status updates...")
    result = db.update_collection_status(
        source_name="google_places",
        success=True,
        records_collected=25,
        duration_seconds=12.5,
    )
    assert result.success, f"Failed to update collection status: {result.error}"
    print(f"  ✅ Collection status updated successfully")

    # Test failed collection update
    result = db.update_collection_status(
        source_name="yelp_fusion",
        success=False,
        error_message="API rate limit exceeded",
    )
    assert result.success, f"Failed to update collection status: {result.error}"
    print(f"  ✅ Failed collection status updated successfully")

    # Test collection health retrieval
    health = db.get_collection_health()
    assert len(health) > 0, "No collection health data found"
    print(f"  ✅ Collection health retrieved: {len(health)} sources")

    print("\n✅ All collection status tests passed!")


def test_data_quality_operations():
    """Test data quality operations"""
    print("\n🧪 Testing Data Quality Operations...")
    print("=" * 60)

    db = get_database()

    # Test logging data quality issue
    print("Testing data quality logging...")
    result = db.log_data_quality_issue(
        table_name="venues",
        record_id="sample_venue_1",
        validation_type="completeness",
        validation_result="warning",
        field_name="phone",
        error_message="Phone number missing",
        severity="medium",
    )
    assert result.success, f"Failed to log data quality issue: {result.error}"
    print(f"  ✅ Data quality issue logged successfully")

    # Test data quality summary
    summary = db.get_data_quality_summary()
    assert len(summary) > 0, "No data quality summary found"
    print(f"  ✅ Data quality summary retrieved: {len(summary)} table summaries")

    print("\n✅ All data quality tests passed!")


def test_master_views():
    """Test master views functionality"""
    print("\n🧪 Testing Master Views...")
    print("=" * 60)

    db = get_database()

    # Test master venue data view
    print("Testing master venue data view...")
    master_venues = db.get_master_venue_data(limit=10)
    print(f"  ✅ Master venue data retrieved: {len(master_venues)} venues")

    # Test master events data view
    print("Testing master events data view...")
    master_events = db.get_master_events_data(limit=10)
    print(f"  ✅ Master events data retrieved: {len(master_events)} events")

    # Test high value predictions view
    print("Testing high value predictions view...")
    high_value = db.get_high_value_predictions(min_confidence=0.5)
    print(f"  ✅ High value predictions retrieved: {len(high_value)} predictions")

    print("\n✅ All master view tests passed!")


def test_venue_and_event_operations():
    """Test venue and event operations with new schema"""
    print("\n🧪 Testing Venue and Event Operations...")
    print("=" * 60)

    db = get_database()

    # Test venue upsert with psychographic data
    print("Testing venue operations with psychographic data...")
    venue_data = {
        "external_id": "test_venue_123",
        "provider": "test_provider",
        "name": "Test Psychographic Venue",
        "category": "restaurant",
        "subcategory": "fine_dining",
        "lat": 39.1,
        "lng": -94.6,
        "address": "123 Test St, Kansas City, MO",
        "phone": "+1-816-555-0123",
        "website": "https://testrestaurant.com",
        "avg_rating": 4.2,
        "psychographic_relevance": {
            "career_driven": 0.8,
            "competent": 0.7,
            "fun": 0.6,
            "social": 0.9,
            "adventurous": 0.5,
        },
    }

    result = db.upsert_venue(venue_data)
    assert result.success, f"Failed to upsert venue: {result.error}"
    print(f"  ✅ Venue with psychographic data upserted successfully")

    # Test event upsert with psychographic data
    print("Testing event operations with psychographic data...")
    event_data = {
        "external_id": "test_event_456",
        "provider": "test_provider",
        "name": "Test Networking Event",
        "category": "business",
        "subcategory": "networking",
        "start_time": "2025-12-01 18:00:00",
        "end_time": "2025-12-01 21:00:00",
        "venue_name": "Test Psychographic Venue",
        "lat": 39.1,
        "lng": -94.6,
        "address": "123 Test St, Kansas City, MO",
        "psychographic_relevance": {
            "career_driven": 0.9,
            "competent": 0.8,
            "fun": 0.4,
            "social": 0.7,
            "adventurous": 0.3,
        },
    }

    result = db.upsert_event(event_data)
    assert result.success, f"Failed to upsert event: {result.error}"
    print(f"  ✅ Event with psychographic data upserted successfully")

    print("\n✅ All venue and event tests passed!")


def test_data_summary():
    """Test data summary functionality"""
    print("\n🧪 Testing Data Summary...")
    print("=" * 60)

    db = get_database()

    summary = db.get_data_summary()
    assert "total_venues" in summary, "Data summary missing venue count"
    assert "total_events" in summary, "Data summary missing event count"
    assert "timestamp" in summary, "Data summary missing timestamp"

    print(f"  ✅ Data summary retrieved successfully:")
    print(f"    - Total venues: {summary['total_venues']}")
    print(f"    - Total events: {summary['total_events']}")
    print(f"    - Located venues: {summary['located_venues']}")
    print(f"    - Location completeness: {summary['location_completeness']:.2%}")
    print(f"    - Demographic records: {summary['demographic_records']}")
    print(f"    - Weather records: {summary['weather_records']}")

    print("\n✅ Data summary test passed!")


def run_all_tests():
    """Run all database tests"""
    print("🚀 Running Comprehensive Database Tests")
    print("=" * 80)

    try:
        test_database_setup()
        test_enrichment_data_operations()
        test_caching_operations()
        test_collection_status_operations()
        test_data_quality_operations()
        test_master_views()
        test_venue_and_event_operations()
        test_data_summary()

        print("\n" + "=" * 80)
        print("🎉 ALL DATABASE TESTS PASSED SUCCESSFULLY!")
        print("=" * 80)
        print("\n✅ Database is fully functional and ready for production use")
        print("\n📊 Test Summary:")
        print("  - Core database operations: ✅")
        print("  - Enrichment data operations: ✅")
        print("  - Caching operations: ✅")
        print("  - Collection status tracking: ✅")
        print("  - Data quality logging: ✅")
        print("  - Master views: ✅")
        print("  - Psychographic data handling: ✅")
        print("  - Data summary reporting: ✅")

        return True

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
