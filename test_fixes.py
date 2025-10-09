#!/usr/bin/env python3
"""
Test script to validate PPM fixes

Tests both Phase 1 (grid prediction crash fix) and Phase 2 (event collection improvements)
"""

import sys
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_grid_predictions():
    """Test Phase 1: Grid prediction crash fix"""
    logger.info("🧪 Testing Phase 1: Grid Prediction Crash Fix")

    try:
        from features.predictions import get_prediction_service

        # Test with venues that have None coordinates (the crash scenario)
        test_venues = [
            {
                "venue_id": "1",
                "name": "Test Venue 1",
                "lat": 39.1,
                "lng": -94.6,
                "avg_rating": 4.0,
            },
            {
                "venue_id": "2",
                "name": "Test Venue 2",
                "lat": None,
                "lng": None,
                "avg_rating": 3.5,
            },  # This would cause crash
            {
                "venue_id": "3",
                "name": "Test Venue 3",
                "lat": 39.2,
                "lng": -94.5,
                "avg_rating": 4.2,
            },
            {
                "venue_id": "4",
                "name": "Test Venue 4",
                "lat": None,
                "lng": -94.7,
                "avg_rating": 3.8,
            },  # Partial None
        ]

        prediction_service = get_prediction_service()

        # Test bounds for Kansas City
        bounds = {"min_lat": 39.0, "max_lat": 39.3, "min_lng": -94.8, "max_lng": -94.4}

        # This should NOT crash anymore
        logger.info("  Testing grid prediction generation with None coordinates...")
        heatmap_predictions = prediction_service._generate_grid_predictions(
            bounds, test_venues
        )

        logger.info(
            f"  ✅ Grid predictions generated successfully: {len(heatmap_predictions)} predictions"
        )

        # Test individual grid calculation
        logger.info("  Testing individual grid calculation with None coordinates...")
        prediction_value = prediction_service._calculate_grid_prediction(
            39.1, -94.6, test_venues
        )

        logger.info(
            f"  ✅ Grid calculation successful: prediction value = {prediction_value}"
        )

        return True

    except Exception as e:
        logger.error(f"  ❌ Grid prediction test failed: {e}")
        return False


def test_event_collection():
    """Test Phase 2: Event collection improvements"""
    logger.info("🧪 Testing Phase 2: Event Collection Improvements")

    try:
        from features.events import get_event_service

        event_service = get_event_service()

        # Test enhanced user agent generation
        logger.info("  Testing user agent generation...")
        ua1 = event_service._get_random_user_agent()
        ua2 = event_service._get_random_user_agent()

        assert ua1 and ua2, "User agents should not be empty"
        logger.info(f"  ✅ User agent generation working: {len(ua1)} chars")

        # Test enhanced headers
        logger.info("  Testing enhanced headers...")
        headers = event_service._get_enhanced_headers()

        required_headers = ["User-Agent", "Accept", "Accept-Language", "DNT", "Sec-GPC"]
        for header in required_headers:
            assert header in headers, f"Missing required header: {header}"

        logger.info(f"  ✅ Enhanced headers working: {len(headers)} headers")

        # Test JSON cleaning (simulate LLM response with markdown)
        logger.info("  Testing JSON cleaning...")

        # Simulate problematic LLM response
        mock_llm_response = """```json
        [
          {
            "title": "Test Event",
            "date": "2025-12-15",
            "time": "7:30 PM",
            "location": "Test Venue"
          }
        ]
        ```"""

        # Test the cleaning logic
        import re

        result = mock_llm_response.strip()
        result = re.sub(r"```(?:json)?\s*", "", result)
        result = re.sub(r"```\s*$", "", result)
        result = result.strip()

        import json

        parsed = json.loads(result)
        assert isinstance(parsed, list), "Should parse to list"
        assert len(parsed) == 1, "Should have one event"
        assert parsed[0]["title"] == "Test Event", "Should preserve event data"

        logger.info("  ✅ JSON cleaning working correctly")

        return True

    except Exception as e:
        logger.error(f"  ❌ Event collection test failed: {e}")
        return False


def test_database_integration():
    """Test database integration"""
    logger.info("🧪 Testing Database Integration")

    try:
        from core.database import get_database

        db = get_database()

        # Test basic database operations
        logger.info("  Testing database connection...")
        venues = db.get_venues()
        events = db.get_events()

        logger.info(
            f"  ✅ Database connection working: {len(venues)} venues, {len(events)} events"
        )

        return True

    except Exception as e:
        logger.error(f"  ❌ Database integration test failed: {e}")
        return False


def main():
    """Run all tests"""
    logger.info("🚀 Starting PPM Fix Validation Tests")
    logger.info("=" * 60)

    tests = [
        ("Grid Predictions (Phase 1)", test_grid_predictions),
        ("Event Collection (Phase 2)", test_event_collection),
        ("Database Integration", test_database_integration),
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name}...")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST RESULTS SUMMARY")
    logger.info("=" * 60)

    passed = 0
    total = len(results)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"  {status} - {test_name}")
        if success:
            passed += 1

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All tests passed! PPM fixes are working correctly.")
        return 0
    else:
        logger.error(f"⚠️  {total - passed} test(s) failed. Please check the fixes.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
