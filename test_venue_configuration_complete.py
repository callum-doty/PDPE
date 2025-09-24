#!/usr/bin/env python3
"""
Comprehensive Venue Configuration Test Script

Tests all 29 venues to ensure they are properly configured and can be scraped.
Validates database storage and categorization.
"""

import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple

# Import the scrapers
from src.etl.ingest_local_venues import (
    VENUE_SCRAPERS,
    scrape_venue_events,
    create_venue_from_config,
)
from src.etl.ingest_dynamic_venues import (
    DYNAMIC_VENUE_SCRAPERS,
    scrape_dynamic_venue_events,
)
from src.etl.venue_processing import process_venues_with_quality_checks
from src.etl.utils import get_db_conn

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("venue_test_results.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

# Expected venue categories mapping
EXPECTED_CATEGORIES = {
    # Major Venues
    "T-Mobile Center": "major_venue",
    "Uptown Theater": "major_venue",
    "Kauffman Center for the Performing Arts": "major_venue",
    "Starlight Theatre": "major_venue",
    "The Midland Theatre": "major_venue",
    "Knuckleheads Saloon": "major_venue",
    "Azura Amphitheater": "major_venue",
    # Entertainment Districts
    "Power & Light District": "entertainment_district",
    "Westport KC": "entertainment_district",
    "18th & Vine Jazz District": "entertainment_district",
    "Crossroads KC": "entertainment_district",
    # Shopping & Cultural
    "Country Club Plaza": "shopping_cultural",
    "Crown Center": "shopping_cultural",
    "Union Station Kansas City": "shopping_cultural",
    # Museums
    "Nelson-Atkins Museum of Art": "museum",
    "National WWI Museum": "museum",
    "Science City": "museum",
    # Theater
    "KC Repertory Theatre": "theater",
    "Unicorn Theatre": "theater",
    # Festival & City
    "Kansas City Parks & Rec": "festival_city",
    "City Market KC": "festival_city",
    "Boulevardia Festival": "festival_city",
    "Irish Fest KC": "festival_city",
    # Aggregators
    "Visit KC": "aggregator",
    "Do816": "aggregator",
    "The Pitch KC": "aggregator",
    "Kansas City Magazine Events": "aggregator",
    "Event KC": "aggregator",
    # Nightlife
    "Aura KC Nightclub": "nightlife",
}


def test_static_venue_configuration():
    """Test all static venue configurations."""
    logger.info("=" * 80)
    logger.info("TESTING STATIC VENUE CONFIGURATIONS")
    logger.info("=" * 80)

    results = {}

    for venue_key, venue_config in VENUE_SCRAPERS.items():
        venue_name = venue_config["name"]
        expected_category = EXPECTED_CATEGORIES.get(venue_name)
        actual_category = venue_config.get("category")

        logger.info(f"\nTesting: {venue_name}")
        logger.info(f"  URL: {venue_config['events_url']}")
        logger.info(f"  Expected Category: {expected_category}")
        logger.info(f"  Actual Category: {actual_category}")

        test_result = {
            "venue_name": venue_name,
            "venue_key": venue_key,
            "url": venue_config["events_url"],
            "expected_category": expected_category,
            "actual_category": actual_category,
            "category_match": expected_category == actual_category,
            "scrape_test": False,
            "events_found": 0,
            "venue_data_created": False,
            "errors": [],
        }

        # Test category match
        if not test_result["category_match"]:
            test_result["errors"].append(
                f"Category mismatch: expected {expected_category}, got {actual_category}"
            )

        # Test scraping (with timeout and error handling)
        try:
            logger.info(f"  Testing scraping...")
            events = scrape_venue_events(venue_config)
            test_result["scrape_test"] = True
            test_result["events_found"] = len(events)
            logger.info(f"  ✓ Scraping successful: {len(events)} events found")

            # Test venue data creation
            venue_data = create_venue_from_config(venue_config)
            if venue_data:
                test_result["venue_data_created"] = True
                logger.info(f"  ✓ Venue data creation successful")
            else:
                test_result["errors"].append("Failed to create venue data")
                logger.warning(f"  ✗ Venue data creation failed")

        except Exception as e:
            test_result["errors"].append(f"Scraping error: {str(e)}")
            logger.error(f"  ✗ Scraping failed: {e}")

        results[venue_name] = test_result

        # Small delay between tests
        time.sleep(1)

    return results


def test_dynamic_venue_configuration():
    """Test all dynamic venue configurations."""
    logger.info("=" * 80)
    logger.info("TESTING DYNAMIC VENUE CONFIGURATIONS")
    logger.info("=" * 80)

    results = {}

    for venue_key, venue_config in DYNAMIC_VENUE_SCRAPERS.items():
        venue_name = venue_config["name"]
        expected_category = EXPECTED_CATEGORIES.get(venue_name)
        actual_category = venue_config.get("category")

        logger.info(f"\nTesting: {venue_name}")
        logger.info(f"  URL: {venue_config['events_url']}")
        logger.info(f"  Expected Category: {expected_category}")
        logger.info(f"  Actual Category: {actual_category}")

        test_result = {
            "venue_name": venue_name,
            "venue_key": venue_key,
            "url": venue_config["events_url"],
            "expected_category": expected_category,
            "actual_category": actual_category,
            "category_match": expected_category == actual_category,
            "scrape_test": False,
            "events_found": 0,
            "errors": [],
        }

        # Test category match
        if not test_result["category_match"]:
            test_result["errors"].append(
                f"Category mismatch: expected {expected_category}, got {actual_category}"
            )

        # Test scraping (with timeout and error handling)
        # Note: Dynamic scraping requires Chrome/Selenium which may not be available in all environments
        try:
            logger.info(f"  Testing dynamic scraping...")
            events = scrape_dynamic_venue_events(venue_config)
            test_result["scrape_test"] = True
            test_result["events_found"] = len(events)
            logger.info(f"  ✓ Dynamic scraping successful: {len(events)} events found")

        except Exception as e:
            test_result["errors"].append(f"Dynamic scraping error: {str(e)}")
            logger.error(f"  ✗ Dynamic scraping failed: {e}")

        results[venue_name] = test_result

        # Longer delay between dynamic tests
        time.sleep(3)

    return results


def test_database_connectivity():
    """Test database connectivity and venue storage."""
    logger.info("=" * 80)
    logger.info("TESTING DATABASE CONNECTIVITY")
    logger.info("=" * 80)

    try:
        conn = get_db_conn()
        if not conn:
            logger.error("✗ Database connection failed")
            return False

        cur = conn.cursor()

        # Test venues table
        cur.execute("SELECT COUNT(*) FROM venues")
        venue_count = cur.fetchone()[0]
        logger.info(f"✓ Database connected - {venue_count} venues in database")

        # Test events table
        cur.execute("SELECT COUNT(*) FROM events")
        event_count = cur.fetchone()[0]
        logger.info(f"✓ Events table accessible - {event_count} events in database")

        # Test venue categories
        cur.execute(
            "SELECT category, COUNT(*) FROM venues GROUP BY category ORDER BY category"
        )
        categories = cur.fetchall()
        logger.info("✓ Venue categories in database:")
        for category, count in categories:
            logger.info(f"    {category}: {count} venues")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"✗ Database test failed: {e}")
        return False


def test_venue_processing_pipeline():
    """Test the venue processing pipeline with sample data."""
    logger.info("=" * 80)
    logger.info("TESTING VENUE PROCESSING PIPELINE")
    logger.info("=" * 80)

    try:
        # Create sample venue data
        sample_venues = [
            {
                "external_id": "test_venue_1",
                "provider": "test_provider",
                "name": "Test Venue 1",
                "category": "major_venue",
                "subcategory": "concert_hall",
                "lat": 39.0997,
                "lng": -94.5786,
                "address": "123 Test St, Kansas City, MO",
                "phone": "555-123-4567",
                "website": "https://test-venue.com",
            },
            {
                "external_id": "test_venue_2",
                "provider": "test_provider",
                "name": "Test Venue 2",
                "category": "entertainment_district",
                "subcategory": "district",
                "lat": 39.1012,
                "lng": -94.5844,
                "address": "456 Test Ave, Kansas City, MO",
                "website": "https://test-venue-2.com",
            },
        ]

        # Process through pipeline
        processed_venues, quality_report = process_venues_with_quality_checks(
            sample_venues
        )

        logger.info(f"✓ Venue processing pipeline test successful")
        logger.info(f"    Input venues: {quality_report['total_input']}")
        logger.info(f"    Output venues: {quality_report['total_output']}")
        logger.info(f"    Validation errors: {quality_report['validation_errors']}")
        logger.info(f"    Duplicates removed: {quality_report['duplicates_removed']}")

        return True

    except Exception as e:
        logger.error(f"✗ Venue processing pipeline test failed: {e}")
        return False


def generate_test_report(
    static_results: Dict, dynamic_results: Dict, db_test: bool, pipeline_test: bool
):
    """Generate comprehensive test report."""
    logger.info("=" * 80)
    logger.info("COMPREHENSIVE TEST REPORT")
    logger.info("=" * 80)

    all_results = {**static_results, **dynamic_results}

    # Overall statistics
    total_venues = len(all_results)
    successful_scrapes = sum(1 for r in all_results.values() if r["scrape_test"])
    category_matches = sum(1 for r in all_results.values() if r["category_match"])
    venues_with_events = sum(1 for r in all_results.values() if r["events_found"] > 0)
    total_events = sum(r["events_found"] for r in all_results.values())

    logger.info(f"\nOVERALL STATISTICS:")
    logger.info(f"  Total venues tested: {total_venues}")
    logger.info(
        f"  Successful scrapes: {successful_scrapes}/{total_venues} ({successful_scrapes/total_venues*100:.1f}%)"
    )
    logger.info(
        f"  Category matches: {category_matches}/{total_venues} ({category_matches/total_venues*100:.1f}%)"
    )
    logger.info(
        f"  Venues with events: {venues_with_events}/{total_venues} ({venues_with_events/total_venues*100:.1f}%)"
    )
    logger.info(f"  Total events found: {total_events}")
    logger.info(f"  Database connectivity: {'✓' if db_test else '✗'}")
    logger.info(f"  Processing pipeline: {'✓' if pipeline_test else '✗'}")

    # Category breakdown
    logger.info(f"\nCATEGORY BREAKDOWN:")
    category_stats = {}
    for result in all_results.values():
        category = result["expected_category"]
        if category not in category_stats:
            category_stats[category] = {"total": 0, "successful": 0, "events": 0}
        category_stats[category]["total"] += 1
        if result["scrape_test"]:
            category_stats[category]["successful"] += 1
        category_stats[category]["events"] += result["events_found"]

    for category, stats in sorted(category_stats.items()):
        success_rate = (
            stats["successful"] / stats["total"] * 100 if stats["total"] > 0 else 0
        )
        logger.info(
            f"  {category}: {stats['successful']}/{stats['total']} successful ({success_rate:.1f}%), {stats['events']} events"
        )

    # Failed venues
    failed_venues = [
        r for r in all_results.values() if not r["scrape_test"] or r["errors"]
    ]
    if failed_venues:
        logger.info(f"\nVENUES WITH ISSUES:")
        for result in failed_venues:
            logger.info(f"  {result['venue_name']}:")
            for error in result["errors"]:
                logger.info(f"    - {error}")

    # Success summary
    all_tests_passed = (
        successful_scrapes == total_venues
        and category_matches == total_venues
        and db_test
        and pipeline_test
    )

    logger.info(f"\n" + "=" * 80)
    if all_tests_passed:
        logger.info("🎉 ALL TESTS PASSED! All venues are properly configured.")
    else:
        logger.info("⚠️  SOME TESTS FAILED. Review the issues above.")
    logger.info("=" * 80)

    return all_tests_passed


def main():
    """Run comprehensive venue configuration tests."""
    logger.info("Starting comprehensive venue configuration tests...")
    logger.info(f"Test started at: {datetime.now()}")

    # Test database connectivity first
    db_test = test_database_connectivity()

    # Test venue processing pipeline
    pipeline_test = test_venue_processing_pipeline()

    # Test static venues
    static_results = test_static_venue_configuration()

    # Test dynamic venues
    dynamic_results = test_dynamic_venue_configuration()

    # Generate comprehensive report
    all_passed = generate_test_report(
        static_results, dynamic_results, db_test, pipeline_test
    )

    logger.info(f"Test completed at: {datetime.now()}")
    logger.info("Test results saved to: venue_test_results.log")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
