#!/usr/bin/env python3
"""
Test script for LLM-based venue scraping functionality.

This script tests the new LLM venue scraping system to ensure it works correctly
before deploying to production.
"""

import logging
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from features.venues import get_venue_service

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def test_openai_connection():
    """Test OpenAI API connection"""
    print("🔧 Testing OpenAI API connection...")

    venue_service = get_venue_service()

    if not venue_service.openai_client:
        print("❌ OpenAI client not initialized. Check CHATGPT_API_KEY in .env file.")
        return False

    print("✅ OpenAI client initialized successfully")
    return True


def test_single_venue_scraping():
    """Test scraping a single venue"""
    print("\n🏛️ Testing single venue scraping...")

    venue_service = get_venue_service()

    # Test with a simple static venue
    test_venue_config = {
        "name": "Nelson-Atkins Museum of Art",
        "url": "https://www.nelson-atkins.org/",
        "category": "museum",
        "scrape_type": "static",
    }

    try:
        # Fetch HTML
        html = venue_service._fetch_static_html(test_venue_config["url"])
        if not html:
            print(f"❌ Failed to fetch HTML from {test_venue_config['url']}")
            return False

        print(f"✅ Successfully fetched HTML ({len(html)} characters)")

        # Extract venue info using LLM
        venue_info = venue_service._extract_venue_with_llm(html, test_venue_config)
        if not venue_info:
            print("❌ Failed to extract venue information using LLM")
            return False

        print("✅ Successfully extracted venue information:")
        for key, value in venue_info.items():
            print(f"   {key}: {value}")

        return True

    except Exception as e:
        print(f"❌ Error during single venue test: {e}")
        return False


def test_venue_storage():
    """Test venue data storage"""
    print("\n💾 Testing venue data storage...")

    venue_service = get_venue_service()

    # Create test venue data
    from features.venues import VenueData

    test_venue = VenueData(
        external_id="test_llm_venue_001",
        provider="llm_scraper",
        name="Test LLM Venue",
        description="A test venue for LLM scraping validation",
        category="test_venue",
        subcategory="testing",
        website="https://example.com",
        address="123 Test St, Kansas City, MO 64111",
        phone="+1-816-555-0123",
        lat=39.0997,
        lng=-94.5786,
        avg_rating=None,
        psychographic_scores={
            "career_driven": 0.2,
            "competent": 0.3,
            "fun": 0.8,
            "social": 0.6,
            "adventurous": 0.4,
        },
        scraped_at=datetime.now(),
        source_type="llm_scraper",
    )

    try:
        # Test storage
        success = venue_service._validate_and_store_venue(test_venue)
        if success:
            print("✅ Successfully stored test venue")

            # Verify retrieval
            stored_venue = venue_service.get_venue_by_id("test_llm_venue_001")
            if stored_venue:
                print("✅ Successfully retrieved stored venue")
                return True
            else:
                print("⚠️  Venue stored but could not retrieve")
                return False
        else:
            print("❌ Failed to store test venue")
            return False

    except Exception as e:
        print(f"❌ Error during venue storage test: {e}")
        return False


def test_limited_venue_collection():
    """Test collecting from a few venues"""
    print("\n🌐 Testing limited venue collection (3 venues)...")

    venue_service = get_venue_service()

    # Temporarily modify the venue list to test only a few venues
    original_venues = venue_service.kc_venues.copy()

    # Test with just 3 static venues
    test_venues = {
        "nelson_atkins": original_venues["nelson_atkins"],
        "union_station": original_venues["union_station"],
        "kauffman_center": original_venues["kauffman_center"],
    }

    venue_service.kc_venues = test_venues

    try:
        result = venue_service.collect_from_scraped_sources()

        # Restore original venues
        venue_service.kc_venues = original_venues

        if result.success:
            print(f"✅ Successfully collected venues: {result.message}")
            print(f"   Venues processed: {result.data}")
            return True
        else:
            print(f"❌ Venue collection failed: {result.error}")
            return False

    except Exception as e:
        # Restore original venues
        venue_service.kc_venues = original_venues
        print(f"❌ Error during limited venue collection: {e}")
        return False


def main():
    """Run all tests"""
    print("🚀 Starting LLM Venue Scraping Tests")
    print("=" * 50)

    tests = [
        ("OpenAI Connection", test_openai_connection),
        ("Single Venue Scraping", test_single_venue_scraping),
        ("Venue Storage", test_venue_storage),
        ("Limited Collection", test_limited_venue_collection),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        print("-" * 30)

        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")

    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! LLM venue scraping is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the issues above.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
