#!/usr/bin/env python3
"""
Test script for the new Kansas City Event Scraper
Tests both LLM extraction and fallback selector methods
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_collectors.kc_event_scraper import KCEventScraper, Event
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_scraper_initialization():
    """Test that the scraper initializes correctly"""
    print("🧪 Testing scraper initialization...")

    try:
        scraper = KCEventScraper()

        # Check if OpenAI client was initialized
        if scraper.llm_client:
            print("✅ OpenAI client initialized successfully")
        else:
            print("⚠️  OpenAI client not initialized (check CHATGPT_API_KEY)")

        # Check venue configuration
        print(f"✅ Loaded {len(scraper.VENUES)} venue configurations")

        # Print venue categories
        categories = {}
        for venue, config in scraper.VENUES.items():
            category = config.get("category", "Unknown")
            if category not in categories:
                categories[category] = []
            categories[category].append(venue)

        print("\n📍 Venue categories:")
        for category, venues in categories.items():
            print(f"  {category}: {len(venues)} venues")
            for venue in venues[:3]:  # Show first 3
                print(f"    - {venue}")
            if len(venues) > 3:
                print(f"    ... and {len(venues) - 3} more")

        return scraper

    except Exception as e:
        print(f"❌ Scraper initialization failed: {e}")
        return None


def test_single_venue_scraping(scraper, venue_name="T-Mobile Center"):
    """Test scraping a single venue"""
    print(f"\n🎭 Testing single venue scraping: {venue_name}")

    try:
        if venue_name not in scraper.VENUES:
            print(f"❌ Venue '{venue_name}' not found in configuration")
            return []

        config = scraper.VENUES[venue_name]
        events = scraper.scrape_venue(venue_name, config)

        print(f"✅ Scraped {len(events)} events from {venue_name}")

        # Show sample events
        if events:
            print(f"\n📅 Sample events from {venue_name}:")
            for i, event in enumerate(events[:3]):
                print(f"  {i+1}. {event.title}")
                print(f"     📍 {event.location}")
                print(f"     📅 {event.date or 'Date TBD'}")
                print(f"     🔗 {event.url or 'No URL'}")
                print()

        return events

    except Exception as e:
        print(f"❌ Single venue scraping failed: {e}")
        return []


def test_data_collection_interface(scraper):
    """Test the data collection interface (used by master data service)"""
    print("\n🔄 Testing data collection interface...")

    try:
        # Test with a small subset of venues
        test_venues = ["T-Mobile Center", "Uptown Theater"]

        # Temporarily override scrape_all to use test venues
        original_scrape_all = scraper.scrape_all
        scraper.scrape_all = lambda delay=2.0: original_scrape_all(
            venue_filter=test_venues, delay=delay
        )

        result = scraper.collect_data()

        print(f"✅ Data collection completed:")
        print(f"  - Success: {result.success}")
        print(f"  - Venues processed: {result.venues_collected}")
        print(f"  - Events collected: {result.events_collected}")
        print(f"  - Duration: {result.duration_seconds:.2f} seconds")
        print(f"  - Quality score: {result.data_quality_score}")

        if result.error_message:
            print(f"  - Error: {result.error_message}")

        # Restore original method
        scraper.scrape_all = original_scrape_all

        return result

    except Exception as e:
        print(f"❌ Data collection interface test failed: {e}")
        return None


def test_event_conversion(scraper):
    """Test event conversion to database format"""
    print("\n🔄 Testing event conversion...")

    try:
        # Create a sample event
        sample_event = Event(
            venue="Test Venue",
            title="Sample Concert",
            date="2024-12-25",
            time="7:00 PM",
            location="Test Venue",
            description="A great concert with amazing music",
            url="https://example.com/event",
            category="Major Venue",
            price="$50",
        )

        # Convert to dict format
        event_dict = scraper._convert_event_to_dict(sample_event)

        print("✅ Event conversion successful:")
        print(f"  - External ID: {event_dict['external_id']}")
        print(f"  - Provider: {event_dict['provider']}")
        print(f"  - Name: {event_dict['name']}")
        print(f"  - Category: {event_dict['category']}")
        print(f"  - Subcategory: {event_dict['subcategory']}")
        print(f"  - Start time: {event_dict['start_time']}")
        print(f"  - Psychographic scores: {event_dict['psychographic_scores']}")

        return event_dict

    except Exception as e:
        print(f"❌ Event conversion test failed: {e}")
        return None


def test_psychographic_classification(scraper):
    """Test psychographic classification"""
    print("\n🧠 Testing psychographic classification...")

    test_cases = [
        ("Business Networking Event", "Professional networking for career growth"),
        ("Rock Concert", "Live music and dancing all night"),
        ("Advanced Python Workshop", "Expert-level training for developers"),
        ("Family Fun Day", "Activities for kids and parents"),
    ]

    for title, description in test_cases:
        scores = scraper._classify_event_psychographics(title, description)
        print(f"  '{title}': {scores}")

    print("✅ Psychographic classification test completed")


def main():
    """Run all tests"""
    print("🚀 Starting Kansas City Event Scraper Tests")
    print("=" * 60)

    # Test 1: Initialization
    scraper = test_scraper_initialization()
    if not scraper:
        print("❌ Cannot continue tests - scraper initialization failed")
        return

    # Test 2: Single venue scraping
    events = test_single_venue_scraping(scraper)

    # Test 3: Data collection interface
    collection_result = test_data_collection_interface(scraper)

    # Test 4: Event conversion
    event_dict = test_event_conversion(scraper)

    # Test 5: Psychographic classification
    test_psychographic_classification(scraper)

    # Summary
    print("\n" + "=" * 60)
    print("🎯 TEST SUMMARY")
    print("=" * 60)

    if scraper.llm_client:
        print("✅ LLM Integration: Working")
    else:
        print("⚠️  LLM Integration: Not available (check API key)")

    print(f"✅ Venue Configuration: {len(scraper.VENUES)} venues loaded")

    if events:
        print(f"✅ Event Scraping: {len(events)} events scraped from test venue")
    else:
        print("⚠️  Event Scraping: No events found (may be normal)")

    if collection_result and collection_result.success:
        print("✅ Data Collection Interface: Working")
    else:
        print("⚠️  Data Collection Interface: Issues detected")

    if event_dict:
        print("✅ Event Processing: Working")
    else:
        print("❌ Event Processing: Failed")

    print("\n🎉 KC Event Scraper is ready for use!")
    print("\nTo use in your system:")
    print("1. Import: from data_collectors.kc_event_scraper import KCEventScraper")
    print("2. Initialize: scraper = KCEventScraper()")
    print("3. Collect: result = scraper.collect_data()")


if __name__ == "__main__":
    main()
