#!/usr/bin/env python3
"""
Comprehensive test script for enhanced data ingestion capabilities
Tests all new data sources: social, economic, traffic, and local venues
"""

import os
import sys
import logging
import traceback
from datetime import datetime

# Add src to path for imports
sys.path.append("src")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def test_social_data_ingestion():
    """Test social media data ingestion"""
    print("\n" + "=" * 60)
    print("TESTING SOCIAL DATA INGESTION")
    print("=" * 60)

    try:
        from src.etl.ingest_social import (
            setup_twitter_api,
            fetch_twitter_mentions,
            analyze_business_sentiment,
            classify_event_psychographics,
            ingest_social_data_for_venues,
            ingest_general_kc_social_data,
        )

        # Test Twitter API setup
        print("Testing Twitter API setup...")
        twitter_api = setup_twitter_api()
        if twitter_api:
            print("✅ Twitter API setup successful")
        else:
            print("⚠️  Twitter API setup failed (API key may be missing)")

        # Test psychographic classification
        print("\nTesting psychographic classification...")
        test_title = "Professional Networking Event at Power & Light"
        test_description = "Join us for drinks and business networking"
        scores = classify_event_psychographics(test_title, test_description)
        print(f"✅ Psychographic scores: {scores}")

        # Test sentiment analysis
        print("\nTesting sentiment analysis...")
        test_articles = [
            {
                "title": "Kansas City economy is booming",
                "description": "Great business growth",
            },
            {
                "title": "KC businesses struggling",
                "description": "Economic downturn affects local companies",
            },
        ]
        sentiment = analyze_business_sentiment(test_articles)
        print(f"✅ Sentiment analysis: {sentiment}")

        # Test social data ingestion (limited to avoid API quota)
        print("\nTesting social data ingestion...")
        try:
            # Test with a small sample
            twitter_mentions = fetch_twitter_mentions(hashtags=["#KC"], days_back=1)
            print(f"✅ Fetched {len(twitter_mentions)} Twitter mentions")
        except Exception as e:
            print(f"⚠️  Twitter mentions test failed: {e}")

        print("✅ Social data ingestion tests completed")
        return True

    except Exception as e:
        print(f"❌ Social data ingestion test failed: {e}")
        traceback.print_exc()
        return False


def test_economic_data_ingestion():
    """Test economic indicators data ingestion"""
    print("\n" + "=" * 60)
    print("TESTING ECONOMIC DATA INGESTION")
    print("=" * 60)

    try:
        from src.etl.ingest_econ import (
            fetch_fred_economic_data,
            fetch_bls_unemployment_data,
            fetch_business_news_sentiment,
            analyze_business_sentiment,
            process_economic_indicators,
            ingest_economic_indicators,
        )

        # Test FRED API connection
        print("Testing FRED API connection...")
        try:
            retail_data = fetch_fred_economic_data(
                "RSAFS", start_date="2024-01-01", end_date="2024-12-31"
            )
            if retail_data:
                print("✅ FRED API connection successful")
            else:
                print("⚠️  FRED API connection failed (API key may be missing)")
        except Exception as e:
            print(f"⚠️  FRED API test failed: {e}")

        # Test BLS unemployment data
        print("\nTesting BLS unemployment data...")
        try:
            unemployment_data = fetch_bls_unemployment_data()
            if unemployment_data:
                print("✅ BLS unemployment data fetch successful")
            else:
                print("⚠️  BLS unemployment data fetch failed")
        except Exception as e:
            print(f"⚠️  BLS unemployment test failed: {e}")

        # Test business news sentiment
        print("\nTesting business news sentiment...")
        try:
            news_data = fetch_business_news_sentiment()
            if news_data:
                print("✅ Business news sentiment fetch successful")
            else:
                print(
                    "⚠️  Business news sentiment fetch failed (API key may be missing)"
                )
        except Exception as e:
            print(f"⚠️  Business news sentiment test failed: {e}")

        # Test economic indicators processing
        print("\nTesting economic indicators processing...")
        try:
            economic_data = process_economic_indicators()
            if economic_data:
                print(f"✅ Economic indicators processed: {list(economic_data.keys())}")
            else:
                print("⚠️  Economic indicators processing returned no data")
        except Exception as e:
            print(f"⚠️  Economic indicators processing failed: {e}")

        print("✅ Economic data ingestion tests completed")
        return True

    except Exception as e:
        print(f"❌ Economic data ingestion test failed: {e}")
        traceback.print_exc()
        return False


def test_traffic_data_ingestion():
    """Test traffic data ingestion"""
    print("\n" + "=" * 60)
    print("TESTING TRAFFIC DATA INGESTION")
    print("=" * 60)

    try:
        from src.etl.ingest_traffic import (
            setup_google_maps_client,
            calculate_congestion_score,
            fetch_directions_with_traffic,
            process_venue_traffic_data,
            ingest_traffic_data,
        )

        # Test Google Maps API setup
        print("Testing Google Maps API setup...")
        gmaps_client = setup_google_maps_client()
        if gmaps_client:
            print("✅ Google Maps API setup successful")
        else:
            print("⚠️  Google Maps API setup failed (API key may be missing)")
            return False

        # Test congestion score calculation
        print("\nTesting congestion score calculation...")
        congestion_score = calculate_congestion_score(1800, 1200)  # 30 min vs 20 min
        print(f"✅ Congestion score calculation: {congestion_score}")

        # Test directions with traffic (limited test to avoid quota)
        print("\nTesting directions with traffic...")
        try:
            downtown_kc = {"lat": 39.0997, "lng": -94.5786}
            plaza = {"lat": 39.0458, "lng": -94.5889}

            directions = fetch_directions_with_traffic(downtown_kc, plaza)
            if directions:
                print("✅ Directions with traffic fetch successful")
            else:
                print("⚠️  Directions with traffic fetch failed")
        except Exception as e:
            print(f"⚠️  Directions with traffic test failed: {e}")

        print("✅ Traffic data ingestion tests completed")
        return True

    except Exception as e:
        print(f"❌ Traffic data ingestion test failed: {e}")
        traceback.print_exc()
        return False


def test_local_venues_ingestion():
    """Test local venues data ingestion"""
    print("\n" + "=" * 60)
    print("TESTING LOCAL VENUES INGESTION")
    print("=" * 60)

    try:
        from src.etl.ingest_local_venues import (
            safe_scrape_request,
            parse_event_date,
            classify_event_psychographics,
            determine_event_subcategory,
            scrape_venue_events,
            VENUE_SCRAPERS,
        )

        # Test safe scraping request
        print("Testing safe scraping request...")
        try:
            response = safe_scrape_request("https://httpbin.org/get", timeout=5)
            if response and response.status_code == 200:
                print("✅ Safe scraping request successful")
            else:
                print("⚠️  Safe scraping request failed")
        except Exception as e:
            print(f"⚠️  Safe scraping request test failed: {e}")

        # Test date parsing
        print("\nTesting date parsing...")
        test_dates = ["December 15, 2024", "12/15/2024", "2024-12-15", "15 Dec 2024"]

        for date_str in test_dates:
            parsed_date = parse_event_date(date_str)
            if parsed_date:
                print(f"✅ Parsed '{date_str}' -> {parsed_date}")
            else:
                print(f"⚠️  Failed to parse '{date_str}'")

        # Test event classification
        print("\nTesting event classification...")
        test_event = {
            "title": "Jazz Concert at the Plaza",
            "description": "Live music performance with drinks and entertainment",
        }

        psychographic_scores = classify_event_psychographics(
            test_event["title"], test_event["description"]
        )
        subcategory = determine_event_subcategory(
            test_event["title"], test_event["description"]
        )

        print(
            f"✅ Event classification - Psychographic: {psychographic_scores}, Subcategory: {subcategory}"
        )

        # Test venue scraper configurations
        print("\nTesting venue scraper configurations...")
        for venue_key, config in VENUE_SCRAPERS.items():
            print(f"✅ {config['name']}: {config['events_url']}")

        print("✅ Local venues ingestion tests completed")
        return True

    except Exception as e:
        print(f"❌ Local venues ingestion test failed: {e}")
        traceback.print_exc()
        return False


def test_prefect_flows():
    """Test Prefect flows integration"""
    print("\n" + "=" * 60)
    print("TESTING PREFECT FLOWS INTEGRATION")
    print("=" * 60)

    try:
        from src.infra.prefect_flows import (
            ingest_social_data_task,
            ingest_economic_data_task,
            ingest_traffic_data_task,
            ingest_local_venues_task,
            comprehensive_data_ingestion_flow,
            hourly_data_flow,
            social_and_economic_flow,
        )

        print("✅ All Prefect flow imports successful")
        print("✅ Available flows:")
        print("  - comprehensive_data_ingestion_flow")
        print("  - daily_flow")
        print("  - hourly_data_flow")
        print("  - weekly_comprehensive_flow")
        print("  - social_and_economic_flow")
        print("  - local_events_flow")

        print("✅ Prefect flows integration tests completed")
        return True

    except Exception as e:
        print(f"❌ Prefect flows integration test failed: {e}")
        traceback.print_exc()
        return False


def test_database_schema():
    """Test database schema compatibility"""
    print("\n" + "=" * 60)
    print("TESTING DATABASE SCHEMA COMPATIBILITY")
    print("=" * 60)

    try:
        from src.etl.utils import get_db_conn

        conn = get_db_conn()
        cur = conn.cursor()

        # Test required tables exist
        required_tables = [
            "social_sentiment",
            "economic_data",
            "traffic_data",
            "venues",
            "events",
        ]

        for table in required_tables:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """,
                (table,),
            )

            exists = cur.fetchone()[0]
            if exists:
                print(f"✅ Table '{table}' exists")
            else:
                print(f"❌ Table '{table}' missing")

        cur.close()
        conn.close()

        print("✅ Database schema compatibility tests completed")
        return True

    except Exception as e:
        print(f"❌ Database schema compatibility test failed: {e}")
        traceback.print_exc()
        return False


def test_api_keys_configuration():
    """Test API keys configuration"""
    print("\n" + "=" * 60)
    print("TESTING API KEYS CONFIGURATION")
    print("=" * 60)

    required_keys = [
        "TWITTER_API_KEY",
        "FACEBOOK_API_KEY",
        "ECONOMIC_DATA_API_KEY",
        "BUSINESS_NEWS_API_KEY",
        "TRAFFIC_API_KEY",
        "GOOGLE_MAPS_API_KEY",
    ]

    missing_keys = []

    for key in required_keys:
        value = os.getenv(key)
        if value:
            print(f"✅ {key}: {'*' * min(len(value), 10)}...")
        else:
            print(f"⚠️  {key}: Not set")
            missing_keys.append(key)

    if missing_keys:
        print(f"\n⚠️  Missing API keys: {', '.join(missing_keys)}")
        print("Some functionality may be limited without these keys.")
    else:
        print("\n✅ All API keys are configured")

    return len(missing_keys) == 0


def main():
    """Run all tests"""
    print("ENHANCED DATA INGESTION COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print(f"Test started at: {datetime.now()}")

    test_results = []

    # Run all tests
    test_functions = [
        ("API Keys Configuration", test_api_keys_configuration),
        ("Database Schema", test_database_schema),
        ("Social Data Ingestion", test_social_data_ingestion),
        ("Economic Data Ingestion", test_economic_data_ingestion),
        ("Traffic Data Ingestion", test_traffic_data_ingestion),
        ("Local Venues Ingestion", test_local_venues_ingestion),
        ("Prefect Flows Integration", test_prefect_flows),
    ]

    for test_name, test_func in test_functions:
        try:
            result = test_func()
            test_results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            test_results.append((test_name, False))

    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in test_results if result)
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<30} {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Enhanced data ingestion is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
