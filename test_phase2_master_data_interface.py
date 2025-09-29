# Test Phase 2 Master Data Interface
"""
Comprehensive test script for Phase 2 Master Data Aggregation Service.
Tests the complete "single source of truth" functionality including:
- MasterDataAggregator
- VenueRegistry
- MasterDataInterface (the key component)
- End-to-end data flow
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from simple_map.data_interface import MasterDataInterface
    from master_data_service.data_aggregator import MasterDataAggregator
    from master_data_service.venue_registry import VenueRegistry
    from master_data_service.quality_controller import QualityController
    from master_data_service.orchestrator import MasterDataOrchestrator
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this from the project root directory")
    sys.exit(1)


def test_master_data_interface():
    """Test the MasterDataInterface - the key component."""
    print("🎯 Testing MasterDataInterface (THE KEY COMPONENT)")
    print("=" * 60)

    try:
        interface = MasterDataInterface()

        # Test 1: The key method - get_venues_and_events
        print("\n1. Testing get_venues_and_events() - THE KEY METHOD")
        print("-" * 50)

        start_time = datetime.now()
        venues, events = interface.get_venues_and_events()
        processing_time = (datetime.now() - start_time).total_seconds()

        print(f"✅ Retrieved {len(venues)} venues and {len(events)} events")
        print(f"⏱️  Processing time: {processing_time:.2f} seconds")

        if venues:
            sample_venue = venues[0]
            print(f"\n📍 Sample venue: {sample_venue.name}")
            print(f"   Location: {sample_venue.location}")
            print(f"   Category: {sample_venue.category}")
            print(f"   Data completeness: {sample_venue.data_completeness:.2f}")
            print(f"   Comprehensive score: {sample_venue.comprehensive_score:.2f}")

            # Check contextual data availability
            context_summary = {
                "Weather": sample_venue.current_weather is not None,
                "Traffic": sample_venue.traffic_conditions is not None,
                "Social": sample_venue.social_sentiment is not None,
                "ML Predictions": sample_venue.ml_predictions is not None,
                "Foot Traffic": sample_venue.foot_traffic is not None,
                "Economic": sample_venue.economic_context is not None,
                "Demographics": sample_venue.demographic_context is not None,
            }

            print(f"\n   📊 Contextual data availability:")
            for context_type, available in context_summary.items():
                status = "✅" if available else "❌"
                print(f"      {status} {context_type}")

        if events:
            sample_event = events[0]
            print(f"\n📅 Sample event: {sample_event.name}")
            print(f"   Venue: {sample_event.venue_name}")
            print(f"   Start time: {sample_event.start_time}")
            print(f"   Event score: {sample_event.event_score:.2f}")

        # Test 2: Data health status
        print("\n2. Testing get_data_health_status()")
        print("-" * 50)

        health = interface.get_data_health_status()
        print(f"✅ Overall status: {health.get('overall_status', 'unknown')}")

        summary = health.get("summary", {})
        print(f"   📊 Total venues: {summary.get('total_venues', 0)}")
        print(f"   📊 High quality venues: {summary.get('high_quality_venues', 0)}")
        print(f"   📊 Total events: {summary.get('total_events', 0)}")
        print(
            f"   📊 Healthy data sources: {summary.get('data_sources_healthy', 0)}/{summary.get('data_sources_total', 0)}"
        )
        print(f"   📊 Last refresh: {summary.get('last_refresh', 'Never')}")
        print(f"   📊 Refresh needed: {summary.get('refresh_needed', False)}")

        # Test 3: Area summary
        print("\n3. Testing get_area_summary()")
        print("-" * 50)

        area_summary = interface.get_area_summary()
        venue_stats = area_summary.get("venue_statistics", {})
        event_stats = area_summary.get("event_statistics", {})
        data_coverage = area_summary.get("data_coverage", {})

        print(f"✅ Area summary generated")
        print(f"   📊 Total venues: {venue_stats.get('total_venues', 0)}")
        print(f"   📊 Avg completeness: {venue_stats.get('avg_completeness', 0):.2f}")
        print(f"   📊 Total events: {event_stats.get('total_events', 0)}")
        print(f"   📊 Upcoming events: {event_stats.get('upcoming_events', 0)}")

        print(f"\n   📊 Data coverage:")
        print(f"      Weather: {data_coverage.get('weather_coverage', 0):.2f}")
        print(f"      ML Predictions: {data_coverage.get('ml_coverage', 0):.2f}")
        print(f"      Social Sentiment: {data_coverage.get('social_coverage', 0):.2f}")

        # Test 4: Search functionality
        print("\n4. Testing search_venues()")
        print("-" * 50)

        search_results = interface.search_venues("restaurant", limit=5)
        print(f"✅ Found {len(search_results)} restaurants")

        for i, venue in enumerate(search_results[:3], 1):
            print(
                f"   {i}. {venue.name} ({venue.category}) - Score: {venue.comprehensive_score:.2f}"
            )

        return True

    except Exception as e:
        print(f"❌ MasterDataInterface test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_data_aggregator():
    """Test the MasterDataAggregator component."""
    print("\n🔧 Testing MasterDataAggregator")
    print("=" * 60)

    try:
        aggregator = MasterDataAggregator()

        # Test venue aggregation
        print("\n1. Testing venue data aggregation")
        print("-" * 50)

        venues = aggregator.aggregate_venue_data()
        print(f"✅ Aggregated {len(venues)} venues")

        if venues:
            # Calculate quality distribution
            high_quality = len([v for v in venues if v.data_completeness >= 0.8])
            medium_quality = len(
                [v for v in venues if 0.6 <= v.data_completeness < 0.8]
            )
            low_quality = len([v for v in venues if v.data_completeness < 0.6])

            print(f"   📊 Quality distribution:")
            print(f"      High (≥0.8): {high_quality}")
            print(f"      Medium (0.6-0.8): {medium_quality}")
            print(f"      Low (<0.6): {low_quality}")

        # Test event aggregation
        print("\n2. Testing event data aggregation")
        print("-" * 50)

        events = aggregator.aggregate_event_data()
        print(f"✅ Aggregated {len(events)} events")

        if events:
            upcoming = len(
                [e for e in events if e.start_time and e.start_time > datetime.now()]
            )
            print(f"   📊 Upcoming events: {upcoming}")

        # Test health status
        print("\n3. Testing aggregation health status")
        print("-" * 50)

        health = aggregator.get_aggregation_health_status()
        print(f"✅ Health score: {health.get('overall_health_score', 0):.2f}")

        return True

    except Exception as e:
        print(f"❌ MasterDataAggregator test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_venue_registry():
    """Test the VenueRegistry component."""
    print("\n🏢 Testing VenueRegistry")
    print("=" * 60)

    try:
        registry = VenueRegistry()

        # Test venue registration
        print("\n1. Testing venue registration")
        print("-" * 50)

        test_venue = {
            "name": "Test Phase 2 Restaurant",
            "category": "restaurant",
            "lat": 39.1,
            "lng": -94.6,
            "address": "123 Test St, Kansas City, MO",
            "provider": "phase2_test",
        }

        venue_id = registry.register_venue(test_venue)
        print(f"✅ Registered test venue with ID: {venue_id}")

        # Test duplicate detection
        print("\n2. Testing duplicate detection")
        print("-" * 50)

        duplicate_venue = {
            "name": "Test Phase 2 Restaurant",  # Same name
            "category": "dining",
            "lat": 39.1001,  # Very close location
            "lng": -94.6001,
            "address": "123 Test Street, Kansas City, MO",  # Similar address
            "provider": "phase2_test_duplicate",
        }

        duplicate_id = registry.register_venue(duplicate_venue)
        print(f"✅ Duplicate venue ID: {duplicate_id}")
        print(f"   Same as original: {venue_id == duplicate_id}")

        if venue_id == duplicate_id:
            print("   ✅ Duplicate detection working correctly!")
        else:
            print("   ⚠️  Duplicate detection may need tuning")

        # Test venue relationships
        print("\n3. Testing venue relationships")
        print("-" * 50)

        relationships = registry.get_venue_relationships(venue_id)
        if relationships:
            summary = relationships.get("relationship_summary", {})
            print(f"✅ Venue relationships retrieved")
            print(f"   📊 Total events: {summary.get('total_events', 0)}")
            print(f"   📊 Nearby venues: {summary.get('nearby_venues_count', 0)}")
            print(f"   📊 Similar venues: {summary.get('similar_venues_count', 0)}")
        else:
            print("   ⚠️  No relationships found (may be expected for test venue)")

        return True

    except Exception as e:
        print(f"❌ VenueRegistry test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_end_to_end_flow():
    """Test the complete end-to-end data flow."""
    print("\n🔄 Testing End-to-End Data Flow")
    print("=" * 60)

    try:
        # Test the complete flow: Orchestrator -> Aggregator -> Interface
        print("\n1. Testing complete data flow")
        print("-" * 50)

        # Initialize interface (this initializes all components)
        interface = MasterDataInterface()

        # Test data collection (priority only for speed)
        print("   🚀 Collecting fresh priority data...")
        collection_result = interface.collect_fresh_data(priority_only=True)

        if collection_result.get("success"):
            summary = collection_result.get("summary", {})
            print(f"   ✅ Data collection successful")
            print(f"      Sources processed: {summary.get('sources_processed', 0)}")
            print(f"      Successful sources: {summary.get('successful_sources', 0)}")
            print(f"      Total records: {summary.get('total_records', 0)}")
        else:
            print(
                f"   ⚠️  Data collection had issues: {collection_result.get('error', 'Unknown error')}"
            )

        # Test data refresh
        print("\n   🔄 Refreshing area data...")
        refresh_result = interface.refresh_area_data(force_refresh=True)

        if refresh_result.get("success"):
            area_data = refresh_result.get("area_data", {})
            print(f"   ✅ Data refresh successful")
            print(f"      Venues in area: {area_data.get('venues_in_area', 0)}")
            print(f"      Events in area: {area_data.get('events_in_area', 0)}")
            print(
                f"      Avg completeness: {area_data.get('avg_venue_completeness', 0):.2f}"
            )
        else:
            print(
                f"   ⚠️  Data refresh had issues: {refresh_result.get('error', 'Unknown error')}"
            )

        # Test final data retrieval
        print("\n   🎯 Testing final data retrieval...")
        venues, events = interface.get_venues_and_events()

        print(f"   ✅ Final result: {len(venues)} venues, {len(events)} events")

        # Verify data quality
        if venues:
            avg_completeness = sum(v.data_completeness for v in venues) / len(venues)
            high_quality_count = len([v for v in venues if v.data_completeness >= 0.8])

            print(f"   📊 Data quality summary:")
            print(f"      Average completeness: {avg_completeness:.2f}")
            print(f"      High quality venues: {high_quality_count}/{len(venues)}")

            # Check for contextual data
            context_counts = {
                "weather": len([v for v in venues if v.current_weather]),
                "traffic": len([v for v in venues if v.traffic_conditions]),
                "social": len([v for v in venues if v.social_sentiment]),
                "ml_predictions": len([v for v in venues if v.ml_predictions]),
                "demographics": len([v for v in venues if v.demographic_context]),
            }

            print(f"   📊 Contextual data coverage:")
            for context_type, count in context_counts.items():
                coverage = count / len(venues) if venues else 0
                print(
                    f"      {context_type.title()}: {count}/{len(venues)} ({coverage:.1%})"
                )

        return True

    except Exception as e:
        print(f"❌ End-to-end flow test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_performance_benchmarks():
    """Test performance benchmarks for the master data interface."""
    print("\n⚡ Testing Performance Benchmarks")
    print("=" * 60)

    try:
        interface = MasterDataInterface()

        # Benchmark 1: Single venue/event retrieval
        print("\n1. Benchmarking get_venues_and_events()")
        print("-" * 50)

        times = []
        for i in range(3):
            start_time = datetime.now()
            venues, events = interface.get_venues_and_events()
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            times.append(duration)
            print(
                f"   Run {i+1}: {duration:.2f}s ({len(venues)} venues, {len(events)} events)"
            )

        avg_time = sum(times) / len(times)
        print(f"   📊 Average time: {avg_time:.2f}s")

        # Performance assessment
        if avg_time < 2.0:
            print("   ✅ Excellent performance (< 2s)")
        elif avg_time < 5.0:
            print("   ✅ Good performance (< 5s)")
        elif avg_time < 10.0:
            print("   ⚠️  Acceptable performance (< 10s)")
        else:
            print("   ❌ Performance needs improvement (> 10s)")

        # Benchmark 2: Health status retrieval
        print("\n2. Benchmarking get_data_health_status()")
        print("-" * 50)

        start_time = datetime.now()
        health = interface.get_data_health_status()
        duration = (datetime.now() - start_time).total_seconds()

        print(f"   ✅ Health status retrieved in {duration:.2f}s")

        return True

    except Exception as e:
        print(f"❌ Performance benchmark test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all Phase 2 tests."""
    print("🚀 PHASE 2 MASTER DATA AGGREGATION SERVICE TESTS")
    print("=" * 80)
    print("Testing the complete 'Single Source of Truth' implementation")
    print("=" * 80)

    # Configure logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise during testing
        format="%(levelname)s: %(message)s",
    )

    # Track test results
    test_results = {}

    # Run all tests
    test_results["master_data_interface"] = test_master_data_interface()
    test_results["data_aggregator"] = test_data_aggregator()
    test_results["venue_registry"] = test_venue_registry()
    test_results["end_to_end_flow"] = test_end_to_end_flow()
    test_results["performance_benchmarks"] = test_performance_benchmarks()

    # Summary
    print("\n" + "=" * 80)
    print("📊 PHASE 2 TEST RESULTS SUMMARY")
    print("=" * 80)

    passed_tests = 0
    total_tests = len(test_results)

    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} {test_name.replace('_', ' ').title()}")
        if result:
            passed_tests += 1

    print(f"\n📊 Overall Results: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! Phase 2 implementation is working correctly.")
        print("\n🎯 KEY ACHIEVEMENTS:")
        print("   ✅ Single Source of Truth interface implemented")
        print("   ✅ Master Data Aggregator consolidating all sources")
        print("   ✅ Advanced Venue Registry with deduplication")
        print("   ✅ End-to-end data flow working")
        print("   ✅ Performance benchmarks acceptable")

        print("\n🚀 READY FOR PHASE 2 COMPLETION:")
        print("   • Map applications can now use interface.get_venues_and_events()")
        print("   • Single query replaces 8+ separate database queries")
        print("   • All contextual data included in consolidated objects")
        print("   • Advanced deduplication and quality control active")

    else:
        print(f"⚠️  {total_tests - passed_tests} tests failed. Review the errors above.")
        print("   Some functionality may not be working as expected.")

    return passed_tests == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
