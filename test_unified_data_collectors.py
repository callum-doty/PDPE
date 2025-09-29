#!/usr/bin/env python3
"""
Test script for the unified data collectors.
Tests all collectors in the new data_collectors package to ensure they work properly.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from data_collectors import (
        UnifiedVenueCollector,
        WeatherCollector,
        TrafficCollector,
        SocialCollector,
        EconomicCollector,
        FootTrafficCollector,
        MLPredictionCollector,
        ExternalAPICollector,
    )
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def test_all_collectors():
    """Test all unified data collectors."""
    print("🧪 Testing Unified Data Collectors")
    print("=" * 50)

    # Kansas City bounds for testing
    kc_bounds = {"lat": 39.0997, "lng": -94.5786, "radius_km": 25}

    # Test time period (last 24 hours)
    time_period = timedelta(hours=24)

    collectors = [
        ("Unified Venue Collector", UnifiedVenueCollector()),
        ("Weather Collector", WeatherCollector()),
        ("Traffic Collector", TrafficCollector()),
        ("Social Collector", SocialCollector()),
        ("Economic Collector", EconomicCollector()),
        ("Foot Traffic Collector", FootTrafficCollector()),
        ("ML Prediction Collector", MLPredictionCollector()),
        ("External API Collector", ExternalAPICollector()),
    ]

    results = []

    for name, collector in collectors:
        print(f"\n🔄 Testing {name}...")

        try:
            start_time = datetime.now()

            # Test data collection
            result = collector.collect_data(
                area_bounds=kc_bounds, time_period=time_period
            )

            duration = (datetime.now() - start_time).total_seconds()

            if result.success:
                print(f"✅ {name}: SUCCESS")
                print(f"   - Records collected: {result.records_collected}")
                print(f"   - Duration: {duration:.2f}s")
                print(f"   - Data quality score: {result.data_quality_score}")
                results.append((name, True, result.records_collected, duration))
            else:
                print(f"❌ {name}: FAILED")
                print(f"   - Error: {result.error_message}")
                print(f"   - Duration: {duration:.2f}s")
                results.append((name, False, 0, duration))

        except Exception as e:
            print(f"❌ {name}: EXCEPTION - {e}")
            results.append((name, False, 0, 0))

    # Summary
    print("\n" + "=" * 50)
    print("📊 SUMMARY")
    print("=" * 50)

    successful = [r for r in results if r[1]]
    failed = [r for r in results if not r[1]]

    print(f"✅ Successful collectors: {len(successful)}/{len(results)}")
    print(f"❌ Failed collectors: {len(failed)}/{len(results)}")

    if successful:
        total_records = sum(r[2] for r in successful)
        total_duration = sum(r[3] for r in successful)
        print(f"📈 Total records collected: {total_records}")
        print(f"⏱️  Total collection time: {total_duration:.2f}s")

    if failed:
        print(f"\n❌ Failed collectors:")
        for name, _, _, _ in failed:
            print(f"   - {name}")

    return len(successful) == len(results)


def test_collector_integration():
    """Test that collectors can work together in the master data service."""
    print("\n🔗 Testing Collector Integration")
    print("=" * 50)

    try:
        # Test importing master data service with new collectors
        from master_data_service.orchestrator import MasterDataOrchestrator

        print("✅ Master Data Service can import new collectors")

        # Test creating orchestrator (should not fail)
        orchestrator = MasterDataOrchestrator()
        print("✅ Master Data Orchestrator created successfully")

        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False


def main():
    """Main test function."""
    # Set up logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise during testing
        format="%(levelname)s: %(message)s",
    )

    print("🚀 Unified Data Collectors Test Suite")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Run tests
    collectors_test = test_all_collectors()
    integration_test = test_collector_integration()

    # Final results
    print("\n" + "=" * 50)
    print("🏁 FINAL RESULTS")
    print("=" * 50)

    if collectors_test and integration_test:
        print("✅ ALL TESTS PASSED")
        print("🎉 Unified Data Collectors are ready for use!")
        return 0
    else:
        print("❌ SOME TESTS FAILED")
        if not collectors_test:
            print("   - Collector tests failed")
        if not integration_test:
            print("   - Integration tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
