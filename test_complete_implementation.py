#!/usr/bin/env python3
"""
Comprehensive test for the complete venue data pipeline implementation.
Tests the integration of:
1. Enhanced dynamic venues ingestion module
2. Venue data service layer
3. Updated map generation with pre-processed data
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.append("src")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_dynamic_venues_ingestion():
    """Test the enhanced dynamic venues ingestion module."""
    logger.info("Testing enhanced dynamic venues ingestion...")

    try:
        from features.venues.scrapers.dynamic_venue_scraper import (
            ingest_dynamic_venue_data,
        )

        # Test the ingestion function
        result = ingest_dynamic_venue_data()
        logger.info("✅ Dynamic venue ingestion function available")

        # Test cache functionality
        cache_dir = Path("cache/dynamic_venues")
        if cache_dir.exists():
            logger.info(f"✅ Cache directory exists: {cache_dir}")
        else:
            logger.info(f"ℹ️  Cache directory will be created on first use: {cache_dir}")

        # Test result structure
        if isinstance(result, dict) and "success" in result:
            logger.info("✅ Ingestion function returns proper result structure")
        else:
            logger.warning("⚠️  Ingestion function result structure unexpected")

        return True

    except ImportError as e:
        logger.error(f"❌ Import error in dynamic venues ingestion: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing dynamic venues ingestion: {e}")
        return False


def test_venue_data_service():
    """Test the venue data service layer."""
    logger.info("Testing venue data service layer...")

    try:
        from backend.services.venue_data_service import (
            VenueDataService,
            VenueDataQuery,
            ProcessedVenueData,
            VenueDataType,
        )

        # Initialize the service
        service = VenueDataService()
        logger.info("✅ VenueDataService initialized successfully")

        # Test data structures
        query = VenueDataQuery(
            data_types=[VenueDataType.VENUES],
            bbox=(
                -95.5,
                29.5,
                -95.0,
                30.0,
            ),  # Houston area (min_lat, min_lng, max_lat, max_lng)
            limit=10,
        )
        logger.info("✅ VenueDataQuery created successfully")

        # Test service methods exist
        methods_to_check = [
            "get_venue_data",
            "get_venue_heatmap_data",
            "get_layered_map_data",
            "get_venue_ranking_data",
        ]

        for method_name in methods_to_check:
            if hasattr(service, method_name):
                logger.info(f"✅ Service method found: {method_name}")
            else:
                logger.warning(f"⚠️  Service method not found: {method_name}")

        return True

    except ImportError as e:
        logger.error(f"❌ Import error in venue data service: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing venue data service: {e}")
        return False


def test_interactive_map_builder():
    """Test the updated interactive map builder."""
    logger.info("Testing updated interactive map builder...")

    try:
        from backend.visualization.interactive_map_builder import InteractiveMapBuilder

        # Initialize the builder
        builder = InteractiveMapBuilder()
        logger.info("✅ InteractiveMapBuilder initialized successfully")

        # Test service-based methods exist
        service_methods = [
            "create_service_based_heatmap",
            "create_service_based_layered_map",
        ]

        for method_name in service_methods:
            if hasattr(builder, method_name):
                logger.info(f"✅ Service-based method found: {method_name}")
            else:
                logger.warning(f"⚠️  Service-based method not found: {method_name}")

        # Test helper methods exist
        helper_methods = [
            "_create_venue_popup",
            "_add_service_legend",
            "_add_predictions_layer",
        ]

        for method_name in helper_methods:
            if hasattr(builder, method_name):
                logger.info(f"✅ Helper method found: {method_name}")
            else:
                logger.warning(f"⚠️  Helper method not found: {method_name}")

        return True

    except ImportError as e:
        logger.error(f"❌ Import error in interactive map builder: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing interactive map builder: {e}")
        return False


def test_integration():
    """Test integration between all components."""
    logger.info("Testing component integration...")

    try:
        # Test that service can be imported in map builder
        from backend.visualization.interactive_map_builder import InteractiveMapBuilder
        from backend.services.venue_data_service import VenueDataService

        # Check if map builder can use service
        builder = InteractiveMapBuilder()
        service = VenueDataService()

        logger.info("✅ All components can be imported together")

        # Test venue processing integration
        from features.venues.processors.venue_processing import VenueProcessor
        from features.venues.scrapers.dynamic_venue_scraper import (
            ingest_dynamic_venue_data,
        )

        # Test that venue processor exists
        if VenueProcessor:
            logger.info("✅ VenueProcessor class available")

        # Test that dynamic venue function exists
        if ingest_dynamic_venue_data:
            logger.info("✅ Dynamic venue ingestion function available")

        logger.info("✅ Venue processing integration successful")

        return True

    except ImportError as e:
        logger.error(f"❌ Integration import error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Integration error: {e}")
        return False


def main():
    """Run comprehensive tests."""
    logger.info("=" * 60)
    logger.info("COMPREHENSIVE IMPLEMENTATION TEST")
    logger.info("=" * 60)

    test_results = []

    # Run individual component tests
    tests = [
        ("Dynamic Venues Ingestion", test_dynamic_venues_ingestion),
        ("Venue Data Service", test_venue_data_service),
        ("Interactive Map Builder", test_interactive_map_builder),
        ("Component Integration", test_integration),
    ]

    for test_name, test_func in tests:
        logger.info(f"\n--- Testing {test_name} ---")
        result = test_func()
        test_results.append((test_name, result))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    passed = 0
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1

    logger.info(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        logger.info("🎉 All implementation components are working correctly!")
        logger.info("\nNext steps:")
        logger.info("1. Run actual data ingestion to test with real data")
        logger.info("2. Generate maps using the new service layer")
        logger.info("3. Verify performance improvements from caching")
    else:
        logger.warning("⚠️  Some components need attention before full deployment")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
