#!/usr/bin/env python3
"""
Test script to verify the refactored Master Data Orchestrator integration
with the new application architecture.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_orchestrator_imports():
    """Test that all orchestrator imports work correctly"""
    logger.info("🔍 Testing orchestrator imports...")

    try:
        from shared.orchestration.master_data_orchestrator import (
            MasterDataOrchestrator,
            MasterDataStatus,
        )

        logger.info("✅ Successfully imported MasterDataOrchestrator")

        from shared.models.core_models import (
            VenueCollectionResult,
            EventCollectionResult,
        )

        logger.info("✅ Successfully imported core models")

        from shared.database.connection import get_database_connection

        logger.info("✅ Successfully imported database connection")

        return True

    except ImportError as e:
        logger.error(f"❌ Import failed: {e}")
        return False


def test_orchestrator_initialization():
    """Test orchestrator initialization"""
    logger.info("🔍 Testing orchestrator initialization...")

    try:
        from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator

        orchestrator = MasterDataOrchestrator()
        logger.info("✅ Successfully initialized MasterDataOrchestrator")

        # Check that components are initialized
        assert hasattr(orchestrator, "venue_collector")
        assert hasattr(orchestrator, "event_scraper")
        assert hasattr(orchestrator, "ml_predictor")
        logger.info("✅ All orchestrator components initialized")

        return True

    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}")
        return False


def test_health_report():
    """Test health report generation"""
    logger.info("🔍 Testing health report generation...")

    try:
        from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator

        orchestrator = MasterDataOrchestrator()
        health_report = orchestrator.get_data_health_report()

        # Check report structure
        assert isinstance(health_report, dict)
        logger.info("✅ Health report generated successfully")

        # Check for expected keys
        expected_keys = [
            "timestamp",
            "venue_statistics",
            "event_statistics",
            "overall_health_score",
        ]
        for key in expected_keys:
            if key in health_report:
                logger.info(f"✅ Found expected key: {key}")
            else:
                logger.warning(f"⚠️ Missing expected key: {key}")

        logger.info(
            f"📊 Overall health score: {health_report.get('overall_health_score', 'N/A')}"
        )

        return True

    except Exception as e:
        logger.error(f"❌ Health report generation failed: {e}")
        return False


def test_priority_data_collection():
    """Test priority data collection (dry run)"""
    logger.info("🔍 Testing priority data collection...")

    try:
        from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator

        orchestrator = MasterDataOrchestrator()

        # Test that the method exists and can be called
        results = orchestrator.collect_priority_data()

        assert isinstance(results, list)
        logger.info(f"✅ Priority data collection returned {len(results)} results")

        # Check result structure
        for result in results:
            assert hasattr(result, "source_name")
            assert hasattr(result, "success")
            assert hasattr(result, "duration_seconds")
            logger.info(
                f"📊 {result.source_name}: {'✅' if result.success else '❌'} ({result.duration_seconds:.1f}s)"
            )

        return True

    except Exception as e:
        logger.error(f"❌ Priority data collection test failed: {e}")
        return False


def test_individual_collectors():
    """Test individual collector components"""
    logger.info("🔍 Testing individual collector components...")

    # Test VenueCollector
    try:
        from features.venues.collectors.venue_collector import VenueCollector

        collector = VenueCollector()
        logger.info("✅ VenueCollector initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️ VenueCollector initialization failed: {e}")

    # Test KCEventScraper
    try:
        from features.venues.scrapers.kc_event_scraper import KCEventScraper

        scraper = KCEventScraper()
        logger.info("✅ KCEventScraper initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️ KCEventScraper initialization failed: {e}")

    # Test MLPredictor
    try:
        from features.ml.models.inference.predictor import MLPredictor

        predictor = MLPredictor()
        logger.info("✅ MLPredictor initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️ MLPredictor initialization failed: {e}")

    return True


def test_database_connection():
    """Test database connection"""
    logger.info("🔍 Testing database connection...")

    try:
        from shared.database.connection import get_database_connection

        with get_database_connection() as db:
            # Try a simple query
            result = db.execute_query("SELECT 1 as test")
            if result and len(result) > 0:
                logger.info("✅ Database connection successful")
                return True
            else:
                logger.warning("⚠️ Database connection established but query failed")
                return False

    except Exception as e:
        logger.warning(f"⚠️ Database connection failed: {e}")
        return False


def test_app_integration():
    """Test that the app can import the orchestrator"""
    logger.info("🔍 Testing app integration...")

    try:
        # Simulate app imports
        sys.path.append(str(project_root / "app"))

        from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator
        from features.venues.collectors.venue_collector import VenueCollector
        from features.venues.scrapers.kc_event_scraper import KCEventScraper
        from features.ml.models.inference.predictor import MLPredictor

        logger.info("✅ All app imports successful")

        # Test orchestrator creation (as the app would do)
        orchestrator = MasterDataOrchestrator()
        logger.info("✅ Orchestrator creation successful")

        return True

    except Exception as e:
        logger.error(f"❌ App integration test failed: {e}")
        return False


def main():
    """Run all tests"""
    logger.info("🚀 Starting Master Data Orchestrator Integration Tests")
    logger.info("=" * 60)

    tests = [
        ("Import Tests", test_orchestrator_imports),
        ("Initialization Tests", test_orchestrator_initialization),
        ("Individual Collectors", test_individual_collectors),
        ("Database Connection", test_database_connection),
        ("Health Report", test_health_report),
        ("Priority Data Collection", test_priority_data_collection),
        ("App Integration", test_app_integration),
    ]

    results = {}

    for test_name, test_func in tests:
        logger.info(f"\n📋 Running {test_name}...")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 60)

    passed = 0
    total = len(tests)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"{status} {test_name}")
        if result:
            passed += 1

    logger.info(
        f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)"
    )

    if passed == total:
        logger.info("🎉 All tests passed! The orchestrator is ready for use.")
        return 0
    elif passed >= total * 0.7:
        logger.info("⚠️ Most tests passed. Some components may need attention.")
        return 1
    else:
        logger.error("❌ Many tests failed. Please check the configuration.")
        return 2


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
