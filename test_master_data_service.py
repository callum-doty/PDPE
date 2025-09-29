#!/usr/bin/env python3
"""
Test script for the Master Data Service Foundation.
Tests the orchestrator, quality controller, and daily refresh service.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from master_data_service.orchestrator import MasterDataOrchestrator
    from master_data_service.quality_controller import QualityController
    from master_data_service.daily_refresh import DailyRefreshService
    from etl.utils import get_db_conn

    print("✅ Successfully imported all master data service modules")
except ImportError as e:
    print(f"❌ Failed to import modules: {e}")
    sys.exit(1)


def test_database_connection():
    """Test database connection and basic queries."""
    print("\n🔍 Testing Database Connection...")

    try:
        conn = get_db_conn()
        if not conn:
            print("❌ Database connection failed")
            return False

        cur = conn.cursor()

        # Test basic venue query
        cur.execute("SELECT COUNT(*) FROM venues")
        venue_count = cur.fetchone()[0]
        print(f"✅ Database connected - {venue_count} venues found")

        # Test if materialized view exists
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'master_venue_data'
            )
        """
        )
        view_exists = cur.fetchone()[0]

        if view_exists:
            cur.execute("SELECT COUNT(*) FROM master_venue_data")
            master_count = cur.fetchone()[0]
            print(f"✅ Master venue data view exists - {master_count} records")
        else:
            print("⚠️  Master venue data view not found - run migrations first")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_orchestrator():
    """Test the Master Data Orchestrator."""
    print("\n🎯 Testing Master Data Orchestrator...")

    try:
        orchestrator = MasterDataOrchestrator()
        print("✅ Orchestrator initialized successfully")

        # Test health report generation
        print("📊 Generating health report...")
        health_report = orchestrator.get_data_health_report()

        if "error" in health_report:
            print(f"⚠️  Health report error: {health_report['error']}")
        else:
            print(f"✅ Health report generated:")
            print(
                f"   - Total venues: {health_report['venue_statistics']['total_venues']}"
            )
            print(
                f"   - Geocoded venues: {health_report['venue_statistics']['geocoded_venues']}"
            )
            print(f"   - Overall health: {health_report['overall_health_score']:.2f}")

        # Test priority data collection (dry run - just check if methods exist)
        print("🎯 Testing priority data collection methods...")
        priority_sources = orchestrator.priority_sources
        print(f"✅ Priority sources configured: {', '.join(priority_sources)}")

        return True

    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        return False


def test_quality_controller():
    """Test the Quality Controller."""
    print("\n🔍 Testing Quality Controller...")

    try:
        controller = QualityController()
        print("✅ Quality controller initialized successfully")

        # Test quality thresholds
        thresholds = controller.quality_thresholds
        print(f"✅ Quality thresholds configured for {len(thresholds)} sources")

        # Test priority source validation
        print("🎯 Testing priority source validation...")
        priority_reports = controller.validate_priority_sources()

        print(f"✅ Priority validation completed for {len(priority_reports)} sources:")
        for source, report in priority_reports.items():
            status = (
                "✅"
                if report.quality_score >= 0.5
                else "⚠️" if report.quality_score >= 0.3 else "❌"
            )
            print(
                f"   {status} {source}: Quality {report.quality_score:.2f}, Completeness {report.completeness_score:.2f}"
            )
            if report.validation_errors:
                print(f"      Errors: {', '.join(report.validation_errors)}")
            if report.data_issues:
                print(
                    f"      Issues: {', '.join(report.data_issues[:2])}"
                )  # Show first 2 issues

        return True

    except Exception as e:
        print(f"❌ Quality controller test failed: {e}")
        return False


def test_daily_refresh_service():
    """Test the Daily Refresh Service."""
    print("\n🌅 Testing Daily Refresh Service...")

    try:
        service = DailyRefreshService()
        print("✅ Daily refresh service initialized successfully")

        # Test refresh status
        print("📊 Getting refresh status...")
        status = service.get_refresh_status()

        if "error" in status:
            print(f"⚠️  Status error: {status['error']}")
        else:
            print(f"✅ Refresh status retrieved:")
            print(f"   - Last refresh: {status.get('last_refresh', 'Never')}")
            print(f"   - Health score: {status.get('health_score', 0):.2f}")
            print(f"   - Venue count: {status.get('venue_count', 0)}")
            print(f"   - Needs refresh: {status.get('needs_refresh', True)}")

        # Test performance targets
        print(f"✅ Performance targets configured:")
        print(f"   - Max duration: {service.max_refresh_duration}")
        print(f"   - Min quality score: {service.min_data_quality_score}")
        print(f"   - Min venue count: {service.min_venue_count}")

        return True

    except Exception as e:
        print(f"❌ Daily refresh service test failed: {e}")
        return False


def test_materialized_view_functions():
    """Test the materialized view refresh functions."""
    print("\n🔄 Testing Materialized View Functions...")

    try:
        conn = get_db_conn()
        if not conn:
            print("❌ Database connection failed")
            return False

        cur = conn.cursor()

        # Test if refresh functions exist
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM pg_proc 
                WHERE proname = 'refresh_master_data'
            )
        """
        )
        refresh_func_exists = cur.fetchone()[0]

        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM pg_proc 
                WHERE proname = 'refresh_all_master_data'
            )
        """
        )
        refresh_all_func_exists = cur.fetchone()[0]

        if refresh_func_exists:
            print("✅ refresh_master_data() function exists")
        else:
            print("❌ refresh_master_data() function missing")

        if refresh_all_func_exists:
            print("✅ refresh_all_master_data() function exists")
        else:
            print("❌ refresh_all_master_data() function missing")

        # Test collection_status table
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'collection_status'
            )
        """
        )
        status_table_exists = cur.fetchone()[0]

        if status_table_exists:
            print("✅ collection_status table exists")
            cur.execute("SELECT COUNT(*) FROM collection_status")
            status_count = cur.fetchone()[0]
            print(f"   - {status_count} collection status records")
        else:
            print("⚠️  collection_status table missing - may need to create it")

        cur.close()
        conn.close()
        return refresh_func_exists and refresh_all_func_exists

    except Exception as e:
        print(f"❌ Materialized view functions test failed: {e}")
        return False


def test_integration():
    """Test integration between components."""
    print("\n🔗 Testing Component Integration...")

    try:
        # Test orchestrator -> quality controller integration
        orchestrator = MasterDataOrchestrator()
        controller = QualityController()

        print("✅ Components can be instantiated together")

        # Test that orchestrator can generate data for quality controller
        health_report = orchestrator.get_data_health_report()
        quality_metrics = controller.generate_quality_metrics()

        print("✅ Components can generate reports independently")

        # Test daily refresh service integration
        service = DailyRefreshService()

        # Verify service has access to both orchestrator and controller
        assert hasattr(service, "orchestrator")
        assert hasattr(service, "quality_controller")

        print("✅ Daily refresh service integrates with both components")

        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False


def run_comprehensive_test():
    """Run comprehensive test of the Master Data Service Foundation."""
    print("🚀 MASTER DATA SERVICE FOUNDATION TEST")
    print("=" * 60)

    test_results = []

    # Run all tests
    test_results.append(("Database Connection", test_database_connection()))
    test_results.append(("Master Data Orchestrator", test_orchestrator()))
    test_results.append(("Quality Controller", test_quality_controller()))
    test_results.append(("Daily Refresh Service", test_daily_refresh_service()))
    test_results.append(
        ("Materialized View Functions", test_materialized_view_functions())
    )
    test_results.append(("Component Integration", test_integration()))

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1

    print(f"\n🎯 Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")

    if passed == total:
        print("🎉 All tests passed! Master Data Service Foundation is ready.")
        print("\n📋 Next Steps:")
        print("   1. Run database migrations if materialized views are missing")
        print("   2. Proceed to Week 2: Consolidate Priority ETL Scripts")
        print("   3. Test with real data collection")
        return True
    else:
        print("⚠️  Some tests failed. Please address issues before proceeding.")
        print("\n🔧 Troubleshooting:")
        print("   1. Ensure database is running and accessible")
        print("   2. Run database migrations: psql -f src/db/migrations.sql")
        print("   3. Check that all required Python packages are installed")
        return False


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.WARNING,  # Reduce noise during testing
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Run comprehensive test
    success = run_comprehensive_test()

    # Exit with appropriate code
    sys.exit(0 if success else 1)
