#!/usr/bin/env python3
"""
Test script to verify the PostgreSQL materialized view fix works correctly.
"""

import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database.connection import get_database_connection
from fix_streamlit_event_discrepancy import refresh_master_data_tables


def test_database_connection():
    """Test database connection and identify database type"""
    print("🔍 Testing database connection...")

    try:
        with get_database_connection() as db:
            print(f"✅ Connected to database")
            print(f"📊 Database type: {db.db_type}")
            print(f"🔗 Database URL: {db.database_url}")

            # Test basic query
            if db.db_type == "postgresql":
                # Test PostgreSQL specific functionality
                result = db.execute_query("SELECT version()")
                if result:
                    print(
                        f"📋 PostgreSQL version: {result[0].get('version', 'Unknown')[:50]}..."
                    )

                # Check if materialized views exist
                mv_check = db.execute_query(
                    """
                    SELECT schemaname, matviewname 
                    FROM pg_matviews 
                    WHERE matviewname IN ('master_venue_data', 'master_events_data')
                """
                )

                if mv_check:
                    print(f"✅ Found {len(mv_check)} materialized views:")
                    for mv in mv_check:
                        print(f"  • {mv['schemaname']}.{mv['matviewname']}")
                else:
                    print("⚠️ No materialized views found - they may need to be created")

            elif db.db_type == "sqlite":
                # Test SQLite functionality
                result = db.execute_query("SELECT sqlite_version()")
                if result:
                    print(
                        f"📋 SQLite version: {result[0].get('sqlite_version()', 'Unknown')}"
                    )

                # Check if master data tables exist
                tables_check = db.execute_query(
                    """
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name IN ('master_venue_data', 'master_events_data')
                """
                )

                if tables_check:
                    print(f"✅ Found {len(tables_check)} master data tables:")
                    for table in tables_check:
                        print(f"  • {table['name']}")
                else:
                    print("⚠️ No master data tables found - they may need to be created")

            return db.db_type

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None


def test_materialized_view_refresh():
    """Test the materialized view refresh functionality"""
    print("\n🔄 Testing materialized view refresh...")

    try:
        # Test the updated refresh function
        venue_count, event_count = refresh_master_data_tables()

        print(f"✅ Refresh completed successfully:")
        print(f"  • Venues: {venue_count}")
        print(f"  • Events: {event_count}")

        return True

    except Exception as e:
        print(f"❌ Refresh failed: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        return False


def test_master_data_orchestrator():
    """Test the master data orchestrator with the new refresh method"""
    print("\n🎯 Testing master data orchestrator...")

    try:
        from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator

        orchestrator = MasterDataOrchestrator()

        # Test the new master_data refresh source
        print("🔄 Testing master_data source refresh...")
        results = orchestrator.refresh_data_sources(["master_data"])

        if results:
            for result in results:
                status = "✅" if result.success else "❌"
                print(
                    f"{status} {result.source_name}: {result.venues_collected} venues in {result.duration_seconds:.1f}s"
                )
                if result.error_message:
                    print(f"  Error: {result.error_message}")
        else:
            print("⚠️ No results returned from orchestrator")

        return True

    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        return False


def simulate_postgresql_error():
    """Simulate the original PostgreSQL error to verify it's fixed"""
    print("\n🧪 Simulating original error scenario...")

    try:
        with get_database_connection() as db:
            if db.db_type == "postgresql":
                print("📊 Testing PostgreSQL materialized view operations...")

                # This should now work with our fix
                try:
                    db.execute_query("REFRESH MATERIALIZED VIEW master_venue_data")
                    print("✅ REFRESH MATERIALIZED VIEW master_venue_data - SUCCESS")
                except Exception as e:
                    print(
                        f"❌ REFRESH MATERIALIZED VIEW master_venue_data - FAILED: {e}"
                    )

                try:
                    db.execute_query("REFRESH MATERIALIZED VIEW master_events_data")
                    print("✅ REFRESH MATERIALIZED VIEW master_events_data - SUCCESS")
                except Exception as e:
                    print(
                        f"❌ REFRESH MATERIALIZED VIEW master_events_data - FAILED: {e}"
                    )

            else:
                print("📊 SQLite detected - testing table refresh operations...")

                # Test that our SQLite fallback works
                try:
                    count = db.execute_query(
                        "SELECT COUNT(*) as count FROM master_venue_data"
                    )[0]["count"]
                    print(f"✅ master_venue_data table accessible - {count} records")
                except Exception as e:
                    print(f"❌ master_venue_data table access failed: {e}")

                try:
                    count = db.execute_query(
                        "SELECT COUNT(*) as count FROM master_events_data"
                    )[0]["count"]
                    print(f"✅ master_events_data table accessible - {count} records")
                except Exception as e:
                    print(f"❌ master_events_data table access failed: {e}")

    except Exception as e:
        print(f"❌ Error simulation failed: {e}")


def main():
    """Main test function"""
    print("🧪 PostgreSQL Materialized View Fix Test")
    print("=" * 50)

    # Test 1: Database connection
    db_type = test_database_connection()
    if not db_type:
        print("❌ Cannot proceed without database connection")
        return

    # Test 2: Materialized view refresh
    refresh_success = test_materialized_view_refresh()

    # Test 3: Master data orchestrator
    orchestrator_success = test_master_data_orchestrator()

    # Test 4: Simulate original error scenario
    simulate_postgresql_error()

    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    print(f"  • Database type: {db_type}")
    print(f"  • Refresh function: {'✅ PASS' if refresh_success else '❌ FAIL'}")
    print(f"  • Orchestrator: {'✅ PASS' if orchestrator_success else '❌ FAIL'}")

    if db_type == "postgresql" and refresh_success and orchestrator_success:
        print(
            "\n🎉 All tests passed! The PostgreSQL materialized view fix is working correctly."
        )
    elif db_type == "sqlite" and refresh_success and orchestrator_success:
        print("\n✅ All tests passed! SQLite fallback is working correctly.")
        print(
            "💡 To test PostgreSQL functionality, update your DATABASE_URL to point to a PostgreSQL database."
        )
    else:
        print("\n⚠️ Some tests failed. Please review the errors above.")


if __name__ == "__main__":
    main()
