#!/usr/bin/env python3
"""
Fix the Streamlit event count discrepancy by ensuring consistent data sources.
"""

import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database.connection import get_database_connection


def refresh_master_data_tables():
    """Refresh master data tables - handles both PostgreSQL materialized views and SQLite tables"""
    print("🔄 Refreshing master data tables...")

    try:
        with get_database_connection() as db:
            # Check if we're using PostgreSQL or SQLite
            if db.db_type == "postgresql":
                print("📊 Using PostgreSQL - refreshing materialized views...")

                # Use PostgreSQL's REFRESH MATERIALIZED VIEW command
                try:
                    # Refresh master_venue_data materialized view
                    db.execute_query("REFRESH MATERIALIZED VIEW master_venue_data")
                    print("✅ Refreshed master_venue_data materialized view")

                    # Refresh master_events_data materialized view
                    db.execute_query("REFRESH MATERIALIZED VIEW master_events_data")
                    print("✅ Refreshed master_events_data materialized view")

                except Exception as mv_error:
                    print(f"⚠️ Materialized view refresh failed: {mv_error}")
                    print("🔄 Attempting to use refresh functions from migrations...")

                    # Try using the refresh functions defined in migrations.sql
                    try:
                        result = db.execute_query(
                            "SELECT * FROM refresh_all_master_data()"
                        )
                        if result:
                            for row in result:
                                print(
                                    f"✅ {row.get('view_name', 'Unknown')}: {row.get('record_count', 0)} records"
                                )
                    except Exception as func_error:
                        print(f"⚠️ Refresh functions also failed: {func_error}")
                        print("🔄 Falling back to manual refresh...")

                        # Last resort: try to recreate the materialized views
                        try:
                            db.execute_query(
                                "REFRESH MATERIALIZED VIEW CONCURRENTLY master_venue_data"
                            )
                            db.execute_query(
                                "REFRESH MATERIALIZED VIEW CONCURRENTLY master_events_data"
                            )
                            print("✅ Used concurrent refresh as fallback")
                        except Exception as concurrent_error:
                            print(
                                f"❌ All PostgreSQL refresh methods failed: {concurrent_error}"
                            )
                            raise concurrent_error

            else:
                print("📊 Using SQLite - refreshing tables manually...")

                # For SQLite, we need to manually refresh the tables
                # Clear existing master data
                db.execute_query("DELETE FROM master_venue_data")
                db.execute_query("DELETE FROM master_events_data")

                # Refresh master_venue_data
                db.execute_query(
                    """
                    INSERT INTO master_venue_data (
                        venue_id, external_id, provider, name, category, subcategory,
                        lat, lng, address, phone, website, psychographic_relevance,
                        created_at, updated_at, data_completeness_score, 
                        comprehensive_score, data_source_type, last_refreshed
                    )
                    SELECT 
                        venue_id, external_id, provider, name, category, subcategory,
                        lat, lng, address, phone, website, psychographic_relevance,
                        created_at, updated_at,
                        CASE 
                            WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 0.8
                            ELSE 0.2
                        END as data_completeness_score,
                        CASE 
                            WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 0.6
                            ELSE 0.1
                        END as comprehensive_score,
                        CASE 
                            WHEN provider LIKE '%google%' OR provider LIKE '%places%' THEN 'api_places'
                            WHEN provider IN ('tmobile_center', 'uptown_theater', 'kauffman_center', 'starlight_theatre', 'midland_theatre', 'knuckleheads', 'azura_amphitheater') THEN 'scraped_static'
                            WHEN provider IN ('visitkc', 'do816', 'thepitchkc', 'aura') THEN 'scraped_dynamic'
                            ELSE 'scraped_local'
                        END as data_source_type,
                        datetime('now') as last_refreshed
                    FROM venues
                    WHERE lat IS NOT NULL AND lng IS NOT NULL
                """
                )

                # Refresh master_events_data
                db.execute_query(
                    """
                    INSERT INTO master_events_data (
                        event_id, external_id, provider, name, description, category, subcategory,
                        start_time, end_time, predicted_attendance, psychographic_relevance,
                        venue_name, lat, lng, venue_address, venue_category,
                        event_score, data_source_type, last_refreshed
                    )
                    SELECT 
                        e.event_id, e.external_id, e.provider, e.name, e.description, 
                        e.category, e.subcategory, e.start_time, e.end_time, 
                        e.predicted_attendance, e.psychographic_relevance,
                        v.name as venue_name, v.lat, v.lng, v.address as venue_address, 
                        v.category as venue_category,
                        0.5 as event_score,
                        CASE 
                            WHEN e.provider LIKE '%predicthq%' THEN 'api_events'
                            WHEN e.provider IN ('visitkc', 'do816', 'thepitchkc') THEN 'scraped_dynamic'
                            ELSE 'scraped_local'
                        END as data_source_type,
                        datetime('now') as last_refreshed
                    FROM events e
                    JOIN venues v ON e.venue_id = v.venue_id
                    WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
                """
                )

            # Get counts (works for both PostgreSQL and SQLite)
            venue_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_venue_data"
            )[0]["count"]
            event_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_events_data"
            )[0]["count"]

            print(f"✅ Refreshed master data:")
            print(f"  • Master venues: {venue_count}")
            print(f"  • Master events: {event_count}")
            print(f"  • Database type: {db.db_type}")

            return venue_count, event_count

    except Exception as e:
        print(f"❌ Error refreshing master data: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        return 0, 0


def fix_streamlit_app():
    """Fix the Streamlit app to use consistent data sources"""
    print("\n🔧 Fixing Streamlit app for consistent data sources...")

    # Read the current Streamlit app
    with open("app/main.py", "r") as f:
        content = f.read()

    # Replace the display_data_summary function to use consistent sources
    old_function = '''def display_data_summary():
    """Display summary statistics"""
    try:
        with get_database_connection() as db:
            venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[0][
                "count"
            ]
            event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[0][
                "count"
            ]

            st.metric("Total Venues", venue_count)
            st.metric("Total Events", event_count)

    except Exception as e:
        st.metric("Total Venues", "N/A")
        st.metric("Total Events", "N/A")'''

    new_function = '''def display_data_summary():
    """Display summary statistics"""
    try:
        with get_database_connection() as db:
            # Use master data tables for consistency with map display
            venue_count = db.execute_query("SELECT COUNT(*) as count FROM master_venue_data")[0][
                "count"
            ]
            event_count = db.execute_query("SELECT COUNT(*) as count FROM master_events_data")[0][
                "count"
            ]
            
            # Also show raw counts for comparison
            raw_venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[0]["count"]
            raw_event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[0]["count"]

            st.metric("Mappable Venues", venue_count, delta=f"{raw_venue_count} total")
            st.metric("Mappable Events", event_count, delta=f"{raw_event_count} total")

    except Exception as e:
        st.metric("Mappable Venues", "N/A")
        st.metric("Mappable Events", "N/A")'''

    # Replace the function
    if old_function in content:
        content = content.replace(old_function, new_function)
        print("✅ Updated display_data_summary function")
    else:
        print(
            "⚠️ Could not find exact function to replace, will add refresh functionality"
        )

    # Add a refresh master data button to the sidebar
    refresh_button_code = """        # Master data refresh
        st.subheader("Data Management")
        refresh_master_data = st.button("🔄 Refresh Master Data")
"""

    # Find the sidebar section and add the refresh button
    if "# Map options" in content:
        content = content.replace(
            "        # Map options", refresh_button_code + "\n        # Map options"
        )
        print("✅ Added refresh master data button")

    # Add the refresh functionality to the main function
    refresh_handler = """
    # Handle refresh master data
    if refresh_master_data:
        with st.spinner("Refreshing master data tables..."):
            refresh_master_data_tables()
            st.success("Master data refreshed! The map and counts should now be consistent.")
            st.rerun()
"""

    # Add the handler before the existing button handlers
    if "# Handle button clicks" in content:
        content = content.replace(
            "    # Handle button clicks",
            refresh_handler + "\n    # Handle button clicks",
        )
        print("✅ Added refresh handler")

    # Add the import for the refresh function
    import_line = (
        "from fix_streamlit_event_discrepancy import refresh_master_data_tables"
    )
    if import_line not in content:
        # Find the imports section and add it
        import_section = content.find(
            "from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator"
        )
        if import_section != -1:
            # Add after the orchestrator import
            end_of_line = content.find("\n", import_section)
            content = (
                content[:end_of_line] + f"\n    {import_line}" + content[end_of_line:]
            )
            print("✅ Added import for refresh function")

    # Write the updated content back
    with open("app/main.py", "w") as f:
        f.write(content)

    print("✅ Streamlit app updated successfully!")


def main():
    """Main function to fix the event discrepancy"""
    print("🎯 Fixing Streamlit Event Count Discrepancy")
    print("=" * 50)

    # Step 1: Refresh master data tables
    venue_count, event_count = refresh_master_data_tables()

    # Step 2: Fix the Streamlit app
    fix_streamlit_app()

    print("\n" + "=" * 50)
    print("✅ Fix completed!")
    print(f"  • Master venues available for mapping: {venue_count}")
    print(f"  • Master events available for mapping: {event_count}")
    print("\n💡 Next steps:")
    print("  1. Run the Streamlit app: streamlit run app/main.py")
    print("  2. Click 'Refresh Master Data' if counts are still inconsistent")
    print("  3. The event count should now match what's displayed on the map")


if __name__ == "__main__":
    main()
