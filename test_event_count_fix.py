#!/usr/bin/env python3
"""
Test the event count fix by adding sample data and verifying consistency.
"""

import sys
import os
import uuid
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database.connection import get_database_connection
from fix_streamlit_event_discrepancy import refresh_master_data_tables


def add_sample_data():
    """Add sample venues and events to test the fix"""
    print("🎯 Adding sample data to test the fix...")

    try:
        with get_database_connection() as db:
            # Add sample venues with coordinates
            venues = [
                {
                    "venue_id": str(uuid.uuid4()),
                    "name": "T-Mobile Center",
                    "provider": "tmobile_center",
                    "lat": 39.0997,
                    "lng": -94.5786,
                    "address": "1407 Grand Blvd, Kansas City, MO 64106",
                    "category": "Arena",
                },
                {
                    "venue_id": str(uuid.uuid4()),
                    "name": "Kauffman Center",
                    "provider": "kauffman_center",
                    "lat": 39.0936,
                    "lng": -94.5844,
                    "address": "1601 Broadway Blvd, Kansas City, MO 64108",
                    "category": "Theater",
                },
                {
                    "venue_id": str(uuid.uuid4()),
                    "name": "Power & Light District",
                    "provider": "power_light",
                    "lat": 39.1012,
                    "lng": -94.5844,
                    "address": "1200 Main St, Kansas City, MO 64105",
                    "category": "Entertainment District",
                },
            ]

            # Insert venues
            for venue in venues:
                db.execute_query(
                    """
                    INSERT INTO venues (venue_id, name, provider, lat, lng, address, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        venue["venue_id"],
                        venue["name"],
                        venue["provider"],
                        venue["lat"],
                        venue["lng"],
                        venue["address"],
                        venue["category"],
                    ),
                )

            print(f"✅ Added {len(venues)} sample venues")

            # Add sample events
            events = []
            for i, venue in enumerate(venues):
                # Add multiple events per venue to create the discrepancy scenario
                for j in range(5):  # 5 events per venue = 15 total events
                    event_id = str(uuid.uuid4())
                    start_time = datetime.now() + timedelta(days=j + 1)
                    events.append(
                        {
                            "event_id": event_id,
                            "name": f"Sample Event {i+1}-{j+1}",
                            "provider": venue["provider"],
                            "venue_id": venue["venue_id"],
                            "start_time": start_time,
                            "category": "Concert",
                        }
                    )

            # Insert events
            for event in events:
                db.execute_query(
                    """
                    INSERT INTO events (event_id, name, provider, venue_id, start_time, category)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        event["event_id"],
                        event["name"],
                        event["provider"],
                        event["venue_id"],
                        event["start_time"],
                        event["category"],
                    ),
                )

            print(f"✅ Added {len(events)} sample events")

            return len(venues), len(events)

    except Exception as e:
        print(f"❌ Error adding sample data: {e}")
        return 0, 0


def test_consistency():
    """Test that the counts are now consistent"""
    print("\n🔍 Testing count consistency...")

    try:
        with get_database_connection() as db:
            # Get raw counts
            raw_venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[
                0
            ]["count"]
            raw_event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[
                0
            ]["count"]

            # Get master data counts (what the map uses)
            master_venue_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_venue_data"
            )[0]["count"]
            master_event_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_events_data"
            )[0]["count"]

            print(f"📊 Raw data counts:")
            print(f"  • Venues: {raw_venue_count}")
            print(f"  • Events: {raw_event_count}")

            print(f"📊 Master data counts (used by map):")
            print(f"  • Venues: {master_venue_count}")
            print(f"  • Events: {master_event_count}")

            # Check consistency
            if (
                master_venue_count == raw_venue_count
                and master_event_count == raw_event_count
            ):
                print("✅ Counts are consistent! The fix worked.")
                return True
            else:
                print(
                    "⚠️ Counts are still inconsistent. This is expected before refreshing master data."
                )
                return False

    except Exception as e:
        print(f"❌ Error testing consistency: {e}")
        return False


def main():
    """Main test function"""
    print("🧪 Testing Event Count Fix")
    print("=" * 50)

    # Step 1: Add sample data
    venue_count, event_count = add_sample_data()

    if venue_count == 0 and event_count == 0:
        print("❌ Failed to add sample data. Cannot test the fix.")
        return

    # Step 2: Test consistency before refresh (should be inconsistent)
    print("\n📋 BEFORE refreshing master data:")
    test_consistency()

    # Step 3: Refresh master data
    print("\n🔄 Refreshing master data...")
    refresh_master_data_tables()

    # Step 4: Test consistency after refresh (should be consistent)
    print("\n📋 AFTER refreshing master data:")
    is_consistent = test_consistency()

    print("\n" + "=" * 50)
    if is_consistent:
        print("✅ SUCCESS: The fix works! Event counts are now consistent.")
        print("\n💡 In the Streamlit app:")
        print("  • The 'Mappable Events' count will match what's shown on the map")
        print("  • The 'Refresh Master Data' button will keep them in sync")
        print("  • Raw totals are shown for reference")
    else:
        print("❌ The fix may need additional work.")

    print(f"\n🌐 Visit the Streamlit app at: http://localhost:8501")
    print("   The event count discrepancy should now be resolved!")


if __name__ == "__main__":
    main()
