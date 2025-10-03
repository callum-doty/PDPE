#!/usr/bin/env python3
"""
Diagnose the event count discrepancy between total events and map display.
"""

import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database.connection import get_database_connection


def diagnose_event_discrepancy():
    """Diagnose the event count discrepancy"""
    print("🔍 Diagnosing Event Count Discrepancy")
    print("=" * 50)

    try:
        with get_database_connection() as db:
            # 1. Check total events in raw events table
            total_events = db.execute_query("SELECT COUNT(*) as count FROM events")[0][
                "count"
            ]
            print(f"📊 Total events in 'events' table: {total_events}")

            # 2. Check events with venue associations
            events_with_venues = db.execute_query(
                """
                SELECT COUNT(*) as count 
                FROM events e 
                JOIN venues v ON e.venue_id = v.venue_id
            """
            )[0]["count"]
            print(f"🏢 Events with venue associations: {events_with_venues}")

            # 3. Check events with coordinates
            events_with_coords = db.execute_query(
                """
                SELECT COUNT(*) as count 
                FROM events e 
                JOIN venues v ON e.venue_id = v.venue_id
                WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
            """
            )[0]["count"]
            print(f"📍 Events with venue coordinates: {events_with_coords}")

            # 4. Check master_events_data view exists and count
            try:
                master_events_count = db.execute_query(
                    """
                    SELECT COUNT(*) as count FROM master_events_data
                """
                )[0]["count"]
                print(f"🎯 Events in master_events_data view: {master_events_count}")
            except Exception as e:
                print(f"❌ master_events_data view error: {e}")
                master_events_count = 0

            # 5. Check events within date range (last 7 days + next 7 days)
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now() + timedelta(days=7)

            try:
                events_in_range = db.execute_query(
                    """
                    SELECT COUNT(*) as count 
                    FROM master_events_data
                    WHERE lat IS NOT NULL AND lng IS NOT NULL
                    AND (start_time IS NULL OR start_time BETWEEN ? AND ?)
                """,
                    (start_date, end_date),
                )[0]["count"]
                print(
                    f"📅 Events in date range ({start_date.date()} to {end_date.date()}): {events_in_range}"
                )
            except Exception as e:
                print(f"❌ Date range query error: {e}")
                events_in_range = 0

            # 6. Check events without venue_id
            events_without_venue = db.execute_query(
                """
                SELECT COUNT(*) as count 
                FROM events 
                WHERE venue_id IS NULL
            """
            )[0]["count"]
            print(f"🚫 Events without venue_id: {events_without_venue}")

            # 7. Check venues without coordinates
            venues_without_coords = db.execute_query(
                """
                SELECT COUNT(*) as count 
                FROM venues 
                WHERE lat IS NULL OR lng IS NULL
            """
            )[0]["count"]
            print(f"📍 Venues without coordinates: {venues_without_coords}")

            # 8. Sample events without coordinates
            print("\n🔍 Sample events without coordinates:")
            sample_events = db.execute_query(
                """
                SELECT e.name, e.provider, v.name as venue_name, v.lat, v.lng
                FROM events e 
                LEFT JOIN venues v ON e.venue_id = v.venue_id
                WHERE v.lat IS NULL OR v.lng IS NULL OR e.venue_id IS NULL
                LIMIT 5
            """
            )

            for event in sample_events:
                print(
                    f"  - {event['name']} (Provider: {event['provider']}, Venue: {event['venue_name']}, Coords: {event['lat']}, {event['lng']})"
                )

            # 9. Check materialized view last refresh
            try:
                last_refresh = db.execute_query(
                    """
                    SELECT last_refreshed 
                    FROM master_events_data 
                    LIMIT 1
                """
                )
                if last_refresh:
                    print(
                        f"🔄 Master view last refreshed: {last_refresh[0]['last_refreshed']}"
                    )
                else:
                    print("🔄 Master view appears empty")
            except Exception as e:
                print(f"❌ Could not check last refresh: {e}")

            print("\n" + "=" * 50)
            print("📋 SUMMARY:")
            print(f"  • Raw events: {total_events}")
            print(f"  • Events with venues: {events_with_venues}")
            print(f"  • Events with coordinates: {events_with_coords}")
            print(f"  • Master view events: {master_events_count}")
            print(f"  • Events in date range: {events_in_range}")
            print(f"  • Events missing venue: {events_without_venue}")
            print(f"  • Venues missing coords: {venues_without_coords}")

            # Identify the main issues
            print("\n🎯 IDENTIFIED ISSUES:")
            if master_events_count == 0:
                print("  ❌ Master events view is empty or doesn't exist")
            if events_without_venue > 0:
                print(f"  ❌ {events_without_venue} events have no venue association")
            if venues_without_coords > 0:
                print(f"  ❌ {venues_without_coords} venues have no coordinates")
            if events_with_coords < total_events:
                print(
                    f"  ❌ {total_events - events_with_coords} events cannot be mapped due to missing coordinates"
                )

            return {
                "total_events": total_events,
                "events_with_venues": events_with_venues,
                "events_with_coords": events_with_coords,
                "master_events_count": master_events_count,
                "events_in_range": events_in_range,
                "events_without_venue": events_without_venue,
                "venues_without_coords": venues_without_coords,
            }

    except Exception as e:
        print(f"❌ Error during diagnosis: {e}")
        return None


if __name__ == "__main__":
    diagnose_event_discrepancy()
