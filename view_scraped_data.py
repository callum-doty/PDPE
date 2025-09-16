#!/usr/bin/env python3
"""
Script to view scraped venue and event data from the PostgreSQL database
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import json

# Add src to path for imports
sys.path.append("src")


def get_db_connection():
    """Get database connection using environment variable"""
    db_dsn = os.getenv("DATABASE_URL")
    if not db_dsn:
        print("❌ DATABASE_URL environment variable not set!")
        print("Please set it in your .env file or export it:")
        print(
            "export DATABASE_URL='postgresql://username:password@localhost:5432/database_name'"
        )
        return None

    try:
        conn = psycopg2.connect(db_dsn)
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None


def view_venues(conn, limit=10):
    """View scraped venues"""
    print(f"\n🏢 VENUES (showing last {limit} entries)")
    print("=" * 80)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT 
                name,
                category,
                provider,
                address,
                website,
                psychographic_relevance,
                created_at
            FROM venues 
            ORDER BY created_at DESC 
            LIMIT %s
        """,
            (limit,),
        )

        venues = cur.fetchall()

        if not venues:
            print("⚠️  No venues found in database")
            return

        for i, venue in enumerate(venues, 1):
            print(f"\n{i}. {venue['name']}")
            print(f"   📍 Category: {venue['category']}")
            print(f"   🔗 Provider: {venue['provider']}")
            if venue["address"]:
                print(f"   📧 Address: {venue['address']}")
            if venue["website"]:
                print(f"   🌐 Website: {venue['website']}")
            if venue["psychographic_relevance"]:
                psycho = venue["psychographic_relevance"]
                print(f"   🧠 Psychographics: {json.dumps(psycho, indent=6)}")
            print(f"   📅 Added: {venue['created_at']}")


def view_events(conn, limit=10):
    """View scraped events"""
    print(f"\n🎉 EVENTS (showing last {limit} entries)")
    print("=" * 80)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT 
                e.name,
                e.description,
                e.category,
                e.subcategory,
                e.start_time,
                e.provider,
                v.name as venue_name,
                e.psychographic_relevance,
                e.created_at
            FROM events e
            LEFT JOIN venues v ON e.venue_id = v.venue_id
            ORDER BY e.created_at DESC 
            LIMIT %s
        """,
            (limit,),
        )

        events = cur.fetchall()

        if not events:
            print("⚠️  No events found in database")
            return

        for i, event in enumerate(events, 1):
            print(f"\n{i}. {event['name']}")
            print(f"   📍 Venue: {event['venue_name'] or 'Unknown'}")
            print(f"   🏷️  Category: {event['category']} / {event['subcategory']}")
            print(f"   🔗 Provider: {event['provider']}")
            if event["start_time"]:
                print(f"   📅 Start: {event['start_time']}")
            if event["description"]:
                desc = (
                    event["description"][:100] + "..."
                    if len(event["description"]) > 100
                    else event["description"]
                )
                print(f"   📝 Description: {desc}")
            if event["psychographic_relevance"]:
                psycho = event["psychographic_relevance"]
                print(f"   🧠 Psychographics: {json.dumps(psycho, indent=6)}")
            print(f"   📅 Scraped: {event['created_at']}")


def view_summary(conn):
    """View summary statistics"""
    print("\n📊 DATABASE SUMMARY")
    print("=" * 80)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Venue counts
        cur.execute("SELECT COUNT(*) as count FROM venues")
        venue_count = cur.fetchone()["count"]

        cur.execute(
            """
            SELECT category, COUNT(*) as count 
            FROM venues 
            GROUP BY category 
            ORDER BY count DESC
        """
        )
        venue_categories = cur.fetchall()

        # Event counts
        cur.execute("SELECT COUNT(*) as count FROM events")
        event_count = cur.fetchone()["count"]

        cur.execute(
            """
            SELECT category, COUNT(*) as count 
            FROM events 
            GROUP BY category 
            ORDER BY count DESC
        """
        )
        event_categories = cur.fetchall()

        cur.execute(
            """
            SELECT provider, COUNT(*) as count 
            FROM events 
            GROUP BY provider 
            ORDER BY count DESC
        """
        )
        event_providers = cur.fetchall()

        # Recent activity
        cur.execute(
            """
            SELECT DATE(created_at) as date, COUNT(*) as count
            FROM events 
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(created_at)
            ORDER BY date DESC
        """
        )
        recent_events = cur.fetchall()

        print(f"🏢 Total Venues: {venue_count}")
        if venue_categories:
            print("   Venue Categories:")
            for cat in venue_categories:
                print(f"     • {cat['category']}: {cat['count']}")

        print(f"\n🎉 Total Events: {event_count}")
        if event_categories:
            print("   Event Categories:")
            for cat in event_categories:
                print(f"     • {cat['category']}: {cat['count']}")

        if event_providers:
            print("\n   Event Providers:")
            for provider in event_providers:
                print(f"     • {provider['provider']}: {provider['count']}")

        if recent_events:
            print("\n📅 Recent Scraping Activity (last 7 days):")
            for activity in recent_events:
                print(f"     • {activity['date']}: {activity['count']} events")


def main():
    print("🔍 Venue Scraping Data Viewer")
    print("=" * 50)

    # Check if .env file exists
    if os.path.exists(".env"):
        from dotenv import load_dotenv

        load_dotenv()
        print("✅ Loaded environment variables from .env")
    else:
        print("⚠️  No .env file found - using system environment variables")

    # Connect to database
    conn = get_db_connection()
    if not conn:
        return

    print("✅ Connected to database successfully")

    try:
        # Show summary first
        view_summary(conn)

        # Show venues
        view_venues(conn)

        # Show events
        view_events(conn)

        print("\n" + "=" * 80)
        print("💡 To run the scrapers and get fresh data:")
        print("   python -m src.etl.ingest_local_venues")
        print("   python -m src.etl.ingest_dynamic_venues")
        print("\n💡 To view more data, you can:")
        print("   - Connect to your database directly with psql or pgAdmin")
        print("   - Modify this script to show more entries")
        print("   - Query specific venues or date ranges")

    except Exception as e:
        print(f"❌ Error viewing data: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
