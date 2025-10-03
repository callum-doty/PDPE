#!/usr/bin/env python3
"""
Set up the SQLite database with the required schema.
"""

import sys
import os
import sqlite3

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database.connection import get_database_connection


def create_sqlite_schema():
    """Create SQLite-compatible schema"""

    # SQLite version of the schema (simplified from PostgreSQL)
    sqlite_schema = """
    -- Venues table
    CREATE TABLE IF NOT EXISTS venues (
        venue_id TEXT PRIMARY KEY,
        external_id TEXT,
        provider TEXT,
        name TEXT,
        category TEXT,
        subcategory TEXT,
        price_tier INTEGER,
        avg_rating REAL,
        review_count INTEGER,
        lat REAL,
        lng REAL,
        address TEXT,
        phone TEXT,
        website TEXT,
        hours_json TEXT,
        amenities TEXT,
        psychographic_relevance TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Events table
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        external_id TEXT,
        provider TEXT,
        name TEXT,
        description TEXT,
        category TEXT,
        subcategory TEXT,
        tags TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        venue_id TEXT,
        ticket_price_min REAL,
        ticket_price_max REAL,
        predicted_attendance INTEGER,
        actual_attendance INTEGER,
        psychographic_relevance TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id),
        UNIQUE(external_id, provider)
    );

    -- Collection status tracking
    CREATE TABLE IF NOT EXISTS collection_status (
        source_name TEXT PRIMARY KEY,
        last_successful_collection TIMESTAMP,
        last_attempted_collection TIMESTAMP,
        collection_health_score REAL DEFAULT 0.0,
        error_count INTEGER DEFAULT 0,
        status_details TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Master venue data view (as a regular table in SQLite)
    CREATE TABLE IF NOT EXISTS master_venue_data (
        venue_id TEXT PRIMARY KEY,
        external_id TEXT,
        provider TEXT,
        name TEXT,
        category TEXT,
        subcategory TEXT,
        lat REAL,
        lng REAL,
        address TEXT,
        phone TEXT,
        website TEXT,
        psychographic_relevance TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        data_completeness_score REAL DEFAULT 0.0,
        comprehensive_score REAL DEFAULT 0.0,
        data_source_type TEXT,
        last_refreshed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Master events data view (as a regular table in SQLite)
    CREATE TABLE IF NOT EXISTS master_events_data (
        event_id TEXT PRIMARY KEY,
        external_id TEXT,
        provider TEXT,
        name TEXT,
        description TEXT,
        category TEXT,
        subcategory TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        predicted_attendance INTEGER,
        psychographic_relevance TEXT,
        venue_name TEXT,
        lat REAL,
        lng REAL,
        venue_address TEXT,
        venue_category TEXT,
        event_score REAL DEFAULT 0.0,
        data_source_type TEXT,
        last_refreshed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_venues_location ON venues(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_venues_category ON venues(category);
    CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_time, end_time);
    CREATE INDEX IF NOT EXISTS idx_events_venue ON events(venue_id);
    CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);
    CREATE INDEX IF NOT EXISTS idx_master_venue_data_location ON master_venue_data(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_master_events_data_location ON master_events_data(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_master_events_data_time ON master_events_data(start_time);
    """

    return sqlite_schema


def setup_database():
    """Set up the database with the required schema"""
    print("🔧 Setting up SQLite database...")
    print("=" * 50)

    try:
        with get_database_connection() as db:
            # Get the SQLite schema
            schema = create_sqlite_schema()

            # Split schema into individual statements
            statements = [stmt.strip() for stmt in schema.split(";") if stmt.strip()]

            print(f"📝 Executing {len(statements)} schema statements...")

            for i, statement in enumerate(statements, 1):
                try:
                    db.execute_query(statement)
                    print(f"  ✅ Statement {i}: {statement.split()[0:3]} ...")
                except Exception as e:
                    print(f"  ❌ Statement {i} failed: {e}")
                    print(f"     Statement: {statement[:100]}...")

            print("\n🎯 Verifying table creation...")

            # Verify tables were created
            tables = db.execute_query(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """
            )

            print(f"📊 Created {len(tables)} tables:")
            for table in tables:
                print(f"  • {table['name']}")

            # Check if we have any data
            print("\n📈 Checking for existing data...")

            try:
                venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[
                    0
                ]["count"]
                event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[
                    0
                ]["count"]
                print(f"  • Venues: {venue_count}")
                print(f"  • Events: {event_count}")

                if venue_count == 0 and event_count == 0:
                    print(
                        "\n💡 Database is empty. You may want to run data collection."
                    )
                    print(
                        '   Try: python -c "from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator; MasterDataOrchestrator().collect_priority_data()"'
                    )

            except Exception as e:
                print(f"  ❌ Error checking data: {e}")

            print("\n✅ Database setup completed successfully!")
            return True

    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False


if __name__ == "__main__":
    setup_database()
