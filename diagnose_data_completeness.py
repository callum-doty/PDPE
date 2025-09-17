#!/usr/bin/env python3
"""
Data Completeness Diagnostic Tool

This script diagnoses why venue data completeness is only 12.5% and identifies
which data sources need to be populated to achieve comprehensive venue profiles.
"""

import sys
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.etl.utils import get_db_conn


def diagnose_data_sources():
    """
    Diagnose which data sources exist and have data vs which are missing
    """
    print("🔍 DIAGNOSING DATA COMPLETENESS ISSUES")
    print("=" * 60)

    # Connect to database
    print("1. Connecting to database...")
    db_conn = get_db_conn()
    if not db_conn:
        print("❌ Database connection failed")
        return
    print("✅ Database connected")

    cur = db_conn.cursor(cursor_factory=RealDictCursor)

    # Check all expected tables and their data
    expected_tables = {
        "venues": "Core venue information",
        "events": "Event data associated with venues",
        "weather_data": "Weather conditions by location",
        "traffic_data": "Traffic conditions by venue",
        "social_sentiment": "Social media sentiment by venue",
        "venue_traffic": "Foot traffic data by venue",
        "demographics": "Demographic data by location",
        "predictions": "ML predictions by venue",
        "psychographic_layers": "Psychographic layer data",
    }

    print(f"\n2. Checking data source availability...")
    print("-" * 60)

    data_status = {}

    for table_name, description in expected_tables.items():
        try:
            # Check if table exists
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """,
                (table_name,),
            )

            table_exists = cur.fetchone()[0]

            if not table_exists:
                print(f"❌ {table_name:20} | Table does not exist | {description}")
                data_status[table_name] = {"exists": False, "count": 0, "recent": 0}
                continue

            # Check row count
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cur.fetchone()[0]

            # Check recent data (last 7 days) if table has timestamp column
            recent_count = 0
            timestamp_cols = ["ts", "created_at", "updated_at", "timestamp"]

            for ts_col in timestamp_cols:
                try:
                    cur.execute(
                        f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE {ts_col} >= NOW() - INTERVAL '7 days'
                    """
                    )
                    recent_count = cur.fetchone()[0]
                    break
                except:
                    continue

            # Status indicator
            if total_count == 0:
                status = "❌ EMPTY"
            elif recent_count > 0:
                status = "✅ ACTIVE"
            else:
                status = "⚠️  OLD DATA"

            print(
                f"{status} {table_name:20} | {total_count:6,} total | {recent_count:6,} recent | {description}"
            )
            data_status[table_name] = {
                "exists": True,
                "count": total_count,
                "recent": recent_count,
                "status": status,
            }

        except Exception as e:
            print(f"❌ {table_name:20} | Error: {str(e)[:50]}... | {description}")
            data_status[table_name] = {
                "exists": False,
                "count": 0,
                "recent": 0,
                "error": str(e),
            }

    # Analyze venue data completeness in detail
    print(f"\n3. Analyzing venue data completeness...")
    print("-" * 60)

    try:
        # Get sample venues and check what data they have
        cur.execute(
            """
            SELECT venue_id, name, category, lat, lng, 
                   psychographic_relevance,
                   (CASE WHEN psychographic_relevance IS NOT NULL THEN 1 ELSE 0 END) as has_psychographic
            FROM venues 
            WHERE lat IS NOT NULL AND lng IS NOT NULL
            LIMIT 5
        """
        )

        sample_venues = cur.fetchall()

        if sample_venues:
            print("Sample venue data completeness analysis:")

            for venue in sample_venues:
                venue_id = venue["venue_id"]
                name = venue["name"][:30]

                print(f"\n🏢 {name} (ID: {venue_id})")

                # Check each data source for this venue
                completeness_score = 0
                total_sources = 8

                # 1. Basic venue data (always present if we got this far)
                print("   ✅ Basic venue data")
                completeness_score += 1

                # 2. Psychographic scores
                if venue["has_psychographic"]:
                    print("   ✅ Psychographic scores")
                    completeness_score += 1
                else:
                    print("   ❌ Psychographic scores")

                # 3. Weather data
                if data_status.get("weather_data", {}).get("count", 0) > 0:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM weather_data 
                        WHERE ST_DWithin(
                            ST_Point(lng, lat)::geography,
                            ST_Point(%s, %s)::geography,
                            5000
                        )
                        AND ts >= NOW() - INTERVAL '6 hours'
                    """,
                        (venue["lng"], venue["lat"]),
                    )

                    weather_count = cur.fetchone()[0]
                    if weather_count > 0:
                        print("   ✅ Weather data")
                        completeness_score += 1
                    else:
                        print("   ❌ Weather data (no recent data within 5km)")
                else:
                    print("   ❌ Weather data (table empty)")

                # 4. Traffic data
                if data_status.get("traffic_data", {}).get("count", 0) > 0:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM traffic_data 
                        WHERE venue_id = %s
                        AND ts >= NOW() - INTERVAL '2 hours'
                    """,
                        (venue_id,),
                    )

                    traffic_count = cur.fetchone()[0]
                    if traffic_count > 0:
                        print("   ✅ Traffic data")
                        completeness_score += 1
                    else:
                        print("   ❌ Traffic data")
                else:
                    print("   ❌ Traffic data (table empty)")

                # 5. Social sentiment
                if data_status.get("social_sentiment", {}).get("count", 0) > 0:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM social_sentiment 
                        WHERE venue_id = %s
                        AND ts >= NOW() - INTERVAL '24 hours'
                    """,
                        (venue_id,),
                    )

                    social_count = cur.fetchone()[0]
                    if social_count > 0:
                        print("   ✅ Social sentiment")
                        completeness_score += 1
                    else:
                        print("   ❌ Social sentiment")
                else:
                    print("   ❌ Social sentiment (table empty)")

                # 6. Foot traffic
                if data_status.get("venue_traffic", {}).get("count", 0) > 0:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM venue_traffic 
                        WHERE venue_id = %s
                        AND ts >= NOW() - INTERVAL '24 hours'
                    """,
                        (venue_id,),
                    )

                    foot_traffic_count = cur.fetchone()[0]
                    if foot_traffic_count > 0:
                        print("   ✅ Foot traffic")
                        completeness_score += 1
                    else:
                        print("   ❌ Foot traffic")
                else:
                    print("   ❌ Foot traffic (table empty)")

                # 7. Demographics
                if data_status.get("demographics", {}).get("count", 0) > 0:
                    print("   ✅ Demographics (table exists)")
                    completeness_score += 1
                else:
                    print("   ❌ Demographics (table empty)")

                # 8. Events
                if data_status.get("events", {}).get("count", 0) > 0:
                    cur.execute(
                        """
                        SELECT COUNT(*) FROM events 
                        WHERE venue_id = %s
                    """,
                        (venue_id,),
                    )

                    events_count = cur.fetchone()[0]
                    if events_count > 0:
                        print("   ✅ Events")
                        completeness_score += 1
                    else:
                        print("   ❌ Events")
                else:
                    print("   ❌ Events (table empty)")

                completeness_pct = (completeness_score / total_sources) * 100
                print(
                    f"   📊 Completeness: {completeness_score}/{total_sources} ({completeness_pct:.1f}%)"
                )

    except Exception as e:
        print(f"❌ Error analyzing venue completeness: {e}")

    # Generate recommendations
    print(f"\n4. Recommendations to improve data completeness...")
    print("-" * 60)

    missing_sources = []
    for table_name, status in data_status.items():
        if not status.get("exists", False) or status.get("count", 0) == 0:
            missing_sources.append(table_name)

    if missing_sources:
        print("🎯 PRIORITY ACTIONS TO INCREASE DATA COMPLETENESS:")

        action_map = {
            "weather_data": "Run: python src/etl/ingest_weather.py",
            "traffic_data": "Run: python src/etl/ingest_traffic.py",
            "social_sentiment": "Run: python src/etl/ingest_social.py",
            "venue_traffic": "Run: python src/etl/ingest_foot_traffic.py",
            "demographics": "Run: python src/etl/ingest_census.py",
            "events": "Run: python src/etl/ingest_events.py",
            "predictions": "Run: python src/backend/models/train.py",
        }

        for i, source in enumerate(missing_sources, 1):
            action = action_map.get(source, f"Investigate {source} data source")
            print(f"   {i}. {source:20} → {action}")

    else:
        print("✅ All expected data sources have data!")

    # Show expected improvement
    current_completeness = 12.5
    potential_sources = len([s for s in data_status.values() if s.get("exists", False)])
    potential_completeness = (potential_sources / 8) * 100

    print(f"\n📈 EXPECTED IMPROVEMENT:")
    print(f"   Current completeness: {current_completeness}%")
    print(f"   Potential completeness: {potential_completeness:.1f}%")
    print(
        f"   Improvement: +{potential_completeness - current_completeness:.1f} percentage points"
    )

    cur.close()
    db_conn.close()

    return data_status, missing_sources


if __name__ == "__main__":
    print("DATA COMPLETENESS DIAGNOSTIC TOOL")
    print("=" * 60)
    print("Analyzing why venue data completeness is only 12.5%...")
    print()

    try:
        data_status, missing_sources = diagnose_data_sources()

        print(f"\n" + "=" * 60)
        print("🎯 DIAGNOSIS COMPLETE")
        print("=" * 60)

        if missing_sources:
            print(
                f"Found {len(missing_sources)} missing data sources causing low completeness."
            )
            print("Run the recommended ingestion scripts to populate missing data.")
        else:
            print(
                "All data sources exist - investigate data quality and linkage issues."
            )

    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        import traceback

        traceback.print_exc()
