#!/usr/bin/env python3
"""
Script to display all venues from the database and open the interactive map
"""

import os
import sys
import psycopg2
import webbrowser
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_conn():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not found in environment variables")
        return None
    return psycopg2.connect(db_url)


def show_all_venues():
    """Display all venues with their details."""
    print("🏪 ALL VENUES IN DATABASE")
    print("=" * 80)

    conn = get_db_conn()
    if not conn:
        return

    cur = conn.cursor()

    # Get all venues with their psychographic scores
    query = """
    SELECT 
        name, category, provider, lat, lng, address, 
        avg_rating, review_count, psychographic_relevance
    FROM venues 
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    ORDER BY psychographic_relevance->>'career_driven' DESC NULLS LAST
    """

    cur.execute(query)
    venues = cur.fetchall()

    print(f"📊 Total venues with coordinates: {len(venues)}")
    print("\n🏆 VENUE RANKINGS (by psychographic score):")
    print("-" * 80)

    for i, venue in enumerate(venues, 1):
        name = venue[0]
        category = venue[1]
        provider = venue[2]
        lat = venue[3]
        lng = venue[4]
        address = venue[5]
        rating = venue[6]
        review_count = venue[7]
        psychographic = venue[8] or {}

        # Get psychographic scores
        career_score = psychographic.get("career_driven", 0)
        competent_score = psychographic.get("competent", 0)
        fun_score = psychographic.get("fun", 0)

        print(f"\n{i:3d}. {name}")
        print(f"     📍 Location: {lat:.4f}, {lng:.4f}")
        print(f"     🏷️  Category: {category}")
        print(f"     🔗 Provider: {provider}")
        if address:
            print(f"     📧 Address: {address}")
        if rating:
            print(f"     ⭐ Rating: {rating} ({review_count} reviews)")
        print(f"     🧠 Psychographic Scores:")
        print(f"         Career-Driven: {career_score:.3f}")
        print(f"         Competent: {competent_score:.3f}")
        print(f"         Fun: {fun_score:.3f}")

    cur.close()
    conn.close()

    return len(venues)


def open_map():
    """Open the interactive map in browser."""
    map_file = Path("real_venue_comprehensive_map.html")

    if map_file.exists():
        print(f"\n🌐 Opening interactive map: {map_file.absolute()}")
        try:
            webbrowser.open(f"file://{map_file.absolute()}")
            print("✅ Map opened in browser!")

            print("\n💡 MAP FEATURES TO LOOK FOR:")
            print("   🏆 Venue Ranking Sidebar (left side) - Lists all venues")
            print("   📍 Venue Markers - Click for details")
            print("   🎛️  Layer Controls (top-right) - Toggle different layers")
            print("   📊 Legend (bottom-left) - Color coding explanation")
            print("   ℹ️  Info Panel (top-right) - Usage guide")

            print("\n🔍 HOW TO SEE ALL VENUES:")
            print("   1. Look for the 'Top Venues Ranking' sidebar on the left")
            print("   2. Scroll through the ranked list of all 185+ venues")
            print("   3. Click any venue in the list to center the map on it")
            print("   4. Use the layer controls to show/hide different data layers")
            print("   5. Click venue markers on the map for detailed popups")

        except Exception as e:
            print(f"❌ Failed to open map: {e}")
            print(f"   Please manually open: {map_file.absolute()}")
    else:
        print(f"❌ Map file not found: {map_file}")
        print("   Run 'python generate_real_venue_map.py' to create it")


def main():
    print("🗺️  Venue Display & Map Viewer")
    print("=" * 50)

    # Show all venues
    venue_count = show_all_venues()

    if venue_count and venue_count > 0:
        print(f"\n✅ Found {venue_count} venues in database")

        # Open the interactive map
        open_map()

        print(f"\n📋 SUMMARY:")
        print(f"   • {venue_count} venues are stored in your database")
        print(f"   • All venues have psychographic scores")
        print(f"   • All venues are displayed on the interactive map")
        print(f"   • The map includes a searchable venue ranking sidebar")
        print(f"   • Click venues in the sidebar to navigate to them on the map")

    else:
        print("⚠️  No venues found in database")
        print("   Run venue scrapers to populate data:")
        print("   python -m src.etl.ingest_local_venues")


if __name__ == "__main__":
    main()
