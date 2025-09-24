#!/usr/bin/env python3
"""
Venue Configuration Audit Script

Compares the required venue list against current scraper configurations
and identifies gaps, mismatches, and required updates.
"""

import json
from typing import Dict, List, Set

# Required venues from user specification
REQUIRED_VENUES = {
    # Major Venues (Static HTML)
    "T-Mobile Center": {
        "url": "https://www.t-mobilecenter.com/events",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "Uptown Theater": {
        "url": "https://www.uptowntheater.com/events",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "Kauffman Center for the Performing Arts": {
        "url": "https://www.kauffmancenter.org/events/",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "Starlight Theatre": {
        "url": "https://www.kcstarlight.com/events/",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "The Midland Theatre": {
        "url": "https://www.midlandkc.com/events",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "Knuckleheads Saloon": {
        "url": "https://knuckleheadskc.com/",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    "Azura Amphitheater": {
        "url": "https://www.azuraamphitheater.com/events",
        "category": "Major Venue",
        "scrape_complexity": "Static HTML",
    },
    # Entertainment Districts (Static HTML)
    "Power & Light District": {
        "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
        "category": "Entertainment District",
        "scrape_complexity": "Static HTML",
    },
    "Westport KC": {
        "url": "https://westportkcmo.com/events/",
        "category": "Entertainment District",
        "scrape_complexity": "Static HTML",
    },
    "18th & Vine Jazz District": {
        "url": "https://kcjazzdistrict.org/events/",
        "category": "Entertainment District",
        "scrape_complexity": "Static HTML",
    },
    "Crossroads KC": {
        "url": "https://www.crossroadskc.com/shows",
        "category": "Entertainment District",
        "scrape_complexity": "Static HTML",
    },
    # Shopping & Cultural (Static HTML)
    "Country Club Plaza": {
        "url": "https://countryclubplaza.com/events/",
        "category": "Shopping & Cultural",
        "scrape_complexity": "Static HTML",
    },
    "Crown Center": {
        "url": "https://www.crowncenter.com/events",
        "category": "Shopping & Cultural",
        "scrape_complexity": "Static HTML",
    },
    "Union Station Kansas City": {
        "url": "https://unionstation.org/events/",
        "category": "Shopping & Cultural",
        "scrape_complexity": "Static HTML",
    },
    # Museums (Static HTML)
    "Nelson-Atkins Museum of Art": {
        "url": "https://www.nelson-atkins.org/events/",
        "category": "Museum",
        "scrape_complexity": "Static HTML",
    },
    "National WWI Museum": {
        "url": "https://theworldwar.org/visit/upcoming-events",
        "category": "Museum",
        "scrape_complexity": "Static HTML",
    },
    "Science City": {
        "url": "https://sciencecity.unionstation.org/",
        "category": "Museum",
        "scrape_complexity": "Static HTML",
    },
    # Theater (Static HTML)
    "KC Repertory Theatre": {
        "url": "https://kcrep.org/season/",
        "category": "Theater",
        "scrape_complexity": "Static HTML",
    },
    "Unicorn Theatre": {
        "url": "https://unicorntheatre.org/",
        "category": "Theater",
        "scrape_complexity": "Static HTML",
    },
    # Festival & City (Static HTML)
    "Kansas City Parks & Rec": {
        "url": "https://kcparks.org/events/",
        "category": "Festival & City",
        "scrape_complexity": "Static HTML",
    },
    "City Market KC": {
        "url": "https://citymarketkc.org/events/",
        "category": "Festival & City",
        "scrape_complexity": "Static HTML",
    },
    "Boulevardia Festival": {
        "url": "https://www.boulevardia.com/",
        "category": "Festival & City",
        "scrape_complexity": "Static HTML",
    },
    "Irish Fest KC": {
        "url": "https://kcirishfest.com/",
        "category": "Festival & City",
        "scrape_complexity": "Static HTML",
    },
    # Aggregators (Dynamic JS)
    "Visit KC": {
        "url": "https://www.visitkc.com/events",
        "category": "Aggregator",
        "scrape_complexity": "Dynamic JS",
    },
    "Do816": {
        "url": "https://do816.com/events",
        "category": "Aggregator",
        "scrape_complexity": "Dynamic JS",
    },
    "The Pitch KC": {
        "url": "https://calendar.thepitchkc.com/",
        "category": "Aggregator",
        "scrape_complexity": "Dynamic JS",
    },
    "Kansas City Magazine Events": {
        "url": "https://events.kansascitymag.com/",
        "category": "Aggregator",
        "scrape_complexity": "Dynamic JS",
    },
    "Event KC": {
        "url": "https://www.eventkc.com/",
        "category": "Aggregator",
        "scrape_complexity": "Dynamic JS",
    },
    # Nightlife (Dynamic JS)
    "Aura KC Nightclub": {
        "url": "https://www.aurakc.com/",
        "category": "Nightlife",
        "scrape_complexity": "Dynamic JS",
    },
}

# Current static venue configurations (from ingest_local_venues.py)
CURRENT_STATIC_VENUES = {
    "T-Mobile Center": "https://www.t-mobilecenter.com/events",
    "Uptown Theater": "https://www.uptowntheater.com/events",
    "Kauffman Center for the Performing Arts": "https://www.kauffmancenter.org/events/",
    "Starlight Theatre": "https://www.kcstarlight.com/events/",
    "The Midland Theatre": "https://www.midlandkc.com/events",
    "Knuckleheads Saloon": "https://knuckleheadskc.com/",
    "Azura Amphitheater": "https://www.azuraamphitheater.com/events",
    "Power & Light District": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
    "Westport KC": "https://westportkcmo.com/events/",
    "18th & Vine Jazz District": "https://kcjazzdistrict.org/events/",
    "Crossroads KC": "https://www.crossroadskc.com/shows",
    "Country Club Plaza": "https://countryclubplaza.com/events/",
    "Crown Center": "https://www.crowncenter.com/events",
    "Union Station Kansas City": "https://unionstation.org/events/",
    "Nelson-Atkins Museum of Art": "https://www.nelson-atkins.org/events/",
    "National WWI Museum": "https://theworldwar.org/visit/upcoming-events",
    "Science City": "https://sciencecity.unionstation.org/",
    "KC Repertory Theatre": "https://kcrep.org/season/",
    "Unicorn Theatre": "https://unicorntheatre.org/",
    "Kansas City Parks & Rec": "https://kcparks.org/events/",
    "City Market KC": "https://citymarketkc.org/events/",
    "Boulevardia Festival": "https://www.boulevardia.com/",
    "Irish Fest KC": "https://kcirishfest.com/",
}

# Current dynamic venue configurations (from ingest_dynamic_venues.py)
CURRENT_DYNAMIC_VENUES = {
    "Visit KC": "https://www.visitkc.com/events",
    "Do816": "https://do816.com/events",
    "The Pitch KC": "https://calendar.thepitchkc.com/",
    "Kansas City Magazine Events": "https://events.kansascitymag.com/",
    "Event KC": "https://www.eventkc.com/",
    "Aura KC Nightclub": "https://www.aurakc.com/",
}


def audit_venue_configurations():
    """Perform comprehensive audit of venue configurations."""

    print("=" * 80)
    print("VENUE CONFIGURATION AUDIT REPORT")
    print("=" * 80)

    # Get all required venue names
    required_names = set(REQUIRED_VENUES.keys())
    current_static_names = set(CURRENT_STATIC_VENUES.keys())
    current_dynamic_names = set(CURRENT_DYNAMIC_VENUES.keys())
    current_all_names = current_static_names | current_dynamic_names

    print(f"\nREQUIRED VENUES: {len(required_names)}")
    print(f"CURRENTLY CONFIGURED: {len(current_all_names)}")

    # Check coverage
    missing_venues = required_names - current_all_names
    extra_venues = current_all_names - required_names

    print(f"\n1. COVERAGE ANALYSIS")
    print(f"   Missing venues: {len(missing_venues)}")
    if missing_venues:
        for venue in sorted(missing_venues):
            print(f"     - {venue}")

    print(f"   Extra venues (not in required list): {len(extra_venues)}")
    if extra_venues:
        for venue in sorted(extra_venues):
            print(f"     - {venue}")

    # Check URL matches
    print(f"\n2. URL VERIFICATION")
    url_mismatches = []

    for venue_name in required_names & current_all_names:
        required_url = REQUIRED_VENUES[venue_name]["url"]

        if venue_name in CURRENT_STATIC_VENUES:
            current_url = CURRENT_STATIC_VENUES[venue_name]
        else:
            current_url = CURRENT_DYNAMIC_VENUES[venue_name]

        if required_url != current_url:
            url_mismatches.append(
                {"venue": venue_name, "required": required_url, "current": current_url}
            )

    print(f"   URL mismatches: {len(url_mismatches)}")
    for mismatch in url_mismatches:
        print(f"     - {mismatch['venue']}")
        print(f"       Required: {mismatch['required']}")
        print(f"       Current:  {mismatch['current']}")

    # Check scrape complexity classification
    print(f"\n3. SCRAPE COMPLEXITY VERIFICATION")
    complexity_mismatches = []

    for venue_name in required_names & current_all_names:
        required_complexity = REQUIRED_VENUES[venue_name]["scrape_complexity"]

        if venue_name in CURRENT_STATIC_VENUES and required_complexity == "Dynamic JS":
            complexity_mismatches.append(
                {
                    "venue": venue_name,
                    "required": "Dynamic JS",
                    "current": "Static HTML",
                }
            )
        elif (
            venue_name in CURRENT_DYNAMIC_VENUES
            and required_complexity == "Static HTML"
        ):
            complexity_mismatches.append(
                {
                    "venue": venue_name,
                    "required": "Static HTML",
                    "current": "Dynamic JS",
                }
            )

    print(f"   Complexity mismatches: {len(complexity_mismatches)}")
    for mismatch in complexity_mismatches:
        print(f"     - {mismatch['venue']}")
        print(f"       Required: {mismatch['required']}")
        print(f"       Current:  {mismatch['current']}")

    # Category mapping analysis
    print(f"\n4. CATEGORY MAPPING")
    category_counts = {}
    for venue_name, venue_info in REQUIRED_VENUES.items():
        category = venue_info["category"]
        if category not in category_counts:
            category_counts[category] = []
        category_counts[category].append(venue_name)

    for category, venues in category_counts.items():
        print(f"   {category}: {len(venues)} venues")
        for venue in sorted(venues):
            status = "✓" if venue in current_all_names else "✗"
            print(f"     {status} {venue}")

    # Summary and recommendations
    print(f"\n5. SUMMARY & RECOMMENDATIONS")

    total_issues = (
        len(missing_venues) + len(url_mismatches) + len(complexity_mismatches)
    )

    if total_issues == 0:
        print("   ✓ All venues are properly configured!")
    else:
        print(
            f"   Found {total_issues} configuration issues that need to be addressed:"
        )

        if missing_venues:
            print(f"     - Add {len(missing_venues)} missing venues")
        if url_mismatches:
            print(f"     - Fix {len(url_mismatches)} URL mismatches")
        if complexity_mismatches:
            print(
                f"     - Reclassify {len(complexity_mismatches)} venues for correct scraping method"
            )

    print(f"\n6. REQUIRED ACTIONS")
    print("   1. Update ingest_local_venues.py with missing static venues")
    print("   2. Update ingest_dynamic_venues.py with missing dynamic venues")
    print("   3. Fix URL mismatches in both files")
    print("   4. Move venues between static/dynamic scrapers as needed")
    print("   5. Update category mappings to match database schema")

    return {
        "missing_venues": missing_venues,
        "url_mismatches": url_mismatches,
        "complexity_mismatches": complexity_mismatches,
        "category_counts": category_counts,
        "total_issues": total_issues,
    }


def generate_category_mapping():
    """Generate mapping from user categories to database categories."""

    category_mapping = {
        "Major Venue": "major_venue",
        "Entertainment District": "entertainment_district",
        "Shopping & Cultural": "shopping_cultural",
        "Museum": "museum",
        "Theater": "theater",
        "Festival & City": "festival_city",
        "Aggregator": "aggregator",
        "Nightlife": "nightlife",
    }

    print(f"\n7. CATEGORY MAPPING FOR DATABASE")
    print("   User Category -> Database Category")
    for user_cat, db_cat in category_mapping.items():
        print(f"   {user_cat} -> {db_cat}")

    return category_mapping


if __name__ == "__main__":
    audit_results = audit_venue_configurations()
    category_mapping = generate_category_mapping()

    print(f"\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
