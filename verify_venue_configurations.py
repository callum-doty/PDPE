#!/usr/bin/env python3
"""
Simple Venue Configuration Verification Script

Verifies that all 29 venues from the user's list are properly configured
in the scraper files with correct URLs and categories.
"""

import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


def verify_configurations():
    """Verify venue configurations without running scrapers."""

    print("=" * 80)
    print("VENUE CONFIGURATION VERIFICATION")
    print("=" * 80)

    # Expected venues from user specification
    expected_venues = {
        # Major Venues (Static HTML)
        "T-Mobile Center": {
            "url": "https://www.t-mobilecenter.com/events",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "Uptown Theater": {
            "url": "https://www.uptowntheater.com/events",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "Kauffman Center for the Performing Arts": {
            "url": "https://www.kauffmancenter.org/events/",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "Starlight Theatre": {
            "url": "https://www.kcstarlight.com/events/",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "The Midland Theatre": {
            "url": "https://www.midlandkc.com/events",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "Knuckleheads Saloon": {
            "url": "https://knuckleheadskc.com/",
            "category": "major_venue",
            "scrape_type": "static",
        },
        "Azura Amphitheater": {
            "url": "https://www.azuraamphitheater.com/events",
            "category": "major_venue",
            "scrape_type": "static",
        },
        # Entertainment Districts (Static HTML)
        "Power & Light District": {
            "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
            "category": "entertainment_district",
            "scrape_type": "static",
        },
        "Westport KC": {
            "url": "https://westportkcmo.com/events/",
            "category": "entertainment_district",
            "scrape_type": "static",
        },
        "18th & Vine Jazz District": {
            "url": "https://kcjazzdistrict.org/events/",
            "category": "entertainment_district",
            "scrape_type": "static",
        },
        "Crossroads KC": {
            "url": "https://www.crossroadskc.com/shows",
            "category": "entertainment_district",
            "scrape_type": "static",
        },
        # Shopping & Cultural (Static HTML)
        "Country Club Plaza": {
            "url": "https://countryclubplaza.com/events/",
            "category": "shopping_cultural",
            "scrape_type": "static",
        },
        "Crown Center": {
            "url": "https://www.crowncenter.com/events",
            "category": "shopping_cultural",
            "scrape_type": "static",
        },
        "Union Station Kansas City": {
            "url": "https://unionstation.org/events/",
            "category": "shopping_cultural",
            "scrape_type": "static",
        },
        # Museums (Static HTML)
        "Nelson-Atkins Museum of Art": {
            "url": "https://www.nelson-atkins.org/events/",
            "category": "museum",
            "scrape_type": "static",
        },
        "National WWI Museum": {
            "url": "https://theworldwar.org/visit/upcoming-events",
            "category": "museum",
            "scrape_type": "static",
        },
        "Science City": {
            "url": "https://sciencecity.unionstation.org/",
            "category": "museum",
            "scrape_type": "static",
        },
        # Theater (Static HTML)
        "KC Repertory Theatre": {
            "url": "https://kcrep.org/season/",
            "category": "theater",
            "scrape_type": "static",
        },
        "Unicorn Theatre": {
            "url": "https://unicorntheatre.org/",
            "category": "theater",
            "scrape_type": "static",
        },
        # Festival & City (Static HTML)
        "Kansas City Parks & Rec": {
            "url": "https://kcparks.org/events/",
            "category": "festival_city",
            "scrape_type": "static",
        },
        "City Market KC": {
            "url": "https://citymarketkc.org/events/",
            "category": "festival_city",
            "scrape_type": "static",
        },
        "Boulevardia Festival": {
            "url": "https://www.boulevardia.com/",
            "category": "festival_city",
            "scrape_type": "static",
        },
        "Irish Fest KC": {
            "url": "https://kcirishfest.com/",
            "category": "festival_city",
            "scrape_type": "static",
        },
        # Aggregators (Dynamic JS)
        "Visit KC": {
            "url": "https://www.visitkc.com/events",
            "category": "aggregator",
            "scrape_type": "dynamic",
        },
        "Do816": {
            "url": "https://do816.com/events",
            "category": "aggregator",
            "scrape_type": "dynamic",
        },
        "The Pitch KC": {
            "url": "https://calendar.thepitchkc.com/",
            "category": "aggregator",
            "scrape_type": "dynamic",
        },
        "Kansas City Magazine Events": {
            "url": "https://events.kansascitymag.com/",
            "category": "aggregator",
            "scrape_type": "dynamic",
        },
        "Event KC": {
            "url": "https://www.eventkc.com/",
            "category": "aggregator",
            "scrape_type": "dynamic",
        },
        # Nightlife (Dynamic JS)
        "Aura KC Nightclub": {
            "url": "https://www.aurakc.com/",
            "category": "nightlife",
            "scrape_type": "dynamic",
        },
    }

    try:
        # Import the scrapers
        from etl.ingest_local_venues import VENUE_SCRAPERS
        from etl.ingest_dynamic_venues import DYNAMIC_VENUE_SCRAPERS

        print(f"✓ Successfully imported scraper configurations")
        print(f"  Static venues configured: {len(VENUE_SCRAPERS)}")
        print(f"  Dynamic venues configured: {len(DYNAMIC_VENUE_SCRAPERS)}")

    except ImportError as e:
        print(f"✗ Failed to import scrapers: {e}")
        return False

    # Verify static venues
    print(f"\nVERIFYING STATIC VENUES:")
    static_issues = []

    for venue_name, expected in expected_venues.items():
        if expected["scrape_type"] == "static":
            found = False
            for key, config in VENUE_SCRAPERS.items():
                if config["name"] == venue_name:
                    found = True
                    # Check URL
                    if config["events_url"] != expected["url"]:
                        static_issues.append(
                            f"{venue_name}: URL mismatch - expected {expected['url']}, got {config['events_url']}"
                        )
                    # Check category
                    if config["category"] != expected["category"]:
                        static_issues.append(
                            f"{venue_name}: Category mismatch - expected {expected['category']}, got {config['category']}"
                        )
                    break

            if not found:
                static_issues.append(f"{venue_name}: Not found in static scrapers")
            else:
                print(f"  ✓ {venue_name}")

    # Verify dynamic venues
    print(f"\nVERIFYING DYNAMIC VENUES:")
    dynamic_issues = []

    for venue_name, expected in expected_venues.items():
        if expected["scrape_type"] == "dynamic":
            found = False
            for key, config in DYNAMIC_VENUE_SCRAPERS.items():
                if config["name"] == venue_name:
                    found = True
                    # Check URL
                    if config["events_url"] != expected["url"]:
                        dynamic_issues.append(
                            f"{venue_name}: URL mismatch - expected {expected['url']}, got {config['events_url']}"
                        )
                    # Check category
                    if config["category"] != expected["category"]:
                        dynamic_issues.append(
                            f"{venue_name}: Category mismatch - expected {expected['category']}, got {config['category']}"
                        )
                    break

            if not found:
                dynamic_issues.append(f"{venue_name}: Not found in dynamic scrapers")
            else:
                print(f"  ✓ {venue_name}")

    # Report results
    print(f"\n" + "=" * 80)
    print("VERIFICATION RESULTS")
    print("=" * 80)

    total_venues = len(expected_venues)
    total_issues = len(static_issues) + len(dynamic_issues)

    print(f"Total venues expected: {total_venues}")
    print(f"Total issues found: {total_issues}")

    if static_issues:
        print(f"\nSTATIC VENUE ISSUES ({len(static_issues)}):")
        for issue in static_issues:
            print(f"  - {issue}")

    if dynamic_issues:
        print(f"\nDYNAMIC VENUE ISSUES ({len(dynamic_issues)}):")
        for issue in dynamic_issues:
            print(f"  - {issue}")

    if total_issues == 0:
        print(f"\n🎉 SUCCESS: All {total_venues} venues are properly configured!")
        return True
    else:
        print(
            f"\n⚠️  ISSUES FOUND: {total_issues} configuration problems need to be fixed."
        )
        return False


if __name__ == "__main__":
    success = verify_configurations()
    sys.exit(0 if success else 1)
