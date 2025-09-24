#!/usr/bin/env python3
"""
Script to update the existing venue scrapers with improved selectors
and create a comprehensive venue event scraping system.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))


def update_local_venue_selectors():
    """Update the local venue scraper with improved selectors"""

    # Enhanced selectors based on diagnostic findings
    enhanced_selectors = {
        # Major Venues
        "tmobile_center": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"], [class*="name"]',
            "date": '.date, [class*="date"], time, [class*="time"], [class*="when"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], [class*="summary"], p',
            "link": "a",
        },
        "uptown_theater": {
            "event_container": '[class*="event"], [class*="show"], [class*="listing"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"], [class*="name"]',
            "date": '.date, [class*="date"], time, [class*="time"], [class*="when"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "kauffman_center": {
            "event_container": '[class*="event"], [class*="performance"], [class*="show"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "starlight_theatre": {
            "event_container": '[class*="event"], [class*="show"], [class*="performance"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "midland_theatre": {
            "event_container": '[class*="event"], [class*="show"], [class*="listing"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "knuckleheads": {
            "event_container": '[class*="event"], [class*="show"], [class*="concert"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "azura_amphitheater": {
            "event_container": '[class*="event"], [class*="concert"], [class*="show"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        # Entertainment Districts
        "powerandlight": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "westport": {
            "event_container": '[class*="event"], [class*="listing"], article, .post',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "jazz_district": {
            "event_container": '[class*="event"], [class*="show"], article, .post',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "crossroads": {
            "event_container": '[class*="show"], [class*="event"], article, .post',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        # Shopping & Cultural
        "plaza": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "crown_center": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "union_station": {
            "event_container": '[class*="event"], article, .post, [class*="card"]',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        # Museums
        "nelson_atkins": {
            "event_container": '[class*="event"], [class*="show"], [class*="card"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "wwi_museum": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "science_city": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        # Theaters
        "kc_rep": {
            "event_container": '[class*="show"], [class*="production"], [class*="event"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "unicorn_theatre": {
            "event_container": '[class*="show"], [class*="production"], [class*="event"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        # Festival & City
        "kc_parks": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "city_market": {
            "event_container": '[class*="event"], [class*="market"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "boulevardia": {
            "event_container": '[class*="event"], [class*="festival"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
        "irish_fest": {
            "event_container": '[class*="event"], [class*="festival"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    }

    return enhanced_selectors


def update_dynamic_venue_selectors():
    """Update the dynamic venue scraper with improved selectors"""

    enhanced_dynamic_selectors = {
        "visitkc_dynamic": {
            "wait_for": '[class*="event"], [class*="listing"], article, .card',
            "selectors": {
                "event_container": '[class*="event"], [class*="listing"], article, .card',
                "title": 'h1, h2, h3, h4, .title, [class*="title"], [class*="name"]',
                "date": '.date, [class*="date"], time, [class*="time"], [class*="when"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
        "do816_dynamic": {
            "wait_for": '[class*="event"], [class*="listing"], article, .card',
            "selectors": {
                "event_container": '[class*="event"], [class*="listing"], article, .card',
                "title": 'h1, h2, h3, h4, .title, [class*="title"]',
                "date": '.date, [class*="date"], time, [class*="time"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
        "thepitchkc_dynamic": {
            "wait_for": '[class*="event"], [class*="listing"], article, .post',
            "selectors": {
                "event_container": '[class*="event"], [class*="listing"], article, .post',
                "title": 'h1, h2, h3, h4, .title, [class*="title"]',
                "date": '.date, [class*="date"], time, [class*="time"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
        "kc_magazine": {
            "wait_for": '[class*="event"], [class*="listing"], article, .card',
            "selectors": {
                "event_container": '[class*="event"], [class*="listing"], article, .card',
                "title": 'h1, h2, h3, h4, .title, [class*="title"]',
                "date": '.date, [class*="date"], time, [class*="time"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
        "eventkc": {
            "wait_for": '[class*="event"], [class*="listing"], article, .card',
            "selectors": {
                "event_container": '[class*="event"], [class*="listing"], article, .card',
                "title": 'h1, h2, h3, h4, .title, [class*="title"]',
                "date": '.date, [class*="date"], time, [class*="time"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
        "aura_kc": {
            "wait_for": '[class*="event"], [class*="party"], article, .card',
            "selectors": {
                "event_container": '[class*="event"], [class*="party"], article, .card',
                "title": 'h1, h2, h3, h4, .title, [class*="title"]',
                "date": '.date, [class*="date"], time, [class*="time"]',
                "venue": '.venue, .location, [class*="venue"], [class*="location"]',
                "description": '.description, .excerpt, [class*="description"], p',
                "link": "a",
            },
        },
    }

    return enhanced_dynamic_selectors


def main():
    """Main function to show the updated selectors"""
    print("🔧 VENUE SCRAPER SELECTOR UPDATES")
    print("=" * 50)

    print("\n📊 Enhanced Static Venue Selectors:")
    static_selectors = update_local_venue_selectors()
    print(f"  Updated {len(static_selectors)} static venue configurations")

    print("\n📊 Enhanced Dynamic Venue Selectors:")
    dynamic_selectors = update_dynamic_venue_selectors()
    print(f"  Updated {len(dynamic_selectors)} dynamic venue configurations")

    print(f"\n✅ Selector updates prepared!")
    print(f"\n💡 Key improvements:")
    print(f'  - Using [class*="event"] for broader matching')
    print(f"  - Added fallback selectors (article, .card, .post)")
    print(f'  - Enhanced title selectors with [class*="title"]')
    print(f'  - Improved date selectors with time and [class*="time"]')
    print(f"  - Added venue/location fallback selectors")

    print(f"\n🚀 Next steps:")
    print(f"  1. Apply these selectors to the existing scraper files")
    print(f"  2. Test the updated scrapers")
    print(f"  3. Store events in database")
    print(f"  4. Generate updated venue map")

    return static_selectors, dynamic_selectors


if __name__ == "__main__":
    main()
