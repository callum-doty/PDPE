#!/usr/bin/env python3
"""
Enhanced venue scraper with improved CSS selectors based on diagnostic findings.
This script updates the venue scraping logic to work with current website structures.
"""

import sys
import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta
import re
import time
import json
from urllib.parse import urljoin

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Headers for web scraping
SCRAPING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Enhanced venue configurations with improved selectors
ENHANCED_VENUE_SCRAPERS = {
    "tmobile_center": {
        "name": "T-Mobile Center",
        "base_url": "https://www.t-mobilecenter.com",
        "events_url": "https://www.t-mobilecenter.com/events",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"], [class*="name"]',
            "date": '.date, [class*="date"], time, [class*="time"], [class*="when"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], [class*="summary"], p',
            "link": "a",
        },
    },
    "union_station": {
        "name": "Union Station Kansas City",
        "base_url": "https://unionstation.org",
        "events_url": "https://unionstation.org/events/",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": '[class*="event"], article, .post, [class*="card"]',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "nelson_atkins": {
        "name": "Nelson-Atkins Museum of Art",
        "base_url": "https://www.nelson-atkins.org",
        "events_url": "https://www.nelson-atkins.org/events/",
        "category": "museum",
        "selectors": {
            "event_container": '[class*="event"], [class*="show"], [class*="card"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "kauffman_center": {
        "name": "Kauffman Center for the Performing Arts",
        "base_url": "https://www.kauffmancenter.org",
        "events_url": "https://www.kauffmancenter.org/events/",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="performance"], [class*="show"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
}


def safe_scrape_request(url, timeout=10):
    """Make a safe request for web scraping with proper headers and error handling"""
    try:
        response = requests.get(url, headers=SCRAPING_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to scrape {url}: {e}")
        return None


def extract_text_safely(element, selector):
    """Safely extract text from a BeautifulSoup element using a selector"""
    try:
        # Try each selector in the comma-separated list
        selector_parts = [s.strip() for s in selector.split(",")]
        for part in selector_parts:
            found = element.select_one(part)
            if found:
                text = found.get_text(strip=True)
                if text:  # Only return non-empty text
                    return text
        return ""
    except Exception:
        return ""


def extract_link_safely(element, selector, base_url):
    """Safely extract and resolve a link from an element"""
    try:
        selector_parts = [s.strip() for s in selector.split(",")]
        for part in selector_parts:
            found = element.select_one(part)
            if found and found.get("href"):
                href = found.get("href")
                if href.startswith("http"):
                    return href
                else:
                    return urljoin(base_url, href)
        return ""
    except Exception:
        return ""


def parse_event_date(date_text):
    """Parse event date from various text formats"""
    if not date_text:
        return None

    # Clean up the date text
    date_text = re.sub(r"\s+", " ", date_text.strip())

    # Common date patterns
    date_patterns = [
        r"(\d{1,2})/(\d{1,2})/(\d{4})",  # MM/DD/YYYY
        r"(\d{4})-(\d{1,2})-(\d{1,2})",  # YYYY-MM-DD
        r"(\w+)\s+(\d{1,2}),?\s+(\d{4})",  # Month DD, YYYY
        r"(\d{1,2})\s+(\w+)\s+(\d{4})",  # DD Month YYYY
    ]

    for pattern in date_patterns:
        match = re.search(pattern, date_text)
        if match:
            try:
                from dateutil import parser

                return parser.parse(date_text, fuzzy=True)
            except Exception:
                continue

    # If no pattern matches, try fuzzy parsing
    try:
        from dateutil import parser

        return parser.parse(date_text, fuzzy=True)
    except Exception:
        print(f"⚠️  Could not parse date: {date_text}")
        return None


def classify_event_psychographics(title, description):
    """Classify event based on psychographic keywords"""
    text = f"{title} {description}".lower()

    psychographic_keywords = {
        "career_driven": [
            "networking",
            "professional",
            "business",
            "career",
            "corporate",
            "conference",
            "seminar",
            "workshop",
            "training",
        ],
        "competent": [
            "expert",
            "masterclass",
            "training",
            "certification",
            "skill",
            "advanced",
            "professional",
            "education",
            "learning",
        ],
        "fun": [
            "party",
            "celebration",
            "festival",
            "concert",
            "nightlife",
            "drinks",
            "social",
            "entertainment",
            "music",
            "dance",
            "comedy",
        ],
    }

    scores = {}
    for category, keywords in psychographic_keywords.items():
        score = sum(1 for keyword in keywords if keyword in text)
        scores[category] = min(score / len(keywords), 1.0)  # Normalize to 0-1

    return scores


def determine_event_subcategory(title, description):
    """Determine event subcategory based on title and description"""
    text = f"{title} {description}".lower()

    subcategories = {
        "music": [
            "concert",
            "music",
            "band",
            "singer",
            "performance",
            "acoustic",
            "jazz",
            "rock",
            "pop",
        ],
        "art": [
            "art",
            "gallery",
            "exhibition",
            "artist",
            "painting",
            "sculpture",
            "creative",
            "design",
        ],
        "theater": [
            "theater",
            "theatre",
            "play",
            "drama",
            "musical",
            "performance",
            "show",
        ],
        "family": ["family", "kids", "children", "child", "parent", "family-friendly"],
        "cultural": [
            "cultural",
            "culture",
            "heritage",
            "history",
            "museum",
            "educational",
        ],
        "business": [
            "business",
            "networking",
            "professional",
            "conference",
            "seminar",
            "workshop",
        ],
        "sports": ["sports", "game", "match", "tournament", "athletic", "fitness"],
        "food": [
            "food",
            "restaurant",
            "dining",
            "taste",
            "culinary",
            "chef",
            "wine",
            "beer",
        ],
        "nightlife": ["nightlife", "bar", "club", "party", "drinks", "dancing", "dj"],
        "outdoor": [
            "outdoor",
            "park",
            "nature",
            "hiking",
            "festival",
            "market",
            "fair",
        ],
    }

    for subcategory, keywords in subcategories.items():
        if any(keyword in text for keyword in keywords):
            return subcategory

    return "general"


def scrape_venue_events_enhanced(venue_config):
    """Enhanced venue event scraping with improved selectors"""
    venue_name = venue_config["name"]
    events_url = venue_config["events_url"]
    base_url = venue_config["base_url"]
    selectors = venue_config["selectors"]

    print(f"🎪 Scraping events from {venue_name}: {events_url}")

    response = safe_scrape_request(events_url)
    if not response:
        return []

    try:
        soup = BeautifulSoup(response.content, "html.parser")

        # Try to find event containers using enhanced selectors
        event_containers = soup.select(selectors["event_container"])
        print(f"  📦 Found {len(event_containers)} potential event containers")

        events = []

        for i, container in enumerate(event_containers):
            try:
                # Extract event data using enhanced selectors
                title = extract_text_safely(container, selectors["title"])
                date_text = extract_text_safely(container, selectors["date"])
                venue_text = extract_text_safely(container, selectors["venue"])
                description = extract_text_safely(container, selectors["description"])
                link = extract_link_safely(container, selectors["link"], base_url)

                # Skip if no meaningful title found
                if not title or len(title.strip()) < 3:
                    continue

                # Skip if title looks like navigation or generic content
                skip_patterns = [
                    "menu",
                    "navigation",
                    "search",
                    "login",
                    "subscribe",
                    "newsletter",
                    "contact",
                    "about",
                    "home",
                    "explore",
                ]
                if any(pattern in title.lower() for pattern in skip_patterns):
                    continue

                # Parse date
                parsed_date = parse_event_date(date_text)

                # Skip past events (older than 7 days to be safe)
                if parsed_date and parsed_date < datetime.now() - timedelta(days=7):
                    continue

                # Classify psychographics
                psychographic_scores = classify_event_psychographics(title, description)

                event = {
                    "external_id": f"{venue_name.lower().replace(' ', '_')}_{hash(title + str(date_text))}",
                    "provider": venue_name.lower().replace(" ", "_"),
                    "name": title,
                    "description": (
                        description[:500] if description else ""
                    ),  # Limit description length
                    "category": "local_event",
                    "subcategory": determine_event_subcategory(title, description),
                    "start_time": parsed_date,
                    "end_time": None,
                    "venue_name": venue_text or venue_name,
                    "source_url": link,
                    "psychographic_scores": psychographic_scores,
                    "scraped_at": datetime.now(),
                }

                events.append(event)
                print(f"  ✅ Event {i+1}: {title[:50]}...")

            except Exception as e:
                print(f"  ❌ Error parsing event container {i+1}: {e}")
                continue

        print(f"  🎉 Successfully scraped {len(events)} events from {venue_name}")
        return events

    except Exception as e:
        print(f"❌ Error parsing HTML from {venue_name}: {e}")
        return []


def test_enhanced_scraping():
    """Test the enhanced scraping on a few venues"""
    print("🚀 TESTING ENHANCED VENUE SCRAPING")
    print("=" * 50)

    all_events = []

    for venue_key, venue_config in ENHANCED_VENUE_SCRAPERS.items():
        try:
            events = scrape_venue_events_enhanced(venue_config)
            all_events.extend(events)

            print(f"\n📊 Summary for {venue_config['name']}:")
            print(f"  Events found: {len(events)}")

            if events:
                # Show sample events
                for i, event in enumerate(events[:3]):
                    print(f"  [{i+1}] {event['name']}")
                    if event["start_time"]:
                        print(
                            f"      Date: {event['start_time'].strftime('%Y-%m-%d %H:%M')}"
                        )
                    print(f"      Category: {event['subcategory']}")
                    print(f"      Psychographic: {event['psychographic_scores']}")

            print(f"\n" + "─" * 50)
            time.sleep(2)  # Be respectful to servers

        except Exception as e:
            print(f"❌ Failed to scrape {venue_config['name']}: {e}")

    print(f"\n🎉 ENHANCED SCRAPING COMPLETE!")
    print(f"📊 Total events found: {len(all_events)}")

    if all_events:
        print(f"\n📈 Event Categories:")
        categories = {}
        for event in all_events:
            cat = event["subcategory"]
            categories[cat] = categories.get(cat, 0) + 1

        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cat}: {count} events")

    return all_events


def main():
    """Main function to test enhanced venue scraping"""
    events = test_enhanced_scraping()

    # Save results to file for analysis
    if events:
        output_file = "enhanced_scraping_results.json"
        with open(output_file, "w") as f:
            json.dump(events, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {output_file}")

    return events


if __name__ == "__main__":
    main()
