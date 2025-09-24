#!/usr/bin/env python3
"""
Comprehensive venue scraper that combines enhanced static and dynamic scraping
to collect events from all configured venues and store them in the database.
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
import psycopg2
from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Load environment variables
load_dotenv()

# Headers for web scraping
SCRAPING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Comprehensive venue configurations with enhanced selectors
COMPREHENSIVE_VENUES = {
    # Major Venues
    "T-Mobile Center": {
        "url": "https://www.t-mobilecenter.com/events",
        "base_url": "https://www.t-mobilecenter.com",
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
    "Uptown Theater": {
        "url": "https://www.uptowntheater.com/events",
        "base_url": "https://www.uptowntheater.com",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="show"], [class*="listing"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"], [class*="name"]',
            "date": '.date, [class*="date"], time, [class*="time"], [class*="when"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Starlight Theatre": {
        "url": "https://www.kcstarlight.com/events/",
        "base_url": "https://www.kcstarlight.com",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="show"], [class*="performance"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "The Midland Theatre": {
        "url": "https://www.midlandkc.com/events",
        "base_url": "https://www.midlandkc.com",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="show"], [class*="listing"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Knuckleheads Saloon": {
        "url": "https://knuckleheadskc.com/",
        "base_url": "https://knuckleheadskc.com",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="show"], [class*="concert"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Azura Amphitheater": {
        "url": "https://www.azuraamphitheater.com/events",
        "base_url": "https://www.azuraamphitheater.com",
        "category": "major_venue",
        "selectors": {
            "event_container": '[class*="event"], [class*="concert"], [class*="show"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    # Entertainment Districts
    "Power & Light District": {
        "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
        "base_url": "https://powerandlightdistrict.com",
        "category": "entertainment_district",
        "selectors": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Westport KC": {
        "url": "https://westportkcmo.com/events/",
        "base_url": "https://westportkcmo.com",
        "category": "entertainment_district",
        "selectors": {
            "event_container": '[class*="event"], [class*="listing"], article, .post',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    # Shopping & Cultural
    "Union Station Kansas City": {
        "url": "https://unionstation.org/events/",
        "base_url": "https://unionstation.org",
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
    "Country Club Plaza": {
        "url": "https://countryclubplaza.com/events/",
        "base_url": "https://countryclubplaza.com",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Crown Center": {
        "url": "https://www.crowncenter.com/events",
        "base_url": "https://www.crowncenter.com",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": '[class*="event"], [class*="listing"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    # Museums
    "Nelson-Atkins Museum of Art": {
        "url": "https://www.nelson-atkins.org/events/",
        "base_url": "https://www.nelson-atkins.org",
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
    "National WWI Museum": {
        "url": "https://theworldwar.org/visit/upcoming-events",
        "base_url": "https://theworldwar.org",
        "category": "museum",
        "selectors": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Science City": {
        "url": "https://sciencecity.unionstation.org/",
        "base_url": "https://sciencecity.unionstation.org",
        "category": "museum",
        "selectors": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    # Theaters
    "KC Repertory Theatre": {
        "url": "https://kcrep.org/season/",
        "base_url": "https://kcrep.org",
        "category": "theater",
        "selectors": {
            "event_container": '[class*="show"], [class*="production"], [class*="event"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    "Unicorn Theatre": {
        "url": "https://unicorntheatre.org/",
        "base_url": "https://unicorntheatre.org",
        "category": "theater",
        "selectors": {
            "event_container": '[class*="show"], [class*="production"], [class*="event"], article',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
    # Festival & City
    "Kansas City Parks & Rec": {
        "url": "https://kcparks.org/events/",
        "base_url": "https://kcparks.org",
        "category": "festival_city",
        "selectors": {
            "event_container": '[class*="event"], [class*="program"], article, .card',
            "title": 'h1, h2, h3, h4, .title, [class*="title"]',
            "date": '.date, [class*="date"], time, [class*="time"]',
            "venue": '.venue, .location, [class*="venue"], [class*="location"]',
            "description": '.description, .excerpt, [class*="description"], p',
            "link": "a",
        },
    },
}


def get_db_conn():
    """Get database connection."""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not found in environment variables")
        return None
    return psycopg2.connect(db_url)


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
                if text and len(text) > 2:  # Only return meaningful text
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

    # Skip obviously invalid dates
    if len(date_text) < 4 or "format" in date_text.lower():
        return None

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


def scrape_venue_events(venue_name, venue_config):
    """Scrape events from a specific venue"""
    print(f"🎪 Scraping events from {venue_name}: {venue_config['url']}")

    response = safe_scrape_request(venue_config["url"])
    if not response:
        return []

    try:
        soup = BeautifulSoup(response.content, "html.parser")

        # Try to find event containers using enhanced selectors
        event_containers = soup.select(venue_config["selectors"]["event_container"])
        print(f"  📦 Found {len(event_containers)} potential event containers")

        events = []

        for i, container in enumerate(event_containers):
            try:
                # Extract event data using enhanced selectors
                title = extract_text_safely(
                    container, venue_config["selectors"]["title"]
                )
                date_text = extract_text_safely(
                    container, venue_config["selectors"]["date"]
                )
                venue_text = extract_text_safely(
                    container, venue_config["selectors"]["venue"]
                )
                description = extract_text_safely(
                    container, venue_config["selectors"]["description"]
                )
                link = extract_link_safely(
                    container,
                    venue_config["selectors"]["link"],
                    venue_config["base_url"],
                )

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
                    "upcoming events",
                    "event list",
                    "see more",
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
                    "description": description[:500] if description else "",
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

            except Exception as e:
                print(f"  ❌ Error parsing event container {i+1}: {e}")
                continue

        print(f"  🎉 Successfully scraped {len(events)} events from {venue_name}")
        return events

    except Exception as e:
        print(f"❌ Error parsing HTML from {venue_name}: {e}")
        return []


def find_or_create_venue(venue_name, provider, category="local_venue"):
    """Find existing venue or create a new one"""
    conn = get_db_conn()
    if not conn:
        return None

    cur = conn.cursor()

    try:
        # Try to find existing venue
        cur.execute(
            """
            SELECT venue_id FROM venues 
            WHERE LOWER(name) = LOWER(%s) OR LOWER(name) LIKE LOWER(%s)
            LIMIT 1
        """,
            (venue_name, f"%{venue_name}%"),
        )

        result = cur.fetchone()
        if result:
            return result[0]

        # Create new venue
        cur.execute(
            """
            INSERT INTO venues (external_id, provider, name, category)
            VALUES (%s, %s, %s, %s)
            RETURNING venue_id
        """,
            (
                f"{provider}_{venue_name.lower().replace(' ', '_')}",
                provider,
                venue_name,
                category,
            ),
        )

        venue_id = cur.fetchone()[0]
        conn.commit()

        print(f"  ✅ Created new venue: {venue_name} ({venue_id})")
        return venue_id

    except Exception as e:
        print(f"  ❌ Error finding/creating venue {venue_name}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def store_events_in_database(events, venue_category="local_venue"):
    """Store events in the database"""
    if not events:
        return 0

    conn = get_db_conn()
    if not conn:
        return 0

    cur = conn.cursor()
    stored_count = 0

    try:
        for event in events:
            # Find or create venue
            venue_id = find_or_create_venue(
                event["venue_name"], event["provider"], venue_category
            )
            if not venue_id:
                continue

            # Check if event already exists
            cur.execute(
                """
                SELECT event_id FROM events 
                WHERE external_id = %s AND provider = %s
            """,
                (event["external_id"], event["provider"]),
            )

            existing_event = cur.fetchone()

            if existing_event:
                # Update existing event
                cur.execute(
                    """
                    UPDATE events SET
                        name = %s,
                        description = %s,
                        category = %s,
                        subcategory = %s,
                        start_time = %s,
                        end_time = %s,
                        venue_id = %s,
                        psychographic_relevance = %s,
                        updated_at = NOW()
                    WHERE event_id = %s
                """,
                    (
                        event["name"],
                        event["description"],
                        event["category"],
                        event["subcategory"],
                        event["start_time"],
                        event["end_time"],
                        venue_id,
                        json.dumps(event["psychographic_scores"]),
                        existing_event[0],
                    ),
                )
            else:
                # Insert new event
                cur.execute(
                    """
                    INSERT INTO events (
                        external_id, provider, name, description, category, subcategory,
                        start_time, end_time, venue_id, psychographic_relevance
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """,
                    (
                        event["external_id"],
                        event["provider"],
                        event["name"],
                        event["description"],
                        event["category"],
                        event["subcategory"],
                        event["start_time"],
                        event["end_time"],
                        venue_id,
                        json.dumps(event["psychographic_scores"]),
                    ),
                )
                stored_count += 1

        conn.commit()
        print(f"  💾 Stored {stored_count} new events in database")
        return stored_count

    except Exception as e:
        print(f"❌ Error storing events in database: {e}")
        conn.rollback()
        return 0
    finally:
        cur.close()
        conn.close()


def scrape_all_venues():
    """Scrape events from all configured venues"""
    print("🚀 COMPREHENSIVE VENUE EVENT SCRAPING")
    print("=" * 60)

    all_events = []
    total_stored = 0
    venue_results = {}

    for venue_name, venue_config in COMPREHENSIVE_VENUES.items():
        try:
            print(f"\n📍 Processing: {venue_name}")
            events = scrape_venue_events(venue_name, venue_config)

            if events:
                # Store events in database
                stored_count = store_events_in_database(
                    events, venue_config["category"]
                )
                total_stored += stored_count
                all_events.extend(events)

                venue_results[venue_name] = {
                    "events_found": len(events),
                    "events_stored": stored_count,
                    "category": venue_config["category"],
                    "status": "success",
                }

                # Show sample events
                print(
                    f"  📊 Found {len(events)} events, stored {stored_count} new events"
                )
                for i, event in enumerate(events[:2]):
                    print(f"    [{i+1}] {event['name'][:60]}...")
                    if event["start_time"]:
                        print(
                            f"        Date: {event['start_time'].strftime('%Y-%m-%d')}"
                        )
                    print(f"        Category: {event['subcategory']}")
            else:
                venue_results[venue_name] = {
                    "events_found": 0,
                    "events_stored": 0,
                    "category": venue_config["category"],
                    "status": "no_events",
                }
                print(f"  ⚠️  No events found")

            # Be respectful to servers
            time.sleep(3)

        except Exception as e:
            print(f"❌ Failed to scrape {venue_name}: {e}")
            venue_results[venue_name] = {
                "events_found": 0,
                "events_stored": 0,
                "category": venue_config.get("category", "unknown"),
                "status": "error",
                "error": str(e),
            }

    print(f"\n🎉 COMPREHENSIVE SCRAPING COMPLETE!")
    print(f"📊 Summary:")
    print(f"  Total venues processed: {len(COMPREHENSIVE_VENUES)}")
    print(f"  Total events found: {len(all_events)}")
    print(f"  Total events stored: {total_stored}")

    # Category breakdown
    print(f"\n📈 Events by Category:")
    categories = {}
    for event in all_events:
        cat = event["subcategory"]
        categories[cat] = categories.get(cat, 0) + 1

    for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        print(f"  {cat}: {count} events")

    # Venue success rate
    successful_venues = sum(
        1 for r in venue_results.values() if r["status"] == "success"
    )
    print(
        f"\n📍 Venue Success Rate: {successful_venues}/{len(COMPREHENSIVE_VENUES)} ({successful_venues/len(COMPREHENSIVE_VENUES)*100:.1f}%)"
    )

    return all_events, venue_results


def main():
    """Main function to run comprehensive venue scraping"""
    events, results = scrape_all_venues()

    # Save results to file for analysis
    if events:
        output_file = "comprehensive_venue_events.json"
        with open(output_file, "w") as f:
            json.dump(events, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {output_file}")

    return events, results


if __name__ == "__main__":
    main()
