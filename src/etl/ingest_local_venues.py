# etl/ingest_local_venues.py
import os
import logging
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from etl.utils import get_db_conn
import time

# Headers for web scraping to appear as a regular browser
SCRAPING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Venue scraper configurations
VENUE_SCRAPERS = {
    "visitkc": {
        "name": "VisitKC",
        "base_url": "https://www.visitkc.com",
        "events_url": "https://www.visitkc.com/events",
        "selectors": {
            "event_container": ".event-item, .listing-item",
            "title": ".event-title, .listing-title, h3, h2",
            "date": ".event-date, .date, .event-time",
            "venue": ".event-venue, .venue, .location",
            "description": ".event-description, .description, .excerpt",
            "link": "a",
        },
    },
    "do816": {
        "name": "Do816",
        "base_url": "https://do816.com",
        "events_url": "https://do816.com/events",
        "selectors": {
            "event_container": ".event-listing, .event-item",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "thepitchkc": {
        "name": "The Pitch KC",
        "base_url": "https://www.thepitchkc.com",
        "events_url": "https://www.thepitchkc.com/events",
        "selectors": {
            "event_container": ".event, .listing",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "plaza": {
        "name": "Country Club Plaza",
        "base_url": "https://www.countryclubplaza.com",
        "events_url": "https://www.countryclubplaza.com/events",
        "selectors": {
            "event_container": ".event, .event-item",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "powerandlight": {
        "name": "Power & Light District",
        "base_url": "https://www.powerandlightdistrict.com",
        "events_url": "https://www.powerandlightdistrict.com/events",
        "selectors": {
            "event_container": ".event, .event-listing",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
}

# Psychographic keywords for event classification
PSYCHOGRAPHIC_KEYWORDS = {
    "career_driven": [
        "networking",
        "professional",
        "business",
        "career",
        "corporate",
        "conference",
        "seminar",
        "workshop",
    ],
    "competent": [
        "expert",
        "masterclass",
        "training",
        "certification",
        "skill",
        "advanced",
        "professional",
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
    ],
}


def safe_scrape_request(url, timeout=10):
    """
    Make a safe request for web scraping with proper headers and error handling

    Args:
        url (str): URL to scrape
        timeout (int): Request timeout in seconds

    Returns:
        requests.Response or None: Response object or None if failed
    """
    try:
        response = requests.get(url, headers=SCRAPING_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to scrape {url}: {e}")
        return None


def extract_text_safely(element, selector):
    """
    Safely extract text from a BeautifulSoup element using a selector

    Args:
        element: BeautifulSoup element
        selector (str): CSS selector

    Returns:
        str: Extracted text or empty string
    """
    try:
        found = element.select_one(selector)
        return found.get_text(strip=True) if found else ""
    except Exception:
        return ""


def extract_link_safely(element, selector, base_url):
    """
    Safely extract and resolve a link from an element

    Args:
        element: BeautifulSoup element
        selector (str): CSS selector
        base_url (str): Base URL for resolving relative links

    Returns:
        str: Full URL or empty string
    """
    try:
        found = element.select_one(selector)
        if found and found.get("href"):
            return urljoin(base_url, found.get("href"))
        return ""
    except Exception:
        return ""


def parse_event_date(date_text):
    """
    Parse event date from various text formats

    Args:
        date_text (str): Date text to parse

    Returns:
        datetime or None: Parsed datetime or None if parsing failed
    """
    if not date_text:
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
                groups = match.groups()
                if len(groups) == 3:
                    # Try different date parsing approaches
                    from dateutil import parser

                    return parser.parse(date_text, fuzzy=True)
            except Exception:
                continue

    # If no pattern matches, try fuzzy parsing
    try:
        from dateutil import parser

        return parser.parse(date_text, fuzzy=True)
    except Exception:
        logging.warning(f"Could not parse date: {date_text}")
        return None


def classify_event_psychographics(title, description):
    """
    Classify event based on psychographic keywords

    Args:
        title (str): Event title
        description (str): Event description

    Returns:
        dict: Psychographic scores
    """
    text = f"{title} {description}".lower()
    scores = {}

    for category, keywords in PSYCHOGRAPHIC_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in text)
        scores[category] = score

    return scores


def scrape_venue_events(venue_config):
    """
    Scrape events from a specific venue website

    Args:
        venue_config (dict): Venue scraping configuration

    Returns:
        list: List of scraped events
    """
    venue_name = venue_config["name"]
    events_url = venue_config["events_url"]
    base_url = venue_config["base_url"]
    selectors = venue_config["selectors"]

    logging.info(f"Scraping events from {venue_name}: {events_url}")

    response = safe_scrape_request(events_url)
    if not response:
        return []

    try:
        soup = BeautifulSoup(response.content, "html.parser")
        event_containers = soup.select(selectors["event_container"])

        events = []

        for container in event_containers:
            try:
                # Extract event data
                title = extract_text_safely(container, selectors["title"])
                date_text = extract_text_safely(container, selectors["date"])
                venue_text = extract_text_safely(container, selectors["venue"])
                description = extract_text_safely(container, selectors["description"])
                link = extract_link_safely(container, selectors["link"], base_url)

                if not title:  # Skip if no title found
                    continue

                # Parse date
                parsed_date = parse_event_date(date_text)

                # Skip past events (older than 1 day)
                if parsed_date and parsed_date < datetime.now() - timedelta(days=1):
                    continue

                # Classify psychographics
                psychographic_scores = classify_event_psychographics(title, description)

                event = {
                    "external_id": f"{venue_name.lower().replace(' ', '_')}_{hash(title + date_text)}",
                    "provider": venue_name.lower().replace(" ", "_"),
                    "name": title,
                    "description": description,
                    "category": "local_event",
                    "subcategory": determine_event_subcategory(title, description),
                    "start_time": parsed_date,
                    "end_time": None,  # Usually not provided
                    "venue_name": venue_text or venue_name,
                    "source_url": link,
                    "psychographic_scores": psychographic_scores,
                    "scraped_at": datetime.now(),
                }

                events.append(event)

            except Exception as e:
                logging.error(f"Error parsing event container from {venue_name}: {e}")
                continue

        logging.info(f"Scraped {len(events)} events from {venue_name}")
        return events

    except Exception as e:
        logging.error(f"Error parsing HTML from {venue_name}: {e}")
        return []


def determine_event_subcategory(title, description):
    """
    Determine event subcategory based on title and description

    Args:
        title (str): Event title
        description (str): Event description

    Returns:
        str: Event subcategory
    """
    text = f"{title} {description}".lower()

    # Define subcategory keywords
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
        "food": [
            "food",
            "restaurant",
            "dining",
            "taste",
            "culinary",
            "chef",
            "wine",
            "beer",
            "cocktail",
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
        "business": [
            "business",
            "networking",
            "professional",
            "conference",
            "seminar",
            "workshop",
            "career",
        ],
        "sports": [
            "sports",
            "game",
            "match",
            "tournament",
            "athletic",
            "fitness",
            "run",
            "race",
        ],
        "family": ["family", "kids", "children", "child", "parent", "family-friendly"],
        "nightlife": ["nightlife", "bar", "club", "party", "drinks", "dancing", "dj"],
        "cultural": [
            "cultural",
            "culture",
            "heritage",
            "history",
            "museum",
            "theater",
            "theatre",
        ],
        "shopping": [
            "shopping",
            "retail",
            "store",
            "sale",
            "market",
            "vendor",
            "boutique",
        ],
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


def find_or_create_venue(venue_name, provider):
    """
    Find existing venue or create a new one

    Args:
        venue_name (str): Name of the venue
        provider (str): Provider/source of the venue data

    Returns:
        str: Venue UUID
    """
    conn = get_db_conn()
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
                "local_venue",
            ),
        )

        venue_id = cur.fetchone()[0]
        conn.commit()

        logging.info(f"Created new venue: {venue_name} ({venue_id})")
        return venue_id

    except Exception as e:
        logging.error(f"Error finding/creating venue {venue_name}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def upsert_events_to_db(events):
    """
    Insert or update events in the database

    Args:
        events (list): List of event dictionaries
    """
    if not events:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for event in events:
            # Find or create venue
            venue_id = find_or_create_venue(event["venue_name"], event["provider"])
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
                        event["psychographic_scores"],
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
                        event["psychographic_scores"],
                    ),
                )

        conn.commit()
        logging.info(f"Processed {len(events)} events in database")

    except Exception as e:
        logging.error(f"Error upserting events to database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def scrape_all_local_venues():
    """
    Scrape events from all configured local venues
    """
    logging.info("Starting local venue scraping")

    all_events = []

    for venue_key, venue_config in VENUE_SCRAPERS.items():
        try:
            events = scrape_venue_events(venue_config)
            all_events.extend(events)

            # Add delay between venues to be respectful
            time.sleep(2)

        except Exception as e:
            logging.error(f"Failed to scrape {venue_config['name']}: {e}")

    # Store all events in database
    if all_events:
        upsert_events_to_db(all_events)
        logging.info(f"Total events scraped: {len(all_events)}")
    else:
        logging.info("No events scraped from local venues")


def scrape_specific_venue(venue_name):
    """
    Scrape events from a specific venue

    Args:
        venue_name (str): Name of the venue to scrape
    """
    venue_config = VENUE_SCRAPERS.get(venue_name.lower())
    if not venue_config:
        logging.error(f"No configuration found for venue: {venue_name}")
        return

    events = scrape_venue_events(venue_config)
    if events:
        upsert_events_to_db(events)
        logging.info(f"Scraped {len(events)} events from {venue_name}")
    else:
        logging.info(f"No events found for {venue_name}")


def ingest_local_venue_data():
    """
    Main function to ingest local venue data
    """
    logging.info("Starting local venue data ingestion")

    try:
        scrape_all_local_venues()
        logging.info("Local venue data ingestion completed successfully")

    except Exception as e:
        logging.error(f"Local venue data ingestion failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_local_venue_data()
