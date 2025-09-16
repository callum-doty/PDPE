# etl/ingest_local_venues.py
import os
import logging
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from etl.utils import get_db_conn
from etl.data_quality import process_events_with_quality_checks, log_quality_metrics
from etl.venue_processing import (
    process_venues_with_quality_checks,
    log_venue_quality_metrics,
)
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
    # Major Venues
    "tmobile_center": {
        "name": "T-Mobile Center",
        "base_url": "https://www.t-mobilecenter.com",
        "events_url": "https://www.t-mobilecenter.com/events",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event-listing, .event",
            "title": ".event-title, .title, h3, h2",
            "date": ".event-date, .date, .event-time",
            "venue": ".venue, .location",
            "description": ".event-description, .description, .excerpt",
            "link": "a",
        },
    },
    "uptown_theater": {
        "name": "Uptown Theater",
        "base_url": "https://www.uptowntheater.com",
        "events_url": "https://www.uptowntheater.com/events",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .show",
            "title": ".event-title, .show-title, h3, h2",
            "date": ".event-date, .show-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    "kauffman_center": {
        "name": "Kauffman Center for the Performing Arts",
        "base_url": "https://www.kauffmancenter.org",
        "events_url": "https://www.kauffmancenter.org/events/",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .performance",
            "title": ".event-title, .performance-title, h3, h2",
            "date": ".event-date, .performance-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    "starlight_theatre": {
        "name": "Starlight Theatre",
        "base_url": "https://www.kcstarlight.com",
        "events_url": "https://www.kcstarlight.com/events/",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .show",
            "title": ".event-title, .show-title, h3, h2",
            "date": ".event-date, .show-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    "midland_theatre": {
        "name": "The Midland Theatre",
        "base_url": "https://www.midlandkc.com",
        "events_url": "https://www.midlandkc.com/events",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .show",
            "title": ".event-title, .show-title, h3, h2",
            "date": ".event-date, .show-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    "knuckleheads": {
        "name": "Knuckleheads Saloon",
        "base_url": "https://knuckleheadskc.com",
        "events_url": "https://knuckleheadskc.com/",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .show, .concert",
            "title": ".event-title, .show-title, h3, h2",
            "date": ".event-date, .show-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    "azura_amphitheater": {
        "name": "Azura Amphitheater",
        "base_url": "https://www.azuraamphitheater.com",
        "events_url": "https://www.azuraamphitheater.com/events",
        "category": "major_venue",
        "selectors": {
            "event_container": ".event-item, .event, .concert",
            "title": ".event-title, .concert-title, h3, h2",
            "date": ".event-date, .concert-date, .date",
            "venue": ".venue, .location",
            "description": ".event-description, .description",
            "link": "a",
        },
    },
    # Entertainment Districts
    "powerandlight": {
        "name": "Power & Light District",
        "base_url": "https://powerandlightdistrict.com",
        "events_url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
        "category": "entertainment_district",
        "selectors": {
            "event_container": ".event, .event-listing",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "westport": {
        "name": "Westport KC",
        "base_url": "https://westportkcmo.com",
        "events_url": "https://westportkcmo.com/events/",
        "category": "entertainment_district",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "jazz_district": {
        "name": "18th & Vine Jazz District",
        "base_url": "https://kcjazzdistrict.org",
        "events_url": "https://kcjazzdistrict.org/events/",
        "category": "entertainment_district",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "crossroads": {
        "name": "Crossroads KC",
        "base_url": "https://www.crossroadskc.com",
        "events_url": "https://www.crossroadskc.com/shows",
        "category": "entertainment_district",
        "selectors": {
            "event_container": ".show, .event",
            "title": ".show-title, .event-title, h3, h2",
            "date": ".show-date, .event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    # Shopping & Cultural
    "plaza": {
        "name": "Country Club Plaza",
        "base_url": "https://countryclubplaza.com",
        "events_url": "https://countryclubplaza.com/events/",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": ".event, .event-item",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "crown_center": {
        "name": "Crown Center",
        "base_url": "https://www.crowncenter.com",
        "events_url": "https://www.crowncenter.com/events",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "union_station": {
        "name": "Union Station Kansas City",
        "base_url": "https://unionstation.org",
        "events_url": "https://unionstation.org/events/",
        "category": "shopping_cultural",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    # Museums
    "nelson_atkins": {
        "name": "Nelson-Atkins Museum of Art",
        "base_url": "https://www.nelson-atkins.org",
        "events_url": "https://www.nelson-atkins.org/events/",
        "category": "museum",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "wwi_museum": {
        "name": "National WWI Museum",
        "base_url": "https://theworldwar.org",
        "events_url": "https://theworldwar.org/visit/upcoming-events",
        "category": "museum",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "science_city": {
        "name": "Science City",
        "base_url": "https://sciencecity.unionstation.org",
        "events_url": "https://sciencecity.unionstation.org/",
        "category": "museum",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    # Theaters
    "kc_rep": {
        "name": "KC Repertory Theatre",
        "base_url": "https://kcrep.org",
        "events_url": "https://kcrep.org/season/",
        "category": "theater",
        "selectors": {
            "event_container": ".show, .production, .event",
            "title": ".show-title, .production-title, h3, h2",
            "date": ".show-date, .production-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "unicorn_theatre": {
        "name": "Unicorn Theatre",
        "base_url": "https://unicorntheatre.org",
        "events_url": "https://unicorntheatre.org/",
        "category": "theater",
        "selectors": {
            "event_container": ".show, .production, .event",
            "title": ".show-title, .production-title, h3, h2",
            "date": ".show-date, .production-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    # Festival & City
    "kc_parks": {
        "name": "Kansas City Parks & Rec",
        "base_url": "https://kcparks.org",
        "events_url": "https://kcparks.org/events/",
        "category": "festival_city",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "city_market": {
        "name": "City Market KC",
        "base_url": "https://citymarketkc.org",
        "events_url": "https://citymarketkc.org/events/",
        "category": "festival_city",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "boulevardia": {
        "name": "Boulevardia Festival",
        "base_url": "https://www.boulevardia.com",
        "events_url": "https://www.boulevardia.com/",
        "category": "festival_city",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
    },
    "irish_fest": {
        "name": "Irish Fest KC",
        "base_url": "https://kcirishfest.com",
        "events_url": "https://kcirishfest.com/",
        "category": "festival_city",
        "selectors": {
            "event_container": ".event-item, .event",
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


def find_or_create_venue(venue_name, provider, category="local_venue"):
    """
    Find existing venue or create a new one

    Args:
        venue_name (str): Name of the venue
        provider (str): Provider/source of the venue data
        category (str): Venue category

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
                category,
            ),
        )

        venue_id = cur.fetchone()[0]
        conn.commit()

        logging.info(
            f"Created new venue: {venue_name} ({venue_id}) - Category: {category}"
        )
        return venue_id

    except Exception as e:
        logging.error(f"Error finding/creating venue {venue_name}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
        conn.close()


def create_venue_from_config(venue_config):
    """
    Create venue data structure from venue configuration for processing.

    Args:
        venue_config (dict): Venue scraping configuration

    Returns:
        dict: Venue data dictionary for processing
    """
    try:
        # Extract basic venue information from config
        venue_name = venue_config["name"]
        base_url = venue_config["base_url"]
        category = venue_config.get("category", "local_venue")

        # Create external_id
        provider = venue_name.lower().replace(" ", "_")
        external_id = f"{provider}_venue"

        # Try to extract additional venue information from the main page
        venue_description = ""
        venue_address = ""
        venue_phone = ""

        # Attempt to scrape venue details from main page
        try:
            response = safe_scrape_request(base_url, timeout=5)
            if response:
                soup = BeautifulSoup(response.content, "html.parser")

                # Try to extract description from common selectors
                desc_selectors = [
                    ".about",
                    ".description",
                    ".venue-info",
                    ".intro",
                    "meta[name='description']",
                    ".hero-text",
                    ".summary",
                ]
                for selector in desc_selectors:
                    if selector.startswith("meta"):
                        element = soup.select_one(selector)
                        if element:
                            venue_description = element.get("content", "")[:500]
                            break
                    else:
                        element = soup.select_one(selector)
                        if element:
                            venue_description = element.get_text(strip=True)[:500]
                            break

                # Try to extract address
                address_selectors = [
                    ".address",
                    ".location",
                    ".contact-address",
                    "[itemtype*='PostalAddress']",
                    ".venue-address",
                ]
                for selector in address_selectors:
                    element = soup.select_one(selector)
                    if element:
                        venue_address = element.get_text(strip=True)[:200]
                        break

                # Try to extract phone
                phone_selectors = [
                    ".phone",
                    ".contact-phone",
                    "[href^='tel:']",
                    ".telephone",
                ]
                for selector in phone_selectors:
                    element = soup.select_one(selector)
                    if element:
                        if selector.startswith("[href"):
                            venue_phone = element.get("href", "").replace("tel:", "")
                        else:
                            venue_phone = element.get_text(strip=True)
                        break

        except Exception as e:
            logging.debug(
                f"Could not scrape additional venue details for {venue_name}: {e}"
            )

        # Create venue data structure
        venue_data = {
            "external_id": external_id,
            "provider": provider,
            "name": venue_name,
            "description": venue_description,
            "category": category,
            "subcategory": category,  # Use category as subcategory for now
            "website": base_url,
            "address": venue_address,
            "phone": venue_phone,
            # Note: lat/lng will need to be geocoded later or obtained from another source
            "lat": None,
            "lng": None,
            "scraped_at": datetime.now(),
        }

        return venue_data

    except Exception as e:
        logging.error(f"Error creating venue data from config: {e}")
        return None


def upsert_venues_to_db(venues):
    """
    Insert or update venues in the database using processed venue data.

    Args:
        venues (list): List of processed venue dictionaries
    """
    if not venues:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for venue in venues:
            # Check if venue already exists
            cur.execute(
                """
                SELECT venue_id FROM venues 
                WHERE external_id = %s AND provider = %s
            """,
                (venue["external_id"], venue["provider"]),
            )

            existing_venue = cur.fetchone()

            if existing_venue:
                # Update existing venue with processed data
                cur.execute(
                    """
                    UPDATE venues SET
                        name = %s,
                        description = %s,
                        category = %s,
                        subcategory = %s,
                        lat = %s,
                        lng = %s,
                        address = %s,
                        phone = %s,
                        website = %s,
                        psychographic_relevance = %s,
                        total_score = %s,
                        quality_score = %s,
                        popularity_score = %s,
                        final_score = %s,
                        venue_type = %s,
                        size_estimate = %s,
                        geocoding_quality = %s,
                        content_hash = %s,
                        updated_at = NOW()
                    WHERE venue_id = %s
                """,
                    (
                        venue["name"],
                        venue.get("description"),
                        venue["category"],
                        venue.get("subcategory"),
                        venue.get("lat"),
                        venue.get("lng"),
                        venue.get("address"),
                        venue.get("phone"),
                        venue.get("website"),
                        venue.get("psychographic_relevance"),
                        venue.get("total_score"),
                        venue.get("quality_score"),
                        venue.get("popularity_score"),
                        venue.get("final_score"),
                        venue.get("venue_type"),
                        venue.get("size_estimate"),
                        venue.get("geocoding_quality"),
                        venue.get("content_hash"),
                        existing_venue[0],
                    ),
                )
            else:
                # Insert new venue with processed data
                cur.execute(
                    """
                    INSERT INTO venues (
                        external_id, provider, name, description, category, subcategory,
                        lat, lng, address, phone, website, psychographic_relevance,
                        total_score, quality_score, popularity_score, final_score,
                        venue_type, size_estimate, geocoding_quality, content_hash
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """,
                    (
                        venue["external_id"],
                        venue["provider"],
                        venue["name"],
                        venue.get("description"),
                        venue["category"],
                        venue.get("subcategory"),
                        venue.get("lat"),
                        venue.get("lng"),
                        venue.get("address"),
                        venue.get("phone"),
                        venue.get("website"),
                        venue.get("psychographic_relevance"),
                        venue.get("total_score"),
                        venue.get("quality_score"),
                        venue.get("popularity_score"),
                        venue.get("final_score"),
                        venue.get("venue_type"),
                        venue.get("size_estimate"),
                        venue.get("geocoding_quality"),
                        venue.get("content_hash"),
                    ),
                )

        conn.commit()
        logging.info(f"Processed {len(venues)} venues in database")

    except Exception as e:
        logging.error(f"Error upserting venues to database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def upsert_events_to_db(events, venue_category="local_venue"):
    """
    Insert or update events in the database

    Args:
        events (list): List of event dictionaries
        venue_category (str): Category for venue creation
    """
    if not events:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for event in events:
            # Find or create venue with category
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
    Scrape events from all configured local venues with data quality processing
    """
    logging.info("Starting local venue scraping with data quality checks")

    # Group events by venue category for proper database insertion
    events_by_category = {}
    venue_quality_reports = {}

    # Collect venues for processing
    venues_to_process = []

    for venue_key, venue_config in VENUE_SCRAPERS.items():
        venue_name = venue_config["name"]
        try:
            # Scrape raw events
            raw_events = scrape_venue_events(venue_config)

            if raw_events:
                # Process events through data quality pipeline
                processed_events, quality_report = process_events_with_quality_checks(
                    raw_events
                )

                # Log quality metrics for this venue
                log_quality_metrics(quality_report, venue_name)
                venue_quality_reports[venue_name] = quality_report

                if processed_events:
                    category = venue_config.get("category", "local_venue")
                    if category not in events_by_category:
                        events_by_category[category] = []
                    events_by_category[category].extend(processed_events)

                    logging.info(
                        f"{venue_name}: {quality_report['total_input']} raw -> {quality_report['total_output']} processed events"
                    )

            # Create venue data for processing
            venue_data = create_venue_from_config(venue_config)
            if venue_data:
                venues_to_process.append(venue_data)

            # Add delay between venues to be respectful
            time.sleep(2)

        except Exception as e:
            logging.error(f"Failed to scrape {venue_name}: {e}")

    # Process venues through unified venue processing pipeline
    if venues_to_process:
        logging.info(
            f"Processing {len(venues_to_process)} venues through quality pipeline"
        )
        processed_venues, venue_quality_report = process_venues_with_quality_checks(
            venues_to_process
        )

        if processed_venues:
            upsert_venues_to_db(processed_venues)
            log_venue_quality_metrics(venue_quality_report, "local_venues")
            logging.info(f"Processed and stored {len(processed_venues)} venues")

    # Store events in database by category
    total_events = 0
    for category, events in events_by_category.items():
        if events:
            upsert_events_to_db(events, category)
            total_events += len(events)
            logging.info(
                f"Processed {len(events)} quality-checked events for category: {category}"
            )

    # Log overall quality summary
    total_raw = sum(
        report.get("total_input", 0) for report in venue_quality_reports.values()
    )
    total_processed = sum(
        report.get("total_output", 0) for report in venue_quality_reports.values()
    )

    if total_events > 0:
        logging.info(
            f"Quality processing summary: {total_raw} raw events -> {total_processed} final events"
        )
        logging.info(f"Total events stored in database: {total_events}")
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
