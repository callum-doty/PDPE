# etl/ingest_dynamic_venues.py
"""
Enhanced dynamic venue scraping module with improved data quality, caching, and processing.
Integrates with the venue processing pipeline for consistent data handling.
"""

import os
import logging
import time
import json
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional, Tuple
from etl.utils import get_db_conn
from etl.venue_processing import VenueProcessor
from etl.ingest_local_venues import (
    classify_event_psychographics,
    determine_event_subcategory,
    parse_event_date,
    find_or_create_venue,
    upsert_events_to_db,
)

# Dynamic venue scraper configurations
DYNAMIC_VENUE_SCRAPERS = {
    # Aggregators (Dynamic JS)
    "visitkc_dynamic": {
        "name": "Visit KC",
        "base_url": "https://www.visitkc.com",
        "events_url": "https://www.visitkc.com/events",
        "category": "aggregator",
        "wait_for": ".event-item, .listing-item",
        "selectors": {
            "event_container": ".event-item, .listing-item",
            "title": ".event-title, .listing-title, h3, h2",
            "date": ".event-date, .date, .event-time",
            "venue": ".event-venue, .venue, .location",
            "description": ".event-description, .description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": True,
        "max_scrolls": 5,
    },
    "do816_dynamic": {
        "name": "Do816",
        "base_url": "https://do816.com",
        "events_url": "https://do816.com/events",
        "category": "aggregator",
        "wait_for": ".event-listing, .event-item",
        "selectors": {
            "event_container": ".event-listing, .event-item",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": True,
        "max_scrolls": 3,
    },
    "thepitchkc_dynamic": {
        "name": "The Pitch KC",
        "base_url": "https://calendar.thepitchkc.com",
        "events_url": "https://calendar.thepitchkc.com/",
        "category": "aggregator",
        "wait_for": ".event, .listing",
        "selectors": {
            "event_container": ".event, .listing",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": False,
        "max_scrolls": 0,
    },
    "kc_magazine": {
        "name": "Kansas City Magazine Events",
        "base_url": "https://events.kansascitymag.com",
        "events_url": "https://events.kansascitymag.com/",
        "category": "aggregator",
        "wait_for": ".event-item, .event",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": True,
        "max_scrolls": 3,
    },
    "eventkc": {
        "name": "Event KC",
        "base_url": "https://www.eventkc.com",
        "events_url": "https://www.eventkc.com/",
        "category": "aggregator",
        "wait_for": ".event-item, .event",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": True,
        "max_scrolls": 4,
    },
    "aura_kc": {
        "name": "Aura KC Nightclub",
        "base_url": "https://www.aurakc.com",
        "events_url": "https://www.aurakc.com/",
        "category": "nightlife",
        "wait_for": ".event-item, .event",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
            "venue": ".venue, .location",
            "description": ".description, .excerpt",
            "link": "a",
        },
        "scroll_to_load": False,
        "max_scrolls": 0,
    },
}


def create_webdriver():
    """
    Create a Chrome WebDriver instance with appropriate options for scraping

    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    # Disable images and CSS for faster loading
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome WebDriver: {e}")
        return None


def extract_text_from_element(driver, element, selector):
    """
    Safely extract text from a web element using CSS selector

    Args:
        driver: WebDriver instance
        element: WebElement to search within
        selector (str): CSS selector

    Returns:
        str: Extracted text or empty string
    """
    try:
        found_element = element.find_element(By.CSS_SELECTOR, selector)
        return found_element.text.strip()
    except NoSuchElementException:
        return ""
    except Exception:
        return ""


def extract_link_from_element(driver, element, selector, base_url):
    """
    Safely extract and resolve a link from a web element

    Args:
        driver: WebDriver instance
        element: WebElement to search within
        selector (str): CSS selector
        base_url (str): Base URL for resolving relative links

    Returns:
        str: Full URL or empty string
    """
    try:
        found_element = element.find_element(By.CSS_SELECTOR, selector)
        href = found_element.get_attribute("href")
        if href:
            if href.startswith("http"):
                return href
            else:
                return f"{base_url.rstrip('/')}/{href.lstrip('/')}"
        return ""
    except NoSuchElementException:
        return ""
    except Exception:
        return ""


def scroll_to_load_content(driver, max_scrolls=3):
    """
    Scroll down the page to trigger lazy loading of content

    Args:
        driver: WebDriver instance
        max_scrolls (int): Maximum number of scrolls to perform
    """
    for i in range(max_scrolls):
        # Scroll to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for content to load

        # Check if we've reached the bottom
        new_height = driver.execute_script("return document.body.scrollHeight")
        if i > 0:
            if new_height == previous_height:
                break
        previous_height = new_height


def scrape_dynamic_venue_events(venue_config):
    """
    Scrape events from a dynamic JavaScript venue website

    Args:
        venue_config (dict): Dynamic venue scraping configuration

    Returns:
        list: List of scraped events
    """
    venue_name = venue_config["name"]
    events_url = venue_config["events_url"]
    base_url = venue_config["base_url"]
    selectors = venue_config["selectors"]
    wait_for = venue_config["wait_for"]
    scroll_to_load = venue_config.get("scroll_to_load", False)
    max_scrolls = venue_config.get("max_scrolls", 0)

    logging.info(f"Scraping dynamic events from {venue_name}: {events_url}")

    driver = create_webdriver()
    if not driver:
        return []

    try:
        # Load the page
        driver.get(events_url)

        # Wait for the main content to load
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_for)))

        # Scroll to load more content if needed
        if scroll_to_load and max_scrolls > 0:
            scroll_to_load_content(driver, max_scrolls)

        # Additional wait after scrolling
        time.sleep(3)

        # Find all event containers
        event_elements = driver.find_elements(
            By.CSS_SELECTOR, selectors["event_container"]
        )

        events = []

        for element in event_elements:
            try:
                # Extract event data
                title = extract_text_from_element(driver, element, selectors["title"])
                date_text = extract_text_from_element(
                    driver, element, selectors["date"]
                )
                venue_text = extract_text_from_element(
                    driver, element, selectors["venue"]
                )
                description = extract_text_from_element(
                    driver, element, selectors["description"]
                )
                link = extract_link_from_element(
                    driver, element, selectors["link"], base_url
                )

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
                logging.error(f"Error parsing event element from {venue_name}: {e}")
                continue

        logging.info(f"Scraped {len(events)} dynamic events from {venue_name}")
        return events

    except TimeoutException:
        logging.error(f"Timeout waiting for content to load from {venue_name}")
        return []
    except Exception as e:
        logging.error(f"Error scraping dynamic venue {venue_name}: {e}")
        return []
    finally:
        driver.quit()


class DynamicVenueIngestionManager:
    """Enhanced manager for dynamic venue data ingestion with caching and quality control."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.venue_processor = VenueProcessor()
        self.cache_duration_hours = 6  # Cache scraped data for 6 hours

    def get_cached_venue_data(self, venue_key: str) -> Optional[Dict]:
        """Get cached venue data if available and not expired."""
        try:
            cache_file = f"/tmp/dynamic_venue_cache_{venue_key}.json"
            if os.path.exists(cache_file):
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)

                cache_time = datetime.fromisoformat(cached_data.get("cached_at", ""))
                if datetime.now() - cache_time < timedelta(
                    hours=self.cache_duration_hours
                ):
                    self.logger.info(f"Using cached data for {venue_key}")
                    return cached_data.get("events", [])
        except Exception as e:
            self.logger.warning(f"Error reading cache for {venue_key}: {e}")

        return None

    def cache_venue_data(self, venue_key: str, events: List[Dict]):
        """Cache venue data for future use."""
        try:
            cache_file = f"/tmp/dynamic_venue_cache_{venue_key}.json"
            cache_data = {
                "cached_at": datetime.now().isoformat(),
                "venue_key": venue_key,
                "events": events,
            }
            with open(cache_file, "w") as f:
                json.dump(cache_data, f, default=str)
            self.logger.debug(f"Cached {len(events)} events for {venue_key}")
        except Exception as e:
            self.logger.warning(f"Error caching data for {venue_key}: {e}")

    def scrape_venue_with_quality_control(
        self, venue_config: Dict
    ) -> Tuple[List[Dict], Dict]:
        """Scrape venue with integrated quality control and processing."""
        venue_key = venue_config.get("name", "").lower().replace(" ", "_")

        # Check cache first
        cached_events = self.get_cached_venue_data(venue_key)
        if cached_events:
            return cached_events, {
                "source": "cache",
                "events_count": len(cached_events),
            }

        # Scrape fresh data
        raw_events = scrape_dynamic_venue_events(venue_config)

        if not raw_events:
            return [], {
                "source": "scrape",
                "events_count": 0,
                "error": "No events found",
            }

        # Convert events to venue format for processing
        venues_for_processing = []
        for event in raw_events:
            venue_data = {
                "external_id": f"{venue_key}_venue_{hash(event.get('venue_name', ''))}",
                "provider": event.get("provider", venue_key),
                "name": event.get("venue_name", "Unknown Venue"),
                "category": "event_venue",
                "subcategory": event.get("subcategory", "general"),
                "lat": None,  # Will need geocoding
                "lng": None,
                "address": None,
                "description": f"Venue hosting: {event.get('name', '')}",
                "psychographic_scores": event.get("psychographic_scores", {}),
                "source_url": event.get("source_url", ""),
                "associated_events": [event],
            }
            venues_for_processing.append(venue_data)

        # Process venues through quality pipeline
        processed_venues, quality_report = self.venue_processor.process_venues_batch(
            venues_for_processing
        )

        # Cache the results
        self.cache_venue_data(venue_key, raw_events)

        return raw_events, {
            "source": "scrape",
            "events_count": len(raw_events),
            "venues_processed": len(processed_venues),
            "quality_report": quality_report,
        }


def scrape_all_dynamic_venues():
    """
    Enhanced scraping of all configured dynamic venues with quality control
    """
    logging.info("Starting enhanced dynamic venue scraping")

    manager = DynamicVenueIngestionManager()

    # Track overall metrics
    total_events = 0
    total_venues_processed = 0
    scraping_results = {}

    # Group events by venue category for proper database insertion
    events_by_category = {}

    for venue_key, venue_config in DYNAMIC_VENUE_SCRAPERS.items():
        try:
            logging.info(f"Processing venue: {venue_config['name']}")

            events, result_info = manager.scrape_venue_with_quality_control(
                venue_config
            )
            scraping_results[venue_key] = result_info

            if events:
                category = venue_config.get("category", "local_venue")
                if category not in events_by_category:
                    events_by_category[category] = []
                events_by_category[category].extend(events)

                total_events += len(events)
                total_venues_processed += result_info.get("venues_processed", 0)

                logging.info(
                    f"Successfully processed {len(events)} events from {venue_config['name']}"
                )
            else:
                logging.warning(f"No events found for {venue_config['name']}")

            # Add delay between venues to be respectful
            time.sleep(5)  # Longer delay for dynamic scraping

        except Exception as e:
            logging.error(f"Failed to scrape dynamic venue {venue_config['name']}: {e}")
            scraping_results[venue_key] = {"error": str(e), "events_count": 0}

    # Store events in database by category
    stored_events = 0
    for category, events in events_by_category.items():
        if events:
            try:
                upsert_events_to_db(events, category)
                stored_events += len(events)
                logging.info(
                    f"Stored {len(events)} dynamic events for category: {category}"
                )
            except Exception as e:
                logging.error(f"Failed to store events for category {category}: {e}")

    # Log comprehensive results
    logging.info(f"Dynamic venue scraping completed:")
    logging.info(f"  - Total events scraped: {total_events}")
    logging.info(f"  - Total events stored: {stored_events}")
    logging.info(f"  - Total venues processed: {total_venues_processed}")

    # Store scraping metrics
    _store_scraping_metrics(scraping_results, total_events, stored_events)

    return {
        "total_events_scraped": total_events,
        "total_events_stored": stored_events,
        "total_venues_processed": total_venues_processed,
        "venue_results": scraping_results,
    }


def _store_scraping_metrics(
    scraping_results: Dict, total_events: int, stored_events: int
):
    """Store scraping metrics in database for monitoring."""
    conn = get_db_conn()
    if not conn:
        return

    cur = conn.cursor()

    try:
        # Create scraping_metrics table if it doesn't exist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scraping_metrics (
                id SERIAL PRIMARY KEY,
                scrape_timestamp TIMESTAMP DEFAULT NOW(),
                venue_provider TEXT,
                events_found INT,
                events_stored INT,
                venues_processed INT,
                success BOOLEAN,
                error_message TEXT,
                metadata JSONB
            )
        """
        )

        # Insert overall metrics
        cur.execute(
            """
            INSERT INTO scraping_metrics (
                venue_provider, events_found, events_stored, venues_processed, 
                success, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
            (
                "dynamic_venues_all",
                total_events,
                stored_events,
                sum(r.get("venues_processed", 0) for r in scraping_results.values()),
                stored_events > 0,
                json.dumps(scraping_results),
            ),
        )

        # Insert individual venue metrics
        for venue_key, result_info in scraping_results.items():
            cur.execute(
                """
                INSERT INTO scraping_metrics (
                    venue_provider, events_found, events_stored, venues_processed,
                    success, error_message, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    f"dynamic_{venue_key}",
                    result_info.get("events_count", 0),
                    (
                        result_info.get("events_count", 0)
                        if "error" not in result_info
                        else 0
                    ),
                    result_info.get("venues_processed", 0),
                    "error" not in result_info,
                    result_info.get("error"),
                    json.dumps(result_info),
                ),
            )

        conn.commit()
        logging.info("Scraping metrics stored successfully")

    except Exception as e:
        logging.error(f"Failed to store scraping metrics: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def scrape_specific_dynamic_venue(venue_name):
    """
    Scrape events from a specific dynamic venue

    Args:
        venue_name (str): Name of the venue to scrape
    """
    venue_config = DYNAMIC_VENUE_SCRAPERS.get(venue_name.lower())
    if not venue_config:
        logging.error(f"No dynamic configuration found for venue: {venue_name}")
        return

    events = scrape_dynamic_venue_events(venue_config)
    if events:
        category = venue_config.get("category", "local_venue")
        upsert_events_to_db(events, category)
        logging.info(f"Scraped {len(events)} dynamic events from {venue_name}")
    else:
        logging.info(f"No dynamic events found for {venue_name}")


def ingest_dynamic_venue_data():
    """
    Main function to ingest dynamic venue data
    """
    logging.info("Starting dynamic venue data ingestion")

    try:
        scrape_all_dynamic_venues()
        logging.info("Dynamic venue data ingestion completed successfully")

    except Exception as e:
        logging.error(f"Dynamic venue data ingestion failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_dynamic_venue_data()
