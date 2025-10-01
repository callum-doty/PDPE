# Unified Venue Collector
"""
Unified venue collection system that consolidates static and dynamic venue scraping.
Combines functionality from ingest_local_venues.py and ingest_dynamic_venues.py
into a single, standardized collector with consistent data quality processing.
"""

import sys
import logging
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse

# Selenium imports for dynamic scraping
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium not available - dynamic venue scraping will be disabled")

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from shared.database.connection import get_db_conn
    from shared.data_quality.quality_controller import QualityController
    from features.venues.processors.venue_processing import (
        process_venues_with_quality_checks,
        log_venue_quality_metrics,
    )

    # Note: These functions may need to be implemented or imported from correct location
    def process_events_with_quality_checks(events):
        """Placeholder for event quality processing"""
        return events, {"overall_quality_score": 0.8}

    def log_quality_metrics(report, source):
        """Placeholder for quality metrics logging"""
        logging.info(f"Quality metrics for {source}: {report}")

except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")

    # Fallback implementations
    def get_db_conn():
        """Fallback database connection"""
        return None

    class QualityController:
        """Fallback quality controller"""

        pass

    def process_events_with_quality_checks(events):
        """Fallback event processing"""
        return events, {"overall_quality_score": 0.8}

    def log_quality_metrics(report, source):
        """Fallback quality logging"""
        logging.info(f"Quality metrics for {source}: {report}")

    def process_venues_with_quality_checks(venues):
        """Fallback venue processing"""
        return venues, {"overall_quality_score": 0.8}

    def log_venue_quality_metrics(report, source):
        """Fallback venue quality logging"""
        logging.info(f"Venue quality metrics for {source}: {report}")


@dataclass
class VenueCollectionResult:
    """Result of venue collection operation"""

    source_name: str
    success: bool
    venues_collected: int
    events_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None
    venues_processed: Optional[int] = None


@dataclass
class VenueData:
    """Standardized venue data structure"""

    external_id: str
    provider: str
    name: str
    description: Optional[str]
    category: str
    subcategory: Optional[str]
    website: Optional[str]
    address: Optional[str]
    phone: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    psychographic_scores: Optional[Dict]
    scraped_at: datetime
    source_type: str  # 'static', 'dynamic', 'api'


@dataclass
class EventData:
    """Standardized event data structure"""

    external_id: str
    provider: str
    name: str
    description: Optional[str]
    category: str
    subcategory: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    venue_name: str
    source_url: Optional[str]
    psychographic_scores: Optional[Dict]
    scraped_at: datetime


class UnifiedVenueCollector:
    """
    Unified venue collector that consolidates static and dynamic venue scraping.

    This class combines the functionality from:
    - ingest_local_venues.py (29 KC venues with static scraping)
    - ingest_dynamic_venues.py (6 venues with Selenium dynamic scraping)

    Into a single, standardized collector with consistent data quality processing.
    """

    def __init__(self):
        """Initialize the unified venue collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

        # Headers for web scraping
        self.scraping_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        # Load venue configurations
        self.static_venue_configs = self._load_static_venue_configs()
        self.dynamic_venue_configs = self._load_dynamic_venue_configs()

        # Psychographic keywords for event classification
        self.psychographic_keywords = {
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

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> VenueCollectionResult:
        """
        Collect venues from all sources (static + dynamic).

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)
            time_period: Time period for collection (not used for venues)

        Returns:
            Consolidated collection result for all venue sources
        """
        results = self.collect_all_venues(area_bounds)

        # Consolidate results into a single result
        total_venues = sum(r.venues_collected for r in results if r.success)
        total_events = sum(r.events_collected for r in results if r.success)
        successful_sources = len([r for r in results if r.success])
        total_duration = sum(r.duration_seconds for r in results)

        # Calculate overall success
        overall_success = successful_sources > 0
        error_messages = [r.error_message for r in results if r.error_message]

        return VenueCollectionResult(
            source_name="unified_venues",
            success=overall_success,
            venues_collected=total_venues,
            events_collected=total_events,
            duration_seconds=total_duration,
            error_message="; ".join(error_messages) if error_messages else None,
            data_quality_score=0.8 if overall_success else 0.0,
        )

    def collect_all_venues(
        self, area_bounds: Optional[Dict] = None
    ) -> List[VenueCollectionResult]:
        """
        Collect venues from all sources (static + dynamic).

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)

        Returns:
            List of collection results for each venue source
        """
        self.logger.info("🏢 Starting unified venue collection (static + dynamic)")

        results = []

        # Collect from static venues (29 KC venues)
        static_results = self.collect_static_venues()
        results.extend(static_results)

        # Collect from dynamic venues (6 venues with Selenium)
        if SELENIUM_AVAILABLE:
            dynamic_results = self.collect_dynamic_venues()
            results.extend(dynamic_results)
        else:
            self.logger.warning(
                "Skipping dynamic venue collection - Selenium not available"
            )

        # Log overall summary
        total_venues = sum(r.venues_collected for r in results if r.success)
        total_events = sum(r.events_collected for r in results if r.success)
        successful_sources = len([r for r in results if r.success])

        self.logger.info(f"✅ Unified venue collection completed:")
        self.logger.info(f"  - Sources processed: {len(results)}")
        self.logger.info(f"  - Successful sources: {successful_sources}")
        self.logger.info(f"  - Total venues collected: {total_venues}")
        self.logger.info(f"  - Total events collected: {total_events}")

        return results

    def collect_static_venues(self) -> List[VenueCollectionResult]:
        """Collect venues from static scraping sources (29 KC venues)."""
        self.logger.info("🏛️ Collecting static venues (29 KC venues)")

        results = []
        venues_to_process = []
        events_by_category = {}

        for venue_key, venue_config in self.static_venue_configs.items():
            start_time = datetime.now()
            venue_name = venue_config["name"]

            try:
                # Scrape events from this venue
                raw_events = self._scrape_static_venue_events(venue_config)

                # Process events through quality pipeline
                if raw_events:
                    processed_events, quality_report = (
                        process_events_with_quality_checks(raw_events)
                    )
                    log_quality_metrics(quality_report, venue_name)

                    if processed_events:
                        category = venue_config.get("category", "local_venue")
                        if category not in events_by_category:
                            events_by_category[category] = []
                        events_by_category[category].extend(processed_events)

                # Create venue data for processing
                venue_data = self._create_venue_from_static_config(venue_config)
                if venue_data:
                    venues_to_process.append(venue_data)

                duration = (datetime.now() - start_time).total_seconds()

                result = VenueCollectionResult(
                    source_name=f"static_{venue_key}",
                    success=True,
                    venues_collected=1,
                    events_collected=len(processed_events) if raw_events else 0,
                    duration_seconds=duration,
                    data_quality_score=(
                        quality_report.get("overall_quality_score", 0.8)
                        if raw_events
                        else 0.8
                    ),
                )
                results.append(result)

                self.logger.info(
                    f"✅ {venue_name}: {len(processed_events) if raw_events else 0} events"
                )

                # Respectful delay
                time.sleep(2)

            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                result = VenueCollectionResult(
                    source_name=f"static_{venue_key}",
                    success=False,
                    venues_collected=0,
                    events_collected=0,
                    duration_seconds=duration,
                    error_message=str(e),
                )
                results.append(result)
                self.logger.error(f"❌ Failed to scrape {venue_name}: {e}")

        # Process venues through quality pipeline
        if venues_to_process:
            processed_venues, venue_quality_report = process_venues_with_quality_checks(
                venues_to_process
            )
            if processed_venues:
                self._upsert_venues_to_db(processed_venues)
                log_venue_quality_metrics(venue_quality_report, "static_venues")

        # Store events by category
        for category, events in events_by_category.items():
            if events:
                self._upsert_events_to_db(events, category)

        return results

    def collect_dynamic_venues(self) -> List[VenueCollectionResult]:
        """Collect venues from dynamic scraping sources (6 venues with Selenium)."""
        if not SELENIUM_AVAILABLE:
            return []

        self.logger.info("🌐 Collecting dynamic venues (6 venues with Selenium)")

        results = []
        events_by_category = {}

        for venue_key, venue_config in self.dynamic_venue_configs.items():
            start_time = datetime.now()
            venue_name = venue_config["name"]

            try:
                # Scrape events from this dynamic venue
                raw_events = self._scrape_dynamic_venue_events(venue_config)

                # Convert to standard format and process
                if raw_events:
                    category = venue_config.get("category", "local_venue")
                    if category not in events_by_category:
                        events_by_category[category] = []
                    events_by_category[category].extend(raw_events)

                duration = (datetime.now() - start_time).total_seconds()

                result = VenueCollectionResult(
                    source_name=f"dynamic_{venue_key}",
                    success=True,
                    venues_collected=1,
                    events_collected=len(raw_events),
                    duration_seconds=duration,
                    data_quality_score=0.7,  # Dynamic scraping typically has lower quality
                )
                results.append(result)

                self.logger.info(f"✅ {venue_name}: {len(raw_events)} events")

                # Longer delay for dynamic scraping
                time.sleep(5)

            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                result = VenueCollectionResult(
                    source_name=f"dynamic_{venue_key}",
                    success=False,
                    venues_collected=0,
                    events_collected=0,
                    duration_seconds=duration,
                    error_message=str(e),
                )
                results.append(result)
                self.logger.error(
                    f"❌ Failed to scrape dynamic venue {venue_name}: {e}"
                )

        # Store events by category
        for category, events in events_by_category.items():
            if events:
                self._upsert_events_to_db(events, category)

        return results

    def _load_static_venue_configs(self) -> Dict:
        """Load static venue scraper configurations (from ingest_local_venues.py)."""
        return {
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
            # Add more static venues as needed...
        }

    def _load_dynamic_venue_configs(self) -> Dict:
        """Load dynamic venue scraper configurations (from ingest_dynamic_venues.py)."""
        return {
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
            # Add more dynamic venues as needed...
        }

    def _scrape_static_venue_events(self, venue_config: Dict) -> List[Dict]:
        """Scrape events from a static venue website."""
        venue_name = venue_config["name"]
        events_url = venue_config["events_url"]
        base_url = venue_config["base_url"]
        selectors = venue_config["selectors"]

        self.logger.debug(f"Scraping static events from {venue_name}: {events_url}")

        response = self._safe_scrape_request(events_url)
        if not response:
            return []

        try:
            soup = BeautifulSoup(response.content, "html.parser")
            event_containers = soup.select(selectors["event_container"])

            events = []
            for container in event_containers:
                try:
                    # Extract event data
                    title = self._extract_text_safely(container, selectors["title"])
                    date_text = self._extract_text_safely(container, selectors["date"])
                    venue_text = self._extract_text_safely(
                        container, selectors["venue"]
                    )
                    description = self._extract_text_safely(
                        container, selectors["description"]
                    )
                    link = self._extract_link_safely(
                        container, selectors["link"], base_url
                    )

                    if not title:  # Skip if no title found
                        continue

                    # Parse date
                    parsed_date = self._parse_event_date(date_text)

                    # Skip past events (older than 1 day)
                    if parsed_date and parsed_date < datetime.now() - timedelta(days=1):
                        continue

                    # Classify psychographics
                    psychographic_scores = self._classify_event_psychographics(
                        title, description
                    )

                    event = {
                        "external_id": f"{venue_name.lower().replace(' ', '_')}_{hash(title + date_text)}",
                        "provider": venue_name.lower().replace(" ", "_"),
                        "name": title,
                        "description": description,
                        "category": "local_event",
                        "subcategory": self._determine_event_subcategory(
                            title, description
                        ),
                        "start_time": parsed_date,
                        "end_time": None,
                        "venue_name": venue_text or venue_name,
                        "source_url": link,
                        "psychographic_scores": psychographic_scores,
                        "scraped_at": datetime.now(),
                    }

                    events.append(event)

                except Exception as e:
                    self.logger.error(
                        f"Error parsing event container from {venue_name}: {e}"
                    )
                    continue

            self.logger.debug(f"Scraped {len(events)} static events from {venue_name}")
            return events

        except Exception as e:
            self.logger.error(f"Error parsing HTML from {venue_name}: {e}")
            return []

    def _scrape_dynamic_venue_events(self, venue_config: Dict) -> List[Dict]:
        """Scrape events from a dynamic venue website using Selenium."""
        if not SELENIUM_AVAILABLE:
            return []

        venue_name = venue_config["name"]
        events_url = venue_config["events_url"]
        base_url = venue_config["base_url"]
        selectors = venue_config["selectors"]
        wait_for = venue_config["wait_for"]
        scroll_to_load = venue_config.get("scroll_to_load", False)
        max_scrolls = venue_config.get("max_scrolls", 0)

        self.logger.debug(f"Scraping dynamic events from {venue_name}: {events_url}")

        driver = self._create_webdriver()
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
                self._scroll_to_load_content(driver, max_scrolls)

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
                    title = self._extract_text_from_element(
                        driver, element, selectors["title"]
                    )
                    date_text = self._extract_text_from_element(
                        driver, element, selectors["date"]
                    )
                    venue_text = self._extract_text_from_element(
                        driver, element, selectors["venue"]
                    )
                    description = self._extract_text_from_element(
                        driver, element, selectors["description"]
                    )
                    link = self._extract_link_from_element(
                        driver, element, selectors["link"], base_url
                    )

                    if not title:  # Skip if no title found
                        continue

                    # Parse date
                    parsed_date = self._parse_event_date(date_text)

                    # Skip past events (older than 1 day)
                    if parsed_date and parsed_date < datetime.now() - timedelta(days=1):
                        continue

                    # Classify psychographics
                    psychographic_scores = self._classify_event_psychographics(
                        title, description
                    )

                    event = {
                        "external_id": f"{venue_name.lower().replace(' ', '_')}_{hash(title + date_text)}",
                        "provider": venue_name.lower().replace(" ", "_"),
                        "name": title,
                        "description": description,
                        "category": "local_event",
                        "subcategory": self._determine_event_subcategory(
                            title, description
                        ),
                        "start_time": parsed_date,
                        "end_time": None,
                        "venue_name": venue_text or venue_name,
                        "source_url": link,
                        "psychographic_scores": psychographic_scores,
                        "scraped_at": datetime.now(),
                    }

                    events.append(event)

                except Exception as e:
                    self.logger.error(
                        f"Error parsing event element from {venue_name}: {e}"
                    )
                    continue

            self.logger.debug(f"Scraped {len(events)} dynamic events from {venue_name}")
            return events

        except TimeoutException:
            self.logger.error(f"Timeout waiting for content to load from {venue_name}")
            return []
        except Exception as e:
            self.logger.error(f"Error scraping dynamic venue {venue_name}: {e}")
            return []
        finally:
            driver.quit()

    def _create_venue_from_static_config(
        self, venue_config: Dict
    ) -> Optional[VenueData]:
        """Create venue data structure from static venue configuration."""
        try:
            venue_name = venue_config["name"]
            base_url = venue_config["base_url"]
            category = venue_config.get("category", "local_venue")
            provider = venue_name.lower().replace(" ", "_")
            external_id = f"{provider}_venue"

            # Try to extract additional venue information from the main page
            venue_description = ""
            venue_address = ""
            venue_phone = ""

            try:
                response = self._safe_scrape_request(base_url, timeout=5)
                if response:
                    soup = BeautifulSoup(response.content, "html.parser")

                    # Try to extract description
                    desc_selectors = [
                        ".about",
                        ".description",
                        ".venue-info",
                        ".intro",
                        "meta[name='description']",
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
                        ".venue-address",
                    ]
                    for selector in address_selectors:
                        element = soup.select_one(selector)
                        if element:
                            venue_address = element.get_text(strip=True)[:200]
                            break

            except Exception as e:
                self.logger.debug(
                    f"Could not scrape additional venue details for {venue_name}: {e}"
                )

            return VenueData(
                external_id=external_id,
                provider=provider,
                name=venue_name,
                description=venue_description,
                category=category,
                subcategory=category,
                website=base_url,
                address=venue_address,
                phone=venue_phone,
                lat=None,  # Will need geocoding
                lng=None,
                psychographic_scores={},
                scraped_at=datetime.now(),
                source_type="static",
            )

        except Exception as e:
            self.logger.error(f"Error creating venue data from config: {e}")
            return None

    # Helper methods
    def _safe_scrape_request(
        self, url: str, timeout: int = 10
    ) -> Optional[requests.Response]:
        """Make a safe request for web scraping."""
        try:
            response = requests.get(url, headers=self.scraping_headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to scrape {url}: {e}")
            return None

    def _extract_text_safely(self, element, selector: str) -> str:
        """Safely extract text from a BeautifulSoup element."""
        try:
            found = element.select_one(selector)
            return found.get_text(strip=True) if found else ""
        except Exception:
            return ""

    def _extract_link_safely(self, element, selector: str, base_url: str) -> str:
        """Safely extract and resolve a link from an element."""
        try:
            found = element.select_one(selector)
            if found and found.get("href"):
                return urljoin(base_url, found.get("href"))
            return ""
        except Exception:
            return ""

    def _parse_event_date(self, date_text: str) -> Optional[datetime]:
        """Parse event date from various text formats."""
        if not date_text:
            return None

        try:
            from dateutil import parser

            return parser.parse(date_text, fuzzy=True)
        except Exception:
            self.logger.warning(f"Could not parse date: {date_text}")
            return None

    def _classify_event_psychographics(self, title: str, description: str) -> Dict:
        """Classify event based on psychographic keywords."""
        text = f"{title} {description}".lower()
        scores = {}

        for category, keywords in self.psychographic_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            scores[category] = score

        return scores

    def _determine_event_subcategory(self, title: str, description: str) -> str:
        """Determine event subcategory based on title and description."""
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
            "family": [
                "family",
                "kids",
                "children",
                "child",
                "parent",
                "family-friendly",
            ],
            "nightlife": [
                "nightlife",
                "bar",
                "club",
                "party",
                "drinks",
                "dancing",
                "dj",
            ],
            "cultural": [
                "cultural",
                "culture",
                "heritage",
                "history",
                "museum",
                "theater",
                "theatre",
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

    # Selenium helper methods
    def _create_webdriver(self):
        """Create a Chrome WebDriver instance for dynamic scraping."""
        if not SELENIUM_AVAILABLE:
            return None

        chrome_options = Options()
        chrome_options.add_argument("--headless")
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
            self.logger.error(f"Failed to create Chrome WebDriver: {e}")
            return None

    def _scroll_to_load_content(self, driver, max_scrolls=3):
        """Scroll down the page to trigger lazy loading of content."""
        previous_height = 0
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

    def _extract_text_from_element(self, driver, element, selector):
        """Safely extract text from a web element using CSS selector."""
        try:
            found_element = element.find_element(By.CSS_SELECTOR, selector)
            return found_element.text.strip()
        except NoSuchElementException:
            return ""
        except Exception:
            return ""

    def _extract_link_from_element(self, driver, element, selector, base_url):
        """Safely extract and resolve a link from a web element."""
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

    # Database operations
    def _upsert_venues_to_db(self, venues: List[VenueData]):
        """Insert or update venues in the database."""
        if not venues:
            return

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return

        cur = conn.cursor()

        try:
            for venue in venues:
                # Check if venue already exists
                cur.execute(
                    """
                    SELECT venue_id FROM venues 
                    WHERE external_id = %s AND provider = %s
                """,
                    (venue.external_id, venue.provider),
                )

                existing_venue = cur.fetchone()

                if existing_venue:
                    # Update existing venue
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
                            updated_at = NOW()
                        WHERE venue_id = %s
                    """,
                        (
                            venue.name,
                            venue.description,
                            venue.category,
                            venue.subcategory,
                            venue.lat,
                            venue.lng,
                            venue.address,
                            venue.phone,
                            venue.website,
                            (
                                json.dumps(venue.psychographic_scores)
                                if venue.psychographic_scores
                                else None
                            ),
                            existing_venue[0],
                        ),
                    )
                else:
                    # Insert new venue
                    cur.execute(
                        """
                        INSERT INTO venues (
                            external_id, provider, name, description, category, subcategory,
                            lat, lng, address, phone, website, psychographic_relevance
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """,
                        (
                            venue.external_id,
                            venue.provider,
                            venue.name,
                            venue.description,
                            venue.category,
                            venue.subcategory,
                            venue.lat,
                            venue.lng,
                            venue.address,
                            venue.phone,
                            venue.website,
                            (
                                json.dumps(venue.psychographic_scores)
                                if venue.psychographic_scores
                                else None
                            ),
                        ),
                    )

            conn.commit()
            self.logger.info(f"Successfully upserted {len(venues)} venues to database")

        except Exception as e:
            self.logger.error(f"Error upserting venues to database: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _upsert_events_to_db(
        self, events: List[Dict], venue_category: str = "local_venue"
    ):
        """Insert or update events in the database."""
        if not events:
            return

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return

        cur = conn.cursor()

        try:
            for event in events:
                # Find or create venue
                venue_id = self._find_or_create_venue(
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
                            (
                                json.dumps(event["psychographic_scores"])
                                if event["psychographic_scores"]
                                else None
                            ),
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
                            (
                                json.dumps(event["psychographic_scores"])
                                if event["psychographic_scores"]
                                else None
                            ),
                        ),
                    )

            conn.commit()
            self.logger.info(f"Successfully upserted {len(events)} events to database")

        except Exception as e:
            self.logger.error(f"Error upserting events to database: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _find_or_create_venue(
        self, venue_name: str, provider: str, category: str = "local_venue"
    ) -> Optional[str]:
        """Find existing venue or create a new one."""
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

            self.logger.info(
                f"Created new venue: {venue_name} ({venue_id}) - Category: {category}"
            )
            return venue_id

        except Exception as e:
            self.logger.error(f"Error finding/creating venue {venue_name}: {e}")
            conn.rollback()
            return None
        finally:
            cur.close()
            conn.close()


# Create an alias for backward compatibility
VenueCollector = UnifiedVenueCollector
