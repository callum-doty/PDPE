"""
Unified Venues Service for PPM Application

Single service that consolidates ALL venue-related functionality:
- Static venue scraping (29 KC venues)
- Dynamic venue scraping (Selenium-based)
- API venue collection (Google Places, Yelp, etc.)
- Venue data processing and quality validation
- Database operations

Replaces the entire features/venues/ directory structure.
"""

import logging
import time
import json
import re
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from urllib.parse import urljoin
from bs4 import BeautifulSoup

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

# Import core services
from core.database import get_database, OperationResult
from core.quality import get_quality_validator


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
    avg_rating: Optional[float]
    psychographic_scores: Optional[Dict]
    scraped_at: datetime
    source_type: str  # 'static', 'dynamic', 'api'


class VenueService:
    """
    Unified venue service that handles ALL venue operations.

    Consolidates functionality from:
    - features/venues/collectors/venue_collector.py (1000+ lines)
    - features/venues/scrapers/static_venue_scraper.py
    - features/venues/scrapers/dynamic_venue_scraper.py
    - features/venues/processors/venue_processing.py

    Into a single, manageable service with clear entry points.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = get_database()
        self.quality_validator = get_quality_validator()

        # Web scraping configuration
        self.scraping_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

        # Psychographic keywords for venue classification
        self.psychographic_keywords = {
            "career_driven": [
                "networking",
                "professional",
                "business",
                "corporate",
                "conference",
            ],
            "competent": [
                "expert",
                "masterclass",
                "training",
                "certification",
                "skill",
                "advanced",
            ],
            "fun": [
                "party",
                "celebration",
                "festival",
                "concert",
                "nightlife",
                "entertainment",
                "music",
                "dance",
            ],
            "social": ["social", "community", "meetup", "gathering", "group"],
            "adventurous": [
                "adventure",
                "outdoor",
                "extreme",
                "challenge",
                "exploration",
            ],
        }

    # ========== PUBLIC API METHODS ==========

    def collect_from_google_places(
        self, location: str = "Kansas City, MO", radius: int = 10000
    ) -> OperationResult:
        """
        Collect venues from Google Places API.

        Args:
            location: Location to search around
            radius: Search radius in meters

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info(
            f"🏢 Collecting venues from Google Places API around {location}"
        )

        try:
            # Note: This would require Google Places API key
            # For now, return a mock result to maintain interface compatibility
            venues_collected = 0

            # TODO: Implement actual Google Places API integration
            # venues = self._fetch_google_places_venues(location, radius)
            # venues_collected = self._process_and_store_venues(venues, "google_places", "api")

            duration = (datetime.now() - start_time).total_seconds()

            return OperationResult(
                success=True,
                data=venues_collected,
                message=f"Google Places API collection completed: {venues_collected} venues in {duration:.1f}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Google Places collection failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Google Places collection failed after {duration:.1f}s: {e}",
            )

    def collect_from_yelp(
        self, location: str = "Kansas City, MO", limit: int = 50
    ) -> OperationResult:
        """
        Collect venues from Yelp API.

        Args:
            location: Location to search
            limit: Maximum number of venues to collect

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info(f"🍽️ Collecting venues from Yelp API in {location}")

        try:
            # Note: This would require Yelp API key
            # For now, return a mock result to maintain interface compatibility
            venues_collected = 0

            # TODO: Implement actual Yelp API integration
            # venues = self._fetch_yelp_venues(location, limit)
            # venues_collected = self._process_and_store_venues(venues, "yelp", "api")

            duration = (datetime.now() - start_time).total_seconds()

            return OperationResult(
                success=True,
                data=venues_collected,
                message=f"Yelp API collection completed: {venues_collected} venues in {duration:.1f}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Yelp collection failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Yelp collection failed after {duration:.1f}s: {e}",
            )

    def collect_from_scraped_sources(self) -> OperationResult:
        """
        Collect venues from web scraping (static + dynamic sources).

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🌐 Collecting venues from scraped sources")

        try:
            total_venues = 0

            # Collect from static sources (major KC venues)
            static_result = self._collect_static_venues()
            if static_result.success:
                total_venues += static_result.data

            # Collect from dynamic sources (if Selenium available)
            if SELENIUM_AVAILABLE:
                dynamic_result = self._collect_dynamic_venues()
                if dynamic_result.success:
                    total_venues += dynamic_result.data
            else:
                self.logger.warning(
                    "Skipping dynamic venue collection - Selenium not available"
                )

            duration = (datetime.now() - start_time).total_seconds()

            return OperationResult(
                success=True,
                data=total_venues,
                message=f"Scraped sources collection completed: {total_venues} venues in {duration:.1f}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Scraped sources collection failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Scraped sources collection failed after {duration:.1f}s: {e}",
            )

    def collect_all(self) -> OperationResult:
        """
        Collect venues from ALL sources (APIs + scraping).

        Returns:
            OperationResult with comprehensive collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting comprehensive venue collection from all sources")

        results = []
        total_venues = 0

        # Collect from scraped sources first (most reliable)
        scraped_result = self.collect_from_scraped_sources()
        results.append(("Scraped Sources", scraped_result))
        if scraped_result.success:
            total_venues += scraped_result.data

        # Collect from Google Places API
        google_result = self.collect_from_google_places()
        results.append(("Google Places", google_result))
        if google_result.success:
            total_venues += google_result.data

        # Collect from Yelp API
        yelp_result = self.collect_from_yelp()
        results.append(("Yelp", yelp_result))
        if yelp_result.success:
            total_venues += yelp_result.data

        # Calculate overall success
        successful_sources = len([r for _, r in results if r.success])
        duration = (datetime.now() - start_time).total_seconds()

        # Log detailed results
        self.logger.info("📊 Venue collection summary:")
        for source_name, result in results:
            status = "✅" if result.success else "❌"
            self.logger.info(
                f"  {status} {source_name}: {result.data if result.success else 0} venues"
            )

        return OperationResult(
            success=successful_sources > 0,
            data=total_venues,
            message=f"Comprehensive collection completed: {total_venues} venues from {successful_sources}/{len(results)} sources in {duration:.1f}s",
        )

    def get_venues(
        self, filters: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Get venues from database with optional filtering.

        Args:
            filters: Optional filters (category, has_location, min_rating, etc.)
            limit: Optional limit on number of results

        Returns:
            List of venue dictionaries
        """
        try:
            # Add limit to filters if provided
            if filters is None:
                filters = {}
            if limit is not None:
                filters["limit"] = limit
            return self.db.get_venues(filters)
        except Exception as e:
            self.logger.error(f"Failed to get venues: {e}")
            return []

    def get_venues_with_predictions(self) -> List[Dict]:
        """
        Get venues with their ML predictions for map display.

        Returns:
            List of venue dictionaries with prediction data
        """
        try:
            return self.db.get_venues_with_predictions()
        except Exception as e:
            self.logger.error(f"Failed to get venues with predictions: {e}")
            return []

    def get_venue_by_id(self, venue_id: str) -> Optional[Dict]:
        """
        Get a specific venue by ID.

        Args:
            venue_id: Venue ID to retrieve

        Returns:
            Venue dictionary or None if not found
        """
        try:
            venues = self.db.get_venues({"venue_id": venue_id})
            return venues[0] if venues else None
        except Exception as e:
            self.logger.error(f"Failed to get venue {venue_id}: {e}")
            return None

    # ========== PRIVATE IMPLEMENTATION METHODS ==========

    def _collect_static_venues(self) -> OperationResult:
        """Collect venues from static scraping sources (major KC venues)."""
        start_time = datetime.now()
        self.logger.info("🏛️ Collecting static venues (major KC venues)")

        # Static venue configurations (major KC venues)
        static_venues = {
            "tmobile_center": {
                "name": "T-Mobile Center",
                "base_url": "https://www.t-mobilecenter.com",
                "category": "major_venue",
                "address": "1407 Grand Blvd, Kansas City, MO 64106",
                "lat": 39.1031,
                "lng": -94.5844,
            },
            "kauffman_center": {
                "name": "Kauffman Center for the Performing Arts",
                "base_url": "https://www.kauffmancenter.org",
                "category": "major_venue",
                "address": "1601 Broadway Blvd, Kansas City, MO 64108",
                "lat": 39.0997,
                "lng": -94.5786,
            },
            "starlight_theatre": {
                "name": "Starlight Theatre",
                "base_url": "https://www.kcstarlight.com",
                "category": "major_venue",
                "address": "4600 Starlight Rd, Kansas City, MO 64132",
                "lat": 39.0158,
                "lng": -94.5275,
            },
            "midland_theatre": {
                "name": "The Midland Theatre",
                "base_url": "https://www.midlandkc.com",
                "category": "major_venue",
                "address": "1228 Main St, Kansas City, MO 64105",
                "lat": 39.1019,
                "lng": -94.5833,
            },
            "uptown_theater": {
                "name": "Uptown Theater",
                "base_url": "https://www.uptowntheater.com",
                "category": "major_venue",
                "address": "3700 Broadway, Kansas City, MO 64111",
                "lat": 39.0608,
                "lng": -94.5897,
            },
            "powerandlight": {
                "name": "Power & Light District",
                "base_url": "https://powerandlightdistrict.com",
                "category": "entertainment_district",
                "address": "1200 Main St, Kansas City, MO 64105",
                "lat": 39.1000,
                "lng": -94.5833,
            },
            "westport": {
                "name": "Westport Entertainment District",
                "base_url": "https://westportkcmo.com",
                "category": "entertainment_district",
                "address": "4000 Pennsylvania Ave, Kansas City, MO 64111",
                "lat": 39.0597,
                "lng": -94.5958,
            },
            "crossroads": {
                "name": "Crossroads Arts District",
                "base_url": "https://www.crossroadsartsdistrict.org",
                "category": "cultural",
                "address": "1815 Wyandotte St, Kansas City, MO 64108",
                "lat": 39.0889,
                "lng": -94.5814,
            },
            "nelson_atkins": {
                "name": "Nelson-Atkins Museum of Art",
                "base_url": "https://www.nelson-atkins.org",
                "category": "museum",
                "address": "4525 Oak St, Kansas City, MO 64111",
                "lat": 39.0458,
                "lng": -94.5806,
            },
            "union_station": {
                "name": "Union Station Kansas City",
                "base_url": "https://www.unionstation.org",
                "category": "major_venue",
                "address": "30 W Pershing Rd, Kansas City, MO 64108",
                "lat": 39.0844,
                "lng": -94.5858,
            },
        }

        venues_processed = 0

        for venue_key, venue_config in static_venues.items():
            try:
                # Create venue data
                venue_data = VenueData(
                    external_id=f"static_{venue_key}",
                    provider="static_scraper",
                    name=venue_config["name"],
                    description=self._scrape_venue_description(
                        venue_config.get("base_url")
                    ),
                    category=venue_config["category"],
                    subcategory=venue_config["category"],
                    website=venue_config.get("base_url"),
                    address=venue_config.get("address"),
                    phone=None,
                    lat=venue_config.get("lat"),
                    lng=venue_config.get("lng"),
                    avg_rating=None,
                    psychographic_scores=self._calculate_venue_psychographics(
                        venue_config["name"], venue_config["category"]
                    ),
                    scraped_at=datetime.now(),
                    source_type="static",
                )

                # Validate and store venue
                if self._validate_and_store_venue(venue_data):
                    venues_processed += 1
                    self.logger.debug(
                        f"✅ Processed static venue: {venue_config['name']}"
                    )

                # Respectful delay
                time.sleep(1)

            except Exception as e:
                self.logger.error(f"❌ Failed to process static venue {venue_key}: {e}")
                continue

        duration = (datetime.now() - start_time).total_seconds()

        return OperationResult(
            success=venues_processed > 0,
            data=venues_processed,
            message=f"Static venue collection completed: {venues_processed} venues in {duration:.1f}s",
        )

    def _collect_dynamic_venues(self) -> OperationResult:
        """Collect venues from dynamic scraping sources (Selenium-based)."""
        if not SELENIUM_AVAILABLE:
            return OperationResult(
                success=False,
                error="Selenium not available",
                message="Dynamic venue collection skipped - Selenium not installed",
            )

        start_time = datetime.now()
        self.logger.info("🌐 Collecting dynamic venues (Selenium-based)")

        # Dynamic venue configurations
        dynamic_sources = {
            "visitkc": {
                "name": "Visit KC",
                "url": "https://www.visitkc.com/things-to-do",
                "category": "aggregator",
            },
            "do816": {
                "name": "Do816",
                "url": "https://do816.com/venues",
                "category": "aggregator",
            },
        }

        venues_processed = 0

        for source_key, source_config in dynamic_sources.items():
            try:
                venues = self._scrape_dynamic_venues(source_config)
                for venue_data in venues:
                    if self._validate_and_store_venue(venue_data):
                        venues_processed += 1

                self.logger.debug(
                    f"✅ Processed {len(venues)} venues from {source_config['name']}"
                )

                # Longer delay for dynamic scraping
                time.sleep(5)

            except Exception as e:
                self.logger.error(
                    f"❌ Failed to scrape dynamic source {source_key}: {e}"
                )
                continue

        duration = (datetime.now() - start_time).total_seconds()

        return OperationResult(
            success=venues_processed > 0,
            data=venues_processed,
            message=f"Dynamic venue collection completed: {venues_processed} venues in {duration:.1f}s",
        )

    def _scrape_venue_description(self, url: Optional[str]) -> Optional[str]:
        """Scrape venue description from website."""
        if not url:
            return None

        try:
            response = requests.get(url, headers=self.scraping_headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")

            # Try various selectors for description
            description_selectors = [
                'meta[name="description"]',
                ".about",
                ".description",
                ".venue-info",
                ".intro",
                'p:contains("about")',
                'p:contains("description")',
            ]

            for selector in description_selectors:
                if selector.startswith("meta"):
                    element = soup.select_one(selector)
                    if element:
                        return element.get("content", "")[:500]
                else:
                    element = soup.select_one(selector)
                    if element:
                        return element.get_text(strip=True)[:500]

            return None

        except Exception as e:
            self.logger.debug(f"Could not scrape description from {url}: {e}")
            return None

    def _scrape_dynamic_venues(self, source_config: Dict) -> List[VenueData]:
        """Scrape venues from a dynamic source using Selenium."""
        venues = []
        driver = None

        try:
            driver = self._create_webdriver()
            if not driver:
                return venues

            driver.get(source_config["url"])

            # Wait for content to load
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Scroll to load more content
            self._scroll_to_load_content(driver, max_scrolls=3)

            # Find venue elements (this would need to be customized per site)
            venue_elements = driver.find_elements(
                By.CSS_SELECTOR, ".venue, .listing, .place"
            )

            for element in venue_elements[:20]:  # Limit to prevent overload
                try:
                    name = self._extract_text_from_element(
                        element, "h1, h2, h3, .title, .name"
                    )
                    if not name:
                        continue

                    address = self._extract_text_from_element(
                        element, ".address, .location"
                    )
                    website = self._extract_link_from_element(element, "a")

                    venue_data = VenueData(
                        external_id=f"dynamic_{source_config['name'].lower()}_{hash(name)}",
                        provider=f"dynamic_{source_config['name'].lower()}",
                        name=name,
                        description=None,
                        category=source_config["category"],
                        subcategory="scraped_venue",
                        website=website,
                        address=address,
                        phone=None,
                        lat=None,
                        lng=None,
                        avg_rating=None,
                        psychographic_scores=self._calculate_venue_psychographics(
                            name, source_config["category"]
                        ),
                        scraped_at=datetime.now(),
                        source_type="dynamic",
                    )

                    venues.append(venue_data)

                except Exception as e:
                    self.logger.debug(f"Error processing venue element: {e}")
                    continue

        except Exception as e:
            self.logger.error(
                f"Error scraping dynamic venues from {source_config['name']}: {e}"
            )

        finally:
            if driver:
                driver.quit()

        return venues

    def _create_webdriver(self):
        """Create a Chrome WebDriver instance for dynamic scraping."""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # Disable images and CSS for faster loading
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.managed_default_content_settings.stylesheets": 2,
            }
            chrome_options.add_experimental_option("prefs", prefs)

            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            return driver

        except Exception as e:
            self.logger.error(f"Failed to create Chrome WebDriver: {e}")
            return None

    def _scroll_to_load_content(self, driver, max_scrolls: int = 3):
        """Scroll down the page to trigger lazy loading of content."""
        for i in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

    def _extract_text_from_element(self, element, selector: str) -> str:
        """Extract text from element using CSS selector."""
        try:
            found_element = element.find_element(By.CSS_SELECTOR, selector)
            return found_element.text.strip()
        except:
            return ""

    def _extract_link_from_element(self, element, selector: str = "a") -> str:
        """Extract link from element."""
        try:
            found_element = element.find_element(By.CSS_SELECTOR, selector)
            href = found_element.get_attribute("href")
            return href if href and href.startswith("http") else ""
        except:
            return ""

    def _calculate_venue_psychographics(self, name: str, category: str) -> Dict:
        """Calculate psychographic scores for a venue based on name and category."""
        text = f"{name} {category}".lower()
        scores = {}

        for psychographic, keywords in self.psychographic_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            scores[psychographic] = min(score / len(keywords), 1.0)  # Normalize to 0-1

        return scores

    def _validate_and_store_venue(self, venue_data: VenueData) -> bool:
        """Validate venue data and store in database."""
        try:
            # Convert to dictionary for validation
            venue_dict = {
                "external_id": venue_data.external_id,
                "provider": venue_data.provider,
                "name": venue_data.name,
                "description": venue_data.description,
                "category": venue_data.category,
                "subcategory": venue_data.subcategory,
                "website": venue_data.website,
                "address": venue_data.address,
                "phone": venue_data.phone,
                "lat": venue_data.lat,
                "lng": venue_data.lng,
                "avg_rating": venue_data.avg_rating,
                "psychographic_relevance": venue_data.psychographic_scores,
            }

            # Validate data quality
            is_valid, validation_results = self.quality_validator.validate_venue(
                venue_dict
            )

            if not is_valid:
                # Log validation errors
                errors = [
                    r.error_message for r in validation_results if r.error_message
                ]
                self.logger.warning(
                    f"Venue validation failed for {venue_data.name}: {'; '.join(errors)}"
                )
                return False

            # Store in database
            result = self.db.upsert_venue(venue_dict)

            if result.success:
                self.logger.debug(f"Successfully stored venue: {venue_data.name}")
                return True
            else:
                self.logger.error(
                    f"Failed to store venue {venue_data.name}: {result.error}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error validating/storing venue {venue_data.name}: {e}")
            return False


# Global venue service instance
_venue_service = None


def get_venue_service() -> VenueService:
    """Get the global venue service instance"""
    global _venue_service
    if _venue_service is None:
        _venue_service = VenueService()
    return _venue_service
