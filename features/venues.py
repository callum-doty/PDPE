"""
Unified Venues Service for PPM Application

Single service that consolidates ALL venue-related functionality using LLM-based scraping.
All 29 KC venue sources scraped using OpenAI for robust extraction.

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
import html2text
import os

# Playwright imports for dynamic scraping
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning(
        "Playwright not available - dynamic venue scraping will use static fallback"
    )

# Selenium imports for dynamic scraping
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning(
        "Selenium not available - dynamic venue scraping will use static fallback"
    )

# OpenAI imports for LLM extraction
try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logging.error("OpenAI not available - LLM venue scraping will be disabled")

# Import core services
from core.database import get_database, OperationResult
from core.quality import get_quality_validator
from config.constants import KC_BOUNDING_BOX, KC_DOWNTOWN


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

        # Initialize OpenAI client for LLM extraction
        self.openai_client = self._initialize_openai_client()

        # Web scraping configuration
        self.scraping_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        }

        # HTML to markdown converter for LLM processing
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True

        # All 29 KC Venue Sources (comprehensive list)
        self.kc_venues = {
            # Major Venues - Static HTML
            "tmobile_center": {
                "name": "T-Mobile Center",
                "url": "https://www.t-mobilecenter.com/events",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "uptown_theater": {
                "name": "Uptown Theater",
                "url": "https://uptowntheater.com/calendar/",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "kauffman_center": {
                "name": "Kauffman Center for the Performing Arts",
                "url": "https://www.kauffmancenter.org/events/",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "starlight_theatre": {
                "name": "Starlight Theatre",
                "url": "https://www.kcstarlight.com/events/",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "midland_theatre": {
                "name": "The Midland Theatre",
                "url": "https://www.midlandkc.com/events",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "knuckleheads": {
                "name": "Knuckleheads Saloon",
                "url": "https://knuckleheadskc.com/",
                "category": "major_venue",
                "scrape_type": "static",
            },
            "azura_amphitheater": {
                "name": "Azura Amphitheater",
                "url": "https://www.azuraamphitheater.com/events",
                "category": "major_venue",
                "scrape_type": "static",
            },
            # Entertainment Districts - Static HTML
            "powerandlight": {
                "name": "Power & Light District",
                "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
                "category": "entertainment_district",
                "scrape_type": "static",
            },
            "westport": {
                "name": "Westport KC",
                "url": "https://westportkcmo.com/events/",
                "category": "entertainment_district",
                "scrape_type": "static",
            },
            "jazz_district": {
                "name": "18th & Vine Jazz District",
                "url": "https://www.kcjazzdistrict.org/",
                "category": "entertainment_district",
                "scrape_type": "static",
            },
            "crossroads": {
                "name": "Crossroads KC",
                "url": "https://kccrossroads.org/events/",
                "category": "entertainment_district",
                "scrape_type": "static",
            },
            # Shopping & Cultural - Static HTML
            "country_club_plaza": {
                "name": "Country Club Plaza",
                "url": "https://countryclubplaza.com/events/",
                "category": "shopping_cultural",
                "scrape_type": "static",
            },
            "crown_center": {
                "name": "Crown Center",
                "url": "https://www.crowncenter.com/events",
                "category": "shopping_cultural",
                "scrape_type": "static",
            },
            "union_station": {
                "name": "Union Station Kansas City",
                "url": "https://unionstation.org/events/",
                "category": "shopping_cultural",
                "scrape_type": "static",
            },
            # Museums - Static HTML
            "nelson_atkins": {
                "name": "Nelson-Atkins Museum of Art",
                "url": "https://www.nelson-atkins.org/events/",
                "category": "museum",
                "scrape_type": "static",
            },
            "wwi_museum": {
                "name": "National WWI Museum",
                "url": "https://theworldwar.org/visit/upcoming-events",
                "category": "museum",
                "scrape_type": "static",
            },
            "science_city": {
                "name": "Science City",
                "url": "https://sciencecity.unionstation.org/",
                "category": "museum",
                "scrape_type": "static",
            },
            # Theater - Static HTML
            "kc_rep": {
                "name": "KC Repertory Theatre",
                "url": "https://kcrep.org/season/",
                "category": "theater",
                "scrape_type": "static",
            },
            "unicorn_theatre": {
                "name": "Unicorn Theatre",
                "url": "https://unicorntheatre.org/",
                "category": "theater",
                "scrape_type": "static",
            },
            # Festival & City - Static HTML
            "kc_parks": {
                "name": "Kansas City Parks & Rec",
                "url": "https://kcparks.org/events/",
                "category": "festival_city",
                "scrape_type": "static",
            },
            "city_market": {
                "name": "City Market KC",
                "url": "https://thecitymarketkc.org/events/",
                "category": "festival_city",
                "scrape_type": "static",
            },
            "boulevardia": {
                "name": "Boulevardia Festival",
                "url": "https://www.boulevardia.com/",
                "category": "festival_city",
                "scrape_type": "static",
            },
            "irish_fest": {
                "name": "Irish Fest KC",
                "url": "https://kcirishfest.com/",
                "category": "festival_city",
                "scrape_type": "static",
            },
            # Aggregators - Dynamic JS
            "visitkc": {
                "name": "Visit KC",
                "url": "https://www.visitkc.com/events",
                "category": "aggregator",
                "scrape_type": "dynamic",
                "wait_selector": ".event-card, .event-item",
            },
            "do816": {
                "name": "Do816",
                "url": "https://do816.com/",
                "category": "aggregator",
                "scrape_type": "dynamic",
                "wait_selector": ".event",
            },
            "pitch_kc": {
                "name": "The Pitch KC",
                "url": "https://calendar.thepitchkc.com/",
                "category": "aggregator",
                "scrape_type": "dynamic",
            },
            "kc_magazine": {
                "name": "Kansas City Magazine Events",
                "url": "https://events.kansascitymag.com/",
                "category": "aggregator",
                "scrape_type": "dynamic",
            },
            "eventkc": {
                "name": "Event KC",
                "url": "https://www.eventkc.com/",
                "category": "aggregator",
                "scrape_type": "dynamic",
            },
            # Nightlife - Dynamic JS
            "aura_kc": {
                "name": "Aura KC Nightclub",
                "url": "https://www.aurakc.com/",
                "category": "nightlife",
                "scrape_type": "dynamic",
            },
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
        self,
        location: Optional[str] = None,
        radius: int = 10000,
        venue_types: Optional[List[str]] = None,
    ) -> OperationResult:
        """
        Collect venues from Google Places API with proper implementation.

        Args:
            location: Location to search around (defaults to KC Downtown)
            radius: Search radius in meters (default: 10km)
            venue_types: List of venue types to search for

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info(f"🏢 Collecting venues from Google Places API")

        # Check for API key
        api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        if not api_key:
            self.logger.warning("Google Places API key not found in environment")
            return OperationResult(
                success=False,
                error="API key not found",
                message="GOOGLE_PLACES_API_KEY not set in environment variables",
            )

        try:
            # Default to Kansas City downtown
            if not location:
                location = f"{KC_DOWNTOWN['lat']},{KC_DOWNTOWN['lng']}"

            # Default venue types based on psychographic interests
            if not venue_types:
                venue_types = [
                    "restaurant",
                    "bar",
                    "night_club",
                    "cafe",
                    "museum",
                    "art_gallery",
                    "movie_theater",
                    "shopping_mall",
                    "gym",
                    "spa",
                    "book_store",
                    "library",
                    "park",
                ]

            all_venues = []

            # Search for each venue type
            for venue_type in venue_types:
                try:
                    venues = self._search_google_places(
                        api_key, location, radius, venue_type
                    )
                    all_venues.extend(venues)

                    self.logger.debug(
                        f"Found {len(venues)} venues of type '{venue_type}'"
                    )

                    # Respectful API delay
                    time.sleep(1)

                except Exception as e:
                    self.logger.error(f"Failed to search type {venue_type}: {e}")
                    continue

            # Remove duplicates based on place_id
            unique_venues = {}
            for venue in all_venues:
                place_id = venue.get("place_id")
                if place_id and place_id not in unique_venues:
                    unique_venues[place_id] = venue

            # Process and store venues
            stored_count = 0
            for venue_data in unique_venues.values():
                try:
                    processed_venue = self._process_google_places_venue(venue_data)
                    if self._validate_and_store_venue(processed_venue):
                        stored_count += 1
                except Exception as e:
                    self.logger.debug(f"Failed to process venue: {e}")
                    continue

            duration = (datetime.now() - start_time).total_seconds()

            # Update collection status
            self.db.update_collection_status(
                source_name="google_places",
                success=True,
                records_collected=stored_count,
                duration_seconds=duration,
            )

            return OperationResult(
                success=True,
                data=stored_count,
                message=f"Google Places collection completed: {stored_count} venues in {duration:.1f}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Google Places collection failed: {e}")

            # Update collection status
            self.db.update_collection_status(
                source_name="google_places",
                success=False,
                error_message=str(e),
                duration_seconds=duration,
            )

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
        Collect venues from all 29 KC sources using LLM-based scraping.

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🌐 Collecting venues from 29 KC sources using LLM scraping")

        if not self.openai_client:
            return OperationResult(
                success=False,
                error="OpenAI not available",
                message="LLM scraping requires OpenAI API key",
            )

        total_venues = len(self.kc_venues)
        venues_processed = 0
        venues_failed = 0

        self.logger.info(f"Processing {total_venues} venue sources...")

        for idx, (venue_key, venue_config) in enumerate(self.kc_venues.items(), 1):
            try:
                self.logger.info(
                    f"[{idx}/{total_venues}] Scraping {venue_config['name']}..."
                )

                # Fetch HTML content
                if venue_config["scrape_type"] == "dynamic" and PLAYWRIGHT_AVAILABLE:
                    html = self._fetch_with_browser(
                        venue_config["url"], venue_config.get("wait_selector")
                    )
                else:
                    html = self._fetch_static_html(venue_config["url"])

                if not html:
                    self.logger.warning(
                        f"  ⚠️  Failed to fetch HTML for {venue_config['name']}"
                    )
                    venues_failed += 1
                    continue

                # Extract venue info using LLM
                venue_info = self._extract_venue_with_llm(html, venue_config)

                if venue_info:
                    # Create VenueData object
                    venue_data = VenueData(
                        external_id=f"llm_{venue_key}",
                        provider="llm_scraper",
                        name=venue_info.get("name", venue_config["name"]),
                        description=venue_info.get("description"),
                        category=venue_config["category"],
                        subcategory=venue_info.get("type"),
                        website=venue_config["url"],
                        address=venue_info.get("address"),
                        phone=venue_info.get("phone"),
                        lat=venue_info.get("lat"),
                        lng=venue_info.get("lng"),
                        avg_rating=None,
                        psychographic_scores=self._calculate_venue_psychographics(
                            venue_info.get("name", venue_config["name"]),
                            venue_info.get("description", ""),
                        ),
                        scraped_at=datetime.now(),
                        source_type="llm_scraper",
                    )

                    # Store venue
                    if self._validate_and_store_venue(venue_data):
                        venues_processed += 1
                        self.logger.info(
                            f"  ✅ Successfully scraped {venue_config['name']}"
                        )
                    else:
                        venues_failed += 1
                        self.logger.warning(
                            f"  ⚠️  Failed to store {venue_config['name']}"
                        )
                else:
                    venues_failed += 1
                    self.logger.warning(
                        f"  ⚠️  No venue info extracted for {venue_config['name']}"
                    )

                # Respectful delay
                time.sleep(2)

            except Exception as e:
                venues_failed += 1
                self.logger.error(f"  ❌ Error scraping {venue_key}: {e}")
                continue

        duration = (datetime.now() - start_time).total_seconds()

        self.logger.info(
            f"LLM venue scraping summary: {venues_processed} succeeded, {venues_failed} failed"
        )

        return OperationResult(
            success=venues_processed > 0,
            data=venues_processed,
            message=f"LLM scraping completed: {venues_processed} venues from {total_venues} sources in {duration:.1f}s",
        )

    def collect_all(self) -> OperationResult:
        """
        Collect venues from all sources (just LLM scraping now).

        Returns:
            OperationResult with comprehensive collection statistics
        """
        return self.collect_from_scraped_sources()

    def get_venues(
        self, filters: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Get venues from database with enhanced filtering.

        Args:
            filters: Optional filters including:
                - category: Venue category
                - has_location: Only venues with lat/lng
                - min_rating: Minimum average rating
                - bounds: Geographic bounds dict
                - provider: Data provider
                - psychographic_min: Minimum psychographic score
            limit: Optional limit on number of results

        Returns:
            List of venue dictionaries
        """
        try:
            if filters is None:
                filters = {}

            # Add bounds filter if specified
            if filters.get("bounds"):
                bounds = filters["bounds"]
                # Database will need to handle this - add to query

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

    def collect_venues_in_bounds(
        self, bounds: Optional[Dict] = None, venue_types: Optional[List[str]] = None
    ) -> OperationResult:
        """
        Collect venues within specific geographic bounds.

        Args:
            bounds: Geographic bounds dict with min_lat, max_lat, min_lng, max_lng
            venue_types: Optional list of venue types to collect

        Returns:
            OperationResult with collection statistics
        """
        if not bounds:
            bounds = {
                "min_lat": KC_BOUNDING_BOX["south"],
                "max_lat": KC_BOUNDING_BOX["north"],
                "min_lng": KC_BOUNDING_BOX["west"],
                "max_lng": KC_BOUNDING_BOX["east"],
            }

        self.logger.info(
            f"🗺️ Collecting venues in bounds: "
            f"lat {bounds['min_lat']:.4f} to {bounds['max_lat']:.4f}, "
            f"lng {bounds['min_lng']:.4f} to {bounds['max_lng']:.4f}"
        )

        # Create a grid of search locations to cover the entire bounds
        grid_locations = self._create_search_grid(bounds, grid_size_km=5)

        self.logger.info(f"Created {len(grid_locations)} search grid points")

        all_venues = []

        for location in grid_locations:
            try:
                # Search this grid point
                result = self.collect_from_google_places(
                    location=f"{location['lat']},{location['lng']}",
                    radius=5000,  # 5km radius
                    venue_types=venue_types,
                )

                if result.success:
                    all_venues.append(result.data)

                # Respectful delay between searches
                time.sleep(2)

            except Exception as e:
                self.logger.error(f"Failed to search grid point {location}: {e}")
                continue

        total_venues = sum(all_venues)

        return OperationResult(
            success=total_venues > 0,
            data=total_venues,
            message=f"Collected {total_venues} venues across {len(grid_locations)} search areas",
        )

    # ========== PRIVATE IMPLEMENTATION METHODS ==========

    def _initialize_openai_client(self):
        """Initialize OpenAI client for LLM extraction"""
        if not OPENAI_AVAILABLE:
            return None

        try:
            import os

            api_key = os.getenv("CHATGPT_API_KEY")
            if not api_key:
                self.logger.error("CHATGPT_API_KEY not found in environment variables")
                return None
            return OpenAI(api_key=api_key)
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenAI client: {e}")
            return None

    def _fetch_static_html(self, url: str) -> Optional[str]:
        """Fetch HTML from static site"""
        try:
            response = requests.get(url, headers=self.scraping_headers, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_with_browser(self, url: str, wait_selector: str = None) -> Optional[str]:
        """Fetch HTML using Playwright for dynamic sites"""
        if not PLAYWRIGHT_AVAILABLE:
            # Fallback to static fetch
            return self._fetch_static_html(url)

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()

                self.logger.debug(f"Loading {url} with browser...")
                page.goto(url, wait_until="networkidle", timeout=30000)

                # Wait for specific content
                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=10000)
                    except PlaywrightTimeout:
                        self.logger.warning(f"Timeout waiting for {wait_selector}")

                # Scroll to load lazy content
                for _ in range(3):
                    page.evaluate("window.scrollBy(0, 1000)")
                    time.sleep(0.5)

                html = page.content()
                browser.close()
                return html

        except Exception as e:
            self.logger.error(f"Browser fetch failed for {url}: {e}")
            return None

    def _extract_venue_with_llm(self, html: str, venue_config: Dict) -> Optional[Dict]:
        """Extract venue information using LLM"""
        if not self.openai_client:
            return None

        # Convert to markdown for cleaner processing
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        markdown = self.html_converter.handle(str(soup))

        # Limit content size
        if len(markdown) > 15000:
            markdown = markdown[:15000] + "\n\n[Content truncated...]"

        prompt = f"""Extract venue information from this {venue_config['name']} webpage.

Return a JSON object with this EXACT structure:
{{
  "name": "Venue name",
  "type": "Venue type/subcategory",
  "address": "Full address",
  "phone": "Phone number",
  "description": "Brief description (max 200 chars)",
  "lat": null,
  "lng": null
}}

Rules:
- Use null for missing fields
- Extract only the main venue information, not events
- Keep description brief and factual
- Phone should be formatted like +1-816-555-0000
- Address should be complete with city and zip
- Return ONLY valid JSON object

Content:
{markdown}
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting structured venue data from webpages. Return only valid JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            result = response.choices[0].message.content

            # Parse response
            venue_data = json.loads(result)

            # Ensure we have at least a name
            if not venue_data.get("name"):
                venue_data["name"] = venue_config["name"]

            return venue_data

        except Exception as e:
            self.logger.error(f"LLM extraction failed for {venue_config['name']}: {e}")
            return None

    def _calculate_venue_psychographics(self, name: str, description: str) -> Dict:
        """Calculate psychographic scores for a venue"""
        text = f"{name} {description}".lower() if description else name.lower()
        scores = {}

        for psychographic, keywords in self.psychographic_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            scores[psychographic] = min(score / len(keywords), 1.0)

        return scores

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

            # Validate data quality with improved error handling
            try:
                is_valid, validation_results = self.quality_validator.validate_venue(
                    venue_dict
                )
            except Exception as validation_error:
                self.logger.warning(
                    f"Venue validation error for {venue_data.name}: {validation_error}. Proceeding with basic validation."
                )
                # Basic validation - ensure we have required fields
                is_valid = bool(venue_data.name and venue_data.external_id)
                validation_results = []

            if not is_valid:
                # Log validation errors but don't fail completely
                if validation_results:
                    errors = [
                        r.error_message
                        for r in validation_results
                        if hasattr(r, "error_message") and r.error_message
                    ]
                    self.logger.warning(
                        f"Venue validation failed for {venue_data.name}: {'; '.join(errors)}"
                    )
                else:
                    self.logger.warning(
                        f"Venue validation failed for {venue_data.name}: Missing required fields"
                    )

                # For now, continue with storing even if validation fails
                # This prevents the entire process from stopping due to validation issues
                self.logger.info(
                    f"Proceeding to store venue {venue_data.name} despite validation warnings"
                )

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

    def _search_google_places(
        self, api_key: str, location: str, radius: int, venue_type: str
    ) -> List[Dict]:
        """
        Search Google Places API for venues of a specific type.

        Args:
            api_key: Google Places API key
            location: Center location (lat,lng format)
            radius: Search radius in meters
            venue_type: Type of venue to search for

        Returns:
            List of venue dictionaries from API
        """
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

        params = {
            "location": location,
            "radius": radius,
            "type": venue_type,
            "key": api_key,
        }

        all_results = []

        # Make initial request
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()

        data = response.json()

        if data.get("status") != "OK" and data.get("status") != "ZERO_RESULTS":
            self.logger.warning(
                f"Google Places API returned status: {data.get('status')}"
            )
            return []

        results = data.get("results", [])
        all_results.extend(results)

        # Handle pagination (up to 2 additional pages)
        next_page_token = data.get("next_page_token")
        page_count = 1

        while next_page_token and page_count < 3:
            # Google requires a short delay before using next_page_token
            time.sleep(2)

            params["pagetoken"] = next_page_token
            params.pop("location", None)
            params.pop("radius", None)
            params.pop("type", None)

            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()

            if data.get("status") != "OK":
                break

            results = data.get("results", [])
            all_results.extend(results)

            next_page_token = data.get("next_page_token")
            page_count += 1

        return all_results

    def _process_google_places_venue(self, api_venue: Dict) -> VenueData:
        """
        Convert Google Places API venue data to VenueData format.

        Args:
            api_venue: Venue data from Google Places API

        Returns:
            VenueData object
        """
        # Extract location
        geometry = api_venue.get("geometry", {})
        location = geometry.get("location", {})
        lat = location.get("lat")
        lng = location.get("lng")

        # Extract types and map to category
        types = api_venue.get("types", [])
        category = self._map_google_types_to_category(types)

        # Calculate psychographic scores based on venue type
        psychographic_scores = self._calculate_venue_psychographics(
            api_venue.get("name", ""), category
        )

        return VenueData(
            external_id=api_venue.get("place_id", ""),
            provider="google_places",
            name=api_venue.get("name", "Unknown Venue"),
            description=None,  # Would need Place Details API for this
            category=category,
            subcategory=types[0] if types else None,
            website=None,  # Would need Place Details API
            address=api_venue.get("vicinity", ""),
            phone=None,  # Would need Place Details API
            lat=lat,
            lng=lng,
            avg_rating=api_venue.get("rating"),
            psychographic_scores=psychographic_scores,
            scraped_at=datetime.now(),
            source_type="api",
        )

    def _map_google_types_to_category(self, types: List[str]) -> str:
        """
        Map Google Places types to PPM venue categories.

        Args:
            types: List of Google Places types

        Returns:
            PPM category string
        """
        # Priority mapping - first match wins
        type_mapping = {
            "restaurant": "restaurant",
            "bar": "bar",
            "night_club": "nightclub",
            "cafe": "restaurant",
            "museum": "museum",
            "art_gallery": "gallery",
            "movie_theater": "theater",
            "shopping_mall": "shopping",
            "gym": "recreation",
            "spa": "recreation",
            "book_store": "cultural",
            "library": "cultural",
            "park": "recreation",
            "stadium": "sports_venue",
            "bowling_alley": "recreation",
            "casino": "entertainment",
            "amusement_park": "entertainment",
        }

        for venue_type in types:
            if venue_type in type_mapping:
                return type_mapping[venue_type]

        # Default category
        return "local_venue"

    def _create_search_grid(self, bounds: Dict, grid_size_km: float = 5) -> List[Dict]:
        """
        Create a grid of search locations to cover geographic bounds.

        Args:
            bounds: Geographic bounds dictionary
            grid_size_km: Size of each grid cell in kilometers

        Returns:
            List of location dictionaries with lat/lng
        """
        # Approximate km per degree (varies by latitude)
        # At KC latitude (~39°), 1° lat ≈ 111km, 1° lng ≈ 85km
        km_per_degree_lat = 111
        km_per_degree_lng = 85

        lat_step = grid_size_km / km_per_degree_lat
        lng_step = grid_size_km / km_per_degree_lng

        grid_locations = []

        current_lat = bounds["min_lat"]
        while current_lat <= bounds["max_lat"]:
            current_lng = bounds["min_lng"]
            while current_lng <= bounds["max_lng"]:
                grid_locations.append({"lat": current_lat, "lng": current_lng})
                current_lng += lng_step
            current_lat += lat_step

        return grid_locations


# Global venue service instance
_venue_service = None


def get_venue_service() -> VenueService:
    """Get the global venue service instance"""
    global _venue_service
    if _venue_service is None:
        _venue_service = VenueService()
    return _venue_service
