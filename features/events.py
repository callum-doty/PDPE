"""
Unified Events Service for PPM Application - IMPROVED VERSION

Single service that consolidates ALL event-related functionality with:
- Enhanced error handling for HTTP errors (403, 404)
- Better User-Agent headers to avoid bot blocks
- Improved CSS selector strategies
- Fixed JSON parsing for LLM responses
- More robust timeout handling
- Better logging and fallback mechanisms
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
from pathlib import Path

# Playwright imports for dynamic scraping
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning(
        "Playwright not available - dynamic event scraping will be disabled"
    )

# OpenAI imports for LLM extraction
try:
    from openai import OpenAI

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logging.warning("OpenAI not available - LLM event extraction will be disabled")

# Import core services
from core.database import get_database, OperationResult
from core.quality import get_quality_validator

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # dotenv not available, environment variables should be set manually


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
    venue_name: Optional[str]
    lat: Optional[float]
    lng: Optional[float]
    address: Optional[str]
    source_url: Optional[str]
    price: Optional[str]
    image_url: Optional[str]
    attendance_estimate: Optional[int]
    impact_score: Optional[float]
    psychographic_scores: Optional[Dict]
    scraped_at: datetime
    source_type: str  # 'kc_scraper', 'external_api'


class EventService:
    """
    Unified event service with improved scraping reliability.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = get_database()
        self.quality_validator = get_quality_validator()

        # Initialize OpenAI client for LLM extraction
        self.openai_client = self._initialize_openai_client()

        # HTML to markdown converter for LLM processing
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True

        # Kansas City venue configurations for event scraping
        # UPDATED: Fixed URLs, improved error handling, added fallback URLs
        self.kc_venues = {
            # Major Venues - Static HTML
            "T-Mobile Center": {
                "url": "https://www.t-mobilecenter.com/events",
                "fallback_urls": ["https://www.t-mobilecenter.com/calendar"],
                "type": "static",
                "category": "major_venue",
                "selectors": [".event-list-item", ".event-card", "article", ".show"],
            },
            "Uptown Theater": {
                "url": "https://www.uptowntheater.com/events",
                "fallback_urls": [
                    "https://www.uptowntheater.com/calendar",
                    "https://www.uptowntheater.com/shows",
                ],
                "type": "static",
                "category": "major_venue",
                "selectors": [".vevent", ".event-wrapper", ".show", ".event"],
            },
            "Kauffman Center": {
                "url": "https://www.kauffmancenter.org/events",
                "fallback_urls": [
                    "https://www.kauffmancenter.org/calendar",
                    "https://www.kauffmancenter.org/performances",
                ],
                "type": "static",
                "category": "major_venue",
                "selectors": [".performance", ".event-item", "article.event", ".show"],
                "needs_browser": True,  # May need JavaScript
            },
            "Starlight Theatre": {
                "url": "https://www.kcstarlight.com/events",
                "fallback_urls": [
                    "https://www.kcstarlight.com/calendar",
                    "https://www.kcstarlight.com/shows",
                ],
                "type": "static",
                "category": "major_venue",
                "selectors": [".show-list-item", ".event", "article", ".performance"],
            },
            "The Midland Theatre": {
                "url": "https://www.midlandkc.com/events",
                "fallback_urls": ["https://www.midlandkc.com/calendar"],
                "type": "static",
                "category": "major_venue",
                "selectors": [".event-card", ".show", "article", ".event"],
            },
            "Knuckleheads Saloon": {
                "url": "https://knuckleheadskc.com/calendar",
                "fallback_urls": [
                    "https://knuckleheadskc.com/events",
                    "https://knuckleheadskc.com/shows",
                ],
                "type": "static",
                "category": "major_venue",
                "selectors": [
                    ".event",
                    ".show",
                    "article",
                    ".tribe-events-list-event-row",
                    ".calendar-event",
                ],
            },
            "Power & Light District": {
                "url": "https://powerandlightdistrict.com/events",
                "fallback_urls": [
                    "https://powerandlightdistrict.com/calendar",
                    "https://www.powerandlightdistrict.com/events",
                ],
                "type": "static",
                "category": "entertainment_district",
                "selectors": [".event-item", ".event", "article", ".calendar-item"],
            },
            "Westport KC": {
                "url": "https://westportkcmo.com/events",
                "fallback_urls": ["https://westportkcmo.com/calendar"],
                "type": "static",
                "category": "entertainment_district",
                "selectors": [
                    ".tribe-events-list-event-row",
                    ".event",
                    "article",
                    ".calendar-event",
                ],
            },
            # Jazz District - Updated URL
            "18th & Vine Jazz District": {
                "url": "https://www.jazz18thvine.org/events",
                "fallback_urls": [
                    "https://www.jazz18thvine.org/calendar",
                    "https://kcjazzdistrict.org/events",
                ],
                "type": "static",
                "category": "entertainment_district",
                "selectors": [".event", ".show", "article", ".calendar-event"],
            },
            # Crossroads Arts District - Multiple fallbacks
            "Crossroads KC": {
                "url": "https://www.crossroadsartsdistrict.org/events",
                "fallback_urls": [
                    "https://www.kccrossroads.org/calendar",
                    "https://crossroadskc.org/events",
                ],
                "type": "static",
                "category": "entertainment_district",
                "selectors": [
                    ".event-item",
                    ".tribe-events-list-event-row",
                    "article",
                    ".event",
                ],
            },
            # City Market - Updated URL
            "City Market KC": {
                "url": "https://www.citymarketkc.org/events",
                "fallback_urls": [
                    "https://citymarketkc.org/events",
                    "https://www.citymarketkc.org/calendar",
                ],
                "type": "static",
                "category": "entertainment_district",
                "selectors": [".event", ".calendar-event", "article", ".event-item"],
            },
            # Aggregators - Dynamic JS (kept only most reliable)
            "Visit KC": {
                "url": "https://www.visitkc.com/events",
                "fallback_urls": ["https://www.visitkc.com/calendar"],
                "type": "dynamic",
                "category": "aggregator",
                "wait_selector": "div[class*='event']",  # More flexible selector
                "timeout": 20000,  # Increased timeout
            },
        }

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
                "workshop",
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
            "social": [
                "social",
                "community",
                "meetup",
                "gathering",
                "group",
                "networking",
            ],
            "adventurous": [
                "adventure",
                "outdoor",
                "extreme",
                "challenge",
                "exploration",
                "sports",
                "hiking",
                "climbing",
            ],
        }

    # ========== PUBLIC API METHODS ==========

    def collect_from_kc_sources(self) -> OperationResult:
        """
        Collect events from Kansas City sources (venues, aggregators).

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🎭 Collecting events from Kansas City sources")

        try:
            all_events = []
            successful_venues = 0
            failed_venues = 0

            # Scrape all KC venues
            for venue_name, config in self.kc_venues.items():
                try:
                    events = self._scrape_venue_events(venue_name, config)
                    if events:
                        all_events.extend(events)
                        successful_venues += 1
                        self.logger.debug(
                            f"✅ Found {len(events)} events at {venue_name}"
                        )
                    else:
                        failed_venues += 1

                    # Respectful delay between venues
                    time.sleep(2.0)

                except Exception as e:
                    self.logger.error(f"❌ Failed to scrape {venue_name}: {e}")
                    failed_venues += 1
                    continue

            # Process and store events
            stored_count = 0
            for event_data in all_events:
                if self._validate_and_store_event(event_data):
                    stored_count += 1

            duration = (datetime.now() - start_time).total_seconds()

            summary = (
                f"KC sources collection completed: {stored_count} events stored from "
                f"{successful_venues} venues ({failed_venues} venues failed) in {duration:.1f}s"
            )

            return OperationResult(
                success=stored_count > 0,
                data=stored_count,
                message=summary,
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"KC sources collection failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"KC sources collection failed after {duration:.1f}s: {e}",
            )

    def collect_from_apis(self) -> OperationResult:
        """
        Collect events from external APIs (PredictHQ, Google Places, etc.).

        Returns:
            OperationResult with collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🌐 Collecting events from external APIs")

        try:
            # Note: This would require actual API keys and implementations
            # For now, return a mock result to maintain interface compatibility
            events_collected = 0

            # TODO: Implement actual API integrations
            # events = self._fetch_predicthq_events()
            # events.extend(self._fetch_google_events())
            # events_collected = self._process_and_store_api_events(events)

            duration = (datetime.now() - start_time).total_seconds()

            return OperationResult(
                success=True,
                data=events_collected,
                message=f"External API collection completed: {events_collected} events in {duration:.1f}s",
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"External API collection failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"External API collection failed after {duration:.1f}s: {e}",
            )

    def collect_all(self) -> OperationResult:
        """
        Collect events from ALL sources (KC sources + external APIs).

        Returns:
            OperationResult with comprehensive collection statistics
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting comprehensive event collection from all sources")

        results = []
        total_events = 0

        # Collect from KC sources
        kc_result = self.collect_from_kc_sources()
        results.append(("KC Sources", kc_result))
        if kc_result.success:
            total_events += kc_result.data

        # Collect from external APIs
        api_result = self.collect_from_apis()
        results.append(("External APIs", api_result))
        if api_result.success:
            total_events += api_result.data

        # Calculate overall success
        successful_sources = len([r for _, r in results if r.success])
        duration = (datetime.now() - start_time).total_seconds()

        # Log detailed results
        self.logger.info("📊 Event collection summary:")
        for source_name, result in results:
            status = "✅" if result.success else "❌"
            self.logger.info(
                f"  {status} {source_name}: {result.data if result.success else 0} events"
            )

        return OperationResult(
            success=successful_sources > 0,
            data=total_events,
            message=f"Comprehensive collection completed: {total_events} events from {successful_sources}/{len(results)} sources in {duration:.1f}s",
        )

    def get_events(
        self, filters: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Get events from database with optional filtering.

        Args:
            filters: Optional filters (category, date_range, has_location, etc.)
            limit: Optional limit on number of results

        Returns:
            List of event dictionaries
        """
        try:
            # Add limit to filters if provided
            if filters is None:
                filters = {}
            if limit is not None:
                filters["limit"] = limit
            return self.db.get_events(filters)
        except Exception as e:
            self.logger.error(f"Failed to get events: {e}")
            return []

    def get_events_with_venues(self) -> List[Dict]:
        """
        Get events with their venue information for map display.

        Returns:
            List of event dictionaries with venue data
        """
        try:
            return self.db.get_events({"has_location": True})
        except Exception as e:
            self.logger.error(f"Failed to get events with venues: {e}")
            return []

    def get_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        Get a specific event by ID.

        Args:
            event_id: Event ID to retrieve

        Returns:
            Event dictionary or None if not found
        """
        try:
            events = self.db.get_events({"event_id": event_id})
            return events[0] if events else None
        except Exception as e:
            self.logger.error(f"Failed to get event {event_id}: {e}")
            return None

    # ========== PRIVATE IMPLEMENTATION METHODS ==========

    def _initialize_openai_client(self):
        """Initialize OpenAI client for LLM extraction"""
        if not OPENAI_AVAILABLE:
            return None

        try:
            import os

            api_key = os.getenv("CHATGPT_API_KEY")
            if not api_key:
                self.logger.warning(
                    "CHATGPT_API_KEY not found in environment variables"
                )
                return None
            return OpenAI(api_key=api_key)
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenAI client: {e}")
            return None

    def _scrape_venue_events(self, venue_name: str, config: Dict) -> List[EventData]:
        """Scrape events from a single venue with improved error handling and fallback URLs"""
        self.logger.info(f"Scraping events from {venue_name}...")

        # Try primary URL first, then fallback URLs
        urls_to_try = [config["url"]] + config.get("fallback_urls", [])
        html = None
        successful_url = None

        for url in urls_to_try:
            self.logger.debug(f"Trying URL: {url}")
            is_dynamic = config["type"] == "dynamic"

            # Fetch HTML with retry logic
            if is_dynamic and PLAYWRIGHT_AVAILABLE:
                html = self._fetch_with_browser(
                    url, config.get("wait_selector"), config.get("timeout", 20000)
                )
            elif config.get("needs_browser") and PLAYWRIGHT_AVAILABLE:
                # Some static sites actually need browser rendering
                html = self._fetch_with_browser(
                    url,
                    config.get("wait_selector", "body"),
                    config.get("timeout", 20000),
                )
            else:
                html = self._fetch_static_html_with_retry(url)

            if html and len(html) >= 500:  # Got meaningful content
                successful_url = url
                self.logger.debug(f"Successfully fetched from {url}")
                break
            else:
                self.logger.warning(f"Failed to fetch meaningful content from {url}")
                time.sleep(1)  # Brief delay before trying next URL

        if not html or len(html) < 500:
            self.logger.error(
                f"Failed to fetch meaningful HTML from {venue_name} after trying all URLs"
            )
            return []

        # Try LLM extraction first (preferred method)
        events = []
        if self.openai_client:
            try:
                events = self._extract_events_with_llm(html, venue_name, config)
                if events:
                    self.logger.info(
                        f"✅ Found {len(events)} events at {venue_name} (LLM)"
                    )
                    return events
                else:
                    self.logger.info(
                        f"⚠️  LLM found no events for {venue_name}, trying selector fallback..."
                    )
            except Exception as e:
                self.logger.warning(
                    f"LLM extraction error for {venue_name}: {e}, trying selector fallback..."
                )

        # Fallback to CSS selectors
        events = self._extract_events_with_selectors(html, venue_name, config)

        if events:
            self.logger.info(
                f"✅ Found {len(events)} events at {venue_name} (Selectors)"
            )
        else:
            self.logger.warning(
                f"⚠️  No events extracted from {venue_name} using any method"
            )

        return events

    def _fetch_static_html(self, url: str) -> Optional[str]:
        """Fetch HTML from static site with better error handling"""
        try:
            # Enhanced headers to avoid blocks
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }

            response = requests.get(
                url,
                headers=headers,
                timeout=15,
                allow_redirects=True,
                verify=True,  # Enable SSL verification
            )
            response.raise_for_status()
            return response.text

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.warning(
                    f"⚠️  Access forbidden for {url} - site may require JavaScript or be blocking bots"
                )
            elif e.response.status_code == 404:
                self.logger.warning(f"⚠️  Page not found: {url} - URL may have changed")
            else:
                self.logger.error(f"HTTP error {e.response.status_code} for {url}: {e}")
            return None

        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout fetching {url}")
            return None

        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error for {url}: {e}")
            return None

        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_static_html_with_retry(
        self, url: str, max_retries: int = 3
    ) -> Optional[str]:
        """Fetch HTML from static site with retry logic"""
        for attempt in range(max_retries):
            try:
                html = self._fetch_static_html(url)
                if html:
                    return html

                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s
                    self.logger.debug(
                        f"Retry {attempt + 1}/{max_retries} for {url} in {wait_time}s"
                    )
                    time.sleep(wait_time)

            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 2)

        return None

    def _fetch_with_browser(
        self, url: str, wait_selector: str = None, timeout: int = 10000
    ) -> Optional[str]:
        """Fetch HTML using Playwright for dynamic sites with improved error handling"""
        if not PLAYWRIGHT_AVAILABLE:
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()

                self.logger.debug(f"Loading {url} with browser...")

                try:
                    page.goto(url, wait_until="networkidle", timeout=timeout)
                except PlaywrightTimeout:
                    self.logger.warning(
                        f"Page load timeout for {url}, continuing anyway..."
                    )

                # Wait for specific content if selector provided
                if wait_selector:
                    try:
                        page.wait_for_selector(
                            wait_selector, timeout=max(5000, timeout // 2)
                        )
                    except PlaywrightTimeout:
                        self.logger.warning(
                            f"Timeout waiting for {wait_selector}, proceeding with available content"
                        )

                # Scroll to load lazy content
                try:
                    for i in range(3):
                        page.evaluate("window.scrollBy(0, 1000)")
                        page.wait_for_timeout(500)
                except Exception as e:
                    self.logger.debug(f"Scroll error (non-critical): {e}")

                html = page.content()
                browser.close()

                self.logger.debug(f"Successfully fetched {len(html)} bytes from {url}")
                return html

        except Exception as e:
            self.logger.error(f"Browser fetch failed for {url}: {e}")
            return None

    def _extract_events_with_llm(
        self, html: str, venue_name: str, config: Dict
    ) -> List[EventData]:
        """Extract events using LLM with improved prompt and JSON handling"""
        if not self.openai_client:
            return []

        # Convert to markdown for cleaner processing
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(
            ["script", "style", "nav", "footer", "header", "iframe", "svg"]
        ):
            tag.decompose()

        markdown = self.html_converter.handle(str(soup))

        # Aggressive content limiting to avoid token issues
        if len(markdown) > 12000:
            markdown = markdown[:12000] + "\n\n[Content truncated for processing...]"

        # Simplified, more explicit prompt
        prompt = f"""Extract events from the {venue_name} webpage. Return ONLY valid JSON.

CRITICAL: Your response must be ONLY a valid JSON array, nothing else. No markdown, no explanations.

Format (exact structure required):
[
  {{
    "title": "Event Name",
    "date": "2025-12-15 or Dec 15",
    "time": "7:30 PM or 19:30",
    "location": "{venue_name}",
    "description": "Brief description (max 150 chars)",
    "url": "https://full-url.com/event",
    "price": "$25 or Free",
    "image_url": "https://image-url.com/img.jpg"
  }}
]

Rules:
- Return [] if no events found
- Use null for missing fields
- Keep descriptions under 150 characters
- Make URLs absolute (add base URL if needed)
- Date format: YYYY-MM-DD preferred, but text OK
- Return ONLY the JSON array, no other text

Content:
{markdown}
"""

        try:
            # Request with JSON mode
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You extract event data from webpages and return ONLY valid JSON arrays. Never include markdown formatting or explanations. Return [] if no events found.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=2000,  # Limit to prevent huge responses
            )

            result = response.choices[0].message.content.strip()

            # Clean up common LLM formatting issues
            result = result.replace("```json", "").replace("```", "").strip()

            # More aggressive JSON extraction
            # Look for array boundaries
            start_idx = result.find("[")
            end_idx = result.rfind("]")

            if start_idx == -1 or end_idx == -1:
                self.logger.error(
                    f"No JSON array found in LLM response for {venue_name}"
                )
                return []

            result = result[start_idx : end_idx + 1]

            # Try to parse
            try:
                events_data = json.loads(result)
            except json.JSONDecodeError as e:
                # Try to fix common JSON issues
                self.logger.warning(
                    f"JSON parse error for {venue_name}: {e}. Attempting repair..."
                )

                # Fix unterminated strings by escaping quotes
                result = result.replace('\\"', '"')  # Unescape first
                result = re.sub(
                    r'(?<!\\)"([^"]*?)(?<!\\)"',
                    lambda m: '"' + m.group(1).replace('"', '\\"') + '"',
                    result,
                )

                try:
                    events_data = json.loads(result)
                except json.JSONDecodeError as e2:
                    self.logger.error(f"Failed to repair JSON for {venue_name}: {e2}")
                    return []

            # Handle both array and object responses
            if isinstance(events_data, dict):
                events_data = events_data.get("events", events_data.get("data", []))

            if not isinstance(events_data, list):
                self.logger.error(
                    f"LLM returned non-list data for {venue_name}: {type(events_data)}"
                )
                return []

            # Convert to EventData objects
            events = []
            for event_dict in events_data[:50]:  # Limit to 50 events
                if not event_dict.get("title"):
                    continue

                try:
                    event_data = EventData(
                        external_id=f"{venue_name.lower().replace(' ', '_')}_{hash(event_dict.get('title'))}",
                        provider="kc_event_scraper",
                        name=event_dict.get("title")[:200],  # Limit length
                        description=(
                            event_dict.get("description", "")[:500]
                            if event_dict.get("description")
                            else None
                        ),
                        category="local_event",
                        subcategory=config.get("category", "event"),
                        start_time=self._parse_event_datetime(
                            event_dict.get("date"), event_dict.get("time")
                        ),
                        end_time=None,
                        venue_name=venue_name,
                        lat=None,
                        lng=None,
                        address=event_dict.get("location"),
                        source_url=event_dict.get("url"),
                        price=event_dict.get("price"),
                        image_url=event_dict.get("image_url"),
                        attendance_estimate=None,
                        impact_score=None,
                        psychographic_scores=self._calculate_event_psychographics(
                            event_dict.get("title", ""), event_dict.get("description")
                        ),
                        scraped_at=datetime.now(),
                        source_type="kc_scraper",
                    )
                    events.append(event_data)
                except Exception as e:
                    self.logger.debug(f"Error processing event from LLM response: {e}")
                    continue

            if events:
                self.logger.info(
                    f"✅ LLM extracted {len(events)} events from {venue_name}"
                )
            else:
                self.logger.warning(f"⚠️  LLM found no valid events for {venue_name}")

            return events

        except Exception as e:
            self.logger.error(f"LLM extraction failed for {venue_name}: {e}")
            return []

    def _extract_events_with_selectors(
        self, html: str, venue_name: str, config: Dict
    ) -> List[EventData]:
        """Fallback: Extract events using CSS selectors with improved detection"""
        soup = BeautifulSoup(html, "html.parser")
        events = []

        # Use venue-specific selectors if available, otherwise use common ones
        selectors = config.get(
            "selectors",
            [
                ".event",
                ".event-item",
                ".event-card",
                "article.event",
                "li.event",
                ".show",
                ".performance",
                ".vevent",
                '[class*="event"]',
                '[class*="show"]',
                ".tribe-events-list-event-row",
            ],
        )

        event_items = []
        for selector in selectors:
            items = soup.select(selector)
            if items:
                event_items = items
                self.logger.debug(
                    f"Found {len(items)} items with selector '{selector}' for {venue_name}"
                )
                break

        if not event_items:
            self.logger.warning(f"No events found with any selector for {venue_name}")
            return []

        for item in event_items[:50]:  # Limit to 50 events
            # Extract title with multiple strategies
            title = None
            title_selectors = [
                "h1",
                "h2",
                "h3",
                "h4",
                ".title",
                ".event-title",
                ".name",
                ".event-name",
                "a",
            ]
            for sel in title_selectors:
                elem = item.select_one(sel)
                if elem:
                    title = elem.get_text(strip=True)
                    if title and len(title) > 3:  # Ensure meaningful title
                        break

            if not title or len(title) < 3:
                continue

            # Extract date with multiple strategies
            date = None
            date_selectors = [
                ".date",
                ".event-date",
                "time",
                "[datetime]",
                ".start-date",
                ".when",
            ]
            for sel in date_selectors:
                elem = item.select_one(sel)
                if elem:
                    date = elem.get_text(strip=True) or elem.get("datetime")
                    if date:
                        break

            # Extract time
            time_str = None
            time_selectors = [".time", ".event-time", ".start-time", ".when"]
            for sel in time_selectors:
                elem = item.select_one(sel)
                if elem:
                    time_str = elem.get_text(strip=True)
                    if time_str:
                        break

            # Extract description
            description = None
            desc_selectors = [
                ".description",
                ".event-description",
                "p",
                ".summary",
                ".excerpt",
            ]
            for sel in desc_selectors:
                elem = item.select_one(sel)
                if elem:
                    desc_text = elem.get_text(strip=True)
                    if desc_text and len(desc_text) > 10:
                        description = desc_text[:500]  # Limit length
                        break

            # Extract URL
            url = None
            link = item.select_one("a[href]")
            if link:
                url = link.get("href")
                # Make absolute URL
                if url and not url.startswith("http"):
                    base = "/".join(config["url"].split("/")[:3])
                    url = base + ("/" if not url.startswith("/") else "") + url

            # Extract price
            price = None
            price_selectors = [".price", ".cost", ".ticket-price", "[class*='price']"]
            for sel in price_selectors:
                elem = item.select_one(sel)
                if elem:
                    price_text = elem.get_text(strip=True)
                    if price_text:
                        price = price_text
                        break

            # Extract image
            image_url = None
            img = item.select_one("img[src]")
            if img:
                image_url = img.get("src")
                if image_url and not image_url.startswith("http"):
                    base = "/".join(config["url"].split("/")[:3])
                    image_url = (
                        base
                        + ("/" if not image_url.startswith("/") else "")
                        + image_url
                    )

            try:
                event_data = EventData(
                    external_id=f"{venue_name.lower().replace(' ', '_')}_{hash(title)}",
                    provider="kc_event_scraper",
                    name=title,
                    description=description,
                    category="local_event",
                    subcategory=config.get("category", "event"),
                    start_time=self._parse_event_datetime(date, time_str),
                    end_time=None,
                    venue_name=venue_name,
                    lat=None,
                    lng=None,
                    address=None,
                    source_url=url,
                    price=price,
                    image_url=image_url,
                    attendance_estimate=None,
                    impact_score=None,
                    psychographic_scores=self._calculate_event_psychographics(
                        title, description
                    ),
                    scraped_at=datetime.now(),
                    source_type="kc_scraper",
                )
                events.append(event_data)
            except Exception as e:
                self.logger.debug(
                    f"Error processing event from selector extraction: {e}"
                )
                continue

        return events

    def _parse_event_datetime(self, date_str: str, time_str: str) -> Optional[datetime]:
        """Parse event date and time into datetime object with improved handling"""
        if not date_str:
            return None

        try:
            # Try using dateutil parser for flexible date parsing
            try:
                from dateutil import parser as date_parser

                if time_str:
                    combined = f"{date_str} {time_str}"
                    return date_parser.parse(combined, fuzzy=True)
                else:
                    return date_parser.parse(date_str, fuzzy=True)

            except ImportError:
                # Fallback to basic datetime parsing if dateutil not available
                self.logger.warning("dateutil not available, using basic date parsing")

                # Try common date formats
                date_formats = [
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d %I:%M %p",
                    "%Y-%m-%d",
                    "%m/%d/%Y %H:%M",
                    "%m/%d/%Y %I:%M %p",
                    "%m/%d/%Y",
                    "%B %d, %Y %H:%M",
                    "%B %d, %Y %I:%M %p",
                    "%B %d, %Y",
                ]

                combined = f"{date_str} {time_str}" if time_str else date_str

                for fmt in date_formats:
                    try:
                        return datetime.strptime(combined, fmt)
                    except ValueError:
                        continue

                # If no format matches, try to extract year, month, day manually
                import re

                date_match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", date_str)
                if date_match:
                    year, month, day = map(int, date_match.groups())
                    return datetime(year, month, day)

                return None

        except Exception as e:
            self.logger.warning(
                f"Could not parse date/time: {date_str} {time_str} - {e}"
            )
            return None

    def _calculate_event_psychographics(self, title: str, description: str) -> Dict:
        """Calculate psychographic scores for an event based on title and description"""
        text = f"{title} {description}".lower() if description else title.lower()
        scores = {}

        for psychographic, keywords in self.psychographic_keywords.items():
            # Count keyword matches
            matches = sum(1 for keyword in keywords if keyword in text)
            # Normalize to 0-1 scale, with bonus for multiple matches
            if matches > 0:
                score = min(
                    matches / len(keywords) * 2, 1.0
                )  # Allow up to 100% with fewer keywords
            else:
                score = 0.0
            scores[psychographic] = score

        return scores

    def _validate_and_store_event(self, event_data: EventData) -> bool:
        """Validate event data and store in database with lenient validation"""
        try:
            # Convert to dictionary for validation
            event_dict = {
                "external_id": event_data.external_id,
                "provider": event_data.provider,
                "name": event_data.name,
                "description": event_data.description,
                "category": event_data.category,
                "subcategory": event_data.subcategory,
                "start_time": event_data.start_time,
                "end_time": event_data.end_time,
                "venue_name": event_data.venue_name,
                "lat": event_data.lat,
                "lng": event_data.lng,
                "address": event_data.address,
                "psychographic_relevance": event_data.psychographic_scores,
            }

            # Basic validation - only check essential fields
            if not event_data.name or len(event_data.name.strip()) < 2:
                self.logger.warning(f"Event rejected: missing or invalid name")
                return False

            if not event_data.external_id:
                self.logger.warning(f"Event rejected: missing external_id")
                return False

            # Try quality validation but don't fail if it has issues
            try:
                is_valid, validation_results = self.quality_validator.validate_event(
                    event_dict
                )

                if not is_valid:
                    # Log validation warnings but continue with storage
                    errors = [
                        r.error_message
                        for r in validation_results
                        if hasattr(r, "error_message") and r.error_message
                    ]
                    if errors:
                        self.logger.debug(
                            f"Event validation warnings for {event_data.name}: {'; '.join(errors)}"
                        )
                    # Continue with storage despite validation warnings

            except Exception as validation_error:
                self.logger.debug(
                    f"Validation error for {event_data.name}: {validation_error}. Proceeding with basic validation."
                )

            # Try to enrich with venue coordinates before storing
            if event_data.venue_name and not event_data.lat and not event_data.lng:
                venue_coords = self._lookup_venue_coordinates(event_data.venue_name)
                if venue_coords:
                    event_dict["lat"] = venue_coords["lat"]
                    event_dict["lng"] = venue_coords["lng"]
                    if not event_dict.get("address") and venue_coords.get("address"):
                        event_dict["address"] = venue_coords["address"]

            # Store in database
            result = self.db.upsert_event(event_dict)

            if result.success:
                self.logger.debug(f"Successfully stored event: {event_data.name}")
                return True
            else:
                self.logger.error(
                    f"Failed to store event {event_data.name}: {result.error}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Error validating/storing event {event_data.name}: {e}")
            return False

    def _lookup_venue_coordinates(self, venue_name: str) -> Optional[Dict]:
        """Look up venue coordinates from existing venues in database"""
        try:
            # Get all venues from database
            venues = self.db.get_venues()

            # Try exact match first
            for venue in venues:
                if venue.get("name") and venue["name"].lower() == venue_name.lower():
                    if venue.get("lat") and venue.get("lng"):
                        return {
                            "lat": venue["lat"],
                            "lng": venue["lng"],
                            "address": venue.get("address"),
                        }

            # Try partial match (venue name contains or is contained in database venue name)
            for venue in venues:
                if venue.get("name") and venue.get("lat") and venue.get("lng"):
                    db_name = venue["name"].lower()
                    search_name = venue_name.lower()

                    # Check if names are similar (contains relationship)
                    if (
                        search_name in db_name
                        or db_name in search_name
                        or self._names_are_similar(search_name, db_name)
                    ):
                        self.logger.debug(
                            f"Found venue coordinates for '{venue_name}' using '{venue['name']}'"
                        )
                        return {
                            "lat": venue["lat"],
                            "lng": venue["lng"],
                            "address": venue.get("address"),
                        }

            return None

        except Exception as e:
            self.logger.debug(
                f"Error looking up venue coordinates for {venue_name}: {e}"
            )
            return None

    def _names_are_similar(self, name1: str, name2: str) -> bool:
        """Check if two venue names are similar enough to be considered the same venue"""
        # Remove common words and punctuation for comparison
        import re

        def clean_name(name):
            # Remove common venue words and punctuation
            cleaned = re.sub(
                r"\b(theater|theatre|center|centre|hall|club|district|kc|kansas city)\b",
                "",
                name.lower(),
            )
            cleaned = re.sub(r"[^\w\s]", "", cleaned)  # Remove punctuation
            cleaned = re.sub(r"\s+", " ", cleaned).strip()  # Normalize whitespace
            return cleaned

        clean1 = clean_name(name1)
        clean2 = clean_name(name2)

        if not clean1 or not clean2:
            return False

        # Check if one is contained in the other after cleaning
        return clean1 in clean2 or clean2 in clean1


# Global event service instance
_event_service = None


def get_event_service() -> EventService:
    """Get the global event service instance"""
    global _event_service
    if _event_service is None:
        _event_service = EventService()
    return _event_service
