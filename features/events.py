"""
Unified Events Service for PPM Application

Single service that consolidates ALL event-related functionality:
- Kansas City event scraping (static and dynamic sites with LLM extraction)
- External API event collection (PredictHQ, Google Places, etc.)
- Event data processing and quality validation
- Database operations

Replaces the entire features/events/ directory structure and scattered event scrapers.
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
    Unified event service that handles ALL event operations.

    Consolidates functionality from:
    - features/venues/scrapers/kc_event_scraper.py (600+ lines)
    - features/events/collectors/external_api_collector.py
    - Event processing and quality validation

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

        # Kansas City venue configurations for event scraping
        self.kc_venues = {
            # Major Venues - Static HTML
            "T-Mobile Center": {
                "url": "https://www.t-mobilecenter.com/events",
                "type": "static",
                "category": "major_venue",
            },
            "Uptown Theater": {
                "url": "https://www.uptowntheater.com/events",
                "type": "static",
                "category": "major_venue",
            },
            "Kauffman Center": {
                "url": "https://www.kauffmancenter.org/events/",
                "type": "static",
                "category": "major_venue",
            },
            "Starlight Theatre": {
                "url": "https://www.kcstarlight.com/events/",
                "type": "static",
                "category": "major_venue",
            },
            "The Midland Theatre": {
                "url": "https://www.midlandkc.com/events",
                "type": "static",
                "category": "major_venue",
            },
            "Knuckleheads Saloon": {
                "url": "https://knuckleheadskc.com/",
                "type": "static",
                "category": "major_venue",
            },
            "Azura Amphitheater": {
                "url": "https://www.azuraamphitheater.com/events",
                "type": "static",
                "category": "major_venue",
            },
            # Entertainment Districts - Static
            "Power & Light District": {
                "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
                "type": "static",
                "category": "entertainment_district",
            },
            "Westport KC": {
                "url": "https://westportkcmo.com/events/",
                "type": "static",
                "category": "entertainment_district",
            },
            "18th & Vine Jazz": {
                "url": "https://kcjazzdistrict.org/events/",
                "type": "static",
                "category": "entertainment_district",
            },
            "Crossroads KC": {
                "url": "https://www.crossroadskc.com/shows",
                "type": "static",
                "category": "entertainment_district",
            },
            # Aggregators - Dynamic JS
            "Visit KC": {
                "url": "https://www.visitkc.com/events",
                "type": "dynamic",
                "category": "aggregator",
                "wait_selector": ".event-card, .event-item",
            },
            "Do816": {
                "url": "https://do816.com/events",
                "type": "dynamic",
                "category": "aggregator",
                "wait_selector": ".event",
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

            # Scrape all KC venues
            for venue_name, config in self.kc_venues.items():
                try:
                    events = self._scrape_venue_events(venue_name, config)
                    all_events.extend(events)
                    self.logger.debug(f"✅ Found {len(events)} events at {venue_name}")

                    # Respectful delay between venues
                    time.sleep(2.0)

                except Exception as e:
                    self.logger.error(f"❌ Failed to scrape {venue_name}: {e}")
                    continue

            # Process and store events
            stored_count = 0
            for event_data in all_events:
                if self._validate_and_store_event(event_data):
                    stored_count += 1

            duration = (datetime.now() - start_time).total_seconds()

            return OperationResult(
                success=True,
                data=stored_count,
                message=f"KC sources collection completed: {stored_count} events from {len(self.kc_venues)} sources in {duration:.1f}s",
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
        """Scrape events from a single venue"""
        self.logger.info(f"Scraping events from {venue_name}...")

        url = config["url"]
        is_dynamic = config["type"] == "dynamic"

        # Fetch HTML
        if is_dynamic and PLAYWRIGHT_AVAILABLE:
            html = self._fetch_with_browser(url, config.get("wait_selector"))
        else:
            html = self._fetch_static_html(url)

        if not html:
            self.logger.error(f"Failed to fetch HTML from {venue_name}")
            return []

        # Extract events using LLM (preferred method)
        if self.openai_client:
            events = self._extract_events_with_llm(html, venue_name, config)
            if events:
                self.logger.info(f"✅ Found {len(events)} events at {venue_name} (LLM)")
                return events

        # Fallback to CSS selectors
        events = self._extract_events_with_selectors(html, venue_name, config)
        self.logger.info(f"✅ Found {len(events)} events at {venue_name} (Selectors)")
        return events

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
            return None

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()

                self.logger.info(f"Loading {url} with browser...")
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

    def _extract_events_with_llm(
        self, html: str, venue_name: str, config: Dict
    ) -> List[EventData]:
        """Extract events using LLM"""
        if not self.openai_client:
            return []

        # Convert to markdown for cleaner processing
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()

        markdown = self.html_converter.handle(str(soup))

        # Limit content size
        if len(markdown) > 15000:
            markdown = markdown[:15000] + "\n\n[Content truncated...]"

        prompt = f"""Extract all events from this {venue_name} webpage.

Return a JSON array with this EXACT structure:
[
  {{
    "title": "Event name",
    "date": "YYYY-MM-DD or text date",
    "time": "HH:MM AM/PM or time text", 
    "location": "Venue/location",
    "description": "Brief description (max 200 chars)",
    "url": "Event detail URL (full URL)",
    "price": "Price if available",
    "image_url": "Image URL if available"
  }}
]

Rules:
- Use null for missing fields
- Extract ALL events on the page
- Dates: prefer ISO format, but text dates OK
- URLs: make absolute (add base URL if needed)
- Return ONLY valid JSON array
- If no events, return []

Content:
{markdown}
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting structured event data from webpages. Return only valid JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            result = response.choices[0].message.content

            # Parse response
            json_match = re.search(r"\[.*\]", result, re.DOTALL)
            if json_match:
                result = json_match.group()

            events_data = json.loads(result)

            # Handle both array and object with array
            if isinstance(events_data, dict):
                events_data = events_data.get("events", events_data.get("data", []))

            # Convert to EventData objects
            events = []
            for event_dict in events_data:
                if event_dict.get("title"):
                    event_data = EventData(
                        external_id=f"{venue_name.lower().replace(' ', '_')}_{hash(event_dict.get('title'))}",
                        provider="kc_event_scraper",
                        name=event_dict.get("title"),
                        description=event_dict.get("description"),
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
                            event_dict.get("title"), event_dict.get("description")
                        ),
                        scraped_at=datetime.now(),
                        source_type="kc_scraper",
                    )
                    events.append(event_data)

            return events

        except Exception as e:
            self.logger.error(f"LLM extraction failed for {venue_name}: {e}")
            return []

    def _extract_events_with_selectors(
        self, html: str, venue_name: str, config: Dict
    ) -> List[EventData]:
        """Fallback: Extract events using CSS selectors"""
        soup = BeautifulSoup(html, "html.parser")
        events = []

        # Common event selectors
        event_selectors = [
            ".event",
            ".event-item",
            ".event-card",
            "article.event",
            "li.event",
            ".show",
            '[class*="event"]',
            '[class*="show"]',
        ]

        event_items = []
        for selector in event_selectors:
            items = soup.select(selector)
            if items:
                event_items = items
                break

        if not event_items:
            self.logger.warning(f"No events found with selectors for {venue_name}")
            return []

        for item in event_items[:50]:  # Limit to 50 events
            # Extract title
            title = None
            for sel in ["h1", "h2", "h3", ".title", ".event-title", ".name"]:
                elem = item.select_one(sel)
                if elem:
                    title = elem.get_text(strip=True)
                    break

            if not title:
                continue

            # Extract other fields
            date = None
            for sel in [".date", ".event-date", "time", "[datetime]"]:
                elem = item.select_one(sel)
                if elem:
                    date = elem.get_text(strip=True) or elem.get("datetime")
                    break

            time_str = None
            for sel in [".time", ".event-time", ".start-time"]:
                elem = item.select_one(sel)
                if elem:
                    time_str = elem.get_text(strip=True)
                    break

            description = None
            for sel in [".description", ".event-description", "p"]:
                elem = item.select_one(sel)
                if elem:
                    description = elem.get_text(strip=True)[:200]
                    break

            url = None
            link = item.select_one("a[href]")
            if link:
                url = link.get("href")
                # Make absolute URL
                if url and not url.startswith("http"):
                    base = "/".join(config["url"].split("/")[:3])
                    url = base + ("/" if not url.startswith("/") else "") + url

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
                price=None,
                image_url=None,
                attendance_estimate=None,
                impact_score=None,
                psychographic_scores=self._calculate_event_psychographics(
                    title, description
                ),
                scraped_at=datetime.now(),
                source_type="kc_scraper",
            )
            events.append(event_data)

        return events

    def _parse_event_datetime(self, date_str: str, time_str: str) -> Optional[datetime]:
        """Parse event date and time into datetime object"""
        if not date_str:
            return None

        try:
            from dateutil import parser

            if time_str:
                combined = f"{date_str} {time_str}"
                return parser.parse(combined, fuzzy=True)
            else:
                return parser.parse(date_str, fuzzy=True)
        except Exception:
            self.logger.warning(f"Could not parse date/time: {date_str} {time_str}")
            return None

    def _calculate_event_psychographics(self, title: str, description: str) -> Dict:
        """Calculate psychographic scores for an event based on title and description"""
        text = f"{title} {description}".lower() if description else title.lower()
        scores = {}

        for psychographic, keywords in self.psychographic_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            scores[psychographic] = min(score / len(keywords), 1.0)  # Normalize to 0-1

        return scores

    def _validate_and_store_event(self, event_data: EventData) -> bool:
        """Validate event data and store in database"""
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

            # Validate data quality
            is_valid, validation_results = self.quality_validator.validate_event(
                event_dict
            )

            if not is_valid:
                # Log validation errors
                errors = [
                    r.error_message for r in validation_results if r.error_message
                ]
                self.logger.warning(
                    f"Event validation failed for {event_data.name}: {'; '.join(errors)}"
                )
                return False

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


# Global event service instance
_event_service = None


def get_event_service() -> EventService:
    """Get the global event service instance"""
    global _event_service
    if _event_service is None:
        _event_service = EventService()
    return _event_service
