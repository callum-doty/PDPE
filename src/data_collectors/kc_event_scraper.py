"""
Kansas City Event Scraper
Handles both static HTML and dynamic JS sites with LLM extraction
Integrated with existing PPM infrastructure
"""

import sys
import os
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from dataclasses import dataclass, asdict
import html2text
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from etl.data_quality import process_events_with_quality_checks, log_quality_metrics
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Event data structure"""

    venue: str
    title: str
    date: Optional[str] = None
    time: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    category: Optional[str] = None
    image_url: Optional[str] = None
    price: Optional[str] = None
    scraped_at: str = None

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()


@dataclass
class KCEventCollectionResult:
    """Result of KC event collection operation"""

    source_name: str
    success: bool
    venues_collected: int
    events_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


class KCEventScraper:
    """Main scraper for Kansas City venues with LLM integration"""

    # Venue configuration
    VENUES = {
        # Major Venues - Static HTML
        "T-Mobile Center": {
            "url": "https://www.t-mobilecenter.com/events",
            "type": "static",
            "category": "Major Venue",
        },
        "Uptown Theater": {
            "url": "https://www.uptowntheater.com/events",
            "type": "static",
            "category": "Major Venue",
        },
        "Kauffman Center": {
            "url": "https://www.kauffmancenter.org/events/",
            "type": "static",
            "category": "Major Venue",
        },
        "Starlight Theatre": {
            "url": "https://www.kcstarlight.com/events/",
            "type": "static",
            "category": "Major Venue",
        },
        "The Midland Theatre": {
            "url": "https://www.midlandkc.com/events",
            "type": "static",
            "category": "Major Venue",
        },
        "Knuckleheads Saloon": {
            "url": "https://knuckleheadskc.com/",
            "type": "static",
            "category": "Major Venue",
        },
        "Azura Amphitheater": {
            "url": "https://www.azuraamphitheater.com/events",
            "type": "static",
            "category": "Major Venue",
        },
        # Entertainment Districts - Static
        "Power & Light District": {
            "url": "https://powerandlightdistrict.com/Events-and-Entertainment/Events",
            "type": "static",
            "category": "Entertainment District",
        },
        "Westport KC": {
            "url": "https://westportkcmo.com/events/",
            "type": "static",
            "category": "Entertainment District",
        },
        "18th & Vine Jazz": {
            "url": "https://kcjazzdistrict.org/events/",
            "type": "static",
            "category": "Entertainment District",
        },
        "Crossroads KC": {
            "url": "https://www.crossroadskc.com/shows",
            "type": "static",
            "category": "Entertainment District",
        },
        # Aggregators - Dynamic JS
        "Visit KC": {
            "url": "https://www.visitkc.com/events",
            "type": "dynamic",
            "category": "Aggregator",
            "wait_selector": ".event-card, .event-item",
        },
        "Do816": {
            "url": "https://do816.com/events",
            "type": "dynamic",
            "category": "Aggregator",
            "wait_selector": ".event",
        },
    }

    def __init__(self, llm_client=None, use_browser_for_all=False):
        """
        Initialize scraper

        Args:
            llm_client: Optional LLM client for extraction
            use_browser_for_all: Force browser rendering for all sites
        """
        self.llm_client = llm_client or self._initialize_openai_client()
        self.use_browser_for_all = use_browser_for_all
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

        # Initialize quality controller
        try:
            self.quality_controller = QualityController()
        except:
            self.quality_controller = None
            logger.warning("Could not initialize QualityController")

    def _initialize_openai_client(self):
        """Initialize OpenAI client using CHATGPT_API_KEY from .env"""
        try:
            from openai import OpenAI

            api_key = os.getenv("CHATGPT_API_KEY")
            if not api_key:
                logger.error("CHATGPT_API_KEY not found in environment variables")
                return None
            return OpenAI(api_key=api_key)
        except ImportError:
            logger.error("OpenAI package not installed")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            return None

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> KCEventCollectionResult:
        """
        Collect events from all KC venues (interface for data collectors)

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)
            time_period: Time period for collection

        Returns:
            Consolidated collection result
        """
        logger.info("🎭 Starting Kansas City event collection with LLM extraction")
        start_time = datetime.now()

        try:
            # Scrape all venues
            all_events = self.scrape_all(delay=2.0)

            # Process through quality pipeline if available
            if all_events and self.quality_controller:
                processed_events, quality_report = process_events_with_quality_checks(
                    [self._convert_event_to_dict(event) for event in all_events]
                )
                log_quality_metrics(quality_report, "kc_event_scraper")

                # Store in database
                if processed_events:
                    self._store_events_in_db(processed_events)

            duration = (datetime.now() - start_time).total_seconds()

            return KCEventCollectionResult(
                source_name="kc_event_scraper",
                success=True,
                venues_collected=len(self.VENUES),
                events_collected=len(all_events),
                duration_seconds=duration,
                data_quality_score=0.85 if self.llm_client else 0.7,
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"KC Event collection failed: {e}")
            return KCEventCollectionResult(
                source_name="kc_event_scraper",
                success=False,
                venues_collected=0,
                events_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

    def fetch_static_html(self, url: str) -> Optional[str]:
        """Fetch HTML from static site"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def fetch_with_browser(
        self, url: str, wait_selector: str = None, scroll: bool = True
    ) -> Optional[str]:
        """Fetch HTML using Playwright for dynamic sites"""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
                page = context.new_page()

                logger.info(f"Loading {url} with browser...")
                page.goto(url, wait_until="networkidle", timeout=30000)

                # Wait for specific content
                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=10000)
                    except PlaywrightTimeout:
                        logger.warning(f"Timeout waiting for {wait_selector}")

                # Scroll to load lazy content
                if scroll:
                    for _ in range(3):
                        page.evaluate("window.scrollBy(0, 1000)")
                        time.sleep(0.5)

                html = page.content()
                browser.close()
                return html

        except Exception as e:
            logger.error(f"Browser fetch failed for {url}: {e}")
            return None

    def extract_with_llm(self, html: str, venue_name: str) -> List[Event]:
        """Extract events using LLM"""
        if not self.llm_client:
            logger.warning(f"No LLM client available for {venue_name}")
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
            response = self.llm_client.chat.completions.create(
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
            import re

            json_match = re.search(r"\[.*\]", result, re.DOTALL)
            if json_match:
                result = json_match.group()

            events_data = json.loads(result)

            # Handle both array and object with array
            if isinstance(events_data, dict):
                events_data = events_data.get("events", events_data.get("data", []))

            # Convert to Event objects
            events = []
            for event_dict in events_data:
                if event_dict.get("title"):
                    events.append(
                        Event(
                            venue=venue_name,
                            title=event_dict.get("title"),
                            date=event_dict.get("date"),
                            time=event_dict.get("time"),
                            location=event_dict.get("location") or venue_name,
                            description=event_dict.get("description"),
                            url=event_dict.get("url"),
                            price=event_dict.get("price"),
                            image_url=event_dict.get("image_url"),
                            category=self.VENUES.get(venue_name, {}).get(
                                "category", "Event"
                            ),
                        )
                    )

            return events

        except Exception as e:
            logger.error(f"LLM extraction failed for {venue_name}: {e}")
            return []

    def extract_with_selectors(self, html: str, venue_name: str) -> List[Event]:
        """Fallback: Extract using CSS selectors"""
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
            logger.warning(f"No events found with selectors for {venue_name}")
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
                    base = "/".join(self.VENUES[venue_name]["url"].split("/")[:3])
                    url = base + ("/" if not url.startswith("/") else "") + url

            events.append(
                Event(
                    venue=venue_name,
                    title=title,
                    date=date,
                    time=time_str,
                    location=venue_name,
                    description=description,
                    url=url,
                    category=self.VENUES.get(venue_name, {}).get("category", "Event"),
                )
            )

        return events

    def scrape_venue(self, venue_name: str, config: dict) -> List[Event]:
        """Scrape a single venue"""
        logger.info(f"Scraping {venue_name}...")

        url = config["url"]
        is_dynamic = config["type"] == "dynamic"

        # Fetch HTML
        if is_dynamic or self.use_browser_for_all:
            html = self.fetch_with_browser(
                url, wait_selector=config.get("wait_selector")
            )
        else:
            html = self.fetch_static_html(url)

        if not html:
            logger.error(f"Failed to fetch {venue_name}")
            return []

        # Extract events
        if self.llm_client:
            events = self.extract_with_llm(html, venue_name)
            if events:
                logger.info(f"✅ Found {len(events)} events at {venue_name} (LLM)")
                return events

        # Fallback to selectors
        events = self.extract_with_selectors(html, venue_name)
        logger.info(f"✅ Found {len(events)} events at {venue_name} (Selectors)")
        return events

    def scrape_all(
        self, venue_filter: List[str] = None, delay: float = 2.0
    ) -> List[Event]:
        """
        Scrape all venues

        Args:
            venue_filter: List of venue names to scrape (None = all)
            delay: Delay between requests (seconds)
        """
        all_events = []
        venues_to_scrape = venue_filter or list(self.VENUES.keys())

        for venue_name in venues_to_scrape:
            if venue_name not in self.VENUES:
                logger.warning(f"Unknown venue: {venue_name}")
                continue

            config = self.VENUES[venue_name]
            events = self.scrape_venue(venue_name, config)
            all_events.extend(events)

            # Be respectful with delays
            if delay > 0:
                time.sleep(delay)

        return all_events

    def save_results(self, events: List[Event], filename: str = "kc_events.json"):
        """Save events to JSON file"""
        data = [asdict(event) for event in events]
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved {len(events)} events to {filename}")

    def _convert_event_to_dict(self, event: Event) -> Dict:
        """Convert Event object to dictionary format for quality pipeline"""
        return {
            "external_id": f"{event.venue.lower().replace(' ', '_')}_{hash(event.title)}",
            "provider": "kc_event_scraper",
            "name": event.title,
            "description": event.description,
            "category": "local_event",
            "subcategory": event.category,
            "start_time": self._parse_event_datetime(event.date, event.time),
            "end_time": None,
            "venue_name": event.venue,
            "source_url": event.url,
            "psychographic_scores": self._classify_event_psychographics(
                event.title, event.description
            ),
            "scraped_at": datetime.now(),
        }

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
            logger.warning(f"Could not parse date/time: {date_str} {time_str}")
            return None

    def _classify_event_psychographics(self, title: str, description: str) -> Dict:
        """Classify event based on psychographic keywords"""
        text = f"{title} {description}".lower() if description else title.lower()

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

        scores = {}
        for category, keywords in psychographic_keywords.items():
            score = sum(1 for keyword in keywords if keyword in text)
            scores[category] = score

        return scores

    def _store_events_in_db(self, events: List[Dict]):
        """Store events in database using existing infrastructure"""
        if not events:
            return

        conn = get_db_conn()
        if not conn:
            logger.error("Could not connect to database")
            return

        cur = conn.cursor()

        try:
            for event in events:
                # Find or create venue
                venue_id = self._find_or_create_venue(
                    event["venue_name"], event["provider"]
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
            logger.info(f"Successfully stored {len(events)} events in database")

        except Exception as e:
            logger.error(f"Error storing events in database: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _find_or_create_venue(self, venue_name: str, provider: str) -> Optional[str]:
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
                    "local_venue",
                ),
            )

            venue_id = cur.fetchone()[0]
            conn.commit()

            logger.info(f"Created new venue: {venue_name} ({venue_id})")
            return venue_id

        except Exception as e:
            logger.error(f"Error finding/creating venue {venue_name}: {e}")
            conn.rollback()
            return None
        finally:
            cur.close()
            conn.close()


# Example usage and testing
if __name__ == "__main__":
    # Initialize scraper with OpenAI client
    scraper = KCEventScraper()

    # Test with a few venues first
    test_venues = ["T-Mobile Center", "Uptown Theater", "Visit KC"]

    print("Starting Kansas City event scraper...")
    events = scraper.scrape_all(venue_filter=test_venues, delay=2.0)

    # Save results
    scraper.save_results(events)

    # Print summary
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Total events found: {len(events)}")

    # Group by venue
    from collections import Counter

    venue_counts = Counter(e.venue for e in events)
    print(f"\nEvents by venue:")
    for venue, count in venue_counts.most_common():
        print(f"  {venue}: {count} events")

    # Show sample events
    print(f"\nSample events:")
    for event in events[:3]:
        print(f"\n  {event.title}")
        print(f"  📍 {event.venue}")
        print(f"  📅 {event.date or 'Date TBD'}")
        print(f"  🔗 {event.url or 'No URL'}")
