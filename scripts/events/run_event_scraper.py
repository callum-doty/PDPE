#!/usr/bin/env python3
"""
Event Scraper Script

Standalone script for scraping events from various sources.
Part of the PPM application restructuring - Phase 9: Standalone Scripts.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from features.events.collectors.external_api_collector import (
    fetch_predicthq_events,
    upsert_events_to_db,
)
from features.venues.scrapers.kc_event_scraper import KCEventScraper
from shared.database.connection import get_db_conn

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("event_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def scrape_predicthq_events(start_date=None, end_date=None):
    """Scrape events from PredictHQ API."""
    logger.info("Starting PredictHQ event scraping...")

    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%d")
    if not end_date:
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        events = fetch_predicthq_events("Kansas City", start=start_date, end=end_date)
        if events:
            upsert_events_to_db(events)
            logger.info(
                f"Successfully scraped and stored {len(events)} PredictHQ events"
            )
            return len(events)
        else:
            logger.warning("No events found from PredictHQ")
            return 0
    except Exception as e:
        logger.error(f"PredictHQ scraping failed: {e}")
        return 0


def scrape_kc_events():
    """Scrape events from Kansas City local sources."""
    logger.info("Starting KC local event scraping...")

    try:
        scraper = KCEventScraper()
        events = scraper.scrape_all_events()

        if events:
            # Store events in database
            conn = get_db_conn()
            if conn:
                cur = conn.cursor()
                stored_count = 0

                for event in events:
                    try:
                        cur.execute(
                            """
                            INSERT INTO events (
                                external_id, provider, name, description, 
                                start_time, end_time, venue_name, venue_address,
                                category, subcategory, url, lat, lng
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (external_id, provider) DO UPDATE SET
                                name = EXCLUDED.name,
                                description = EXCLUDED.description,
                                start_time = EXCLUDED.start_time,
                                end_time = EXCLUDED.end_time,
                                venue_name = EXCLUDED.venue_name,
                                venue_address = EXCLUDED.venue_address,
                                category = EXCLUDED.category,
                                subcategory = EXCLUDED.subcategory,
                                url = EXCLUDED.url,
                                lat = EXCLUDED.lat,
                                lng = EXCLUDED.lng,
                                updated_at = CURRENT_TIMESTAMP
                        """,
                            (
                                event.get("external_id"),
                                event.get("provider", "kc_scraper"),
                                event.get("name"),
                                event.get("description"),
                                event.get("start_time"),
                                event.get("end_time"),
                                event.get("venue_name"),
                                event.get("venue_address"),
                                event.get("category"),
                                event.get("subcategory"),
                                event.get("url"),
                                event.get("lat"),
                                event.get("lng"),
                            ),
                        )
                        stored_count += 1
                    except Exception as e:
                        logger.warning(
                            f"Failed to store event {event.get('name', 'Unknown')}: {e}"
                        )

                conn.commit()
                cur.close()
                conn.close()

                logger.info(
                    f"Successfully scraped and stored {stored_count} KC local events"
                )
                return stored_count
            else:
                logger.error("Database connection failed")
                return 0
        else:
            logger.warning("No events found from KC local sources")
            return 0

    except Exception as e:
        logger.error(f"KC local event scraping failed: {e}")
        return 0


def generate_summary_report(predicthq_count, kc_count):
    """Generate a summary report of the scraping session."""
    total_events = predicthq_count + kc_count

    logger.info("=" * 60)
    logger.info("EVENT SCRAPING SUMMARY REPORT")
    logger.info("=" * 60)
    logger.info(f"PredictHQ events scraped: {predicthq_count}")
    logger.info(f"KC local events scraped: {kc_count}")
    logger.info(f"Total events scraped: {total_events}")
    logger.info(f"Scraping completed at: {datetime.now()}")

    # Database summary
    try:
        conn = get_db_conn()
        if conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM events")
            total_db_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM events WHERE start_time > NOW()")
            upcoming_events = cur.fetchone()[0]

            logger.info(f"Total events in database: {total_db_events}")
            logger.info(f"Upcoming events: {upcoming_events}")

            cur.close()
            conn.close()
    except Exception as e:
        logger.warning(f"Could not generate database summary: {e}")

    logger.info("=" * 60)

    return total_events


def main():
    """Main function to run event scraping."""
    parser = argparse.ArgumentParser(
        description="Run event scraping from various sources"
    )
    parser.add_argument(
        "--source",
        choices=["all", "predicthq", "kc"],
        default="all",
        help="Event source to scrape (default: all)",
    )
    parser.add_argument(
        "--start-date", help="Start date for PredictHQ scraping (YYYY-MM-DD format)"
    )
    parser.add_argument(
        "--end-date", help="End date for PredictHQ scraping (YYYY-MM-DD format)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting event scraping script...")
    logger.info(f"Source: {args.source}")

    predicthq_count = 0
    kc_count = 0

    if args.source in ["all", "predicthq"]:
        predicthq_count = scrape_predicthq_events(args.start_date, args.end_date)

    if args.source in ["all", "kc"]:
        kc_count = scrape_kc_events()

    total_events = generate_summary_report(predicthq_count, kc_count)

    logger.info("Event scraping script completed successfully")
    return 0 if total_events > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
