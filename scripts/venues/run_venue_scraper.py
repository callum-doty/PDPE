#!/usr/bin/env python3
"""
Standalone script to run venue scraping operations.
Usage: python scripts/venues/run_venue_scraper.py [--static] [--dynamic] [--all]
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Add project root to path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from features.venues.scrapers.static_venue_scraper import StaticVenueScraper
from features.venues.scrapers.dynamic_venue_scraper import DynamicVenueScraper
from features.venues.collectors.venue_collector import VenueCollector
from features.venues.processors.venue_processing import VenueProcessor
from shared.database.connection import get_database_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_static_scraping():
    """Run static venue scraping"""
    logger.info("Starting static venue scraping...")

    try:
        scraper = StaticVenueScraper()
        result = scraper.scrape_all_static_venues()

        logger.info(f"Static scraping completed: {result}")
        return result

    except Exception as e:
        logger.error(f"Static scraping failed: {e}")
        return {"success": False, "error": str(e)}


def run_dynamic_scraping():
    """Run dynamic venue scraping"""
    logger.info("Starting dynamic venue scraping...")

    try:
        scraper = DynamicVenueScraper()
        result = scraper.scrape_all_dynamic_venues()

        logger.info(f"Dynamic scraping completed: {result}")
        return result

    except Exception as e:
        logger.error(f"Dynamic scraping failed: {e}")
        return {"success": False, "error": str(e)}


def run_venue_collection():
    """Run venue data collection"""
    logger.info("Starting venue data collection...")

    try:
        collector = VenueCollector()
        result = collector.collect_all_venues()

        logger.info(f"Venue collection completed: {result}")
        return result

    except Exception as e:
        logger.error(f"Venue collection failed: {e}")
        return {"success": False, "error": str(e)}


def run_venue_processing():
    """Run venue data processing"""
    logger.info("Starting venue data processing...")

    try:
        processor = VenueProcessor()
        result = processor.process_all_venues()

        logger.info(f"Venue processing completed: {result}")
        return result

    except Exception as e:
        logger.error(f"Venue processing failed: {e}")
        return {"success": False, "error": str(e)}


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Run venue scraping operations")
    parser.add_argument(
        "--static", action="store_true", help="Run static venue scraping"
    )
    parser.add_argument(
        "--dynamic", action="store_true", help="Run dynamic venue scraping"
    )
    parser.add_argument(
        "--collect", action="store_true", help="Run venue data collection"
    )
    parser.add_argument(
        "--process", action="store_true", help="Run venue data processing"
    )
    parser.add_argument("--all", action="store_true", help="Run all venue operations")

    args = parser.parse_args()

    # If no specific operation is specified, run all
    if not any([args.static, args.dynamic, args.collect, args.process]):
        args.all = True

    results = []

    try:
        if args.all or args.static:
            results.append(("Static Scraping", run_static_scraping()))

        if args.all or args.dynamic:
            results.append(("Dynamic Scraping", run_dynamic_scraping()))

        if args.all or args.collect:
            results.append(("Venue Collection", run_venue_collection()))

        if args.all or args.process:
            results.append(("Venue Processing", run_venue_processing()))

        # Print summary
        print("\n" + "=" * 50)
        print("VENUE SCRAPING SUMMARY")
        print("=" * 50)

        for operation, result in results:
            status = "✅ SUCCESS" if result.get("success", False) else "❌ FAILED"
            print(f"{operation}: {status}")

            if not result.get("success", False) and result.get("error"):
                print(f"  Error: {result['error']}")
            elif result.get("venues_collected"):
                print(f"  Venues collected: {result['venues_collected']}")

        print("=" * 50)

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
