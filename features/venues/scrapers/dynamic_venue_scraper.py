"""
Dynamic Venues Ingestion Module - Compatibility wrapper for data collectors approach
Provides compatibility functions that delegate to the unified data collectors
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def ingest_dynamic_venue_data(area_bounds: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Ingest dynamic venue data - compatibility wrapper

    Args:
        area_bounds: Geographic bounds for data collection

    Returns:
        Dictionary with ingestion results
    """
    logger.info("Ingesting dynamic venue data (compatibility mode)")

    try:
        # Import the unified venue collector
        from data_collectors.venue_collector import UnifiedVenueCollector

        # Initialize and run the collector
        collector = UnifiedVenueCollector()
        result = collector.collect_data(area_bounds=area_bounds)

        return {
            "success": result.success if hasattr(result, "success") else True,
            "venues_ingested": (
                result.venues_collected if hasattr(result, "venues_collected") else 0
            ),
            "message": "Dynamic venue data ingested successfully",
        }

    except ImportError as e:
        logger.warning(f"Could not import UnifiedVenueCollector: {e}")
        return {"success": False, "venues_ingested": 0, "message": f"Import error: {e}"}
    except Exception as e:
        logger.error(f"Error ingesting dynamic venue data: {e}")
        return {"success": False, "venues_ingested": 0, "message": f"Error: {e}"}


def scrape_dynamic_venues() -> Dict[str, Any]:
    """
    Scrape dynamic venues - compatibility wrapper

    Returns:
        Dictionary with scraping results
    """
    logger.info("Scraping dynamic venues (compatibility mode)")

    try:
        # Import the unified venue collector
        from data_collectors.venue_collector import UnifiedVenueCollector

        # Initialize and run the collector
        collector = UnifiedVenueCollector()
        result = collector.collect_data()

        return {
            "success": result.success if hasattr(result, "success") else True,
            "venues_collected": (
                result.venues_collected if hasattr(result, "venues_collected") else 0
            ),
            "message": "Dynamic venues scraped successfully",
        }

    except ImportError as e:
        logger.warning(f"Could not import UnifiedVenueCollector: {e}")
        return {
            "success": False,
            "venues_collected": 0,
            "message": f"Import error: {e}",
        }
    except Exception as e:
        logger.error(f"Error scraping dynamic venues: {e}")
        return {"success": False, "venues_collected": 0, "message": f"Error: {e}"}


def get_dynamic_venues_from_db() -> List[Dict]:
    """
    Get dynamic venues from database - compatibility wrapper

    Returns:
        List of venue dictionaries
    """
    logger.info("Getting dynamic venues from database (compatibility mode)")

    try:
        from shared.database.connection import get_db_conn

        conn = get_db_conn()
        if not conn:
            logger.error("Could not connect to database")
            return []

        cur = conn.cursor()

        # Get dynamic venues from database
        cur.execute(
            """
            SELECT venue_id, external_id, provider, name, category, 
                   location, comprehensive_score, data_completeness,
                   created_at, updated_at
            FROM venues 
            WHERE provider IN ('dynamic_scraper', 'api_collector', 'external_api')
            ORDER BY comprehensive_score DESC NULLS LAST
        """
        )

        venues = []
        for row in cur.fetchall():
            venue = {
                "venue_id": row[0],
                "external_id": row[1],
                "provider": row[2],
                "name": row[3],
                "category": row[4],
                "location": row[5],
                "comprehensive_score": row[6],
                "data_completeness": row[7],
                "created_at": row[8],
                "updated_at": row[9],
            }
            venues.append(venue)

        cur.close()
        conn.close()

        logger.info(f"Retrieved {len(venues)} dynamic venues from database")
        return venues

    except Exception as e:
        logger.error(f"Error getting dynamic venues from database: {e}")
        return []
