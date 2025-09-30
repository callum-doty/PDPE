"""
Local Venues Ingestion Module - Compatibility wrapper for data collectors approach
Provides compatibility functions that delegate to the unified data collectors
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def scrape_all_local_venues() -> Dict[str, Any]:
    """
    Scrape all local venues - compatibility wrapper

    Returns:
        Dictionary with scraping results
    """
    logger.info("Scraping all local venues (compatibility mode)")

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
            "message": "Local venues scraped successfully",
        }

    except ImportError as e:
        logger.warning(f"Could not import UnifiedVenueCollector: {e}")
        return {
            "success": False,
            "venues_collected": 0,
            "message": f"Import error: {e}",
        }
    except Exception as e:
        logger.error(f"Error scraping local venues: {e}")
        return {"success": False, "venues_collected": 0, "message": f"Error: {e}"}


def ingest_local_venue_data(area_bounds: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Ingest local venue data - compatibility wrapper

    Args:
        area_bounds: Geographic bounds for data collection

    Returns:
        Dictionary with ingestion results
    """
    logger.info("Ingesting local venue data (compatibility mode)")

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
            "message": "Local venue data ingested successfully",
        }

    except ImportError as e:
        logger.warning(f"Could not import UnifiedVenueCollector: {e}")
        return {"success": False, "venues_ingested": 0, "message": f"Import error: {e}"}
    except Exception as e:
        logger.error(f"Error ingesting local venue data: {e}")
        return {"success": False, "venues_ingested": 0, "message": f"Error: {e}"}


def get_local_venues_from_db() -> List[Dict]:
    """
    Get local venues from database - compatibility wrapper

    Returns:
        List of venue dictionaries
    """
    logger.info("Getting local venues from database (compatibility mode)")

    try:
        from etl.utils import get_db_conn

        conn = get_db_conn()
        if not conn:
            logger.error("Could not connect to database")
            return []

        cur = conn.cursor()

        # Get venues from database
        cur.execute(
            """
            SELECT venue_id, external_id, provider, name, category, 
                   location, comprehensive_score, data_completeness,
                   created_at, updated_at
            FROM venues 
            WHERE provider IN ('local_scraper', 'venue_collector', 'unified_venue_collector')
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

        logger.info(f"Retrieved {len(venues)} local venues from database")
        return venues

    except Exception as e:
        logger.error(f"Error getting local venues from database: {e}")
        return []


def update_venue_scores() -> Dict[str, Any]:
    """
    Update venue comprehensive scores - compatibility wrapper

    Returns:
        Dictionary with update results
    """
    logger.info("Updating venue scores (compatibility mode)")

    try:
        from etl.utils import get_db_conn

        conn = get_db_conn()
        if not conn:
            logger.error("Could not connect to database")
            return {"success": False, "message": "Database connection failed"}

        cur = conn.cursor()

        # Simple score update based on data completeness
        cur.execute(
            """
            UPDATE venues 
            SET comprehensive_score = COALESCE(data_completeness, 0.5),
                updated_at = NOW()
            WHERE comprehensive_score IS NULL
        """
        )

        updated_count = cur.rowcount
        conn.commit()

        cur.close()
        conn.close()

        logger.info(f"Updated scores for {updated_count} venues")
        return {
            "success": True,
            "venues_updated": updated_count,
            "message": f"Updated scores for {updated_count} venues",
        }

    except Exception as e:
        logger.error(f"Error updating venue scores: {e}")
        return {"success": False, "venues_updated": 0, "message": f"Error: {e}"}
