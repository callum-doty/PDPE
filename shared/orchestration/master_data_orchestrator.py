# Master Data Orchestrator
"""
Central orchestrator for all data collection and aggregation processes.
Coordinates existing data collection components into a unified data collection system.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import unified models
from shared.models.core_models import (
    VenueCollectionResult,
    EventCollectionResult,
    DataQualityMetrics,
    ProcessingStatus,
)

# Import application components
try:
    from features.venues.collectors.venue_collector import VenueCollector
    from features.venues.scrapers.kc_event_scraper import KCEventScraper
    from features.ml.models.inference.predictor import MLPredictor
    from shared.database.connection import get_database_connection
    from shared.data_quality.quality_controller import QualityController

    COMPONENTS_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Could not import some application components: {e}")
    COMPONENTS_AVAILABLE = False

    # Create fallback classes
    class VenueCollector:
        def collect_all_venues(self):
            return []

    class KCEventScraper:
        def collect_data(self):
            return VenueCollectionResult(
                "events", False, 0, 0.0, error_message="Component not available"
            )

    class MLPredictor:
        def generate_venue_predictions(self):
            return []

    def get_database_connection():
        return None

    class QualityController:
        pass


@dataclass
class MasterDataStatus:
    """Overall status of the master data system"""

    last_refresh: datetime
    total_venues: int
    total_events: int
    data_completeness: float
    collection_results: List[VenueCollectionResult]
    health_score: float


class MasterDataOrchestrator:
    """
    Master orchestrator that coordinates all data collection processes.

    This class serves as the single point of control for all data collection operations,
    integrating with the new application architecture and components.
    """

    def __init__(self):
        """Initialize the master data orchestrator."""
        self.logger = logging.getLogger(__name__)
        self.collection_results = []
        self.quality_controller = QualityController()

        # Initialize collectors
        self.venue_collector = VenueCollector()
        self.event_scraper = KCEventScraper()
        self.ml_predictor = MLPredictor()

        # Kansas City area bounds for data collection
        self.kc_bounds = {"north": 39.3, "south": 38.9, "east": -94.3, "west": -94.8}

        # Priority data sources (based on user requirements)
        self.priority_sources = ["venues", "events", "ml_predictions"]
        self.secondary_sources = ["weather", "traffic", "foot_traffic", "economic"]

    def collect_all_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> MasterDataStatus:
        """
        Orchestrate collection of all data sources.

        Args:
            area_bounds: Geographic bounds for data collection (defaults to KC)
            time_period: Time period for data collection (defaults to 30 days)

        Returns:
            MasterDataStatus: Overall status of data collection
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting master data collection orchestration")

        # Use defaults if not provided
        if area_bounds is None:
            area_bounds = self.kc_bounds
        if time_period is None:
            time_period = timedelta(days=30)

        self.collection_results = []

        # Phase 1: Collect priority data sources
        self.logger.info("📊 Phase 1: Collecting priority data sources")
        self._collect_priority_data()

        # Phase 2: Collect secondary data sources (if available)
        self.logger.info("📈 Phase 2: Collecting secondary data sources")
        self._collect_secondary_data()

        # Phase 3: Generate ML predictions (depends on venue data)
        self.logger.info("🤖 Phase 3: Generating ML predictions")
        self._generate_ml_predictions()

        # Calculate overall status
        total_duration = (datetime.now() - start_time).total_seconds()
        status = self._calculate_master_status()

        self.logger.info(
            f"✅ Master data collection completed in {total_duration:.1f} seconds"
        )
        self.logger.info(f"📊 Overall health score: {status.health_score:.2f}")

        return status

    def collect_priority_data(self) -> List[VenueCollectionResult]:
        """
        Collect only priority data sources (venues, events, ML).

        Returns:
            List of collection results for priority sources
        """
        self.logger.info("🎯 Collecting priority data sources only")
        self.collection_results = []

        self._collect_priority_data()
        self._generate_ml_predictions()

        return [
            r for r in self.collection_results if r.source_name in self.priority_sources
        ]

    def refresh_data_sources(
        self, sources: Optional[List[str]] = None
    ) -> List[VenueCollectionResult]:
        """
        Refresh specific data sources.

        Args:
            sources: List of source names to refresh (None = all sources)

        Returns:
            List of collection results
        """
        if sources is None:
            return self.collect_all_data().collection_results

        self.collection_results = []

        for source in sources:
            if source == "venues":
                self._collect_venue_data()
            elif source == "events":
                self._collect_event_data()
            elif source == "ml_predictions":
                self._generate_ml_predictions()
            elif source == "master_data":
                self._refresh_master_data_views()
            elif source in self.secondary_sources:
                self.logger.info(
                    f"Secondary source {source} not yet implemented in new architecture"
                )
            else:
                self.logger.warning(f"Unknown data source: {source}")

        return self.collection_results

    def get_data_health_report(self) -> Dict:
        """
        Generate comprehensive data health report.

        Returns:
            Dictionary containing health metrics
        """
        try:
            with get_database_connection() as db:
                # Get venue count and data completeness
                venue_stats = db.execute_query(
                    """
                    SELECT 
                        COUNT(*) as total_venues,
                        COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as geocoded_venues,
                        COUNT(CASE WHEN psychographic_relevance IS NOT NULL THEN 1 END) as venues_with_psychographic
                    FROM venues
                """
                )

                # Get event count
                event_stats = db.execute_query(
                    "SELECT COUNT(*) as total_events FROM events"
                )

                # Calculate health metrics
                total_venues = venue_stats[0]["total_venues"] if venue_stats else 0
                geocoded_venues = (
                    venue_stats[0]["geocoded_venues"] if venue_stats else 0
                )
                psychographic_venues = (
                    venue_stats[0]["venues_with_psychographic"] if venue_stats else 0
                )
                total_events = event_stats[0]["total_events"] if event_stats else 0

                geocoding_completeness = geocoded_venues / max(total_venues, 1)
                psychographic_completeness = psychographic_venues / max(total_venues, 1)

                health_report = {
                    "timestamp": datetime.now().isoformat(),
                    "venue_statistics": {
                        "total_venues": total_venues,
                        "geocoded_venues": geocoded_venues,
                        "venues_with_psychographic": psychographic_venues,
                        "geocoding_completeness": geocoding_completeness,
                        "psychographic_completeness": psychographic_completeness,
                    },
                    "event_statistics": {
                        "total_events": total_events,
                    },
                    "overall_health_score": (
                        geocoding_completeness + psychographic_completeness
                    )
                    / 2,
                    "recent_collections": len(self.collection_results),
                }

                return health_report

        except Exception as e:
            self.logger.error(f"Error generating health report: {e}")
            return {"error": str(e)}

    def _collect_priority_data(self):
        """Collect priority data sources: venues, events, and ML predictions."""
        self._collect_venue_data()
        self._collect_event_data()

    def _collect_secondary_data(self):
        """Collect secondary data sources: weather, traffic, etc."""
        # These would be implemented as additional collectors in the future
        self.logger.info(
            "Secondary data sources not yet implemented in new architecture"
        )
        pass

    def _collect_venue_data(self):
        """Collect venue data using the VenueCollector."""
        start_time = datetime.now()

        try:
            self.logger.info("🏢 Collecting venue data")

            # Use the VenueCollector to collect all venues
            results = self.venue_collector.collect_all_venues()

            # Handle different result formats
            if isinstance(results, list):
                # Multiple results from different sources
                total_venues = sum(r.venues_collected for r in results if r.success)
                total_events = sum(
                    getattr(r, "events_collected", 0) for r in results if r.success
                )
                successful_sources = len([r for r in results if r.success])

                duration = (datetime.now() - start_time).total_seconds()

                # Create consolidated result
                result = VenueCollectionResult(
                    source_name="venues",
                    success=successful_sources > 0,
                    venues_collected=total_venues,
                    duration_seconds=duration,
                    data_quality_score=0.8 if successful_sources > 0 else 0.0,
                )

                # Add individual results to collection_results
                self.collection_results.extend(results)

            else:
                # Single result
                result = results
                self.collection_results.append(result)

            if result.success:
                self.logger.info(
                    f"✅ Venue collection completed: {result.venues_collected} venues in {result.duration_seconds:.1f}s"
                )
            else:
                self.logger.error(f"❌ Venue collection failed: {result.error_message}")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = VenueCollectionResult(
                source_name="venues",
                success=False,
                venues_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Venue collection failed: {e}")

    def _collect_event_data(self):
        """Collect event data using the KCEventScraper."""
        start_time = datetime.now()

        try:
            self.logger.info("🎭 Collecting event data")

            # Use the KCEventScraper to collect events
            result = self.event_scraper.collect_data()

            # Convert to VenueCollectionResult format for consistency
            if hasattr(result, "success"):
                venue_result = VenueCollectionResult(
                    source_name="events",
                    success=result.success,
                    venues_collected=0,  # Events don't count as venues
                    duration_seconds=(
                        result.duration_seconds
                        if hasattr(result, "duration_seconds")
                        else (datetime.now() - start_time).total_seconds()
                    ),
                    error_message=(
                        result.error_message
                        if hasattr(result, "error_message")
                        else None
                    ),
                    data_quality_score=(
                        result.data_quality_score
                        if hasattr(result, "data_quality_score")
                        else 0.7
                    ),
                )
            else:
                # Fallback for unexpected result format
                venue_result = VenueCollectionResult(
                    source_name="events",
                    success=False,
                    venues_collected=0,
                    duration_seconds=(datetime.now() - start_time).total_seconds(),
                    error_message="Unexpected result format",
                )

            self.collection_results.append(venue_result)

            if venue_result.success:
                events_collected = getattr(result, "events_collected", 0)
                self.logger.info(
                    f"✅ Event collection completed: {events_collected} events in {venue_result.duration_seconds:.1f}s"
                )
            else:
                self.logger.error(
                    f"❌ Event collection failed: {venue_result.error_message}"
                )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = VenueCollectionResult(
                source_name="events",
                success=False,
                venues_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Event collection failed: {e}")

    def _generate_ml_predictions(self):
        """Generate ML predictions using the MLPredictor."""
        start_time = datetime.now()

        try:
            self.logger.info("🤖 Generating ML predictions")

            # Use the MLPredictor to generate predictions
            predictions = self.ml_predictor.generate_venue_predictions()

            duration = (datetime.now() - start_time).total_seconds()

            result = VenueCollectionResult(
                source_name="ml_predictions",
                success=len(predictions) > 0 if predictions else False,
                venues_collected=len(predictions) if predictions else 0,
                duration_seconds=duration,
                data_quality_score=0.8 if predictions else 0.0,
            )

            self.collection_results.append(result)

            if result.success:
                self.logger.info(
                    f"✅ ML predictions completed: {len(predictions)} predictions in {duration:.1f}s"
                )
            else:
                self.logger.warning("⚠️ No ML predictions generated")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = VenueCollectionResult(
                source_name="ml_predictions",
                success=False,
                venues_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ ML predictions failed: {e}")

    def _refresh_master_data_views(self):
        """Refresh master data materialized views using the proper method for the database type."""
        start_time = datetime.now()

        try:
            self.logger.info("🔄 Refreshing master data views")

            # Import the refresh function from the fix module
            from fix_streamlit_event_discrepancy import refresh_master_data_tables

            # Use the updated refresh function that handles both PostgreSQL and SQLite
            venue_count, event_count = refresh_master_data_tables()

            duration = (datetime.now() - start_time).total_seconds()

            result = VenueCollectionResult(
                source_name="master_data",
                success=venue_count > 0 or event_count > 0,
                venues_collected=venue_count,
                duration_seconds=duration,
                data_quality_score=0.9 if venue_count > 0 else 0.0,
            )

            self.collection_results.append(result)

            if result.success:
                self.logger.info(
                    f"✅ Master data refresh completed: {venue_count} venues, {event_count} events in {duration:.1f}s"
                )
            else:
                self.logger.warning("⚠️ Master data refresh returned no data")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = VenueCollectionResult(
                source_name="master_data",
                success=False,
                venues_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Master data refresh failed: {e}")

    def _calculate_master_status(self) -> MasterDataStatus:
        """Calculate overall master data status."""
        try:
            # Get venue and event counts from database
            with get_database_connection() as db:
                venue_stats = db.execute_query(
                    "SELECT COUNT(*) as count FROM venues WHERE lat IS NOT NULL AND lng IS NOT NULL"
                )
                event_stats = db.execute_query("SELECT COUNT(*) as count FROM events")

                total_venues = venue_stats[0]["count"] if venue_stats else 0
                total_events = event_stats[0]["count"] if event_stats else 0

        except Exception as e:
            self.logger.warning(f"Could not get database counts: {e}")
            total_venues = 0
            total_events = 0

        # Calculate data completeness
        successful_collections = len([r for r in self.collection_results if r.success])
        total_collections = len(self.collection_results)
        data_completeness = successful_collections / max(total_collections, 1)

        # Calculate health score
        quality_scores = [
            r.data_quality_score
            for r in self.collection_results
            if r.data_quality_score
        ]
        avg_quality = (
            sum(quality_scores) / len(quality_scores) if quality_scores else 0.5
        )
        health_score = (data_completeness + avg_quality) / 2

        return MasterDataStatus(
            last_refresh=datetime.now(),
            total_venues=total_venues,
            total_events=total_events,
            data_completeness=data_completeness,
            collection_results=self.collection_results,
            health_score=health_score,
        )


# Convenience functions for backward compatibility
def collect_all_data():
    """Convenience function to collect all data using the orchestrator."""
    orchestrator = MasterDataOrchestrator()
    return orchestrator.collect_all_data()


def collect_priority_data():
    """Convenience function to collect only priority data."""
    orchestrator = MasterDataOrchestrator()
    return orchestrator.collect_priority_data()


if __name__ == "__main__":
    # Test the orchestrator
    logging.basicConfig(level=logging.INFO)

    orchestrator = MasterDataOrchestrator()

    # Test priority data collection
    print("Testing priority data collection...")
    results = orchestrator.collect_priority_data()

    for result in results:
        status = "✅" if result.success else "❌"
        print(
            f"{status} {result.source_name}: {result.venues_collected} records in {result.duration_seconds:.1f}s"
        )

    # Test health report
    print("\nGenerating health report...")
    health_report = orchestrator.get_data_health_report()
    print(f"Overall health score: {health_report.get('overall_health_score', 0):.2f}")
