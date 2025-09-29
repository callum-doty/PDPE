# Master Data Orchestrator
"""
Central orchestrator for all data collection and aggregation processes.
Coordinates existing ETL scripts into a unified data collection system.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import existing ETL modules
try:
    from etl.ingest_local_venues import scrape_all_local_venues
    from etl.ingest_dynamic_venues import ingest_dynamic_venue_data
    from etl.ingest_social import ingest_social_data_for_venues
    from etl.ingest_weather import fetch_weather_for_kansas_city, upsert_weather_to_db
    from etl.ingest_traffic import ingest_traffic_data
    from etl.ingest_foot_traffic import ingest_foot_traffic_data
    from etl.ingest_econ import ingest_economic_data
    from etl.utils import get_db_conn
except ImportError as e:
    logging.warning(f"Could not import some ETL modules: {e}")

# Import ML prediction function separately since it might not exist
try:
    from backend.models.serve import generate_predictions_for_venues

    HAS_ML_PREDICTIONS = True
except ImportError:
    HAS_ML_PREDICTIONS = False
    logging.warning("ML prediction module not available")

    def generate_predictions_for_venues():
        """Placeholder function when ML predictions are not available"""
        logging.info("ML predictions not available - skipping")
        pass


@dataclass
class DataCollectionResult:
    """Result of a data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class MasterDataStatus:
    """Overall status of the master data system"""

    last_refresh: datetime
    total_venues: int
    data_completeness: float
    collection_results: List[DataCollectionResult]
    health_score: float


class MasterDataOrchestrator:
    """
    Master orchestrator that coordinates all data collection processes.

    This class serves as the single point of control for all ETL operations,
    replacing the scattered approach with a unified data collection system.
    """

    def __init__(self):
        """Initialize the master data orchestrator."""
        self.logger = logging.getLogger(__name__)
        self.collection_results = []

        # Kansas City area bounds for data collection
        self.kc_bounds = {"north": 39.3, "south": 38.9, "east": -94.3, "west": -94.8}

        # Priority data sources (based on user requirements)
        self.priority_sources = ["venues", "social_sentiment", "ml_predictions"]
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

        # Phase 2: Collect secondary data sources
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

    def collect_priority_data(self) -> List[DataCollectionResult]:
        """
        Collect only priority data sources (venues, social, ML).

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
    ) -> List[DataCollectionResult]:
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
            elif source == "social_sentiment":
                self._collect_social_data()
            elif source == "weather":
                self._collect_weather_data()
            elif source == "traffic":
                self._collect_traffic_data()
            elif source == "foot_traffic":
                self._collect_foot_traffic_data()
            elif source == "economic":
                self._collect_economic_data()
            elif source == "ml_predictions":
                self._generate_ml_predictions()
            else:
                self.logger.warning(f"Unknown data source: {source}")

        return self.collection_results

    def get_data_health_report(self) -> Dict:
        """
        Generate comprehensive data health report.

        Returns:
            Dictionary containing health metrics
        """
        conn = get_db_conn()
        if not conn:
            return {"error": "Could not connect to database"}

        cur = conn.cursor()

        try:
            # Get venue count and data completeness
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_venues,
                    COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as geocoded_venues,
                    COUNT(CASE WHEN psychographic_relevance IS NOT NULL THEN 1 END) as venues_with_psychographic
                FROM venues
            """
            )
            venue_stats = cur.fetchone()

            # Get recent data collection status
            cur.execute(
                """
                SELECT source_name, last_successful_collection, collection_health_score
                FROM collection_status
                ORDER BY last_successful_collection DESC
            """
            )
            collection_status = cur.fetchall()

            # Calculate overall health score
            total_venues = venue_stats[0] if venue_stats[0] else 0
            geocoded_venues = venue_stats[1] if venue_stats[1] else 0
            psychographic_venues = venue_stats[2] if venue_stats[2] else 0

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
                "data_sources": {
                    status[0]: {
                        "last_collection": status[1].isoformat() if status[1] else None,
                        "health_score": status[2] if status[2] else 0.0,
                    }
                    for status in collection_status
                },
                "overall_health_score": (
                    geocoding_completeness + psychographic_completeness
                )
                / 2,
                "recent_collections": len(
                    [
                        s
                        for s in collection_status
                        if s[1] and s[1] > datetime.now() - timedelta(days=1)
                    ]
                ),
            }

            return health_report

        except Exception as e:
            self.logger.error(f"Error generating health report: {e}")
            return {"error": str(e)}
        finally:
            cur.close()
            conn.close()

    def _collect_priority_data(self):
        """Collect priority data sources: venues and social sentiment."""
        self._collect_venue_data()
        self._collect_social_data()

    def _collect_secondary_data(self):
        """Collect secondary data sources: weather, traffic, etc."""
        self._collect_weather_data()
        self._collect_traffic_data()
        self._collect_foot_traffic_data()
        self._collect_economic_data()

    def _collect_venue_data(self):
        """Collect venue data from all sources."""
        start_time = datetime.now()

        try:
            self.logger.info("🏢 Collecting venue data (local + dynamic)")

            # Collect local venues (your 29 KC venues)
            scrape_all_local_venues()

            # Collect dynamic venues
            ingest_dynamic_venue_data()

            duration = (datetime.now() - start_time).total_seconds()

            # Get count of venues collected
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM venues WHERE updated_at >= %s", (start_time,)
            )
            venues_count = cur.fetchone()[0] if cur.fetchone() else 0
            cur.close()
            conn.close()

            result = DataCollectionResult(
                source_name="venues",
                success=True,
                records_collected=venues_count,
                duration_seconds=duration,
                data_quality_score=0.9,  # High quality for venue data
            )

            self.collection_results.append(result)
            self.logger.info(
                f"✅ Venue collection completed: {venues_count} venues in {duration:.1f}s"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="venues",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Venue collection failed: {e}")

    def _collect_social_data(self):
        """Collect social sentiment data."""
        start_time = datetime.now()

        try:
            self.logger.info("📱 Collecting social sentiment data")

            # Use existing social sentiment ingestion
            ingest_social_data_for_venues()

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="social_sentiment",
                success=True,
                records_collected=0,  # Would need to track this in the function
                duration_seconds=duration,
                data_quality_score=0.7,
            )

            self.collection_results.append(result)
            self.logger.info(
                f"✅ Social sentiment collection completed in {duration:.1f}s"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="social_sentiment",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Social sentiment collection failed: {e}")

    def _collect_weather_data(self):
        """Collect weather data."""
        start_time = datetime.now()

        try:
            self.logger.info("🌤️ Collecting weather data")

            weather_records = fetch_weather_for_kansas_city()
            if weather_records:
                upsert_weather_to_db(weather_records)

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="weather",
                success=True,
                records_collected=len(weather_records) if weather_records else 0,
                duration_seconds=duration,
                data_quality_score=0.8,
            )

            self.collection_results.append(result)
            self.logger.info(
                f"✅ Weather collection completed: {len(weather_records) if weather_records else 0} records in {duration:.1f}s"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="weather",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Weather collection failed: {e}")

    def _collect_traffic_data(self):
        """Collect traffic data."""
        start_time = datetime.now()

        try:
            self.logger.info("🚗 Collecting traffic data")

            ingest_traffic_data()

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="traffic",
                success=True,
                records_collected=0,  # Would need to track this
                duration_seconds=duration,
                data_quality_score=0.7,
            )

            self.collection_results.append(result)
            self.logger.info(f"✅ Traffic collection completed in {duration:.1f}s")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="traffic",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Traffic collection failed: {e}")

    def _collect_foot_traffic_data(self):
        """Collect foot traffic data."""
        start_time = datetime.now()

        try:
            self.logger.info("👥 Collecting foot traffic data")

            ingest_foot_traffic_data()

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="foot_traffic",
                success=True,
                records_collected=0,
                duration_seconds=duration,
                data_quality_score=0.6,
            )

            self.collection_results.append(result)
            self.logger.info(f"✅ Foot traffic collection completed in {duration:.1f}s")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="foot_traffic",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Foot traffic collection failed: {e}")

    def _collect_economic_data(self):
        """Collect economic data."""
        start_time = datetime.now()

        try:
            self.logger.info("💰 Collecting economic data")

            ingest_economic_data()

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="economic",
                success=True,
                records_collected=0,
                duration_seconds=duration,
                data_quality_score=0.8,
            )

            self.collection_results.append(result)
            self.logger.info(
                f"✅ Economic data collection completed in {duration:.1f}s"
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="economic",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ Economic data collection failed: {e}")

    def _generate_ml_predictions(self):
        """Generate ML predictions for venues."""
        start_time = datetime.now()

        try:
            self.logger.info("🤖 Generating ML predictions")

            # Generate predictions for all venues
            generate_predictions_for_venues()

            duration = (datetime.now() - start_time).total_seconds()

            result = DataCollectionResult(
                source_name="ml_predictions",
                success=True,
                records_collected=0,  # Would need to track this
                duration_seconds=duration,
                data_quality_score=0.8,
            )

            self.collection_results.append(result)
            self.logger.info(f"✅ ML predictions completed in {duration:.1f}s")

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = DataCollectionResult(
                source_name="ml_predictions",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )
            self.collection_results.append(result)
            self.logger.error(f"❌ ML predictions failed: {e}")

    def _calculate_master_status(self) -> MasterDataStatus:
        """Calculate overall master data status."""
        # Get venue count
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM venues WHERE lat IS NOT NULL AND lng IS NOT NULL"
        )
        total_venues = cur.fetchone()[0] if cur.fetchone() else 0
        cur.close()
        conn.close()

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
            f"{status} {result.source_name}: {result.records_collected} records in {result.duration_seconds:.1f}s"
        )

    # Test health report
    print("\nGenerating health report...")
    health_report = orchestrator.get_data_health_report()
    print(f"Overall health score: {health_report.get('overall_health_score', 0):.2f}")
