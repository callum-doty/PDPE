# Traffic Data Collector
"""
Standardized traffic data collector that consolidates traffic data collection
from ingest_traffic.py into a unified collector with consistent interfaces.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class TrafficCollectionResult:
    """Result of traffic data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class TrafficData:
    """Standardized traffic data structure"""

    venue_id: str
    timestamp: datetime
    congestion_score: float  # 0-1 scale
    travel_time_to_downtown: float  # minutes
    travel_time_index: float  # ratio to free-flow time
    source: str
    collected_at: datetime


class TrafficCollector:
    """
    Standardized traffic data collector.

    Consolidates traffic data collection functionality from ingest_traffic.py
    into a unified collector with consistent data quality processing.
    """

    def __init__(self):
        """Initialize the traffic collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()
        self.api_timeout = 15

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> TrafficCollectionResult:
        """
        Collect traffic data for specified area and time period.

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)
            time_period: Time period for collection (defaults to current)

        Returns:
            TrafficCollectionResult with collection status and metrics
        """
        start_time = datetime.now()
        self.logger.info("🚗 Starting traffic data collection")

        try:
            # Mock traffic data collection
            traffic_records = self._fetch_traffic_data(area_bounds)

            if traffic_records:
                validated_records = self._validate_traffic_data(traffic_records)
                stored_count = self._upsert_traffic_to_db(validated_records)

                duration = (datetime.now() - start_time).total_seconds()

                result = TrafficCollectionResult(
                    source_name="traffic_api",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.8,
                )

                self.logger.info(
                    f"✅ Traffic collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = TrafficCollectionResult(
                    source_name="traffic_api",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No traffic data retrieved",
                )

                self.logger.warning("⚠️ No traffic data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = TrafficCollectionResult(
                source_name="traffic_api",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ Traffic collection failed: {e}")
            return result

    def _fetch_traffic_data(self, area_bounds: Optional[Dict]) -> List[TrafficData]:
        """Fetch traffic data from API (mock implementation)."""
        # Mock implementation - replace with actual API calls
        mock_traffic = TrafficData(
            venue_id="mock_venue_id",
            timestamp=datetime.now(),
            congestion_score=0.6,
            travel_time_to_downtown=25.5,
            travel_time_index=1.3,
            source="google_maps",
            collected_at=datetime.now(),
        )
        return [mock_traffic]

    def _validate_traffic_data(
        self, traffic_records: List[TrafficData]
    ) -> List[TrafficData]:
        """Validate traffic data records."""
        return [r for r in traffic_records if 0 <= r.congestion_score <= 1]

    def _upsert_traffic_to_db(self, traffic_records: List[TrafficData]) -> int:
        """Store traffic data in database."""
        if not traffic_records:
            return 0

        conn = get_db_conn()
        if not conn:
            return 0

        cur = conn.cursor()
        stored_count = 0

        try:
            for record in traffic_records:
                cur.execute(
                    """
                    INSERT INTO traffic_data (
                        venue_id, ts, congestion_score, travel_time_to_downtown,
                        travel_time_index, source
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (venue_id, ts) DO UPDATE SET
                        congestion_score = EXCLUDED.congestion_score,
                        travel_time_to_downtown = EXCLUDED.travel_time_to_downtown,
                        travel_time_index = EXCLUDED.travel_time_index
                """,
                    (
                        record.venue_id,
                        record.timestamp,
                        record.congestion_score,
                        record.travel_time_to_downtown,
                        record.travel_time_index,
                        record.source,
                    ),
                )
                stored_count += 1

            conn.commit()

        except Exception as e:
            self.logger.error(f"Error storing traffic data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count


# Convenience functions for backward compatibility
def ingest_traffic_data():
    """Convenience function to collect traffic data."""
    collector = TrafficCollector()
    result = collector.collect_data()
    return result.success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = TrafficCollector()
    result = collector.collect_data()
    status = "✅" if result.success else "❌"
    print(
        f"{status} Traffic collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )
