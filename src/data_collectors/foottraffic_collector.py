# Foot Traffic Data Collector
"""
Standardized foot traffic data collector that consolidates foot traffic data collection
from ingest_foot_traffic.py into a unified collector with consistent interfaces.
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
class FootTrafficCollectionResult:
    """Result of foot traffic data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class FootTrafficData:
    """Standardized foot traffic data structure"""

    venue_id: str
    timestamp: datetime
    visitors_count: int
    median_dwell_seconds: int
    visitors_change_24h: float
    visitors_change_7d: float
    peak_hour_ratio: float
    source: str
    collected_at: datetime


class FootTrafficCollector:
    """Standardized foot traffic data collector."""

    def __init__(self):
        """Initialize the foot traffic collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()
        self.api_timeout = 15

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> FootTrafficCollectionResult:
        """Collect foot traffic data for specified area and time period."""
        start_time = datetime.now()
        self.logger.info("👥 Starting foot traffic data collection")

        try:
            traffic_records = self._fetch_foot_traffic_data(area_bounds)

            if traffic_records:
                validated_records = self._validate_foot_traffic_data(traffic_records)
                stored_count = self._upsert_foot_traffic_to_db(validated_records)

                duration = (datetime.now() - start_time).total_seconds()

                result = FootTrafficCollectionResult(
                    source_name="foot_traffic_api",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.7,
                )

                self.logger.info(
                    f"✅ Foot traffic collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = FootTrafficCollectionResult(
                    source_name="foot_traffic_api",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No foot traffic data retrieved",
                )

                self.logger.warning("⚠️ No foot traffic data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = FootTrafficCollectionResult(
                source_name="foot_traffic_api",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ Foot traffic collection failed: {e}")
            return result

    def _fetch_foot_traffic_data(
        self, area_bounds: Optional[Dict]
    ) -> List[FootTrafficData]:
        """Fetch foot traffic data from API (mock implementation)."""
        mock_traffic = FootTrafficData(
            venue_id="mock_venue_id",
            timestamp=datetime.now(),
            visitors_count=150,
            median_dwell_seconds=1800,
            visitors_change_24h=0.15,
            visitors_change_7d=-0.05,
            peak_hour_ratio=0.8,
            source="besttime_api",
            collected_at=datetime.now(),
        )
        return [mock_traffic]

    def _validate_foot_traffic_data(
        self, traffic_records: List[FootTrafficData]
    ) -> List[FootTrafficData]:
        """Validate foot traffic data records."""
        return [r for r in traffic_records if r.visitors_count >= 0]

    def _upsert_foot_traffic_to_db(self, traffic_records: List[FootTrafficData]) -> int:
        """Store foot traffic data in database."""
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
                    INSERT INTO venue_traffic (
                        venue_id, ts, visitors_count, median_dwell_seconds,
                        visitors_change_24h, visitors_change_7d, peak_hour_ratio, source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (venue_id, ts) DO UPDATE SET
                        visitors_count = EXCLUDED.visitors_count,
                        median_dwell_seconds = EXCLUDED.median_dwell_seconds,
                        visitors_change_24h = EXCLUDED.visitors_change_24h,
                        visitors_change_7d = EXCLUDED.visitors_change_7d,
                        peak_hour_ratio = EXCLUDED.peak_hour_ratio
                """,
                    (
                        record.venue_id,
                        record.timestamp,
                        record.visitors_count,
                        record.median_dwell_seconds,
                        record.visitors_change_24h,
                        record.visitors_change_7d,
                        record.peak_hour_ratio,
                        record.source,
                    ),
                )
                stored_count += 1

            conn.commit()

        except Exception as e:
            self.logger.error(f"Error storing foot traffic data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count


# Convenience functions for backward compatibility
def ingest_foot_traffic_data():
    """Convenience function to collect foot traffic data."""
    collector = FootTrafficCollector()
    result = collector.collect_data()
    return result.success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = FootTrafficCollector()
    result = collector.collect_data()
    status = "✅" if result.success else "❌"
    print(
        f"{status} Foot traffic collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )
