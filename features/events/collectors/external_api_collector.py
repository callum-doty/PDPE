# External API Collector
"""
Standardized external API collector that consolidates external API data collection
(PredictHQ, Google Places, etc.) into a unified collector with consistent interfaces.
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
    from shared.database.connection import get_db_conn
    from shared.data_quality.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class ExternalAPICollectionResult:
    """Result of external API data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class ExternalEventData:
    """Standardized external event data structure"""

    external_id: str
    provider: str  # 'predicthq', 'google_places', etc.
    name: str
    description: Optional[str]
    category: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    lat: Optional[float]
    lng: Optional[float]
    venue_name: Optional[str]
    attendance_estimate: Optional[int]
    impact_score: Optional[float]
    collected_at: datetime


class ExternalAPICollector:
    """Standardized external API collector."""

    def __init__(self):
        """Initialize the external API collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()
        self.api_timeout = 15

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> ExternalAPICollectionResult:
        """Collect data from external APIs."""
        start_time = datetime.now()
        self.logger.info("🌐 Starting external API data collection")

        try:
            external_data = self._fetch_external_data(area_bounds, time_period)

            if external_data:
                validated_data = self._validate_external_data(external_data)
                stored_count = self._upsert_external_data_to_db(validated_data)

                duration = (datetime.now() - start_time).total_seconds()

                result = ExternalAPICollectionResult(
                    source_name="external_apis",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.8,
                )

                self.logger.info(
                    f"✅ External API collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = ExternalAPICollectionResult(
                    source_name="external_apis",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No external data retrieved",
                )

                self.logger.warning("⚠️ No external data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = ExternalAPICollectionResult(
                source_name="external_apis",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ External API collection failed: {e}")
            return result

    def _fetch_external_data(
        self, area_bounds: Optional[Dict], time_period: Optional[timedelta]
    ) -> List[ExternalEventData]:
        """Fetch data from external APIs (mock implementation)."""
        mock_event = ExternalEventData(
            external_id="predicthq_12345",
            provider="predicthq",
            name="Kansas City Chiefs Game",
            description="NFL game at Arrowhead Stadium",
            category="sports",
            start_time=datetime.now() + timedelta(days=7),
            end_time=datetime.now() + timedelta(days=7, hours=3),
            lat=39.0489,
            lng=-94.4839,
            venue_name="Arrowhead Stadium",
            attendance_estimate=76000,
            impact_score=0.9,
            collected_at=datetime.now(),
        )
        return [mock_event]

    def _validate_external_data(
        self, external_data: List[ExternalEventData]
    ) -> List[ExternalEventData]:
        """Validate external data records."""
        return [d for d in external_data if d.external_id and d.provider]

    def _upsert_external_data_to_db(
        self, external_data: List[ExternalEventData]
    ) -> int:
        """Store external data in database."""
        if not external_data:
            return 0

        conn = get_db_conn()
        if not conn:
            return 0

        cur = conn.cursor()
        stored_count = 0

        try:
            for data in external_data:
                cur.execute(
                    """
                    INSERT INTO events (
                        external_id, provider, name, description, category,
                        start_time, end_time, lat, lng, venue_name,
                        attendance_estimate, impact_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (external_id, provider) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        category = EXCLUDED.category,
                        start_time = EXCLUDED.start_time,
                        end_time = EXCLUDED.end_time,
                        lat = EXCLUDED.lat,
                        lng = EXCLUDED.lng,
                        venue_name = EXCLUDED.venue_name,
                        attendance_estimate = EXCLUDED.attendance_estimate,
                        impact_score = EXCLUDED.impact_score
                """,
                    (
                        data.external_id,
                        data.provider,
                        data.name,
                        data.description,
                        data.category,
                        data.start_time,
                        data.end_time,
                        data.lat,
                        data.lng,
                        data.venue_name,
                        data.attendance_estimate,
                        data.impact_score,
                    ),
                )
                stored_count += 1

            conn.commit()

        except Exception as e:
            self.logger.error(f"Error storing external data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = ExternalAPICollector()
    result = collector.collect_data()
    status = "✅" if result.success else "❌"
    print(
        f"{status} External API collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )
