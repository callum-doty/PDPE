# Economic Data Collector
"""
Standardized economic data collector that consolidates economic data collection
from ingest_econ.py into a unified collector with consistent interfaces.
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
class EconomicCollectionResult:
    """Result of economic data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class EconomicData:
    """Standardized economic data structure"""

    timestamp: datetime
    geographic_area: str
    unemployment_rate: Optional[float]
    median_household_income: Optional[float]
    business_openings: Optional[int]
    business_closures: Optional[int]
    consumer_confidence: Optional[float]
    local_spending_index: Optional[float]
    collected_at: datetime


class EconomicCollector:
    """Standardized economic data collector."""

    def __init__(self):
        """Initialize the economic collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()
        self.api_timeout = 15

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> EconomicCollectionResult:
        """Collect economic data for specified area and time period."""
        start_time = datetime.now()
        self.logger.info("💰 Starting economic data collection")

        try:
            economic_records = self._fetch_economic_data(area_bounds)

            if economic_records:
                validated_records = self._validate_economic_data(economic_records)
                stored_count = self._upsert_economic_to_db(validated_records)

                duration = (datetime.now() - start_time).total_seconds()

                result = EconomicCollectionResult(
                    source_name="economic_api",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.8,
                )

                self.logger.info(
                    f"✅ Economic collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = EconomicCollectionResult(
                    source_name="economic_api",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No economic data retrieved",
                )

                self.logger.warning("⚠️ No economic data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = EconomicCollectionResult(
                source_name="economic_api",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ Economic collection failed: {e}")
            return result

    def _fetch_economic_data(self, area_bounds: Optional[Dict]) -> List[EconomicData]:
        """Fetch economic data from API (mock implementation)."""
        mock_economic = EconomicData(
            timestamp=datetime.now(),
            geographic_area="kansas_city",
            unemployment_rate=3.2,
            median_household_income=65000.0,
            business_openings=45,
            business_closures=12,
            consumer_confidence=0.75,
            local_spending_index=1.1,
            collected_at=datetime.now(),
        )
        return [mock_economic]

    def _validate_economic_data(
        self, economic_records: List[EconomicData]
    ) -> List[EconomicData]:
        """Validate economic data records."""
        return [r for r in economic_records if r.geographic_area]

    def _upsert_economic_to_db(self, economic_records: List[EconomicData]) -> int:
        """Store economic data in database."""
        if not economic_records:
            return 0

        conn = get_db_conn()
        if not conn:
            return 0

        cur = conn.cursor()
        stored_count = 0

        try:
            for record in economic_records:
                cur.execute(
                    """
                    INSERT INTO economic_data (
                        ts, geographic_area, unemployment_rate, median_household_income,
                        business_openings, business_closures, consumer_confidence,
                        local_spending_index
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts, geographic_area) DO UPDATE SET
                        unemployment_rate = EXCLUDED.unemployment_rate,
                        median_household_income = EXCLUDED.median_household_income,
                        business_openings = EXCLUDED.business_openings,
                        business_closures = EXCLUDED.business_closures,
                        consumer_confidence = EXCLUDED.consumer_confidence,
                        local_spending_index = EXCLUDED.local_spending_index
                """,
                    (
                        record.timestamp,
                        record.geographic_area,
                        record.unemployment_rate,
                        record.median_household_income,
                        record.business_openings,
                        record.business_closures,
                        record.consumer_confidence,
                        record.local_spending_index,
                    ),
                )
                stored_count += 1

            conn.commit()

        except Exception as e:
            self.logger.error(f"Error storing economic data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count


# Convenience functions for backward compatibility
def ingest_economic_data():
    """Convenience function to collect economic data."""
    collector = EconomicCollector()
    result = collector.collect_data()
    return result.success


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = EconomicCollector()
    result = collector.collect_data()
    status = "✅" if result.success else "❌"
    print(
        f"{status} Economic collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )
