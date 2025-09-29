# Daily Refresh Service
"""
Automated daily refresh service for the master data system.
Orchestrates data collection and materialized view refresh on a daily schedule.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
import time

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from master_data_service.orchestrator import (
        MasterDataOrchestrator,
        MasterDataStatus,
    )
    from master_data_service.quality_controller import QualityController, QualityMetrics
    from etl.utils import get_db_conn
except ImportError as e:
    logging.warning(f"Could not import master data modules: {e}")


@dataclass
class DailyRefreshResult:
    """Result of a daily refresh operation"""

    refresh_date: datetime
    total_duration: timedelta
    data_collection_status: MasterDataStatus
    quality_metrics: QualityMetrics
    materialized_view_refresh: Dict
    success: bool
    error_message: Optional[str] = None


class DailyRefreshService:
    """
    Daily refresh service for the master data system.

    Coordinates the complete daily refresh cycle:
    1. Data collection from all sources
    2. Data quality validation
    3. Materialized view refresh
    4. Health monitoring and reporting
    """

    def __init__(self):
        """Initialize the daily refresh service."""
        self.logger = logging.getLogger(__name__)
        self.orchestrator = MasterDataOrchestrator()
        self.quality_controller = QualityController()

        # Performance targets (based on user requirements)
        self.max_refresh_duration = timedelta(hours=1)  # <1 hour target
        self.min_data_quality_score = 0.7
        self.min_venue_count = 50  # Minimum expected venues

    def run_daily_refresh(self, force_refresh: bool = False) -> DailyRefreshResult:
        """
        Execute the complete daily refresh cycle.

        Args:
            force_refresh: Force refresh even if recent data exists

        Returns:
            DailyRefreshResult with comprehensive refresh status
        """
        refresh_start = datetime.now()
        self.logger.info("🌅 Starting daily master data refresh cycle")

        try:
            # Check if refresh is needed (unless forced)
            if not force_refresh and self._is_recent_refresh_available():
                self.logger.info("⏭️  Recent refresh found, skipping daily refresh")
                return self._get_last_refresh_result()

            # Phase 1: Data Collection
            self.logger.info("📊 Phase 1: Data Collection")
            collection_status = self._execute_data_collection()

            # Phase 2: Data Quality Validation
            self.logger.info("🔍 Phase 2: Data Quality Validation")
            quality_metrics = self._execute_quality_validation()

            # Phase 3: Materialized View Refresh
            self.logger.info("🔄 Phase 3: Materialized View Refresh")
            view_refresh_result = self._execute_view_refresh()

            # Phase 4: Health Check and Reporting
            self.logger.info("📋 Phase 4: Health Check and Reporting")
            health_status = self._execute_health_check(
                collection_status, quality_metrics, view_refresh_result
            )

            # Calculate total duration
            total_duration = datetime.now() - refresh_start

            # Create result
            result = DailyRefreshResult(
                refresh_date=refresh_start,
                total_duration=total_duration,
                data_collection_status=collection_status,
                quality_metrics=quality_metrics,
                materialized_view_refresh=view_refresh_result,
                success=health_status["overall_success"],
                error_message=health_status.get("error_message"),
            )

            # Log summary
            self._log_refresh_summary(result)

            return result

        except Exception as e:
            total_duration = datetime.now() - refresh_start
            error_result = DailyRefreshResult(
                refresh_date=refresh_start,
                total_duration=total_duration,
                data_collection_status=None,
                quality_metrics=None,
                materialized_view_refresh={},
                success=False,
                error_message=str(e),
            )

            self.logger.error(f"❌ Daily refresh failed: {e}")
            return error_result

    def run_priority_refresh(self) -> DailyRefreshResult:
        """
        Execute refresh for priority data sources only (venues, social, ML).

        Returns:
            DailyRefreshResult with priority refresh status
        """
        refresh_start = datetime.now()
        self.logger.info("🎯 Starting priority data refresh cycle")

        try:
            # Phase 1: Priority Data Collection
            self.logger.info("📊 Collecting priority data sources")
            priority_results = self.orchestrator.collect_priority_data()

            # Create minimal status for priority refresh
            collection_status = MasterDataStatus(
                last_refresh=datetime.now(),
                total_venues=0,  # Will be updated after view refresh
                data_completeness=0.0,  # Will be calculated
                collection_results=priority_results,
                health_score=0.0,  # Will be calculated
            )

            # Phase 2: Priority Quality Validation
            self.logger.info("🔍 Validating priority data quality")
            priority_quality = self.quality_controller.validate_priority_sources()

            # Calculate priority quality metrics
            quality_scores = [
                report.quality_score for report in priority_quality.values()
            ]
            avg_quality = (
                sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
            )

            quality_metrics = QualityMetrics(
                overall_quality_score=avg_quality,
                data_completeness=avg_quality,  # Simplified for priority refresh
                priority_sources_health={
                    source: report.quality_score
                    for source, report in priority_quality.items()
                },
                validation_summary={
                    "total_sources": len(priority_quality),
                    "healthy_sources": len(
                        [r for r in priority_quality.values() if r.quality_score >= 0.7]
                    ),
                    "warning_sources": len(
                        [
                            r
                            for r in priority_quality.values()
                            if 0.5 <= r.quality_score < 0.7
                        ]
                    ),
                    "critical_sources": len(
                        [r for r in priority_quality.values() if r.quality_score < 0.5]
                    ),
                },
                last_assessment=datetime.now(),
            )

            # Phase 3: Materialized View Refresh
            self.logger.info("🔄 Refreshing materialized views")
            view_refresh_result = self._execute_view_refresh()

            # Calculate total duration
            total_duration = datetime.now() - refresh_start

            # Update collection status with actual venue count
            if view_refresh_result.get("master_venue_data", {}).get("record_count"):
                collection_status.total_venues = view_refresh_result[
                    "master_venue_data"
                ]["record_count"]

            result = DailyRefreshResult(
                refresh_date=refresh_start,
                total_duration=total_duration,
                data_collection_status=collection_status,
                quality_metrics=quality_metrics,
                materialized_view_refresh=view_refresh_result,
                success=True,
            )

            self.logger.info(f"✅ Priority refresh completed in {total_duration}")
            return result

        except Exception as e:
            total_duration = datetime.now() - refresh_start
            error_result = DailyRefreshResult(
                refresh_date=refresh_start,
                total_duration=total_duration,
                data_collection_status=None,
                quality_metrics=None,
                materialized_view_refresh={},
                success=False,
                error_message=str(e),
            )

            self.logger.error(f"❌ Priority refresh failed: {e}")
            return error_result

    def get_refresh_status(self) -> Dict:
        """
        Get current refresh status and health metrics.

        Returns:
            Dictionary with current system status
        """
        try:
            # Get last refresh info from database
            conn = get_db_conn()
            if not conn:
                return {"error": "Database connection failed"}

            cur = conn.cursor()

            # Get last master data refresh
            cur.execute(
                """
                SELECT last_successful_collection, collection_health_score, status_details
                FROM collection_status 
                WHERE source_name = 'master_data_refresh'
                ORDER BY last_successful_collection DESC 
                LIMIT 1
            """
            )
            refresh_status = cur.fetchone()

            # Get venue count from materialized view
            cur.execute("SELECT COUNT(*) FROM master_venue_data")
            venue_count = cur.fetchone()[0] if cur.fetchone() else 0

            # Get data completeness
            cur.execute("SELECT AVG(data_completeness_score) FROM master_venue_data")
            avg_completeness = cur.fetchone()[0] if cur.fetchone() else 0.0

            cur.close()
            conn.close()

            status = {
                "timestamp": datetime.now().isoformat(),
                "last_refresh": (
                    refresh_status[0].isoformat()
                    if refresh_status and refresh_status[0]
                    else None
                ),
                "health_score": (
                    refresh_status[1] if refresh_status and refresh_status[1] else 0.0
                ),
                "refresh_details": (
                    refresh_status[2] if refresh_status and refresh_status[2] else {}
                ),
                "venue_count": venue_count,
                "avg_data_completeness": (
                    float(avg_completeness) if avg_completeness else 0.0
                ),
                "refresh_age_hours": (
                    (datetime.now() - refresh_status[0]).total_seconds() / 3600
                    if refresh_status and refresh_status[0]
                    else None
                ),
                "needs_refresh": (
                    not refresh_status
                    or not refresh_status[0]
                    or (datetime.now() - refresh_status[0]) > timedelta(hours=25)
                ),  # Refresh if older than 25 hours
            }

            return status

        except Exception as e:
            self.logger.error(f"Error getting refresh status: {e}")
            return {"error": str(e)}

    def _execute_data_collection(self) -> MasterDataStatus:
        """Execute data collection phase."""
        try:
            # Collect all data sources
            collection_status = self.orchestrator.collect_all_data()

            self.logger.info(
                f"📊 Data collection completed: {len(collection_status.collection_results)} sources, "
                f"health score {collection_status.health_score:.2f}"
            )

            return collection_status

        except Exception as e:
            self.logger.error(f"❌ Data collection failed: {e}")
            raise

    def _execute_quality_validation(self) -> QualityMetrics:
        """Execute data quality validation phase."""
        try:
            # Generate comprehensive quality metrics
            quality_metrics = self.quality_controller.generate_quality_metrics()

            self.logger.info(
                f"🔍 Quality validation completed: overall score {quality_metrics.overall_quality_score:.2f}, "
                f"completeness {quality_metrics.data_completeness:.2f}"
            )

            # Check if quality meets minimum standards
            if quality_metrics.overall_quality_score < self.min_data_quality_score:
                self.logger.warning(
                    f"⚠️  Data quality below threshold: {quality_metrics.overall_quality_score:.2f} "
                    f"< {self.min_data_quality_score}"
                )

            return quality_metrics

        except Exception as e:
            self.logger.error(f"❌ Quality validation failed: {e}")
            raise

    def _execute_view_refresh(self) -> Dict:
        """Execute materialized view refresh phase."""
        try:
            conn = get_db_conn()
            if not conn:
                raise Exception("Database connection failed")

            cur = conn.cursor()

            # Execute the refresh function
            cur.execute("SELECT * FROM refresh_all_master_data()")
            refresh_results = cur.fetchall()

            cur.close()
            conn.close()

            # Process results
            results = {}
            for result in refresh_results:
                view_name, status, record_count, duration = result
                results[view_name] = {
                    "status": status,
                    "record_count": record_count,
                    "duration_seconds": duration.total_seconds(),
                }

            self.logger.info(
                f"🔄 Materialized view refresh completed: "
                f"{results.get('master_venue_data', {}).get('record_count', 0)} venues, "
                f"{results.get('master_events_data', {}).get('record_count', 0)} events"
            )

            return results

        except Exception as e:
            self.logger.error(f"❌ Materialized view refresh failed: {e}")
            raise

    def _execute_health_check(
        self,
        collection_status: MasterDataStatus,
        quality_metrics: QualityMetrics,
        view_refresh: Dict,
    ) -> Dict:
        """Execute health check and determine overall success."""
        health_issues = []
        warnings = []

        # Check collection health
        failed_collections = [
            r for r in collection_status.collection_results if not r.success
        ]
        if failed_collections:
            health_issues.append(f"{len(failed_collections)} data collection failures")

        # Check quality metrics
        if quality_metrics.overall_quality_score < self.min_data_quality_score:
            health_issues.append(
                f"Quality score below threshold: {quality_metrics.overall_quality_score:.2f}"
            )

        # Check venue count
        venue_count = view_refresh.get("master_venue_data", {}).get("record_count", 0)
        if venue_count < self.min_venue_count:
            health_issues.append(
                f"Venue count below minimum: {venue_count} < {self.min_venue_count}"
            )

        # Check priority sources
        priority_health = quality_metrics.priority_sources_health
        for source, health_score in priority_health.items():
            if health_score < 0.5:
                health_issues.append(
                    f"Critical issue with {source}: {health_score:.2f}"
                )
            elif health_score < 0.7:
                warnings.append(f"Warning for {source}: {health_score:.2f}")

        # Determine overall success
        overall_success = len(health_issues) == 0

        return {
            "overall_success": overall_success,
            "health_issues": health_issues,
            "warnings": warnings,
            "venue_count": venue_count,
            "quality_score": quality_metrics.overall_quality_score,
            "error_message": "; ".join(health_issues) if health_issues else None,
        }

    def _is_recent_refresh_available(self) -> bool:
        """Check if a recent refresh is available (within 24 hours)."""
        try:
            conn = get_db_conn()
            if not conn:
                return False

            cur = conn.cursor()
            cur.execute(
                """
                SELECT last_successful_collection 
                FROM collection_status 
                WHERE source_name = 'master_data_refresh'
                AND last_successful_collection > NOW() - INTERVAL '24 hours'
            """
            )
            recent_refresh = cur.fetchone()

            cur.close()
            conn.close()

            return recent_refresh is not None

        except Exception as e:
            self.logger.error(f"Error checking recent refresh: {e}")
            return False

    def _get_last_refresh_result(self) -> DailyRefreshResult:
        """Get the last refresh result from database."""
        try:
            status = self.get_refresh_status()

            # Create a minimal result based on stored status
            return DailyRefreshResult(
                refresh_date=(
                    datetime.fromisoformat(status["last_refresh"])
                    if status.get("last_refresh")
                    else datetime.now()
                ),
                total_duration=timedelta(seconds=0),  # Not stored
                data_collection_status=None,  # Not stored
                quality_metrics=None,  # Not stored
                materialized_view_refresh=status.get("refresh_details", {}),
                success=status.get("health_score", 0) > 0.5,
            )

        except Exception as e:
            self.logger.error(f"Error getting last refresh result: {e}")
            return DailyRefreshResult(
                refresh_date=datetime.now(),
                total_duration=timedelta(seconds=0),
                data_collection_status=None,
                quality_metrics=None,
                materialized_view_refresh={},
                success=False,
                error_message=str(e),
            )

    def _log_refresh_summary(self, result: DailyRefreshResult):
        """Log comprehensive refresh summary."""
        self.logger.info("=" * 60)
        self.logger.info("📊 DAILY REFRESH SUMMARY")
        self.logger.info("=" * 60)

        if result.success:
            self.logger.info(f"✅ Status: SUCCESS")
        else:
            self.logger.info(f"❌ Status: FAILED - {result.error_message}")

        self.logger.info(f"⏱️  Duration: {result.total_duration}")
        self.logger.info(
            f"📅 Date: {result.refresh_date.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        if result.data_collection_status:
            self.logger.info(f"🏢 Venues: {result.data_collection_status.total_venues}")
            self.logger.info(
                f"📊 Health Score: {result.data_collection_status.health_score:.2f}"
            )

        if result.quality_metrics:
            self.logger.info(
                f"🔍 Quality Score: {result.quality_metrics.overall_quality_score:.2f}"
            )
            self.logger.info(
                f"📈 Completeness: {result.quality_metrics.data_completeness:.2f}"
            )

        if result.materialized_view_refresh:
            for view_name, view_result in result.materialized_view_refresh.items():
                self.logger.info(
                    f"🔄 {view_name}: {view_result.get('record_count', 0)} records "
                    f"in {view_result.get('duration_seconds', 0):.1f}s"
                )

        # Performance check
        if result.total_duration > self.max_refresh_duration:
            self.logger.warning(
                f"⚠️  Refresh duration exceeded target: {result.total_duration} > {self.max_refresh_duration}"
            )
        else:
            self.logger.info(
                f"🎯 Performance target met: {result.total_duration} < {self.max_refresh_duration}"
            )

        self.logger.info("=" * 60)


# Convenience functions for external use
def run_daily_refresh(force_refresh: bool = False) -> DailyRefreshResult:
    """Convenience function to run daily refresh."""
    service = DailyRefreshService()
    return service.run_daily_refresh(force_refresh)


def run_priority_refresh() -> DailyRefreshResult:
    """Convenience function to run priority refresh."""
    service = DailyRefreshService()
    return service.run_priority_refresh()


def get_refresh_status() -> Dict:
    """Convenience function to get refresh status."""
    service = DailyRefreshService()
    return service.get_refresh_status()


if __name__ == "__main__":
    # Test the daily refresh service
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    service = DailyRefreshService()

    print("Testing daily refresh service...")

    # Test refresh status
    print("\n1. Getting current refresh status...")
    status = service.get_refresh_status()
    print(f"   Last refresh: {status.get('last_refresh', 'Never')}")
    print(f"   Health score: {status.get('health_score', 0):.2f}")
    print(f"   Venue count: {status.get('venue_count', 0)}")
    print(f"   Needs refresh: {status.get('needs_refresh', True)}")

    # Test priority refresh (faster for testing)
    print("\n2. Running priority refresh...")
    result = service.run_priority_refresh()

    if result.success:
        print(f"   ✅ Priority refresh completed in {result.total_duration}")
        if result.quality_metrics:
            print(
                f"   📊 Quality score: {result.quality_metrics.overall_quality_score:.2f}"
            )
        if result.materialized_view_refresh:
            venue_count = result.materialized_view_refresh.get(
                "master_venue_data", {}
            ).get("record_count", 0)
            print(f"   🏢 Venues processed: {venue_count}")
    else:
        print(f"   ❌ Priority refresh failed: {result.error_message}")

    print("\n3. Final status check...")
    final_status = service.get_refresh_status()
    print(f"   Health score: {final_status.get('health_score', 0):.2f}")
    print(f"   Data completeness: {final_status.get('avg_data_completeness', 0):.2f}")
