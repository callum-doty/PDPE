# Quality Controller
"""
Unified data quality control for all data sources in the master data system.
Focuses on priority data validation (venues, social sentiment, ML predictions).
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.data_quality import (
        process_events_with_quality_checks,
        log_quality_metrics,
    )
    from etl.venue_processing import (
        process_venues_with_quality_checks,
        log_venue_quality_metrics,
    )
    from etl.utils import get_db_conn
except ImportError as e:
    logging.warning(f"Could not import quality modules: {e}")


@dataclass
class QualityReport:
    """Quality assessment report for a data source"""

    source_name: str
    total_input: int
    total_output: int
    quality_score: float
    completeness_score: float
    validation_errors: List[str]
    data_issues: List[str]
    processing_time: float
    timestamp: datetime


@dataclass
class QualityMetrics:
    """Overall quality metrics for the master data system"""

    overall_quality_score: float
    data_completeness: float
    priority_sources_health: Dict[str, float]
    validation_summary: Dict[str, int]
    last_assessment: datetime


class QualityController:
    """
    Unified data quality controller for all master data sources.

    Focuses on priority data validation while providing comprehensive
    quality assessment across all data collection processes.
    """

    def __init__(self):
        """Initialize the quality controller."""
        self.logger = logging.getLogger(__name__)

        # Quality thresholds for different data sources
        self.quality_thresholds = {
            "venues": {
                "min_completeness": 0.8,  # 80% of venues must have lat/lng
                "min_quality_score": 0.7,
                "required_fields": ["name", "lat", "lng"],
            },
            "social_sentiment": {
                "min_completeness": 0.6,  # Social data can be sparse
                "min_quality_score": 0.6,
                "required_fields": ["mention_count", "positive_sentiment"],
            },
            "ml_predictions": {
                "min_completeness": 0.9,  # ML predictions should be comprehensive
                "min_quality_score": 0.8,
                "required_fields": ["psychographic_density", "confidence_lower"],
            },
            "weather": {
                "min_completeness": 0.9,
                "min_quality_score": 0.8,
                "required_fields": ["temperature_f", "weather_condition"],
            },
            "traffic": {
                "min_completeness": 0.7,
                "min_quality_score": 0.7,
                "required_fields": ["congestion_score"],
            },
        }

    def validate_all_sources(self) -> Dict[str, QualityReport]:
        """
        Validate all data sources and generate quality reports.

        Returns:
            Dictionary mapping source names to quality reports
        """
        self.logger.info("🔍 Starting comprehensive data quality validation")

        quality_reports = {}

        # Validate priority sources first
        priority_sources = ["venues", "social_sentiment", "ml_predictions"]
        for source in priority_sources:
            try:
                report = self._validate_data_source(source)
                quality_reports[source] = report
                self.logger.info(
                    f"✅ {source}: Quality score {report.quality_score:.2f}, "
                    f"Completeness {report.completeness_score:.2f}"
                )
            except Exception as e:
                self.logger.error(f"❌ Failed to validate {source}: {e}")
                quality_reports[source] = self._create_error_report(source, str(e))

        # Validate secondary sources
        secondary_sources = ["weather", "traffic", "foot_traffic", "economic"]
        for source in secondary_sources:
            try:
                report = self._validate_data_source(source)
                quality_reports[source] = report
                self.logger.info(
                    f"✅ {source}: Quality score {report.quality_score:.2f}"
                )
            except Exception as e:
                self.logger.error(f"❌ Failed to validate {source}: {e}")
                quality_reports[source] = self._create_error_report(source, str(e))

        return quality_reports

    def validate_priority_sources(self) -> Dict[str, QualityReport]:
        """
        Validate only priority data sources (venues, social, ML).

        Returns:
            Dictionary mapping priority source names to quality reports
        """
        self.logger.info("🎯 Validating priority data sources")

        priority_sources = ["venues", "social_sentiment", "ml_predictions"]
        quality_reports = {}

        for source in priority_sources:
            try:
                report = self._validate_data_source(source)
                quality_reports[source] = report
                self.logger.info(
                    f"✅ {source}: Quality {report.quality_score:.2f}, "
                    f"Completeness {report.completeness_score:.2f}"
                )
            except Exception as e:
                self.logger.error(f"❌ Failed to validate {source}: {e}")
                quality_reports[source] = self._create_error_report(source, str(e))

        return quality_reports

    def clean_and_normalize_data(
        self, raw_data: List[Dict], source_type: str
    ) -> List[Dict]:
        """
        Clean and normalize data from a specific source.

        Args:
            raw_data: List of raw data records
            source_type: Type of data source

        Returns:
            List of cleaned and normalized data records
        """
        if not raw_data:
            return []

        self.logger.info(f"🧹 Cleaning {len(raw_data)} records from {source_type}")

        if source_type == "venues":
            return self._clean_venue_data(raw_data)
        elif source_type == "social_sentiment":
            return self._clean_social_data(raw_data)
        elif source_type == "ml_predictions":
            return self._clean_ml_data(raw_data)
        else:
            return self._clean_generic_data(raw_data, source_type)

    def detect_and_resolve_duplicates(
        self, data_list: List[Dict], source_type: str
    ) -> List[Dict]:
        """
        Detect and resolve duplicate records.

        Args:
            data_list: List of data records
            source_type: Type of data source

        Returns:
            List of deduplicated records
        """
        if not data_list:
            return []

        original_count = len(data_list)
        self.logger.info(
            f"🔍 Detecting duplicates in {original_count} {source_type} records"
        )

        if source_type == "venues":
            deduplicated = self._deduplicate_venues(data_list)
        elif source_type == "social_sentiment":
            deduplicated = self._deduplicate_social_data(data_list)
        else:
            deduplicated = self._deduplicate_generic(data_list, source_type)

        duplicates_removed = original_count - len(deduplicated)
        if duplicates_removed > 0:
            self.logger.info(
                f"🗑️  Removed {duplicates_removed} duplicate {source_type} records"
            )

        return deduplicated

    def generate_quality_metrics(self) -> QualityMetrics:
        """
        Generate comprehensive quality metrics for the master data system.

        Returns:
            QualityMetrics object with overall system quality assessment
        """
        self.logger.info("📊 Generating comprehensive quality metrics")

        # Validate all sources
        quality_reports = self.validate_all_sources()

        # Calculate overall quality score
        quality_scores = [report.quality_score for report in quality_reports.values()]
        overall_quality = (
            sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
        )

        # Calculate data completeness
        completeness_scores = [
            report.completeness_score for report in quality_reports.values()
        ]
        overall_completeness = (
            sum(completeness_scores) / len(completeness_scores)
            if completeness_scores
            else 0.0
        )

        # Priority sources health
        priority_sources = ["venues", "social_sentiment", "ml_predictions"]
        priority_health = {
            source: quality_reports.get(
                source, self._create_error_report(source, "No data")
            ).quality_score
            for source in priority_sources
        }

        # Validation summary
        validation_summary = {
            "total_sources": len(quality_reports),
            "healthy_sources": len(
                [r for r in quality_reports.values() if r.quality_score >= 0.7]
            ),
            "warning_sources": len(
                [r for r in quality_reports.values() if 0.5 <= r.quality_score < 0.7]
            ),
            "critical_sources": len(
                [r for r in quality_reports.values() if r.quality_score < 0.5]
            ),
        }

        return QualityMetrics(
            overall_quality_score=overall_quality,
            data_completeness=overall_completeness,
            priority_sources_health=priority_health,
            validation_summary=validation_summary,
            last_assessment=datetime.now(),
        )

    def _validate_data_source(self, source_name: str) -> QualityReport:
        """Validate a specific data source."""
        start_time = datetime.now()

        if source_name == "venues":
            return self._validate_venues()
        elif source_name == "social_sentiment":
            return self._validate_social_sentiment()
        elif source_name == "ml_predictions":
            return self._validate_ml_predictions()
        elif source_name == "weather":
            return self._validate_weather()
        elif source_name == "traffic":
            return self._validate_traffic()
        else:
            return self._validate_generic_source(source_name)

    def _validate_venues(self) -> QualityReport:
        """Validate venue data quality."""
        start_time = datetime.now()
        validation_errors = []
        data_issues = []

        conn = get_db_conn()
        if not conn:
            return self._create_error_report("venues", "Database connection failed")

        cur = conn.cursor()

        try:
            # Get venue statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_venues,
                    COUNT(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 END) as geocoded_venues,
                    COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as named_venues,
                    COUNT(CASE WHEN psychographic_relevance IS NOT NULL THEN 1 END) as venues_with_psychographic,
                    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_venues
                FROM venues
            """
            )
            stats = cur.fetchone()

            total_venues = stats[0] if stats[0] else 0
            geocoded_venues = stats[1] if stats[1] else 0
            named_venues = stats[2] if stats[2] else 0
            psychographic_venues = stats[3] if stats[3] else 0
            recent_venues = stats[4] if stats[4] else 0

            # Calculate quality metrics
            if total_venues == 0:
                validation_errors.append("No venues found in database")
                quality_score = 0.0
                completeness_score = 0.0
            else:
                geocoding_completeness = geocoded_venues / total_venues
                naming_completeness = named_venues / total_venues
                psychographic_completeness = psychographic_venues / total_venues
                recency_score = recent_venues / total_venues

                # Overall completeness (weighted average)
                completeness_score = (
                    geocoding_completeness * 0.4
                    + naming_completeness * 0.3
                    + psychographic_completeness * 0.2
                    + recency_score * 0.1
                )

                # Quality score based on thresholds
                quality_score = completeness_score

                # Check for issues
                if (
                    geocoding_completeness
                    < self.quality_thresholds["venues"]["min_completeness"]
                ):
                    data_issues.append(
                        f"Low geocoding completeness: {geocoding_completeness:.2f} "
                        f"(threshold: {self.quality_thresholds['venues']['min_completeness']})"
                    )

                if psychographic_completeness < 0.5:
                    data_issues.append(
                        f"Low psychographic data coverage: {psychographic_completeness:.2f}"
                    )

                if recency_score < 0.1:
                    data_issues.append("Most venue data is stale (>7 days old)")

            processing_time = (datetime.now() - start_time).total_seconds()

            return QualityReport(
                source_name="venues",
                total_input=total_venues,
                total_output=geocoded_venues,  # Only geocoded venues are usable
                quality_score=quality_score,
                completeness_score=completeness_score,
                validation_errors=validation_errors,
                data_issues=data_issues,
                processing_time=processing_time,
                timestamp=datetime.now(),
            )

        except Exception as e:
            return self._create_error_report("venues", str(e))
        finally:
            cur.close()
            conn.close()

    def _validate_social_sentiment(self) -> QualityReport:
        """Validate social sentiment data quality."""
        start_time = datetime.now()
        validation_errors = []
        data_issues = []

        conn = get_db_conn()
        if not conn:
            return self._create_error_report(
                "social_sentiment", "Database connection failed"
            )

        cur = conn.cursor()

        try:
            # Get social sentiment statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN mention_count > 0 THEN 1 END) as records_with_mentions,
                    COUNT(CASE WHEN positive_sentiment IS NOT NULL THEN 1 END) as records_with_sentiment,
                    COUNT(CASE WHEN ts >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_records,
                    AVG(mention_count) as avg_mentions,
                    AVG(positive_sentiment) as avg_positive_sentiment
                FROM social_sentiment
            """
            )
            stats = cur.fetchone()

            total_records = stats[0] if stats[0] else 0
            records_with_mentions = stats[1] if stats[1] else 0
            records_with_sentiment = stats[2] if stats[2] else 0
            recent_records = stats[3] if stats[3] else 0
            avg_mentions = stats[4] if stats[4] else 0
            avg_positive_sentiment = stats[5] if stats[5] else 0

            # Calculate quality metrics
            if total_records == 0:
                validation_errors.append("No social sentiment data found")
                quality_score = 0.0
                completeness_score = 0.0
            else:
                mention_completeness = records_with_mentions / total_records
                sentiment_completeness = records_with_sentiment / total_records
                recency_score = recent_records / total_records

                completeness_score = (
                    mention_completeness * 0.4
                    + sentiment_completeness * 0.4
                    + recency_score * 0.2
                )

                quality_score = completeness_score

                # Check for issues
                if mention_completeness < 0.5:
                    data_issues.append(
                        f"Low mention coverage: {mention_completeness:.2f}"
                    )

                if avg_mentions < 1:
                    data_issues.append(
                        f"Low average mentions per record: {avg_mentions:.1f}"
                    )

                if avg_positive_sentiment < 0.3:
                    data_issues.append("Unusually low positive sentiment scores")

            processing_time = (datetime.now() - start_time).total_seconds()

            return QualityReport(
                source_name="social_sentiment",
                total_input=total_records,
                total_output=records_with_sentiment,
                quality_score=quality_score,
                completeness_score=completeness_score,
                validation_errors=validation_errors,
                data_issues=data_issues,
                processing_time=processing_time,
                timestamp=datetime.now(),
            )

        except Exception as e:
            return self._create_error_report("social_sentiment", str(e))
        finally:
            cur.close()
            conn.close()

    def _validate_ml_predictions(self) -> QualityReport:
        """Validate ML predictions data quality."""
        start_time = datetime.now()
        validation_errors = []
        data_issues = []

        conn = get_db_conn()
        if not conn:
            return self._create_error_report(
                "ml_predictions", "Database connection failed"
            )

        cur = conn.cursor()

        try:
            # Get ML predictions statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_predictions,
                    COUNT(CASE WHEN psychographic_density IS NOT NULL THEN 1 END) as predictions_with_density,
                    COUNT(CASE WHEN confidence_lower IS NOT NULL AND confidence_upper IS NOT NULL THEN 1 END) as predictions_with_confidence,
                    COUNT(CASE WHEN ts >= NOW() - INTERVAL '7 days' THEN 1 END) as recent_predictions,
                    AVG(psychographic_density) as avg_density,
                    AVG(confidence_upper - confidence_lower) as avg_confidence_interval
                FROM predictions
            """
            )
            stats = cur.fetchone()

            total_predictions = stats[0] if stats[0] else 0
            predictions_with_density = stats[1] if stats[1] else 0
            predictions_with_confidence = stats[2] if stats[2] else 0
            recent_predictions = stats[3] if stats[3] else 0
            avg_density = stats[4] if stats[4] else 0
            avg_confidence_interval = stats[5] if stats[5] else 0

            # Calculate quality metrics
            if total_predictions == 0:
                validation_errors.append("No ML predictions found")
                quality_score = 0.0
                completeness_score = 0.0
            else:
                density_completeness = predictions_with_density / total_predictions
                confidence_completeness = (
                    predictions_with_confidence / total_predictions
                )
                recency_score = recent_predictions / total_predictions

                completeness_score = (
                    density_completeness * 0.5
                    + confidence_completeness * 0.3
                    + recency_score * 0.2
                )

                quality_score = completeness_score

                # Check for issues
                if density_completeness < 0.9:
                    data_issues.append(
                        f"Missing density values: {density_completeness:.2f}"
                    )

                if avg_confidence_interval > 0.5:
                    data_issues.append(
                        f"High prediction uncertainty: {avg_confidence_interval:.2f}"
                    )

                if avg_density < 0.1 or avg_density > 0.9:
                    data_issues.append(f"Unusual average density: {avg_density:.2f}")

            processing_time = (datetime.now() - start_time).total_seconds()

            return QualityReport(
                source_name="ml_predictions",
                total_input=total_predictions,
                total_output=predictions_with_density,
                quality_score=quality_score,
                completeness_score=completeness_score,
                validation_errors=validation_errors,
                data_issues=data_issues,
                processing_time=processing_time,
                timestamp=datetime.now(),
            )

        except Exception as e:
            return self._create_error_report("ml_predictions", str(e))
        finally:
            cur.close()
            conn.close()

    def _validate_weather(self) -> QualityReport:
        """Validate weather data quality."""
        # Simplified validation for secondary data source
        start_time = datetime.now()
        conn = get_db_conn()

        if not conn:
            return self._create_error_report("weather", "Database connection failed")

        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN temperature_f IS NOT NULL THEN 1 END) as records_with_temp,
                    COUNT(CASE WHEN ts >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM weather_data
            """
            )
            stats = cur.fetchone()

            total_records = stats[0] if stats[0] else 0
            records_with_temp = stats[1] if stats[1] else 0
            recent_records = stats[2] if stats[2] else 0

            if total_records == 0:
                quality_score = 0.0
                completeness_score = 0.0
            else:
                completeness_score = records_with_temp / total_records
                recency_score = recent_records / total_records
                quality_score = (completeness_score + recency_score) / 2

            processing_time = (datetime.now() - start_time).total_seconds()

            return QualityReport(
                source_name="weather",
                total_input=total_records,
                total_output=records_with_temp,
                quality_score=quality_score,
                completeness_score=completeness_score,
                validation_errors=[],
                data_issues=[],
                processing_time=processing_time,
                timestamp=datetime.now(),
            )

        except Exception as e:
            return self._create_error_report("weather", str(e))
        finally:
            cur.close()
            conn.close()

    def _validate_traffic(self) -> QualityReport:
        """Validate traffic data quality."""
        # Simplified validation for secondary data source
        start_time = datetime.now()
        conn = get_db_conn()

        if not conn:
            return self._create_error_report("traffic", "Database connection failed")

        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN congestion_score IS NOT NULL THEN 1 END) as records_with_congestion,
                    COUNT(CASE WHEN ts >= NOW() - INTERVAL '24 hours' THEN 1 END) as recent_records
                FROM traffic_data
            """
            )
            stats = cur.fetchone()

            total_records = stats[0] if stats[0] else 0
            records_with_congestion = stats[1] if stats[1] else 0
            recent_records = stats[2] if stats[2] else 0

            if total_records == 0:
                quality_score = 0.0
                completeness_score = 0.0
            else:
                completeness_score = records_with_congestion / total_records
                recency_score = recent_records / total_records
                quality_score = (completeness_score + recency_score) / 2

            processing_time = (datetime.now() - start_time).total_seconds()

            return QualityReport(
                source_name="traffic",
                total_input=total_records,
                total_output=records_with_congestion,
                quality_score=quality_score,
                completeness_score=completeness_score,
                validation_errors=[],
                data_issues=[],
                processing_time=processing_time,
                timestamp=datetime.now(),
            )

        except Exception as e:
            return self._create_error_report("traffic", str(e))
        finally:
            cur.close()
            conn.close()

    def _validate_generic_source(self, source_name: str) -> QualityReport:
        """Generic validation for other data sources."""
        return QualityReport(
            source_name=source_name,
            total_input=0,
            total_output=0,
            quality_score=0.5,  # Neutral score for unknown sources
            completeness_score=0.5,
            validation_errors=[f"No specific validation implemented for {source_name}"],
            data_issues=[],
            processing_time=0.0,
            timestamp=datetime.now(),
        )

    def _create_error_report(
        self, source_name: str, error_message: str
    ) -> QualityReport:
        """Create an error quality report."""
        return QualityReport(
            source_name=source_name,
            total_input=0,
            total_output=0,
            quality_score=0.0,
            completeness_score=0.0,
            validation_errors=[error_message],
            data_issues=[],
            processing_time=0.0,
            timestamp=datetime.now(),
        )

    def _clean_venue_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean venue data using existing venue processing."""
        try:
            # Use existing venue processing pipeline
            processed_venues, quality_report = process_venues_with_quality_checks(
                raw_data
            )
            return processed_venues
        except Exception as e:
            self.logger.error(f"Error cleaning venue data: {e}")
            return raw_data

    def _clean_social_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean social sentiment data."""
        cleaned_data = []

        for record in raw_data:
            # Basic cleaning for social data
            if (
                record.get("mention_count", 0) >= 0
                and record.get("positive_sentiment") is not None
            ):
                # Normalize sentiment scores to 0-1 range
                if "positive_sentiment" in record:
                    record["positive_sentiment"] = max(
                        0, min(1, record["positive_sentiment"])
                    )
                if "negative_sentiment" in record:
                    record["negative_sentiment"] = max(
                        0, min(1, record["negative_sentiment"])
                    )

                cleaned_data.append(record)

        return cleaned_data

    def _clean_ml_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean ML predictions data."""
        cleaned_data = []

        for record in raw_data:
            # Validate ML prediction data
            if (
                record.get("psychographic_density") is not None
                and 0 <= record["psychographic_density"] <= 1
            ):
                # Ensure confidence bounds are valid
                if (
                    record.get("confidence_lower") is not None
                    and record.get("confidence_upper") is not None
                ):
                    record["confidence_lower"] = max(
                        0, min(1, record["confidence_lower"])
                    )
                    record["confidence_upper"] = max(
                        0, min(1, record["confidence_upper"])
                    )

                    # Ensure lower <= upper
                    if record["confidence_lower"] > record["confidence_upper"]:
                        record["confidence_lower"], record["confidence_upper"] = (
                            record["confidence_upper"],
                            record["confidence_lower"],
                        )

                cleaned_data.append(record)

        return cleaned_data

    def _clean_generic_data(self, raw_data: List[Dict], source_type: str) -> List[Dict]:
        """Generic data cleaning."""
        # Basic cleaning - remove records with all null values
        cleaned_data = []

        for record in raw_data:
            if any(value is not None for value in record.values()):
                cleaned_data.append(record)

        return cleaned_data

    def _deduplicate_venues(self, venues: List[Dict]) -> List[Dict]:
        """Deduplicate venue records."""
        seen_venues = set()
        deduplicated = []

        for venue in venues:
            # Create a key based on name and location
            name = venue.get("name", "").lower().strip()
            lat = venue.get("lat")
            lng = venue.get("lng")

            if lat is not None and lng is not None:
                # Round coordinates to avoid minor differences
                key = (name, round(lat, 4), round(lng, 4))
            else:
                key = (name, venue.get("address", "").lower().strip())

            if key not in seen_venues:
                seen_venues.add(key)
                deduplicated.append(venue)

        return deduplicated

    def _deduplicate_social_data(self, social_data: List[Dict]) -> List[Dict]:
        """Deduplicate social sentiment records."""
        seen_records = set()
        deduplicated = []

        for record in social_data:
            # Create key based on venue, platform, and timestamp
            venue_id = record.get("venue_id")
            platform = record.get("platform", "")
            timestamp = record.get("ts")

            key = (venue_id, platform, timestamp)

            if key not in seen_records:
                seen_records.add(key)
                deduplicated.append(record)

        return deduplicated

    def _deduplicate_generic(
        self, data_list: List[Dict], source_type: str
    ) -> List[Dict]:
        """Generic deduplication."""
        # Simple deduplication based on all field values
        seen_records = set()
        deduplicated = []

        for record in data_list:
            # Create a hash of all values
            key = tuple(sorted(record.items()))

            if key not in seen_records:
                seen_records.add(key)
                deduplicated.append(record)

        return deduplicated


if __name__ == "__main__":
    # Test the quality controller
    logging.basicConfig(level=logging.INFO)

    controller = QualityController()

    # Test priority source validation
    print("Testing priority source validation...")
    priority_reports = controller.validate_priority_sources()

    for source, report in priority_reports.items():
        print(f"\n{source.upper()}:")
        print(f"  Quality Score: {report.quality_score:.2f}")
        print(f"  Completeness: {report.completeness_score:.2f}")
        print(f"  Records: {report.total_input} → {report.total_output}")
        if report.data_issues:
            print(f"  Issues: {', '.join(report.data_issues)}")

    # Test overall quality metrics
    print("\nGenerating overall quality metrics...")
    metrics = controller.generate_quality_metrics()
    print(f"Overall Quality Score: {metrics.overall_quality_score:.2f}")
    print(f"Data Completeness: {metrics.data_completeness:.2f}")
    print(f"Priority Sources Health: {metrics.priority_sources_health}")
