# Master Data Aggregator
"""
Master data aggregation service that consolidates all data sources into unified structures.
Creates ConsolidatedVenueData objects from the master_venue_data materialized view,
providing the single source of truth for map generation and other applications.
"""

import sys
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class ConsolidatedVenueData:
    """Single data structure for all venue information from all sources"""

    # Core venue information
    venue_id: str
    name: str
    location: Tuple[float, float]  # (lat, lng)
    category: str
    subcategory: Optional[str] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None

    # All contextual data in one place
    current_weather: Optional[Dict] = None
    traffic_conditions: Optional[Dict] = None
    social_sentiment: Optional[Dict] = None
    economic_context: Optional[Dict] = None
    foot_traffic: Optional[Dict] = None
    ml_predictions: Optional[Dict] = None
    demographic_context: Optional[Dict] = None
    upcoming_events: List[Dict] = None

    # Data quality metrics
    data_completeness: float = 0.0
    last_updated: datetime = None
    source_reliability: Dict[str, float] = None
    data_source_type: str = "unknown"
    comprehensive_score: float = 0.0


@dataclass
class ConsolidatedEventData:
    """Single data structure for all event information"""

    # Core event information
    event_id: str
    name: str
    description: Optional[str]
    category: str
    subcategory: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]

    # Venue information
    venue_id: str
    venue_name: str
    venue_location: Tuple[float, float]  # (lat, lng)
    venue_address: Optional[str]

    # Event context
    predicted_attendance: Optional[int]
    psychographic_relevance: Optional[Dict]
    social_sentiment: Optional[Dict]
    event_score: float = 0.0

    # Data quality
    data_source_type: str = "unknown"
    last_updated: datetime = None


@dataclass
class AggregationResult:
    """Result of data aggregation operation"""

    venues_aggregated: int
    events_aggregated: int
    data_completeness_avg: float
    processing_time_seconds: float
    area_bounds: Dict
    time_period: timedelta
    timestamp: datetime
    quality_summary: Dict


class MasterDataAggregator:
    """
    Master data aggregator that consolidates all data sources into unified structures.

    This class transforms data from the master_venue_data and master_events_data
    materialized views into clean, standardized ConsolidatedVenueData and
    ConsolidatedEventData objects for consumption by map generation and other applications.
    """

    def __init__(self):
        """Initialize the master data aggregator."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

        # Kansas City default bounds
        self.default_kc_bounds = {
            "north": 39.3,
            "south": 38.9,
            "east": -94.3,
            "west": -94.8,
        }

    def aggregate_venue_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> List[ConsolidatedVenueData]:
        """
        Aggregate venue data from master_venue_data materialized view.

        Args:
            area_bounds: Geographic bounds for filtering (defaults to KC)
            time_period: Time period for data freshness (not used for venues)

        Returns:
            List of ConsolidatedVenueData objects with all contextual information
        """
        start_time = datetime.now()

        if area_bounds is None:
            area_bounds = self.default_kc_bounds

        self.logger.info(f"🏢 Aggregating venue data for bounds: {area_bounds}")

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return []

        cur = conn.cursor()

        try:
            # Query master_venue_data materialized view with area bounds
            query = """
                SELECT 
                    venue_id,
                    name,
                    category,
                    subcategory,
                    lat,
                    lng,
                    address,
                    phone,
                    website,
                    psychographic_relevance,
                    
                    -- Social sentiment data
                    mention_count,
                    positive_sentiment,
                    negative_sentiment,
                    neutral_sentiment,
                    engagement_score,
                    psychographic_keywords,
                    social_last_updated,
                    
                    -- ML predictions
                    psychographic_density,
                    confidence_lower,
                    confidence_upper,
                    model_version,
                    contributing_factors,
                    predictions_last_updated,
                    
                    -- Foot traffic data
                    visitors_count,
                    median_dwell_seconds,
                    visitors_change_24h,
                    visitors_change_7d,
                    peak_hour_ratio,
                    traffic_last_updated,
                    
                    -- Traffic congestion
                    congestion_score,
                    travel_time_to_downtown,
                    travel_time_index,
                    congestion_last_updated,
                    
                    -- Weather data
                    temperature_f,
                    feels_like_f,
                    humidity,
                    rain_probability,
                    weather_condition,
                    weather_description,
                    weather_last_updated,
                    
                    -- Economic data
                    unemployment_rate,
                    median_household_income,
                    consumer_confidence,
                    local_spending_index,
                    economic_last_updated,
                    
                    -- Demographics
                    median_income,
                    median_income_z,
                    pct_bachelors,
                    pct_graduate,
                    pct_age_20_40,
                    population_density,
                    pct_professional_occupation,
                    
                    -- Quality metrics
                    data_completeness_score,
                    comprehensive_score,
                    data_source_type,
                    last_refreshed
                    
                FROM master_venue_data
                WHERE lat BETWEEN %s AND %s
                AND lng BETWEEN %s AND %s
                ORDER BY data_completeness_score DESC, comprehensive_score DESC
            """

            cur.execute(
                query,
                (
                    area_bounds["south"],
                    area_bounds["north"],
                    area_bounds["west"],
                    area_bounds["east"],
                ),
            )

            venue_records = cur.fetchall()

            # Get column names for easier access
            column_names = [desc[0] for desc in cur.description]

            venues = []
            for record in venue_records:
                venue_dict = dict(zip(column_names, record))
                consolidated_venue = self._create_consolidated_venue(venue_dict)
                if consolidated_venue:
                    venues.append(consolidated_venue)

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"✅ Aggregated {len(venues)} venues in {processing_time:.2f}s"
            )

            return venues

        except Exception as e:
            self.logger.error(f"Error aggregating venue data: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def aggregate_event_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> List[ConsolidatedEventData]:
        """
        Aggregate event data from master_events_data materialized view.

        Args:
            area_bounds: Geographic bounds for filtering (defaults to KC)
            time_period: Time period for events (defaults to next 30 days)

        Returns:
            List of ConsolidatedEventData objects
        """
        start_time = datetime.now()

        if area_bounds is None:
            area_bounds = self.default_kc_bounds
        if time_period is None:
            time_period = timedelta(days=30)

        self.logger.info(f"📅 Aggregating event data for {time_period.days} days")

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return []

        cur = conn.cursor()

        try:
            # Query master_events_data materialized view
            query = """
                SELECT 
                    event_id,
                    name,
                    description,
                    category,
                    subcategory,
                    start_time,
                    end_time,
                    predicted_attendance,
                    psychographic_relevance,
                    
                    -- Venue information
                    venue_name,
                    lat,
                    lng,
                    venue_address,
                    venue_category,
                    
                    -- Social sentiment
                    mention_count,
                    positive_sentiment,
                    engagement_score,
                    
                    -- Quality metrics
                    event_score,
                    data_source_type,
                    last_refreshed
                    
                FROM master_events_data
                WHERE lat BETWEEN %s AND %s
                AND lng BETWEEN %s AND %s
                AND (start_time IS NULL OR start_time BETWEEN NOW() AND NOW() + INTERVAL '%s days')
                ORDER BY 
                    COALESCE(start_time, NOW() + INTERVAL '1 year') ASC,
                    event_score DESC
            """

            cur.execute(
                query,
                (
                    area_bounds["south"],
                    area_bounds["north"],
                    area_bounds["west"],
                    area_bounds["east"],
                    time_period.days,
                ),
            )

            event_records = cur.fetchall()

            # Get column names for easier access
            column_names = [desc[0] for desc in cur.description]

            events = []
            for record in event_records:
                event_dict = dict(zip(column_names, record))
                consolidated_event = self._create_consolidated_event(event_dict)
                if consolidated_event:
                    events.append(consolidated_event)

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"✅ Aggregated {len(events)} events in {processing_time:.2f}s"
            )

            return events

        except Exception as e:
            self.logger.error(f"Error aggregating event data: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def aggregate_area_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> Tuple[List[ConsolidatedVenueData], List[ConsolidatedEventData]]:
        """
        Aggregate both venue and event data for a specific area.

        Args:
            area_bounds: Geographic bounds for filtering (defaults to KC)
            time_period: Time period for events (defaults to next 30 days)

        Returns:
            Tuple of (venues, events) as consolidated data objects
        """
        start_time = datetime.now()

        self.logger.info("🌍 Aggregating complete area data (venues + events)")

        # Aggregate venues and events in parallel
        venues = self.aggregate_venue_data(area_bounds, time_period)
        events = self.aggregate_event_data(area_bounds, time_period)

        # Link events to venues where possible
        self._link_events_to_venues(venues, events)

        processing_time = (datetime.now() - start_time).total_seconds()

        self.logger.info(
            f"✅ Complete area aggregation: {len(venues)} venues, "
            f"{len(events)} events in {processing_time:.2f}s"
        )

        return venues, events

    def refresh_aggregations(self, force_refresh: bool = False) -> AggregationResult:
        """
        Refresh the underlying materialized views and return aggregation status.

        Args:
            force_refresh: Whether to force refresh even if recently updated

        Returns:
            AggregationResult with refresh statistics
        """
        start_time = datetime.now()

        self.logger.info("🔄 Refreshing master data aggregations")

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return self._create_error_result("Database connection failed")

        cur = conn.cursor()

        try:
            # Check if refresh is needed (unless forced)
            if not force_refresh:
                cur.execute(
                    """
                    SELECT last_refreshed 
                    FROM master_venue_data 
                    LIMIT 1
                """
                )
                result = cur.fetchone()
                if result and result[0]:
                    last_refresh = result[0]
                    if datetime.now() - last_refresh < timedelta(hours=1):
                        self.logger.info("Skipping refresh - data is recent")
                        return self._get_current_aggregation_status()

            # Refresh materialized views
            self.logger.info("Refreshing master_venue_data materialized view...")
            cur.execute("REFRESH MATERIALIZED VIEW master_venue_data")

            self.logger.info("Refreshing master_events_data materialized view...")
            cur.execute("REFRESH MATERIALIZED VIEW master_events_data")

            conn.commit()

            # Get post-refresh statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as venue_count,
                    AVG(data_completeness_score) as avg_completeness
                FROM master_venue_data
            """
            )
            venue_stats = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM master_events_data")
            event_count = cur.fetchone()[0]

            processing_time = (datetime.now() - start_time).total_seconds()

            # Generate quality summary
            quality_reports = self.quality_controller.validate_priority_sources()
            quality_summary = {
                source: {
                    "quality_score": report.quality_score,
                    "completeness": report.completeness_score,
                }
                for source, report in quality_reports.items()
            }

            result = AggregationResult(
                venues_aggregated=venue_stats[0] if venue_stats[0] else 0,
                events_aggregated=event_count if event_count else 0,
                data_completeness_avg=venue_stats[1] if venue_stats[1] else 0.0,
                processing_time_seconds=processing_time,
                area_bounds=self.default_kc_bounds,
                time_period=timedelta(days=30),
                timestamp=datetime.now(),
                quality_summary=quality_summary,
            )

            self.logger.info(
                f"✅ Aggregation refresh completed: {result.venues_aggregated} venues, "
                f"{result.events_aggregated} events, "
                f"avg completeness: {result.data_completeness_avg:.2f}"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error refreshing aggregations: {e}")
            return self._create_error_result(str(e))
        finally:
            cur.close()
            conn.close()

    def get_aggregation_health_status(self) -> Dict:
        """
        Get comprehensive health status of the aggregation system.

        Returns:
            Dictionary with health metrics and status information
        """
        self.logger.info("📊 Generating aggregation health status")

        conn = get_db_conn()
        if not conn:
            return {"error": "Database connection failed"}

        cur = conn.cursor()

        try:
            # Get materialized view statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_venues,
                    COUNT(CASE WHEN data_completeness_score >= 0.8 THEN 1 END) as high_quality_venues,
                    COUNT(CASE WHEN data_completeness_score >= 0.6 THEN 1 END) as medium_quality_venues,
                    AVG(data_completeness_score) as avg_completeness,
                    AVG(comprehensive_score) as avg_comprehensive_score,
                    MAX(last_refreshed) as last_refresh
                FROM master_venue_data
            """
            )
            venue_stats = cur.fetchone()

            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN start_time >= NOW() THEN 1 END) as future_events,
                    AVG(event_score) as avg_event_score
                FROM master_events_data
            """
            )
            event_stats = cur.fetchone()

            # Get data source distribution
            cur.execute(
                """
                SELECT data_source_type, COUNT(*) 
                FROM master_venue_data 
                GROUP BY data_source_type
            """
            )
            source_distribution = dict(cur.fetchall())

            # Calculate health scores
            total_venues = venue_stats[0] if venue_stats[0] else 0
            high_quality_venues = venue_stats[1] if venue_stats[1] else 0
            avg_completeness = venue_stats[3] if venue_stats[3] else 0.0

            quality_ratio = high_quality_venues / max(total_venues, 1)

            # Overall health score (weighted)
            overall_health = (
                avg_completeness * 0.4  # Data completeness
                + quality_ratio * 0.3  # High quality venue ratio
                + min(total_venues / 100, 1.0) * 0.2  # Volume score
                + (
                    1.0
                    if venue_stats[5]
                    and (datetime.now() - venue_stats[5]).total_seconds() < 3600
                    else 0.5
                )
                * 0.1  # Freshness
            )

            health_status = {
                "timestamp": datetime.now().isoformat(),
                "overall_health_score": overall_health,
                "venue_statistics": {
                    "total_venues": total_venues,
                    "high_quality_venues": high_quality_venues,
                    "medium_quality_venues": venue_stats[2] if venue_stats[2] else 0,
                    "avg_completeness": avg_completeness,
                    "avg_comprehensive_score": (
                        venue_stats[4] if venue_stats[4] else 0.0
                    ),
                    "quality_ratio": quality_ratio,
                },
                "event_statistics": {
                    "total_events": event_stats[0] if event_stats[0] else 0,
                    "future_events": event_stats[1] if event_stats[1] else 0,
                    "avg_event_score": event_stats[2] if event_stats[2] else 0.0,
                },
                "data_source_distribution": source_distribution,
                "last_refresh": venue_stats[5].isoformat() if venue_stats[5] else None,
                "refresh_needed": (
                    not venue_stats[5]
                    or (datetime.now() - venue_stats[5]).total_seconds() > 3600
                ),
            }

            return health_status

        except Exception as e:
            self.logger.error(f"Error generating health status: {e}")
            return {"error": str(e)}
        finally:
            cur.close()
            conn.close()

    def _create_consolidated_venue(
        self, venue_dict: Dict
    ) -> Optional[ConsolidatedVenueData]:
        """Create ConsolidatedVenueData object from database record."""
        try:
            # Parse psychographic relevance JSON
            psychographic_relevance = None
            if venue_dict.get("psychographic_relevance"):
                try:
                    psychographic_relevance = json.loads(
                        venue_dict["psychographic_relevance"]
                    )
                except (json.JSONDecodeError, TypeError):
                    psychographic_relevance = venue_dict["psychographic_relevance"]

            # Parse contributing factors JSON
            contributing_factors = None
            if venue_dict.get("contributing_factors"):
                try:
                    contributing_factors = json.loads(
                        venue_dict["contributing_factors"]
                    )
                except (json.JSONDecodeError, TypeError):
                    contributing_factors = venue_dict["contributing_factors"]

            # Build consolidated data structure
            consolidated_venue = ConsolidatedVenueData(
                venue_id=str(venue_dict["venue_id"]),
                name=venue_dict["name"],
                location=(venue_dict["lat"], venue_dict["lng"]),
                category=venue_dict["category"] or "unknown",
                subcategory=venue_dict.get("subcategory"),
                address=venue_dict.get("address"),
                phone=venue_dict.get("phone"),
                website=venue_dict.get("website"),
                # Weather context
                current_weather=(
                    {
                        "temperature_f": venue_dict.get("temperature_f"),
                        "feels_like_f": venue_dict.get("feels_like_f"),
                        "humidity": venue_dict.get("humidity"),
                        "rain_probability": venue_dict.get("rain_probability"),
                        "condition": venue_dict.get("weather_condition"),
                        "description": venue_dict.get("weather_description"),
                        "last_updated": venue_dict.get("weather_last_updated"),
                    }
                    if venue_dict.get("temperature_f") is not None
                    else None
                ),
                # Traffic conditions
                traffic_conditions=(
                    {
                        "congestion_score": venue_dict.get("congestion_score"),
                        "travel_time_to_downtown": venue_dict.get(
                            "travel_time_to_downtown"
                        ),
                        "travel_time_index": venue_dict.get("travel_time_index"),
                        "last_updated": venue_dict.get("congestion_last_updated"),
                    }
                    if venue_dict.get("congestion_score") is not None
                    else None
                ),
                # Social sentiment
                social_sentiment=(
                    {
                        "mention_count": venue_dict.get("mention_count"),
                        "positive_sentiment": venue_dict.get("positive_sentiment"),
                        "negative_sentiment": venue_dict.get("negative_sentiment"),
                        "neutral_sentiment": venue_dict.get("neutral_sentiment"),
                        "engagement_score": venue_dict.get("engagement_score"),
                        "psychographic_keywords": venue_dict.get(
                            "psychographic_keywords"
                        ),
                        "last_updated": venue_dict.get("social_last_updated"),
                    }
                    if venue_dict.get("mention_count") is not None
                    else None
                ),
                # Economic context
                economic_context=(
                    {
                        "unemployment_rate": venue_dict.get("unemployment_rate"),
                        "median_household_income": venue_dict.get(
                            "median_household_income"
                        ),
                        "consumer_confidence": venue_dict.get("consumer_confidence"),
                        "local_spending_index": venue_dict.get("local_spending_index"),
                        "last_updated": venue_dict.get("economic_last_updated"),
                    }
                    if venue_dict.get("unemployment_rate") is not None
                    else None
                ),
                # Foot traffic
                foot_traffic=(
                    {
                        "visitors_count": venue_dict.get("visitors_count"),
                        "median_dwell_seconds": venue_dict.get("median_dwell_seconds"),
                        "visitors_change_24h": venue_dict.get("visitors_change_24h"),
                        "visitors_change_7d": venue_dict.get("visitors_change_7d"),
                        "peak_hour_ratio": venue_dict.get("peak_hour_ratio"),
                        "last_updated": venue_dict.get("traffic_last_updated"),
                    }
                    if venue_dict.get("visitors_count") is not None
                    else None
                ),
                # ML predictions
                ml_predictions=(
                    {
                        "psychographic_density": venue_dict.get(
                            "psychographic_density"
                        ),
                        "confidence_lower": venue_dict.get("confidence_lower"),
                        "confidence_upper": venue_dict.get("confidence_upper"),
                        "model_version": venue_dict.get("model_version"),
                        "contributing_factors": contributing_factors,
                        "last_updated": venue_dict.get("predictions_last_updated"),
                    }
                    if venue_dict.get("psychographic_density") is not None
                    else None
                ),
                # Demographic context
                demographic_context=(
                    {
                        "median_income": venue_dict.get("median_income"),
                        "median_income_z": venue_dict.get("median_income_z"),
                        "pct_bachelors": venue_dict.get("pct_bachelors"),
                        "pct_graduate": venue_dict.get("pct_graduate"),
                        "pct_age_20_40": venue_dict.get("pct_age_20_40"),
                        "population_density": venue_dict.get("population_density"),
                        "pct_professional_occupation": venue_dict.get(
                            "pct_professional_occupation"
                        ),
                    }
                    if venue_dict.get("median_income") is not None
                    else None
                ),
                # Initialize empty events list (will be populated by linking)
                upcoming_events=[],
                # Quality metrics
                data_completeness=venue_dict.get("data_completeness_score", 0.0),
                last_updated=venue_dict.get("last_refreshed", datetime.now()),
                source_reliability={
                    "venue_data": 1.0,  # Venue data is always reliable
                    "social_sentiment": 0.8 if venue_dict.get("mention_count") else 0.0,
                    "ml_predictions": (
                        0.9 if venue_dict.get("psychographic_density") else 0.0
                    ),
                    "weather": 0.9 if venue_dict.get("temperature_f") else 0.0,
                    "traffic": 0.7 if venue_dict.get("congestion_score") else 0.0,
                    "foot_traffic": 0.6 if venue_dict.get("visitors_count") else 0.0,
                    "economic": 0.8 if venue_dict.get("unemployment_rate") else 0.0,
                    "demographic": 0.9 if venue_dict.get("median_income") else 0.0,
                },
                data_source_type=venue_dict.get("data_source_type", "unknown"),
                comprehensive_score=venue_dict.get("comprehensive_score", 0.0),
            )

            return consolidated_venue

        except Exception as e:
            self.logger.error(f"Error creating consolidated venue: {e}")
            return None

    def _create_consolidated_event(
        self, event_dict: Dict
    ) -> Optional[ConsolidatedEventData]:
        """Create ConsolidatedEventData object from database record."""
        try:
            # Parse psychographic relevance JSON
            psychographic_relevance = None
            if event_dict.get("psychographic_relevance"):
                try:
                    psychographic_relevance = json.loads(
                        event_dict["psychographic_relevance"]
                    )
                except (json.JSONDecodeError, TypeError):
                    psychographic_relevance = event_dict["psychographic_relevance"]

            consolidated_event = ConsolidatedEventData(
                event_id=str(event_dict["event_id"]),
                name=event_dict["name"],
                description=event_dict.get("description"),
                category=event_dict["category"] or "unknown",
                subcategory=event_dict.get("subcategory"),
                start_time=event_dict.get("start_time"),
                end_time=event_dict.get("end_time"),
                # Venue information (from join)
                venue_id="",  # Will be populated during linking
                venue_name=event_dict["venue_name"],
                venue_location=(event_dict["lat"], event_dict["lng"]),
                venue_address=event_dict.get("venue_address"),
                # Event context
                predicted_attendance=event_dict.get("predicted_attendance"),
                psychographic_relevance=psychographic_relevance,
                social_sentiment=(
                    {
                        "mention_count": event_dict.get("mention_count"),
                        "positive_sentiment": event_dict.get("positive_sentiment"),
                        "engagement_score": event_dict.get("engagement_score"),
                    }
                    if event_dict.get("mention_count") is not None
                    else None
                ),
                event_score=event_dict.get("event_score", 0.0),
                # Quality metrics
                data_source_type=event_dict.get("data_source_type", "unknown"),
                last_updated=event_dict.get("last_refreshed", datetime.now()),
            )

            return consolidated_event

        except Exception as e:
            self.logger.error(f"Error creating consolidated event: {e}")
            return None

    def _link_events_to_venues(
        self, venues: List[ConsolidatedVenueData], events: List[ConsolidatedEventData]
    ):
        """Link events to their corresponding venues."""
        # Create venue lookup by name and location
        venue_lookup = {}
        for venue in venues:
            # Create multiple lookup keys for better matching
            keys = [
                venue.name.lower().strip(),
                f"{venue.location[0]:.4f},{venue.location[1]:.4f}",  # lat,lng key
            ]
            for key in keys:
                if key not in venue_lookup:
                    venue_lookup[key] = venue

        # Link events to venues
        for event in events:
            venue = None

            # Try to match by venue name
            venue_name_key = event.venue_name.lower().strip()
            if venue_name_key in venue_lookup:
                venue = venue_lookup[venue_name_key]
            else:
                # Try to match by location (within 100m)
                for v in venues:
                    distance = self._calculate_distance(
                        event.venue_location, v.location
                    )
                    if distance < 0.1:  # 100m threshold
                        venue = v
                        break

            if venue:
                # Link event to venue
                event.venue_id = venue.venue_id

                # Add event to venue's upcoming events
                event_summary = {
                    "event_id": event.event_id,
                    "name": event.name,
                    "category": event.category,
                    "start_time": (
                        event.start_time.isoformat() if event.start_time else None
                    ),
                    "predicted_attendance": event.predicted_attendance,
                    "event_score": event.event_score,
                }
                venue.upcoming_events.append(event_summary)

    def _calculate_distance(
        self, location1: Tuple[float, float], location2: Tuple[float, float]
    ) -> float:
        """Calculate distance between two lat/lng points in kilometers."""
        import math

        lat1, lng1 = location1
        lat2, lng2 = location2

        # Convert to radians
        lat1_rad = math.radians(lat1)
        lng1_rad = math.radians(lng1)
        lat2_rad = math.radians(lat2)
        lng2_rad = math.radians(lng2)

        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlng = lng2_rad - lng1_rad

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Earth's radius in kilometers
        earth_radius = 6371.0

        return earth_radius * c

    def _create_error_result(self, error_message: str) -> AggregationResult:
        """Create an error aggregation result."""
        return AggregationResult(
            venues_aggregated=0,
            events_aggregated=0,
            data_completeness_avg=0.0,
            processing_time_seconds=0.0,
            area_bounds=self.default_kc_bounds,
            time_period=timedelta(days=30),
            timestamp=datetime.now(),
            quality_summary={"error": error_message},
        )

    def _get_current_aggregation_status(self) -> AggregationResult:
        """Get current aggregation status without refresh."""
        conn = get_db_conn()
        if not conn:
            return self._create_error_result("Database connection failed")

        cur = conn.cursor()

        try:
            # Get current statistics
            cur.execute(
                """
                SELECT 
                    COUNT(*) as venue_count,
                    AVG(data_completeness_score) as avg_completeness
                FROM master_venue_data
            """
            )
            venue_stats = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM master_events_data")
            event_count = cur.fetchone()[0]

            # Generate quality summary
            quality_reports = self.quality_controller.validate_priority_sources()
            quality_summary = {
                source: {
                    "quality_score": report.quality_score,
                    "completeness": report.completeness_score,
                }
                for source, report in quality_reports.items()
            }

            return AggregationResult(
                venues_aggregated=venue_stats[0] if venue_stats[0] else 0,
                events_aggregated=event_count if event_count else 0,
                data_completeness_avg=venue_stats[1] if venue_stats[1] else 0.0,
                processing_time_seconds=0.0,
                area_bounds=self.default_kc_bounds,
                time_period=timedelta(days=30),
                timestamp=datetime.now(),
                quality_summary=quality_summary,
            )

        except Exception as e:
            return self._create_error_result(str(e))
        finally:
            cur.close()
            conn.close()


# Convenience functions for backward compatibility
def aggregate_venue_data(area_bounds=None, time_period=None):
    """Convenience function to aggregate venue data."""
    aggregator = MasterDataAggregator()
    return aggregator.aggregate_venue_data(area_bounds, time_period)


def aggregate_area_data(area_bounds=None, time_period=None):
    """Convenience function to aggregate both venues and events."""
    aggregator = MasterDataAggregator()
    return aggregator.aggregate_area_data(area_bounds, time_period)


def refresh_master_data(force_refresh=False):
    """Convenience function to refresh master data aggregations."""
    aggregator = MasterDataAggregator()
    return aggregator.refresh_aggregations(force_refresh)


if __name__ == "__main__":
    # Test the aggregator
    import logging

    logging.basicConfig(level=logging.INFO)

    aggregator = MasterDataAggregator()

    # Test venue aggregation
    print("Testing venue data aggregation...")
    venues = aggregator.aggregate_venue_data()
    print(f"Aggregated {len(venues)} venues")

    if venues:
        sample_venue = venues[0]
        print(f"\nSample venue: {sample_venue.name}")
        print(f"Location: {sample_venue.location}")
        print(f"Data completeness: {sample_venue.data_completeness:.2f}")
        print(f"Has weather data: {sample_venue.current_weather is not None}")
        print(f"Has ML predictions: {sample_venue.ml_predictions is not None}")
        print(f"Has social sentiment: {sample_venue.social_sentiment is not None}")

    # Test event aggregation
    print("\nTesting event data aggregation...")
    events = aggregator.aggregate_event_data()
    print(f"Aggregated {len(events)} events")

    if events:
        sample_event = events[0]
        print(f"\nSample event: {sample_event.name}")
        print(f"Venue: {sample_event.venue_name}")
        print(f"Start time: {sample_event.start_time}")
        print(f"Event score: {sample_event.event_score:.2f}")

    # Test complete area aggregation
    print("\nTesting complete area aggregation...")
    venues, events = aggregator.aggregate_area_data()
    print(f"Complete aggregation: {len(venues)} venues, {len(events)} events")

    # Test health status
    print("\nTesting health status...")
    health = aggregator.get_aggregation_health_status()
    print(f"Overall health score: {health.get('overall_health_score', 0):.2f}")
    print(f"Total venues: {health.get('venue_statistics', {}).get('total_venues', 0)}")
