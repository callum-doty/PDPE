"""
Unified Database Interface for PPM Application

Single database interface that consolidates all database operations,
replacing scattered DB access patterns throughout the application.
"""

import logging
import sqlite3
import psycopg2
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
import json
import os
from datetime import datetime


@dataclass
class OperationResult:
    """Unified result type for all operations"""

    success: bool
    data: Any = None
    message: str = ""
    error: Optional[str] = None


class Database:
    """
    Single database interface for the entire PPM application.

    Handles both SQLite (development) and PostgreSQL (production) with
    a unified API that eliminates the need for scattered database operations.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._connection = None
        self._db_type = self._detect_database_type()

    def _detect_database_type(self) -> str:
        """Detect whether to use SQLite or PostgreSQL"""
        if os.getenv("DATABASE_URL") or os.getenv("POSTGRES_HOST"):
            return "postgresql"
        return "sqlite"

    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup"""
        conn = None
        try:
            if self._db_type == "postgresql":
                conn = self._get_postgres_connection()
            else:
                conn = self._get_sqlite_connection()

            yield conn

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _get_postgres_connection(self):
        """Get PostgreSQL connection"""
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            return psycopg2.connect(database_url)

        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", 5432),
            database=os.getenv("POSTGRES_DB", "ppm"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )

    def _get_sqlite_connection(self):
        """Get SQLite connection"""
        db_path = os.getenv("SQLITE_DB_PATH", "ppm.db")
        return sqlite3.connect(db_path)

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a SELECT query and return results as list of dictionaries"""
        try:
            with self.get_connection() as conn:
                if self._db_type == "postgresql":
                    cursor = conn.cursor()
                    cursor.execute(query, params or ())
                    columns = [desc[0] for desc in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                else:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(query, params or ())
                    results = [dict(row) for row in cursor.fetchall()]

                return results

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return []

    def execute_update(self, query: str, params: tuple = None) -> OperationResult:
        """Execute an INSERT/UPDATE/DELETE query"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())
                conn.commit()

                return OperationResult(
                    success=True,
                    data=cursor.rowcount,
                    message=f"Successfully affected {cursor.rowcount} rows",
                )

        except Exception as e:
            self.logger.error(f"Update execution failed: {e}")
            return OperationResult(
                success=False, error=str(e), message=f"Database update failed: {e}"
            )

    # ========== VENUE OPERATIONS ==========

    def get_venues(
        self, filters: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """Get venues with optional filtering"""
        query = """
            SELECT venue_id, external_id, provider, name, description, category, subcategory,
                   lat, lng, address, phone, website, avg_rating, psychographic_relevance,
                   created_at, updated_at
            FROM venues
            WHERE 1=1
        """
        params = []

        if filters:
            if filters.get("category"):
                query += " AND category = ?"
                params.append(filters["category"])

            if filters.get("has_location"):
                query += " AND lat IS NOT NULL AND lng IS NOT NULL"

            if filters.get("min_rating"):
                query += " AND avg_rating >= ?"
                params.append(filters["min_rating"])

        if limit:
            query += f" LIMIT {limit}"

        return self.execute_query(query, tuple(params))

    def get_venues_with_predictions(self) -> List[Dict]:
        """Get venues with their ML predictions - optimized for map display"""
        query = """
            SELECT v.venue_id, v.name, v.lat, v.lng, v.category, v.avg_rating,
                   v.address, v.psychographic_relevance,
                   p.prediction_value, p.confidence_score
            FROM venues v
            LEFT JOIN ml_predictions p ON v.venue_id = p.venue_id
            WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
            ORDER BY COALESCE(p.prediction_value, 0) DESC
        """
        return self.execute_query(query)

    def upsert_venue(self, venue_data: Dict) -> OperationResult:
        """Insert or update a venue"""
        try:
            # Check if venue exists
            existing = self.execute_query(
                "SELECT venue_id FROM venues WHERE external_id = ? AND provider = ?",
                (venue_data.get("external_id"), venue_data.get("provider")),
            )

            if existing:
                # Update existing venue
                query = """
                    UPDATE venues SET
                        name = ?, description = ?, category = ?, subcategory = ?,
                        lat = ?, lng = ?, address = ?, phone = ?, website = ?,
                        avg_rating = ?, psychographic_relevance = ?, updated_at = ?
                    WHERE venue_id = ?
                """
                params = (
                    venue_data.get("name"),
                    venue_data.get("description"),
                    venue_data.get("category"),
                    venue_data.get("subcategory"),
                    venue_data.get("lat"),
                    venue_data.get("lng"),
                    venue_data.get("address"),
                    venue_data.get("phone"),
                    venue_data.get("website"),
                    venue_data.get("avg_rating"),
                    (
                        json.dumps(venue_data.get("psychographic_relevance"))
                        if venue_data.get("psychographic_relevance")
                        else None
                    ),
                    datetime.now(),
                    existing[0]["venue_id"],
                )

                result = self.execute_update(query, params)
                result.message = f"Updated venue: {venue_data.get('name')}"
                return result
            else:
                # Insert new venue
                query = """
                    INSERT INTO venues (
                        external_id, provider, name, description, category, subcategory,
                        lat, lng, address, phone, website, avg_rating, psychographic_relevance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    venue_data.get("external_id"),
                    venue_data.get("provider"),
                    venue_data.get("name"),
                    venue_data.get("description"),
                    venue_data.get("category"),
                    venue_data.get("subcategory"),
                    venue_data.get("lat"),
                    venue_data.get("lng"),
                    venue_data.get("address"),
                    venue_data.get("phone"),
                    venue_data.get("website"),
                    venue_data.get("avg_rating"),
                    (
                        json.dumps(venue_data.get("psychographic_relevance"))
                        if venue_data.get("psychographic_relevance")
                        else None
                    ),
                )

                result = self.execute_update(query, params)
                result.message = f"Inserted new venue: {venue_data.get('name')}"
                return result

        except Exception as e:
            return OperationResult(
                success=False, error=str(e), message=f"Failed to upsert venue: {e}"
            )

    # ========== EVENT OPERATIONS ==========

    def get_events(
        self, filters: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """Get events with optional filtering"""
        query = """
            SELECT e.event_id, e.external_id, e.provider, e.name, e.description,
                   e.category, e.subcategory, e.start_time, e.end_time,
                   e.psychographic_relevance, e.created_at,
                   v.name as venue_name, v.lat, v.lng, v.address
            FROM events e
            LEFT JOIN venues v ON e.venue_id = v.venue_id
            WHERE 1=1
        """
        params = []

        if filters:
            if filters.get("category"):
                query += " AND e.category = ?"
                params.append(filters["category"])

            if filters.get("start_date"):
                query += " AND e.start_time >= ?"
                params.append(filters["start_date"])

            if filters.get("end_date"):
                query += " AND e.start_time <= ?"
                params.append(filters["end_date"])

            if filters.get("has_location"):
                query += " AND v.lat IS NOT NULL AND v.lng IS NOT NULL"

        query += " ORDER BY e.start_time DESC"

        if limit:
            query += f" LIMIT {limit}"

        return self.execute_query(query, tuple(params))

    def upsert_event(self, event_data: Dict) -> OperationResult:
        """Insert or update an event"""
        try:
            # Find or create venue
            venue_id = self._find_or_create_venue_for_event(event_data)
            if not venue_id:
                return OperationResult(
                    success=False,
                    error="Could not find or create venue for event",
                    message=f"Failed to process venue for event: {event_data.get('name')}",
                )

            # Check if event exists
            existing = self.execute_query(
                "SELECT event_id FROM events WHERE external_id = ? AND provider = ?",
                (event_data.get("external_id"), event_data.get("provider")),
            )

            if existing:
                # Update existing event
                query = """
                    UPDATE events SET
                        name = ?, description = ?, category = ?, subcategory = ?,
                        start_time = ?, end_time = ?, venue_id = ?,
                        psychographic_relevance = ?, updated_at = ?
                    WHERE event_id = ?
                """
                params = (
                    event_data.get("name"),
                    event_data.get("description"),
                    event_data.get("category"),
                    event_data.get("subcategory"),
                    event_data.get("start_time"),
                    event_data.get("end_time"),
                    venue_id,
                    (
                        json.dumps(event_data.get("psychographic_relevance"))
                        if event_data.get("psychographic_relevance")
                        else None
                    ),
                    datetime.now(),
                    existing[0]["event_id"],
                )

                result = self.execute_update(query, params)
                result.message = f"Updated event: {event_data.get('name')}"
                return result
            else:
                # Insert new event
                query = """
                    INSERT INTO events (
                        external_id, provider, name, description, category, subcategory,
                        start_time, end_time, venue_id, psychographic_relevance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    event_data.get("external_id"),
                    event_data.get("provider"),
                    event_data.get("name"),
                    event_data.get("description"),
                    event_data.get("category"),
                    event_data.get("subcategory"),
                    event_data.get("start_time"),
                    event_data.get("end_time"),
                    venue_id,
                    (
                        json.dumps(event_data.get("psychographic_relevance"))
                        if event_data.get("psychographic_relevance")
                        else None
                    ),
                )

                result = self.execute_update(query, params)
                result.message = f"Inserted new event: {event_data.get('name')}"
                return result

        except Exception as e:
            return OperationResult(
                success=False, error=str(e), message=f"Failed to upsert event: {e}"
            )

    def _find_or_create_venue_for_event(self, event_data: Dict) -> Optional[str]:
        """Find existing venue or create a new one for an event"""
        venue_name = event_data.get("venue_name")
        if not venue_name:
            return None

        # Try to find existing venue
        existing = self.execute_query(
            "SELECT venue_id FROM venues WHERE LOWER(name) = LOWER(?) LIMIT 1",
            (venue_name,),
        )

        if existing:
            return existing[0]["venue_id"]

        # Create new venue
        venue_data = {
            "external_id": f"{event_data.get('provider', 'unknown')}_{venue_name.lower().replace(' ', '_')}",
            "provider": event_data.get("provider", "unknown"),
            "name": venue_name,
            "category": "event_venue",
            "lat": event_data.get("lat"),
            "lng": event_data.get("lng"),
            "address": event_data.get("address"),
        }

        result = self.upsert_venue(venue_data)
        if result.success:
            # Get the newly created venue ID
            new_venue = self.execute_query(
                "SELECT venue_id FROM venues WHERE external_id = ? AND provider = ?",
                (venue_data["external_id"], venue_data["provider"]),
            )
            return new_venue[0]["venue_id"] if new_venue else None

        return None

    # ========== ML PREDICTION OPERATIONS ==========

    def get_predictions(self, venue_ids: Optional[List[str]] = None) -> List[Dict]:
        """Get ML predictions"""
        query = """
            SELECT p.prediction_id, p.venue_id, p.prediction_type, p.prediction_value,
                   p.confidence_score, p.model_version, p.generated_at,
                   v.name as venue_name, v.lat, v.lng
            FROM ml_predictions p
            LEFT JOIN venues v ON p.venue_id = v.venue_id
            WHERE 1=1
        """
        params = []

        if venue_ids:
            placeholders = ",".join(["?" for _ in venue_ids])
            query += f" AND p.venue_id IN ({placeholders})"
            params.extend(venue_ids)

        query += " ORDER BY p.prediction_value DESC"

        return self.execute_query(query, tuple(params))

    def upsert_prediction(self, prediction_data: Dict) -> OperationResult:
        """Insert or update an ML prediction"""
        try:
            # Check if prediction exists
            existing = self.execute_query(
                "SELECT prediction_id FROM ml_predictions WHERE venue_id = ? AND prediction_type = ?",
                (
                    prediction_data.get("venue_id"),
                    prediction_data.get("prediction_type"),
                ),
            )

            if existing:
                # Update existing prediction
                query = """
                    UPDATE ml_predictions SET
                        prediction_value = ?, confidence_score = ?, model_version = ?,
                        features_used = ?, generated_at = ?
                    WHERE prediction_id = ?
                """
                params = (
                    prediction_data.get("prediction_value"),
                    prediction_data.get("confidence_score"),
                    prediction_data.get("model_version"),
                    (
                        json.dumps(prediction_data.get("features_used"))
                        if prediction_data.get("features_used")
                        else None
                    ),
                    datetime.now(),
                    existing[0]["prediction_id"],
                )

                result = self.execute_update(query, params)
                result.message = (
                    f"Updated prediction for venue {prediction_data.get('venue_id')}"
                )
                return result
            else:
                # Insert new prediction
                query = """
                    INSERT INTO ml_predictions (
                        venue_id, prediction_type, prediction_value, confidence_score,
                        model_version, features_used, generated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    prediction_data.get("venue_id"),
                    prediction_data.get("prediction_type"),
                    prediction_data.get("prediction_value"),
                    prediction_data.get("confidence_score"),
                    prediction_data.get("model_version"),
                    (
                        json.dumps(prediction_data.get("features_used"))
                        if prediction_data.get("features_used")
                        else None
                    ),
                    datetime.now(),
                )

                result = self.execute_update(query, params)
                result.message = f"Inserted new prediction for venue {prediction_data.get('venue_id')}"
                return result

        except Exception as e:
            return OperationResult(
                success=False, error=str(e), message=f"Failed to upsert prediction: {e}"
            )

    # ========== ENRICHMENT DATA OPERATIONS ==========

    def get_demographic_data(
        self, lat: float, lng: float, radius_degrees: float = 0.01
    ) -> Optional[Dict]:
        """Get demographic data for a location within radius"""
        query = """
            SELECT * FROM demographic_data
            WHERE ABS(lat - ?) < ? AND ABS(lng - ?) < ?
            ORDER BY 
                (ABS(lat - ?) + ABS(lng - ?)) ASC
            LIMIT 1
        """
        results = self.execute_query(
            query, (lat, radius_degrees, lng, radius_degrees, lat, lng)
        )
        return results[0] if results else None

    def upsert_demographic_data(self, demo_data: Dict) -> OperationResult:
        """Insert or update demographic data"""
        try:
            query = """
                INSERT OR REPLACE INTO demographic_data (
                    lat, lng, census_tract, median_income, bachelor_degree_pct,
                    age_20_40_pct, professional_occupation_pct, data_source, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                demo_data.get("lat"),
                demo_data.get("lng"),
                demo_data.get("census_tract"),
                demo_data.get("median_income"),
                demo_data.get("bachelor_degree_pct"),
                demo_data.get("age_20_40_pct"),
                demo_data.get("professional_occupation_pct"),
                demo_data.get("data_source", "census_bureau"),
                demo_data.get("year"),
            )

            return self.execute_update(query, params)

        except Exception as e:
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Failed to upsert demographic data: {e}",
            )

    def get_foot_traffic(self, venue_id: str, hours: int = 24) -> List[Dict]:
        """Get recent foot traffic data for venue"""
        query = """
            SELECT * FROM foot_traffic_data
            WHERE venue_id = ?
            AND timestamp >= datetime('now', '-{} hours')
            ORDER BY timestamp DESC
        """.format(
            hours
        )
        return self.execute_query(query, (venue_id,))

    def upsert_foot_traffic(self, traffic_data: Dict) -> OperationResult:
        """Insert or update foot traffic data"""
        try:
            query = """
                INSERT OR REPLACE INTO foot_traffic_data (
                    venue_id, timestamp, hour_of_day, day_of_week,
                    visit_count, unique_visitors, dwell_time_minutes, provider
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                traffic_data.get("venue_id"),
                traffic_data.get("timestamp"),
                traffic_data.get("hour_of_day"),
                traffic_data.get("day_of_week"),
                traffic_data.get("visit_count"),
                traffic_data.get("unique_visitors"),
                traffic_data.get("dwell_time_minutes"),
                traffic_data.get("provider", "foot_traffic_api"),
            )

            return self.execute_update(query, params)

        except Exception as e:
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Failed to upsert foot traffic data: {e}",
            )

    def get_weather_forecast(
        self, lat: float, lng: float, hours: int = 24
    ) -> List[Dict]:
        """Get weather forecast for location"""
        query = """
            SELECT * FROM weather_data
            WHERE ABS(lat - ?) < 0.1 AND ABS(lng - ?) < 0.1
            AND is_forecast = 1
            AND forecast_timestamp BETWEEN datetime('now') AND datetime('now', '+{} hours')
            ORDER BY forecast_timestamp ASC
        """.format(
            hours
        )
        return self.execute_query(query, (lat, lng))

    def upsert_weather_data(self, weather_data: Dict) -> OperationResult:
        """Insert or update weather data"""
        try:
            query = """
                INSERT OR REPLACE INTO weather_data (
                    lat, lng, timestamp, forecast_timestamp, temperature_f,
                    humidity_pct, precipitation_probability, conditions,
                    is_forecast, provider
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                weather_data.get("lat"),
                weather_data.get("lng"),
                weather_data.get("timestamp"),
                weather_data.get("forecast_timestamp"),
                weather_data.get("temperature_f"),
                weather_data.get("humidity_pct"),
                weather_data.get("precipitation_probability"),
                weather_data.get("conditions"),
                weather_data.get("is_forecast", False),
                weather_data.get("provider", "openweather"),
            )

            return self.execute_update(query, params)

        except Exception as e:
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Failed to upsert weather data: {e}",
            )

    def get_social_sentiment(
        self,
        venue_id: Optional[str] = None,
        event_id: Optional[str] = None,
        platform: Optional[str] = None,
    ) -> List[Dict]:
        """Get social sentiment data"""
        query = "SELECT * FROM social_sentiment_data WHERE 1=1"
        params = []

        if venue_id:
            query += " AND venue_id = ?"
            params.append(venue_id)
        if event_id:
            query += " AND event_id = ?"
            params.append(event_id)
        if platform:
            query += " AND platform = ?"
            params.append(platform)

        query += " ORDER BY timestamp DESC LIMIT 100"
        return self.execute_query(query, tuple(params))

    def upsert_social_sentiment(self, sentiment_data: Dict) -> OperationResult:
        """Insert or update social sentiment data"""
        try:
            query = """
                INSERT OR REPLACE INTO social_sentiment_data (
                    venue_id, event_id, platform, timestamp, mention_count,
                    sentiment_score, sentiment_category, total_engagement
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                sentiment_data.get("venue_id"),
                sentiment_data.get("event_id"),
                sentiment_data.get("platform"),
                sentiment_data.get("timestamp"),
                sentiment_data.get("mention_count", 0),
                sentiment_data.get("sentiment_score"),
                sentiment_data.get("sentiment_category"),
                sentiment_data.get("total_engagement", 0),
            )

            return self.execute_update(query, params)

        except Exception as e:
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Failed to upsert social sentiment: {e}",
            )

    # ========== CACHING OPERATIONS ==========

    def get_api_cache(self, cache_key: str) -> Optional[Dict]:
        """Get cached API response if not expired"""
        query = """
            SELECT * FROM api_cache
            WHERE cache_key = ?
            AND expires_at > datetime('now')
            AND is_valid = 1
        """
        results = self.execute_query(query, (cache_key,))

        if results:
            # Update last_accessed
            self.execute_update(
                "UPDATE api_cache SET last_accessed = datetime('now') WHERE cache_key = ?",
                (cache_key,),
            )
            return results[0]
        return None

    def set_api_cache(
        self, cache_key: str, api_source: str, response_data: str, ttl_hours: int = 24
    ) -> OperationResult:
        """Set API response cache"""
        query = """
            INSERT OR REPLACE INTO api_cache 
            (cache_key, api_source, response_data, expires_at, response_size_bytes)
            VALUES (?, ?, ?, datetime('now', '+{} hours'), ?)
        """.format(
            ttl_hours
        )

        return self.execute_update(
            query, (cache_key, api_source, response_data, len(response_data))
        )

    def geocode_cached(self, address: str) -> Optional[Tuple[float, float]]:
        """Get geocoded coordinates from cache"""
        query = "SELECT lat, lng FROM geocoding_cache WHERE address = ?"
        results = self.execute_query(query, (address,))

        if results:
            # Increment access count
            self.execute_update(
                "UPDATE geocoding_cache SET access_count = access_count + 1 WHERE address = ?",
                (address,),
            )
            return (results[0]["lat"], results[0]["lng"])
        return None

    def cache_geocoding(
        self,
        address: str,
        lat: float,
        lng: float,
        formatted_address: Optional[str] = None,
    ) -> OperationResult:
        """Cache geocoding result"""
        query = """
            INSERT OR REPLACE INTO geocoding_cache 
            (address, lat, lng, formatted_address)
            VALUES (?, ?, ?, ?)
        """
        return self.execute_update(query, (address, lat, lng, formatted_address))

    # ========== COLLECTION STATUS OPERATIONS ==========

    def update_collection_status(
        self,
        source_name: str,
        success: bool,
        records_collected: int = 0,
        duration_seconds: float = 0,
        error_message: Optional[str] = None,
    ) -> OperationResult:
        """Update collection status after a run"""
        if success:
            query = """
                UPDATE collection_status SET
                    last_successful_collection = datetime('now'),
                    last_attempted_collection = datetime('now'),
                    total_runs = total_runs + 1,
                    successful_runs = successful_runs + 1,
                    consecutive_errors = 0,
                    total_records_collected = total_records_collected + ?,
                    records_last_run = ?,
                    last_duration_seconds = ?,
                    avg_duration_seconds = 
                        CASE WHEN avg_duration_seconds IS NULL 
                        THEN ? 
                        ELSE (avg_duration_seconds * successful_runs + ?) / (successful_runs + 1)
                        END,
                    collection_health_score = 
                        CASE WHEN collection_health_score < 1.0
                        THEN MIN(collection_health_score + 0.1, 1.0)
                        ELSE 1.0
                        END
                WHERE source_name = ?
            """
            params = (
                records_collected,
                records_collected,
                duration_seconds,
                duration_seconds,
                duration_seconds,
                source_name,
            )
        else:
            query = """
                UPDATE collection_status SET
                    last_attempted_collection = datetime('now'),
                    last_error_message = ?,
                    total_runs = total_runs + 1,
                    error_count = error_count + 1,
                    consecutive_errors = consecutive_errors + 1,
                    collection_health_score = MAX(collection_health_score - 0.2, 0.0)
                WHERE source_name = ?
            """
            params = (error_message, source_name)

        return self.execute_update(query, params)

    def get_collection_health(self) -> List[Dict]:
        """Get collection health status for all sources"""
        return self.execute_query("SELECT * FROM vw_collection_health")

    # ========== DATA QUALITY OPERATIONS ==========

    def log_data_quality_issue(
        self,
        table_name: str,
        record_id: str,
        validation_type: str,
        validation_result: str,
        field_name: Optional[str] = None,
        error_message: Optional[str] = None,
        severity: str = "medium",
    ) -> OperationResult:
        """Log data quality issue"""
        query = """
            INSERT INTO data_quality_log 
            (table_name, record_id, validation_type, validation_result,
             field_name, error_message, severity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        return self.execute_update(
            query,
            (
                table_name,
                record_id,
                validation_type,
                validation_result,
                field_name,
                error_message,
                severity,
            ),
        )

    def get_data_quality_summary(self) -> List[Dict]:
        """Get data quality summary from view"""
        return self.execute_query("SELECT * FROM vw_data_quality_summary")

    # ========== SYSTEM CONFIGURATION ==========

    def get_system_config(self, config_key: str, default_value: Any = None) -> Any:
        """Get system configuration value"""
        query = (
            "SELECT config_value, config_type FROM system_config WHERE config_key = ?"
        )
        results = self.execute_query(query, (config_key,))

        if not results:
            return default_value

        value = results[0]["config_value"]
        config_type = results[0]["config_type"]

        # Convert to appropriate type
        if config_type == "integer":
            return int(value)
        elif config_type == "float":
            return float(value)
        elif config_type == "boolean":
            return value.lower() in ("true", "1", "yes")
        elif config_type == "json":
            import json

            return json.loads(value)
        else:
            return value

    def set_system_config(
        self, config_key: str, config_value: Any, config_type: str = "string"
    ) -> OperationResult:
        """Set system configuration value"""
        # Convert value to string
        if isinstance(config_value, (dict, list)):
            import json

            value_str = json.dumps(config_value)
            config_type = "json"
        else:
            value_str = str(config_value)

        query = """
            INSERT OR REPLACE INTO system_config 
            (config_key, config_value, config_type, last_modified)
            VALUES (?, ?, ?, datetime('now'))
        """
        return self.execute_update(query, (config_key, value_str, config_type))

    # ========== UTILITY OPERATIONS ==========

    def get_data_summary(self) -> Dict:
        """Get summary statistics for the application"""
        try:
            venue_count = self.execute_query("SELECT COUNT(*) as count FROM venues")[0][
                "count"
            ]
            event_count = self.execute_query("SELECT COUNT(*) as count FROM events")[0][
                "count"
            ]
            prediction_count = self.execute_query(
                "SELECT COUNT(*) as count FROM ml_predictions"
            )[0]["count"]

            # Get venues with location data
            located_venues = self.execute_query(
                "SELECT COUNT(*) as count FROM venues WHERE lat IS NOT NULL AND lng IS NOT NULL"
            )[0]["count"]

            # Get recent activity
            recent_venues = self.execute_query(
                "SELECT COUNT(*) as count FROM venues WHERE created_at >= datetime('now', '-7 days')"
            )[0]["count"]

            recent_events = self.execute_query(
                "SELECT COUNT(*) as count FROM events WHERE created_at >= datetime('now', '-7 days')"
            )[0]["count"]

            # Get enrichment data counts
            demographic_count = self.execute_query(
                "SELECT COUNT(*) as count FROM demographic_data"
            )[0]["count"]
            weather_count = self.execute_query(
                "SELECT COUNT(*) as count FROM weather_data"
            )[0]["count"]
            traffic_count = self.execute_query(
                "SELECT COUNT(*) as count FROM foot_traffic_data"
            )[0]["count"]

            return {
                "total_venues": venue_count,
                "total_events": event_count,
                "total_predictions": prediction_count,
                "located_venues": located_venues,
                "recent_venues": recent_venues,
                "recent_events": recent_events,
                "demographic_records": demographic_count,
                "weather_records": weather_count,
                "traffic_records": traffic_count,
                "location_completeness": located_venues / max(venue_count, 1),
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to get data summary: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}

    def cleanup_old_data(self, days: int = 30) -> OperationResult:
        """Clean up old data to keep database size manageable"""
        try:
            # Remove old predictions
            old_predictions = self.execute_update(
                "DELETE FROM ml_predictions WHERE generated_at < datetime('now', '-{} days')".format(
                    days
                )
            )

            # Remove old weather data
            old_weather = self.execute_update(
                "DELETE FROM weather_data WHERE collected_at < datetime('now', '-{} days')".format(
                    days
                )
            )

            # Remove old API cache
            old_cache = self.execute_update(
                "DELETE FROM api_cache WHERE expires_at < datetime('now')"
            )

            # Remove events that are very old and have no venue association
            old_events = self.execute_update(
                """DELETE FROM events 
                   WHERE start_time < datetime('now', '-{} days') 
                   AND venue_id NOT IN (SELECT venue_id FROM venues WHERE lat IS NOT NULL)""".format(
                    days * 2
                )
            )

            return OperationResult(
                success=True,
                data={
                    "predictions_removed": old_predictions.data,
                    "events_removed": old_events.data,
                    "weather_removed": old_weather.data,
                    "cache_removed": old_cache.data,
                },
                message=f"Cleaned up old data: {old_predictions.data} predictions, {old_events.data} events, {old_weather.data} weather records, {old_cache.data} cache entries",
            )

        except Exception as e:
            return OperationResult(
                success=False, error=str(e), message=f"Failed to cleanup old data: {e}"
            )

    def get_master_venue_data(self, limit: Optional[int] = None) -> List[Dict]:
        """Get enriched venue data from master view"""
        query = "SELECT * FROM vw_master_venue_data ORDER BY prediction_value DESC"
        if limit:
            query += f" LIMIT {limit}"
        return self.execute_query(query)

    def get_master_events_data(self, limit: Optional[int] = None) -> List[Dict]:
        """Get enriched event data from master view"""
        query = "SELECT * FROM vw_master_events_data ORDER BY start_time ASC"
        if limit:
            query += f" LIMIT {limit}"
        return self.execute_query(query)

    def get_high_value_predictions(self, min_confidence: float = 0.7) -> List[Dict]:
        """Get high-value predictions for recommendations"""
        query = """
            SELECT * FROM vw_high_value_predictions 
            WHERE confidence_score >= ? 
            ORDER BY psychographic_fit_score DESC
        """
        return self.execute_query(query, (min_confidence,))


# Global database instance
_db_instance = None


def get_database() -> Database:
    """Get the global database instance"""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance


# Backward compatibility functions
def get_database_connection():
    """Backward compatibility function"""
    return get_database().get_connection()
