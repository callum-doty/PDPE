"""
Unified Database Interface for PPM Application

Single database interface that consolidates all database operations,
replacing scattered DB access patterns throughout the application.
"""

import logging
import sqlite3
import psycopg2
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Union
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

            return {
                "total_venues": venue_count,
                "total_events": event_count,
                "total_predictions": prediction_count,
                "located_venues": located_venues,
                "recent_venues": recent_venues,
                "recent_events": recent_events,
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
                },
                message=f"Cleaned up old data: {old_predictions.data} predictions, {old_events.data} events",
            )

        except Exception as e:
            return OperationResult(
                success=False, error=str(e), message=f"Failed to cleanup old data: {e}"
            )


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
