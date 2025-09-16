"""
Venue Data Service Layer

Provides a clean interface for accessing pre-processed venue data for map generation
and other components. Handles caching, aggregation, and data transformation.
"""

import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import psycopg2
from psycopg2.extras import RealDictCursor
from etl.utils import get_db_conn


class VenueDataType(Enum):
    """Types of venue data available."""

    VENUES = "venues"
    EVENTS = "events"
    PREDICTIONS = "predictions"
    FEATURES = "features"
    TRAFFIC = "traffic"
    SOCIAL = "social"


@dataclass
class VenueDataQuery:
    """Query parameters for venue data requests."""

    data_types: List[VenueDataType]
    bbox: Optional[Tuple[float, float, float, float]] = (
        None  # (min_lat, min_lng, max_lat, max_lng)
    )
    time_range: Optional[Tuple[datetime, datetime]] = None
    min_score: Optional[float] = None
    categories: Optional[List[str]] = None
    limit: Optional[int] = None
    include_metadata: bool = True
    cache_duration_minutes: int = 30


@dataclass
class ProcessedVenueData:
    """Container for processed venue data ready for visualization."""

    venues: List[Dict]
    events: List[Dict]
    predictions: List[Dict]
    features: List[Dict]
    metadata: Dict
    query_info: Dict
    generated_at: datetime


class VenueDataService:
    """Service for accessing and processing venue data for visualization."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._cache = {}
        self._cache_timestamps = {}

    def get_venue_data(self, query: VenueDataQuery) -> ProcessedVenueData:
        """
        Get processed venue data based on query parameters.

        Args:
            query: VenueDataQuery specifying what data to retrieve

        Returns:
            ProcessedVenueData containing all requested data
        """
        # Check cache first
        cache_key = self._generate_cache_key(query)
        if self._is_cache_valid(cache_key, query.cache_duration_minutes):
            self.logger.info(f"Returning cached venue data for key: {cache_key}")
            return self._cache[cache_key]

        self.logger.info(f"Fetching fresh venue data for query: {query}")

        # Fetch fresh data
        data = ProcessedVenueData(
            venues=[],
            events=[],
            predictions=[],
            features=[],
            metadata={},
            query_info=self._create_query_info(query),
            generated_at=datetime.now(),
        )

        conn = get_db_conn()
        if not conn:
            self.logger.error("No database connection available")
            return data

        try:
            # Fetch each requested data type
            if VenueDataType.VENUES in query.data_types:
                data.venues = self._fetch_venues(conn, query)

            if VenueDataType.EVENTS in query.data_types:
                data.events = self._fetch_events(conn, query)

            if VenueDataType.PREDICTIONS in query.data_types:
                data.predictions = self._fetch_predictions(conn, query)

            if VenueDataType.FEATURES in query.data_types:
                data.features = self._fetch_features(conn, query)

            # Generate metadata
            data.metadata = self._generate_metadata(data, query)

            # Cache the results
            self._cache[cache_key] = data
            self._cache_timestamps[cache_key] = datetime.now()

            self.logger.info(
                f"Successfully fetched venue data: {len(data.venues)} venues, {len(data.events)} events"
            )

        except Exception as e:
            self.logger.error(f"Error fetching venue data: {e}")
        finally:
            conn.close()

        return data

    def get_venue_heatmap_data(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        min_score: float = 0.0,
        categories: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get venue data optimized for heatmap visualization.

        Args:
            bbox: Bounding box (min_lat, min_lng, max_lat, max_lng)
            min_score: Minimum psychographic score threshold
            categories: List of venue categories to include

        Returns:
            Dictionary with heatmap-ready data
        """
        query = VenueDataQuery(
            data_types=[VenueDataType.VENUES, VenueDataType.PREDICTIONS],
            bbox=bbox,
            min_score=min_score,
            categories=categories,
            cache_duration_minutes=15,
        )

        data = self.get_venue_data(query)

        # Transform for heatmap
        heatmap_data = {
            "venues": [],
            "predictions": [],
            "bounds": self._calculate_bounds(data.venues + data.predictions),
            "score_stats": self._calculate_score_statistics(data.venues),
            "metadata": data.metadata,
        }

        # Process venues for heatmap
        for venue in data.venues:
            if venue.get("lat") and venue.get("lng"):
                heatmap_venue = {
                    "latitude": venue["lat"],
                    "longitude": venue["lng"],
                    "name": venue.get("name", "Unknown"),
                    "category": venue.get("category", "unknown"),
                    "total_score": venue.get(
                        "final_score", venue.get("total_score", 0)
                    ),
                    "psychographic_scores": venue.get("psychographic_relevance", {}),
                    "venue_id": venue.get("venue_id"),
                    "avg_rating": venue.get("avg_rating"),
                    "review_count": venue.get("review_count"),
                    "address": venue.get("address"),
                }
                heatmap_data["venues"].append(heatmap_venue)

        # Process predictions for heatmap
        for prediction in data.predictions:
            if prediction.get("lat") and prediction.get("lng"):
                heatmap_prediction = {
                    "latitude": prediction["lat"],
                    "longitude": prediction["lng"],
                    "psychographic_density": prediction.get("psychographic_density", 0),
                    "confidence_lower": prediction.get("confidence_lower", 0),
                    "confidence_upper": prediction.get("confidence_upper", 0),
                    "model_version": prediction.get("model_version"),
                    "prediction_id": prediction.get("prediction_id"),
                }
                heatmap_data["predictions"].append(heatmap_prediction)

        return heatmap_data

    def get_layered_map_data(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        include_api_layers: bool = True,
        include_assumption_layers: bool = True,
    ) -> Dict[str, Any]:
        """
        Get data for layered interactive map visualization.

        Args:
            bbox: Bounding box for data
            include_api_layers: Include API-sourced data layers
            include_assumption_layers: Include calculated/assumption layers

        Returns:
            Dictionary with layered map data
        """
        data_types = [VenueDataType.VENUES, VenueDataType.EVENTS]
        if include_api_layers:
            data_types.extend([VenueDataType.TRAFFIC, VenueDataType.SOCIAL])
        if include_assumption_layers:
            data_types.extend([VenueDataType.PREDICTIONS, VenueDataType.FEATURES])

        query = VenueDataQuery(
            data_types=data_types, bbox=bbox, cache_duration_minutes=20
        )

        data = self.get_venue_data(query)

        # Organize data into API and assumption layers
        layered_data = {
            "api_layers": {},
            "assumption_layers": {},
            "metadata": data.metadata,
        }

        if include_api_layers:
            layered_data["api_layers"] = {
                "events": self._format_events_for_map(data.events),
                "places": self._format_venues_for_map(data.venues, source_type="api"),
                "weather": self._get_weather_data(bbox),
                "foot_traffic": self._get_traffic_data(bbox),
            }

        if include_assumption_layers:
            layered_data["assumption_layers"] = {
                "college_density": self._get_psychographic_layer_data(
                    "college_density", bbox
                ),
                "spending_propensity": self._get_psychographic_layer_data(
                    "spending_propensity", bbox
                ),
                "predictions": self._format_predictions_for_map(data.predictions),
            }

        return layered_data

    def get_venue_ranking_data(
        self, bbox: Optional[Tuple[float, float, float, float]] = None, limit: int = 50
    ) -> List[Dict]:
        """
        Get ranked venue data for sidebar display.

        Args:
            bbox: Bounding box for venues
            limit: Maximum number of venues to return

        Returns:
            List of venues ranked by score
        """
        query = VenueDataQuery(
            data_types=[VenueDataType.VENUES],
            bbox=bbox,
            limit=limit,
            cache_duration_minutes=10,
        )

        data = self.get_venue_data(query)

        # Sort venues by score and format for ranking
        ranked_venues = []
        for venue in sorted(
            data.venues, key=lambda x: x.get("final_score", 0), reverse=True
        ):
            ranked_venue = {
                "venue_id": venue.get("venue_id"),
                "name": venue.get("name", "Unknown"),
                "category": venue.get("category", "unknown"),
                "subcategory": venue.get("subcategory"),
                "total_score": venue.get("final_score", venue.get("total_score", 0)),
                "psychographic_scores": venue.get("psychographic_relevance", {}),
                "latitude": venue.get("lat"),
                "longitude": venue.get("lng"),
                "avg_rating": venue.get("avg_rating"),
                "review_count": venue.get("review_count"),
                "address": venue.get("address"),
                "quality_score": venue.get("quality_score"),
                "popularity_score": venue.get("popularity_score"),
            }
            ranked_venues.append(ranked_venue)

        return ranked_venues[:limit]

    def _fetch_venues(self, conn, query: VenueDataQuery) -> List[Dict]:
        """Fetch venue data from database."""
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build query
        where_conditions = []
        params = []

        if query.bbox:
            where_conditions.append("lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s")
            params.extend([query.bbox[0], query.bbox[2], query.bbox[1], query.bbox[3]])

        if query.min_score:
            where_conditions.append(
                "(psychographic_relevance->>'total_score')::float >= %s"
            )
            params.append(query.min_score)

        if query.categories:
            where_conditions.append("category = ANY(%s)")
            params.append(query.categories)

        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        limit_clause = f"LIMIT {query.limit}" if query.limit else ""

        sql = f"""
            SELECT 
                venue_id, external_id, provider, name, category, subcategory,
                price_tier, avg_rating, review_count, lat, lng, address,
                phone, website, hours_json, amenities, psychographic_relevance,
                created_at, updated_at
            FROM venues 
            WHERE {where_clause}
            ORDER BY (psychographic_relevance->>'total_score')::float DESC NULLS LAST
            {limit_clause}
        """

        cur.execute(sql, params)
        venues = [dict(row) for row in cur.fetchall()]
        cur.close()

        return venues

    def _fetch_events(self, conn, query: VenueDataQuery) -> List[Dict]:
        """Fetch event data from database."""
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build query with venue join for location data
        where_conditions = [
            "e.start_time >= NOW() - INTERVAL '1 day'"
        ]  # Only recent/future events
        params = []

        if query.bbox:
            where_conditions.append(
                "v.lat BETWEEN %s AND %s AND v.lng BETWEEN %s AND %s"
            )
            params.extend([query.bbox[0], query.bbox[2], query.bbox[1], query.bbox[3]])

        if query.time_range:
            where_conditions.append("e.start_time BETWEEN %s AND %s")
            params.extend(query.time_range)

        if query.categories:
            where_conditions.append("e.category = ANY(%s)")
            params.append(query.categories)

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {query.limit}" if query.limit else ""

        sql = f"""
            SELECT 
                e.event_id, e.external_id, e.provider, e.name, e.description,
                e.category, e.subcategory, e.tags, e.start_time, e.end_time,
                e.ticket_price_min, e.ticket_price_max, e.predicted_attendance,
                e.psychographic_relevance, e.created_at,
                v.venue_id, v.name as venue_name, v.lat, v.lng, v.address as venue_address
            FROM events e
            LEFT JOIN venues v ON e.venue_id = v.venue_id
            WHERE {where_clause}
            ORDER BY e.start_time ASC
            {limit_clause}
        """

        cur.execute(sql, params)
        events = [dict(row) for row in cur.fetchall()]
        cur.close()

        return events

    def _fetch_predictions(self, conn, query: VenueDataQuery) -> List[Dict]:
        """Fetch prediction data from database."""
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build query
        where_conditions = [
            "ts >= NOW() - INTERVAL '7 days'"
        ]  # Recent predictions only
        params = []

        if query.bbox:
            where_conditions.append("lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s")
            params.extend([query.bbox[0], query.bbox[2], query.bbox[1], query.bbox[3]])

        if query.min_score:
            where_conditions.append("psychographic_density >= %s")
            params.append(query.min_score)

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {query.limit}" if query.limit else ""

        sql = f"""
            SELECT 
                prediction_id, venue_id, ts, lat, lng, psychographic_density,
                confidence_lower, confidence_upper, model_version, model_ensemble,
                contributing_factors, created_at
            FROM predictions 
            WHERE {where_clause}
            ORDER BY psychographic_density DESC
            {limit_clause}
        """

        cur.execute(sql, params)
        predictions = [dict(row) for row in cur.fetchall()]
        cur.close()

        return predictions

    def _fetch_features(self, conn, query: VenueDataQuery) -> List[Dict]:
        """Fetch feature data from database."""
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Build query for recent features
        where_conditions = ["ts >= NOW() - INTERVAL '30 days'"]
        params = []

        if query.bbox:
            # Join with venues table for location filtering
            where_conditions.append(
                "v.lat BETWEEN %s AND %s AND v.lng BETWEEN %s AND %s"
            )
            params.extend([query.bbox[0], query.bbox[2], query.bbox[1], query.bbox[3]])

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {query.limit}" if query.limit else ""

        sql = f"""
            SELECT 
                f.feature_id, f.venue_id, f.ts, f.psychographic_density,
                f.college_layer_score, f.spending_propensity_score,
                f.foot_traffic_hourly, f.social_sentiment_score,
                v.lat, v.lng, v.name as venue_name
            FROM features f
            LEFT JOIN venues v ON f.venue_id = v.venue_id
            WHERE {where_clause}
            ORDER BY f.ts DESC
            {limit_clause}
        """

        cur.execute(sql, params)
        features = [dict(row) for row in cur.fetchall()]
        cur.close()

        return features

    def _get_psychographic_layer_data(
        self, layer_name: str, bbox: Optional[Tuple] = None
    ) -> Dict:
        """Get psychographic layer data from database."""
        conn = get_db_conn()
        if not conn:
            return {}

        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            where_conditions = ["layer_name = %s"]
            params = [layer_name]

            if bbox:
                where_conditions.append(
                    "lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s"
                )
                params.extend([bbox[0], bbox[2], bbox[1], bbox[3]])

            where_clause = " AND ".join(where_conditions)

            sql = f"""
                SELECT lat, lng, score, confidence, metadata
                FROM psychographic_layers
                WHERE {where_clause}
                ORDER BY score DESC
                LIMIT 1000
            """

            cur.execute(sql, params)
            rows = cur.fetchall()

            # Convert to coordinate -> score mapping
            layer_data = {}
            for row in rows:
                layer_data[(row["lat"], row["lng"])] = row["score"]

            return layer_data

        except Exception as e:
            self.logger.error(f"Error fetching psychographic layer {layer_name}: {e}")
            return {}
        finally:
            cur.close()
            conn.close()

    def _get_weather_data(self, bbox: Optional[Tuple] = None) -> List[Dict]:
        """Get recent weather data."""
        conn = get_db_conn()
        if not conn:
            return []

        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            where_conditions = ["ts >= NOW() - INTERVAL '24 hours'"]
            params = []

            if bbox:
                where_conditions.append(
                    "lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s"
                )
                params.extend([bbox[0], bbox[2], bbox[1], bbox[3]])

            where_clause = " AND ".join(where_conditions)

            sql = f"""
                SELECT lat, lng, temperature_f, feels_like_f, humidity, 
                       weather_condition, ts
                FROM weather_data
                WHERE {where_clause}
                ORDER BY ts DESC
                LIMIT 100
            """

            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

        except Exception as e:
            self.logger.error(f"Error fetching weather data: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def _get_traffic_data(self, bbox: Optional[Tuple] = None) -> List[Dict]:
        """Get recent foot traffic data."""
        conn = get_db_conn()
        if not conn:
            return []

        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            where_conditions = ["vt.ts >= NOW() - INTERVAL '24 hours'"]
            params = []

            if bbox:
                where_conditions.append(
                    "v.lat BETWEEN %s AND %s AND v.lng BETWEEN %s AND %s"
                )
                params.extend([bbox[0], bbox[2], bbox[1], bbox[3]])

            where_clause = " AND ".join(where_conditions)

            sql = f"""
                SELECT v.lat, v.lng, vt.visitors_count, vt.median_dwell_seconds,
                       vt.visitors_change_24h, vt.ts, v.name as venue_name
                FROM venue_traffic vt
                JOIN venues v ON vt.venue_id = v.venue_id
                WHERE {where_clause}
                ORDER BY vt.ts DESC
                LIMIT 200
            """

            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

        except Exception as e:
            self.logger.error(f"Error fetching traffic data: {e}")
            return []
        finally:
            cur.close()
            conn.close()

    def _format_events_for_map(self, events: List[Dict]) -> List[Dict]:
        """Format events for map visualization."""
        formatted_events = []
        for event in events:
            if event.get("lat") and event.get("lng"):
                formatted_event = {
                    "latitude": event["lat"],
                    "longitude": event["lng"],
                    "name": event.get("name", "Unknown Event"),
                    "venue_name": event.get("venue_name", "Unknown Venue"),
                    "category": event.get("category", "unknown"),
                    "start_time": event.get("start_time"),
                    "description": event.get("description", ""),
                    "total_score": self._calculate_event_score(event),
                    "psychographic_scores": event.get("psychographic_relevance", {}),
                    "event_id": event.get("event_id"),
                }
                formatted_events.append(formatted_event)
        return formatted_events

    def _format_venues_for_map(
        self, venues: List[Dict], source_type: str = "api"
    ) -> List[Dict]:
        """Format venues for map visualization."""
        formatted_venues = []
        for venue in venues:
            if venue.get("lat") and venue.get("lng"):
                formatted_venue = {
                    "latitude": venue["lat"],
                    "longitude": venue["lng"],
                    "name": venue.get("name", "Unknown Venue"),
                    "category": venue.get("category", "unknown"),
                    "total_score": venue.get(
                        "final_score", venue.get("total_score", 0)
                    ),
                    "psychographic_scores": venue.get("psychographic_relevance", {}),
                    "avg_rating": venue.get("avg_rating"),
                    "review_count": venue.get("review_count"),
                    "address": venue.get("address"),
                    "venue_id": venue.get("venue_id"),
                    "source_type": source_type,
                }
                formatted_venues.append(formatted_venue)
        return formatted_venues

    def _format_predictions_for_map(self, predictions: List[Dict]) -> List[Dict]:
        """Format predictions for map visualization."""
        formatted_predictions = []
        for prediction in predictions:
            if prediction.get("lat") and prediction.get("lng"):
                formatted_prediction = {
                    "latitude": prediction["lat"],
                    "longitude": prediction["lng"],
                    "psychographic_density": prediction.get("psychographic_density", 0),
                    "confidence_lower": prediction.get("confidence_lower", 0),
                    "confidence_upper": prediction.get("confidence_upper", 0),
                    "model_version": prediction.get("model_version"),
                    "prediction_id": prediction.get("prediction_id"),
                }
                formatted_predictions.append(formatted_prediction)
        return formatted_predictions

    def _calculate_event_score(self, event: Dict) -> float:
        """Calculate a composite score for an event."""
        psychographic_scores = event.get("psychographic_relevance", {})
        if isinstance(psychographic_scores, dict):
            # Average the psychographic scores
            scores = [
                v for v in psychographic_scores.values() if isinstance(v, (int, float))
            ]
            return sum(scores) / len(scores) if scores else 0.0
        return 0.0

    def _calculate_bounds(self, data_points: List[Dict]) -> Dict:
        """Calculate bounding box for data points."""
        if not data_points:
            return {"min_lat": 0, "max_lat": 0, "min_lng": 0, "max_lng": 0}

        lats = [p.get("lat", 0) for p in data_points if p.get("lat")]
        lngs = [p.get("lng", 0) for p in data_points if p.get("lng")]

        if not lats or not lngs:
            return {"min_lat": 0, "max_lat": 0, "min_lng": 0, "max_lng": 0}

        return {
            "min_lat": min(lats),
            "max_lat": max(lats),
            "min_lng": min(lngs),
            "max_lng": max(lngs),
        }

    def _calculate_score_statistics(self, venues: List[Dict]) -> Dict:
        """Calculate score statistics for venues."""
        scores = []
        for venue in venues:
            score = venue.get("final_score", venue.get("total_score", 0))
            if isinstance(score, (int, float)):
                scores.append(score)

        if not scores:
            return {"min": 0, "max": 0, "mean": 0, "count": 0}

        return {
            "min": min(scores),
            "max": max(scores),
            "mean": sum(scores) / len(scores),
            "count": len(scores),
        }

    def _generate_metadata(
        self, data: ProcessedVenueData, query: VenueDataQuery
    ) -> Dict:
        """Generate metadata about the fetched data."""
        return {
            "query_timestamp": datetime.now().isoformat(),
            "data_counts": {
                "venues": len(data.venues),
                "events": len(data.events),
                "predictions": len(data.predictions),
                "features": len(data.features),
            },
            "query_parameters": {
                "data_types": [dt.value for dt in query.data_types],
                "bbox": query.bbox,
                "time_range": [
                    t.isoformat() if t else None
                    for t in (query.time_range or [None, None])
                ],
                "min_score": query.min_score,
                "categories": query.categories,
                "limit": query.limit,
            },
            "data_freshness": {
                "venues_latest": self._get_latest_timestamp(data.venues, "updated_at"),
                "events_latest": self._get_latest_timestamp(data.events, "created_at"),
                "predictions_latest": self._get_latest_timestamp(
                    data.predictions, "created_at"
                ),
            },
        }

    def _get_latest_timestamp(
        self, data_list: List[Dict], timestamp_field: str
    ) -> Optional[str]:
        """Get the latest timestamp from a list of data items."""
        timestamps = []
        for item in data_list:
            ts = item.get(timestamp_field)
            if ts:
                if isinstance(ts, datetime):
                    timestamps.append(ts)
                elif isinstance(ts, str):
                    try:
                        timestamps.append(
                            datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        )
                    except:
                        pass

        return max(timestamps).isoformat() if timestamps else None

    def _create_query_info(self, query: VenueDataQuery) -> Dict:
        """Create query information for tracking."""
        return {
            "data_types_requested": [dt.value for dt in query.data_types],
            "bbox": query.bbox,
            "filters_applied": {
                "min_score": query.min_score,
                "categories": query.categories,
                "time_range": query.time_range,
                "limit": query.limit,
            },
            "cache_duration_minutes": query.cache_duration_minutes,
        }

    def _generate_cache_key(self, query: VenueDataQuery) -> str:
        """Generate a cache key for the query."""
        key_parts = [
            ",".join(sorted([dt.value for dt in query.data_types])),
            str(query.bbox) if query.bbox else "no_bbox",
            str(query.time_range) if query.time_range else "no_time",
            str(query.min_score) if query.min_score else "no_min_score",
            ",".join(sorted(query.categories)) if query.categories else "no_categories",
            str(query.limit) if query.limit else "no_limit",
        ]
        return "|".join(key_parts)

    def _is_cache_valid(self, cache_key: str, duration_minutes: int) -> bool:
        """Check if cached data is still valid."""
        if cache_key not in self._cache or cache_key not in self._cache_timestamps:
            return False

        cache_time = self._cache_timestamps[cache_key]
        return datetime.now() - cache_time < timedelta(minutes=duration_minutes)

    def clear_cache(self):
        """Clear all cached data."""
        self._cache.clear()
        self._cache_timestamps.clear()
        self.logger.info("Venue data service cache cleared")


# Convenience functions for easy access
def get_heatmap_data(
    bbox: Optional[Tuple[float, float, float, float]] = None,
    min_score: float = 0.0,
    categories: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Get venue data optimized for heatmap visualization."""
    service = VenueDataService()
    return service.get_venue_heatmap_data(bbox, min_score, categories)


def get_layered_map_data(
    bbox: Optional[Tuple[float, float, float, float]] = None,
    include_api_layers: bool = True,
    include_assumption_layers: bool = True,
) -> Dict[str, Any]:
    """Get data for layered interactive map visualization."""
    service = VenueDataService()
    return service.get_layered_map_data(
        bbox, include_api_layers, include_assumption_layers
    )


def get_venue_rankings(
    bbox: Optional[Tuple[float, float, float, float]] = None, limit: int = 50
) -> List[Dict]:
    """Get ranked venue data for sidebar display."""
    service = VenueDataService()
    return service.get_venue_ranking_data(bbox, limit)
