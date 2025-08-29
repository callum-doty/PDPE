"""
Event-driven assumption layer engine for real-time computation.
"""

import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from cachetools import TTLCache, LRUCache
import hashlib
import json

from src.data_acquisition.assumptions.spending_propensity_layer import (
    get_week_of_month_factor,
    get_monthly_factor,
    get_holiday_factor_simple,
    get_dow_factor,
    get_hour_factor,
)
from src.data_acquisition.assumptions.college_layer import (
    get_college_presence_score,
    calculate_distance,
)

logger = logging.getLogger(__name__)


class SmartCache:
    """Smart caching with TTL and event-based invalidation."""

    def __init__(self, maxsize: int = 1000, ttl: int = 3600):
        self.ttl_cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self.dependency_map = {}  # Maps cache keys to their dependencies
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            return self.ttl_cache.get(key)

    def set(self, key: str, value: Any, dependencies: Optional[List[str]] = None):
        """Set value in cache with optional dependencies."""
        with self.lock:
            self.ttl_cache[key] = value
            if dependencies:
                self.dependency_map[key] = dependencies

    def invalidate(self, key: str):
        """Invalidate a specific cache key."""
        with self.lock:
            if key in self.ttl_cache:
                del self.ttl_cache[key]
            if key in self.dependency_map:
                del self.dependency_map[key]

    def invalidate_by_dependency(self, dependency: str):
        """Invalidate all cache entries that depend on a specific dependency."""
        with self.lock:
            keys_to_invalidate = []
            for key, deps in self.dependency_map.items():
                if dependency in deps:
                    keys_to_invalidate.append(key)

            for key in keys_to_invalidate:
                self.invalidate(key)

    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.ttl_cache.clear()
            self.dependency_map.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                "size": len(self.ttl_cache),
                "maxsize": self.ttl_cache.maxsize,
                "hits": getattr(self.ttl_cache, "hits", 0),
                "misses": getattr(self.ttl_cache, "misses", 0),
                "dependencies": len(self.dependency_map),
            }


class AssumptionLayerEngine:
    """
    Event-driven engine for computing assumption layer scores in real-time.
    """

    def __init__(self):
        # Caches for different types of computations
        self.spending_cache = SmartCache(maxsize=2000, ttl=3600)  # 1 hour TTL
        self.college_cache = SmartCache(maxsize=1000, ttl=7200)  # 2 hour TTL
        self.combined_cache = SmartCache(maxsize=500, ttl=1800)  # 30 min TTL

        # Statistics
        self.stats = {
            "spending_calculations": 0,
            "college_calculations": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "recalculations": 0,
        }

        self.lock = threading.RLock()

        # Configuration
        self.config = {
            "spending_weight": 0.4,
            "college_weight": 0.3,
            "event_proximity_weight": 0.3,
            "cache_enabled": True,
            "min_recalc_interval": 300,  # 5 minutes minimum between recalculations
        }

        self._last_full_recalc = {}  # Track last recalculation times

    def compute_spending_propensity(
        self, lat: float, lon: float, time: datetime
    ) -> float:
        """
        Compute spending propensity score for a location and time.

        Args:
            lat: Latitude
            lon: Longitude
            time: DateTime for the calculation

        Returns:
            Spending propensity score (0.0 to 3.0)
        """
        # Create cache key
        cache_key = self._create_cache_key("spending", lat, lon, time)

        # Check cache first
        if self.config["cache_enabled"]:
            cached_result = self.spending_cache.get(cache_key)
            if cached_result is not None:
                self.stats["cache_hits"] += 1
                return cached_result

        self.stats["cache_misses"] += 1
        self.stats["spending_calculations"] += 1

        # Compute spending propensity factors
        week_factor = get_week_of_month_factor(time)
        monthly_factor = get_monthly_factor(time.month)
        holiday_factor = get_holiday_factor_simple(time)
        dow_factor = get_dow_factor(time.weekday())
        hour_factor = get_hour_factor(time.hour)

        # Base score starts at 1.0
        base_score = 1.0

        # Apply all factors multiplicatively
        total_score = (
            base_score
            * week_factor
            * monthly_factor
            * holiday_factor
            * dow_factor
            * hour_factor
        )

        # Normalize to 0-3 range
        normalized_score = min(max(total_score, 0.0), 3.0)

        # Cache the result
        if self.config["cache_enabled"]:
            dependencies = [
                f"time_hour_{time.hour}",
                f"time_day_{time.day}",
                f"time_month_{time.month}",
                f"time_dow_{time.weekday()}",
            ]
            self.spending_cache.set(cache_key, normalized_score, dependencies)

        return normalized_score

    def compute_college_presence(self, lat: float, lon: float, time: datetime) -> float:
        """
        Compute college presence score for a location and time.

        Args:
            lat: Latitude
            lon: Longitude
            time: DateTime for the calculation

        Returns:
            College presence score (0.0 to 3.0)
        """
        # Create cache key
        cache_key = self._create_cache_key("college", lat, lon, time)

        # Check cache first
        if self.config["cache_enabled"]:
            cached_result = self.college_cache.get(cache_key)
            if cached_result is not None:
                self.stats["cache_hits"] += 1
                return cached_result

        self.stats["cache_misses"] += 1
        self.stats["college_calculations"] += 1

        # Use the existing college presence calculation
        score = get_college_presence_score(lat, lon, time)

        # Cache the result
        if self.config["cache_enabled"]:
            dependencies = [
                f"time_hour_{time.hour}",
                f"time_dow_{time.weekday()}",
                f"location_{lat:.3f}_{lon:.3f}",
            ]
            self.college_cache.set(cache_key, score, dependencies)

        return score

    def compute_combined_score(
        self,
        lat: float,
        lon: float,
        time: datetime,
        event_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        """
        Compute combined assumption layer scores.

        Args:
            lat: Latitude
            lon: Longitude
            time: DateTime for the calculation
            event_data: Optional event data for proximity calculations

        Returns:
            Dictionary with individual and combined scores
        """
        # Create cache key
        cache_key = self._create_cache_key("combined", lat, lon, time, event_data)

        # Check cache first
        if self.config["cache_enabled"]:
            cached_result = self.combined_cache.get(cache_key)
            if cached_result is not None:
                self.stats["cache_hits"] += 1
                return cached_result

        self.stats["cache_misses"] += 1

        # Compute individual scores
        spending_score = self.compute_spending_propensity(lat, lon, time)
        college_score = self.compute_college_presence(lat, lon, time)

        # Compute event proximity bonus if event data provided
        event_bonus = 0.0
        if event_data and "location" in event_data:
            event_lat = event_data["location"].get("lat", 0)
            event_lon = event_data["location"].get("lon", 0)
            distance = calculate_distance(lat, lon, event_lat, event_lon)

            # Proximity bonus decreases with distance (max 1.0 bonus within 1km)
            if distance < 5.0:  # Within 5km
                event_bonus = max(0, 1.0 - (distance / 5.0))

        # Combine scores with weights
        combined_score = (
            spending_score * self.config["spending_weight"]
            + college_score * self.config["college_weight"]
            + event_bonus * self.config["event_proximity_weight"]
        )

        result = {
            "spending_propensity": spending_score,
            "college_presence": college_score,
            "event_proximity_bonus": event_bonus,
            "combined_score": combined_score,
            "timestamp": time.isoformat(),
        }

        # Cache the result
        if self.config["cache_enabled"]:
            dependencies = [
                f"time_hour_{time.hour}",
                f"time_day_{time.day}",
                f"time_month_{time.month}",
                f"location_{lat:.3f}_{lon:.3f}",
            ]
            if event_data:
                dependencies.append(f"event_{event_data.get('id', 'unknown')}")

            self.combined_cache.set(cache_key, result, dependencies)

        return result

    def recalculate_spending_propensity(self, time: Optional[datetime] = None):
        """Recalculate spending propensity scores (triggered by time changes)."""
        if time is None:
            time = datetime.now()

        # Check if enough time has passed since last recalculation
        last_recalc = self._last_full_recalc.get("spending", datetime.min)
        if (time - last_recalc).total_seconds() < self.config["min_recalc_interval"]:
            logger.debug("Skipping spending propensity recalculation - too soon")
            return

        logger.info("Recalculating spending propensity scores")

        # Invalidate time-dependent cache entries
        self.spending_cache.invalidate_by_dependency(f"time_hour_{time.hour}")
        self.spending_cache.invalidate_by_dependency(f"time_day_{time.day}")
        self.spending_cache.invalidate_by_dependency(f"time_month_{time.month}")
        self.spending_cache.invalidate_by_dependency(f"time_dow_{time.weekday()}")

        # Also invalidate combined scores
        self.combined_cache.invalidate_by_dependency(f"time_hour_{time.hour}")
        self.combined_cache.invalidate_by_dependency(f"time_day_{time.day}")

        self._last_full_recalc["spending"] = time
        self.stats["recalculations"] += 1

    def recalculate_college_presence(self, time: Optional[datetime] = None):
        """Recalculate college presence scores (triggered by time/schedule changes)."""
        if time is None:
            time = datetime.now()

        # Check if enough time has passed since last recalculation
        last_recalc = self._last_full_recalc.get("college", datetime.min)
        if (time - last_recalc).total_seconds() < self.config["min_recalc_interval"]:
            logger.debug("Skipping college presence recalculation - too soon")
            return

        logger.info("Recalculating college presence scores")

        # Invalidate time-dependent cache entries
        self.college_cache.invalidate_by_dependency(f"time_hour_{time.hour}")
        self.college_cache.invalidate_by_dependency(f"time_dow_{time.weekday()}")

        # Also invalidate combined scores
        self.combined_cache.invalidate_by_dependency(f"time_hour_{time.hour}")

        self._last_full_recalc["college"] = time
        self.stats["recalculations"] += 1

    def recalculate_college_presence_for_area(
        self, location: Dict[str, float], time: Optional[datetime] = None
    ):
        """Recalculate college presence for a specific area (triggered by events)."""
        if time is None:
            time = datetime.now()

        lat, lon = location["lat"], location["lon"]
        logger.info(f"Recalculating college presence for area: {lat:.3f}, {lon:.3f}")

        # Invalidate cache entries for this location
        location_key = f"location_{lat:.3f}_{lon:.3f}"
        self.college_cache.invalidate_by_dependency(location_key)
        self.combined_cache.invalidate_by_dependency(location_key)

        self.stats["recalculations"] += 1

    def invalidate_event_cache(self, event_id: str):
        """Invalidate cache entries related to a specific event."""
        logger.info(f"Invalidating cache for event: {event_id}")

        event_key = f"event_{event_id}"
        self.combined_cache.invalidate_by_dependency(event_key)

        self.stats["recalculations"] += 1

    def _create_cache_key(
        self,
        layer_type: str,
        lat: float,
        lon: float,
        time: datetime,
        event_data: Optional[Dict] = None,
    ) -> str:
        """Create a cache key for the given parameters."""
        # Round coordinates to reduce cache key variations
        lat_rounded = round(lat, 4)
        lon_rounded = round(lon, 4)

        # Create time key based on layer type requirements
        if layer_type == "spending":
            # Spending is sensitive to hour, day, month, weekday
            time_key = (
                f"{time.year}-{time.month}-{time.day}-{time.hour}-{time.weekday()}"
            )
        elif layer_type == "college":
            # College is sensitive to hour and weekday
            time_key = f"{time.hour}-{time.weekday()}"
        else:
            # Combined uses both
            time_key = (
                f"{time.year}-{time.month}-{time.day}-{time.hour}-{time.weekday()}"
            )

        # Base key
        key_parts = [layer_type, str(lat_rounded), str(lon_rounded), time_key]

        # Add event data hash if provided
        if event_data:
            event_hash = hashlib.md5(
                json.dumps(event_data, sort_keys=True).encode()
            ).hexdigest()[:8]
            key_parts.append(event_hash)

        return "_".join(key_parts)

    def get_statistics(self) -> Dict[str, Any]:
        """Get engine statistics."""
        with self.lock:
            stats = self.stats.copy()
            stats.update(
                {
                    "spending_cache": self.spending_cache.get_stats(),
                    "college_cache": self.college_cache.get_stats(),
                    "combined_cache": self.combined_cache.get_stats(),
                    "config": self.config.copy(),
                    "last_recalculations": {
                        k: v.isoformat() if isinstance(v, datetime) else str(v)
                        for k, v in self._last_full_recalc.items()
                    },
                }
            )
            return stats

    def clear_all_caches(self):
        """Clear all caches."""
        logger.info("Clearing all assumption layer caches")
        self.spending_cache.clear()
        self.college_cache.clear()
        self.combined_cache.clear()
        self.stats["recalculations"] += 1

    def update_config(self, config_updates: Dict[str, Any]):
        """Update engine configuration."""
        with self.lock:
            self.config.update(config_updates)
            logger.info(f"Updated assumption engine config: {config_updates}")

            # Clear caches if weights changed
            if any(key.endswith("_weight") for key in config_updates.keys()):
                self.combined_cache.clear()

    def warmup_cache(
        self,
        locations: List[Tuple[float, float]],
        time_range: Tuple[datetime, datetime],
    ):
        """Warm up caches for specific locations and time range."""
        logger.info(f"Warming up cache for {len(locations)} locations")

        start_time, end_time = time_range
        current_time = start_time

        while current_time <= end_time:
            for lat, lon in locations:
                # Compute scores to populate cache
                self.compute_combined_score(lat, lon, current_time)

            # Move to next hour
            current_time += timedelta(hours=1)

        logger.info("Cache warmup completed")


# Global assumption engine instance
_global_engine: Optional[AssumptionLayerEngine] = None
_engine_lock = threading.Lock()


def get_assumption_engine() -> AssumptionLayerEngine:
    """Get the global assumption layer engine instance."""
    global _global_engine

    if _global_engine is None:
        with _engine_lock:
            if _global_engine is None:
                _global_engine = AssumptionLayerEngine()

    return _global_engine


def reset_assumption_engine():
    """Reset the global assumption engine (mainly for testing)."""
    global _global_engine

    with _engine_lock:
        if _global_engine:
            _global_engine.clear_all_caches()
        _global_engine = None
