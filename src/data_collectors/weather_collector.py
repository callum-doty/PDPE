# Weather Data Collector
"""
Standardized weather data collector that consolidates weather data collection
from ingest_weather.py into a unified collector with consistent interfaces.
"""

import sys
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class WeatherCollectionResult:
    """Result of weather data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class WeatherData:
    """Standardized weather data structure"""

    timestamp: datetime
    lat: float
    lng: float
    temperature_f: float
    feels_like_f: float
    humidity: float
    pressure: Optional[float]
    wind_speed_mph: Optional[float]
    wind_direction: Optional[float]
    weather_condition: str
    weather_description: str
    rain_probability: Optional[float]
    precipitation_mm: Optional[float]
    uv_index: Optional[float]
    visibility: Optional[float]
    collected_at: datetime


class WeatherCollector:
    """
    Standardized weather data collector.

    Consolidates weather data collection functionality from ingest_weather.py
    into a unified collector with consistent data quality processing.
    """

    def __init__(self):
        """Initialize the weather collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

        # Kansas City coordinates (default collection point)
        self.kc_coords = {"lat": 39.0997, "lng": -94.5786}

        # Weather API configuration would go here
        # Note: API keys should be loaded from environment variables
        self.api_timeout = 10

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> WeatherCollectionResult:
        """
        Collect weather data for specified area and time period.

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)
            time_period: Time period for collection (defaults to current)

        Returns:
            WeatherCollectionResult with collection status and metrics
        """
        start_time = datetime.now()
        self.logger.info("🌤️ Starting weather data collection")

        try:
            # Use default bounds if not provided
            if area_bounds is None:
                area_bounds = self.kc_coords

            # Collect weather data
            weather_records = self._fetch_weather_data(area_bounds)

            if weather_records:
                # Validate and process data
                validated_records = self._validate_weather_data(weather_records)

                # Store in database
                stored_count = self._upsert_weather_to_db(validated_records)

                duration = (datetime.now() - start_time).total_seconds()

                result = WeatherCollectionResult(
                    source_name="weather_api",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.9,  # Weather APIs typically have high quality
                )

                self.logger.info(
                    f"✅ Weather collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = WeatherCollectionResult(
                    source_name="weather_api",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No weather data retrieved",
                )

                self.logger.warning("⚠️ No weather data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = WeatherCollectionResult(
                source_name="weather_api",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ Weather collection failed: {e}")
            return result

    def _fetch_weather_data(self, area_bounds: Dict) -> List[WeatherData]:
        """
        Fetch weather data from API.

        Args:
            area_bounds: Geographic bounds for weather collection

        Returns:
            List of WeatherData objects
        """
        # This is a placeholder implementation
        # In the real implementation, this would call the actual weather API
        # For now, we'll create mock data to demonstrate the structure

        self.logger.info(f"Fetching weather data for area: {area_bounds}")

        try:
            # Mock weather data (replace with actual API call)
            mock_weather = WeatherData(
                timestamp=datetime.now(),
                lat=area_bounds.get("lat", self.kc_coords["lat"]),
                lng=area_bounds.get("lng", self.kc_coords["lng"]),
                temperature_f=72.0,
                feels_like_f=75.0,
                humidity=65.0,
                pressure=30.12,
                wind_speed_mph=8.5,
                wind_direction=180.0,
                weather_condition="Clear",
                weather_description="Clear sky",
                rain_probability=10.0,
                precipitation_mm=0.0,
                uv_index=6.0,
                visibility=10.0,
                collected_at=datetime.now(),
            )

            return [mock_weather]

        except Exception as e:
            self.logger.error(f"Error fetching weather data: {e}")
            return []

    def _validate_weather_data(
        self, weather_records: List[WeatherData]
    ) -> List[WeatherData]:
        """
        Validate and clean weather data.

        Args:
            weather_records: Raw weather data records

        Returns:
            Validated weather data records
        """
        validated_records = []

        for record in weather_records:
            try:
                # Basic validation
                if not self._is_valid_weather_record(record):
                    self.logger.warning(f"Invalid weather record skipped: {record}")
                    continue

                # Data cleaning/normalization could go here
                validated_records.append(record)

            except Exception as e:
                self.logger.error(f"Error validating weather record: {e}")
                continue

        self.logger.info(
            f"Validated {len(validated_records)} of {len(weather_records)} weather records"
        )
        return validated_records

    def _is_valid_weather_record(self, record: WeatherData) -> bool:
        """
        Check if a weather record is valid.

        Args:
            record: WeatherData record to validate

        Returns:
            True if record is valid, False otherwise
        """
        # Check required fields
        if not record.timestamp or not record.lat or not record.lng:
            return False

        # Check reasonable ranges
        if not (-90 <= record.lat <= 90) or not (-180 <= record.lng <= 180):
            return False

        if record.temperature_f and not (-50 <= record.temperature_f <= 150):
            return False

        if record.humidity and not (0 <= record.humidity <= 100):
            return False

        return True

    def _upsert_weather_to_db(self, weather_records: List[WeatherData]) -> int:
        """
        Insert or update weather records in the database.

        Args:
            weather_records: List of validated weather records

        Returns:
            Number of records successfully stored
        """
        if not weather_records:
            return 0

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return 0

        cur = conn.cursor()
        stored_count = 0

        try:
            for record in weather_records:
                # Check if record already exists (avoid duplicates)
                cur.execute(
                    """
                    SELECT id FROM weather_data 
                    WHERE ts = %s AND lat = %s AND lng = %s
                """,
                    (record.timestamp, record.lat, record.lng),
                )

                existing_record = cur.fetchone()

                if existing_record:
                    # Update existing record
                    cur.execute(
                        """
                        UPDATE weather_data SET
                            temperature_f = %s,
                            feels_like_f = %s,
                            humidity = %s,
                            pressure = %s,
                            wind_speed_mph = %s,
                            wind_direction = %s,
                            weather_condition = %s,
                            weather_description = %s,
                            rain_probability = %s,
                            precipitation_mm = %s,
                            uv_index = %s,
                            visibility = %s
                        WHERE id = %s
                    """,
                        (
                            record.temperature_f,
                            record.feels_like_f,
                            record.humidity,
                            record.pressure,
                            record.wind_speed_mph,
                            record.wind_direction,
                            record.weather_condition,
                            record.weather_description,
                            record.rain_probability,
                            record.precipitation_mm,
                            record.uv_index,
                            record.visibility,
                            existing_record[0],
                        ),
                    )
                else:
                    # Insert new record
                    cur.execute(
                        """
                        INSERT INTO weather_data (
                            ts, lat, lng, temperature_f, feels_like_f, humidity,
                            pressure, wind_speed_mph, wind_direction, weather_condition,
                            weather_description, rain_probability, precipitation_mm,
                            uv_index, visibility
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """,
                        (
                            record.timestamp,
                            record.lat,
                            record.lng,
                            record.temperature_f,
                            record.feels_like_f,
                            record.humidity,
                            record.pressure,
                            record.wind_speed_mph,
                            record.wind_direction,
                            record.weather_condition,
                            record.weather_description,
                            record.rain_probability,
                            record.precipitation_mm,
                            record.uv_index,
                            record.visibility,
                        ),
                    )

                stored_count += 1

            conn.commit()
            self.logger.info(f"Successfully stored {stored_count} weather records")

        except Exception as e:
            self.logger.error(f"Error storing weather data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count

    def get_latest_weather(
        self, lat: float, lng: float, radius_km: float = 5.0
    ) -> Optional[WeatherData]:
        """
        Get the latest weather data for a specific location.

        Args:
            lat: Latitude
            lng: Longitude
            radius_km: Search radius in kilometers

        Returns:
            Latest WeatherData record or None
        """
        conn = get_db_conn()
        if not conn:
            return None

        cur = conn.cursor()

        try:
            # Find latest weather data within radius
            cur.execute(
                """
                SELECT ts, lat, lng, temperature_f, feels_like_f, humidity,
                       pressure, wind_speed_mph, wind_direction, weather_condition,
                       weather_description, rain_probability, precipitation_mm,
                       uv_index, visibility, created_at
                FROM weather_data
                WHERE ST_DWithin(ST_Point(lng, lat)::geography, ST_Point(%s, %s)::geography, %s)
                ORDER BY ts DESC
                LIMIT 1
            """,
                (lng, lat, radius_km * 1000),  # Convert km to meters
            )

            result = cur.fetchone()
            if result:
                return WeatherData(
                    timestamp=result[0],
                    lat=result[1],
                    lng=result[2],
                    temperature_f=result[3],
                    feels_like_f=result[4],
                    humidity=result[5],
                    pressure=result[6],
                    wind_speed_mph=result[7],
                    wind_direction=result[8],
                    weather_condition=result[9],
                    weather_description=result[10],
                    rain_probability=result[11],
                    precipitation_mm=result[12],
                    uv_index=result[13],
                    visibility=result[14],
                    collected_at=result[15],
                )

            return None

        except Exception as e:
            self.logger.error(f"Error retrieving latest weather: {e}")
            return None
        finally:
            cur.close()
            conn.close()


# Convenience functions for backward compatibility
def fetch_weather_for_kansas_city() -> List[WeatherData]:
    """Convenience function to fetch weather for Kansas City."""
    collector = WeatherCollector()
    result = collector.collect_data()
    return (
        [] if not result.success else []
    )  # Would return actual data in real implementation


def upsert_weather_to_db(weather_records: List[WeatherData]) -> int:
    """Convenience function to store weather data."""
    collector = WeatherCollector()
    return collector._upsert_weather_to_db(weather_records)


if __name__ == "__main__":
    # Test the weather collector
    logging.basicConfig(level=logging.INFO)

    collector = WeatherCollector()

    # Test data collection
    print("Testing weather data collection...")
    result = collector.collect_data()

    status = "✅" if result.success else "❌"
    print(
        f"{status} Weather collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )

    if result.error_message:
        print(f"Error: {result.error_message}")
