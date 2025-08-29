"""
Weather data formatter.
"""

from typing import Dict


def format_weather_data(raw_data: Dict) -> Dict:
    """Format raw weather API response into standardized format."""
    try:
        periods = raw_data.get("properties", {}).get("periods", [])
        if not periods:
            return {}

        current_period = periods[0]

        # Parse weather condition
        detailed_forecast = current_period.get("detailedForecast", "").lower()
        short_forecast = current_period.get("shortForecast", "").lower()

        # Determine condition based on forecast text
        condition = "sunny"  # default
        precipitation = 0.0

        if any(
            word in detailed_forecast or word in short_forecast
            for word in ["rain", "shower", "drizzle", "storm"]
        ):
            condition = "rain"
            precipitation = 2.0  # Assume moderate rain
        elif any(
            word in detailed_forecast or word in short_forecast
            for word in ["cloud", "overcast", "partly"]
        ):
            condition = "cloudy"
        elif any(
            word in detailed_forecast or word in short_forecast
            for word in ["clear", "sunny", "fair"]
        ):
            condition = "sunny"

        return {
            "condition": condition,
            "temperature": float(current_period.get("temperature", 70)),
            "precipitation": precipitation,
            "humidity": current_period.get("relativeHumidity", {}).get("value"),
            "wind_speed": current_period.get("windSpeed", ""),
            "wind_direction": current_period.get("windDirection", ""),
            "short_forecast": current_period.get("shortForecast", ""),
            "detailed_forecast": current_period.get("detailedForecast", ""),
        }

    except Exception as e:
        print(f"Error formatting weather data: {e}")
        # Fallback to reasonable defaults
        return {
            "condition": "sunny",
            "temperature": 72.0,
            "precipitation": 0.0,
        }
