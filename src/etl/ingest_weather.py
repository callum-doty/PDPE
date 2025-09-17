# etl/ingest_weather.py
import os
import requests
from datetime import datetime, timedelta
from etl.utils import safe_request, get_db_conn, logging

# Weather API configuration
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
WEATHER_API_BASE_URL_V25 = "https://api.openweathermap.org/data/2.5"
WEATHER_API_BASE_URL_V30 = "https://api.openweathermap.org/data/3.0"


def fetch_current_weather(lat, lng):
    """
    Fetch current weather data for a specific location using OpenWeatherMap API v2.5

    Args:
        lat (float): Latitude
        lng (float): Longitude

    Returns:
        dict: Weather data response
    """
    url = f"{WEATHER_API_BASE_URL_V25}/weather"
    params = {
        "lat": lat,
        "lon": lng,
        "appid": WEATHER_API_KEY,
        "units": "imperial",  # Fahrenheit, mph
    }

    return safe_request(url, params=params)


def fetch_onecall_weather(lat, lng, exclude=None, units="imperial", lang="en"):
    """
    Fetch current weather and forecast data using OpenWeatherMap One Call API v3.0

    Args:
        lat (float): Latitude (-90 to 90)
        lng (float): Longitude (-180 to 180)
        exclude (str, optional): Comma-delimited list of parts to exclude from response.
                                Available values: current, minutely, hourly, daily, alerts
        units (str, optional): Units of measurement. Options: standard, metric, imperial.
                              Default: imperial (Fahrenheit, mph)
        lang (str, optional): Language for weather descriptions. Default: en

    Returns:
        dict: One Call API response with current, hourly, and daily forecasts
    """
    url = f"{WEATHER_API_BASE_URL_V30}/onecall"
    params = {
        "lat": lat,
        "lon": lng,
        "appid": WEATHER_API_KEY,
        "units": units,
    }

    # Add optional parameters if provided
    if exclude:
        params["exclude"] = exclude
    if lang != "en":
        params["lang"] = lang

    return safe_request(url, params=params)


def fetch_weather_forecast(lat, lng, days=5):
    """
    Fetch weather forecast for a specific location using v2.5 API

    Args:
        lat (float): Latitude
        lng (float): Longitude
        days (int): Number of days to forecast (max 5 for free tier)

    Returns:
        dict: Weather forecast response
    """
    url = f"{WEATHER_API_BASE_URL_V25}/forecast"
    params = {
        "lat": lat,
        "lon": lng,
        "appid": WEATHER_API_KEY,
        "units": "imperial",
        "cnt": days * 8,  # 8 forecasts per day (3-hour intervals)
    }

    return safe_request(url, params=params)


def fetch_historical_weather(lat, lng, dt):
    """
    Fetch historical weather data using OpenWeatherMap One Call API v3.0
    Note: Requires paid OpenWeatherMap subscription

    Args:
        lat (float): Latitude
        lng (float): Longitude
        dt (int): Unix timestamp for the historical date

    Returns:
        dict: Historical weather data response
    """
    url = f"{WEATHER_API_BASE_URL_V30}/onecall/timemachine"
    params = {
        "lat": lat,
        "lon": lng,
        "dt": dt,
        "appid": WEATHER_API_KEY,
        "units": "imperial",
    }

    return safe_request(url, params=params)


def fetch_daily_aggregation(lat, lng, date):
    """
    Fetch daily aggregation weather data using OpenWeatherMap One Call API v3.0

    Args:
        lat (float): Latitude
        lng (float): Longitude
        date (str): Date in YYYY-MM-DD format

    Returns:
        dict: Daily aggregation weather data response
    """
    url = f"{WEATHER_API_BASE_URL_V30}/onecall/day_summary"
    params = {
        "lat": lat,
        "lon": lng,
        "date": date,
        "appid": WEATHER_API_KEY,
        "units": "imperial",
    }

    return safe_request(url, params=params)


def fetch_weather_overview(lat, lng):
    """
    Fetch weather overview with human-readable summary using OpenWeatherMap One Call API v3.0

    Args:
        lat (float): Latitude
        lng (float): Longitude

    Returns:
        dict: Weather overview with human-readable summary
    """
    url = f"{WEATHER_API_BASE_URL_V30}/onecall/overview"
    params = {
        "lat": lat,
        "lon": lng,
        "appid": WEATHER_API_KEY,
    }

    return safe_request(url, params=params)


def process_current_weather_data(weather_data, lat, lng):
    """
    Process current weather API response into database format

    Args:
        weather_data (dict): Raw weather API response
        lat (float): Latitude
        lng (float): Longitude

    Returns:
        dict: Processed weather data
    """
    if not weather_data:
        return None

    main = weather_data.get("main", {})
    weather = weather_data.get("weather", [{}])[0]
    wind = weather_data.get("wind", {})

    processed_data = {
        "ts": datetime.utcnow(),
        "lat": lat,
        "lng": lng,
        "temperature_f": main.get("temp"),
        "feels_like_f": main.get("feels_like"),
        "humidity": main.get("humidity"),
        "pressure": main.get("pressure"),
        "wind_speed_mph": wind.get("speed"),
        "wind_direction": wind.get("deg"),
        "weather_condition": weather.get("main", "").lower(),
        "weather_description": weather.get("description", ""),
        "visibility": weather_data.get("visibility"),
        "uv_index": None,  # Not available in current weather endpoint
        "rain_probability": None,  # Not available in current weather
        "precipitation_mm": None,  # Would need to calculate from rain data
    }

    # Handle precipitation data if available
    if "rain" in weather_data:
        rain_1h = weather_data["rain"].get("1h", 0)
        processed_data["precipitation_mm"] = rain_1h
    elif "snow" in weather_data:
        snow_1h = weather_data["snow"].get("1h", 0)
        processed_data["precipitation_mm"] = snow_1h
    else:
        processed_data["precipitation_mm"] = 0

    return processed_data


def process_forecast_data(forecast_data, lat, lng):
    """
    Process weather forecast API response into database format

    Args:
        forecast_data (dict): Raw forecast API response
        lat (float): Latitude
        lng (float): Longitude

    Returns:
        list: List of processed forecast entries
    """
    if not forecast_data or "list" not in forecast_data:
        return []

    processed_forecasts = []

    for forecast in forecast_data["list"]:
        main = forecast.get("main", {})
        weather = forecast.get("weather", [{}])[0]
        wind = forecast.get("wind", {})

        processed_entry = {
            "ts": datetime.fromtimestamp(forecast.get("dt", 0)),
            "lat": lat,
            "lng": lng,
            "temperature_f": main.get("temp"),
            "feels_like_f": main.get("feels_like"),
            "humidity": main.get("humidity"),
            "pressure": main.get("pressure"),
            "wind_speed_mph": wind.get("speed"),
            "wind_direction": wind.get("deg"),
            "weather_condition": weather.get("main", "").lower(),
            "weather_description": weather.get("description", ""),
            "rain_probability": forecast.get("pop", 0) * 100,  # Convert to percentage
            "precipitation_mm": 0,
            "uv_index": None,
            "visibility": forecast.get("visibility"),
        }

        # Handle precipitation
        if "rain" in forecast:
            rain_3h = forecast["rain"].get("3h", 0)
            processed_entry["precipitation_mm"] = rain_3h
        elif "snow" in forecast:
            snow_3h = forecast["snow"].get("3h", 0)
            processed_entry["precipitation_mm"] = snow_3h

        processed_forecasts.append(processed_entry)

    return processed_forecasts


def upsert_weather_to_db(weather_records):
    """
    Insert or update weather data in the database

    Args:
        weather_records (list): List of weather data dictionaries
    """
    if not weather_records:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for record in weather_records:
            # Convert datetime to string if needed
            ts = record["ts"]
            if isinstance(ts, datetime):
                ts = ts.isoformat()

            cur.execute(
                """
                INSERT INTO weather_data (
                    ts, lat, lng, temperature_f, feels_like_f, humidity, pressure,
                    wind_speed_mph, wind_direction, weather_condition, weather_description,
                    rain_probability, precipitation_mm, uv_index, visibility
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (ts, lat, lng) DO UPDATE SET
                    temperature_f = EXCLUDED.temperature_f,
                    feels_like_f = EXCLUDED.feels_like_f,
                    humidity = EXCLUDED.humidity,
                    pressure = EXCLUDED.pressure,
                    wind_speed_mph = EXCLUDED.wind_speed_mph,
                    wind_direction = EXCLUDED.wind_direction,
                    weather_condition = EXCLUDED.weather_condition,
                    weather_description = EXCLUDED.weather_description,
                    rain_probability = EXCLUDED.rain_probability,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    uv_index = EXCLUDED.uv_index,
                    visibility = EXCLUDED.visibility
            """,
                (
                    ts,
                    record["lat"],
                    record["lng"],
                    record["temperature_f"],
                    record["feels_like_f"],
                    record["humidity"],
                    record["pressure"],
                    record["wind_speed_mph"],
                    record["wind_direction"],
                    record["weather_condition"],
                    record["weather_description"],
                    record["rain_probability"],
                    record["precipitation_mm"],
                    record["uv_index"],
                    record["visibility"],
                ),
            )

        conn.commit()
        logging.info(f"Successfully upserted {len(weather_records)} weather records")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting weather data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def fetch_weather_for_locations(locations, include_forecast=True):
    """
    Fetch weather data for multiple locations

    Args:
        locations (list): List of (lat, lng) tuples
        include_forecast (bool): Whether to include forecast data

    Returns:
        list: Combined list of current and forecast weather records
    """
    all_weather_records = []

    for lat, lng in locations:
        try:
            # Fetch current weather
            current_weather = fetch_current_weather(lat, lng)
            if current_weather:
                processed_current = process_current_weather_data(
                    current_weather, lat, lng
                )
                if processed_current:
                    all_weather_records.append(processed_current)

            # Fetch forecast if requested
            if include_forecast:
                forecast_data = fetch_weather_forecast(lat, lng)
                if forecast_data:
                    processed_forecasts = process_forecast_data(forecast_data, lat, lng)
                    all_weather_records.extend(processed_forecasts)

            logging.info(
                f"Successfully fetched weather data for location ({lat}, {lng})"
            )

        except Exception as e:
            logging.error(f"Error fetching weather for location ({lat}, {lng}): {e}")
            continue

    return all_weather_records


def fetch_weather_for_kansas_city():
    """
    Convenience function to fetch weather data for Kansas City area

    Returns:
        list: Weather records for KC area
    """
    # Kansas City area coordinates
    kc_locations = [
        (39.0997, -94.5786),  # Downtown KC
        (39.1012, -94.5844),  # Power & Light District
        (39.0739, -94.5861),  # Crossroads Arts District
        (39.0458, -94.5833),  # Plaza area
        (39.1167, -94.6275),  # Westport
    ]

    return fetch_weather_for_locations(kc_locations, include_forecast=True)


# Main execution function for testing
if __name__ == "__main__":
    if not WEATHER_API_KEY:
        print("Error: WEATHER_API_KEY environment variable not set")
        print("Please get an API key from https://openweathermap.org/api")
        exit(1)

    try:
        # Test with Kansas City downtown
        print("Testing weather API with Kansas City downtown...")
        weather_records = fetch_weather_for_kansas_city()

        if weather_records:
            print(f"Successfully fetched {len(weather_records)} weather records")

            # Print sample record
            sample_record = weather_records[0]
            print(
                f"Sample record: {sample_record['weather_condition']} at {sample_record['temperature_f']}°F"
            )

            # Save to database
            upsert_weather_to_db(weather_records)
            print("Weather data saved to database")
        else:
            print("No weather records fetched")

    except Exception as e:
        print(f"Error testing weather API: {e}")
