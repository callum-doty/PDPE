#!/usr/bin/env python3
"""
Test script to verify OpenWeatherMap API v3.0 functionality
Tests all the new API endpoints including One Call API v3.0
"""

import os
import sys
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from etl.ingest_weather import (
    fetch_current_weather,
    fetch_onecall_weather,
    fetch_weather_forecast,
    fetch_historical_weather,
    fetch_daily_aggregation,
    fetch_weather_overview,
    process_current_weather_data,
    process_forecast_data,
)


def test_weather_api_v3():
    """Test OpenWeatherMap API v3.0 functionality"""

    # Set API key from environment
    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        print("❌ WEATHER_API_KEY environment variable not set!")
        print("Please set it in your .env file or export it:")
        print("export WEATHER_API_KEY=your_api_key_here")
        return
    os.environ["WEATHER_API_KEY"] = api_key

    print("🌤️  Testing OpenWeatherMap API v3.0 Functionality")
    print("=" * 60)

    # Test coordinates - Kansas City downtown
    lat, lng = 39.0997, -94.5786

    # Test 1: One Call API v3.0 - Full data (current + forecast)
    print("\n1. Testing One Call API v3.0 (full data)...")
    try:
        onecall_data = fetch_onecall_weather(lat, lng)

        if onecall_data:
            print("✅ One Call API response received")

            # Check current weather
            if "current" in onecall_data:
                current = onecall_data["current"]
                temp_f = current.get("temp", 0)
                condition = current.get("weather", [{}])[0].get("main", "Unknown")
                print(f"   Current: {condition} at {temp_f:.1f}°F")

                # Check for UV index (available in One Call API)
                if "uvi" in current:
                    print(f"   UV Index: {current['uvi']}")

            # Check hourly forecast
            if "hourly" in onecall_data:
                hourly_count = len(onecall_data["hourly"])
                print(f"   Hourly forecasts: {hourly_count} hours")

            # Check daily forecast
            if "daily" in onecall_data:
                daily_count = len(onecall_data["daily"])
                print(f"   Daily forecasts: {daily_count} days")

                # Show next day forecast
                if daily_count > 1:
                    tomorrow = onecall_data["daily"][1]
                    temp_max = tomorrow.get("temp", {}).get("max", 0)
                    temp_min = tomorrow.get("temp", {}).get("min", 0)
                    condition = tomorrow.get("weather", [{}])[0].get("main", "Unknown")
                    print(
                        f"   Tomorrow: {condition}, High {temp_max:.1f}°F, Low {temp_min:.1f}°F"
                    )
        else:
            print("❌ Failed to fetch One Call API data")

    except Exception as e:
        print(f"❌ Error testing One Call API: {e}")

    # Test 1b: One Call API v3.0 - Current weather only (using exclude parameter)
    print("\n1b. Testing One Call API v3.0 (current only - exclude hourly,daily)...")
    try:
        current_only_data = fetch_onecall_weather(
            lat, lng, exclude="minutely,hourly,daily,alerts"
        )

        if current_only_data:
            print("✅ One Call API (current only) response received")
            print(f"   Response sections: {list(current_only_data.keys())}")

            if "current" in current_only_data:
                current = current_only_data["current"]
                temp_f = current.get("temp", 0)
                condition = current.get("weather", [{}])[0].get("main", "Unknown")
                print(f"   Current: {condition} at {temp_f:.1f}°F")
        else:
            print("❌ Failed to fetch current-only data")

    except Exception as e:
        print(f"❌ Error testing current-only API: {e}")

    # Test 1c: One Call API v3.0 - Metric units
    print("\n1c. Testing One Call API v3.0 (metric units)...")
    try:
        metric_data = fetch_onecall_weather(
            lat, lng, exclude="minutely,hourly,daily,alerts", units="metric"
        )

        if metric_data:
            print("✅ One Call API (metric) response received")

            if "current" in metric_data:
                current = metric_data["current"]
                temp_c = current.get("temp", 0)
                condition = current.get("weather", [{}])[0].get("main", "Unknown")
                print(f"   Current: {condition} at {temp_c:.1f}°C")
        else:
            print("❌ Failed to fetch metric data")

    except Exception as e:
        print(f"❌ Error testing metric API: {e}")

    # Test 2: Weather Overview (human-readable summary)
    print("\n2. Testing Weather Overview API...")
    try:
        overview_data = fetch_weather_overview(lat, lng)

        if overview_data:
            print("✅ Weather Overview response received")

            # Look for weather summary/description
            if "weather_overview" in overview_data:
                print(f"   Overview: {overview_data['weather_overview']}")
            elif "description" in overview_data:
                print(f"   Description: {overview_data['description']}")
            else:
                print(f"   Raw response keys: {list(overview_data.keys())}")
        else:
            print("❌ Failed to fetch Weather Overview data")

    except Exception as e:
        print(f"❌ Error testing Weather Overview: {e}")

    # Test 3: Daily Aggregation (for yesterday)
    print("\n3. Testing Daily Aggregation API...")
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        daily_agg_data = fetch_daily_aggregation(lat, lng, yesterday)

        if daily_agg_data:
            print("✅ Daily Aggregation response received")
            print(f"   Date: {yesterday}")

            # Look for aggregated temperature data
            if "temperature" in daily_agg_data:
                temp_data = daily_agg_data["temperature"]
                if isinstance(temp_data, dict):
                    avg_temp = temp_data.get("afternoon", temp_data.get("day", 0))
                    print(f"   Average temperature: {avg_temp:.1f}°F")

            # Look for other aggregated data
            if "humidity" in daily_agg_data:
                humidity_data = daily_agg_data["humidity"]
                if isinstance(humidity_data, dict):
                    avg_humidity = humidity_data.get("afternoon", 0)
                    print(f"   Average humidity: {avg_humidity}%")
        else:
            print("❌ Failed to fetch Daily Aggregation data")

    except Exception as e:
        print(f"❌ Error testing Daily Aggregation: {e}")

    # Test 4: Historical Weather (for 5 days ago)
    print("\n4. Testing Historical Weather API...")
    try:
        five_days_ago = datetime.now() - timedelta(days=5)
        historical_timestamp = int(five_days_ago.timestamp())

        historical_data = fetch_historical_weather(lat, lng, historical_timestamp)

        if historical_data:
            print("✅ Historical Weather response received")
            print(f"   Date: {five_days_ago.strftime('%Y-%m-%d')}")

            # Look for historical current weather
            if "current" in historical_data:
                current = historical_data["current"]
                temp_f = current.get("temp", 0)
                condition = current.get("weather", [{}])[0].get("main", "Unknown")
                print(f"   Historical weather: {condition} at {temp_f:.1f}°F")

            # Look for hourly historical data
            if "hourly" in historical_data:
                hourly_count = len(historical_data["hourly"])
                print(f"   Historical hourly records: {hourly_count}")
        else:
            print("❌ Failed to fetch Historical Weather data")

    except Exception as e:
        print(f"❌ Error testing Historical Weather: {e}")

    # Test 5: Compare v2.5 vs v3.0 current weather
    print("\n5. Comparing v2.5 vs v3.0 current weather...")
    try:
        # v2.5 current weather
        v25_weather = fetch_current_weather(lat, lng)

        if v25_weather and onecall_data:
            v25_temp = v25_weather.get("main", {}).get("temp", 0)
            v30_temp = onecall_data.get("current", {}).get("temp", 0)

            print(f"   v2.5 temperature: {v25_temp:.1f}°F")
            print(f"   v3.0 temperature: {v30_temp:.1f}°F")

            temp_diff = abs(v25_temp - v30_temp)
            if temp_diff < 2:  # Allow small differences
                print("✅ Temperature readings are consistent")
            else:
                print(f"⚠️  Temperature difference: {temp_diff:.1f}°F")

    except Exception as e:
        print(f"❌ Error comparing API versions: {e}")

    # Test 6: API Response Structure Analysis
    print("\n6. API Response Structure Analysis...")
    try:
        if onecall_data:
            print("✅ One Call API structure:")
            print(f"   Top-level keys: {list(onecall_data.keys())}")

            if "current" in onecall_data:
                current_keys = list(onecall_data["current"].keys())
                print(
                    f"   Current weather keys: {current_keys[:10]}..."
                )  # Show first 10

            if "hourly" in onecall_data and onecall_data["hourly"]:
                hourly_keys = list(onecall_data["hourly"][0].keys())
                print(
                    f"   Hourly forecast keys: {hourly_keys[:10]}..."
                )  # Show first 10

            if "daily" in onecall_data and onecall_data["daily"]:
                daily_keys = list(onecall_data["daily"][0].keys())
                print(f"   Daily forecast keys: {daily_keys[:10]}...")  # Show first 10

    except Exception as e:
        print(f"❌ Error analyzing API structure: {e}")

    print("\n" + "=" * 60)
    print("🎯 OpenWeatherMap API v3.0 Test Summary:")
    print("   • One Call API v3.0: ✅ Tested")
    print("   • Weather Overview: ✅ Tested")
    print("   • Daily Aggregation: ✅ Tested")
    print("   • Historical Weather: ✅ Tested")
    print("   • API Comparison: ✅ Tested")
    print("   • Structure Analysis: ✅ Completed")
    print("\n✨ All OpenWeatherMap API v3.0 endpoints have been tested!")
    print("\n📋 Available API Endpoints:")
    print("   • v2.5 Current Weather: /data/2.5/weather")
    print("   • v2.5 Forecast: /data/2.5/forecast")
    print("   • v3.0 One Call: /data/3.0/onecall")
    print("   • v3.0 Historical: /data/3.0/onecall/timemachine")
    print("   • v3.0 Daily Summary: /data/3.0/onecall/day_summary")
    print("   • v3.0 Overview: /data/3.0/onecall/overview")


def test_example_urls():
    """Test the example URLs provided in the task"""

    print("\n" + "=" * 60)
    print("🔗 Testing Example API URLs")
    print("=" * 60)

    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        print("❌ WEATHER_API_KEY environment variable not set!")
        print("Please set it in your .env file")
        return

    # Example coordinates from the task
    lat, lng = 33.44, -94.04

    example_urls = [
        f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lng}&appid={api_key}",
        f"https://api.openweathermap.org/data/3.0/onecall/overview?lat={lat}&lon={lng}&appid={api_key}",
        f"https://api.openweathermap.org/data/3.0/onecall/day_summary?lat={lat}&lon={lng}&date=2024-01-01&appid={api_key}",
    ]

    print(f"\nTesting with coordinates: {lat}, {lng}")
    print("Example URLs that can be used:")

    for i, url in enumerate(example_urls, 1):
        print(f"\n{i}. {url}")

    print(f"\n🔑 Your API Key: {api_key}")
    print("🌐 Weather Assistant Web Interface:")
    print(f"   https://openweathermap.org/weather-assistant?apikey={api_key}")
    print("\n⚠️  Note: Keep your API key private!")


if __name__ == "__main__":
    test_weather_api_v3()
    test_example_urls()
