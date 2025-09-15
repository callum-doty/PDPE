#!/usr/bin/env python3
"""
Weather API Diagnostic Script
Helps diagnose and fix OpenWeatherMap API authentication issues
"""

import os
import requests
import sys
from datetime import datetime


def test_api_key(api_key, description=""):
    """Test an API key against different OpenWeatherMap endpoints"""

    print(f"\n🔑 Testing API Key: {api_key[:8]}...{api_key[-8:]} {description}")
    print("=" * 60)

    # Test coordinates
    lat, lng = 39.0997, -94.5786  # Kansas City

    results = {}

    # Test 1: v2.5 Current Weather (Free tier)
    print("1. Testing v2.5 Current Weather (Free tier)...")
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather"
        params = {"lat": lat, "lon": lng, "appid": api_key, "units": "imperial"}

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            temp = data.get("main", {}).get("temp", "N/A")
            condition = data.get("weather", [{}])[0].get("main", "N/A")
            print(f"   ✅ SUCCESS: {condition} at {temp}°F")
            results["v2.5_current"] = "SUCCESS"
        elif response.status_code == 401:
            print(f"   ❌ 401 UNAUTHORIZED: Invalid API key or not activated")
            results["v2.5_current"] = "UNAUTHORIZED"
        elif response.status_code == 403:
            print(f"   ❌ 403 FORBIDDEN: API key doesn't have access to this endpoint")
            results["v2.5_current"] = "FORBIDDEN"
        elif response.status_code == 429:
            print(f"   ❌ 429 RATE LIMITED: Too many requests")
            results["v2.5_current"] = "RATE_LIMITED"
        else:
            print(f"   ❌ ERROR {response.status_code}: {response.text}")
            results["v2.5_current"] = f"ERROR_{response.status_code}"

    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        results["v2.5_current"] = "EXCEPTION"

    # Test 2: v2.5 Forecast (Free tier)
    print("\n2. Testing v2.5 Forecast (Free tier)...")
    try:
        url = f"https://api.openweathermap.org/data/2.5/forecast"
        params = {
            "lat": lat,
            "lon": lng,
            "appid": api_key,
            "units": "imperial",
            "cnt": 8,  # 1 day of forecasts
        }

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            forecast_count = len(data.get("list", []))
            print(f"   ✅ SUCCESS: {forecast_count} forecast entries received")
            results["v2.5_forecast"] = "SUCCESS"
        elif response.status_code == 401:
            print(f"   ❌ 401 UNAUTHORIZED: Invalid API key or not activated")
            results["v2.5_forecast"] = "UNAUTHORIZED"
        elif response.status_code == 403:
            print(f"   ❌ 403 FORBIDDEN: API key doesn't have access to this endpoint")
            results["v2.5_forecast"] = "FORBIDDEN"
        else:
            print(f"   ❌ ERROR {response.status_code}: {response.text}")
            results["v2.5_forecast"] = f"ERROR_{response.status_code}"

    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        results["v2.5_forecast"] = "EXCEPTION"

    # Test 3: v3.0 One Call API (Requires subscription)
    print("\n3. Testing v3.0 One Call API (Requires subscription)...")
    try:
        url = f"https://api.openweathermap.org/data/3.0/onecall"
        params = {
            "lat": lat,
            "lon": lng,
            "appid": api_key,
            "units": "imperial",
            "exclude": "minutely,alerts",  # Reduce response size
        }

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            current_temp = data.get("current", {}).get("temp", "N/A")
            hourly_count = len(data.get("hourly", []))
            daily_count = len(data.get("daily", []))
            print(
                f"   ✅ SUCCESS: Current {current_temp}°F, {hourly_count} hourly, {daily_count} daily forecasts"
            )
            results["v3.0_onecall"] = "SUCCESS"
        elif response.status_code == 401:
            print(
                f"   ❌ 401 UNAUTHORIZED: Invalid API key or One Call API not subscribed"
            )
            results["v3.0_onecall"] = "UNAUTHORIZED"
        elif response.status_code == 403:
            print(
                f"   ❌ 403 FORBIDDEN: API key doesn't have One Call API subscription"
            )
            results["v3.0_onecall"] = "FORBIDDEN"
        else:
            print(f"   ❌ ERROR {response.status_code}: {response.text}")
            results["v3.0_onecall"] = f"ERROR_{response.status_code}"

    except Exception as e:
        print(f"   ❌ EXCEPTION: {e}")
        results["v3.0_onecall"] = "EXCEPTION"

    return results


def main():
    """Main diagnostic function"""

    print("🌤️  OpenWeatherMap API Diagnostic Tool")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get API keys to test
    api_keys_to_test = []

    # 1. From environment variable
    env_key = os.getenv("WEATHER_API_KEY")
    if env_key:
        api_keys_to_test.append((env_key, "(from WEATHER_API_KEY env var)"))

    # 2. From .env file
    env_file_key = "0a546641c7ab9abe01f524ba691a5b8c"  # From your .env file
    if env_file_key and env_file_key != env_key:
        api_keys_to_test.append((env_file_key, "(from .env file)"))

    # 3. From test script
    test_script_key = "25b3b6fe5ce360b97baa2defc4815b68"  # From test_weather_api_v3.py
    if test_script_key not in [key for key, _ in api_keys_to_test]:
        api_keys_to_test.append((test_script_key, "(from test script)"))

    if not api_keys_to_test:
        print("❌ No API keys found to test!")
        print("\nPlease:")
        print("1. Get a valid API key from https://openweathermap.org/api")
        print("2. Set it in your .env file: WEATHER_API_KEY=your_key_here")
        print("3. For One Call API 3.0, subscribe at https://openweathermap.org/price")
        return

    # Test each API key
    all_results = {}
    for api_key, description in api_keys_to_test:
        results = test_api_key(api_key, description)
        all_results[api_key] = results

    # Summary
    print("\n" + "=" * 60)
    print("📊 DIAGNOSTIC SUMMARY")
    print("=" * 60)

    for api_key, results in all_results.items():
        key_display = f"{api_key[:8]}...{api_key[-8:]}"
        print(f"\n🔑 API Key: {key_display}")

        v25_current = results.get("v2.5_current", "NOT_TESTED")
        v25_forecast = results.get("v2.5_forecast", "NOT_TESTED")
        v30_onecall = results.get("v3.0_onecall", "NOT_TESTED")

        print(f"   • v2.5 Current Weather: {v25_current}")
        print(f"   • v2.5 Forecast: {v25_forecast}")
        print(f"   • v3.0 One Call API: {v30_onecall}")

        # Determine overall status
        if v25_current == "SUCCESS" and v25_forecast == "SUCCESS":
            if v30_onecall == "SUCCESS":
                print(f"   ✅ STATUS: FULLY FUNCTIONAL (v2.5 + v3.0)")
            else:
                print(f"   ⚠️  STATUS: v2.5 WORKING, v3.0 NEEDS SUBSCRIPTION")
        elif v25_current == "UNAUTHORIZED" or v25_forecast == "UNAUTHORIZED":
            print(f"   ❌ STATUS: INVALID/EXPIRED API KEY")
        else:
            print(f"   ❌ STATUS: ISSUES DETECTED")

    # Recommendations
    print("\n" + "=" * 60)
    print("💡 RECOMMENDATIONS")
    print("=" * 60)

    # Check if any key works for v2.5
    working_v25_keys = [
        key
        for key, results in all_results.items()
        if results.get("v2.5_current") == "SUCCESS"
    ]

    working_v30_keys = [
        key
        for key, results in all_results.items()
        if results.get("v3.0_onecall") == "SUCCESS"
    ]

    if not working_v25_keys:
        print("\n❌ NO WORKING API KEYS FOUND")
        print("\n🔧 IMMEDIATE ACTIONS NEEDED:")
        print("1. 🌐 Visit https://openweathermap.org/api")
        print("2. 📝 Sign up for a free account (if you don't have one)")
        print("3. 🔑 Generate a new API key")
        print("4. ⏰ Wait 10-60 minutes for activation")
        print("5. 📄 Update your .env file with the new key")
        print("6. 🧪 Run this diagnostic script again")

        print("\n📋 For One Call API 3.0 (if needed):")
        print("1. 💳 Visit https://openweathermap.org/price")
        print("2. 📊 Subscribe to 'One Call API 3.0' plan")
        print("3. 💰 Default: 2000 calls/day (can be adjusted)")
        print("4. ⚙️  Manage limits in 'Billing plans' tab")

    elif working_v25_keys and not working_v30_keys:
        best_key = working_v25_keys[0]
        print(f"\n✅ WORKING v2.5 API KEY FOUND: {best_key[:8]}...{best_key[-8:]}")
        print("\n🔧 ACTIONS:")
        print(f"1. ✏️  Update .env file to use: WEATHER_API_KEY={best_key}")
        print("2. 🧪 Test v2.5 functionality (current weather, forecasts)")

        print("\n📋 For One Call API 3.0 (if needed):")
        print("1. 💳 Visit https://openweathermap.org/price")
        print("2. 📊 Subscribe to 'One Call API 3.0' plan using the same account")
        print("3. 🔑 Use the same API key (no new key needed)")

    elif working_v30_keys:
        best_key = working_v30_keys[0]
        print(f"\n🎉 FULLY WORKING API KEY FOUND: {best_key[:8]}...{best_key[-8:]}")
        print("\n🔧 ACTIONS:")
        print(f"1. ✏️  Update .env file to use: WEATHER_API_KEY={best_key}")
        print("2. 🧪 Test both v2.5 and v3.0 functionality")
        print("3. 🚀 You're ready for production!")

    # Show example URLs for testing
    if working_v25_keys or working_v30_keys:
        test_key = (working_v30_keys or working_v25_keys)[0]
        print(f"\n🔗 TEST URLS (replace with your coordinates):")
        print(
            f"• v2.5 Current: https://api.openweathermap.org/data/2.5/weather?lat=39.0997&lon=-94.5786&appid={test_key}&units=imperial"
        )
        if working_v30_keys:
            print(
                f"• v3.0 One Call: https://api.openweathermap.org/data/3.0/onecall?lat=39.0997&lon=-94.5786&appid={test_key}&units=imperial"
            )

    print(
        f"\n⚠️  SECURITY NOTE: Keep your API keys private and never commit them to version control!"
    )


if __name__ == "__main__":
    main()
