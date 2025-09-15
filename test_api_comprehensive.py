#!/usr/bin/env python3
"""
Comprehensive API Testing Suite for PPM Project
Tests all external API connections and validates data quality
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path for imports
sys.path.append("src")

try:
    from etl.utils import safe_request, get_db_conn
    from etl.ingest_places import fetch_nearby_places
    from etl.ingest_events import fetch_predicthq_events
    from etl.ingest_foot_traffic import fetch_foot_traffic
except ImportError as e:
    print(f"Warning: Could not import ETL modules: {e}")
    safe_request = None


class APITester:
    """Comprehensive API testing class"""

    def __init__(self):
        self.results = {}
        self.api_keys = {
            "google_places": os.getenv("GOOGLE_PLACES_API_KEY"),
            "predicthq": os.getenv("PREDICT_HQ_API_KEY"),
            "foot_traffic": os.getenv("FOOT_TRAFFIC_API_KEY"),
            "weather": os.getenv("WEATHER_API_KEY"),
            "database_url": os.getenv("DATABASE_URL"),
        }

        # Test coordinates (Kansas City downtown)
        self.test_lat = 39.0997
        self.test_lng = -94.5786

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all API tests and return comprehensive results"""
        print("🚀 Starting Comprehensive API Testing Suite")
        print("=" * 60)

        # Test API key availability
        self.test_api_keys()

        # Test individual APIs
        self.test_google_places_api()
        self.test_predicthq_api()
        self.test_foot_traffic_api()
        self.test_weather_api()
        self.test_database_connection()

        # Generate summary report
        self.generate_summary_report()

        return self.results

    def test_api_keys(self):
        """Test if all required API keys are present"""
        print("\n📋 Testing API Key Availability")
        print("-" * 40)

        key_status = {}
        for api_name, key_value in self.api_keys.items():
            if key_value:
                key_status[api_name] = "✅ Available"
                print(f"{api_name}: ✅ Available")
            else:
                key_status[api_name] = "❌ Missing"
                print(f"{api_name}: ❌ Missing")

        self.results["api_keys"] = key_status

    def test_google_places_api(self):
        """Test Google Places API connection and data quality"""
        print("\n🗺️  Testing Google Places API")
        print("-" * 40)

        test_result = {
            "status": "unknown",
            "response_time": None,
            "data_quality": {},
            "error": None,
            "sample_data": None,
        }

        if not self.api_keys["google_places"]:
            test_result["status"] = "skipped"
            test_result["error"] = "API key not available"
            print("❌ Skipped - No API key")
            self.results["google_places"] = test_result
            return

        try:
            start_time = datetime.now()

            # Test using the existing function
            if safe_request:
                response = fetch_nearby_places(
                    self.test_lat, self.test_lng, radius=1000
                )
                response_time = (datetime.now() - start_time).total_seconds()

                test_result["response_time"] = response_time
                test_result["status"] = "success"

                # Validate response structure
                if "results" in response:
                    results = response["results"]
                    test_result["data_quality"]["total_venues"] = len(results)

                    if results:
                        sample_venue = results[0]
                        test_result["sample_data"] = sample_venue

                        # Check required fields
                        required_fields = ["place_id", "name", "geometry", "types"]
                        missing_fields = [
                            field
                            for field in required_fields
                            if field not in sample_venue
                        ]
                        test_result["data_quality"]["missing_fields"] = missing_fields
                        test_result["data_quality"]["has_ratings"] = (
                            "rating" in sample_venue
                        )
                        test_result["data_quality"]["has_price_level"] = (
                            "price_level" in sample_venue
                        )

                        print(f"✅ Success - Found {len(results)} venues")
                        print(f"⏱️  Response time: {response_time:.2f}s")
                        print(f"📊 Sample venue: {sample_venue.get('name', 'Unknown')}")

                        if missing_fields:
                            print(f"⚠️  Missing fields: {missing_fields}")
                    else:
                        print("⚠️  No venues returned")
                        test_result["data_quality"]["total_venues"] = 0
                else:
                    test_result["status"] = "error"
                    test_result["error"] = "Invalid response structure"
                    print("❌ Invalid response structure")
            else:
                # Direct API test if imports failed
                self.test_google_places_direct()
                return

        except Exception as e:
            test_result["status"] = "error"
            test_result["error"] = str(e)
            print(f"❌ Error: {e}")

        self.results["google_places"] = test_result

    def test_google_places_direct(self):
        """Direct test of Google Places API"""
        print("🔄 Testing Google Places API directly...")

        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        params = {
            "key": self.api_keys["google_places"],
            "location": f"{self.test_lat},{self.test_lng}",
            "radius": 1000,
        }

        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if data.get("status") == "OK":
                print(
                    f"✅ Direct API test successful - {len(data.get('results', []))} venues"
                )
                self.results["google_places"] = {
                    "status": "success",
                    "data_quality": {"total_venues": len(data.get("results", []))},
                    "sample_data": (
                        data.get("results", [{}])[0] if data.get("results") else None
                    ),
                }
            else:
                print(f"❌ API returned status: {data.get('status')}")
                self.results["google_places"] = {
                    "status": "error",
                    "error": f"API status: {data.get('status')}",
                }
        except Exception as e:
            print(f"❌ Direct API test failed: {e}")
            self.results["google_places"] = {"status": "error", "error": str(e)}

    def test_predicthq_api(self):
        """Test PredictHQ API connection and data quality"""
        print("\n🎉 Testing PredictHQ API")
        print("-" * 40)

        test_result = {
            "status": "unknown",
            "response_time": None,
            "data_quality": {},
            "error": None,
            "sample_data": None,
        }

        if not self.api_keys["predicthq"]:
            test_result["status"] = "skipped"
            test_result["error"] = "API key not available"
            print("❌ Skipped - No API key")
            self.results["predicthq"] = test_result
            return

        try:
            start_time = datetime.now()

            # Test using existing function
            response = fetch_predicthq_events(
                lat=39.0997,
                lng=-94.5786,
                start_date="2025-01-01",
                end_date="2025-02-01",
            )
            response_time = (datetime.now() - start_time).total_seconds()

            test_result["response_time"] = response_time
            test_result["status"] = "success"

            # Validate response structure
            if "results" in response:
                results = response["results"]
                test_result["data_quality"]["total_events"] = len(results)

                if results:
                    sample_event = results[0]
                    test_result["sample_data"] = sample_event

                    # Check required fields
                    required_fields = ["id", "title", "category", "start", "end"]
                    missing_fields = [
                        field for field in required_fields if field not in sample_event
                    ]
                    test_result["data_quality"]["missing_fields"] = missing_fields
                    test_result["data_quality"]["has_rank"] = "rank" in sample_event
                    test_result["data_quality"]["has_location"] = (
                        "location" in sample_event
                    )

                    print(f"✅ Success - Found {len(results)} events")
                    print(f"⏱️  Response time: {response_time:.2f}s")
                    print(f"📊 Sample event: {sample_event.get('title', 'Unknown')}")

                    if missing_fields:
                        print(f"⚠️  Missing fields: {missing_fields}")
                else:
                    print("⚠️  No events returned")
                    test_result["data_quality"]["total_events"] = 0
            else:
                test_result["status"] = "error"
                test_result["error"] = "Invalid response structure"
                print("❌ Invalid response structure")

        except Exception as e:
            test_result["status"] = "error"
            test_result["error"] = str(e)
            print(f"❌ Error: {e}")

        self.results["predicthq"] = test_result

    def test_foot_traffic_api(self):
        """Test Foot Traffic API connection"""
        print("\n👥 Testing Foot Traffic API")
        print("-" * 40)

        test_result = {
            "status": "unknown",
            "response_time": None,
            "data_quality": {},
            "error": None,
            "sample_data": None,
        }

        if not self.api_keys["foot_traffic"]:
            test_result["status"] = "skipped"
            test_result["error"] = "API key not available"
            print("❌ Skipped - No API key")
            self.results["foot_traffic"] = test_result
            return

        try:
            # Test with a sample venue ID
            test_venue_id = "test_venue_123"
            start_time = datetime.now()

            response = fetch_foot_traffic(test_venue_id)
            response_time = (datetime.now() - start_time).total_seconds()

            test_result["response_time"] = response_time
            test_result["status"] = "success"
            test_result["sample_data"] = response

            print(f"✅ Success - Response received")
            print(f"⏱️  Response time: {response_time:.2f}s")

        except Exception as e:
            error_msg = str(e)
            if "foottrafficprovider.example" in error_msg:
                test_result["status"] = "placeholder"
                test_result["error"] = (
                    "Using placeholder URL - needs real implementation"
                )
                print("⚠️  Placeholder implementation detected")
                print("🔧 Action needed: Replace with real foot traffic API provider")
            else:
                test_result["status"] = "error"
                test_result["error"] = error_msg
                print(f"❌ Error: {e}")

        self.results["foot_traffic"] = test_result

    def test_weather_api(self):
        """Test Weather API connection"""
        print("\n🌤️  Testing Weather API")
        print("-" * 40)

        test_result = {
            "status": "unknown",
            "response_time": None,
            "data_quality": {},
            "error": None,
            "sample_data": None,
        }

        # Check if weather ingestion file exists and has implementation
        try:
            with open("src/etl/ingest_weather.py", "r") as f:
                content = f.read()
                if len(content.strip()) == 0:
                    test_result["status"] = "not_implemented"
                    test_result["error"] = "Weather API ingestion not implemented"
                    print("❌ Weather API ingestion not implemented")
                    print(
                        "🔧 Action needed: Implement weather API in src/etl/ingest_weather.py"
                    )
                else:
                    test_result["status"] = "implementation_found"
                    print("✅ Weather ingestion file has content")
        except FileNotFoundError:
            test_result["status"] = "file_missing"
            test_result["error"] = "Weather ingestion file not found"
            print("❌ Weather ingestion file not found")

        self.results["weather"] = test_result

    def test_database_connection(self):
        """Test database connection"""
        print("\n🗄️  Testing Database Connection")
        print("-" * 40)

        test_result = {
            "status": "unknown",
            "connection_time": None,
            "tables_exist": {},
            "error": None,
        }

        if not self.api_keys["database_url"]:
            test_result["status"] = "skipped"
            test_result["error"] = "Database URL not available"
            print("❌ Skipped - No database URL")
            self.results["database"] = test_result
            return

        try:
            start_time = datetime.now()
            conn = get_db_conn()
            connection_time = (datetime.now() - start_time).total_seconds()

            test_result["connection_time"] = connection_time

            # Test if key tables exist
            cur = conn.cursor()
            tables_to_check = [
                "venues",
                "venue_traffic",
                "events",
                "features",
                "demographics",
            ]

            for table in tables_to_check:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table} LIMIT 1")
                    count = cur.fetchone()[0]
                    test_result["tables_exist"][table] = f"✅ Exists ({count} rows)"
                    print(f"✅ Table '{table}' exists with {count} rows")
                except Exception as e:
                    test_result["tables_exist"][
                        table
                    ] = f"❌ Missing or error: {str(e)}"
                    print(f"❌ Table '{table}' missing or error: {e}")

            cur.close()
            conn.close()

            test_result["status"] = "success"
            print(f"✅ Database connection successful")
            print(f"⏱️  Connection time: {connection_time:.2f}s")

        except Exception as e:
            test_result["status"] = "error"
            test_result["error"] = str(e)
            print(f"❌ Database connection failed: {e}")

        self.results["database"] = test_result

    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE API TEST SUMMARY")
        print("=" * 60)

        total_tests = len(self.results)
        successful_tests = sum(
            1
            for result in self.results.values()
            if isinstance(result, dict) and result.get("status") == "success"
        )

        print(f"Total APIs tested: {total_tests}")
        print(f"Successful connections: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")

        print("\n🔍 Detailed Status:")
        for api_name, result in self.results.items():
            if api_name == "api_keys":
                continue

            status = result.get("status", "unknown")
            if status == "success":
                print(f"  ✅ {api_name}: Working correctly")
            elif status == "error":
                print(
                    f"  ❌ {api_name}: Error - {result.get('error', 'Unknown error')}"
                )
            elif status == "skipped":
                print(
                    f"  ⏭️  {api_name}: Skipped - {result.get('error', 'No reason given')}"
                )
            elif status == "placeholder":
                print(f"  ⚠️  {api_name}: Placeholder implementation")
            elif status == "not_implemented":
                print(f"  🔧 {api_name}: Not implemented")
            else:
                print(f"  ❓ {api_name}: Unknown status")

        print("\n🚨 Action Items:")
        action_items = []

        # Check for missing API keys
        for api_name, status in self.results.get("api_keys", {}).items():
            if "Missing" in status:
                action_items.append(f"Add {api_name} API key to environment variables")

        # Check for implementation issues
        for api_name, result in self.results.items():
            if isinstance(result, dict):
                status = result.get("status")
                if status == "placeholder":
                    action_items.append(
                        f"Replace {api_name} placeholder with real implementation"
                    )
                elif status == "not_implemented":
                    action_items.append(f"Implement {api_name} functionality")
                elif status == "error":
                    action_items.append(
                        f"Fix {api_name} error: {result.get('error', 'Unknown')}"
                    )

        if action_items:
            for i, item in enumerate(action_items, 1):
                print(f"  {i}. {item}")
        else:
            print("  🎉 No action items - all APIs working correctly!")

        # Save results to file
        self.save_results_to_file()

    def save_results_to_file(self):
        """Save test results to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"api_test_results_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump(self.results, f, indent=2, default=str)

        print(f"\n💾 Test results saved to: {filename}")


def main():
    """Main function to run API tests"""
    tester = APITester()
    results = tester.run_all_tests()

    # Return exit code based on results
    failed_tests = sum(
        1
        for result in results.values()
        if isinstance(result, dict)
        and result.get("status") in ["error", "not_implemented"]
    )

    if failed_tests > 0:
        print(f"\n⚠️  {failed_tests} tests failed or need implementation")
        return 1
    else:
        print(f"\n🎉 All tests passed successfully!")
        return 0


if __name__ == "__main__":
    exit(main())
