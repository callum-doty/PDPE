#!/usr/bin/env python3
"""
API Health Monitor for PPM Project
Monitors the health and availability of all external APIs used in the ML pipeline
"""

import os
import sys
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
import time

# Add src to path for imports
sys.path.append("src")


class APIHealthMonitor:
    """Monitor health of external APIs used in the ML pipeline"""

    def __init__(self):
        self.api_configs = {
            "google_places": {
                "name": "Google Places API",
                "test_url": "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
                "test_params": {
                    "location": "39.0997,-94.5786",
                    "radius": "1000",
                    "key": os.getenv("GOOGLE_PLACES_API_KEY"),
                },
                "expected_status": 200,
                "timeout": 10,
            },
            "predicthq": {
                "name": "PredictHQ Events API",
                "test_url": "https://api.predicthq.com/v1/events/",
                "test_params": {
                    "q": "Kansas City",
                    "active.gte": "2025-01-01",
                    "active.lte": "2025-01-02",
                    "limit": "1",
                },
                "headers": {
                    "Authorization": f"Bearer {os.getenv('PREDICT_HQ_API_KEY')}"
                },
                "expected_status": 200,
                "timeout": 15,
            },
            "weather": {
                "name": "OpenWeatherMap API",
                "test_url": "https://api.openweathermap.org/data/2.5/weather",
                "test_params": {
                    "lat": "39.0997",
                    "lon": "-94.5786",
                    "appid": os.getenv("WEATHER_API_KEY"),
                },
                "expected_status": 200,
                "timeout": 10,
            },
            "foot_traffic": {
                "name": "BestTime Foot Traffic API",
                "test_url": "https://besttime.app/api/v1/forecasts",
                "test_params": {
                    "api_key_private": os.getenv("FOOT_TRAFFIC_API_KEY"),
                    "venue_name": "Test Restaurant",
                    "venue_address": "Kansas City, MO",
                },
                "expected_status": 200,
                "timeout": 15,
            },
        }

        self.results = {}

    def check_api_health(self, api_key: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Check health of a single API"""
        result = {
            "api_name": config["name"],
            "status": "unknown",
            "response_time_ms": None,
            "status_code": None,
            "error": None,
            "timestamp": datetime.utcnow().isoformat(),
            "details": {},
        }

        try:
            start_time = time.time()

            # Make the API request
            response = requests.get(
                config["test_url"],
                params=config.get("test_params", {}),
                headers=config.get("headers", {}),
                timeout=config.get("timeout", 10),
            )

            end_time = time.time()
            response_time_ms = int((end_time - start_time) * 1000)

            result["response_time_ms"] = response_time_ms
            result["status_code"] = response.status_code

            # Check if response is successful
            if response.status_code == config.get("expected_status", 200):
                result["status"] = "healthy"

                # Try to parse JSON response for additional details
                try:
                    json_data = response.json()
                    if api_key == "google_places":
                        result["details"]["results_count"] = len(
                            json_data.get("results", [])
                        )
                        result["details"]["status"] = json_data.get("status")
                    elif api_key == "predicthq":
                        result["details"]["events_count"] = json_data.get("count", 0)
                        result["details"]["results_count"] = len(
                            json_data.get("results", [])
                        )
                    elif api_key == "weather":
                        result["details"]["weather"] = json_data.get("weather", [{}])[
                            0
                        ].get("main", "Unknown")
                        result["details"]["temperature"] = json_data.get(
                            "main", {}
                        ).get("temp")
                    elif api_key == "foot_traffic":
                        result["details"]["response_type"] = type(json_data).__name__

                except json.JSONDecodeError:
                    result["details"]["response_format"] = "non-json"

            elif response.status_code == 401:
                result["status"] = "auth_error"
                result["error"] = "Authentication failed - check API key"
            elif response.status_code == 403:
                result["status"] = "forbidden"
                result["error"] = "Access forbidden - check API permissions"
            elif response.status_code == 429:
                result["status"] = "rate_limited"
                result["error"] = "Rate limit exceeded"
            elif response.status_code >= 500:
                result["status"] = "server_error"
                result["error"] = f"Server error: {response.status_code}"
            else:
                result["status"] = "error"
                result["error"] = f"Unexpected status code: {response.status_code}"

        except requests.exceptions.Timeout:
            result["status"] = "timeout"
            result["error"] = (
                f"Request timed out after {config.get('timeout', 10)} seconds"
            )
        except requests.exceptions.ConnectionError:
            result["status"] = "connection_error"
            result["error"] = "Failed to connect to API endpoint"
        except requests.exceptions.RequestException as e:
            result["status"] = "request_error"
            result["error"] = f"Request error: {str(e)}"
        except Exception as e:
            result["status"] = "unknown_error"
            result["error"] = f"Unexpected error: {str(e)}"

        return result

    def check_all_apis(self) -> Dict[str, Any]:
        """Check health of all configured APIs"""
        print("🔍 Starting API Health Check")
        print("=" * 50)

        overall_results = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_apis": len(self.api_configs),
            "healthy_apis": 0,
            "unhealthy_apis": 0,
            "api_results": {},
            "summary": {},
        }

        for api_key, config in self.api_configs.items():
            print(f"\n📡 Checking {config['name']}...")

            # Skip if API key is missing
            api_key_env = None
            if api_key == "google_places":
                api_key_env = os.getenv("GOOGLE_PLACES_API_KEY")
            elif api_key == "predicthq":
                api_key_env = os.getenv("PREDICT_HQ_API_KEY")
            elif api_key == "weather":
                api_key_env = os.getenv("WEATHER_API_KEY")
            elif api_key == "foot_traffic":
                api_key_env = os.getenv("FOOT_TRAFFIC_API_KEY")

            if not api_key_env:
                result = {
                    "api_name": config["name"],
                    "status": "missing_key",
                    "error": "API key not configured",
                    "timestamp": datetime.utcnow().isoformat(),
                }
                print(f"❌ {config['name']}: Missing API key")
            else:
                result = self.check_api_health(api_key, config)

                if result["status"] == "healthy":
                    print(
                        f"✅ {config['name']}: Healthy ({result['response_time_ms']}ms)"
                    )
                    overall_results["healthy_apis"] += 1
                else:
                    print(
                        f"❌ {config['name']}: {result['status']} - {result.get('error', 'Unknown error')}"
                    )
                    overall_results["unhealthy_apis"] += 1

            overall_results["api_results"][api_key] = result

        # Generate summary
        health_percentage = (
            overall_results["healthy_apis"] / overall_results["total_apis"]
        ) * 100
        overall_results["summary"] = {
            "health_percentage": health_percentage,
            "status": (
                "healthy"
                if health_percentage >= 75
                else "degraded" if health_percentage >= 50 else "unhealthy"
            ),
        }

        print(f"\n" + "=" * 50)
        print(f"📊 API HEALTH SUMMARY")
        print(f"=" * 50)
        print(f"Total APIs: {overall_results['total_apis']}")
        print(f"Healthy: {overall_results['healthy_apis']}")
        print(f"Unhealthy: {overall_results['unhealthy_apis']}")
        print(f"Health Score: {health_percentage:.1f}%")
        print(f"Overall Status: {overall_results['summary']['status'].upper()}")

        # Save results to file
        self.save_results(overall_results)

        return overall_results

    def save_results(self, results: Dict[str, Any]):
        """Save health check results to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"api_health_check_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💾 Health check results saved to: {filename}")

    def get_health_status(self) -> str:
        """Get simple health status for monitoring systems"""
        results = self.check_all_apis()
        return results["summary"]["status"]


def main():
    """Main function to run API health checks"""
    monitor = APIHealthMonitor()
    results = monitor.check_all_apis()

    # Return exit code based on health status
    if results["summary"]["status"] == "healthy":
        return 0
    elif results["summary"]["status"] == "degraded":
        return 1
    else:
        return 2


if __name__ == "__main__":
    exit(main())
