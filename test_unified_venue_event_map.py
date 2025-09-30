#!/usr/bin/env python3
"""
Comprehensive Test Script for Unified Venue-Event Map
Tests the complete workflow: event scraping → database storage → unified map generation
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
import time

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.unified_venue_event_map import (
        UnifiedVenueEventMap,
        create_unified_map,
    )
    from simple_map.data_interface import MasterDataInterface
    from data_collectors.kc_event_scraper import KCEventScraper
    from etl.utils import get_db_conn
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running this from the PPM root directory")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UnifiedMapTester:
    """Comprehensive tester for unified venue-event map functionality."""

    def __init__(self):
        """Initialize the tester."""
        self.unified_map = UnifiedVenueEventMap()
        self.data_interface = MasterDataInterface()
        self.event_scraper = KCEventScraper()
        self.test_results = {
            "tests_run": 0,
            "tests_passed": 0,
            "tests_failed": 0,
            "errors": [],
            "warnings": [],
        }

    def run_all_tests(self) -> dict:
        """Run all comprehensive tests."""
        print("🧪 Starting Unified Venue-Event Map Comprehensive Tests")
        print("=" * 60)

        start_time = datetime.now()

        # Test 1: Database Connection
        self._test_database_connection()

        # Test 2: Event Scraper Database Storage
        self._test_event_scraper_storage()

        # Test 3: Master Data Interface
        self._test_master_data_interface()

        # Test 4: Unified Map Generation
        self._test_unified_map_generation()

        # Test 5: Complete Workflow
        self._test_complete_workflow()

        # Test 6: Data Validation
        self._test_data_validation()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Print summary
        self._print_test_summary(duration)

        return self.test_results

    def _test_database_connection(self):
        """Test database connectivity."""
        print("\n🔍 Test 1: Database Connection")
        print("-" * 40)

        try:
            conn = get_db_conn()
            if conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                result = cur.fetchone()
                cur.close()
                conn.close()

                if result and result[0] == 1:
                    self._pass_test("Database connection successful")
                else:
                    self._fail_test("Database query returned unexpected result")
            else:
                self._fail_test("Could not establish database connection")

        except Exception as e:
            self._fail_test(f"Database connection error: {e}")

    def _test_event_scraper_storage(self):
        """Test event scraper database storage functionality."""
        print("\n🎭 Test 2: Event Scraper Database Storage")
        print("-" * 40)

        try:
            # Test with a small subset of venues
            test_venues = ["T-Mobile Center", "Uptown Theater"]

            print(f"Testing event collection from {len(test_venues)} venues...")
            collection_result = self.event_scraper.collect_data()

            if collection_result.success:
                events_collected = collection_result.events_collected
                venues_processed = collection_result.venues_collected

                print(
                    f"✅ Collected {events_collected} events from {venues_processed} venues"
                )

                # Verify events are in database
                conn = get_db_conn()
                if conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT COUNT(*) FROM events WHERE provider = 'kc_event_scraper'"
                    )
                    db_event_count = cur.fetchone()[0]
                    cur.close()
                    conn.close()

                    if db_event_count > 0:
                        self._pass_test(
                            f"Events successfully stored in database ({db_event_count} events found)"
                        )
                    else:
                        self._fail_test("No events found in database after collection")
                else:
                    self._fail_test("Could not verify database storage")

            else:
                self._fail_test(
                    f"Event collection failed: {collection_result.error_message}"
                )

        except Exception as e:
            self._fail_test(f"Event scraper storage test error: {e}")

    def _test_master_data_interface(self):
        """Test Master Data Interface functionality."""
        print("\n📊 Test 3: Master Data Interface")
        print("-" * 40)

        try:
            # Test getting venues and events
            venues, events = self.data_interface.get_venues_and_events()

            print(
                f"Retrieved {len(venues)} venues and {len(events)} events from interface"
            )

            if len(venues) > 0:
                self._pass_test(f"Master Data Interface returned {len(venues)} venues")

                # Test venue data structure
                sample_venue = venues[0]
                required_attrs = ["name", "location"]
                missing_attrs = [
                    attr for attr in required_attrs if not hasattr(sample_venue, attr)
                ]

                if not missing_attrs:
                    self._pass_test("Venue data structure is valid")
                else:
                    self._fail_test(
                        f"Venue missing required attributes: {missing_attrs}"
                    )

            else:
                self._fail_test("Master Data Interface returned no venues")

            if len(events) > 0:
                self._pass_test(f"Master Data Interface returned {len(events)} events")

                # Test event data structure
                sample_event = events[0]
                required_attrs = ["name", "venue_name"]
                missing_attrs = [
                    attr for attr in required_attrs if not hasattr(sample_event, attr)
                ]

                if not missing_attrs:
                    self._pass_test("Event data structure is valid")
                else:
                    self._fail_test(
                        f"Event missing required attributes: {missing_attrs}"
                    )

            else:
                self._warning("Master Data Interface returned no events")

        except Exception as e:
            self._fail_test(f"Master Data Interface test error: {e}")

    def _test_unified_map_generation(self):
        """Test unified map generation."""
        print("\n🗺️ Test 4: Unified Map Generation")
        print("-" * 40)

        try:
            # Test map generation without opening browser
            test_output = "test_unified_map.html"

            print("Generating unified map...")
            map_path = self.unified_map.create_unified_map(
                output_path=test_output,
                include_event_heatmap=True,
                include_venue_clustering=True,
            )

            if map_path and map_path.exists():
                file_size = map_path.stat().st_size
                self._pass_test(
                    f"Unified map generated successfully ({file_size:,} bytes)"
                )

                # Verify HTML content
                with open(map_path, "r", encoding="utf-8") as f:
                    content = f.read()

                required_elements = [
                    "folium-map",
                    "Venues",
                    "Events",
                    "Unified Map Legend",
                    "centerMapOnVenue",
                ]

                missing_elements = [
                    elem for elem in required_elements if elem not in content
                ]

                if not missing_elements:
                    self._pass_test("Map HTML contains all required elements")
                else:
                    self._fail_test(f"Map HTML missing elements: {missing_elements}")

                # Clean up test file
                try:
                    map_path.unlink()
                    print("🧹 Cleaned up test map file")
                except:
                    pass

            else:
                self._fail_test("Unified map generation failed")

        except Exception as e:
            self._fail_test(f"Map generation test error: {e}")

    def _test_complete_workflow(self):
        """Test the complete workflow."""
        print("\n🚀 Test 5: Complete Workflow")
        print("-" * 40)

        try:
            # Test complete workflow without opening browser
            print("Running complete workflow...")
            result = self.unified_map.run_complete_workflow(
                collect_events=False,  # Skip collection to save time
                output_path="test_workflow_map.html",
                open_browser=False,
            )

            if result["success"]:
                steps = result["steps_completed"]
                self._pass_test(
                    f"Complete workflow successful (steps: {', '.join(steps)})"
                )

                # Verify map file exists
                if result.get("map_path"):
                    map_path = Path(result["map_path"])
                    if map_path.exists():
                        self._pass_test("Workflow generated map file successfully")

                        # Clean up
                        try:
                            map_path.unlink()
                            print("🧹 Cleaned up workflow test file")
                        except:
                            pass
                    else:
                        self._fail_test("Workflow did not create map file")

            else:
                errors = result.get("errors", [])
                self._fail_test(f"Complete workflow failed: {'; '.join(errors)}")

        except Exception as e:
            self._fail_test(f"Complete workflow test error: {e}")

    def _test_data_validation(self):
        """Test data validation and quality."""
        print("\n✅ Test 6: Data Validation")
        print("-" * 40)

        try:
            # Get data for validation
            venues, events = self.data_interface.get_venues_and_events()

            # Validate venue data quality
            venues_with_coords = 0
            venues_with_scores = 0

            for venue in venues:
                if hasattr(venue, "location") and venue.location:
                    lat = venue.location.get("lat", 0)
                    lng = venue.location.get("lng", 0)
                    if lat != 0 and lng != 0:
                        venues_with_coords += 1

                if (
                    hasattr(venue, "comprehensive_score")
                    and venue.comprehensive_score > 0
                ):
                    venues_with_scores += 1

            coord_percentage = (venues_with_coords / len(venues)) * 100 if venues else 0
            score_percentage = (venues_with_scores / len(venues)) * 100 if venues else 0

            if coord_percentage >= 50:
                self._pass_test(
                    f"Good venue coordinate coverage: {coord_percentage:.1f}%"
                )
            else:
                self._fail_test(
                    f"Poor venue coordinate coverage: {coord_percentage:.1f}%"
                )

            if score_percentage >= 30:
                self._pass_test(
                    f"Acceptable venue scoring coverage: {score_percentage:.1f}%"
                )
            else:
                self._warning(f"Low venue scoring coverage: {score_percentage:.1f}%")

            # Validate event data quality
            if events:
                events_with_venues = 0
                events_with_times = 0

                for event in events:
                    if hasattr(event, "venue_name") and event.venue_name:
                        events_with_venues += 1

                    if hasattr(event, "start_time") and event.start_time:
                        events_with_times += 1

                venue_link_percentage = (events_with_venues / len(events)) * 100
                time_percentage = (events_with_times / len(events)) * 100

                if venue_link_percentage >= 80:
                    self._pass_test(
                        f"Good event-venue linking: {venue_link_percentage:.1f}%"
                    )
                else:
                    self._fail_test(
                        f"Poor event-venue linking: {venue_link_percentage:.1f}%"
                    )

                if time_percentage >= 50:
                    self._pass_test(f"Good event time data: {time_percentage:.1f}%")
                else:
                    self._warning(f"Limited event time data: {time_percentage:.1f}%")

            else:
                self._warning("No events available for validation")

        except Exception as e:
            self._fail_test(f"Data validation test error: {e}")

    def _pass_test(self, message: str):
        """Record a passed test."""
        self.test_results["tests_run"] += 1
        self.test_results["tests_passed"] += 1
        print(f"✅ PASS: {message}")

    def _fail_test(self, message: str):
        """Record a failed test."""
        self.test_results["tests_run"] += 1
        self.test_results["tests_failed"] += 1
        self.test_results["errors"].append(message)
        print(f"❌ FAIL: {message}")

    def _warning(self, message: str):
        """Record a warning."""
        self.test_results["warnings"].append(message)
        print(f"⚠️  WARN: {message}")

    def _print_test_summary(self, duration: float):
        """Print comprehensive test summary."""
        print("\n" + "=" * 60)
        print("🧪 UNIFIED VENUE-EVENT MAP TEST SUMMARY")
        print("=" * 60)

        results = self.test_results
        total_tests = results["tests_run"]
        passed = results["tests_passed"]
        failed = results["tests_failed"]
        warnings = len(results["warnings"])

        print(f"📊 Tests Run: {total_tests}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Warnings: {warnings}")
        print(f"⏱️  Duration: {duration:.2f} seconds")

        if failed == 0:
            print(
                f"\n🎉 ALL TESTS PASSED! Success rate: {(passed/total_tests)*100:.1f}%"
            )
        else:
            print(
                f"\n⚠️  Some tests failed. Success rate: {(passed/total_tests)*100:.1f}%"
            )

        if results["errors"]:
            print(f"\n❌ Errors encountered:")
            for i, error in enumerate(results["errors"], 1):
                print(f"   {i}. {error}")

        if results["warnings"]:
            print(f"\n⚠️  Warnings:")
            for i, warning in enumerate(results["warnings"], 1):
                print(f"   {i}. {warning}")

        print("\n" + "=" * 60)


def run_quick_demo():
    """Run a quick demonstration of the unified map."""
    print("🎯 Quick Unified Map Demo")
    print("=" * 30)

    try:
        # Create unified map with minimal data collection
        result = create_unified_map(
            output_path="demo_unified_venue_event_map.html",
            collect_fresh_events=False,  # Use existing data
            open_browser=True,
        )

        if result["success"]:
            print("✅ Demo map created successfully!")
            print(f"📍 Map location: {result.get('map_path', 'N/A')}")
            print(f"🔧 Steps completed: {', '.join(result['steps_completed'])}")

            if result.get("statistics"):
                stats = result["statistics"]
                print(f"📊 Events: {stats.get('events_collected', 'N/A')}")
                print(f"🏢 Venues: {stats.get('venues_collected', 'N/A')}")

        else:
            print("❌ Demo failed:")
            for error in result.get("errors", []):
                print(f"   - {error}")

    except Exception as e:
        print(f"❌ Demo error: {e}")


def main():
    """Main test runner."""
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        run_quick_demo()
        return

    print("🎯 Unified Venue-Event Map Test Suite")
    print("=" * 50)
    print("This will test the complete unified mapping workflow:")
    print("• Database connectivity")
    print("• Event scraper storage")
    print("• Master data interface")
    print("• Map generation")
    print("• Complete workflow")
    print("• Data validation")
    print()

    response = input("Run comprehensive tests? (y/n): ").lower().strip()

    if response in ["y", "yes"]:
        tester = UnifiedMapTester()
        results = tester.run_all_tests()

        # Exit with appropriate code
        if results["tests_failed"] == 0:
            print("\n🎉 All tests completed successfully!")
            sys.exit(0)
        else:
            print(f"\n⚠️  {results['tests_failed']} test(s) failed.")
            sys.exit(1)

    else:
        print("Tests cancelled.")
        sys.exit(0)


if __name__ == "__main__":
    main()
