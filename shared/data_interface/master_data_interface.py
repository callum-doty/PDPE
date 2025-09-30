# Master Data Interface
"""
Single interface for accessing all consolidated data from the master data system.
This is the key component that provides the "single source of truth"
by abstracting away the complexity of multiple data sources and providing clean,
consolidated data structures for map generation and other applications.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

try:
    from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator
    from shared.data_quality.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import shared modules: {e}")


# Define data classes since master data service modules don't exist yet
class MasterDataAggregator:
    def __init__(self):
        pass

    def aggregate_area_data(self, area_bounds, time_period=None):
        return [], []

    def aggregate_venue_data(self, area_bounds=None):
        return []

    def aggregate_event_data(self, area_bounds, time_period):
        return []

    def refresh_aggregations(self, force_refresh=False):
        return type(
            "AggregationResult",
            (),
            {
                "venues_aggregated": 0,
                "events_aggregated": 0,
                "data_completeness_avg": 0.0,
                "processing_time_seconds": 0.0,
            },
        )()

    def get_aggregation_health_status(self):
        return {"overall_health_score": 0.0}


class VenueRegistry:
    def __init__(self):
        pass

    def get_venue_relationships(self, venue_id):
        return None


class ConsolidatedVenueData:
    def __init__(self):
        self.venue_id = ""
        self.name = ""
        self.location = {}
        self.category = ""
        self.subcategory = ""
        self.address = ""
        self.phone = ""
        self.website = ""
        self.data_completeness = 0.0
        self.comprehensive_score = 0.0
        self.data_source_type = ""
        self.last_updated = None
        self.current_weather = None
        self.traffic_conditions = None
        self.social_sentiment = None
        self.economic_context = None
        self.foot_traffic = None
        self.ml_predictions = None
        self.demographic_context = None
        self.source_reliability = 0.0


class ConsolidatedEventData:
    def __init__(self):
        self.event_id = ""
        self.name = ""
        self.category = ""
        self.start_time = None


class AggregationResult:
    def __init__(self):
        self.venues_aggregated = 0
        self.events_aggregated = 0
        self.data_completeness_avg = 0.0
        self.processing_time_seconds = 0.0


class MasterDataInterface:
    """
    Single interface for accessing all consolidated data.

    This class provides the clean, simple API that map generation and other
    applications use to access all venue and event data. It abstracts away
    the complexity of the underlying master data system and provides a
    single source of truth for all data needs.
    """

    def __init__(self):
        """Initialize the master data interface."""
        self.logger = logging.getLogger(__name__)

        # Initialize core components
        self.aggregator = MasterDataAggregator()
        self.venue_registry = VenueRegistry()

        try:
            self.quality_controller = QualityController()
        except:
            self.quality_controller = None

        try:
            self.orchestrator = MasterDataOrchestrator()
        except:
            self.orchestrator = None

        # Default Kansas City bounds
        self.default_area_bounds = {
            "north": 39.3,
            "south": 38.9,
            "east": -94.3,
            "west": -94.8,
        }

    def get_venues_and_events(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> Tuple[List[ConsolidatedVenueData], List[ConsolidatedEventData]]:
        """
        THE KEY METHOD: Single call to get all data needed for map generation.

        This method provides the core "single source of truth" functionality
        by returning all venues and events with their complete contextual data
        in one simple call.

        Args:
            area_bounds: Geographic bounds (defaults to Kansas City)
            time_period: Time period for events (defaults to next 30 days)

        Returns:
            Tuple of (venues, events) with all contextual data included
        """
        start_time = datetime.now()

        if area_bounds is None:
            area_bounds = self.default_area_bounds
        if time_period is None:
            time_period = timedelta(days=30)

        self.logger.info(
            f"🎯 Getting venues and events for area: {area_bounds}, "
            f"time period: {time_period.days} days"
        )

        try:
            # Single call to get all consolidated data
            venues, events = self.aggregator.aggregate_area_data(
                area_bounds, time_period
            )

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"✅ Retrieved {len(venues)} venues and {len(events)} events "
                f"in {processing_time:.2f}s"
            )

            # Log data completeness summary
            if venues:
                avg_completeness = sum(v.data_completeness for v in venues) / len(
                    venues
                )
                high_quality_venues = len(
                    [v for v in venues if v.data_completeness >= 0.8]
                )

                self.logger.info(
                    f"📊 Data quality: {avg_completeness:.2f} avg completeness, "
                    f"{high_quality_venues}/{len(venues)} high-quality venues"
                )

            return venues, events

        except Exception as e:
            self.logger.error(f"Error getting venues and events: {e}")
            return [], []

    def get_venues_only(
        self, area_bounds: Optional[Dict] = None
    ) -> List[ConsolidatedVenueData]:
        """
        Get only venues with all contextual data.

        Args:
            area_bounds: Geographic bounds (defaults to Kansas City)

        Returns:
            List of ConsolidatedVenueData with all contextual information
        """
        if area_bounds is None:
            area_bounds = self.default_area_bounds

        self.logger.info(f"🏢 Getting venues for area: {area_bounds}")

        try:
            venues = self.aggregator.aggregate_venue_data(area_bounds)

            self.logger.info(f"✅ Retrieved {len(venues)} venues")
            return venues

        except Exception as e:
            self.logger.error(f"Error getting venues: {e}")
            return []

    def get_events_only(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> List[ConsolidatedEventData]:
        """
        Get only events with all contextual data.

        Args:
            area_bounds: Geographic bounds (defaults to Kansas City)
            time_period: Time period for events (defaults to next 30 days)

        Returns:
            List of ConsolidatedEventData with all contextual information
        """
        if area_bounds is None:
            area_bounds = self.default_area_bounds
        if time_period is None:
            time_period = timedelta(days=30)

        self.logger.info(
            f"📅 Getting events for area: {area_bounds}, "
            f"time period: {time_period.days} days"
        )

        try:
            events = self.aggregator.aggregate_event_data(area_bounds, time_period)

            self.logger.info(f"✅ Retrieved {len(events)} events")
            return events

        except Exception as e:
            self.logger.error(f"Error getting events: {e}")
            return []

    def refresh_area_data(
        self, area_bounds: Optional[Dict] = None, force_refresh: bool = False
    ) -> Dict:
        """
        Refresh all data for a specific geographic area.

        Args:
            area_bounds: Geographic bounds (defaults to Kansas City)
            force_refresh: Whether to force refresh even if data is recent

        Returns:
            Dictionary with refresh status and statistics
        """
        if area_bounds is None:
            area_bounds = self.default_area_bounds

        self.logger.info(f"🔄 Refreshing data for area: {area_bounds}")

        try:
            # Refresh underlying materialized views
            aggregation_result = self.aggregator.refresh_aggregations(force_refresh)

            # Get updated data statistics
            venues, events = self.aggregator.aggregate_area_data(area_bounds)

            refresh_status = {
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "area_bounds": area_bounds,
                "aggregation_result": {
                    "venues_aggregated": aggregation_result.venues_aggregated,
                    "events_aggregated": aggregation_result.events_aggregated,
                    "data_completeness_avg": aggregation_result.data_completeness_avg,
                    "processing_time_seconds": aggregation_result.processing_time_seconds,
                },
                "area_data": {
                    "venues_in_area": len(venues),
                    "events_in_area": len(events),
                    "avg_venue_completeness": (
                        sum(v.data_completeness for v in venues) / len(venues)
                        if venues
                        else 0.0
                    ),
                },
            }

            self.logger.info(
                f"✅ Refresh completed: {len(venues)} venues, {len(events)} events"
            )

            return refresh_status

        except Exception as e:
            self.logger.error(f"Error refreshing area data: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def get_data_health_status(self) -> Dict:
        """
        Get comprehensive health status of all data sources.

        Returns:
            Dictionary with health metrics and status information
        """
        self.logger.info("📊 Getting comprehensive data health status")

        try:
            # Get aggregation health status
            aggregation_health = self.aggregator.get_aggregation_health_status()

            # Get quality controller status
            quality_reports = {}
            if self.quality_controller:
                try:
                    quality_reports = (
                        self.quality_controller.validate_priority_sources()
                    )
                except:
                    pass

            # Get orchestrator health report
            orchestrator_health = {}
            if self.orchestrator:
                try:
                    orchestrator_health = self.orchestrator.get_data_health_report()
                except:
                    pass

            # Combine all health information
            comprehensive_health = {
                "timestamp": datetime.now().isoformat(),
                "overall_status": (
                    "healthy"
                    if aggregation_health.get("overall_health_score", 0) > 0.7
                    else "warning"
                ),
                "aggregation_health": aggregation_health,
                "quality_reports": {
                    source: {
                        "quality_score": report.quality_score,
                        "completeness_score": report.completeness_score,
                        "validation_errors": report.validation_errors,
                        "data_issues": report.data_issues,
                    }
                    for source, report in quality_reports.items()
                },
                "orchestrator_health": orchestrator_health,
                "summary": {
                    "total_venues": aggregation_health.get("venue_statistics", {}).get(
                        "total_venues", 0
                    ),
                    "high_quality_venues": aggregation_health.get(
                        "venue_statistics", {}
                    ).get("high_quality_venues", 0),
                    "total_events": aggregation_health.get("event_statistics", {}).get(
                        "total_events", 0
                    ),
                    "data_sources_healthy": len(
                        [r for r in quality_reports.values() if r.quality_score >= 0.7]
                    ),
                    "data_sources_total": len(quality_reports),
                    "last_refresh": aggregation_health.get("last_refresh"),
                    "refresh_needed": aggregation_health.get("refresh_needed", False),
                },
            }

            return comprehensive_health

        except Exception as e:
            self.logger.error(f"Error getting health status: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "error",
                "error": str(e),
            }

    def collect_fresh_data(self, priority_only: bool = False) -> Dict:
        """
        Collect fresh data from all sources.

        Args:
            priority_only: Whether to collect only priority data sources

        Returns:
            Dictionary with collection results
        """
        self.logger.info(f"🚀 Collecting fresh data (priority_only: {priority_only})")

        try:
            collection_results = []

            if self.orchestrator:
                if priority_only:
                    # Collect only priority data (venues, social, ML)
                    collection_results = self.orchestrator.collect_priority_data()
                else:
                    # Collect all data
                    master_status = self.orchestrator.collect_all_data()
                    collection_results = master_status.collection_results

            # Refresh aggregations after collection
            self.aggregator.refresh_aggregations(force_refresh=True)

            collection_summary = {
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "priority_only": priority_only,
                "collection_results": [
                    {
                        "source_name": result.source_name,
                        "success": result.success,
                        "records_collected": result.records_collected,
                        "duration_seconds": result.duration_seconds,
                        "data_quality_score": result.data_quality_score,
                        "error_message": result.error_message,
                    }
                    for result in collection_results
                ],
                "summary": {
                    "sources_processed": len(collection_results),
                    "successful_sources": len(
                        [r for r in collection_results if r.success]
                    ),
                    "total_records": sum(
                        r.records_collected for r in collection_results
                    ),
                    "total_duration": sum(
                        r.duration_seconds for r in collection_results
                    ),
                },
            }

            self.logger.info(
                f"✅ Data collection completed: {collection_summary['summary']['successful_sources']}/"
                f"{collection_summary['summary']['sources_processed']} sources successful"
            )

            return collection_summary

        except Exception as e:
            self.logger.error(f"Error collecting fresh data: {e}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    def get_venue_details(self, venue_id: str) -> Optional[Dict]:
        """
        Get detailed information about a specific venue.

        Args:
            venue_id: Master venue ID

        Returns:
            Dictionary with detailed venue information including relationships
        """
        self.logger.info(f"🏢 Getting details for venue: {venue_id}")

        try:
            # Get venue relationships from registry
            relationships = self.venue_registry.get_venue_relationships(venue_id)

            if not relationships:
                return None

            # Get consolidated venue data
            venues = self.aggregator.aggregate_venue_data()
            venue_data = None

            for venue in venues:
                if venue.venue_id == venue_id:
                    venue_data = venue
                    break

            if not venue_data:
                return (
                    relationships  # Return basic relationships if no consolidated data
                )

            # Combine consolidated data with relationships
            detailed_info = {
                "venue_data": {
                    "venue_id": venue_data.venue_id,
                    "name": venue_data.name,
                    "location": venue_data.location,
                    "category": venue_data.category,
                    "subcategory": venue_data.subcategory,
                    "address": venue_data.address,
                    "phone": venue_data.phone,
                    "website": venue_data.website,
                    "data_completeness": venue_data.data_completeness,
                    "comprehensive_score": venue_data.comprehensive_score,
                    "data_source_type": venue_data.data_source_type,
                    "last_updated": (
                        venue_data.last_updated.isoformat()
                        if venue_data.last_updated
                        else None
                    ),
                },
                "contextual_data": {
                    "current_weather": venue_data.current_weather,
                    "traffic_conditions": venue_data.traffic_conditions,
                    "social_sentiment": venue_data.social_sentiment,
                    "economic_context": venue_data.economic_context,
                    "foot_traffic": venue_data.foot_traffic,
                    "ml_predictions": venue_data.ml_predictions,
                    "demographic_context": venue_data.demographic_context,
                },
                "relationships": relationships,
                "data_quality": {
                    "source_reliability": venue_data.source_reliability,
                    "data_completeness": venue_data.data_completeness,
                    "has_weather": venue_data.current_weather is not None,
                    "has_traffic": venue_data.traffic_conditions is not None,
                    "has_social": venue_data.social_sentiment is not None,
                    "has_ml_predictions": venue_data.ml_predictions is not None,
                    "has_foot_traffic": venue_data.foot_traffic is not None,
                    "has_economic": venue_data.economic_context is not None,
                    "has_demographics": venue_data.demographic_context is not None,
                },
            }

            return detailed_info

        except Exception as e:
            self.logger.error(f"Error getting venue details: {e}")
            return None

    def search_venues(
        self, query: str, area_bounds: Optional[Dict] = None, limit: int = 10
    ) -> List[ConsolidatedVenueData]:
        """
        Search for venues by name or category.

        Args:
            query: Search query (venue name or category)
            area_bounds: Geographic bounds (defaults to Kansas City)
            limit: Maximum number of results

        Returns:
            List of matching ConsolidatedVenueData objects
        """
        if area_bounds is None:
            area_bounds = self.default_area_bounds

        self.logger.info(f"🔍 Searching venues for query: '{query}'")

        try:
            # Get all venues in area
            venues = self.aggregator.aggregate_venue_data(area_bounds)

            # Filter venues by query
            query_lower = query.lower()
            matching_venues = []

            for venue in venues:
                # Check name match
                if query_lower in venue.name.lower():
                    matching_venues.append(
                        (venue, 1.0)
                    )  # High relevance for name match
                # Check category match
                elif query_lower in venue.category.lower():
                    matching_venues.append(
                        (venue, 0.8)
                    )  # Medium relevance for category match
                # Check subcategory match
                elif venue.subcategory and query_lower in venue.subcategory.lower():
                    matching_venues.append(
                        (venue, 0.6)
                    )  # Lower relevance for subcategory match

            # Sort by relevance and comprehensive score
            matching_venues.sort(
                key=lambda x: (x[1], x[0].comprehensive_score), reverse=True
            )

            # Return top results
            results = [venue for venue, relevance in matching_venues[:limit]]

            self.logger.info(f"✅ Found {len(results)} matching venues")
            return results

        except Exception as e:
            self.logger.error(f"Error searching venues: {e}")
            return []

    def get_area_summary(self, area_bounds: Optional[Dict] = None) -> Dict:
        """
        Get summary statistics for a geographic area.

        Args:
            area_bounds: Geographic bounds (defaults to Kansas City)

        Returns:
            Dictionary with area summary statistics
        """
        if area_bounds is None:
            area_bounds = self.default_area_bounds

        self.logger.info(f"📊 Getting area summary for: {area_bounds}")

        try:
            venues, events = self.aggregator.aggregate_area_data(area_bounds)

            # Calculate venue statistics
            venue_categories = {}
            venue_quality_distribution = {"high": 0, "medium": 0, "low": 0}
            venues_with_context = {
                "weather": 0,
                "traffic": 0,
                "social": 0,
                "ml_predictions": 0,
                "foot_traffic": 0,
                "economic": 0,
                "demographics": 0,
            }

            for venue in venues:
                # Category distribution
                category = venue.category or "unknown"
                venue_categories[category] = venue_categories.get(category, 0) + 1

                # Quality distribution
                if venue.data_completeness >= 0.8:
                    venue_quality_distribution["high"] += 1
                elif venue.data_completeness >= 0.6:
                    venue_quality_distribution["medium"] += 1
                else:
                    venue_quality_distribution["low"] += 1

                # Context availability
                if venue.current_weather:
                    venues_with_context["weather"] += 1
                if venue.traffic_conditions:
                    venues_with_context["traffic"] += 1
                if venue.social_sentiment:
                    venues_with_context["social"] += 1
                if venue.ml_predictions:
                    venues_with_context["ml_predictions"] += 1
                if venue.foot_traffic:
                    venues_with_context["foot_traffic"] += 1
                if venue.economic_context:
                    venues_with_context["economic"] += 1
                if venue.demographic_context:
                    venues_with_context["demographics"] += 1

            # Calculate event statistics
            event_categories = {}
            upcoming_events = 0

            for event in events:
                # Category distribution
                category = event.category or "unknown"
                event_categories[category] = event_categories.get(category, 0) + 1

                # Upcoming events
                if event.start_time and event.start_time > datetime.now():
                    upcoming_events += 1

            area_summary = {
                "timestamp": datetime.now().isoformat(),
                "area_bounds": area_bounds,
                "venue_statistics": {
                    "total_venues": len(venues),
                    "category_distribution": venue_categories,
                    "quality_distribution": venue_quality_distribution,
                    "avg_completeness": (
                        sum(v.data_completeness for v in venues) / len(venues)
                        if venues
                        else 0.0
                    ),
                    "context_availability": venues_with_context,
                },
                "event_statistics": {
                    "total_events": len(events),
                    "upcoming_events": upcoming_events,
                    "category_distribution": event_categories,
                },
                "data_coverage": {
                    "venues_with_weather": venues_with_context["weather"],
                    "venues_with_ml_predictions": venues_with_context["ml_predictions"],
                    "venues_with_social": venues_with_context["social"],
                    "weather_coverage": (
                        venues_with_context["weather"] / len(venues) if venues else 0.0
                    ),
                    "ml_coverage": (
                        venues_with_context["ml_predictions"] / len(venues)
                        if venues
                        else 0.0
                    ),
                    "social_coverage": (
                        venues_with_context["social"] / len(venues) if venues else 0.0
                    ),
                },
            }

            return area_summary

        except Exception as e:
            self.logger.error(f"Error getting area summary: {e}")
            return {"timestamp": datetime.now().isoformat(), "error": str(e)}


# Convenience functions for backward compatibility and easy access
def get_venues_and_events(area_bounds=None, time_period=None):
    """Convenience function to get venues and events."""
    interface = MasterDataInterface()
    return interface.get_venues_and_events(area_bounds, time_period)


def get_data_health_status():
    """Convenience function to get data health status."""
    interface = MasterDataInterface()
    return interface.get_data_health_status()


def refresh_area_data(area_bounds=None, force_refresh=False):
    """Convenience function to refresh area data."""
    interface = MasterDataInterface()
    return interface.refresh_area_data(area_bounds, force_refresh)


def collect_fresh_data(priority_only=False):
    """Convenience function to collect fresh data."""
    interface = MasterDataInterface()
    return interface.collect_fresh_data(priority_only)


if __name__ == "__main__":
    # Test the master data interface
    import logging

    logging.basicConfig(level=logging.INFO)

    interface = MasterDataInterface()

    # Test the key method
    print("Testing get_venues_and_events (THE KEY METHOD)...")
    venues, events = interface.get_venues_and_events()
    print(f"Retrieved {len(venues)} venues and {len(events)} events")

    if venues:
        sample_venue = venues[0]
        print(f"\nSample venue: {sample_venue.name}")
        print(f"Data completeness: {sample_venue.data_completeness:.2f}")
        print(f"Has weather: {sample_venue.current_weather is not None}")
        print(f"Has ML predictions: {sample_venue.ml_predictions is not None}")
        print(f"Has social sentiment: {sample_venue.social_sentiment is not None}")

    # Test health status
    print("\nTesting data health status...")
    health = interface.get_data_health_status()
    print(f"Overall status: {health.get('overall_status', 'unknown')}")
    print(f"Total venues: {health.get('summary', {}).get('total_venues', 0)}")

    # Test area summary
    print("\nTesting area summary...")
    summary = interface.get_area_summary()
    print(
        f"Venue categories: {list(summary.get('venue_statistics', {}).get('category_distribution', {}).keys())}"
    )
    print(
        f"Data coverage - Weather: {summary.get('data_coverage', {}).get('weather_coverage', 0):.2f}"
    )
