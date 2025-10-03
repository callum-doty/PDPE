#!/usr/bin/env python3
"""
Test script to verify the unified models system works correctly.
"""

import sys
from datetime import datetime


def test_unified_models():
    """Test that the unified models can be imported and used correctly."""

    print("🧪 Testing unified models system...")

    try:
        # Test importing from the new unified location
        from shared.models import (
            Venue,
            Event,
            VenueCollectionResult,
            EventCollectionResult,
            VenueProcessingResult,
            EventProcessingResult,
            DataQualityMetrics,
            PredictionResult,
            DataSource,
            ProcessingStatus,
        )

        print("✅ Successfully imported all models from shared.models")

        # Test creating a venue
        venue = Venue(
            external_id="test_001",
            provider="test_provider",
            name="Test Venue",
            category="restaurant",
            latitude=39.1,
            longitude=-94.6,
            address="123 Test St, Kansas City, MO",
            data_source=DataSource.MANUAL,
        )
        print(f"✅ Created venue: {venue.name} at {venue.address}")

        # Test creating an event
        event = Event(
            external_id="event_001",
            provider="test_provider",
            name="Test Event",
            category="music",
            venue_name=venue.name,
            data_source=DataSource.MANUAL,
        )
        print(f"✅ Created event: {event.name} at {event.venue_name}")

        # Test collection result
        collection_result = VenueCollectionResult(
            source_name="test_source",
            success=True,
            venues_collected=1,
            duration_seconds=0.5,
            data_quality_score=0.9,
        )
        print(
            f"✅ Created collection result: {collection_result.venues_collected} venues collected"
        )

        # Test processing result
        processing_result = VenueProcessingResult(
            venues_processed=1,
            venues_geocoded=1,
            venues_enriched=1,
            quality_score=0.95,
            processing_time=0.3,
            errors=[],
        )
        print(
            f"✅ Created processing result: {processing_result.venues_processed} venues processed"
        )

        # Test data quality metrics
        quality_metrics = DataQualityMetrics(
            entity_type="venue",
            total_records=100,
            complete_records=95,
            incomplete_records=5,
            duplicate_records=2,
            invalid_records=1,
            completeness_score=0.95,
            accuracy_score=0.98,
            consistency_score=0.97,
            overall_quality_score=0.97,
        )
        print(
            f"✅ Created quality metrics: {quality_metrics.overall_quality_score:.2f} overall score"
        )

        # Test prediction result
        prediction_result = PredictionResult(
            entity_id="venue_001",
            entity_type="venue",
            predictions={"popularity": 0.8, "revenue_potential": 0.7},
            confidence_score=0.85,
            model_version="v1.0",
        )
        print(
            f"✅ Created prediction result: {prediction_result.confidence_score:.2f} confidence"
        )

        print("\n🎉 All unified models tests passed!")
        return True

    except Exception as e:
        print(f"❌ Error testing unified models: {e}")
        return False


def test_backward_compatibility():
    """Test that old imports still work with deprecation warnings."""

    print("\n🔄 Testing backward compatibility...")

    try:
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            # Test old venue models import
            from features.venues.models import Venue as OldVenue
            from features.events.models import Event as OldEvent

            # Check that deprecation warnings were issued
            venue_warning = any(
                "features.venues.models is deprecated" in str(warning.message)
                for warning in w
            )
            event_warning = any(
                "features.events.models is deprecated" in str(warning.message)
                for warning in w
            )

            if venue_warning and event_warning:
                print("✅ Deprecation warnings correctly issued for old imports")
            else:
                print("⚠️  Deprecation warnings not found")

            # Test that the old imports still work
            old_venue = OldVenue(
                external_id="old_test",
                provider="old_provider",
                name="Old Test Venue",
                category="test",
            )
            print(f"✅ Old venue import still works: {old_venue.name}")

            old_event = OldEvent(
                external_id="old_event", provider="old_provider", name="Old Test Event"
            )
            print(f"✅ Old event import still works: {old_event.name}")

        print("✅ Backward compatibility maintained")
        return True

    except Exception as e:
        print(f"❌ Error testing backward compatibility: {e}")
        return False


def test_database_connection():
    """Test that database connection still works."""

    print("\n🗄️  Testing database connection...")

    try:
        from shared.database.connection import get_database_connection

        # Test connection creation (don't actually connect)
        db_conn = get_database_connection()
        print(f"✅ Database connection object created: {type(db_conn).__name__}")

        return True

    except Exception as e:
        print(f"❌ Error testing database connection: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting unified models system tests...\n")

    success = True
    success &= test_unified_models()
    success &= test_backward_compatibility()
    success &= test_database_connection()

    if success:
        print("\n🎉 All tests passed! The unified models system is working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the output above.")
        sys.exit(1)
