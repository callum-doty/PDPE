#!/usr/bin/env python3
"""
Test script for the InteractiveMapBuilder implementation.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder

    print("✓ Successfully imported InteractiveMapBuilder")
except ImportError as e:
    print(f"✗ Failed to import InteractiveMapBuilder: {e}")
    sys.exit(1)


def test_basic_functionality():
    """Test basic functionality of InteractiveMapBuilder."""
    print("\n=== Testing InteractiveMapBuilder ===")

    # Initialize map builder
    try:
        map_builder = InteractiveMapBuilder()
        print("✓ InteractiveMapBuilder initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize InteractiveMapBuilder: {e}")
        return False

    # Test sample events data
    sample_events = [
        {
            "latitude": 39.0997,
            "longitude": -94.5786,
            "name": "Jazz Night",
            "venue_name": "The Blue Room",
            "date": "2024-01-15",
            "total_score": 0.85,
        },
        {
            "latitude": 39.1012,
            "longitude": -94.5844,
            "name": "Food Truck Festival",
            "venue_name": "Power & Light District",
            "date": "2024-01-20",
            "total_score": 0.72,
        },
        {
            "latitude": 39.0739,
            "longitude": -94.5861,
            "name": "Art Gallery Opening",
            "venue_name": "Crossroads Arts District",
            "date": "2024-01-25",
            "total_score": 0.68,
        },
    ]

    # Test event heatmap creation
    try:
        output_file = map_builder.create_event_heatmap(
            events_data=sample_events,
            output_path="test_event_heatmap.html",
            style="streets",
        )
        if output_file and output_file.exists():
            print(f"✓ Event heatmap created successfully: {output_file}")
        else:
            print("✗ Event heatmap creation failed")
            return False
    except Exception as e:
        print(f"✗ Error creating event heatmap: {e}")
        return False

    # Test probability heatmap creation
    sample_probability_data = {
        (39.0997, -94.5786): 0.85,
        (39.1012, -94.5844): 0.72,
        (39.0739, -94.5861): 0.68,
        (39.0458, -94.5833): 0.55,
        (39.1167, -94.6275): 0.63,
    }

    try:
        output_file = map_builder.create_probability_heatmap(
            probability_data=sample_probability_data,
            output_path="test_probability_heatmap.html",
            style="light",
        )
        if output_file and output_file.exists():
            print(f"✓ Probability heatmap created successfully: {output_file}")
        else:
            print("✗ Probability heatmap creation failed")
            return False
    except Exception as e:
        print(f"✗ Error creating probability heatmap: {e}")
        return False

    # Test combined visualization
    try:
        output_file = map_builder.create_combined_visualization(
            events_data=sample_events,
            probability_data=sample_probability_data,
            output_path="test_combined_visualization.html",
            style="streets",
        )
        if output_file and output_file.exists():
            print(f"✓ Combined visualization created successfully: {output_file}")
        else:
            print("✗ Combined visualization creation failed")
            return False
    except Exception as e:
        print(f"✗ Error creating combined visualization: {e}")
        return False

    # Test GeoJSON export
    try:
        output_file = map_builder.export_to_geojson(
            data=sample_events, output_path="test_export.geojson"
        )
        if output_file and output_file.exists():
            print(f"✓ GeoJSON export created successfully: {output_file}")
        else:
            print("✗ GeoJSON export creation failed")
            return False
    except Exception as e:
        print(f"✗ Error creating GeoJSON export: {e}")
        return False

    return True


def test_api_integration():
    """Test API integration."""
    print("\n=== Testing API Integration ===")

    try:
        from backend.models.serve import app, generate_sample_events, GridBounds

        print("✓ Successfully imported API components")
    except ImportError as e:
        print(f"✗ Failed to import API components: {e}")
        return False

    # Test sample events generation
    try:
        bounds = GridBounds(north=39.15, south=39.05, east=-94.50, west=-94.65)
        events = generate_sample_events(bounds, count=5)

        if len(events) == 5:
            print(f"✓ Generated {len(events)} sample events successfully")
            print(f"  Sample event: {events[0]['name']} at {events[0]['venue_name']}")
        else:
            print(f"✗ Expected 5 events, got {len(events)}")
            return False
    except Exception as e:
        print(f"✗ Error generating sample events: {e}")
        return False

    return True


def main():
    """Run all tests."""
    print("Starting InteractiveMapBuilder tests...")

    success = True

    # Test basic functionality
    if not test_basic_functionality():
        success = False

    # Test API integration
    if not test_api_integration():
        success = False

    if success:
        print("\n🎉 All tests passed!")
        print("\nGenerated files:")
        for filename in [
            "test_event_heatmap.html",
            "test_probability_heatmap.html",
            "test_combined_visualization.html",
            "test_export.geojson",
        ]:
            if Path(filename).exists():
                print(f"  - {filename}")

        print(
            "\nYou can open the HTML files in your browser to view the visualizations."
        )
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
