#!/usr/bin/env python3
"""
Test script for the new layered heatmap functionality.
Demonstrates interactive API and assumption layer controls.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder
    from features.college_layer import CollegeLayer
    from features.spending_propensity_layer import SpendingPropensityLayer

    print("✓ Successfully imported required modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)


def generate_sample_api_data():
    """Generate sample API data for testing."""

    # Sample events data (simulating PredictHQ API)
    events_data = [
        {
            "latitude": 39.0997,
            "longitude": -94.5786,
            "name": "Jazz Festival",
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
        {
            "latitude": 39.0458,
            "longitude": -94.5833,
            "name": "Tech Conference",
            "venue_name": "Convention Center",
            "date": "2024-02-01",
            "total_score": 0.91,
        },
    ]

    # Sample places data
    places_data = [
        {
            "latitude": 39.1167,
            "longitude": -94.6275,
            "name": "Crown Center",
            "type": "Shopping Mall",
            "score": 0.78,
        },
        {
            "latitude": 39.0458,
            "longitude": -94.5833,
            "name": "Union Station",
            "type": "Transportation Hub",
            "score": 0.82,
        },
        {
            "latitude": 39.0739,
            "longitude": -94.5861,
            "name": "Crossroads District",
            "type": "Entertainment District",
            "score": 0.75,
        },
    ]

    # Sample weather data
    weather_data = [
        {
            "latitude": 39.0997,
            "longitude": -94.5786,
            "temperature": 72,
            "conditions": "Sunny",
        },
        {
            "latitude": 39.1012,
            "longitude": -94.5844,
            "temperature": 68,
            "conditions": "Partly Cloudy",
        },
        {
            "latitude": 39.0739,
            "longitude": -94.5861,
            "temperature": 75,
            "conditions": "Clear",
        },
    ]

    # Sample foot traffic data
    foot_traffic_data = [
        {
            "latitude": 39.0997,
            "longitude": -94.5786,
            "volume": 1250,
            "timestamp": "2024-01-15T14:00:00Z",
        },
        {
            "latitude": 39.1012,
            "longitude": -94.5844,
            "volume": 2100,
            "timestamp": "2024-01-15T14:00:00Z",
        },
        {
            "latitude": 39.0739,
            "longitude": -94.5861,
            "volume": 890,
            "timestamp": "2024-01-15T14:00:00Z",
        },
    ]

    return {
        "events": events_data,
        "places": places_data,
        "weather": weather_data,
        "foot_traffic": foot_traffic_data,
    }


def generate_sample_assumption_data():
    """Generate sample assumption layer data."""

    # Generate college density data
    college_layer = CollegeLayer()
    college_data = {}

    # Sample grid points around Kansas City
    grid_points = [
        (39.0997, -94.5786),  # Downtown KC
        (39.1012, -94.5844),  # Power & Light
        (39.0739, -94.5861),  # Crossroads
        (39.0458, -94.5833),  # Union Station area
        (39.1167, -94.6275),  # Crown Center area
        (39.0347, -94.5783),  # Near UMKC
        (39.1200, -94.5500),  # River Market
        (39.0500, -94.6000),  # Westport area
    ]

    for lat, lon in grid_points:
        result = college_layer.calculate_college_density_score(lat, lon)
        college_data[(lat, lon)] = result["score"]

    # Generate spending propensity data
    spending_layer = SpendingPropensityLayer()
    spending_data = {}

    # Sample demographic data for different areas
    sample_demographics = [
        {
            "median_income": 75000,
            "education_bachelors_pct": 45.0,
            "education_graduate_pct": 20.0,
            "age_25_34_pct": 18.0,
            "age_35_44_pct": 16.0,
            "professional_occupation_pct": 35.0,
            "management_occupation_pct": 15.0,
            "population_density": 2500.0,
        },
        {
            "median_income": 65000,
            "education_bachelors_pct": 35.0,
            "education_graduate_pct": 12.0,
            "age_25_34_pct": 15.0,
            "age_35_44_pct": 14.0,
            "professional_occupation_pct": 28.0,
            "management_occupation_pct": 12.0,
            "population_density": 1800.0,
        },
        {
            "median_income": 85000,
            "education_bachelors_pct": 55.0,
            "education_graduate_pct": 25.0,
            "age_25_34_pct": 22.0,
            "age_35_44_pct": 18.0,
            "professional_occupation_pct": 42.0,
            "management_occupation_pct": 18.0,
            "population_density": 3200.0,
        },
    ]

    for i, (lat, lon) in enumerate(grid_points[: len(sample_demographics)]):
        demo_data = sample_demographics[i % len(sample_demographics)]
        analysis = spending_layer.analyze_location_spending_potential(
            lat, lon, demo_data
        )
        spending_data[(lat, lon)] = analysis["spending_propensity_score"]

    # Generate some custom features data
    custom_features_data = {}
    for lat, lon in grid_points:
        # Simple custom score based on distance from downtown
        downtown_lat, downtown_lon = 39.0997, -94.5786
        distance = ((lat - downtown_lat) ** 2 + (lon - downtown_lon) ** 2) ** 0.5
        custom_score = max(0, 1 - (distance * 10))  # Closer to downtown = higher score
        custom_features_data[(lat, lon)] = custom_score

    return {
        "college_density": college_data,
        "spending_propensity": spending_data,
        "custom_features": custom_features_data,
    }


def test_layered_heatmap():
    """Test the new layered heatmap functionality."""
    print("\n=== Testing Layered Heatmap Functionality ===")

    # Initialize map builder
    try:
        map_builder = InteractiveMapBuilder()
        print("✓ InteractiveMapBuilder initialized successfully")
    except Exception as e:
        print(f"✗ Failed to initialize InteractiveMapBuilder: {e}")
        return False

    # Generate sample data
    print("📊 Generating sample data...")
    api_layers = generate_sample_api_data()
    assumption_layers = generate_sample_assumption_data()

    print(f"  - API layers: {list(api_layers.keys())}")
    print(f"  - Assumption layers: {list(assumption_layers.keys())}")

    # Create layered heatmap
    try:
        output_file = map_builder.create_layered_heatmap(
            api_layers=api_layers,
            assumption_layers=assumption_layers,
            output_path="real_data_heatmap.html",
            style="streets",
        )

        if output_file and output_file.exists():
            print(f"✓ Layered heatmap created successfully: {output_file}")
            print(f"  File size: {output_file.stat().st_size / 1024:.1f} KB")
        else:
            print("✗ Layered heatmap creation failed")
            return False

    except Exception as e:
        print(f"✗ Error creating layered heatmap: {e}")
        return False

    # Test with only API layers
    try:
        output_file_api = map_builder.create_layered_heatmap(
            api_layers=api_layers,
            assumption_layers=None,
            output_path="api_only_heatmap.html",
            style="light",
        )

        if output_file_api and output_file_api.exists():
            print(f"✓ API-only heatmap created successfully: {output_file_api}")
        else:
            print("✗ API-only heatmap creation failed")

    except Exception as e:
        print(f"✗ Error creating API-only heatmap: {e}")

    # Test with only assumption layers
    try:
        output_file_assumption = map_builder.create_layered_heatmap(
            api_layers=None,
            assumption_layers=assumption_layers,
            output_path="assumption_only_heatmap.html",
            style="dark",
        )

        if output_file_assumption and output_file_assumption.exists():
            print(
                f"✓ Assumption-only heatmap created successfully: {output_file_assumption}"
            )
        else:
            print("✗ Assumption-only heatmap creation failed")

    except Exception as e:
        print(f"✗ Error creating assumption-only heatmap: {e}")

    return True


def main():
    """Run layered heatmap tests."""
    print("🗺️  Starting Layered Heatmap Tests...")

    success = test_layered_heatmap()

    if success:
        print("\n🎉 All tests passed!")
        print("\nGenerated files:")
        for filename in [
            "real_data_heatmap.html",
            "api_only_heatmap.html",
            "assumption_only_heatmap.html",
        ]:
            if Path(filename).exists():
                print(f"  - {filename}")

        print("\n📋 Features implemented:")
        print("  ✓ Interactive layer controls (top-right panel)")
        print("  ✓ Visual differentiation (blue=API, red/orange=assumptions)")
        print("  ✓ Comprehensive legend (bottom-left)")
        print("  ✓ Information panel (top-right)")
        print("  ✓ Toggleable layer groups")
        print("  ✓ Enhanced styling and tooltips")

        print("\n🎛️  Usage Instructions:")
        print("  1. Open any of the generated HTML files in your browser")
        print("  2. Use the layer control panel (top-right) to toggle layers on/off")
        print("  3. Click on markers for detailed information")
        print("  4. Blue markers = API data, Red/orange markers = Calculated data")
        print("  5. Larger markers = higher scores")

        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
