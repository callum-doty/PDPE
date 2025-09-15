#!/usr/bin/env python3
"""
Generate a comprehensive layered heatmap combining all available data sources.
This creates an interactive map with toggleable layers for API data and assumption-based calculations.
"""

import sys
import os
from pathlib import Path
import random
import math
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder

    print("✓ Successfully imported InteractiveMapBuilder")
except ImportError as e:
    print(f"✗ Failed to import InteractiveMapBuilder: {e}")
    sys.exit(1)


def generate_sample_events_data(count=25):
    """Generate sample events data for Kansas City area."""
    # Kansas City area bounds
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    event_types = [
        "Jazz Night",
        "Food Truck Festival",
        "Art Gallery Opening",
        "Tech Meetup",
        "Networking Event",
        "Business Conference",
        "Startup Pitch Night",
        "Wine Tasting",
        "Live Music",
        "Comedy Show",
        "Cultural Festival",
        "Career Fair",
        "Professional Workshop",
        "Social Mixer",
        "Rooftop Party",
        "Craft Beer Event",
    ]

    venues = [
        "The Blue Room",
        "Power & Light District",
        "Crossroads Arts District",
        "Sprint Center",
        "Union Station",
        "Country Club Plaza",
        "Westport",
        "River Market",
        "18th & Vine",
        "Crown Center",
        "Brookside",
        "Midtown",
        "Downtown KC",
        "The Kauffman Center",
        "Nelson-Atkins Museum",
    ]

    events = []
    for i in range(count):
        lat = random.uniform(lat_min, lat_max)
        lng = random.uniform(lng_min, lng_max)

        # Generate psychographic score based on location (higher scores near downtown/cultural areas)
        downtown_distance = math.sqrt((lat - 39.0997) ** 2 + (lng + 94.5786) ** 2)
        base_score = max(
            0.1, 1.0 - (downtown_distance * 10)
        )  # Closer to downtown = higher score
        score = min(1.0, base_score + random.uniform(-0.2, 0.3))

        event = {
            "latitude": lat,
            "longitude": lng,
            "name": random.choice(event_types),
            "venue_name": random.choice(venues),
            "date": (datetime.now() + timedelta(days=random.randint(1, 30))).strftime(
                "%Y-%m-%d"
            ),
            "total_score": round(score, 3),
            "predicted_attendance": random.randint(50, 500),
            "ticket_price": random.choice([0, 15, 25, 35, 50, 75]),
            "event_type": random.choice(
                ["professional", "social", "cultural", "entertainment"]
            ),
        }
        events.append(event)

    return events


def generate_sample_places_data(count=30):
    """Generate sample places data for Kansas City area."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    place_types = [
        "coworking_space",
        "coffee_shop",
        "restaurant",
        "bar",
        "gym",
        "retail",
        "business_center",
        "hotel",
        "entertainment",
        "cultural",
        "educational",
    ]

    place_names = [
        "Starbucks",
        "Local Coffee Co",
        "The Roasterie",
        "Bluestem",
        "LC's Bar-B-Q",
        "Joe's Kansas City",
        "Power & Light",
        "Sprint Center",
        "Union Station",
        "Nelson-Atkins",
        "UMKC",
        "Rockhurst University",
        "WeWork",
        "Regus",
    ]

    places = []
    for i in range(count):
        lat = random.uniform(lat_min, lat_max)
        lng = random.uniform(lng_min, lng_max)

        # Generate score based on place type and location
        place_type = random.choice(place_types)

        # Higher scores for business/professional venues
        type_multiplier = {
            "coworking_space": 0.9,
            "business_center": 0.85,
            "coffee_shop": 0.7,
            "restaurant": 0.6,
            "bar": 0.5,
            "cultural": 0.75,
            "educational": 0.8,
        }.get(place_type, 0.5)

        downtown_distance = math.sqrt((lat - 39.0997) ** 2 + (lng + 94.5786) ** 2)
        location_score = max(0.1, 1.0 - (downtown_distance * 8))

        score = min(1.0, (type_multiplier * location_score) + random.uniform(-0.1, 0.2))

        place = {
            "latitude": lat,
            "longitude": lng,
            "name": random.choice(place_names),
            "type": place_type,
            "score": round(score, 3),
            "rating": round(random.uniform(3.5, 5.0), 1),
            "price_tier": random.randint(1, 4),
        }
        places.append(place)

    return places


def generate_sample_weather_data(count=15):
    """Generate sample weather data points."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    conditions = ["Clear", "Partly Cloudy", "Cloudy", "Light Rain", "Sunny", "Overcast"]

    weather_data = []
    for i in range(count):
        lat = random.uniform(lat_min, lat_max)
        lng = random.uniform(lng_min, lng_max)

        weather = {
            "latitude": lat,
            "longitude": lng,
            "temperature": random.randint(45, 85),
            "conditions": random.choice(conditions),
            "humidity": random.randint(30, 80),
            "wind_speed": random.randint(0, 15),
        }
        weather_data.append(weather)

    return weather_data


def generate_sample_foot_traffic_data(count=20):
    """Generate sample foot traffic data."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    traffic_data = []
    for i in range(count):
        lat = random.uniform(lat_min, lat_max)
        lng = random.uniform(lng_min, lng_max)

        # Higher traffic near downtown and popular areas
        downtown_distance = math.sqrt((lat - 39.0997) ** 2 + (lng + 94.5786) ** 2)
        base_volume = max(10, 200 - (downtown_distance * 500))
        volume = int(base_volume + random.uniform(-50, 100))

        traffic = {
            "latitude": lat,
            "longitude": lng,
            "volume": max(0, volume),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dwell_time": random.randint(5, 45),
        }
        traffic_data.append(traffic)

    return traffic_data


def generate_college_density_data(grid_size=50):
    """Generate college density assumption layer data."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    # Known college locations in KC area
    college_locations = [
        (39.0354, -94.5781),  # UMKC
        (39.0189, -94.6708),  # Rockhurst University
        (39.1142, -94.6275),  # Park University
        (39.0458, -94.5833),  # UMKC Downtown
    ]

    college_density = {}

    # Create grid
    lat_step = (lat_max - lat_min) / grid_size
    lng_step = (lng_max - lng_min) / grid_size

    for i in range(grid_size):
        for j in range(grid_size):
            lat = lat_min + (i * lat_step)
            lng = lng_min + (j * lng_step)

            # Calculate density based on distance to colleges
            total_influence = 0
            for college_lat, college_lng in college_locations:
                distance = math.sqrt(
                    (lat - college_lat) ** 2 + (lng - college_lng) ** 2
                )
                influence = max(
                    0, 1.0 - (distance * 20)
                )  # Influence decreases with distance
                total_influence += influence

            # Normalize and add some randomness
            density = min(1.0, total_influence + random.uniform(-0.1, 0.1))

            if density > 0.1:  # Only include points with meaningful density
                college_density[(lat, lng)] = round(density, 3)

    return college_density


def generate_spending_propensity_data(grid_size=40):
    """Generate spending propensity assumption layer data."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    # High-income/high-spending areas in KC
    affluent_areas = [
        (39.0458, -94.5833),  # Country Club Plaza
        (39.0739, -94.5861),  # Crossroads
        (39.0997, -94.5786),  # Downtown/Power & Light
        (39.0354, -94.5781),  # Midtown
        (39.1167, -94.6275),  # Northland
    ]

    spending_propensity = {}

    # Create grid
    lat_step = (lat_max - lat_min) / grid_size
    lng_step = (lng_max - lng_min) / grid_size

    for i in range(grid_size):
        for j in range(grid_size):
            lat = lat_min + (i * lat_step)
            lng = lng_min + (j * lng_step)

            # Calculate propensity based on distance to affluent areas
            total_influence = 0
            for area_lat, area_lng in affluent_areas:
                distance = math.sqrt((lat - area_lat) ** 2 + (lng - area_lng) ** 2)
                influence = max(0, 1.0 - (distance * 15))
                total_influence += influence

            # Add demographic factors (simulated)
            demographic_factor = random.uniform(0.3, 0.8)
            propensity = min(1.0, (total_influence * 0.7) + (demographic_factor * 0.3))

            if propensity > 0.2:  # Only include meaningful propensity scores
                spending_propensity[(lat, lng)] = round(propensity, 3)

    return spending_propensity


def generate_custom_features_data(grid_size=30):
    """Generate custom features assumption layer data."""
    lat_min, lat_max = 38.9517, 39.3209
    lng_min, lng_max = -94.7417, -94.3461

    custom_features = {}

    # Create grid
    lat_step = (lat_max - lat_min) / grid_size
    lng_step = (lng_max - lng_min) / grid_size

    for i in range(grid_size):
        for j in range(grid_size):
            lat = lat_min + (i * lat_step)
            lng = lng_min + (j * lng_step)

            # Combine multiple factors for custom feature score
            downtown_proximity = max(
                0, 1.0 - (math.sqrt((lat - 39.0997) ** 2 + (lng + 94.5786) ** 2) * 12)
            )
            transit_accessibility = random.uniform(0.2, 0.9)
            business_density = random.uniform(0.1, 0.8)
            cultural_activity = random.uniform(0.3, 0.7)

            # Weighted combination
            feature_score = (
                downtown_proximity * 0.3
                + transit_accessibility * 0.25
                + business_density * 0.25
                + cultural_activity * 0.2
            )

            if feature_score > 0.3:  # Only include meaningful scores
                custom_features[(lat, lng)] = round(feature_score, 3)

    return custom_features


def main():
    """Generate comprehensive layered heatmap with all data sources."""
    print("🗺️  Generating Comprehensive Layered Heatmap")
    print("=" * 50)

    # Initialize map builder
    try:
        map_builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))
        print("✓ InteractiveMapBuilder initialized")
    except Exception as e:
        print(f"✗ Failed to initialize InteractiveMapBuilder: {e}")
        return 1

    # Generate API data layers
    print("\n📡 Generating API Data Layers...")

    print("  - Generating events data...")
    events_data = generate_sample_events_data(25)
    print(f"    Generated {len(events_data)} events")

    print("  - Generating places data...")
    places_data = generate_sample_places_data(30)
    print(f"    Generated {len(places_data)} places")

    print("  - Generating weather data...")
    weather_data = generate_sample_weather_data(15)
    print(f"    Generated {len(weather_data)} weather points")

    print("  - Generating foot traffic data...")
    foot_traffic_data = generate_sample_foot_traffic_data(20)
    print(f"    Generated {len(foot_traffic_data)} traffic points")

    api_layers = {
        "events": events_data,
        "places": places_data,
        "weather": weather_data,
        "foot_traffic": foot_traffic_data,
    }

    # Generate assumption layers
    print("\n🧠 Generating Assumption Layers...")

    print("  - Calculating college density...")
    college_density = generate_college_density_data(50)
    print(f"    Generated {len(college_density)} college density points")

    print("  - Calculating spending propensity...")
    spending_propensity = generate_spending_propensity_data(40)
    print(f"    Generated {len(spending_propensity)} spending propensity points")

    print("  - Calculating custom features...")
    custom_features = generate_custom_features_data(30)
    print(f"    Generated {len(custom_features)} custom feature points")

    assumption_layers = {
        "college_density": college_density,
        "spending_propensity": spending_propensity,
        "custom_features": custom_features,
    }

    # Create comprehensive layered heatmap
    print("\n🎨 Creating Comprehensive Layered Heatmap...")

    try:
        output_file = map_builder.create_layered_heatmap(
            api_layers=api_layers,
            assumption_layers=assumption_layers,
            output_path="comprehensive_layered_heatmap.html",
            style="streets",
        )

        if output_file and output_file.exists():
            print(f"✓ Comprehensive layered heatmap created: {output_file}")

            # Display summary statistics
            print(f"\n📊 Data Summary:")
            print(f"  API Data Layers:")
            print(f"    - Events: {len(events_data)} points")
            print(f"    - Places: {len(places_data)} points")
            print(f"    - Weather: {len(weather_data)} points")
            print(f"    - Foot Traffic: {len(foot_traffic_data)} points")
            print(f"  Assumption Layers:")
            print(f"    - College Density: {len(college_density)} grid points")
            print(f"    - Spending Propensity: {len(spending_propensity)} grid points")
            print(f"    - Custom Features: {len(custom_features)} grid points")

            # Open in browser
            try:
                map_builder.open_in_browser(output_file)
                print(f"\n🌐 Map opened in browser!")
            except Exception as e:
                print(f"⚠️  Could not auto-open browser: {e}")
                print(f"   Please manually open: {output_file.absolute()}")

            print(f"\n🎉 Comprehensive map generation completed successfully!")
            print(f"📁 Output file: {output_file.absolute()}")

            return 0

        else:
            print("✗ Failed to create comprehensive layered heatmap")
            return 1

    except Exception as e:
        print(f"✗ Error creating comprehensive layered heatmap: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
