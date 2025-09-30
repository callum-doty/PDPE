#!/usr/bin/env python3
"""
Heatmap Generation Script

Standalone script for generating interactive heatmaps and visualizations.
Part of the PPM application restructuring - Phase 9: Standalone Scripts.
"""

import argparse
import logging
import sys
import json
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from features.visualization.builders.interactive_map_builder import (
    InteractiveMapBuilder,
)
from shared.database.connection import get_db_conn

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("heatmap_generation.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def get_venue_data_for_heatmap():
    """Get venue data with predictions for heatmap generation."""
    logger.info("Fetching venue data for heatmap...")

    try:
        conn = get_db_conn()
        if not conn:
            logger.error("Database connection failed")
            return []

        cur = conn.cursor()

        # Get venues with their predictions and basic info
        query = """
            SELECT DISTINCT 
                v.id, v.name, v.lat, v.lng, v.category, v.subcategory,
                v.address, v.website, v.phone,
                p.prediction_type, p.prediction_value, p.confidence_score
            FROM venues v
            LEFT JOIN predictions p ON v.id = p.venue_id
            WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
            ORDER BY v.id, p.prediction_type
        """

        cur.execute(query)
        rows = cur.fetchall()

        cur.close()
        conn.close()

        # Group predictions by venue
        venues = {}
        for row in rows:
            (
                venue_id,
                name,
                lat,
                lng,
                category,
                subcategory,
                address,
                website,
                phone,
                pred_type,
                pred_value,
                confidence,
            ) = row

            if venue_id not in venues:
                venues[venue_id] = {
                    "id": venue_id,
                    "name": name,
                    "lat": lat,
                    "lng": lng,
                    "category": category,
                    "subcategory": subcategory,
                    "address": address,
                    "website": website,
                    "phone": phone,
                    "predictions": {},
                }

            if pred_type and pred_value is not None:
                venues[venue_id]["predictions"][pred_type] = {
                    "value": pred_value,
                    "confidence": confidence,
                }

        venue_list = list(venues.values())
        logger.info(f"Found {len(venue_list)} venues for heatmap")
        return venue_list

    except Exception as e:
        logger.error(f"Error fetching venue data: {e}")
        return []


def get_event_data_for_heatmap():
    """Get event data for heatmap overlay."""
    logger.info("Fetching event data for heatmap...")

    try:
        conn = get_db_conn()
        if not conn:
            logger.error("Database connection failed")
            return []

        cur = conn.cursor()

        # Get upcoming events
        query = """
            SELECT id, name, start_time, end_time, venue_name, venue_address,
                   category, subcategory, lat, lng, url
            FROM events
            WHERE start_time > NOW()
            AND lat IS NOT NULL AND lng IS NOT NULL
            ORDER BY start_time
            LIMIT 1000
        """

        cur.execute(query)
        events = cur.fetchall()

        cur.close()
        conn.close()

        event_list = []
        for event in events:
            (
                event_id,
                name,
                start_time,
                end_time,
                venue_name,
                venue_address,
                category,
                subcategory,
                lat,
                lng,
                url,
            ) = event
            event_list.append(
                {
                    "id": event_id,
                    "name": name,
                    "start_time": start_time.isoformat() if start_time else None,
                    "end_time": end_time.isoformat() if end_time else None,
                    "venue_name": venue_name,
                    "venue_address": venue_address,
                    "category": category,
                    "subcategory": subcategory,
                    "lat": lat,
                    "lng": lng,
                    "url": url,
                }
            )

        logger.info(f"Found {len(event_list)} events for heatmap")
        return event_list

    except Exception as e:
        logger.error(f"Error fetching event data: {e}")
        return []


def generate_basic_heatmap(venues, events, output_file):
    """Generate a basic heatmap with venues and events."""
    logger.info("Generating basic heatmap...")

    try:
        builder = InteractiveMapBuilder()

        # Add venues to map
        for venue in venues:
            # Determine marker color based on predictions
            color = "blue"  # default
            if venue["predictions"]:
                # Use the highest prediction value to determine color intensity
                max_pred = max(venue["predictions"].values(), key=lambda x: x["value"])
                if max_pred["value"] > 0.8:
                    color = "red"
                elif max_pred["value"] > 0.6:
                    color = "orange"
                elif max_pred["value"] > 0.4:
                    color = "yellow"
                else:
                    color = "green"

            builder.add_venue_marker(
                lat=venue["lat"],
                lng=venue["lng"],
                name=venue["name"],
                category=venue["category"],
                predictions=venue["predictions"],
                color=color,
            )

        # Add events to map
        for event in events:
            builder.add_event_marker(
                lat=event["lat"],
                lng=event["lng"],
                name=event["name"],
                start_time=event["start_time"],
                venue_name=event["venue_name"],
                category=event["category"],
            )

        # Generate the map
        map_html = builder.generate_map()

        # Save to file
        with open(output_file, "w") as f:
            f.write(map_html)

        logger.info(f"Basic heatmap saved to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error generating basic heatmap: {e}")
        return False


def generate_prediction_heatmap(
    venues, output_file, prediction_type="psychographic_score"
):
    """Generate a heatmap focused on specific predictions."""
    logger.info(f"Generating prediction heatmap for {prediction_type}...")

    try:
        builder = InteractiveMapBuilder()

        # Filter venues that have the specified prediction
        venues_with_prediction = [
            v for v in venues if prediction_type in v["predictions"]
        ]

        if not venues_with_prediction:
            logger.warning(f"No venues found with prediction type: {prediction_type}")
            return False

        # Add venues with prediction-based styling
        for venue in venues_with_prediction:
            pred_data = venue["predictions"][prediction_type]
            pred_value = pred_data["value"]
            confidence = pred_data["confidence"]

            # Color based on prediction value
            if pred_value > 0.8:
                color = "darkred"
            elif pred_value > 0.6:
                color = "red"
            elif pred_value > 0.4:
                color = "orange"
            elif pred_value > 0.2:
                color = "yellow"
            else:
                color = "green"

            # Size based on confidence
            radius = max(5, int(confidence * 20))

            builder.add_prediction_marker(
                lat=venue["lat"],
                lng=venue["lng"],
                name=venue["name"],
                prediction_value=pred_value,
                confidence=confidence,
                color=color,
                radius=radius,
            )

        # Generate the map with prediction legend
        map_html = builder.generate_prediction_map(prediction_type)

        # Save to file
        with open(output_file, "w") as f:
            f.write(map_html)

        logger.info(f"Prediction heatmap saved to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error generating prediction heatmap: {e}")
        return False


def generate_category_heatmap(venues, output_file):
    """Generate a heatmap showing venue categories."""
    logger.info("Generating category heatmap...")

    try:
        builder = InteractiveMapBuilder()

        # Color mapping for categories
        category_colors = {
            "major_venue": "red",
            "entertainment_district": "blue",
            "shopping_cultural": "green",
            "museum": "purple",
            "theater": "orange",
            "festival_city": "yellow",
            "aggregator": "pink",
            "nightlife": "darkred",
        }

        # Add venues with category-based styling
        for venue in venues:
            category = venue["category"] or "unknown"
            color = category_colors.get(category, "gray")

            builder.add_category_marker(
                lat=venue["lat"],
                lng=venue["lng"],
                name=venue["name"],
                category=category,
                subcategory=venue["subcategory"],
                color=color,
            )

        # Generate the map with category legend
        map_html = builder.generate_category_map(category_colors)

        # Save to file
        with open(output_file, "w") as f:
            f.write(map_html)

        logger.info(f"Category heatmap saved to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error generating category heatmap: {e}")
        return False


def generate_summary_report(venues, events, generated_maps):
    """Generate a summary report of the heatmap generation."""
    logger.info("=" * 60)
    logger.info("HEATMAP GENERATION SUMMARY")
    logger.info("=" * 60)

    # Basic statistics
    logger.info(f"Total venues processed: {len(venues)}")
    logger.info(f"Total events processed: {len(events)}")

    # Venue category breakdown
    category_counts = {}
    venues_with_predictions = 0

    for venue in venues:
        category = venue["category"] or "unknown"
        category_counts[category] = category_counts.get(category, 0) + 1

        if venue["predictions"]:
            venues_with_predictions += 1

    logger.info("Venue categories:")
    for category, count in sorted(category_counts.items()):
        logger.info(f"  {category}: {count}")

    logger.info(f"Venues with predictions: {venues_with_predictions}")

    # Prediction types
    prediction_types = set()
    for venue in venues:
        prediction_types.update(venue["predictions"].keys())

    if prediction_types:
        logger.info(
            f"Available prediction types: {', '.join(sorted(prediction_types))}"
        )

    # Generated maps
    logger.info("Generated heatmaps:")
    for map_type, success in generated_maps.items():
        status = "✓" if success else "✗"
        logger.info(f"  {status} {map_type}")

    logger.info(f"Heatmap generation completed at: {datetime.now()}")
    logger.info("=" * 60)


def main():
    """Main function to generate heatmaps."""
    parser = argparse.ArgumentParser(description="Generate interactive heatmaps")
    parser.add_argument(
        "--type",
        choices=["basic", "prediction", "category", "all"],
        default="all",
        help="Type of heatmap to generate (default: all)",
    )
    parser.add_argument(
        "--prediction-type",
        default="psychographic_score",
        help="Prediction type for prediction heatmap (default: psychographic_score)",
    )
    parser.add_argument(
        "--output-dir",
        default="./heatmaps",
        help="Output directory for heatmap files (default: ./heatmaps)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting heatmap generation script...")

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # Get data
    venues = get_venue_data_for_heatmap()
    events = get_event_data_for_heatmap()

    if not venues:
        logger.error("No venue data available for heatmap generation")
        return 1

    generated_maps = {}

    # Generate requested heatmaps
    if args.type in ["basic", "all"]:
        output_file = output_dir / "basic_heatmap.html"
        generated_maps["basic"] = generate_basic_heatmap(venues, events, output_file)

    if args.type in ["prediction", "all"]:
        output_file = output_dir / f"prediction_heatmap_{args.prediction_type}.html"
        generated_maps["prediction"] = generate_prediction_heatmap(
            venues, output_file, args.prediction_type
        )

    if args.type in ["category", "all"]:
        output_file = output_dir / "category_heatmap.html"
        generated_maps["category"] = generate_category_heatmap(venues, output_file)

    # Generate summary
    generate_summary_report(venues, events, generated_maps)

    # Check if any maps were successfully generated
    if any(generated_maps.values()):
        logger.info("Heatmap generation script completed successfully")
        return 0
    else:
        logger.error("No heatmaps were successfully generated")
        return 1


if __name__ == "__main__":
    sys.exit(main())
