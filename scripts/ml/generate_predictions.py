#!/usr/bin/env python3
"""
ML Predictions Generation Script

Standalone script for generating psychographic predictions using trained models.
Part of the PPM application restructuring - Phase 9: Standalone Scripts.
"""

import argparse
import logging
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from features.ml.models.inference.predictor import PsychographicPredictor
from shared.database.connection import get_db_conn

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ml_predictions.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def load_trained_model():
    """Load the latest trained model."""
    logger.info("Loading trained model...")

    try:
        predictor = PsychographicPredictor()
        if predictor.load_model():
            logger.info("Model loaded successfully")
            return predictor
        else:
            logger.error("Failed to load model")
            return None
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        return None


def get_venues_for_prediction(limit=None):
    """Get venues that need predictions."""
    logger.info("Fetching venues for prediction...")

    try:
        conn = get_db_conn()
        if not conn:
            logger.error("Database connection failed")
            return []

        cur = conn.cursor()

        # Get venues with their latest features
        query = """
            SELECT DISTINCT v.id, v.name, v.lat, v.lng, v.category, v.subcategory
            FROM venues v
            WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
        """

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        venues = cur.fetchall()

        cur.close()
        conn.close()

        logger.info(f"Found {len(venues)} venues for prediction")
        return venues

    except Exception as e:
        logger.error(f"Error fetching venues: {e}")
        return []


def generate_predictions_for_venues(predictor, venues, output_file=None):
    """Generate predictions for a list of venues."""
    logger.info(f"Generating predictions for {len(venues)} venues...")

    predictions = []
    successful_predictions = 0

    for venue in venues:
        venue_id, name, lat, lng, category, subcategory = venue

        try:
            # Generate prediction for this venue
            prediction_data = {
                "venue_id": venue_id,
                "lat": lat,
                "lng": lng,
                "category": category,
                "subcategory": subcategory,
            }

            prediction = predictor.predict_single_venue(prediction_data)

            if prediction:
                prediction_result = {
                    "venue_id": venue_id,
                    "venue_name": name,
                    "lat": lat,
                    "lng": lng,
                    "category": category,
                    "subcategory": subcategory,
                    "prediction": prediction,
                    "timestamp": datetime.now().isoformat(),
                }

                predictions.append(prediction_result)
                successful_predictions += 1

                logger.debug(f"Generated prediction for {name}: {prediction}")
            else:
                logger.warning(f"No prediction generated for venue {name}")

        except Exception as e:
            logger.error(f"Error generating prediction for venue {name}: {e}")

    logger.info(f"Successfully generated {successful_predictions} predictions")

    # Save predictions to file if specified
    if output_file:
        try:
            with open(output_file, "w") as f:
                json.dump(predictions, f, indent=2, default=str)
            logger.info(f"Predictions saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving predictions to file: {e}")

    return predictions


def store_predictions_in_database(predictions):
    """Store predictions in the database."""
    logger.info(f"Storing {len(predictions)} predictions in database...")

    try:
        conn = get_db_conn()
        if not conn:
            logger.error("Database connection failed")
            return False

        cur = conn.cursor()

        # Create predictions table if it doesn't exist
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                venue_id INTEGER REFERENCES venues(id),
                prediction_type VARCHAR(50),
                prediction_value FLOAT,
                confidence_score FLOAT,
                model_version VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(venue_id, prediction_type, model_version)
            )
        """
        )

        stored_count = 0

        for pred in predictions:
            try:
                # Extract prediction details
                venue_id = pred["venue_id"]
                prediction = pred["prediction"]

                # Store main prediction
                if isinstance(prediction, dict):
                    for pred_type, pred_value in prediction.items():
                        if isinstance(pred_value, (int, float)):
                            cur.execute(
                                """
                                INSERT INTO predictions (
                                    venue_id, prediction_type, prediction_value, 
                                    confidence_score, model_version
                                ) VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (venue_id, prediction_type, model_version) 
                                DO UPDATE SET
                                    prediction_value = EXCLUDED.prediction_value,
                                    confidence_score = EXCLUDED.confidence_score,
                                    created_at = CURRENT_TIMESTAMP
                            """,
                                (
                                    venue_id,
                                    pred_type,
                                    pred_value,
                                    prediction.get("confidence", 0.5),
                                    "v1.0",  # Model version
                                ),
                            )
                            stored_count += 1

            except Exception as e:
                logger.warning(
                    f"Failed to store prediction for venue {pred.get('venue_name', 'Unknown')}: {e}"
                )

        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Successfully stored {stored_count} predictions in database")
        return True

    except Exception as e:
        logger.error(f"Error storing predictions in database: {e}")
        return False


def generate_prediction_summary(predictions):
    """Generate a summary of the predictions."""
    logger.info("=" * 60)
    logger.info("PREDICTION GENERATION SUMMARY")
    logger.info("=" * 60)

    if not predictions:
        logger.info("No predictions generated")
        return

    # Basic statistics
    total_predictions = len(predictions)
    logger.info(f"Total predictions generated: {total_predictions}")

    # Category breakdown
    category_counts = {}
    for pred in predictions:
        category = pred.get("category", "Unknown")
        category_counts[category] = category_counts.get(category, 0) + 1

    logger.info("Predictions by category:")
    for category, count in sorted(category_counts.items()):
        logger.info(f"  {category}: {count}")

    # Prediction type analysis
    prediction_types = set()
    for pred in predictions:
        if isinstance(pred.get("prediction"), dict):
            prediction_types.update(pred["prediction"].keys())

    logger.info(f"Prediction types generated: {', '.join(sorted(prediction_types))}")

    # Sample predictions
    logger.info("Sample predictions:")
    for i, pred in enumerate(predictions[:3]):
        logger.info(f"  {i+1}. {pred['venue_name']}: {pred['prediction']}")

    logger.info(f"Prediction generation completed at: {datetime.now()}")
    logger.info("=" * 60)


def main():
    """Main function to generate ML predictions."""
    parser = argparse.ArgumentParser(description="Generate psychographic predictions")
    parser.add_argument("--limit", type=int, help="Limit number of venues to process")
    parser.add_argument(
        "--output", "-o", help="Output file to save predictions (JSON format)"
    )
    parser.add_argument(
        "--no-database", action="store_true", help="Don't store predictions in database"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting ML predictions generation script...")

    # Load trained model
    predictor = load_trained_model()
    if not predictor:
        logger.error("Cannot proceed without a trained model")
        return 1

    # Get venues for prediction
    venues = get_venues_for_prediction(args.limit)
    if not venues:
        logger.error("No venues found for prediction")
        return 1

    # Generate predictions
    predictions = generate_predictions_for_venues(predictor, venues, args.output)

    if not predictions:
        logger.error("No predictions were generated")
        return 1

    # Store in database (unless disabled)
    if not args.no_database:
        if not store_predictions_in_database(predictions):
            logger.warning("Failed to store predictions in database")

    # Generate summary
    generate_prediction_summary(predictions)

    logger.info("ML predictions generation script completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())
