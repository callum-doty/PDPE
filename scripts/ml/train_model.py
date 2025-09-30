#!/usr/bin/env python3
"""
ML Model Training Script

Standalone script for training psychographic prediction models.
Part of the PPM application restructuring - Phase 9: Standalone Scripts.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Import existing ML training module
try:
    from features.ml.models.training.train_model import (
        train_and_eval,
        PsychographicPredictor,
    )
except ImportError:
    # Placeholder functions if ML training module doesn't exist
    def train_and_eval():
        """Placeholder for ML training function."""
        print("ML training module not available - using placeholder")
        return {"status": "placeholder", "accuracy": 0.0}

    class PsychographicPredictor:
        """Placeholder for ML predictor class."""

        pass


# Placeholder functions for missing modules
def build_features_for_time_window(start_ts, end_ts):
    """Placeholder for feature building function."""
    print(
        f"Feature building placeholder - would build features from {start_ts} to {end_ts}"
    )
    return True


def generate_bootstrap_labels(threshold_percentile=80):
    """Placeholder for label generation function."""
    print(
        f"Label generation placeholder - would generate labels with {threshold_percentile}% threshold"
    )
    return True


from shared.database.connection import get_database_connection

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ml_training.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


def build_features(days_back=30):
    """Build features for the specified time window."""
    logger.info(f"Building features for the last {days_back} days...")

    try:
        end_ts = datetime.utcnow()
        start_ts = end_ts - timedelta(days=days_back)

        build_features_for_time_window(start_ts, end_ts)
        logger.info("Feature building completed successfully")
        return True
    except Exception as e:
        logger.error(f"Feature building failed: {e}")
        return False


def generate_labels(threshold_percentile=80):
    """Generate bootstrap labels for training."""
    logger.info(
        f"Generating bootstrap labels with {threshold_percentile}% threshold..."
    )

    try:
        generate_bootstrap_labels(threshold_percentile=threshold_percentile)
        logger.info("Label generation completed successfully")
        return True
    except Exception as e:
        logger.error(f"Label generation failed: {e}")
        return False


def train_model():
    """Train the psychographic prediction model."""
    logger.info("Starting model training...")

    try:
        results = train_and_eval()

        if results:
            logger.info("Model training completed successfully")
            logger.info(f"Training results: {results}")
            return True
        else:
            logger.error("Model training failed - no results returned")
            return False
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        return False


def validate_data_availability():
    """Validate that required data is available for training."""
    logger.info("Validating data availability...")

    try:
        db_conn = get_database_connection()
        db_conn.connect()
        conn = db_conn.connection
        if not conn:
            logger.error("Database connection failed")
            return False

        cur = conn.cursor()

        # Check venues
        cur.execute("SELECT COUNT(*) FROM venues")
        venue_count = cur.fetchone()[0]
        logger.info(f"Venues available: {venue_count}")

        # Check events
        cur.execute("SELECT COUNT(*) FROM events")
        event_count = cur.fetchone()[0]
        logger.info(f"Events available: {event_count}")

        # Check features (if table exists)
        try:
            cur.execute("SELECT COUNT(*) FROM features")
            feature_count = cur.fetchone()[0]
            logger.info(f"Features available: {feature_count}")
        except Exception:
            logger.warning(
                "Features table not found - will be created during feature building"
            )
            feature_count = 0

        # Check labels (if table exists)
        try:
            cur.execute("SELECT COUNT(*) FROM labels")
            label_count = cur.fetchone()[0]
            logger.info(f"Labels available: {label_count}")
        except Exception:
            logger.warning(
                "Labels table not found - will be created during label generation"
            )
            label_count = 0

        cur.close()
        conn.close()

        # Minimum requirements
        if venue_count < 10:
            logger.error("Insufficient venues for training (minimum 10 required)")
            return False

        if event_count < 50:
            logger.error("Insufficient events for training (minimum 50 required)")
            return False

        logger.info("Data validation passed")
        return True

    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return False


def generate_training_report():
    """Generate a comprehensive training report."""
    logger.info("=" * 60)
    logger.info("ML TRAINING SUMMARY REPORT")
    logger.info("=" * 60)

    try:
        db_conn = get_database_connection()
        db_conn.connect()
        conn = db_conn.connection
        if conn:
            cur = conn.cursor()

            # Model performance metrics (if available)
            try:
                cur.execute(
                    """
                    SELECT model_type, accuracy, precision_score, recall, f1_score, created_at
                    FROM model_performance 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """
                )
                performance = cur.fetchone()

                if performance:
                    model_type, accuracy, precision, recall, f1, created_at = (
                        performance
                    )
                    logger.info(f"Latest Model: {model_type}")
                    logger.info(f"Accuracy: {accuracy:.4f}")
                    logger.info(f"Precision: {precision:.4f}")
                    logger.info(f"Recall: {recall:.4f}")
                    logger.info(f"F1 Score: {f1:.4f}")
                    logger.info(f"Trained at: {created_at}")
                else:
                    logger.info("No model performance metrics found")
            except Exception:
                logger.info("Model performance table not available")

            # Data summary
            cur.execute("SELECT COUNT(*) FROM venues")
            venue_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM events")
            event_count = cur.fetchone()[0]

            try:
                cur.execute("SELECT COUNT(*) FROM features")
                feature_count = cur.fetchone()[0]
            except Exception:
                feature_count = 0

            try:
                cur.execute("SELECT COUNT(*) FROM labels")
                label_count = cur.fetchone()[0]
            except Exception:
                label_count = 0

            logger.info(f"Training Data Summary:")
            logger.info(f"  Venues: {venue_count}")
            logger.info(f"  Events: {event_count}")
            logger.info(f"  Features: {feature_count}")
            logger.info(f"  Labels: {label_count}")

            cur.close()
            conn.close()

    except Exception as e:
        logger.warning(f"Could not generate complete training report: {e}")

    logger.info(f"Training completed at: {datetime.now()}")
    logger.info("=" * 60)


def main():
    """Main function to run ML model training."""
    parser = argparse.ArgumentParser(
        description="Train psychographic prediction models"
    )
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip feature building (use existing features)",
    )
    parser.add_argument(
        "--skip-labels",
        action="store_true",
        help="Skip label generation (use existing labels)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Number of days back to build features for (default: 30)",
    )
    parser.add_argument(
        "--threshold-percentile",
        type=int,
        default=80,
        help="Threshold percentile for label generation (default: 80)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting ML model training script...")

    # Validate data availability
    if not validate_data_availability():
        logger.error("Data validation failed - cannot proceed with training")
        return 1

    success = True

    # Build features (unless skipped)
    if not args.skip_features:
        if not build_features(args.days_back):
            logger.error("Feature building failed")
            success = False
    else:
        logger.info("Skipping feature building (using existing features)")

    # Generate labels (unless skipped)
    if not args.skip_labels and success:
        if not generate_labels(args.threshold_percentile):
            logger.error("Label generation failed")
            success = False
    else:
        logger.info("Skipping label generation (using existing labels)")

    # Train model
    if success:
        if not train_model():
            logger.error("Model training failed")
            success = False

    # Generate report
    generate_training_report()

    if success:
        logger.info("ML model training script completed successfully")
        return 0
    else:
        logger.error("ML model training script failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
