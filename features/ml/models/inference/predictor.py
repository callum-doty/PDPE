# ML Prediction Collector
"""
Standardized ML prediction collector that consolidates ML prediction generation
into a unified collector with consistent interfaces.
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Also add the current working directory to ensure imports work
import os

current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import modules with fallback handling
get_db_conn = None
QualityController = None

try:
    from shared.database.connection import get_db_conn
    from shared.data_quality.quality_controller import QualityController

    logging.info("Successfully imported shared modules")
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")
    logging.warning(f"Current working directory: {os.getcwd()}")
    logging.warning(f"Project root: {project_root}")
    logging.warning(f"Python path: {sys.path[:3]}...")

    # Create fallback implementations
    def get_db_conn():
        logging.warning("Database connection not available - using fallback")
        return None

    class QualityController:
        def __init__(self):
            logging.warning("QualityController not available - using fallback")


@dataclass
class MLPredictionResult:
    """Result of ML prediction generation operation"""

    source_name: str
    success: bool
    predictions_generated: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class MLPredictionData:
    """Standardized ML prediction data structure"""

    venue_id: str
    prediction_type: str  # 'attendance', 'psychographic', 'popularity'
    prediction_value: float
    confidence_score: float
    prediction_horizon_hours: int
    model_version: str
    features_used: List[str]
    generated_at: datetime


class MLPredictor:
    """ML Predictor class for generating venue predictions."""

    def __init__(self):
        """Initialize the ML predictor."""
        self.logger = logging.getLogger(__name__)
        self.collector = MLPredictionCollector()

    def generate_venue_predictions(self) -> List[Dict]:
        """Generate predictions for venues."""
        try:
            result = self.collector.collect_data()

            if result.success:
                # Convert MLPredictionData to dict format expected by the app
                predictions = []
                # For now, return mock predictions since we don't have actual venue data
                mock_predictions = [
                    {
                        "venue_id": "venue_1",
                        "venue_name": "Mock Venue 1",
                        "prediction_type": "attendance",
                        "prediction_value": 0.75,
                        "confidence_score": 0.85,
                    },
                    {
                        "venue_id": "venue_2",
                        "venue_name": "Mock Venue 2",
                        "prediction_type": "popularity",
                        "prediction_value": 0.68,
                        "confidence_score": 0.72,
                    },
                ]
                return mock_predictions
            else:
                self.logger.warning("Prediction generation failed")
                return []

        except Exception as e:
            self.logger.error(f"Error generating venue predictions: {e}")
            return []


class MLPredictionCollector:
    """Standardized ML prediction collector."""

    def __init__(self):
        """Initialize the ML prediction collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> MLPredictionResult:
        """Generate ML predictions for venues."""
        start_time = datetime.now()
        self.logger.info("🤖 Starting ML prediction generation")

        try:
            predictions = self._generate_predictions(area_bounds)

            if predictions:
                validated_predictions = self._validate_predictions(predictions)
                stored_count = self._upsert_predictions_to_db(validated_predictions)

                duration = (datetime.now() - start_time).total_seconds()

                result = MLPredictionResult(
                    source_name="ml_predictions",
                    success=True,
                    predictions_generated=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.8,
                )

                self.logger.info(
                    f"✅ ML prediction generation completed: {stored_count} predictions in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = MLPredictionResult(
                    source_name="ml_predictions",
                    success=False,
                    predictions_generated=0,
                    duration_seconds=duration,
                    error_message="No predictions generated",
                )

                self.logger.warning("⚠️ No predictions generated")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = MLPredictionResult(
                source_name="ml_predictions",
                success=False,
                predictions_generated=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ ML prediction generation failed: {e}")
            return result

    def _generate_predictions(
        self, area_bounds: Optional[Dict]
    ) -> List[MLPredictionData]:
        """Generate ML predictions (mock implementation)."""
        mock_prediction = MLPredictionData(
            venue_id="mock_venue_id",
            prediction_type="attendance",
            prediction_value=0.75,
            confidence_score=0.85,
            prediction_horizon_hours=24,
            model_version="v1.0",
            features_used=["weather", "social_sentiment", "foot_traffic"],
            generated_at=datetime.now(),
        )
        return [mock_prediction]

    def _validate_predictions(
        self, predictions: List[MLPredictionData]
    ) -> List[MLPredictionData]:
        """Validate ML predictions."""
        return [p for p in predictions if 0 <= p.confidence_score <= 1]

    def _upsert_predictions_to_db(self, predictions: List[MLPredictionData]) -> int:
        """Store ML predictions in database."""
        if not predictions:
            return 0

        db_conn = get_db_conn()
        if not db_conn:
            return 0

        stored_count = 0

        try:
            with db_conn:
                for prediction in predictions:
                    # Convert features list to JSON string for storage
                    features_json = str(prediction.features_used)

                    query = """
                        INSERT INTO ml_predictions (
                            venue_id, prediction_type, prediction_value, confidence_score,
                            prediction_horizon_hours, model_version, features_used, generated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    params = (
                        prediction.venue_id,
                        prediction.prediction_type,
                        prediction.prediction_value,
                        prediction.confidence_score,
                        prediction.prediction_horizon_hours,
                        prediction.model_version,
                        features_json,
                        prediction.generated_at,
                    )

                    db_conn.execute_query(query, params)
                    stored_count += 1

        except Exception as e:
            self.logger.error(f"Error storing ML predictions: {e}")

        return stored_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    collector = MLPredictionCollector()
    result = collector.collect_data()
    status = "✅" if result.success else "❌"
    print(
        f"{status} ML predictions: {result.predictions_generated} predictions in {result.duration_seconds:.1f}s"
    )
