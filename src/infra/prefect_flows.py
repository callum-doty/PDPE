# infra/prefect_flows.py
from prefect import flow, task
from etl.ingest_places import fetch_nearby_places, upsert_places_to_db
from etl.ingest_events import fetch_predicthq_events, upsert_events_to_db
from etl.ingest_social import (
    ingest_social_data_for_venues,
    ingest_general_kc_social_data,
)
from etl.ingest_econ import ingest_economic_indicators
from etl.ingest_traffic import ingest_traffic_data
from etl.ingest_local_venues import ingest_local_venue_data
from etl.ingest_weather import ingest_weather_data
from etl.ingest_foot_traffic import ingest_foot_traffic_data
from features.build_features import build_features_for_time_window
from features.labeling import generate_bootstrap_labels
from src.backend.models.train import train_and_eval
from datetime import datetime, timedelta
import logging


@task
def ingest_places_task():
    """Ingest places data from Google Places API"""
    try:
        # Example: iterate a grid of points across KC or use bounding box
        results = fetch_nearby_places(39.0997, -94.5786, radius=30000)
        upsert_places_to_db(results.get("results", []))
        logging.info("Places ingestion completed successfully")
    except Exception as e:
        logging.error(f"Places ingestion failed: {e}")
        raise


@task
def ingest_events_task():
    """Ingest events data from PredictHQ"""
    try:
        events = fetch_predicthq_events(
            "Kansas City", start="2025-01-01", end="2026-01-01"
        )
        upsert_events_to_db(events)
        logging.info("Events ingestion completed successfully")
    except Exception as e:
        logging.error(f"Events ingestion failed: {e}")
        raise


@task
def ingest_social_data_task():
    """Ingest social media data from Twitter and Facebook"""
    try:
        # Ingest venue-specific social data
        ingest_social_data_for_venues()

        # Ingest general KC social data
        ingest_general_kc_social_data()

        logging.info("Social data ingestion completed successfully")
    except Exception as e:
        logging.error(f"Social data ingestion failed: {e}")
        raise


@task
def ingest_economic_data_task():
    """Ingest economic indicators data"""
    try:
        ingest_economic_indicators()
        logging.info("Economic data ingestion completed successfully")
    except Exception as e:
        logging.error(f"Economic data ingestion failed: {e}")
        raise


@task
def ingest_traffic_data_task():
    """Ingest traffic data from Google Maps API"""
    try:
        ingest_traffic_data()
        logging.info("Traffic data ingestion completed successfully")
    except Exception as e:
        logging.error(f"Traffic data ingestion failed: {e}")
        raise


@task
def ingest_local_venues_task():
    """Ingest local venue events from KC websites"""
    try:
        ingest_local_venue_data()
        logging.info("Local venues ingestion completed successfully")
    except Exception as e:
        logging.error(f"Local venues ingestion failed: {e}")
        raise


@task
def ingest_weather_data_task():
    """Ingest weather data"""
    try:
        ingest_weather_data()
        logging.info("Weather data ingestion completed successfully")
    except Exception as e:
        logging.error(f"Weather data ingestion failed: {e}")
        raise


@task
def ingest_foot_traffic_data_task():
    """Ingest foot traffic data"""
    try:
        ingest_foot_traffic_data()
        logging.info("Foot traffic data ingestion completed successfully")
    except Exception as e:
        logging.error(f"Foot traffic data ingestion failed: {e}")
        raise


@task
def build_features_task():
    """Build features for ML pipeline"""
    try:
        end_ts = datetime.utcnow()
        start_ts = end_ts - timedelta(days=7)
        build_features_for_time_window(start_ts, end_ts)
        logging.info("Feature building completed successfully")
    except Exception as e:
        logging.error(f"Feature building failed: {e}")
        raise


@task
def label_task():
    """Generate bootstrap labels for training"""
    try:
        generate_bootstrap_labels(threshold_percentile=80)
        logging.info("Label generation completed successfully")
    except Exception as e:
        logging.error(f"Label generation failed: {e}")
        raise


@task
def train_task():
    """Train and evaluate ML models"""
    try:
        train_and_eval()
        logging.info("Model training completed successfully")
    except Exception as e:
        logging.error(f"Model training failed: {e}")
        raise


@flow
def comprehensive_data_ingestion_flow():
    """
    Comprehensive data ingestion flow that includes all data sources
    """
    logging.info("Starting comprehensive data ingestion flow")

    # Core data ingestion tasks (can run in parallel)
    ingest_places_task()
    ingest_events_task()
    ingest_weather_data_task()
    ingest_foot_traffic_data_task()

    # Enhanced data ingestion tasks
    ingest_social_data_task()
    ingest_economic_data_task()
    ingest_traffic_data_task()
    ingest_local_venues_task()

    logging.info("Comprehensive data ingestion flow completed")


@flow
def daily_flow():
    """
    Daily flow that includes data ingestion, feature building, and model training
    """
    logging.info("Starting daily flow")

    # Run comprehensive data ingestion
    comprehensive_data_ingestion_flow()

    # Build features and train models
    build_features_task()
    label_task()
    train_task()

    logging.info("Daily flow completed")


@flow
def hourly_data_flow():
    """
    Hourly flow for time-sensitive data sources
    """
    logging.info("Starting hourly data flow")

    # Only ingest time-sensitive data sources
    ingest_social_data_task()
    ingest_traffic_data_task()
    ingest_weather_data_task()
    ingest_foot_traffic_data_task()

    logging.info("Hourly data flow completed")


@flow
def weekly_comprehensive_flow():
    """
    Weekly comprehensive flow that includes all data sources and full retraining
    """
    logging.info("Starting weekly comprehensive flow")

    # Full data ingestion
    comprehensive_data_ingestion_flow()

    # Extended feature building (longer time window)
    end_ts = datetime.utcnow()
    start_ts = end_ts - timedelta(days=30)  # 30-day window for weekly flow

    @task
    def extended_build_features_task():
        try:
            build_features_for_time_window(start_ts, end_ts)
            logging.info("Extended feature building completed successfully")
        except Exception as e:
            logging.error(f"Extended feature building failed: {e}")
            raise

    extended_build_features_task()
    label_task()
    train_task()

    logging.info("Weekly comprehensive flow completed")


@flow
def social_and_economic_flow():
    """
    Specialized flow for social and economic data (can be run more frequently)
    """
    logging.info("Starting social and economic data flow")

    ingest_social_data_task()
    ingest_economic_data_task()

    logging.info("Social and economic data flow completed")


@flow
def local_events_flow():
    """
    Specialized flow for local venue events (can be run daily)
    """
    logging.info("Starting local events flow")

    ingest_local_venues_task()
    ingest_events_task()  # Also refresh PredictHQ events

    logging.info("Local events flow completed")


if __name__ == "__main__":
    # Run the daily flow by default
    daily_flow()
