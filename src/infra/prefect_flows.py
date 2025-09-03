# infra/prefect_flows.py
from prefect import flow, task
from etl.ingest_places import fetch_nearby_places, upsert_places_to_db
from etl.ingest_events import fetch_predicthq_events, upsert_events_to_db
from features.build_features import build_features_for_time_window
from features.labeling import generate_bootstrap_labels
from src.backend.models.train import train_and_eval
from datetime import datetime, timedelta


@task
def ingest_places_task():
    # Example: iterate a grid of points across KC or use bounding box
    results = fetch_nearby_places(39.0997, -94.5786, radius=30000)
    upsert_places_to_db(results.get("results", []))


@task
def ingest_events_task():
    events = fetch_predicthq_events("Kansas City", start="2025-01-01", end="2026-01-01")
    upsert_events_to_db(events)


@task
def build_features_task():
    end_ts = datetime.utcnow()
    start_ts = end_ts - timedelta(days=7)
    build_features_for_time_window(start_ts, end_ts)


@task
def label_task():
    generate_bootstrap_labels(threshold_percentile=80)


@task
def train_task():
    train_and_eval()


@flow
def daily_flow():
    ingest_places_task()
    ingest_events_task()
    build_features_task()
    label_task()
    train_task()


if __name__ == "__main__":
    daily_flow()
