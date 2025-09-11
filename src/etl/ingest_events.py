# etl/ingest_events.py
import os
import logging
from datetime import datetime, timedelta
from etl.utils import safe_request, get_db_conn

PREDICT_HQ_KEY = os.getenv("PREDICT_HQ_API_KEY")


def fetch_predicthq_events(
    lat=39.0997, lng=-94.5786, radius="50km", start_date=None, end_date=None
):
    """
    Fetch events from PredictHQ API for Kansas City area

    Args:
        lat (float): Latitude for Kansas City
        lng (float): Longitude for Kansas City
        radius (str): Search radius (e.g., "50km")
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        dict: PredictHQ API response
    """
    if not PREDICT_HQ_KEY:
        logging.error("PREDICT_HQ_API_KEY not set - cannot fetch events data")
        raise ValueError("PREDICT_HQ_API_KEY is required for events data")

    # Default to next 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%d")
    if not end_date:
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

    url = "https://api.predicthq.com/v1/events/"
    headers = {"Authorization": f"Bearer {PREDICT_HQ_KEY}"}

    # Use location-based search instead of text search
    params = {
        "within": f"{radius}@{lat},{lng}",  # Geographic search
        "active.gte": start_date,
        "active.lte": end_date,
        "limit": 100,
        "sort": "rank",  # Sort by impact/rank
        "category": "concerts,festivals,sports,conferences,expos,community,performing-arts",  # Relevant categories
    }

    logging.info(
        f"Fetching PredictHQ events for Kansas City area ({lat}, {lng}) from {start_date} to {end_date}"
    )
    return safe_request(url, headers=headers, params=params)


def upsert_events_to_db(events_json):
    conn = get_db_conn()
    cur = conn.cursor()
    rows = []
    for e in events_json.get("results", []):
        # map fields according to PredictHQ spec
        rows.append(
            (
                e.get("id"),
                "predicthq",
                e.get("title"),
                e.get("description"),
                e.get("category"),
                e.get("start"),
                e.get("end"),
                None,  # venue cross-reference later (geocoding)
                e.get("rank"),
                None,
            )
        )
    # inserts...
    cur.close()
    conn.commit()
    conn.close()
