# etl/ingest_events.py
import os
from etl.utils import safe_request, get_db_conn

PREDICT_HQ_KEY = os.getenv("PREDICT_HQ_API_KEY")


def fetch_predicthq_events(
    place_name="Kansas City", start="2025-01-01", end="2026-01-01"
):
    url = "https://api.predicthq.com/v1/events/"
    headers = {"Authorization": f"Bearer {PREDICT_HQ_KEY}"}
    params = {"q": place_name, "active.gte": start, "active.lte": end, "limit": 100}
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
