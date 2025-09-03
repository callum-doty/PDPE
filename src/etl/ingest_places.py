# etl/ingest_places.py
import os
from etl.utils import safe_request, get_db_conn, logging

API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")


def fetch_nearby_places(lat, lng, radius=5000, pagetoken=None):
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "key": API_KEY,
        "location": f"{lat},{lng}",
        "radius": radius,
        # use types list or keyword
    }
    if pagetoken:
        params["pagetoken"] = pagetoken
    return safe_request(url, params=params)


def upsert_places_to_db(place_results):
    conn = get_db_conn()
    cur = conn.cursor()
    rows = []
    for p in place_results:
        rows.append(
            (
                p.get("place_id"),
                "google_places",
                p.get("name"),
                ",".join(p.get("types", [])),
                p.get("price_level"),
                p.get("rating"),
                p.get("user_ratings_total"),
                p["geometry"]["location"]["lat"],
                p["geometry"]["location"]["lng"],
            )
        )
    sql = """
    INSERT INTO venues (external_id, provider, name, category, price_tier, avg_rating, review_count, lat, lng, geo)
    VALUES %s
    ON CONFLICT (external_id) DO UPDATE SET
        name = EXCLUDED.name,
        category = EXCLUDED.category,
        price_tier = EXCLUDED.price_tier,
        avg_rating = EXCLUDED.avg_rating,
        review_count = EXCLUDED.review_count,
        lat = EXCLUDED.lat,
        lng = EXCLUDED.lng,
        geo = ST_SetSRID(ST_MakePoint(EXCLUDED.lng, EXCLUDED.lat), 4326)
    """
    # Convert rows to include geo in db statement - use execute_values with placeholder adaptation
    execute_values(cur, sql, rows, template=None)
    conn.commit()
    cur.close()
    conn.close()
