# etl/enrich_geo.py
from etl.utils import get_db_conn
import psycopg2.extras


def associate_event_with_nearest_venue(event_id, lat, lng, search_radius_m=200):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT venue_id
        FROM venues
        WHERE ST_DWithin(geo, ST_SetSRID(ST_MakePoint(%s, %s),4326)::geography, %s)
        ORDER BY ST_Distance(geo, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)
        LIMIT 1
    """,
        (lng, lat, search_radius_m, lng, lat),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None
