# features/labeling.py
import pandas as pd
import numpy as np
from datetime import timedelta
from etl.utils import get_db_conn


def generate_bootstrap_labels(threshold_percentile=80):
    """
    Simple bootstrapped label:
      label = 1 if visitors_count >= percentile(threshold_percentile) for that venue or globally
    """
    conn = get_db_conn()
    df = pd.read_sql("SELECT venue_id, ts, visitors_count FROM venue_traffic", conn)
    cutoff = np.percentile(df["visitors_count"].dropna(), threshold_percentile)
    df["label"] = (df["visitors_count"] >= cutoff).astype(int)
    # upsert into features.label for matching venue_id/ts
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute(
            """
            UPDATE features SET label = %s
            WHERE venue_id = %s AND ts = %s
        """,
            (int(r["label"]), r["venue_id"], r["ts"]),
        )
    conn.commit()
    cur.close()
    conn.close()
