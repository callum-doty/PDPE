"""
SQLAlchemy models and database operations for the whereabouts engine.
"""

import sqlite3
from typing import Optional, List, Tuple, Any, Dict
from pathlib import Path
from dataclasses import dataclass
from config import settings


@dataclass
class Location:
    name: str
    address: str
    latitude: float
    longitude: float
    category: str
    base_score: int = 0


@dataclass
class Event:
    source: str
    external_id: str
    name: str
    description: str
    start_time: str  # ISO datetime string
    end_time: str
    location: Location
    category: str
    tags: List[str]


def get_connection(db_path: Path = settings.DB_PATH) -> sqlite3.Connection:
    """Get a database connection with proper configuration."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(
    schema_path: Path = settings.SQL_SCHEMA_PATH, db_path: Path = settings.DB_PATH
):
    """Initialize the database with the schema."""
    if not schema_path.exists():
        raise FileNotFoundError(f"SQL schema not found: {schema_path}")
    conn = get_connection(db_path)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()
    print("Database initialized.")


# Generic helpers
def upsert_location(
    conn: sqlite3.Connection,
    name: str,
    address: str,
    lat: float,
    lon: float,
    category: str,
    base_score: int,
) -> int:
    """Insert or update a location and return its ID."""
    cur = conn.cursor()
    cur.execute(
        "SELECT location_id FROM locations WHERE name = ? AND address = ?",
        (name, address),
    )
    row = cur.fetchone()
    if row:
        return row["location_id"]
    cur.execute(
        """
        INSERT INTO locations (name, address, latitude, longitude, category, base_score)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (name, address, lat, lon, category, base_score),
    )
    conn.commit()
    return cur.lastrowid


def insert_event(
    conn: sqlite3.Connection,
    source: str,
    external_id: str,
    name: str,
    description: str,
    start_time: str,
    end_time: str,
    location_id: int,
    category: str,
    tags: str,
) -> int:
    """Insert an event and return its ID."""
    cur = conn.cursor()
    # dedupe by source+external_id
    cur.execute(
        "SELECT event_id FROM events WHERE source = ? AND external_id = ?",
        (source, external_id),
    )
    row = cur.fetchone()
    if row:
        return row["event_id"]
    cur.execute(
        """
        INSERT INTO events (source, external_id, name, description, start_time, end_time, location_id, category, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source,
            external_id,
            name,
            description,
            start_time,
            end_time,
            location_id,
            category,
            tags,
        ),
    )
    conn.commit()
    return cur.lastrowid


def insert_weather(
    conn: sqlite3.Connection,
    timestamp: str,
    location_id: Optional[int],
    condition: str,
    temperature: float,
    precipitation: float,
) -> int:
    """Insert weather data and return its ID."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO weather (timestamp, location_id, condition, temperature, precipitation)
        VALUES (?, ?, ?, ?, ?)
        """,
        (timestamp, location_id, condition, temperature, precipitation),
    )
    conn.commit()
    return cur.lastrowid


def insert_score(
    conn: sqlite3.Connection,
    event_id: int,
    location_id: int,
    demographic_score: int,
    event_score: int,
    weather_score: int,
    total_score: int,
) -> int:
    """Insert event score and return its ID."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO scores (event_id, location_id, demographic_score, event_score, weather_score, total_score)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            location_id,
            demographic_score,
            event_score,
            weather_score,
            total_score,
        ),
    )
    conn.commit()
    return cur.lastrowid


def query_top_scores(conn: sqlite3.Connection, limit: int = 5):
    """Query the top scoring events."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.total_score, s.timestamp, e.name as event_name, e.start_time, l.name as location_name,
               l.latitude, l.longitude
        FROM scores s
        JOIN events e ON s.event_id = e.event_id
        JOIN locations l ON s.location_id = l.location_id
        ORDER BY s.total_score DESC, e.start_time ASC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(r) for r in cur.fetchall()]


def query_events_with_scores(
    conn: sqlite3.Connection, min_score: int = 0
) -> List[Dict]:
    """Query all events with their scores above a minimum threshold."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 
            e.name as event_name,
            e.description,
            e.start_time,
            e.end_time,
            e.category,
            e.tags,
            l.name as location_name,
            l.address,
            l.latitude,
            l.longitude,
            l.category as venue_category,
            s.demographic_score,
            s.event_score,
            s.weather_score,
            s.total_score
        FROM events e
        JOIN locations l ON e.location_id = l.location_id
        JOIN scores s ON e.event_id = s.event_id
        WHERE s.total_score >= ?
        ORDER BY s.total_score DESC
        """,
        (min_score,),
    )
    return [dict(r) for r in cur.fetchall()]


def query_locations_by_category(conn: sqlite3.Connection, category: str) -> List[Dict]:
    """Query all locations of a specific category."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT location_id, name, address, latitude, longitude, category, base_score
        FROM locations
        WHERE category = ?
        ORDER BY base_score DESC
        """,
        (category,),
    )
    return [dict(r) for r in cur.fetchall()]


def get_database_stats(conn: sqlite3.Connection) -> Dict:
    """Get basic statistics about the database contents."""
    cur = conn.cursor()

    # Count events
    cur.execute("SELECT COUNT(*) as count FROM events")
    event_count = cur.fetchone()["count"]

    # Count locations
    cur.execute("SELECT COUNT(*) as count FROM locations")
    location_count = cur.fetchone()["count"]

    # Count scores
    cur.execute("SELECT COUNT(*) as count FROM scores")
    score_count = cur.fetchone()["count"]

    # Average score
    cur.execute("SELECT AVG(total_score) as avg_score FROM scores")
    avg_score_result = cur.fetchone()
    avg_score = avg_score_result["avg_score"] if avg_score_result["avg_score"] else 0

    # Top categories
    cur.execute(
        """
        SELECT l.category, COUNT(*) as count 
        FROM locations l 
        JOIN events e ON l.location_id = e.location_id 
        GROUP BY l.category 
        ORDER BY count DESC 
        LIMIT 5
    """
    )
    top_categories = [dict(r) for r in cur.fetchall()]

    return {
        "total_events": event_count,
        "total_locations": location_count,
        "total_scores": score_count,
        "average_score": round(avg_score, 2),
        "top_venue_categories": top_categories,
    }
