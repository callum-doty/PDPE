# etl/ingest_events.py
import os
import logging
from datetime import datetime, timedelta
from etl.utils import safe_request, get_db_conn

EVENTBRITE_API_KEY = os.getenv("EVENTBRITE_API_KEY")
PREDICT_HQ_KEY = os.getenv("PREDICT_HQ_API_KEY")
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")


def fetch_eventbrite_events(
    lat=39.0997, lng=-94.5786, radius="50km", start_date=None, end_date=None
):
    """
    Fetch events from Eventbrite API for Kansas City area

    Args:
        lat (float): Latitude for Kansas City
        lng (float): Longitude for Kansas City
        radius (str): Search radius (e.g., "50km")
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        dict: Eventbrite API response
    """
    if not EVENTBRITE_API_KEY:
        logging.error("EVENTBRITE_API_KEY not set - cannot fetch events data")
        raise ValueError("EVENTBRITE_API_KEY is required for events data")

    # Default to next 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    if not end_date:
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")

    url = "https://www.eventbriteapi.com/v3/events/search/"
    headers = {"Authorization": f"Bearer {EVENTBRITE_API_KEY}"}

    # Convert radius from "50km" to meters for Eventbrite API
    radius_km = int(radius.replace("km", ""))
    radius_meters = radius_km * 1000

    params = {
        "location.latitude": lat,
        "location.longitude": lng,
        "location.within": f"{radius_meters}m",
        "start_date.range_start": start_date,
        "start_date.range_end": end_date,
        "expand": "venue,category",
        "sort_by": "date",
    }

    logging.info(
        f"Fetching Eventbrite events for Kansas City area ({lat}, {lng}) from {start_date} to {end_date}"
    )
    return safe_request(url, headers=headers, params=params)


def fetch_ticketmaster_events(
    lat=39.0997, lng=-94.5786, radius="50", start_date=None, end_date=None
):
    """
    Fetch events from Ticketmaster API for Kansas City area

    Args:
        lat (float): Latitude for Kansas City
        lng (float): Longitude for Kansas City
        radius (str): Search radius in miles (e.g., "50")
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        dict: Ticketmaster API response
    """
    if not TICKETMASTER_API_KEY:
        logging.error("TICKETMASTER_API_KEY not set - cannot fetch events data")
        raise ValueError("TICKETMASTER_API_KEY is required for events data")

    # Default to next 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    if not end_date:
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

    url = "https://app.ticketmaster.com/discovery/v2/events.json"

    params = {
        "apikey": TICKETMASTER_API_KEY,
        "latlong": f"{lat},{lng}",
        "radius": radius,
        "unit": "miles",
        "startDateTime": start_date,
        "endDateTime": end_date,
        "size": 100,
        "sort": "date,asc",
    }

    logging.info(
        f"Fetching Ticketmaster events for Kansas City area ({lat}, {lng}) from {start_date} to {end_date}"
    )
    return safe_request(url, params=params)


def fetch_predicthq_events(
    lat=39.0997, lng=-94.5786, radius="50km", start_date=None, end_date=None
):
    """
    Fetch events from PredictHQ API for Kansas City area (requires paid subscription)

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


def upsert_eventbrite_events_to_db(events_json):
    """
    Insert or update Eventbrite events data in the database

    Args:
        events_json (dict): Eventbrite API response with events data
    """
    if not events_json or not events_json.get("events"):
        logging.info("No events data to insert")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        inserted_count = 0
        for e in events_json.get("events", []):
            # Parse dates from Eventbrite format
            start_time = None
            end_time = None
            if e.get("start") and e.get("start", {}).get("utc"):
                try:
                    start_time = datetime.fromisoformat(
                        e.get("start", {}).get("utc").replace("Z", "+00:00")
                    )
                except:
                    start_time = None
            if e.get("end") and e.get("end", {}).get("utc"):
                try:
                    end_time = datetime.fromisoformat(
                        e.get("end", {}).get("utc").replace("Z", "+00:00")
                    )
                except:
                    end_time = None

            # Extract category and tags for psychographic matching
            tags = []
            category_name = None
            if e.get("category") and e.get("category", {}).get("name"):
                category_name = e.get("category", {}).get("name")
                tags.append(category_name)

            # Get description text
            description = None
            if e.get("description") and e.get("description", {}).get("text"):
                description = e.get("description", {}).get("text")

            # Get event name
            event_name = None
            if e.get("name") and e.get("name", {}).get("text"):
                event_name = e.get("name", {}).get("text")

            cur.execute(
                """
                INSERT INTO events (
                    external_id, provider, name, description, category, 
                    tags, start_time, end_time, venue_id, predicted_attendance
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (external_id, provider) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    predicted_attendance = EXCLUDED.predicted_attendance,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    e.get("id"),
                    "eventbrite",
                    event_name,
                    description,
                    category_name,
                    tags,
                    start_time,
                    end_time,
                    None,  # venue cross-reference later (geocoding)
                    e.get(
                        "capacity", 100
                    ),  # Using capacity as predicted attendance proxy
                ),
            )
            inserted_count += 1

        conn.commit()
        logging.info(f"Successfully upserted {inserted_count} events")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting events data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upsert_ticketmaster_events_to_db(events_json):
    """
    Insert or update Ticketmaster events data in the database

    Args:
        events_json (dict): Ticketmaster API response with events data
    """
    if not events_json or not events_json.get("_embedded", {}).get("events"):
        logging.info("No events data to insert")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        inserted_count = 0
        for e in events_json.get("_embedded", {}).get("events", []):
            # Parse dates from Ticketmaster format
            start_time = None
            end_time = None
            if e.get("dates", {}).get("start", {}).get("dateTime"):
                try:
                    start_time = datetime.fromisoformat(
                        e.get("dates", {})
                        .get("start", {})
                        .get("dateTime")
                        .replace("Z", "+00:00")
                    )
                except:
                    start_time = None

            # Ticketmaster doesn't always provide end time, estimate 3 hours
            if start_time:
                end_time = start_time + timedelta(hours=3)

            # Extract category and tags for psychographic matching
            tags = []
            category_name = None
            if e.get("classifications"):
                for classification in e.get("classifications", []):
                    if classification.get("segment", {}).get("name"):
                        category_name = classification.get("segment", {}).get("name")
                        tags.append(category_name)
                    if classification.get("genre", {}).get("name"):
                        tags.append(classification.get("genre", {}).get("name"))

            # Get event name
            event_name = e.get("name")

            # Get description (Ticketmaster uses info or pleaseNote)
            description = e.get("info") or e.get("pleaseNote")

            cur.execute(
                """
                INSERT INTO events (
                    external_id, provider, name, description, category, 
                    tags, start_time, end_time, venue_id, predicted_attendance
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (external_id, provider) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    predicted_attendance = EXCLUDED.predicted_attendance,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    e.get("id"),
                    "ticketmaster",
                    event_name,
                    description,
                    category_name,
                    tags,
                    start_time,
                    end_time,
                    None,  # venue cross-reference later (geocoding)
                    200,  # Default estimated attendance for Ticketmaster events
                ),
            )
            inserted_count += 1

        conn.commit()
        logging.info(f"Successfully upserted {inserted_count} events")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting events data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def upsert_events_to_db(events_json):
    """
    Insert or update events data in the database (PredictHQ format)

    Args:
        events_json (dict): PredictHQ API response with events data
    """
    if not events_json or not events_json.get("results"):
        logging.info("No events data to insert")
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        inserted_count = 0
        for e in events_json.get("results", []):
            # Parse dates
            start_time = None
            end_time = None
            if e.get("start"):
                try:
                    start_time = datetime.fromisoformat(
                        e.get("start").replace("Z", "+00:00")
                    )
                except:
                    start_time = None
            if e.get("end"):
                try:
                    end_time = datetime.fromisoformat(
                        e.get("end").replace("Z", "+00:00")
                    )
                except:
                    end_time = None

            # Extract tags/labels for psychographic matching
            tags = []
            if e.get("labels"):
                tags = [label for label in e.get("labels", [])]
            if e.get("category"):
                tags.append(e.get("category"))

            cur.execute(
                """
                INSERT INTO events (
                    external_id, provider, name, description, category, 
                    tags, start_time, end_time, venue_id, predicted_attendance
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (external_id, provider) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    tags = EXCLUDED.tags,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    predicted_attendance = EXCLUDED.predicted_attendance,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    e.get("id"),
                    "predicthq",
                    e.get("title"),
                    e.get("description"),
                    e.get("category"),
                    tags,
                    start_time,
                    end_time,
                    None,  # venue cross-reference later (geocoding)
                    e.get("rank"),  # Using rank as predicted attendance proxy
                ),
            )
            inserted_count += 1

        conn.commit()
        logging.info(f"Successfully upserted {inserted_count} events")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting events data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def main():
    """Main execution function for testing"""
    # Try Ticketmaster first (reliable free API)
    if TICKETMASTER_API_KEY:
        try:
            print("Fetching events from Ticketmaster...")
            events_data = fetch_ticketmaster_events()

            if events_data and events_data.get("_embedded", {}).get("events"):
                print(
                    f"Successfully fetched {len(events_data['_embedded']['events'])} events from Ticketmaster"
                )

                # Save to database
                upsert_ticketmaster_events_to_db(events_data)
                print("Ticketmaster events data saved to database")
                return
            else:
                print("No events data fetched from Ticketmaster")

        except Exception as e:
            print(f"Error fetching Ticketmaster events: {e}")

    # Try Eventbrite as backup (free API)
    if EVENTBRITE_API_KEY:
        try:
            print("Fetching events from Eventbrite...")
            events_data = fetch_eventbrite_events()

            if events_data and events_data.get("events"):
                print(
                    f"Successfully fetched {len(events_data['events'])} events from Eventbrite"
                )

                # Save to database
                upsert_eventbrite_events_to_db(events_data)
                print("Eventbrite events data saved to database")
                return
            else:
                print("No events data fetched from Eventbrite")

        except Exception as e:
            print(f"Error fetching Eventbrite events: {e}")

    # Fallback to PredictHQ if available (requires paid subscription)
    if PREDICT_HQ_KEY:
        try:
            print("Fetching events from PredictHQ...")
            events_data = fetch_predicthq_events()

            if events_data and events_data.get("results"):
                print(
                    f"Successfully fetched {len(events_data['results'])} events from PredictHQ"
                )

                # Save to database
                upsert_events_to_db(events_data)
                print("PredictHQ events data saved to database")
            else:
                print("No events data fetched from PredictHQ")

        except Exception as e:
            print(f"Error fetching PredictHQ events: {e}")
    else:
        print("No event API keys available")


if __name__ == "__main__":
    main()
