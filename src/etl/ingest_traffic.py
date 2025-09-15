# etl/ingest_traffic.py
import os
import logging
from datetime import datetime, timedelta
import googlemaps
from etl.utils import get_db_conn

# API Keys
TRAFFIC_API_KEY = os.getenv("TRAFFIC_API_KEY")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# Kansas City reference points
KC_DOWNTOWN = {"lat": 39.0997, "lng": -94.5786}  # Downtown Kansas City
KC_MAJOR_HIGHWAYS = [
    {"name": "I-35", "lat": 39.0997, "lng": -94.5786},
    {"name": "I-70", "lat": 39.0997, "lng": -94.5786},
    {"name": "I-435", "lat": 39.0458, "lng": -94.5786},
    {"name": "US-71", "lat": 39.0458, "lng": -94.5786},
]

# Key KC destinations for travel time analysis
KC_DESTINATIONS = [
    {"name": "Downtown KC", "lat": 39.0997, "lng": -94.5786},
    {"name": "Country Club Plaza", "lat": 39.0458, "lng": -94.5889},
    {"name": "Power & Light District", "lat": 39.1012, "lng": -94.5844},
    {"name": "Crossroads Arts District", "lat": 39.0858, "lng": -94.5844},
    {"name": "Westport", "lat": 39.0458, "lng": -94.5889},
    {"name": "KCI Airport", "lat": 39.2976, "lng": -94.7139},
]


def setup_google_maps_client():
    """Initialize Google Maps client"""
    api_key = TRAFFIC_API_KEY or GOOGLE_MAPS_API_KEY
    if not api_key:
        logging.error("No Google Maps API key available - cannot fetch traffic data")
        return None

    try:
        gmaps = googlemaps.Client(key=api_key)
        return gmaps
    except Exception as e:
        logging.error(f"Failed to setup Google Maps client: {e}")
        return None


def fetch_distance_matrix(origins, destinations, departure_time=None):
    """
    Fetch distance matrix with traffic data from Google Maps

    Args:
        origins (list): List of origin coordinates
        destinations (list): List of destination coordinates
        departure_time (datetime): Departure time for traffic calculation

    Returns:
        dict: Distance matrix response
    """
    gmaps = setup_google_maps_client()
    if not gmaps:
        return None

    if not departure_time:
        departure_time = datetime.now()

    try:
        # Convert coordinates to lat,lng strings
        origin_coords = [f"{origin['lat']},{origin['lng']}" for origin in origins]
        dest_coords = [f"{dest['lat']},{dest['lng']}" for dest in destinations]

        logging.info(
            f"Fetching distance matrix for {len(origin_coords)} origins to {len(dest_coords)} destinations"
        )

        result = gmaps.distance_matrix(
            origins=origin_coords,
            destinations=dest_coords,
            mode="driving",
            departure_time=departure_time,
            traffic_model="best_guess",
            units="metric",
        )

        return result

    except Exception as e:
        logging.error(f"Failed to fetch distance matrix: {e}")
        return None


def fetch_directions_with_traffic(origin, destination, departure_time=None):
    """
    Fetch detailed directions with traffic information

    Args:
        origin (dict): Origin coordinates
        destination (dict): Destination coordinates
        departure_time (datetime): Departure time for traffic calculation

    Returns:
        dict: Directions response with traffic data
    """
    gmaps = setup_google_maps_client()
    if not gmaps:
        return None

    if not departure_time:
        departure_time = datetime.now()

    try:
        origin_str = f"{origin['lat']},{origin['lng']}"
        dest_str = f"{destination['lat']},{destination['lng']}"

        logging.info(f"Fetching directions from {origin_str} to {dest_str}")

        result = gmaps.directions(
            origin=origin_str,
            destination=dest_str,
            mode="driving",
            departure_time=departure_time,
            traffic_model="best_guess",
            alternatives=True,
        )

        return result

    except Exception as e:
        logging.error(f"Failed to fetch directions: {e}")
        return None


def calculate_congestion_score(duration_in_traffic, duration_normal):
    """
    Calculate congestion score based on traffic vs normal duration

    Args:
        duration_in_traffic (int): Duration with traffic in seconds
        duration_normal (int): Normal duration without traffic in seconds

    Returns:
        float: Congestion score (0-1, where 1 is heavily congested)
    """
    if not duration_normal or duration_normal == 0:
        return 0.0

    # Calculate traffic index (ratio of traffic time to normal time)
    traffic_index = duration_in_traffic / duration_normal

    # Convert to 0-1 congestion score
    # 1.0 = no congestion, 2.0+ = heavy congestion
    if traffic_index <= 1.0:
        return 0.0
    elif traffic_index >= 2.0:
        return 1.0
    else:
        return traffic_index - 1.0  # Linear scale from 0 to 1


def process_venue_traffic_data(venue_id, venue_lat, venue_lng):
    """
    Process traffic data for a specific venue

    Args:
        venue_id (str): Venue UUID
        venue_lat (float): Venue latitude
        venue_lng (float): Venue longitude

    Returns:
        list: Traffic data records for the venue
    """
    venue_location = {"lat": venue_lat, "lng": venue_lng}
    traffic_records = []

    # Calculate travel times to major KC destinations
    for destination in KC_DESTINATIONS:
        directions = fetch_directions_with_traffic(venue_location, destination)

        if directions and len(directions) > 0:
            route = directions[0]  # Use the first (recommended) route
            leg = route["legs"][0]

            # Extract traffic data
            duration_in_traffic = leg.get("duration_in_traffic", {}).get("value", 0)
            duration_normal = leg.get("duration", {}).get("value", 0)
            distance = leg.get("distance", {}).get("value", 0)

            if duration_in_traffic and duration_normal:
                congestion_score = calculate_congestion_score(
                    duration_in_traffic, duration_normal
                )
                travel_time_index = (
                    duration_in_traffic / duration_normal
                    if duration_normal > 0
                    else 1.0
                )

                # Special handling for downtown travel time
                travel_time_to_downtown = None
                if destination["name"] == "Downtown KC":
                    travel_time_to_downtown = (
                        duration_in_traffic / 60.0
                    )  # Convert to minutes

                traffic_record = {
                    "venue_id": venue_id,
                    "ts": datetime.now(),
                    "destination": destination["name"],
                    "congestion_score": congestion_score,
                    "travel_time_to_downtown": travel_time_to_downtown,
                    "travel_time_index": travel_time_index,
                    "duration_in_traffic_seconds": duration_in_traffic,
                    "duration_normal_seconds": duration_normal,
                    "distance_meters": distance,
                    "source": "google_maps",
                }

                traffic_records.append(traffic_record)

                logging.info(
                    f"Processed traffic data for venue {venue_id} to {destination['name']}: "
                    f"congestion={congestion_score:.2f}, travel_time_index={travel_time_index:.2f}"
                )

    return traffic_records


def fetch_real_time_traffic_conditions():
    """
    Fetch real-time traffic conditions for major KC highways

    Returns:
        list: Traffic condition data for major routes
    """
    gmaps = setup_google_maps_client()
    if not gmaps:
        return []

    traffic_conditions = []

    # Check traffic on major highways by sampling multiple points
    for highway in KC_MAJOR_HIGHWAYS:
        try:
            # Create a route along the highway (simplified approach)
            start_point = highway
            end_point = {
                "lat": highway["lat"] + 0.05,  # ~5km north
                "lng": highway["lng"],
            }

            directions = fetch_directions_with_traffic(start_point, end_point)

            if directions and len(directions) > 0:
                route = directions[0]
                leg = route["legs"][0]

                duration_in_traffic = leg.get("duration_in_traffic", {}).get("value", 0)
                duration_normal = leg.get("duration", {}).get("value", 0)
                distance = leg.get("distance", {}).get("value", 0)

                if duration_in_traffic and duration_normal:
                    congestion_score = calculate_congestion_score(
                        duration_in_traffic, duration_normal
                    )

                    condition = {
                        "highway": highway["name"],
                        "ts": datetime.now(),
                        "congestion_score": congestion_score,
                        "travel_time_index": duration_in_traffic / duration_normal,
                        "avg_speed_kmh": (
                            (distance / 1000) / (duration_in_traffic / 3600)
                            if duration_in_traffic > 0
                            else 0
                        ),
                        "source": "google_maps",
                    }

                    traffic_conditions.append(condition)

        except Exception as e:
            logging.error(
                f"Failed to fetch traffic conditions for {highway['name']}: {e}"
            )

    return traffic_conditions


def upsert_traffic_data_to_db(traffic_records):
    """
    Insert or update traffic data in the database

    Args:
        traffic_records (list): List of traffic data records
    """
    if not traffic_records:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        insert_query = """
        INSERT INTO traffic_data (
            venue_id, ts, congestion_score, travel_time_to_downtown,
            travel_time_index, source
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        """

        for record in traffic_records:
            # Only insert records with downtown travel time for the main table
            if record.get("travel_time_to_downtown") is not None:
                cur.execute(
                    insert_query,
                    (
                        record["venue_id"],
                        record["ts"],
                        record["congestion_score"],
                        record["travel_time_to_downtown"],
                        record["travel_time_index"],
                        record["source"],
                    ),
                )

        conn.commit()
        logging.info(f"Inserted {len(traffic_records)} traffic data records")

    except Exception as e:
        logging.error(f"Failed to insert traffic data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def ingest_venue_traffic_data():
    """
    Main function to ingest traffic data for all venues
    """
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Get all venues with coordinates
        cur.execute(
            """
            SELECT venue_id, name, lat, lng 
            FROM venues 
            WHERE lat IS NOT NULL AND lng IS NOT NULL
            LIMIT 50
        """
        )  # Limit to avoid API quota issues

        venues = cur.fetchall()
        logging.info(f"Processing traffic data for {len(venues)} venues")

        all_traffic_records = []

        for venue_id, venue_name, venue_lat, venue_lng in venues:
            logging.info(f"Processing traffic data for venue: {venue_name}")

            try:
                traffic_records = process_venue_traffic_data(
                    venue_id, venue_lat, venue_lng
                )
                all_traffic_records.extend(traffic_records)

                # Add small delay to respect API rate limits
                import time

                time.sleep(0.1)

            except Exception as e:
                logging.error(
                    f"Failed to process traffic data for venue {venue_name}: {e}"
                )

        # Insert all traffic records
        if all_traffic_records:
            upsert_traffic_data_to_db(all_traffic_records)

    except Exception as e:
        logging.error(f"Failed to process venues for traffic data: {e}")
    finally:
        cur.close()
        conn.close()


def ingest_highway_traffic_conditions():
    """
    Ingest real-time traffic conditions for major KC highways
    """
    logging.info("Processing highway traffic conditions")

    try:
        traffic_conditions = fetch_real_time_traffic_conditions()

        if traffic_conditions:
            # Store highway conditions in a separate way or log them
            for condition in traffic_conditions:
                logging.info(
                    f"Highway {condition['highway']}: "
                    f"congestion={condition['congestion_score']:.2f}, "
                    f"avg_speed={condition['avg_speed_kmh']:.1f} km/h"
                )

        logging.info(f"Processed {len(traffic_conditions)} highway traffic conditions")

    except Exception as e:
        logging.error(f"Failed to process highway traffic conditions: {e}")


def calculate_venue_accessibility_scores():
    """
    Calculate accessibility scores for venues based on traffic data
    """
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Get recent traffic data for venues
        cur.execute(
            """
            SELECT venue_id, AVG(congestion_score) as avg_congestion,
                   AVG(travel_time_to_downtown) as avg_travel_time,
                   AVG(travel_time_index) as avg_travel_index
            FROM traffic_data 
            WHERE ts >= %s
            GROUP BY venue_id
        """,
            (datetime.now() - timedelta(hours=24),),
        )

        traffic_stats = cur.fetchall()

        for (
            venue_id,
            avg_congestion,
            avg_travel_time,
            avg_travel_index,
        ) in traffic_stats:
            # Calculate accessibility score (0-1, where 1 is most accessible)
            # Lower congestion and travel time = higher accessibility
            congestion_factor = 1.0 - min(avg_congestion or 0, 1.0)
            travel_time_factor = max(
                0, 1.0 - (avg_travel_time or 30) / 60.0
            )  # Normalize by 60 minutes

            accessibility_score = (congestion_factor + travel_time_factor) / 2.0

            logging.info(
                f"Venue {venue_id}: accessibility_score={accessibility_score:.2f}"
            )

    except Exception as e:
        logging.error(f"Failed to calculate accessibility scores: {e}")
    finally:
        cur.close()
        conn.close()


def ingest_traffic_data():
    """
    Main function to ingest all traffic data
    """
    logging.info("Starting traffic data ingestion")

    try:
        # Ingest venue-specific traffic data
        ingest_venue_traffic_data()

        # Ingest highway traffic conditions
        ingest_highway_traffic_conditions()

        # Calculate accessibility scores
        calculate_venue_accessibility_scores()

        logging.info("Traffic data ingestion completed successfully")

    except Exception as e:
        logging.error(f"Traffic data ingestion failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_traffic_data()
