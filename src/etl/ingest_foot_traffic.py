# etl/ingest_foot_traffic.py
import os
import random
import numpy as np
from datetime import datetime, timedelta
from etl.utils import safe_request, get_db_conn, logging

FOOT_KEY = os.getenv("FOOT_TRAFFIC_API_KEY")

# Mock foot traffic data generator for development/testing
# In production, replace with real foot traffic API provider like SafeGraph, Veraset, etc.


def generate_realistic_foot_traffic(venue_external_id, venue_type=None, hours_back=24):
    """
    Generate realistic foot traffic data for testing purposes

    Args:
        venue_external_id (str): Venue identifier
        venue_type (str): Type of venue (restaurant, retail, etc.)
        hours_back (int): Number of hours of historical data to generate

    Returns:
        dict: Mock foot traffic data
    """
    current_time = datetime.utcnow()
    traffic_data = []

    # Base traffic patterns by venue type
    venue_patterns = {
        "restaurant": {
            "base": 50,
            "peak_hours": [12, 13, 18, 19, 20],
            "multiplier": 2.5,
        },
        "retail": {"base": 30, "peak_hours": [14, 15, 16, 17, 18], "multiplier": 2.0},
        "bar": {"base": 20, "peak_hours": [20, 21, 22, 23], "multiplier": 3.0},
        "coffee": {"base": 25, "peak_hours": [7, 8, 9, 14, 15], "multiplier": 2.2},
        "gym": {"base": 15, "peak_hours": [6, 7, 17, 18, 19], "multiplier": 2.8},
        "default": {"base": 25, "peak_hours": [12, 17, 18], "multiplier": 2.0},
    }

    pattern = venue_patterns.get(venue_type, venue_patterns["default"])

    for i in range(hours_back):
        timestamp = current_time - timedelta(hours=i)
        hour = timestamp.hour
        day_of_week = timestamp.weekday()  # 0=Monday, 6=Sunday

        # Base visitor count
        base_visitors = pattern["base"]

        # Hour-based multiplier
        hour_multiplier = 1.0
        if hour in pattern["peak_hours"]:
            hour_multiplier = pattern["multiplier"]
        elif 6 <= hour <= 22:  # Normal business hours
            hour_multiplier = 1.2
        else:  # Late night/early morning
            hour_multiplier = 0.3

        # Day of week multiplier
        if day_of_week < 5:  # Weekday
            day_multiplier = 1.0
        elif day_of_week == 5:  # Saturday
            day_multiplier = 1.4
        else:  # Sunday
            day_multiplier = 1.2

        # Calculate visitors with some randomness
        visitors = int(
            base_visitors * hour_multiplier * day_multiplier * random.uniform(0.7, 1.3)
        )
        visitors = max(0, visitors)  # Ensure non-negative

        # Dwell time varies by venue type and time
        base_dwell = {
            "restaurant": 3600,  # 1 hour
            "retail": 1800,  # 30 minutes
            "bar": 7200,  # 2 hours
            "coffee": 1200,  # 20 minutes
            "gym": 4800,  # 80 minutes
            "default": 2400,  # 40 minutes
        }

        dwell_seconds = base_dwell.get(venue_type, base_dwell["default"])
        dwell_seconds = int(dwell_seconds * random.uniform(0.6, 1.4))

        # Calculate changes from previous periods
        visitors_change_24h = random.uniform(-0.3, 0.3) if i < hours_back - 24 else 0
        visitors_change_7d = (
            random.uniform(-0.2, 0.4) if i < hours_back - (7 * 24) else 0
        )

        # Peak hour ratio (current hour visitors / daily average)
        daily_avg = base_visitors * 1.2  # Rough daily average
        peak_hour_ratio = visitors / max(daily_avg, 1)

        traffic_entry = {
            "timestamp": timestamp.isoformat(),
            "visitors_count": visitors,
            "median_dwell_seconds": dwell_seconds,
            "visitors_change_24h": visitors_change_24h,
            "visitors_change_7d": visitors_change_7d,
            "peak_hour_ratio": peak_hour_ratio,
            "confidence_score": random.uniform(0.7, 0.95),
        }

        traffic_data.append(traffic_entry)

    return {
        "venue_id": venue_external_id,
        "data_source": "mock_generator",
        "generated_at": current_time.isoformat(),
        "traffic_data": traffic_data,
        "metadata": {
            "venue_type": venue_type or "unknown",
            "data_points": len(traffic_data),
            "time_range_hours": hours_back,
        },
    }


def fetch_foot_traffic(venue_external_id, venue_type=None):
    """
    Fetch foot traffic data for a venue

    Args:
        venue_external_id (str): External venue identifier
        venue_type (str): Type of venue for realistic data generation

    Returns:
        dict: Foot traffic data
    """
    # TODO: Replace with real foot traffic API when available
    # For now, generate realistic mock data

    if not FOOT_KEY:
        logging.warning("FOOT_TRAFFIC_API_KEY not set, using mock data generator")
        return generate_realistic_foot_traffic(venue_external_id, venue_type)

    # If we had a real API, it would look like this:
    # url = f"https://api.realfoottrafficprovider.com/v1/visits/{venue_external_id}"
    # headers = {"Authorization": f"Bearer {FOOT_KEY}"}
    # return safe_request(url, headers=headers)

    # For now, return mock data even with API key
    logging.info(f"Generating mock foot traffic data for venue {venue_external_id}")
    return generate_realistic_foot_traffic(venue_external_id, venue_type)


def process_foot_traffic_data(raw_data):
    """
    Process raw foot traffic data into standardized format

    Args:
        raw_data (dict): Raw foot traffic response

    Returns:
        list: Processed traffic records
    """
    if not raw_data or "traffic_data" not in raw_data:
        return []

    processed_records = []
    venue_id = raw_data.get("venue_id")

    for entry in raw_data["traffic_data"]:
        processed_record = {
            "venue_external_id": venue_id,
            "ts": entry["timestamp"],
            "visitors_count": entry["visitors_count"],
            "median_dwell_seconds": entry["median_dwell_seconds"],
            "visitors_change_24h": entry.get("visitors_change_24h", 0),
            "visitors_change_7d": entry.get("visitors_change_7d", 0),
            "peak_hour_ratio": entry.get("peak_hour_ratio", 1.0),
            "source": raw_data.get("data_source", "unknown"),
            "confidence_score": entry.get("confidence_score", 0.8),
        }
        processed_records.append(processed_record)

    return processed_records


def upsert_venue_traffic(venue_id, ts, visitors_count, dwell, **kwargs):
    """
    Insert or update venue traffic data in database

    Args:
        venue_id (str): Venue ID
        ts (str/datetime): Timestamp
        visitors_count (int): Number of visitors
        dwell (int): Median dwell time in seconds
        **kwargs: Additional fields like visitors_change_24h, etc.
    """
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Convert datetime to string if needed
        if isinstance(ts, datetime):
            ts = ts.isoformat()

        cur.execute(
            """
            INSERT INTO venue_traffic (
                venue_id, ts, visitors_count, median_dwell_seconds, 
                visitors_change_24h, visitors_change_7d, peak_hour_ratio, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (venue_id, ts) DO UPDATE SET
                visitors_count = EXCLUDED.visitors_count,
                median_dwell_seconds = EXCLUDED.median_dwell_seconds,
                visitors_change_24h = EXCLUDED.visitors_change_24h,
                visitors_change_7d = EXCLUDED.visitors_change_7d,
                peak_hour_ratio = EXCLUDED.peak_hour_ratio,
                source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                venue_id,
                ts,
                visitors_count,
                dwell,
                kwargs.get("visitors_change_24h", 0),
                kwargs.get("visitors_change_7d", 0),
                kwargs.get("peak_hour_ratio", 1.0),
                kwargs.get("source", "foottraffic"),
            ),
        )

        conn.commit()
        logging.info(f"Successfully upserted traffic data for venue {venue_id} at {ts}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting venue traffic data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def bulk_upsert_venue_traffic(traffic_records):
    """
    Bulk insert/update multiple traffic records

    Args:
        traffic_records (list): List of traffic record dictionaries
    """
    if not traffic_records:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        for record in traffic_records:
            upsert_venue_traffic(
                venue_id=record["venue_external_id"],
                ts=record["ts"],
                visitors_count=record["visitors_count"],
                dwell=record["median_dwell_seconds"],
                visitors_change_24h=record.get("visitors_change_24h", 0),
                visitors_change_7d=record.get("visitors_change_7d", 0),
                peak_hour_ratio=record.get("peak_hour_ratio", 1.0),
                source=record.get("source", "foottraffic"),
            )

        logging.info(
            f"Successfully bulk upserted {len(traffic_records)} traffic records"
        )

    except Exception as e:
        logging.error(f"Error in bulk upsert: {e}")
        raise


def fetch_and_store_foot_traffic_for_venues(venue_list):
    """
    Fetch and store foot traffic data for multiple venues

    Args:
        venue_list (list): List of venue dictionaries with 'external_id' and 'category'

    Returns:
        dict: Summary of processing results
    """
    results = {"processed_venues": 0, "total_records": 0, "errors": []}

    for venue in venue_list:
        try:
            venue_id = venue.get("external_id")
            venue_type = venue.get("category", "").lower()

            # Map venue categories to our internal types
            category_mapping = {
                "restaurant": "restaurant",
                "food": "restaurant",
                "store": "retail",
                "shopping": "retail",
                "bar": "bar",
                "nightlife": "bar",
                "cafe": "coffee",
                "coffee": "coffee",
                "gym": "gym",
                "fitness": "gym",
            }

            mapped_type = None
            for key, value in category_mapping.items():
                if key in venue_type:
                    mapped_type = value
                    break

            # Fetch traffic data
            raw_data = fetch_foot_traffic(venue_id, mapped_type)

            if raw_data:
                # Process and store data
                processed_records = process_foot_traffic_data(raw_data)
                if processed_records:
                    bulk_upsert_venue_traffic(processed_records)
                    results["total_records"] += len(processed_records)

                results["processed_venues"] += 1

        except Exception as e:
            error_msg = (
                f"Error processing venue {venue.get('external_id', 'unknown')}: {e}"
            )
            results["errors"].append(error_msg)
            logging.error(error_msg)

    return results


# Main execution for testing
if __name__ == "__main__":
    # Test the foot traffic generation
    print("Testing foot traffic data generation...")

    test_venues = [
        {"external_id": "test_restaurant_1", "category": "restaurant"},
        {"external_id": "test_retail_1", "category": "store"},
        {"external_id": "test_bar_1", "category": "bar"},
    ]

    for venue in test_venues:
        print(f"\nTesting {venue['external_id']} ({venue['category']})...")

        # Generate sample data
        raw_data = fetch_foot_traffic(venue["external_id"], venue["category"])

        if raw_data and "traffic_data" in raw_data:
            print(f"Generated {len(raw_data['traffic_data'])} traffic records")

            # Show sample record
            sample = raw_data["traffic_data"][0]
            print(
                f"Sample: {sample['visitors_count']} visitors, {sample['median_dwell_seconds']}s dwell time"
            )

            # Process data
            processed = process_foot_traffic_data(raw_data)
            print(f"Processed {len(processed)} records for database storage")
        else:
            print("No data generated")
