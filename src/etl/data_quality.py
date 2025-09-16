# etl/data_quality.py
import logging
import re
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from etl.utils import get_db_conn
import hashlib


def normalize_text(text):
    """
    Normalize text for comparison by removing extra whitespace,
    converting to lowercase, and removing special characters

    Args:
        text (str): Text to normalize

    Returns:
        str: Normalized text
    """
    if not text:
        return ""

    # Convert to lowercase and strip whitespace
    normalized = text.lower().strip()

    # Remove extra whitespace
    normalized = re.sub(r"\s+", " ", normalized)

    # Remove common punctuation that doesn't affect meaning
    normalized = re.sub(r"[^\w\s]", "", normalized)

    return normalized


def calculate_text_similarity(text1, text2):
    """
    Calculate similarity between two text strings using SequenceMatcher

    Args:
        text1 (str): First text string
        text2 (str): Second text string

    Returns:
        float: Similarity score between 0 and 1
    """
    if not text1 or not text2:
        return 0.0

    normalized1 = normalize_text(text1)
    normalized2 = normalize_text(text2)

    return SequenceMatcher(None, normalized1, normalized2).ratio()


def is_duplicate_event(event1, event2, similarity_threshold=0.85):
    """
    Determine if two events are likely duplicates based on multiple criteria

    Args:
        event1 (dict): First event dictionary
        event2 (dict): Second event dictionary
        similarity_threshold (float): Minimum similarity score to consider duplicate

    Returns:
        bool: True if events are likely duplicates
    """
    # Check title similarity
    title_similarity = calculate_text_similarity(
        event1.get("name", ""), event2.get("name", "")
    )

    # Check venue similarity
    venue_similarity = calculate_text_similarity(
        event1.get("venue_name", ""), event2.get("venue_name", "")
    )

    # Check date proximity (within 1 day)
    date1 = event1.get("start_time")
    date2 = event2.get("start_time")
    date_match = False

    if date1 and date2:
        if isinstance(date1, str):
            try:
                date1 = datetime.fromisoformat(date1.replace("Z", "+00:00"))
            except:
                date1 = None
        if isinstance(date2, str):
            try:
                date2 = datetime.fromisoformat(date2.replace("Z", "+00:00"))
            except:
                date2 = None

        if date1 and date2:
            date_diff = abs((date1 - date2).total_seconds())
            date_match = date_diff <= 86400  # Within 24 hours

    # Consider duplicate if high title similarity and either venue match or date match
    if title_similarity >= similarity_threshold:
        if venue_similarity >= 0.7 or date_match:
            return True

    # Also check for exact title match with venue or date match
    if title_similarity >= 0.95:
        if venue_similarity >= 0.5 or date_match:
            return True

    return False


def deduplicate_events(events):
    """
    Remove duplicate events from a list based on similarity analysis

    Args:
        events (list): List of event dictionaries

    Returns:
        list: Deduplicated list of events
    """
    if not events:
        return events

    deduplicated = []
    duplicates_found = 0

    for event in events:
        is_duplicate = False

        for existing_event in deduplicated:
            if is_duplicate_event(event, existing_event):
                is_duplicate = True
                duplicates_found += 1
                logging.debug(
                    f"Duplicate found: '{event.get('name')}' matches '{existing_event.get('name')}'"
                )
                break

        if not is_duplicate:
            deduplicated.append(event)

    if duplicates_found > 0:
        logging.info(
            f"Removed {duplicates_found} duplicate events from {len(events)} total events"
        )

    return deduplicated


def validate_event_data(event):
    """
    Validate event data quality and completeness

    Args:
        event (dict): Event dictionary to validate

    Returns:
        tuple: (is_valid, validation_errors)
    """
    errors = []

    # Required fields
    if not event.get("name"):
        errors.append("Missing event name")

    if not event.get("provider"):
        errors.append("Missing provider")

    # Validate event name quality
    name = event.get("name", "")
    if name:
        if len(name) < 3:
            errors.append("Event name too short")
        elif len(name) > 200:
            errors.append("Event name too long")
        elif name.lower() in ["test", "example", "placeholder"]:
            errors.append("Event name appears to be placeholder")

    # Validate date
    start_time = event.get("start_time")
    if start_time:
        if isinstance(start_time, str):
            try:
                parsed_date = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            except:
                errors.append("Invalid date format")
                parsed_date = None
        else:
            parsed_date = start_time

        if parsed_date:
            # Check if date is too far in the past (more than 1 year)
            if parsed_date < datetime.now() - timedelta(days=365):
                errors.append("Event date is too far in the past")
            # Check if date is too far in the future (more than 2 years)
            elif parsed_date > datetime.now() + timedelta(days=730):
                errors.append("Event date is too far in the future")

    # Validate venue name
    venue_name = event.get("venue_name", "")
    if venue_name and len(venue_name) > 100:
        errors.append("Venue name too long")

    # Validate description length
    description = event.get("description", "")
    if description and len(description) > 2000:
        errors.append("Description too long")

    # Validate external_id
    if not event.get("external_id"):
        errors.append("Missing external_id")

    return len(errors) == 0, errors


def clean_event_data(event):
    """
    Clean and normalize event data

    Args:
        event (dict): Event dictionary to clean

    Returns:
        dict: Cleaned event dictionary
    """
    cleaned_event = event.copy()

    # Clean and normalize text fields
    text_fields = ["name", "description", "venue_name"]
    for field in text_fields:
        if cleaned_event.get(field):
            # Strip whitespace
            cleaned_event[field] = cleaned_event[field].strip()

            # Remove excessive whitespace
            cleaned_event[field] = re.sub(r"\s+", " ", cleaned_event[field])

            # Remove HTML tags if present
            cleaned_event[field] = re.sub(r"<[^>]+>", "", cleaned_event[field])

            # Decode HTML entities
            import html

            cleaned_event[field] = html.unescape(cleaned_event[field])

    # Ensure category and subcategory are lowercase
    if cleaned_event.get("category"):
        cleaned_event["category"] = cleaned_event["category"].lower()

    if cleaned_event.get("subcategory"):
        cleaned_event["subcategory"] = cleaned_event["subcategory"].lower()

    # Ensure provider is lowercase and normalized
    if cleaned_event.get("provider"):
        cleaned_event["provider"] = cleaned_event["provider"].lower().replace(" ", "_")

    return cleaned_event


def generate_content_hash(event):
    """
    Generate a hash of event content for duplicate detection

    Args:
        event (dict): Event dictionary

    Returns:
        str: SHA256 hash of normalized event content
    """
    # Create a normalized string representation of key event fields
    content_parts = [
        normalize_text(event.get("name", "")),
        normalize_text(event.get("venue_name", "")),
        str(event.get("start_time", "")),
        normalize_text(event.get("description", ""))[
            :100
        ],  # First 100 chars of description
    ]

    content_string = "|".join(content_parts)
    return hashlib.sha256(content_string.encode("utf-8")).hexdigest()


def find_database_duplicates(events):
    """
    Find events that already exist in the database based on content similarity

    Args:
        events (list): List of event dictionaries to check

    Returns:
        list: List of events that are not duplicates of existing database entries
    """
    if not events:
        return events

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Get recent events from database (last 6 months)
        cur.execute(
            """
            SELECT name, venue_id, start_time, description, external_id, provider
            FROM events 
            WHERE start_time >= %s OR start_time IS NULL
        """,
            (datetime.now() - timedelta(days=180),),
        )

        db_events = cur.fetchall()

        # Convert to dictionaries for comparison
        db_event_dicts = []
        for db_event in db_events:
            db_event_dict = {
                "name": db_event[0],
                "venue_id": db_event[1],
                "start_time": db_event[2],
                "description": db_event[3],
                "external_id": db_event[4],
                "provider": db_event[5],
            }
            db_event_dicts.append(db_event_dict)

        # Filter out events that are duplicates of database entries
        unique_events = []
        duplicates_found = 0

        for event in events:
            is_db_duplicate = False

            # First check for exact external_id and provider match
            for db_event in db_event_dicts:
                if event.get("external_id") == db_event.get(
                    "external_id"
                ) and event.get("provider") == db_event.get("provider"):
                    is_db_duplicate = True
                    duplicates_found += 1
                    break

            # If not exact match, check for content similarity
            if not is_db_duplicate:
                for db_event in db_event_dicts:
                    if is_duplicate_event(event, db_event, similarity_threshold=0.9):
                        is_db_duplicate = True
                        duplicates_found += 1
                        logging.debug(
                            f"Database duplicate found: '{event.get('name')}' matches existing event"
                        )
                        break

            if not is_db_duplicate:
                unique_events.append(event)

        if duplicates_found > 0:
            logging.info(
                f"Filtered out {duplicates_found} events that already exist in database"
            )

        return unique_events

    except Exception as e:
        logging.error(f"Error checking for database duplicates: {e}")
        return events  # Return original events if error occurs
    finally:
        cur.close()
        conn.close()


def process_events_with_quality_checks(events):
    """
    Process events through complete data quality pipeline

    Args:
        events (list): List of raw event dictionaries

    Returns:
        tuple: (processed_events, quality_report)
    """
    if not events:
        return [], {"total_input": 0, "total_output": 0, "errors": []}

    quality_report = {
        "total_input": len(events),
        "validation_errors": 0,
        "duplicates_removed": 0,
        "database_duplicates_filtered": 0,
        "total_output": 0,
        "errors": [],
    }

    logging.info(f"Starting data quality processing for {len(events)} events")

    # Step 1: Clean event data
    cleaned_events = [clean_event_data(event) for event in events]

    # Step 2: Validate events
    valid_events = []
    for event in cleaned_events:
        is_valid, validation_errors = validate_event_data(event)
        if is_valid:
            valid_events.append(event)
        else:
            quality_report["validation_errors"] += 1
            quality_report["errors"].extend(validation_errors)

    logging.info(
        f"Validation: {len(valid_events)} valid events out of {len(cleaned_events)}"
    )

    # Step 3: Remove duplicates within the batch
    initial_count = len(valid_events)
    deduplicated_events = deduplicate_events(valid_events)
    quality_report["duplicates_removed"] = initial_count - len(deduplicated_events)

    # Step 4: Filter out database duplicates
    initial_db_count = len(deduplicated_events)
    final_events = find_database_duplicates(deduplicated_events)
    quality_report["database_duplicates_filtered"] = initial_db_count - len(
        final_events
    )

    quality_report["total_output"] = len(final_events)

    logging.info(
        f"Data quality processing complete: {quality_report['total_input']} -> {quality_report['total_output']} events"
    )

    return final_events, quality_report


def log_quality_metrics(quality_report, venue_name="Unknown"):
    """
    Log data quality metrics to database for monitoring

    Args:
        quality_report (dict): Quality report from processing
        venue_name (str): Name of the venue being processed
    """
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Insert quality metrics (assuming we have a quality_metrics table)
        cur.execute(
            """
            INSERT INTO scraping_metrics (
                venue_provider, scrape_timestamp, events_found, events_new, 
                events_updated, scrape_duration_seconds, success, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                venue_name.lower().replace(" ", "_"),
                datetime.now(),
                quality_report.get("total_input", 0),
                quality_report.get("total_output", 0),
                0,  # events_updated - would need to track this separately
                0,  # scrape_duration_seconds - would need to track this
                quality_report.get("total_output", 0) > 0,
                "; ".join(quality_report.get("errors", [])[:5]),  # First 5 errors
            ),
        )

        conn.commit()
        logging.info(f"Quality metrics logged for {venue_name}")

    except Exception as e:
        logging.error(f"Failed to log quality metrics for {venue_name}: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
