"""
Data Quality Module - Lightweight wrapper for data collectors approach
Provides compatibility functions that delegate to the unified data collectors
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def process_events_with_quality_checks(events: List[Dict]) -> Tuple[List[Dict], Dict]:
    """
    Process events with quality checks - compatibility wrapper

    Args:
        events: List of event dictionaries

    Returns:
        Tuple of (processed_events, quality_report)
    """
    logger.info(
        f"Processing {len(events)} events with quality checks (compatibility mode)"
    )

    # Basic quality processing for compatibility
    processed_events = []
    quality_metrics = {
        "total_events": len(events),
        "valid_events": 0,
        "invalid_events": 0,
        "missing_fields": {},
        "data_completeness_score": 0.0,
        "quality_issues": [],
    }

    required_fields = ["name"]

    for event in events:
        # Basic validation
        is_valid = True
        missing_fields = []

        for field in required_fields:
            if not event.get(field):
                missing_fields.append(field)
                is_valid = False

        if is_valid or not missing_fields:  # Accept events with minimal requirements
            # Clean the event data
            cleaned_event = event.copy()
            if cleaned_event.get("name"):
                cleaned_event["name"] = cleaned_event["name"].strip()
            if cleaned_event.get("venue_name"):
                cleaned_event["venue_name"] = cleaned_event["venue_name"].strip()

            processed_events.append(cleaned_event)
            quality_metrics["valid_events"] += 1
        else:
            quality_metrics["invalid_events"] += 1
            quality_metrics["quality_issues"].append(
                f"Event missing required fields: {missing_fields}"
            )

    # Calculate data completeness score
    if quality_metrics["total_events"] > 0:
        quality_metrics["data_completeness_score"] = (
            quality_metrics["valid_events"] / quality_metrics["total_events"]
        )

    logger.info(
        f"Quality check complete: {quality_metrics['valid_events']}/{quality_metrics['total_events']} events valid"
    )

    return processed_events, quality_metrics


def log_quality_metrics(quality_report: Dict, source: str):
    """
    Log quality metrics for monitoring - compatibility wrapper

    Args:
        quality_report: Quality metrics dictionary
        source: Data source name
    """
    logger.info(f"Quality Report for {source}:")
    logger.info(
        f"  Total items: {quality_report.get('total_events', quality_report.get('total_venues', 0))}"
    )
    logger.info(
        f"  Valid items: {quality_report.get('valid_events', quality_report.get('valid_venues', 0))}"
    )
    logger.info(
        f"  Data completeness: {quality_report.get('data_completeness_score', 0):.2%}"
    )

    if quality_report.get("missing_fields"):
        logger.info("  Missing fields:")
        for field, count in quality_report["missing_fields"].items():
            logger.info(f"    {field}: {count} items")

    if quality_report.get("quality_issues"):
        logger.warning(f"  Quality issues: {len(quality_report['quality_issues'])}")
        for issue in quality_report["quality_issues"][:5]:  # Show first 5 issues
            logger.warning(f"    {issue}")


def process_venues_with_quality_checks(venues: List[Dict]) -> Tuple[List[Dict], Dict]:
    """
    Process venues with quality checks - compatibility wrapper

    Args:
        venues: List of venue dictionaries

    Returns:
        Tuple of (processed_venues, quality_report)
    """
    logger.info(
        f"Processing {len(venues)} venues with quality checks (compatibility mode)"
    )

    processed_venues = []
    quality_metrics = {
        "total_venues": len(venues),
        "valid_venues": 0,
        "invalid_venues": 0,
        "missing_fields": {},
        "data_completeness_score": 0.0,
        "quality_issues": [],
    }

    required_fields = ["name"]

    for venue in venues:
        # Basic validation
        is_valid = True
        missing_fields = []

        for field in required_fields:
            if not venue.get(field):
                missing_fields.append(field)
                is_valid = False

        if is_valid or not missing_fields:  # Accept venues with minimal requirements
            # Clean the venue data
            cleaned_venue = venue.copy()
            if cleaned_venue.get("name"):
                cleaned_venue["name"] = cleaned_venue["name"].strip()

            processed_venues.append(cleaned_venue)
            quality_metrics["valid_venues"] += 1
        else:
            quality_metrics["invalid_venues"] += 1
            quality_metrics["quality_issues"].append(
                f"Venue missing required fields: {missing_fields}"
            )

    # Calculate data completeness score
    if quality_metrics["total_venues"] > 0:
        quality_metrics["data_completeness_score"] = (
            quality_metrics["valid_venues"] / quality_metrics["total_venues"]
        )

    logger.info(
        f"Quality check complete: {quality_metrics['valid_venues']}/{quality_metrics['total_venues']} venues valid"
    )

    return processed_venues, quality_metrics
