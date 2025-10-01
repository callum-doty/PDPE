"""
Venue Processing Module - Compatibility wrapper for data collectors approach
Provides compatibility functions that delegate to the unified data collectors
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


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

    # Use fallback implementation since ETL module no longer exists
    # Fallback implementation
    processed_venues = []
    quality_metrics = {
        "total_venues": len(venues),
        "valid_venues": 0,
        "invalid_venues": 0,
        "missing_fields": {},
        "data_completeness_score": 0.0,
        "quality_issues": [],
    }

    for venue in venues:
        if venue.get("name"):
            processed_venues.append(venue)
            quality_metrics["valid_venues"] += 1
        else:
            quality_metrics["invalid_venues"] += 1

    if quality_metrics["total_venues"] > 0:
        quality_metrics["data_completeness_score"] = (
            quality_metrics["valid_venues"] / quality_metrics["total_venues"]
        )

    return processed_venues, quality_metrics


def enrich_venue_data(venue: Dict) -> Dict:
    """
    Enrich venue data with additional information - compatibility wrapper

    Args:
        venue: Venue dictionary

    Returns:
        Enriched venue dictionary
    """
    logger.debug(f"Enriching venue data for {venue.get('name', 'Unknown')}")

    enriched_venue = venue.copy()

    # Add processing timestamp
    enriched_venue["processed_at"] = datetime.now().isoformat()

    # Ensure comprehensive_score exists
    if not enriched_venue.get("comprehensive_score"):
        enriched_venue["comprehensive_score"] = calculate_venue_score(enriched_venue)

    # Ensure data_completeness exists
    if not enriched_venue.get("data_completeness"):
        enriched_venue["data_completeness"] = calculate_data_completeness(
            enriched_venue
        )

    return enriched_venue


def calculate_venue_score(venue: Dict) -> float:
    """
    Calculate comprehensive score for a venue - compatibility wrapper

    Args:
        venue: Venue dictionary

    Returns:
        Comprehensive score (0.0 to 1.0)
    """
    score = 0.0
    max_score = 0.0

    # Name (required)
    if venue.get("name"):
        score += 0.3
    max_score += 0.3

    # Location
    if venue.get("location") and isinstance(venue["location"], dict):
        if venue["location"].get("lat") and venue["location"].get("lng"):
            score += 0.2
    max_score += 0.2

    # Category
    if venue.get("category"):
        score += 0.1
    max_score += 0.1

    # Description or additional info
    if venue.get("description") or venue.get("address"):
        score += 0.1
    max_score += 0.1

    # Contact information
    if venue.get("phone") or venue.get("website") or venue.get("email"):
        score += 0.1
    max_score += 0.1

    # Hours or operational info
    if venue.get("hours") or venue.get("operating_hours"):
        score += 0.1
    max_score += 0.1

    # Additional data sources
    if venue.get("social_data") or venue.get("reviews") or venue.get("ratings"):
        score += 0.1
    max_score += 0.1

    return score / max_score if max_score > 0 else 0.0


def calculate_data_completeness(venue: Dict) -> float:
    """
    Calculate data completeness score for a venue - compatibility wrapper

    Args:
        venue: Venue dictionary

    Returns:
        Data completeness score (0.0 to 1.0)
    """
    total_fields = 0
    completed_fields = 0

    # Core fields
    core_fields = ["name", "category", "location", "address"]
    for field in core_fields:
        total_fields += 1
        if venue.get(field):
            completed_fields += 1

    # Optional fields
    optional_fields = ["phone", "website", "email", "description", "hours", "ratings"]
    for field in optional_fields:
        total_fields += 1
        if venue.get(field):
            completed_fields += 1

    return completed_fields / total_fields if total_fields > 0 else 0.0


def standardize_venue_category(category: str) -> str:
    """
    Standardize venue category - compatibility wrapper

    Args:
        category: Raw category string

    Returns:
        Standardized category
    """
    if not category:
        return "unknown"

    category = category.lower().strip()

    # Category mapping
    category_mapping = {
        "restaurant": ["restaurant", "dining", "food", "eatery", "cafe", "bistro"],
        "bar": ["bar", "pub", "tavern", "lounge", "nightclub", "brewery"],
        "entertainment": [
            "entertainment",
            "theater",
            "cinema",
            "venue",
            "club",
            "music",
        ],
        "retail": ["retail", "shop", "store", "shopping", "mall", "boutique"],
        "service": ["service", "business", "office", "professional", "medical"],
        "recreation": ["recreation", "park", "gym", "fitness", "sports", "outdoor"],
        "accommodation": ["hotel", "motel", "inn", "lodge", "accommodation", "resort"],
        "transportation": [
            "transportation",
            "transit",
            "station",
            "airport",
            "parking",
        ],
        "education": ["education", "school", "university", "college", "library"],
        "healthcare": ["healthcare", "hospital", "clinic", "medical", "pharmacy"],
    }

    for standard_category, keywords in category_mapping.items():
        if any(keyword in category for keyword in keywords):
            return standard_category

    return "other"


def validate_venue_location(venue: Dict) -> bool:
    """
    Validate venue location data - compatibility wrapper

    Args:
        venue: Venue dictionary

    Returns:
        True if location is valid, False otherwise
    """
    location = venue.get("location")
    if not location or not isinstance(location, dict):
        return False

    lat = location.get("lat")
    lng = location.get("lng")

    if not lat or not lng:
        return False

    try:
        lat_float = float(lat)
        lng_float = float(lng)

        # Basic coordinate validation (rough world bounds)
        if -90 <= lat_float <= 90 and -180 <= lng_float <= 180:
            return True
    except (ValueError, TypeError):
        pass

    return False


def merge_venue_data(existing_venue: Dict, new_venue: Dict) -> Dict:
    """
    Merge venue data from multiple sources - compatibility wrapper

    Args:
        existing_venue: Existing venue data
        new_venue: New venue data to merge

    Returns:
        Merged venue dictionary
    """
    merged = existing_venue.copy()

    # Update with non-empty values from new venue
    for key, value in new_venue.items():
        if value and (not merged.get(key) or key in ["updated_at", "last_seen"]):
            merged[key] = value

    # Update processing timestamp
    merged["updated_at"] = datetime.now().isoformat()

    # Recalculate scores
    merged["comprehensive_score"] = calculate_venue_score(merged)
    merged["data_completeness"] = calculate_data_completeness(merged)

    return merged


def filter_venues_by_quality(venues: List[Dict], min_score: float = 0.3) -> List[Dict]:
    """
    Filter venues by quality score - compatibility wrapper

    Args:
        venues: List of venue dictionaries
        min_score: Minimum comprehensive score threshold

    Returns:
        Filtered list of venues
    """
    filtered_venues = []

    for venue in venues:
        score = venue.get("comprehensive_score", 0.0)
        if score >= min_score:
            filtered_venues.append(venue)

    logger.info(
        f"Filtered {len(venues)} venues to {len(filtered_venues)} venues with score >= {min_score}"
    )

    return filtered_venues


def get_venue_statistics(venues: List[Dict]) -> Dict[str, Any]:
    """
    Get statistics about venue data - compatibility wrapper

    Args:
        venues: List of venue dictionaries

    Returns:
        Statistics dictionary
    """
    if not venues:
        return {
            "total_venues": 0,
            "avg_comprehensive_score": 0.0,
            "avg_data_completeness": 0.0,
            "categories": {},
            "providers": {},
        }

    total_venues = len(venues)
    total_score = sum(venue.get("comprehensive_score", 0.0) for venue in venues)
    total_completeness = sum(venue.get("data_completeness", 0.0) for venue in venues)

    # Count categories
    categories = {}
    for venue in venues:
        category = venue.get("category", "unknown")
        categories[category] = categories.get(category, 0) + 1

    # Count providers
    providers = {}
    for venue in venues:
        provider = venue.get("provider", "unknown")
        providers[provider] = providers.get(provider, 0) + 1

    return {
        "total_venues": total_venues,
        "avg_comprehensive_score": total_score / total_venues,
        "avg_data_completeness": total_completeness / total_venues,
        "categories": categories,
        "providers": providers,
    }


def log_venue_quality_metrics(quality_report: Dict, source: str):
    """
    Log venue quality metrics for monitoring - compatibility wrapper

    Args:
        quality_report: Quality metrics dictionary
        source: Data source name
    """
    logger.info(f"Venue Quality Report for {source}:")
    logger.info(
        f"  Total venues: {quality_report.get('total_venues', quality_report.get('total_items', 0))}"
    )
    logger.info(
        f"  Valid venues: {quality_report.get('valid_venues', quality_report.get('valid_items', 0))}"
    )
    logger.info(
        f"  Data completeness: {quality_report.get('data_completeness_score', 0):.2%}"
    )

    if quality_report.get("missing_fields"):
        logger.info("  Missing fields:")
        for field, count in quality_report["missing_fields"].items():
            logger.info(f"    {field}: {count} venues")

    if quality_report.get("quality_issues"):
        logger.warning(f"  Quality issues: {len(quality_report['quality_issues'])}")
        for issue in quality_report["quality_issues"][:5]:  # Show first 5 issues
            logger.warning(f"    {issue}")

    # Log venue statistics if available
    if quality_report.get("categories"):
        logger.info("  Venue categories:")
        for category, count in quality_report["categories"].items():
            logger.info(f"    {category}: {count} venues")
