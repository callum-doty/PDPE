# etl/venue_processing.py
"""
Unified venue processing pipeline for data quality, validation, scoring, and enrichment.
Ensures all venues are properly processed before database storage and map generation.
"""

import logging
import re
import hashlib
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import List, Dict, Tuple, Optional
from etl.utils import get_db_conn
import json


class VenueProcessor:
    """Unified venue processing pipeline for all venue data sources."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Venue validation rules
        self.min_name_length = 2
        self.max_name_length = 200
        self.max_description_length = 2000
        self.max_address_length = 500

        # Similarity thresholds for deduplication
        self.name_similarity_threshold = 0.85
        self.location_similarity_threshold = 0.9

        # Psychographic scoring weights
        self.psychographic_weights = {
            "career_driven": 0.4,
            "competent": 0.3,
            "fun": 0.3,
        }

    def process_venues_batch(self, venues: List[Dict]) -> Tuple[List[Dict], Dict]:
        """
        Process a batch of venues through the complete quality pipeline.

        Args:
            venues: List of raw venue dictionaries

        Returns:
            Tuple of (processed_venues, quality_report)
        """
        if not venues:
            return [], self._create_empty_quality_report()

        quality_report = self._initialize_quality_report(len(venues))

        self.logger.info(f"Starting venue processing pipeline for {len(venues)} venues")

        # Step 1: Clean and normalize venue data
        cleaned_venues = []
        for venue in venues:
            try:
                cleaned_venue = self._clean_venue_data(venue)
                cleaned_venues.append(cleaned_venue)
            except Exception as e:
                self.logger.error(
                    f"Error cleaning venue {venue.get('name', 'Unknown')}: {e}"
                )
                quality_report["cleaning_errors"] += 1

        # Step 2: Validate venues
        valid_venues = []
        for venue in cleaned_venues:
            is_valid, validation_errors = self._validate_venue_data(venue)
            if is_valid:
                valid_venues.append(venue)
            else:
                quality_report["validation_errors"] += 1
                quality_report["errors"].extend(validation_errors)

        self.logger.info(
            f"Validation: {len(valid_venues)} valid venues out of {len(cleaned_venues)}"
        )

        # Step 3: Enrich venues with calculated data
        enriched_venues = []
        for venue in valid_venues:
            try:
                enriched_venue = self._enrich_venue_data(venue)
                enriched_venues.append(enriched_venue)
            except Exception as e:
                self.logger.error(
                    f"Error enriching venue {venue.get('name', 'Unknown')}: {e}"
                )
                quality_report["enrichment_errors"] += 1

        # Step 4: Calculate psychographic scores
        scored_venues = []
        for venue in enriched_venues:
            try:
                scored_venue = self._calculate_venue_scores(venue)
                scored_venues.append(scored_venue)
            except Exception as e:
                self.logger.error(
                    f"Error scoring venue {venue.get('name', 'Unknown')}: {e}"
                )
                quality_report["scoring_errors"] += 1

        # Step 5: Remove duplicates within batch
        initial_count = len(scored_venues)
        deduplicated_venues = self._deduplicate_venues(scored_venues)
        quality_report["duplicates_removed"] = initial_count - len(deduplicated_venues)

        # Step 6: Filter out database duplicates
        initial_db_count = len(deduplicated_venues)
        final_venues = self._filter_database_duplicates(deduplicated_venues)
        quality_report["database_duplicates_filtered"] = initial_db_count - len(
            final_venues
        )

        # Step 7: Add processing metadata
        processed_venues = []
        for venue in final_venues:
            venue["processed_at"] = datetime.now()
            venue["processing_version"] = "1.0"
            processed_venues.append(venue)

        quality_report["total_output"] = len(processed_venues)

        self.logger.info(
            f"Venue processing complete: {quality_report['total_input']} -> {quality_report['total_output']} venues"
        )

        return processed_venues, quality_report

    def _clean_venue_data(self, venue: Dict) -> Dict:
        """Clean and normalize venue data."""
        cleaned_venue = venue.copy()

        # Clean text fields
        text_fields = ["name", "description", "address", "phone", "website"]
        for field in text_fields:
            if cleaned_venue.get(field):
                # Strip whitespace and normalize
                cleaned_venue[field] = self._normalize_text(cleaned_venue[field])

        # Clean and validate coordinates
        if cleaned_venue.get("lat") is not None:
            try:
                cleaned_venue["lat"] = float(cleaned_venue["lat"])
            except (ValueError, TypeError):
                cleaned_venue["lat"] = None

        if cleaned_venue.get("lng") is not None:
            try:
                cleaned_venue["lng"] = float(cleaned_venue["lng"])
            except (ValueError, TypeError):
                cleaned_venue["lng"] = None

        # Normalize category and subcategory
        if cleaned_venue.get("category"):
            cleaned_venue["category"] = cleaned_venue["category"].lower().strip()

        if cleaned_venue.get("subcategory"):
            cleaned_venue["subcategory"] = cleaned_venue["subcategory"].lower().strip()

        # Normalize provider
        if cleaned_venue.get("provider"):
            cleaned_venue["provider"] = (
                cleaned_venue["provider"].lower().replace(" ", "_")
            )

        # Clean price tier
        if cleaned_venue.get("price_tier"):
            price_tier = str(cleaned_venue["price_tier"]).strip()
            if price_tier.isdigit() and 1 <= int(price_tier) <= 4:
                cleaned_venue["price_tier"] = int(price_tier)
            else:
                cleaned_venue["price_tier"] = None

        # Clean rating
        if cleaned_venue.get("avg_rating"):
            try:
                rating = float(cleaned_venue["avg_rating"])
                if 0 <= rating <= 5:
                    cleaned_venue["avg_rating"] = rating
                else:
                    cleaned_venue["avg_rating"] = None
            except (ValueError, TypeError):
                cleaned_venue["avg_rating"] = None

        return cleaned_venue

    def _normalize_text(self, text: str) -> str:
        """Normalize text by removing extra whitespace and HTML."""
        if not text:
            return ""

        # Strip whitespace
        normalized = text.strip()

        # Remove excessive whitespace
        normalized = re.sub(r"\s+", " ", normalized)

        # Remove HTML tags
        normalized = re.sub(r"<[^>]+>", "", normalized)

        # Decode HTML entities
        import html

        normalized = html.unescape(normalized)

        return normalized

    def _validate_venue_data(self, venue: Dict) -> Tuple[bool, List[str]]:
        """Validate venue data quality and completeness."""
        errors = []

        # Required fields
        if not venue.get("name"):
            errors.append("Missing venue name")
        elif len(venue["name"]) < self.min_name_length:
            errors.append("Venue name too short")
        elif len(venue["name"]) > self.max_name_length:
            errors.append("Venue name too long")

        if not venue.get("provider"):
            errors.append("Missing provider")

        # Validate coordinates
        lat = venue.get("lat")
        lng = venue.get("lng")

        if lat is None or lng is None:
            errors.append("Missing coordinates")
        else:
            # Check if coordinates are within reasonable bounds (roughly North America)
            if not (25.0 <= lat <= 50.0):
                errors.append("Latitude out of reasonable bounds")
            if not (-130.0 <= lng <= -65.0):
                errors.append("Longitude out of reasonable bounds")

        # Validate external_id
        if not venue.get("external_id"):
            errors.append("Missing external_id")

        # Validate description length
        description = venue.get("description", "")
        if description and len(description) > self.max_description_length:
            errors.append("Description too long")

        # Validate address length
        address = venue.get("address", "")
        if address and len(address) > self.max_address_length:
            errors.append("Address too long")

        # Validate category
        if not venue.get("category"):
            errors.append("Missing category")

        # Check for placeholder data
        name = venue.get("name", "").lower()
        if name in ["test", "example", "placeholder", "unknown"]:
            errors.append("Venue name appears to be placeholder")

        return len(errors) == 0, errors

    def _enrich_venue_data(self, venue: Dict) -> Dict:
        """Enrich venue data with calculated fields and metadata."""
        enriched_venue = venue.copy()

        # Generate content hash for duplicate detection
        enriched_venue["content_hash"] = self._generate_venue_hash(venue)

        # Add geocoding quality score
        enriched_venue["geocoding_quality"] = self._assess_geocoding_quality(venue)

        # Standardize amenities
        if venue.get("amenities"):
            enriched_venue["amenities"] = self._standardize_amenities(
                venue["amenities"]
            )

        # Parse and validate hours
        if venue.get("hours_json"):
            enriched_venue["hours_parsed"] = self._parse_hours(venue["hours_json"])

        # Add venue type classification
        enriched_venue["venue_type"] = self._classify_venue_type(venue)

        # Calculate venue size estimate
        enriched_venue["size_estimate"] = self._estimate_venue_size(venue)

        return enriched_venue

    def _calculate_venue_scores(self, venue: Dict) -> Dict:
        """Calculate comprehensive psychographic and quality scores for venue."""
        scored_venue = venue.copy()

        # Calculate psychographic relevance scores
        psychographic_scores = self._calculate_psychographic_scores(venue)
        scored_venue["psychographic_relevance"] = psychographic_scores

        # Calculate total weighted score
        total_score = 0.0
        for category, score in psychographic_scores.items():
            weight = self.psychographic_weights.get(category, 0.0)
            total_score += score * weight

        scored_venue["total_score"] = min(1.0, max(0.0, total_score))

        # Calculate quality score
        scored_venue["quality_score"] = self._calculate_quality_score(venue)

        # Calculate popularity score
        scored_venue["popularity_score"] = self._calculate_popularity_score(venue)

        # Calculate composite final score
        final_score = (
            scored_venue["total_score"] * 0.6
            + scored_venue["quality_score"] * 0.3
            + scored_venue["popularity_score"] * 0.1
        )
        scored_venue["final_score"] = min(1.0, max(0.0, final_score))

        return scored_venue

    def _calculate_psychographic_scores(self, venue: Dict) -> Dict:
        """Calculate psychographic relevance scores based on venue characteristics."""
        name = venue.get("name", "").lower()
        description = venue.get("description", "").lower()
        category = venue.get("category", "").lower()
        subcategory = venue.get("subcategory", "").lower()

        # Combine all text for analysis
        text_content = f"{name} {description} {category} {subcategory}"

        # Define psychographic keywords
        psychographic_keywords = {
            "career_driven": [
                "business",
                "professional",
                "networking",
                "conference",
                "corporate",
                "coworking",
                "office",
                "meeting",
                "executive",
                "entrepreneur",
                "startup",
                "innovation",
                "technology",
                "finance",
                "consulting",
            ],
            "competent": [
                "expert",
                "premium",
                "quality",
                "professional",
                "certified",
                "award",
                "excellence",
                "master",
                "specialist",
                "advanced",
                "luxury",
                "high-end",
                "exclusive",
                "elite",
                "sophisticated",
            ],
            "fun": [
                "entertainment",
                "party",
                "music",
                "dance",
                "nightlife",
                "bar",
                "club",
                "festival",
                "concert",
                "comedy",
                "game",
                "arcade",
                "bowling",
                "karaoke",
                "social",
                "celebration",
                "happy hour",
            ],
        }

        scores = {}
        for category, keywords in psychographic_keywords.items():
            score = 0.0
            keyword_matches = 0

            for keyword in keywords:
                if keyword in text_content:
                    keyword_matches += 1
                    # Weight by keyword importance and frequency
                    score += 0.1 * (1 + text_content.count(keyword) * 0.1)

            # Normalize score based on keyword matches and venue characteristics
            if keyword_matches > 0:
                score = min(1.0, score)

                # Boost score based on venue category alignment
                if category == "career_driven" and any(
                    cat in venue.get("category", "")
                    for cat in ["business", "office", "coworking"]
                ):
                    score *= 1.2
                elif category == "competent" and venue.get("avg_rating", 0) >= 4.0:
                    score *= 1.1
                elif category == "fun" and any(
                    cat in venue.get("category", "")
                    for cat in ["entertainment", "nightlife", "restaurant"]
                ):
                    score *= 1.2

            scores[category] = min(1.0, max(0.0, score))

        return scores

    def _calculate_quality_score(self, venue: Dict) -> float:
        """Calculate venue quality score based on available data completeness and quality."""
        score = 0.0
        max_score = 0.0

        # Rating quality (30% of quality score)
        if venue.get("avg_rating"):
            rating = venue["avg_rating"]
            review_count = venue.get("review_count", 0)

            # Normalize rating (0-5 scale to 0-1)
            rating_score = rating / 5.0

            # Boost for higher review counts (more reliable)
            if review_count >= 100:
                rating_score *= 1.1
            elif review_count >= 50:
                rating_score *= 1.05

            score += rating_score * 0.3
        max_score += 0.3

        # Data completeness (40% of quality score)
        completeness_fields = [
            "name",
            "address",
            "phone",
            "website",
            "description",
            "category",
        ]
        completed_fields = sum(1 for field in completeness_fields if venue.get(field))
        completeness_score = completed_fields / len(completeness_fields)
        score += completeness_score * 0.4
        max_score += 0.4

        # Geocoding quality (20% of quality score)
        geocoding_quality = venue.get("geocoding_quality", 0.5)
        score += geocoding_quality * 0.2
        max_score += 0.2

        # Additional data richness (10% of quality score)
        richness_score = 0.0
        if venue.get("amenities"):
            richness_score += 0.3
        if venue.get("hours_json"):
            richness_score += 0.3
        if venue.get("price_tier"):
            richness_score += 0.4
        score += richness_score * 0.1
        max_score += 0.1

        return score / max_score if max_score > 0 else 0.0

    def _calculate_popularity_score(self, venue: Dict) -> float:
        """Calculate venue popularity score based on reviews, ratings, and social signals."""
        score = 0.0

        # Review count contribution
        review_count = venue.get("review_count", 0)
        if review_count > 0:
            # Logarithmic scaling for review count
            import math

            review_score = min(1.0, math.log10(review_count + 1) / 3.0)  # Scale to 0-1
            score += review_score * 0.6

        # Rating contribution
        if venue.get("avg_rating"):
            rating_score = venue["avg_rating"] / 5.0
            score += rating_score * 0.4

        return min(1.0, max(0.0, score))

    def _deduplicate_venues(self, venues: List[Dict]) -> List[Dict]:
        """Remove duplicate venues within the batch."""
        if not venues:
            return venues

        deduplicated = []
        duplicates_found = 0

        for venue in venues:
            is_duplicate = False

            for existing_venue in deduplicated:
                if self._are_venues_duplicate(venue, existing_venue):
                    is_duplicate = True
                    duplicates_found += 1
                    self.logger.debug(
                        f"Duplicate venue found: '{venue.get('name')}' matches '{existing_venue.get('name')}'"
                    )
                    break

            if not is_duplicate:
                deduplicated.append(venue)

        if duplicates_found > 0:
            self.logger.info(f"Removed {duplicates_found} duplicate venues from batch")

        return deduplicated

    def _are_venues_duplicate(self, venue1: Dict, venue2: Dict) -> bool:
        """Determine if two venues are duplicates based on multiple criteria."""
        # Check name similarity
        name_similarity = self._calculate_text_similarity(
            venue1.get("name", ""), venue2.get("name", "")
        )

        # Check location proximity (within ~100 meters)
        location_similar = self._are_locations_similar(
            venue1.get("lat"), venue1.get("lng"), venue2.get("lat"), venue2.get("lng")
        )

        # Check address similarity
        address_similarity = self._calculate_text_similarity(
            venue1.get("address", ""), venue2.get("address", "")
        )

        # Consider duplicate if high name similarity and either location or address match
        if name_similarity >= self.name_similarity_threshold:
            if location_similar or address_similarity >= 0.8:
                return True

        # Also check for exact external_id match from same provider
        if (
            venue1.get("external_id") == venue2.get("external_id")
            and venue1.get("provider") == venue2.get("provider")
            and venue1.get("external_id")
        ):
            return True

        return False

    def _filter_database_duplicates(self, venues: List[Dict]) -> List[Dict]:
        """Filter out venues that already exist in the database."""
        if not venues:
            return venues

        conn = get_db_conn()
        if not conn:
            self.logger.warning("No database connection, skipping duplicate check")
            return venues

        cur = conn.cursor()

        try:
            # Get existing venues from database
            cur.execute(
                """
                SELECT name, lat, lng, address, external_id, provider, content_hash
                FROM venues 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
            """
            )

            db_venues = cur.fetchall()
            db_venue_dicts = []

            for db_venue in db_venues:
                db_venue_dict = {
                    "name": db_venue[0],
                    "lat": float(db_venue[1]) if db_venue[1] else None,
                    "lng": float(db_venue[2]) if db_venue[2] else None,
                    "address": db_venue[3],
                    "external_id": db_venue[4],
                    "provider": db_venue[5],
                    "content_hash": db_venue[6],
                }
                db_venue_dicts.append(db_venue_dict)

            # Filter out duplicates
            unique_venues = []
            duplicates_found = 0

            for venue in venues:
                is_db_duplicate = False

                # Check for exact external_id and provider match first
                for db_venue in db_venue_dicts:
                    if (
                        venue.get("external_id") == db_venue.get("external_id")
                        and venue.get("provider") == db_venue.get("provider")
                        and venue.get("external_id")
                    ):
                        is_db_duplicate = True
                        duplicates_found += 1
                        break

                # Check for content hash match
                if not is_db_duplicate and venue.get("content_hash"):
                    for db_venue in db_venue_dicts:
                        if venue["content_hash"] == db_venue.get("content_hash"):
                            is_db_duplicate = True
                            duplicates_found += 1
                            break

                # Check for similarity-based duplicates
                if not is_db_duplicate:
                    for db_venue in db_venue_dicts:
                        if self._are_venues_duplicate(venue, db_venue):
                            is_db_duplicate = True
                            duplicates_found += 1
                            self.logger.debug(
                                f"Database duplicate found: '{venue.get('name')}' matches existing venue"
                            )
                            break

                if not is_db_duplicate:
                    unique_venues.append(venue)

            if duplicates_found > 0:
                self.logger.info(
                    f"Filtered out {duplicates_found} venues that already exist in database"
                )

            return unique_venues

        except Exception as e:
            self.logger.error(f"Error checking for database duplicates: {e}")
            return venues
        finally:
            cur.close()
            conn.close()

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two text strings."""
        if not text1 or not text2:
            return 0.0

        # Normalize texts
        norm1 = self._normalize_text_for_comparison(text1)
        norm2 = self._normalize_text_for_comparison(text2)

        return SequenceMatcher(None, norm1, norm2).ratio()

    def _normalize_text_for_comparison(self, text: str) -> str:
        """Normalize text for similarity comparison."""
        if not text:
            return ""

        # Convert to lowercase and strip
        normalized = text.lower().strip()

        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", normalized)

        # Remove punctuation
        normalized = re.sub(r"[^\w\s]", "", normalized)

        return normalized

    def _are_locations_similar(
        self,
        lat1: float,
        lng1: float,
        lat2: float,
        lng2: float,
        threshold_meters: float = 100.0,
    ) -> bool:
        """Check if two locations are within a certain distance threshold."""
        if None in [lat1, lng1, lat2, lng2]:
            return False

        # Simple distance calculation (approximate for small distances)
        import math

        # Convert to radians
        lat1_rad = math.radians(lat1)
        lng1_rad = math.radians(lng1)
        lat2_rad = math.radians(lat2)
        lng2_rad = math.radians(lng2)

        # Haversine formula
        dlat = lat2_rad - lat1_rad
        dlng = lng2_rad - lng1_rad

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Earth radius in meters
        earth_radius = 6371000
        distance = earth_radius * c

        return distance <= threshold_meters

    def _generate_venue_hash(self, venue: Dict) -> str:
        """Generate a hash of venue content for duplicate detection."""
        content_parts = [
            self._normalize_text_for_comparison(venue.get("name", "")),
            self._normalize_text_for_comparison(venue.get("address", "")),
            str(round(venue.get("lat", 0), 4)),  # Round to ~10m precision
            str(round(venue.get("lng", 0), 4)),
        ]

        content_string = "|".join(content_parts)
        return hashlib.sha256(content_string.encode("utf-8")).hexdigest()

    def _assess_geocoding_quality(self, venue: Dict) -> float:
        """Assess the quality of geocoding for the venue."""
        score = 0.5  # Default neutral score

        lat = venue.get("lat")
        lng = venue.get("lng")

        if lat is None or lng is None:
            return 0.0

        # Check coordinate precision (more decimal places = higher quality)
        lat_str = str(lat)
        lng_str = str(lng)

        lat_decimals = len(lat_str.split(".")[-1]) if "." in lat_str else 0
        lng_decimals = len(lng_str.split(".")[-1]) if "." in lng_str else 0

        avg_decimals = (lat_decimals + lng_decimals) / 2
        precision_score = min(1.0, avg_decimals / 6.0)  # 6 decimals = ~1m precision

        # Check if coordinates look like they're in a reasonable location
        # (not exactly 0,0 or other suspicious values)
        if lat == 0.0 and lng == 0.0:
            return 0.0

        # Boost score if we have address to validate against
        if venue.get("address"):
            score += 0.2

        return min(1.0, score + precision_score * 0.3)

    def _standardize_amenities(self, amenities) -> List[str]:
        """Standardize amenities list."""
        if not amenities:
            return []

        if isinstance(amenities, str):
            # Try to parse as JSON first
            try:
                amenities = json.loads(amenities)
            except:
                # Split by common delimiters
                amenities = re.split(r"[,;|]", amenities)

        if not isinstance(amenities, list):
            return []

        # Clean and standardize each amenity
        standardized = []
        for amenity in amenities:
            if isinstance(amenity, str):
                cleaned = amenity.strip().lower()
                if cleaned and len(cleaned) > 1:
                    standardized.append(cleaned)

        return list(set(standardized))  # Remove duplicates

    def _parse_hours(self, hours_data) -> Dict:
        """Parse and validate hours data."""
        if not hours_data:
            return {}

        if isinstance(hours_data, str):
            try:
                hours_data = json.loads(hours_data)
            except:
                return {}

        if not isinstance(hours_data, dict):
            return {}

        # Validate and clean hours data
        cleaned_hours = {}
        days = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]

        for day in days:
            if day in hours_data:
                day_hours = hours_data[day]
                if isinstance(day_hours, str) and day_hours.strip():
                    cleaned_hours[day] = day_hours.strip()

        return cleaned_hours

    def _classify_venue_type(self, venue: Dict) -> str:
        """Classify venue into a standardized type."""
        category = venue.get("category", "").lower()
        subcategory = venue.get("subcategory", "").lower()
        name = venue.get("name", "").lower()

        # Define venue type mappings
        type_mappings = {
            "restaurant": ["restaurant", "food", "dining", "cafe", "bistro", "eatery"],
            "bar": ["bar", "pub", "tavern", "brewery", "lounge"],
            "entertainment": ["entertainment", "theater", "cinema", "club", "venue"],
            "retail": ["retail", "shop", "store", "boutique", "market"],
            "service": ["service", "business", "office", "professional"],
            "accommodation": ["hotel", "motel", "inn", "lodge", "resort"],
            "recreation": ["recreation", "park", "gym", "fitness", "sports"],
            "cultural": ["museum", "gallery", "cultural", "historic", "art"],
        }

        # Check category and subcategory against mappings
        for venue_type, keywords in type_mappings.items():
            if any(keyword in category for keyword in keywords):
                return venue_type
            if any(keyword in subcategory for keyword in keywords):
                return venue_type
            if any(keyword in name for keyword in keywords):
                return venue_type

        return "other"

    def _estimate_venue_size(self, venue: Dict) -> str:
        """Estimate venue size based on available indicators."""
        # Default to medium
        size = "medium"

        # Use review count as a proxy for size/popularity
        review_count = venue.get("review_count", 0)

        if review_count >= 500:
            size = "large"
        elif review_count >= 100:
            size = "medium"
        elif review_count > 0:
            size = "small"

        # Adjust based on venue type
        venue_type = venue.get("venue_type", "other")
        if venue_type in ["entertainment", "accommodation"]:
            # These tend to be larger
            if size == "small":
                size = "medium"
        elif venue_type in ["retail", "service"]:
            # These tend to be smaller
            if size == "large":
                size = "medium"

        return size

    def _initialize_quality_report(self, input_count: int) -> Dict:
        """Initialize quality report structure."""
        return {
            "total_input": input_count,
            "cleaning_errors": 0,
            "validation_errors": 0,
            "enrichment_errors": 0,
            "scoring_errors": 0,
            "duplicates_removed": 0,
            "database_duplicates_filtered": 0,
            "total_output": 0,
            "errors": [],
        }

    def _create_empty_quality_report(self) -> Dict:
        """Create empty quality report for empty input."""
        return {
            "total_input": 0,
            "cleaning_errors": 0,
            "validation_errors": 0,
            "enrichment_errors": 0,
            "scoring_errors": 0,
            "duplicates_removed": 0,
            "database_duplicates_filtered": 0,
            "total_output": 0,
            "errors": [],
        }

    def log_processing_metrics(self, quality_report: Dict, provider: str = "unknown"):
        """Log venue processing metrics to database."""
        conn = get_db_conn()
        if not conn:
            return

        cur = conn.cursor()

        try:
            # Insert processing metrics (assuming we have a venue_processing_metrics table)
            cur.execute(
                """
                INSERT INTO scraping_metrics (
                    venue_provider, scrape_timestamp, events_found, events_new, 
                    events_updated, scrape_duration_seconds, success, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    provider,
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
            self.logger.info(f"Venue processing metrics logged for {provider}")

        except Exception as e:
            self.logger.error(
                f"Failed to log venue processing metrics for {provider}: {e}"
            )
            conn.rollback()
        finally:
            cur.close()
            conn.close()


# Convenience functions for backward compatibility and easy usage
def process_venues_with_quality_checks(venues: List[Dict]) -> Tuple[List[Dict], Dict]:
    """
    Process venues through complete quality pipeline.

    Args:
        venues: List of raw venue dictionaries

    Returns:
        Tuple of (processed_venues, quality_report)
    """
    processor = VenueProcessor()
    return processor.process_venues_batch(venues)


def log_venue_quality_metrics(quality_report: Dict, provider: str = "unknown"):
    """
    Log venue quality metrics to database.

    Args:
        quality_report: Quality report from processing
        provider: Name of the venue provider
    """
    processor = VenueProcessor()
    processor.log_processing_metrics(quality_report, provider)
