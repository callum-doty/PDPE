# Venue Registry
"""
Master venue registry for deduplication and relationship management.
Provides advanced venue matching, duplicate consolidation, and event-to-venue linking.
"""

import sys
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
import re
from difflib import SequenceMatcher

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class VenueMatch:
    """Result of venue matching operation"""

    master_venue_id: str
    matched_venue_id: str
    match_confidence: float
    match_type: str  # 'exact_name', 'fuzzy_name', 'location', 'combined'
    match_details: Dict


@dataclass
class DeduplicationResult:
    """Result of venue deduplication operation"""

    duplicates_found: int
    duplicates_consolidated: int
    master_venues_created: int
    processing_time_seconds: float
    consolidation_details: List[Dict]


class VenueRegistry:
    """
    Master venue registry for deduplication and relationship management.

    This class provides advanced venue matching algorithms, duplicate detection,
    and consolidation capabilities to maintain a clean master venue registry.
    """

    def __init__(self):
        """Initialize the venue registry."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

        # Matching thresholds
        self.name_similarity_threshold = 0.85
        self.location_distance_threshold = 0.1  # 100 meters
        self.fuzzy_name_threshold = 0.75

        # Common venue name variations for normalization
        self.venue_name_normalizations = {
            # Common abbreviations
            r"\bst\b": "street",
            r"\bave\b": "avenue",
            r"\bblvd\b": "boulevard",
            r"\bdr\b": "drive",
            r"\brd\b": "road",
            r"\bln\b": "lane",
            r"\bct\b": "court",
            r"\bpl\b": "place",
            r"\bpkwy\b": "parkway",
            r"\b&\b": "and",
            r"\bco\b": "company",
            r"\binc\b": "incorporated",
            r"\bllc\b": "limited liability company",
            # Venue type normalizations
            r"\brest\b": "restaurant",
            r"\bcafe\b": "coffee",
            r"\bbar\b": "bar",
            r"\bpub\b": "bar",
            r"\btavern\b": "bar",
            r"\bgrille?\b": "grill",
            r"\bbistro\b": "restaurant",
            r"\beatery\b": "restaurant",
            r"\bdiner\b": "restaurant",
            # Remove common suffixes that don't affect identity
            r"\s+(kc|kansas city)$": "",
            r"\s+(mo|missouri)$": "",
            r"\s+downtown$": "",
            r"\s+midtown$": "",
            r"\s+crossroads$": "",
            r"\s+plaza$": "",
        }

    def register_venue(self, venue_data: Dict) -> str:
        """
        Register a venue and return its master venue ID.

        Args:
            venue_data: Dictionary containing venue information

        Returns:
            Master venue ID (existing or newly created)
        """
        self.logger.debug(f"Registering venue: {venue_data.get('name', 'Unknown')}")

        # Try to find existing venue
        existing_venue_id = self.find_existing_venue(venue_data)

        if existing_venue_id:
            self.logger.debug(f"Found existing venue: {existing_venue_id}")
            # Update existing venue with new information
            self._update_venue_data(existing_venue_id, venue_data)
            return existing_venue_id
        else:
            # Create new master venue
            new_venue_id = self._create_master_venue(venue_data)
            self.logger.debug(f"Created new master venue: {new_venue_id}")
            return new_venue_id

    def find_or_create_venue(self, venue_data: Dict) -> str:
        """
        Find existing venue or create new one.

        Args:
            venue_data: Dictionary containing venue information

        Returns:
            Master venue ID
        """
        return self.register_venue(venue_data)

    def find_existing_venue(self, venue_data: Dict) -> Optional[str]:
        """
        Find existing venue using multiple matching strategies.

        Args:
            venue_data: Dictionary containing venue information

        Returns:
            Existing venue ID if found, None otherwise
        """
        venue_name = venue_data.get("name", "").strip()
        venue_lat = venue_data.get("lat")
        venue_lng = venue_data.get("lng")

        if not venue_name:
            return None

        conn = get_db_conn()
        if not conn:
            return None

        cur = conn.cursor()

        try:
            # Strategy 1: Exact name match
            exact_match = self._find_exact_name_match(cur, venue_name)
            if exact_match:
                return exact_match

            # Strategy 2: Normalized name match
            normalized_match = self._find_normalized_name_match(cur, venue_name)
            if normalized_match:
                return normalized_match

            # Strategy 3: Location-based match (if coordinates available)
            if venue_lat is not None and venue_lng is not None:
                location_match = self._find_location_match(
                    cur, venue_lat, venue_lng, venue_name
                )
                if location_match:
                    return location_match

            # Strategy 4: Fuzzy name matching
            fuzzy_match = self._find_fuzzy_name_match(
                cur, venue_name, venue_lat, venue_lng
            )
            if fuzzy_match:
                return fuzzy_match

            return None

        except Exception as e:
            self.logger.error(f"Error finding existing venue: {e}")
            return None
        finally:
            cur.close()
            conn.close()

    def link_events_to_venues(self, events: List[Dict]) -> List[Dict]:
        """
        Link events to their corresponding venues using advanced matching.

        Args:
            events: List of event dictionaries

        Returns:
            List of events with venue_id populated
        """
        self.logger.info(f"Linking {len(events)} events to venues")

        linked_events = []

        for event in events:
            venue_name = event.get("venue_name", "").strip()
            event_lat = event.get("lat")
            event_lng = event.get("lng")

            if not venue_name:
                linked_events.append(event)
                continue

            # Try to find venue for this event
            venue_data = {
                "name": venue_name,
                "lat": event_lat,
                "lng": event_lng,
                "category": "event_venue",
                "provider": event.get("provider", "unknown"),
            }

            venue_id = self.find_or_create_venue(venue_data)

            # Add venue_id to event
            event_copy = event.copy()
            event_copy["venue_id"] = venue_id
            linked_events.append(event_copy)

        self.logger.info(f"Successfully linked {len(linked_events)} events to venues")
        return linked_events

    def get_venue_relationships(self, venue_id: str) -> Dict:
        """
        Get relationships and connections for a venue.

        Args:
            venue_id: Master venue ID

        Returns:
            Dictionary containing venue relationships
        """
        conn = get_db_conn()
        if not conn:
            return {}

        cur = conn.cursor()

        try:
            # Get venue details
            cur.execute(
                """
                SELECT name, category, lat, lng, address
                FROM venues 
                WHERE venue_id = %s
            """,
                (venue_id,),
            )

            venue_info = cur.fetchone()
            if not venue_info:
                return {}

            venue_name, category, lat, lng, address = venue_info

            # Get related events
            cur.execute(
                """
                SELECT event_id, name, category, start_time, predicted_attendance
                FROM events 
                WHERE venue_id = %s
                AND (start_time IS NULL OR start_time >= NOW() - INTERVAL '30 days')
                ORDER BY start_time ASC
                LIMIT 10
            """,
                (venue_id,),
            )

            events = [
                {
                    "event_id": str(row[0]),
                    "name": row[1],
                    "category": row[2],
                    "start_time": row[3].isoformat() if row[3] else None,
                    "predicted_attendance": row[4],
                }
                for row in cur.fetchall()
            ]

            # Get nearby venues (within 500m)
            if lat is not None and lng is not None:
                cur.execute(
                    """
                    SELECT venue_id, name, category, 
                           ST_Distance(ST_Point(%s, %s)::geography, ST_Point(lng, lat)::geography) as distance
                    FROM venues 
                    WHERE venue_id != %s
                    AND lat IS NOT NULL AND lng IS NOT NULL
                    AND ST_DWithin(ST_Point(%s, %s)::geography, ST_Point(lng, lat)::geography, 500)
                    ORDER BY distance
                    LIMIT 5
                """,
                    (lng, lat, venue_id, lng, lat),
                )

                nearby_venues = [
                    {
                        "venue_id": str(row[0]),
                        "name": row[1],
                        "category": row[2],
                        "distance_meters": round(row[3]),
                    }
                    for row in cur.fetchall()
                ]
            else:
                nearby_venues = []

            # Get venues with similar names
            normalized_name = self._normalize_venue_name(venue_name)
            cur.execute(
                """
                SELECT venue_id, name, category
                FROM venues 
                WHERE venue_id != %s
                AND LOWER(name) LIKE %s
                LIMIT 5
            """,
                (venue_id, f"%{normalized_name[:10].lower()}%"),
            )

            similar_venues = [
                {
                    "venue_id": str(row[0]),
                    "name": row[1],
                    "category": row[2],
                    "similarity": SequenceMatcher(
                        None, venue_name.lower(), row[1].lower()
                    ).ratio(),
                }
                for row in cur.fetchall()
            ]

            # Sort by similarity
            similar_venues.sort(key=lambda x: x["similarity"], reverse=True)

            relationships = {
                "venue_info": {
                    "venue_id": venue_id,
                    "name": venue_name,
                    "category": category,
                    "location": (lat, lng) if lat and lng else None,
                    "address": address,
                },
                "events": events,
                "nearby_venues": nearby_venues,
                "similar_venues": similar_venues[:3],  # Top 3 most similar
                "relationship_summary": {
                    "total_events": len(events),
                    "nearby_venues_count": len(nearby_venues),
                    "similar_venues_count": len(similar_venues),
                },
            }

            return relationships

        except Exception as e:
            self.logger.error(f"Error getting venue relationships: {e}")
            return {}
        finally:
            cur.close()
            conn.close()

    def consolidate_venue_duplicates(self) -> DeduplicationResult:
        """
        Find and consolidate duplicate venues in the database.

        Returns:
            DeduplicationResult with consolidation statistics
        """
        start_time = datetime.now()
        self.logger.info("🔍 Starting venue deduplication process")

        conn = get_db_conn()
        if not conn:
            return DeduplicationResult(0, 0, 0, 0.0, [])

        cur = conn.cursor()

        try:
            # Get all venues for deduplication
            cur.execute(
                """
                SELECT venue_id, name, lat, lng, address, category, provider
                FROM venues 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                ORDER BY name
            """
            )

            venues = cur.fetchall()
            self.logger.info(f"Analyzing {len(venues)} venues for duplicates")

            duplicates_found = 0
            duplicates_consolidated = 0
            consolidation_details = []

            # Track processed venues to avoid double-processing
            processed_venues = set()

            for i, venue1 in enumerate(venues):
                if venue1[0] in processed_venues:
                    continue

                venue1_id, name1, lat1, lng1, addr1, cat1, prov1 = venue1

                # Find potential duplicates
                potential_duplicates = []

                for j, venue2 in enumerate(venues[i + 1 :], i + 1):
                    if venue2[0] in processed_venues:
                        continue

                    venue2_id, name2, lat2, lng2, addr2, cat2, prov2 = venue2

                    # Check if venues are duplicates
                    match_result = self._check_venue_duplicate(
                        (venue1_id, name1, lat1, lng1, addr1),
                        (venue2_id, name2, lat2, lng2, addr2),
                    )

                    if match_result["is_duplicate"]:
                        potential_duplicates.append(
                            {
                                "venue_id": venue2_id,
                                "name": name2,
                                "match_confidence": match_result["confidence"],
                                "match_type": match_result["match_type"],
                            }
                        )

                if potential_duplicates:
                    duplicates_found += len(potential_duplicates)

                    # Consolidate duplicates
                    master_venue_id = venue1_id
                    duplicate_ids = [dup["venue_id"] for dup in potential_duplicates]

                    consolidation_result = self._consolidate_duplicate_venues(
                        cur, master_venue_id, duplicate_ids
                    )

                    if consolidation_result["success"]:
                        duplicates_consolidated += len(duplicate_ids)

                        consolidation_details.append(
                            {
                                "master_venue_id": master_venue_id,
                                "master_venue_name": name1,
                                "consolidated_venues": potential_duplicates,
                                "events_moved": consolidation_result["events_moved"],
                                "timestamp": datetime.now().isoformat(),
                            }
                        )

                        # Mark duplicates as processed
                        for dup_id in duplicate_ids:
                            processed_venues.add(dup_id)

                        self.logger.info(
                            f"Consolidated {len(duplicate_ids)} duplicates into {master_venue_id}"
                        )

                processed_venues.add(venue1_id)

            conn.commit()

            processing_time = (datetime.now() - start_time).total_seconds()

            result = DeduplicationResult(
                duplicates_found=duplicates_found,
                duplicates_consolidated=duplicates_consolidated,
                master_venues_created=0,  # We're consolidating, not creating new ones
                processing_time_seconds=processing_time,
                consolidation_details=consolidation_details,
            )

            self.logger.info(
                f"✅ Deduplication completed: {duplicates_found} duplicates found, "
                f"{duplicates_consolidated} consolidated in {processing_time:.2f}s"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error during deduplication: {e}")
            conn.rollback()
            return DeduplicationResult(0, 0, 0, 0.0, [])
        finally:
            cur.close()
            conn.close()

    # Private helper methods

    def _find_exact_name_match(self, cur, venue_name: str) -> Optional[str]:
        """Find venue with exact name match."""
        cur.execute(
            """
            SELECT venue_id FROM venues 
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(%s))
            LIMIT 1
        """,
            (venue_name,),
        )

        result = cur.fetchone()
        return str(result[0]) if result else None

    def _find_normalized_name_match(self, cur, venue_name: str) -> Optional[str]:
        """Find venue with normalized name match."""
        normalized_name = self._normalize_venue_name(venue_name)

        cur.execute(
            """
            SELECT venue_id, name FROM venues
        """
        )

        venues = cur.fetchall()

        for venue_id, existing_name in venues:
            normalized_existing = self._normalize_venue_name(existing_name)
            if normalized_existing == normalized_name:
                return str(venue_id)

        return None

    def _find_location_match(
        self, cur, lat: float, lng: float, venue_name: str
    ) -> Optional[str]:
        """Find venue within location threshold."""
        cur.execute(
            """
            SELECT venue_id, name,
                   ST_Distance(ST_Point(%s, %s)::geography, ST_Point(lng, lat)::geography) as distance
            FROM venues 
            WHERE lat IS NOT NULL AND lng IS NOT NULL
            AND ST_DWithin(ST_Point(%s, %s)::geography, ST_Point(lng, lat)::geography, %s)
            ORDER BY distance
        """,
            (lng, lat, lng, lat, self.location_distance_threshold * 1000),
        )  # Convert km to meters

        results = cur.fetchall()

        for venue_id, existing_name, distance in results:
            # If very close location, check if names are reasonably similar
            name_similarity = SequenceMatcher(
                None, venue_name.lower(), existing_name.lower()
            ).ratio()

            if (
                distance < 50 and name_similarity > 0.5
            ):  # Within 50m and 50% name similarity
                return str(venue_id)
            elif distance < 20:  # Within 20m, assume same venue regardless of name
                return str(venue_id)

        return None

    def _find_fuzzy_name_match(
        self, cur, venue_name: str, lat: Optional[float], lng: Optional[float]
    ) -> Optional[str]:
        """Find venue using fuzzy name matching."""
        # Get venues with similar names
        name_words = venue_name.lower().split()
        if not name_words:
            return None

        # Build a query to find venues containing any of the main words
        main_words = [word for word in name_words if len(word) > 3]  # Skip short words
        if not main_words:
            main_words = name_words

        word_conditions = []
        params = []
        for word in main_words[:3]:  # Limit to first 3 words
            word_conditions.append("LOWER(name) LIKE %s")
            params.append(f"%{word}%")

        if not word_conditions:
            return None

        query = f"""
            SELECT venue_id, name, lat, lng
            FROM venues 
            WHERE ({' OR '.join(word_conditions)})
        """

        cur.execute(query, params)
        candidates = cur.fetchall()

        best_match = None
        best_score = 0

        for venue_id, existing_name, existing_lat, existing_lng in candidates:
            # Calculate name similarity
            name_similarity = SequenceMatcher(
                None, venue_name.lower(), existing_name.lower()
            ).ratio()

            # Calculate location bonus if both have coordinates
            location_bonus = 0
            if (
                lat is not None
                and lng is not None
                and existing_lat is not None
                and existing_lng is not None
            ):

                distance = self._calculate_distance(
                    (lat, lng), (existing_lat, existing_lng)
                )
                if distance < 1.0:  # Within 1km
                    location_bonus = 0.2 * (1.0 - distance)  # Closer = higher bonus

            total_score = name_similarity + location_bonus

            if total_score > best_score and name_similarity > self.fuzzy_name_threshold:
                best_score = total_score
                best_match = str(venue_id)

        return best_match

    def _normalize_venue_name(self, name: str) -> str:
        """Normalize venue name for better matching."""
        if not name:
            return ""

        # Convert to lowercase and strip
        normalized = name.lower().strip()

        # Remove special characters except spaces and alphanumeric
        normalized = re.sub(r"[^\w\s]", " ", normalized)

        # Apply normalizations
        for pattern, replacement in self.venue_name_normalizations.items():
            normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

        # Remove extra whitespace
        normalized = " ".join(normalized.split())

        return normalized

    def _calculate_distance(
        self, location1: Tuple[float, float], location2: Tuple[float, float]
    ) -> float:
        """Calculate distance between two lat/lng points in kilometers."""
        import math

        lat1, lng1 = location1
        lat2, lng2 = location2

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

        # Earth's radius in kilometers
        earth_radius = 6371.0

        return earth_radius * c

    def _check_venue_duplicate(self, venue1: Tuple, venue2: Tuple) -> Dict:
        """Check if two venues are duplicates."""
        venue1_id, name1, lat1, lng1, addr1 = venue1
        venue2_id, name2, lat2, lng2, addr2 = venue2

        # Name similarity
        name_similarity = SequenceMatcher(None, name1.lower(), name2.lower()).ratio()

        # Location distance
        location_distance = None
        if lat1 and lng1 and lat2 and lng2:
            location_distance = self._calculate_distance((lat1, lng1), (lat2, lng2))

        # Address similarity
        address_similarity = 0
        if addr1 and addr2:
            address_similarity = SequenceMatcher(
                None, addr1.lower(), addr2.lower()
            ).ratio()

        # Determine if duplicate
        is_duplicate = False
        match_type = "none"
        confidence = 0

        if name_similarity > 0.95 and (
            location_distance is None or location_distance < 0.1
        ):
            # Very high name similarity and close/unknown location
            is_duplicate = True
            match_type = "exact_name"
            confidence = 0.95
        elif (
            name_similarity > 0.85
            and location_distance is not None
            and location_distance < 0.05
        ):
            # High name similarity and very close location
            is_duplicate = True
            match_type = "name_location"
            confidence = 0.9
        elif (
            location_distance is not None
            and location_distance < 0.02
            and name_similarity > 0.6
        ):
            # Very close location and reasonable name similarity
            is_duplicate = True
            match_type = "location"
            confidence = 0.85
        elif (
            name_similarity > 0.8
            and address_similarity > 0.8
            and (location_distance is None or location_distance < 0.2)
        ):
            # High name and address similarity
            is_duplicate = True
            match_type = "name_address"
            confidence = 0.8

        return {
            "is_duplicate": is_duplicate,
            "match_type": match_type,
            "confidence": confidence,
            "name_similarity": name_similarity,
            "location_distance": location_distance,
            "address_similarity": address_similarity,
        }

    def _consolidate_duplicate_venues(
        self, cur, master_venue_id: str, duplicate_ids: List[str]
    ) -> Dict:
        """Consolidate duplicate venues into master venue."""
        try:
            events_moved = 0

            # Move events from duplicate venues to master venue
            for dup_id in duplicate_ids:
                cur.execute(
                    """
                    UPDATE events 
                    SET venue_id = %s 
                    WHERE venue_id = %s
                """,
                    (master_venue_id, dup_id),
                )

                events_moved += cur.rowcount

            # Delete duplicate venues
            cur.execute(
                """
                DELETE FROM venues 
                WHERE venue_id = ANY(%s)
            """,
                (duplicate_ids,),
            )

            return {
                "success": True,
                "events_moved": events_moved,
                "venues_deleted": len(duplicate_ids),
            }

        except Exception as e:
            self.logger.error(f"Error consolidating duplicates: {e}")
            return {"success": False, "error": str(e)}

    def _update_venue_data(self, venue_id: str, venue_data: Dict):
        """Update existing venue with new data."""
        conn = get_db_conn()
        if not conn:
            return

        cur = conn.cursor()

        try:
            # Update venue with any new information
            update_fields = []
            params = []

            if venue_data.get("address") and venue_data["address"].strip():
                update_fields.append("address = %s")
                params.append(venue_data["address"].strip())

            if venue_data.get("phone") and venue_data["phone"].strip():
                update_fields.append("phone = %s")
                params.append(venue_data["phone"].strip())

            if venue_data.get("website") and venue_data["website"].strip():
                update_fields.append("website = %s")
                params.append(venue_data["website"].strip())

            if venue_data.get("category") and venue_data["category"].strip():
                update_fields.append("category = %s")
                params.append(venue_data["category"].strip())

            if update_fields:
                update_fields.append("updated_at = NOW()")
                params.append(venue_id)

                query = f"""
                    UPDATE venues 
                    SET {', '.join(update_fields)}
                    WHERE venue_id = %s
                """

                cur.execute(query, params)
                conn.commit()

                self.logger.debug(f"Updated venue {venue_id} with new data")

        except Exception as e:
            self.logger.error(f"Error updating venue data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def _create_master_venue(self, venue_data: Dict) -> str:
        """Create a new master venue."""
        conn = get_db_conn()
        if not conn:
            return ""

        cur = conn.cursor()

        try:
            # Create new venue
            cur.execute(
                """
                INSERT INTO venues (
                    external_id, provider, name, category, subcategory,
                    lat, lng, address, phone, website
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING venue_id
            """,
                (
                    venue_data.get("external_id", ""),
                    venue_data.get("provider", "venue_registry"),
                    venue_data.get("name", ""),
                    venue_data.get("category", "unknown"),
                    venue_data.get("subcategory"),
                    venue_data.get("lat"),
                    venue_data.get("lng"),
                    venue_data.get("address"),
                    venue_data.get("phone"),
                    venue_data.get("website"),
                ),
            )

            venue_id = cur.fetchone()[0]
            conn.commit()

            self.logger.debug(f"Created new master venue: {venue_id}")
            return str(venue_id)

        except Exception as e:
            self.logger.error(f"Error creating master venue: {e}")
            conn.rollback()
            return ""
        finally:
            cur.close()
            conn.close()


# Convenience functions
def register_venue(venue_data):
    """Convenience function to register a venue."""
    registry = VenueRegistry()
    return registry.register_venue(venue_data)


def consolidate_duplicates():
    """Convenience function to consolidate venue duplicates."""
    registry = VenueRegistry()
    return registry.consolidate_venue_duplicates()


def link_events_to_venues(events):
    """Convenience function to link events to venues."""
    registry = VenueRegistry()
    return registry.link_events_to_venues(events)


if __name__ == "__main__":
    # Test the venue registry
    import logging

    logging.basicConfig(level=logging.INFO)

    registry = VenueRegistry()

    # Test venue registration
    print("Testing venue registration...")
    test_venue = {
        "name": "Test Restaurant",
        "category": "restaurant",
        "lat": 39.1,
        "lng": -94.6,
        "address": "123 Main St, Kansas City, MO",
        "provider": "test",
    }

    venue_id = registry.register_venue(test_venue)
    print(f"Registered venue with ID: {venue_id}")

    # Test duplicate detection
    print("\nTesting duplicate detection...")
    duplicate_venue = {
        "name": "Test Restaurant",  # Same name
        "category": "dining",
        "lat": 39.1001,  # Very close location
        "lng": -94.6001,
        "address": "123 Main Street, Kansas City, MO",  # Similar address
        "provider": "test2",
    }

    duplicate_id = registry.register_venue(duplicate_venue)
    print(f"Duplicate venue ID: {duplicate_id}")
    print(f"Same as original: {venue_id == duplicate_id}")

    # Test venue relationships
    print("\nTesting venue relationships...")
    relationships = registry.get_venue_relationships(venue_id)
    print(f"Venue relationships: {relationships.get('relationship_summary', {})}")

    # Test deduplication
    print("\nTesting venue deduplication...")
    dedup_result = registry.consolidate_venue_duplicates()
    print(
        f"Deduplication result: {dedup_result.duplicates_found} found, {dedup_result.duplicates_consolidated} consolidated"
    )
