"""
Venue data models for the PPM venue aggregation feature.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime


@dataclass
class Venue:
    """Venue data model"""

    external_id: str
    provider: str
    name: str
    category: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    avg_rating: Optional[float] = None
    review_count: Optional[int] = None
    price_level: Optional[int] = None
    psychographic_relevance: Optional[Dict] = None
    scraped_at: Optional[datetime] = None

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now()


@dataclass
class VenueCollectionResult:
    """Result of venue collection operation"""

    source_name: str
    success: bool
    venues_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class VenueProcessingResult:
    """Result of venue processing operation"""

    venues_processed: int
    venues_geocoded: int
    venues_enriched: int
    quality_score: float
    processing_time: float
    errors: List[str]
