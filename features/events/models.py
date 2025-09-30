"""
Event data models for the PPM event aggregation feature.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime


@dataclass
class Event:
    """Event data model"""

    external_id: str
    provider: str
    name: str
    description: Optional[str] = None
    category: str = "event"
    subcategory: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    venue_name: Optional[str] = None
    venue_id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source_url: Optional[str] = None
    image_url: Optional[str] = None
    price: Optional[str] = None
    psychographic_scores: Optional[Dict] = None
    scraped_at: Optional[datetime] = None

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now()


@dataclass
class EventCollectionResult:
    """Result of event collection operation"""

    source_name: str
    success: bool
    events_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class EventProcessingResult:
    """Result of event processing operation"""

    events_processed: int
    events_geocoded: int
    events_enriched: int
    quality_score: float
    processing_time: float
    errors: List[str]
