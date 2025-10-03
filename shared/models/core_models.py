"""
Core data models for the PPM application.

This module contains all the unified data models, consolidating previously
scattered model definitions into a single, consistent location.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from datetime import datetime
from enum import Enum


class DataSource(Enum):
    """Enumeration of data sources"""

    GOOGLE_PLACES = "google_places"
    YELP = "yelp"
    FOURSQUARE = "foursquare"
    EVENTBRITE = "eventbrite"
    FACEBOOK = "facebook"
    MANUAL = "manual"
    SCRAPED = "scraped"


class ProcessingStatus(Enum):
    """Enumeration of processing statuses"""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Venue:
    """Unified venue data model"""

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
    updated_at: Optional[datetime] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    data_source: Optional[DataSource] = None

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now()
        if not self.updated_at:
            self.updated_at = datetime.now()


@dataclass
class Event:
    """Unified event data model"""

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
    updated_at: Optional[datetime] = None
    status: ProcessingStatus = ProcessingStatus.PENDING
    data_source: Optional[DataSource] = None

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now()
        if not self.updated_at:
            self.updated_at = datetime.now()


@dataclass
class VenueCollectionResult:
    """Result of venue collection operation"""

    source_name: str
    success: bool
    venues_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class EventCollectionResult:
    """Result of event collection operation"""

    source_name: str
    success: bool
    events_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class VenueProcessingResult:
    """Result of venue processing operation"""

    venues_processed: int
    venues_geocoded: int
    venues_enriched: int
    quality_score: float
    processing_time: float
    errors: List[str]
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class EventProcessingResult:
    """Result of event processing operation"""

    events_processed: int
    events_geocoded: int
    events_enriched: int
    quality_score: float
    processing_time: float
    errors: List[str]
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class PredictionResult:
    """Result of ML prediction operation"""

    entity_id: str
    entity_type: str  # 'venue' or 'event'
    predictions: Dict[str, float]
    confidence_score: float
    model_version: str
    prediction_timestamp: Optional[datetime] = None
    features_used: Optional[List[str]] = None

    def __post_init__(self):
        if not self.prediction_timestamp:
            self.prediction_timestamp = datetime.now()


@dataclass
class DataQualityMetrics:
    """Data quality metrics for monitoring"""

    entity_type: str
    total_records: int
    complete_records: int
    incomplete_records: int
    duplicate_records: int
    invalid_records: int
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    overall_quality_score: float
    timestamp: Optional[datetime] = None
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class DatabaseOperation:
    """Result of database operation"""

    operation_type: str  # 'insert', 'update', 'delete', 'select'
    table_name: str
    records_affected: int
    success: bool
    duration_seconds: float
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()


@dataclass
class APIResponse:
    """Standardized API response model"""

    endpoint: str
    status_code: int
    success: bool
    data: Optional[Any] = None
    error_message: Optional[str] = None
    response_time_ms: Optional[float] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now()
