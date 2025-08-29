"""
Event type definitions for the PDPE event-driven architecture.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional, List
from dataclasses import dataclass
from enum import Enum


class EventPriority(Enum):
    """Event priority levels for processing order."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class BaseEvent(ABC):
    """Base class for all events in the system."""

    event_id: str
    timestamp: datetime
    priority: EventPriority = EventPriority.NORMAL
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    @abstractmethod
    def get_event_type(self) -> str:
        """Return the event type identifier."""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "event_type": self.get_event_type(),
            "timestamp": self.timestamp.isoformat(),
            "priority": self.priority.value,
            "metadata": self.metadata,
        }


@dataclass
class TimeChangeEvent(BaseEvent):
    """Event triggered when time crosses significant boundaries."""

    previous_time: datetime = None
    new_time: datetime = None
    boundary_type: str = None  # 'hour', 'day', 'week', 'month'

    def get_event_type(self) -> str:
        return "time_change"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "previous_time": self.previous_time.isoformat(),
                "new_time": self.new_time.isoformat(),
                "boundary_type": self.boundary_type,
            }
        )
        return data


@dataclass
class EventAddedEvent(BaseEvent):
    """Event triggered when a new event is added to the system."""

    event_data: Dict[str, Any] = None
    source: str = None  # 'eventbrite', 'ticketmaster', etc.
    location: Dict[str, float] = None  # {'lat': float, 'lon': float}

    def get_event_type(self) -> str:
        return "event_added"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "event_data": self.event_data,
                "source": self.source,
                "location": self.location,
            }
        )
        return data


@dataclass
class EventRemovedEvent(BaseEvent):
    """Event triggered when an event is removed/cancelled."""

    removed_event_id: str = None
    source: str = None
    location: Dict[str, float] = None
    reason: str = None  # 'cancelled', 'expired', 'deleted'

    def get_event_type(self) -> str:
        return "event_removed"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "removed_event_id": self.event_id,
                "source": self.source,
                "location": self.location,
                "reason": self.reason,
            }
        )
        return data


@dataclass
class EventUpdatedEvent(BaseEvent):
    """Event triggered when an existing event is updated."""

    updated_event_id: str = None
    source: str = None
    old_data: Dict[str, Any] = None
    new_data: Dict[str, Any] = None
    changed_fields: List[str] = None

    def get_event_type(self) -> str:
        return "event_updated"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "updated_event_id": self.updated_event_id,
                "source": self.source,
                "old_data": self.old_data,
                "new_data": self.new_data,
                "changed_fields": self.changed_fields,
            }
        )
        return data


@dataclass
class GridUpdateEvent(BaseEvent):
    """Event triggered when grid cells need to be updated."""

    affected_cells: List[Dict[str, float]] = (
        None  # List of {'lat': float, 'lon': float}
    )
    update_type: str = None  # 'full', 'incremental', 'assumption_only'
    trigger_event_id: Optional[str] = None

    def get_event_type(self) -> str:
        return "grid_update"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "affected_cells": self.affected_cells,
                "update_type": self.update_type,
                "trigger_event_id": self.trigger_event_id,
            }
        )
        return data


@dataclass
class AssumptionLayerUpdateEvent(BaseEvent):
    """Event triggered when assumption layers need recalculation."""

    layer_type: str = None  # 'spending_propensity', 'college_presence', 'all'
    affected_area: Optional[Dict[str, Any]] = None  # Geographic bounds
    recalculation_reason: str = "time_change"

    def get_event_type(self) -> str:
        return "assumption_layer_update"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "layer_type": self.layer_type,
                "affected_area": self.affected_area,
                "recalculation_reason": self.recalculation_reason,
            }
        )
        return data


@dataclass
class VisualizationUpdateEvent(BaseEvent):
    """Event triggered when visualizations need to be updated."""

    visualization_type: str = None  # 'heatmap', 'grid', 'combined', 'all'
    update_data: Dict[str, Any] = None
    real_time: bool = True

    def get_event_type(self) -> str:
        return "visualization_update"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "visualization_type": self.visualization_type,
                "update_data": self.update_data,
                "real_time": self.real_time,
            }
        )
        return data


@dataclass
class WebhookReceivedEvent(BaseEvent):
    """Event triggered when a webhook is received from external service."""

    source: str = None  # 'eventbrite', 'twitter', etc.
    webhook_data: Dict[str, Any] = None
    webhook_type: str = None  # 'event.created', 'event.updated', etc.

    def get_event_type(self) -> str:
        return "webhook_received"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "source": self.source,
                "webhook_data": self.webhook_data,
                "webhook_type": self.webhook_type,
            }
        )
        return data


@dataclass
class CacheInvalidationEvent(BaseEvent):
    """Event triggered when cache needs to be invalidated."""

    cache_keys: List[str] = None
    cache_type: str = None  # 'assumption_layer', 'grid_data', 'api_response'
    invalidation_reason: str = None

    def get_event_type(self) -> str:
        return "cache_invalidation"

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data.update(
            {
                "cache_keys": self.cache_keys,
                "cache_type": self.cache_type,
                "invalidation_reason": self.invalidation_reason,
            }
        )
        return data


# Event factory function
def create_event_from_dict(event_dict: Dict[str, Any]) -> BaseEvent:
    """Create an event instance from a dictionary."""
    event_type = event_dict.get("event_type")

    event_classes = {
        "time_change": TimeChangeEvent,
        "event_added": EventAddedEvent,
        "event_removed": EventRemovedEvent,
        "event_updated": EventUpdatedEvent,
        "grid_update": GridUpdateEvent,
        "assumption_layer_update": AssumptionLayerUpdateEvent,
        "visualization_update": VisualizationUpdateEvent,
        "webhook_received": WebhookReceivedEvent,
        "cache_invalidation": CacheInvalidationEvent,
    }

    event_class = event_classes.get(event_type)
    if not event_class:
        raise ValueError(f"Unknown event type: {event_type}")

    # Convert timestamp back to datetime
    if "timestamp" in event_dict:
        event_dict["timestamp"] = datetime.fromisoformat(event_dict["timestamp"])

    # Convert priority back to enum
    if "priority" in event_dict:
        event_dict["priority"] = EventPriority(event_dict["priority"])

    # Handle specific event type fields
    if event_type == "time_change":
        event_dict["previous_time"] = datetime.fromisoformat(
            event_dict["previous_time"]
        )
        event_dict["new_time"] = datetime.fromisoformat(event_dict["new_time"])

    return event_class(**event_dict)
