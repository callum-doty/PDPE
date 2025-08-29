"""
Event handlers for the PDPE event-driven architecture.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from .event_types import (
    BaseEvent,
    TimeChangeEvent,
    EventAddedEvent,
    EventRemovedEvent,
    EventUpdatedEvent,
    GridUpdateEvent,
    AssumptionLayerUpdateEvent,
    VisualizationUpdateEvent,
    WebhookReceivedEvent,
)

logger = logging.getLogger(__name__)


class BaseEventHandler:
    """Base class for event handlers."""

    def __init__(self, name: str):
        self.name = name
        self.processed_events = 0
        self.errors = 0

    def handle_event(self, event: BaseEvent):
        """Handle an event. Override in subclasses."""
        try:
            self._process_event(event)
            self.processed_events += 1
            logger.debug(f"{self.name} processed event: {event.get_event_type()}")
        except Exception as e:
            self.errors += 1
            logger.error(f"{self.name} error processing {event.get_event_type()}: {e}")
            raise

    def _process_event(self, event: BaseEvent):
        """Override this method in subclasses."""
        raise NotImplementedError

    def get_stats(self) -> Dict[str, Any]:
        """Get handler statistics."""
        return {
            "name": self.name,
            "processed_events": self.processed_events,
            "errors": self.errors,
        }


class AssumptionLayerHandler(BaseEventHandler):
    """Handler for assumption layer updates."""

    def __init__(self):
        super().__init__("AssumptionLayerHandler")
        self.assumption_engine = None

    def set_assumption_engine(self, engine):
        """Set the assumption layer engine."""
        self.assumption_engine = engine

    def _process_event(self, event: BaseEvent):
        """Process assumption layer related events."""
        if isinstance(event, TimeChangeEvent):
            self._handle_time_change(event)
        elif isinstance(event, EventAddedEvent):
            self._handle_event_added(event)
        elif isinstance(event, EventRemovedEvent):
            self._handle_event_removed(event)
        elif isinstance(event, AssumptionLayerUpdateEvent):
            self._handle_assumption_update(event)

    def _handle_time_change(self, event: TimeChangeEvent):
        """Handle time boundary changes."""
        logger.info(f"Time changed: {event.boundary_type} boundary crossed")

        if not self.assumption_engine:
            logger.warning("No assumption engine set, skipping time change processing")
            return

        # Trigger assumption layer recalculation based on boundary type
        if event.boundary_type in ["hour", "day"]:
            # Recalculate spending propensity for time-sensitive changes
            self.assumption_engine.recalculate_spending_propensity(event.new_time)

        if event.boundary_type in ["day", "week"]:
            # Recalculate college presence for schedule changes
            self.assumption_engine.recalculate_college_presence(event.new_time)

    def _handle_event_added(self, event: EventAddedEvent):
        """Handle new events being added."""
        logger.info(f"New event added from {event.source}")

        if not self.assumption_engine:
            return

        # Check if event affects college presence calculations
        if self._is_near_college_area(event.location):
            self.assumption_engine.recalculate_college_presence_for_area(
                event.location, event.event_data.get("start_time")
            )

    def _handle_event_removed(self, event: EventRemovedEvent):
        """Handle events being removed."""
        logger.info(f"Event removed from {event.source}: {event.reason}")

        if not self.assumption_engine:
            return

        # Recalculate affected areas
        if self._is_near_college_area(event.location):
            self.assumption_engine.recalculate_college_presence_for_area(event.location)

    def _handle_assumption_update(self, event: AssumptionLayerUpdateEvent):
        """Handle explicit assumption layer update requests."""
        logger.info(f"Assumption layer update requested: {event.layer_type}")

        if not self.assumption_engine:
            return

        if event.layer_type == "spending_propensity" or event.layer_type == "all":
            self.assumption_engine.recalculate_spending_propensity()

        if event.layer_type == "college_presence" or event.layer_type == "all":
            self.assumption_engine.recalculate_college_presence()

    def _is_near_college_area(self, location: Dict[str, float]) -> bool:
        """Check if location is near a college area."""
        # This would use the college hotspots from the assumption layer
        # For now, simplified implementation
        college_areas = [
            {"lat": 39.0334, "lon": -94.5760, "radius": 2.0},  # UMKC
            {"lat": 38.9584, "lon": -95.2448, "radius": 2.0},  # KU
        ]

        for area in college_areas:
            # Simple distance check (would use proper haversine in production)
            lat_diff = abs(location["lat"] - area["lat"])
            lon_diff = abs(location["lon"] - area["lon"])
            if lat_diff < 0.02 and lon_diff < 0.02:  # Rough 2km approximation
                return True
        return False


class GridUpdateHandler(BaseEventHandler):
    """Handler for grid updates."""

    def __init__(self):
        super().__init__("GridUpdateHandler")
        self.grid_manager = None

    def set_grid_manager(self, manager):
        """Set the grid manager."""
        self.grid_manager = manager

    def _process_event(self, event: BaseEvent):
        """Process grid update related events."""
        if isinstance(event, GridUpdateEvent):
            self._handle_grid_update(event)
        elif isinstance(event, EventAddedEvent):
            self._handle_event_added_for_grid(event)
        elif isinstance(event, EventRemovedEvent):
            self._handle_event_removed_for_grid(event)
        elif isinstance(event, AssumptionLayerUpdateEvent):
            self._handle_assumption_update_for_grid(event)

    def _handle_grid_update(self, event: GridUpdateEvent):
        """Handle explicit grid update requests."""
        logger.info(f"Grid update requested: {event.update_type}")

        if not self.grid_manager:
            logger.warning("No grid manager set, skipping grid update")
            return

        if event.update_type == "full":
            self.grid_manager.recalculate_all_cells()
        elif event.update_type == "incremental":
            self.grid_manager.update_cells(event.affected_cells)
        elif event.update_type == "assumption_only":
            self.grid_manager.update_assumption_scores(event.affected_cells)

    def _handle_event_added_for_grid(self, event: EventAddedEvent):
        """Handle new events for grid updates."""
        if not self.grid_manager:
            return

        # Add event to grid and update affected cells
        affected_cells = self.grid_manager.add_event_to_grid(
            event.event_data, event.location
        )

        # Trigger visualization update if needed
        if affected_cells:
            from .event_bus import publish_event
            from .event_types import VisualizationUpdateEvent
            import uuid

            viz_event = VisualizationUpdateEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(),
                visualization_type="heatmap",
                update_data={"affected_cells": affected_cells},
                real_time=True,
            )
            publish_event(viz_event)

    def _handle_event_removed_for_grid(self, event: EventRemovedEvent):
        """Handle removed events for grid updates."""
        if not self.grid_manager:
            return

        # Remove event from grid and update affected cells
        affected_cells = self.grid_manager.remove_event_from_grid(
            event.event_id, event.location
        )

        if affected_cells:
            from .event_bus import publish_event
            from .event_types import VisualizationUpdateEvent
            import uuid

            viz_event = VisualizationUpdateEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(),
                visualization_type="heatmap",
                update_data={"affected_cells": affected_cells},
                real_time=True,
            )
            publish_event(viz_event)

    def _handle_assumption_update_for_grid(self, event: AssumptionLayerUpdateEvent):
        """Handle assumption layer updates that affect grid."""
        if not self.grid_manager:
            return

        # Update grid cells with new assumption layer scores
        if event.affected_area:
            # Update specific area
            self.grid_manager.update_assumption_scores_for_area(event.affected_area)
        else:
            # Update all cells
            self.grid_manager.update_all_assumption_scores()


class VisualizationUpdateHandler(BaseEventHandler):
    """Handler for visualization updates."""

    def __init__(self):
        super().__init__("VisualizationUpdateHandler")
        self.visualization_manager = None
        self.websocket_manager = None

    def set_visualization_manager(self, manager):
        """Set the visualization manager."""
        self.visualization_manager = manager

    def set_websocket_manager(self, manager):
        """Set the WebSocket manager for real-time updates."""
        self.websocket_manager = manager

    def _process_event(self, event: BaseEvent):
        """Process visualization update events."""
        if isinstance(event, VisualizationUpdateEvent):
            self._handle_visualization_update(event)
        elif isinstance(event, GridUpdateEvent):
            self._handle_grid_update_for_viz(event)

    def _handle_visualization_update(self, event: VisualizationUpdateEvent):
        """Handle visualization update requests."""
        logger.info(f"Visualization update: {event.visualization_type}")

        if not self.visualization_manager:
            logger.warning("No visualization manager set, skipping update")
            return

        # Update visualizations based on type
        if event.visualization_type == "heatmap" or event.visualization_type == "all":
            self.visualization_manager.update_heatmap(event.update_data)

        if event.visualization_type == "grid" or event.visualization_type == "all":
            self.visualization_manager.update_grid_visualization(event.update_data)

        if event.visualization_type == "combined" or event.visualization_type == "all":
            self.visualization_manager.update_combined_visualization(event.update_data)

        # Send real-time updates via WebSocket if enabled
        if event.real_time and self.websocket_manager:
            self.websocket_manager.broadcast_update(
                {
                    "type": "visualization_update",
                    "visualization_type": event.visualization_type,
                    "data": event.update_data,
                    "timestamp": event.timestamp.isoformat(),
                }
            )

    def _handle_grid_update_for_viz(self, event: GridUpdateEvent):
        """Handle grid updates that require visualization refresh."""
        if not self.visualization_manager:
            return

        # Create visualization update data from grid update
        update_data = {
            "affected_cells": event.affected_cells,
            "update_type": event.update_type,
        }

        # Update relevant visualizations
        if event.update_type in ["full", "incremental"]:
            self.visualization_manager.update_heatmap(update_data)
            self.visualization_manager.update_grid_visualization(update_data)


class WebhookHandler(BaseEventHandler):
    """Handler for webhook events."""

    def __init__(self):
        super().__init__("WebhookHandler")
        self.webhook_processors = {}

    def register_webhook_processor(self, source: str, processor):
        """Register a processor for a specific webhook source."""
        self.webhook_processors[source] = processor

    def _process_event(self, event: BaseEvent):
        """Process webhook events."""
        if isinstance(event, WebhookReceivedEvent):
            self._handle_webhook(event)

    def _handle_webhook(self, event: WebhookReceivedEvent):
        """Handle incoming webhook events."""
        logger.info(f"Webhook received from {event.source}: {event.webhook_type}")

        processor = self.webhook_processors.get(event.source)
        if not processor:
            logger.warning(
                f"No processor registered for webhook source: {event.source}"
            )
            return

        try:
            # Process the webhook data
            processed_data = processor.process_webhook(
                event.webhook_type, event.webhook_data
            )

            # Generate appropriate events based on webhook type
            if event.webhook_type in ["event.created", "event.published"]:
                self._create_event_added_event(processed_data, event.source)
            elif event.webhook_type in ["event.updated", "event.changed"]:
                self._create_event_updated_event(processed_data, event.source)
            elif event.webhook_type in ["event.cancelled", "event.deleted"]:
                self._create_event_removed_event(processed_data, event.source)

        except Exception as e:
            logger.error(f"Error processing webhook from {event.source}: {e}")
            raise

    def _create_event_added_event(self, data: Dict[str, Any], source: str):
        """Create an EventAddedEvent from webhook data."""
        from .event_bus import publish_event
        from .event_types import EventAddedEvent
        import uuid

        event = EventAddedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            event_data=data,
            source=source,
            location=data.get("location", {"lat": 0.0, "lon": 0.0}),
        )
        publish_event(event)

    def _create_event_updated_event(self, data: Dict[str, Any], source: str):
        """Create an EventUpdatedEvent from webhook data."""
        from .event_bus import publish_event
        from .event_types import EventUpdatedEvent
        import uuid

        event = EventUpdatedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            updated_event_id=data.get("event_id", "unknown"),
            source=source,
            old_data=data.get("old_data", {}),
            new_data=data.get("new_data", {}),
            changed_fields=data.get("changed_fields", []),
        )
        publish_event(event)

    def _create_event_removed_event(self, data: Dict[str, Any], source: str):
        """Create an EventRemovedEvent from webhook data."""
        from .event_bus import publish_event
        from .event_types import EventRemovedEvent
        import uuid

        event = EventRemovedEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            source=source,
            location=data.get("location", {"lat": 0.0, "lon": 0.0}),
            reason=data.get("reason", "webhook_notification"),
        )
        publish_event(event)


# Handler registry for easy management
class EventHandlerRegistry:
    """Registry for managing event handlers."""

    def __init__(self):
        self.handlers = {}
        self.subscriptions = {}

    def register_handler(self, name: str, handler: BaseEventHandler):
        """Register an event handler."""
        self.handlers[name] = handler
        logger.info(f"Registered event handler: {name}")

    def get_handler(self, name: str) -> Optional[BaseEventHandler]:
        """Get a handler by name."""
        return self.handlers.get(name)

    def subscribe_handler_to_events(self, handler_name: str, event_types: List[str]):
        """Subscribe a handler to multiple event types."""
        handler = self.handlers.get(handler_name)
        if not handler:
            raise ValueError(f"Handler not found: {handler_name}")

        from .event_bus import subscribe_to_event

        for event_type in event_types:
            subscribe_to_event(event_type, handler.handle_event)

        self.subscriptions[handler_name] = event_types
        logger.info(f"Subscribed {handler_name} to events: {event_types}")

    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all handlers."""
        return {name: handler.get_stats() for name, handler in self.handlers.items()}

    def setup_default_handlers(self):
        """Set up the default event handlers."""
        # Register default handlers
        self.register_handler("assumption_layer", AssumptionLayerHandler())
        self.register_handler("grid_update", GridUpdateHandler())
        self.register_handler("visualization", VisualizationUpdateHandler())
        self.register_handler("webhook", WebhookHandler())

        # Subscribe handlers to relevant events
        self.subscribe_handler_to_events(
            "assumption_layer",
            [
                "time_change",
                "event_added",
                "event_removed",
                "assumption_layer_update",
            ],
        )

        self.subscribe_handler_to_events(
            "grid_update",
            [
                "grid_update",
                "event_added",
                "event_removed",
                "assumption_layer_update",
            ],
        )

        self.subscribe_handler_to_events(
            "visualization",
            [
                "visualization_update",
                "grid_update",
            ],
        )

        self.subscribe_handler_to_events(
            "webhook",
            [
                "webhook_received",
            ],
        )


# Global handler registry
_global_registry: Optional[EventHandlerRegistry] = None


def get_handler_registry() -> EventHandlerRegistry:
    """Get the global event handler registry."""
    global _global_registry

    if _global_registry is None:
        _global_registry = EventHandlerRegistry()
        _global_registry.setup_default_handlers()

    return _global_registry
