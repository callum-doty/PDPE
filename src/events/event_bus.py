"""
Event bus implementation for the PDPE event-driven architecture.
"""

import asyncio
import logging
import threading
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from .event_types import BaseEvent, EventPriority


logger = logging.getLogger(__name__)


class EventBus:
    """
    Central event bus for publishing and subscribing to events.
    Supports both synchronous and asynchronous event handling.
    """

    def __init__(self, max_workers: int = 4):
        """
        Initialize the event bus.

        Args:
            max_workers: Maximum number of worker threads for async processing
        """
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._async_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_queue: asyncio.Queue = None
        self._processing_task: Optional[asyncio.Task] = None
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.RLock()
        self._running = False
        self._event_history: List[BaseEvent] = []
        self._max_history = 1000

        # Statistics
        self._stats = {
            "events_published": 0,
            "events_processed": 0,
            "processing_errors": 0,
            "subscribers_count": 0,
        }

    async def start(self):
        """Start the event bus processing loop."""
        if self._running:
            return

        self._running = True
        self._event_queue = asyncio.Queue()
        self._processing_task = asyncio.create_task(self._process_events())
        logger.info("Event bus started")

    async def stop(self):
        """Stop the event bus and cleanup resources."""
        if not self._running:
            return

        self._running = False

        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        if self._event_queue:
            # Process remaining events
            while not self._event_queue.empty():
                try:
                    event = self._event_queue.get_nowait()
                    await self._handle_event(event)
                except asyncio.QueueEmpty:
                    break

        self._executor.shutdown(wait=True)
        logger.info("Event bus stopped")

    def subscribe(self, event_type: str, handler: Callable[[BaseEvent], None]):
        """
        Subscribe to events of a specific type (synchronous handler).

        Args:
            event_type: Type of event to subscribe to
            handler: Function to call when event is received
        """
        with self._lock:
            self._subscribers[event_type].append(handler)
            self._stats["subscribers_count"] += 1
            logger.debug(f"Subscribed to {event_type} events")

    def subscribe_async(self, event_type: str, handler: Callable[[BaseEvent], None]):
        """
        Subscribe to events of a specific type (asynchronous handler).

        Args:
            event_type: Type of event to subscribe to
            handler: Async function to call when event is received
        """
        with self._lock:
            self._async_subscribers[event_type].append(handler)
            self._stats["subscribers_count"] += 1
            logger.debug(f"Subscribed to {event_type} events (async)")

    def unsubscribe(self, event_type: str, handler: Callable):
        """
        Unsubscribe from events of a specific type.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler function to remove
        """
        with self._lock:
            if handler in self._subscribers[event_type]:
                self._subscribers[event_type].remove(handler)
                self._stats["subscribers_count"] -= 1

            if handler in self._async_subscribers[event_type]:
                self._async_subscribers[event_type].remove(handler)
                self._stats["subscribers_count"] -= 1

            logger.debug(f"Unsubscribed from {event_type} events")

    def publish(self, event: BaseEvent):
        """
        Publish an event to all subscribers (synchronous).

        Args:
            event: Event to publish
        """
        with self._lock:
            self._stats["events_published"] += 1
            self._add_to_history(event)

        # Handle synchronous subscribers immediately
        event_type = event.get_event_type()
        sync_handlers = self._subscribers.get(event_type, [])

        for handler in sync_handlers:
            try:
                handler(event)
                self._stats["events_processed"] += 1
            except Exception as e:
                self._stats["processing_errors"] += 1
                logger.error(f"Error in sync handler for {event_type}: {e}")

        # Queue for async processing if we have async subscribers
        async_handlers = self._async_subscribers.get(event_type, [])
        if async_handlers and self._event_queue:
            try:
                self._event_queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning(f"Event queue full, dropping event: {event_type}")

    async def publish_async(self, event: BaseEvent):
        """
        Publish an event asynchronously.

        Args:
            event: Event to publish
        """
        if not self._running:
            await self.start()

        await self._event_queue.put(event)

        with self._lock:
            self._stats["events_published"] += 1
            self._add_to_history(event)

    async def _process_events(self):
        """Main event processing loop for async events."""
        while self._running:
            try:
                # Wait for events with timeout to allow checking _running flag
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._handle_event(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processing loop: {e}")

    async def _handle_event(self, event: BaseEvent):
        """Handle a single event by calling all async subscribers."""
        event_type = event.get_event_type()
        handlers = self._async_subscribers.get(event_type, [])

        if not handlers:
            return

        # Create tasks for all handlers
        tasks = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(asyncio.create_task(handler(event)))
            else:
                # Run sync handler in executor
                tasks.append(
                    asyncio.create_task(
                        asyncio.get_event_loop().run_in_executor(
                            self._executor, handler, event
                        )
                    )
                )

        # Wait for all handlers to complete
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self._stats["processing_errors"] += 1
                    logger.error(f"Error in async handler for {event_type}: {result}")
                else:
                    self._stats["events_processed"] += 1

    def _add_to_history(self, event: BaseEvent):
        """Add event to history, maintaining max size."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

    def get_event_history(
        self, event_type: Optional[str] = None, limit: int = 100
    ) -> List[BaseEvent]:
        """
        Get recent event history.

        Args:
            event_type: Filter by event type (optional)
            limit: Maximum number of events to return

        Returns:
            List of recent events
        """
        with self._lock:
            events = self._event_history

            if event_type:
                events = [e for e in events if e.get_event_type() == event_type]

            return events[-limit:]

    def get_statistics(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            stats = self._stats.copy()
            stats["queue_size"] = self._event_queue.qsize() if self._event_queue else 0
            stats["running"] = self._running
            stats["subscriber_types"] = {
                event_type: len(handlers)
                for event_type, handlers in self._subscribers.items()
            }
            stats["async_subscriber_types"] = {
                event_type: len(handlers)
                for event_type, handlers in self._async_subscribers.items()
            }
            return stats

    def clear_history(self):
        """Clear event history."""
        with self._lock:
            self._event_history.clear()

    def get_subscribers(self, event_type: str) -> Dict[str, int]:
        """Get subscriber count for an event type."""
        with self._lock:
            return {
                "sync": len(self._subscribers.get(event_type, [])),
                "async": len(self._async_subscribers.get(event_type, [])),
            }


# Global event bus instance
_global_event_bus: Optional[EventBus] = None
_bus_lock = threading.Lock()


def get_event_bus() -> EventBus:
    """Get the global event bus instance (singleton pattern)."""
    global _global_event_bus

    if _global_event_bus is None:
        with _bus_lock:
            if _global_event_bus is None:
                _global_event_bus = EventBus()

    return _global_event_bus


def reset_event_bus():
    """Reset the global event bus (mainly for testing)."""
    global _global_event_bus

    with _bus_lock:
        if _global_event_bus and _global_event_bus._running:
            # Note: This is sync, so we can't await stop()
            # In production, proper shutdown should be handled elsewhere
            _global_event_bus._running = False

        _global_event_bus = None


# Convenience functions for common operations
def publish_event(event: BaseEvent):
    """Publish an event using the global event bus."""
    bus = get_event_bus()
    bus.publish(event)


async def publish_event_async(event: BaseEvent):
    """Publish an event asynchronously using the global event bus."""
    bus = get_event_bus()
    await bus.publish_async(event)


def subscribe_to_event(event_type: str, handler: Callable[[BaseEvent], None]):
    """Subscribe to an event type using the global event bus."""
    bus = get_event_bus()
    bus.subscribe(event_type, handler)


def subscribe_to_event_async(event_type: str, handler: Callable[[BaseEvent], None]):
    """Subscribe to an event type asynchronously using the global event bus."""
    bus = get_event_bus()
    bus.subscribe_async(event_type, handler)


# Event creation helpers
def create_time_change_event(
    previous_time: datetime, new_time: datetime, boundary_type: str
) -> BaseEvent:
    """Create a time change event."""
    from .event_types import TimeChangeEvent

    return TimeChangeEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        previous_time=previous_time,
        new_time=new_time,
        boundary_type=boundary_type,
    )


def create_event_added_event(
    event_data: Dict[str, Any], source: str, location: Dict[str, float]
) -> BaseEvent:
    """Create an event added event."""
    from .event_types import EventAddedEvent

    return EventAddedEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        event_data=event_data,
        source=source,
        location=location,
    )


def create_assumption_layer_update_event(
    layer_type: str, reason: str = "time_change"
) -> BaseEvent:
    """Create an assumption layer update event."""
    from .event_types import AssumptionLayerUpdateEvent

    return AssumptionLayerUpdateEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(),
        layer_type=layer_type,
        recalculation_reason=reason,
    )
