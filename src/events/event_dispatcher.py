"""
Event dispatcher for coordinating event flow in the PDPE system.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable

from .event_bus import (
    get_event_bus,
    create_time_change_event,
    create_assumption_layer_update_event,
)
from .event_types import EventPriority

logger = logging.getLogger(__name__)


class EventDispatcher:
    """
    Coordinates event dispatching and manages time-based events.
    """

    def __init__(self):
        self.event_bus = get_event_bus()
        self._running = False
        self._time_monitor_task: Optional[asyncio.Task] = None
        self._last_hour = None
        self._last_day = None
        self._last_week = None
        self._last_month = None
        self._monitor_interval = 60  # Check every minute

        # Event scheduling
        self._scheduled_events = []
        self._scheduler_task: Optional[asyncio.Task] = None

        # Statistics
        self._stats = {
            "time_events_dispatched": 0,
            "scheduled_events_dispatched": 0,
            "errors": 0,
        }

    async def start(self):
        """Start the event dispatcher."""
        if self._running:
            return

        self._running = True
        await self.event_bus.start()

        # Initialize time tracking
        now = datetime.now()
        self._last_hour = now.hour
        self._last_day = now.day
        self._last_week = now.isocalendar()[1]
        self._last_month = now.month

        # Start monitoring tasks
        self._time_monitor_task = asyncio.create_task(self._monitor_time_changes())
        self._scheduler_task = asyncio.create_task(self._process_scheduled_events())

        logger.info("Event dispatcher started")

    async def stop(self):
        """Stop the event dispatcher."""
        if not self._running:
            return

        self._running = False

        # Cancel monitoring tasks
        if self._time_monitor_task:
            self._time_monitor_task.cancel()
            try:
                await self._time_monitor_task
            except asyncio.CancelledError:
                pass

        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        await self.event_bus.stop()
        logger.info("Event dispatcher stopped")

    async def _monitor_time_changes(self):
        """Monitor for time boundary changes and dispatch events."""
        while self._running:
            try:
                now = datetime.now()
                previous_time = now - timedelta(seconds=self._monitor_interval)

                # Check for hour boundary
                if now.hour != self._last_hour:
                    await self._dispatch_time_change_event(previous_time, now, "hour")
                    self._last_hour = now.hour

                # Check for day boundary
                if now.day != self._last_day:
                    await self._dispatch_time_change_event(previous_time, now, "day")
                    self._last_day = now.day

                # Check for week boundary
                current_week = now.isocalendar()[1]
                if current_week != self._last_week:
                    await self._dispatch_time_change_event(previous_time, now, "week")
                    self._last_week = current_week

                # Check for month boundary
                if now.month != self._last_month:
                    await self._dispatch_time_change_event(previous_time, now, "month")
                    self._last_month = now.month

                await asyncio.sleep(self._monitor_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in time monitoring: {e}")
                await asyncio.sleep(self._monitor_interval)

    async def _dispatch_time_change_event(
        self, previous_time: datetime, new_time: datetime, boundary_type: str
    ):
        """Dispatch a time change event."""
        try:
            event = create_time_change_event(previous_time, new_time, boundary_type)
            event.priority = EventPriority.HIGH

            await self.event_bus.publish_async(event)
            self._stats["time_events_dispatched"] += 1

            logger.info(f"Dispatched time change event: {boundary_type}")

            # Also trigger assumption layer updates for significant time changes
            if boundary_type in ["hour", "day"]:
                assumption_event = create_assumption_layer_update_event(
                    "all", f"{boundary_type}_change"
                )
                await self.event_bus.publish_async(assumption_event)

        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Error dispatching time change event: {e}")

    def schedule_event(self, event, dispatch_time: datetime):
        """Schedule an event to be dispatched at a specific time."""
        self._scheduled_events.append(
            {
                "event": event,
                "dispatch_time": dispatch_time,
                "scheduled_at": datetime.now(),
            }
        )

        # Sort by dispatch time
        self._scheduled_events.sort(key=lambda x: x["dispatch_time"])

        logger.debug(f"Scheduled event {event.get_event_type()} for {dispatch_time}")

    def schedule_recurring_event(
        self,
        event_factory: Callable,
        interval: timedelta,
        start_time: Optional[datetime] = None,
    ):
        """Schedule a recurring event."""
        if start_time is None:
            start_time = datetime.now() + interval

        # For now, just schedule the next occurrence
        # In a full implementation, this would set up a recurring schedule
        next_event = event_factory()
        self.schedule_event(next_event, start_time)

    async def _process_scheduled_events(self):
        """Process scheduled events."""
        while self._running:
            try:
                now = datetime.now()

                # Process all events that are due
                events_to_dispatch = []
                remaining_events = []

                for scheduled_event in self._scheduled_events:
                    if scheduled_event["dispatch_time"] <= now:
                        events_to_dispatch.append(scheduled_event)
                    else:
                        remaining_events.append(scheduled_event)

                self._scheduled_events = remaining_events

                # Dispatch due events
                for scheduled_event in events_to_dispatch:
                    try:
                        await self.event_bus.publish_async(scheduled_event["event"])
                        self._stats["scheduled_events_dispatched"] += 1
                        logger.debug(
                            f"Dispatched scheduled event: {scheduled_event['event'].get_event_type()}"
                        )
                    except Exception as e:
                        self._stats["errors"] += 1
                        logger.error(f"Error dispatching scheduled event: {e}")

                # Sleep for a short interval
                await asyncio.sleep(10)  # Check every 10 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Error in scheduled event processing: {e}")
                await asyncio.sleep(10)

    def dispatch_event_now(self, event):
        """Dispatch an event immediately (synchronous)."""
        try:
            self.event_bus.publish(event)
            logger.debug(f"Dispatched immediate event: {event.get_event_type()}")
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Error dispatching immediate event: {e}")
            raise

    async def dispatch_event_async(self, event):
        """Dispatch an event immediately (asynchronous)."""
        try:
            await self.event_bus.publish_async(event)
            logger.debug(f"Dispatched async event: {event.get_event_type()}")
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Error dispatching async event: {e}")
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """Get dispatcher statistics."""
        stats = self._stats.copy()
        stats.update(
            {
                "running": self._running,
                "scheduled_events_pending": len(self._scheduled_events),
                "event_bus_stats": self.event_bus.get_statistics(),
            }
        )
        return stats

    def get_scheduled_events(self) -> list:
        """Get list of scheduled events."""
        return [
            {
                "event_type": se["event"].get_event_type(),
                "dispatch_time": se["dispatch_time"].isoformat(),
                "scheduled_at": se["scheduled_at"].isoformat(),
            }
            for se in self._scheduled_events
        ]

    def clear_scheduled_events(self):
        """Clear all scheduled events."""
        self._scheduled_events.clear()
        logger.info("Cleared all scheduled events")

    def set_monitor_interval(self, seconds: int):
        """Set the time monitoring interval."""
        if seconds < 1:
            raise ValueError("Monitor interval must be at least 1 second")
        self._monitor_interval = seconds
        logger.info(f"Set monitor interval to {seconds} seconds")


class TimeBasedEventScheduler:
    """Helper class for scheduling time-based events."""

    def __init__(self, dispatcher: EventDispatcher):
        self.dispatcher = dispatcher

    def schedule_daily_assumption_update(self, hour: int = 0, minute: int = 0):
        """Schedule daily assumption layer updates."""

        def create_daily_update():
            return create_assumption_layer_update_event("all", "daily_schedule")

        # Calculate next occurrence
        now = datetime.now()
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if next_run <= now:
            next_run += timedelta(days=1)

        self.dispatcher.schedule_recurring_event(
            create_daily_update, timedelta(days=1), next_run
        )

        logger.info(f"Scheduled daily assumption updates at {hour:02d}:{minute:02d}")

    def schedule_hourly_spending_update(self):
        """Schedule hourly spending propensity updates."""

        def create_hourly_update():
            return create_assumption_layer_update_event(
                "spending_propensity", "hourly_schedule"
            )

        # Schedule for the next hour
        now = datetime.now()
        next_run = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        self.dispatcher.schedule_recurring_event(
            create_hourly_update, timedelta(hours=1), next_run
        )

        logger.info("Scheduled hourly spending propensity updates")

    def schedule_weekend_college_update(self):
        """Schedule weekend college presence updates."""

        def create_weekend_update():
            return create_assumption_layer_update_event(
                "college_presence", "weekend_schedule"
            )

        # Calculate next weekend (Saturday)
        now = datetime.now()
        days_until_saturday = (5 - now.weekday()) % 7
        if (
            days_until_saturday == 0 and now.hour >= 18
        ):  # If it's Saturday evening, schedule for next week
            days_until_saturday = 7

        next_saturday = now + timedelta(days=days_until_saturday)
        next_saturday = next_saturday.replace(
            hour=18, minute=0, second=0, microsecond=0
        )

        self.dispatcher.schedule_event(create_weekend_update(), next_saturday)

        logger.info(f"Scheduled weekend college update for {next_saturday}")


# Global dispatcher instance
_global_dispatcher: Optional[EventDispatcher] = None
_dispatcher_lock = threading.Lock()


def get_event_dispatcher() -> EventDispatcher:
    """Get the global event dispatcher instance."""
    global _global_dispatcher

    if _global_dispatcher is None:
        with _dispatcher_lock:
            if _global_dispatcher is None:
                _global_dispatcher = EventDispatcher()

    return _global_dispatcher


def get_time_scheduler() -> TimeBasedEventScheduler:
    """Get a time-based event scheduler."""
    dispatcher = get_event_dispatcher()
    return TimeBasedEventScheduler(dispatcher)


async def start_event_system():
    """Start the complete event system."""
    dispatcher = get_event_dispatcher()
    await dispatcher.start()

    # Set up default time-based schedules
    scheduler = get_time_scheduler()
    scheduler.schedule_daily_assumption_update(hour=1)  # 1 AM daily updates
    scheduler.schedule_hourly_spending_update()

    logger.info("Event system started with default schedules")


async def stop_event_system():
    """Stop the complete event system."""
    dispatcher = get_event_dispatcher()
    await dispatcher.stop()
    logger.info("Event system stopped")
