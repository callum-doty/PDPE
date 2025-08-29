"""
Event-driven main application for the PDPE system.
This integrates the event system, assumption engine, webhooks, and real-time processing.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Optional

from src.events.event_dispatcher import (
    start_event_system,
    stop_event_system,
    get_event_dispatcher,
)
from src.events.event_handlers import get_handler_registry
from src.processing.assumption_engine import get_assumption_engine
from src.webhooks.webhook_server import start_webhook_server, stop_webhook_server
from src.webhooks.eventbrite_webhook import EventbriteWebhookProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pdpe_event_driven.log"),
    ],
)

logger = logging.getLogger(__name__)


class EventDrivenPDPE:
    """Main event-driven PDPE application."""

    def __init__(self):
        self.running = False
        self.webhook_server = None
        self.event_dispatcher = None
        self.assumption_engine = None
        self.handler_registry = None

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.stop())

    async def start(self):
        """Start the event-driven PDPE system."""
        if self.running:
            logger.warning("PDPE system is already running")
            return

        logger.info("Starting Event-Driven PDPE System...")

        try:
            # 1. Start the event system (bus, dispatcher, handlers)
            logger.info("Starting event system...")
            await start_event_system()
            self.event_dispatcher = get_event_dispatcher()

            # 2. Initialize assumption engine
            logger.info("Initializing assumption engine...")
            self.assumption_engine = get_assumption_engine()

            # 3. Setup event handlers with dependencies
            logger.info("Setting up event handlers...")
            self.handler_registry = get_handler_registry()

            # Connect assumption engine to handlers
            assumption_handler = self.handler_registry.get_handler("assumption_layer")
            if assumption_handler:
                assumption_handler.set_assumption_engine(self.assumption_engine)

            # Setup webhook processors
            webhook_handler = self.handler_registry.get_handler("webhook")
            if webhook_handler:
                eventbrite_processor = EventbriteWebhookProcessor()
                webhook_handler.register_webhook_processor(
                    "eventbrite", eventbrite_processor
                )

            # 4. Start webhook server
            logger.info("Starting webhook server...")
            self.webhook_server = await start_webhook_server(host="0.0.0.0", port=8001)

            # 5. Warm up caches with common locations (Kansas City area)
            logger.info("Warming up assumption layer caches...")
            kc_locations = [
                (39.0997, -94.5786),  # Downtown KC
                (39.0334, -94.5760),  # UMKC
                (39.0917, -94.5833),  # Crossroads
                (39.1000, -94.5833),  # Power & Light
                (39.0528, -94.5958),  # Westport
            ]

            time_range = (datetime.now(), datetime.now().replace(hour=23, minute=59))
            self.assumption_engine.warmup_cache(kc_locations, time_range)

            self.running = True
            logger.info("✅ Event-Driven PDPE System started successfully!")

            # Print system status
            await self._print_system_status()

        except Exception as e:
            logger.error(f"Failed to start PDPE system: {e}")
            await self.stop()
            raise

    async def stop(self):
        """Stop the event-driven PDPE system."""
        if not self.running:
            return

        logger.info("Stopping Event-Driven PDPE System...")

        try:
            # Stop webhook server
            if self.webhook_server:
                logger.info("Stopping webhook server...")
                await stop_webhook_server()
                self.webhook_server = None

            # Stop event system
            logger.info("Stopping event system...")
            await stop_event_system()

            # Clear caches
            if self.assumption_engine:
                logger.info("Clearing assumption engine caches...")
                self.assumption_engine.clear_all_caches()

            self.running = False
            logger.info("✅ Event-Driven PDPE System stopped successfully!")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    async def _print_system_status(self):
        """Print current system status."""
        print("\n" + "=" * 60)
        print("🚀 EVENT-DRIVEN PDPE SYSTEM STATUS")
        print("=" * 60)

        # Event system status
        if self.event_dispatcher:
            dispatcher_stats = self.event_dispatcher.get_statistics()
            print(
                f"📡 Event Dispatcher: {'✅ Running' if dispatcher_stats['running'] else '❌ Stopped'}"
            )
            print(
                f"   - Events dispatched: {dispatcher_stats['time_events_dispatched']}"
            )
            print(
                f"   - Scheduled events: {dispatcher_stats['scheduled_events_pending']}"
            )

        # Assumption engine status
        if self.assumption_engine:
            engine_stats = self.assumption_engine.get_statistics()
            print(f"🧠 Assumption Engine: ✅ Ready")
            print(f"   - Cache hits: {engine_stats['cache_hits']}")
            print(f"   - Cache misses: {engine_stats['cache_misses']}")
            print(
                f"   - Calculations: {engine_stats['spending_calculations'] + engine_stats['college_calculations']}"
            )

        # Webhook server status
        if self.webhook_server:
            webhook_stats = self.webhook_server.get_stats()
            print(f"🔗 Webhook Server: ✅ Running on port 8001")
            print(f"   - Webhooks received: {webhook_stats['webhooks_received']}")
            print(f"   - Webhooks processed: {webhook_stats['webhooks_processed']}")

        # Handler registry status
        if self.handler_registry:
            handler_stats = self.handler_registry.get_all_stats()
            print(f"⚡ Event Handlers: ✅ {len(handler_stats)} handlers registered")
            for name, stats in handler_stats.items():
                print(f"   - {name}: {stats['processed_events']} events processed")

        print("\n📋 AVAILABLE ENDPOINTS:")
        print("   - Health Check: http://localhost:8001/health")
        print("   - Eventbrite Webhook: http://localhost:8001/webhooks/eventbrite")
        print("   - Generic Webhook: http://localhost:8001/webhooks/generic/{source}")

        print("\n🔄 REAL-TIME FEATURES:")
        print("   - ✅ Time-based assumption layer updates")
        print("   - ✅ Event-driven cache invalidation")
        print("   - ✅ Webhook processing for real-time events")
        print("   - ✅ Smart caching with TTL and dependencies")

        print("=" * 60)
        print("System ready for real-time event processing! 🎉")
        print("=" * 60 + "\n")

    async def run_demo(self):
        """Run a demonstration of the event-driven system."""
        logger.info("Running event-driven system demonstration...")

        # Simulate some events
        from src.events.event_bus import get_event_bus, create_event_added_event
        from src.events.event_types import AssumptionLayerUpdateEvent
        import uuid

        event_bus = get_event_bus()

        # 1. Simulate a new event being added
        logger.info("Simulating new event addition...")
        new_event = create_event_added_event(
            event_data={
                "id": "demo_event_123",
                "name": "Demo Tech Meetup",
                "start_time": datetime.now().replace(hour=19).isoformat(),
                "category": "tech",
                "tags": ["networking", "demo"],
            },
            source="demo",
            location={"lat": 39.0997, "lon": -94.5786},
        )

        await event_bus.publish_async(new_event)
        await asyncio.sleep(2)  # Let the event propagate

        # 2. Trigger assumption layer update
        logger.info("Triggering assumption layer update...")
        assumption_event = AssumptionLayerUpdateEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            layer_type="all",
            recalculation_reason="demo_trigger",
        )

        await event_bus.publish_async(assumption_event)
        await asyncio.sleep(2)

        # 3. Show updated statistics
        await self._print_system_status()

        logger.info("Demo completed!")

    async def run_forever(self):
        """Run the system indefinitely."""
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        finally:
            await self.stop()


async def main():
    """Main entry point for the event-driven PDPE system."""
    app = EventDrivenPDPE()

    try:
        # Start the system
        await app.start()

        # Run demo if requested
        if len(sys.argv) > 1 and sys.argv[1] == "--demo":
            await app.run_demo()
            await asyncio.sleep(5)  # Keep running for a bit after demo
        else:
            # Run indefinitely
            await app.run_forever()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await app.stop()


if __name__ == "__main__":
    # Run the event-driven PDPE system
    asyncio.run(main())
