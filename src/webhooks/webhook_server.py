"""
FastAPI webhook server for receiving real-time updates from external APIs.
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel

from src.events.event_bus import get_event_bus
from src.events.event_types import WebhookReceivedEvent
from src.events.event_handlers import get_handler_registry

logger = logging.getLogger(__name__)


class WebhookPayload(BaseModel):
    """Base webhook payload model."""

    api_url: str
    config: Dict[str, Any]


class EventbriteWebhookPayload(BaseModel):
    """Eventbrite webhook payload model."""

    api_url: str
    config: Dict[str, Any]


class WebhookServer:
    """FastAPI server for handling webhooks."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8001):
        self.host = host
        self.port = port
        self.app = FastAPI(
            title="PDPE Webhook Server",
            description="Webhook endpoints for real-time event updates",
            version="1.0.0",
        )
        self.event_bus = get_event_bus()
        self.handler_registry = get_handler_registry()
        self.server = None
        self._setup_routes()

        # Statistics
        self.stats = {
            "webhooks_received": 0,
            "webhooks_processed": 0,
            "webhooks_failed": 0,
            "uptime_start": datetime.now(),
        }

    def _setup_routes(self):
        """Set up webhook routes."""

        @self.app.get("/")
        async def root():
            """Health check endpoint."""
            return {
                "status": "healthy",
                "service": "PDPE Webhook Server",
                "uptime": (datetime.now() - self.stats["uptime_start"]).total_seconds(),
                "stats": self.stats,
            }

        @self.app.get("/health")
        async def health_check():
            """Detailed health check."""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "event_bus_stats": self.event_bus.get_statistics(),
                "webhook_stats": self.stats,
            }

        @self.app.post("/webhooks/eventbrite")
        async def eventbrite_webhook(
            request: Request, background_tasks: BackgroundTasks
        ):
            """Handle Eventbrite webhooks."""
            return await self._handle_webhook(request, background_tasks, "eventbrite")

        @self.app.post("/webhooks/twitter")
        async def twitter_webhook(request: Request, background_tasks: BackgroundTasks):
            """Handle Twitter webhooks."""
            return await self._handle_webhook(request, background_tasks, "twitter")

        @self.app.post("/webhooks/ticketmaster")
        async def ticketmaster_webhook(
            request: Request, background_tasks: BackgroundTasks
        ):
            """Handle Ticketmaster webhooks."""
            return await self._handle_webhook(request, background_tasks, "ticketmaster")

        @self.app.post("/webhooks/generic/{source}")
        async def generic_webhook(
            source: str, request: Request, background_tasks: BackgroundTasks
        ):
            """Handle generic webhooks from any source."""
            return await self._handle_webhook(request, background_tasks, source)

    async def _handle_webhook(
        self, request: Request, background_tasks: BackgroundTasks, source: str
    ) -> JSONResponse:
        """Handle incoming webhook requests."""
        try:
            # Get request data
            headers = dict(request.headers)
            body = await request.body()

            # Try to parse JSON body
            try:
                json_data = await request.json()
            except Exception:
                json_data = {}

            # Log webhook receipt
            logger.info(f"Received webhook from {source}")
            self.stats["webhooks_received"] += 1

            # Create webhook event
            webhook_event = WebhookReceivedEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now(),
                source=source,
                webhook_data={
                    "headers": headers,
                    "body": body.decode("utf-8") if body else "",
                    "json": json_data,
                    "url": str(request.url),
                    "method": request.method,
                },
                webhook_type=self._determine_webhook_type(headers, json_data, source),
            )

            # Process webhook in background
            background_tasks.add_task(self._process_webhook_event, webhook_event)

            return JSONResponse(
                status_code=200,
                content={
                    "status": "received",
                    "event_id": webhook_event.event_id,
                    "timestamp": webhook_event.timestamp.isoformat(),
                },
            )

        except Exception as e:
            logger.error(f"Error handling webhook from {source}: {e}")
            self.stats["webhooks_failed"] += 1

            raise HTTPException(
                status_code=500, detail=f"Error processing webhook: {str(e)}"
            )

    async def _process_webhook_event(self, webhook_event: WebhookReceivedEvent):
        """Process webhook event asynchronously."""
        try:
            # Publish to event bus
            await self.event_bus.publish_async(webhook_event)
            self.stats["webhooks_processed"] += 1

            logger.debug(f"Processed webhook event: {webhook_event.event_id}")

        except Exception as e:
            logger.error(f"Error processing webhook event: {e}")
            self.stats["webhooks_failed"] += 1

    def _determine_webhook_type(
        self, headers: Dict[str, str], json_data: Dict[str, Any], source: str
    ) -> str:
        """Determine the type of webhook based on headers and data."""

        if source == "eventbrite":
            # Eventbrite webhook types
            if "x-eventbrite-event" in headers:
                return headers["x-eventbrite-event"]
            elif "action" in json_data:
                return f"event.{json_data['action']}"
            else:
                return "event.unknown"

        elif source == "twitter":
            # Twitter webhook types
            if "tweet_create_events" in json_data:
                return "tweet.created"
            elif "tweet_delete_events" in json_data:
                return "tweet.deleted"
            else:
                return "tweet.unknown"

        elif source == "ticketmaster":
            # Ticketmaster webhook types
            event_type = json_data.get("eventType", "unknown")
            return f"event.{event_type}"

        else:
            # Generic webhook type detection
            if "type" in json_data:
                return json_data["type"]
            elif "event" in json_data:
                return json_data["event"]
            elif "action" in json_data:
                return json_data["action"]
            else:
                return "webhook.generic"

    async def start(self):
        """Start the webhook server."""
        if self.server is not None:
            logger.warning("Webhook server is already running")
            return

        # Start event bus if not running
        if not self.event_bus._running:
            await self.event_bus.start()

        # Create server config
        config = uvicorn.Config(
            app=self.app,
            host=self.host,
            port=self.port,
            log_level="info",
            access_log=True,
        )

        self.server = uvicorn.Server(config)

        logger.info(f"Starting webhook server on {self.host}:{self.port}")

        # Start server in background task
        asyncio.create_task(self.server.serve())

        # Wait a moment for server to start
        await asyncio.sleep(1)

        logger.info("Webhook server started successfully")

    async def stop(self):
        """Stop the webhook server."""
        if self.server is None:
            logger.warning("Webhook server is not running")
            return

        logger.info("Stopping webhook server...")

        # Stop the server
        self.server.should_exit = True

        # Wait for server to stop
        await asyncio.sleep(1)

        self.server = None
        logger.info("Webhook server stopped")

    def get_stats(self) -> Dict[str, Any]:
        """Get webhook server statistics."""
        stats = self.stats.copy()
        stats["uptime_seconds"] = (
            datetime.now() - stats["uptime_start"]
        ).total_seconds()
        stats["uptime_start"] = stats["uptime_start"].isoformat()
        return stats


# Global webhook server instance
_global_webhook_server: Optional[WebhookServer] = None


def get_webhook_server(host: str = "0.0.0.0", port: int = 8001) -> WebhookServer:
    """Get the global webhook server instance."""
    global _global_webhook_server

    if _global_webhook_server is None:
        _global_webhook_server = WebhookServer(host=host, port=port)

    return _global_webhook_server


async def start_webhook_server(host: str = "0.0.0.0", port: int = 8001):
    """Start the webhook server."""
    server = get_webhook_server(host, port)
    await server.start()
    return server


async def stop_webhook_server():
    """Stop the webhook server."""
    global _global_webhook_server

    if _global_webhook_server:
        await _global_webhook_server.stop()
        _global_webhook_server = None


# Standalone server runner
async def run_webhook_server(host: str = "0.0.0.0", port: int = 8001):
    """Run the webhook server as a standalone application."""
    server = await start_webhook_server(host, port)

    try:
        # Keep server running
        while server.server and not server.server.should_exit:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await stop_webhook_server()


if __name__ == "__main__":
    # Run server directly
    asyncio.run(run_webhook_server())
