"""
Eventbrite webhook processor for handling real-time event updates.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from src.models.database import Event, Location

logger = logging.getLogger(__name__)


class EventbriteWebhookProcessor:
    """Processes Eventbrite webhook events and converts them to internal format."""

    def __init__(self):
        self.supported_events = {
            "event.created",
            "event.published",
            "event.updated",
            "event.unpublished",
            "event.cancelled",
            "event.deleted",
            "order.placed",
            "attendee.updated",
        }

    def process_webhook(
        self, webhook_type: str, webhook_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process an Eventbrite webhook and return standardized event data.

        Args:
            webhook_type: Type of webhook (e.g., 'event.created')
            webhook_data: Raw webhook data from Eventbrite

        Returns:
            Standardized event data dictionary
        """
        logger.info(f"Processing Eventbrite webhook: {webhook_type}")

        if webhook_type not in self.supported_events:
            logger.warning(f"Unsupported Eventbrite webhook type: {webhook_type}")
            return self._create_unknown_event_data(webhook_type, webhook_data)

        try:
            # Extract the main event data from webhook
            json_data = webhook_data.get("json", {})

            if webhook_type in ["event.created", "event.published", "event.updated"]:
                return self._process_event_webhook(webhook_type, json_data)
            elif webhook_type in [
                "event.unpublished",
                "event.cancelled",
                "event.deleted",
            ]:
                return self._process_event_removal_webhook(webhook_type, json_data)
            elif webhook_type == "order.placed":
                return self._process_order_webhook(webhook_type, json_data)
            elif webhook_type == "attendee.updated":
                return self._process_attendee_webhook(webhook_type, json_data)
            else:
                return self._create_unknown_event_data(webhook_type, webhook_data)

        except Exception as e:
            logger.error(f"Error processing Eventbrite webhook {webhook_type}: {e}")
            return self._create_error_event_data(webhook_type, webhook_data, str(e))

    def _process_event_webhook(
        self, webhook_type: str, json_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process event creation/update webhooks."""

        # Eventbrite webhook structure varies, but typically contains:
        # - api_url: URL to fetch full event data
        # - config: Configuration data including event ID

        api_url = json_data.get("api_url", "")
        config = json_data.get("config", {})

        # Extract event ID from various possible locations
        event_id = self._extract_event_id(json_data)

        if not event_id:
            logger.warning("No event ID found in Eventbrite webhook")
            return self._create_unknown_event_data(webhook_type, json_data)

        # For real implementation, you would fetch full event data from api_url
        # For now, we'll create a mock event based on available data
        event_data = self._create_mock_event_data(event_id, config, api_url)

        return {
            "id": event_id,
            "event_data": event_data,
            "webhook_type": webhook_type,
            "api_url": api_url,
            "config": config,
            "location": {
                "lat": event_data.get("latitude", 39.0997),  # Default to KC
                "lon": event_data.get("longitude", -94.5786),
            },
            "processed_at": datetime.now().isoformat(),
        }

    def _process_event_removal_webhook(
        self, webhook_type: str, json_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process event removal/cancellation webhooks."""

        event_id = self._extract_event_id(json_data)
        config = json_data.get("config", {})

        reason_map = {
            "event.unpublished": "unpublished",
            "event.cancelled": "cancelled",
            "event.deleted": "deleted",
        }

        return {
            "id": event_id,
            "event_id": event_id,
            "webhook_type": webhook_type,
            "reason": reason_map.get(webhook_type, "unknown"),
            "config": config,
            "location": {
                "lat": 39.0997,  # Default location
                "lon": -94.5786,
            },
            "processed_at": datetime.now().isoformat(),
        }

    def _process_order_webhook(
        self, webhook_type: str, json_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process order placement webhooks (for attendance tracking)."""

        config = json_data.get("config", {})
        event_id = config.get("event_id")
        order_id = config.get("order_id")

        return {
            "id": f"order_{order_id}",
            "event_id": event_id,
            "order_id": order_id,
            "webhook_type": webhook_type,
            "config": config,
            "processed_at": datetime.now().isoformat(),
        }

    def _process_attendee_webhook(
        self, webhook_type: str, json_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process attendee update webhooks."""

        config = json_data.get("config", {})
        event_id = config.get("event_id")
        attendee_id = config.get("attendee_id")

        return {
            "id": f"attendee_{attendee_id}",
            "event_id": event_id,
            "attendee_id": attendee_id,
            "webhook_type": webhook_type,
            "config": config,
            "processed_at": datetime.now().isoformat(),
        }

    def _extract_event_id(self, json_data: Dict[str, Any]) -> Optional[str]:
        """Extract event ID from various possible locations in webhook data."""

        # Try different possible locations for event ID
        config = json_data.get("config", {})

        # Common locations for event ID
        possible_keys = [
            "event_id",
            "id",
            "object_id",
            "resource_id",
        ]

        # Check config first
        for key in possible_keys:
            if key in config and config[key]:
                return str(config[key])

        # Check top level
        for key in possible_keys:
            if key in json_data and json_data[key]:
                return str(json_data[key])

        # Try to extract from API URL
        api_url = json_data.get("api_url", "")
        if "/events/" in api_url:
            try:
                # Extract event ID from URL like: https://www.eventbriteapi.com/v3/events/123456789/
                parts = api_url.split("/events/")
                if len(parts) > 1:
                    event_id = parts[1].split("/")[0].split("?")[0]
                    if event_id.isdigit():
                        return event_id
            except Exception:
                pass

        return None

    def _create_mock_event_data(
        self, event_id: str, config: Dict[str, Any], api_url: str
    ) -> Dict[str, Any]:
        """Create mock event data for demonstration purposes."""

        # In a real implementation, you would fetch full event data from the API URL
        # For now, create realistic mock data

        now = datetime.now()

        return {
            "id": event_id,
            "name": f"Event {event_id}",
            "description": f"Event received via webhook from Eventbrite API",
            "start_time": (now.replace(hour=19, minute=0, second=0)).isoformat(),
            "end_time": (now.replace(hour=22, minute=0, second=0)).isoformat(),
            "category": "business",
            "tags": ["networking", "webhook"],
            "venue": {
                "name": "Webhook Event Venue",
                "address": "123 Webhook St, Kansas City, MO",
                "latitude": 39.0997,
                "longitude": -94.5786,
            },
            "url": api_url,
            "source": "eventbrite_webhook",
            "webhook_config": config,
        }

    def _create_unknown_event_data(
        self, webhook_type: str, webhook_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create data for unknown webhook types."""

        return {
            "id": f"unknown_{datetime.now().timestamp()}",
            "webhook_type": webhook_type,
            "webhook_data": webhook_data,
            "status": "unknown_webhook_type",
            "processed_at": datetime.now().isoformat(),
        }

    def _create_error_event_data(
        self, webhook_type: str, webhook_data: Dict[str, Any], error: str
    ) -> Dict[str, Any]:
        """Create data for webhook processing errors."""

        return {
            "id": f"error_{datetime.now().timestamp()}",
            "webhook_type": webhook_type,
            "webhook_data": webhook_data,
            "error": error,
            "status": "processing_error",
            "processed_at": datetime.now().isoformat(),
        }

    def is_supported_webhook(self, webhook_type: str) -> bool:
        """Check if a webhook type is supported."""
        return webhook_type in self.supported_events

    def get_supported_events(self) -> set:
        """Get the set of supported webhook event types."""
        return self.supported_events.copy()


# Example webhook payloads for testing
EXAMPLE_WEBHOOKS = {
    "event_created": {
        "api_url": "https://www.eventbriteapi.com/v3/events/123456789/",
        "config": {
            "event_id": "123456789",
            "user_id": "987654321",
            "webhook_id": "555666777",
            "action": "event.created",
        },
    },
    "event_updated": {
        "api_url": "https://www.eventbriteapi.com/v3/events/123456789/",
        "config": {
            "event_id": "123456789",
            "user_id": "987654321",
            "webhook_id": "555666777",
            "action": "event.updated",
            "changed_fields": ["name", "description", "start_time"],
        },
    },
    "event_cancelled": {
        "api_url": "https://www.eventbriteapi.com/v3/events/123456789/",
        "config": {
            "event_id": "123456789",
            "user_id": "987654321",
            "webhook_id": "555666777",
            "action": "event.cancelled",
        },
    },
    "order_placed": {
        "api_url": "https://www.eventbriteapi.com/v3/orders/111222333/",
        "config": {
            "event_id": "123456789",
            "order_id": "111222333",
            "user_id": "987654321",
            "webhook_id": "555666777",
            "action": "order.placed",
        },
    },
}


def get_example_webhook(webhook_type: str) -> Optional[Dict[str, Any]]:
    """Get an example webhook payload for testing."""
    return EXAMPLE_WEBHOOKS.get(webhook_type)


def test_webhook_processor():
    """Test the webhook processor with example data."""
    processor = EventbriteWebhookProcessor()

    for webhook_name, webhook_data in EXAMPLE_WEBHOOKS.items():
        print(f"\nTesting {webhook_name}:")

        # Determine webhook type from action
        action = webhook_data["config"].get("action", webhook_name)

        # Process webhook
        result = processor.process_webhook(action, {"json": webhook_data})

        print(f"Result: {result}")


if __name__ == "__main__":
    # Run tests
    test_webhook_processor()
