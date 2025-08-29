# PDPE Event-Driven Architecture

This document describes the new event-driven architecture implemented for the PDPE (Predictive Demographics and Psychographic Engine) system, which enables real-time processing and webhook integration.

## 🚀 Quick Start

### Running the Event-Driven System

```bash
# Install dependencies
pip install -r requirements.txt

# Run the event-driven system
python src/main_event_driven.py

# Run with demo
python src/main_event_driven.py --demo
```

### Testing Webhooks

```bash
# Test Eventbrite webhook
curl -X POST http://localhost:8001/webhooks/eventbrite \
  -H "Content-Type: application/json" \
  -d '{
    "api_url": "https://www.eventbriteapi.com/v3/events/123456789/",
    "config": {
      "event_id": "123456789",
      "action": "event.created"
    }
  }'

# Check system health
curl http://localhost:8001/health
```

## 🏗️ Architecture Overview

The event-driven architecture consists of several key components:

### 1. Event System (`src/events/`)

- **EventBus**: Central message bus for publishing and subscribing to events
- **Event Types**: Strongly-typed event definitions for different system events
- **Event Dispatcher**: Coordinates event flow and manages time-based events
- **Event Handlers**: Process different types of events and trigger appropriate actions

### 2. Assumption Engine (`src/processing/assumption_engine.py`)

- **Real-time Computation**: Computes assumption layer scores on-demand
- **Smart Caching**: TTL-based caching with event-driven invalidation
- **Event-Driven Updates**: Automatically recalculates when time boundaries change

### 3. Webhook System (`src/webhooks/`)

- **Webhook Server**: FastAPI server for receiving real-time updates
- **Eventbrite Processor**: Handles Eventbrite-specific webhook events
- **Generic Processing**: Supports webhooks from multiple sources

### 4. Real-time Processing Pipeline

Events flow through the system as follows:

```
External API Webhook → Webhook Server → Event Bus → Event Handlers →
Assumption Engine → Grid Updates → Visualization Updates
```

## 📊 Event Types

### Core Events

- **TimeChangeEvent**: Triggered when time crosses significant boundaries (hour, day, week, month)
- **EventAddedEvent**: New event detected from external sources
- **EventRemovedEvent**: Event cancelled or removed
- **EventUpdatedEvent**: Existing event modified

### System Events

- **AssumptionLayerUpdateEvent**: Triggers recalculation of assumption layers
- **GridUpdateEvent**: Updates spatial grid calculations
- **VisualizationUpdateEvent**: Refreshes visualizations
- **WebhookReceivedEvent**: Raw webhook data received

## 🔄 Real-time Features

### 1. Time-Based Updates

The system automatically triggers updates when time boundaries are crossed:

- **Hourly**: Spending propensity recalculation
- **Daily**: Full assumption layer refresh
- **Weekly**: College presence schedule updates
- **Monthly**: Seasonal factor adjustments

### 2. Event-Driven Cache Invalidation

Smart caching system that invalidates cache entries based on:

- **Time Dependencies**: Cache expires when time-sensitive factors change
- **Location Dependencies**: Cache invalidated for specific geographic areas
- **Event Dependencies**: Cache cleared when related events change

### 3. Webhook Processing

Real-time webhook processing for:

- **Eventbrite**: Event creation, updates, cancellations
- **Twitter**: Social media sentiment updates
- **Ticketmaster**: Additional event sources
- **Generic**: Custom webhook sources

## 🧠 Assumption Layer Engine

### Smart Caching Strategy

```python
# Cache with TTL and dependencies
spending_cache = SmartCache(maxsize=2000, ttl=3600)  # 1 hour
college_cache = SmartCache(maxsize=1000, ttl=7200)   # 2 hours
combined_cache = SmartCache(maxsize=500, ttl=1800)   # 30 minutes
```

### Event-Driven Computation

```python
# Automatic recalculation on time changes
def recalculate_spending_propensity(self, time):
    # Invalidate time-dependent cache entries
    self.spending_cache.invalidate_by_dependency(f"time_hour_{time.hour}")
    self.spending_cache.invalidate_by_dependency(f"time_day_{time.day}")
```

### Cache Dependencies

- **Time Dependencies**: `time_hour_19`, `time_day_15`, `time_month_12`
- **Location Dependencies**: `location_39.099_-94.578`
- **Event Dependencies**: `event_123456789`

## 🔗 Webhook Integration

### Supported Webhook Types

#### Eventbrite Webhooks

- `event.created` - New event published
- `event.updated` - Event details changed
- `event.cancelled` - Event cancelled
- `event.deleted` - Event removed
- `order.placed` - New ticket purchase
- `attendee.updated` - Attendee information changed

#### Generic Webhooks

- Custom webhook types from any source
- Automatic event type detection
- Configurable processing pipelines

### Webhook Processing Flow

1. **Receive**: FastAPI endpoint receives webhook
2. **Validate**: Check webhook format and source
3. **Process**: Extract relevant data using source-specific processor
4. **Publish**: Create internal event and publish to event bus
5. **Handle**: Event handlers process the event and trigger updates

## 📈 Performance Optimizations

### 1. Asynchronous Processing

- Non-blocking webhook processing
- Concurrent event handling
- Background task execution

### 2. Intelligent Caching

- Multi-level cache hierarchy
- Dependency-based invalidation
- Cache warming strategies

### 3. Incremental Updates

- Only recalculate affected areas
- Partial grid updates
- Selective assumption layer refresh

## 🛠️ Configuration

### Environment Variables

```bash
# Webhook server configuration
WEBHOOK_HOST=0.0.0.0
WEBHOOK_PORT=8001

# Cache configuration
CACHE_TTL_SPENDING=3600
CACHE_TTL_COLLEGE=7200
CACHE_TTL_COMBINED=1800

# Event system configuration
EVENT_MONITOR_INTERVAL=60
MIN_RECALC_INTERVAL=300
```

### Assumption Engine Configuration

```python
config = {
    "spending_weight": 0.4,
    "college_weight": 0.3,
    "event_proximity_weight": 0.3,
    "cache_enabled": True,
    "min_recalc_interval": 300,  # 5 minutes
}
```

## 📊 Monitoring and Statistics

### System Statistics

```python
# Event dispatcher stats
{
    "running": True,
    "time_events_dispatched": 24,
    "scheduled_events_pending": 3,
    "errors": 0
}

# Assumption engine stats
{
    "cache_hits": 1250,
    "cache_misses": 180,
    "spending_calculations": 95,
    "college_calculations": 85,
    "recalculations": 12
}

# Webhook server stats
{
    "webhooks_received": 15,
    "webhooks_processed": 15,
    "webhooks_failed": 0,
    "uptime_seconds": 3600
}
```

### Health Check Endpoints

- `GET /` - Basic health check
- `GET /health` - Detailed system status
- `GET /stats` - Performance statistics

## 🔧 Development

### Adding New Event Types

1. Define event class in `src/events/event_types.py`:

```python
@dataclass
class CustomEvent(BaseEvent):
    custom_data: Dict[str, Any]

    def get_event_type(self) -> str:
        return "custom_event"
```

2. Add event handler in `src/events/event_handlers.py`:

```python
def handle_custom_event(self, event: CustomEvent):
    # Process custom event
    pass
```

3. Subscribe handler to event type:

```python
subscribe_to_event("custom_event", handler.handle_custom_event)
```

### Adding New Webhook Sources

1. Create processor in `src/webhooks/`:

```python
class CustomWebhookProcessor:
    def process_webhook(self, webhook_type: str, webhook_data: Dict[str, Any]):
        # Process webhook data
        return processed_data
```

2. Register processor:

```python
webhook_handler.register_webhook_processor("custom_source", processor)
```

3. Add endpoint in webhook server:

```python
@app.post("/webhooks/custom")
async def custom_webhook(request: Request, background_tasks: BackgroundTasks):
    return await self._handle_webhook(request, background_tasks, "custom_source")
```

## 🧪 Testing

### Unit Tests

```bash
# Test event system
python -m pytest tests/test_events.py

# Test assumption engine
python -m pytest tests/test_assumption_engine.py

# Test webhooks
python -m pytest tests/test_webhooks.py
```

### Integration Tests

```bash
# Test full event-driven pipeline
python -m pytest tests/test_integration.py

# Test webhook processing
python -m pytest tests/test_webhook_integration.py
```

### Manual Testing

```bash
# Start system in demo mode
python src/main_event_driven.py --demo

# Send test webhook
curl -X POST http://localhost:8001/webhooks/eventbrite \
  -H "Content-Type: application/json" \
  -d @tests/fixtures/eventbrite_webhook.json
```

## 🚀 Deployment

### Docker Deployment

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8001

CMD ["python", "src/main_event_driven.py"]
```

### Production Configuration

```bash
# Use production logging
export LOG_LEVEL=INFO
export LOG_FILE=/var/log/pdpe/event_driven.log

# Configure Redis for distributed caching
export REDIS_URL=redis://localhost:6379

# Set webhook server configuration
export WEBHOOK_HOST=0.0.0.0
export WEBHOOK_PORT=8001
```

## 📚 API Reference

### Event Bus API

```python
from src.events.event_bus import get_event_bus, publish_event

# Get event bus instance
bus = get_event_bus()

# Publish event synchronously
publish_event(event)

# Publish event asynchronously
await bus.publish_async(event)

# Subscribe to events
bus.subscribe("event_type", handler_function)
```

### Assumption Engine API

```python
from src.processing.assumption_engine import get_assumption_engine

# Get engine instance
engine = get_assumption_engine()

# Compute scores
spending_score = engine.compute_spending_propensity(lat, lon, time)
college_score = engine.compute_college_presence(lat, lon, time)
combined_scores = engine.compute_combined_score(lat, lon, time, event_data)

# Manual cache management
engine.clear_all_caches()
engine.invalidate_event_cache(event_id)
```

### Webhook Server API

```python
from src.webhooks.webhook_server import start_webhook_server, stop_webhook_server

# Start webhook server
server = await start_webhook_server(host="0.0.0.0", port=8001)

# Stop webhook server
await stop_webhook_server()
```

## 🔍 Troubleshooting

### Common Issues

1. **Event Bus Not Starting**

   - Check for port conflicts
   - Verify async event loop is running
   - Check log files for detailed errors

2. **Webhook Processing Failures**

   - Verify webhook payload format
   - Check processor registration
   - Review webhook server logs

3. **Cache Performance Issues**

   - Monitor cache hit/miss ratios
   - Adjust TTL values
   - Consider cache warming strategies

4. **Memory Usage**
   - Monitor cache sizes
   - Adjust maxsize parameters
   - Implement cache cleanup strategies

### Debug Mode

```bash
# Enable debug logging
export DEBUG=true
export LOG_LEVEL=DEBUG

# Run with verbose output
python src/main_event_driven.py --demo
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Update documentation
5. Submit pull request

### Development Setup

```bash
# Clone repository
git clone https://github.com/callum-doty/PDPE.git
cd PDPE

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install development dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Run tests
python -m pytest

# Start development server
python src/main_event_driven.py --demo
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **FastAPI** for webhook server framework
- **asyncio** for asynchronous processing
- **cachetools** for intelligent caching
- **pydantic** for data validation
