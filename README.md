# Whereabouts Engine

A psychodemographic event analysis system that identifies optimal locations for target demographics using event data, venue characteristics, and environmental factors.

## Project Structure

```
/whereabouts-engine
│
├── /config
│   ├── __init__.py
│   ├── settings.py          # Loads environment variables
│   └── constants.py         # Trait mappings, KC bounding box, etc.
│
├── /src
│   ├── /data_acquisition
│   │   ├── __init__.py
│   │   ├── api_clients.py   # All API client classes
│   │   └── data_fetchers.py # Functions to fetch from each API
│   │
│   ├── /processing
│   │   ├── __init__.py
│   │   ├── grid_manager.py  # Creates and manages spatial grid
│   │   ├── layer_builders.py # Builds probability layers
│   │   └── data_fusion.py   # Combines layers using Bayesian fusion
│   │
│   ├── /visualization
│   │   ├── __init__.py
│   │   ├── map_builder.py   # Creates interactive maps
│   │   └── data_exporter.py # Exports data for external use
│   │
│   ├── /models
│   │   ├── __init__.py
│   │   └── database.py      # SQLAlchemy models
│   │
│   └── main.py              # Main orchestration script
│
├── /data
│   ├── /raw                 # Raw API responses (JSON)
│   ├── /processed           # Processed DataFrames (parquet)
│   └── /exports             # Final maps and exports
│
├── /notebooks               # For exploration and analysis
├── requirements.txt
├── Dockerfile
└── README.md
```

## Features

### Data Acquisition

- **Eventbrite API Integration**: Fetches real-time event data
- **Google Places API**: Enriches venue data with ratings and categories
- **Weather API**: Incorporates weather conditions for event timing
- **Fallback Stub Data**: Provides sample data when APIs are unavailable

### Processing & Analysis

- **Spatial Grid System**: Divides geographic areas into analysis cells
- **Multi-layer Scoring**:
  - Demographic scoring based on venue types
  - Event scoring based on tags and categories
  - Weather impact scoring
- **Bayesian Fusion**: Combines probability layers using statistical inference
- **High-confidence Area Detection**: Identifies optimal locations above threshold

### Visualization & Export

- **Interactive Maps**: Folium-based heatmaps and visualizations
- **Multiple Export Formats**: CSV, JSON, GeoJSON, Parquet
- **Grid Visualizations**: Spatial analysis results
- **Combined Dashboards**: Integrated view of all analysis layers

## Installation

1. **Clone and navigate to the project:**

   ```bash
   cd whereabouts-engine
   ```

2. **Create and activate virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   Create a `.env` file with your API keys:
   ```
   EVENTBRITE_API_KEY=your_eventbrite_key
   GOOGLE_PLACES_API_KEY=your_google_places_key
   OPENWEATHER_API_KEY=your_openweather_key
   ```

## Usage

### Basic Analysis

```python
from src.main import WhereaboutsEngine

# Initialize the engine
engine = WhereaboutsEngine()

# Run full analysis for a city
results = engine.run_full_analysis("Kansas City")

# Results include:
# - events_data: Processed event information
# - analysis_results: Spatial analysis and probability maps
# - visualizations: Generated map files
# - exports: Data export files
```

### Command Line

```bash
python -m src.main
```

### Individual Components

#### Data Acquisition

```python
from src.data_acquisition.data_fetchers import fetch_eventbrite_events

events = fetch_eventbrite_events("Kansas City")
```

#### Spatial Analysis

```python
from src.processing.grid_manager import GridManager
from src.processing.data_fusion import BayesianFusion

grid = GridManager()
fusion = BayesianFusion()

# Add events to grid
for event in events:
    grid.add_event_to_grid(event, score_data)

# Fuse probability layers
layers = {"demographic": demo_layer, "activity": activity_layer}
probabilities = fusion.fuse_layers(layers)
```

#### Visualization

```python
from src.visualization.map_builder import InteractiveMapBuilder

builder = InteractiveMapBuilder()
heatmap = builder.create_event_heatmap(events_data)
```

## Configuration

### Constants (config/constants.py)

- **KC_BOUNDING_BOX**: Geographic boundaries for analysis
- **VENUE_CATEGORY_SCORES**: Scoring weights for different venue types
- **EVENT_TAG_WEIGHTS**: Scoring weights for event tags
- **GRID_CELL_SIZE_M**: Spatial grid resolution in meters

### Settings (config/settings.py)

- **API Keys**: Configuration for external services
- **Database Paths**: SQLite database location
- **City Settings**: Default search parameters

## API Integration

### Eventbrite API

- Fetches live event data with venue information
- Handles pagination and rate limiting
- Falls back to stub data if API unavailable

### Google Places API

- Enriches venues with ratings and categories
- Provides additional demographic indicators
- Enhances venue categorization accuracy

### Weather API (National Weather Service)

- Fetches weather conditions for event times
- Impacts scoring for outdoor events
- No API key required (US government service)

## Database Schema

The system uses SQLite with the following main tables:

- **locations**: Venue information and coordinates
- **events**: Event details and timing
- **weather**: Weather conditions by location/time
- **scores**: Calculated demographic and event scores

## Output Files

### Visualizations

- `event_heatmap.html`: Interactive map of events by score
- `probability_heatmap.html`: Probability distribution map
- `grid_visualization.html`: Spatial grid analysis
- `combined_visualization.html`: Integrated dashboard

### Data Exports

- `events.csv`: Event data in tabular format
- `events.geojson`: GIS-compatible event data
- `probability_map.json`: Probability data for external use
- `analysis_report.json`: Summary statistics and insights

## Development

### Adding New APIs

1. Create client class in `src/data_acquisition/api_clients.py`
2. Add fetcher function in `src/data_acquisition/data_fetchers.py`
3. Update constants and scoring logic as needed

### Custom Scoring

Modify scoring functions in `src/processing/layer_builders.py`:

- `calculate_demographic_score()`: Venue-based scoring
- `calculate_event_score()`: Event tag-based scoring
- `calculate_weather_score()`: Weather impact scoring

### New Visualizations

Add visualization methods to `src/visualization/map_builder.py` or create new visualization modules.

## Troubleshooting

### Common Issues

1. **API Rate Limits**: System automatically falls back to stub data
2. **Missing Dependencies**: Run `pip install -r requirements.txt`
3. **Database Errors**: Database is auto-created on first run
4. **Import Errors**: Ensure you're running from the project root

### Debug Mode

Set environment variable for verbose logging:

```bash
export DEBUG=1
python -m src.main
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
