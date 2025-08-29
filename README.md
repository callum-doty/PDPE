# PDPE - Predictive Demographics and Psychographic Engine

A sophisticated geospatial analytics platform that combines real-time event data, demographic insights, and psychographic modeling to predict optimal locations for target audiences and business opportunities.

## 🎯 Overview

PDPE (Predictive Demographics and Psychographic Engine) is an advanced data fusion system that analyzes multiple data streams to identify high-probability locations where specific demographic and psychographic profiles are likely to congregate. By combining event data, venue characteristics, demographic patterns, and environmental factors, PDPE provides actionable insights for marketing, business development, and strategic planning.

### Key Capabilities

- **Multi-Source Data Integration**: Combines Eventbrite, Google Places, Census, Weather, and social media APIs
- **Geospatial Analysis**: Advanced grid-based spatial modeling with Bayesian fusion
- **Real-Time Processing**: Live event data processing with fallback mechanisms
- **Interactive Visualizations**: Dynamic heatmaps and probability distributions
- **Multiple Export Formats**: CSV, JSON, GeoJSON, Parquet for downstream analysis

## 🏗️ Architecture

```
PDPE/
├── config/                     # Configuration and settings
│   ├── settings.py            # Environment and API configuration
│   └── constants.py           # Scoring weights and geographic bounds
│
├── src/
│   ├── data_acquisition/      # Data ingestion layer
│   │   ├── apis/             # API client implementations
│   │   │   ├── eventbrite_api.py
│   │   │   ├── google_apis.py
│   │   │   ├── census_api.py
│   │   │   ├── weather_api.py
│   │   │   ├── news_api.py
│   │   │   ├── ticketmaster_api.py
│   │   │   ├── twitter_api.py
│   │   │   └── besttime_api.py
│   │   ├── assumptions/      # Demographic modeling
│   │   │   ├── college_layer.py
│   │   │   └── spending_propensity_layer.py
│   │   ├── api_clients.py    # Unified API management
│   │   └── data_fetchers.py  # Data retrieval orchestration
│   │
│   ├── processing/           # Data processing and analysis
│   │   ├── data_formatters/ # API response standardization
│   │   ├── grid_manager.py  # Spatial grid management
│   │   ├── layer_builders.py # Probability layer construction
│   │   └── data_fusion.py   # Bayesian data fusion
│   │
│   ├── visualization/       # Output generation
│   │   ├── map_builder.py   # Interactive map creation
│   │   └── data_exporter.py # Multi-format data export
│   │
│   ├── models/             # Data models and persistence
│   │   └── database.py     # SQLAlchemy models
│   │
│   └── main.py            # Main application orchestrator
│
├── data/                  # Data storage
│   ├── raw/              # Raw API responses
│   ├── processed/        # Processed datasets
│   └── exports/          # Generated outputs
│
├── notebooks/            # Jupyter analysis notebooks
├── requirements.txt      # Python dependencies
├── Dockerfile           # Container configuration
└── .gitignore          # Version control exclusions
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Virtual environment (recommended)
- API keys for external services (optional - fallback data available)

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/callum-doty/PDPE.git
   cd PDPE
   ```

2. **Set up virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   Create a `.env` file in the project root:

   ```env
   # API Keys (optional - system uses fallback data if unavailable)
   EVENTBRITE_API_KEY=your_eventbrite_key
   GOOGLE_PLACES_API_KEY=your_google_places_key
   OPENWEATHER_API_KEY=your_openweather_key
   NEWS_API_KEY=your_news_api_key
   TWITTER_API_KEY=your_twitter_key

   # Configuration
   CITY_NAME=Kansas City
   DEBUG=false
   ```

### Running the Analysis

**Command Line:**

```bash
PYTHONPATH=. python src/main.py
```

**Programmatic Usage:**

```python
from src.main import WhereaboutsEngine

# Initialize the engine
engine = WhereaboutsEngine()

# Run comprehensive analysis
results = engine.run_full_analysis()

# Access results
events_data = results['events_data']
analysis_results = results['analysis_results']
visualizations = results['visualizations']
```

## 📊 Features

### Data Acquisition

- **Event Data**: Real-time event information from Eventbrite with venue details
- **Venue Intelligence**: Google Places API integration for ratings and categories
- **Demographic Data**: Census API integration for population characteristics
- **Weather Integration**: National Weather Service data for environmental factors
- **Social Media**: Twitter API for sentiment and activity analysis
- **News Integration**: Current events and local news impact analysis

### Advanced Analytics

- **Spatial Grid System**: Configurable resolution grid-based analysis
- **Multi-Layer Scoring**:
  - Demographic affinity scoring
  - Event category relevance
  - Venue type preferences
  - Weather impact modeling
  - Social sentiment analysis
- **Bayesian Fusion**: Statistical combination of probability layers
- **Confidence Mapping**: High/medium/low confidence area identification

### Visualization & Export

- **Interactive Maps**:
  - Event heatmaps with scoring overlays
  - Probability distribution visualizations
  - Grid-based analysis displays
  - Combined multi-layer dashboards
- **Export Formats**:
  - CSV for spreadsheet analysis
  - GeoJSON for GIS applications
  - Parquet for big data processing
  - JSON for web applications

## 🔧 Configuration

### Scoring Configuration

Modify `config/constants.py` to adjust:

```python
# Venue category scoring weights
VENUE_CATEGORY_SCORES = {
    'restaurant': 0.8,
    'bar': 0.9,
    'entertainment': 0.7,
    'retail': 0.6,
    # ... additional categories
}

# Event tag importance weights
EVENT_TAG_WEIGHTS = {
    'music': 0.9,
    'food': 0.8,
    'business': 0.7,
    # ... additional tags
}

# Geographic boundaries
KC_BOUNDING_BOX = {
    'north': 39.3209,
    'south': 38.9517,
    'east': -94.3461,
    'west': -94.7417
}
```

### API Configuration

Update `config/settings.py` for API endpoints and parameters:

```python
# API rate limiting
EVENTBRITE_RATE_LIMIT = 1000  # requests per hour
GOOGLE_PLACES_RATE_LIMIT = 100000  # requests per day

# Analysis parameters
GRID_CELL_SIZE_M = 500  # meters
CONFIDENCE_THRESHOLD = 0.7
MAX_EVENTS_PER_ANALYSIS = 100
```

## 📈 Use Cases

### Marketing & Advertising

- **Target Audience Location**: Identify where specific demographics congregate
- **Campaign Optimization**: Optimize ad placement based on probability maps
- **Event Planning**: Select optimal venues based on audience analysis

### Business Intelligence

- **Site Selection**: Evaluate locations for new business ventures
- **Competitive Analysis**: Understand competitor audience patterns
- **Market Research**: Analyze demographic trends and preferences

### Urban Planning

- **Event Impact Assessment**: Understand how events affect local areas
- **Infrastructure Planning**: Plan based on predicted crowd patterns
- **Economic Development**: Identify areas with growth potential

## 🔍 API Integration Details

### Eventbrite API

- **Endpoint**: `/v3/events/search/`
- **Rate Limit**: 1000 requests/hour
- **Fallback**: Enhanced stub data with realistic Kansas City venues
- **Data**: Event details, venue information, categories, timing

### Google Places API

- **Endpoint**: `/maps/api/place/`
- **Rate Limit**: 100,000 requests/day
- **Enhancement**: Venue ratings, categories, demographic indicators
- **Fallback**: Basic venue categorization

### Census API

- **Endpoint**: `/data/2021/acs/acs5`
- **Rate Limit**: No official limit
- **Data**: Demographic characteristics by geographic area
- **Integration**: Population density, age distribution, income levels

## 📊 Output Analysis

### Generated Visualizations

1. **Event Heatmap** (`event_heatmap.html`)

   - Color-coded event locations by demographic score
   - Interactive tooltips with event details
   - Zoom and pan capabilities

2. **Probability Heatmap** (`probability_heatmap.html`)

   - Probability distribution across geographic grid
   - Confidence level indicators
   - Statistical overlay information

3. **Grid Visualization** (`grid_visualization.html`)

   - Spatial analysis grid display
   - Cell-by-cell scoring breakdown
   - Aggregated statistics

4. **Combined Dashboard** (`combined_visualization.html`)
   - Integrated view of all analysis layers
   - Toggle between different data views
   - Comprehensive analysis summary

### Data Exports

- **Events CSV**: Tabular event data with scores and metadata
- **GeoJSON**: Geographic event data for GIS applications
- **Probability JSON**: Grid-based probability distributions
- **Analysis Report**: Summary statistics and insights
- **Parquet Files**: Optimized format for big data analysis

## 🛠️ Development

### Adding New Data Sources

1. **Create API Client**:

   ```python
   # src/data_acquisition/apis/new_api.py
   class NewAPIClient:
       def __init__(self, api_key):
           self.api_key = api_key

       def fetch_data(self, location):
           # Implementation
           pass
   ```

2. **Add Data Formatter**:

   ```python
   # src/processing/data_formatters/new_formatter.py
   def format_new_data(raw_data):
       # Standardization logic
       return formatted_data
   ```

3. **Update Main Engine**:
   ```python
   # Add to src/main.py
   new_data = self.fetch_new_data(city)
   formatted_data = format_new_data(new_data)
   ```

### Custom Scoring Models

Extend scoring in `src/processing/layer_builders.py`:

```python
def calculate_custom_score(event_data, venue_data, demographic_data):
    """Custom scoring algorithm implementation."""
    base_score = calculate_base_score(event_data)
    demographic_multiplier = get_demographic_multiplier(demographic_data)
    venue_bonus = get_venue_bonus(venue_data)

    return base_score * demographic_multiplier + venue_bonus
```

### Testing

```bash
# Run unit tests
python -m pytest tests/

# Run integration tests
python -m pytest tests/integration/

# Run with coverage
python -m pytest --cov=src tests/
```

## 🐳 Docker Deployment

```bash
# Build container
docker build -t pdpe .

# Run analysis
docker run -v $(pwd)/data:/app/data -e EVENTBRITE_API_KEY=your_key pdpe

# Run with custom configuration
docker run -v $(pwd)/config:/app/config -v $(pwd)/data:/app/data pdpe
```

## 📝 Troubleshooting

### Common Issues

**API Rate Limits**

- System automatically switches to fallback data
- Check API key validity and quotas
- Consider upgrading API plans for production use

**Memory Issues**

- Reduce `MAX_EVENTS_PER_ANALYSIS` in settings
- Increase `GRID_CELL_SIZE_M` for lower resolution
- Use streaming processing for large datasets

**Import Errors**

- Ensure `PYTHONPATH=.` when running from project root
- Verify virtual environment activation
- Check all dependencies are installed

### Debug Mode

Enable verbose logging:

```bash
export DEBUG=true
PYTHONPATH=. python src/main.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add unit tests for new functionality
- Update documentation for API changes
- Use type hints for function signatures

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Eventbrite API** for event data access
- **Google Places API** for venue intelligence
- **U.S. Census Bureau** for demographic data
- **National Weather Service** for weather data
- **Folium** for interactive mapping capabilities

## 📞 Support

For questions, issues, or contributions:

- **GitHub Issues**: [Report bugs or request features](https://github.com/callum-doty/PDPE/issues)
- **Documentation**: [Wiki pages](https://github.com/callum-doty/PDPE/wiki)
- **Email**: [Contact the maintainer](mailto:doty.callum9@gmail.com)

---

**PDPE** - Turning data into demographic intelligence, one location at a time. 🎯📊🗺️
