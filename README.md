# PPM - Psychographic Prediction Machine

A sophisticated machine learning system that predicts where and when your target psychographic group (career-driven, competent, fun individuals) will be in Kansas City. The system combines multi-source data fusion, advanced feature engineering, and machine learning to generate real-time psychographic density predictions visualized through interactive maps.

## 🎯 Overview

PPM (Psychographic Prediction Machine) is an advanced geospatial analytics platform that aggregates data from multiple APIs, engineers features representing psychographic relevance, trains machine learning models to predict psychographic density scores, and serves predictions through interactive map visualizations for strategic decision-making.

### Key Capabilities

- **Multi-Source Data Fusion**: Integrates 8+ APIs including weather, foot traffic, events, demographics, social sentiment
- **Advanced Feature Engineering**: 13 comprehensive feature groups with psychographic relevance scoring
- **Multiple ML Models**: XGBoost, Neural Networks, Graph Neural Networks, and Bayesian models
- **Real-Time Predictions**: FastAPI service with live psychographic density scoring
- **Interactive Map Visualizations**: Rich, multi-layer maps with heatmaps, event markers, and confidence intervals
- **Uncertainty Quantification**: Bayesian models provide prediction confidence levels

## 🏗️ Architecture

PPM has been restructured into a clean, feature-based architecture optimized for personal use and maintainability:

```
PPM/
├── app/                              # Personal web interface
│   ├── main.py                       # Streamlit application
│   └── static/                       # Static assets
│
├── features/                         # Core feature modules
│   ├── venues/                       # Venue aggregation
│   │   ├── scrapers/                 # Venue data scrapers
│   │   │   ├── static_venue_scraper.py
│   │   │   ├── dynamic_venue_scraper.py
│   │   │   └── kc_event_scraper.py
│   │   ├── collectors/               # Venue data collectors
│   │   │   └── venue_collector.py
│   │   ├── processors/               # Venue data processing
│   │   │   └── venue_processing.py
│   │   └── models.py                 # Venue data models
│   │
│   ├── events/                       # Event aggregation
│   │   ├── collectors/               # Event data collectors
│   │   │   └── external_api_collector.py
│   │   ├── scrapers/                 # Event scrapers
│   │   ├── processors/               # Event processing
│   │   └── models.py                 # Event data models
│   │
│   ├── ml/                           # ML predictions
│   │   ├── models/
│   │   │   ├── training/             # Training pipeline
│   │   │   │   └── train_model.py
│   │   │   └── inference/            # Prediction service
│   │   │       └── predictor.py
│   │   └── features/                 # Psychographic layers
│   │       ├── college_layer.py
│   │       └── spending_propensity_layer.py
│   │
│   └── visualization/                # Map creation
│       ├── builders/                 # Map builders
│       │   └── interactive_map_builder.py
│       ├── exporters/                # Export utilities
│       └── styles/                   # Visualization styles
│
├── shared/                           # Shared utilities
│   ├── database/                     # Database utilities
│   │   ├── connection.py             # Database connections
│   │   └── migrations.sql            # Database schema
│   ├── data_quality/                 # Quality control
│   │   └── quality_controller.py
│   ├── orchestration/               # Data orchestration
│   │   └── master_data_orchestrator.py
│   └── data_interface/              # Unified data access
│       └── master_data_interface.py  # Single source of truth
│
├── scripts/                          # Standalone scripts
│   ├── venues/                       # Venue scripts
│   │   └── run_venue_scraper.py
│   ├── events/                       # Event scripts
│   │   └── run_event_scraper.py
│   ├── ml/                           # ML scripts
│   │   ├── train_model.py
│   │   └── generate_predictions.py
│   └── visualization/                # Visualization scripts
│       └── generate_heatmap.py
│
├── config/                           # Configuration management
│   ├── settings.py                   # Environment configuration
│   └── constants.py                  # Psychographic scoring weights
│
├── data/                             # Data storage
│   ├── raw/                          # Raw API responses
│   ├── processed/                    # Feature-engineered datasets
│   ├── cache/                        # Cached data
│   └── exports/                      # Generated predictions
│
├── tests/                            # Test suites
├── docs/                             # Documentation
├── requirements.txt                  # Python dependencies
└── .env                              # Environment variables
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL 14+ with PostGIS extension
- Redis (for caching)
- API keys for external services

### Installation

1. **Clone and setup environment:**

   ```bash
   git clone https://github.com/callum-doty/PPM.git
   cd PPM
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Database setup:**

   ```bash
   # Start PostgreSQL with PostGIS
   docker-compose up -d postgres redis

   # Run migrations
   psql -h localhost -U postgres -d ppm -f src/db/migrations.sql
   ```

3. **Configure environment variables:**

   ```env
   # Database
   DATABASE_URL=postgresql://postgres:password@localhost:5432/ppm
   REDIS_URL=redis://localhost:6379

   # API Keys
   GOOGLE_PLACES_API_KEY=your_google_places_key
   EVENTBRITE_API_KEY=your_eventbrite_key
   PREDICTHQ_API_KEY=your_predicthq_key
   OPENWEATHER_API_KEY=your_weather_key
   TWITTER_API_KEY=your_twitter_key
   FACEBOOK_API_KEY=your_facebook_key
   MAPBOX_ACCESS_TOKEN=your_mapbox_token

   # ML Configuration
   MODEL_TYPE=ensemble  # xgboost, neural, graph, bayesian, ensemble
   CONFIDENCE_THRESHOLD=0.7
   GRID_RESOLUTION_METERS=500

   # Geographic Bounds (Kansas City)
   BBOX_NORTH=39.3209
   BBOX_SOUTH=38.9517
   BBOX_EAST=-94.3461
   BBOX_WEST=-94.7417
   ```

4. **Run the system:**

   ```bash
   # Start the personal web interface
   python app/main.py

   # Or run individual components:

   # Collect venue data
   python scripts/venues/run_venue_scraper.py

   # Collect event data
   python scripts/events/run_event_scraper.py

   # Train ML models
   python scripts/ml/train_model.py

   # Generate predictions
   python scripts/ml/generate_predictions.py

   # Create visualizations
   python scripts/visualization/generate_heatmap.py

   # Test the unified data interface
   python test_unified_simple_map_demo.py
   ```

## 🗺️ Interactive Map Visualizations

### Features

- **Multi-Layer Support**: Combine probability heatmaps, event markers, and grid analysis
- **Rich Popups**: Detailed event information with psychographic scores
- **Layer Controls**: Toggle different data layers on/off
- **Multiple Base Maps**: Streets, satellite, light, dark, and outdoor styles
- **Export Capabilities**: Save as HTML, GeoJSON, or other formats
- **Automatic Browser Opening**: Quick testing and demonstration

### Usage Example

```python
from features.visualization.builders.interactive_map_builder import InteractiveMapBuilder

# Initialize map builder
builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))

# Create combined visualization
map_file = builder.create_combined_visualization(
    events_data=events,
    probability_data=predictions,
    output_path="psychographic_map.html",
    style="streets"
)

# Open in browser
builder.open_in_browser(map_file)
```

## 📊 Feature Engineering

### 13 Comprehensive Feature Groups

| Feature Group          | Example Features                                           | Data Source                         |
| ---------------------- | ---------------------------------------------------------- | ----------------------------------- |
| **Venue Demographics** | Median income z-score, %Bachelor's degree, %Age 20-40      | Census API                          |
| **Venue Attributes**   | Venue type (one-hot), price tier, avg rating, review count | Google Places                       |
| **Foot Traffic**       | Hourly visits, dwell time, 24h change trend                | Foot Traffic API                    |
| **Traffic**            | Road congestion index, travel time to venue                | Traffic API                         |
| **Event Data**         | Predicted attendance, ticket price, event type             | Eventbrite, Ticketmaster, PredictHQ |
| **Weather**            | Temperature, rain probability, conditions                  | Weather API                         |
| **Economic Sentiment** | Local economic score, business closures                    | Economic APIs                       |
| **Social Sentiment**   | Tweets/posts sentiment, FB engagement count                | Twitter, Facebook                   |
| **Custom Layers**      | College density score, spending propensity score           | Custom algorithms                   |
| **Temporal**           | Hour sin/cos, day of week, seasonality                     | Derived                             |
| **Spatial**            | Distance to city center, neighborhood type                 | Geospatial analysis                 |
| **Competitive**        | Nearby venue density, market saturation                    | Spatial aggregation                 |
| **Historical**         | Venue popularity trends, event success rates               | Time series analysis                |

## 🤖 Machine Learning Pipeline

### Model Selection Strategy

| Model Type                | Use Case                       | Strengths                                  |
| ------------------------- | ------------------------------ | ------------------------------------------ |
| **XGBoost/LightGBM**      | Baseline, interpretable        | Great for tabular data, feature importance |
| **Neural Networks**       | Large datasets (>100k samples) | Non-linear relationships, complex patterns |
| **Graph Neural Networks** | Venue-event relationships      | Captures spatial and social connections    |
| **Bayesian Models**       | Uncertainty quantification     | Confidence intervals, risk assessment      |
| **Ensemble**              | Production deployment          | Combines strengths of multiple models      |

### Training Pipeline

```python
from features.ml.models.training.train_model import PsychographicPredictor

# Initialize multi-model trainer
predictor = PsychographicPredictor(
    models=['xgboost', 'neural', 'bayesian'],
    ensemble_method='weighted_average'
)

# Train with time-series cross-validation
results = predictor.train(
    start_date='2024-01-01',
    end_date='2024-12-31',
    validation_strategy='time_series_split'
)

# Evaluate performance
print(f"AUC-ROC: {results['auc_roc']:.3f}")
print(f"Precision@K: {results['precision_at_k']:.3f}")
print(f"Calibration Score: {results['calibration']:.3f}")
```

## 🎯 Labeling Strategy

### Ground Truth Collection

1. **Manual Labeling Interface**

   - Web-based annotation tool for location-time pairs
   - Quality control with inter-annotator agreement
   - Active learning for efficient labeling

2. **Proxy Labels**

   - Meetup.com RSVPs for career-focused events
   - LinkedIn event attendance data
   - Professional networking event identification
   - Automated psychographic relevance scoring

3. **Heuristic Bootstrap**
   - Initial labels from weighted formula: `Score = αD + βV + γE + δT + εF + ζW`
   - Gradually replaced with manual/proxy labels

## 🌐 Real-Time API

### Prediction Endpoints

```python
# Get psychographic density prediction
GET /api/v1/predict?lat=39.0997&lng=-94.5786&timestamp=2024-01-15T14:00:00Z

Response:
{
  "psychographic_density": 0.847,
  "confidence_interval": [0.782, 0.912],
  "contributing_factors": {
    "venue_attributes": 0.23,
    "foot_traffic": 0.19,
    "event_data": 0.31,
    "demographics": 0.27
  },
  "model_ensemble": {
    "xgboost": 0.851,
    "neural": 0.843,
    "bayesian": 0.847
  }
}

# Batch predictions for heatmap
POST /api/v1/predict/batch
{
  "grid_bounds": {
    "north": 39.3209, "south": 38.9517,
    "east": -94.3461, "west": -94.7417
  },
  "resolution_meters": 500,
  "timestamp": "2024-01-15T14:00:00Z"
}

# Filter predictions by psychographic focus
GET /api/v1/predict/filtered?focus=career_driven&confidence_min=0.7
```

## 📈 Use Cases

### Marketing & Business Intelligence

- **Target Audience Location**: Identify optimal locations for career-focused marketing
- **Event Planning**: Select venues with highest psychographic alignment
- **Competitive Analysis**: Understand where competitors' target audiences congregate
- **Site Selection**: Evaluate locations for new business ventures

### Real Estate & Urban Planning

- **Commercial Real Estate**: Assess locations for businesses targeting young professionals
- **Event Impact Assessment**: Predict crowd patterns and infrastructure needs
- **Economic Development**: Identify areas with growth potential

### Research & Analytics

- **Demographic Trend Analysis**: Track psychographic shifts over time
- **Social Behavior Modeling**: Understand movement patterns of target groups
- **Market Research**: Validate assumptions about target audience preferences

## 🔧 Configuration

### Psychographic Scoring Weights

```python
# config/constants.py
PSYCHOGRAPHIC_WEIGHTS = {
    'career_driven': {
        'venue_categories': {
            'coworking_space': 0.95,
            'business_center': 0.90,
            'networking_venue': 0.85,
            'upscale_restaurant': 0.75,
            'coffee_shop': 0.70
        },
        'event_types': {
            'professional_networking': 0.95,
            'business_conference': 0.90,
            'startup_event': 0.85,
            'career_fair': 0.80
        }
    },
    'competent': {
        'demographic_indicators': {
            'education_bachelors_plus': 0.90,
            'income_above_median': 0.80,
            'professional_occupation': 0.85
        }
    },
    'fun': {
        'venue_categories': {
            'entertainment': 0.90,
            'nightlife': 0.85,
            'restaurant': 0.80,
            'recreational': 0.75
        },
        'event_types': {
            'music': 0.90,
            'food_drink': 0.85,
            'social': 0.80,
            'cultural': 0.75
        }
    }
}
```

## 📊 Performance Monitoring

### Model Performance Metrics

- **Accuracy Metrics**: AUC-ROC, Precision@K, F1-Score
- **Calibration**: Reliability diagrams, Brier score
- **Business Metrics**: Prediction-to-outcome correlation
- **Uncertainty**: Confidence interval coverage

### System Performance

- **API Latency**: <200ms for single predictions, <2s for batch
- **Throughput**: 1000+ predictions per second
- **Uptime**: 99.9% availability target
- **Data Freshness**: Real-time updates within 15 minutes

## 🐳 Production Deployment

```yaml
# docker-compose.yml
version: "3.8"
services:
  postgres:
    image: postgis/postgis:14-3.2
    environment:
      POSTGRES_DB: ppm
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./src/db/migrations.sql:/docker-entrypoint-initdb.d/init.sql

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes

  api:
    build: .
    command: python -m src.backend.models.serve
    environment:
      - DATABASE_URL=postgresql://postgres:password@postgres:5432/ppm
      - REDIS_URL=redis://redis:6379
    depends_on:
      - postgres
      - redis
    ports:
      - "8000:8000"

  worker:
    build: .
    command: python -m src.infra.prefect_flows
    depends_on:
      - postgres
      - redis
```

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Model performance tests
pytest tests/models/

# API tests
pytest tests/api/

# End-to-end tests
pytest tests/e2e/

# Test interactive map builder
python test_visualization.py
```

## 📚 Documentation

- **API Documentation**: Auto-generated OpenAPI/Swagger docs at `/docs`
- **Model Documentation**: Jupyter notebooks in `/notebooks`
- **Interactive Map Guide**: See `INTERACTIVE_MAP_BUILDER.md`
- **Architecture Decisions**: ADRs in `/docs/architecture`
- **User Guides**: Step-by-step tutorials in `/docs/guides`

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/psychographic-enhancement`)
3. Implement changes with tests
4. Update documentation
5. Submit pull request

### Development Guidelines

- Follow PEP 8 for Python code
- Use type hints for all functions
- Maintain >90% test coverage
- Document all API endpoints
- Use semantic versioning

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Google Places API** for venue intelligence
- **Eventbrite/PredictHQ** for event data
- **U.S. Census Bureau** for demographic insights
- **Twitter/Facebook APIs** for social sentiment
- **PostGIS** for geospatial capabilities
- **Mapbox** for visualization platform
- **Folium** for interactive map generation

---

**PPM** - Predicting psychographic patterns, one location at a time. 🎯🧠📍
