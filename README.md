# PPM - Psychographic Prediction Machine

A machine learning system that predicts where and when your target psychographic group (career-driven, competent, fun individuals) will be in Kansas City. The system combines multi-source data aggregation, advanced feature engineering, and machine learning to generate real-time psychographic density predictions visualized through interactive maps.

## 🎯 Overview

PPM (Psychographic Prediction Machine) is a geospatial analytics platform that:

- Aggregates data from web scraping and APIs (venues, events, demographics, weather)
- Engineers features representing psychographic relevance using ML
- Trains models to predict psychographic density scores
- Visualizes predictions through interactive maps for strategic decision-making

### Key Capabilities

- **Multi-Source Data Collection**: Web scraping (LLM-powered) + API integration
- **Advanced Feature Engineering**: 11 comprehensive feature groups with psychographic scoring
- **Machine Learning**: LightGBM models with time series validation
- **Real-Time Predictions**: Psychographic density scoring for venues and events
- **Interactive Maps**: Folium-based visualizations with heatmaps and markers
- **Data Quality Validation**: Comprehensive validation system for all data sources

## 🏗️ Simplified Architecture

PPM uses a clean, feature-based architecture optimized for maintainability:

```
PPM/
├── app.py                           # Main Streamlit application
├── setup_database.py                # Database initialization script
│
├── core/                            # Core infrastructure
│   ├── database.py                  # Unified database interface
│   └── quality.py                   # Data quality validation
│
├── features/                        # Feature services (4 files = entire system)
│   ├── venues.py                    # Venue collection & processing
│   ├── events.py                    # Event collection & processing (LLM-powered)
│   ├── predictions.py               # ML training & predictions
│   └── maps.py                      # Map visualization
│
├── config/                          # Configuration
│   ├── settings.py                  # Environment settings
│   └── constants.py                 # Psychographic weights
│
├── models/                          # Trained ML models
│   └── ppm_model_v1.0.pkl          # LightGBM model
│
├── data/                            # Data storage (gitignored)
├── tests/                           # Test suites
└── docs/                            # Documentation
```

### Architecture Highlights

**Before (Complex):**

- 50+ Python files across nested directories
- Scattered functionality requiring navigation of multiple layers
- Complex orchestration with tight coupling

**After (Simplified):**

- 4 feature files contain the entire system
- Each file is a complete, self-contained service
- Clean interfaces with no orchestration layer needed
- Direct imports: `from features.venues import get_venue_service`

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- SQLite (included with Python)
- Optional: API keys for external services

### Installation

```bash
# Clone repository
git clone https://github.com/callum-doty/PPM.git
cd PPM

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys (optional)

# Initialize database
python setup_database.py
```

### Running the Application

```bash
# Start Streamlit app
python -m streamlit run app.py

# Or use Python directly
python app.py
```

The app will open in your browser at http://localhost:8501

## 🎭 Data Collection

### Event Scraping (LLM-Powered)

PPM uses GPT-4 for intelligent event extraction from Kansas City venue websites:

```python
from features.events import get_event_service

events = get_event_service()

# Collect events from all KC sources (uses LLM extraction)
result = events.collect_from_kc_sources()

# Or collect from specific sources
result = events.collect_all()  # KC sources + external APIs
```

**How LLM Event Scraping Works:**

1. Fetches HTML from 12+ KC venue websites (T-Mobile Center, Kauffman Center, etc.)
2. Converts to Markdown for cleaner processing
3. Sends to GPT-4o-mini with structured prompt requesting JSON
4. Extracts: title, date, time, location, description, price, URL
5. Falls back to CSS selectors if LLM fails
6. Validates & stores with psychographic scoring

**Supported KC Venues:**

- Major venues: T-Mobile Center, Kauffman Center, Starlight Theatre, Midland Theatre
- Districts: Power & Light, Westport, Crossroads, 18th & Vine Jazz
- Aggregators: Visit KC, Do816 (dynamic JavaScript sites)

### Venue Collection

```python
from features.venues import get_venue_service

venues = get_venue_service()

# Collect from all sources
result = venues.collect_all()  # Scraped sources + APIs

# Or specific sources
result = venues.collect_from_scraped_sources()  # 29 KC venues
result = venues.collect_from_google_places()     # Google Places API
result = venues.collect_from_yelp()              # Yelp Fusion API
```

### ML Predictions

```python
from features.predictions import get_prediction_service

predictions = get_prediction_service()

# Train model
train_result = predictions.train_model()

# Generate predictions for venues
venue_prediction = predictions.predict_venue_attendance("venue_123")

# Generate heatmap predictions
heatmap_data = predictions.generate_heatmap_predictions()
```

### Map Visualization

```python
from features.maps import get_map_service

maps = get_map_service()

# Create combined map (venues + events + predictions)
result = maps.create_combined_map(
    include_venues=True,
    include_events=True,
    include_predictions=True,
    output_path="psychographic_map.html"
)

# Open in browser
maps.open_map_in_browser(result.data)
```

## 📊 Feature Engineering

### Psychographic Scoring System

Each venue and event receives scores (0-1) across 5 psychographic dimensions:

| Dimension     | Keywords                           | Example Categories                               |
| ------------- | ---------------------------------- | ------------------------------------------------ |
| Career-Driven | networking, professional, business | coworking_space (0.95), conference_center (0.85) |
| Competent     | expert, training, certification    | university (0.90), museum (0.80)                 |
| Fun           | party, entertainment, music        | nightclub (0.90), music_venue (0.90)             |
| Social        | community, meetup, gathering       | bar (0.80), restaurant (0.70)                    |
| Adventurous   | outdoor, extreme, exploration      | sports_venue (0.80), adventure (0.90)            |

**Scoring Formula:**

```python
score = (keywords_found / total_keywords) * category_weight
```

### Feature Groups (11 Total)

1. **Venue Attributes**: category, rating, review count, price tier
2. **Location Features**: lat/lng, has_location boolean, distance to downtown
3. **Temporal Features**: venue age, hour/day encodings
4. **Event Features**: event count, attendance estimates
5. **Psychographic Scores**: 5 dimension scores (career, competent, fun, social, adventurous)
6. **Demographic Data**: income, education, age distribution (Census API)
7. **Weather Data**: temperature, precipitation, conditions (OpenWeather API)
8. **Foot Traffic**: visit counts, dwell time (optional API)
9. **Social Sentiment**: mentions, engagement (optional social APIs)
10. **Economic Indicators**: business sentiment, spending indices
11. **Data Quality**: completeness, accuracy scores

## 🤖 Machine Learning Pipeline

### Model Architecture

**Current Implementation: LightGBM**

- Gradient boosting decision trees
- Time series cross-validation (5-fold)
- Early stopping with validation monitoring
- Feature importance tracking

**Model Selection Strategy:**

```python
# Small datasets (<1k samples): LightGBM baseline
# Large datasets (>100k): Neural networks
# Complex relationships: Graph neural networks
# Uncertainty needed: Bayesian models
```

### Training Process

```python
from features.predictions import get_prediction_service

predictor = get_prediction_service()

# Train with validation
result = predictor.train_model(retrain=True)

print(f"Validation Score: {result.validation_score:.3f}")
print(f"Training Samples: {result.training_samples}")
print(f"Features Used: {len(result.features_used)}")
```

### Prediction Types

- **Venue Attendance**: Likelihood of target demographic visiting
- **Event Attendance**: Event-specific attendance probability
- **Psychographic Match**: Overall alignment with target demographic
- **Heatmap Predictions**: Grid-based density predictions for visualization

### Model Performance Targets

- AUC-ROC: >0.75
- Precision@K: >0.70
- Calibration: Well-calibrated probabilities
- Latency: <200ms single prediction, <2s batch

## 🗺️ Interactive Visualizations

### Map Features

- **Multi-layer support**: Venues, events, predictions (toggleable)
- **Rich popups**: Detailed information with ML predictions integrated
- **Mapbox integration**: High-quality satellite/street tiles (with OpenStreetMap fallback)
- **Heatmaps**: Prediction density visualization
- **Export**: HTML, GeoJSON formats

### Creating Maps

```python
from features.maps import get_map_service

maps = get_map_service()

# Venue map only
result = maps.create_venue_map(output_path="venues.html")

# Event map only
result = maps.create_event_map(
    event_filters={"start_date": "2025-11-01", "end_date": "2025-12-01"},
    output_path="events.html"
)

# Prediction heatmap
result = maps.create_prediction_heatmap(output_path="predictions.html")

# Combined map (recommended)
result = maps.create_combined_map(
    include_venues=True,
    include_events=True,
    include_predictions=True,
    output_path="combined.html"
)

# Open in browser
if result.success:
    maps.open_map_in_browser(result.data)
```

### Map Visualization Details

**Venue Markers:**

- Size indicates rating quality
- Color indicates rating score
- Tooltips show name, rating, and ML prediction
- Popups include full details + ML predictions

**Event Markers:**

- Yellow/orange color scheme
- Show venue name, category, time
- Filtered by date range

**Prediction Integration:**

- ML predictions appear in venue tooltips
- Color-coded by prediction value (red = high likelihood)
- Confidence scores displayed
- Model version shown

## 📁 Database Schema

### Core Tables

**Venues**

```sql
CREATE TABLE venues (
    venue_id TEXT PRIMARY KEY,
    external_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    lat REAL, lng REAL,
    address TEXT,
    avg_rating REAL,
    psychographic_relevance TEXT,  -- JSON
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**Events**

```sql
CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    external_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    start_time TIMESTAMP,
    venue_id TEXT,
    lat REAL, lng REAL,
    psychographic_relevance TEXT,  -- JSON
    FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
);
```

**ML Predictions**

```sql
CREATE TABLE ml_predictions (
    prediction_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    prediction_type TEXT NOT NULL,
    prediction_value REAL NOT NULL,
    confidence_score REAL,
    model_version TEXT NOT NULL,
    features_used TEXT,  -- JSON array
    generated_at TIMESTAMP,
    FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
);
```

### Enrichment Tables

- **demographic_data**: Census income, education, age data
- **foot_traffic_data**: Visit counts, dwell time
- **weather_data**: Temperature, precipitation forecasts
- **social_sentiment_data**: Social media mentions, engagement
- **traffic_data**: Congestion, travel times
- **economic_indicators**: Business sentiment, spending

### Views

- **vw_master_venue_data**: Venues with all enrichments
- **vw_master_events_data**: Events with venue info
- **vw_high_value_predictions**: Top prediction recommendations
- **vw_data_quality_summary**: Data quality metrics
- **vw_collection_health**: Source health monitoring

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Database (SQLite by default, optional PostgreSQL)
SQLITE_DB_PATH=ppm.db
# DATABASE_URL=postgresql://user:pass@localhost:5432/ppm

# API Keys (all optional)
CHATGPT_API_KEY=sk-...              # Required for LLM event scraping
GOOGLE_PLACES_API_KEY=...           # Optional: venue enrichment
MAPBOX_ACCESS_TOKEN=pk....          # Optional: better map tiles
OPENWEATHER_API_KEY=...             # Optional: weather data
PREDICTHQ_API_KEY=...               # Optional: event data

# Geographic Bounds (Kansas City)
BBOX_NORTH=39.3209
BBOX_SOUTH=38.9517
BBOX_EAST=-94.3461
BBOX_WEST=-94.7417
```

### Psychographic Weights (config/constants.py)

```python
PSYCHOGRAPHIC_WEIGHTS = {
    'career_driven': {
        'venue_categories': {
            'coworking_space': 0.95,
            'business_center': 0.90,
            'networking_venue': 0.85,
        },
        'event_types': {
            'professional_networking': 0.95,
            'business_conference': 0.90,
        }
    },
    'fun': {
        'venue_categories': {
            'nightclub': 0.90,
            'music_venue': 0.90,
            'bar': 0.85,
        }
    },
    # ... more configurations
}
```

## 📈 Use Cases

### Marketing & Business Intelligence

- Identify optimal locations for career-focused marketing campaigns
- Select event venues with highest psychographic alignment
- Understand competitor target audience locations

### Real Estate & Urban Planning

- Assess commercial locations for businesses targeting young professionals
- Predict crowd patterns for infrastructure planning
- Identify areas with growth potential

### Event Planning

- Choose venues based on psychographic fit
- Predict attendance likelihood
- Optimize event timing and location

## 🧪 Testing

```bash
# Test database setup
python tests/test_database.py

# Test individual services
python -c "from features.venues import get_venue_service; print(get_venue_service().collect_all())"
python -c "from features.events import get_event_service; print(get_event_service().collect_all())"

# Run all tests
pytest tests/
```

## 📊 Performance Monitoring

### System Health Dashboard

```python
from core.database import get_database

db = get_database()

# Overall summary
summary = db.get_data_summary()
print(f"Venues: {summary['total_venues']}")
print(f"Events: {summary['total_events']}")
print(f"Predictions: {summary['total_predictions']}")
print(f"Location Completeness: {summary['location_completeness']:.1%}")

# Collection health
health = db.get_collection_health()
for source in health:
    print(f"{source['source_name']}: {source['status']}")

# Data quality
quality = db.get_data_quality_summary()
```

### Performance Metrics

- **Data Collection**: ~1-5 seconds per source
- **ML Training**: ~5-30 seconds (depends on data size)
- **Prediction Generation**: <1 second for single venue
- **Map Creation**: ~2-5 seconds for combined map
- **Database Queries**: <100ms for most operations

## 🐳 Deployment

### Local Development

```bash
python app.py
```

### Production Deployment

```bash
# Using Streamlit Cloud
streamlit run app.py

# Or Docker
docker build -t ppm .
docker run -p 8501:8501 ppm
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/enhancement`)
3. Make changes with tests
4. Update documentation
5. Submit pull request

### Development Guidelines

- Follow PEP 8 for Python code
- Use type hints for all functions
- Document all API endpoints
- Maintain data quality validation
- Test with real data sources

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **OpenAI GPT-4**: Powers intelligent event extraction
- **Google Places API**: Venue intelligence
- **Mapbox**: High-quality map tiles
- **Folium**: Interactive map generation
- **LightGBM**: Fast gradient boosting
- **Streamlit**: Rapid app development
- **U.S. Census Bureau**: Demographic data

---

**PPM** - Predicting psychographic patterns, one location at a time. 🎯🧠📍

## 🔍 Architecture Deep Dive

### Why This Architecture?

**Problem with Original Structure:**

- 50+ files across 8+ directories
- Complex orchestration layer
- Tight coupling between components
- Difficult to understand data flow

**Solution - 4 Feature Services:**

```
features/
├── venues.py     (500 lines) - Complete venue collection
├── events.py     (800 lines) - LLM-powered event scraping
├── predictions.py (600 lines) - ML training & inference
└── maps.py       (800 lines) - Interactive visualizations
```

**Benefits:**

- ✅ Each file is self-contained and complete
- ✅ Clear, single-responsibility services
- ✅ No orchestration layer needed
- ✅ Easy to understand and maintain
- ✅ Direct imports with clean interfaces

### Service Architecture Pattern

Each service follows this pattern:

```python
class ServiceName:
    def __init__(self):
        # Initialize dependencies
        self.db = get_database()
        self.quality_validator = get_quality_validator()

    # ========== PUBLIC API METHODS ==========
    def collect_all(self) -> OperationResult:
        """Main public interface"""
        pass

    def get_data(self, filters: Dict) -> List[Dict]:
        """Query interface"""
        pass

    # ========== PRIVATE IMPLEMENTATION ==========
    def _internal_method(self):
        """Implementation details"""
        pass

# Global singleton
_service_instance = None

def get_service() -> ServiceName:
    """Factory function"""
    global _service_instance
    if _service_instance is None:
        _service_instance = ServiceName()
    return _service_instance
```

### Data Flow

```
User Input (app.py)
    ↓
Feature Services (venues.py, events.py, predictions.py, maps.py)
    ↓
Core Infrastructure (database.py, quality.py)
    ↓
SQLite Database (ppm.db)
    ↓
Visualization (maps.py) → HTML Output
```

## 🚦 Getting Started Checklist

- [ ] Install Python 3.9+
- [ ] Clone repository
- [ ] Install dependencies (`pip install -r requirements.txt`)
- [ ] Copy `.env.example` to `.env`
- [ ] Add `CHATGPT_API_KEY` to `.env` (required for event scraping)
- [ ] Add `MAPBOX_ACCESS_TOKEN` to `.env` (optional, for better maps)
- [ ] Run `python setup_database.py`
- [ ] Run `python app.py`
- [ ] Click "🚀 Collect All Data" in sidebar
- [ ] View results on interactive map

First-time setup takes ~5 minutes total.

## 💡 Tips & Tricks

### Optimizing Data Collection

```python
# Collect only what you need
venues = get_venue_service()
result = venues.collect_from_scraped_sources()  # Fastest, no API keys needed

# Use filters to reduce data volume
events = get_event_service()
filtered_events = events.get_events({
    'start_date': '2025-11-01',
    'end_date': '2025-11-30',
    'has_location': True
})
```

### Improving ML Predictions

```python
# Train model more frequently
predictions = get_prediction_service()
predictions.train_model(retrain=True)

# Check model performance
summary = predictions.get_prediction_summary()
print(f"Avg Confidence: {summary['avg_confidence']:.1%}")
```

### Custom Map Styling

```python
from features.maps import get_map_service, MapConfig

config = MapConfig(
    center_coords=(39.0997, -94.5786),
    zoom_level=13,
    style='satellite'  # or 'streets', 'dark', 'light'
)

maps = get_map_service(config)
```

## 📚 Additional Resources

- **API Documentation**: See inline docstrings in each feature file
- **Database Schema**: Run `python setup_database.py` to see full schema
- **Example Notebooks**: Coming soon in `/notebooks` directory
- **Video Tutorials**: Coming soon

## ⚠️ Known Limitations

- **LLM Event Scraping**: Requires OpenAI API key (costs ~$0.01 per venue)
- **ML Model**: Limited by small initial training dataset (improves over time)
- **Real-time Data**: Some enrichment APIs (foot traffic, social) are optional
- **Geographic Scope**: Currently optimized for Kansas City area only
- **Browser Compatibility**: Maps work best in Chrome/Firefox

## 🔮 Future Enhancements

- [ ] Add more external API integrations (Ticketmaster, Eventbrite)
- [ ] Implement user feedback loop for ground truth labels
- [ ] Add recommendation engine for venue suggestions
- [ ] Build mobile app interface
- [ ] Add time series forecasting for attendance trends
- [ ] Expand to other cities beyond Kansas City
- [ ] Add A/B testing framework for model improvements
