# Enhanced Data Ingestion Guide

This guide covers the new data ingestion capabilities added to the PPM (Predictive Population Mapping) system, including social media data, economic indicators, traffic data, and local venue scrapers.

## Overview

The enhanced data ingestion system adds four major new data sources to improve the accuracy and coverage of population density predictions:

1. **Social Media Data** - Twitter and Facebook mentions with sentiment analysis
2. **Economic Indicators** - Local unemployment, consumer confidence, business health
3. **Traffic Data** - Real-time congestion and travel times via Google Maps
4. **Local Venue Events** - Web scrapers for KC-specific event sources

## New Data Sources

### 1. Social Media Integration (`ingest_social.py`)

**Purpose**: Capture social sentiment and engagement around venues and events in Kansas City.

**Data Sources**:

- Twitter API (mentions, hashtags, sentiment)
- Facebook Graph API (posts, engagement)

**Key Features**:

- Psychographic keyword classification (career_driven, competent, fun)
- Sentiment analysis using TextBlob
- Venue-specific and general KC social data
- Real-time social engagement scoring

**Configuration**:

```bash
# Required API Keys
TWITTER_API_KEY=your_twitter_api_key
FACEBOOK_API_KEY=your_facebook_api_key
```

**Usage**:

```python
from etl.ingest_social import ingest_social_data_for_venues, ingest_general_kc_social_data

# Ingest venue-specific social data
ingest_social_data_for_venues()

# Ingest general Kansas City social data
ingest_general_kc_social_data()
```

### 2. Economic Indicators (`ingest_econ.py`)

**Purpose**: Provide economic context for population density predictions.

**Data Sources**:

- Federal Reserve Economic Data (FRED) API
- Bureau of Labor Statistics (BLS) API
- News API for business sentiment

**Key Metrics**:

- Unemployment rates (KC Metro)
- Consumer confidence indices
- Retail sales data
- Business news sentiment analysis

**Configuration**:

```bash
# Required API Keys
ECONOMIC_DATA_API_KEY=your_fred_api_key
BUSINESS_NEWS_API_KEY=your_news_api_key
```

**Usage**:

```python
from etl.ingest_econ import ingest_economic_indicators

# Ingest all economic indicators
ingest_economic_indicators()
```

### 3. Traffic Data (`ingest_traffic.py`)

**Purpose**: Analyze venue accessibility and traffic patterns for population flow prediction.

**Data Sources**:

- Google Maps Directions API
- Google Maps Distance Matrix API

**Key Features**:

- Real-time congestion scoring (0-1 scale)
- Travel time calculations to downtown KC
- Traffic index (actual vs. free-flow time)
- Highway condition monitoring
- Venue accessibility scoring

**Configuration**:

```bash
# Required API Keys (either one works)
TRAFFIC_API_KEY=your_google_maps_api_key
GOOGLE_MAPS_API_KEY=your_google_maps_api_key
```

**Usage**:

```python
from etl.ingest_traffic import ingest_traffic_data

# Ingest all traffic data
ingest_traffic_data()
```

### 4. Local Venue Scrapers (`ingest_local_venues.py`)

**Purpose**: Capture local KC events not covered by major platforms like PredictHQ.

**Data Sources**:

- VisitKC.com
- Do816.com
- The Pitch KC
- Country Club Plaza
- Power & Light District

**Key Features**:

- Respectful web scraping with rate limiting
- Event date parsing and validation
- Psychographic event classification
- Automatic venue creation and matching
- Event deduplication

**Configuration**:
No API keys required - uses web scraping with proper headers.

**Usage**:

```python
from etl.ingest_local_venues import ingest_local_venue_data, scrape_specific_venue

# Scrape all configured venues
ingest_local_venue_data()

# Scrape a specific venue
scrape_specific_venue('visitkc')
```

## Database Schema

The enhanced ingestion system uses the following new tables:

### `social_sentiment`

```sql
CREATE TABLE social_sentiment (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  event_id UUID REFERENCES events(event_id),
  ts TIMESTAMP,
  platform TEXT,
  mention_count INT,
  positive_sentiment FLOAT,
  negative_sentiment FLOAT,
  neutral_sentiment FLOAT,
  engagement_score FLOAT,
  psychographic_keywords TEXT[],
  created_at TIMESTAMP DEFAULT now()
);
```

### `economic_data`

```sql
CREATE TABLE economic_data (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  ts TIMESTAMP,
  geographic_area TEXT,
  unemployment_rate FLOAT,
  median_household_income FLOAT,
  business_openings INT,
  business_closures INT,
  consumer_confidence FLOAT,
  local_spending_index FLOAT,
  created_at TIMESTAMP DEFAULT now()
);
```

### `traffic_data`

```sql
CREATE TABLE traffic_data (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  congestion_score FLOAT,
  travel_time_to_downtown FLOAT,
  travel_time_index FLOAT,
  source TEXT,
  created_at TIMESTAMP DEFAULT now()
);
```

## Prefect Flows

The enhanced system includes several new Prefect flows for orchestrating data ingestion:

### Available Flows

1. **`comprehensive_data_ingestion_flow()`** - All data sources
2. **`daily_flow()`** - Full daily pipeline with ML training
3. **`hourly_data_flow()`** - Time-sensitive data only
4. **`weekly_comprehensive_flow()`** - Extended feature building
5. **`social_and_economic_flow()`** - Social and economic data only
6. **`local_events_flow()`** - Local venue events only

### Flow Scheduling Recommendations

```python
# Hourly: Time-sensitive data
# - Social media data
# - Traffic conditions
# - Weather data
# - Foot traffic data

# Daily: Comprehensive ingestion
# - All data sources
# - Feature building
# - Model training

# Weekly: Full retraining
# - Extended feature windows
# - Complete model retraining
```

## Installation and Setup

### 1. Install Dependencies

Add the following to your `requirements.txt`:

```txt
# Social media and text processing
tweepy>=4.14.0
textblob>=0.17.1
python-dateutil>=2.8.2

# Web scraping
beautifulsoup4>=4.12.0
requests>=2.31.0

# Google Maps integration
googlemaps>=4.10.0

# Existing dependencies...
```

### 2. Configure API Keys

Update your `.env` file:

```bash
# Social Media APIs
TWITTER_API_KEY=your_twitter_api_key
FACEBOOK_API_KEY=your_facebook_api_key

# Economic Data APIs
ECONOMIC_DATA_API_KEY=your_fred_api_key
BUSINESS_NEWS_API_KEY=your_news_api_key

# Traffic APIs
TRAFFIC_API_KEY=your_google_maps_api_key
GOOGLE_MAPS_API_KEY=your_google_maps_api_key
```

### 3. Run Database Migrations

Ensure your database has the required tables by running the migrations:

```bash
psql -d your_database -f src/db/migrations.sql
```

### 4. Test the Installation

Run the comprehensive test suite:

```bash
python test_enhanced_data_ingestion.py
```

## Usage Examples

### Manual Data Ingestion

```python
# Individual data source ingestion
from etl.ingest_social import ingest_social_data_for_venues
from etl.ingest_econ import ingest_economic_indicators
from etl.ingest_traffic import ingest_traffic_data
from etl.ingest_local_venues import ingest_local_venue_data

# Run individual ingestion tasks
ingest_social_data_for_venues()
ingest_economic_indicators()
ingest_traffic_data()
ingest_local_venue_data()
```

### Prefect Flow Execution

```python
from src.infra.prefect_flows import daily_flow, hourly_data_flow

# Run daily comprehensive flow
daily_flow()

# Run hourly time-sensitive data flow
hourly_data_flow()
```

### Specific Venue Scraping

```python
from etl.ingest_local_venues import scrape_specific_venue

# Scrape specific venues
scrape_specific_venue('visitkc')
scrape_specific_venue('do816')
scrape_specific_venue('thepitchkc')
```

## API Rate Limits and Best Practices

### Twitter API

- **Rate Limit**: 300 requests per 15-minute window
- **Best Practice**: Use small time windows (1 day) for frequent updates
- **Fallback**: Graceful degradation if API key is missing

### Google Maps API

- **Rate Limit**: 40,000 requests per month (free tier)
- **Best Practice**: Limit venue processing to 50 venues per run
- **Cost Management**: Use traffic data sparingly for cost control

### Web Scraping

- **Rate Limit**: 2-second delays between venue scrapes
- **Best Practice**: Respectful scraping with proper headers
- **Error Handling**: Continue processing if individual sites fail

### Economic APIs

- **FRED API**: 120 requests per minute
- **News API**: 1,000 requests per day (free tier)
- **Best Practice**: Cache results and update daily

## Monitoring and Troubleshooting

### Health Checks

```python
# Test all data sources
python test_enhanced_data_ingestion.py

# Test specific components
from etl.ingest_social import setup_twitter_api
twitter_api = setup_twitter_api()
print("Twitter API:", "✅" if twitter_api else "❌")
```

### Common Issues

1. **Missing API Keys**

   - Check `.env` file configuration
   - Verify API key validity
   - Some features gracefully degrade without keys

2. **Database Connection Issues**

   - Ensure PostgreSQL is running
   - Check `DATABASE_URL` in `.env`
   - Verify table schema is up to date

3. **Web Scraping Failures**

   - Website structure changes may break scrapers
   - Check selector configurations in `VENUE_SCRAPERS`
   - Monitor for HTTP errors and timeouts

4. **API Quota Exceeded**
   - Reduce frequency of data ingestion
   - Implement caching for expensive API calls
   - Consider upgrading to paid API tiers

### Logging

All ingestion scripts use Python's logging module:

```python
import logging
logging.basicConfig(level=logging.INFO)

# Logs will show:
# - API connection status
# - Data processing progress
# - Error messages and warnings
# - Performance metrics
```

## Performance Optimization

### Parallel Processing

The Prefect flows support parallel execution of independent tasks:

```python
# These can run in parallel
ingest_places_task()
ingest_events_task()
ingest_weather_data_task()
ingest_foot_traffic_data_task()
```

### Caching Strategies

1. **Economic Data**: Cache daily, update once per day
2. **Social Data**: Cache hourly, update every 2-4 hours
3. **Traffic Data**: Real-time, cache for 15-30 minutes
4. **Local Events**: Cache daily, update once per day

### Database Optimization

```sql
-- Recommended indexes for performance
CREATE INDEX idx_social_sentiment_venue_ts ON social_sentiment(venue_id, ts);
CREATE INDEX idx_economic_data_ts_area ON economic_data(ts, geographic_area);
CREATE INDEX idx_traffic_data_venue_ts ON traffic_data(venue_id, ts);
```

## Integration with ML Pipeline

The enhanced data sources integrate seamlessly with the existing ML pipeline:

### Feature Engineering

New features are automatically available in the `features` table:

- `social_mention_count`
- `social_sentiment_score`
- `social_engagement_score`
- `unemployment_rate`
- `consumer_confidence`
- `traffic_congestion_score`
- `travel_time_downtown`

### Model Training

The enhanced features improve model performance by providing:

1. **Social Context**: Real-time sentiment and engagement
2. **Economic Context**: Local economic health indicators
3. **Accessibility Context**: Traffic and transportation factors
4. **Event Coverage**: Comprehensive local event data

## Future Enhancements

### Planned Improvements

1. **Real-time Streaming**: Kafka integration for real-time social data
2. **Advanced NLP**: BERT-based sentiment analysis
3. **Geospatial Analysis**: Traffic pattern clustering
4. **Event Prediction**: ML-based event attendance forecasting

### Extensibility

The system is designed for easy extension:

1. **New Social Platforms**: Add Instagram, LinkedIn scrapers
2. **Additional Economic Sources**: Local government APIs
3. **More Venue Sources**: Eventbrite, Meetup integration
4. **Enhanced Traffic**: Waze API integration

## Support and Maintenance

### Regular Maintenance Tasks

1. **Weekly**: Review API usage and costs
2. **Monthly**: Update web scraper selectors
3. **Quarterly**: Evaluate new data sources
4. **Annually**: Review API key renewals

### Getting Help

For issues with the enhanced data ingestion system:

1. Check the test suite output: `python test_enhanced_data_ingestion.py`
2. Review logs for specific error messages
3. Verify API key configuration and quotas
4. Check database schema compatibility

The enhanced data ingestion system significantly improves the PPM system's ability to predict population density by incorporating real-time social, economic, traffic, and local event data. This comprehensive approach provides a more accurate and nuanced understanding of Kansas City's dynamic population patterns.
