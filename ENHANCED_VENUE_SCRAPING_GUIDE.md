# Enhanced Venue Scraping System for Kansas City Events

## Overview

This enhanced venue scraping system provides comprehensive coverage of Kansas City's event landscape, integrating 27+ venues across multiple categories with advanced data quality controls and seamless database integration.

## System Architecture

### Core Components

1. **Static HTML Scrapers** (`src/etl/ingest_local_venues.py`)

   - Handles 22 venues with static HTML content
   - BeautifulSoup-based parsing
   - Configurable CSS selectors per venue

2. **Dynamic JavaScript Scrapers** (`src/etl/ingest_dynamic_venues.py`)

   - Handles 6 venues requiring JavaScript rendering
   - Selenium WebDriver with Chrome
   - Lazy loading and scroll support

3. **Data Quality Pipeline** (`src/etl/data_quality.py`)

   - Event validation and cleaning
   - Duplicate detection and removal
   - Database duplicate filtering
   - Quality metrics logging

4. **Database Integration**
   - Seamless integration with existing PPM database schema
   - Venue categorization and relationship management
   - Psychographic event classification

## Venue Coverage

### Static HTML Venues (22 venues)

#### Major Venues (7)

- T-Mobile Center
- Uptown Theater
- Kauffman Center for the Performing Arts
- Starlight Theatre
- The Midland Theatre
- Knuckleheads Saloon
- Azura Amphitheater

#### Entertainment Districts (4)

- Power & Light District
- Westport KC
- 18th & Vine Jazz District
- Crossroads KC

#### Shopping & Cultural (3)

- Country Club Plaza
- Crown Center
- Union Station Kansas City

#### Museums (3)

- Nelson-Atkins Museum of Art
- National WWI Museum
- Science City

#### Theaters (2)

- KC Repertory Theatre
- Unicorn Theatre

#### Festival & City (4)

- Kansas City Parks & Rec
- City Market KC
- Boulevardia Festival
- Irish Fest KC

### Dynamic JavaScript Venues (6 venues)

#### Aggregators (5)

- Visit KC
- Do816
- The Pitch KC
- Kansas City Magazine Events
- Event KC

#### Nightlife (1)

- Aura KC Nightclub

## Features

### Data Quality Controls

1. **Event Validation**

   - Required field validation
   - Date range validation
   - Content quality checks
   - Placeholder detection

2. **Duplicate Detection**

   - Title similarity analysis
   - Venue and date matching
   - Cross-source deduplication
   - Database duplicate filtering

3. **Data Cleaning**
   - HTML tag removal
   - Text normalization
   - Whitespace cleanup
   - HTML entity decoding

### Psychographic Classification

Events are automatically classified using keyword analysis:

- **Career Driven**: networking, professional, business, conference
- **Competent**: expert, masterclass, training, certification
- **Fun**: party, celebration, festival, concert, nightlife

### Event Categorization

Automatic subcategory detection:

- Music, Food, Art, Business, Sports
- Family, Nightlife, Cultural, Shopping, Outdoor

## Usage

### Running Static Venue Scraping

```bash
# Scrape all static venues
python -m src.etl.ingest_local_venues

# Scrape specific venue
python -c "from src.etl.ingest_local_venues import scrape_specific_venue; scrape_specific_venue('tmobile_center')"
```

### Running Dynamic Venue Scraping

```bash
# Scrape all dynamic venues (requires Chrome WebDriver)
python -m src.etl.ingest_dynamic_venues

# Scrape specific dynamic venue
python -c "from src.etl.ingest_dynamic_venues import scrape_specific_dynamic_venue; scrape_specific_dynamic_venue('visitkc_dynamic')"
```

### Testing the System

```bash
# Run comprehensive test suite
python test_enhanced_venue_scraping.py
```

## Configuration

### Adding New Static Venues

Add to `VENUE_SCRAPERS` in `src/etl/ingest_local_venues.py`:

```python
"new_venue_key": {
    "name": "Venue Name",
    "base_url": "https://venue.com",
    "events_url": "https://venue.com/events",
    "category": "major_venue",  # or appropriate category
    "selectors": {
        "event_container": ".event-item",
        "title": ".event-title",
        "date": ".event-date",
        "venue": ".venue",
        "description": ".description",
        "link": "a",
    },
}
```

### Adding New Dynamic Venues

Add to `DYNAMIC_VENUE_SCRAPERS` in `src/etl/ingest_dynamic_venues.py`:

```python
"new_dynamic_venue": {
    "name": "Dynamic Venue Name",
    "base_url": "https://dynamicvenue.com",
    "events_url": "https://dynamicvenue.com/events",
    "category": "aggregator",
    "wait_for": ".event-item",  # Element to wait for
    "selectors": {
        "event_container": ".event-item",
        "title": ".event-title",
        "date": ".event-date",
        "venue": ".venue",
        "description": ".description",
        "link": "a",
    },
    "scroll_to_load": True,  # Enable lazy loading
    "max_scrolls": 3,
}
```

## Database Schema Integration

### Venues Table

Events are linked to venues through the existing `venues` table with proper categorization:

```sql
INSERT INTO venues (external_id, provider, name, category)
VALUES ('provider_venue_name', 'provider', 'Venue Name', 'major_venue');
```

### Events Table

Events are stored with full psychographic and categorization data:

```sql
INSERT INTO events (
    external_id, provider, name, description, category, subcategory,
    start_time, end_time, venue_id, psychographic_relevance
) VALUES (...);
```

### Quality Metrics

Scraping performance is tracked in the `scraping_metrics` table:

```sql
INSERT INTO scraping_metrics (
    venue_provider, scrape_timestamp, events_found, events_new,
    events_updated, success, error_message
) VALUES (...);
```

## Monitoring and Maintenance

### Quality Metrics

The system automatically logs:

- Events found vs. processed
- Validation error rates
- Duplicate detection rates
- Scraping success/failure rates

### Error Handling

- Graceful failure handling per venue
- Detailed error logging
- Retry mechanisms for transient failures
- Rate limiting to respect website policies

### Performance Considerations

- **Static Scraping**: ~2 seconds delay between venues
- **Dynamic Scraping**: ~5 seconds delay between venues
- **Memory Usage**: Optimized for large event datasets
- **Database Load**: Efficient upsert operations

## Dependencies

### Python Packages

```
requests>=2.25.1
beautifulsoup4>=4.9.3
selenium>=4.0.0
python-dateutil>=2.8.2
psycopg2-binary>=2.9.0
```

### System Requirements

- Chrome browser (for dynamic scraping)
- ChromeDriver (automatically managed by Selenium)
- PostgreSQL database with PostGIS extension

## Integration with PPM System

### Data Flow

1. **Scraping**: Events collected from 27+ venues
2. **Quality Control**: Validation, cleaning, deduplication
3. **Database Storage**: Integration with existing PPM schema
4. **Feature Engineering**: Events feed into ML pipeline
5. **Prediction**: Enhanced population modeling with event data

### Psychographic Integration

Events are classified and stored with psychographic relevance scores that integrate directly with the PPM system's psychographic layers for:

- Career-driven population modeling
- Competent demographic targeting
- Fun-seeking behavior prediction

## Troubleshooting

### Common Issues

1. **Chrome WebDriver Issues**

   ```bash
   # Update Chrome and ChromeDriver
   pip install --upgrade selenium
   ```

2. **Database Connection Issues**

   ```bash
   # Check DATABASE_URL environment variable
   echo $DATABASE_URL
   ```

3. **Venue Scraping Failures**
   - Check website structure changes
   - Update CSS selectors if needed
   - Verify rate limiting compliance

### Debugging

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Future Enhancements

### Planned Features

1. **Machine Learning Selectors**: Auto-adapt to website changes
2. **Real-time Monitoring**: Live venue scraping status dashboard
3. **API Integration**: Direct venue API connections where available
4. **Mobile App Events**: Integration with venue mobile apps
5. **Social Media Events**: Facebook/Instagram event scraping

### Scalability Improvements

1. **Distributed Scraping**: Multi-worker venue processing
2. **Caching Layer**: Redis-based event caching
3. **Queue System**: Celery-based task management
4. **Auto-scaling**: Cloud-based scraping infrastructure

## Support

For issues or questions:

1. Check the test suite: `python test_enhanced_venue_scraping.py`
2. Review logs in the scraping_metrics table
3. Validate venue configurations
4. Test individual venues before full deployment

---

**Last Updated**: December 2024  
**Version**: 1.0  
**Maintainer**: PPM Development Team
