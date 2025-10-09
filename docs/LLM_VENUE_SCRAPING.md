# LLM-Based Venue Scraping Documentation

## Overview

The PPM application now uses an advanced LLM-based venue scraping system that leverages OpenAI's GPT models to intelligently extract venue information from web pages. This approach replaces the previous mix of static scraping and Selenium-based dynamic scraping with a unified, intelligent solution.

## Key Features

### 🧠 Intelligent Data Extraction

- Uses OpenAI GPT-4o-mini for robust venue information extraction
- Handles both static HTML and dynamic JavaScript content uniformly
- Adapts to different website layouts and structures automatically
- Extracts structured data including name, address, phone, description, and venue type

### 🌐 Comprehensive Coverage

- Scrapes **29 Kansas City venue sources** including:
  - Major venues (T-Mobile Center, Kauffman Center, etc.)
  - Entertainment districts (Power & Light, Westport, etc.)
  - Museums and cultural sites
  - Theater venues
  - Festival and city sources
  - Event aggregators
  - Nightlife venues

### 🔄 Hybrid Scraping Approach

- **Static scraping** for traditional HTML sites
- **Playwright browser automation** for dynamic JavaScript sites
- **Automatic fallback** from dynamic to static when needed

### 🎯 Smart Content Processing

- Converts HTML to clean markdown for LLM processing
- Removes unnecessary elements (scripts, styles, navigation)
- Limits content size to 15,000 characters for cost efficiency
- Structured JSON output with validation

## Architecture

### Core Components

```
VenueService
├── OpenAI Client (LLM extraction)
├── Playwright Browser (dynamic content)
├── Static HTML Fetcher (traditional scraping)
├── HTML to Markdown Converter
├── Psychographic Analyzer
└── Database Integration
```

### Data Flow

1. **Venue Configuration** → Define 29 KC venue sources with URLs and types
2. **Content Fetching** → Use appropriate method (static/dynamic) per venue
3. **LLM Processing** → Extract structured venue data using GPT-4o-mini
4. **Validation** → Quality check extracted data
5. **Storage** → Store in unified database with psychographic scores

## Configuration

### Environment Variables

```bash
# Required for LLM extraction
CHATGPT_API_KEY=your_openai_api_key_here

# Optional for enhanced dynamic scraping
PLAYWRIGHT_BROWSERS=chromium
```

### Venue Source Configuration

Each venue source is configured with:

```python
{
    "name": "Venue Display Name",
    "url": "https://venue-website.com/events",
    "category": "venue_category",
    "scrape_type": "static|dynamic",
    "wait_selector": ".event-card"  # For dynamic sites
}
```

## Usage

### Basic Venue Collection

```python
from features.venues import get_venue_service

venue_service = get_venue_service()

# Collect from all 29 KC sources
result = venue_service.collect_from_scraped_sources()

if result.success:
    print(f"Collected {result.data} venues")
else:
    print(f"Collection failed: {result.error}")
```

### Retrieve Venues

```python
# Get all venues
venues = venue_service.get_venues()

# Get venues with filters
filtered_venues = venue_service.get_venues(
    filters={"category": "major_venue", "has_location": True},
    limit=10
)

# Get venues with ML predictions
venues_with_predictions = venue_service.get_venues_with_predictions()
```

## LLM Extraction Process

### Prompt Engineering

The system uses carefully crafted prompts to extract venue information:

```
Extract venue information from this {venue_name} webpage.

Return a JSON object with this EXACT structure:
{
  "name": "Venue name",
  "type": "Venue type/subcategory",
  "address": "Full address",
  "phone": "Phone number",
  "description": "Brief description (max 200 chars)",
  "lat": null,
  "lng": null
}

Rules:
- Use null for missing fields
- Extract only the main venue information, not events
- Keep description brief and factual
- Phone should be formatted like +1-816-555-0000
- Address should be complete with city and zip
- Return ONLY valid JSON object
```

### Response Processing

- **JSON Validation**: Ensures valid JSON structure
- **Field Validation**: Checks required fields are present
- **Fallback Handling**: Uses venue config name if extraction fails
- **Error Recovery**: Continues processing other venues on individual failures

## Psychographic Analysis

The system automatically calculates psychographic scores for each venue based on content analysis:

### Psychographic Categories

- **Career Driven**: Professional, business, networking events
- **Competent**: Expert, training, skill-building activities
- **Fun**: Entertainment, music, celebration, nightlife
- **Social**: Community, meetups, group activities
- **Adventurous**: Outdoor, extreme, challenge activities

### Scoring Algorithm

```python
def _calculate_venue_psychographics(self, name: str, description: str) -> Dict:
    text = f"{name} {description}".lower()
    scores = {}

    for psychographic, keywords in self.psychographic_keywords.items():
        score = sum(1 for keyword in keywords if keyword in text)
        scores[psychographic] = min(score / len(keywords), 1.0)

    return scores
```

## Performance & Cost Management

### Optimization Strategies

- **Content Truncation**: Limits HTML content to 15,000 characters
- **Respectful Delays**: 2-second delays between requests
- **Efficient Model**: Uses GPT-4o-mini for cost-effectiveness
- **Caching**: Avoids re-scraping unchanged venues
- **Batch Processing**: Processes venues sequentially with progress tracking

### Cost Estimates

- **Per Venue**: ~$0.001-0.003 per venue extraction
- **Full Collection**: ~$0.03-0.09 for all 29 venues
- **Monthly**: ~$1-3 for daily collection runs

## Error Handling & Resilience

### Failure Recovery

- **Individual Venue Failures**: Continue processing other venues
- **Network Timeouts**: Retry with exponential backoff
- **LLM API Errors**: Log and continue with next venue
- **Validation Failures**: Store with warnings, don't block process

### Monitoring & Logging

```python
# Comprehensive logging at each stage
self.logger.info(f"[{idx}/{total}] Scraping {venue_name}...")
self.logger.info(f"✅ Successfully scraped {venue_name}")
self.logger.warning(f"⚠️ Failed to fetch HTML for {venue_name}")
self.logger.error(f"❌ Error scraping {venue_key}: {e}")
```

### Health Metrics

- **Success Rate**: Tracks successful vs failed extractions
- **Processing Time**: Monitors performance per venue
- **Data Quality**: Validates extracted information
- **API Usage**: Monitors OpenAI API consumption

## Testing

### Test Suite

Run the comprehensive test suite:

```bash
python test_llm_venue_scraping.py
```

### Test Coverage

1. **OpenAI Connection**: Verifies API key and client initialization
2. **Single Venue Scraping**: Tests HTML fetching and LLM extraction
3. **Venue Storage**: Validates database integration
4. **Limited Collection**: Tests end-to-end collection process

### Expected Results

```
🚀 Starting LLM Venue Scraping Tests
📊 Test Results: 4/4 tests passed
🎉 All tests passed! LLM venue scraping is ready.
```

## Migration from Previous System

### What Changed

- **Removed**: Selenium-based dynamic scraping complexity
- **Replaced**: Static HTML parsing with LLM intelligence
- **Enhanced**: Data extraction quality and consistency
- **Simplified**: Single unified scraping approach

### Backward Compatibility

- **API Interface**: Unchanged public methods
- **Database Schema**: Compatible with existing venue structure
- **Data Format**: Same VenueData structure and validation

### Migration Steps

1. **Deploy**: New LLM-based venue service
2. **Test**: Run limited collection to verify functionality
3. **Monitor**: Check data quality and extraction success rates
4. **Scale**: Gradually increase to full 29-venue collection

## Troubleshooting

### Common Issues

#### OpenAI API Key Not Found

```
❌ OpenAI client not initialized. Check CHATGPT_API_KEY in .env file.
```

**Solution**: Ensure `CHATGPT_API_KEY` is set in your `.env` file

#### Venue Extraction Failed

```
❌ Failed to extract venue information using LLM
```

**Solutions**:

- Check venue website accessibility
- Verify HTML content is meaningful
- Review LLM prompt effectiveness

#### High API Costs

**Solutions**:

- Reduce collection frequency
- Implement smarter caching
- Use content truncation more aggressively

### Debug Mode

Enable detailed logging:

```python
import logging
logging.getLogger('features.venues').setLevel(logging.DEBUG)
```

## Future Enhancements

### Planned Improvements

1. **Smart Caching**: Detect venue content changes to avoid unnecessary re-scraping
2. **Confidence Scoring**: Add LLM confidence metrics to extraction results
3. **Multi-Model Support**: Support for different LLM providers (Anthropic, etc.)
4. **Real-time Updates**: WebSocket-based live venue monitoring
5. **Geographic Expansion**: Extend beyond Kansas City to other metropolitan areas

### Performance Optimizations

1. **Parallel Processing**: Concurrent venue scraping with rate limiting
2. **Incremental Updates**: Only scrape changed venues
3. **Content Fingerprinting**: Detect meaningful content changes
4. **Smart Retry Logic**: Exponential backoff for failed requests

## Support & Maintenance

### Monitoring Checklist

- [ ] Daily collection success rates > 80%
- [ ] Average processing time < 15 seconds per venue
- [ ] OpenAI API costs within budget
- [ ] Data quality scores > 0.7
- [ ] No critical errors in logs

### Regular Maintenance

- **Weekly**: Review failed venue extractions
- **Monthly**: Analyze cost trends and optimize
- **Quarterly**: Update venue source configurations
- **Annually**: Evaluate new LLM models and capabilities

---

_Last Updated: October 2025_
_Version: 1.0_
