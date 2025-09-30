# KC Event Scraper Map Generator Guide

## Overview

The KC Event Scraper Map Generator is a standalone tool that creates interactive maps focused on scraped Kansas City events. It uses your existing KC Event Scraper to collect event data and generates beautiful, interactive maps with detailed event information and psychographic scoring.

## Features

### 🎭 Event Data Collection

- **Smart Scraping**: Uses the KC Event Scraper to collect events from major KC venues
- **Caching Support**: Option to use previously scraped data for faster map generation
- **Venue Filtering**: Choose specific venues or scrape all available venues
- **Error Handling**: Graceful handling of failed venue scrapes

### 🗺️ Interactive Map Features

- **Event Markers**: Color-coded circular markers for individual events
- **Venue Clusters**: Larger markers showing venue locations with event counts
- **Category Layers**: Toggle different event types on/off
- **Interactive Popups**: Click events for detailed information
- **Multiple Map Styles**: OpenStreetMap and CartoDB Light options

### 📊 Data Visualization

- **Psychographic Scoring**: Events scored on Career-Driven, Competent, and Fun appeal
- **Color-Coded Markers**: Visual representation of event relevance scores
- **Statistics Panel**: Overview of total events, venues, and average scores
- **Legend**: Clear explanation of marker colors and categories

### 🎯 Event Categories

- **Major Venue**: Theaters, arenas, concert halls (🎭)
- **Entertainment District**: Nightlife areas, entertainment districts (🌃)
- **Aggregator**: Event listing sites and aggregators (📅)
- **Local Event**: Community and local events (🎪)

## Files

### Main Scripts

- `create_event_scraper_map_simple.py` - **Recommended**: Simple version using Folium directly
- `create_event_scraper_map.py` - Advanced version with full Interactive Map Builder integration

### Dependencies

- KC Event Scraper (`src/data_collectors/kc_event_scraper.py`)
- Folium for map generation
- Standard Python libraries (json, logging, pathlib, etc.)

## Usage

### Quick Start

```bash
# Run the simple version (recommended)
python create_event_scraper_map_simple.py
```

### Interactive Setup

The script will prompt you for:

1. **Cache Usage**: Use previously scraped data or fetch fresh events
2. **Venue Selection**:
   - All venues (default)
   - Major venues only (T-Mobile Center, Uptown Theater, etc.)
   - Custom selection (choose specific venues)
3. **Output File**: HTML filename (default: `kc_events_map.html`)

### Example Session

```
🎭 KC Event Scraper Map Generator (Simple)
==================================================
Use cached event data if available? (y/n): y

Venue Selection:
1. All venues (default)
2. Major venues only
3. Custom selection
Choose option (1-3): 1
Output filename (default: kc_events_map.html):

🚀 Starting event map generation...
  - Cached data: True
  - Venues: All
  - Output: kc_events_map.html

✅ Event map created successfully!
📍 Location: /Users/callumd/Desktop/PPM/kc_events_map.html
📊 Events: 86
🏢 Venues: 4

Open map in browser? (y/n): y

🎉 KC Event Map generation complete!
```

## Map Interface

### Legend (Bottom Left)

- **Event Categories**: Shows active event types with counts
- **Score Ranges**: Color coding explanation for psychographic scores
- **Statistics**: Total events and venues

### Info Panel (Top Right)

- **Data Overview**: Summary of scraped events and venues
- **Map Features**: Explanation of interactive elements
- **Psychographic Scoring**: Details on scoring methodology
- **Data Sources**: Information about scraped venues

### Interactive Elements

- **Event Markers**: Click for detailed event information including:
  - Event title and venue
  - Date, time, and price
  - Description and psychographic scores
  - Link to original event page (if available)
- **Venue Markers**: Click for venue statistics and event counts
- **Layer Control**: Toggle different event categories on/off

## Psychographic Scoring

Events are automatically scored based on appeal to three segments:

### Career-Driven (Professional/Networking)

- Business events, conferences, networking
- Professional development, workshops
- Industry meetups and seminars

### Competent (Educational/Skill-Building)

- Educational events, classes, workshops
- Cultural events, museum exhibitions
- Skill-building activities, tutorials

### Fun (Entertainment/Social)

- Concerts, shows, entertainment
- Social events, parties, festivals
- Sports events, recreational activities

**Score Visualization**:

- **High (0.8+)**: Large dark green markers
- **Med-High (0.6-0.8)**: Medium dark green markers
- **Medium (0.4-0.6)**: Medium green markers
- **Low-Med (0.2-0.4)**: Light green markers
- **Low (0-0.2)**: Very light green markers

## Venue Coverage

### Successfully Scraped Venues

- **T-Mobile Center**: Major arena events and concerts
- **Uptown Theater**: Live music and entertainment
- **Kauffman Center**: Classical music and performing arts
- **Do816**: Local event aggregator

### Venue Coordinates

The system includes approximate coordinates for major KC venues:

- T-Mobile Center: (39.1031, -94.5844)
- Uptown Theater: (39.0997, -94.5786)
- Kauffman Center: (39.0908, -94.5844)
- Starlight Theatre: (39.0331, -94.5708)
- And more...

## Technical Details

### Data Flow

1. **Event Collection**: KC Event Scraper gathers events from configured venues
2. **Data Enrichment**: Events enhanced with coordinates and psychographic scores
3. **Map Generation**: Folium creates interactive HTML map with layers
4. **Visualization**: Events displayed with color-coded markers and popups

### Caching

- Events cached in `kc_events_cache.json`
- Speeds up subsequent map generations
- Use cached data option to avoid re-scraping

### Error Handling

- Graceful handling of failed venue scrapes
- Continues processing even if some venues fail
- Detailed logging of scraping issues

## Customization

### Adding New Venues

Edit the venue coordinates in `_get_venue_coordinates()`:

```python
venue_coords = {
    "New Venue Name": (latitude, longitude),
    # ... existing venues
}
```

### Modifying Categories

Update `category_config` to change icons, colors, or descriptions:

```python
self.category_config = {
    "New Category": {
        "icon": "🎪",
        "color": "#custom_color",
        "description": "Custom description",
    },
}
```

### Map Styling

The simple version uses OpenStreetMap and CartoDB Light tiles. Additional tile layers can be added in the `create_event_map()` method.

## Troubleshooting

### Common Issues

**Import Errors**:

```bash
pip install folium
```

**No Events Collected**:

- Check internet connection
- Verify venue websites are accessible
- Some venues may have changed their event page structure

**Map Not Opening**:

- Check that the HTML file was created
- Try opening the file manually in a browser
- Verify file permissions

### Logging

The script provides detailed logging:

- Event collection progress
- Scraping errors and warnings
- Map generation status
- File creation confirmation

## Output

### Generated Files

- **HTML Map**: Interactive map file (default: `kc_events_map.html`)
- **Cache File**: `kc_events_cache.json` (if caching enabled)

### Map Features

- Responsive design works on desktop and mobile
- Zoom and pan functionality
- Layer controls for filtering
- Popup information panels
- Legend and help information

## Integration

### With Existing PPM System

The event scraper map generator integrates seamlessly with your existing PPM system:

- Uses the same KC Event Scraper
- Compatible with existing venue configurations
- Can be run alongside other mapping tools

### Standalone Usage

Can be used independently for:

- Event discovery and visualization
- Venue analysis and comparison
- Event planning and research
- Data exploration and insights

## Performance

### Optimization Tips

- Use cached data for faster generation
- Filter to specific venues for smaller maps
- Consider major venues only for focused analysis

### Resource Usage

- Memory usage scales with number of events
- Map file size typically 1-5MB depending on event count
- Browser performance good up to several hundred events

## Future Enhancements

### Potential Improvements

- Real-time event updates
- Event filtering by date range
- Advanced search and filtering
- Export functionality for event data
- Integration with calendar applications
- Mobile-optimized interface

### Data Enhancements

- Additional venue coverage
- Enhanced psychographic modeling
- Event popularity predictions
- Social media integration
- User rating and review system

---

## Quick Reference

### Commands

```bash
# Generate map with all venues
python create_event_scraper_map_simple.py

# Use cached data, all venues, default filename
# Answer: y, 1, [enter]
```

### Key Files

- `create_event_scraper_map_simple.py` - Main script
- `kc_events_map.html` - Generated map
- `kc_events_cache.json` - Cached event data

### Map Elements

- 🎭 Major Venue events
- 🌃 Entertainment District events
- 📅 Aggregator events
- 🎪 Local events
- 🏢 Venue cluster markers

The KC Event Scraper Map Generator provides a powerful, user-friendly way to visualize and explore Kansas City's event landscape using your existing scraping infrastructure.
