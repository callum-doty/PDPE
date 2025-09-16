# Comprehensive Venue & Event Map System

## Overview

This system creates interactive maps that display all venues and events from your various APIs and scraped data sources. The maps include multiple toggleable layers for different data types and provide comprehensive visualization of your predictive population modeling data.

## 🗺️ Generated Maps

### Main Map Files

- **`unified_venue_event_map.html`** - Comprehensive map with all data sources
- **`real_venue_comprehensive_map.html`** - Venue-focused map (from existing system)

## 📊 Data Sources Integrated

### API Data Layers (Blue Color Scheme)

- **🏢 Venues/Places** - Google Places API data with psychographic scoring
- **🎪 Events** - PredictHQ API and local scraped events
- **🌤️ Weather** - OpenWeatherMap API current conditions
- **🚶 Foot Traffic** - BestTime API venue traffic data
- **📱 Social Sentiment** - Twitter/Facebook mentions and engagement
- **🚗 Traffic** - Google Maps traffic and congestion data
- **💰 Economic** - FRED/BLS economic indicators

### Assumption Layers (Red/Orange Color Scheme)

- **🎓 College Density** - Calculated based on proximity to universities
- **💰 Spending Propensity** - Modeled based on demographic and location factors
- **⚙️ Custom Features** - Additional calculated psychographic layers

## 🚀 Quick Start

### Create Basic Map (Existing Data)

```bash
python create_unified_venue_event_map.py
```

### Create Comprehensive Map (With Sample Data)

```bash
python populate_and_create_comprehensive_map.py
```

### Show Existing Venues Only

```bash
python show_all_venues.py
```

## 🎛️ Map Features

### Interactive Elements

- **🏆 Venue Ranking Sidebar** - Browse all venues sorted by psychographic score
- **📍 Clickable Markers** - Detailed popups with venue/event information
- **🎛️ Layer Controls** - Toggle different data layers on/off
- **📊 Comprehensive Legend** - Color coding and data source explanation
- **ℹ️ Info Panel** - Usage guide and layer descriptions

### Data Visualization

- **Heatmaps** - Density visualization for venue concentrations
- **Scored Markers** - Color-coded by psychographic relevance
- **Multi-layer Display** - Separate toggleable layers for each data type
- **Real-time Data** - Current weather, traffic, and social sentiment

## 📋 Map Usage Guide

### Navigation

1. **Browse Venues**: Use the left sidebar to see all venues ranked by score
2. **Toggle Layers**: Use top-right controls to show/hide data types
3. **Get Details**: Click any marker for detailed information
4. **Center Map**: Click venue names in sidebar to center map on location

### Understanding Colors

- **Blue Markers**: API-sourced data (venues, events, weather, etc.)
- **Red/Orange Markers**: Calculated/modeled data (psychographic layers)
- **Marker Size**: Larger markers indicate higher psychographic scores
- **Heatmap Colors**: Red = high density, Blue = low density

## 🔧 System Architecture

### Core Components

```
create_unified_venue_event_map.py     # Main map generator
populate_and_create_comprehensive_map.py  # Data population + map creation
src/backend/visualization/interactive_map_builder.py  # Map building engine
```

### Data Flow

1. **ETL Scripts** → Database tables (venues, events, weather, etc.)
2. **Map Generator** → Queries database for all data sources
3. **Interactive Map Builder** → Creates layered Folium/Mapbox visualization
4. **HTML Output** → Interactive map with JavaScript controls

### Database Tables Used

- `venues` - Venue data with psychographic scores
- `events` - Event data with venue associations
- `weather_data` - Weather conditions by location/time
- `venue_traffic` - Foot traffic data per venue
- `social_sentiment` - Social media mentions and sentiment
- `traffic_data` - Traffic congestion and travel times
- `psychographic_layers` - Custom calculated layers
- `economic_data` - Economic indicators by area

## 📊 Data Integration

### Adding New Data Sources

1. Create ETL script in `src/etl/ingest_[source].py`
2. Add database table in `src/db/migrations.sql`
3. Update `fetch_[data_type]_data()` in map generator
4. Add layer visualization in `interactive_map_builder.py`

### Supported Data Types

- **Point Data**: Venues, events, weather stations
- **Time Series**: Traffic, foot traffic, social sentiment
- **Grid Data**: Psychographic layers, demographic data
- **Polygon Data**: Census tracts, neighborhoods (future)

## 🎯 Psychographic Scoring

### Venue Scoring

- **Career-Driven**: 50% weight - Professional relevance
- **Competent**: 30% weight - Skill/education focus
- **Fun**: 20% weight - Entertainment/social value

### Event Scoring

- **Career-Driven**: 40% weight - Professional networking
- **Competent**: 30% weight - Learning/skill development
- **Fun**: 30% weight - Entertainment/social aspects

## 🔍 Troubleshooting

### Common Issues

1. **No data showing**: Check database connection and run ETL scripts
2. **Map not opening**: Check file path and browser permissions
3. **Missing layers**: Verify data exists in database tables
4. **Performance issues**: Reduce data points or use data sampling

### Debug Commands

```bash
# Check venue data
python show_all_venues.py

# Test database connection
python -c "from src.etl.utils import get_db_conn; print('✅ DB Connected' if get_db_conn() else '❌ DB Failed')"

# Run specific ETL process
python -m src.etl.ingest_local_venues
```

## 🚀 Advanced Usage

### Custom Map Styles

The system supports multiple Mapbox styles:

- `streets` (default)
- `satellite`
- `light`
- `dark`
- `outdoors`

### API Integration

To populate with real API data:

1. Configure API keys in `.env` file
2. Run individual ETL scripts: `python -m src.etl.ingest_[source]`
3. Generate map: `python create_unified_venue_event_map.py`

### Performance Optimization

- Use data sampling for large datasets
- Implement caching for frequently accessed data
- Consider clustering for high-density areas
- Use progressive loading for time-series data

## 📈 Future Enhancements

### Planned Features

- **Real-time Updates**: Live data streaming and map updates
- **Predictive Overlays**: ML model predictions as map layers
- **User Interaction**: Click-to-predict functionality
- **Export Options**: PDF, PNG, and data export capabilities
- **Mobile Optimization**: Responsive design for mobile devices

### Integration Opportunities

- **CRM Systems**: Customer location and preference mapping
- **Marketing Platforms**: Campaign targeting visualization
- **Business Intelligence**: KPI dashboards with geographic context
- **Event Planning**: Optimal venue and timing recommendations

## 📞 Support

For issues or questions:

1. Check this guide and troubleshooting section
2. Review the generated map HTML for JavaScript errors
3. Verify database connectivity and data availability
4. Check ETL script logs for data ingestion issues

---

**Generated by**: Comprehensive Venue & Event Map System  
**Last Updated**: 2025-01-16  
**Version**: 2.0
