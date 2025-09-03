# Interactive Map Builder for PPM

This document provides comprehensive documentation for the Interactive Map Builder with Mapbox integration for the Psychographic Prediction Machine (PPM).

## Overview

The InteractiveMapBuilder is a powerful visualization tool that creates interactive maps with multiple data layers including:

- Event heatmaps with psychographic scores
- Probability density visualizations
- Combined multi-layer visualizations
- GeoJSON data export capabilities

## Features

### 🗺️ Map Styles

- **Mapbox Integration**: Full support for Mapbox tiles with multiple styles
- **Fallback Support**: Automatic fallback to OpenStreetMap when Mapbox token is unavailable
- **Style Options**: Streets, Satellite, Light, Dark, and Outdoors themes

### 📊 Visualization Types

1. **Event Heatmaps**: Display events with score-based styling
2. **Probability Heatmaps**: Show psychographic density distributions
3. **Combined Visualizations**: Multi-layer maps with events and probability data
4. **Interactive Elements**: Popups, tooltips, and layer controls

### 🎨 Styling Features

- Score-based marker sizing and coloring
- Gradient heatmaps with customizable color schemes
- Interactive legends and controls
- Responsive design for different screen sizes

## Installation

### Prerequisites

```bash
pip install folium>=0.20.0
pip install numpy>=1.24.0
pip install pandas>=2.0.0
```

### Mapbox Setup (Optional but Recommended)

1. Get a Mapbox access token from [mapbox.com](https://mapbox.com)
2. Set environment variable:
   ```bash
   export MAPBOX_ACCESS_TOKEN="pk.your_token_here"
   ```
3. Or add to your `.env` file:
   ```
   MAPBOX_ACCESS_TOKEN=pk.your_token_here
   ```

## Usage

### Basic Usage

```python
from src.backend.visualization.interactive_map_builder import InteractiveMapBuilder

# Initialize map builder
map_builder = InteractiveMapBuilder(center_coords=(39.0997, -94.5786))

# Sample events data
events_data = [
    {
        "latitude": 39.0997,
        "longitude": -94.5786,
        "name": "Jazz Night",
        "venue_name": "The Blue Room",
        "date": "2024-01-15",
        "total_score": 0.85
    }
]

# Create event heatmap
output_file = map_builder.create_event_heatmap(
    events_data=events_data,
    output_path="event_heatmap.html",
    style="streets"
)
```

### Advanced Usage

```python
# Probability data
probability_data = {
    (39.0997, -94.5786): 0.85,
    (39.1012, -94.5844): 0.72,
    (39.0739, -94.5861): 0.68
}

# Create combined visualization
output_file = map_builder.create_combined_visualization(
    events_data=events_data,
    probability_data=probability_data,
    output_path="combined_map.html",
    style="dark"
)

# Export to GeoJSON
geojson_file = map_builder.export_to_geojson(
    data=events_data,
    output_path="events.geojson"
)
```

## API Integration

### FastAPI Endpoints

The InteractiveMapBuilder is integrated with FastAPI endpoints:

#### 1. Generate Heatmap Visualization

```http
POST /api/v1/visualize/heatmap
Content-Type: application/json

{
    "grid_bounds": {
        "north": 39.15,
        "south": 39.05,
        "east": -94.50,
        "west": -94.65
    },
    "resolution_meters": 500,
    "timestamp": "2024-01-01T00:00:00",
    "style": "streets",
    "include_events": true,
    "include_probability": true
}
```

#### 2. Generate Events Visualization

```http
POST /api/v1/visualize/events
Content-Type: application/json

[
    {
        "latitude": 39.0997,
        "longitude": -94.5786,
        "name": "Jazz Night",
        "venue_name": "The Blue Room",
        "date": "2024-01-15",
        "total_score": 0.85
    }
]
```

#### 3. Get Sample Events

```http
GET /api/v1/visualize/sample-events?north=39.15&south=39.05&east=-94.50&west=-94.65&count=20
```

### Browser Integration

The Interactive Map Builder generates standalone HTML files that can be:

- Opened directly in any web browser
- Embedded in web applications using iframes
- Served through web servers for remote access
- Integrated with existing web dashboards

```python
# Generate and open visualization
output_file = map_builder.create_combined_visualization(
    events_data=events_data,
    probability_data=probability_data,
    output_path="psychographic_map.html"
)

# Automatically open in browser
map_builder.open_in_browser(output_file)
```

## Configuration

### Map Styles

Available styles:

- `streets`: Standard street map (default)
- `satellite`: Satellite imagery with street overlays
- `light`: Light theme for better readability
- `dark`: Dark theme for modern appearance
- `outdoors`: Optimized for outdoor activities

### Marker Styling

Score-based styling ranges:

- **High (0.8+)**: Large red markers (radius: 12)
- **Medium-High (0.6-0.8)**: Medium orange markers (radius: 10)
- **Medium (0.4-0.6)**: Medium yellow markers (radius: 8)
- **Low-Medium (0.2-0.4)**: Small light blue markers (radius: 6)
- **Low (0-0.2)**: Small blue markers (radius: 4)

### Heatmap Colors

Probability heatmap gradient:

- **0.0**: Navy (low probability)
- **0.3**: Blue
- **0.5**: Green
- **0.7**: Yellow
- **1.0**: Red (high probability)

## Testing

Run the test suite to verify functionality:

```bash
python test_visualization.py
```

This will:

1. Test basic InteractiveMapBuilder functionality
2. Verify API integration
3. Generate sample visualizations
4. Export test data to GeoJSON

## File Structure

```
src/backend/visualization/
├── __init__.py                    # Module initialization
└── interactive_map_builder.py     # Main InteractiveMapBuilder class

Generated Files:
├── event_heatmap.html            # Event visualization
├── probability_heatmap.html      # Probability visualization
├── combined_visualization.html   # Multi-layer visualization
└── export.geojson               # GeoJSON export
```

## Error Handling

The InteractiveMapBuilder includes comprehensive error handling:

- **Missing Mapbox Token**: Automatically falls back to OpenStreetMap
- **Invalid Data**: Validates input data and provides meaningful error messages
- **File I/O Errors**: Handles file creation and permission issues
- **Import Errors**: Graceful degradation when optional dependencies are missing

## Performance Considerations

### Optimization Tips

1. **Data Filtering**: Filter out low-significance data points (< 0.1 probability)
2. **Grid Resolution**: Use appropriate resolution for your use case (500m default)
3. **Batch Processing**: Process large datasets in chunks
4. **Caching**: Cache generated visualizations for repeated requests

### Memory Usage

- Large datasets (>10,000 points) may require additional memory
- Consider using data sampling for very large datasets
- Monitor memory usage during batch processing

## Troubleshooting

### Common Issues

#### 1. Mapbox Token Issues

```
Error: Invalid Mapbox token format
Solution: Ensure token starts with 'pk.' and is properly set in environment
```

#### 2. Import Errors

```
Error: No module named 'folium'
Solution: pip install folium>=0.20.0
```

#### 3. File Permission Errors

```
Error: Permission denied when saving file
Solution: Check write permissions for output directory
```

#### 4. Empty Visualizations

```
Error: No data points in visualization
Solution: Verify data format and coordinate validity
```

## Integration with Existing Components

### Data Pipeline Integration

Integrates with your existing ETL pipeline:

1. **Event Data**: Processes data from `src/etl/ingest_events.py`
2. **Venue Data**: Uses venue information from `src/etl/ingest_places.py`
3. **Feature Data**: Incorporates features from `src/features/build_features.py`

## Future Enhancements

### Planned Features

1. **Real-time Updates**: WebSocket integration for live data
2. **Custom Markers**: Support for custom marker icons and styles
3. **Animation Support**: Time-series animations
4. **3D Visualizations**: Height-based data representation
5. **Export Formats**: Additional export formats (PNG, SVG, PDF)

### Performance Improvements

1. **Clustering**: Marker clustering for large datasets
2. **Lazy Loading**: Progressive data loading
3. **Caching**: Intelligent caching strategies
4. **Compression**: Data compression for faster transfers

## Contributing

When contributing to the InteractiveMapBuilder:

1. **Follow Code Style**: Use the existing code formatting
2. **Add Tests**: Include tests for new functionality
3. **Update Documentation**: Keep this documentation current
4. **Performance Testing**: Test with large datasets
5. **Error Handling**: Include comprehensive error handling

## License

This InteractiveMapBuilder is part of the PDPE project and follows the same licensing terms.

---

For questions or support, please refer to the main PDPE documentation or contact the development team.
