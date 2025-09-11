# Interactive Layered Heatmap Guide

## Overview

The enhanced InteractiveMapBuilder now supports creating interactive heatmaps with toggleable API and assumption layers. This allows you to visually compare real-time API data against calculated model predictions and assumptions.

## Key Features

### 🎛️ Interactive Layer Controls

- **Layer Control Panel**: Located in the top-right corner
- **Toggleable Groups**: Separate controls for API and assumption layers
- **Individual Layer Control**: Toggle specific layers on/off independently
- **Visual Feedback**: Layers show/hide in real-time

### 🎨 Visual Differentiation

- **API Layers**: Blue color scheme (darker blue = higher scores)
- **Assumption Layers**: Red/orange color scheme (darker red = higher scores)
- **Size Coding**: Larger markers indicate higher scores
- **Transparency**: Layers use appropriate opacity for better visibility

### 📊 Comprehensive Legend

- **Bottom-left panel** with color-coded layer explanations
- **Score intensity guide** showing size-to-score relationship
- **Data source attribution** for each layer type

### ℹ️ Information Panel

- **Top-right guide** explaining layer types and controls
- **Usage instructions** for interactive features
- **Color scheme explanation**

## Layer Types

### 📡 API Data Layers

Real-time data from external APIs:

- **🎪 Events (PredictHQ)**: Event locations and psychographic scores
- **📍 Places**: Points of interest with relevance scores
- **🌤️ Weather**: Weather data with temperature-based scoring
- **🚶 Foot Traffic**: Pedestrian volume data

### 🧠 Assumption Layers

Calculated/modeled data based on assumptions:

- **🎓 College Density**: University proximity influence scores
- **💰 Spending Propensity**: Demographic-based spending likelihood
- **⚙️ Custom Features**: Additional calculated features

## Usage

### Basic Usage

```python
from backend.visualization.interactive_map_builder import InteractiveMapBuilder

# Initialize map builder
map_builder = InteractiveMapBuilder()

# Prepare your data
api_layers = {
    "events": events_data,
    "places": places_data,
    "weather": weather_data,
    "foot_traffic": traffic_data
}

assumption_layers = {
    "college_density": college_scores,
    "spending_propensity": spending_scores,
    "custom_features": custom_scores
}

# Create layered heatmap
output_file = map_builder.create_layered_heatmap(
    api_layers=api_layers,
    assumption_layers=assumption_layers,
    output_path="real_data_heatmap.html",
    style="streets"
)
```

### Data Format Requirements

#### API Layers Format

**Events Data:**

```python
events_data = [
    {
        "latitude": 39.0997,
        "longitude": -94.5786,
        "name": "Event Name",
        "venue_name": "Venue Name",
        "date": "2024-01-15",
        "total_score": 0.85
    }
]
```

**Places Data:**

```python
places_data = [
    {
        "latitude": 39.1167,
        "longitude": -94.6275,
        "name": "Place Name",
        "type": "Place Type",
        "score": 0.78
    }
]
```

**Weather Data:**

```python
weather_data = [
    {
        "latitude": 39.0997,
        "longitude": -94.5786,
        "temperature": 72,
        "conditions": "Sunny"
    }
]
```

**Foot Traffic Data:**

```python
foot_traffic_data = [
    {
        "latitude": 39.0997,
        "longitude": -94.5786,
        "volume": 1250,
        "timestamp": "2024-01-15T14:00:00Z"
    }
]
```

#### Assumption Layers Format

**Grid-based Data (College Density, Spending Propensity, Custom Features):**

```python
grid_data = {
    (39.0997, -94.5786): 0.85,  # (lat, lon): score
    (39.1012, -94.5844): 0.72,
    # ... more coordinate-score pairs
}
```

### Advanced Configuration

#### Custom Styling

```python
# Different map styles available
styles = ["streets", "satellite", "light", "dark", "outdoors"]

output_file = map_builder.create_layered_heatmap(
    api_layers=api_layers,
    assumption_layers=assumption_layers,
    style="dark",  # Choose your preferred style
    output_path="custom_heatmap.html"
)
```

#### Layer-specific Maps

```python
# API layers only
api_only = map_builder.create_layered_heatmap(
    api_layers=api_layers,
    assumption_layers=None,
    output_path="api_only.html"
)

# Assumption layers only
assumption_only = map_builder.create_layered_heatmap(
    api_layers=None,
    assumption_layers=assumption_layers,
    output_path="assumptions_only.html"
)
```

## Interactive Features

### Layer Control Panel

1. **Expand/Collapse**: Click the layer control icon (top-right)
2. **Toggle Groups**: Check/uncheck "📡 API Data Layers" or "🧠 Assumption Layers"
3. **Individual Layers**: Toggle specific layers within each group
4. **Real-time Updates**: Changes apply immediately to the map

### Marker Interactions

1. **Hover**: Shows tooltip with basic information
2. **Click**: Opens detailed popup with full data
3. **Visual Cues**: Marker size indicates score intensity

### Map Navigation

1. **Zoom**: Mouse wheel or zoom controls
2. **Pan**: Click and drag to move around
3. **Reset**: Double-click to reset view

## Testing

Run the test script to generate sample heatmaps:

```bash
python test_layered_heatmap.py
```

This will create three example files:

- `real_data_heatmap.html` - Combined API and assumption layers
- `api_only_heatmap.html` - API layers only
- `assumption_only_heatmap.html` - Assumption layers only

## Integration with Existing Code

### Using with College Layer

```python
from features.college_layer import CollegeLayer

college_layer = CollegeLayer()
college_data = {}

# Generate scores for grid points
for lat, lon in grid_points:
    result = college_layer.calculate_college_density_score(lat, lon)
    college_data[(lat, lon)] = result["score"]

assumption_layers = {"college_density": college_data}
```

### Using with Spending Propensity Layer

```python
from features.spending_propensity_layer import SpendingPropensityLayer

spending_layer = SpendingPropensityLayer()
spending_data = {}

for lat, lon in grid_points:
    demo_data = get_demographic_data(lat, lon)  # Your demographic data
    analysis = spending_layer.analyze_location_spending_potential(lat, lon, demo_data)
    spending_data[(lat, lon)] = analysis["spending_propensity_score"]

assumption_layers = {"spending_propensity": spending_data}
```

## Best Practices

### Data Preparation

1. **Consistent Scoring**: Ensure all scores are normalized to 0-1 range
2. **Coordinate Precision**: Use consistent decimal precision for coordinates
3. **Data Validation**: Validate data structure before passing to map builder

### Performance Optimization

1. **Grid Resolution**: Use appropriate grid resolution for assumption layers
2. **Data Filtering**: Filter out low-relevance data points
3. **File Size**: Monitor output HTML file size for large datasets

### Visual Design

1. **Layer Balance**: Don't overcrowd the map with too many visible layers
2. **Color Contrast**: The blue/red color scheme provides good contrast
3. **Score Ranges**: Ensure score distributions make visual sense

## Troubleshooting

### Common Issues

**No layers visible:**

- Check that data is properly formatted
- Verify coordinate ranges are reasonable
- Ensure scores are in 0-1 range

**Layer control not working:**

- Verify folium version compatibility
- Check browser JavaScript console for errors

**Poor performance:**

- Reduce number of data points
- Use appropriate grid resolution
- Consider data aggregation

### Debug Mode

Enable logging to see detailed information:

```python
import logging
logging.basicConfig(level=logging.INFO)

# Your map creation code here
```

## API Reference

### `create_layered_heatmap()`

**Parameters:**

- `api_layers` (Dict, optional): Dictionary of API data layers
- `assumption_layers` (Dict, optional): Dictionary of calculated layers
- `output_path` (str): Output HTML file path
- `style` (str): Map style ("streets", "satellite", "light", "dark", "outdoors")
- `layer_config` (Dict, optional): Additional layer configuration

**Returns:**

- `Path`: Path to generated HTML file, or None if failed

**Example:**

```python
output_file = map_builder.create_layered_heatmap(
    api_layers={"events": events_data},
    assumption_layers={"college_density": college_data},
    output_path="my_heatmap.html",
    style="streets"
)
```

## Version History

- **v2.0**: Added layered heatmap functionality with interactive controls
- **v1.0**: Basic heatmap functionality

## Support

For issues or questions:

1. Check the troubleshooting section above
2. Review the test script for working examples
3. Examine generated HTML files for reference
