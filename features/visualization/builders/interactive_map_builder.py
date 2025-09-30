"""
Enhanced InteractiveMapBuilder with proper Mapbox integration for PDPE.
Integrates with the venue data service layer for pre-processed data.
"""

import folium
import os
import logging
from typing import List, Dict, Tuple, Optional
from pathlib import Path
import webbrowser
import json
import numpy as np
from backend.services.venue_data_service import (
    VenueDataService,
    VenueDataType,
    VenueDataQuery,
    get_heatmap_data,
    get_layered_map_data,
    get_venue_rankings,
)

logger = logging.getLogger(__name__)


class InteractiveMapBuilder:
    """Enhanced map builder with Mapbox integration."""

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        """
        Initialize map builder with Mapbox support.

        Args:
            center_coords: (latitude, longitude) for map center (Kansas City default)
        """
        self.center_coords = center_coords

        # Get Mapbox token from environment variables
        self.mapbox_token = self._get_mapbox_token()

        # Map style configurations
        self.map_styles = {
            "streets": "mapbox/streets-v12",
            "satellite": "mapbox/satellite-streets-v12",
            "light": "mapbox/light-v11",
            "dark": "mapbox/dark-v11",
            "outdoors": "mapbox/outdoors-v12",
        }

        self.default_style = "streets"

    def _get_mapbox_token(self) -> Optional[str]:
        """Get Mapbox access token from environment variables."""
        token = os.getenv("MAPBOX_ACCESS_TOKEN") or os.getenv("MAPBOX_API_KEY")

        if not token:
            logger.warning(
                "No Mapbox token found. Set MAPBOX_ACCESS_TOKEN environment variable. "
                "Falling back to OpenStreetMap tiles."
            )
            return None

        if not token.startswith("pk."):
            logger.error("Invalid Mapbox token format. Token should start with 'pk.'")
            return None

        return token

    def _create_base_map(
        self, center: List[float], zoom: int = 13, style: str = None
    ) -> folium.Map:
        """
        Create base map with Mapbox tiles if token is available.

        Args:
            center: [latitude, longitude]
            zoom: Initial zoom level
            style: Map style ('streets', 'satellite', 'light', 'dark', 'outdoors')

        Returns:
            Configured folium.Map object
        """
        style = style or self.default_style

        # Create map without default tiles
        m = folium.Map(location=center, zoom_start=zoom, tiles=None)

        if self.mapbox_token:
            # Add Mapbox tile layer
            try:
                mapbox_style = self.map_styles.get(style, self.map_styles["streets"])
                tile_url = (
                    f"https://api.mapbox.com/styles/v1/{mapbox_style}/tiles/"
                    f"{{z}}/{{x}}/{{y}}@2x?access_token={self.mapbox_token}"
                )

                folium.TileLayer(
                    tiles=tile_url,
                    attr="© Mapbox © OpenStreetMap",
                    name=f"Mapbox {style.title()}",
                    overlay=False,
                    control=True,
                    max_zoom=22,
                ).add_to(m)

                logger.info(f"Mapbox {style} tiles loaded successfully")

            except Exception as e:
                logger.error(f"Failed to load Mapbox tiles: {e}")
                self._add_fallback_tiles(m)
        else:
            # Use OpenStreetMap as fallback
            self._add_fallback_tiles(m)

        return m

    def _add_fallback_tiles(self, map_obj: folium.Map):
        """Add fallback OpenStreetMap tiles."""
        folium.TileLayer(
            tiles="OpenStreetMap", name="OpenStreetMap", overlay=False, control=True
        ).add_to(map_obj)

        # Add additional tile options
        folium.TileLayer(
            tiles="CartoDB positron", name="CartoDB Light", overlay=False, control=True
        ).add_to(map_obj)

        logger.info("Using OpenStreetMap fallback tiles")

    def create_event_heatmap(
        self,
        events_data: List[Dict],
        output_path: str = "event_heatmap.html",
        style: str = "streets",
    ) -> Optional[Path]:
        """
        Create a heatmap showing event locations and scores.

        Args:
            events_data: List of event dictionaries with lat/lon and scores
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        if not events_data:
            logger.warning("No events data provided for heatmap")
            return None

        try:
            # Calculate center from data
            avg_lat = sum(event["latitude"] for event in events_data) / len(events_data)
            avg_lon = sum(event["longitude"] for event in events_data) / len(
                events_data
            )
            center = [avg_lat, avg_lon]

            # Create base map
            m = self._create_base_map(center, zoom=12, style=style)

            # Add event markers with score-based styling
            for event in events_data:
                lat, lon = event["latitude"], event["longitude"]
                score = event.get("total_score", 0)

                # Determine marker properties based on score
                radius, color, fill_color = self._get_marker_style(score)

                # Create popup content
                popup_content = self._create_event_popup(event)

                folium.CircleMarker(
                    location=(lat, lon),
                    radius=radius,
                    popup=folium.Popup(popup_content, max_width=300),
                    tooltip=f"Score: {score:.1f}",
                    color=color,
                    fill=True,
                    fillColor=fill_color,
                    fillOpacity=0.7,
                    weight=2,
                ).add_to(m)

            # Add legend
            self._add_score_legend(m)

            # Add layer control
            folium.LayerControl().add_to(m)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Event heatmap saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating event heatmap: {e}")
            return None

    def create_probability_heatmap(
        self,
        probability_data: Dict,
        output_path: str = "probability_heatmap.html",
        style: str = "light",
    ) -> Optional[Path]:
        """
        Create heatmap showing probability distribution.

        Args:
            probability_data: Dict mapping (lat, lon) tuples to probability values
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        if not probability_data:
            logger.warning("No probability data provided")
            return None

        try:
            # Calculate center from data
            coords = list(probability_data.keys())
            avg_lat = sum(coord[0] for coord in coords) / len(coords)
            avg_lon = sum(coord[1] for coord in coords) / len(coords)
            center = [avg_lat, avg_lon]

            # Create base map
            m = self._create_base_map(center, zoom=11, style=style)

            # Prepare data for heatmap
            heat_data = []
            max_prob = max(probability_data.values()) if probability_data else 1.0

            for (lat, lon), probability in probability_data.items():
                intensity = probability / max_prob if max_prob > 0 else 0
                heat_data.append([lat, lon, intensity])

            # Add heatmap layer using HeatMap plugin
            try:
                from folium.plugins import HeatMap

                HeatMap(
                    heat_data,
                    radius=20,
                    blur=15,
                    max_zoom=15,
                    gradient={
                        0.0: "navy",
                        0.3: "blue",
                        0.5: "green",
                        0.7: "yellow",
                        1.0: "red",
                    },
                ).add_to(m)

            except ImportError:
                logger.warning(
                    "HeatMap plugin not available, using circle markers instead"
                )
                self._add_probability_circles(m, probability_data, max_prob)

            # Add high-probability markers
            self._add_high_probability_markers(m, probability_data, max_prob)

            # Add probability legend
            self._add_probability_legend(m)

            # Add layer control
            folium.LayerControl().add_to(m)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Probability heatmap saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating probability heatmap: {e}")
            return None

    def create_combined_visualization(
        self,
        events_data: List[Dict] = None,
        probability_data: Dict = None,
        grid_data: Dict = None,
        output_path: str = "combined_visualization.html",
        style: str = "streets",
    ) -> Optional[Path]:
        """
        Create comprehensive visualization combining multiple data layers.

        Args:
            events_data: Event data for markers
            probability_data: Probability distribution data
            grid_data: Grid analysis data
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        if not any([events_data, probability_data, grid_data]):
            logger.warning("No data provided for combined visualization")
            return None

        try:
            # Determine center from available data
            center = self._calculate_center_from_data(
                events_data, probability_data, grid_data
            )

            # Create base map
            m = self._create_base_map(center, zoom=11, style=style)

            # Add probability heatmap layer if available
            if probability_data:
                try:
                    from folium.plugins import HeatMap

                    heat_data = []
                    max_prob = max(probability_data.values())

                    for (lat, lon), probability in probability_data.items():
                        intensity = probability / max_prob if max_prob > 0 else 0
                        heat_data.append([lat, lon, intensity])

                    heatmap_layer = folium.FeatureGroup(name="Probability Heatmap")
                    HeatMap(heat_data, radius=15, blur=10, max_zoom=15).add_to(
                        heatmap_layer
                    )
                    heatmap_layer.add_to(m)

                except ImportError:
                    logger.warning("HeatMap plugin not available")

            # Add event markers layer if available
            if events_data:
                event_layer = folium.FeatureGroup(name="Events")

                for event in events_data:
                    lat, lon = event["latitude"], event["longitude"]
                    score = event.get("total_score", 0)

                    radius, color, fill_color = self._get_marker_style(score)
                    popup_content = self._create_event_popup(event)

                    folium.CircleMarker(
                        location=(lat, lon),
                        radius=radius,
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=f"Score: {score:.1f}",
                        color=color,
                        fill=True,
                        fillColor=fill_color,
                        fillOpacity=0.7,
                        weight=2,
                    ).add_to(event_layer)

                event_layer.add_to(m)

            # Add grid data layer if available
            if grid_data:
                grid_layer = folium.FeatureGroup(name="Grid Analysis")
                self._add_grid_layer(grid_layer, grid_data)
                grid_layer.add_to(m)

            # Add comprehensive legend
            self._add_combined_legend(m, events_data, probability_data, grid_data)

            # Add layer control
            folium.LayerControl().add_to(m)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Combined visualization saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating combined visualization: {e}")
            return None

    def create_layered_heatmap(
        self,
        api_layers: Dict = None,
        assumption_layers: Dict = None,
        output_path: str = "real_data_heatmap.html",
        style: str = "streets",
        layer_config: Dict = None,
    ) -> Optional[Path]:
        """
        Create interactive heatmap with toggleable API and assumption layers.

        Args:
            api_layers: Dictionary of API-sourced data layers
                - events: List of event data
                - places: List of place data
                - weather: Weather data
                - foot_traffic: Foot traffic data
            assumption_layers: Dictionary of calculated/modeled layers
                - college_density: College layer data
                - spending_propensity: Spending propensity data
                - custom_features: Other feature layers
            output_path: Output HTML file path
            style: Map style to use
            layer_config: Configuration for layer display options

        Returns:
            Path to generated HTML file
        """
        if not any([api_layers, assumption_layers]):
            logger.warning("No data layers provided for layered heatmap")
            return None

        try:
            # Determine center from available data
            center = self._calculate_center_from_layered_data(
                api_layers, assumption_layers
            )

            # Create base map
            m = self._create_base_map(center, zoom=11, style=style)

            # Create layer groups for API data
            if api_layers:
                api_group = folium.FeatureGroup(name="📡 API Data Layers", show=True)

                # Events layer
                if api_layers.get("events"):
                    events_layer = folium.FeatureGroup(
                        name="🎪 Events (PredictHQ)", show=True
                    )
                    self._add_events_layer(
                        events_layer, api_layers["events"], color_scheme="api"
                    )
                    events_layer.add_to(api_group)

                # Places layer
                if api_layers.get("places"):
                    places_layer = folium.FeatureGroup(name="📍 Places", show=True)
                    self._add_places_layer(
                        places_layer, api_layers["places"], color_scheme="api"
                    )
                    places_layer.add_to(api_group)

                # Weather layer
                if api_layers.get("weather"):
                    weather_layer = folium.FeatureGroup(name="🌤️ Weather", show=False)
                    self._add_weather_layer(
                        weather_layer, api_layers["weather"], color_scheme="api"
                    )
                    weather_layer.add_to(api_group)

                # Foot traffic layer
                if api_layers.get("foot_traffic"):
                    traffic_layer = folium.FeatureGroup(
                        name="🚶 Foot Traffic", show=False
                    )
                    self._add_traffic_layer(
                        traffic_layer, api_layers["foot_traffic"], color_scheme="api"
                    )
                    traffic_layer.add_to(api_group)

                api_group.add_to(m)

            # Create layer groups for assumption/calculated data
            if assumption_layers:
                assumption_group = folium.FeatureGroup(
                    name="🧠 Assumption Layers", show=True
                )

                # College density layer
                if assumption_layers.get("college_density"):
                    college_layer = folium.FeatureGroup(
                        name="🎓 College Density", show=True
                    )
                    self._add_college_layer(
                        college_layer,
                        assumption_layers["college_density"],
                        color_scheme="assumption",
                    )
                    college_layer.add_to(assumption_group)

                # Spending propensity layer
                if assumption_layers.get("spending_propensity"):
                    spending_layer = folium.FeatureGroup(
                        name="💰 Spending Propensity", show=True
                    )
                    self._add_spending_layer(
                        spending_layer,
                        assumption_layers["spending_propensity"],
                        color_scheme="assumption",
                    )
                    spending_layer.add_to(assumption_group)

                # Custom features layer
                if assumption_layers.get("custom_features"):
                    features_layer = folium.FeatureGroup(
                        name="⚙️ Custom Features", show=False
                    )
                    self._add_custom_features_layer(
                        features_layer,
                        assumption_layers["custom_features"],
                        color_scheme="assumption",
                    )
                    features_layer.add_to(assumption_group)

                assumption_group.add_to(m)

            # Add enhanced layer control with custom styling
            self._add_enhanced_layer_control(m)

            # Add comprehensive legend for layered visualization
            self._add_layered_legend(m, api_layers, assumption_layers)

            # Add layer information panel
            self._add_layer_info_panel(m)

            # Add venue ranking sidebar if places data is available
            if api_layers and api_layers.get("places"):
                self._add_venue_ranking_sidebar(m, api_layers["places"])

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Layered heatmap saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating layered heatmap: {e}")
            return None

    def _get_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get marker styling based on score."""
        if score >= 0.8:
            return 12, "#d73027", "#d73027"  # High score - red
        elif score >= 0.6:
            return 10, "#fc8d59", "#fc8d59"  # Medium-high - orange
        elif score >= 0.4:
            return 8, "#fee08b", "#fee08b"  # Medium - yellow
        elif score >= 0.2:
            return 6, "#e0f3f8", "#e0f3f8"  # Low-medium - light blue
        else:
            return 4, "#91bfdb", "#91bfdb"  # Low - blue

    def _create_event_popup(self, event: Dict) -> str:
        """Create HTML popup content for event markers."""
        score = event.get("total_score", 0)
        name = event.get("name", "Unknown Event")
        venue = event.get("venue_name", "Unknown Venue")
        date = event.get("date", "Unknown Date")

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 250px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{name}</h4>
            <p style="margin: 5px 0;"><strong>Venue:</strong> {venue}</p>
            <p style="margin: 5px 0;"><strong>Date:</strong> {date}</p>
            <p style="margin: 5px 0;"><strong>Score:</strong> {score:.2f}</p>
            <div style="margin-top: 10px; padding: 5px; background-color: #f0f0f0; border-radius: 3px;">
                <small>Psychographic Prediction Score</small>
            </div>
        </div>
        """

    def _add_score_legend(self, map_obj: folium.Map):
        """Add score legend to map."""
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 150px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <h4 style="margin-top:0">Score Legend</h4>
        <p><i class="fa fa-circle" style="color:#d73027"></i> High (0.8+)</p>
        <p><i class="fa fa-circle" style="color:#fc8d59"></i> Med-High (0.6-0.8)</p>
        <p><i class="fa fa-circle" style="color:#fee08b"></i> Medium (0.4-0.6)</p>
        <p><i class="fa fa-circle" style="color:#e0f3f8"></i> Low-Med (0.2-0.4)</p>
        <p><i class="fa fa-circle" style="color:#91bfdb"></i> Low (0-0.2)</p>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_probability_legend(self, map_obj: folium.Map):
        """Add probability legend to map."""
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 180px; height: 140px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <h4 style="margin-top:0">Probability Density</h4>
        <div style="background: linear-gradient(to right, navy, blue, green, yellow, red); 
                    height: 20px; width: 100%; margin: 10px 0;"></div>
        <div style="display: flex; justify-content: space-between; font-size: 12px;">
            <span>Low</span>
            <span>High</span>
        </div>
        <p style="margin-top: 10px; font-size: 12px;">
            Psychographic density prediction
        </p>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_probability_circles(
        self, map_obj: folium.Map, probability_data: Dict, max_prob: float
    ):
        """Add probability data as circle markers when HeatMap plugin is not available."""
        for (lat, lon), probability in probability_data.items():
            intensity = probability / max_prob if max_prob > 0 else 0

            # Color based on intensity
            if intensity >= 0.8:
                color = "#d73027"
            elif intensity >= 0.6:
                color = "#fc8d59"
            elif intensity >= 0.4:
                color = "#fee08b"
            elif intensity >= 0.2:
                color = "#e0f3f8"
            else:
                color = "#91bfdb"

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(3, intensity * 15),
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.6,
                popup=f"Probability: {probability:.3f}",
                tooltip=f"P: {probability:.3f}",
            ).add_to(map_obj)

    def _add_high_probability_markers(
        self, map_obj: folium.Map, probability_data: Dict, max_prob: float
    ):
        """Add special markers for high probability areas."""
        threshold = max_prob * 0.8  # Top 20% of probabilities

        for (lat, lon), probability in probability_data.items():
            if probability >= threshold:
                folium.Marker(
                    location=(lat, lon),
                    popup=f"High Probability Zone<br>Score: {probability:.3f}",
                    tooltip=f"High P: {probability:.3f}",
                    icon=folium.Icon(color="red", icon="star"),
                ).add_to(map_obj)

    def _calculate_center_from_data(
        self,
        events_data: List[Dict] = None,
        probability_data: Dict = None,
        grid_data: Dict = None,
    ) -> List[float]:
        """Calculate map center from available data."""
        lats, lons = [], []

        if events_data:
            lats.extend([event["latitude"] for event in events_data])
            lons.extend([event["longitude"] for event in events_data])

        if probability_data:
            coords = list(probability_data.keys())
            lats.extend([coord[0] for coord in coords])
            lons.extend([coord[1] for coord in coords])

        if grid_data:
            # Assuming grid_data has coordinate keys
            if isinstance(grid_data, dict):
                for key in grid_data.keys():
                    if isinstance(key, tuple) and len(key) == 2:
                        lats.append(key[0])
                        lons.append(key[1])

        if lats and lons:
            return [sum(lats) / len(lats), sum(lons) / len(lons)]
        else:
            return list(self.center_coords)

    def _add_grid_layer(self, layer: folium.FeatureGroup, grid_data: Dict):
        """Add grid analysis layer to map."""
        for (lat, lon), value in grid_data.items():
            if isinstance(value, (int, float)) and value > 0:
                folium.CircleMarker(
                    location=(lat, lon),
                    radius=max(2, value * 10),
                    color="purple",
                    fill=True,
                    fillColor="purple",
                    fillOpacity=0.4,
                    popup=f"Grid Value: {value:.3f}",
                    tooltip=f"Grid: {value:.3f}",
                ).add_to(layer)

    def _add_combined_legend(
        self,
        map_obj: folium.Map,
        events_data: List[Dict] = None,
        probability_data: Dict = None,
        grid_data: Dict = None,
    ):
        """Add comprehensive legend for combined visualization."""
        legend_items = []

        if events_data:
            legend_items.append("<h5>Events</h5>")
            legend_items.append(
                '<p><i class="fa fa-circle" style="color:#d73027"></i> High Score</p>'
            )
            legend_items.append(
                '<p><i class="fa fa-circle" style="color:#91bfdb"></i> Low Score</p>'
            )

        if probability_data:
            legend_items.append("<h5>Probability</h5>")
            legend_items.append(
                '<div style="background: linear-gradient(to right, navy, red); height: 15px; width: 100px;"></div>'
            )

        if grid_data:
            legend_items.append("<h5>Grid Analysis</h5>")
            legend_items.append(
                '<p><i class="fa fa-circle" style="color:purple"></i> Grid Points</p>'
            )

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:12px; padding: 10px">
        <h4 style="margin-top:0">Legend</h4>
        {"".join(legend_items)}
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def open_in_browser(self, file_path: Path):
        """Open generated map in default browser."""
        try:
            webbrowser.open(f"file://{file_path.absolute()}")
            logger.info(f"Opened map in browser: {file_path}")
        except Exception as e:
            logger.error(f"Failed to open map in browser: {e}")

    def export_to_geojson(
        self, data: List[Dict], output_path: str = "export.geojson"
    ) -> Optional[Path]:
        """Export data to GeoJSON format."""
        try:
            features = []

            for item in data:
                if "latitude" in item and "longitude" in item:
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [item["longitude"], item["latitude"]],
                        },
                        "properties": {
                            k: v
                            for k, v in item.items()
                            if k not in ["latitude", "longitude"]
                        },
                    }
                    features.append(feature)

            geojson_data = {"type": "FeatureCollection", "features": features}

            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w") as f:
                json.dump(geojson_data, f, indent=2)

            logger.info(f"GeoJSON exported to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error exporting to GeoJSON: {e}")
            return None

    def _calculate_center_from_layered_data(
        self, api_layers: Dict = None, assumption_layers: Dict = None
    ) -> List[float]:
        """Calculate map center from layered data."""
        lats, lons = [], []

        # Extract coordinates from API layers
        if api_layers:
            if api_layers.get("events"):
                for event in api_layers["events"]:
                    if "latitude" in event and "longitude" in event:
                        lats.append(event["latitude"])
                        lons.append(event["longitude"])

            if api_layers.get("places"):
                for place in api_layers["places"]:
                    if "latitude" in place and "longitude" in place:
                        lats.append(place["latitude"])
                        lons.append(place["longitude"])

        # Extract coordinates from assumption layers
        if assumption_layers:
            for layer_name, layer_data in assumption_layers.items():
                if isinstance(layer_data, dict):
                    for key, value in layer_data.items():
                        if isinstance(key, tuple) and len(key) == 2:
                            lats.append(key[0])
                            lons.append(key[1])
                elif isinstance(layer_data, list):
                    for item in layer_data:
                        if isinstance(item, dict):
                            if "latitude" in item and "longitude" in item:
                                lats.append(item["latitude"])
                                lons.append(item["longitude"])
                            elif "lat" in item and "lng" in item:
                                lats.append(item["lat"])
                                lons.append(item["lng"])

        if lats and lons:
            return [sum(lats) / len(lats), sum(lons) / len(lons)]
        else:
            return list(self.center_coords)

    def _get_api_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get marker styling for API layers (blue color scheme)."""
        if score >= 0.8:
            return 12, "#08519c", "#08519c"  # Dark blue
        elif score >= 0.6:
            return 10, "#3182bd", "#3182bd"  # Medium blue
        elif score >= 0.4:
            return 8, "#6baed6", "#6baed6"  # Light blue
        elif score >= 0.2:
            return 6, "#9ecae1", "#9ecae1"  # Very light blue
        else:
            return 4, "#c6dbef", "#c6dbef"  # Pale blue

    def _get_assumption_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get marker styling for assumption layers (red/orange color scheme)."""
        if score >= 0.8:
            return 12, "#a50f15", "#a50f15"  # Dark red
        elif score >= 0.6:
            return 10, "#de2d26", "#de2d26"  # Medium red
        elif score >= 0.4:
            return 8, "#fb6a4a", "#fb6a4a"  # Orange-red
        elif score >= 0.2:
            return 6, "#fc9272", "#fc9272"  # Light orange
        else:
            return 4, "#fcbba1", "#fcbba1"  # Pale orange

    def _add_events_layer(
        self,
        layer: folium.FeatureGroup,
        events_data: List[Dict],
        color_scheme: str = "api",
    ):
        """Add events layer to map with appropriate styling."""
        for event in events_data:
            lat = event.get("latitude", 0)
            lon = event.get("longitude", 0)
            score = event.get("total_score", 0)

            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = self._create_event_popup(event)

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Event: {event.get('name', 'Unknown')} | Score: {score:.2f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(layer)

    def _add_places_layer(
        self,
        layer: folium.FeatureGroup,
        places_data: List[Dict],
        color_scheme: str = "api",
    ):
        """Add places layer to map as a heatmap with venue markers."""
        if not places_data:
            return

        # Create heatmap data
        heat_data = []
        max_score = (
            max([place.get("total_score", 0) for place in places_data])
            if places_data
            else 1.0
        )

        for place in places_data:
            lat = place.get("latitude", 0)
            lon = place.get("longitude", 0)
            score = place.get("total_score", 0)

            # Normalize score for heatmap intensity
            intensity = score / max_score if max_score > 0 else 0
            heat_data.append([lat, lon, intensity])

        # Add heatmap layer
        try:
            from folium.plugins import HeatMap

            HeatMap(
                heat_data,
                radius=25,
                blur=20,
                max_zoom=18,
                gradient={
                    0.0: "#313695",
                    0.2: "#4575b4",
                    0.4: "#74add1",
                    0.6: "#abd9e9",
                    0.8: "#fee090",
                    1.0: "#d73027",
                },
                name="Venue Heatmap",
            ).add_to(layer)

        except ImportError:
            logger.warning(
                "HeatMap plugin not available, using circle markers as fallback"
            )
            # Fallback to circle markers if HeatMap is not available
            for place in places_data:
                lat = place.get("latitude", 0)
                lon = place.get("longitude", 0)
                score = place.get("total_score", 0)

                if color_scheme == "api":
                    radius, color, fill_color = self._get_api_marker_style(score)
                else:
                    radius, color, fill_color = self._get_assumption_marker_style(score)

                popup_content = f"""
                <div style="font-family: Arial, sans-serif; max-width: 250px;">
                    <h4 style="margin: 0 0 10px 0; color: #333;">{place.get('name', 'Unknown Place')}</h4>
                    <p style="margin: 5px 0;"><strong>Type:</strong> {place.get('category', 'Unknown')}</p>
                    <p style="margin: 5px 0;"><strong>Score:</strong> {score:.2f}</p>
                    <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                        <small>📡 API Data Source</small>
                    </div>
                </div>
                """

                folium.CircleMarker(
                    location=(lat, lon),
                    radius=radius,
                    popup=folium.Popup(popup_content, max_width=300),
                    tooltip=f"Place: {place.get('name', 'Unknown')} | Score: {score:.2f}",
                    color=color,
                    fill=True,
                    fillColor=fill_color,
                    fillOpacity=0.7,
                    weight=2,
                ).add_to(layer)

        # Add top venue markers for reference
        self._add_top_venue_markers(layer, places_data)

    def _add_top_venue_markers(
        self, layer: folium.FeatureGroup, places_data: List[Dict]
    ):
        """Add markers for top-scoring venues as reference points on the heatmap."""
        if not places_data:
            return

        # Sort venues by score and get top 10
        sorted_venues = sorted(
            places_data, key=lambda x: x.get("total_score", 0), reverse=True
        )
        top_venues = sorted_venues[:10]

        for i, venue in enumerate(top_venues):
            lat = venue.get("latitude", 0)
            lon = venue.get("longitude", 0)
            score = venue.get("total_score", 0)
            name = venue.get("name", "Unknown Venue")

            # Create detailed popup for top venues
            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 300px;">
                <h4 style="margin: 0 0 10px 0; color: #d73027;">🏆 Top Venue #{i+1}</h4>
                <h5 style="margin: 0 0 8px 0; color: #333;">{name}</h5>
                <p style="margin: 5px 0;"><strong>Category:</strong> {venue.get('category', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Rating:</strong> {venue.get('avg_rating', 'N/A')}</p>
                <p style="margin: 5px 0;"><strong>Address:</strong> {venue.get('address', 'N/A')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #fff3cd; border-radius: 3px;">
                    <small>⭐ High-Scoring Venue</small>
                </div>
            </div>
            """

            # Use star icon for top venues
            folium.Marker(
                location=(lat, lon),
                popup=folium.Popup(popup_content, max_width=350),
                tooltip=f"🏆 #{i+1}: {name} (Score: {score:.3f})",
                icon=folium.Icon(color="red", icon="star", prefix="fa"),
            ).add_to(layer)

    def _add_weather_layer(
        self,
        layer: folium.FeatureGroup,
        weather_data: List[Dict],
        color_scheme: str = "api",
    ):
        """Add weather layer to map."""
        for weather in weather_data:
            lat = weather.get("latitude", 0)
            lon = weather.get("longitude", 0)
            temp = weather.get("temperature", 0)

            # Normalize temperature to 0-1 score for styling
            score = min(max((temp - 32) / 68, 0), 1)  # 32-100°F range

            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Weather Data</h4>
                <p style="margin: 5px 0;"><strong>Temperature:</strong> {temp}°F</p>
                <p style="margin: 5px 0;"><strong>Conditions:</strong> {weather.get('conditions', 'Unknown')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>🌤️ Weather API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Weather: {temp}°F",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_traffic_layer(
        self,
        layer: folium.FeatureGroup,
        traffic_data: List[Dict],
        color_scheme: str = "api",
    ):
        """Add foot traffic layer to map."""
        for traffic in traffic_data:
            lat = traffic.get("latitude", 0)
            lon = traffic.get("longitude", 0)
            volume = traffic.get("volume", 0)

            # Normalize traffic volume to 0-1 score
            max_volume = (
                max([t.get("volume", 0) for t in traffic_data]) if traffic_data else 1
            )
            score = volume / max_volume if max_volume > 0 else 0

            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Foot Traffic</h4>
                <p style="margin: 5px 0;"><strong>Volume:</strong> {volume}</p>
                <p style="margin: 5px 0;"><strong>Time:</strong> {traffic.get('timestamp', 'Unknown')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>🚶 Traffic API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Traffic Volume: {volume}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_college_layer(
        self,
        layer: folium.FeatureGroup,
        college_data: Dict,
        color_scheme: str = "assumption",
    ):
        """Add college density layer to map."""
        for (lat, lon), score in college_data.items():
            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">College Density</h4>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Location:</strong> {lat:.4f}, {lon:.4f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                    <small>🎓 Calculated Layer</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(3, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"College Density: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def _add_spending_layer(
        self,
        layer: folium.FeatureGroup,
        spending_data: Dict,
        color_scheme: str = "assumption",
    ):
        """Add spending propensity layer to map."""
        for (lat, lon), score in spending_data.items():
            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Spending Propensity</h4>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Location:</strong> {lat:.4f}, {lon:.4f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                    <small>💰 Calculated Layer</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(3, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Spending Score: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def _add_custom_features_layer(
        self,
        layer: folium.FeatureGroup,
        features_data: Dict,
        color_scheme: str = "assumption",
    ):
        """Add custom features layer to map."""
        for (lat, lon), score in features_data.items():
            if color_scheme == "api":
                radius, color, fill_color = self._get_api_marker_style(score)
            else:
                radius, color, fill_color = self._get_assumption_marker_style(score)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Custom Features</h4>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Location:</strong> {lat:.4f}, {lon:.4f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                    <small>⚙️ Custom Calculation</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(3, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Feature Score: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def _add_enhanced_layer_control(self, map_obj: folium.Map):
        """Add enhanced layer control with custom styling."""
        # Add standard layer control
        folium.LayerControl(
            position="topright",
            collapsed=False,
            autoZIndex=True,
        ).add_to(map_obj)

        # Add custom CSS for better layer control styling
        custom_css = """
        <style>
        .leaflet-control-layers {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            font-family: Arial, sans-serif;
        }
        .leaflet-control-layers-expanded {
            padding: 10px;
            min-width: 200px;
        }
        .leaflet-control-layers-list {
            max-height: 400px;
            overflow-y: auto;
        }
        .leaflet-control-layers label {
            font-size: 13px;
            margin: 3px 0;
            display: flex;
            align-items: center;
        }
        .leaflet-control-layers input[type="checkbox"] {
            margin-right: 8px;
            transform: scale(1.2);
        }
        </style>
        """
        map_obj.get_root().html.add_child(folium.Element(custom_css))

    def _add_layered_legend(
        self,
        map_obj: folium.Map,
        api_layers: Dict = None,
        assumption_layers: Dict = None,
    ):
        """Add comprehensive legend for layered visualization."""
        legend_items = []

        # API Layers section
        if api_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #1976d2; border-bottom: 2px solid #1976d2; padding-bottom: 3px;">📡 API Data Layers</h4>'
            )

            if api_layers.get("events"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #08519c; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">Events (PredictHQ)</span>'
                )
                legend_items.append("</div>")

            if api_layers.get("places"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #3182bd; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append('<span style="font-size: 12px;">Places</span>')
                legend_items.append("</div>")

            if api_layers.get("weather"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #6baed6; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">Weather Data</span>'
                )
                legend_items.append("</div>")

            if api_layers.get("foot_traffic"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #9ecae1; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">Foot Traffic</span>'
                )
                legend_items.append("</div>")

            legend_items.append("</div>")

        # Assumption Layers section
        if assumption_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #d32f2f; border-bottom: 2px solid #d32f2f; padding-bottom: 3px;">🧠 Assumption Layers</h4>'
            )

            if assumption_layers.get("college_density"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #a50f15; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">College Density</span>'
                )
                legend_items.append("</div>")

            if assumption_layers.get("spending_propensity"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #de2d26; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">Spending Propensity</span>'
                )
                legend_items.append("</div>")

            if assumption_layers.get("custom_features"):
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #fb6a4a; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    '<span style="font-size: 12px;">Custom Features</span>'
                )
                legend_items.append("</div>")

            legend_items.append("</div>")

        # Score intensity guide
        legend_items.append(
            '<div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">'
        )
        legend_items.append(
            '<h5 style="margin: 0 0 8px 0; font-size: 12px; color: #666;">Score Intensity</h5>'
        )
        legend_items.append(
            '<div style="display: flex; align-items: center; margin: 3px 0;">'
        )
        legend_items.append(
            '<div style="width: 8px; height: 8px; border-radius: 50%; margin-right: 6px;"></div>'
        )
        legend_items.append(
            '<span style="font-size: 11px;">Larger = Higher Score</span>'
        )
        legend_items.append("</div>")
        legend_items.append("</div>")

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 280px; height: auto; max-height: 500px;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            Data Layer Legend
        </h3>
        {"".join(legend_items)}
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_layer_info_panel(self, map_obj: folium.Map):
        """Add information panel explaining layer types."""
        info_html = """
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 300px; height: auto;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            Interactive Heatmap Guide
        </h3>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #1976d2; font-size: 13px;">📡 API Data Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Real-time data from external APIs including events, places, weather, and foot traffic.
                <strong>Blue color scheme</strong> indicates API-sourced data.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #d32f2f; font-size: 13px;">🧠 Assumption Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Calculated layers based on models and assumptions including college density and spending propensity.
                <strong>Red/orange color scheme</strong> indicates calculated data.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #333; font-size: 13px;">🎛️ Controls</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Use the layer control panel (top-right) to toggle individual layers on/off.
                Click markers for detailed information.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #999; font-size: 10px;">
                PDPE Interactive Heatmap v2.0
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))

    def _add_venue_ranking_sidebar(self, map_obj: folium.Map, places_data: List[Dict]):
        """Add a sidebar with ranked list of venues."""
        if not places_data:
            return

        # Sort venues by score
        sorted_venues = sorted(
            places_data, key=lambda x: x.get("total_score", 0), reverse=True
        )

        # Create venue list HTML
        venue_items = []
        for i, venue in enumerate(sorted_venues[:25]):  # Show top 25 venues
            name = venue.get("name", "Unknown Venue")
            score = venue.get("total_score", 0)
            category = venue.get("category", "Unknown")
            rating = venue.get("avg_rating", "N/A")
            lat = venue.get("latitude", 0)
            lon = venue.get("longitude", 0)

            # Determine score color
            if score >= 0.8:
                score_color = "#d73027"
            elif score >= 0.6:
                score_color = "#fc8d59"
            elif score >= 0.4:
                score_color = "#fee08b"
            elif score >= 0.2:
                score_color = "#91bfdb"
            else:
                score_color = "#c6dbef"

            venue_items.append(
                f"""
            <div class="venue-item" onclick="centerMapOnVenue({lat}, {lon})" 
                 style="padding: 8px; margin: 4px 0; border-left: 4px solid {score_color}; 
                        background: rgba(255,255,255,0.9); cursor: pointer; border-radius: 4px;
                        transition: background-color 0.2s;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="flex: 1;">
                        <div style="font-weight: bold; font-size: 13px; color: #333; margin-bottom: 2px;">
                            #{i+1}. {name[:30]}{"..." if len(name) > 30 else ""}
                        </div>
                        <div style="font-size: 11px; color: #666; margin-bottom: 2px;">
                            {category} • Rating: {rating}
                        </div>
                        <div style="font-size: 10px; color: #999;">
                            Score: {score:.3f}
                        </div>
                    </div>
                </div>
            </div>
            """
            )

        sidebar_html = f"""
        <div id="venue-sidebar" style="position: fixed; 
                    top: 20px; left: 20px; width: 320px; height: 70vh;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9998; font-size: 12px; padding: 0;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    display: flex; flex-direction: column;">
            
            <!-- Header -->
            <div style="padding: 15px; border-bottom: 2px solid #333; background: #f8f9fa; border-radius: 6px 6px 0 0;">
                <h3 style="margin: 0; color: #333; text-align: center; font-size: 14px;">
                    🏆 Top Venues Ranking
                </h3>
                <div style="text-align: center; margin-top: 5px;">
                    <small style="color: #666; font-size: 11px;">
                        Click venue to center map • Total: {len(places_data)} venues
                    </small>
                </div>
            </div>
            
            <!-- Venue List -->
            <div style="flex: 1; overflow-y: auto; padding: 10px;">
                {"".join(venue_items)}
            </div>
            
            <!-- Footer -->
            <div style="padding: 10px; border-top: 1px solid #ddd; background: #f8f9fa; border-radius: 0 0 6px 6px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <small style="color: #666; font-size: 10px;">
                        Psychographic Scores
                    </small>
                    <button onclick="toggleSidebar()" style="background: #007bff; color: white; border: none; 
                            padding: 4px 8px; border-radius: 3px; font-size: 10px; cursor: pointer;">
                        Hide
                    </button>
                </div>
            </div>
        </div>

        <script>
        // Function to center map on venue
        function centerMapOnVenue(lat, lon) {{
            // Get the map instance
            var mapContainer = document.querySelector('.folium-map');
            if (mapContainer && mapContainer._leaflet_map) {{
                var map = mapContainer._leaflet_map;
                map.setView([lat, lon], 16);
                
                // Add a temporary marker
                var tempMarker = L.marker([lat, lon], {{
                    icon: L.icon({{
                        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png',
                        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                        iconSize: [25, 41],
                        iconAnchor: [12, 41],
                        popupAnchor: [1, -34],
                        shadowSize: [41, 41]
                    }})
                }}).addTo(map);
                
                // Remove the temporary marker after 3 seconds
                setTimeout(function() {{
                    map.removeLayer(tempMarker);
                }}, 3000);
            }}
        }}

        // Function to toggle sidebar visibility
        function toggleSidebar() {{
            var sidebar = document.getElementById('venue-sidebar');
            if (sidebar.style.display === 'none') {{
                sidebar.style.display = 'flex';
            }} else {{
                sidebar.style.display = 'none';
            }}
        }}

        // Add hover effects
        document.addEventListener('DOMContentLoaded', function() {{
            var venueItems = document.querySelectorAll('.venue-item');
            venueItems.forEach(function(item) {{
                item.addEventListener('mouseenter', function() {{
                    this.style.backgroundColor = 'rgba(0, 123, 255, 0.1)';
                }});
                item.addEventListener('mouseleave', function() {{
                    this.style.backgroundColor = 'rgba(255,255,255,0.9)';
                }});
            }});
        }});
        </script>

        <style>
        .venue-item:hover {{
            background-color: rgba(0, 123, 255, 0.1) !important;
        }}
        
        #venue-sidebar::-webkit-scrollbar {{
            width: 6px;
        }}
        
        #venue-sidebar::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 3px;
        }}
        
        #venue-sidebar::-webkit-scrollbar-thumb {{
            background: #888;
            border-radius: 3px;
        }}
        
        #venue-sidebar::-webkit-scrollbar-thumb:hover {{
            background: #555;
        }}
        </style>
        """

        map_obj.get_root().html.add_child(folium.Element(sidebar_html))

    # New methods using the venue data service
    def create_service_based_heatmap(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        min_score: float = 0.0,
        categories: Optional[List[str]] = None,
        output_path: str = "service_heatmap.html",
        style: str = "streets",
    ) -> Optional[Path]:
        """
        Create heatmap using the venue data service for pre-processed data.

        Args:
            bbox: Bounding box (min_lat, min_lng, max_lat, max_lng)
            min_score: Minimum psychographic score threshold
            categories: List of venue categories to include
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        try:
            logger.info("Creating service-based heatmap with pre-processed data")

            # Get data from service
            heatmap_data = get_heatmap_data(bbox, min_score, categories)

            if not heatmap_data.get("venues") and not heatmap_data.get("predictions"):
                logger.warning("No venue or prediction data available for heatmap")
                return None

            # Calculate center from bounds
            bounds = heatmap_data.get("bounds", {})
            if bounds.get("min_lat") and bounds.get("max_lat"):
                center_lat = (bounds["min_lat"] + bounds["max_lat"]) / 2
                center_lng = (bounds["min_lng"] + bounds["max_lng"]) / 2
                center = [center_lat, center_lng]
            else:
                center = list(self.center_coords)

            # Create base map
            m = self._create_base_map(center, zoom=12, style=style)

            # Add venue markers
            venues = heatmap_data.get("venues", [])
            if venues:
                for venue in venues:
                    lat, lon = venue["latitude"], venue["longitude"]
                    score = venue.get("total_score", 0)

                    radius, color, fill_color = self._get_marker_style(score)
                    popup_content = self._create_venue_popup(venue)

                    folium.CircleMarker(
                        location=(lat, lon),
                        radius=radius,
                        popup=folium.Popup(popup_content, max_width=300),
                        tooltip=f"Score: {score:.2f}",
                        color=color,
                        fill=True,
                        fillColor=fill_color,
                        fillOpacity=0.7,
                        weight=2,
                    ).add_to(m)

            # Add prediction heatmap if available
            predictions = heatmap_data.get("predictions", [])
            if predictions:
                try:
                    from folium.plugins import HeatMap

                    heat_data = []
                    for pred in predictions:
                        heat_data.append(
                            [
                                pred["latitude"],
                                pred["longitude"],
                                pred.get("psychographic_density", 0),
                            ]
                        )

                    if heat_data:
                        HeatMap(
                            heat_data,
                            radius=20,
                            blur=15,
                            max_zoom=15,
                            gradient={
                                0.0: "navy",
                                0.3: "blue",
                                0.5: "green",
                                0.7: "yellow",
                                1.0: "red",
                            },
                        ).add_to(m)

                except ImportError:
                    logger.warning("HeatMap plugin not available")

            # Add enhanced legend with statistics
            self._add_service_legend(m, heatmap_data)

            # Add layer control
            folium.LayerControl().add_to(m)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Service-based heatmap saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating service-based heatmap: {e}")
            return None

    def create_service_based_layered_map(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        include_api_layers: bool = True,
        include_assumption_layers: bool = True,
        output_path: str = "service_layered_map.html",
        style: str = "streets",
    ) -> Optional[Path]:
        """
        Create layered map using the venue data service.

        Args:
            bbox: Bounding box for data
            include_api_layers: Include API-sourced data layers
            include_assumption_layers: Include calculated/assumption layers
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        try:
            logger.info("Creating service-based layered map")

            # Get layered data from service
            layered_data = get_layered_map_data(
                bbox, include_api_layers, include_assumption_layers
            )

            api_layers = layered_data.get("api_layers", {})
            assumption_layers = layered_data.get("assumption_layers", {})

            if not api_layers and not assumption_layers:
                logger.warning("No layered data available")
                return None

            # Calculate center from available data
            center = self._calculate_center_from_layered_data(
                api_layers, assumption_layers
            )

            # Create base map
            m = self._create_base_map(center, zoom=11, style=style)

            # Add API layers
            if api_layers and include_api_layers:
                api_group = folium.FeatureGroup(name="📡 API Data Layers", show=True)

                # Events layer
                events = api_layers.get("events", [])
                if events:
                    events_layer = folium.FeatureGroup(name="🎪 Events", show=True)
                    self._add_events_layer(events_layer, events, color_scheme="api")
                    events_layer.add_to(api_group)

                # Places layer with heatmap
                places = api_layers.get("places", [])
                if places:
                    places_layer = folium.FeatureGroup(name="📍 Places", show=True)
                    self._add_places_layer(places_layer, places, color_scheme="api")
                    places_layer.add_to(api_group)

                # Weather layer
                weather = api_layers.get("weather", [])
                if weather:
                    weather_layer = folium.FeatureGroup(name="🌤️ Weather", show=False)
                    self._add_weather_layer(weather_layer, weather, color_scheme="api")
                    weather_layer.add_to(api_group)

                # Traffic layer
                traffic = api_layers.get("foot_traffic", [])
                if traffic:
                    traffic_layer = folium.FeatureGroup(
                        name="🚶 Foot Traffic", show=False
                    )
                    self._add_traffic_layer(traffic_layer, traffic, color_scheme="api")
                    traffic_layer.add_to(api_group)

                api_group.add_to(m)

            # Add assumption layers
            if assumption_layers and include_assumption_layers:
                assumption_group = folium.FeatureGroup(
                    name="🧠 Assumption Layers", show=True
                )

                # College density
                college_data = assumption_layers.get("college_density", {})
                if college_data:
                    college_layer = folium.FeatureGroup(
                        name="🎓 College Density", show=True
                    )
                    self._add_college_layer(
                        college_layer, college_data, color_scheme="assumption"
                    )
                    college_layer.add_to(assumption_group)

                # Spending propensity
                spending_data = assumption_layers.get("spending_propensity", {})
                if spending_data:
                    spending_layer = folium.FeatureGroup(
                        name="💰 Spending Propensity", show=True
                    )
                    self._add_spending_layer(
                        spending_layer, spending_data, color_scheme="assumption"
                    )
                    spending_layer.add_to(assumption_group)

                # Predictions
                predictions = assumption_layers.get("predictions", [])
                if predictions:
                    pred_layer = folium.FeatureGroup(
                        name="🔮 ML Predictions", show=False
                    )
                    self._add_predictions_layer(pred_layer, predictions)
                    pred_layer.add_to(assumption_group)

                assumption_group.add_to(m)

            # Add enhanced controls and legends
            self._add_enhanced_layer_control(m)
            self._add_layered_legend(m, api_layers, assumption_layers)
            self._add_layer_info_panel(m)

            # Add venue ranking sidebar if places data is available
            if api_layers.get("places"):
                self._add_venue_ranking_sidebar(m, api_layers["places"])

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Service-based layered map saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating service-based layered map: {e}")
            return None

    def _create_venue_popup(self, venue: Dict) -> str:
        """Create HTML popup content for venue markers."""
        score = venue.get("total_score", 0)
        name = venue.get("name", "Unknown Venue")
        category = venue.get("category", "unknown")
        rating = venue.get("avg_rating", "N/A")
        address = venue.get("address", "N/A")

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 280px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{name}</h4>
            <p style="margin: 5px 0;"><strong>Category:</strong> {category.title()}</p>
            <p style="margin: 5px 0;"><strong>Rating:</strong> {rating}</p>
            <p style="margin: 5px 0;"><strong>Address:</strong> {address}</p>
            <p style="margin: 5px 0;"><strong>Psychographic Score:</strong> {score:.3f}</p>
            <div style="margin-top: 10px; padding: 5px; background-color: #e8f5e8; border-radius: 3px;">
                <small>✨ Pre-processed Venue Data</small>
            </div>
        </div>
        """

    def _add_service_legend(self, map_obj: folium.Map, heatmap_data: Dict):
        """Add enhanced legend with service data statistics."""
        stats = heatmap_data.get("score_stats", {})
        venue_count = stats.get("count", 0)
        avg_score = stats.get("mean", 0)
        max_score = stats.get("max", 0)

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 8px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
        <h4 style="margin-top:0; color: #333; border-bottom: 1px solid #ddd; padding-bottom: 5px;">
            Venue Heatmap Legend
        </h4>
        
        <div style="margin: 10px 0;">
            <h5 style="margin: 5px 0; color: #666;">Score Ranges</h5>
            <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#d73027"></i> High (0.8+)</p>
            <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#fc8d59"></i> Med-High (0.6-0.8)</p>
            <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#fee08b"></i> Medium (0.4-0.6)</p>
            <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#e0f3f8"></i> Low-Med (0.2-0.4)</p>
            <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#91bfdb"></i> Low (0-0.2)</p>
        </div>
        
        <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <h5 style="margin: 5px 0; color: #666;">Statistics</h5>
            <p style="margin: 3px 0; font-size: 12px;">Venues: {venue_count}</p>
            <p style="margin: 3px 0; font-size: 12px;">Avg Score: {avg_score:.3f}</p>
            <p style="margin: 3px 0; font-size: 12px;">Max Score: {max_score:.3f}</p>
        </div>
        
        <div style="margin-top: 10px; padding: 5px; background-color: #f0f8ff; border-radius: 3px;">
            <small style="color: #666;">Data from Venue Service</small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_predictions_layer(
        self, layer: folium.FeatureGroup, predictions: List[Dict]
    ):
        """Add ML predictions layer to map."""
        for prediction in predictions:
            lat = prediction.get("latitude", 0)
            lon = prediction.get("longitude", 0)
            density = prediction.get("psychographic_density", 0)
            confidence_lower = prediction.get("confidence_lower", 0)
            confidence_upper = prediction.get("confidence_upper", 0)

            # Style based on prediction density
            radius, color, fill_color = self._get_assumption_marker_style(density)

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">ML Prediction</h4>
                <p style="margin: 5px 0;"><strong>Density:</strong> {density:.3f}</p>
                <p style="margin: 5px 0;"><strong>Confidence:</strong> {confidence_lower:.3f} - {confidence_upper:.3f}</p>
                <p style="margin: 5px 0;"><strong>Model:</strong> {prediction.get('model_version', 'Unknown')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #fff3e0; border-radius: 3px;">
                    <small>🔮 Machine Learning Prediction</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(4, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"ML Prediction: {density:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def create_comprehensive_all_data_map(
        self,
        all_layers: Dict,
        output_path: str = "comprehensive_all_data_map.html",
        style: str = "streets",
    ) -> Optional[Path]:
        """
        Create comprehensive map with ALL data layers organized by source type.

        Args:
            all_layers: Dictionary containing all organized data layers:
                - api_layers: API-sourced data
                - scraped_layers: Scraped venue/event data
                - calculated_layers: ML predictions, demographics, psychographic layers
                - ground_truth_layers: Manual and proxy labels
            output_path: Output HTML file path
            style: Map style to use

        Returns:
            Path to generated HTML file
        """
        if not all_layers:
            logger.warning("No data layers provided for comprehensive map")
            return None

        try:
            # Calculate center from all available data
            center = self._calculate_center_from_all_layers(all_layers)

            # Create base map
            m = self._create_base_map(center, zoom=11, style=style)

            # Add API Data Layers (Blue color scheme)
            api_layers = all_layers.get("api_layers", {})
            if api_layers:
                api_group = folium.FeatureGroup(name="📡 API Data", show=True)

                # Places layer
                if api_layers.get("places"):
                    places_layer = folium.FeatureGroup(
                        name="🏢 Places (Google)", show=True
                    )
                    self._add_comprehensive_places_layer(
                        places_layer, api_layers["places"], "api"
                    )
                    places_layer.add_to(api_group)

                # Events layer
                if api_layers.get("events"):
                    events_layer = folium.FeatureGroup(
                        name="🎪 Events (PredictHQ)", show=True
                    )
                    self._add_comprehensive_events_layer(
                        events_layer, api_layers["events"], "api"
                    )
                    events_layer.add_to(api_group)

                # Weather layer
                if api_layers.get("weather"):
                    weather_layer = folium.FeatureGroup(name="🌤️ Weather", show=False)
                    self._add_comprehensive_weather_layer(
                        weather_layer, api_layers["weather"]
                    )
                    weather_layer.add_to(api_group)

                # Traffic congestion layer
                if api_layers.get("traffic_congestion"):
                    traffic_layer = folium.FeatureGroup(name="🚗 Traffic", show=False)
                    self._add_comprehensive_traffic_layer(
                        traffic_layer, api_layers["traffic_congestion"]
                    )
                    traffic_layer.add_to(api_group)

                # Social sentiment layer
                if api_layers.get("social_sentiment"):
                    social_layer = folium.FeatureGroup(
                        name="📱 Social Sentiment", show=False
                    )
                    self._add_comprehensive_social_layer(
                        social_layer, api_layers["social_sentiment"]
                    )
                    social_layer.add_to(api_group)

                # Economic indicators layer
                if api_layers.get("economic_indicators"):
                    economic_layer = folium.FeatureGroup(
                        name="💰 Economic Data", show=False
                    )
                    self._add_comprehensive_economic_layer(
                        economic_layer, api_layers["economic_indicators"]
                    )
                    economic_layer.add_to(api_group)

                api_group.add_to(m)

            # Add Scraped Data Layers (Green color scheme)
            scraped_layers = all_layers.get("scraped_layers", {})
            if scraped_layers:
                scraped_group = folium.FeatureGroup(name="🌐 Scraped Data", show=True)

                # Static venues layer
                if scraped_layers.get("static_venues"):
                    static_layer = folium.FeatureGroup(
                        name="🏛️ Static Venues", show=True
                    )
                    self._add_comprehensive_places_layer(
                        static_layer, scraped_layers["static_venues"], "scraped"
                    )
                    static_layer.add_to(scraped_group)

                # Dynamic venues layer
                if scraped_layers.get("dynamic_venues"):
                    dynamic_layer = folium.FeatureGroup(
                        name="🌐 Dynamic Venues", show=True
                    )
                    self._add_comprehensive_places_layer(
                        dynamic_layer, scraped_layers["dynamic_venues"], "scraped"
                    )
                    dynamic_layer.add_to(scraped_group)

                # Local venues layer
                if scraped_layers.get("local_venues"):
                    local_layer = folium.FeatureGroup(name="🏪 Local Venues", show=True)
                    self._add_comprehensive_places_layer(
                        local_layer, scraped_layers["local_venues"], "scraped"
                    )
                    local_layer.add_to(scraped_group)

                # Scraped events layer
                if scraped_layers.get("scraped_events"):
                    scraped_events_layer = folium.FeatureGroup(
                        name="📅 Scraped Events", show=True
                    )
                    self._add_comprehensive_events_layer(
                        scraped_events_layer,
                        scraped_layers["scraped_events"],
                        "scraped",
                    )
                    scraped_events_layer.add_to(scraped_group)

                scraped_group.add_to(m)

            # Add Calculated Data Layers (Red/Orange color scheme)
            calculated_layers = all_layers.get("calculated_layers", {})
            if calculated_layers:
                calculated_group = folium.FeatureGroup(
                    name="🧠 Calculated Data", show=True
                )

                # Demographics layer
                if calculated_layers.get("demographics"):
                    demographics_layer = folium.FeatureGroup(
                        name="👥 Demographics", show=False
                    )
                    self._add_comprehensive_demographics_layer(
                        demographics_layer, calculated_layers["demographics"]
                    )
                    demographics_layer.add_to(calculated_group)

                # ML predictions layer
                if calculated_layers.get("ml_predictions"):
                    predictions_layer = folium.FeatureGroup(
                        name="🤖 ML Predictions", show=False
                    )
                    self._add_comprehensive_predictions_layer(
                        predictions_layer, calculated_layers["ml_predictions"]
                    )
                    predictions_layer.add_to(calculated_group)

                # Psychographic layers
                for layer_name, layer_data in calculated_layers.items():
                    if (
                        layer_name not in ["demographics", "ml_predictions"]
                        and layer_data
                    ):
                        psycho_layer = folium.FeatureGroup(
                            name=f"🧠 {layer_name.replace('_', ' ').title()}",
                            show=False,
                        )
                        self._add_comprehensive_psychographic_layer(
                            psycho_layer, layer_data, layer_name
                        )
                        psycho_layer.add_to(calculated_group)

                calculated_group.add_to(m)

            # Add Ground Truth Layers (Purple color scheme)
            ground_truth_layers = all_layers.get("ground_truth_layers", {})
            if ground_truth_layers:
                ground_truth_group = folium.FeatureGroup(
                    name="✅ Ground Truth", show=False
                )

                # Manual labels layer
                if ground_truth_layers.get("manual_labels"):
                    manual_layer = folium.FeatureGroup(
                        name="✅ Manual Labels", show=False
                    )
                    self._add_comprehensive_manual_labels_layer(
                        manual_layer, ground_truth_layers["manual_labels"]
                    )
                    manual_layer.add_to(ground_truth_group)

                # Proxy labels layer
                if ground_truth_layers.get("proxy_labels"):
                    proxy_layer = folium.FeatureGroup(
                        name="🔗 Proxy Labels", show=False
                    )
                    self._add_comprehensive_proxy_labels_layer(
                        proxy_layer, ground_truth_layers["proxy_labels"]
                    )
                    proxy_layer.add_to(ground_truth_group)

                ground_truth_group.add_to(m)

            # Add comprehensive controls and legends
            self._add_comprehensive_layer_control(m)
            self._add_comprehensive_legend(m, all_layers)
            self._add_comprehensive_info_panel(m)

            # Add comprehensive venue ranking sidebar
            all_venues = []
            for layer_group in [api_layers, scraped_layers]:
                for layer_name, layer_data in layer_group.items():
                    if "venues" in layer_name or layer_name == "places":
                        all_venues.extend(layer_data)

            if all_venues:
                self._add_comprehensive_venue_ranking_sidebar(m, all_venues)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"Comprehensive all-data map saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating comprehensive all-data map: {e}")
            return None

    def _calculate_center_from_all_layers(self, all_layers: Dict) -> List[float]:
        """Calculate map center from all available data layers."""
        lats, lons = [], []

        # Extract coordinates from all layer types
        for layer_group_name, layer_group in all_layers.items():
            for layer_name, layer_data in layer_group.items():
                if isinstance(layer_data, list):
                    for item in layer_data:
                        if isinstance(item, dict):
                            if "latitude" in item and "longitude" in item:
                                lats.append(item["latitude"])
                                lons.append(item["longitude"])
                            elif "lat" in item and "lng" in item:
                                lats.append(item["lat"])
                                lons.append(item["lng"])
                elif isinstance(layer_data, dict):
                    for key, value in layer_data.items():
                        if isinstance(key, tuple) and len(key) == 2:
                            lats.append(key[0])
                            lons.append(key[1])

        if lats and lons:
            return [sum(lats) / len(lats), sum(lons) / len(lons)]
        else:
            return list(self.center_coords)

    def _get_comprehensive_color_scheme(
        self, data_source_type: str, score: float
    ) -> Tuple[int, str, str]:
        """Get color scheme based on data source type and score."""
        if data_source_type == "api":
            # Blue color scheme for API data
            if score >= 0.8:
                return 12, "#08519c", "#08519c"  # Dark blue
            elif score >= 0.6:
                return 10, "#3182bd", "#3182bd"  # Medium blue
            elif score >= 0.4:
                return 8, "#6baed6", "#6baed6"  # Light blue
            elif score >= 0.2:
                return 6, "#9ecae1", "#9ecae1"  # Very light blue
            else:
                return 4, "#c6dbef", "#c6dbef"  # Pale blue
        elif data_source_type == "scraped":
            # Green color scheme for scraped data
            if score >= 0.8:
                return 12, "#00441b", "#00441b"  # Dark green
            elif score >= 0.6:
                return 10, "#238b45", "#238b45"  # Medium green
            elif score >= 0.4:
                return 8, "#66c2a4", "#66c2a4"  # Light green
            elif score >= 0.2:
                return 6, "#b2e2ab", "#b2e2ab"  # Very light green
            else:
                return 4, "#edf8e9", "#edf8e9"  # Pale green
        elif data_source_type == "calculated":
            # Red/Orange color scheme for calculated data
            if score >= 0.8:
                return 12, "#a50f15", "#a50f15"  # Dark red
            elif score >= 0.6:
                return 10, "#de2d26", "#de2d26"  # Medium red
            elif score >= 0.4:
                return 8, "#fb6a4a", "#fb6a4a"  # Orange-red
            elif score >= 0.2:
                return 6, "#fc9272", "#fc9272"  # Light orange
            else:
                return 4, "#fcbba1", "#fcbba1"  # Pale orange
        else:  # ground_truth
            # Purple color scheme for ground truth data
            if score >= 0.8:
                return 12, "#4a1486", "#4a1486"  # Dark purple
            elif score >= 0.6:
                return 10, "#6a51a3", "#6a51a3"  # Medium purple
            elif score >= 0.4:
                return 8, "#9e9ac8", "#9e9ac8"  # Light purple
            elif score >= 0.2:
                return 6, "#cbc9e2", "#cbc9e2"  # Very light purple
            else:
                return 4, "#f2f0f7", "#f2f0f7"  # Pale purple

    def _add_comprehensive_places_layer(
        self, layer: folium.FeatureGroup, places_data: List[Dict], data_source_type: str
    ):
        """Add comprehensive places layer with appropriate color scheme."""
        for place in places_data:
            lat = place.get("latitude", 0)
            lon = place.get("longitude", 0)
            score = place.get("total_score", 0)

            radius, color, fill_color = self._get_comprehensive_color_scheme(
                data_source_type, score
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 280px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{place.get('name', 'Unknown Place')}</h4>
                <p style="margin: 5px 0;"><strong>Category:</strong> {place.get('category', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Provider:</strong> {place.get('provider', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Rating:</strong> {place.get('avg_rating', 'N/A')}</p>
                <p style="margin: 5px 0;"><strong>Address:</strong> {place.get('address', 'N/A')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: {'#e3f2fd' if data_source_type == 'api' else '#e8f5e8' if data_source_type == 'scraped' else '#ffebee'}; border-radius: 3px;">
                    <small>{'📡 API Data' if data_source_type == 'api' else '🌐 Scraped Data' if data_source_type == 'scraped' else '🧠 Calculated Data'}</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=320),
                tooltip=f"Place: {place.get('name', 'Unknown')} | Score: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_events_layer(
        self, layer: folium.FeatureGroup, events_data: List[Dict], data_source_type: str
    ):
        """Add comprehensive events layer with appropriate color scheme."""
        for event in events_data:
            lat = event.get("latitude", 0)
            lon = event.get("longitude", 0)
            score = event.get("total_score", 0)

            radius, color, fill_color = self._get_comprehensive_color_scheme(
                data_source_type, score
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 280px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{event.get('name', 'Unknown Event')}</h4>
                <p style="margin: 5px 0;"><strong>Venue:</strong> {event.get('venue_name', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Date:</strong> {event.get('date', 'TBD')}</p>
                <p style="margin: 5px 0;"><strong>Category:</strong> {event.get('category', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Provider:</strong> {event.get('provider', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: {'#e3f2fd' if data_source_type == 'api' else '#e8f5e8' if data_source_type == 'scraped' else '#ffebee'}; border-radius: 3px;">
                    <small>{'📡 API Data' if data_source_type == 'api' else '🌐 Scraped Data' if data_source_type == 'scraped' else '🧠 Calculated Data'}</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=320),
                tooltip=f"Event: {event.get('name', 'Unknown')} | Score: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_weather_layer(
        self, layer: folium.FeatureGroup, weather_data: List[Dict]
    ):
        """Add comprehensive weather layer."""
        for weather in weather_data:
            lat = weather.get("latitude", 0)
            lon = weather.get("longitude", 0)
            temp = weather.get("temperature", 0)

            # Normalize temperature to 0-1 score for styling
            score = min(max((temp - 32) / 68, 0), 1)  # 32-100°F range
            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "api", score
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Weather Data</h4>
                <p style="margin: 5px 0;"><strong>Temperature:</strong> {temp}°F</p>
                <p style="margin: 5px 0;"><strong>Feels Like:</strong> {weather.get('feels_like', 'N/A')}°F</p>
                <p style="margin: 5px 0;"><strong>Conditions:</strong> {weather.get('conditions', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Humidity:</strong> {weather.get('humidity', 'N/A')}%</p>
                <p style="margin: 5px 0;"><strong>Wind:</strong> {weather.get('wind_speed', 'N/A')} mph</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>🌤️ Weather API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Weather: {temp}°F, {weather.get('conditions', 'Unknown')}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_traffic_layer(
        self, layer: folium.FeatureGroup, traffic_data: List[Dict]
    ):
        """Add comprehensive traffic congestion layer."""
        for traffic in traffic_data:
            lat = traffic.get("latitude", 0)
            lon = traffic.get("longitude", 0)
            congestion = traffic.get("congestion_score", 0)

            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "api", congestion
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Traffic Congestion</h4>
                <p style="margin: 5px 0;"><strong>Venue:</strong> {traffic.get('venue_name', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Congestion Score:</strong> {congestion:.3f}</p>
                <p style="margin: 5px 0;"><strong>Travel Time Downtown:</strong> {traffic.get('travel_time_downtown', 'N/A')} min</p>
                <p style="margin: 5px 0;"><strong>Travel Time Index:</strong> {traffic.get('travel_time_index', 'N/A')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>🚗 Traffic API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Traffic: {congestion:.2f} congestion",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_social_layer(
        self, layer: folium.FeatureGroup, social_data: List[Dict]
    ):
        """Add comprehensive social sentiment layer."""
        for social in social_data:
            lat = social.get("latitude")
            lon = social.get("longitude")

            if lat is None or lon is None:
                continue

            sentiment = social.get("positive_sentiment", 0)
            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "api", sentiment
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Social Sentiment</h4>
                <p style="margin: 5px 0;"><strong>Venue:</strong> {social.get('venue_name', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Platform:</strong> {social.get('platform', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Mentions:</strong> {social.get('mention_count', 0)}</p>
                <p style="margin: 5px 0;"><strong>Positive:</strong> {sentiment:.3f}</p>
                <p style="margin: 5px 0;"><strong>Engagement:</strong> {social.get('engagement_score', 0):.3f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>📱 Social API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Social: {sentiment:.2f} sentiment",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_economic_layer(
        self, layer: folium.FeatureGroup, economic_data: List[Dict]
    ):
        """Add comprehensive economic indicators layer."""
        # For economic data, we'll create representative points in Kansas City area
        kc_center = (39.0997, -94.5786)

        for i, econ in enumerate(economic_data):
            # Create distributed points around KC for economic indicators
            lat_offset = (i % 5 - 2) * 0.02  # Spread points around center
            lon_offset = ((i // 5) % 5 - 2) * 0.02
            lat = kc_center[0] + lat_offset
            lon = kc_center[1] + lon_offset

            # Use consumer confidence as score
            confidence = econ.get("consumer_confidence", 0.5)
            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "api", confidence
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Economic Indicators</h4>
                <p style="margin: 5px 0;"><strong>Area:</strong> {econ.get('geographic_area', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Unemployment:</strong> {econ.get('unemployment_rate', 'N/A')}%</p>
                <p style="margin: 5px 0;"><strong>Median Income:</strong> ${econ.get('median_household_income', 'N/A'):,.0f}</p>
                <p style="margin: 5px 0;"><strong>Consumer Confidence:</strong> {confidence:.3f}</p>
                <p style="margin: 5px 0;"><strong>Spending Index:</strong> {econ.get('local_spending_index', 'N/A')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                    <small>💰 Economic API Data</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Economic: {confidence:.2f} confidence",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_demographics_layer(
        self, layer: folium.FeatureGroup, demographics_data: List[Dict]
    ):
        """Add comprehensive demographics layer with choropleth."""
        for demo in demographics_data:
            geometry = demo.get("geometry")
            if not geometry:
                continue

            income_z = demo.get("median_income_z", 0)

            # Create choropleth polygon
            try:
                folium.GeoJson(
                    geometry,
                    style_function=lambda feature, income_z=income_z: {
                        "fillColor": self._get_demographic_color(income_z),
                        "color": "black",
                        "weight": 1,
                        "fillOpacity": 0.5,
                    },
                    popup=folium.Popup(
                        f"""
                    <div style="font-family: Arial, sans-serif; max-width: 250px;">
                        <h4 style="margin: 0 0 10px 0; color: #333;">Demographics</h4>
                        <p style="margin: 5px 0;"><strong>Tract:</strong> {demo.get('tract_id', 'Unknown')}</p>
                        <p style="margin: 5px 0;"><strong>Median Income:</strong> ${demo.get('median_income', 0):,.0f}</p>
                        <p style="margin: 5px 0;"><strong>Bachelor's Degree:</strong> {demo.get('pct_bachelors', 0):.1f}%</p>
                        <p style="margin: 5px 0;"><strong>Age 20-40:</strong> {demo.get('pct_age_20_40', 0):.1f}%</p>
                        <p style="margin: 5px 0;"><strong>Professional Occupation:</strong> {demo.get('pct_professional_occupation', 0):.1f}%</p>
                        <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                            <small>👥 Census Demographics</small>
                        </div>
                    </div>
                    """,
                        max_width=300,
                    ),
                    tooltip=f"Demographics: Tract {demo.get('tract_id', 'Unknown')}",
                ).add_to(layer)
            except Exception as e:
                logger.warning(f"Error adding demographic polygon: {e}")

    def _get_demographic_color(self, income_z: float) -> str:
        """Get color for demographic choropleth based on income z-score."""
        if income_z >= 2:
            return "#a50f15"  # High income - dark red
        elif income_z >= 1:
            return "#de2d26"  # Above average - medium red
        elif income_z >= 0:
            return "#fb6a4a"  # Average - orange-red
        elif income_z >= -1:
            return "#fc9272"  # Below average - light orange
        else:
            return "#fcbba1"  # Low income - pale orange

    def _add_comprehensive_predictions_layer(
        self, layer: folium.FeatureGroup, predictions_data: List[Dict]
    ):
        """Add comprehensive ML predictions layer."""
        for pred in predictions_data:
            lat = pred.get("latitude", 0)
            lon = pred.get("longitude", 0)
            density = pred.get("psychographic_density", 0)

            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "calculated", density
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">ML Prediction</h4>
                <p style="margin: 5px 0;"><strong>Density:</strong> {density:.3f}</p>
                <p style="margin: 5px 0;"><strong>Confidence:</strong> {pred.get('confidence_lower', 0):.3f} - {pred.get('confidence_upper', 1):.3f}</p>
                <p style="margin: 5px 0;"><strong>Model:</strong> {pred.get('model_version', 'Unknown')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                    <small>🤖 ML Prediction</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(4, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"ML: {density:.3f} density",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def _add_comprehensive_psychographic_layer(
        self, layer: folium.FeatureGroup, layer_data: Dict, layer_name: str
    ):
        """Add comprehensive psychographic layer."""
        for (lat, lon), data in layer_data.items():
            score = data.get("score", 0) if isinstance(data, dict) else data
            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "calculated", score
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">{layer_name.replace('_', ' ').title()}</h4>
                <p style="margin: 5px 0;"><strong>Score:</strong> {score:.3f}</p>
                <p style="margin: 5px 0;"><strong>Location:</strong> {lat:.4f}, {lon:.4f}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                    <small>🧠 Calculated Layer</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(3, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"{layer_name}: {score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=1,
            ).add_to(layer)

    def _add_comprehensive_manual_labels_layer(
        self, layer: folium.FeatureGroup, labels_data: List[Dict]
    ):
        """Add comprehensive manual labels layer."""
        for label in labels_data:
            lat = label.get("latitude", 0)
            lon = label.get("longitude", 0)
            density = label.get("psychographic_density", 0)

            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "ground_truth", density
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Manual Label</h4>
                <p style="margin: 5px 0;"><strong>Density:</strong> {density:.3f}</p>
                <p style="margin: 5px 0;"><strong>Labeler:</strong> {label.get('labeler_id', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Confidence:</strong> {label.get('confidence', 'N/A')}/5</p>
                <p style="margin: 5px 0;"><strong>Status:</strong> {label.get('validation_status', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Notes:</strong> {label.get('notes', 'None')[:50]}...</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #f3e5f5; border-radius: 3px;">
                    <small>✅ Manual Ground Truth</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(4, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Manual: {density:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_proxy_labels_layer(
        self, layer: folium.FeatureGroup, proxy_data: List[Dict]
    ):
        """Add comprehensive proxy labels layer."""
        for proxy in proxy_data:
            lat = proxy.get("latitude")
            lon = proxy.get("longitude")

            if lat is None or lon is None:
                continue

            density = proxy.get("psychographic_density", 0)
            radius, color, fill_color = self._get_comprehensive_color_scheme(
                "ground_truth", density
            )

            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 250px;">
                <h4 style="margin: 0 0 10px 0; color: #333;">Proxy Label</h4>
                <p style="margin: 5px 0;"><strong>Source:</strong> {proxy.get('source', 'Unknown')}</p>
                <p style="margin: 5px 0;"><strong>Density:</strong> {density:.3f}</p>
                <p style="margin: 5px 0;"><strong>Confidence:</strong> {proxy.get('confidence', 'N/A'):.3f}</p>
                <p style="margin: 5px 0;"><strong>Venue:</strong> {proxy.get('venue_name', 'N/A')}</p>
                <p style="margin: 5px 0;"><strong>Event:</strong> {proxy.get('event_name', 'N/A')}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #f3e5f5; border-radius: 3px;">
                    <small>🔗 Proxy Ground Truth</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=max(4, radius),
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"Proxy: {density:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(layer)

    def _add_comprehensive_layer_control(self, map_obj: folium.Map):
        """Add comprehensive layer control with enhanced styling."""
        folium.LayerControl(
            position="topright",
            collapsed=False,
            autoZIndex=True,
        ).add_to(map_obj)

        # Enhanced CSS for comprehensive layer control
        custom_css = """
        <style>
        .leaflet-control-layers {
            background: rgba(255, 255, 255, 0.98);
            border-radius: 10px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
            font-family: 'Segoe UI', Arial, sans-serif;
            border: 2px solid #333;
        }
        .leaflet-control-layers-expanded {
            padding: 15px;
            min-width: 250px;
            max-width: 350px;
        }
        .leaflet-control-layers-list {
            max-height: 500px;
            overflow-y: auto;
        }
        .leaflet-control-layers label {
            font-size: 14px;
            margin: 5px 0;
            display: flex;
            align-items: center;
            padding: 3px;
            border-radius: 4px;
            transition: background-color 0.2s;
        }
        .leaflet-control-layers label:hover {
            background-color: rgba(0, 123, 255, 0.1);
        }
        .leaflet-control-layers input[type="checkbox"] {
            margin-right: 10px;
            transform: scale(1.3);
        }
        .leaflet-control-layers-separator {
            border-top: 2px solid #ddd;
            margin: 10px 0;
        }
        </style>
        """
        map_obj.get_root().html.add_child(folium.Element(custom_css))

    def _add_comprehensive_legend(self, map_obj: folium.Map, all_layers: Dict):
        """Add comprehensive legend for all data layers."""
        legend_items = []

        # API Layers section
        api_layers = all_layers.get("api_layers", {})
        if api_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #1976d2; border-bottom: 2px solid #1976d2; padding-bottom: 3px;">📡 API Data Layers</h4>'
            )
            for layer_name in api_layers.keys():
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #08519c; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    f'<span style="font-size: 12px;">{layer_name.replace("_", " ").title()}</span>'
                )
                legend_items.append("</div>")
            legend_items.append("</div>")

        # Scraped Layers section
        scraped_layers = all_layers.get("scraped_layers", {})
        if scraped_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #2e7d32; border-bottom: 2px solid #2e7d32; padding-bottom: 3px;">🌐 Scraped Data Layers</h4>'
            )
            for layer_name in scraped_layers.keys():
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #00441b; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    f'<span style="font-size: 12px;">{layer_name.replace("_", " ").title()}</span>'
                )
                legend_items.append("</div>")
            legend_items.append("</div>")

        # Calculated Layers section
        calculated_layers = all_layers.get("calculated_layers", {})
        if calculated_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #d32f2f; border-bottom: 2px solid #d32f2f; padding-bottom: 3px;">🧠 Calculated Data Layers</h4>'
            )
            for layer_name in calculated_layers.keys():
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #a50f15; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    f'<span style="font-size: 12px;">{layer_name.replace("_", " ").title()}</span>'
                )
                legend_items.append("</div>")
            legend_items.append("</div>")

        # Ground Truth Layers section
        ground_truth_layers = all_layers.get("ground_truth_layers", {})
        if ground_truth_layers:
            legend_items.append('<div style="margin-bottom: 15px;">')
            legend_items.append(
                '<h4 style="margin: 0 0 8px 0; color: #7b1fa2; border-bottom: 2px solid #7b1fa2; padding-bottom: 3px;">✅ Ground Truth Layers</h4>'
            )
            for layer_name in ground_truth_layers.keys():
                legend_items.append(
                    '<div style="margin: 5px 0; display: flex; align-items: center;">'
                )
                legend_items.append(
                    '<div style="width: 12px; height: 12px; background: #4a1486; border-radius: 50%; margin-right: 8px;"></div>'
                )
                legend_items.append(
                    f'<span style="font-size: 12px;">{layer_name.replace("_", " ").title()}</span>'
                )
                legend_items.append("</div>")
            legend_items.append("</div>")

        # Score intensity guide
        legend_items.append(
            '<div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">'
        )
        legend_items.append(
            '<h5 style="margin: 0 0 8px 0; font-size: 12px; color: #666;">Score Intensity</h5>'
        )
        legend_items.append(
            '<div style="display: flex; align-items: center; margin: 3px 0;">'
        )
        legend_items.append(
            '<div style="width: 8px; height: 8px; border-radius: 50%; margin-right: 6px;"></div>'
        )
        legend_items.append(
            '<span style="font-size: 11px;">Larger = Higher Score</span>'
        )
        legend_items.append("</div>")
        legend_items.append("</div>")

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 320px; height: auto; max-height: 600px;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 10px;
                    z-index: 9999; font-size: 12px; padding: 20px;
                    box-shadow: 0 6px 25px rgba(0,0,0,0.3);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 20px 0; color: #333; text-align: center; border-bottom: 3px solid #333; padding-bottom: 10px; font-size: 16px;">
            🗺️ Comprehensive Data Legend
        </h3>
        {"".join(legend_items)}
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_comprehensive_info_panel(self, map_obj: folium.Map):
        """Add comprehensive information panel explaining all layer types."""
        info_html = """
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 350px; height: auto; max-height: 80vh;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 10px;
                    z-index: 9999; font-size: 12px; padding: 20px;
                    box-shadow: 0 6px 25px rgba(0,0,0,0.3);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 20px 0; color: #333; text-align: center; border-bottom: 3px solid #333; padding-bottom: 10px; font-size: 16px;">
            🎯 Comprehensive Map Guide
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #1976d2; font-size: 14px;">📡 API Data Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.5;">
                Real-time data from external APIs including Google Places, PredictHQ events, weather services, 
                traffic APIs, social media sentiment, and economic indicators.
                <strong>Blue color scheme</strong> indicates API-sourced data.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 14px;">🌐 Scraped Data Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.5;">
                Data scraped from local venue websites including static venues (major theaters, museums) 
                and dynamic venues (event aggregators, nightlife sites).
                <strong>Green color scheme</strong> indicates scraped data.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #d32f2f; font-size: 14px;">🧠 Calculated Data Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.5;">
                Calculated layers including ML predictions, demographics, psychographic models 
                (college density, spending propensity), and feature engineering results.
                <strong>Red/orange color scheme</strong> indicates calculated data.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #7b1fa2; font-size: 14px;">✅ Ground Truth Layers</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.5;">
                Validation data including manual labels from human annotators and proxy labels 
                inferred from external sources for model training and validation.
                <strong>Purple color scheme</strong> indicates ground truth data.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #333; font-size: 14px;">🎛️ Interactive Controls</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.5;">
                Use the layer control panel (top-right) to toggle individual layers on/off.
                Click any marker for detailed information. Use the venue ranking sidebar to browse top venues.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 20px; padding-top: 15px; border-top: 2px solid #ddd;">
            <small style="color: #999; font-size: 10px; font-weight: bold;">
                PDPE Comprehensive All-Data Map v3.0<br>
                Psychographic Prediction & Data Engineering
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))

    def _add_comprehensive_venue_ranking_sidebar(
        self, map_obj: folium.Map, all_venues: List[Dict]
    ):
        """Add comprehensive venue ranking sidebar for all venue types."""
        if not all_venues:
            return

        # Sort venues by score and categorize by data source
        sorted_venues = sorted(
            all_venues, key=lambda x: x.get("total_score", 0), reverse=True
        )

        # Create venue list HTML with data source indicators
        venue_items = []
        for i, venue in enumerate(sorted_venues[:50]):  # Show top 30 venues
            name = venue.get("name", "Unknown Venue")
            score = venue.get("total_score", 0)
            category = venue.get("category", "Unknown")
            provider = venue.get("provider", "Unknown")
            data_source = venue.get("data_source", "unknown")
            lat = venue.get("latitude", 0)
            lon = venue.get("longitude", 0)

            # Determine score color and data source icon
            if score >= 0.8:
                score_color = "#d73027"
            elif score >= 0.6:
                score_color = "#fc8d59"
            elif score >= 0.4:
                score_color = "#fee08b"
            elif score >= 0.2:
                score_color = "#91bfdb"
            else:
                score_color = "#c6dbef"

            # Data source icon and color
            if data_source == "api_places":
                source_icon = "📡"
                source_color = "#e3f2fd"
            elif data_source in ["scraped_static", "scraped_dynamic", "scraped_local"]:
                source_icon = "🌐"
                source_color = "#e8f5e8"
            else:
                source_icon = "🔍"
                source_color = "#fff3e0"

            venue_items.append(
                f"""
            <div class="venue-item" onclick="centerMapOnVenue({lat}, {lon})" 
                 style="padding: 10px; margin: 5px 0; border-left: 4px solid {score_color}; 
                        background: rgba(255,255,255,0.95); cursor: pointer; border-radius: 6px;
                        transition: all 0.3s; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div style="flex: 1;">
                        <div style="font-weight: bold; font-size: 13px; color: #333; margin-bottom: 3px;">
                            #{i+1}. {name[:35]}{"..." if len(name) > 35 else ""}
                        </div>
                        <div style="font-size: 11px; color: #666; margin-bottom: 3px;">
                            {category} • {provider}
                        </div>
                        <div style="font-size: 10px; color: #999; margin-bottom: 3px;">
                            Score: {score:.3f}
                        </div>
                        <div style="display: inline-block; padding: 2px 6px; background-color: {source_color}; 
                                    border-radius: 3px; font-size: 9px; color: #666;">
                            {source_icon} {data_source.replace('_', ' ').title()}
                        </div>
                    </div>
                </div>
            </div>
            """
            )

        sidebar_html = f"""
        <div id="comprehensive-venue-sidebar" style="position: fixed; 
                    top: 20px; left: 20px; width: 360px; height: 75vh;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 10px;
                    z-index: 9998; font-size: 12px; padding: 0;
                    box-shadow: 0 6px 25px rgba(0,0,0,0.3);
                    display: flex; flex-direction: column;">
            
            <!-- Header -->
            <div style="padding: 20px; border-bottom: 3px solid #333; background: linear-gradient(135deg, #f8f9fa, #e9ecef); border-radius: 8px 8px 0 0;">
                <h3 style="margin: 0; color: #333; text-align: center; font-size: 16px; font-weight: bold;">
                    🏆 Comprehensive Venue Rankings
                </h3>
                <div style="text-align: center; margin-top: 8px;">
                    <small style="color: #666; font-size: 11px;">
                        All Data Sources • Click venue to center map<br>
                        Total: {len(all_venues)} venues from {len(set(v.get('data_source', 'unknown') for v in all_venues))} sources
                    </small>
                </div>
            </div>
            
            <!-- Venue List -->
            <div style="flex: 1; overflow-y: auto; padding: 15px;">
                {"".join(venue_items)}
            </div>
            
            <!-- Footer -->
            <div style="padding: 15px; border-top: 2px solid #ddd; background: linear-gradient(135deg, #f8f9fa, #e9ecef); border-radius: 0 0 8px 8px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-size: 10px; color: #666;">
                        <div>📡 API Data • 🌐 Scraped Data</div>
                        <div>Psychographic Scores (0.0-1.0)</div>
                    </div>
                    <button onclick="toggleComprehensiveSidebar()" style="background: linear-gradient(135deg, #007bff, #0056b3); 
                            color: white; border: none; padding: 6px 12px; border-radius: 4px; 
                            font-size: 11px; cursor: pointer; font-weight: bold;">
                        Hide
                    </button>
                </div>
            </div>
        </div>

        <script>
        // Function to center map on venue
        function centerMapOnVenue(lat, lon) {{
            var mapContainer = document.querySelector('.folium-map');
            if (mapContainer && mapContainer._leaflet_map) {{
                var map = mapContainer._leaflet_map;
                map.setView([lat, lon], 16);
                
                // Add a temporary marker with enhanced styling
                var tempMarker = L.marker([lat, lon], {{
                    icon: L.icon({{
                        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-gold.png',
                        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                        iconSize: [30, 48],
                        iconAnchor: [15, 48],
                        popupAnchor: [1, -42],
                        shadowSize: [48, 48]
                    }})
                }}).addTo(map);
                
                // Remove the temporary marker after 4 seconds
                setTimeout(function() {{
                    map.removeLayer(tempMarker);
                }}, 4000);
            }}
        }}

        // Function to toggle comprehensive sidebar visibility
        function toggleComprehensiveSidebar() {{
            var sidebar = document.getElementById('comprehensive-venue-sidebar');
            if (sidebar.style.display === 'none') {{
                sidebar.style.display = 'flex';
            }} else {{
                sidebar.style.display = 'none';
            }}
        }}

        // Add enhanced hover effects
        document.addEventListener('DOMContentLoaded', function() {{
            var venueItems = document.querySelectorAll('.venue-item');
            venueItems.forEach(function(item) {{
                item.addEventListener('mouseenter', function() {{
                    this.style.backgroundColor = 'rgba(0, 123, 255, 0.15)';
                    this.style.transform = 'translateX(5px)';
                    this.style.boxShadow = '0 4px 12px rgba(0,0,0,0.2)';
                }});
                item.addEventListener('mouseleave', function() {{
                    this.style.backgroundColor = 'rgba(255,255,255,0.95)';
                    this.style.transform = 'translateX(0)';
                    this.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
                }});
            }});
        }});
        </script>

        <style>
        .venue-item:hover {{
            background-color: rgba(0, 123, 255, 0.15) !important;
            transform: translateX(5px) !important;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2) !important;
        }}
        
        #comprehensive-venue-sidebar::-webkit-scrollbar {{
            width: 8px;
        }}
        
        #comprehensive-venue-sidebar::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 4px;
        }}
        
        #comprehensive-venue-sidebar::-webkit-scrollbar-thumb {{
            background: linear-gradient(135deg, #888, #555);
            border-radius: 4px;
        }}
        
        #comprehensive-venue-sidebar::-webkit-scrollbar-thumb:hover {{
            background: linear-gradient(135deg, #555, #333);
        }}
        </style>
        """

        map_obj.get_root().html.add_child(folium.Element(sidebar_html))
