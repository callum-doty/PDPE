"""
Enhanced InteractiveMapBuilder with proper Mapbox integration for PDPE.
"""

import folium
import os
import logging
from typing import List, Dict, Tuple, Optional
from pathlib import Path
import webbrowser
import json
import numpy as np

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
