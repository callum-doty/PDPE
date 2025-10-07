"""
Unified Maps Service for PPM Application

Single service that consolidates ALL map visualization functionality:
- Interactive map creation with Folium
- Venue and event marker visualization
- Heatmap generation for predictions
- Multi-layer map support with toggleable layers
- Export capabilities (HTML, GeoJSON)
- Mapbox integration with fallback to OpenStreetMap

Replaces the entire features/visualization/ directory structure.
"""

import logging
import os
import json
import webbrowser
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import folium
import numpy as np

# Folium plugins with graceful fallback
try:
    from folium.plugins import HeatMap

    HEATMAP_AVAILABLE = True
except ImportError:
    HEATMAP_AVAILABLE = False
    logging.warning("HeatMap plugin not available - using circle markers as fallback")

# Import core services
from core.database import get_database, OperationResult
from core.quality import get_quality_validator


@dataclass
class MapConfig:
    """Configuration for map creation"""

    center_coords: Tuple[float, float] = (39.0997, -94.5786)  # Kansas City
    zoom_level: int = 11
    style: str = "streets"  # streets, satellite, light, dark, outdoors
    width: int = 900
    height: int = 600


@dataclass
class LayerConfig:
    """Configuration for map layers"""

    name: str
    show_by_default: bool = True
    color_scheme: str = "default"  # default, api, scraped, calculated, ground_truth
    marker_size_multiplier: float = 1.0
    opacity: float = 0.7


class MapService:
    """
    Unified map service that handles ALL visualization operations.

    Consolidates functionality from:
    - features/visualization/builders/interactive_map_builder.py (2000+ lines)
    - Map creation, styling, and export functionality
    - Multi-layer support with comprehensive legends

    Into a single, manageable service with clear entry points.
    """

    def __init__(self, config: Optional[MapConfig] = None):
        self.logger = logging.getLogger(__name__)
        self.db = get_database()
        self.quality_validator = get_quality_validator()

        # Map configuration
        self.config = config or MapConfig()

        # Get Mapbox token from environment
        self.mapbox_token = self._get_mapbox_token()

        # Map style configurations
        self.map_styles = {
            "streets": "mapbox/streets-v12",
            "satellite": "mapbox/satellite-streets-v12",
            "light": "mapbox/light-v11",
            "dark": "mapbox/dark-v11",
            "outdoors": "mapbox/outdoors-v12",
        }

        # Color schemes for different data sources
        self.color_schemes = {
            "default": {
                "high": "#d73027",
                "med_high": "#fc8d59",
                "medium": "#fee08b",
                "low_med": "#e0f3f8",
                "low": "#91bfdb",
            },
            "api": {
                "high": "#08519c",
                "med_high": "#3182bd",
                "medium": "#6baed6",
                "low_med": "#9ecae1",
                "low": "#c6dbef",
            },
            "scraped": {
                "high": "#00441b",
                "med_high": "#238b45",
                "medium": "#66c2a4",
                "low_med": "#b2e2ab",
                "low": "#edf8e9",
            },
            "calculated": {
                "high": "#a50f15",
                "med_high": "#de2d26",
                "medium": "#fb6a4a",
                "low_med": "#fc9272",
                "low": "#fcbba1",
            },
            "ground_truth": {
                "high": "#4a1486",
                "med_high": "#6a51a3",
                "medium": "#9e9ac8",
                "low_med": "#cbc9e2",
                "low": "#f2f0f7",
            },
        }

    # ========== PUBLIC API METHODS ==========

    def create_venue_map(
        self, venue_filters: Optional[Dict] = None, output_path: str = "venue_map.html"
    ) -> OperationResult:
        """
        Create interactive map showing venues.

        Args:
            venue_filters: Optional filters for venues (category, has_location, etc.)
            output_path: Output HTML file path

        Returns:
            OperationResult with map creation status
        """
        start_time = self.logger.info("🗺️ Creating venue map")

        try:
            # Get venue data
            venues = self.db.get_venues(venue_filters)
            if not venues:
                return OperationResult(
                    success=False,
                    error="No venues found",
                    message="No venues available for map creation",
                )

            # Create base map
            center = self._calculate_center_from_venues(venues)
            m = self._create_base_map(center, self.config.zoom_level, self.config.style)

            # Add venue markers
            venue_layer = folium.FeatureGroup(name="Venues", show=True)
            for venue in venues:
                if venue.get("lat") and venue.get("lng"):
                    self._add_venue_marker(venue_layer, venue, "default")
            venue_layer.add_to(m)

            # Add controls and legend
            folium.LayerControl().add_to(m)
            self._add_venue_legend(m, len(venues))

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            return OperationResult(
                success=True,
                data=str(output_file),
                message=f"Venue map created with {len(venues)} venues: {output_file}",
            )

        except Exception as e:
            self.logger.error(f"Venue map creation failed: {e}")
            return OperationResult(
                success=False, error=str(e), message=f"Venue map creation failed: {e}"
            )

    def create_event_map(
        self, event_filters: Optional[Dict] = None, output_path: str = "event_map.html"
    ) -> OperationResult:
        """
        Create interactive map showing events.

        Args:
            event_filters: Optional filters for events (category, date_range, etc.)
            output_path: Output HTML file path

        Returns:
            OperationResult with map creation status
        """
        start_time = self.logger.info("🎭 Creating event map")

        try:
            # Get event data
            events = self.db.get_events(event_filters)
            if not events:
                return OperationResult(
                    success=False,
                    error="No events found",
                    message="No events available for map creation",
                )

            # Filter events with location data
            located_events = [e for e in events if e.get("lat") and e.get("lng")]
            if not located_events:
                return OperationResult(
                    success=False,
                    error="No events with location data",
                    message="No events have location coordinates for mapping",
                )

            # Create base map
            center = self._calculate_center_from_events(located_events)
            m = self._create_base_map(center, self.config.zoom_level, self.config.style)

            # Add event markers
            event_layer = folium.FeatureGroup(name="Events", show=True)
            for event in located_events:
                self._add_event_marker(event_layer, event, "default")
            event_layer.add_to(m)

            # Add controls and legend
            folium.LayerControl().add_to(m)
            self._add_event_legend(m, len(located_events))

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            return OperationResult(
                success=True,
                data=str(output_file),
                message=f"Event map created with {len(located_events)} events: {output_file}",
            )

        except Exception as e:
            self.logger.error(f"Event map creation failed: {e}")
            return OperationResult(
                success=False, error=str(e), message=f"Event map creation failed: {e}"
            )

    def create_prediction_heatmap(
        self,
        bounds: Optional[Dict] = None,
        output_path: str = "prediction_heatmap.html",
    ) -> OperationResult:
        """
        Create heatmap showing ML predictions.

        Args:
            bounds: Geographic bounds (min_lat, max_lat, min_lng, max_lng)
            output_path: Output HTML file path

        Returns:
            OperationResult with map creation status
        """
        self.logger.info("🤖 Creating prediction heatmap")

        try:
            # Get prediction data
            predictions = self.db.get_predictions()
            if not predictions:
                return OperationResult(
                    success=False,
                    error="No predictions found",
                    message="No ML predictions available for heatmap creation",
                )

            # Filter predictions with location data
            located_predictions = []
            for pred in predictions:
                if pred.get("lat") and pred.get("lng"):
                    located_predictions.append(pred)

            if not located_predictions:
                return OperationResult(
                    success=False,
                    error="No predictions with location data",
                    message="No predictions have location coordinates for mapping",
                )

            # Create base map
            center = self._calculate_center_from_predictions(located_predictions)
            m = self._create_base_map(center, self.config.zoom_level, self.config.style)

            # Add prediction heatmap
            if HEATMAP_AVAILABLE:
                self._add_prediction_heatmap_layer(m, located_predictions)
            else:
                # Fallback to circle markers
                pred_layer = folium.FeatureGroup(name="Predictions", show=True)
                for pred in located_predictions:
                    self._add_prediction_marker(pred_layer, pred, "calculated")
                pred_layer.add_to(m)

            # Add high-value prediction markers
            self._add_high_value_prediction_markers(m, located_predictions)

            # Add controls and legend
            folium.LayerControl().add_to(m)
            self._add_prediction_legend(m, len(located_predictions))

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            return OperationResult(
                success=True,
                data=str(output_file),
                message=f"Prediction heatmap created with {len(located_predictions)} predictions: {output_file}",
            )

        except Exception as e:
            self.logger.error(f"Prediction heatmap creation failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Prediction heatmap creation failed: {e}",
            )

    def create_combined_map(
        self,
        include_venues: bool = True,
        include_events: bool = True,
        include_predictions: bool = True,
        venue_filters: Optional[Dict] = None,
        event_filters: Optional[Dict] = None,
        output_path: str = "combined_map.html",
    ) -> OperationResult:
        """
        Create comprehensive map with multiple data layers.

        Args:
            include_venues: Include venue markers
            include_events: Include event markers
            include_predictions: Include prediction heatmap
            venue_filters: Optional filters for venues
            event_filters: Optional filters for events
            output_path: Output HTML file path

        Returns:
            OperationResult with map creation status
        """
        self.logger.info("🌟 Creating combined map with multiple layers")

        try:
            # Collect all data
            venues = self.db.get_venues(venue_filters) if include_venues else []
            events = self.db.get_events(event_filters) if include_events else []
            predictions = self.db.get_predictions() if include_predictions else []

            # Filter for location data
            located_venues = [v for v in venues if v.get("lat") and v.get("lng")]
            located_events = [e for e in events if e.get("lat") and e.get("lng")]
            located_predictions = [
                p for p in predictions if p.get("lat") and p.get("lng")
            ]

            if not any([located_venues, located_events, located_predictions]):
                return OperationResult(
                    success=False,
                    error="No data with location coordinates",
                    message="No venues, events, or predictions have location data for mapping",
                )

            # Calculate center from all available data
            center = self._calculate_center_from_all_data(
                located_venues, located_events, located_predictions
            )

            # Create base map
            m = self._create_base_map(center, self.config.zoom_level, self.config.style)

            # Add venue layer
            if located_venues:
                venue_layer = folium.FeatureGroup(name="🏢 Venues", show=True)
                for venue in located_venues:
                    self._add_venue_marker(venue_layer, venue, "default")
                venue_layer.add_to(m)

            # Add event layer
            if located_events:
                event_layer = folium.FeatureGroup(name="🎭 Events", show=True)
                for event in located_events:
                    self._add_event_marker(event_layer, event, "default")
                event_layer.add_to(m)

            # Add prediction layer
            if located_predictions:
                if HEATMAP_AVAILABLE:
                    pred_layer = folium.FeatureGroup(name="🤖 Predictions", show=False)
                    self._add_prediction_heatmap_layer(pred_layer, located_predictions)
                    pred_layer.add_to(m)
                else:
                    pred_layer = folium.FeatureGroup(name="🤖 Predictions", show=False)
                    for pred in located_predictions:
                        self._add_prediction_marker(pred_layer, pred, "calculated")
                    pred_layer.add_to(m)

            # Add controls and comprehensive legend
            folium.LayerControl().add_to(m)
            self._add_combined_legend(
                m, len(located_venues), len(located_events), len(located_predictions)
            )

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            total_items = (
                len(located_venues) + len(located_events) + len(located_predictions)
            )
            return OperationResult(
                success=True,
                data=str(output_file),
                message=f"Combined map created with {total_items} total items: {output_file}",
            )

        except Exception as e:
            self.logger.error(f"Combined map creation failed: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Combined map creation failed: {e}",
            )

    def export_to_geojson(
        self,
        data_type: str = "venues",
        filters: Optional[Dict] = None,
        output_path: str = "export.geojson",
    ) -> OperationResult:
        """
        Export data to GeoJSON format.

        Args:
            data_type: Type of data to export ('venues', 'events', 'predictions')
            filters: Optional filters for data
            output_path: Output GeoJSON file path

        Returns:
            OperationResult with export status
        """
        self.logger.info(f"📄 Exporting {data_type} to GeoJSON")

        try:
            # Get data based on type
            if data_type == "venues":
                data = self.db.get_venues(filters)
            elif data_type == "events":
                data = self.db.get_events(filters)
            elif data_type == "predictions":
                data = self.db.get_predictions()
            else:
                return OperationResult(
                    success=False,
                    error="Invalid data type",
                    message=f"Data type '{data_type}' not supported. Use 'venues', 'events', or 'predictions'",
                )

            if not data:
                return OperationResult(
                    success=False,
                    error="No data found",
                    message=f"No {data_type} data available for export",
                )

            # Convert to GeoJSON
            features = []
            for item in data:
                lat = item.get("lat") or item.get("latitude")
                lng = item.get("lng") or item.get("longitude")

                if lat and lng:
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lng, lat],  # GeoJSON uses [lng, lat] order
                        },
                        "properties": {
                            k: v
                            for k, v in item.items()
                            if k not in ["lat", "lng", "latitude", "longitude"]
                        },
                    }
                    features.append(feature)

            geojson_data = {"type": "FeatureCollection", "features": features}

            # Save GeoJSON file
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, "w") as f:
                json.dump(geojson_data, f, indent=2, default=str)

            return OperationResult(
                success=True,
                data=str(output_file),
                message=f"Exported {len(features)} {data_type} features to GeoJSON: {output_file}",
            )

        except Exception as e:
            self.logger.error(f"GeoJSON export failed: {e}")
            return OperationResult(
                success=False, error=str(e), message=f"GeoJSON export failed: {e}"
            )

    def open_map_in_browser(self, file_path: str) -> OperationResult:
        """
        Open generated map in default browser.

        Args:
            file_path: Path to HTML map file

        Returns:
            OperationResult with browser open status
        """
        try:
            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                return OperationResult(
                    success=False,
                    error="File not found",
                    message=f"Map file not found: {file_path}",
                )

            webbrowser.open(f"file://{file_path_obj.absolute()}")

            return OperationResult(
                success=True,
                data=str(file_path_obj.absolute()),
                message=f"Opened map in browser: {file_path}",
            )

        except Exception as e:
            self.logger.error(f"Failed to open map in browser: {e}")
            return OperationResult(
                success=False,
                error=str(e),
                message=f"Failed to open map in browser: {e}",
            )

    # ========== PRIVATE IMPLEMENTATION METHODS ==========

    def _get_mapbox_token(self) -> Optional[str]:
        """Get Mapbox access token from environment variables"""
        token = os.getenv("MAPBOX_ACCESS_TOKEN") or os.getenv("MAPBOX_API_KEY")

        if not token:
            self.logger.warning(
                "No Mapbox token found. Using OpenStreetMap tiles as fallback."
            )
            return None

        if not token.startswith("pk."):
            self.logger.error(
                "Invalid Mapbox token format. Token should start with 'pk.'"
            )
            return None

        return token

    def _create_base_map(
        self, center: List[float], zoom: int, style: str
    ) -> folium.Map:
        """Create base map with Mapbox tiles if available, otherwise OpenStreetMap"""
        # Create map without default tiles
        m = folium.Map(location=center, zoom_start=zoom, tiles=None)

        if self.mapbox_token:
            try:
                # Add Mapbox tile layer
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

                self.logger.debug(f"Mapbox {style} tiles loaded successfully")

            except Exception as e:
                self.logger.error(f"Failed to load Mapbox tiles: {e}")
                self._add_fallback_tiles(m)
        else:
            # Use OpenStreetMap as fallback
            self._add_fallback_tiles(m)

        return m

    def _add_fallback_tiles(self, map_obj: folium.Map):
        """Add fallback OpenStreetMap tiles"""
        folium.TileLayer(
            tiles="OpenStreetMap", name="OpenStreetMap", overlay=False, control=True
        ).add_to(map_obj)

        folium.TileLayer(
            tiles="CartoDB positron", name="CartoDB Light", overlay=False, control=True
        ).add_to(map_obj)

        self.logger.debug("Using OpenStreetMap fallback tiles")

    def _get_marker_style(
        self, score: float, color_scheme: str = "default"
    ) -> Tuple[int, str, str]:
        """Get marker styling based on score and color scheme"""
        colors = self.color_schemes.get(color_scheme, self.color_schemes["default"])

        if score >= 0.8:
            return 12, colors["high"], colors["high"]
        elif score >= 0.6:
            return 10, colors["med_high"], colors["med_high"]
        elif score >= 0.4:
            return 8, colors["medium"], colors["medium"]
        elif score >= 0.2:
            return 6, colors["low_med"], colors["low_med"]
        else:
            return 4, colors["low"], colors["low"]

    def _add_venue_marker(
        self, layer: folium.FeatureGroup, venue: Dict, color_scheme: str
    ):
        """Add venue marker to layer"""
        lat, lng = venue.get("lat"), venue.get("lng")
        if not lat or not lng:
            return

        # Safe rating normalization with None check
        rating = venue.get("avg_rating") or 3.0
        score = rating / 5.0 if rating is not None else 0.6  # Normalize rating to 0-1
        radius, color, fill_color = self._get_marker_style(score, color_scheme)

        popup_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 280px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{venue.get('name', 'Unknown Venue')}</h4>
            <p style="margin: 5px 0;"><strong>Category:</strong> {venue.get('category', 'Unknown')}</p>
            <p style="margin: 5px 0;"><strong>Rating:</strong> {venue.get('avg_rating', 'N/A')}</p>
            <p style="margin: 5px 0;"><strong>Address:</strong> {venue.get('address', 'N/A')}</p>
            <p style="margin: 5px 0;"><strong>Provider:</strong> {venue.get('provider', 'Unknown')}</p>
            <div style="margin-top: 10px; padding: 5px; background-color: #f0f8ff; border-radius: 3px;">
                <small>🏢 Venue Data</small>
            </div>
        </div>
        """

        folium.CircleMarker(
            location=(lat, lng),
            radius=radius,
            popup=folium.Popup(popup_content, max_width=320),
            tooltip=f"Venue: {venue.get('name', 'Unknown')} | Rating: {venue.get('avg_rating', 'N/A')}",
            color=color,
            fill=True,
            fillColor=fill_color,
            fillOpacity=0.7,
            weight=2,
        ).add_to(layer)

    def _add_event_marker(
        self, layer: folium.FeatureGroup, event: Dict, color_scheme: str
    ):
        """Add event marker to layer"""
        lat, lng = event.get("lat"), event.get("lng")
        if not lat or not lng:
            return

        # Use a default score for events or calculate from available data
        score = 0.5  # Default middle score
        radius, color, fill_color = self._get_marker_style(score, color_scheme)

        popup_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 280px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">{event.get('name', 'Unknown Event')}</h4>
            <p style="margin: 5px 0;"><strong>Venue:</strong> {event.get('venue_name', 'Unknown')}</p>
            <p style="margin: 5px 0;"><strong>Category:</strong> {event.get('category', 'Unknown')}</p>
            <p style="margin: 5px 0;"><strong>Start Time:</strong> {event.get('start_time', 'TBD')}</p>
            <p style="margin: 5px 0;"><strong>Provider:</strong> {event.get('provider', 'Unknown')}</p>
            <div style="margin-top: 10px; padding: 5px; background-color: #fff3e0; border-radius: 3px;">
                <small>🎭 Event Data</small>
            </div>
        </div>
        """

        folium.CircleMarker(
            location=(lat, lng),
            radius=radius,
            popup=folium.Popup(popup_content, max_width=320),
            tooltip=f"Event: {event.get('name', 'Unknown')}",
            color=color,
            fill=True,
            fillColor=fill_color,
            fillOpacity=0.7,
            weight=2,
        ).add_to(layer)

    def _add_prediction_marker(
        self, layer: folium.FeatureGroup, prediction: Dict, color_scheme: str
    ):
        """Add prediction marker to layer"""
        lat, lng = prediction.get("lat"), prediction.get("lng")
        if not lat or not lng:
            return

        score = prediction.get("prediction_value", 0.5)
        radius, color, fill_color = self._get_marker_style(score, color_scheme)

        popup_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 280px;">
            <h4 style="margin: 0 0 10px 0; color: #333;">ML Prediction</h4>
            <p style="margin: 5px 0;"><strong>Prediction Value:</strong> {score:.3f}</p>
            <p style="margin: 5px 0;"><strong>Confidence:</strong> {prediction.get('confidence_score', 'N/A')}</p>
            <p style="margin: 5px 0;"><strong>Type:</strong> {prediction.get('prediction_type', 'Unknown')}</p>
            <p style="margin: 5px 0;"><strong>Model:</strong> {prediction.get('model_version', 'Unknown')}</p>
            <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                <small>🤖 ML Prediction</small>
            </div>
        </div>
        """

        folium.CircleMarker(
            location=(lat, lng),
            radius=max(4, radius),
            popup=folium.Popup(popup_content, max_width=320),
            tooltip=f"Prediction: {score:.3f}",
            color=color,
            fill=True,
            fillColor=fill_color,
            fillOpacity=0.6,
            weight=1,
        ).add_to(layer)

    def _add_prediction_heatmap_layer(
        self, map_obj: folium.Map, predictions: List[Dict]
    ):
        """Add prediction heatmap layer using HeatMap plugin"""
        if not HEATMAP_AVAILABLE:
            return

        # Prepare heatmap data
        heat_data = []

        # Get prediction values, filtering out None values
        prediction_values = [
            p.get("prediction_value", 0)
            for p in predictions
            if p.get("prediction_value") is not None and p.get("prediction_value") > 0
        ]

        # Calculate max_value safely
        if prediction_values:
            max_value = max(prediction_values)
        else:
            max_value = 1.0

        # Ensure max_value is not zero or None
        if max_value is None or max_value <= 0:
            max_value = 1.0

        for pred in predictions:
            lat, lng = pred.get("lat"), pred.get("lng")
            value = pred.get("prediction_value")

            if lat and lng and value is not None and value > 0:
                # Safe division with additional check
                try:
                    intensity = value / max_value if max_value > 0 else 0.5
                    heat_data.append(
                        [lat, lng, max(0.1, intensity)]
                    )  # Ensure minimum intensity
                except (TypeError, ZeroDivisionError):
                    heat_data.append([lat, lng, 0.5])  # Default intensity

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
            ).add_to(map_obj)

    def _add_high_value_prediction_markers(
        self, map_obj: folium.Map, predictions: List[Dict]
    ):
        """Add special markers for high-value predictions"""
        if not predictions:
            return

        # Find high-value predictions (top 20%)
        values = [p.get("prediction_value", 0) for p in predictions]
        threshold = np.percentile(values, 80) if values else 0.8

        for pred in predictions:
            value = pred.get("prediction_value", 0)
            if value >= threshold:
                lat, lng = pred.get("lat"), pred.get("lng")
                if lat and lng:
                    folium.Marker(
                        location=(lat, lng),
                        popup=f"High Value Prediction: {value:.3f}",
                        tooltip=f"High Value: {value:.3f}",
                        icon=folium.Icon(color="red", icon="star"),
                    ).add_to(map_obj)

    def _calculate_center_from_venues(self, venues: List[Dict]) -> List[float]:
        """Calculate map center from venue coordinates"""
        lats = [v["lat"] for v in venues if v.get("lat")]
        lngs = [v["lng"] for v in venues if v.get("lng")]

        if lats and lngs:
            return [sum(lats) / len(lats), sum(lngs) / len(lngs)]
        else:
            return list(self.config.center_coords)

    def _calculate_center_from_events(self, events: List[Dict]) -> List[float]:
        """Calculate map center from event coordinates"""
        lats = [e["lat"] for e in events if e.get("lat")]
        lngs = [e["lng"] for e in events if e.get("lng")]

        if lats and lngs:
            return [sum(lats) / len(lats), sum(lngs) / len(lngs)]
        else:
            return list(self.config.center_coords)

    def _calculate_center_from_predictions(
        self, predictions: List[Dict]
    ) -> List[float]:
        """Calculate map center from prediction coordinates"""
        lats = [p["lat"] for p in predictions if p.get("lat")]
        lngs = [p["lng"] for p in predictions if p.get("lng")]

        if lats and lngs:
            return [sum(lats) / len(lats), sum(lngs) / len(lngs)]
        else:
            return list(self.config.center_coords)

    def _calculate_center_from_all_data(
        self, venues: List[Dict], events: List[Dict], predictions: List[Dict]
    ) -> List[float]:
        """Calculate map center from all available data"""
        lats, lngs = [], []

        # Collect coordinates from all data sources
        for venue in venues:
            if venue.get("lat") and venue.get("lng"):
                lats.append(venue["lat"])
                lngs.append(venue["lng"])

        for event in events:
            if event.get("lat") and event.get("lng"):
                lats.append(event["lat"])
                lngs.append(event["lng"])

        for pred in predictions:
            if pred.get("lat") and pred.get("lng"):
                lats.append(pred["lat"])
                lngs.append(pred["lng"])

        if lats and lngs:
            return [sum(lats) / len(lats), sum(lngs) / len(lngs)]
        else:
            return list(self.config.center_coords)

    def _add_venue_legend(self, map_obj: folium.Map, venue_count: int):
        """Add venue legend to map"""
        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 8px;">
        <h4 style="margin-top:0; color: #333;">Venue Map Legend</h4>
        <p><i class="fa fa-circle" style="color:#d73027"></i> High Rating (4.0+)</p>
        <p><i class="fa fa-circle" style="color:#fc8d59"></i> Good Rating (3.0-4.0)</p>
        <p><i class="fa fa-circle" style="color:#fee08b"></i> Average Rating (2.0-3.0)</p>
        <p><i class="fa fa-circle" style="color:#91bfdb"></i> Low Rating (0-2.0)</p>
        <div style="margin-top: 10px; padding: 5px; background-color: #f0f8ff; border-radius: 3px;">
            <small>Total Venues: {venue_count}</small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_event_legend(self, map_obj: folium.Map, event_count: int):
        """Add event legend to map"""
        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 8px;">
        <h4 style="margin-top:0; color: #333;">Event Map Legend</h4>
        <p><i class="fa fa-circle" style="color:#fee08b"></i> Events</p>
        <div style="margin-top: 10px; padding: 5px; background-color: #fff3e0; border-radius: 3px;">
            <small>Total Events: {event_count}</small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_prediction_legend(self, map_obj: folium.Map, prediction_count: int):
        """Add prediction legend to map"""
        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 8px;">
        <h4 style="margin-top:0; color: #333;">Prediction Heatmap</h4>
        <div style="background: linear-gradient(to right, navy, blue, green, yellow, red); 
                    height: 20px; width: 100%; margin: 10px 0;"></div>
        <div style="display: flex; justify-content: space-between; font-size: 12px;">
            <span>Low</span>
            <span>High</span>
        </div>
        <p style="margin-top: 10px;"><i class="fa fa-star" style="color:red"></i> High Value Areas</p>
        <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
            <small>Total Predictions: {prediction_count}</small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_combined_legend(
        self,
        map_obj: folium.Map,
        venue_count: int,
        event_count: int,
        prediction_count: int,
    ):
        """Add comprehensive legend for combined map"""
        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 220px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 15px; border-radius: 8px;">
        <h4 style="margin-top:0; color: #333;">Combined Map Legend</h4>
        
        <h5 style="margin: 10px 0 5px 0; color: #666;">🏢 Venues ({venue_count})</h5>
        <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#d73027"></i> High Rating</p>
        <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#91bfdb"></i> Low Rating</p>
        
        <h5 style="margin: 10px 0 5px 0; color: #666;">🎭 Events ({event_count})</h5>
        <p style="margin: 3px 0;"><i class="fa fa-circle" style="color:#fee08b"></i> Event Locations</p>
        
        <h5 style="margin: 10px 0 5px 0; color: #666;">🤖 Predictions ({prediction_count})</h5>
        <div style="background: linear-gradient(to right, navy, red); height: 15px; width: 100px; margin: 5px 0;"></div>
        <p style="margin: 3px 0;"><i class="fa fa-star" style="color:red"></i> High Value</p>
        
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))


# Global map service instance
_map_service = None


def get_map_service(config: Optional[MapConfig] = None) -> MapService:
    """Get the global map service instance"""
    global _map_service
    if _map_service is None:
        _map_service = MapService(config)
    return _map_service
