"""
Unified Venue-Event Map Generator
Combines venues and events from database into a single interactive map.
Uses Master Data Interface as single source of truth.
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import folium
from folium.plugins import HeatMap, MarkerCluster
import json
import webbrowser

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from simple_map.data_interface import MasterDataInterface
    from data_collectors.kc_event_scraper import KCEventScraper
except ImportError as e:
    logging.error(f"Import error: {e}")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UnifiedVenueEventMap:
    """
    Unified map generator that combines venues and events from database.

    Features:
    - Database-first approach using Master Data Interface
    - Interactive venue markers with event counts
    - Event overlays linked to venues
    - Time-based filtering
    - Psychographic scoring display
    - Comprehensive testing and validation
    """

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        """Initialize unified map generator."""
        self.center_coords = center_coords
        self.data_interface = MasterDataInterface()
        self.event_scraper = KCEventScraper()

        # Kansas City default bounds
        self.default_area_bounds = {
            "north": 39.3,
            "south": 38.9,
            "east": -94.3,
            "west": -94.8,
        }

    def collect_and_store_events(self, force_refresh: bool = False) -> Dict:
        """
        Collect events using the scraper and store in database.

        Args:
            force_refresh: Whether to force fresh data collection

        Returns:
            Collection result summary
        """
        logger.info("🎭 Collecting and storing events in database...")

        try:
            # Use the event scraper's collect_data method which stores in DB
            collection_result = self.event_scraper.collect_data()

            if collection_result.success:
                logger.info(
                    f"✅ Successfully collected {collection_result.events_collected} events from {collection_result.venues_collected} venues"
                )
                return {
                    "success": True,
                    "events_collected": collection_result.events_collected,
                    "venues_collected": collection_result.venues_collected,
                    "duration_seconds": collection_result.duration_seconds,
                    "data_quality_score": collection_result.data_quality_score,
                }
            else:
                logger.error(
                    f"❌ Event collection failed: {collection_result.error_message}"
                )
                return {"success": False, "error": collection_result.error_message}

        except Exception as e:
            logger.error(f"Error in event collection: {e}")
            return {"success": False, "error": str(e)}

    def get_unified_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> Tuple[List, List]:
        """
        Get unified venue and event data from database.

        Args:
            area_bounds: Geographic bounds for data
            time_period: Time period for events

        Returns:
            Tuple of (venues, events) from database
        """
        logger.info("📊 Retrieving unified venue and event data from database...")

        if area_bounds is None:
            area_bounds = self.default_area_bounds
        if time_period is None:
            time_period = timedelta(days=30)

        try:
            # Use Master Data Interface to get consolidated data
            venues, events = self.data_interface.get_venues_and_events(
                area_bounds, time_period
            )

            logger.info(
                f"✅ Retrieved {len(venues)} venues and {len(events)} events from database"
            )

            return venues, events

        except Exception as e:
            logger.error(f"Error retrieving unified data: {e}")
            return [], []

    def create_unified_map(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
        output_path: str = "unified_venue_event_map.html",
        style: str = "streets",
        include_event_heatmap: bool = True,
        include_venue_clustering: bool = True,
    ) -> Optional[Path]:
        """
        Create unified interactive map with venues and events.

        Args:
            area_bounds: Geographic bounds for data
            time_period: Time period for events
            output_path: Output HTML file path
            style: Map style to use
            include_event_heatmap: Whether to include event heatmap layer
            include_venue_clustering: Whether to cluster venue markers

        Returns:
            Path to generated HTML file
        """
        logger.info("🗺️ Creating unified venue-event map...")

        try:
            # Get unified data from database
            venues, events = self.get_unified_data(area_bounds, time_period)

            if not venues and not events:
                logger.warning("No venue or event data available for map generation")
                return None

            # Calculate map center from data
            center = self._calculate_center_from_data(venues, events)

            # Create base map
            m = folium.Map(
                location=center,
                zoom_start=12,
                tiles="OpenStreetMap",
                control_scale=True,
                prefer_canvas=True,
            )

            # Add venue layers
            if venues:
                self._add_venue_layers(m, venues, include_venue_clustering)

            # Add event layers
            if events:
                self._add_event_layers(m, events, include_event_heatmap)

            # Add venue-event relationship indicators
            if venues and events:
                self._add_venue_event_relationships(m, venues, events)

            # Add interactive controls and legends
            self._add_unified_controls(m, venues, events)
            self._add_unified_legend(m, venues, events)
            self._add_info_panel(m)

            # Add venue ranking sidebar
            if venues:
                self._add_venue_ranking_sidebar(m, venues, events)

            # Save map
            output_file = Path(output_path).resolve()
            output_file.parent.mkdir(parents=True, exist_ok=True)
            m.save(str(output_file))

            logger.info(f"✅ Unified map saved to {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error creating unified map: {e}")
            return None

    def _calculate_center_from_data(self, venues: List, events: List) -> List[float]:
        """Calculate map center from venue and event data."""
        lats, lons = [], []

        # Extract coordinates from venues
        for venue in venues:
            if hasattr(venue, "location") and venue.location:
                lats.append(venue.location[0])  # lat
                lons.append(venue.location[1])  # lng

        # Extract coordinates from events
        for event in events:
            if hasattr(event, "venue_location") and event.venue_location:
                lats.append(event.venue_location[0])  # lat
                lons.append(event.venue_location[1])  # lng

        if lats and lons:
            return [sum(lats) / len(lats), sum(lons) / len(lons)]
        else:
            return list(self.center_coords)

    def _add_venue_layers(
        self, map_obj: folium.Map, venues: List, use_clustering: bool = True
    ):
        """Add venue layers to map."""
        logger.info(f"Adding {len(venues)} venues to map...")

        if use_clustering:
            # Create marker cluster for venues
            venue_cluster = MarkerCluster(
                name="🏢 Venues", overlay=True, control=True, show=True
            )
        else:
            venue_cluster = folium.FeatureGroup(name="🏢 Venues", show=True)

        for venue in venues:
            if not hasattr(venue, "location") or not venue.location:
                continue

            lat = venue.location[0]  # lat
            lng = venue.location[1]  # lng

            if lat == 0 and lng == 0:
                continue

            # Get venue properties
            name = getattr(venue, "name", "Unknown Venue")
            category = getattr(venue, "category", "unknown")
            comprehensive_score = getattr(venue, "comprehensive_score", 0)
            data_completeness = getattr(venue, "data_completeness", 0)

            # Determine marker style based on comprehensive score
            radius, color, fill_color = self._get_venue_marker_style(
                comprehensive_score
            )

            # Create venue popup
            popup_content = self._create_venue_popup(venue)

            # Add venue marker
            folium.CircleMarker(
                location=(lat, lng),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=350),
                tooltip=f"🏢 {name} | Score: {comprehensive_score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.8,
                weight=2,
            ).add_to(venue_cluster)

        venue_cluster.add_to(map_obj)

    def _add_event_layers(
        self, map_obj: folium.Map, events: List, include_heatmap: bool = True
    ):
        """Add event layers to map."""
        logger.info(f"Adding {len(events)} events to map...")

        # Create event marker layer
        event_layer = folium.FeatureGroup(name="🎭 Events", show=True)

        # Prepare heatmap data
        heat_data = []

        for event in events:
            if not hasattr(event, "venue_location") or not event.venue_location:
                continue

            lat = event.venue_location[0]  # lat
            lng = event.venue_location[1]  # lng

            if lat == 0 and lng == 0:
                continue

            # Get event properties
            name = getattr(event, "name", "Unknown Event")
            event_score = getattr(event, "event_score", 0)
            start_time = getattr(event, "start_time", None)

            # Determine marker style based on event score
            radius, color, fill_color = self._get_event_marker_style(event_score)

            # Create event popup
            popup_content = self._create_event_popup(event)

            # Add event marker
            folium.CircleMarker(
                location=(lat, lng),
                radius=radius,
                popup=folium.Popup(popup_content, max_width=350),
                tooltip=f"🎭 {name} | Score: {event_score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(event_layer)

            # Add to heatmap data
            heat_data.append([lat, lng, event_score])

        event_layer.add_to(map_obj)

        # Add event heatmap layer if requested
        if include_heatmap and heat_data:
            try:
                heatmap_layer = folium.FeatureGroup(name="🔥 Event Heatmap", show=False)
                HeatMap(
                    heat_data,
                    radius=20,
                    blur=15,
                    max_zoom=15,
                    gradient={
                        0.0: "#313695",
                        0.2: "#4575b4",
                        0.4: "#74add1",
                        0.6: "#abd9e9",
                        0.8: "#fee090",
                        1.0: "#d73027",
                    },
                ).add_to(heatmap_layer)
                heatmap_layer.add_to(map_obj)
            except Exception as e:
                logger.warning(f"Could not add heatmap layer: {e}")

    def _add_venue_event_relationships(
        self, map_obj: folium.Map, venues: List, events: List
    ):
        """Add visual indicators showing venue-event relationships."""
        logger.info("Adding venue-event relationship indicators...")

        # Create relationship layer
        relationship_layer = folium.FeatureGroup(
            name="🔗 Venue-Event Links", show=False
        )

        # Group events by venue
        venue_events = {}
        for event in events:
            venue_name = getattr(event, "venue_name", None)
            if venue_name:
                if venue_name not in venue_events:
                    venue_events[venue_name] = []
                venue_events[venue_name].append(event)

        # Add connection lines between venues and their events
        for venue in venues:
            venue_name = getattr(venue, "name", "")
            if (
                venue_name in venue_events
                and hasattr(venue, "location")
                and venue.location
            ):
                venue_lat = venue.location[0]  # lat
                venue_lng = venue.location[1]  # lng

                for event in venue_events[venue_name]:
                    if hasattr(event, "venue_location") and event.venue_location:
                        event_lat = event.venue_location[0]  # lat
                        event_lng = event.venue_location[1]  # lng

                        if event_lat != 0 and event_lng != 0:
                            # Add connection line
                            folium.PolyLine(
                                locations=[
                                    (venue_lat, venue_lng),
                                    (event_lat, event_lng),
                                ],
                                color="#666666",
                                weight=1,
                                opacity=0.5,
                                popup=f"Connection: {venue_name} → {getattr(event, 'name', 'Event')}",
                            ).add_to(relationship_layer)

        relationship_layer.add_to(map_obj)

    def _get_venue_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get venue marker styling based on comprehensive score."""
        if score >= 0.8:
            return 15, "#1a5490", "#1a5490"  # Large dark blue
        elif score >= 0.6:
            return 12, "#2e7d32", "#2e7d32"  # Medium green
        elif score >= 0.4:
            return 10, "#f57c00", "#f57c00"  # Medium orange
        elif score >= 0.2:
            return 8, "#d32f2f", "#d32f2f"  # Small red
        else:
            return 6, "#757575", "#757575"  # Very small gray

    def _get_event_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get event marker styling based on event score."""
        if score >= 0.8:
            return 12, "#d73027", "#d73027"  # Large red
        elif score >= 0.6:
            return 10, "#fc8d59", "#fc8d59"  # Medium orange
        elif score >= 0.4:
            return 8, "#fee08b", "#fee08b"  # Medium yellow
        elif score >= 0.2:
            return 6, "#e0f3f8", "#e0f3f8"  # Small light blue
        else:
            return 4, "#91bfdb", "#91bfdb"  # Very small blue

    def _create_venue_popup(self, venue) -> str:
        """Create HTML popup content for venue markers."""
        name = getattr(venue, "name", "Unknown Venue")
        category = getattr(venue, "category", "unknown")
        comprehensive_score = getattr(venue, "comprehensive_score", 0)
        data_completeness = getattr(venue, "data_completeness", 0)
        data_source_type = getattr(venue, "data_source_type", "unknown")

        # Get contextual data
        current_weather = getattr(venue, "current_weather", None)
        social_sentiment = getattr(venue, "social_sentiment", None)
        ml_predictions = getattr(venue, "ml_predictions", None)

        weather_info = ""
        if current_weather:
            weather_info = f"<p style='margin: 3px 0; font-size: 11px;'>🌤️ Weather: {current_weather.get('temperature_f', 'N/A')}°F</p>"

        social_info = ""
        if social_sentiment:
            social_info = f"<p style='margin: 3px 0; font-size: 11px;'>📱 Sentiment: {social_sentiment.get('positive_sentiment', 0):.2f}</p>"

        ml_info = ""
        if ml_predictions:
            ml_info = f"<p style='margin: 3px 0; font-size: 11px;'>🤖 ML Score: {ml_predictions.get('psychographic_density', 0):.3f}</p>"

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 320px;">
            <h4 style="margin: 0 0 10px 0; color: #1a5490; border-bottom: 2px solid #1a5490; padding-bottom: 5px;">
                🏢 {name}
            </h4>
            <p style="margin: 5px 0;"><strong>Category:</strong> {category.title()}</p>
            <p style="margin: 5px 0;"><strong>Comprehensive Score:</strong> {comprehensive_score:.3f}</p>
            <p style="margin: 5px 0;"><strong>Data Completeness:</strong> {data_completeness:.2f}</p>
            <p style="margin: 5px 0;"><strong>Data Source:</strong> {data_source_type.replace('_', ' ').title()}</p>
            
            <div style="margin: 10px 0; padding: 8px; background-color: #f8f9fa; border-radius: 4px; border-left: 4px solid #1a5490;">
                <h5 style="margin: 0 0 5px 0; color: #333; font-size: 12px;">Contextual Data</h5>
                {weather_info}
                {social_info}
                {ml_info}
            </div>
            
            <div style="margin-top: 10px; padding: 5px; background-color: #e3f2fd; border-radius: 3px;">
                <small>📊 Database-Sourced Venue Data</small>
            </div>
        </div>
        """

    def _create_event_popup(self, event) -> str:
        """Create HTML popup content for event markers."""
        name = getattr(event, "name", "Unknown Event")
        venue_name = getattr(event, "venue_name", "Unknown Venue")
        category = getattr(event, "category", "unknown")
        event_score = getattr(event, "event_score", 0)
        start_time = getattr(event, "start_time", None)

        # Format start time
        time_str = "TBD"
        if start_time:
            if isinstance(start_time, datetime):
                time_str = start_time.strftime("%Y-%m-%d %H:%M")
            else:
                time_str = str(start_time)

        # Get psychographic relevance
        psychographic_relevance = getattr(event, "psychographic_relevance", None)
        psycho_info = ""
        if psychographic_relevance:
            psycho_info = f"""
            <div style="margin: 8px 0; padding: 6px; background-color: #fff3e0; border-radius: 3px;">
                <h6 style="margin: 0 0 3px 0; font-size: 11px; color: #666;">Psychographic Scores</h6>
                <p style="margin: 2px 0; font-size: 10px;">Career: {psychographic_relevance.get('career_driven', 0)}</p>
                <p style="margin: 2px 0; font-size: 10px;">Competent: {psychographic_relevance.get('competent', 0)}</p>
                <p style="margin: 2px 0; font-size: 10px;">Fun: {psychographic_relevance.get('fun', 0)}</p>
            </div>
            """

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 320px;">
            <h4 style="margin: 0 0 10px 0; color: #d73027; border-bottom: 2px solid #d73027; padding-bottom: 5px;">
                🎭 {name}
            </h4>
            <p style="margin: 5px 0;"><strong>Venue:</strong> {venue_name}</p>
            <p style="margin: 5px 0;"><strong>Category:</strong> {category.title()}</p>
            <p style="margin: 5px 0;"><strong>Date/Time:</strong> {time_str}</p>
            <p style="margin: 5px 0;"><strong>Event Score:</strong> {event_score:.3f}</p>
            
            {psycho_info}
            
            <div style="margin-top: 10px; padding: 5px; background-color: #ffebee; border-radius: 3px;">
                <small>🎪 Database-Sourced Event Data</small>
            </div>
        </div>
        """

    def _add_unified_controls(self, map_obj: folium.Map, venues: List, events: List):
        """Add unified layer controls."""
        folium.LayerControl(
            position="topright",
            collapsed=False,
            autoZIndex=True,
        ).add_to(map_obj)

    def _add_unified_legend(self, map_obj: folium.Map, venues: List, events: List):
        """Add comprehensive legend for unified map."""
        venue_count = len(venues)
        event_count = len(events)

        # Calculate statistics
        venue_avg_score = (
            sum(getattr(v, "comprehensive_score", 0) for v in venues) / len(venues)
            if venues
            else 0
        )
        event_avg_score = (
            sum(getattr(e, "event_score", 0) for e in events) / len(events)
            if events
            else 0
        )

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 300px; height: auto; max-height: 500px;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            🗺️ Unified Map Legend
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #1a5490; font-size: 13px;">🏢 Venues ({venue_count})</h4>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#1a5490"></i> High Score (0.8+)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#2e7d32"></i> Good Score (0.6-0.8)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#f57c00"></i> Medium Score (0.4-0.6)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#d32f2f"></i> Low Score (0.2-0.4)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#757575"></i> Very Low (0-0.2)</p>
            <p style="margin: 5px 0; font-size: 10px; color: #666;">Avg Score: {venue_avg_score:.3f}</p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #d73027; font-size: 13px;">🎭 Events ({event_count})</h4>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#d73027"></i> High Score (0.8+)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#fc8d59"></i> Good Score (0.6-0.8)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#fee08b"></i> Medium Score (0.4-0.6)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#e0f3f8"></i> Low Score (0.2-0.4)</p>
            <p style="margin: 3px 0; font-size: 11px;"><i class="fa fa-circle" style="color:#91bfdb"></i> Very Low (0-0.2)</p>
            <p style="margin: 5px 0; font-size: 10px; color: #666;">Avg Score: {event_avg_score:.3f}</p>
        </div>
        
        <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <h5 style="margin: 0 0 5px 0; font-size: 11px; color: #666;">Interactive Features</h5>
            <p style="margin: 2px 0; font-size: 10px;">• Click markers for details</p>
            <p style="margin: 2px 0; font-size: 10px;">• Toggle layers on/off</p>
            <p style="margin: 2px 0; font-size: 10px;">• View venue-event connections</p>
        </div>
        
        <div style="margin-top: 10px; padding: 5px; background-color: #f0f8ff; border-radius: 3px;">
            <small style="color: #666;">📊 Data from Master Database</small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_info_panel(self, map_obj: folium.Map):
        """Add information panel explaining the unified map."""
        info_html = """
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 320px; height: auto; max-height: 70vh;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            🎯 Unified Venue-Event Map
        </h3>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #1a5490; font-size: 13px;">🏢 Venue Layer</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Venues from database with comprehensive scoring based on multiple data sources.
                Larger markers indicate higher-scoring venues.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #d73027; font-size: 13px;">🎭 Event Layer</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Events scraped and stored in database with psychographic scoring.
                Includes upcoming events with venue associations.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #666; font-size: 13px;">🔗 Relationships</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Visual connections between venues and their associated events.
                Toggle the "Venue-Event Links" layer to see relationships.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #333; font-size: 13px;">🎛️ Controls</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Use layer control (top-right) to toggle different data layers.
                Click any marker for detailed information and contextual data.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #999; font-size: 10px;">
                Unified Database-Driven Map v1.0<br>
                Psychographic Prediction & Data Engineering
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))

    def _add_venue_ranking_sidebar(
        self, map_obj: folium.Map, venues: List, events: List
    ):
        """Add venue ranking sidebar with event counts."""
        if not venues:
            return

        # Sort venues by comprehensive score
        sorted_venues = sorted(
            venues, key=lambda x: getattr(x, "comprehensive_score", 0), reverse=True
        )

        # Count events per venue
        venue_event_counts = {}
        for event in events:
            venue_name = getattr(event, "venue_name", None)
            if venue_name:
                venue_event_counts[venue_name] = (
                    venue_event_counts.get(venue_name, 0) + 1
                )

        # Create venue list HTML
        venue_items = []
        for i, venue in enumerate(sorted_venues[:25]):  # Show top 25 venues
            name = getattr(venue, "name", "Unknown Venue")
            score = getattr(venue, "comprehensive_score", 0)
            category = getattr(venue, "category", "Unknown")
            event_count = venue_event_counts.get(name, 0)

            if hasattr(venue, "location") and venue.location:
                lat = venue.location[0]  # lat
                lng = venue.location[1]  # lng
            else:
                lat, lng = 0, 0

            # Determine score color
            if score >= 0.8:
                score_color = "#1a5490"
            elif score >= 0.6:
                score_color = "#2e7d32"
            elif score >= 0.4:
                score_color = "#f57c00"
            elif score >= 0.2:
                score_color = "#d32f2f"
            else:
                score_color = "#757575"

            venue_items.append(
                f"""
            <div class="venue-item" onclick="centerMapOnVenue({lat}, {lng})" 
                 style="padding: 10px; margin: 5px 0; border-left: 4px solid {score_color}; 
                        background: rgba(255,255,255,0.95); cursor: pointer; border-radius: 6px;
                        transition: all 0.3s; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div style="flex: 1;">
                        <div style="font-weight: bold; font-size: 13px; color: #333; margin-bottom: 3px;">
                            #{i+1}. {name[:30]}{"..." if len(name) > 30 else ""}
                        </div>
                        <div style="font-size: 11px; color: #666; margin-bottom: 3px;">
                            {category} • {event_count} events
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
        <div id="unified-venue-sidebar" style="position: fixed; 
                    top: 20px; left: 20px; width: 340px; height: 70vh;
                    background-color: rgba(255, 255, 255, 0.98); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9998; font-size: 12px; padding: 0;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    display: flex; flex-direction: column;">
            
            <!-- Header -->
            <div style="padding: 15px; border-bottom: 2px solid #333; background: #f8f9fa; border-radius: 6px 6px 0 0;">
                <h3 style="margin: 0; color: #333; text-align: center; font-size: 14px;">
                    🏆 Unified Venue Rankings
                </h3>
                <div style="text-align: center; margin-top: 5px;">
                    <small style="color: #666; font-size: 11px;">
                        Click venue to center map • {len(venues)} venues • {len(events)} events
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
                        Unified Database Data
                    </small>
                    <button onclick="toggleUnifiedSidebar()" style="background: #007bff; color: white; border: none; 
                            padding: 4px 8px; border-radius: 3px; font-size: 10px; cursor: pointer;">
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
                
                // Add a temporary marker
                var tempMarker = L.marker([lat, lon], {{
                    icon: L.icon({{
                        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-gold.png',
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
        function toggleUnifiedSidebar() {{
            var sidebar = document.getElementById('unified-venue-sidebar');
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
                    this.style.transform = 'translateX(3px)';
                }});
                item.addEventListener('mouseleave', function() {{
                    this.style.backgroundColor = 'rgba(255,255,255,0.95)';
                    this.style.transform = 'translateX(0)';
                }});
            }});
        }});
        </script>

        <style>
        .venue-item:hover {{
            background-color: rgba(0, 123, 255, 0.1) !important;
            transform: translateX(3px) !important;
        }}
        
        #unified-venue-sidebar::-webkit-scrollbar {{
            width: 6px;
        }}
        
        #unified-venue-sidebar::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 3px;
        }}
        
        #unified-venue-sidebar::-webkit-scrollbar-thumb {{
            background: #888;
            border-radius: 3px;
        }}
        
        #unified-venue-sidebar::-webkit-scrollbar-thumb:hover {{
            background: #555;
        }}
        </style>
        """

        map_obj.get_root().html.add_child(folium.Element(sidebar_html))

    def run_complete_workflow(
        self,
        collect_events: bool = True,
        output_path: str = "unified_venue_event_map.html",
        open_browser: bool = True,
    ) -> Dict:
        """
        Run complete workflow: collect events, store in database, create unified map.

        Args:
            collect_events: Whether to collect fresh event data
            output_path: Output HTML file path
            open_browser: Whether to open map in browser

        Returns:
            Workflow result summary
        """
        logger.info("🚀 Starting complete unified venue-event workflow...")

        workflow_result = {
            "success": False,
            "steps_completed": [],
            "errors": [],
            "map_path": None,
            "statistics": {},
        }

        try:
            # Step 1: Collect and store events if requested
            if collect_events:
                logger.info("Step 1: Collecting and storing events...")
                collection_result = self.collect_and_store_events()

                if collection_result["success"]:
                    workflow_result["steps_completed"].append("event_collection")
                    workflow_result["statistics"]["events_collected"] = (
                        collection_result.get("events_collected", 0)
                    )
                    workflow_result["statistics"]["venues_collected"] = (
                        collection_result.get("venues_collected", 0)
                    )
                else:
                    workflow_result["errors"].append(
                        f"Event collection failed: {collection_result.get('error', 'Unknown error')}"
                    )
                    logger.warning(
                        "Event collection failed, continuing with existing data..."
                    )

            # Step 2: Create unified map
            logger.info("Step 2: Creating unified map from database...")
            map_path = self.create_unified_map(output_path=output_path)

            if map_path:
                workflow_result["steps_completed"].append("map_creation")
                workflow_result["map_path"] = str(map_path)

                # Step 3: Open in browser if requested
                if open_browser:
                    logger.info("Step 3: Opening map in browser...")
                    try:
                        webbrowser.open(f"file://{map_path.absolute()}")
                        workflow_result["steps_completed"].append("browser_open")
                    except Exception as e:
                        workflow_result["errors"].append(f"Could not open browser: {e}")

                workflow_result["success"] = True
                logger.info("✅ Complete workflow finished successfully!")

            else:
                workflow_result["errors"].append("Map creation failed")
                logger.error("❌ Map creation failed")

            return workflow_result

        except Exception as e:
            workflow_result["errors"].append(f"Workflow error: {e}")
            logger.error(f"❌ Workflow failed: {e}")
            return workflow_result


# Convenience functions for easy usage
def create_unified_map(
    output_path: str = "unified_venue_event_map.html",
    collect_fresh_events: bool = True,
    open_browser: bool = True,
) -> Dict:
    """
    Convenience function to create unified venue-event map.

    Args:
        output_path: Output HTML file path
        collect_fresh_events: Whether to collect fresh event data
        open_browser: Whether to open map in browser

    Returns:
        Workflow result summary
    """
    unified_map = UnifiedVenueEventMap()
    return unified_map.run_complete_workflow(
        collect_events=collect_fresh_events,
        output_path=output_path,
        open_browser=open_browser,
    )


if __name__ == "__main__":
    # Test the unified venue-event map
    import logging

    logging.basicConfig(level=logging.INFO)

    print("🎯 Unified Venue-Event Map Generator")
    print("=" * 50)

    # Create unified map with complete workflow
    result = create_unified_map(
        output_path="unified_venue_event_map.html",
        collect_fresh_events=True,
        open_browser=True,
    )

    print(f"\n📊 Workflow Results:")
    print(f"Success: {result['success']}")
    print(f"Steps completed: {result['steps_completed']}")
    print(f"Map path: {result.get('map_path', 'N/A')}")

    if result.get("statistics"):
        stats = result["statistics"]
        print(f"Events collected: {stats.get('events_collected', 0)}")
        print(f"Venues processed: {stats.get('venues_collected', 0)}")

    if result.get("errors"):
        print(f"Errors: {result['errors']}")

    print("\n🎉 Unified venue-event map generation complete!")
