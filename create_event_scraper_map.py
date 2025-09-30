#!/usr/bin/env python3
"""
Event Scraper Interactive Map Generator
Creates an interactive map focused on scraped Kansas City events
Uses the KC Event Scraper and Interactive Map Builder
"""

import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict, Counter
import webbrowser

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from data_collectors.kc_event_scraper import KCEventScraper, Event
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the PPM root directory")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EventScraperMapGenerator:
    """Generates interactive maps from scraped event data"""

    def __init__(self, use_cached_data: bool = False):
        """
        Initialize the map generator

        Args:
            use_cached_data: Use previously scraped data if available
        """
        self.use_cached_data = use_cached_data
        self.scraper = KCEventScraper()
        self.map_builder = InteractiveMapBuilder()
        self.events_data = []
        self.venues_data = {}

        # Kansas City coordinates
        self.kc_center = (39.0997, -94.5786)

        # Event category icons and colors
        self.category_config = {
            "Major Venue": {
                "icon": "🎭",
                "color": "#2e7d32",  # Dark green
                "description": "Major theaters, arenas, and concert halls",
            },
            "Entertainment District": {
                "icon": "🌃",
                "color": "#388e3c",  # Medium green
                "description": "Entertainment districts and nightlife areas",
            },
            "Aggregator": {
                "icon": "📅",
                "color": "#43a047",  # Light green
                "description": "Event aggregator sites and listings",
            },
            "local_event": {
                "icon": "🎪",
                "color": "#66bb6a",  # Very light green
                "description": "Local community events",
            },
        }

    def collect_event_data(
        self, venue_filter: Optional[List[str]] = None
    ) -> List[Event]:
        """
        Collect event data using the KC Event Scraper

        Args:
            venue_filter: List of specific venues to scrape (None = all venues)

        Returns:
            List of scraped events
        """
        logger.info("🎭 Starting event data collection...")

        try:
            # Check for cached data
            cache_file = Path("kc_events_cache.json")
            if self.use_cached_data and cache_file.exists():
                logger.info("📂 Loading cached event data...")
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)

                events = []
                for event_dict in cached_data:
                    event = Event(
                        venue=event_dict["venue"],
                        title=event_dict["title"],
                        date=event_dict.get("date"),
                        time=event_dict.get("time"),
                        location=event_dict.get("location"),
                        description=event_dict.get("description"),
                        url=event_dict.get("url"),
                        category=event_dict.get("category"),
                        image_url=event_dict.get("image_url"),
                        price=event_dict.get("price"),
                        scraped_at=event_dict.get("scraped_at"),
                    )
                    events.append(event)

                logger.info(f"✅ Loaded {len(events)} cached events")
                return events

            # Scrape fresh data
            events = self.scraper.scrape_all(venue_filter=venue_filter, delay=2.0)

            # Cache the results
            self.scraper.save_results(events, "kc_events_cache.json")

            logger.info(
                f"✅ Collected {len(events)} events from {len(venue_filter or self.scraper.VENUES)} venues"
            )
            return events

        except Exception as e:
            logger.error(f"❌ Event collection failed: {e}")
            return []

    def enrich_event_data(self, events: List[Event]) -> List[Dict]:
        """
        Enrich event data with location coordinates and additional metadata

        Args:
            events: List of Event objects

        Returns:
            List of enriched event dictionaries
        """
        logger.info("🔍 Enriching event data with coordinates...")

        enriched_events = []
        venue_coordinates = self._get_venue_coordinates()

        for event in events:
            # Get coordinates for the venue
            coords = venue_coordinates.get(event.venue, self.kc_center)

            # Add some random offset to avoid overlapping markers
            import random

            lat_offset = random.uniform(-0.002, 0.002)
            lon_offset = random.uniform(-0.002, 0.002)

            # Calculate psychographic score
            psycho_scores = self.scraper._classify_event_psychographics(
                event.title, event.description or ""
            )
            total_score = sum(psycho_scores.values()) / 10.0  # Normalize to 0-1

            enriched_event = {
                "title": event.title,
                "venue": event.venue,
                "date": event.date,
                "time": event.time,
                "location": event.location or event.venue,
                "description": event.description,
                "url": event.url,
                "category": event.category or "local_event",
                "price": event.price,
                "image_url": event.image_url,
                "scraped_at": event.scraped_at,
                "latitude": coords[0] + lat_offset,
                "longitude": coords[1] + lon_offset,
                "psychographic_scores": psycho_scores,
                "total_score": min(total_score, 1.0),  # Cap at 1.0
                "data_source": "kc_event_scraper",
            }

            enriched_events.append(enriched_event)

        logger.info(
            f"✅ Enriched {len(enriched_events)} events with coordinates and scores"
        )
        return enriched_events

    def _get_venue_coordinates(self) -> Dict[str, Tuple[float, float]]:
        """Get approximate coordinates for known venues"""
        # These are approximate coordinates for major KC venues
        venue_coords = {
            "T-Mobile Center": (39.1031, -94.5844),
            "Uptown Theater": (39.0997, -94.5786),
            "Kauffman Center": (39.0908, -94.5844),
            "Starlight Theatre": (39.0331, -94.5708),
            "The Midland Theatre": (39.1031, -94.5844),
            "Knuckleheads Saloon": (39.1242, -94.5533),
            "Azura Amphitheater": (38.8339, -94.8208),
            "Power & Light District": (39.1031, -94.5844),
            "Westport KC": (39.0631, -94.5997),
            "18th & Vine Jazz": (39.0886, -94.5608),
            "Crossroads KC": (39.0886, -94.5844),
            "Visit KC": (39.0997, -94.5786),  # Default KC center
            "Do816": (39.0997, -94.5786),  # Default KC center
        }
        return venue_coords

    def generate_venue_statistics(self, events: List[Dict]) -> Dict:
        """Generate statistics about venues and events"""
        logger.info("📊 Generating venue and event statistics...")

        venue_stats = defaultdict(
            lambda: {
                "event_count": 0,
                "events": [],
                "avg_score": 0.0,
                "categories": set(),
                "date_range": {"earliest": None, "latest": None},
            }
        )

        category_stats = Counter()
        total_events = len(events)

        for event in events:
            venue = event["venue"]
            category = event["category"]
            score = event["total_score"]
            date_str = event.get("date")

            # Update venue stats
            venue_stats[venue]["event_count"] += 1
            venue_stats[venue]["events"].append(event)
            venue_stats[venue]["categories"].add(category)

            # Update category stats
            category_stats[category] += 1

            # Parse date for range calculation
            if date_str:
                try:
                    from dateutil import parser

                    event_date = parser.parse(date_str, fuzzy=True)

                    if venue_stats[venue]["date_range"]["earliest"] is None:
                        venue_stats[venue]["date_range"]["earliest"] = event_date
                        venue_stats[venue]["date_range"]["latest"] = event_date
                    else:
                        if event_date < venue_stats[venue]["date_range"]["earliest"]:
                            venue_stats[venue]["date_range"]["earliest"] = event_date
                        if event_date > venue_stats[venue]["date_range"]["latest"]:
                            venue_stats[venue]["date_range"]["latest"] = event_date
                except:
                    pass  # Skip unparseable dates

        # Calculate average scores
        for venue in venue_stats:
            if venue_stats[venue]["event_count"] > 0:
                total_score = sum(
                    e["total_score"] for e in venue_stats[venue]["events"]
                )
                venue_stats[venue]["avg_score"] = (
                    total_score / venue_stats[venue]["event_count"]
                )
                venue_stats[venue]["categories"] = list(
                    venue_stats[venue]["categories"]
                )

        stats = {
            "total_events": total_events,
            "total_venues": len(venue_stats),
            "venue_stats": dict(venue_stats),
            "category_stats": dict(category_stats),
            "avg_psychographic_score": (
                sum(e["total_score"] for e in events) / total_events if events else 0
            ),
        }

        logger.info(
            f"✅ Generated statistics for {stats['total_venues']} venues and {stats['total_events']} events"
        )
        return stats

    def create_event_map(
        self,
        events: List[Dict],
        output_path: str = "kc_events_map.html",
        map_style: str = "streets",
    ) -> Optional[Path]:
        """
        Create interactive map with event data

        Args:
            events: List of enriched event dictionaries
            output_path: Output HTML file path
            map_style: Mapbox style to use

        Returns:
            Path to generated map file
        """
        logger.info(f"🗺️ Creating interactive event map with {len(events)} events...")

        if not events:
            logger.warning("No events to map")
            return None

        try:
            # Generate statistics
            stats = self.generate_venue_statistics(events)

            # Create the map using the enhanced map builder
            map_file = self._create_enhanced_event_map(
                events, stats, output_path, map_style
            )

            if map_file:
                logger.info(f"✅ Event map created: {map_file}")
                return map_file
            else:
                logger.error("❌ Failed to create event map")
                return None

        except Exception as e:
            logger.error(f"❌ Map creation failed: {e}")
            return None

    def _create_enhanced_event_map(
        self, events: List[Dict], stats: Dict, output_path: str, map_style: str
    ) -> Optional[Path]:
        """Create enhanced event map with custom features"""

        import folium
        from folium.plugins import MarkerCluster

        # Calculate map center from events
        if events:
            avg_lat = sum(e["latitude"] for e in events) / len(events)
            avg_lon = sum(e["longitude"] for e in events) / len(events)
            center = [avg_lat, avg_lon]
        else:
            center = list(self.kc_center)

        # Create base map
        m = self.map_builder._create_base_map(center, zoom=12, style=map_style)

        # Create event layers by category
        category_layers = {}
        for category, config in self.category_config.items():
            layer = folium.FeatureGroup(
                name=f"{config['icon']} {category.replace('_', ' ').title()}", show=True
            )
            category_layers[category] = layer

        # Add events to appropriate layers
        for event in events:
            category = event.get("category", "local_event")
            layer = category_layers.get(category, category_layers["local_event"])

            self._add_event_marker(layer, event)

        # Add all layers to map
        for layer in category_layers.values():
            layer.add_to(m)

        # Add venue cluster layer
        venue_layer = self._create_venue_cluster_layer(events, stats)
        venue_layer.add_to(m)

        # Add enhanced controls and legends
        self._add_event_map_legend(m, stats)
        self._add_event_info_panel(m, stats)
        self._add_event_sidebar(m, events, stats)

        # Add layer control
        folium.LayerControl(position="topright", collapsed=False).add_to(m)

        # Save map
        output_file = Path(output_path).resolve()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        m.save(str(output_file))

        return output_file

    def _add_event_marker(self, layer: folium.FeatureGroup, event: Dict):
        """Add individual event marker to layer"""
        import folium

        lat = event["latitude"]
        lon = event["longitude"]
        score = event["total_score"]
        category = event.get("category", "local_event")

        # Get category configuration
        config = self.category_config.get(category, self.category_config["local_event"])

        # Determine marker size and color based on score
        if score >= 0.8:
            radius = 12
            color = "#1b5e20"  # Dark green
        elif score >= 0.6:
            radius = 10
            color = "#2e7d32"  # Medium dark green
        elif score >= 0.4:
            radius = 8
            color = "#388e3c"  # Medium green
        elif score >= 0.2:
            radius = 6
            color = "#43a047"  # Light green
        else:
            radius = 4
            color = "#66bb6a"  # Very light green

        # Create popup content
        popup_content = self._create_event_popup(event)

        # Create marker
        folium.CircleMarker(
            location=(lat, lon),
            radius=radius,
            popup=folium.Popup(popup_content, max_width=350),
            tooltip=f"{event['title']} | Score: {score:.2f}",
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.8,
            weight=2,
        ).add_to(layer)

    def _create_event_popup(self, event: Dict) -> str:
        """Create HTML popup content for event markers"""
        title = event.get("title", "Unknown Event")
        venue = event.get("venue", "Unknown Venue")
        date = event.get("date", "Date TBD")
        time = event.get("time", "Time TBD")
        description = event.get("description", "No description available")
        price = event.get("price", "Price not listed")
        url = event.get("url", "")
        category = event.get("category", "local_event")
        score = event.get("total_score", 0)
        psycho_scores = event.get("psychographic_scores", {})

        # Truncate long descriptions
        if description and len(description) > 200:
            description = description[:200] + "..."

        # Create URL link if available
        url_link = ""
        if url:
            url_link = f'<p style="margin: 5px 0;"><a href="{url}" target="_blank" style="color: #1976d2;">🔗 Event Details</a></p>'

        # Format psychographic scores
        psycho_display = ""
        if psycho_scores:
            psycho_items = []
            for key, value in psycho_scores.items():
                if value > 0:
                    psycho_items.append(f"{key.replace('_', ' ').title()}: {value}")
            if psycho_items:
                psycho_display = f'<p style="margin: 5px 0; font-size: 11px;"><strong>Psychographic:</strong> {", ".join(psycho_items)}</p>'

        config = self.category_config.get(category, self.category_config["local_event"])

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 320px;">
            <h4 style="margin: 0 0 10px 0; color: #1b5e20;">{config['icon']} {title}</h4>
            <p style="margin: 5px 0;"><strong>📍 Venue:</strong> {venue}</p>
            <p style="margin: 5px 0;"><strong>📅 Date:</strong> {date}</p>
            <p style="margin: 5px 0;"><strong>🕐 Time:</strong> {time}</p>
            <p style="margin: 5px 0;"><strong>💰 Price:</strong> {price}</p>
            <p style="margin: 5px 0;"><strong>📊 Score:</strong> {score:.3f}</p>
            {psycho_display}
            <div style="margin: 8px 0; padding: 8px; background-color: #e8f5e8; border-radius: 4px; border-left: 4px solid #2e7d32;">
                <p style="margin: 0; font-size: 12px; line-height: 1.4;"><strong>Description:</strong> {description}</p>
            </div>
            {url_link}
            <div style="margin-top: 10px; padding: 5px; background-color: #f1f8e9; border-radius: 3px;">
                <small style="color: #2e7d32;">🌐 Scraped Event Data</small>
            </div>
        </div>
        """

    def _create_venue_cluster_layer(
        self, events: List[Dict], stats: Dict
    ) -> folium.FeatureGroup:
        """Create venue cluster layer showing venue locations with event counts"""
        import folium

        venue_layer = folium.FeatureGroup(name="🏢 Venues (Clustered)", show=True)
        venue_stats = stats["venue_stats"]
        venue_coordinates = self._get_venue_coordinates()

        for venue_name, venue_data in venue_stats.items():
            coords = venue_coordinates.get(venue_name, self.kc_center)
            event_count = venue_data["event_count"]
            avg_score = venue_data["avg_score"]
            categories = venue_data["categories"]

            # Determine marker size based on event count
            if event_count >= 10:
                radius = 20
                color = "#1b5e20"
            elif event_count >= 5:
                radius = 15
                color = "#2e7d32"
            elif event_count >= 2:
                radius = 12
                color = "#388e3c"
            else:
                radius = 8
                color = "#66bb6a"

            # Create venue popup
            popup_content = f"""
            <div style="font-family: Arial, sans-serif; max-width: 300px;">
                <h4 style="margin: 0 0 10px 0; color: #1b5e20;">🏢 {venue_name}</h4>
                <p style="margin: 5px 0;"><strong>📊 Events:</strong> {event_count}</p>
                <p style="margin: 5px 0;"><strong>⭐ Avg Score:</strong> {avg_score:.3f}</p>
                <p style="margin: 5px 0;"><strong>🏷️ Categories:</strong> {', '.join(categories)}</p>
                <div style="margin-top: 10px; padding: 5px; background-color: #e8f5e8; border-radius: 3px;">
                    <small style="color: #2e7d32;">Venue Event Hub</small>
                </div>
            </div>
            """

            folium.CircleMarker(
                location=coords,
                radius=radius,
                popup=folium.Popup(popup_content, max_width=350),
                tooltip=f"{venue_name}: {event_count} events",
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.6,
                weight=3,
            ).add_to(venue_layer)

        return venue_layer

    def _add_event_map_legend(self, map_obj: folium.Map, stats: Dict):
        """Add legend for event map"""
        category_stats = stats["category_stats"]
        total_events = stats["total_events"]

        # Create category legend items
        legend_items = []
        for category, config in self.category_config.items():
            count = category_stats.get(category, 0)
            if count > 0:
                legend_items.append(
                    f"""
                <div style="margin: 5px 0; display: flex; align-items: center;">
                    <div style="width: 12px; height: 12px; background: {config['color']}; border-radius: 50%; margin-right: 8px;"></div>
                    <span style="font-size: 12px;">{config['icon']} {category.replace('_', ' ').title()} ({count})</span>
                </div>
                """
                )

        legend_html = f"""
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 280px; height: auto; max-height: 400px;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #2e7d32; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #1b5e20; text-align: center; border-bottom: 2px solid #2e7d32; padding-bottom: 8px;">
            🎭 KC Events Legend
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">Event Categories</h4>
            {"".join(legend_items)}
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">Score Ranges</h4>
            <div style="margin: 3px 0;"><div style="display: inline-block; width: 12px; height: 12px; background: #1b5e20; border-radius: 50%; margin-right: 6px;"></div><span style="font-size: 11px;">High (0.8+)</span></div>
            <div style="margin: 3px 0;"><div style="display: inline-block; width: 10px; height: 10px; background: #2e7d32; border-radius: 50%; margin-right: 6px;"></div><span style="font-size: 11px;">Med-High (0.6-0.8)</span></div>
            <div style="margin: 3px 0;"><div style="display: inline-block; width: 8px; height: 8px; background: #388e3c; border-radius: 50%; margin-right: 6px;"></div><span style="font-size: 11px;">Medium (0.4-0.6)</span></div>
            <div style="margin: 3px 0;"><div style="display: inline-block; width: 6px; height: 6px; background: #43a047; border-radius: 50%; margin-right: 6px;"></div><span style="font-size: 11px;">Low-Med (0.2-0.4)</span></div>
            <div style="margin: 3px 0;"><div style="display: inline-block; width: 4px; height: 4px; background: #66bb6a; border-radius: 50%; margin-right: 6px;"></div><span style="font-size: 11px;">Low (0-0.2)</span></div>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #666; font-size: 10px;">
                Total: {total_events} events from {stats['total_venues']} venues<br>
                KC Event Scraper Map v1.0
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_event_info_panel(self, map_obj: folium.Map, stats: Dict):
        """Add information panel for the event map"""
        avg_score = stats["avg_psychographic_score"]

        info_html = f"""
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 320px; height: auto; max-height: 70vh;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #2e7d32; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #1b5e20; text-align: center; border-bottom: 2px solid #2e7d32; padding-bottom: 8px;">
            🎯 KC Events Map Guide
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">📊 Data Overview</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                This map shows <strong>{stats['total_events']} events</strong> scraped from <strong>{stats['total_venues']} Kansas City venues</strong> 
                using the KC Event Scraper. Events are color-coded by psychographic relevance score.
            </p>
            <p style="margin: 8px 0 0 0; font-size: 11px; color: #666;">
                <strong>Average Score:</strong> {avg_score:.3f}
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">🎛️ Map Features</h4>
            <ul style="margin: 0; padding-left: 15px; font-size: 11px; color: #666; line-height: 1.4;">
                <li><strong>Event Markers:</strong> Individual events with details</li>
                <li><strong>Venue Clusters:</strong> Venue locations with event counts</li>
                <li><strong>Category Layers:</strong> Toggle event types on/off</li>
                <li><strong>Interactive Popups:</strong> Click for event details</li>
                <li><strong>Venue Sidebar:</strong> Browse events by venue</li>
            </ul>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">🧠 Psychographic Scoring</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Events are scored based on appeal to three psychographic segments:
            </p>
            <ul style="margin: 5px 0 0 0; padding-left: 15px; font-size: 11px; color: #666;">
                <li><strong>Career-Driven:</strong> Professional/networking events</li>
                <li><strong>Competent:</strong> Educational/skill-building events</li>
                <li><strong>Fun:</strong> Entertainment/social events</li>
            </ul>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #2e7d32; font-size: 13px;">🔍 Data Sources</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Events scraped from major KC venues including T-Mobile Center, Kauffman Center, 
                Uptown Theater, and local event aggregators. Data includes LLM-enhanced extraction 
                for better accuracy.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #999; font-size: 10px;">
                Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}<br>
                KC Event Scraper Map v1.0
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))

    def _add_event_sidebar(self, map_obj: folium.Map, events: List[Dict], stats: Dict):
        """Add event sidebar for browsing events by venue"""
        venue_stats = stats["venue_stats"]

        # Sort venues by event count
        sorted_venues = sorted(
            venue_stats.items(), key=lambda x: x[1]["event_count"], reverse=True
        )

        # Create venue list HTML
        venue_items = []
        for venue_name, venue_data in sorted_venues:
            event_count = venue_data["event_count"]
            avg_score = venue_data["avg_score"]
            categories = venue_data["categories"]

            # Get venue coordinates for centering
            venue_coords = self._get_venue_coordinates()
            coords = venue_coords.get(venue_name, self.kc_center)

            venue_items.append(
                f"""
            <div class="venue-item" onclick="centerMapOnVenue({coords[0]}, {coords[1]})" 
                 style="padding: 10px; margin: 5px 0; border-left: 4px solid #2e7d32; 
                        background: rgba(255,255,255,0.95); cursor: pointer; border-radius: 6px;
                        transition: all 0.3s; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                <div style="font-weight: bold; font-size: 13px; color: #1b5e20; margin-bottom: 3px;">
                    🏢 {venue_name}
                </div>
                <div style="font-size: 11px; color: #666; margin-bottom: 3px;">
                    📊 {event_count} events • ⭐ {avg_score:.2f} avg score
                </div>
                <div style="font-size: 10px; color: #999;">
                    🏷️ {', '.join(categories[:2])}{'...' if len(categories) > 2 else ''}
                </div>
            </div>
            """
            )

        sidebar_html = f"""
        <div id="event-venue-sidebar" style="position: fixed; 
                    top: 20px; left: 20px; width: 300px; height: 60vh;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #2e7d32; border-radius: 8px;
                    z-index: 9998; font-size: 12px; padding: 0;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    display: flex; flex-direction: column;">
            
            <!-- Header -->
            <div style="padding: 15px; border-bottom: 2px solid #2e7d32; background: linear-gradient(135deg, #e8f5e8, #f1f8e9); border-radius: 6px 6px 0 0;">
                <h3 style="margin: 0; color: #1b5e20; text-align: center; font-size: 14px; font-weight: bold;">
                    🎭 Event Venues
                </h3>
                <div style="text-align: center; margin-top: 5px;">
                    <small style="color: #666; font-size: 11px;">
                        Click venue to center map • {stats['total_venues']} venues
                    </small>
                </div>
            </div>
            
            <!-- Venue List -->
            <div style="flex: 1; overflow-y: auto; padding: 10px;">
                {"".join(venue_items)}
            </div>
            
            <!-- Footer -->
            <div style="padding: 10px; border-top: 1px solid #ddd; background: linear-gradient(135deg, #e8f5e8, #f1f8e9); border-radius: 0 0 6px 6px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <small style="color: #666; font-size: 10px;">
                        Scraped Event Data
                    </small>
                    <button onclick="toggleEventSidebar()" style="background: linear-gradient(135deg, #2e7d32, #1b5e20); 
                            color: white; border: none; padding: 4px 8px; border-radius: 3px; 
                            font-size: 10px; cursor: pointer; font-weight: bold;">
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
                map.setView([lat, lon], 15);
                
                // Add a temporary marker
                var tempMarker = L.marker([lat, lon], {{
                    icon: L.icon({{
                        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-green.png',
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
        function toggleEventSidebar() {{
            var sidebar = document.getElementById('event-venue-sidebar');
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
                    this.style.backgroundColor = 'rgba(46, 125, 50, 0.1)';
                    this.style.transform = 'translateX(5px)';
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
            background-color: rgba(46, 125, 50, 0.1) !important;
            transform: translateX(5px) !important;
        }}
        
        #event-venue-sidebar::-webkit-scrollbar {{
            width: 6px;
        }}
        
        #event-venue-sidebar::-webkit-scrollbar-track {{
            background: #f1f1f1;
            border-radius: 3px;
        }}
        
        #event-venue-sidebar::-webkit-scrollbar-thumb {{
            background: #2e7d32;
            border-radius: 3px;
        }}
        
        #event-venue-sidebar::-webkit-scrollbar-thumb:hover {{
            background: #1b5e20;
        }}
        </style>
        """

        map_obj.get_root().html.add_child(folium.Element(sidebar_html))

    def open_map_in_browser(self, map_file: Path):
        """Open the generated map in the default browser"""
        try:
            webbrowser.open(f"file://{map_file.absolute()}")
            logger.info(f"🌐 Opened map in browser: {map_file}")
        except Exception as e:
            logger.error(f"❌ Failed to open map in browser: {e}")


def main():
    """Main function to run the event scraper map generator"""
    print("🎭 KC Event Scraper Map Generator")
    print("=" * 50)

    # Configuration options
    use_cached = (
        input("Use cached event data if available? (y/n): ").lower().startswith("y")
    )

    # Venue selection
    print("\nVenue Selection:")
    print("1. All venues (default)")
    print("2. Major venues only")
    print("3. Custom selection")

    choice = input("Choose option (1-3): ").strip()

    venue_filter = None
    if choice == "2":
        venue_filter = [
            "T-Mobile Center",
            "Uptown Theater",
            "Kauffman Center",
            "Starlight Theatre",
            "The Midland Theatre",
        ]
    elif choice == "3":
        print("\nAvailable venues:")
        scraper = KCEventScraper()
        for i, venue in enumerate(scraper.VENUES.keys(), 1):
            print(f"  {i}. {venue}")

        selected = input("Enter venue numbers (comma-separated): ").strip()
        if selected:
            try:
                indices = [int(x.strip()) - 1 for x in selected.split(",")]
                venue_list = list(scraper.VENUES.keys())
                venue_filter = [
                    venue_list[i] for i in indices if 0 <= i < len(venue_list)
                ]
            except:
                print("Invalid selection, using all venues")

    # Map style selection
    print("\nMap Style:")
    print("1. Streets (default)")
    print("2. Satellite")
    print("3. Light")
    print("4. Dark")

    style_choice = input("Choose style (1-4): ").strip()
    style_map = {"1": "streets", "2": "satellite", "3": "light", "4": "dark"}
    map_style = style_map.get(style_choice, "streets")

    # Output file
    output_file = input("Output filename (default: kc_events_map.html): ").strip()
    if not output_file:
        output_file = "kc_events_map.html"

    print(f"\n🚀 Starting event map generation...")
    print(f"  - Cached data: {use_cached}")
    print(f"  - Venues: {len(venue_filter) if venue_filter else 'All'}")
    print(f"  - Style: {map_style}")
    print(f"  - Output: {output_file}")

    try:
        # Initialize generator
        generator = EventScraperMapGenerator(use_cached_data=use_cached)

        # Collect event data
        events = generator.collect_event_data(venue_filter=venue_filter)

        if not events:
            print(
                "❌ No events collected. Check your internet connection and API keys."
            )
            return

        # Enrich event data
        enriched_events = generator.enrich_event_data(events)

        # Create map
        map_file = generator.create_event_map(
            enriched_events, output_path=output_file, map_style=map_style
        )

        if map_file:
            print(f"\n✅ Event map created successfully!")
            print(f"📍 Location: {map_file}")
            print(f"📊 Events: {len(enriched_events)}")
            print(f"🏢 Venues: {len(set(e['venue'] for e in enriched_events))}")

            # Ask to open in browser
            if input("\nOpen map in browser? (y/n): ").lower().startswith("y"):
                generator.open_map_in_browser(map_file)

            print(f"\n🎉 KC Event Map generation complete!")
            print(f"You can open the map file directly: {map_file}")

        else:
            print("❌ Failed to create event map")

    except KeyboardInterrupt:
        print("\n⏹️ Operation cancelled by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        logger.exception("Map generation failed")


if __name__ == "__main__":
    main()
