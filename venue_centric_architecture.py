#!/usr/bin/env python3
"""
Venue-Centric Data Architecture for PDPE

This module provides a venue-centric approach to data organization and visualization,
consolidating all contextual data around venues as the primary entity.

Key Components:
- VenueData: Comprehensive venue data structure
- VenueCentricDataService: Service for enriching venues with all contextual data
- VenueCentricMapBuilder: Map builder focused on venue-centric visualization

Fixes the following issues:
1. ✅ Scattered Data → Venue-Centric Consolidation
2. ✅ Missing Dropdown → Interactive Venue Navigator
3. ✅ Layer Visibility Issues → Comprehensive Venue Profiles
4. ✅ ML Disconnected from Context → Context-Aware Scoring
"""

import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
import psycopg2
from psycopg2.extras import RealDictCursor
import folium
from folium.plugins import HeatMap
import numpy as np
import webbrowser
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class VenueData:
    """
    Comprehensive venue data structure that consolidates all contextual information
    around each venue as the primary entity.
    """

    # Core venue information
    venue_id: str
    external_id: Optional[str]
    provider: str
    name: str
    category: str
    subcategory: Optional[str]
    lat: float
    lng: float
    address: Optional[str]

    # Venue characteristics
    price_tier: Optional[str] = None
    avg_rating: Optional[float] = None
    review_count: Optional[int] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours_json: Optional[Dict] = None
    amenities: List[str] = field(default_factory=list)

    # Psychographic scores (core ML predictions)
    psychographic_scores: Dict[str, float] = field(default_factory=dict)
    overall_psychographic_score: float = 0.0

    # Environmental context (real-time data)
    weather_conditions: Dict[str, Any] = field(default_factory=dict)
    traffic_conditions: Dict[str, Any] = field(default_factory=dict)
    social_sentiment: Dict[str, Any] = field(default_factory=dict)
    foot_traffic: Dict[str, Any] = field(default_factory=dict)

    # Demographic context (area characteristics)
    local_median_income: Optional[float] = None
    local_education_level: Optional[float] = None
    local_age_distribution: Dict[str, float] = field(default_factory=dict)
    local_professional_pct: Optional[float] = None

    # Associated events
    upcoming_events: List[Dict] = field(default_factory=list)
    event_frequency: int = 0
    avg_event_attendance: Optional[float] = None

    # ML predictions with context
    ml_predictions: Dict[str, float] = field(default_factory=dict)
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    prediction_factors: List[str] = field(default_factory=list)

    # Metadata
    data_completeness_score: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)
    data_sources: List[str] = field(default_factory=list)


class VenueCentricDataService:
    """
    Service for enriching venues with comprehensive contextual data.

    This service consolidates data from multiple sources around each venue,
    providing a single point of access for all venue-related information.
    """

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.logger = logging.getLogger(__name__)
        self._cache = {}
        self._cache_timestamps = {}

    def get_enriched_venue_data(
        self,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        min_score_threshold: float = 0.0,
        limit: Optional[int] = None,
    ) -> List[VenueData]:
        """
        Get venues enriched with all contextual data.

        Args:
            bbox: Bounding box (min_lat, min_lng, max_lat, max_lng)
            min_score_threshold: Minimum psychographic score threshold
            limit: Maximum number of venues to return

        Returns:
            List of enriched VenueData objects
        """
        self.logger.info("Enriching venues with comprehensive contextual data...")

        # Get base venue data
        venues = self._fetch_base_venues(bbox, min_score_threshold, limit)

        if not venues:
            self.logger.warning("No venues found matching criteria")
            return []

        # Enrich each venue with contextual data
        enriched_venues = []
        for venue in venues:
            try:
                enriched_venue = self._enrich_venue_with_context(venue)
                enriched_venues.append(enriched_venue)
            except Exception as e:
                self.logger.error(
                    f"Error enriching venue {venue.get('name', 'Unknown')}: {e}"
                )
                continue

        # Sort by overall psychographic score
        enriched_venues.sort(key=lambda v: v.overall_psychographic_score, reverse=True)

        self.logger.info(f"Successfully enriched {len(enriched_venues)} venues")
        return enriched_venues

    def _fetch_base_venues(
        self, bbox: Optional[Tuple], min_score_threshold: float, limit: Optional[int]
    ) -> List[Dict]:
        """Fetch base venue data from database."""
        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)

        # Build query conditions
        where_conditions = ["lat IS NOT NULL", "lng IS NOT NULL"]
        params = []

        if bbox:
            where_conditions.append("lat BETWEEN %s AND %s AND lng BETWEEN %s AND %s")
            params.extend([bbox[0], bbox[2], bbox[1], bbox[3]])

        if min_score_threshold > 0:
            where_conditions.append(
                "COALESCE((psychographic_relevance->>'career_driven')::float, 0) >= %s"
            )
            params.append(min_score_threshold)

        where_clause = " AND ".join(where_conditions)
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
        SELECT 
            venue_id, external_id, provider, name, category, subcategory,
            price_tier, avg_rating, review_count, lat, lng, address,
            phone, website, hours_json, amenities, psychographic_relevance,
            created_at, updated_at
        FROM venues 
        WHERE {where_clause}
        ORDER BY COALESCE((psychographic_relevance->>'career_driven')::float, 0) DESC
        {limit_clause}
        """

        cur.execute(query, params)
        venues = [dict(row) for row in cur.fetchall()]
        cur.close()

        return venues

    def _enrich_venue_with_context(self, venue_data: Dict) -> VenueData:
        """Enrich a single venue with all contextual data."""
        venue_id = str(venue_data["venue_id"])

        # Create base VenueData object
        venue = VenueData(
            venue_id=venue_id,
            external_id=venue_data.get("external_id"),
            provider=venue_data.get("provider", "unknown"),
            name=venue_data.get("name", "Unknown Venue"),
            category=venue_data.get("category", "unknown"),
            subcategory=venue_data.get("subcategory"),
            lat=float(venue_data["lat"]),
            lng=float(venue_data["lng"]),
            address=venue_data.get("address"),
            price_tier=venue_data.get("price_tier"),
            avg_rating=venue_data.get("avg_rating"),
            review_count=venue_data.get("review_count"),
            phone=venue_data.get("phone"),
            website=venue_data.get("website"),
            hours_json=venue_data.get("hours_json"),
            amenities=venue_data.get("amenities", []),
            data_sources=["venues_table"],
        )

        # Enrich with psychographic scores
        self._enrich_psychographic_scores(
            venue, venue_data.get("psychographic_relevance", {})
        )

        # Enrich with environmental context
        self._enrich_environmental_context(venue)

        # Enrich with demographic context
        self._enrich_demographic_context(venue)

        # Enrich with associated events
        self._enrich_associated_events(venue)

        # Enrich with ML predictions (context-aware)
        self._enrich_ml_predictions(venue)

        # Calculate data completeness score
        venue.data_completeness_score = self._calculate_data_completeness(venue)

        return venue

    def _enrich_psychographic_scores(self, venue: VenueData, psychographic_data: Dict):
        """Enrich venue with psychographic scores."""
        if psychographic_data:
            venue.psychographic_scores = {
                "career_driven": float(psychographic_data.get("career_driven", 0)),
                "competent": float(psychographic_data.get("competent", 0)),
                "fun": float(psychographic_data.get("fun", 0)),
            }

            # Calculate overall score with weighted average
            venue.overall_psychographic_score = (
                venue.psychographic_scores["career_driven"] * 0.5
                + venue.psychographic_scores["competent"] * 0.3
                + venue.psychographic_scores["fun"] * 0.2
            )
            venue.data_sources.append("psychographic_scores")

    def _enrich_environmental_context(self, venue: VenueData):
        """Enrich venue with environmental context (weather, traffic, social)."""
        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)

        # Get recent weather data near venue
        cur.execute(
            """
            SELECT temperature_f, feels_like_f, humidity, weather_condition, 
                   wind_speed_mph, rain_probability, ts
            FROM weather_data 
            WHERE ST_DWithin(
                ST_Point(lng, lat)::geography,
                ST_Point(%s, %s)::geography,
                5000  -- 5km radius
            )
            AND ts >= NOW() - INTERVAL '6 hours'
            ORDER BY ts DESC
            LIMIT 1
        """,
            (venue.lng, venue.lat),
        )

        weather_row = cur.fetchone()
        if weather_row:
            venue.weather_conditions = {
                "temperature_f": weather_row["temperature_f"],
                "feels_like_f": weather_row["feels_like_f"],
                "humidity": weather_row["humidity"],
                "condition": weather_row["weather_condition"],
                "wind_speed_mph": weather_row["wind_speed_mph"],
                "rain_probability": weather_row["rain_probability"],
                "timestamp": weather_row["ts"],
            }
            venue.data_sources.append("weather_data")

        # Get traffic conditions
        cur.execute(
            """
            SELECT congestion_score, travel_time_to_downtown, ts
            FROM traffic_data 
            WHERE venue_id = %s
            AND ts >= NOW() - INTERVAL '2 hours'
            ORDER BY ts DESC
            LIMIT 1
        """,
            (venue.venue_id,),
        )

        traffic_row = cur.fetchone()
        if traffic_row:
            venue.traffic_conditions = {
                "congestion_score": traffic_row["congestion_score"],
                "travel_time_to_downtown": traffic_row["travel_time_to_downtown"],
                "timestamp": traffic_row["ts"],
            }
            venue.data_sources.append("traffic_data")

        # Get social sentiment
        cur.execute(
            """
            SELECT mention_count, positive_sentiment, engagement_score, ts
            FROM social_sentiment 
            WHERE venue_id = %s
            AND ts >= NOW() - INTERVAL '24 hours'
            ORDER BY ts DESC
            LIMIT 1
        """,
            (venue.venue_id,),
        )

        social_row = cur.fetchone()
        if social_row:
            venue.social_sentiment = {
                "mention_count": social_row["mention_count"],
                "positive_sentiment": social_row["positive_sentiment"],
                "engagement_score": social_row["engagement_score"],
                "timestamp": social_row["ts"],
            }
            venue.data_sources.append("social_sentiment")

        # Get foot traffic
        cur.execute(
            """
            SELECT visitors_count, median_dwell_seconds, visitors_change_24h, ts
            FROM venue_traffic 
            WHERE venue_id = %s
            AND ts >= NOW() - INTERVAL '24 hours'
            ORDER BY ts DESC
            LIMIT 1
        """,
            (venue.venue_id,),
        )

        foot_traffic_row = cur.fetchone()
        if foot_traffic_row:
            venue.foot_traffic = {
                "visitors_count": foot_traffic_row["visitors_count"],
                "median_dwell_seconds": foot_traffic_row["median_dwell_seconds"],
                "visitors_change_24h": foot_traffic_row["visitors_change_24h"],
                "timestamp": foot_traffic_row["ts"],
            }
            venue.data_sources.append("venue_traffic")

        cur.close()

    def _enrich_demographic_context(self, venue: VenueData):
        """Enrich venue with local demographic context."""
        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)

        try:
            # First try to check if demographics table exists
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'demographics'
                );
            """
            )

            table_exists = cur.fetchone()[0]
            if not table_exists:
                self.logger.debug(
                    "Demographics table does not exist, skipping demographic enrichment"
                )
                cur.close()
                return

            # Try different approaches for demographic data based on available columns
            # First, try to get column information
            cur.execute(
                """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'demographics'
            """
            )

            columns = [row[0] for row in cur.fetchall()]

            if "geometry" in columns:
                # Try PostGIS approach with proper casting
                try:
                    cur.execute(
                        """
                        SELECT median_income, pct_bachelors, pct_age_20_40, pct_professional_occupation
                        FROM demographics 
                        WHERE ST_Contains(geometry::geometry, ST_SetSRID(ST_Point(%s, %s), 4326))
                        LIMIT 1
                    """,
                        (venue.lng, venue.lat),
                    )
                    demo_row = cur.fetchone()
                except Exception as e:
                    self.logger.debug(f"PostGIS geometry query failed: {e}")
                    demo_row = None
            else:
                # Fallback: try to find nearest demographic data by distance
                try:
                    cur.execute(
                        """
                        SELECT median_income, pct_bachelors, pct_age_20_40, pct_professional_occupation
                        FROM demographics 
                        WHERE lat IS NOT NULL AND lng IS NOT NULL
                        ORDER BY (lat - %s)^2 + (lng - %s)^2
                        LIMIT 1
                    """,
                        (venue.lat, venue.lng),
                    )
                    demo_row = cur.fetchone()
                except Exception as e:
                    self.logger.debug(f"Distance-based demographic query failed: {e}")
                    demo_row = None

            if demo_row:
                venue.local_median_income = demo_row.get("median_income")
                venue.local_education_level = demo_row.get("pct_bachelors")
                venue.local_age_distribution = {
                    "age_20_40_pct": demo_row.get("pct_age_20_40")
                }
                venue.local_professional_pct = demo_row.get(
                    "pct_professional_occupation"
                )
                venue.data_sources.append("demographics")

        except Exception as e:
            self.logger.debug(f"Demographic enrichment failed: {e}")
        finally:
            cur.close()

    def _enrich_associated_events(self, venue: VenueData):
        """Enrich venue with associated events."""
        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)

        # Get upcoming events at this venue
        cur.execute(
            """
            SELECT event_id, name, category, start_time, end_time, 
                   predicted_attendance, psychographic_relevance
            FROM events 
            WHERE venue_id = %s
            AND (start_time IS NULL OR start_time >= NOW())
            ORDER BY start_time ASC NULLS LAST
            LIMIT 10
        """,
            (venue.venue_id,),
        )

        events = cur.fetchall()
        venue.upcoming_events = [dict(event) for event in events]
        venue.event_frequency = len(venue.upcoming_events)

        if venue.upcoming_events:
            # Calculate average predicted attendance
            attendances = [
                e["predicted_attendance"]
                for e in venue.upcoming_events
                if e["predicted_attendance"]
            ]
            if attendances:
                venue.avg_event_attendance = sum(attendances) / len(attendances)

            venue.data_sources.append("events")

        cur.close()

    def _enrich_ml_predictions(self, venue: VenueData):
        """Enrich venue with context-aware ML predictions."""
        cur = self.db_conn.cursor(cursor_factory=RealDictCursor)

        # Get stored ML predictions
        cur.execute(
            """
            SELECT psychographic_density, confidence_lower, confidence_upper, 
                   model_version, contributing_factors
            FROM predictions 
            WHERE venue_id = %s
            ORDER BY ts DESC
            LIMIT 1
        """,
            (venue.venue_id,),
        )

        prediction_row = cur.fetchone()
        if prediction_row:
            base_prediction = prediction_row["psychographic_density"]

            # Apply contextual adjustments
            contextual_score = self._calculate_contextual_ml_score(
                venue, base_prediction
            )

            venue.ml_predictions = {
                "base_psychographic_density": base_prediction,
                "contextual_psychographic_density": contextual_score,
                "model_version": prediction_row["model_version"],
            }

            venue.confidence_scores = {
                "lower": prediction_row["confidence_lower"],
                "upper": prediction_row["confidence_upper"],
            }

            venue.prediction_factors = prediction_row["contributing_factors"] or []
            venue.data_sources.append("ml_predictions")

        cur.close()

    def _calculate_contextual_ml_score(
        self, venue: VenueData, base_score: float
    ) -> float:
        """Calculate context-aware ML score incorporating venue-specific factors."""
        contextual_score = base_score

        # Weather adjustment
        if venue.weather_conditions:
            temp = venue.weather_conditions.get("temperature_f", 70)
            if 65 <= temp <= 80:  # Ideal temperature range
                contextual_score *= 1.1
            elif temp < 40 or temp > 90:  # Extreme temperatures
                contextual_score *= 0.9

        # Social sentiment adjustment
        if venue.social_sentiment:
            sentiment = venue.social_sentiment.get("positive_sentiment", 0.5)
            if sentiment > 0.7:
                contextual_score *= 1.15
            elif sentiment < 0.3:
                contextual_score *= 0.85

        # Event frequency adjustment
        if venue.event_frequency > 0:
            contextual_score *= 1 + min(venue.event_frequency * 0.05, 0.2)

        # Demographic alignment adjustment
        if venue.local_median_income and venue.local_education_level:
            if venue.local_median_income > 60000 and venue.local_education_level > 30:
                contextual_score *= 1.1

        return min(1.0, max(0.0, contextual_score))

    def _calculate_data_completeness(self, venue: VenueData) -> float:
        """Calculate data completeness score for venue."""
        total_fields = 8  # Number of major data categories
        completed_fields = 0

        if venue.psychographic_scores:
            completed_fields += 1
        if venue.weather_conditions:
            completed_fields += 1
        if venue.traffic_conditions:
            completed_fields += 1
        if venue.social_sentiment:
            completed_fields += 1
        if venue.foot_traffic:
            completed_fields += 1
        if venue.local_median_income is not None:
            completed_fields += 1
        if venue.upcoming_events:
            completed_fields += 1
        if venue.ml_predictions:
            completed_fields += 1

        return completed_fields / total_fields


class VenueCentricMapBuilder:
    """
    Map builder focused on venue-centric visualization with comprehensive data access.

    Creates interactive maps where venues are the primary focus, with all contextual
    data accessible through venue interactions.
    """

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        self.center_coords = center_coords
        self.logger = logging.getLogger(__name__)

    def create_venue_centric_map(
        self, venue_data: List[VenueData], title: str = "Venue-Centric Analysis Map"
    ) -> folium.Map:
        """
        Create comprehensive venue-centric map with interactive dropdown and rich popups.

        Args:
            venue_data: List of enriched VenueData objects
            title: Map title

        Returns:
            Folium map object
        """
        if not venue_data:
            self.logger.warning("No venue data provided for map creation")
            return None

        # Calculate map center from venue data
        avg_lat = sum(v.lat for v in venue_data) / len(venue_data)
        avg_lng = sum(v.lng for v in venue_data) / len(venue_data)
        center = [avg_lat, avg_lng]

        # Create base map
        m = folium.Map(location=center, zoom_start=12, tiles="OpenStreetMap")

        # Add venue markers with comprehensive popups
        self._add_venue_markers(m, venue_data)

        # Add venue heatmap layer
        self._add_venue_heatmap(m, venue_data)

        # Add interactive dropdown sidebar
        self._add_interactive_dropdown_sidebar(m, venue_data, title)

        # Add comprehensive legend
        self._add_venue_centric_legend(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        return m

    def _add_venue_markers(self, map_obj: folium.Map, venue_data: List[VenueData]):
        """Add venue markers with comprehensive popups."""
        venue_layer = folium.FeatureGroup(name="🏢 Venues", show=True)

        for venue in venue_data:
            # Determine marker style based on overall score
            radius, color, fill_color = self._get_venue_marker_style(
                venue.overall_psychographic_score
            )

            # Create comprehensive popup
            popup_content = self._create_comprehensive_venue_popup(venue)

            # Add marker
            folium.CircleMarker(
                location=[venue.lat, venue.lng],
                radius=radius,
                popup=folium.Popup(popup_content, max_width=400),
                tooltip=f"{venue.name} | Score: {venue.overall_psychographic_score:.3f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.8,
                weight=2,
            ).add_to(venue_layer)

        venue_layer.add_to(map_obj)

    def _add_venue_heatmap(self, map_obj: folium.Map, venue_data: List[VenueData]):
        """Add venue score heatmap layer."""
        heatmap_layer = folium.FeatureGroup(name="🔥 Venue Score Heatmap", show=True)

        # Prepare heatmap data
        heat_data = []
        for venue in venue_data:
            if venue.overall_psychographic_score > 0:
                heat_data.append(
                    [venue.lat, venue.lng, venue.overall_psychographic_score]
                )

        if heat_data:
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
            ).add_to(heatmap_layer)

        heatmap_layer.add_to(map_obj)

    def _add_interactive_dropdown_sidebar(
        self, map_obj: folium.Map, venue_data: List[VenueData], title: str
    ):
        """Add interactive dropdown sidebar for venue navigation."""
        # Sort venues by score for dropdown
        sorted_venues = sorted(
            venue_data, key=lambda v: v.overall_psychographic_score, reverse=True
        )

        # Create venue options for dropdown
        venue_options = []
        for i, venue in enumerate(sorted_venues[:50]):  # Top 50 venues
            venue_options.append(
                {
                    "rank": i + 1,
                    "name": venue.name,
                    "category": venue.category,
                    "score": venue.overall_psychographic_score,
                    "lat": venue.lat,
                    "lng": venue.lng,
                    "completeness": venue.data_completeness_score,
                    "events": len(venue.upcoming_events),
                }
            )

        # Create dropdown HTML
        dropdown_html = self._create_dropdown_html(venue_options, title)
        map_obj.get_root().html.add_child(folium.Element(dropdown_html))

    def _create_dropdown_html(self, venue_options: List[Dict], title: str) -> str:
        """Create HTML for interactive dropdown sidebar."""
        options_html = ""
        for venue in venue_options:
            score_color = self._get_score_color(venue["score"])
            completeness_bar = int(venue["completeness"] * 100)

            options_html += f"""
            <div class="venue-option" onclick="selectVenue({venue['lat']}, {venue['lng']}, '{venue['name']}')"
                 data-score="{venue['score']:.3f}" data-category="{venue['category']}">
                <div class="venue-header">
                    <span class="venue-rank">#{venue['rank']}</span>
                    <span class="venue-name">{venue['name'][:30]}{'...' if len(venue['name']) > 30 else ''}</span>
                    <span class="venue-score" style="color: {score_color};">{venue['score']:.3f}</span>
                </div>
                <div class="venue-details">
                    <span class="venue-category">{venue['category'].title()}</span>
                    <span class="venue-events">{venue['events']} events</span>
                </div>
                <div class="completeness-bar">
                    <div class="completeness-fill" style="width: {completeness_bar}%;"></div>
                </div>
            </div>
            """

        return f"""
        <div id="venue-dropdown-sidebar" style="
            position: fixed; top: 20px; left: 20px; width: 350px; height: 80vh;
            background: rgba(255, 255, 255, 0.98); border: 2px solid #333;
            border-radius: 10px; z-index: 9999; font-family: Arial, sans-serif;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3); display: flex; flex-direction: column;
        ">
            <!-- Header -->
            <div style="padding: 20px; border-bottom: 2px solid #333; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        color: white; border-radius: 8px 8px 0 0;">
                <h3 style="margin: 0; font-size: 16px; font-weight: bold; text-align: center;">
                    🎯 {title}
                </h3>
                <div style="margin-top: 10px;">
                    <input type="text" id="venue-search" placeholder="Search venues..." 
                           onkeyup="filterVenues()" style="width: 100%; padding: 8px; border: none; 
                           border-radius: 5px; font-size: 14px;">
                </div>
                <div style="margin-top: 8px; font-size: 12px; text-align: center; opacity: 0.9;">
                    {len(venue_options)} venues • Click to navigate
                </div>
            </div>
            
            <!-- Venue List -->
            <div id="venue-list" style="flex: 1; overflow-y: auto; padding: 15px;">
                {options_html}
            </div>
            
            <!-- Footer -->
            <div style="padding: 15px; border-top: 1px solid #ddd; background: #f8f9fa; 
                        border-radius: 0 0 8px 8px; text-align: center;">
                <button onclick="toggleSidebar()" style="background: #007bff; color: white; 
                        border: none; padding: 8px 16px; border-radius: 5px; cursor: pointer;">
                    Hide Sidebar
                </button>
            </div>
        </div>

        <style>
        .venue-option {{
            margin: 8px 0; padding: 12px; border: 1px solid #ddd; border-radius: 8px;
            cursor: pointer; transition: all 0.3s; background: white;
        }}
        .venue-option:hover {{
            background: #f0f8ff; border-color: #007bff; transform: translateX(5px);
            box-shadow: 0 2px 8px rgba(0,123,255,0.2);
        }}
        .venue-header {{
            display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;
        }}
        .venue-rank {{
            background: #007bff; color: white; padding: 2px 6px; border-radius: 3px; 
            font-size: 11px; font-weight: bold;
        }}
        .venue-name {{
            font-weight: bold; font-size: 13px; color: #333; flex: 1; margin: 0 10px;
        }}
        .venue-score {{
            font-weight: bold; font-size: 12px;
        }}
        .venue-details {{
            display: flex; justify-content: space-between; font-size: 11px; color: #666; margin-bottom: 5px;
        }}
        .completeness-bar {{
            height: 4px; background: #e0e0e0; border-radius: 2px; overflow: hidden;
        }}
        .completeness-fill {{
            height: 100%; background: linear-gradient(90deg, #ff6b6b, #feca57, #48dbfb, #0abde3);
            transition: width 0.3s;
        }}
        #venue-dropdown-sidebar::-webkit-scrollbar {{
            width: 6px;
        }}
        #venue-dropdown-sidebar::-webkit-scrollbar-track {{
            background: #f1f1f1; border-radius: 3px;
        }}
        #venue-dropdown-sidebar::-webkit-scrollbar-thumb {{
            background: #888; border-radius: 3px;
        }}
        </style>

        <script>
        function selectVenue(lat, lng, name) {{
            // Get the map instance and center on venue
            var mapContainer = document.querySelector('.folium-map');
            if (mapContainer && mapContainer._leaflet_map) {{
                var map = mapContainer._leaflet_map;
                map.setView([lat, lng], 16);
                
                // Add temporary highlight marker
                var tempMarker = L.marker([lat, lng], {{
                    icon: L.icon({{
                        iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-gold.png',
                        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
                        iconSize: [25, 41],
                        iconAnchor: [12, 41],
                        popupAnchor: [1, -34],
                        shadowSize: [41, 41]
                    }})
                }}).addTo(map);
                
                // Remove marker after 3 seconds
                setTimeout(function() {{
                    map.removeLayer(tempMarker);
                }}, 3000);
            }}
        }}

        function filterVenues() {{
            var input = document.getElementById('venue-search');
            var filter = input.value.toLowerCase();
            var venueList = document.getElementById('venue-list');
            var venues = venueList.getElementsByClassName('venue-option');
            
            for (var i = 0; i < venues.length; i++) {{
                var venueName = venues[i].querySelector('.venue-name').textContent.toLowerCase();
                var venueCategory = venues[i].getAttribute('data-category').toLowerCase();
                
                if (venueName.includes(filter) || venueCategory.includes(filter)) {{
                    venues[i].style.display = '';
                }} else {{
                    venues[i].style.display = 'none';
                }}
            }}
        }}

        function toggleSidebar() {{
            var sidebar = document.getElementById('venue-dropdown-sidebar');
            if (sidebar.style.display === 'none') {{
                sidebar.style.display = 'flex';
            }} else {{
                sidebar.style.display = 'none';
            }}
        }}
        </script>
        """

    def _get_venue_marker_style(self, score: float) -> Tuple[int, str, str]:
        """Get marker styling based on venue score."""
        if score >= 0.8:
            return 14, "#d73027", "#d73027"  # High score - red
        elif score >= 0.6:
            return 12, "#fc8d59", "#fc8d59"  # Medium-high - orange
        elif score >= 0.4:
            return 10, "#fee08b", "#fee08b"  # Medium - yellow
        elif score >= 0.2:
            return 8, "#e0f3f8", "#e0f3f8"  # Low-medium - light blue
        else:
            return 6, "#91bfdb", "#91bfdb"  # Low - blue

    def _get_score_color(self, score: float) -> str:
        """Get color for score display."""
        if score >= 0.8:
            return "#d73027"
        elif score >= 0.6:
            return "#fc8d59"
        elif score >= 0.4:
            return "#fee08b"
        elif score >= 0.2:
            return "#4575b4"
        else:
            return "#91bfdb"

    def _create_comprehensive_venue_popup(self, venue: VenueData) -> str:
        """Create comprehensive popup content for venue."""
        # Format psychographic scores
        psycho_scores = ""
        if venue.psychographic_scores:
            for key, value in venue.psychographic_scores.items():
                psycho_scores += (
                    f"<li>{key.replace('_', ' ').title()}: {value:.3f}</li>"
                )

        # Format environmental context
        env_context = ""
        if venue.weather_conditions:
            temp = venue.weather_conditions.get("temperature_f", "N/A")
            condition = venue.weather_conditions.get("condition", "N/A")
            env_context += f"<li>Weather: {temp}°F, {condition}</li>"

        if venue.traffic_conditions:
            congestion = venue.traffic_conditions.get("congestion_score", "N/A")
            env_context += f"<li>Traffic: {congestion:.2f} congestion</li>"

        if venue.social_sentiment:
            sentiment = venue.social_sentiment.get("positive_sentiment", "N/A")
            mentions = venue.social_sentiment.get("mention_count", "N/A")
            env_context += (
                f"<li>Social: {sentiment:.2f} sentiment, {mentions} mentions</li>"
            )

        # Format demographic context
        demo_context = ""
        if venue.local_median_income:
            demo_context += f"<li>Area Income: ${venue.local_median_income:,.0f}</li>"
        if venue.local_education_level:
            demo_context += (
                f"<li>Education: {venue.local_education_level:.1f}% Bachelor's+</li>"
            )

        # Format events
        events_info = ""
        if venue.upcoming_events:
            events_info = f"<li>{len(venue.upcoming_events)} upcoming events</li>"
            if venue.avg_event_attendance:
                events_info += (
                    f"<li>Avg Attendance: {venue.avg_event_attendance:.0f}</li>"
                )

        # Format ML predictions
        ml_info = ""
        if venue.ml_predictions:
            base_pred = venue.ml_predictions.get("base_psychographic_density", "N/A")
            context_pred = venue.ml_predictions.get(
                "contextual_psychographic_density", "N/A"
            )
            ml_info += f"<li>Base ML Score: {base_pred:.3f}</li>"
            ml_info += f"<li>Context-Aware Score: {context_pred:.3f}</li>"

        return f"""
        <div style="font-family: Arial, sans-serif; max-width: 380px; line-height: 1.4;">
            <h3 style="margin: 0 0 15px 0; color: #333; border-bottom: 2px solid #007bff; padding-bottom: 8px;">
                🏢 {venue.name}
            </h3>
            
            <div style="margin-bottom: 12px;">
                <strong>📍 Basic Info:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    <li>Category: {venue.category.title()}</li>
                    <li>Provider: {venue.provider}</li>
                    <li>Rating: {venue.avg_rating or 'N/A'} ({venue.review_count or 0} reviews)</li>
                    <li>Address: {venue.address or 'N/A'}</li>
                </ul>
            </div>
            
            <div style="margin-bottom: 12px;">
                <strong>🎯 Psychographic Scores:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    {psycho_scores}
                    <li><strong>Overall Score: {venue.overall_psychographic_score:.3f}</strong></li>
                </ul>
            </div>
            
            {f'''<div style="margin-bottom: 12px;">
                <strong>🌍 Environmental Context:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    {env_context}
                </ul>
            </div>''' if env_context else ''}
            
            {f'''<div style="margin-bottom: 12px;">
                <strong>👥 Demographics:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    {demo_context}
                </ul>
            </div>''' if demo_context else ''}
            
            {f'''<div style="margin-bottom: 12px;">
                <strong>🎪 Events:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    {events_info}
                </ul>
            </div>''' if events_info else ''}
            
            {f'''<div style="margin-bottom: 12px;">
                <strong>🤖 ML Predictions:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
                    {ml_info}
                </ul>
            </div>''' if ml_info else ''}
            
            <div style="margin-top: 15px; padding: 8px; background: linear-gradient(135deg, #e3f2fd, #f3e5f5); 
                        border-radius: 5px; text-align: center;">
                <small style="color: #666;">
                    Data Completeness: {venue.data_completeness_score:.1%} | 
                    Sources: {len(venue.data_sources)}
                </small>
            </div>
        </div>
        """

    def _add_venue_centric_legend(self, map_obj: folium.Map):
        """Add comprehensive legend for venue-centric map."""
        legend_html = """
        <div style="position: fixed; bottom: 20px; right: 20px; width: 280px; height: auto;
                    background: rgba(255, 255, 255, 0.98); border: 2px solid #333; 
                    border-radius: 10px; z-index: 9999; font-size: 12px; padding: 20px;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.3);">
            <h4 style="margin: 0 0 15px 0; color: #333; text-align: center; 
                       border-bottom: 2px solid #333; padding-bottom: 8px;">
                🎯 Venue-Centric Legend
            </h4>
            
            <div style="margin-bottom: 15px;">
                <h5 style="margin: 0 0 8px 0; color: #007bff;">Psychographic Scores</h5>
                <div style="display: flex; align-items: center; margin: 3px 0;">
                    <div style="width: 12px; height: 12px; background: #d73027; border-radius: 50%; margin-right: 8px;"></div>
                    <span>High (0.8+)</span>
                </div>
                <div style="display: flex; align-items: center; margin: 3px 0;">
                    <div style="width: 10px; height: 10px; background: #fc8d59; border-radius: 50%; margin-right: 8px;"></div>
                    <span>Medium-High (0.6-0.8)</span>
                </div>
                <div style="display: flex; align-items: center; margin: 3px 0;">
                    <div style="width: 8px; height: 8px; background: #fee08b; border-radius: 50%; margin-right: 8px;"></div>
                    <span>Medium (0.4-0.6)</span>
                </div>
                <div style="display: flex; align-items: center; margin: 3px 0;">
                    <div style="width: 6px; height: 6px; background: #e0f3f8; border-radius: 50%; margin-right: 8px;"></div>
                    <span>Low-Medium (0.2-0.4)</span>
                </div>
                <div style="display: flex; align-items: center; margin: 3px 0;">
                    <div style="width: 4px; height: 4px; background: #91bfdb; border-radius: 50%; margin-right: 8px;"></div>
                    <span>Low (0-0.2)</span>
                </div>
            </div>
            
            <div style="margin-bottom: 15px;">
                <h5 style="margin: 0 0 8px 0; color: #007bff;">Features</h5>
                <div style="font-size: 11px; line-height: 1.4;">
                    <div>🏢 Venue markers with comprehensive data</div>
                    <div>🔥 Score-based heatmap overlay</div>
                    <div>🎯 Interactive dropdown navigation</div>
                    <div>📊 Context-aware ML predictions</div>
                    <div>🌍 Environmental & demographic context</div>
                </div>
            </div>
            
            <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
                <small style="color: #666; font-size: 10px;">
                    Venue-Centric Architecture v1.0<br>
                    All data consolidated around venues
                </small>
            </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))


def create_venue_centric_map_from_db(
    db_conn,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    min_score_threshold: float = 0.1,
    limit: Optional[int] = 100,
    title: str = "Kansas City Venue-Centric Analysis",
) -> folium.Map:
    """
    Convenience function to create venue-centric map directly from database connection.

    Args:
        db_conn: Database connection
        bbox: Bounding box (min_lat, min_lng, max_lat, max_lng)
        min_score_threshold: Minimum psychographic score threshold
        limit: Maximum number of venues
        title: Map title

    Returns:
        Folium map object
    """
    # Initialize services
    data_service = VenueCentricDataService(db_conn)
    map_builder = VenueCentricMapBuilder()

    # Get enriched venue data
    enriched_venues = data_service.get_enriched_venue_data(
        bbox=bbox, min_score_threshold=min_score_threshold, limit=limit
    )

    if not enriched_venues:
        logger.warning("No enriched venues found for map creation")
        return None

    # Create venue-centric map
    venue_map = map_builder.create_venue_centric_map(
        venue_data=enriched_venues, title=title
    )

    return venue_map
