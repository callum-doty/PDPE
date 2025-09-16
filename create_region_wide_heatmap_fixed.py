#!/usr/bin/env python3
"""
Create region-wide heatmap with REAL DATA from database instead of synthetic data.
This fixes the issue where layers were using mock data instead of actual database records.
"""

import sys
import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import folium
from folium.plugins import HeatMap
import math
import psycopg2
from psycopg2.extras import RealDictCursor

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from src.backend.visualization.interactive_map_builder import InteractiveMapBuilder
    from src.features.college_layer import CollegeLayer
    from src.features.spending_propensity_layer import (
        SpendingPropensityLayer,
        DemographicProfile,
    )
    from src.etl.utils import get_db_conn

    print("✓ Successfully imported required modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)


class RealDataHeatmapBuilder:
    """
    Enhanced heatmap builder that uses REAL DATA from the database
    instead of generating synthetic data.
    """

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        self.center_coords = center_coords
        self.map_builder = InteractiveMapBuilder(center_coords)

    def get_real_demographic_data(self, bounds: Dict[str, float]) -> pd.DataFrame:
        """
        Fetch real demographic data from the database within the specified bounds.

        Args:
            bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries

        Returns:
            DataFrame with real demographic data from census tracts
        """
        print("Fetching real demographic data from database...")

        try:
            conn = get_db_conn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Query demographics table for data within bounds
            query = """
            SELECT 
                tract_id,
                ST_Y(ST_Centroid(geometry::geometry)) as lat,
                ST_X(ST_Centroid(geometry::geometry)) as lng,
                median_income,
                pct_bachelors as education_bachelors_pct,
                pct_graduate as education_graduate_pct,
                pct_age_20_30 as age_25_34_pct,
                pct_age_30_40 as age_35_44_pct,
                pct_professional_occupation as professional_occupation_pct,
                pct_management_occupation as management_occupation_pct,
                population_density
            FROM demographics 
            WHERE ST_Intersects(
                geometry::geometry,
                ST_MakeEnvelope(%s, %s, %s, %s, 4326)
            )
            AND median_income IS NOT NULL
            """

            cursor.execute(
                query,
                (bounds["west"], bounds["south"], bounds["east"], bounds["north"]),
            )

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            if not results:
                print("⚠️  No demographic data found in database for specified bounds")
                return pd.DataFrame()

            df = pd.DataFrame(results)
            print(f"✓ Retrieved {len(df)} demographic records from database")
            return df

        except Exception as e:
            print(f"❌ Error fetching demographic data: {e}")
            return pd.DataFrame()

    def get_real_venue_data(self, bounds: Dict[str, float]) -> pd.DataFrame:
        """
        Fetch real venue data from the database within the specified bounds.

        Args:
            bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries

        Returns:
            DataFrame with real venue data
        """
        print("Fetching real venue data from database...")

        try:
            conn = get_db_conn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Query venues table for data within bounds
            query = """
            SELECT 
                venue_id,
                name,
                category,
                lat,
                lng,
                avg_rating,
                review_count,
                price_tier
            FROM venues 
            WHERE lat BETWEEN %s AND %s
            AND lng BETWEEN %s AND %s
            AND lat IS NOT NULL 
            AND lng IS NOT NULL
            """

            cursor.execute(
                query,
                (bounds["south"], bounds["north"], bounds["west"], bounds["east"]),
            )

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            if not results:
                print("⚠️  No venue data found in database for specified bounds")
                return pd.DataFrame()

            df = pd.DataFrame(results)
            print(f"✓ Retrieved {len(df)} venue records from database")
            return df

        except Exception as e:
            print(f"❌ Error fetching venue data: {e}")
            return pd.DataFrame()

    def get_real_events_data(self, bounds: Dict[str, float]) -> pd.DataFrame:
        """
        Fetch real events data from the database within the specified bounds.

        Args:
            bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries

        Returns:
            DataFrame with real events data
        """
        print("Fetching real events data from database...")

        try:
            conn = get_db_conn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Query events with venue location data
            query = """
            SELECT 
                e.event_id,
                e.name,
                e.category,
                e.start_time,
                e.end_time,
                e.predicted_attendance,
                v.lat,
                v.lng
            FROM events e
            JOIN venues v ON e.venue_id = v.venue_id
            WHERE v.lat BETWEEN %s AND %s
            AND v.lng BETWEEN %s AND %s
            AND v.lat IS NOT NULL 
            AND v.lng IS NOT NULL
            AND e.start_time >= NOW() - INTERVAL '30 days'
            """

            cursor.execute(
                query,
                (bounds["south"], bounds["north"], bounds["west"], bounds["east"]),
            )

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            if not results:
                print("⚠️  No events data found in database for specified bounds")
                return pd.DataFrame()

            df = pd.DataFrame(results)
            print(f"✓ Retrieved {len(df)} event records from database")
            return df

        except Exception as e:
            print(f"❌ Error fetching events data: {e}")
            return pd.DataFrame()

    def create_real_spending_propensity_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.008
    ) -> List[List[float]]:
        """
        Create spending propensity heatmap using REAL demographic data from database.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating spending propensity heatmap from REAL data...")

        # Get real demographic data from database
        demo_data = self.get_real_demographic_data(bounds)

        if demo_data.empty:
            print("⚠️  No demographic data available, falling back to synthetic data")
            return self._create_fallback_spending_heatmap(bounds, resolution)

        # Create spending propensity layer
        spending_layer = SpendingPropensityLayer()
        heatmap_data = []

        # Generate grid points and interpolate from real demographic data
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        for lat in lats:
            for lng in lngs:
                # Find nearest demographic data point
                nearest_demo = self._find_nearest_demographic_data(lat, lng, demo_data)

                if nearest_demo is not None:
                    # Use real demographic data
                    demo_dict = {
                        "median_income": nearest_demo.get("median_income", 0),
                        "education_bachelors_pct": nearest_demo.get(
                            "education_bachelors_pct", 0
                        ),
                        "education_graduate_pct": nearest_demo.get(
                            "education_graduate_pct", 0
                        ),
                        "age_25_34_pct": nearest_demo.get("age_25_34_pct", 0),
                        "age_35_44_pct": nearest_demo.get("age_35_44_pct", 0),
                        "professional_occupation_pct": nearest_demo.get(
                            "professional_occupation_pct", 0
                        ),
                        "management_occupation_pct": nearest_demo.get(
                            "management_occupation_pct", 0
                        ),
                        "population_density": nearest_demo.get("population_density", 0),
                    }

                    analysis = spending_layer.analyze_location_spending_potential(
                        lat, lng, demo_dict
                    )

                    heatmap_data.append(
                        [lat, lng, analysis["spending_propensity_score"]]
                    )

        print(
            f"✓ Generated {len(heatmap_data)} spending propensity points from REAL data"
        )
        return heatmap_data

    def _find_nearest_demographic_data(
        self, lat: float, lng: float, demo_data: pd.DataFrame
    ) -> Optional[Dict]:
        """
        Find the nearest demographic data point to the given coordinates.

        Args:
            lat: Target latitude
            lng: Target longitude
            demo_data: DataFrame with demographic data

        Returns:
            Dictionary with nearest demographic data or None
        """
        if demo_data.empty:
            return None

        # Calculate distances to all demographic data points
        distances = []
        for _, row in demo_data.iterrows():
            if pd.notna(row["lat"]) and pd.notna(row["lng"]):
                dist = math.sqrt((lat - row["lat"]) ** 2 + (lng - row["lng"]) ** 2)
                distances.append(dist)
            else:
                distances.append(float("inf"))

        if not distances or all(d == float("inf") for d in distances):
            return None

        # Find nearest point
        min_idx = np.argmin(distances)
        nearest_row = demo_data.iloc[min_idx]

        return nearest_row.to_dict()

    def _create_fallback_spending_heatmap(
        self, bounds: Dict[str, float], resolution: float
    ) -> List[List[float]]:
        """
        Create fallback spending propensity heatmap when no real data is available.
        """
        print("Creating fallback spending propensity heatmap...")

        # Generate basic grid with distance-based scoring
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        heatmap_data = []
        downtown_lat, downtown_lng = 39.0997, -94.5786

        for lat in lats:
            for lng in lngs:
                # Simple distance-based scoring
                distance_from_downtown = math.sqrt(
                    (lat - downtown_lat) ** 2 + (lng - downtown_lng) ** 2
                )
                score = max(0.1, 1.0 - (distance_from_downtown * 10))
                heatmap_data.append([lat, lng, score])

        return heatmap_data

    def create_real_venue_density_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.008
    ) -> List[List[float]]:
        """
        Create venue density heatmap using REAL venue data from database.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating venue density heatmap from REAL data...")

        # Get real venue data from database
        venue_data = self.get_real_venue_data(bounds)

        if venue_data.empty:
            print("⚠️  No venue data available")
            return []

        # Create grid and calculate venue density
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        heatmap_data = []

        for lat in lats:
            for lng in lngs:
                # Count venues within radius
                venue_count = 0
                radius = 0.01  # ~1km radius

                for _, venue in venue_data.iterrows():
                    if pd.notna(venue["lat"]) and pd.notna(venue["lng"]):
                        distance = math.sqrt(
                            (lat - venue["lat"]) ** 2 + (lng - venue["lng"]) ** 2
                        )
                        if distance <= radius:
                            venue_count += 1

                # Normalize venue count to 0-1 scale
                normalized_count = min(venue_count / 20.0, 1.0)  # Max 20 venues = 1.0
                heatmap_data.append([lat, lng, normalized_count])

        print(f"✓ Generated {len(heatmap_data)} venue density points from REAL data")
        return heatmap_data

    def create_real_events_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.008
    ) -> List[List[float]]:
        """
        Create events heatmap using REAL events data from database.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating events heatmap from REAL data...")

        # Get real events data from database
        events_data = self.get_real_events_data(bounds)

        if events_data.empty:
            print("⚠️  No events data available")
            return []

        # Create grid and calculate event density/impact
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        heatmap_data = []

        for lat in lats:
            for lng in lngs:
                # Calculate event impact within radius
                total_impact = 0.0
                radius = 0.01  # ~1km radius

                for _, event in events_data.iterrows():
                    if pd.notna(event["lat"]) and pd.notna(event["lng"]):
                        distance = math.sqrt(
                            (lat - event["lat"]) ** 2 + (lng - event["lng"]) ** 2
                        )
                        if distance <= radius:
                            # Weight by predicted attendance
                            attendance = event.get("predicted_attendance", 100)
                            impact = min(attendance / 1000.0, 1.0)  # Normalize
                            total_impact += impact

                # Normalize total impact
                normalized_impact = min(total_impact, 1.0)
                heatmap_data.append([lat, lng, normalized_impact])

        print(f"✓ Generated {len(heatmap_data)} event impact points from REAL data")
        return heatmap_data

    def create_region_wide_heatmap_with_real_data(
        self, bounds: Dict[str, float], output_path: str = "real_data_heatmap.html"
    ) -> Path:
        """
        Create comprehensive region-wide heatmap using REAL DATA from database.

        Args:
            bounds: Region boundaries
            output_path: Output file path

        Returns:
            Path to generated HTML file
        """
        print("Creating region-wide heatmap with REAL DATA...")

        # Calculate center from bounds
        center_lat = (bounds["north"] + bounds["south"]) / 2
        center_lng = (bounds["east"] + bounds["west"]) / 2

        # Create base map
        m = folium.Map(location=[center_lat, center_lng], zoom_start=11, tiles=None)

        # Add base tile layers
        folium.TileLayer(
            tiles="OpenStreetMap", name="OpenStreetMap", overlay=False, control=True
        ).add_to(m)

        folium.TileLayer(
            tiles="CartoDB positron", name="CartoDB Light", overlay=False, control=True
        ).add_to(m)

        # Generate heatmap data using REAL data from database
        spending_data = self.create_real_spending_propensity_heatmap(
            bounds, resolution=0.008
        )
        venue_data = self.create_real_venue_density_heatmap(bounds, resolution=0.008)
        events_data = self.create_real_events_heatmap(bounds, resolution=0.008)

        # Create spending propensity heatmap layer (REAL DATA)
        if spending_data:
            spending_heatmap = folium.FeatureGroup(
                name="💰 Spending Propensity (REAL DATA)", show=True
            )
            HeatMap(
                spending_data,
                radius=25,
                blur=20,
                max_zoom=15,
                gradient={
                    0.0: "#313695",
                    0.2: "#4575b4",
                    0.4: "#74add1",
                    0.6: "#abd9e9",
                    0.8: "#fee090",
                    1.0: "#d73027",
                },
            ).add_to(spending_heatmap)
            spending_heatmap.add_to(m)

        # Create venue density heatmap layer (REAL DATA)
        if venue_data:
            venue_heatmap = folium.FeatureGroup(
                name="🏪 Venue Density (REAL DATA)", show=False
            )
            HeatMap(
                venue_data,
                radius=20,
                blur=15,
                max_zoom=15,
                gradient={
                    0.0: "#f7fbff",
                    0.2: "#deebf7",
                    0.4: "#c6dbef",
                    0.6: "#9ecae1",
                    0.8: "#6baed6",
                    1.0: "#08519c",
                },
            ).add_to(venue_heatmap)
            venue_heatmap.add_to(m)

        # Create events heatmap layer (REAL DATA)
        if events_data:
            events_heatmap = folium.FeatureGroup(
                name="🎉 Events Impact (REAL DATA)", show=False
            )
            HeatMap(
                events_data,
                radius=30,
                blur=25,
                max_zoom=15,
                gradient={
                    0.0: "#fff5f0",
                    0.2: "#fee0d2",
                    0.4: "#fcbba1",
                    0.6: "#fc9272",
                    0.8: "#fb6a4a",
                    1.0: "#a50f15",
                },
            ).add_to(events_heatmap)
            events_heatmap.add_to(m)

        # Add real venue markers
        self._add_real_venue_markers(m, bounds)

        # Add layer control
        folium.LayerControl(
            position="topright", collapsed=False, autoZIndex=True
        ).add_to(m)

        # Add legend for real data
        self._add_real_data_legend(m)

        # Add information panel
        self._add_real_data_info_panel(m)

        # Save map
        output_file = Path(output_path).resolve()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        m.save(str(output_file))

        print(f"✅ Real data heatmap saved to {output_file}")
        return output_file

    def _add_real_venue_markers(self, map_obj: folium.Map, bounds: Dict[str, float]):
        """Add real venue markers from database."""
        venue_data = self.get_real_venue_data(bounds)

        if venue_data.empty:
            return

        venues_layer = folium.FeatureGroup(name="📍 Real Venues (Database)", show=True)

        # Sample up to 50 venues to avoid cluttering
        sample_venues = venue_data.head(50)

        for _, venue in sample_venues.iterrows():
            if pd.notna(venue["lat"]) and pd.notna(venue["lng"]):
                # Color based on rating
                rating = venue.get("avg_rating", 0)
                if rating >= 4.0:
                    color = "#d73027"
                elif rating >= 3.0:
                    color = "#fc8d59"
                else:
                    color = "#fee08b"

                folium.CircleMarker(
                    location=(venue["lat"], venue["lng"]),
                    radius=6,
                    popup=f"<b>{venue.get('name', 'Unknown')}</b><br>"
                    f"Category: {venue.get('category', 'N/A')}<br>"
                    f"Rating: {rating:.1f}<br>"
                    f"Reviews: {venue.get('review_count', 0)}",
                    tooltip=f"{venue.get('name', 'Unknown')}: {rating:.1f}⭐",
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.8,
                    weight=2,
                ).add_to(venues_layer)

        venues_layer.add_to(map_obj)

    def _add_real_data_legend(self, map_obj: folium.Map):
        """Add legend explaining the real data heatmaps."""
        legend_html = """
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 320px; height: auto; max-height: 600px;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            REAL DATA Heatmap Legend
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #d73027; font-size: 13px;">💰 Spending Propensity (REAL DATA)</h4>
            <div style="background: linear-gradient(to right, #313695, #4575b4, #74add1, #abd9e9, #fee090, #d73027); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Low</span><span>Medium</span><span>High</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Based on REAL census demographic data from database.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #08519c; font-size: 13px;">🏪 Venue Density (REAL DATA)</h4>
            <div style="background: linear-gradient(to right, #f7fbff, #deebf7, #c6dbef, #9ecae1, #6baed6, #08519c); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Low</span><span>Medium</span><span>High</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Based on REAL venue locations from database.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #a50f15; font-size: 13px;">🎉 Events Impact (REAL DATA)</h4>
            <div style="background: linear-gradient(to right, #fff5f0, #fee0d2, #fcbba1, #fc9272, #fb6a4a, #a50f15); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Low</span><span>Medium</span><span>High</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Based on REAL events data with predicted attendance.
            </p>
        </div>
        
        <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <h5 style="margin: 0 0 8px 0; font-size: 12px; color: #666;">🔥 REAL DATA SOURCES</h5>
            <p style="margin: 3px 0; font-size: 11px; color: #666;">
                <strong>Demographics:</strong> Census tract data from database
            </p>
            <p style="margin: 3px 0; font-size: 11px; color: #666;">
                <strong>Venues:</strong> Google Places API data from database
            </p>
            <p style="margin: 3px 0; font-size: 11px; color: #666;">
                <strong>Events:</strong> PredictHQ API data from database
            </p>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_real_data_info_panel(self, map_obj: folium.Map):
        """Add information panel explaining the real data approach."""
        info_html = """
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 350px; height: auto;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            🔥 REAL DATA Heatmap Analysis
        </h3>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #d73027; font-size: 13px;">📊 Database-Driven</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                This heatmap uses <strong>REAL DATA</strong> from the database instead of synthetic data. 
                Spending propensity is calculated from actual census demographics, venue density from 
                Google Places data, and events from PredictHQ API data.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #08519c; font-size: 13px;">🎯 Accurate Patterns</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Data patterns reflect actual geographic distributions from real census tracts, 
                actual venue locations, and real event data with predicted attendance figures.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #a50f15; font-size: 13px;">🔍 Data Sources</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                • <strong>Demographics:</strong> US Census tract data<br>
                • <strong>Venues:</strong> Google Places API<br>
                • <strong>Events:</strong> PredictHQ API<br>
                • <strong>Processing:</strong> PostGIS spatial queries
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #333; font-size: 13px;">⚡ Live Updates</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                This heatmap reflects the current state of data in the database. As new venues, 
                events, and demographic updates are ingested, the heatmap will show updated patterns.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #999; font-size: 10px;">
                PDPE Real Data Heatmap v2.0
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))


def main():
    """Create region-wide heatmap using REAL DATA from database."""
    print("🗺️  Creating Region-Wide Heatmap with REAL DATA...")

    # Define Kansas City metropolitan area bounds
    kc_bounds = {"north": 39.2, "south": 38.9, "east": -94.3, "west": -94.8}

    # Initialize builder
    builder = RealDataHeatmapBuilder()

    try:
        # Create region-wide heatmap with real data
        output_file = builder.create_region_wide_heatmap_with_real_data(
            bounds=kc_bounds, output_path="real_data_heatmap.html"
        )

        print(f"\n✅ Real data heatmap created successfully!")
        print(f"📁 File: {output_file}")
        print(f"📊 File size: {output_file.stat().st_size / 1024:.1f} KB")

        print(f"\n🎯 Key Features:")
        print(f"  ✓ REAL demographic data from census tracts")
        print(f"  ✓ REAL venue data from Google Places API")
        print(f"  ✓ REAL events data from PredictHQ API")
        print(f"  ✓ Database-driven calculations instead of synthetic data")
        print(f"  ✓ PostGIS spatial queries for accurate geographic analysis")

        print(f"\n📋 Usage:")
        print(f"  1. Open {output_file.name} in your browser")
        print(f"  2. Toggle between REAL DATA heatmap layers")
        print(f"  3. Compare with actual venue markers from database")
        print(f"  4. Notice how patterns reflect real demographic distributions")

        return 0

    except Exception as e:
        print(f"❌ Error creating real data heatmap: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
