#!/usr/bin/env python3
"""
Create region-wide heatmap with continuous coverage instead of point-based data.
This addresses the issue where layers only show at specific event locations.
"""

import sys
import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import folium
from folium.plugins import HeatMap
import math

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder
    from features.college_layer import CollegeLayer
    from features.spending_propensity_layer import (
        SpendingPropensityLayer,
        DemographicProfile,
    )

    print("✓ Successfully imported required modules")
except ImportError as e:
    print(f"✗ Failed to import modules: {e}")
    sys.exit(1)


class RegionWideHeatmapBuilder:
    """
    Enhanced heatmap builder that creates continuous region-wide coverage
    instead of just showing data at specific event locations.
    """

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        self.center_coords = center_coords
        self.map_builder = InteractiveMapBuilder(center_coords)

    def generate_demographic_grid(
        self, bounds: Dict[str, float], resolution: float = 0.005
    ) -> pd.DataFrame:
        """
        Generate a grid of demographic data across the region.

        Args:
            bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries
            resolution: Grid resolution in degrees (0.005 ≈ 500m)

        Returns:
            DataFrame with demographic data for each grid point
        """
        print(f"Generating demographic grid with resolution {resolution} degrees...")

        # Create grid points
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        grid_data = []

        for i, lat in enumerate(lats):
            for j, lng in enumerate(lngs):
                # Generate realistic demographic data based on location
                demo_data = self._generate_realistic_demographics(lat, lng)

                grid_data.append(
                    {"lat": lat, "lng": lng, "grid_id": f"{i}_{j}", **demo_data}
                )

        print(f"Generated {len(grid_data)} grid points")
        return pd.DataFrame(grid_data)

    def _generate_realistic_demographics(self, lat: float, lng: float) -> Dict:
        """
        Generate realistic demographic data based on location.
        Uses distance from city center and some randomness to simulate real patterns.
        """
        # Distance from downtown Kansas City
        downtown_lat, downtown_lng = 39.0997, -94.5786
        distance_from_downtown = math.sqrt(
            (lat - downtown_lat) ** 2 + (lng - downtown_lng) ** 2
        )

        # Base demographics on distance from downtown (urban vs suburban patterns)
        urban_factor = max(0, 1 - (distance_from_downtown * 20))  # Closer = more urban

        # Add some randomness for realism
        noise = np.random.normal(0, 0.1)

        # Generate demographics with urban/suburban patterns
        base_income = 45000 + (urban_factor * 30000) + (noise * 10000)
        median_income = max(25000, min(150000, base_income))

        # Education tends to be higher in urban areas and near universities
        education_base = 20 + (urban_factor * 25) + (noise * 10)
        education_bachelors_pct = max(5, min(70, education_base))
        education_graduate_pct = max(2, min(30, education_base * 0.4))

        # Age distribution
        age_25_34_pct = max(8, min(25, 15 + (urban_factor * 8) + (noise * 3)))
        age_35_44_pct = max(8, min(20, 13 + (urban_factor * 5) + (noise * 3)))

        # Professional occupations higher in urban areas
        professional_pct = max(10, min(60, 25 + (urban_factor * 20) + (noise * 8)))
        management_pct = max(5, min(25, 12 + (urban_factor * 8) + (noise * 5)))

        # Population density much higher in urban areas
        density_base = 500 + (urban_factor * 4000) + (noise * 1000)
        population_density = max(50, density_base)

        return {
            "median_income": median_income,
            "education_bachelors_pct": education_bachelors_pct,
            "education_graduate_pct": education_graduate_pct,
            "age_25_34_pct": age_25_34_pct,
            "age_35_44_pct": age_35_44_pct,
            "professional_occupation_pct": professional_pct,
            "management_occupation_pct": management_pct,
            "population_density": population_density,
        }

    def create_spending_propensity_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.005
    ) -> List[List[float]]:
        """
        Create continuous spending propensity heatmap data.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating spending propensity heatmap...")

        # Generate demographic grid
        demo_grid = self.generate_demographic_grid(bounds, resolution)

        # Calculate spending propensity for each grid point
        spending_layer = SpendingPropensityLayer()
        heatmap_data = []

        for _, row in demo_grid.iterrows():
            demo_data = {
                "median_income": row["median_income"],
                "education_bachelors_pct": row["education_bachelors_pct"],
                "education_graduate_pct": row["education_graduate_pct"],
                "age_25_34_pct": row["age_25_34_pct"],
                "age_35_44_pct": row["age_35_44_pct"],
                "professional_occupation_pct": row["professional_occupation_pct"],
                "management_occupation_pct": row["management_occupation_pct"],
                "population_density": row["population_density"],
            }

            analysis = spending_layer.analyze_location_spending_potential(
                row["lat"], row["lng"], demo_data
            )

            # Add to heatmap data with intensity
            heatmap_data.append(
                [row["lat"], row["lng"], analysis["spending_propensity_score"]]
            )

        print(f"Generated {len(heatmap_data)} spending propensity points")
        return heatmap_data

    def create_college_density_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.005
    ) -> List[List[float]]:
        """
        Create continuous college density heatmap data.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating college density heatmap...")

        # Generate grid points
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        college_layer = CollegeLayer()
        heatmap_data = []

        for lat in lats:
            for lng in lngs:
                result = college_layer.calculate_college_density_score(lat, lng)

                # Add to heatmap data
                heatmap_data.append([lat, lng, result["score"]])

        print(f"Generated {len(heatmap_data)} college density points")
        return heatmap_data

    def create_population_density_heatmap(
        self, bounds: Dict[str, float], resolution: float = 0.005
    ) -> List[List[float]]:
        """
        Create population density heatmap based on distance from urban centers.

        Args:
            bounds: Region boundaries
            resolution: Grid resolution

        Returns:
            List of [lat, lng, intensity] points for heatmap
        """
        print("Creating population density heatmap...")

        # Define urban centers in Kansas City area
        urban_centers = [
            (39.0997, -94.5786, 1.0),  # Downtown KC
            (39.1012, -94.5844, 0.8),  # Power & Light
            (39.0739, -94.5861, 0.7),  # Crossroads
            (39.0458, -94.5833, 0.6),  # Union Station
            (39.1167, -94.6275, 0.5),  # Crown Center
        ]

        # Generate grid points
        lats = np.arange(bounds["south"], bounds["north"], resolution)
        lngs = np.arange(bounds["west"], bounds["east"], resolution)

        heatmap_data = []

        for lat in lats:
            for lng in lngs:
                # Calculate density based on proximity to urban centers
                max_density = 0

                for center_lat, center_lng, center_weight in urban_centers:
                    distance = math.sqrt(
                        (lat - center_lat) ** 2 + (lng - center_lng) ** 2
                    )
                    # Density decreases with distance
                    density = center_weight * math.exp(
                        -distance * 50
                    )  # Exponential decay
                    max_density = max(max_density, density)

                # Add some base density everywhere
                final_density = max_density + 0.1

                heatmap_data.append([lat, lng, min(final_density, 1.0)])

        print(f"Generated {len(heatmap_data)} population density points")
        return heatmap_data

    def create_region_wide_heatmap(
        self, bounds: Dict[str, float], output_path: str = "region_wide_heatmap.html"
    ) -> Path:
        """
        Create comprehensive region-wide heatmap with multiple continuous layers.

        Args:
            bounds: Region boundaries
            output_path: Output file path

        Returns:
            Path to generated HTML file
        """
        print("Creating region-wide heatmap...")

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

        # Generate heatmap data for different layers
        spending_data = self.create_spending_propensity_heatmap(
            bounds, resolution=0.008
        )
        college_data = self.create_college_density_heatmap(bounds, resolution=0.008)
        population_data = self.create_population_density_heatmap(
            bounds, resolution=0.008
        )

        # Create spending propensity heatmap layer
        spending_heatmap = folium.FeatureGroup(
            name="💰 Spending Propensity (Region-wide)", show=True
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

        # Create college density heatmap layer
        college_heatmap = folium.FeatureGroup(
            name="🎓 College Density (Region-wide)", show=False
        )
        HeatMap(
            college_data,
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
        ).add_to(college_heatmap)
        college_heatmap.add_to(m)

        # Create population density heatmap layer
        population_heatmap = folium.FeatureGroup(
            name="🏙️ Population Density (Region-wide)", show=False
        )
        HeatMap(
            population_data,
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
        ).add_to(population_heatmap)
        population_heatmap.add_to(m)

        # Add sample event markers for comparison
        self._add_sample_events(m)

        # Add layer control
        folium.LayerControl(
            position="topright", collapsed=False, autoZIndex=True
        ).add_to(m)

        # Add comprehensive legend
        self._add_region_wide_legend(m)

        # Add information panel
        self._add_region_info_panel(m)

        # Save map
        output_file = Path(output_path).resolve()
        output_file.parent.mkdir(parents=True, exist_ok=True)
        m.save(str(output_file))

        print(f"Region-wide heatmap saved to {output_file}")
        return output_file

    def _add_sample_events(self, map_obj: folium.Map):
        """Add sample event markers to show the difference between point and region data."""
        events_layer = folium.FeatureGroup(
            name="📍 Sample Events (Point Data)", show=True
        )

        sample_events = [
            (39.0997, -94.5786, "Jazz Festival", 0.85),
            (39.1012, -94.5844, "Food Truck Festival", 0.72),
            (39.0739, -94.5861, "Art Gallery Opening", 0.68),
            (39.0458, -94.5833, "Tech Conference", 0.91),
        ]

        for lat, lng, name, score in sample_events:
            # Color based on score
            if score >= 0.8:
                color = "#d73027"
            elif score >= 0.6:
                color = "#fc8d59"
            else:
                color = "#fee08b"

            folium.CircleMarker(
                location=(lat, lng),
                radius=8,
                popup=f"<b>{name}</b><br>Score: {score:.2f}",
                tooltip=f"{name}: {score:.2f}",
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.8,
                weight=2,
            ).add_to(events_layer)

        events_layer.add_to(map_obj)

    def _add_region_wide_legend(self, map_obj: folium.Map):
        """Add legend explaining the region-wide heatmaps."""
        legend_html = """
        <div style="position: fixed; 
                    bottom: 20px; left: 20px; width: 320px; height: auto; max-height: 600px;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);
                    overflow-y: auto;">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            Region-Wide Heatmap Legend
        </h3>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #d73027; font-size: 13px;">💰 Spending Propensity</h4>
            <div style="background: linear-gradient(to right, #313695, #4575b4, #74add1, #abd9e9, #fee090, #d73027); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Low</span><span>Medium</span><span>High</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Continuous coverage showing spending potential across the entire region based on demographics.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #08519c; font-size: 13px;">🎓 College Density</h4>
            <div style="background: linear-gradient(to right, #f7fbff, #deebf7, #c6dbef, #9ecae1, #6baed6, #08519c); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Low</span><span>Medium</span><span>High</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Regional coverage of college and university proximity influence.
            </p>
        </div>
        
        <div style="margin-bottom: 15px;">
            <h4 style="margin: 0 0 8px 0; color: #a50f15; font-size: 13px;">🏙️ Population Density</h4>
            <div style="background: linear-gradient(to right, #fff5f0, #fee0d2, #fcbba1, #fc9272, #fb6a4a, #a50f15); 
                        height: 15px; width: 100%; margin: 5px 0; border-radius: 3px;"></div>
            <div style="display: flex; justify-content: space-between; font-size: 10px; color: #666;">
                <span>Rural</span><span>Suburban</span><span>Urban</span>
            </div>
            <p style="margin: 5px 0 0 0; font-size: 11px; color: #666;">
                Population density patterns across the metropolitan area.
            </p>
        </div>
        
        <div style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <h5 style="margin: 0 0 8px 0; font-size: 12px; color: #666;">Key Differences</h5>
            <p style="margin: 3px 0; font-size: 11px; color: #666;">
                <strong>Region-wide layers:</strong> Show continuous coverage across the entire area
            </p>
            <p style="margin: 3px 0; font-size: 11px; color: #666;">
                <strong>Point data:</strong> Only shows values at specific event locations
            </p>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(legend_html))

    def _add_region_info_panel(self, map_obj: folium.Map):
        """Add information panel explaining the region-wide approach."""
        info_html = """
        <div style="position: fixed; 
                    top: 20px; right: 20px; width: 350px; height: auto;
                    background-color: rgba(255, 255, 255, 0.95); 
                    border: 2px solid #333; border-radius: 8px;
                    z-index: 9999; font-size: 12px; padding: 15px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.2);">
        <h3 style="margin: 0 0 15px 0; color: #333; text-align: center; border-bottom: 2px solid #333; padding-bottom: 8px;">
            Region-Wide Heatmap Analysis
        </h3>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #d73027; font-size: 13px;">🌍 Continuous Coverage</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Unlike point-based data that only shows values at specific locations, these heatmaps provide 
                <strong>continuous regional coverage</strong>. Spending propensity, college influence, and 
                population density are calculated for the entire metropolitan area.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #08519c; font-size: 13px;">📊 Realistic Patterns</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Data patterns reflect real-world geographic distributions: higher spending propensity in 
                affluent areas, college influence radiating from universities, and population density 
                concentrated in urban centers.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #a50f15; font-size: 13px;">🎛️ Layer Controls</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                Toggle between different heatmap layers to see how various factors influence the region. 
                Compare with point-based event data to see the difference in coverage approaches.
            </p>
        </div>
        
        <div style="margin-bottom: 12px;">
            <h4 style="margin: 0 0 6px 0; color: #333; font-size: 13px;">💡 Business Applications</h4>
            <p style="margin: 0; font-size: 11px; color: #666; line-height: 1.4;">
                This approach is more realistic for business planning as spending propensity, demographics, 
                and other factors exist everywhere, not just at specific event locations.
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd;">
            <small style="color: #999; font-size: 10px;">
                PDPE Region-Wide Heatmap v1.0
            </small>
        </div>
        </div>
        """
        map_obj.get_root().html.add_child(folium.Element(info_html))


def main():
    """Create region-wide heatmap demonstration."""
    print("🗺️  Creating Region-Wide Heatmap...")

    # Define Kansas City metropolitan area bounds
    kc_bounds = {"north": 39.2, "south": 38.9, "east": -94.3, "west": -94.8}

    # Initialize builder
    builder = RegionWideHeatmapBuilder()

    try:
        # Create region-wide heatmap
        output_file = builder.create_region_wide_heatmap(
            bounds=kc_bounds, output_path="region_wide_heatmap.html"
        )

        print(f"\n✅ Region-wide heatmap created successfully!")
        print(f"📁 File: {output_file}")
        print(f"📊 File size: {output_file.stat().st_size / 1024:.1f} KB")

        print(f"\n🎯 Key Features:")
        print(f"  ✓ Continuous spending propensity coverage across entire region")
        print(f"  ✓ College density influence radiating from universities")
        print(f"  ✓ Population density patterns from urban centers")
        print(f"  ✓ Realistic demographic-based calculations")
        print(f"  ✓ Comparison with traditional point-based event data")

        print(f"\n📋 Usage:")
        print(f"  1. Open {output_file.name} in your browser")
        print(f"  2. Toggle between heatmap layers using the control panel")
        print(f"  3. Compare region-wide coverage vs point-based event data")
        print(
            f"  4. Notice how spending propensity exists everywhere, not just at events"
        )

        return 0

    except Exception as e:
        print(f"❌ Error creating region-wide heatmap: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
