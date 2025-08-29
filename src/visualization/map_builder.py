"""
Creates interactive maps for visualizing probability data.
"""

import folium
from typing import List, Dict, Tuple
import webbrowser
from pathlib import Path
import json


class InteractiveMapBuilder:
    """Builds interactive maps using Folium."""

    def __init__(self, center_coords: Tuple[float, float] = (39.0997, -94.5786)):
        """
        Initialize map builder.

        Args:
            center_coords: (latitude, longitude) for map center
        """
        self.center_coords = center_coords

    def create_event_heatmap(
        self, events_data: List[Dict], output_path: str = "event_heatmap.html"
    ) -> Path:
        """
        Create a heatmap showing event locations and scores.

        Args:
            events_data: List of dicts with keys: event_name, location_name, latitude, longitude, total_score
            output_path: Output HTML file path

        Returns:
            Path to the generated HTML file
        """
        if not events_data:
            print("No events data to plot.")
            return None

        # Calculate center from data if not provided
        if len(events_data) > 0:
            avg_lat = sum(event["latitude"] for event in events_data) / len(events_data)
            avg_lon = sum(event["longitude"] for event in events_data) / len(
                events_data
            )
            center = [avg_lat, avg_lon]
        else:
            center = list(self.center_coords)

        # Create base map
        m = folium.Map(location=center, zoom_start=13)

        # Add event markers
        for event in events_data:
            # Scale marker size based on score
            radius = max(5, min(20, event["total_score"] * 2))

            # Color based on score
            if event["total_score"] >= 8:
                color = "red"
                fill_color = "red"
            elif event["total_score"] >= 5:
                color = "orange"
                fill_color = "orange"
            elif event["total_score"] >= 2:
                color = "yellow"
                fill_color = "yellow"
            else:
                color = "blue"
                fill_color = "lightblue"

            folium.CircleMarker(
                location=(event["latitude"], event["longitude"]),
                radius=radius,
                popup=f"{event.get('event_name', 'Event')} @ {event.get('location_name', 'Location')} (score: {event['total_score']})",
                tooltip=f"Score: {event['total_score']}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.7,
                weight=2,
            ).add_to(m)

        # Save map
        output_file = Path(output_path).resolve()
        m.save(str(output_file))
        print(f"Event heatmap saved to {output_file}")

        return output_file

    def create_probability_heatmap(
        self, probability_data: Dict, output_path: str = "probability_heatmap.html"
    ) -> Path:
        """
        Create a heatmap showing probability distribution.

        Args:
            probability_data: Dict mapping (lat, lon) tuples to probability values
            output_path: Output HTML file path

        Returns:
            Path to the generated HTML file
        """
        if not probability_data:
            print("No probability data to plot.")
            return None

        # Calculate center from data
        coords = list(probability_data.keys())
        avg_lat = sum(coord[0] for coord in coords) / len(coords)
        avg_lon = sum(coord[1] for coord in coords) / len(coords)
        center = [avg_lat, avg_lon]

        # Create base map
        m = folium.Map(location=center, zoom_start=12)

        # Prepare data for heatmap
        heat_data = []
        max_prob = max(probability_data.values())

        for (lat, lon), probability in probability_data.items():
            # Normalize probability for heatmap intensity
            intensity = probability / max_prob if max_prob > 0 else 0
            heat_data.append([lat, lon, intensity])

        # Add heatmap layer
        from folium.plugins import HeatMap

        HeatMap(heat_data, radius=15, blur=10, max_zoom=1).add_to(m)

        # Add markers for high-probability areas
        high_prob_threshold = max_prob * 0.7 if max_prob > 0 else 0.7
        for (lat, lon), probability in probability_data.items():
            if probability >= high_prob_threshold:
                folium.CircleMarker(
                    location=(lat, lon),
                    radius=8,
                    popup=f"High Probability Area<br>Probability: {probability:.3f}",
                    tooltip=f"P: {probability:.3f}",
                    color="red",
                    fill=True,
                    fillColor="red",
                    fillOpacity=0.8,
                    weight=2,
                ).add_to(m)

        # Save map
        output_file = Path(output_path).resolve()
        m.save(str(output_file))
        print(f"Probability heatmap saved to {output_file}")

        return output_file

    def create_grid_visualization(
        self, grid_data: Dict, output_path: str = "grid_visualization.html"
    ) -> Path:
        """
        Create a visualization of the spatial grid with statistics.

        Args:
            grid_data: Grid data from GridManager.export_grid_data()
            output_path: Output HTML file path

        Returns:
            Path to the generated HTML file
        """
        if not grid_data or not grid_data.get("cells"):
            print("No grid data to visualize.")
            return None

        # Calculate center from bounding box
        bbox = grid_data["bounding_box"]
        center = [
            (bbox["north"] + bbox["south"]) / 2,
            (bbox["east"] + bbox["west"]) / 2,
        ]

        # Create base map
        m = folium.Map(location=center, zoom_start=11)

        # Add grid cells
        max_score = max(
            cell["max_score"] for cell in grid_data["cells"] if cell["max_score"] > 0
        )

        for cell in grid_data["cells"]:
            if cell["event_count"] == 0:
                continue

            center_lat = cell["center"]["lat"]
            center_lon = cell["center"]["lon"]

            # Color and size based on average score
            avg_score = cell["avg_score"]
            if avg_score >= 8:
                color = "red"
                fill_color = "red"
            elif avg_score >= 5:
                color = "orange"
                fill_color = "orange"
            elif avg_score >= 2:
                color = "yellow"
                fill_color = "yellow"
            else:
                color = "blue"
                fill_color = "lightblue"

            # Radius based on event count
            radius = min(50, max(10, cell["event_count"] * 5))

            # Create popup with cell statistics
            popup_text = f"""
            <b>Grid Cell: {cell['cell_id']}</b><br>
            Events: {cell['event_count']}<br>
            Avg Score: {avg_score:.2f}<br>
            Max Score: {cell['max_score']}<br>
            Venue Types: {', '.join(cell['venue_categories'].keys())}
            """

            folium.CircleMarker(
                location=(center_lat, center_lon),
                radius=radius,
                popup=popup_text,
                tooltip=f"Cell {cell['cell_id']}: {cell['event_count']} events, avg score {avg_score:.1f}",
                color=color,
                fill=True,
                fillColor=fill_color,
                fillOpacity=0.6,
                weight=2,
            ).add_to(m)

        # Add legend
        legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 150px; height: 90px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><b>Grid Visualization</b></p>
        <p><i class="fa fa-circle" style="color:red"></i> High Score (8+)</p>
        <p><i class="fa fa-circle" style="color:orange"></i> Medium Score (5-8)</p>
        <p><i class="fa fa-circle" style="color:yellow"></i> Low Score (2-5)</p>
        <p><i class="fa fa-circle" style="color:blue"></i> Very Low Score (<2)</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

        # Save map
        output_file = Path(output_path).resolve()
        m.save(str(output_file))
        print(f"Grid visualization saved to {output_file}")

        return output_file

    def create_combined_visualization(
        self,
        events_data: List[Dict],
        probability_data: Dict = None,
        grid_data: Dict = None,
        output_path: str = "combined_visualization.html",
    ) -> Path:
        """
        Create a comprehensive visualization combining events, probabilities, and grid data.

        Args:
            events_data: Event data for markers
            probability_data: Probability data for heatmap layer
            grid_data: Grid data for grid overlay
            output_path: Output HTML file path

        Returns:
            Path to the generated HTML file
        """
        if not events_data and not probability_data and not grid_data:
            print("No data provided for visualization.")
            return None

        # Calculate center
        if events_data:
            avg_lat = sum(event["latitude"] for event in events_data) / len(events_data)
            avg_lon = sum(event["longitude"] for event in events_data) / len(
                events_data
            )
            center = [avg_lat, avg_lon]
        elif probability_data:
            coords = list(probability_data.keys())
            avg_lat = sum(coord[0] for coord in coords) / len(coords)
            avg_lon = sum(coord[1] for coord in coords) / len(coords)
            center = [avg_lat, avg_lon]
        else:
            center = list(self.center_coords)

        # Create base map with layer control
        m = folium.Map(location=center, zoom_start=12)

        # Add probability heatmap layer if provided
        if probability_data:
            heat_data = []
            max_prob = max(probability_data.values())
            for (lat, lon), probability in probability_data.items():
                intensity = probability / max_prob if max_prob > 0 else 0
                heat_data.append([lat, lon, intensity])

            from folium.plugins import HeatMap

            heatmap_layer = HeatMap(
                heat_data, radius=15, blur=10, max_zoom=1, name="Probability Heatmap"
            )
            heatmap_layer.add_to(m)

        # Add event markers layer if provided
        if events_data:
            event_layer = folium.FeatureGroup(name="Events")
            for event in events_data:
                radius = max(5, min(20, event["total_score"] * 2))

                if event["total_score"] >= 8:
                    color = "red"
                elif event["total_score"] >= 5:
                    color = "orange"
                elif event["total_score"] >= 2:
                    color = "yellow"
                else:
                    color = "blue"

                folium.CircleMarker(
                    location=(event["latitude"], event["longitude"]),
                    radius=radius,
                    popup=f"{event.get('event_name', 'Event')} @ {event.get('location_name', 'Location')} (score: {event['total_score']})",
                    tooltip=f"Score: {event['total_score']}",
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.7,
                    weight=2,
                ).add_to(event_layer)

            event_layer.add_to(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Save map
        output_file = Path(output_path).resolve()
        m.save(str(output_file))
        print(f"Combined visualization saved to {output_file}")

        return output_file

    def open_in_browser(self, file_path: Path):
        """Open the generated map in the default web browser."""
        try:
            webbrowser.open(str(file_path.as_uri()))
        except Exception as e:
            print(f"Could not open browser: {e}")


def generate_heatmap(rows: List[Dict], output_html: str = "heatmap.html") -> Path:
    """
    Legacy function for backward compatibility.
    Generate a simple heatmap from event data.
    """
    builder = InteractiveMapBuilder()
    return builder.create_event_heatmap(rows, output_html)
