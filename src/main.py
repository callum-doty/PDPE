"""
Main orchestration script for the whereabouts engine.
"""

import os
from pathlib import Path
from typing import List, Dict

# Import modules from the new structure
from config import settings
from src.models.database import (
    get_connection,
    init_db,
    upsert_location,
    insert_event,
    insert_weather,
    insert_score,
    query_top_scores,
    query_events_with_scores,
)
from src.data_acquisition.data_fetchers import (
    fetch_eventbrite_events,
    fetch_google_place_for_location,
    fetch_weather_for_datetime,
)
from src.processing.layer_builders import (
    score_event,
    build_demographic_layer,
    build_event_activity_layer,
    build_weather_layer,
)
from src.processing.grid_manager import GridManager
from src.processing.data_fusion import BayesianFusion, simple_weighted_fusion
from src.visualization.map_builder import InteractiveMapBuilder
from src.visualization.data_exporter import DataExporter


class WhereaboutsEngine:
    """Main engine for psychodemographic event analysis."""

    def __init__(self):
        """Initialize the whereabouts engine."""
        self.db_path = settings.DB_PATH
        self.grid_manager = GridManager()
        self.map_builder = InteractiveMapBuilder()
        self.data_exporter = DataExporter()
        self.fusion_engine = BayesianFusion()

        # Ensure database is initialized
        if not self.db_path.exists():
            init_db()

    def ingest_and_store_events(self, city: str = None) -> List[Dict]:
        """
        Ingest events from APIs and store in database with scores.

        Args:
            city: City name to search for events

        Returns:
            List of processed event data
        """
        city = city or settings.CITY_NAME
        conn = get_connection()

        print(f"Fetching events for {city}...")
        events = fetch_eventbrite_events(city)
        print(f"Fetched {len(events)} events.")

        processed_events = []

        for event in events:
            try:
                # Store location
                loc = event.location
                loc_id = upsert_location(
                    conn,
                    loc.name,
                    loc.address,
                    loc.latitude,
                    loc.longitude,
                    loc.category,
                    loc.base_score,
                )

                # Store event
                tags_csv = ",".join(event.tags)
                event_id = insert_event(
                    conn,
                    event.source,
                    event.external_id,
                    event.name,
                    event.description,
                    event.start_time,
                    event.end_time,
                    loc_id,
                    event.category,
                    tags_csv,
                )

                # Fetch weather for event time
                weather = fetch_weather_for_datetime(
                    event.start_time, loc.latitude, loc.longitude
                )
                insert_weather(
                    conn,
                    event.start_time,
                    loc_id,
                    weather["condition"],
                    weather["temperature"],
                    weather["precipitation"],
                )

                # Enrich with Google Places data
                google_data = fetch_google_place_for_location(loc.name, loc.address)

                # Calculate scores
                score_data = score_event(event, weather, google_data)
                insert_score(
                    conn,
                    event_id,
                    loc_id,
                    score_data["demographic_score"],
                    score_data["event_score"],
                    score_data["weather_score"],
                    score_data["total_score"],
                )

                # Add to grid for spatial analysis
                self.grid_manager.add_event_to_grid(event, score_data)

                # Store processed event data
                event_data = {
                    "event_name": event.name,
                    "location_name": loc.name,
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "total_score": score_data["total_score"],
                    "demographic_score": score_data["demographic_score"],
                    "event_score": score_data["event_score"],
                    "weather_score": score_data["weather_score"],
                    "start_time": event.start_time,
                    "category": event.category,
                    "venue_category": loc.category,
                }
                processed_events.append(event_data)

                print(
                    f"Processed '{event.name}' with total score {score_data['total_score']}"
                )

            except Exception as e:
                print(f"Error processing event {event.name}: {e}")
                continue

        conn.close()
        return processed_events

    def analyze_spatial_patterns(self, events_data: List[Dict]) -> Dict:
        """
        Analyze spatial patterns using grid-based analysis and layer fusion.

        Args:
            events_data: List of processed event data

        Returns:
            Dictionary containing analysis results
        """
        print("Analyzing spatial patterns...")

        # Build probability layers
        demographic_layer = build_demographic_layer([])  # Would need Event objects
        activity_layer = build_event_activity_layer([])  # Would need Event objects
        weather_layer = build_weather_layer(
            [], {}
        )  # Would need Event objects and weather data

        # For now, create simplified layers from events_data
        coord_scores = {}
        for event in events_data:
            coord = (event["latitude"], event["longitude"])
            if coord not in coord_scores:
                coord_scores[coord] = []
            coord_scores[coord].append(event["total_score"])

        # Average scores by coordinate
        simplified_layer = {
            coord: sum(scores) / len(scores) for coord, scores in coord_scores.items()
        }

        # Fuse layers using Bayesian fusion
        layers = {"combined": simplified_layer}
        fused_probabilities = self.fusion_engine.fuse_layers(layers)

        # Get high-confidence areas
        high_confidence_areas = self.fusion_engine.get_high_confidence_areas(
            fused_probabilities
        )

        # Export grid data
        grid_data = self.grid_manager.export_grid_data()

        return {
            "fused_probabilities": fused_probabilities,
            "high_confidence_areas": high_confidence_areas,
            "grid_data": grid_data,
            "statistics": self.fusion_engine.calculate_area_statistics(
                fused_probabilities
            ),
        }

    def generate_visualizations(
        self, events_data: List[Dict], analysis_results: Dict = None
    ) -> Dict[str, Path]:
        """
        Generate various visualizations of the analysis results.

        Args:
            events_data: List of processed event data
            analysis_results: Results from spatial analysis

        Returns:
            Dictionary mapping visualization types to file paths
        """
        print("Generating visualizations...")

        visualizations = {}

        # Event heatmap
        event_heatmap = self.map_builder.create_event_heatmap(
            events_data, "data/exports/event_heatmap.html"
        )
        if event_heatmap:
            visualizations["event_heatmap"] = event_heatmap

        # Probability heatmap if analysis results available
        if analysis_results and analysis_results.get("fused_probabilities"):
            prob_heatmap = self.map_builder.create_probability_heatmap(
                analysis_results["fused_probabilities"],
                "data/exports/probability_heatmap.html",
            )
            if prob_heatmap:
                visualizations["probability_heatmap"] = prob_heatmap

        # Grid visualization
        if analysis_results and analysis_results.get("grid_data"):
            grid_viz = self.map_builder.create_grid_visualization(
                analysis_results["grid_data"],
                "data/exports/grid_visualization.html",
            )
            if grid_viz:
                visualizations["grid_visualization"] = grid_viz

        # Combined visualization
        combined_viz = self.map_builder.create_combined_visualization(
            events_data,
            analysis_results.get("fused_probabilities") if analysis_results else None,
            analysis_results.get("grid_data") if analysis_results else None,
            "data/exports/combined_visualization.html",
        )
        if combined_viz:
            visualizations["combined_visualization"] = combined_viz

        return visualizations

    def export_analysis_data(
        self, events_data: List[Dict], analysis_results: Dict = None
    ) -> Dict[str, Path]:
        """
        Export analysis data in various formats.

        Args:
            events_data: List of processed event data
            analysis_results: Results from spatial analysis

        Returns:
            Dictionary mapping export formats to file paths
        """
        print("Exporting analysis data...")

        return self.data_exporter.export_all_formats(
            events_data,
            analysis_results.get("fused_probabilities") if analysis_results else None,
            analysis_results.get("grid_data") if analysis_results else None,
            "whereabouts_analysis",
        )

    def run_full_analysis(self, city: str = None) -> Dict:
        """
        Run the complete whereabouts analysis pipeline.

        Args:
            city: City name to analyze

        Returns:
            Dictionary containing all analysis results and file paths
        """
        print("Starting full whereabouts analysis...")

        # Step 1: Ingest and store events
        events_data = self.ingest_and_store_events(city)

        if not events_data:
            print("No events data available for analysis.")
            return {"error": "No events data available"}

        # Step 2: Analyze spatial patterns
        analysis_results = self.analyze_spatial_patterns(events_data)

        # Step 3: Generate visualizations
        visualizations = self.generate_visualizations(events_data, analysis_results)

        # Step 4: Export data
        exports = self.export_analysis_data(events_data, analysis_results)

        # Step 5: Show top results
        conn = get_connection()
        top_events = query_top_scores(conn, limit=10)
        conn.close()

        print(
            f"\nAnalysis complete! Generated {len(visualizations)} visualizations and {len(exports)} export files."
        )
        print(f"Top {len(top_events)} events:")
        for event in top_events[:5]:  # Show top 5
            print(
                f"  {event['event_name']} @ {event['location_name']} | Score: {event['total_score']}"
            )

        return {
            "events_data": events_data,
            "analysis_results": analysis_results,
            "visualizations": visualizations,
            "exports": exports,
            "top_events": top_events,
        }


def main():
    """Main entry point for the whereabouts engine."""
    engine = WhereaboutsEngine()
    results = engine.run_full_analysis()

    if "error" not in results:
        print(f"\n🎉 Analysis completed successfully!")
        print(f"📊 Processed {len(results['events_data'])} events")
        print(f"🗺️  Generated {len(results['visualizations'])} visualizations")
        print(f"📁 Exported {len(results['exports'])} data files")

        # Open the main visualization
        if "combined_visualization" in results["visualizations"]:
            engine.map_builder.open_in_browser(
                results["visualizations"]["combined_visualization"]
            )


if __name__ == "__main__":
    main()
