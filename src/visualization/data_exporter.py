"""
Exports data for external use in various formats.
"""

import json
import csv
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd


class DataExporter:
    """Handles exporting data in various formats."""

    def __init__(self, output_dir: str = "data/exports"):
        """
        Initialize data exporter.

        Args:
            output_dir: Directory to save exported files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_events_to_csv(
        self, events_data: List[Dict], filename: str = "events.csv"
    ) -> Path:
        """
        Export events data to CSV format.

        Args:
            events_data: List of event dictionaries
            filename: Output filename

        Returns:
            Path to the exported CSV file
        """
        if not events_data:
            print("No events data to export.")
            return None

        output_path = self.output_dir / filename

        # Flatten nested data for CSV export
        flattened_data = []
        for event in events_data:
            flat_event = {
                "event_name": event.get("event_name", ""),
                "location_name": event.get("location_name", ""),
                "latitude": event.get("latitude", 0),
                "longitude": event.get("longitude", 0),
                "total_score": event.get("total_score", 0),
                "demographic_score": event.get("demographic_score", 0),
                "event_score": event.get("event_score", 0),
                "weather_score": event.get("weather_score", 0),
                "start_time": event.get("start_time", ""),
                "category": event.get("category", ""),
                "venue_category": event.get("venue_category", ""),
            }
            flattened_data.append(flat_event)

        # Write to CSV
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            if flattened_data:
                fieldnames = flattened_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)

        print(f"Events data exported to {output_path}")
        return output_path

    def export_probability_map_to_json(
        self, probability_data: Dict, filename: str = "probability_map.json"
    ) -> Path:
        """
        Export probability map to JSON format.

        Args:
            probability_data: Dictionary mapping coordinates to probabilities
            filename: Output filename

        Returns:
            Path to the exported JSON file
        """
        if not probability_data:
            print("No probability data to export.")
            return None

        output_path = self.output_dir / filename

        # Convert coordinate tuples to strings for JSON serialization
        json_data = {
            "probability_map": {
                f"{coord[0]},{coord[1]}": prob
                for coord, prob in probability_data.items()
            },
            "metadata": {
                "total_areas": len(probability_data),
                "max_probability": (
                    max(probability_data.values()) if probability_data else 0
                ),
                "min_probability": (
                    min(probability_data.values()) if probability_data else 0
                ),
                "mean_probability": (
                    sum(probability_data.values()) / len(probability_data)
                    if probability_data
                    else 0
                ),
            },
        }

        with open(output_path, "w", encoding="utf-8") as jsonfile:
            json.dump(json_data, jsonfile, indent=2)

        print(f"Probability map exported to {output_path}")
        return output_path

    def export_grid_data_to_json(
        self, grid_data: Dict, filename: str = "grid_analysis.json"
    ) -> Path:
        """
        Export grid analysis data to JSON format.

        Args:
            grid_data: Grid data from GridManager.export_grid_data()
            filename: Output filename

        Returns:
            Path to the exported JSON file
        """
        if not grid_data:
            print("No grid data to export.")
            return None

        output_path = self.output_dir / filename

        with open(output_path, "w", encoding="utf-8") as jsonfile:
            json.dump(grid_data, jsonfile, indent=2)

        print(f"Grid analysis data exported to {output_path}")
        return output_path

    def export_to_geojson(
        self, events_data: List[Dict], filename: str = "events.geojson"
    ) -> Path:
        """
        Export events data to GeoJSON format for GIS applications.

        Args:
            events_data: List of event dictionaries
            filename: Output filename

        Returns:
            Path to the exported GeoJSON file
        """
        if not events_data:
            print("No events data to export.")
            return None

        output_path = self.output_dir / filename

        # Create GeoJSON structure
        geojson_data = {"type": "FeatureCollection", "features": []}

        for event in events_data:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        event.get("longitude", 0),
                        event.get("latitude", 0),
                    ],
                },
                "properties": {
                    "event_name": event.get("event_name", ""),
                    "location_name": event.get("location_name", ""),
                    "total_score": event.get("total_score", 0),
                    "demographic_score": event.get("demographic_score", 0),
                    "event_score": event.get("event_score", 0),
                    "weather_score": event.get("weather_score", 0),
                    "start_time": event.get("start_time", ""),
                    "category": event.get("category", ""),
                    "venue_category": event.get("venue_category", ""),
                },
            }
            geojson_data["features"].append(feature)

        with open(output_path, "w", encoding="utf-8") as geojsonfile:
            json.dump(geojson_data, geojsonfile, indent=2)

        print(f"Events data exported to GeoJSON: {output_path}")
        return output_path

    def export_summary_report(
        self,
        events_data: List[Dict],
        probability_data: Dict = None,
        grid_data: Dict = None,
        filename: str = "analysis_report.json",
    ) -> Path:
        """
        Export a comprehensive summary report.

        Args:
            events_data: List of event dictionaries
            probability_data: Probability map data
            grid_data: Grid analysis data
            filename: Output filename

        Returns:
            Path to the exported report file
        """
        output_path = self.output_dir / filename

        # Calculate summary statistics
        report = {
            "analysis_summary": {
                "total_events": len(events_data) if events_data else 0,
                "date_generated": None,  # Could add datetime.now().isoformat()
            },
            "event_statistics": {},
            "probability_statistics": {},
            "grid_statistics": {},
        }

        # Event statistics
        if events_data:
            scores = [event.get("total_score", 0) for event in events_data]
            categories = {}
            venue_types = {}

            for event in events_data:
                cat = event.get("category", "unknown")
                venue_cat = event.get("venue_category", "unknown")
                categories[cat] = categories.get(cat, 0) + 1
                venue_types[venue_cat] = venue_types.get(venue_cat, 0) + 1

            report["event_statistics"] = {
                "total_events": len(events_data),
                "average_score": sum(scores) / len(scores) if scores else 0,
                "max_score": max(scores) if scores else 0,
                "min_score": min(scores) if scores else 0,
                "categories": categories,
                "venue_types": venue_types,
                "high_scoring_events": len([s for s in scores if s >= 8]),
                "medium_scoring_events": len([s for s in scores if 5 <= s < 8]),
                "low_scoring_events": len([s for s in scores if s < 5]),
            }

        # Probability statistics
        if probability_data:
            probs = list(probability_data.values())
            report["probability_statistics"] = {
                "total_areas": len(probability_data),
                "average_probability": sum(probs) / len(probs) if probs else 0,
                "max_probability": max(probs) if probs else 0,
                "min_probability": min(probs) if probs else 0,
                "high_confidence_areas": len([p for p in probs if p >= 0.7]),
                "medium_confidence_areas": len([p for p in probs if 0.4 <= p < 0.7]),
                "low_confidence_areas": len([p for p in probs if p < 0.4]),
            }

        # Grid statistics
        if grid_data:
            report["grid_statistics"] = grid_data.get("grid_stats", {})

        with open(output_path, "w", encoding="utf-8") as reportfile:
            json.dump(report, reportfile, indent=2)

        print(f"Analysis report exported to {output_path}")
        return output_path

    def export_to_pandas_parquet(
        self, events_data: List[Dict], filename: str = "events.parquet"
    ) -> Path:
        """
        Export events data to Parquet format using pandas.

        Args:
            events_data: List of event dictionaries
            filename: Output filename

        Returns:
            Path to the exported Parquet file
        """
        if not events_data:
            print("No events data to export.")
            return None

        try:
            output_path = self.output_dir / filename

            # Convert to DataFrame
            df = pd.DataFrame(events_data)

            # Save as Parquet
            df.to_parquet(output_path, index=False)

            print(f"Events data exported to Parquet: {output_path}")
            return output_path

        except ImportError:
            print("Pandas not available. Cannot export to Parquet format.")
            return None
        except Exception as e:
            print(f"Error exporting to Parquet: {e}")
            return None

    def export_all_formats(
        self,
        events_data: List[Dict],
        probability_data: Dict = None,
        grid_data: Dict = None,
        base_filename: str = "whereabouts_analysis",
    ) -> Dict[str, Path]:
        """
        Export data in all available formats.

        Args:
            events_data: List of event dictionaries
            probability_data: Probability map data
            grid_data: Grid analysis data
            base_filename: Base filename for all exports

        Returns:
            Dictionary mapping format names to file paths
        """
        exported_files = {}

        # Export events in various formats
        if events_data:
            exported_files["csv"] = self.export_events_to_csv(
                events_data, f"{base_filename}_events.csv"
            )
            exported_files["geojson"] = self.export_to_geojson(
                events_data, f"{base_filename}_events.geojson"
            )
            exported_files["parquet"] = self.export_to_pandas_parquet(
                events_data, f"{base_filename}_events.parquet"
            )

        # Export probability data
        if probability_data:
            exported_files["probability_json"] = self.export_probability_map_to_json(
                probability_data, f"{base_filename}_probabilities.json"
            )

        # Export grid data
        if grid_data:
            exported_files["grid_json"] = self.export_grid_data_to_json(
                grid_data, f"{base_filename}_grid.json"
            )

        # Export summary report
        exported_files["report"] = self.export_summary_report(
            events_data, probability_data, grid_data, f"{base_filename}_report.json"
        )

        print(
            f"Data exported in {len([f for f in exported_files.values() if f])} formats"
        )
        return {k: v for k, v in exported_files.items() if v is not None}
