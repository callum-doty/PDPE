"""
Creates and manages spatial grid for geographic analysis.
"""

import math
from typing import List, Dict, Tuple
from config.constants import KC_BOUNDING_BOX, GRID_CELL_SIZE_M


class GridManager:
    """Manages spatial grid for geographic analysis."""

    def __init__(self, bounding_box: Dict = None, cell_size_m: int = None):
        """
        Initialize grid manager.

        Args:
            bounding_box: Dict with 'north', 'south', 'east', 'west' coordinates
            cell_size_m: Grid cell size in meters
        """
        self.bounding_box = bounding_box or KC_BOUNDING_BOX
        self.cell_size_m = cell_size_m or GRID_CELL_SIZE_M
        self.grid = {}
        self._create_grid()

    def _create_grid(self):
        """Create the spatial grid based on bounding box and cell size."""
        # Convert cell size from meters to degrees (approximate)
        # 1 degree latitude ≈ 111,000 meters
        # 1 degree longitude ≈ 111,000 * cos(latitude) meters

        lat_center = (self.bounding_box["north"] + self.bounding_box["south"]) / 2

        cell_size_lat = self.cell_size_m / 111000  # degrees
        cell_size_lon = self.cell_size_m / (
            111000 * math.cos(math.radians(lat_center))
        )  # degrees

        # Calculate grid dimensions
        lat_range = self.bounding_box["north"] - self.bounding_box["south"]
        lon_range = self.bounding_box["east"] - self.bounding_box["west"]

        rows = int(math.ceil(lat_range / cell_size_lat))
        cols = int(math.ceil(lon_range / cell_size_lon))

        # Create grid cells
        for row in range(rows):
            for col in range(cols):
                cell_south = self.bounding_box["south"] + (row * cell_size_lat)
                cell_north = cell_south + cell_size_lat
                cell_west = self.bounding_box["west"] + (col * cell_size_lon)
                cell_east = cell_west + cell_size_lon

                cell_id = f"{row}_{col}"
                self.grid[cell_id] = {
                    "id": cell_id,
                    "row": row,
                    "col": col,
                    "bounds": {
                        "north": cell_north,
                        "south": cell_south,
                        "east": cell_east,
                        "west": cell_west,
                    },
                    "center": {
                        "lat": (cell_north + cell_south) / 2,
                        "lon": (cell_east + cell_west) / 2,
                    },
                    "events": [],
                    "scores": {},
                }

    def get_cell_for_coordinates(self, lat: float, lon: float) -> str:
        """Get the grid cell ID for given coordinates."""
        if not self._is_within_bounds(lat, lon):
            return None

        lat_center = (self.bounding_box["north"] + self.bounding_box["south"]) / 2
        cell_size_lat = self.cell_size_m / 111000
        cell_size_lon = self.cell_size_m / (111000 * math.cos(math.radians(lat_center)))

        row = int((lat - self.bounding_box["south"]) / cell_size_lat)
        col = int((lon - self.bounding_box["west"]) / cell_size_lon)

        cell_id = f"{row}_{col}"
        return cell_id if cell_id in self.grid else None

    def _is_within_bounds(self, lat: float, lon: float) -> bool:
        """Check if coordinates are within the bounding box."""
        return (
            self.bounding_box["south"] <= lat <= self.bounding_box["north"]
            and self.bounding_box["west"] <= lon <= self.bounding_box["east"]
        )

    def add_event_to_grid(self, event, score_data: Dict = None):
        """Add an event to the appropriate grid cell."""
        cell_id = self.get_cell_for_coordinates(
            event.location.latitude, event.location.longitude
        )

        if cell_id and cell_id in self.grid:
            self.grid[cell_id]["events"].append(event)
            if score_data:
                self.grid[cell_id]["scores"][event.external_id] = score_data

    def get_cells_with_events(self, min_events: int = 1) -> List[Dict]:
        """Get all grid cells that have at least min_events events."""
        return [
            cell for cell in self.grid.values() if len(cell["events"]) >= min_events
        ]

    def get_cell_statistics(self, cell_id: str) -> Dict:
        """Get statistics for a specific grid cell."""
        if cell_id not in self.grid:
            return None

        cell = self.grid[cell_id]
        events = cell["events"]

        if not events:
            return {
                "cell_id": cell_id,
                "event_count": 0,
                "avg_score": 0,
                "max_score": 0,
                "venue_categories": {},
            }

        scores = [
            score_data.get("total_score", 0) for score_data in cell["scores"].values()
        ]

        venue_categories = {}
        for event in events:
            category = event.location.category
            venue_categories[category] = venue_categories.get(category, 0) + 1

        return {
            "cell_id": cell_id,
            "event_count": len(events),
            "avg_score": sum(scores) / len(scores) if scores else 0,
            "max_score": max(scores) if scores else 0,
            "venue_categories": venue_categories,
            "center": cell["center"],
        }

    def get_high_probability_cells(self, threshold: float = 0.7) -> List[Dict]:
        """Get cells with probability above threshold."""
        high_prob_cells = []

        for cell in self.grid.values():
            if not cell["events"]:
                continue

            stats = self.get_cell_statistics(cell["id"])
            # Normalize score to probability (simple approach)
            probability = min(1.0, stats["avg_score"] / 10.0)  # Assuming max score ~10

            if probability >= threshold:
                stats["probability"] = probability
                high_prob_cells.append(stats)

        return sorted(high_prob_cells, key=lambda x: x["probability"], reverse=True)

    def export_grid_data(self) -> Dict:
        """Export grid data for external use."""
        return {
            "bounding_box": self.bounding_box,
            "cell_size_m": self.cell_size_m,
            "grid_stats": {
                "total_cells": len(self.grid),
                "cells_with_events": len(self.get_cells_with_events()),
                "total_events": sum(len(cell["events"]) for cell in self.grid.values()),
            },
            "cells": [
                self.get_cell_statistics(cell_id)
                for cell_id in self.grid.keys()
                if self.grid[cell_id]["events"]  # Only export cells with events
            ],
        }
