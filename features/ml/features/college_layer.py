"""
College population density layer for psychographic prediction.
Calculates influence scores based on proximity to universities and colleges.
"""

import math
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple
from dataclasses import dataclass
from config.constants import CUSTOM_LAYERS
from config.settings import settings


@dataclass
class University:
    """University data structure."""

    name: str
    lat: float
    lng: float
    weight: float
    enrollment: int = 10000  # Default enrollment estimate


class CollegeLayer:
    """
    Generates college population density scores for geographic locations.
    Higher scores indicate areas with higher concentration of college-aged individuals.
    """

    def __init__(self):
        self.universities = self._load_universities()
        self.influence_radius_km = CUSTOM_LAYERS["college_layer"]["influence_radius_km"]
        self.decay_rate = CUSTOM_LAYERS["college_layer"]["decay_rate"]

    def _load_universities(self) -> List[University]:
        """Load university data from configuration."""
        universities = []
        for uni_data in CUSTOM_LAYERS["college_layer"]["universities"]:
            universities.append(
                University(
                    name=uni_data["name"],
                    lat=uni_data["lat"],
                    lng=uni_data["lng"],
                    weight=uni_data["weight"],
                )
            )
        return universities

    def _haversine_distance(
        self, lat1: float, lng1: float, lat2: float, lng2: float
    ) -> float:
        """
        Calculate the great circle distance between two points on Earth.
        Returns distance in kilometers.
        """
        # Convert latitude and longitude from degrees to radians
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])

        # Haversine formula
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Radius of Earth in kilometers
        r = 6371
        return c * r

    def _calculate_influence_score(
        self, lat: float, lng: float, university: University
    ) -> float:
        """
        Calculate the influence score of a university on a given location.
        Uses exponential decay based on distance.
        """
        distance_km = self._haversine_distance(lat, lng, university.lat, university.lng)

        # If beyond influence radius, no influence
        if distance_km > self.influence_radius_km:
            return 0.0

        # Exponential decay function
        # Score = weight * exp(-decay_rate * distance)
        influence = university.weight * math.exp(-self.decay_rate * distance_km)

        return influence

    def calculate_college_density_score(
        self, lat: float, lng: float
    ) -> Dict[str, float]:
        """
        Calculate college density score for a given location.

        Args:
            lat: Latitude of the location
            lng: Longitude of the location

        Returns:
            Dictionary with score and metadata
        """
        total_influence = 0.0
        university_influences = {}

        for university in self.universities:
            influence = self._calculate_influence_score(lat, lng, university)
            total_influence += influence

            if influence > 0:
                university_influences[university.name] = {
                    "influence": influence,
                    "distance_km": self._haversine_distance(
                        lat, lng, university.lat, university.lng
                    ),
                }

        # Normalize score to 0-1 range
        # Maximum possible score would be sum of all university weights
        max_possible_score = sum(uni.weight for uni in self.universities)
        normalized_score = min(total_influence / max_possible_score, 1.0)

        return {
            "score": normalized_score,
            "total_influence": total_influence,
            "university_influences": university_influences,
            "confidence": self._calculate_confidence(university_influences),
        }

    def _calculate_confidence(self, university_influences: Dict) -> float:
        """
        Calculate confidence in the college density score.
        Higher confidence when multiple universities contribute.
        """
        if not university_influences:
            return 0.0

        # Base confidence on number of contributing universities
        num_universities = len(university_influences)
        base_confidence = min(num_universities / len(self.universities), 1.0)

        # Boost confidence if there's a strong primary influence
        max_influence = max(
            data["influence"] for data in university_influences.values()
        )
        influence_boost = min(max_influence, 0.3)  # Cap at 0.3

        return min(base_confidence + influence_boost, 1.0)

    def generate_grid_scores(
        self, grid_bounds: Dict[str, float], resolution_meters: int = 500
    ) -> pd.DataFrame:
        """
        Generate college density scores for a grid of locations.

        Args:
            grid_bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries
            resolution_meters: Grid cell size in meters

        Returns:
            DataFrame with lat, lng, score, and metadata columns
        """
        # Convert resolution from meters to degrees (approximate)
        lat_step = resolution_meters / 111000  # ~111km per degree latitude
        lng_step = resolution_meters / (
            111000
            * math.cos(math.radians((grid_bounds["north"] + grid_bounds["south"]) / 2))
        )

        grid_data = []

        lat = grid_bounds["south"]
        while lat <= grid_bounds["north"]:
            lng = grid_bounds["west"]
            while lng <= grid_bounds["east"]:
                result = self.calculate_college_density_score(lat, lng)

                grid_data.append(
                    {
                        "lat": lat,
                        "lng": lng,
                        "grid_cell_id": f"{lat:.6f}_{lng:.6f}",
                        "score": result["score"],
                        "confidence": result["confidence"],
                        "total_influence": result["total_influence"],
                        "university_count": len(result["university_influences"]),
                        "metadata": result,
                    }
                )

                lng += lng_step
            lat += lat_step

        return pd.DataFrame(grid_data)

    def get_nearby_universities(
        self, lat: float, lng: float, radius_km: float = 10.0
    ) -> List[Dict]:
        """
        Get universities within a specified radius of a location.

        Args:
            lat: Latitude of the location
            lng: Longitude of the location
            radius_km: Search radius in kilometers

        Returns:
            List of nearby universities with distance information
        """
        nearby = []

        for university in self.universities:
            distance = self._haversine_distance(
                lat, lng, university.lat, university.lng
            )

            if distance <= radius_km:
                nearby.append(
                    {
                        "name": university.name,
                        "lat": university.lat,
                        "lng": university.lng,
                        "weight": university.weight,
                        "distance_km": distance,
                        "influence": self._calculate_influence_score(
                            lat, lng, university
                        ),
                    }
                )

        # Sort by distance
        nearby.sort(key=lambda x: x["distance_km"])
        return nearby

    def analyze_location(self, lat: float, lng: float) -> Dict:
        """
        Comprehensive analysis of college influence for a location.

        Args:
            lat: Latitude of the location
            lng: Longitude of the location

        Returns:
            Detailed analysis dictionary
        """
        score_data = self.calculate_college_density_score(lat, lng)
        nearby_unis = self.get_nearby_universities(lat, lng)

        return {
            "location": {"lat": lat, "lng": lng},
            "college_density_score": score_data["score"],
            "confidence": score_data["confidence"],
            "nearby_universities": nearby_unis,
            "analysis": {
                "primary_influence": (
                    max(
                        score_data["university_influences"].items(),
                        key=lambda x: x[1]["influence"],
                    )
                    if score_data["university_influences"]
                    else None
                ),
                "total_universities_in_range": len(score_data["university_influences"]),
                "average_distance_to_universities": (
                    np.mean(
                        [
                            data["distance_km"]
                            for data in score_data["university_influences"].values()
                        ]
                    )
                    if score_data["university_influences"]
                    else None
                ),
            },
        }


def calculate_college_scores_for_venues(venue_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate college density scores for a DataFrame of venues.

    Args:
        venue_data: DataFrame with 'lat' and 'lng' columns

    Returns:
        DataFrame with added college score columns
    """
    college_layer = CollegeLayer()

    scores = []
    confidences = []

    for _, row in venue_data.iterrows():
        result = college_layer.calculate_college_density_score(row["lat"], row["lng"])
        scores.append(result["score"])
        confidences.append(result["confidence"])

    venue_data = venue_data.copy()
    venue_data["college_layer_score"] = scores
    venue_data["college_layer_confidence"] = confidences

    return venue_data


if __name__ == "__main__":
    # Example usage and testing
    college_layer = CollegeLayer()

    # Test with a location near UMKC
    test_lat, test_lng = 39.0347, -94.5783

    print("College Layer Analysis")
    print("=" * 50)

    analysis = college_layer.analyze_location(test_lat, test_lng)
    print(f"Location: {analysis['location']}")
    print(f"College Density Score: {analysis['college_density_score']:.3f}")
    print(f"Confidence: {analysis['confidence']:.3f}")
    print(
        f"Universities in range: {analysis['analysis']['total_universities_in_range']}"
    )

    if analysis["analysis"]["primary_influence"]:
        primary = analysis["analysis"]["primary_influence"]
        print(
            f"Primary influence: {primary[0]} (influence: {primary[1]['influence']:.3f})"
        )

    print("\nNearby Universities:")
    for uni in analysis["nearby_universities"][:3]:  # Top 3
        print(
            f"  {uni['name']}: {uni['distance_km']:.2f}km, influence: {uni['influence']:.3f}"
        )
