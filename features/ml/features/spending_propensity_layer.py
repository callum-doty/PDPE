"""
Spending propensity layer for psychographic prediction.
Calculates spending potential scores based on demographic and economic indicators.
"""

import math
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from config.constants import CUSTOM_LAYERS
from config.settings import settings


@dataclass
class DemographicProfile:
    """Demographic profile for spending propensity calculation."""

    median_income: float
    education_bachelors_pct: float
    education_graduate_pct: float
    age_25_34_pct: float
    age_35_44_pct: float
    professional_occupation_pct: float
    management_occupation_pct: float
    population_density: float


class SpendingPropensityLayer:
    """
    Generates spending propensity scores for geographic locations.
    Higher scores indicate areas with higher likelihood of discretionary spending.
    """

    def __init__(self):
        self.config = CUSTOM_LAYERS["spending_propensity"]
        self.high_income_multiplier = self.config["high_income_multiplier"]
        self.education_multiplier = self.config["education_multiplier"]
        self.age_optimal_range = self.config["age_optimal_range"]
        self.professional_bonus = self.config["professional_occupation_bonus"]

    def _normalize_income_score(
        self, median_income: float, area_median: float = 65000
    ) -> float:
        """
        Normalize income to a 0-1 score with higher incomes getting higher scores.

        Args:
            median_income: Median income for the area
            area_median: Reference median income for the region

        Returns:
            Normalized income score (0-1)
        """
        if median_income <= 0:
            return 0.0

        # Calculate income ratio relative to area median
        income_ratio = median_income / area_median

        # Apply sigmoid function to normalize to 0-1 range
        # Higher incomes get exponentially higher scores
        normalized = 1 / (1 + math.exp(-2 * (income_ratio - 1)))

        return min(normalized, 1.0)

    def _calculate_education_score(
        self, bachelors_pct: float, graduate_pct: float
    ) -> float:
        """
        Calculate education score based on bachelor's and graduate degree percentages.

        Args:
            bachelors_pct: Percentage with bachelor's degree
            graduate_pct: Percentage with graduate degree

        Returns:
            Education score (0-1)
        """
        # Weight graduate degrees more heavily
        education_score = (bachelors_pct * 0.6 + graduate_pct * 1.0) / 100.0

        # Apply education multiplier
        education_score *= self.education_multiplier

        return min(education_score, 1.0)

    def _calculate_age_score(self, age_25_34_pct: float, age_35_44_pct: float) -> float:
        """
        Calculate age score based on optimal spending age ranges.

        Args:
            age_25_34_pct: Percentage aged 25-34
            age_35_44_pct: Percentage aged 35-44

        Returns:
            Age score (0-1)
        """
        # Peak spending years are typically 25-44
        optimal_age_pct = age_25_34_pct + age_35_44_pct

        # Normalize to 0-1 scale (assuming max 40% in optimal age range)
        age_score = min(optimal_age_pct / 40.0, 1.0)

        return age_score

    def _calculate_occupation_score(
        self, professional_pct: float, management_pct: float
    ) -> float:
        """
        Calculate occupation score based on professional and management percentages.

        Args:
            professional_pct: Percentage in professional occupations
            management_pct: Percentage in management occupations

        Returns:
            Occupation score (0-1)
        """
        # Combine professional and management percentages
        high_earning_occupation_pct = professional_pct + management_pct

        # Normalize and apply professional bonus
        occupation_score = min(high_earning_occupation_pct / 50.0, 1.0)
        occupation_score += self.professional_bonus

        return min(occupation_score, 1.0)

    def _calculate_density_multiplier(self, population_density: float) -> float:
        """
        Calculate population density multiplier for spending propensity.
        Higher density areas typically have more spending opportunities.

        Args:
            population_density: Population per square mile

        Returns:
            Density multiplier (0.5-1.2)
        """
        if population_density <= 0:
            return 0.5

        # Log scale for density (urban areas have much higher density)
        log_density = math.log10(max(population_density, 1))

        # Normalize to 0.5-1.2 range
        # Rural (100/sq mi) = 0.5, Suburban (1000/sq mi) = 0.8, Urban (10000/sq mi) = 1.2
        density_multiplier = 0.5 + (log_density / 4.0) * 0.7

        return min(max(density_multiplier, 0.5), 1.2)

    def calculate_spending_propensity_score(
        self, demographic_profile: DemographicProfile
    ) -> Dict[str, float]:
        """
        Calculate comprehensive spending propensity score.

        Args:
            demographic_profile: Demographic data for the location

        Returns:
            Dictionary with score and component breakdowns
        """
        # Calculate component scores
        income_score = self._normalize_income_score(demographic_profile.median_income)
        education_score = self._calculate_education_score(
            demographic_profile.education_bachelors_pct,
            demographic_profile.education_graduate_pct,
        )
        age_score = self._calculate_age_score(
            demographic_profile.age_25_34_pct, demographic_profile.age_35_44_pct
        )
        occupation_score = self._calculate_occupation_score(
            demographic_profile.professional_occupation_pct,
            demographic_profile.management_occupation_pct,
        )
        density_multiplier = self._calculate_density_multiplier(
            demographic_profile.population_density
        )

        # Weighted combination of scores
        base_score = (
            income_score * 0.4
            + education_score * 0.25
            + age_score * 0.2
            + occupation_score * 0.15
        )

        # Apply density multiplier
        final_score = base_score * density_multiplier

        # Apply high income multiplier if income is significantly above median
        if demographic_profile.median_income > 80000:  # Above median threshold
            final_score *= self.high_income_multiplier

        # Ensure score stays within 0-1 range
        final_score = min(final_score, 1.0)

        return {
            "score": final_score,
            "components": {
                "income_score": income_score,
                "education_score": education_score,
                "age_score": age_score,
                "occupation_score": occupation_score,
                "density_multiplier": density_multiplier,
            },
            "confidence": self._calculate_confidence(demographic_profile),
        }

    def _calculate_confidence(self, demographic_profile: DemographicProfile) -> float:
        """
        Calculate confidence in the spending propensity score.

        Args:
            demographic_profile: Demographic data for the location

        Returns:
            Confidence score (0-1)
        """
        # Base confidence on data completeness
        data_completeness = 0.0
        total_fields = 7

        if demographic_profile.median_income > 0:
            data_completeness += 1
        if demographic_profile.education_bachelors_pct >= 0:
            data_completeness += 1
        if demographic_profile.education_graduate_pct >= 0:
            data_completeness += 1
        if demographic_profile.age_25_34_pct >= 0:
            data_completeness += 1
        if demographic_profile.age_35_44_pct >= 0:
            data_completeness += 1
        if demographic_profile.professional_occupation_pct >= 0:
            data_completeness += 1
        if demographic_profile.population_density > 0:
            data_completeness += 1

        base_confidence = data_completeness / total_fields

        # Boost confidence for high-quality indicators
        quality_boost = 0.0
        if demographic_profile.median_income > 50000:
            quality_boost += 0.1
        if demographic_profile.education_bachelors_pct > 30:
            quality_boost += 0.1
        if demographic_profile.population_density > 1000:
            quality_boost += 0.05

        return min(base_confidence + quality_boost, 1.0)

    def analyze_location_spending_potential(
        self, lat: float, lng: float, demographic_data: Dict
    ) -> Dict:
        """
        Comprehensive spending potential analysis for a location.

        Args:
            lat: Latitude of the location
            lng: Longitude of the location
            demographic_data: Dictionary with demographic information

        Returns:
            Detailed analysis dictionary
        """
        # Create demographic profile from input data
        profile = DemographicProfile(
            median_income=demographic_data.get("median_income", 0),
            education_bachelors_pct=demographic_data.get("education_bachelors_pct", 0),
            education_graduate_pct=demographic_data.get("education_graduate_pct", 0),
            age_25_34_pct=demographic_data.get("age_25_34_pct", 0),
            age_35_44_pct=demographic_data.get("age_35_44_pct", 0),
            professional_occupation_pct=demographic_data.get(
                "professional_occupation_pct", 0
            ),
            management_occupation_pct=demographic_data.get(
                "management_occupation_pct", 0
            ),
            population_density=demographic_data.get("population_density", 0),
        )

        # Calculate spending propensity
        result = self.calculate_spending_propensity_score(profile)

        # Determine spending category
        score = result["score"]
        if score >= 0.8:
            spending_category = "Very High"
        elif score >= 0.6:
            spending_category = "High"
        elif score >= 0.4:
            spending_category = "Medium"
        elif score >= 0.2:
            spending_category = "Low"
        else:
            spending_category = "Very Low"

        return {
            "location": {"lat": lat, "lng": lng},
            "spending_propensity_score": score,
            "spending_category": spending_category,
            "confidence": result["confidence"],
            "component_scores": result["components"],
            "demographic_profile": profile,
            "recommendations": self._generate_recommendations(
                score, result["components"]
            ),
        }

    def _generate_recommendations(
        self, score: float, components: Dict[str, float]
    ) -> List[str]:
        """
        Generate business recommendations based on spending propensity analysis.

        Args:
            score: Overall spending propensity score
            components: Component scores breakdown

        Returns:
            List of recommendation strings
        """
        recommendations = []

        if score >= 0.7:
            recommendations.append("Excellent location for premium/luxury businesses")
            recommendations.append("High potential for discretionary spending")

        if components["income_score"] >= 0.8:
            recommendations.append("Target high-income demographics")

        if components["education_score"] >= 0.7:
            recommendations.append(
                "Educational/professional services would perform well"
            )

        if components["age_score"] >= 0.6:
            recommendations.append("Target young professionals and families")

        if components["density_multiplier"] >= 1.0:
            recommendations.append("Urban location with high foot traffic potential")

        if score < 0.4:
            recommendations.append("Consider value-oriented business models")
            recommendations.append("Focus on essential services rather than luxury")

        return recommendations

    def generate_grid_scores(
        self,
        grid_bounds: Dict[str, float],
        demographic_data: pd.DataFrame,
        resolution_meters: int = 500,
    ) -> pd.DataFrame:
        """
        Generate spending propensity scores for a grid of locations.

        Args:
            grid_bounds: Dictionary with 'north', 'south', 'east', 'west' boundaries
            demographic_data: DataFrame with demographic data by location
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
                # Find nearest demographic data point
                # In a real implementation, this would use spatial joins
                # For now, we'll use a simplified approach
                demo_data = self._get_nearest_demographic_data(
                    lat, lng, demographic_data
                )

                if demo_data is not None:
                    analysis = self.analyze_location_spending_potential(
                        lat, lng, demo_data
                    )

                    grid_data.append(
                        {
                            "lat": lat,
                            "lng": lng,
                            "grid_cell_id": f"{lat:.6f}_{lng:.6f}",
                            "score": analysis["spending_propensity_score"],
                            "confidence": analysis["confidence"],
                            "spending_category": analysis["spending_category"],
                            "metadata": analysis,
                        }
                    )

                lng += lng_step
            lat += lat_step

        return pd.DataFrame(grid_data)

    def _get_nearest_demographic_data(
        self, lat: float, lng: float, demographic_data: pd.DataFrame
    ) -> Optional[Dict]:
        """
        Get nearest demographic data point for a location.
        Simplified implementation - in production would use spatial indexing.

        Args:
            lat: Target latitude
            lng: Target longitude
            demographic_data: DataFrame with demographic data

        Returns:
            Dictionary with demographic data or None
        """
        if demographic_data.empty:
            return None

        # Calculate distances to all demographic data points
        distances = []
        for _, row in demographic_data.iterrows():
            if "lat" in row and "lng" in row:
                dist = math.sqrt((lat - row["lat"]) ** 2 + (lng - row["lng"]) ** 2)
                distances.append(dist)
            else:
                distances.append(float("inf"))

        # Find nearest point
        min_idx = np.argmin(distances)
        nearest_row = demographic_data.iloc[min_idx]

        return nearest_row.to_dict()


def calculate_spending_scores_for_venues(
    venue_data: pd.DataFrame, demographic_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate spending propensity scores for a DataFrame of venues.

    Args:
        venue_data: DataFrame with 'lat' and 'lng' columns
        demographic_data: DataFrame with demographic information

    Returns:
        DataFrame with added spending propensity columns
    """
    spending_layer = SpendingPropensityLayer()

    scores = []
    confidences = []
    categories = []

    for _, row in venue_data.iterrows():
        demo_data = spending_layer._get_nearest_demographic_data(
            row["lat"], row["lng"], demographic_data
        )

        if demo_data:
            analysis = spending_layer.analyze_location_spending_potential(
                row["lat"], row["lng"], demo_data
            )
            scores.append(analysis["spending_propensity_score"])
            confidences.append(analysis["confidence"])
            categories.append(analysis["spending_category"])
        else:
            scores.append(0.0)
            confidences.append(0.0)
            categories.append("Unknown")

    venue_data = venue_data.copy()
    venue_data["spending_propensity_score"] = scores
    venue_data["spending_propensity_confidence"] = confidences
    venue_data["spending_category"] = categories

    return venue_data


if __name__ == "__main__":
    # Example usage and testing
    spending_layer = SpendingPropensityLayer()

    # Test with sample demographic data
    sample_demographics = {
        "median_income": 75000,
        "education_bachelors_pct": 45.0,
        "education_graduate_pct": 20.0,
        "age_25_34_pct": 18.0,
        "age_35_44_pct": 16.0,
        "professional_occupation_pct": 35.0,
        "management_occupation_pct": 15.0,
        "population_density": 2500.0,
    }

    print("Spending Propensity Analysis")
    print("=" * 50)

    analysis = spending_layer.analyze_location_spending_potential(
        39.0997, -94.5786, sample_demographics
    )

    print(f"Location: {analysis['location']}")
    print(f"Spending Propensity Score: {analysis['spending_propensity_score']:.3f}")
    print(f"Spending Category: {analysis['spending_category']}")
    print(f"Confidence: {analysis['confidence']:.3f}")

    print("\nComponent Scores:")
    for component, score in analysis["component_scores"].items():
        print(f"  {component}: {score:.3f}")

    print("\nRecommendations:")
    for rec in analysis["recommendations"]:
        print(f"  - {rec}")
