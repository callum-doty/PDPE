"""
Combines probability layers using Bayesian fusion techniques.
"""

from typing import Dict, List, Tuple
import math
from config.constants import PRIOR_PROBABILITY, CONFIDENCE_THRESHOLD


class BayesianFusion:
    """Combines multiple probability layers using Bayesian inference."""

    def __init__(self, prior_probability: float = None):
        """
        Initialize Bayesian fusion engine.

        Args:
            prior_probability: Base probability for any location
        """
        self.prior_probability = prior_probability or PRIOR_PROBABILITY

    def fuse_layers(self, layers: Dict[str, Dict]) -> Dict:
        """
        Fuse multiple probability layers using Bayesian inference.

        Args:
            layers: Dictionary of layer_name -> {coordinate_tuple: score} mappings

        Returns:
            Dictionary mapping coordinates to fused probability scores
        """
        if not layers:
            return {}

        # Get all unique coordinates across all layers
        all_coordinates = set()
        for layer in layers.values():
            all_coordinates.update(layer.keys())

        fused_probabilities = {}

        for coord in all_coordinates:
            # Start with prior probability
            posterior_odds = self._probability_to_odds(self.prior_probability)

            # Apply each layer as evidence
            for layer_name, layer_data in layers.items():
                if coord in layer_data:
                    score = layer_data[coord]
                    likelihood_ratio = self._score_to_likelihood_ratio(
                        score, layer_name
                    )
                    posterior_odds *= likelihood_ratio

            # Convert back to probability
            fused_probability = self._odds_to_probability(posterior_odds)
            fused_probabilities[coord] = fused_probability

        return fused_probabilities

    def _probability_to_odds(self, probability: float) -> float:
        """Convert probability to odds."""
        if probability >= 1.0:
            return float("inf")
        if probability <= 0.0:
            return 0.0
        return probability / (1.0 - probability)

    def _odds_to_probability(self, odds: float) -> float:
        """Convert odds to probability."""
        if odds == float("inf"):
            return 1.0
        if odds <= 0.0:
            return 0.0
        return odds / (1.0 + odds)

    def _score_to_likelihood_ratio(self, score: float, layer_name: str) -> float:
        """
        Convert a layer score to a likelihood ratio.
        This is where domain knowledge is encoded.
        """
        # Normalize scores to likelihood ratios based on layer type
        if layer_name == "demographic":
            # Demographic scores typically range 0-8
            # Higher scores strongly indicate target demographic presence
            if score >= 6:
                return 5.0  # Strong positive evidence
            elif score >= 4:
                return 2.0  # Moderate positive evidence
            elif score >= 2:
                return 1.2  # Weak positive evidence
            elif score > 0:
                return 1.0  # Neutral
            else:
                return 0.5  # Weak negative evidence

        elif layer_name == "event_activity":
            # Event activity scores can vary widely
            # Higher activity indicates more opportunities
            if score >= 10:
                return 3.0  # Strong positive evidence
            elif score >= 5:
                return 2.0  # Moderate positive evidence
            elif score >= 2:
                return 1.5  # Weak positive evidence
            elif score > 0:
                return 1.0  # Neutral
            else:
                return 0.8  # Slight negative evidence

        elif layer_name == "weather":
            # Weather scores typically range -4 to +2
            # Positive scores indicate favorable conditions
            if score >= 2:
                return 1.5  # Positive evidence
            elif score >= 0:
                return 1.0  # Neutral
            elif score >= -2:
                return 0.8  # Slight negative evidence
            else:
                return 0.5  # Strong negative evidence

        else:
            # Default: simple linear mapping
            return max(0.1, min(10.0, 1.0 + (score / 5.0)))

    def get_high_confidence_areas(
        self, fused_probabilities: Dict, threshold: float = None
    ) -> List[Tuple]:
        """
        Get areas with high confidence (probability above threshold).

        Args:
            fused_probabilities: Result from fuse_layers()
            threshold: Minimum probability threshold

        Returns:
            List of (coordinate, probability) tuples sorted by probability
        """
        threshold = threshold or CONFIDENCE_THRESHOLD

        high_confidence = [
            (coord, prob)
            for coord, prob in fused_probabilities.items()
            if prob >= threshold
        ]

        return sorted(high_confidence, key=lambda x: x[1], reverse=True)

    def calculate_area_statistics(self, fused_probabilities: Dict) -> Dict:
        """Calculate statistics for the fused probability map."""
        if not fused_probabilities:
            return {
                "total_areas": 0,
                "mean_probability": 0,
                "max_probability": 0,
                "high_confidence_areas": 0,
                "coverage_area_km2": 0,
            }

        probabilities = list(fused_probabilities.values())
        high_confidence_count = sum(
            1 for p in probabilities if p >= CONFIDENCE_THRESHOLD
        )

        # Estimate coverage area (assuming each coordinate represents ~0.25 km²)
        coverage_area = len(fused_probabilities) * 0.25

        return {
            "total_areas": len(fused_probabilities),
            "mean_probability": sum(probabilities) / len(probabilities),
            "max_probability": max(probabilities),
            "high_confidence_areas": high_confidence_count,
            "coverage_area_km2": coverage_area,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
        }

    def export_probability_map(
        self, fused_probabilities: Dict, include_metadata: bool = True
    ) -> Dict:
        """
        Export the fused probability map for external use.

        Args:
            fused_probabilities: Result from fuse_layers()
            include_metadata: Whether to include statistics and metadata

        Returns:
            Dictionary suitable for JSON export
        """
        # Convert coordinate tuples to strings for JSON serialization
        probability_map = {
            f"{coord[0]},{coord[1]}": prob
            for coord, prob in fused_probabilities.items()
        }

        result = {
            "probability_map": probability_map,
            "timestamp": None,  # Could add datetime.now().isoformat()
        }

        if include_metadata:
            result["metadata"] = {
                "fusion_method": "bayesian",
                "prior_probability": self.prior_probability,
                "statistics": self.calculate_area_statistics(fused_probabilities),
                "high_confidence_areas": [
                    {"lat": coord[0], "lon": coord[1], "probability": prob}
                    for coord, prob in self.get_high_confidence_areas(
                        fused_probabilities
                    )
                ],
            }

        return result


def simple_weighted_fusion(
    layers: Dict[str, Dict], weights: Dict[str, float] = None
) -> Dict:
    """
    Simple weighted average fusion of probability layers.
    Alternative to Bayesian fusion for simpler use cases.

    Args:
        layers: Dictionary of layer_name -> {coordinate_tuple: score} mappings
        weights: Dictionary of layer_name -> weight mappings

    Returns:
        Dictionary mapping coordinates to weighted average scores
    """
    if not layers:
        return {}

    # Default equal weights
    if weights is None:
        weights = {layer_name: 1.0 for layer_name in layers.keys()}

    # Normalize weights
    total_weight = sum(weights.values())
    normalized_weights = {k: v / total_weight for k, v in weights.items()}

    # Get all unique coordinates
    all_coordinates = set()
    for layer in layers.values():
        all_coordinates.update(layer.keys())

    fused_scores = {}

    for coord in all_coordinates:
        weighted_sum = 0.0
        total_applicable_weight = 0.0

        for layer_name, layer_data in layers.items():
            if coord in layer_data:
                score = layer_data[coord]
                weight = normalized_weights.get(layer_name, 0.0)
                weighted_sum += score * weight
                total_applicable_weight += weight

        # Average by applicable weights only
        if total_applicable_weight > 0:
            fused_scores[coord] = weighted_sum / total_applicable_weight
        else:
            fused_scores[coord] = 0.0

    return fused_scores
