"""
Comprehensive feature engineering pipeline for psychographic prediction.
Implements all 13 feature groups as outlined in the refined approach.
"""

import os
import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from etl.utils import get_db_conn
from config.constants import (
    PSYCHOGRAPHIC_WEIGHTS,
    TIME_MULTIPLIERS,
    WEATHER_MULTIPLIERS,
    DISTANCE_DECAY,
    KC_DOWNTOWN,
)
from config.settings import settings
from features.college_layer import CollegeLayer
from features.spending_propensity_layer import SpendingPropensityLayer


@dataclass
class FeatureConfig:
    """Configuration for feature engineering."""

    start_ts: datetime
    end_ts: datetime
    grid_resolution_meters: int = 500
    spatial_buffer_meters: int = 1000
    include_confidence_scores: bool = True


class ComprehensiveFeatureBuilder:
    """
    Builds comprehensive feature vectors for psychographic prediction.
    Implements all 13 feature groups from the refined approach.
    """

    def __init__(self):
        self.college_layer = CollegeLayer()
        self.spending_layer = SpendingPropensityLayer()
        self.downtown_coords = (KC_DOWNTOWN["lat"], KC_DOWNTOWN["lng"])

    def build_features_for_time_window(self, config: FeatureConfig) -> pd.DataFrame:
        """
        Build comprehensive features for a time window.

        Args:
            config: Feature engineering configuration

        Returns:
            DataFrame with engineered features
        """
        conn = get_db_conn()

        # Load base data
        venues_df = self._load_venues(conn)
        traffic_df = self._load_traffic_data(conn, config.start_ts, config.end_ts)
        events_df = self._load_events_data(conn, config.start_ts, config.end_ts)
        weather_df = self._load_weather_data(conn, config.start_ts, config.end_ts)
        demographics_df = self._load_demographics(conn)
        social_df = self._load_social_sentiment(conn, config.start_ts, config.end_ts)
        economic_df = self._load_economic_data(conn, config.start_ts, config.end_ts)
        traffic_congestion_df = self._load_traffic_congestion(
            conn, config.start_ts, config.end_ts
        )

        # Create base feature matrix from venue-time combinations
        feature_df = self._create_base_feature_matrix(venues_df, traffic_df, config)

        # Apply all 13 feature groups
        feature_df = self._add_venue_demographic_features(feature_df, demographics_df)
        feature_df = self._add_venue_attribute_features(feature_df, venues_df)
        feature_df = self._add_foot_traffic_features(feature_df, traffic_df)
        feature_df = self._add_traffic_features(feature_df, traffic_congestion_df)
        feature_df = self._add_event_features(feature_df, events_df)
        feature_df = self._add_weather_features(feature_df, weather_df)
        feature_df = self._add_economic_features(feature_df, economic_df)
        feature_df = self._add_social_sentiment_features(feature_df, social_df)
        feature_df = self._add_custom_layer_features(feature_df)
        feature_df = self._add_temporal_features(feature_df)
        feature_df = self._add_spatial_features(feature_df, venues_df)
        feature_df = self._add_competitive_features(feature_df, venues_df)
        feature_df = self._add_historical_features(feature_df, conn)

        # Save to database
        self._save_features_to_db(feature_df, conn)

        conn.close()
        return feature_df

    def _load_venues(self, conn) -> pd.DataFrame:
        """Load venue data with psychographic relevance scores."""
        query = """
        SELECT venue_id, external_id, provider, name, category, subcategory,
               price_tier, avg_rating, review_count, lat, lng, address,
               psychographic_relevance, created_at, updated_at
        FROM venues
        """
        return pd.read_sql(query, conn)

    def _load_traffic_data(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load foot traffic data for time window."""
        query = """
        SELECT venue_id, ts, visitors_count, median_dwell_seconds,
               visitors_change_24h, visitors_change_7d, peak_hour_ratio, source
        FROM venue_traffic
        WHERE ts BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts))

    def _load_events_data(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load events data for time window."""
        query = """
        SELECT event_id, venue_id, name, category, subcategory, tags,
               start_time, end_time, predicted_attendance, actual_attendance,
               ticket_price_min, ticket_price_max, psychographic_relevance
        FROM events
        WHERE start_time BETWEEN %s AND %s OR end_time BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts, start_ts, end_ts))

    def _load_weather_data(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load weather data for time window."""
        query = """
        SELECT ts, lat, lng, temperature_f, feels_like_f, humidity,
               rain_probability, precipitation_mm, wind_speed_mph,
               weather_condition, uv_index
        FROM weather_data
        WHERE ts BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts))

    def _load_demographics(self, conn) -> pd.DataFrame:
        """Load demographic data by census tract."""
        query = """
        SELECT tract_id, ST_AsText(geometry) as geometry_wkt,
               median_income, median_income_z, pct_bachelors, pct_graduate,
               pct_age_20_30, pct_age_30_40, pct_age_20_40, population,
               population_density, pct_professional_occupation, pct_management_occupation
        FROM demographics
        """
        return pd.read_sql(query, conn)

    def _load_social_sentiment(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load social sentiment data."""
        query = """
        SELECT venue_id, event_id, ts, platform, mention_count,
               positive_sentiment, negative_sentiment, neutral_sentiment,
               engagement_score, psychographic_keywords
        FROM social_sentiment
        WHERE ts BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts))

    def _load_economic_data(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load economic indicators."""
        query = """
        SELECT ts, geographic_area, unemployment_rate, median_household_income,
               business_openings, business_closures, consumer_confidence,
               local_spending_index
        FROM economic_data
        WHERE ts BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts))

    def _load_traffic_congestion(
        self, conn, start_ts: datetime, end_ts: datetime
    ) -> pd.DataFrame:
        """Load traffic congestion data."""
        query = """
        SELECT venue_id, ts, congestion_score, travel_time_to_downtown,
               travel_time_index, source
        FROM traffic_data
        WHERE ts BETWEEN %s AND %s
        """
        return pd.read_sql(query, conn, params=(start_ts, end_ts))

    def _create_base_feature_matrix(
        self, venues_df: pd.DataFrame, traffic_df: pd.DataFrame, config: FeatureConfig
    ) -> pd.DataFrame:
        """Create base feature matrix from venue-time combinations."""
        # Use traffic data as the base since it provides venue-time pairs
        base_df = traffic_df.merge(venues_df, on="venue_id", how="left")

        # Ensure we have the required columns
        base_df["ts"] = pd.to_datetime(base_df["ts"])

        return base_df

    def _add_venue_demographic_features(
        self, df: pd.DataFrame, demographics_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add venue demographic features from census data."""
        # Simplified spatial join - in production would use PostGIS
        # For now, find nearest demographic tract for each venue

        for idx, row in df.iterrows():
            nearest_demo = self._find_nearest_demographic(
                row["lat"], row["lng"], demographics_df
            )
            if nearest_demo is not None:
                df.loc[idx, "median_income_z"] = nearest_demo.get("median_income_z", 0)
                df.loc[idx, "education_bachelors_pct"] = nearest_demo.get(
                    "pct_bachelors", 0
                )
                df.loc[idx, "education_graduate_pct"] = nearest_demo.get(
                    "pct_graduate", 0
                )
                df.loc[idx, "age_20_30_pct"] = nearest_demo.get("pct_age_20_30", 0)
                df.loc[idx, "age_30_40_pct"] = nearest_demo.get("pct_age_30_40", 0)
                df.loc[idx, "professional_occupation_pct"] = nearest_demo.get(
                    "pct_professional_occupation", 0
                )
                df.loc[idx, "population_density"] = nearest_demo.get(
                    "population_density", 0
                )

        return df

    def _add_venue_attribute_features(
        self, df: pd.DataFrame, venues_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add venue attribute features."""
        df["venue_type"] = df["category"]
        df["venue_subcategory"] = df["subcategory"]
        df["psychographic_venue_score"] = df.apply(
            self._calculate_venue_psychographic_score, axis=1
        )

        return df

    def _add_foot_traffic_features(
        self, df: pd.DataFrame, traffic_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add foot traffic features."""
        df["foot_traffic_hourly"] = df["visitors_count"]
        df["foot_traffic_change_24h"] = df["visitors_change_24h"]
        df["foot_traffic_change_7d"] = df["visitors_change_7d"]
        df["dwell_time_median"] = (
            df["median_dwell_seconds"] / 60.0
        )  # Convert to minutes
        df["peak_hour_ratio"] = df["peak_hour_ratio"]

        return df

    def _add_traffic_features(
        self, df: pd.DataFrame, traffic_congestion_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add traffic congestion features."""
        # Merge traffic congestion data
        traffic_features = (
            traffic_congestion_df.groupby(["venue_id", "ts"])
            .agg(
                {
                    "congestion_score": "mean",
                    "travel_time_to_downtown": "mean",
                    "travel_time_index": "mean",
                }
            )
            .reset_index()
        )

        df = df.merge(traffic_features, on=["venue_id", "ts"], how="left")
        df["traffic_congestion_score"] = df["congestion_score"].fillna(0.5)
        df["travel_time_downtown"] = df["travel_time_to_downtown"].fillna(30.0)
        df["travel_time_index"] = df["travel_time_index"].fillna(1.0)

        return df

    def _add_event_features(
        self, df: pd.DataFrame, events_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add event-related features."""
        # For each venue-time pair, find relevant events
        for idx, row in df.iterrows():
            venue_events = events_df[
                (events_df["venue_id"] == row["venue_id"])
                & (events_df["start_time"] <= row["ts"])
                & (events_df["end_time"] >= row["ts"])
            ]

            if not venue_events.empty:
                df.loc[idx, "event_predicted_attendance"] = venue_events[
                    "predicted_attendance"
                ].sum()
                df.loc[idx, "event_category"] = venue_events["category"].iloc[0]
                df.loc[idx, "event_psychographic_score"] = (
                    self._calculate_event_psychographic_score(venue_events)
                )
                df.loc[idx, "event_ticket_price_avg"] = (
                    venue_events[["ticket_price_min", "ticket_price_max"]].mean().mean()
                )
            else:
                df.loc[idx, "event_predicted_attendance"] = 0
                df.loc[idx, "event_category"] = None
                df.loc[idx, "event_psychographic_score"] = 0
                df.loc[idx, "event_ticket_price_avg"] = 0

            # Count nearby events
            nearby_events = events_df[
                (abs(events_df["start_time"] - row["ts"]) <= timedelta(hours=2))
            ]
            df.loc[idx, "events_nearby_count"] = len(nearby_events)

        return df

    def _add_weather_features(
        self, df: pd.DataFrame, weather_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add weather features."""
        # Find nearest weather data for each venue-time pair
        for idx, row in df.iterrows():
            nearest_weather = self._find_nearest_weather(
                row["lat"], row["lng"], row["ts"], weather_df
            )
            if nearest_weather is not None:
                df.loc[idx, "temp_fahrenheit"] = nearest_weather.get(
                    "temperature_f", 72.0
                )
                df.loc[idx, "feels_like_f"] = nearest_weather.get("feels_like_f", 72.0)
                df.loc[idx, "rain_prob"] = nearest_weather.get("rain_probability", 0.0)
                df.loc[idx, "humidity"] = nearest_weather.get("humidity", 50.0)
                df.loc[idx, "weather_condition"] = nearest_weather.get(
                    "weather_condition", "clear"
                )
                df.loc[idx, "uv_index"] = nearest_weather.get("uv_index", 5.0)

        return df

    def _add_economic_features(
        self, df: pd.DataFrame, economic_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add economic indicator features."""
        # Use most recent economic data for the time period
        if not economic_df.empty:
            latest_econ = economic_df.loc[economic_df["ts"].idxmax()]
            df["unemployment_rate"] = latest_econ.get("unemployment_rate", 5.0)
            df["business_health_score"] = self._calculate_business_health_score(
                latest_econ
            )
            df["consumer_confidence"] = latest_econ.get("consumer_confidence", 100.0)
            df["local_spending_index"] = latest_econ.get("local_spending_index", 1.0)
        else:
            df["unemployment_rate"] = 5.0
            df["business_health_score"] = 0.5
            df["consumer_confidence"] = 100.0
            df["local_spending_index"] = 1.0

        return df

    def _add_social_sentiment_features(
        self, df: pd.DataFrame, social_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add social sentiment features."""
        # Aggregate social sentiment by venue and time
        social_agg = (
            social_df.groupby(["venue_id", "ts"])
            .agg(
                {
                    "mention_count": "sum",
                    "positive_sentiment": "mean",
                    "negative_sentiment": "mean",
                    "engagement_score": "mean",
                }
            )
            .reset_index()
        )

        df = df.merge(social_agg, on=["venue_id", "ts"], how="left")
        df["social_mention_count"] = df["mention_count"].fillna(0)
        df["social_sentiment_score"] = (
            df["positive_sentiment"] - df["negative_sentiment"]
        ).fillna(0)
        df["social_engagement_score"] = df["engagement_score"].fillna(0)

        # Count psychographic keywords
        df["psychographic_keyword_count"] = 0  # Simplified for now

        return df

    def _add_custom_layer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add custom psychographic layer features."""
        for idx, row in df.iterrows():
            # College layer score
            college_result = self.college_layer.calculate_college_density_score(
                row["lat"], row["lng"]
            )
            df.loc[idx, "college_layer_score"] = college_result["score"]

            # Spending propensity score
            demo_data = {
                "median_income": row.get("median_income_z", 0) * 20000
                + 65000,  # Approximate conversion
                "education_bachelors_pct": row.get("education_bachelors_pct", 0),
                "education_graduate_pct": row.get("education_graduate_pct", 0),
                "age_25_34_pct": row.get("age_20_30_pct", 0),
                "age_35_44_pct": row.get("age_30_40_pct", 0),
                "professional_occupation_pct": row.get(
                    "professional_occupation_pct", 0
                ),
                "management_occupation_pct": 0,  # Not available in current schema
                "population_density": row.get("population_density", 0),
            }
            spending_result = self.spending_layer.analyze_location_spending_potential(
                row["lat"], row["lng"], demo_data
            )
            df.loc[idx, "spending_propensity_score"] = spending_result[
                "spending_propensity_score"
            ]

        return df

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add temporal features."""
        df["ts"] = pd.to_datetime(df["ts"])
        df["hour_sin"] = np.sin(2 * np.pi * df["ts"].dt.hour / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["ts"].dt.hour / 24)
        df["day_of_week"] = df["ts"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6])
        df["is_holiday"] = False  # Simplified - would need holiday calendar

        return df

    def _add_spatial_features(
        self, df: pd.DataFrame, venues_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add spatial features."""
        # Distance to downtown
        df["distance_to_downtown"] = df.apply(
            lambda row: self._haversine_distance(
                row["lat"], row["lng"], self.downtown_coords[0], self.downtown_coords[1]
            ),
            axis=1,
        )

        # Neighborhood type (simplified)
        df["neighborhood_type"] = df["distance_to_downtown"].apply(
            lambda d: "urban" if d < 5 else "suburban" if d < 15 else "rural"
        )

        return df

    def _add_competitive_features(
        self, df: pd.DataFrame, venues_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Add competitive landscape features."""
        # Venue density within 500m (simplified calculation)
        for idx, row in df.iterrows():
            nearby_venues = venues_df[
                (abs(venues_df["lat"] - row["lat"]) < 0.005)  # Rough 500m approximation
                & (abs(venues_df["lng"] - row["lng"]) < 0.005)
            ]
            df.loc[idx, "venue_density_500m"] = len(nearby_venues)

        return df

    def _add_historical_features(self, df: pd.DataFrame, conn) -> pd.DataFrame:
        """Add historical trend features."""
        # Simplified historical features
        df["venue_popularity_trend"] = 0.0  # Would calculate from historical data
        df["event_success_rate"] = 0.5  # Would calculate from historical event data

        return df

    def _save_features_to_db(self, df: pd.DataFrame, conn):
        """Save engineered features to database."""
        # Prepare data for insertion
        feature_columns = [
            "venue_id",
            "ts",
            "median_income_z",
            "education_bachelors_pct",
            "education_graduate_pct",
            "age_20_30_pct",
            "age_30_40_pct",
            "professional_occupation_pct",
            "population_density",
            "venue_type",
            "venue_subcategory",
            "avg_rating",
            "review_count",
            "price_tier",
            "psychographic_venue_score",
            "foot_traffic_hourly",
            "foot_traffic_change_24h",
            "foot_traffic_change_7d",
            "dwell_time_median",
            "peak_hour_ratio",
            "traffic_congestion_score",
            "travel_time_downtown",
            "travel_time_index",
            "event_predicted_attendance",
            "event_category",
            "event_psychographic_score",
            "event_ticket_price_avg",
            "events_nearby_count",
            "temp_fahrenheit",
            "feels_like_f",
            "rain_prob",
            "humidity",
            "weather_condition",
            "uv_index",
            "unemployment_rate",
            "business_health_score",
            "consumer_confidence",
            "local_spending_index",
            "social_mention_count",
            "social_sentiment_score",
            "social_engagement_score",
            "psychographic_keyword_count",
            "college_layer_score",
            "spending_propensity_score",
            "hour_sin",
            "hour_cos",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "distance_to_downtown",
            "neighborhood_type",
            "venue_density_500m",
            "venue_popularity_trend",
            "event_success_rate",
        ]

        # Insert features into database
        cur = conn.cursor()
        for _, row in df.iterrows():
            values = [row.get(col) for col in feature_columns]
            placeholders = ",".join(["%s"] * len(values))

            cur.execute(
                f"""
                INSERT INTO features ({",".join(feature_columns)})
                VALUES ({placeholders})
                ON CONFLICT (venue_id, ts) DO UPDATE SET
                {",".join([f"{col} = EXCLUDED.{col}" for col in feature_columns[2:]])}
            """,
                values,
            )

        conn.commit()
        cur.close()

    # Helper methods
    def _find_nearest_demographic(
        self, lat: float, lng: float, demographics_df: pd.DataFrame
    ) -> Optional[Dict]:
        """Find nearest demographic data point."""
        if demographics_df.empty:
            return None

        # Simplified distance calculation
        distances = (
            (demographics_df["lat"] - lat) ** 2 + (demographics_df["lng"] - lng) ** 2
        ) ** 0.5
        nearest_idx = distances.idxmin()
        return demographics_df.iloc[nearest_idx].to_dict()

    def _find_nearest_weather(
        self, lat: float, lng: float, ts: datetime, weather_df: pd.DataFrame
    ) -> Optional[Dict]:
        """Find nearest weather data point."""
        if weather_df.empty:
            return None

        # Find weather data closest in time and space
        weather_df["time_diff"] = abs(
            (pd.to_datetime(weather_df["ts"]) - ts).dt.total_seconds()
        )
        weather_df["spatial_diff"] = (
            (weather_df["lat"] - lat) ** 2 + (weather_df["lng"] - lng) ** 2
        ) ** 0.5
        weather_df["combined_diff"] = (
            weather_df["time_diff"] / 3600 + weather_df["spatial_diff"] * 100
        )  # Weight time vs space

        nearest_idx = weather_df["combined_diff"].idxmin()
        return weather_df.iloc[nearest_idx].to_dict()

    def _calculate_venue_psychographic_score(self, row) -> float:
        """Calculate psychographic relevance score for venue."""
        category = row.get("category", "")
        subcategory = row.get("subcategory", "")

        score = 0.0
        for psychographic, weights in PSYCHOGRAPHIC_WEIGHTS.items():
            venue_weights = weights.get("venue_categories", {})
            score += venue_weights.get(category, 0) * 0.33
            score += venue_weights.get(subcategory, 0) * 0.33

        return min(score, 1.0)

    def _calculate_event_psychographic_score(self, events_df: pd.DataFrame) -> float:
        """Calculate psychographic relevance score for events."""
        if events_df.empty:
            return 0.0

        total_score = 0.0
        for _, event in events_df.iterrows():
            category = event.get("category", "")
            score = 0.0

            for psychographic, weights in PSYCHOGRAPHIC_WEIGHTS.items():
                event_weights = weights.get("event_types", {})
                score += event_weights.get(category, 0) * 0.5

            total_score += score

        return min(total_score / len(events_df), 1.0)

    def _calculate_business_health_score(self, economic_data: Dict) -> float:
        """Calculate business health score from economic indicators."""
        openings = economic_data.get("business_openings", 0)
        closures = economic_data.get("business_closures", 0)

        if openings + closures == 0:
            return 0.5

        health_ratio = openings / (openings + closures)
        return min(max(health_ratio, 0.0), 1.0)

    def _haversine_distance(
        self, lat1: float, lng1: float, lat2: float, lng2: float
    ) -> float:
        """Calculate haversine distance between two points in kilometers."""
        lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
        dlat = lat2 - lat1
        dlng = lng2 - lng1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))
        return c * 6371  # Earth's radius in km


# Convenience functions for backward compatibility
def build_features_for_time_window(
    start_ts: datetime, end_ts: datetime
) -> pd.DataFrame:
    """Build features for a time window using the comprehensive feature builder."""
    config = FeatureConfig(start_ts=start_ts, end_ts=end_ts)
    builder = ComprehensiveFeatureBuilder()
    return builder.build_features_for_time_window(config)


def cyclical_hour_feature(df, ts_col="ts"):
    """Legacy function for backward compatibility."""
    df["hour"] = df[ts_col].dt.hour
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    return df


def compute_24h_change(
    df, venue_id_col="venue_id", ts_col="ts", value_col="visitors_count"
):
    """Legacy function for backward compatibility."""
    df = df.sort_values([venue_id_col, ts_col])
    df["visitors_prev_24h"] = df.groupby(venue_id_col)[value_col].shift(24)
    df["visitors_pct_change_24h"] = (df[value_col] - df["visitors_prev_24h"]) / (
        df["visitors_prev_24h"] + 1e-6
    )
    return df
