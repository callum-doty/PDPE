"""
spending propensity formatter data formatter.
"""

from typing import Dict, List
from src.data_acquisition.assumptions.spending_propensity_layer import (
    get_dow_factor,
    get_hour_factor,
    get_holiday_factor_simple,
    get_monthly_factor,
    get_week_of_month_factor,
)


def format_spending_propensity_data(raw_data: Dict) -> Dict:
    """Format raw API response into standardized format."""
    # TODO: Implement specific formatting logic for spending_propensity
    return {
        "source": "spending_propensity",
        "data": raw_data,
        "formatted": True,
    }


def calculate_spending_propensity(datetime_obj, grid_cell_info=None):
    """
    Calculates a spending multiplier for a specific time and location.
    datetime_obj: Python datetime object for the time we want to check.
    grid_cell_info: (Optional) A dictionary containing data about the specific grid cell (e.g., local income).
    """

    # Get all our individual factors
    base_weekly = get_week_of_month_factor(datetime_obj)
    monthly = get_monthly_factor(datetime_obj.month)
    holiday = get_holiday_factor_simple(datetime_obj)  # Start with the simple version
    dow = get_dow_factor(datetime_obj.weekday())
    hour = get_hour_factor(datetime_obj.hour)

    # Start with the product of the core factors
    total_multiplier = base_weekly * monthly * holiday * dow * hour

    # If we have local grid cell data, factor it in
    if grid_cell_info:
        income_factor = (
            grid_cell_info.get("avg_income", 50000) / 50000
        )  # Normalize to a city average
        # total_multiplier = total_multiplier * income_factor

    # Optional: Add a cap and a floor to prevent insane values
    # e.g., no less than 10% of normal, no more than 5x normal
    total_multiplier = max(0.1, min(total_multiplier, 5.0))

    return total_multiplier
