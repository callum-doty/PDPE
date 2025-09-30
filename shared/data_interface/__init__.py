"""
Data Interface Module

Provides unified interfaces for accessing consolidated data from the master data system.
"""

from .master_data_interface import (
    MasterDataInterface,
    get_venues_and_events,
    get_data_health_status,
    refresh_area_data,
    collect_fresh_data,
)

__all__ = [
    "MasterDataInterface",
    "get_venues_and_events",
    "get_data_health_status",
    "refresh_area_data",
    "collect_fresh_data",
]
