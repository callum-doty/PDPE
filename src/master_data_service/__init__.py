# Master Data Service Package
"""
Master Data Service - Single Source of Truth for PPM Data Collection

This package provides a unified interface for all data collection and aggregation,
consolidating the scattered ETL scripts into a cohesive master data system.
"""

from .orchestrator import MasterDataOrchestrator
from .quality_controller import QualityController

__all__ = ["MasterDataOrchestrator", "QualityController"]
