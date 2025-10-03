"""
DEPRECATED: This module has been moved to shared.models

All event models have been consolidated into the unified models system.
Please update your imports to use:

    from shared.models import Event, EventCollectionResult, EventProcessingResult

This file will be removed in a future version.
"""

import warnings
from shared.models import Event, EventCollectionResult, EventProcessingResult

warnings.warn(
    "features.events.models is deprecated. Use shared.models instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export for backward compatibility
__all__ = ["Event", "EventCollectionResult", "EventProcessingResult"]
