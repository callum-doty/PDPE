"""
Unified Quality Validation for PPM Application

Single quality validation system that consolidates all data quality checks,
replacing scattered validation logic throughout the application.
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import json


@dataclass
class ValidationResult:
    """Result of a single validation check"""

    is_valid: bool
    field_name: str
    error_message: Optional[str] = None
    warning_message: Optional[str] = None
    score: float = 1.0  # 0.0 = invalid, 1.0 = perfect


@dataclass
class QualityMetrics:
    """Overall quality metrics for a dataset"""

    total_records: int
    valid_records: int
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    overall_score: float
    validation_results: List[ValidationResult]
    timestamp: datetime


class QualityValidator:
    """
    Unified quality validator for all PPM data types.

    Consolidates validation logic from quality_controller.py and scattered
    validation checks throughout the application into a single, consistent system.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # Validation thresholds
        self.min_name_length = 2
        self.max_name_length = 200
        self.min_description_length = 10
        self.max_description_length = 2000

        # Geographic bounds for Kansas City area
        self.kc_bounds = {
            "min_lat": 38.5,
            "max_lat": 39.5,
            "min_lng": -95.0,
            "max_lng": -94.0,
        }

        # Valid categories
        self.valid_venue_categories = {
            "restaurant",
            "bar",
            "nightclub",
            "theater",
            "museum",
            "gallery",
            "music_venue",
            "sports_venue",
            "shopping",
            "hotel",
            "event_venue",
            "entertainment",
            "recreation",
            "cultural",
            "local_venue",
            "major_venue",
        }

        self.valid_event_categories = {
            "music",
            "theater",
            "sports",
            "food",
            "art",
            "business",
            "cultural",
            "nightlife",
            "outdoor",
            "family",
            "local_event",
            "major_event",
        }

    def validate_venue(self, venue_data: Dict) -> Tuple[bool, List[ValidationResult]]:
        """
        Validate venue data comprehensively.

        Args:
            venue_data: Dictionary containing venue information

        Returns:
            Tuple of (is_valid, list_of_validation_results)
        """
        results = []

        # Required fields validation
        results.extend(
            self._validate_required_fields(
                venue_data, ["name", "external_id", "provider"]
            )
        )

        # Name validation
        results.append(self._validate_name(venue_data.get("name"), "venue_name"))

        # Description validation
        if venue_data.get("description"):
            results.append(self._validate_description(venue_data.get("description")))

        # Category validation
        results.append(self._validate_venue_category(venue_data.get("category")))

        # Location validation
        if venue_data.get("lat") is not None and venue_data.get("lng") is not None:
            results.extend(
                self._validate_coordinates(venue_data.get("lat"), venue_data.get("lng"))
            )

        # Contact information validation
        if venue_data.get("phone"):
            results.append(self._validate_phone(venue_data.get("phone")))

        if venue_data.get("website"):
            results.append(self._validate_url(venue_data.get("website"), "website"))

        # Rating validation
        if venue_data.get("avg_rating") is not None:
            results.append(self._validate_rating(venue_data.get("avg_rating")))

        # Psychographic data validation
        if venue_data.get("psychographic_relevance"):
            results.append(
                self._validate_psychographic_data(
                    venue_data.get("psychographic_relevance")
                )
            )

        # Overall validity
        is_valid = all(r.is_valid for r in results if r.is_valid is not None)

        return is_valid, results

    def validate_event(self, event_data: Dict) -> Tuple[bool, List[ValidationResult]]:
        """
        Validate event data comprehensively.

        Args:
            event_data: Dictionary containing event information

        Returns:
            Tuple of (is_valid, list_of_validation_results)
        """
        results = []

        # Required fields validation
        results.extend(
            self._validate_required_fields(
                event_data, ["name", "external_id", "provider"]
            )
        )

        # Name validation
        results.append(self._validate_name(event_data.get("name"), "event_name"))

        # Description validation
        if event_data.get("description"):
            results.append(self._validate_description(event_data.get("description")))

        # Category validation
        results.append(self._validate_event_category(event_data.get("category")))

        # Date/time validation
        if event_data.get("start_time"):
            results.append(self._validate_event_datetime(event_data.get("start_time")))

        if event_data.get("end_time") and event_data.get("start_time"):
            results.append(
                self._validate_event_duration(
                    event_data.get("start_time"), event_data.get("end_time")
                )
            )

        # Venue validation
        if event_data.get("venue_name"):
            results.append(
                self._validate_name(event_data.get("venue_name"), "venue_name")
            )

        # Location validation (if provided)
        if event_data.get("lat") is not None and event_data.get("lng") is not None:
            results.extend(
                self._validate_coordinates(event_data.get("lat"), event_data.get("lng"))
            )

        # Psychographic data validation
        if event_data.get("psychographic_relevance"):
            results.append(
                self._validate_psychographic_data(
                    event_data.get("psychographic_relevance")
                )
            )

        # Overall validity
        is_valid = all(r.is_valid for r in results if r.is_valid is not None)

        return is_valid, results

    def validate_prediction(
        self, prediction_data: Dict
    ) -> Tuple[bool, List[ValidationResult]]:
        """
        Validate ML prediction data.

        Args:
            prediction_data: Dictionary containing prediction information

        Returns:
            Tuple of (is_valid, list_of_validation_results)
        """
        results = []

        # Required fields validation
        results.extend(
            self._validate_required_fields(
                prediction_data, ["venue_id", "prediction_type", "prediction_value"]
            )
        )

        # Prediction value validation (should be 0-1 probability)
        if prediction_data.get("prediction_value") is not None:
            results.append(
                self._validate_probability(
                    prediction_data.get("prediction_value"), "prediction_value"
                )
            )

        # Confidence score validation
        if prediction_data.get("confidence_score") is not None:
            results.append(
                self._validate_probability(
                    prediction_data.get("confidence_score"), "confidence_score"
                )
            )

        # Model version validation
        if prediction_data.get("model_version"):
            results.append(
                self._validate_model_version(prediction_data.get("model_version"))
            )

        # Features validation
        if prediction_data.get("features_used"):
            results.append(
                self._validate_feature_list(prediction_data.get("features_used"))
            )

        # Overall validity
        is_valid = all(r.is_valid for r in results if r.is_valid is not None)

        return is_valid, results

    def calculate_dataset_quality(
        self, data: List[Dict], data_type: str
    ) -> QualityMetrics:
        """
        Calculate comprehensive quality metrics for a dataset.

        Args:
            data: List of data records
            data_type: Type of data ('venue', 'event', 'prediction')

        Returns:
            QualityMetrics object with comprehensive quality assessment
        """
        if not data:
            return QualityMetrics(
                total_records=0,
                valid_records=0,
                completeness_score=0.0,
                accuracy_score=0.0,
                consistency_score=0.0,
                overall_score=0.0,
                validation_results=[],
                timestamp=datetime.now(),
            )

        all_results = []
        valid_count = 0

        # Validate each record
        for record in data:
            if data_type == "venue":
                is_valid, results = self.validate_venue(record)
            elif data_type == "event":
                is_valid, results = self.validate_event(record)
            elif data_type == "prediction":
                is_valid, results = self.validate_prediction(record)
            else:
                # Generic validation for unknown types
                is_valid, results = self._validate_generic_record(record)

            all_results.extend(results)
            if is_valid:
                valid_count += 1

        # Calculate quality scores
        completeness_score = self._calculate_completeness_score(data, data_type)
        accuracy_score = valid_count / len(data) if data else 0.0
        consistency_score = self._calculate_consistency_score(data, data_type)

        # Overall score is weighted average
        overall_score = (
            completeness_score * 0.3 + accuracy_score * 0.4 + consistency_score * 0.3
        )

        return QualityMetrics(
            total_records=len(data),
            valid_records=valid_count,
            completeness_score=completeness_score,
            accuracy_score=accuracy_score,
            consistency_score=consistency_score,
            overall_score=overall_score,
            validation_results=all_results,
            timestamp=datetime.now(),
        )

    # ========== PRIVATE VALIDATION METHODS ==========

    def _validate_required_fields(
        self, data: Dict, required_fields: List[str]
    ) -> List[ValidationResult]:
        """Validate that required fields are present and not empty"""
        results = []

        for field in required_fields:
            value = data.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                results.append(
                    ValidationResult(
                        is_valid=False,
                        field_name=field,
                        error_message=f"Required field '{field}' is missing or empty",
                        score=0.0,
                    )
                )
            else:
                results.append(
                    ValidationResult(is_valid=True, field_name=field, score=1.0)
                )

        return results

    def _validate_name(self, name: Any, field_name: str) -> ValidationResult:
        """Validate name fields"""
        if not isinstance(name, str):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"{field_name} must be a string",
                score=0.0,
            )

        name = name.strip()

        if len(name) < self.min_name_length:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"{field_name} too short (minimum {self.min_name_length} characters)",
                score=0.2,
            )

        if len(name) > self.max_name_length:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"{field_name} too long (maximum {self.max_name_length} characters)",
                score=0.3,
            )

        # Check for suspicious patterns
        if re.match(r"^[^a-zA-Z]*$", name):  # No letters at all
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"{field_name} should contain letters",
                score=0.4,
            )

        # Quality scoring based on length and content
        score = 1.0
        if len(name) < 5:
            score = 0.7
        elif len(name) > 100:
            score = 0.8

        return ValidationResult(is_valid=True, field_name=field_name, score=score)

    def _validate_description(self, description: Any) -> ValidationResult:
        """Validate description fields"""
        if not isinstance(description, str):
            return ValidationResult(
                is_valid=False,
                field_name="description",
                error_message="Description must be a string",
                score=0.0,
            )

        description = description.strip()

        if len(description) < self.min_description_length:
            return ValidationResult(
                is_valid=False,
                field_name="description",
                warning_message=f"Description is quite short ({len(description)} characters)",
                score=0.6,
            )

        if len(description) > self.max_description_length:
            return ValidationResult(
                is_valid=False,
                field_name="description",
                error_message=f"Description too long (maximum {self.max_description_length} characters)",
                score=0.3,
            )

        # Quality scoring
        score = 1.0
        if len(description) < 50:
            score = 0.7
        elif len(description) < 20:
            score = 0.5

        return ValidationResult(is_valid=True, field_name="description", score=score)

    def _validate_venue_category(self, category: Any) -> ValidationResult:
        """Validate venue category"""
        if not isinstance(category, str):
            return ValidationResult(
                is_valid=False,
                field_name="category",
                error_message="Category must be a string",
                score=0.0,
            )

        category = category.lower().strip()

        if category in self.valid_venue_categories:
            return ValidationResult(is_valid=True, field_name="category", score=1.0)
        else:
            return ValidationResult(
                is_valid=False,
                field_name="category",
                warning_message=f"Unknown venue category: {category}",
                score=0.5,
            )

    def _validate_event_category(self, category: Any) -> ValidationResult:
        """Validate event category"""
        if not isinstance(category, str):
            return ValidationResult(
                is_valid=False,
                field_name="category",
                error_message="Category must be a string",
                score=0.0,
            )

        category = category.lower().strip()

        if category in self.valid_event_categories:
            return ValidationResult(is_valid=True, field_name="category", score=1.0)
        else:
            return ValidationResult(
                is_valid=False,
                field_name="category",
                warning_message=f"Unknown event category: {category}",
                score=0.5,
            )

    def _validate_coordinates(self, lat: Any, lng: Any) -> List[ValidationResult]:
        """Validate latitude and longitude coordinates"""
        results = []

        # Validate latitude
        try:
            lat_float = float(lat)
            if -90 <= lat_float <= 90:
                # Check if within Kansas City area
                if self.kc_bounds["min_lat"] <= lat_float <= self.kc_bounds["max_lat"]:
                    score = 1.0
                else:
                    score = 0.7  # Valid but outside KC area

                results.append(
                    ValidationResult(is_valid=True, field_name="latitude", score=score)
                )
            else:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        field_name="latitude",
                        error_message=f"Latitude {lat_float} is out of valid range (-90 to 90)",
                        score=0.0,
                    )
                )
        except (ValueError, TypeError):
            results.append(
                ValidationResult(
                    is_valid=False,
                    field_name="latitude",
                    error_message=f"Invalid latitude value: {lat}",
                    score=0.0,
                )
            )

        # Validate longitude
        try:
            lng_float = float(lng)
            if -180 <= lng_float <= 180:
                # Check if within Kansas City area
                if self.kc_bounds["min_lng"] <= lng_float <= self.kc_bounds["max_lng"]:
                    score = 1.0
                else:
                    score = 0.7  # Valid but outside KC area

                results.append(
                    ValidationResult(is_valid=True, field_name="longitude", score=score)
                )
            else:
                results.append(
                    ValidationResult(
                        is_valid=False,
                        field_name="longitude",
                        error_message=f"Longitude {lng_float} is out of valid range (-180 to 180)",
                        score=0.0,
                    )
                )
        except (ValueError, TypeError):
            results.append(
                ValidationResult(
                    is_valid=False,
                    field_name="longitude",
                    error_message=f"Invalid longitude value: {lng}",
                    score=0.0,
                )
            )

        return results

    def _validate_phone(self, phone: Any) -> ValidationResult:
        """Validate phone number"""
        if not isinstance(phone, str):
            return ValidationResult(
                is_valid=False,
                field_name="phone",
                error_message="Phone must be a string",
                score=0.0,
            )

        # Clean phone number
        clean_phone = re.sub(r"[^\d]", "", phone)

        if len(clean_phone) == 10:
            return ValidationResult(is_valid=True, field_name="phone", score=1.0)
        elif len(clean_phone) == 11 and clean_phone.startswith("1"):
            return ValidationResult(is_valid=True, field_name="phone", score=1.0)
        else:
            return ValidationResult(
                is_valid=False,
                field_name="phone",
                warning_message=f"Phone number format may be invalid: {phone}",
                score=0.5,
            )

    def _validate_url(self, url: Any, field_name: str) -> ValidationResult:
        """Validate URL fields"""
        if not isinstance(url, str):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"{field_name} must be a string",
                score=0.0,
            )

        url = url.strip()

        # Basic URL pattern validation
        url_pattern = re.compile(
            r"^https?://"  # http:// or https://
            r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain...
            r"localhost|"  # localhost...
            r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
            r"(?::\d+)?"  # optional port
            r"(?:/?|[/?]\S+)$",
            re.IGNORECASE,
        )

        if url_pattern.match(url):
            score = 1.0 if url.startswith("https://") else 0.8
            return ValidationResult(is_valid=True, field_name=field_name, score=score)
        else:
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"Invalid URL format: {url}",
                score=0.0,
            )

    def _validate_rating(self, rating: Any) -> ValidationResult:
        """Validate rating values"""
        try:
            rating_float = float(rating)
            if 0 <= rating_float <= 5:
                return ValidationResult(is_valid=True, field_name="rating", score=1.0)
            else:
                return ValidationResult(
                    is_valid=False,
                    field_name="rating",
                    error_message=f"Rating {rating_float} is out of valid range (0-5)",
                    score=0.0,
                )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                field_name="rating",
                error_message=f"Invalid rating value: {rating}",
                score=0.0,
            )

    def _validate_probability(self, value: Any, field_name: str) -> ValidationResult:
        """Validate probability values (0-1)"""
        try:
            prob_float = float(value)
            if 0 <= prob_float <= 1:
                return ValidationResult(is_valid=True, field_name=field_name, score=1.0)
            else:
                return ValidationResult(
                    is_valid=False,
                    field_name=field_name,
                    error_message=f"{field_name} {prob_float} is out of valid range (0-1)",
                    score=0.0,
                )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                field_name=field_name,
                error_message=f"Invalid {field_name} value: {value}",
                score=0.0,
            )

    def _validate_event_datetime(self, datetime_value: Any) -> ValidationResult:
        """Validate event datetime"""
        if isinstance(datetime_value, datetime):
            # Check if event is not too far in the past or future
            now = datetime.now()
            days_diff = (datetime_value - now).days

            if days_diff < -30:  # More than 30 days in the past
                return ValidationResult(
                    is_valid=True,
                    field_name="start_time",
                    warning_message="Event is more than 30 days in the past",
                    score=0.6,
                )
            elif days_diff > 365:  # More than 1 year in the future
                return ValidationResult(
                    is_valid=True,
                    field_name="start_time",
                    warning_message="Event is more than 1 year in the future",
                    score=0.7,
                )
            else:
                return ValidationResult(
                    is_valid=True, field_name="start_time", score=1.0
                )
        else:
            return ValidationResult(
                is_valid=False,
                field_name="start_time",
                error_message=f"Invalid datetime format: {datetime_value}",
                score=0.0,
            )

    def _validate_event_duration(
        self, start_time: datetime, end_time: datetime
    ) -> ValidationResult:
        """Validate event duration"""
        try:
            duration = end_time - start_time
            duration_hours = duration.total_seconds() / 3600

            if duration_hours < 0:
                return ValidationResult(
                    is_valid=False,
                    field_name="event_duration",
                    error_message="End time is before start time",
                    score=0.0,
                )
            elif duration_hours > 24:
                return ValidationResult(
                    is_valid=True,
                    field_name="event_duration",
                    warning_message=f"Event duration is very long ({duration_hours:.1f} hours)",
                    score=0.7,
                )
            else:
                return ValidationResult(
                    is_valid=True, field_name="event_duration", score=1.0
                )
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                field_name="event_duration",
                error_message=f"Error validating event duration: {e}",
                score=0.0,
            )

    def _validate_psychographic_data(self, psychographic_data: Any) -> ValidationResult:
        """Validate psychographic relevance data"""
        if isinstance(psychographic_data, str):
            try:
                psychographic_data = json.loads(psychographic_data)
            except json.JSONDecodeError:
                return ValidationResult(
                    is_valid=False,
                    field_name="psychographic_relevance",
                    error_message="Invalid JSON format for psychographic data",
                    score=0.0,
                )

        if not isinstance(psychographic_data, dict):
            return ValidationResult(
                is_valid=False,
                field_name="psychographic_relevance",
                error_message="Psychographic data must be a dictionary",
                score=0.0,
            )

        # Validate psychographic scores
        valid_keys = {"career_driven", "competent", "fun", "social", "adventurous"}
        score = 1.0

        for key, value in psychographic_data.items():
            if key not in valid_keys:
                score *= 0.9  # Slight penalty for unknown keys

            try:
                float_value = float(value)
                if not (0 <= float_value <= 1):
                    score *= 0.7  # Penalty for out-of-range values
            except (ValueError, TypeError):
                score *= 0.5  # Penalty for non-numeric values

        return ValidationResult(
            is_valid=score > 0.3, field_name="psychographic_relevance", score=score
        )

    def _validate_model_version(self, model_version: Any) -> ValidationResult:
        """Validate ML model version"""
        if not isinstance(model_version, str):
            return ValidationResult(
                is_valid=False,
                field_name="model_version",
                error_message="Model version must be a string",
                score=0.0,
            )

        # Basic version pattern validation (e.g., v1.0, 2.1.3, etc.)
        version_pattern = re.compile(r"^v?\d+(\.\d+)*$")

        if version_pattern.match(model_version.strip()):
            return ValidationResult(
                is_valid=True, field_name="model_version", score=1.0
            )
        else:
            return ValidationResult(
                is_valid=False,
                field_name="model_version",
                warning_message=f"Unusual model version format: {model_version}",
                score=0.7,
            )

    def _validate_feature_list(self, features: Any) -> ValidationResult:
        """Validate ML feature list"""
        if isinstance(features, str):
            try:
                features = json.loads(features)
            except json.JSONDecodeError:
                return ValidationResult(
                    is_valid=False,
                    field_name="features_used",
                    error_message="Invalid JSON format for features list",
                    score=0.0,
                )

        if not isinstance(features, list):
            return ValidationResult(
                is_valid=False,
                field_name="features_used",
                error_message="Features must be a list",
                score=0.0,
            )

        if len(features) == 0:
            return ValidationResult(
                is_valid=False,
                field_name="features_used",
                warning_message="Empty features list",
                score=0.3,
            )

        # Check if all features are strings
        non_string_features = [f for f in features if not isinstance(f, str)]
        if non_string_features:
            return ValidationResult(
                is_valid=False,
                field_name="features_used",
                warning_message="Some features are not strings",
                score=0.6,
            )

        return ValidationResult(is_valid=True, field_name="features_used", score=1.0)

    def _validate_generic_record(
        self, record: Dict
    ) -> Tuple[bool, List[ValidationResult]]:
        """Generic validation for unknown record types"""
        results = []

        # Basic structure validation
        if not isinstance(record, dict):
            results.append(
                ValidationResult(
                    is_valid=False,
                    field_name="record_structure",
                    error_message="Record must be a dictionary",
                    score=0.0,
                )
            )
            return False, results

        if len(record) == 0:
            results.append(
                ValidationResult(
                    is_valid=False,
                    field_name="record_content",
                    error_message="Record is empty",
                    score=0.0,
                )
            )
            return False, results

        # Check for basic required fields
        if "name" in record:
            results.append(self._validate_name(record["name"], "name"))

        results.append(
            ValidationResult(
                is_valid=True,
                field_name="generic_validation",
                score=0.8,  # Lower score for generic validation
            )
        )

        is_valid = all(r.is_valid for r in results if r.is_valid is not None)
        return is_valid, results

    def _calculate_completeness_score(self, data: List[Dict], data_type: str) -> float:
        """Calculate data completeness score"""
        if not data:
            return 0.0

        # Define important fields for each data type
        important_fields = {
            "venue": ["name", "category", "lat", "lng", "address"],
            "event": ["name", "category", "start_time", "venue_name"],
            "prediction": ["venue_id", "prediction_value", "confidence_score"],
        }

        fields_to_check = important_fields.get(data_type, ["name"])
        total_possible = len(data) * len(fields_to_check)
        total_present = 0

        for record in data:
            for field in fields_to_check:
                value = record.get(field)
                if value is not None and (not isinstance(value, str) or value.strip()):
                    total_present += 1

        return total_present / total_possible if total_possible > 0 else 0.0

    def _calculate_consistency_score(self, data: List[Dict], data_type: str) -> float:
        """Calculate data consistency score"""
        if len(data) < 2:
            return 1.0  # Perfect consistency with 0-1 records

        consistency_checks = []

        # Check provider consistency
        providers = [
            record.get("provider") for record in data if record.get("provider")
        ]
        if providers:
            unique_providers = len(set(providers))
            provider_consistency = 1.0 - (unique_providers - 1) / len(providers)
            consistency_checks.append(provider_consistency)

        # Check category consistency
        categories = [
            record.get("category") for record in data if record.get("category")
        ]
        if categories:
            unique_categories = len(set(categories))
            category_consistency = 1.0 - (unique_categories - 1) / len(categories)
            consistency_checks.append(category_consistency)

        # Return average consistency score
        return (
            sum(consistency_checks) / len(consistency_checks)
            if consistency_checks
            else 1.0
        )


# Global quality validator instance
_quality_validator = None


def get_quality_validator() -> QualityValidator:
    """Get the global quality validator instance"""
    global _quality_validator
    if _quality_validator is None:
        _quality_validator = QualityValidator()
    return _quality_validator
