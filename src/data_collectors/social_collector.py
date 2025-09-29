# Social Sentiment Data Collector
"""
Standardized social sentiment data collector that consolidates social data collection
from ingest_social.py into a unified collector with consistent interfaces.
"""

import sys
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from etl.utils import get_db_conn
    from master_data_service.quality_controller import QualityController
except ImportError as e:
    logging.warning(f"Could not import some modules: {e}")


@dataclass
class SocialCollectionResult:
    """Result of social sentiment data collection operation"""

    source_name: str
    success: bool
    records_collected: int
    duration_seconds: float
    error_message: Optional[str] = None
    data_quality_score: Optional[float] = None


@dataclass
class SocialSentimentData:
    """Standardized social sentiment data structure"""

    venue_id: Optional[str]
    event_id: Optional[str]
    timestamp: datetime
    platform: str  # 'twitter', 'facebook', 'instagram'
    mention_count: int
    positive_sentiment: float
    negative_sentiment: float
    neutral_sentiment: float
    engagement_score: float
    psychographic_keywords: List[str]
    raw_data: Optional[Dict]  # Store original API response
    collected_at: datetime


class SocialCollector:
    """
    Standardized social sentiment data collector.

    Consolidates social sentiment data collection functionality from ingest_social.py
    into a unified collector with consistent data quality processing.
    """

    def __init__(self):
        """Initialize the social collector."""
        self.logger = logging.getLogger(__name__)
        self.quality_controller = QualityController()

        # Social media platforms to collect from
        self.platforms = ["twitter", "facebook", "instagram"]

        # Psychographic keywords for classification
        self.psychographic_keywords = {
            "career_driven": [
                "networking",
                "professional",
                "business",
                "career",
                "corporate",
                "conference",
                "seminar",
                "workshop",
                "leadership",
                "entrepreneur",
            ],
            "competent": [
                "expert",
                "masterclass",
                "training",
                "certification",
                "skill",
                "advanced",
                "professional",
                "education",
                "learning",
                "development",
            ],
            "fun": [
                "party",
                "celebration",
                "festival",
                "concert",
                "nightlife",
                "drinks",
                "social",
                "entertainment",
                "music",
                "dance",
                "fun",
                "exciting",
                "amazing",
                "awesome",
                "love",
            ],
        }

        # API configuration would go here
        self.api_timeout = 15

    def collect_data(
        self,
        area_bounds: Optional[Dict] = None,
        time_period: Optional[timedelta] = None,
    ) -> SocialCollectionResult:
        """
        Collect social sentiment data for specified area and time period.

        Args:
            area_bounds: Geographic bounds for collection (defaults to KC)
            time_period: Time period for collection (defaults to last 24 hours)

        Returns:
            SocialCollectionResult with collection status and metrics
        """
        start_time = datetime.now()
        self.logger.info("📱 Starting social sentiment data collection")

        try:
            # Use default time period if not provided
            if time_period is None:
                time_period = timedelta(hours=24)

            # Collect social sentiment data from all platforms
            all_sentiment_records = []

            for platform in self.platforms:
                platform_records = self._fetch_platform_sentiment(
                    platform, area_bounds, time_period
                )
                all_sentiment_records.extend(platform_records)

            if all_sentiment_records:
                # Validate and process data
                validated_records = self._validate_sentiment_data(all_sentiment_records)

                # Store in database
                stored_count = self._upsert_sentiment_to_db(validated_records)

                duration = (datetime.now() - start_time).total_seconds()

                result = SocialCollectionResult(
                    source_name="social_sentiment",
                    success=True,
                    records_collected=stored_count,
                    duration_seconds=duration,
                    data_quality_score=0.7,  # Social data quality varies
                )

                self.logger.info(
                    f"✅ Social sentiment collection completed: {stored_count} records in {duration:.1f}s"
                )
                return result
            else:
                duration = (datetime.now() - start_time).total_seconds()
                result = SocialCollectionResult(
                    source_name="social_sentiment",
                    success=False,
                    records_collected=0,
                    duration_seconds=duration,
                    error_message="No social sentiment data retrieved",
                )

                self.logger.warning("⚠️ No social sentiment data retrieved")
                return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            result = SocialCollectionResult(
                source_name="social_sentiment",
                success=False,
                records_collected=0,
                duration_seconds=duration,
                error_message=str(e),
            )

            self.logger.error(f"❌ Social sentiment collection failed: {e}")
            return result

    def _fetch_platform_sentiment(
        self, platform: str, area_bounds: Optional[Dict], time_period: timedelta
    ) -> List[SocialSentimentData]:
        """
        Fetch sentiment data from a specific social media platform.

        Args:
            platform: Social media platform ('twitter', 'facebook', 'instagram')
            area_bounds: Geographic bounds for collection
            time_period: Time period for collection

        Returns:
            List of SocialSentimentData objects
        """
        self.logger.info(f"Fetching sentiment data from {platform}")

        try:
            # This is a placeholder implementation
            # In the real implementation, this would call the actual social media APIs
            # For now, we'll create mock data to demonstrate the structure

            # Mock sentiment data (replace with actual API calls)
            mock_sentiment = SocialSentimentData(
                venue_id=None,  # Would be linked to venues later
                event_id=None,  # Would be linked to events later
                timestamp=datetime.now(),
                platform=platform,
                mention_count=15,
                positive_sentiment=0.6,
                negative_sentiment=0.2,
                neutral_sentiment=0.2,
                engagement_score=0.75,
                psychographic_keywords=self._extract_psychographic_keywords(
                    "Great networking event in Kansas City!"
                ),
                raw_data={"mock": True, "platform": platform},
                collected_at=datetime.now(),
            )

            return [mock_sentiment]

        except Exception as e:
            self.logger.error(f"Error fetching sentiment data from {platform}: {e}")
            return []

    def _extract_psychographic_keywords(self, text: str) -> List[str]:
        """
        Extract psychographic keywords from text content.

        Args:
            text: Text content to analyze

        Returns:
            List of relevant psychographic keywords
        """
        if not text:
            return []

        text_lower = text.lower()
        found_keywords = []

        for category, keywords in self.psychographic_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    found_keywords.append(keyword)

        return list(set(found_keywords))  # Remove duplicates

    def _validate_sentiment_data(
        self, sentiment_records: List[SocialSentimentData]
    ) -> List[SocialSentimentData]:
        """
        Validate and clean social sentiment data.

        Args:
            sentiment_records: Raw sentiment data records

        Returns:
            Validated sentiment data records
        """
        validated_records = []

        for record in sentiment_records:
            try:
                # Basic validation
                if not self._is_valid_sentiment_record(record):
                    self.logger.warning(
                        f"Invalid sentiment record skipped: {record.platform}"
                    )
                    continue

                # Data cleaning/normalization
                record = self._normalize_sentiment_record(record)
                validated_records.append(record)

            except Exception as e:
                self.logger.error(f"Error validating sentiment record: {e}")
                continue

        self.logger.info(
            f"Validated {len(validated_records)} of {len(sentiment_records)} sentiment records"
        )
        return validated_records

    def _is_valid_sentiment_record(self, record: SocialSentimentData) -> bool:
        """
        Check if a sentiment record is valid.

        Args:
            record: SocialSentimentData record to validate

        Returns:
            True if record is valid, False otherwise
        """
        # Check required fields
        if not record.timestamp or not record.platform:
            return False

        # Check sentiment scores are valid probabilities
        total_sentiment = (
            record.positive_sentiment
            + record.negative_sentiment
            + record.neutral_sentiment
        )
        if not (0.9 <= total_sentiment <= 1.1):  # Allow small floating point errors
            return False

        # Check engagement score is valid
        if not (0 <= record.engagement_score <= 1):
            return False

        # Check mention count is non-negative
        if record.mention_count < 0:
            return False

        return True

    def _normalize_sentiment_record(
        self, record: SocialSentimentData
    ) -> SocialSentimentData:
        """
        Normalize sentiment record data.

        Args:
            record: SocialSentimentData record to normalize

        Returns:
            Normalized sentiment record
        """
        # Normalize sentiment scores to sum to 1.0
        total_sentiment = (
            record.positive_sentiment
            + record.negative_sentiment
            + record.neutral_sentiment
        )
        if total_sentiment > 0:
            record.positive_sentiment = record.positive_sentiment / total_sentiment
            record.negative_sentiment = record.negative_sentiment / total_sentiment
            record.neutral_sentiment = record.neutral_sentiment / total_sentiment

        # Ensure engagement score is within bounds
        record.engagement_score = max(0.0, min(1.0, record.engagement_score))

        return record

    def _upsert_sentiment_to_db(
        self, sentiment_records: List[SocialSentimentData]
    ) -> int:
        """
        Insert or update sentiment records in the database.

        Args:
            sentiment_records: List of validated sentiment records

        Returns:
            Number of records successfully stored
        """
        if not sentiment_records:
            return 0

        conn = get_db_conn()
        if not conn:
            self.logger.error("Could not connect to database")
            return 0

        cur = conn.cursor()
        stored_count = 0

        try:
            for record in sentiment_records:
                # Check if record already exists (avoid duplicates)
                cur.execute(
                    """
                    SELECT id FROM social_sentiment 
                    WHERE ts = %s AND platform = %s AND venue_id IS NULL AND event_id IS NULL
                """,
                    (record.timestamp, record.platform),
                )

                existing_record = cur.fetchone()

                if existing_record:
                    # Update existing record
                    cur.execute(
                        """
                        UPDATE social_sentiment SET
                            mention_count = %s,
                            positive_sentiment = %s,
                            negative_sentiment = %s,
                            neutral_sentiment = %s,
                            engagement_score = %s,
                            psychographic_keywords = %s
                        WHERE id = %s
                    """,
                        (
                            record.mention_count,
                            record.positive_sentiment,
                            record.negative_sentiment,
                            record.neutral_sentiment,
                            record.engagement_score,
                            record.psychographic_keywords,
                            existing_record[0],
                        ),
                    )
                else:
                    # Insert new record
                    cur.execute(
                        """
                        INSERT INTO social_sentiment (
                            venue_id, event_id, ts, platform, mention_count,
                            positive_sentiment, negative_sentiment, neutral_sentiment,
                            engagement_score, psychographic_keywords
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """,
                        (
                            record.venue_id,
                            record.event_id,
                            record.timestamp,
                            record.platform,
                            record.mention_count,
                            record.positive_sentiment,
                            record.negative_sentiment,
                            record.neutral_sentiment,
                            record.engagement_score,
                            record.psychographic_keywords,
                        ),
                    )

                stored_count += 1

            conn.commit()
            self.logger.info(f"Successfully stored {stored_count} sentiment records")

        except Exception as e:
            self.logger.error(f"Error storing sentiment data: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

        return stored_count

    def get_venue_sentiment(self, venue_id: str, days_back: int = 7) -> Optional[Dict]:
        """
        Get aggregated sentiment data for a specific venue.

        Args:
            venue_id: Venue ID to get sentiment for
            days_back: Number of days to look back

        Returns:
            Aggregated sentiment data or None
        """
        conn = get_db_conn()
        if not conn:
            return None

        cur = conn.cursor()

        try:
            # Get aggregated sentiment for venue
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_mentions,
                    AVG(positive_sentiment) as avg_positive,
                    AVG(negative_sentiment) as avg_negative,
                    AVG(neutral_sentiment) as avg_neutral,
                    AVG(engagement_score) as avg_engagement,
                    array_agg(DISTINCT unnest(psychographic_keywords)) as all_keywords
                FROM social_sentiment
                WHERE venue_id = %s 
                AND ts >= NOW() - INTERVAL '%s days'
            """,
                (venue_id, days_back),
            )

            result = cur.fetchone()
            if result and result[0] > 0:
                return {
                    "total_mentions": result[0],
                    "avg_positive_sentiment": result[1],
                    "avg_negative_sentiment": result[2],
                    "avg_neutral_sentiment": result[3],
                    "avg_engagement_score": result[4],
                    "psychographic_keywords": result[5] or [],
                }

            return None

        except Exception as e:
            self.logger.error(f"Error retrieving venue sentiment: {e}")
            return None
        finally:
            cur.close()
            conn.close()


# Convenience functions for backward compatibility
def ingest_social_sentiment_data():
    """Convenience function to collect social sentiment data."""
    collector = SocialCollector()
    result = collector.collect_data()
    return result.success


if __name__ == "__main__":
    # Test the social collector
    logging.basicConfig(level=logging.INFO)

    collector = SocialCollector()

    # Test data collection
    print("Testing social sentiment data collection...")
    result = collector.collect_data()

    status = "✅" if result.success else "❌"
    print(
        f"{status} Social sentiment collection: {result.records_collected} records in {result.duration_seconds:.1f}s"
    )

    if result.error_message:
        print(f"Error: {result.error_message}")
