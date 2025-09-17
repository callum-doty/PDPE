# etl/ingest_social.py
import os
import logging
import re
import random
from datetime import datetime, timedelta
from etl.utils import get_db_conn

# Try to import optional dependencies
try:
    from textblob import TextBlob

    HAS_TEXTBLOB = True
except ImportError:
    HAS_TEXTBLOB = False
    logging.warning("TextBlob not available - using simple sentiment analysis")

try:
    import tweepy

    HAS_TWEEPY = True
except ImportError:
    HAS_TWEEPY = False
    logging.warning("Tweepy not available - using mock Twitter data")

# API Keys
TWITTER_API_KEY = os.getenv("TWITTER_API_KEY")
FACEBOOK_API_KEY = os.getenv("FACEBOOK_API_KEY")

# Psychographic keywords for classification
PSYCHOGRAPHIC_KEYWORDS = {
    "career_driven": [
        "career",
        "professional",
        "networking",
        "business",
        "corporate",
        "executive",
        "leadership",
        "entrepreneur",
    ],
    "competent": [
        "skilled",
        "expert",
        "qualified",
        "experienced",
        "accomplished",
        "certified",
        "trained",
        "educated",
    ],
    "fun": [
        "fun",
        "party",
        "celebration",
        "entertainment",
        "social",
        "friends",
        "weekend",
        "nightlife",
        "drinks",
    ],
}

KC_HASHTAGS = [
    "#KC",
    "#KansasCity",
    "#KCMO",
    "#PowerAndLight",
    "#Plaza",
    "#Crossroads",
    "#WestportKC",
]
KC_LOCATIONS = [
    "Kansas City",
    "KC",
    "Power and Light",
    "Country Club Plaza",
    "Crossroads",
    "Westport",
]


def setup_twitter_api():
    """Initialize Twitter API client"""
    if not TWITTER_API_KEY:
        logging.error("TWITTER_API_KEY not set - cannot fetch Twitter data")
        return None

    if not HAS_TWEEPY:
        logging.error("Tweepy not installed - cannot fetch Twitter data")
        return None

    try:
        # Try Bearer Token authentication first (Twitter API v2)
        if len(TWITTER_API_KEY) > 50:  # Bearer tokens are longer
            client = tweepy.Client(
                bearer_token=TWITTER_API_KEY, wait_on_rate_limit=True
            )
            return client
        else:
            # Fallback to API v1.1 with consumer key (requires consumer secret)
            # For now, we'll skip Twitter if we don't have proper bearer token
            logging.warning(
                "Twitter API key appears to be consumer key, not bearer token. Skipping Twitter integration."
            )
            return None
    except Exception as e:
        logging.error(f"Failed to setup Twitter API: {e}")
        return None


def extract_psychographic_keywords(text):
    """Extract psychographic keywords from text and return scores"""
    text_lower = text.lower()
    scores = {}

    for category, keywords in PSYCHOGRAPHIC_KEYWORDS.items():
        count = sum(1 for keyword in keywords if keyword in text_lower)
        scores[category] = count

    return scores


def analyze_sentiment(text):
    """Analyze sentiment using TextBlob"""
    try:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity  # -1 to 1

        # Convert to positive/negative/neutral scores
        if polarity > 0.1:
            return {"positive": polarity, "negative": 0, "neutral": 0}
        elif polarity < -0.1:
            return {"positive": 0, "negative": abs(polarity), "neutral": 0}
        else:
            return {"positive": 0, "negative": 0, "neutral": 1}
    except Exception as e:
        logging.error(f"Sentiment analysis failed: {e}")
        return {"positive": 0, "negative": 0, "neutral": 1}


def fetch_twitter_mentions(
    venue_name=None, hashtags=None, location_query=None, days_back=1
):
    """
    Fetch Twitter mentions for venues/events in Kansas City

    Args:
        venue_name (str): Specific venue name to search for
        hashtags (list): List of hashtags to search
        location_query (str): Location-based search query
        days_back (int): Number of days to look back

    Returns:
        list: List of tweet data
    """
    client = setup_twitter_api()
    if not client:
        return []

    tweets_data = []

    try:
        # Build search query
        query_parts = []

        if venue_name:
            query_parts.append(f'"{venue_name}"')

        if hashtags:
            query_parts.extend(hashtags)
        else:
            query_parts.extend(KC_HASHTAGS[:3])  # Use default KC hashtags

        if location_query:
            query_parts.append(location_query)
        else:
            query_parts.append("Kansas City")

        query = " OR ".join(query_parts)

        # Search tweets using Twitter API v2
        start_time = (datetime.now() - timedelta(days=days_back)).isoformat()

        tweets = client.search_recent_tweets(
            query=query,
            start_time=start_time,
            max_results=100,
            tweet_fields=["created_at", "public_metrics", "author_id"],
            user_fields=["public_metrics"],
        )

        if tweets.data:
            for tweet in tweets.data:
                sentiment = analyze_sentiment(tweet.text)
                psychographic_keywords = extract_psychographic_keywords(tweet.text)

                # Get metrics safely
                metrics = tweet.public_metrics or {}
                retweet_count = metrics.get("retweet_count", 0)
                like_count = metrics.get("like_count", 0)

                tweet_data = {
                    "platform": "twitter",
                    "text": tweet.text,
                    "created_at": tweet.created_at,
                    "user_followers": 0,  # Would need additional API call to get user info
                    "retweet_count": retweet_count,
                    "favorite_count": like_count,
                    "sentiment": sentiment,
                    "psychographic_keywords": psychographic_keywords,
                    "engagement_score": retweet_count + like_count,
                }
                tweets_data.append(tweet_data)

    except Exception as e:
        logging.error(f"Failed to fetch Twitter data: {e}")

    return tweets_data


def fetch_facebook_mentions(venue_name=None, location="Kansas City", days_back=1):
    """
    Fetch Facebook mentions (simplified - requires proper Facebook Graph API setup)

    Args:
        venue_name (str): Venue name to search for
        location (str): Location to search in
        days_back (int): Number of days to look back

    Returns:
        list: List of Facebook post data
    """
    if not FACEBOOK_API_KEY:
        logging.error("FACEBOOK_API_KEY not set - cannot fetch Facebook data")
        return []

    # Note: This is a simplified implementation
    # Full Facebook Graph API integration would require proper app setup and permissions
    facebook_data = []

    try:
        # Placeholder for Facebook Graph API calls
        # In a real implementation, you would use the Facebook Graph API to search for posts
        # mentioning venues in Kansas City
        logging.info(
            "Facebook API integration placeholder - requires full Graph API setup"
        )

    except Exception as e:
        logging.error(f"Failed to fetch Facebook data: {e}")

    return facebook_data


def aggregate_social_data(social_mentions, venue_id=None, event_id=None):
    """
    Aggregate social media mentions into summary statistics

    Args:
        social_mentions (list): List of social media mentions
        venue_id (str): UUID of venue if applicable
        event_id (str): UUID of event if applicable

    Returns:
        dict: Aggregated social data
    """
    if not social_mentions:
        return None

    total_mentions = len(social_mentions)
    total_engagement = sum(
        mention.get("engagement_score", 0) for mention in social_mentions
    )

    # Aggregate sentiment
    positive_sum = sum(
        mention["sentiment"].get("positive", 0) for mention in social_mentions
    )
    negative_sum = sum(
        mention["sentiment"].get("negative", 0) for mention in social_mentions
    )
    neutral_sum = sum(
        mention["sentiment"].get("neutral", 0) for mention in social_mentions
    )

    # Aggregate psychographic keywords
    psychographic_totals = {}
    for mention in social_mentions:
        for category, count in mention.get("psychographic_keywords", {}).items():
            psychographic_totals[category] = (
                psychographic_totals.get(category, 0) + count
            )

    platforms = list(set(mention.get("platform") for mention in social_mentions))

    return {
        "venue_id": venue_id,
        "event_id": event_id,
        "ts": datetime.now(),
        "platforms": platforms,
        "mention_count": total_mentions,
        "positive_sentiment": (
            positive_sum / total_mentions if total_mentions > 0 else 0
        ),
        "negative_sentiment": (
            negative_sum / total_mentions if total_mentions > 0 else 0
        ),
        "neutral_sentiment": neutral_sum / total_mentions if total_mentions > 0 else 0,
        "engagement_score": total_engagement,
        "psychographic_keywords": list(psychographic_totals.keys()),
    }


def upsert_social_sentiment_to_db(social_data):
    """
    Insert or update social sentiment data in the database

    Args:
        social_data (dict): Aggregated social sentiment data
    """
    if not social_data:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        insert_query = """
        INSERT INTO social_sentiment (
            venue_id, event_id, ts, platform, mention_count,
            positive_sentiment, negative_sentiment, neutral_sentiment,
            engagement_score, psychographic_keywords
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        platform_str = (
            ",".join(social_data["platforms"]) if social_data["platforms"] else "mixed"
        )

        cur.execute(
            insert_query,
            (
                social_data.get("venue_id"),
                social_data.get("event_id"),
                social_data["ts"],
                platform_str,
                social_data["mention_count"],
                social_data["positive_sentiment"],
                social_data["negative_sentiment"],
                social_data["neutral_sentiment"],
                social_data["engagement_score"],
                social_data["psychographic_keywords"],
            ),
        )

        conn.commit()
        logging.info(
            f"Inserted social sentiment data: {social_data['mention_count']} mentions"
        )

    except Exception as e:
        logging.error(f"Failed to insert social sentiment data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def ingest_social_data_for_venues():
    """
    Main function to ingest social data for all venues in the system
    """
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # Get all venues from database
        cur.execute("SELECT venue_id, name FROM venues WHERE name IS NOT NULL")
        venues = cur.fetchall()

        logging.info(f"Processing social data for {len(venues)} venues")

        for venue_id, venue_name in venues:
            logging.info(f"Processing social data for venue: {venue_name}")

            # Fetch social mentions for this venue
            twitter_mentions = fetch_twitter_mentions(
                venue_name=venue_name, days_back=1
            )
            facebook_mentions = fetch_facebook_mentions(
                venue_name=venue_name, days_back=1
            )

            all_mentions = twitter_mentions + facebook_mentions

            if all_mentions:
                # Aggregate and store social data
                social_data = aggregate_social_data(all_mentions, venue_id=venue_id)
                upsert_social_sentiment_to_db(social_data)
            else:
                logging.info(f"No social mentions found for {venue_name}")

    except Exception as e:
        logging.error(f"Failed to process venues for social data: {e}")
    finally:
        cur.close()
        conn.close()


def ingest_general_kc_social_data():
    """
    Ingest general Kansas City social media data (not venue-specific)
    """
    logging.info("Processing general Kansas City social media data")

    # Fetch general KC mentions
    twitter_mentions = fetch_twitter_mentions(hashtags=KC_HASHTAGS, days_back=1)
    facebook_mentions = fetch_facebook_mentions(location="Kansas City", days_back=1)

    all_mentions = twitter_mentions + facebook_mentions

    if all_mentions:
        # Aggregate and store general social data
        social_data = aggregate_social_data(all_mentions)
        upsert_social_sentiment_to_db(social_data)
        logging.info(f"Processed {len(all_mentions)} general KC social mentions")
    else:
        logging.info("No general KC social mentions found")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Ingest social data for specific venues
    ingest_social_data_for_venues()

    # Ingest general Kansas City social data
    ingest_general_kc_social_data()

    logging.info("Social data ingestion completed")
