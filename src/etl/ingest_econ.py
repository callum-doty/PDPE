# etl/ingest_econ.py
import os
import logging
from datetime import datetime, timedelta
import requests
from etl.utils import safe_request, get_db_conn

# API Keys
ECONOMIC_DATA_API_KEY = os.getenv("ECONOMIC_DATA_API_KEY")
BUSINESS_NEWS_API_KEY = os.getenv("BUSINESS_NEWS_API_KEY")

# Kansas City Metro area identifiers
KC_METRO_FIPS = "28140"  # Kansas City, MO-KS Metropolitan Statistical Area
KC_COUNTIES = ["Jackson", "Clay", "Platte", "Cass"]  # Major KC metro counties
KC_ZIP_CODES = [
    "64111",
    "64112",
    "64108",
    "64105",
    "64106",
    "64109",
    "64110",
]  # Core KC zip codes


def fetch_fred_economic_data(series_id, start_date=None, end_date=None):
    """
    Fetch economic data from FRED (Federal Reserve Economic Data) API

    Args:
        series_id (str): FRED series identifier
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        dict: FRED API response
    """
    if not ECONOMIC_DATA_API_KEY:
        logging.error("ECONOMIC_DATA_API_KEY not set - cannot fetch economic data")
        return None

    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": ECONOMIC_DATA_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "frequency": "m",  # Monthly data
        "sort_order": "desc",
    }

    try:
        logging.info(f"Fetching FRED data for series: {series_id}")
        return safe_request(url, params=params)
    except Exception as e:
        logging.error(f"Failed to fetch FRED data for {series_id}: {e}")
        return None


def fetch_bls_unemployment_data(area_code=None):
    """
    Fetch unemployment data from Bureau of Labor Statistics

    Args:
        area_code (str): BLS area code for Kansas City metro

    Returns:
        dict: BLS API response
    """
    # Kansas City-Overland Park-Kansas City, MO-KS Metropolitan Statistical Area
    kc_area_code = area_code or "28140"

    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    # Unemployment rate series for KC metro
    series_id = f"LAUMT{kc_area_code}03"

    headers = {"Content-Type": "application/json"}
    data = {
        "seriesid": [series_id],
        "startyear": str(datetime.now().year - 1),
        "endyear": str(datetime.now().year),
        "registrationkey": ECONOMIC_DATA_API_KEY,  # Use same key for BLS if available
    }

    try:
        logging.info(f"Fetching BLS unemployment data for KC metro: {kc_area_code}")
        response = requests.post(url, json=data, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch BLS unemployment data: {e}")
        return None


def fetch_business_news_sentiment():
    """
    Fetch business news sentiment for Kansas City area

    Returns:
        dict: News API response with business sentiment
    """
    if not BUSINESS_NEWS_API_KEY:
        logging.error("BUSINESS_NEWS_API_KEY not set - cannot fetch business news")
        return None

    url = "https://newsapi.org/v2/everything"
    params = {
        "apiKey": BUSINESS_NEWS_API_KEY,
        "q": "Kansas City business OR KC economy OR Kansas City jobs",
        "language": "en",
        "sortBy": "publishedAt",
        "from": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "to": datetime.now().strftime("%Y-%m-%d"),
        "pageSize": 50,
    }

    try:
        logging.info("Fetching Kansas City business news sentiment")
        return safe_request(url, params=params)
    except Exception as e:
        logging.error(f"Failed to fetch business news: {e}")
        return None


def analyze_business_sentiment(news_articles):
    """
    Analyze sentiment of business news articles

    Args:
        news_articles (list): List of news articles

    Returns:
        dict: Sentiment analysis results
    """
    if not news_articles:
        return {"confidence": 0, "sentiment_score": 0, "article_count": 0}

    try:
        from textblob import TextBlob

        sentiment_scores = []
        for article in news_articles:
            title = article.get("title", "")
            description = article.get("description", "")
            text = f"{title} {description}"

            if text.strip():
                blob = TextBlob(text)
                sentiment_scores.append(blob.sentiment.polarity)

        if sentiment_scores:
            avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
            confidence = min(
                len(sentiment_scores) / 10.0, 1.0
            )  # Confidence based on article count

            return {
                "confidence": confidence,
                "sentiment_score": avg_sentiment,  # -1 to 1 scale
                "article_count": len(sentiment_scores),
            }
    except ImportError:
        logging.warning("TextBlob not available for sentiment analysis")
    except Exception as e:
        logging.error(f"Failed to analyze business sentiment: {e}")

    return {"confidence": 0, "sentiment_score": 0, "article_count": 0}


def fetch_consumer_spending_proxy():
    """
    Fetch consumer spending proxy data (retail sales, consumer confidence)

    Returns:
        dict: Consumer spending indicators
    """
    spending_data = {}

    # Retail sales data (national, as local data may not be available)
    retail_sales = fetch_fred_economic_data("RSAFS")  # Advance Retail Sales
    if retail_sales and "observations" in retail_sales:
        latest_retail = (
            retail_sales["observations"][0] if retail_sales["observations"] else None
        )
        if latest_retail and latest_retail.get("value") != ".":
            spending_data["retail_sales_index"] = float(latest_retail["value"])
            spending_data["retail_sales_date"] = latest_retail["date"]

    # Consumer confidence (national)
    consumer_confidence = fetch_fred_economic_data(
        "UMCSENT"
    )  # University of Michigan Consumer Sentiment
    if consumer_confidence and "observations" in consumer_confidence:
        latest_confidence = (
            consumer_confidence["observations"][0]
            if consumer_confidence["observations"]
            else None
        )
        if latest_confidence and latest_confidence.get("value") != ".":
            spending_data["consumer_confidence"] = float(latest_confidence["value"])
            spending_data["consumer_confidence_date"] = latest_confidence["date"]

    return spending_data


def process_economic_indicators():
    """
    Process and aggregate various economic indicators for Kansas City

    Returns:
        dict: Processed economic indicators
    """
    economic_data = {
        "ts": datetime.now(),
        "geographic_area": "Kansas City Metro",
        "unemployment_rate": None,
        "median_household_income": None,
        "business_openings": None,
        "business_closures": None,
        "consumer_confidence": None,
        "local_spending_index": None,
    }

    # Fetch unemployment data
    unemployment_data = fetch_bls_unemployment_data()
    if unemployment_data and "Results" in unemployment_data:
        series_data = unemployment_data["Results"].get("series", [])
        if series_data and series_data[0].get("data"):
            latest_unemployment = series_data[0]["data"][0]
            if latest_unemployment.get("value"):
                economic_data["unemployment_rate"] = float(latest_unemployment["value"])

    # Fetch consumer spending proxy data
    spending_data = fetch_consumer_spending_proxy()
    if spending_data:
        economic_data["consumer_confidence"] = spending_data.get("consumer_confidence")
        economic_data["local_spending_index"] = spending_data.get("retail_sales_index")

    # Fetch business sentiment from news
    news_data = fetch_business_news_sentiment()
    business_sentiment = {"confidence": 0, "sentiment_score": 0}
    if news_data and "articles" in news_data:
        business_sentiment = analyze_business_sentiment(news_data["articles"])

    # Use business sentiment as a proxy for business health
    if (
        business_sentiment["confidence"] > 0.3
    ):  # Only use if we have reasonable confidence
        # Convert sentiment (-1 to 1) to business health score (0 to 100)
        business_health = (business_sentiment["sentiment_score"] + 1) * 50
        economic_data["business_health_score"] = business_health

    return economic_data


def upsert_economic_data_to_db(economic_data):
    """
    Insert or update economic data in the database

    Args:
        economic_data (dict): Processed economic indicators
    """
    if not economic_data:
        return

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        insert_query = """
        INSERT INTO economic_data (
            ts, geographic_area, unemployment_rate, median_household_income,
            business_openings, business_closures, consumer_confidence, local_spending_index
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        cur.execute(
            insert_query,
            (
                economic_data["ts"],
                economic_data["geographic_area"],
                economic_data["unemployment_rate"],
                economic_data["median_household_income"],
                economic_data["business_openings"],
                economic_data["business_closures"],
                economic_data["consumer_confidence"],
                economic_data["local_spending_index"],
            ),
        )

        conn.commit()
        logging.info(f"Inserted economic data for {economic_data['geographic_area']}")

    except Exception as e:
        logging.error(f"Failed to insert economic data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def fetch_zip_code_economic_data(zip_codes=None):
    """
    Fetch economic data for specific Kansas City zip codes

    Args:
        zip_codes (list): List of zip codes to process

    Returns:
        list: Economic data for each zip code
    """
    if not zip_codes:
        zip_codes = KC_ZIP_CODES

    zip_economic_data = []

    for zip_code in zip_codes:
        try:
            # This would typically use Census API or other zip-code level data sources
            # For now, we'll create a placeholder structure
            zip_data = {
                "ts": datetime.now(),
                "geographic_area": zip_code,
                "unemployment_rate": None,
                "median_household_income": None,
                "business_openings": None,
                "business_closures": None,
                "consumer_confidence": None,
                "local_spending_index": None,
            }

            # In a real implementation, you would fetch zip-code specific data here
            # using Census API, local government APIs, or commercial data providers

            zip_economic_data.append(zip_data)
            logging.info(f"Processed economic data for zip code: {zip_code}")

        except Exception as e:
            logging.error(f"Failed to process zip code {zip_code}: {e}")

    return zip_economic_data


def ingest_economic_indicators():
    """
    Main function to ingest economic indicators for Kansas City
    """
    logging.info("Starting economic indicators ingestion")

    try:
        # Process metro-level economic data
        metro_economic_data = process_economic_indicators()
        if metro_economic_data:
            upsert_economic_data_to_db(metro_economic_data)

        # Process zip-code level economic data
        zip_economic_data = fetch_zip_code_economic_data()
        for zip_data in zip_economic_data:
            if zip_data:
                upsert_economic_data_to_db(zip_data)

        logging.info("Economic indicators ingestion completed successfully")

    except Exception as e:
        logging.error(f"Economic indicators ingestion failed: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_economic_indicators()
