import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

DB_PATH = BASE_DIR / "data" / "psycho_demo_mvp.db"
SQL_SCHEMA_PATH = BASE_DIR / "psycho_demo_mvp.sql"

# Put your API keys here when ready
EVENTBRITE_API_KEY = os.getenv("EVENTBRITE_API_KEY", "INSERT_YOUR_KEY")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "INSERT_YOUR_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "INSERT_YOUR_KEY")

# City / bounding box settings
CITY_NAME = "Kansas City"
DEFAULT_SEARCH_RADIUS_M = 15000  # meters
