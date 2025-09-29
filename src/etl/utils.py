# etl/utils.py
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:@localhost:5432/ppm")


def get_db_conn():
    if not DB_DSN:
        raise ValueError("DATABASE_URL not configured")
    return psycopg2.connect(DB_DSN)


def safe_request(url, headers=None, params=None):
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()
