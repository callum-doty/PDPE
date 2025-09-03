# etl/utils.py
import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
import logging

DB_DSN = os.getenv("DATABASE_URL")  # e.g. postgres://user:pass@host:5432/dbname


def get_db_conn():
    return psycopg2.connect(DB_DSN)


def safe_request(url, headers=None, params=None):
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()
