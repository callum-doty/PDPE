"""
Database connection utilities for the PPM application.
"""

import os
import sqlite3
import psycopg2
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection manager supporting SQLite and PostgreSQL"""

    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv(
            "DATABASE_URL", "sqlite:///ppm.db"
        )
        self.connection = None
        self.db_type = None

    def connect(self):
        """Establish database connection"""
        try:
            if self.database_url.startswith("sqlite"):
                db_path = self.database_url.replace("sqlite:///", "")
                self.connection = sqlite3.connect(db_path)
                self.connection.row_factory = sqlite3.Row
                self.db_type = "sqlite"
                logger.info(f"Connected to SQLite database: {db_path}")
            elif self.database_url.startswith("postgresql"):
                self.connection = psycopg2.connect(self.database_url)
                self.db_type = "postgresql"
                logger.info("Connected to PostgreSQL database")
            else:
                raise ValueError(f"Unsupported database URL: {self.database_url}")

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")

    def _convert_query_placeholders(self, query: str) -> str:
        """Convert query placeholders based on database type"""
        if not self.db_type:
            return query

        if self.db_type == "postgresql":
            # Convert ? placeholders to %s for PostgreSQL
            return query.replace("?", "%s")
        elif self.db_type == "sqlite":
            # Convert %s placeholders to ? for SQLite
            return query.replace("%s", "?")

        return query

    def execute_query(self, query: str, params: tuple = None) -> Any:
        """Execute a query and return results"""
        if not self.connection:
            self.connect()

        # Convert placeholders based on database type
        converted_query = self._convert_query_placeholders(query)

        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(converted_query, params)
            else:
                cursor.execute(converted_query)

            if converted_query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                # Convert results to list of dictionaries for consistent access
                if results and hasattr(results[0], "keys"):
                    # SQLite Row objects or psycopg2 RealDictRow
                    return [dict(row) for row in results]
                elif results and isinstance(results[0], (tuple, list)):
                    # Raw tuples - convert using column names
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in results]
                return results
            else:
                self.connection.commit()
                return cursor.rowcount

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            self.connection.rollback()
            raise

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


def get_database_connection() -> DatabaseConnection:
    """Get a database connection instance"""
    return DatabaseConnection()


def get_db_conn() -> DatabaseConnection:
    """Legacy function name for backward compatibility"""
    return get_database_connection()
