"""PostgreSQL connector wrapper with context-manager and parameterized queries."""

from __future__ import annotations

import os
from typing import Any, Optional, Sequence

import psycopg2
from dotenv import load_dotenv


class PsqlConnector:
    """Thin PostgreSQL helper returning dict rows and supporting `with` blocks."""

    def __init__(self) -> None:
        load_dotenv()
        self.connection = psycopg2.connect(
            host=os.getenv("PG_HOST", "127.0.0.1"),
            port=os.getenv("PG_PORT", "5432"),
            dbname=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )

    def __enter__(self) -> "PsqlConnector":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_connection()

    def execute_query(
        self,
        sql_query: str,
        params: Optional[Sequence[Any]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a SELECT query and return rows as dictionaries."""
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_command(
        self,
        sql_command: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        """Execute a DDL/DML statement with optional parameters and commit."""
        with self.connection.cursor() as cursor:
            cursor.execute(sql_command, params)
        self.connection.commit()

    def close_connection(self) -> None:
        """Close the underlying PostgreSQL connection if still open."""
        if self.connection and not self.connection.closed:
            self.connection.close()
