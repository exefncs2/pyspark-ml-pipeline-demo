"""PostgreSQL 連線封裝模組：支援 Context Manager 與參數化查詢防注入。"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import psycopg2

from config import PostgresConfig


class PsqlConnector:
    """封裝 PostgreSQL 查詢與異動操作，強制透過參數化查詢避免 SQL 注入。"""

    def __init__(self, config: Optional[PostgresConfig] = None) -> None:
        resolved = config or PostgresConfig.from_env()
        self.connection = psycopg2.connect(
            host=resolved.host,
            port=resolved.port,
            dbname=resolved.database,
            user=resolved.user,
            password=resolved.password,
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
        """執行查詢並以字典清單回傳結果列。"""
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_command(
        self,
        sql_command: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        """執行 DDL／DML 指令並提交交易。"""
        with self.connection.cursor() as cursor:
            cursor.execute(sql_command, params)
        self.connection.commit()

    def close_connection(self) -> None:
        """若連線尚未關閉則安全釋放。"""
        if self.connection and not self.connection.closed:
            self.connection.close()
