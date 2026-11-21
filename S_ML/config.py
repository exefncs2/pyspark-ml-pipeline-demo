"""參數設定模組：集中管理 PostgreSQL、Redis、RabbitMQ 與 Spark 環境設定。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# 以模組實際位置推算前端靜態目錄，確保無論從專案根目錄或 S_ML/ 內啟動都能正確載入
S_ML_DIR = Path(__file__).resolve().parent
INTERFACE_DIR = S_ML_DIR / "interface"

WINE_EXCHANGE_NAME = "wine_quality_exchange"
WINE_QUEUE_NAME = "wine_quality_predictions"
WINE_ROUTING_KEY = "wine_quality"
WINE_TARGET_TABLE = "wine_quality_predictions"
WINE_REDIS_PREFIX = "wine_quality"


@dataclass(frozen=True)
class PostgresConfig:
    """不可變 PostgreSQL 連線設定。"""

    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            host=os.getenv("PG_HOST", "127.0.0.1"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DB", ""),
            user=os.getenv("PG_USER", ""),
            password=os.getenv("PG_PASSWORD", ""),
        )


@dataclass(frozen=True)
class RedisConfig:
    """不可變 Redis 連線設定。"""

    host: str
    port: int
    password: str | None

    @classmethod
    def from_env(cls) -> "RedisConfig":
        return cls(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=os.getenv("REDIS_PASSWORD") or None,
        )


@dataclass(frozen=True)
class RabbitMQConfig:
    """不可變 RabbitMQ 連線設定。"""

    host: str
    port: int
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "RabbitMQConfig":
        return cls(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            user=os.getenv("RABBITMQ_USER", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
        )


@dataclass(frozen=True)
class SparkConfig:
    """不可變 Apache Spark 連線設定。"""

    master_url: str

    @classmethod
    def from_env(cls) -> "SparkConfig":
        return cls(master_url=os.getenv("SPARK_MASTER_URL", "local[*]"))
