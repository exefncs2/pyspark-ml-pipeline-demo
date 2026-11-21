"""資料匯入快取模組：從 PostgreSQL 讀取資料表並快取至 Redis。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from S_ML.base.psql_connector import PsqlConnector
from S_ML.base.redis_engine import RedisEngine
from S_ML.config import WINE_REDIS_PREFIX

logger = logging.getLogger(__name__)


def load_table_to_redis(schema_name: str, table_name: str, redis_key_prefix: str) -> str:
    """將指定 PostgreSQL 資料表序列化為 JSON 後寫入 Redis，回傳寫入鍵名。"""
    query = f'SELECT * FROM "{schema_name}".{table_name}'
    with PsqlConnector() as psql:
        rows = psql.execute_query(query)

    df = pd.DataFrame(rows)
    payload_json = df.to_json(orient="split")

    redis_key = f"{redis_key_prefix}:{table_name}"
    redis_engine = RedisEngine()
    redis_engine.set_value(redis_key, payload_json)
    logger.info("已將 %s.%s（共 %d 筆）載入 Redis 鍵 `%s`", schema_name, table_name, len(rows), redis_key)
    return redis_key


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    for table in ("wine_quality_red", "wine_quality_white"):
        load_table_to_redis("ODS", table, WINE_REDIS_PREFIX)
