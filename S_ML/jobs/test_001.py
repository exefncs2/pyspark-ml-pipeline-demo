"""批次排程進入點：PostgreSQL（ODS）-> Redis -> PySpark 預測 -> RabbitMQ。"""

from __future__ import annotations

import logging

from center.psql_to_redis import load_table_to_redis
from center.redis_ml_to_rabbitmq import (
    process_data_with_spark,
    read_data_from_redis,
    send_predictions_to_rabbitmq,
)
from config import WINE_REDIS_PREFIX

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """依序執行資料快取、模型預測與訊息推播流程。"""
    source_schema = "ODS"
    target_tables = ("wine_quality_red", "wine_quality_white")

    for table_name in target_tables:
        load_table_to_redis(source_schema, table_name, WINE_REDIS_PREFIX)

    redis_key = f"{WINE_REDIS_PREFIX}:wine_quality_red"
    data = read_data_from_redis(redis_key)
    if data is None:
        logger.warning("略過預測流程：Redis 找不到鍵 `%s`", redis_key)
        return

    predictions = process_data_with_spark(data)
    send_predictions_to_rabbitmq(predictions)


if __name__ == "__main__":
    main()
