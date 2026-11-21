"""Batch pipeline job: PostgreSQL (ODS) -> Redis -> PySpark ML -> RabbitMQ."""

from __future__ import annotations

import logging

from center.psql_to_redis import load_table_to_redis
from center.redis_ml_to_rabbitmq import (
    process_data_with_spark,
    read_data_from_redis,
    send_predictions_to_rabbitmq,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    schema = "ODS"
    tables = ["wine_quality_red", "wine_quality_white"]
    redis_key_prefix = "wine_quality"

    for table in tables:
        logger.info("Loading %s.%s into Redis...", schema, table)
        load_table_to_redis(schema, table, redis_key_prefix)

    redis_key = f"{redis_key_prefix}:wine_quality_red"
    data = read_data_from_redis(redis_key)

    if data is None:
        logger.warning("No data found in Redis under key '%s'", redis_key)
        return

    predictions = process_data_with_spark(data)
    send_predictions_to_rabbitmq(predictions)
    logger.info("Predictions published to RabbitMQ successfully.")


if __name__ == "__main__":
    main()
