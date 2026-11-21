"""RabbitMQ consumer worker: consumes predictions, transforms via PySpark, writes to PostgreSQL DWH."""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid

import orjson as json
from dotenv import load_dotenv

from base.psql_connector import PsqlConnector
from base.rabbitmq_consumer import RabbitMQConsumerEngine
from base.spark_engine import SparkEngine
from model.wine_quality_predictions_model import WineQualityPrediction

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

local_queue: queue.Queue = queue.Queue()

EXCHANGE_NAME = "wine_quality_exchange"
QUEUE_NAME = "wine_quality_predictions"
ROUTING_KEY = "wine_quality"
TARGET_TABLE = "wine_quality_predictions"


def on_message(body: bytes) -> None:
    """Decode incoming RabbitMQ message and enqueue for micro-batch Spark processing."""
    message = json.loads(body)
    logger.info("Enqueued message from RabbitMQ: %s", message)
    local_queue.put(message)


def process_messages() -> None:
    """Drain queued messages, run Spark processing, and persist validated rows to PostgreSQL."""
    messages = []
    while not local_queue.empty():
        messages.append(local_queue.get())

    if not messages:
        return

    with SparkEngine("WineQualityPrediction") as spark_engine:
        processed_df = spark_engine.process_data(messages)
        if processed_df is None:
            return
        pandas_df = processed_df.toPandas()

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "DWH".{TARGET_TABLE} (
            id uuid PRIMARY KEY,
            prediction FLOAT,
            residual_sugar FLOAT
        );
    """
    insert_sql = f"""
        INSERT INTO "DWH".{TARGET_TABLE} (id, prediction, residual_sugar)
        VALUES (%s, %s, %s);
    """

    with PsqlConnector() as psql:
        psql.execute_command(create_table_sql)
        inserted = 0
        for _, row in pandas_df.iterrows():
            record = WineQualityPrediction(
                id=str(uuid.uuid4()),
                prediction=float(row["prediction"]),
                residual_sugar=float(row["residual_sugar"]),
            )
            psql.execute_command(insert_sql, (record.id, record.prediction, record.residual_sugar))
            inserted += 1
        logger.info("Inserted %d prediction row(s) into DWH.%s", inserted, TARGET_TABLE)


def consume_predictions() -> None:
    """Start blocking RabbitMQ consumer loop."""
    consumer = RabbitMQConsumerEngine(
        exchange=EXCHANGE_NAME,
        exchange_type="direct",
        queue=QUEUE_NAME,
        routing_key=ROUTING_KEY,
        callback=on_message,
        durable=True,
        auto_delete=False,
    )
    logger.info("Listening on queue '%s'. Press CTRL+C to exit.", QUEUE_NAME)
    consumer.start_consuming()


def monitor_and_process() -> None:
    """Poll the thread-safe buffer and trigger micro-batch persistence."""
    while True:
        if not local_queue.empty():
            try:
                process_messages()
            except Exception as exc:
                logger.exception("Failed while processing prediction batch: %s", exc)
        time.sleep(1)


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_predictions, daemon=True)
    consumer_thread.start()
    try:
        monitor_and_process()
    except KeyboardInterrupt:
        logger.info("Shutting down prediction worker...")
