"""RabbitMQ 消費端常駐服務：接收預測訊息並經 PySpark 處理後寫入 PostgreSQL DWH。"""

from __future__ import annotations

import logging
import queue
import sys
import threading
import time
import uuid
from pathlib import Path

import orjson

# 確保直接以 `python S_ML/worker/test002.py` 執行時仍可正確匯入 S_ML 套件
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from S_ML.base.psql_connector import PsqlConnector
from S_ML.base.rabbitmq_consumer import RabbitMQConsumerEngine
from S_ML.base.spark_engine import SparkEngine
from S_ML.config import (
    WINE_EXCHANGE_NAME,
    WINE_QUEUE_NAME,
    WINE_ROUTING_KEY,
    WINE_TARGET_TABLE,
)
from S_ML.model.wine_quality_predictions_model import WineQualityPrediction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 1
local_queue: queue.Queue = queue.Queue()


def on_message(body: bytes) -> None:
    """解析 RabbitMQ 訊息並放入執行緒安全佇列等待微批次處理。"""
    message = orjson.loads(body)
    logger.info("收到 RabbitMQ 預測訊息：%s", message)
    local_queue.put(message)


def process_messages() -> None:
    """排空佇列中的累積訊息，經 Spark 轉換後以參數化 SQL 寫入 DWH。"""
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
        CREATE TABLE IF NOT EXISTS "DWH".{WINE_TARGET_TABLE} (
            id uuid PRIMARY KEY,
            prediction FLOAT,
            residual_sugar FLOAT
        );
    """
    insert_sql = f"""
        INSERT INTO "DWH".{WINE_TARGET_TABLE} (id, prediction, residual_sugar)
        VALUES (%s, %s, %s);
    """

    with PsqlConnector() as psql:
        psql.execute_command(create_table_sql)
        inserted_count = 0
        for _, row in pandas_df.iterrows():
            record = WineQualityPrediction(
                id=str(uuid.uuid4()),
                prediction=float(row["prediction"]),
                residual_sugar=float(row["residual_sugar"]),
            )
            psql.execute_command(insert_sql, (record.id, record.prediction, record.residual_sugar))
            inserted_count += 1
        logger.info("已寫入 %d 筆預測結果至 DWH.%s", inserted_count, WINE_TARGET_TABLE)


def consume_predictions() -> None:
    """啟動 RabbitMQ 消費循環。"""
    consumer = RabbitMQConsumerEngine(
        exchange=WINE_EXCHANGE_NAME,
        exchange_type="direct",
        queue=WINE_QUEUE_NAME,
        routing_key=WINE_ROUTING_KEY,
        callback=on_message,
        durable=True,
        auto_delete=False,
    )
    logger.info("開始監聽佇列 `%s`，按 CTRL+C 結束", WINE_QUEUE_NAME)
    consumer.start_consuming()


def monitor_and_process() -> None:
    """定時輪詢本機佇列並觸發微批次落地寫入。"""
    while True:
        if not local_queue.empty():
            try:
                process_messages()
            except Exception as exc:
                logger.exception("處理預測微批次時發生錯誤：%s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_predictions, daemon=True)
    consumer_thread.start()
    try:
        monitor_and_process()
    except KeyboardInterrupt:
        logger.info("收到中斷訊號，正在關閉預測消費端服務")
