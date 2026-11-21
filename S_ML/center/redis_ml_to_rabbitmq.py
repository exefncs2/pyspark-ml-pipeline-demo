"""機器學習預測推播模組：由 Redis 載入特徵、PySpark 訓練預測後送往 RabbitMQ。"""

from __future__ import annotations

import logging
from typing import Optional

import orjson
import pandas as pd
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor

from base.rabbitmq_consumer import RabbitMQConsumerEngine
from base.redis_engine import RedisEngine
from base.spark_engine import SparkEngine
from config import (
    WINE_EXCHANGE_NAME,
    WINE_QUEUE_NAME,
    WINE_REDIS_PREFIX,
    WINE_ROUTING_KEY,
)

logger = logging.getLogger(__name__)

TARGET_LABEL_COL = "residual_sugar"
TRAIN_TEST_SPLIT_WEIGHTS = [0.8, 0.2]
RANDOM_SEED = 1234


def read_data_from_redis(redis_key: str) -> Optional[pd.DataFrame]:
    """由 Redis 讀取 JSON 格式資料並還原為 Pandas DataFrame。"""
    redis_engine = RedisEngine()
    data_json = redis_engine.get_value(redis_key)
    if not data_json:
        logger.warning("Redis 查無資料鍵：`%s`", redis_key)
        return None
    return pd.read_json(data_json, orient="split")


def process_data_with_spark(data: pd.DataFrame) -> pd.DataFrame:
    """以 PySpark 決策樹回歸模型進行訓練與測試集預測，回傳預測結果 DataFrame。"""
    feature_columns = [col for col in data.columns if col != TARGET_LABEL_COL]

    with SparkEngine("WineQualityPrediction") as spark_engine:
        spark_df = spark_engine.spark.createDataFrame(data)
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        assembled_df = assembler.transform(spark_df)

        train_df, test_df = assembled_df.randomSplit(TRAIN_TEST_SPLIT_WEIGHTS, seed=RANDOM_SEED)
        model = DecisionTreeRegressor(labelCol=TARGET_LABEL_COL, featuresCol="features").fit(train_df)
        predictions = model.transform(test_df)

        evaluator = RegressionEvaluator(
            labelCol=TARGET_LABEL_COL,
            predictionCol="prediction",
            metricName="rmse",
        )
        rmse = evaluator.evaluate(predictions)
        logger.info("測試集均方根誤差（RMSE）=%.4f", rmse)
        return predictions.select("prediction", TARGET_LABEL_COL).toPandas()


def send_predictions_to_rabbitmq(
    predictions: pd.DataFrame,
    durable: bool = True,
    auto_delete: bool = False,
) -> int:
    """將預測結果逐筆序列化並發佈至 RabbitMQ，回傳成功發佈筆數。"""
    with RabbitMQConsumerEngine(
        exchange=WINE_EXCHANGE_NAME,
        exchange_type="direct",
        queue=WINE_QUEUE_NAME,
        routing_key=WINE_ROUTING_KEY,
        callback=None,
        durable=durable,
        auto_delete=auto_delete,
    ) as mq_engine:
        published_count = 0
        for _, row in predictions.iterrows():
            message = orjson.dumps(row.to_dict())
            mq_engine.channel.basic_publish(
                exchange=WINE_EXCHANGE_NAME,
                routing_key=WINE_ROUTING_KEY,
                body=message,
            )
            published_count += 1
        logger.info("已成功發佈 %d 筆預測結果至 RabbitMQ 佇列 `%s`", published_count, WINE_QUEUE_NAME)
        return published_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    df = read_data_from_redis(f"{WINE_REDIS_PREFIX}:wine_quality_red")
    if df is not None:
        pred_df = process_data_with_spark(df)
        send_predictions_to_rabbitmq(pred_df)
