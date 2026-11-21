"""PySpark 執行引擎封裝模組：統一管理 SparkSession 建立與釋放。"""

from __future__ import annotations

import logging
from typing import Any, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from config import SparkConfig

logger = logging.getLogger(__name__)


class SparkEngine:
    """封裝 SparkSession 生命週期，支援 Context Manager 自動釋放資源。"""

    def __init__(
        self,
        app_name: str = "SparkMLDemo",
        config: Optional[SparkConfig] = None,
    ) -> None:
        resolved = config or SparkConfig.from_env()
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master(resolved.master_url)
            .getOrCreate()
        )
        logger.info("已初始化 SparkSession '%s'（master=%s）", app_name, resolved.master_url)

    def __enter__(self) -> "SparkEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def run_job(self, data: Sequence[Any], columns: Sequence[str]) -> DataFrame:
        """將原始二維資料建立為 Spark DataFrame 並輸出檢視。"""
        df = self.spark.createDataFrame(data, columns)
        df.show()
        return df

    def process_data(self, data: Sequence[dict[str, Any]]) -> Optional[DataFrame]:
        """將收到的字典清單轉為 Spark DataFrame，空資料時直接回傳 None。"""
        if not data:
            return None
        df = self.spark.createDataFrame(data)
        df.printSchema()
        df.show()
        return df

    def stop(self) -> None:
        """安全停止 SparkSession。"""
        if self.spark:
            self.spark.stop()
