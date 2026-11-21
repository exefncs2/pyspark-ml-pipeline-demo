"""PySpark session wrapper for batch and stream-like micro-batch processing."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional, Sequence

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class SparkEngine:
    """Manage a PySpark SparkSession lifecycle with environment configuration."""

    def __init__(self, app_name: str = "SparkMLDemo") -> None:
        load_dotenv()
        master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master(master_url)
            .getOrCreate()
        )
        logger.info("Initialized SparkSession '%s' (master=%s)", app_name, master_url)

    def __enter__(self) -> "SparkEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def run_job(self, data: Sequence[Any], columns: Sequence[str]) -> DataFrame:
        """Create and display a DataFrame from raw tabular rows."""
        df = self.spark.createDataFrame(data, columns)
        df.show()
        return df

    def process_data(self, data: Sequence[dict[str, Any]]) -> Optional[DataFrame]:
        """Convert incoming JSON-decoded message payloads into a Spark DataFrame."""
        if not data:
            return None
        df = self.spark.createDataFrame(data)
        df.printSchema()
        df.show()
        return df

    def stop(self) -> None:
        """Stop the SparkSession cleanly."""
        if self.spark:
            self.spark.stop()


if __name__ == "__main__":
    with SparkEngine("SparkDemoTest") as engine:
        sample_data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
        engine.run_job(sample_data, ["Name", "Age"])
