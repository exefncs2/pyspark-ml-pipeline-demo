"""Unit tests for SparkEngine lifecycle and DataFrame creation."""

import pytest
from pyspark.sql import SparkSession

from base.spark_engine import SparkEngine


class TestSparkEngine:
    @pytest.fixture(scope="class")
    def spark_engine(self):
        engine = SparkEngine("TestSparkEngine")
        yield engine
        engine.stop()

    def test_spark_session(self, spark_engine):
        assert isinstance(spark_engine.spark, SparkSession)

    def test_run_job(self, spark_engine):
        sample_data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
        columns = ["Name", "Age"]
        df = spark_engine.run_job(sample_data, columns)

        collected = df.collect()
        assert len(collected) == 3
        assert collected[0]["Name"] == "Alice"
        assert collected[1]["Age"] == 45

    def test_stop(self):
        engine = SparkEngine("TestSparkStop")
        engine.stop()
        assert engine.spark._jsparkSession.isStopped()
