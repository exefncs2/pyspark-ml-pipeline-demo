"""集中設定模組（S_ML/config.py）單元測試。"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

if "dotenv" not in sys.modules:
    sys.modules["dotenv"] = MagicMock()

from config import PostgresConfig, RabbitMQConfig, RedisConfig, SparkConfig  # noqa: E402


class TestCentralizedConfig(unittest.TestCase):
    """驗證各服務不可變設定類別的環境變數讀取。"""

    def test_postgres_config_from_env(self) -> None:
        env = {"PG_HOST": "db.local", "PG_PORT": "5433", "PG_DB": "warehouse", "PG_USER": "u", "PG_PASSWORD": "p"}
        with patch.dict(os.environ, env, clear=True):
            cfg = PostgresConfig.from_env()
            self.assertEqual(cfg.host, "db.local")
            self.assertEqual(cfg.port, 5433)
            self.assertEqual(cfg.database, "warehouse")

    def test_redis_and_rabbitmq_and_spark_defaults(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(RedisConfig.from_env().port, 6379)
            self.assertEqual(RabbitMQConfig.from_env().port, 5672)
            self.assertEqual(SparkConfig.from_env().master_url, "local[*]")


if __name__ == "__main__":
    unittest.main()
