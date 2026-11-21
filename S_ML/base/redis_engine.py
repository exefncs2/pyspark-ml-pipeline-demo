"""Redis 客戶端封裝模組：無匯入副作用，統一由環境設定注入與結構化日誌記錄。"""

from __future__ import annotations

import logging
from typing import Optional

import redis

try:
    from S_ML.config import RedisConfig
except ModuleNotFoundError:
    from config import RedisConfig

logger = logging.getLogger(__name__)


class RedisEngine:
    """封裝常用 Redis 鍵值讀寫操作。"""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
    ) -> None:
        env_cfg = RedisConfig.from_env()
        self.host = host or env_cfg.host
        self.port = port or env_cfg.port
        self.password = password if password is not None else env_cfg.password
        self.client: Optional[redis.StrictRedis] = None
        self.connect()

    def connect(self) -> None:
        """建立並驗證 Redis 伺服器連線。"""
        self.client = redis.StrictRedis(
            host=self.host,
            port=self.port,
            password=self.password,
            decode_responses=True,
        )
        self.client.ping()
        logger.info("已成功連線至 Redis（%s:%d）", self.host, self.port)

    def set_value(self, key: str, value: str, ttl: Optional[int] = None) -> None:
        """寫入鍵值對，可選擇設定存活時間（秒）。"""
        if self.client is None:
            raise RuntimeError("Redis 尚未建立連線")
        if ttl is not None:
            self.client.setex(key, ttl, value)
            logger.info("已寫入 Redis 鍵值 `%s`（TTL=%d 秒）", key, ttl)
            return
        self.client.set(key, value)
        logger.info("已寫入 Redis 鍵值 `%s`", key)

    def get_value(self, key: str) -> Optional[str]:
        """讀取指定鍵值，不存在時回傳 None。"""
        if self.client is None:
            raise RuntimeError("Redis 尚未建立連線")
        return self.client.get(key)

    def delete_value(self, key: str) -> None:
        """刪除指定鍵值。"""
        if self.client is None:
            raise RuntimeError("Redis 尚未建立連線")
        self.client.delete(key)
        logger.info("已刪除 Redis 鍵值 `%s`", key)

    def exists(self, key: str) -> bool:
        """判斷指定鍵是否存在。"""
        if self.client is None:
            raise RuntimeError("Redis 尚未建立連線")
        return bool(self.client.exists(key))
