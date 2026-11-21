"""Redis 引擎整合測試（需已啟動 Redis 服務）。"""

import time

import pytest

from base.redis_engine import RedisEngine


@pytest.fixture(scope="module")
def redis_engine():
    return RedisEngine()


def test_set_and_get_value(redis_engine) -> None:
    redis_engine.set_value("test_key", "test_value")
    assert redis_engine.get_value("test_key") == "test_value"
    redis_engine.delete_value("test_key")


def test_set_value_with_ttl(redis_engine) -> None:
    redis_engine.set_value("test_ttl_key", "test_value", ttl=1)
    time.sleep(2)
    assert redis_engine.get_value("test_ttl_key") is None


def test_exists_and_delete(redis_engine) -> None:
    redis_engine.set_value("test_exists_key", "test_value")
    assert redis_engine.exists("test_exists_key") is True
    redis_engine.delete_value("test_exists_key")
    assert redis_engine.exists("test_exists_key") is False
