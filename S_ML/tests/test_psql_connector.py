"""PostgreSQL 連線封裝整合測試（需已啟動 PostgreSQL 服務）。"""

import pytest

from base.psql_connector import PsqlConnector


@pytest.fixture
def psql_connector():
    with PsqlConnector() as connector:
        yield connector


def test_query(psql_connector) -> None:
    result = psql_connector.execute_query("SELECT 1 AS value")
    assert result == [{"value": 1}]


def test_invalid_query(psql_connector) -> None:
    with pytest.raises(Exception):
        psql_connector.execute_query("SELECT * FROM non_existent_table")
