"""FastAPI 查詢服務進入點：提供 DWH 預測結果給 Vue 3 視覺化前端。"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from base.psql_connector import PsqlConnector
from config import INTERFACE_DIR, WINE_TARGET_TABLE

app = FastAPI(
    title="PySpark ML Pipeline Demo API",
    description="提供 DWH 紅酒品質預測資料與 Vue 3 前端頁面",
    version="1.0.0",
)
app.mount("/static", StaticFiles(directory=str(INTERFACE_DIR)), name="static")


@app.get("/")
def read_root() -> FileResponse:
    """回傳 Vue 3 示範頁面。"""
    return FileResponse(INTERFACE_DIR / "vue3_demo.html")


@app.get("/predictions/")
def get_predictions() -> list[dict]:
    """查詢並回傳 DWH 中所有的預測紀錄。"""
    query = f'SELECT * FROM "DWH".{WINE_TARGET_TABLE}'
    with PsqlConnector() as connector:
        return connector.execute_query(query)
