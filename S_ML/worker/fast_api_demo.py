"""FastAPI endpoint serving DWH wine quality predictions to the Vue 3 UI."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from base.psql_connector import PsqlConnector

INTERFACE_DIR = Path("S_ML/interface")

app = FastAPI(
    title="PySpark ML Pipeline Demo API",
    description="Serves DWH prediction results for the Vue 3 dashboard",
    version="1.0.0",
)
app.mount("/static", StaticFiles(directory=str(INTERFACE_DIR)), name="static")


@app.get("/")
def read_root():
    return FileResponse(INTERFACE_DIR / "vue3_demo.html")


@app.get("/predictions/")
def get_predictions():
    query = 'SELECT * FROM "DWH".wine_quality_predictions'
    with PsqlConnector() as connector:
        return connector.execute_query(query)
