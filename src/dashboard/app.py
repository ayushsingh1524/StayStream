"""
app.py – FastAPI application factory for StayStream Live Dashboard.

Run with:
    uvicorn src.dashboard.app:app --host 0.0.0.0 --port 8000 --reload
"""
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.dashboard.routes.api import router as api_router
from src.dashboard.routes.metrics import router as metrics_router
from src.dashboard.routes.pages import router as pages_router

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="StayStream Live Dashboard", version="1.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(pages_router)
app.include_router(api_router)
app.include_router(metrics_router)
