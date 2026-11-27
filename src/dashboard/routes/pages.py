"""
routes/pages.py – HTML page routes (separate HTML file per page).
"""
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

router = APIRouter(tags=["pages"])


def _serve_page(filename: str) -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / filename).read_text(encoding="utf-8"))


@router.get("/", response_class=HTMLResponse)
def overview_page() -> HTMLResponse:
    return _serve_page("index.html")


@router.get("/cities", response_class=HTMLResponse)
def cities_page() -> HTMLResponse:
    return _serve_page("cities.html")


@router.get("/dlq", response_class=HTMLResponse)
def dlq_page() -> HTMLResponse:
    return _serve_page("dlq.html")
