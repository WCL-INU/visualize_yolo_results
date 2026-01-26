from __future__ import annotations

from functools import lru_cache
from fastapi.responses import HTMLResponse

from src.settings import FPS, TEMPLATES_DIR


@lru_cache(maxsize=1)
def _index_template() -> str:
    return (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")


def render_index() -> HTMLResponse:
    html = _index_template().replace("{{FPS}}", str(FPS))
    return HTMLResponse(html)
