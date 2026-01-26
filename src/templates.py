from __future__ import annotations

import re
from functools import lru_cache
from fastapi.responses import HTMLResponse

from src.settings import FPS, TEMPLATES_DIR


@lru_cache(maxsize=1)
def _index_template() -> str:
    return (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")


def render_index() -> HTMLResponse:
    html = _index_template()
    # Replace any whitespace variation inside the template braces, e.g. "{{FPS}}", "{{ FPS }}"
    html = re.sub(r'\{\{\s*FPS\s*\}\}', str(FPS), html)
    return HTMLResponse(html)
