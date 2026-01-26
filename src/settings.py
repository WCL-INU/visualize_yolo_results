from __future__ import annotations

from pathlib import Path

# Application-wide constants and resolved paths
FPS: float = 24.0
# src/ -> project root
BASE = Path(__file__).resolve().parent.parent
VIDEOS_DIR = BASE / "data" / "videos"
BOXES_DIR = BASE / "data" / "boxes"
TEMPLATES_DIR = BASE / "templates"
