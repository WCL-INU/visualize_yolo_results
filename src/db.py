from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
from fastapi import HTTPException

from src.settings import BOXES_DIR, FPS, VIDEOS_DIR

# DuckDB connection (in-memory) and a tiny cache for created views
con = duckdb.connect(database=":memory:")
_video_cache: Dict[str, Tuple[Path, str]] = {}


def video_id_from_name(path: Path) -> str:
    return path.stem


def get_video_list() -> List[Dict]:
    if not VIDEOS_DIR.exists():
        return []
    out = []
    for path in sorted(VIDEOS_DIR.glob("*.mp4")):
        vid = video_id_from_name(path)
        out.append(
            {
                "video_id": vid,
                "file": path.name,
                "url": f"videos/{path.name}",
                "fps": FPS,
            }
        )
    return out


def ensure_view(video_id: str) -> str:
    """
    Create or reuse a parquet_scan view for the given video id.
    """
    if video_id in _video_cache:
        return _video_cache[video_id][1]

    # Support split parquet files like "{video_id}_*.parquet"
    pattern = BOXES_DIR / f"{video_id}_*.parquet"
    matches = sorted(BOXES_DIR.glob(f"{video_id}_*.parquet"))

    # Use the glob pattern so parquet_scan reads all parts when there are multiple files
    if not matches:
        raise HTTPException(status_code=404, detail=f"Parquet not found: {pattern}")

    pq = pattern

    view = f"v_{video_id}".replace("-", "_").replace(".", "_")
    con.execute(
        f"""
        CREATE VIEW {view} AS
        SELECT
          frame::INTEGER AS frame,
          box_index::INTEGER AS box_index,
          x::DOUBLE AS x,
          y::DOUBLE AS y,
          width::DOUBLE AS width,
          height::DOUBLE AS height
        FROM parquet_scan('{pq.as_posix()}');
        """
    )
    _video_cache[video_id] = (pq, view)
    return view


def query_boxes(view: str, frame: int) -> List[Dict]:
    rows = con.execute(
        f"""
        SELECT x, y, width, height, box_index
        FROM {view}
        WHERE frame = ?
        ORDER BY box_index
        """,
        [frame],
    ).fetchall()

    return [
        {"x": r[0], "y": r[1], "width": r[2], "height": r[3], "box_index": r[4]}
        for r in rows
    ]


def query_boxes_range(
    view: str, start_frame: int, end_frame: int
) -> Dict[int, List[Dict]]:
    if start_frame > end_frame:
        start_frame, end_frame = end_frame, start_frame

    rows = con.execute(
        f"""
        SELECT frame::INTEGER AS frame, x, y, width, height, box_index
        FROM {view}
        WHERE frame BETWEEN ? AND ?
        ORDER BY frame, box_index
        """,
        [start_frame, end_frame],
    ).fetchall()

    out: Dict[int, List[Dict]] = {}
    for frame, x, y, w, h, idx in rows:
        out.setdefault(int(frame), []).append(
            {"x": x, "y": y, "width": w, "height": h, "box_index": idx}
        )
    return out


def query_timeline(view: str, bin_sec: int) -> List[int]:
    rows = con.execute(
        f"""
        WITH s AS (
          SELECT CAST(FLOOR(frame / {FPS}) AS INTEGER) AS sec
          FROM {view}
        ),
        b AS (
          SELECT CAST(FLOOR(sec / {bin_sec}) AS INTEGER) AS bin, COUNT(*) AS cnt
          FROM s
          GROUP BY bin
        )
        SELECT bin, cnt FROM b ORDER BY bin
        """
    ).fetchall()

    if not rows:
        return []

    max_bin = rows[-1][0]
    counts = [0] * (max_bin + 1)
    for b, cnt in rows:
        counts[b] = int(cnt)
    return counts


def query_next_hit(view: str, frame: int) -> int | None:
    row = con.execute(
        f"""
        SELECT MIN(frame) FROM {view} WHERE frame > ?
        """,
        [frame],
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def query_prev_hit(view: str, frame: int) -> int | None:
    row = con.execute(
        f"""
        SELECT MAX(frame) FROM {view} WHERE frame < ?
        """,
        [frame],
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])
