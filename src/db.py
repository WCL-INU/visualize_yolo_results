from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
from threading import Lock

import duckdb
from fastapi import HTTPException

from src.settings import BOXES_DIR, FPS, VIDEOS_DIR, LOG_DIR

# DuckDB connection (in-memory) and a tiny cache for created views
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=1;")
db_lock = Lock()
_video_cache: Dict[str, Tuple[Path, str]] = {}


def video_id_from_name(path: Path) -> str:
    return path.stem


def video_id_from_parquet(path: Path) -> str:
    name = path.stem
    if "_" in name:
        return "_".join(name.split("_")[:-2])  # Remove the last two parts
    return name


def get_video_list() -> List[Dict]:
    if not BOXES_DIR.exists():
        return []
    out = []
    for path in sorted(BOXES_DIR.glob("*.mp4")):
        vid = video_id_from_name(path)

        pattern_glob = f"*{vid}*.parquet"
        matches = list(BOXES_DIR.glob(pattern_glob))
        
        if not matches:
            # 데이터 파일이 없으면 목록에 추가하지 않고 건너뜁니다.
            continue

        log_data = load_video_log(vid)

        status = "new"
        if log_data.get("is_completed", False):
            status = "completed"
        elif log_data.get("is_in_progress", False):
            status = "in_progress"

        out.append(
            {
                "video_id": vid,
                "file": path.name,
                "url": f"/videos/{path.name}",
                "fps": FPS,
                "status": status,
                "in_count": log_data.get("in", 0),
                "out_count": log_data.get("out", 0)
            }
        )
    return out

def load_video_log(video_id: str) -> Dict[str, Any]:
    """
    서버에 저장된 해당 비디오의 로그 파일(JSON)을 읽어서 반환합니다.
    파일이 없으면 기본 초기값을 반환합니다.
    """
    log_path = LOG_DIR / f"{video_id}.json"
    
    # 기본 구조
    default_log = {
        "in": 0,
        "out": 0,
        "is_completed": False,
        "logs": []
    }

    if not log_path.exists():
        return default_log
    
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # 키 누락 방지를 위해 default와 병합
            return {**default_log, **data}
    except Exception as e:
        print(f"[DB] Error loading log for {video_id}: {e}")
        return default_log


def save_video_log(video_id: str, data: Dict[str, Any]) -> bool:
    """
    프론트엔드에서 받은 작업 데이터를 JSON 파일로 저장합니다.
    """
    log_path = LOG_DIR / f"{video_id}.json"
    
    try:
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[DB] Error saving log for {video_id}: {e}")
        return False

def ensure_view(video_id: str) -> str:
    """
    Create or reuse a parquet_scan view for the given video id.
    """
    with db_lock:
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
    with db_lock:
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

    with db_lock:
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
    with db_lock:
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
    with db_lock:
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
    with db_lock:
        row = con.execute(
            f"""
            SELECT MAX(frame) FROM {view} WHERE frame < ?
            """,
            [frame],
        ).fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])
