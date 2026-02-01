from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import List, Tuple

from src.db import (
    ensure_view,
    get_video_list,
    query_boxes,
    query_boxes_range,
    query_next_hit,
    query_next_hit_with_blacklist,
    query_prev_hit,
    query_prev_hit_with_blacklist,
    query_timeline,
    load_video_log,
    save_video_log,
)
from src.templates import render_index

router = APIRouter()


class LogItem(BaseModel):
    rawTime: float
    type: str


class VideoLogData(BaseModel):
    in_count: int  # Python 예약어 in 피하기 위해 in_count 사용
    out_count: int
    is_completed: bool
    is_in_progress: bool = False
    logs: List[LogItem]


@router.get("/")
def index():
    return render_index()


@router.get("/api/videos")
def api_videos():
    # 비디오 목록을 줄 때, 각 비디오의 완료 여부도 같이 주면 좋음
    videos = get_video_list()
    # (선택사항) 리스트 로딩 시 완료 여부를 체크해서 넣어줄 수도 있음
    # 성능을 위해 여기서는 단순 목록만 반환하고,
    # 개별 선택 시 로그를 로드하는 방식으로 진행
    return videos


# --- [추가] 로그 API ---


@router.get("/api/logs/{video_id}")
def api_get_logs(video_id: str):
    """서버에 저장된 해당 비디오의 로그 불러오기"""
    data = load_video_log(video_id)
    return data


@router.post("/api/logs/{video_id}")
def api_save_logs(video_id: str, payload: VideoLogData):
    """작업 내용을 서버 파일에 저장"""
    # Pydantic 모델을 dict로 변환 (in_count -> in)
    data_dict = {
        "in": payload.in_count,
        "out": payload.out_count,
        "is_completed": payload.is_completed,
        "is_in_progress": payload.is_in_progress,
        "logs": [log.dict() for log in payload.logs],
    }
    success = save_video_log(video_id, data_dict)
    return {"success": success}


@router.get("/api/videos/{video_id}/boxes")
def api_boxes(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    return query_boxes(view, frame)


@router.get("/api/videos/{video_id}/boxes_range")
def api_boxes_range(
    video_id: str,
    start_frame: int = Query(..., ge=0),
    end_frame: int = Query(..., ge=0),
):
    view = ensure_view(video_id)
    boxes = query_boxes_range(view, start_frame, end_frame)
    return {"boxes": boxes, "start_frame": start_frame, "end_frame": end_frame}


@router.get("/api/videos/{video_id}/timeline")
def api_timeline(video_id: str, bin_sec: int = Query(1, ge=1, le=60)):
    view = ensure_view(video_id)
    counts = query_timeline(view, bin_sec)
    return {"bin_sec": bin_sec, "counts": counts}


@router.get("/api/videos/{video_id}/next_hit")
def api_next_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    next_frame = query_next_hit(view, frame)
    return {"frame": next_frame}


@router.get("/api/videos/{video_id}/next_hit_with_blacklist")
def api_next_hit_with_blacklist(
    video_id: str,
    frame: int = Query(..., ge=0),
    blacklist: str = Query(""),
):
    view = ensure_view(video_id)
    if not blacklist:
        next_frame = query_next_hit(view, frame)
        return {"frame": next_frame}
    # 블랙리스트 파싱
    blacklist_boxes: List[Tuple[int, int, int, int]] = []
    for item in blacklist.split(";"):
        try:
            x1, y1, x2, y2 = map(int, item.split(","))
            blacklist_boxes.append((x1, y1, x2, y2))
        except Exception:
            continue
    next_frame = query_next_hit_with_blacklist(view, frame, blacklist_boxes)
    return {"frame": next_frame}


@router.get("/api/videos/{video_id}/prev_hit")
def api_prev_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    prev_frame = query_prev_hit(view, frame)
    return {"frame": prev_frame}


@router.get("/api/videos/{video_id}/prev_hit_with_blacklist")
def api_prev_hit_with_blacklist(
    video_id: str,
    frame: int = Query(..., ge=0),
    blacklist: str = Query(""),
):
    view = ensure_view(video_id)
    if not blacklist:
        prev_frame = query_prev_hit(view, frame)
        return {"frame": prev_frame}
    # 블랙리스트 파싱
    blacklist_boxes: List[Tuple[int, int, int, int]] = []
    for item in blacklist.split(";"):
        try:
            x1, y1, x2, y2 = map(int, item.split(","))
            blacklist_boxes.append((x1, y1, x2, y2))
        except Exception:
            continue
    prev_frame = query_prev_hit_with_blacklist(view, frame, blacklist_boxes)
    return {"frame": prev_frame}
