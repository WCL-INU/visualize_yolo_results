from __future__ import annotations

from fastapi import APIRouter, Query

from src.db import (
    ensure_view,
    get_video_list,
    query_boxes,
    query_boxes_range,
    query_next_hit,
    query_prev_hit,
    query_timeline,
)
from src.templates import render_index

router = APIRouter()

@router.get("/")
def index():
    return render_index()

@router.get("/api/videos")
def api_videos():
    return get_video_list()

@router.get("/api/videos/{video_id}/boxes")
def api_boxes(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    # 필터링 없이 바로 DB 결과 반환 (이미 전처리됨)
    return query_boxes(view, frame)

@router.get("/api/videos/{video_id}/boxes_range")
def api_boxes_range(
    video_id: str,
    start_frame: int = Query(..., ge=0),
    end_frame: int = Query(..., ge=0),
):
    view = ensure_view(video_id)
    # 필터링 없이 바로 DB 결과 반환
    boxes = query_boxes_range(view, start_frame, end_frame)
    return {"boxes": boxes, "start_frame": start_frame, "end_frame": end_frame}

@router.get("/api/videos/{video_id}/timeline")
def api_timeline(video_id: str, bin_sec: int = Query(1, ge=1, le=60)):
    view = ensure_view(video_id)
    # 타임라인도 깨끗한 데이터 기준
    counts = query_timeline(view, bin_sec)
    return {"bin_sec": bin_sec, "counts": counts}

@router.get("/api/videos/{video_id}/next_hit")
def api_next_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    # 루프나 검사 없이 바로 다음 히트 반환
    next_frame = query_next_hit(view, frame)
    return {"frame": next_frame}

@router.get("/api/videos/{video_id}/prev_hit")
def api_prev_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = ensure_view(video_id)
    prev_frame = query_prev_hit(view, frame)
    return {"frame": prev_frame}