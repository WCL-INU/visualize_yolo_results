from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

FPS = 24.0

BASE = Path(__file__).resolve().parent
VIDEOS_DIR = BASE / "data" / "videos"
BOXES_DIR = BASE / "data" / "boxes"

app = FastAPI()

# 정적 서빙: MP4는 브라우저가 Range 요청을 사용 (Starlette FileResponse 기반)
app.mount("/videos", StaticFiles(directory=str(VIDEOS_DIR)), name="videos")

# DuckDB 연결 (단일 프로세스/로컬 기준)
con = duckdb.connect(database=":memory:")

# 간단 캐시: video_id -> (parquet_path, view_name)
_video_cache: Dict[str, Tuple[Path, str]] = {}


def _video_id_from_name(p: Path) -> str:
    return p.stem


def _get_video_list() -> List[Dict]:
    if not VIDEOS_DIR.exists():
        return []
    out = []
    for p in sorted(VIDEOS_DIR.glob("*.mp4")):
        vid = _video_id_from_name(p)
        out.append(
            {
                "video_id": vid,
                "file": p.name,
                "url": f"/videos/{p.name}",
                "fps": FPS,
            }
        )
    return out


def _ensure_view(video_id: str) -> str:
    """
    video_id에 해당하는 parquet_scan VIEW를 생성/재사용.
    """
    if video_id in _video_cache:
        return _video_cache[video_id][1]

    pq = BOXES_DIR / f"{video_id}.parquet"
    if not pq.exists():
        raise HTTPException(status_code=404, detail=f"Parquet not found: {pq}")

    view = f"v_{video_id}".replace("-", "_").replace(".", "_")
    # parquet_scan: 파일을 직접 스캔
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


@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(
        f"""
<!doctype html>
<meta charset="utf-8">
<title>Video + YOLO Boxes Viewer</title>
<style>
  body {{ font-family: sans-serif; margin: 0; }}
  .wrap {{ display: grid; grid-template-columns: 2fr 1fr; gap: 12px; padding: 12px; }}
  .player {{ position: relative; width: 100%; }}
  video {{ width: 100%; max-height: 78vh; background: #000; object-fit: contain; }}
  canvas {{ position: absolute; left: 0; top: 0; pointer-events: none; }}
  .panel {{ border: 1px solid #ddd; padding: 12px; }}
  .row {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: center; margin: 8px 0; }}
  button {{ padding: 8px 12px; }}
  select {{ padding: 6px 10px; }}
  #timeline {{ width: 100%; height: 60px; border: 1px solid #ddd; }}
  pre {{ white-space: pre-wrap; font-size: 12px; }}
</style>

<div class="wrap">
  <div>
    <div class="row">
      <label>Video:</label>
      <select id="sel"></select>
      <button id="reload">Reload list</button>
      <label style="margin-left:12px;"><input type="checkbox" id="overlay" checked> Overlay</label>
    </div>

    <div class="player" id="player">
      <video id="v" controls></video>
      <canvas id="c"></canvas>
    </div>

    <div class="row">
      <button id="prevHit">Prev hit</button>
      <button id="nextHit">Next hit</button>
      <button id="prevF">-1 frame</button>
      <button id="nextF">+1 frame</button>
      <span id="info"></span>
    </div>

    <div class="row">
      <div style="flex:1;">
        <div style="font-size:12px; margin-bottom:4px;">Timeline (click to seek)</div>
        <canvas id="timeline"></canvas>
      </div>
    </div>
  </div>

  <div class="panel">
    <h3 style="margin-top:0;">Current boxes</h3>
    <pre id="boxes">-</pre>
  </div>
</div>

<script>
const FPS = {FPS};

const sel = document.getElementById('sel');
const v = document.getElementById('v');
const player = document.getElementById('player');
const c = document.getElementById('c');
const ctx = c.getContext('2d');
const overlayToggle = document.getElementById('overlay');
const boxesPre = document.getElementById('boxes');
const info = document.getElementById('info');

const tl = document.getElementById('timeline');
const tlx = tl.getContext('2d');

let currentVideo = null;
let lastFrame = -1;
let timeline = null; // Uint16Array-ish (counts per bin)
let binSec = 1;

function clamp(x,a,b) {{ return Math.max(a, Math.min(b, x)); }}

function resizeCanvasToVideo() {{
  // video 렌더 크기(표시 크기)에 맞춤
  const rect = v.getBoundingClientRect();
  c.width = Math.floor(rect.width);
  c.height = Math.floor(rect.height);
  c.style.width = rect.width + 'px';
  c.style.height = rect.height + 'px';
}}

function resizeTimeline() {{
  const rect = tl.getBoundingClientRect();
  tl.width = Math.floor(rect.width);
  tl.height = Math.floor(rect.height);
}}

async function loadVideos() {{
  const r = await fetch('/api/videos');
  const list = await r.json();
  sel.innerHTML = '';
  for (const it of list) {{
    const opt = document.createElement('option');
    opt.value = it.video_id;
    opt.textContent = it.video_id;
    opt.dataset.url = it.url;
    sel.appendChild(opt);
  }}
  if (list.length) {{
    sel.value = list[0].video_id;
    await selectVideo(sel.value);
  }}
}}

async function selectVideo(video_id) {{
  currentVideo = video_id;
  lastFrame = -1;
  boxesPre.textContent = '-';
  info.textContent = '';

  const url = sel.selectedOptions[0].dataset.url;
  v.src = url;
  v.load();

  // timeline 로드(1초 bin)
  const tr = await fetch(`/api/videos/${{video_id}}/timeline?bin_sec=${{binSec}}`);
  const tj = await tr.json();
  timeline = tj.counts;
  drawTimeline();

  // 재생 준비 후 캔버스 리사이즈
  v.onloadedmetadata = () => {{
    resizeCanvasToVideo();
    resizeTimeline();
    drawTimeline();
  }};
}}

function drawBoxes(boxes) {{
  ctx.clearRect(0,0,c.width,c.height);
  if (!overlayToggle.checked) return;
  if (!boxes || boxes.length === 0) return;

  // 원본 픽셀 좌표 -> 현재 표시 크기로 스케일
  const vw = v.videoWidth;
  const vh = v.videoHeight;
  if (!vw || !vh) return;
  const sx = c.width / vw;
  const sy = c.height / vh;

  ctx.lineWidth = 2;
  for (const b of boxes) {{
    const x = b.x * sx;
    const y = b.y * sy;
    const w = b.width * sx;
    const h = b.height * sy;
    ctx.strokeRect(x, y, w, h);
  }}
}}

async function fetchBoxes(frame) {{
  if (!currentVideo) return [];
  const r = await fetch(`/api/videos/${{currentVideo}}/boxes?frame=${{frame}}`);
  if (!r.ok) return [];
  return await r.json();
}}

function currentFrame() {{
  // 고정 FPS 가정
  return Math.max(0, Math.round(v.currentTime * FPS));
}}

async function tick() {{
  if (v.readyState >= 2 && currentVideo) {{
    const f = currentFrame();
    if (f !== lastFrame) {{
      lastFrame = f;
      const boxes = await fetchBoxes(f);
      boxesPre.textContent = JSON.stringify(boxes, null, 2);
      drawBoxes(boxes);
      info.textContent = `t=${{v.currentTime.toFixed(3)}}s, frame=${{f}}`;
    }}
  }}
  requestAnimationFrame(tick);
}}

function drawTimeline() {{
  if (!timeline) return;
  if (!tl.width) resizeTimeline();

  const W = tl.width, H = tl.height;
  tlx.clearRect(0,0,W,H);

  const n = timeline.length;
  if (n === 0) return;

  // 간단히: count>0 이면 막대 높이를 올림(정규화)
  let maxv = 1;
  for (let i=0;i<n;i++) maxv = Math.max(maxv, timeline[i]);

  for (let i=0;i<n;i++) {{
    const x0 = Math.floor(i * W / n);
    const x1 = Math.floor((i+1) * W / n);
    const v = timeline[i];
    if (v <= 0) continue;
    const h = Math.floor((v / maxv) * (H-2));
    tlx.fillRect(x0, H - h, Math.max(1, x1-x0), h);
  }}
}}

tl.addEventListener('click', (e) => {{
  if (!timeline || !currentVideo || !v.duration) return;
  const rect = tl.getBoundingClientRect();
  const x = e.clientX - rect.left;
  const n = timeline.length;
  const idx = clamp(Math.floor(x * n / rect.width), 0, n-1);
  const t = idx * binSec;
  v.currentTime = clamp(t, 0, Math.max(0, v.duration - 0.01));
}});

document.getElementById('reload').onclick = loadVideos;
sel.onchange = async () => selectVideo(sel.value);

document.getElementById('prevF').onclick = () => {{
  v.currentTime = Math.max(0, v.currentTime - 1/FPS);
}};
document.getElementById('nextF').onclick = () => {{
  v.currentTime = v.currentTime + 1/FPS;
}};

async function jumpHit(dir) {{
  if (!currentVideo) return;
  const f = currentFrame();
  const r = await fetch(`/api/videos/${{currentVideo}}/${{dir}}_hit?frame=${{f}}`);
  if (!r.ok) return;
  const j = await r.json();
  if (j && j.frame != null) {{
    v.currentTime = j.frame / FPS;
  }}
}}
document.getElementById('prevHit').onclick = () => jumpHit('prev');
document.getElementById('nextHit').onclick = () => jumpHit('next');

window.addEventListener('resize', () => {{
  resizeCanvasToVideo();
  resizeTimeline();
  drawTimeline();
}});

loadVideos();
tick();
</script>
"""
    )


@app.get("/api/videos")
def api_videos():
    return _get_video_list()


@app.get("/api/videos/{video_id}/boxes")
def api_boxes(video_id: str, frame: int = Query(..., ge=0)):
    view = _ensure_view(video_id)
    # frame 한 개 조회
    rows = con.execute(
        f"""
        SELECT x, y, width, height, box_index
        FROM {view}
        WHERE frame = ?
        ORDER BY box_index
    """,
        [frame],
    ).fetchall()

    # 단일 객체라도 리스트로 반환(확장 대비)
    return [
        {"x": r[0], "y": r[1], "width": r[2], "height": r[3], "box_index": r[4]}
        for r in rows
    ]


@app.get("/api/videos/{video_id}/timeline")
def api_timeline(video_id: str, bin_sec: int = Query(1, ge=1, le=60)):
    """
    bin_sec 초 단위로 '검출 개수' 배열 반환.
    frame -> sec = floor(frame / FPS)
    """
    view = _ensure_view(video_id)
    # 초 단위로 aggregation 후, 0..max_sec까지 dense array로 채움
    # (bin_sec>1이면 floor(sec/bin_sec)로 bin)
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
        return {"bin_sec": bin_sec, "counts": []}

    max_bin = rows[-1][0]
    counts = [0] * (max_bin + 1)
    for b, cnt in rows:
        counts[b] = int(cnt)

    return {"bin_sec": bin_sec, "counts": counts}


@app.get("/api/videos/{video_id}/next_hit")
def api_next_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = _ensure_view(video_id)
    row = con.execute(
        f"""
        SELECT MIN(frame) FROM {view} WHERE frame > ?
    """,
        [frame],
    ).fetchone()
    if not row or row[0] is None:
        return {"frame": None}
    return {"frame": int(row[0])}


@app.get("/api/videos/{video_id}/prev_hit")
def api_prev_hit(video_id: str, frame: int = Query(..., ge=0)):
    view = _ensure_view(video_id)
    row = con.execute(
        f"""
        SELECT MAX(frame) FROM {view} WHERE frame < ?
    """,
        [frame],
    ).fetchone()
    if not row or row[0] is None:
        return {"frame": None}
    return {"frame": int(row[0])}
