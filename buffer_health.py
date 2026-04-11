"""Detect stalled HLS buffer (process alive but output not advancing)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from settings import EncoderSettings


@dataclass
class BufferHealthSnapshot:
    playlist_mtime: float | None
    newest_segment: Path | None
    newest_segment_mtime: float | None
    newest_segment_size: int | None
    segment_count: int
    healthy: bool
    detail: str


def snapshot_buffer_health(s: EncoderSettings) -> BufferHealthSnapshot:
    buf = s.buffer_dir.resolve()
    playlist = buf / "live.m3u8"
    if not playlist.is_file():
        return BufferHealthSnapshot(
            playlist_mtime=None,
            newest_segment=None,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=0,
            healthy=False,
            detail="playlist missing",
        )

    try:
        pl_mtime = playlist.stat().st_mtime
    except OSError:
        pl_mtime = None

    ts_files: list[Path] = []
    try:
        for p in buf.iterdir():
            if p.suffix.lower() == ".ts" and p.is_file():
                ts_files.append(p)
    except OSError:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=None,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=0,
            healthy=False,
            detail="cannot list buffer dir",
        )

    ts_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    newest = ts_files[0] if ts_files else None
    if newest is None:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=None,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=0,
            healthy=False,
            detail="no .ts segments",
        )

    try:
        st = newest.stat()
        nmtime = st.st_mtime
        nsize = st.st_size
    except OSError:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=newest,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=len(ts_files),
            healthy=False,
            detail="cannot stat newest segment",
        )

    now = time.time()
    stale_pl = pl_mtime is not None and (now - pl_mtime) > s.buffer_stall_threshold_seconds
    stale_seg = (now - nmtime) > s.buffer_stall_threshold_seconds
    zero = nsize == 0

    if zero:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=newest,
            newest_segment_mtime=nmtime,
            newest_segment_size=nsize,
            segment_count=len(ts_files),
            healthy=False,
            detail="newest segment is 0 bytes",
        )

    if stale_pl and stale_seg:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=newest,
            newest_segment_mtime=nmtime,
            newest_segment_size=nsize,
            segment_count=len(ts_files),
            healthy=False,
            detail=f"stalled (playlist and newest segment older than {s.buffer_stall_threshold_seconds}s)",
        )

    if stale_seg and len(ts_files) < 2:
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=newest,
            newest_segment_mtime=nmtime,
            newest_segment_size=nsize,
            segment_count=len(ts_files),
            healthy=False,
            detail="newest segment stale with too few segments",
        )

    return BufferHealthSnapshot(
        playlist_mtime=pl_mtime,
        newest_segment=newest,
        newest_segment_mtime=nmtime,
        newest_segment_size=nsize,
        segment_count=len(ts_files),
        healthy=True,
        detail="ok",
    )
