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
    """If True, watchdog may auto-restart for this unhealthy condition (stall)."""
    stall_restart_eligible: bool
    """Buffer is within startup grace waiting for first HLS output."""
    in_startup_grace: bool
    """Playlist non-empty and/or at least one .ts present this tick."""
    observed_hls_output: bool


def snapshot_buffer_health(
    s: EncoderSettings,
    *,
    buffer_running: bool = False,
    seconds_since_buffer_start: float | None = None,
    buffer_had_successful_hls: bool = False,
) -> BufferHealthSnapshot:
    """
    Assess buffer directory health. When ``buffer_running`` is True and
    ``seconds_since_buffer_start`` is set, startup grace applies for missing
    first HLS output.

    ``buffer_had_successful_hls`` is True once we have ever seen a non-empty
    playlist or a segment in this buffer session (operator-owned flag).
    """
    buf = s.buffer_dir.resolve()
    playlist = buf / "live.m3u8"
    grace = float(s.buffer_health_startup_grace_seconds)

    in_grace = (
        buffer_running
        and seconds_since_buffer_start is not None
        and seconds_since_buffer_start >= 0
        and seconds_since_buffer_start < grace
    )

    if not playlist.is_file():
        waiting = not buffer_had_successful_hls
        if buffer_running and in_grace and waiting:
            return BufferHealthSnapshot(
                playlist_mtime=None,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="startup grace: waiting for live.m3u8",
                stall_restart_eligible=False,
                in_startup_grace=True,
                observed_hls_output=False,
            )
        if buffer_running and waiting:
            return BufferHealthSnapshot(
                playlist_mtime=None,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="startup failed: playlist still missing after grace period",
                stall_restart_eligible=False,
                in_startup_grace=False,
                observed_hls_output=False,
            )
        if buffer_had_successful_hls:
            return BufferHealthSnapshot(
                playlist_mtime=None,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="playlist missing (was present after prior HLS output)",
                stall_restart_eligible=True,
                in_startup_grace=False,
                observed_hls_output=False,
            )
        return BufferHealthSnapshot(
            playlist_mtime=None,
            newest_segment=None,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=0,
            healthy=False,
            detail="playlist missing",
            stall_restart_eligible=False,
            in_startup_grace=False,
            observed_hls_output=False,
        )

    try:
        pl_mtime = playlist.stat().st_mtime
        pl_size = playlist.stat().st_size
    except OSError:
        pl_mtime = None
        pl_size = 0

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
            stall_restart_eligible=buffer_had_successful_hls,
            in_startup_grace=in_grace,
            observed_hls_output=False,
        )

    ts_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    newest = ts_files[0] if ts_files else None
    observed_hls = pl_size > 0 or len(ts_files) > 0

    if newest is None:
        if buffer_running and in_grace and not buffer_had_successful_hls:
            return BufferHealthSnapshot(
                playlist_mtime=pl_mtime,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="startup grace: waiting for first .ts segment",
                stall_restart_eligible=False,
                in_startup_grace=True,
                observed_hls_output=observed_hls,
            )
        if buffer_running and not buffer_had_successful_hls and not in_grace:
            return BufferHealthSnapshot(
                playlist_mtime=pl_mtime,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="startup failed: no .ts segments after grace period",
                stall_restart_eligible=False,
                in_startup_grace=False,
                observed_hls_output=observed_hls,
            )
        if buffer_had_successful_hls:
            return BufferHealthSnapshot(
                playlist_mtime=pl_mtime,
                newest_segment=None,
                newest_segment_mtime=None,
                newest_segment_size=None,
                segment_count=0,
                healthy=False,
                detail="no .ts segments after HLS was previously produced",
                stall_restart_eligible=True,
                in_startup_grace=False,
                observed_hls_output=observed_hls,
            )
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=None,
            newest_segment_mtime=None,
            newest_segment_size=None,
            segment_count=0,
            healthy=False,
            detail="no .ts segments",
            stall_restart_eligible=buffer_had_successful_hls,
            in_startup_grace=False,
            observed_hls_output=observed_hls,
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
            stall_restart_eligible=buffer_had_successful_hls,
            in_startup_grace=in_grace,
            observed_hls_output=True,
        )

    now = time.time()
    stale_pl = pl_mtime is not None and (now - pl_mtime) > s.buffer_stall_threshold_seconds
    stale_seg = (now - nmtime) > s.buffer_stall_threshold_seconds
    zero = nsize == 0

    if zero:
        elig = buffer_had_successful_hls
        if buffer_running and in_grace and not buffer_had_successful_hls:
            return BufferHealthSnapshot(
                playlist_mtime=pl_mtime,
                newest_segment=newest,
                newest_segment_mtime=nmtime,
                newest_segment_size=nsize,
                segment_count=len(ts_files),
                healthy=False,
                detail="startup grace: newest segment still 0 bytes",
                stall_restart_eligible=False,
                in_startup_grace=True,
                observed_hls_output=True,
            )
        return BufferHealthSnapshot(
            playlist_mtime=pl_mtime,
            newest_segment=newest,
            newest_segment_mtime=nmtime,
            newest_segment_size=nsize,
            segment_count=len(ts_files),
            healthy=False,
            detail="newest segment is 0 bytes",
            stall_restart_eligible=elig,
            in_startup_grace=False,
            observed_hls_output=True,
        )

    if in_grace and buffer_running and not buffer_had_successful_hls:
        if stale_pl or stale_seg:
            return BufferHealthSnapshot(
                playlist_mtime=pl_mtime,
                newest_segment=newest,
                newest_segment_mtime=nmtime,
                newest_segment_size=nsize,
                segment_count=len(ts_files),
                healthy=False,
                detail="startup grace: HLS present; allowing time for steady output",
                stall_restart_eligible=False,
                in_startup_grace=True,
                observed_hls_output=True,
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
            stall_restart_eligible=buffer_had_successful_hls,
            in_startup_grace=False,
            observed_hls_output=True,
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
            stall_restart_eligible=buffer_had_successful_hls,
            in_startup_grace=False,
            observed_hls_output=True,
        )

    return BufferHealthSnapshot(
        playlist_mtime=pl_mtime,
        newest_segment=newest,
        newest_segment_mtime=nmtime,
        newest_segment_size=nsize,
        segment_count=len(ts_files),
        healthy=True,
        detail="ok",
        stall_restart_eligible=False,
        in_startup_grace=False,
        observed_hls_output=True,
    )
