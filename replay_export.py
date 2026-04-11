"""
Build concat lists from rolling HLS using #EXTINF durations when present.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

from settings import EncoderSettings


_EXTINF_RE = re.compile(r"#EXTINF:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)


@dataclass(frozen=True)
class ReplaySegmentPlan:
    concat_list_path: Path
    segments: tuple[Path, ...]
    approx_seconds: float
    skipped_incomplete: Path | None
    parse_note: str


def _parse_hls_segments_with_durations(
    playlist: Path,
    default_duration: float,
) -> list[tuple[Path, float]]:
    text = playlist.read_text(encoding="utf-8", errors="replace")
    base = playlist.parent
    out: list[tuple[Path, float]] = []
    pending_dur: float | None = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = _EXTINF_RE.match(line)
        if m:
            pending_dur = float(m.group(1))
            continue
        if line.startswith("#"):
            continue
        if re.match(r"^https?://", line):
            continue
        p = Path(line)
        if not p.is_absolute():
            p = base / p
        p = p.resolve()
        dur = pending_dur if pending_dur is not None else default_duration
        pending_dur = None
        if p.suffix.lower() == ".ts":
            out.append((p, dur))

    return out


def _newest_ts_in_dir(buf: Path) -> Path | None:
    best: tuple[float, Path] | None = None
    try:
        for p in buf.iterdir():
            if p.suffix.lower() != ".ts" or not p.is_file():
                continue
            try:
                m = p.stat().st_mtime
            except OSError:
                continue
            if best is None or m > best[0]:
                best = (m, p)
    except OSError:
        return None
    return best[1] if best else None


def _looks_incomplete(path: Path, s: EncoderSettings) -> bool:
    try:
        st = path.stat()
    except OSError:
        return True
    age = time.time() - st.st_mtime
    if age <= s.incomplete_segment_max_age_seconds and st.st_size < s.incomplete_segment_min_bytes:
        return True
    return False


def build_replay_plan(s: EncoderSettings) -> ReplaySegmentPlan | None:
    """
    Select segments covering approximately ``replay_seconds`` (from the end of the playlist).
    Writes ``_replay_concat.txt`` in the buffer directory.
    """
    playlist = s.buffer_dir / "live.m3u8"
    if not playlist.is_file():
        return None

    buf = s.buffer_dir.resolve()
    entries = _parse_hls_segments_with_durations(
        playlist,
        default_duration=float(s.hls_segment_seconds),
    )
    if not entries:
        return None

    target = float(s.replay_seconds)
    chosen: list[tuple[Path, float]] = []
    acc = 0.0
    for path, dur in reversed(entries):
        chosen.insert(0, (path, dur))
        acc += dur
        if acc >= target:
            break

    skipped: Path | None = None
    newest = _newest_ts_in_dir(buf)
    if chosen and newest is not None:
        last_path = chosen[-1][0]
        if last_path.resolve() == newest.resolve() and _looks_incomplete(last_path, s):
            skipped = last_path
            chosen = chosen[:-1]
            acc = sum(d for _, d in chosen)

    if not chosen:
        return None

    paths = tuple(p for p, _ in chosen)
    list_path = buf / "_replay_concat.txt"
    lines = "\n".join(f"file '{p.name}'" for p in paths) + "\n"
    list_path.write_text(lines, encoding="utf-8")

    note = (
        f"selected {len(paths)} segment(s), ~{acc:.2f}s parsed duration"
        + (f"; skipped incomplete tail {skipped.name}" if skipped else "")
    )

    return ReplaySegmentPlan(
        concat_list_path=list_path,
        segments=paths,
        approx_seconds=acc,
        skipped_incomplete=skipped,
        parse_note=note,
    )
