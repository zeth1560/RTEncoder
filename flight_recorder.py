"""Flight-recorder style JSONL logging for encoder field diagnostics."""

from __future__ import annotations

import json
import logging
import re
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from settings import EncoderSettings
from subprocess_win import no_console_creationflags


def new_encoder_run_id() -> str:
    """UTC run id: ``YYYYMMDDTHHMMSSZ-shorthex``."""
    d = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{d}-{uuid.uuid4().hex[:6]}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_jsonl = logging.getLogger("replaytrove.encoder.jsonl")


class FlightJsonlEmitter:
    """Structured one-line JSON events; ``component`` is always ``encoder``."""

    def __init__(
        self,
        run_id: str,
        mode: str,
        state_provider: Callable[[], dict[str, Any]],
    ) -> None:
        self.run_id = run_id
        self.mode = mode
        self._state_provider = state_provider

    def emit(
        self,
        event: str,
        message: str,
        *,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "ts": utc_now_iso(),
            "level": level.upper(),
            "event": event,
            "run_id": self.run_id,
            "component": "encoder",
            "mode": self.mode,
            "message": message,
            "state": self._state_provider(),
        }
        if data:
            payload["data"] = data
        _jsonl.info(json.dumps(payload, ensure_ascii=True))


_STREAM_VIDEO_RE = re.compile(
    r"Stream\s+#\d+:\d+:\s*Video:\s*(\S+).*?,\s*(\d{2,})x(\d{2,}).*?,\s*(\d+(?:\.\d+)?)\s*fps",
    re.IGNORECASE,
)
_INPUT_DSHOW_RE = re.compile(
    r"Input\s+#\d+,\s*(\S+),\s*from\s*'([^']+)'",
    re.IGNORECASE,
)


def parse_ffmpeg_input_stream(stderr_text: str) -> dict[str, Any]:
    """Best-effort parse of ffmpeg stderr for input / video stream lines."""
    out: dict[str, Any] = {}
    m_in = _INPUT_DSHOW_RE.search(stderr_text)
    if m_in:
        out["input_format"] = m_in.group(1)
        out["device_name"] = m_in.group(2)
    m_st = _STREAM_VIDEO_RE.search(stderr_text)
    if m_st:
        out["detected_codec"] = m_st.group(1)
        out["detected_resolution"] = f"{m_st.group(2)}x{m_st.group(3)}"
        out["detected_fps"] = float(m_st.group(4))
    return out


def resolve_ffprobe_path(s: EncoderSettings) -> Path | None:
    prob = s.ffmpeg_path.with_name(
        "ffprobe.exe" if s.ffmpeg_path.suffix.lower() == ".exe" else "ffprobe"
    )
    if prob.is_file():
        return prob
    alt = s.ffmpeg_path.parent / "ffprobe.exe"
    if alt.is_file():
        return alt
    return None


def redact_argv(argv: list[str]) -> list[str]:
    """Drop overly long args; keep structure for support."""
    out: list[str] = []
    for a in argv:
        if len(a) > 200:
            out.append(a[:80] + "…[redacted]")
        else:
            out.append(a)
    return out


@dataclass
class FfprobeVideoReport:
    duration_seconds: float | None = None
    video_codec: str | None = None
    width: int | None = None
    height: int | None = None
    avg_frame_rate: str | None = None
    error: str | None = None


def ffprobe_video_report(path: Path, ffprobe: Path, *, timeout: int = 45) -> FfprobeVideoReport:
    rep = FfprobeVideoReport()
    try:
        r = subprocess.run(
            [
                str(ffprobe),
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,width,height,avg_frame_rate",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
            **no_console_creationflags(),
        )
        if r.returncode != 0:
            rep.error = (r.stderr or "")[-800:] or f"exit {r.returncode}"
            return rep
        blob = json.loads(r.stdout or "{}")
        fmt = blob.get("format") or {}
        if "duration" in fmt:
            try:
                rep.duration_seconds = float(fmt["duration"])
            except (TypeError, ValueError):
                pass
        streams = blob.get("streams") or []
        if streams:
            st0 = streams[0]
            rep.video_codec = st0.get("codec_name")
            rep.width = int(st0["width"]) if st0.get("width") is not None else None
            rep.height = int(st0["height"]) if st0.get("height") is not None else None
            rep.avg_frame_rate = st0.get("avg_frame_rate")
    except (OSError, json.JSONDecodeError, subprocess.TimeoutExpired, ValueError, KeyError) as e:
        rep.error = str(e)
    return rep
