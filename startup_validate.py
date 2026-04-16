"""Pre-flight checks before starting the rolling buffer."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from ffmpeg_cmd import uvc_probe_decode_args
from flight_recorder import parse_ffmpeg_input_stream
from settings import EncoderSettings
from subprocess_win import no_console_creationflags


@dataclass
class UvcProbeDetail:
    ok: bool
    exit_code: int
    stderr: str
    probe_duration_seconds: float = 0.0
    detected_resolution: str | None = None
    detected_fps: float | None = None
    detected_codec: str | None = None
    error_kind: str | None = None


def _run_ffmpeg_version(ffmpeg: Path) -> tuple[bool, str]:
    try:
        r = subprocess.run(
            [str(ffmpeg), "-hide_banner", "-version"],
            capture_output=True,
            text=True,
            timeout=15,
            **no_console_creationflags(),
        )
        if r.returncode != 0:
            return False, f"ffmpeg -version exited {r.returncode}"
        line = (r.stdout or r.stderr or "").splitlines()[0] if (r.stdout or r.stderr) else ""
        return True, line.strip() or "ffmpeg ok"
    except OSError as e:
        return False, f"Cannot execute ffmpeg: {e}"
    except subprocess.TimeoutExpired:
        return False, "ffmpeg -version timed out"


def _probe_uvc_open_detailed(s: EncoderSettings, timeout_sec: float = 15.0) -> UvcProbeDetail:
    t0 = time.monotonic()
    detail = UvcProbeDetail(ok=False, exit_code=-1, stderr="")
    try:
        args = [str(s.ffmpeg_path)] + uvc_probe_decode_args(s)
    except ValueError as e:
        detail.stderr = str(e)
        detail.error_kind = "config_error"
        detail.probe_duration_seconds = time.monotonic() - t0
        return detail
    try:
        r = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            **no_console_creationflags(),
        )
        detail.probe_duration_seconds = time.monotonic() - t0
        detail.exit_code = r.returncode
        detail.stderr = (r.stderr or "").strip()
        parsed = parse_ffmpeg_input_stream(detail.stderr)
        detail.detected_codec = parsed.get("detected_codec")
        if "detected_resolution" in parsed:
            detail.detected_resolution = parsed["detected_resolution"]
        if "detected_fps" in parsed:
            detail.detected_fps = parsed["detected_fps"]
        if r.returncode == 0:
            detail.ok = True
        else:
            detail.error_kind = "probe_failed"
    except subprocess.TimeoutExpired:
        detail.probe_duration_seconds = time.monotonic() - t0
        detail.error_kind = "timeout"
        detail.stderr = "UVC probe timed out"
    except OSError as e:
        detail.probe_duration_seconds = time.monotonic() - t0
        detail.error_kind = "os_error"
        detail.stderr = str(e)
    return detail


def _probe_uvc_open(s: EncoderSettings, timeout_sec: float = 15.0) -> tuple[bool, str]:
    d = _probe_uvc_open_detailed(s, timeout_sec)
    if d.ok:
        return True, "UVC source opened (short probe ok)"
    tail = d.stderr[-1200:] if d.stderr else "(no stderr)"
    if d.error_kind == "timeout":
        return False, "UVC probe timed out"
    return False, f"UVC probe failed (exit {d.exit_code}): {tail}"


def _dir_writable(d: Path) -> tuple[bool, str]:
    try:
        d.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=d, delete=True, prefix=".enc_w_", suffix=".tmp"):
            pass
        return True, f"writable: {d}"
    except OSError as e:
        return False, f"not writable: {d} ({e})"


def validate_startup_detailed(
    s: EncoderSettings,
) -> tuple[list[str], list[str], UvcProbeDetail | None]:
    """
    Returns (errors, warnings, uvc_probe_detail).
    ``uvc_probe_detail`` is set when a UVC probe was attempted (device non-empty and prior checks passed).
    """
    errors: list[str] = []
    warnings: list[str] = []
    probe_detail: UvcProbeDetail | None = None

    if not s.ffmpeg_path.exists():
        errors.append(f"ffmpeg not found at {s.ffmpeg_path}")
    elif not os.access(s.ffmpeg_path, os.X_OK) and os.name != "nt":
        errors.append(f"ffmpeg is not executable: {s.ffmpeg_path}")
    else:
        ok, msg = _run_ffmpeg_version(s.ffmpeg_path)
        if not ok:
            errors.append(msg)
        else:
            warnings.append(msg)

    if not s.uvc_video_device.strip():
        errors.append("UVC_VIDEO_DEVICE is empty")

    if not errors and s.uvc_video_device.strip():
        probe_detail = _probe_uvc_open_detailed(s)
        if not probe_detail.ok:
            tail = probe_detail.stderr[-1200:] if probe_detail.stderr else "(no stderr)"
            if probe_detail.error_kind == "timeout":
                errors.append("UVC probe timed out")
            else:
                errors.append(
                    f"UVC probe failed (exit {probe_detail.exit_code}): {tail}"
                )
        else:
            warnings.append("UVC source opened (short probe ok)")

    ok_ir, msg_ir = _dir_writable(s.instant_replay_path.parent)
    if not ok_ir:
        errors.append(f"instant_replay_path parent: {msg_ir}")

    for label, path in (
        ("buffer_dir", s.buffer_dir),
        ("long_clips_folder", s.long_clips_folder),
    ):
        ok, msg = _dir_writable(path)
        if not ok:
            errors.append(f"{label}: {msg}")

    if s.instant_replay_trigger is not None:
        ok, msg = _dir_writable(s.instant_replay_trigger.parent)
        if not ok:
            errors.append(f"instant_replay_trigger parent: {msg}")

    if s.long_clips_trigger is not None:
        ok, msg = _dir_writable(s.long_clips_trigger.parent)
        if not ok:
            errors.append(f"long_clips_trigger parent: {msg}")

    ffprobe = s.ffmpeg_path.parent / "ffprobe.exe"
    if os.name == "nt" and not ffprobe.exists():
        alt = s.ffmpeg_path.with_name("ffprobe.exe")
        if not alt.exists():
            warnings.append(
                "ffprobe.exe not found next to ffmpeg; long-record ffprobe verify will be skipped"
            )

    return errors, warnings, probe_detail


def validate_startup(s: EncoderSettings) -> tuple[list[str], list[str]]:
    errs, warns, _probe = validate_startup_detailed(s)
    return errs, warns
