"""Pre-flight checks before starting the rolling buffer."""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

from ffmpeg_cmd import uvc_probe_decode_args
from settings import EncoderSettings


def _run_ffmpeg_version(ffmpeg: Path) -> tuple[bool, str]:
    try:
        r = subprocess.run(
            [str(ffmpeg), "-hide_banner", "-version"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if r.returncode != 0:
            return False, f"ffmpeg -version exited {r.returncode}"
        line = (r.stdout or r.stderr or "").splitlines()[0] if (r.stdout or r.stderr) else ""
        return True, line.strip() or "ffmpeg ok"
    except OSError as e:
        return False, f"Cannot execute ffmpeg: {e}"
    except subprocess.TimeoutExpired:
        return False, "ffmpeg -version timed out"


def _probe_uvc_open(s: EncoderSettings, timeout_sec: float = 15.0) -> tuple[bool, str]:
    """Try to open the configured UVC path briefly (decode a few video frames)."""
    try:
        args = [str(s.ffmpeg_path)] + uvc_probe_decode_args(s)
    except ValueError as e:
        return False, str(e)
    try:
        r = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        err = (r.stderr or "").strip()
        if r.returncode == 0:
            return True, "UVC source opened (short probe ok)"
        tail = err[-1200:] if err else "(no stderr)"
        return False, f"UVC probe failed (exit {r.returncode}): {tail}"
    except subprocess.TimeoutExpired:
        return False, "UVC probe timed out"
    except OSError as e:
        return False, f"UVC probe could not run: {e}"


def _dir_writable(d: Path) -> tuple[bool, str]:
    try:
        d.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=d, delete=True, prefix=".enc_w_", suffix=".tmp"):
            pass
        return True, f"writable: {d}"
    except OSError as e:
        return False, f"not writable: {d} ({e})"


def validate_startup(s: EncoderSettings) -> tuple[list[str], list[str]]:
    """
    Returns (errors, warnings). Non-empty errors mean the buffer must not start.
    """
    errors: list[str] = []
    warnings: list[str] = []

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
        ok, msg = _probe_uvc_open(s)
        if not ok:
            errors.append(msg)
        else:
            warnings.append(msg)

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

    return errors, warnings
