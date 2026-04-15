"""Post-export checks for instant replay MKV."""

from __future__ import annotations

import subprocess
from pathlib import Path

from settings import EncoderSettings


def verify_replay_mkv(path: Path, s: EncoderSettings) -> tuple[bool, str]:
    """Size check; optional ffprobe duration sanity."""
    try:
        sz = path.stat().st_size
    except OSError as e:
        return False, f"replay verify: stat failed: {e}"

    if sz < s.replay_export_min_bytes:
        return (
            False,
            f"replay verify: file too small ({sz} < {s.replay_export_min_bytes} bytes)",
        )

    if not s.replay_export_ffprobe_verify:
        return True, "replay verify: size ok"

    prob = s.ffmpeg_path.with_name(
        "ffprobe.exe" if s.ffmpeg_path.suffix.lower() == ".exe" else "ffprobe"
    )
    if not prob.is_file():
        prob = s.ffmpeg_path.parent / "ffprobe.exe"
    if not prob.is_file():
        return True, "replay verify: size ok (ffprobe missing, skipped)"

    try:
        r = subprocess.run(
            [
                str(prob),
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode != 0:
            return (
                False,
                f"replay verify: ffprobe exit {r.returncode}: {(r.stderr or '')[-500:]}",
            )
        dur = float((r.stdout or "").strip() or "0")
        if dur < s.replay_export_min_duration_seconds:
            return (
                False,
                f"replay verify: duration too short ({dur}s < "
                f"{s.replay_export_min_duration_seconds}s)",
            )
    except (OSError, ValueError, subprocess.TimeoutExpired) as e:
        return False, f"replay verify: ffprobe failed: {e}"

    return True, "replay verify: size + ffprobe ok"
