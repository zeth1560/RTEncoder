"""
Environment-driven settings for the NDI recorder operator.
Instant replay is always written to a fixed path (INSTANT_REPLAY_SOURCE), atomically via *.copying.mp4.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path


def _opt(name: str, default: str) -> str:
    v = os.environ.get(name)
    if v is None or not str(v).strip():
        return default
    return str(v).strip()


def _opt_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = _opt(name, str(default))
    n = int(raw)
    if minimum is not None and n < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return n


def _opt_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = _opt(name, str(default))
    n = float(raw)
    if minimum is not None and n < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return n


@dataclass(frozen=True)
class EncoderSettings:
    ffmpeg_path: Path
    ndi_extra_ips: str
    ndi_source_name: str
    buffer_preset: str
    long_preset: str
    buffer_crf: int
    long_crf: int
    audio_bitrate_k: int
    buffer_dir: Path
    hls_segment_seconds: int
    hls_list_size: int
    instant_replay_path: Path
    instant_replay_trigger: Path | None
    long_clips_folder: Path
    long_clips_trigger: Path | None
    replay_seconds: float
    encoder_log_file: Path
    buffer_stall_threshold_seconds: float
    long_record_min_bytes: int
    long_record_verify_stable_seconds: float
    incomplete_segment_max_age_seconds: float
    incomplete_segment_min_bytes: int
    hotkey_save_replay: str
    hotkey_start_long: str
    hotkey_stop_long: str
    scoreboard_state_path: Path | None
    scoreboard_state_max_age_seconds: float


def _hotkey_combo(name: str, default: str) -> str:
    return _opt(name, default).strip().lower()


def _optional_path(name: str, default: str) -> Path | None:
    """Empty env value disables the path (None). Unset uses default."""
    raw = os.environ.get(name)
    if raw is None:
        return Path(default)
    s = str(raw).strip()
    if not s:
        return None
    return Path(s)


def load_encoder_settings() -> EncoderSettings:
    ff = Path(_opt("FFMPEG_PATH", r"C:\ffmpeg\bin\ffmpeg.exe"))
    if not ff.exists():
        w = shutil.which("ffmpeg")
        if w:
            ff = Path(w)

    trig_ir = _opt("INSTANT_REPLAY_TRIGGER_FILE", "")
    trig_lc = _opt("LONG_CLIPS_TRIGGER_FILE", "")

    log_dir = Path(_opt("ENCODER_LOG_DIR", r"C:\ReplayTrove\logs"))
    log_file = log_dir / "encoder_operator.log"

    instant = Path(
        _opt("INSTANT_REPLAY_SOURCE", r"C:\ReplayTrove\INSTANTREPLAY.mp4")
    )

    return EncoderSettings(
        ffmpeg_path=ff,
        ndi_extra_ips=_opt("NDI_EXTRA_IPS", ""),
        ndi_source_name=_opt("NDI_SOURCE_NAME", ""),
        buffer_preset=_opt("X264_PRESET_BUFFER", "ultrafast"),
        long_preset=_opt("X264_PRESET_LONG", "veryfast"),
        buffer_crf=_opt_int("X264_CRF_BUFFER", 26, 0),
        long_crf=_opt_int("X264_CRF_LONG", 23, 0),
        audio_bitrate_k=_opt_int("AUDIO_BITRATE_K", 192, 32),
        buffer_dir=Path(_opt("ENCODER_BUFFER_DIR", r"C:\ReplayTrove\encoder_buffer")),
        hls_segment_seconds=_opt_int("HLS_SEGMENT_SECONDS", 2, 1),
        hls_list_size=_opt_int("HLS_LIST_SIZE", 15, 2),
        instant_replay_path=instant,
        instant_replay_trigger=Path(trig_ir) if trig_ir else None,
        long_clips_folder=Path(_opt("LONG_CLIPS_FOLDER", r"C:\ReplayTrove\long_clips")),
        long_clips_trigger=Path(trig_lc) if trig_lc else None,
        replay_seconds=_opt_float("INSTANT_REPLAY_SECONDS", 25.0, 1.0),
        encoder_log_file=log_file,
        buffer_stall_threshold_seconds=_opt_float(
            "BUFFER_STALL_THRESHOLD_SECONDS", 12.0, 3.0
        ),
        long_record_min_bytes=_opt_int("LONG_RECORD_MIN_BYTES", 256 * 1024, 1024),
        long_record_verify_stable_seconds=_opt_float(
            "LONG_RECORD_VERIFY_STABLE_SECONDS", 3.0, 0.5
        ),
        incomplete_segment_max_age_seconds=_opt_float(
            "INCOMPLETE_SEGMENT_MAX_AGE_SECONDS", 3.0, 0.0
        ),
        incomplete_segment_min_bytes=_opt_int("INCOMPLETE_SEGMENT_MIN_BYTES", 64 * 1024, 0),
        hotkey_save_replay=_hotkey_combo("ENCODER_HOTKEY_SAVE_REPLAY", "ctrl+shift+v"),
        hotkey_start_long=_hotkey_combo("ENCODER_HOTKEY_START_LONG", "ctrl+shift+r"),
        hotkey_stop_long=_hotkey_combo("ENCODER_HOTKEY_STOP_LONG", "ctrl+shift+s"),
        # Scoreboard state.json (replay_display_active). Set SCOREBOARD_STATE_PATH= empty to disable.
        scoreboard_state_path=_optional_path(
            "SCOREBOARD_STATE_PATH", r"C:\ReplayTrove\scoreboard\state.json"
        ),
        scoreboard_state_max_age_seconds=_opt_float(
            "SCOREBOARD_STATE_MAX_AGE_SECONDS",
            "60",
            minimum=5.0,
        ),
    )
