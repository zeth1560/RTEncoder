"""
Environment-driven settings for the UVC recorder operator.
Instant replay is always written to a fixed path (INSTANT_REPLAY_SOURCE), atomically via *.copying.mkv.

Optional: copy .env.example to .env in this directory (or set variables in the system environment).
Values already set in the process environment are not overridden by .env.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


def load_dotenv_if_present() -> None:
    """Load ``.env`` from the encoder package directory, then from the current working directory."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    encoder_dir = Path(__file__).resolve().parent
    load_dotenv(encoder_dir / ".env")
    load_dotenv(Path.cwd() / ".env")


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
    uvc_capture_backend: str
    uvc_video_device: str
    uvc_audio_device: str
    uvc_rtbufsize: str
    uvc_dshow_video_size: str
    uvc_dshow_framerate: int
    uvc_v4l2_input_format: str
    uvc_v4l2_framerate: int
    uvc_v4l2_video_size: str
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
    buffer_health_startup_grace_seconds: float
    long_record_min_bytes: int
    long_record_verify_stable_seconds: float
    incomplete_segment_max_age_seconds: float
    incomplete_segment_min_bytes: int
    hotkey_save_replay: str
    hotkey_start_long: str
    hotkey_stop_long: str
    hotkey_restart_app: str
    buffer_output_width: int
    buffer_output_height: int
    buffer_output_fps: int
    long_output_width: int
    long_output_height: int
    long_output_fps: int
    long_record_max_seconds: int
    long_record_rtbufsize: str
    long_record_input_fps: str
    long_record_output_fps: str
    long_record_video_codec: str
    long_record_video_preset: str
    long_record_video_crf: int
    long_record_pix_fmt: str
    long_record_audio_codec: str
    long_record_audio_bitrate: str
    encoder_state_path: Path
    encoder_self_restart_enabled: bool
    encoder_max_auto_restarts_before_app_restart: int
    encoder_unhealthy_window_seconds: float
    encoder_max_replay_export_failures: int
    encoder_app_restart_exit_code: int
    ffmpeg_child_graceful_wait_seconds: float
    ffmpeg_child_terminate_wait_seconds: float
    replay_export_min_bytes: int
    replay_export_ffprobe_verify: bool
    replay_export_min_duration_seconds: float
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
    load_dotenv_if_present()
    ff = Path(_opt("FFMPEG_PATH", r"C:\ffmpeg\bin\ffmpeg.exe"))
    if not ff.exists():
        w = shutil.which("ffmpeg")
        if w:
            ff = Path(w)

    default_uvc_backend = "dshow" if sys.platform == "win32" else "v4l2"
    uvc_backend = _opt("UVC_CAPTURE_BACKEND", default_uvc_backend).lower()
    if uvc_backend not in ("dshow", "v4l2"):
        raise ValueError("UVC_CAPTURE_BACKEND must be 'dshow' or 'v4l2'")

    trig_ir = _opt("INSTANT_REPLAY_TRIGGER_FILE", "")
    trig_lc = _opt("LONG_CLIPS_TRIGGER_FILE", "")

    log_dir = Path(_opt("ENCODER_LOG_DIR", r"C:\ReplayTrove\logs"))
    log_file = log_dir / "encoder_operator.log"

    instant = Path(
        _opt("INSTANT_REPLAY_SOURCE", r"C:\ReplayTrove\INSTANTREPLAY.mkv")
    )

    return EncoderSettings(
        ffmpeg_path=ff,
        uvc_capture_backend=uvc_backend,
        uvc_video_device=_opt("UVC_VIDEO_DEVICE", "USB3.0 HD Video Capture"),
        uvc_audio_device=_opt(
            "UVC_AUDIO_DEVICE",
            "Microphone (USB3.0 HD Audio Capture)",
        ),
        uvc_rtbufsize=_opt("UVC_DSHOW_RTBUFSIZE", ""),
        uvc_dshow_video_size=_opt("UVC_DSHOW_VIDEO_SIZE", ""),
        uvc_dshow_framerate=_opt_int("UVC_DSHOW_FRAMERATE", 0, 0),
        uvc_v4l2_input_format=_opt("UVC_V4L2_INPUT_FORMAT", ""),
        uvc_v4l2_framerate=_opt_int("UVC_V4L2_FRAMERATE", 60, 1),
        uvc_v4l2_video_size=_opt("UVC_V4L2_VIDEO_SIZE", "1920x1080"),
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
        buffer_health_startup_grace_seconds=_opt_float(
            "BUFFER_HEALTH_STARTUP_GRACE_SECONDS", 10.0, 0.0
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
        hotkey_restart_app=_hotkey_combo(
            "ENCODER_HOTKEY_RESTART_APP", "q+p+backspace"
        ),
        buffer_output_width=_opt_int("BUFFER_OUTPUT_WIDTH", 1920, 16),
        buffer_output_height=_opt_int("BUFFER_OUTPUT_HEIGHT", 1080, 16),
        buffer_output_fps=_opt_int("BUFFER_OUTPUT_FPS", 60, 1),
        long_output_width=_opt_int("LONG_OUTPUT_WIDTH", 1920, 16),
        long_output_height=_opt_int("LONG_OUTPUT_HEIGHT", 1080, 16),
        long_output_fps=_opt_int("LONG_OUTPUT_FPS", 30, 1),
        long_record_max_seconds=_opt_int("LONG_RECORD_MAX_SECONDS", 1200, 1),
        long_record_rtbufsize=_opt("LONG_RECORD_RTBUFSIZE", "512M"),
        long_record_input_fps=_opt("LONG_RECORD_INPUT_FRAMERATE", "30"),
        long_record_output_fps=_opt("LONG_RECORD_OUTPUT_FRAMERATE", "30"),
        long_record_video_codec=_opt("LONG_RECORD_VIDEO_CODEC", "libx264"),
        long_record_video_preset=_opt("LONG_RECORD_VIDEO_PRESET", "superfast"),
        long_record_video_crf=_opt_int("LONG_RECORD_VIDEO_CRF", 23, 0),
        long_record_pix_fmt=_opt("LONG_RECORD_PIX_FMT", "yuv420p"),
        long_record_audio_codec=_opt("LONG_RECORD_AUDIO_CODEC", "aac"),
        long_record_audio_bitrate=_opt("LONG_RECORD_AUDIO_BITRATE", "128k"),
        encoder_state_path=Path(
            _opt(
                "ENCODER_STATE_PATH",
                r"C:\ReplayTrove\scoreboard\encoder_state.json",
            )
        ),
        encoder_self_restart_enabled=(
            _opt("ENCODER_SELF_RESTART_ENABLED", "0").lower()
            in ("1", "true", "yes", "on")
        ),
        encoder_max_auto_restarts_before_app_restart=_opt_int(
            "ENCODER_MAX_AUTO_RESTARTS_BEFORE_APP_RESTART", 5, 1
        ),
        encoder_unhealthy_window_seconds=_opt_float(
            "ENCODER_UNHEALTHY_WINDOW_SECONDS", 120.0, 5.0
        ),
        encoder_max_replay_export_failures=_opt_int(
            "ENCODER_MAX_REPLAY_EXPORT_FAILURES", 5, 1
        ),
        encoder_app_restart_exit_code=_opt_int(
            "ENCODER_APP_RESTART_EXIT_CODE", 75, 1
        ),
        ffmpeg_child_graceful_wait_seconds=_opt_float(
            "FFMPEG_CHILD_GRACEFUL_WAIT_SECONDS", 2.0, 0.1
        ),
        ffmpeg_child_terminate_wait_seconds=_opt_float(
            "FFMPEG_CHILD_TERMINATE_WAIT_SECONDS", 4.0, 0.1
        ),
        replay_export_min_bytes=_opt_int(
            "REPLAY_EXPORT_MIN_BYTES", 32 * 1024, 1024
        ),
        replay_export_ffprobe_verify=(
            _opt("REPLAY_EXPORT_FFPROBE_VERIFY", "1").lower()
            in ("1", "true", "yes", "on")
        ),
        replay_export_min_duration_seconds=_opt_float(
            "REPLAY_EXPORT_MIN_DURATION_SECONDS", 0.25, 0.01
        ),
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
