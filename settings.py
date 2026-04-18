"""
Environment-driven settings for the UVC long-record operator.

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
    long_preset: str
    long_crf: int
    audio_bitrate_k: int
    long_clips_folder: Path
    long_clips_trigger: Path | None
    encoder_log_file: Path
    long_record_min_bytes: int
    long_record_verify_stable_seconds: float
    hotkey_start_long: str
    hotkey_stop_long: str
    hotkey_restart_app: str
    long_output_width: int
    long_output_height: int
    long_output_fps: int
    long_record_max_seconds: int
    long_record_rtbufsize: str
    long_record_input_fps: str
    long_record_output_fps: str
    long_record_encode_width: int
    long_record_encode_height: int
    long_record_keyint_seconds: int
    long_record_video_codec: str
    long_record_video_preset: str
    long_record_video_crf: int
    long_record_pix_fmt: str
    long_record_libx264_tune: str
    long_record_nvenc_preset: str
    long_record_nvenc_tune: str
    long_record_audio_codec: str
    long_record_audio_bitrate: str
    long_record_audio_aresample_async_max: int
    long_record_thread_queue_size: int
    long_record_max_muxing_queue_size: int
    long_record_use_wallclock_timestamps: bool
    long_record_dshow_split_audio: bool
    encoder_state_path: Path
    encoder_self_restart_enabled: bool
    encoder_max_auto_restarts_before_app_restart: int
    encoder_unhealthy_window_seconds: float
    encoder_app_restart_exit_code: int
    ffmpeg_child_graceful_wait_seconds: float
    ffmpeg_child_terminate_wait_seconds: float
    long_record_stall_threshold_seconds: float
    long_record_ffprobe_verify: bool
    long_record_ffprobe_min_duration_seconds: float


def _hotkey_combo(name: str, default: str) -> str:
    return _opt(name, default).strip().lower()


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

    trig_lc = _opt("LONG_CLIPS_TRIGGER_FILE", "")

    log_dir = Path(_opt("ENCODER_LOG_DIR", r"C:\ReplayTrove\logs"))
    log_file = log_dir / "encoder_operator.log"

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
        long_preset=_opt("X264_PRESET_LONG", "veryfast"),
        long_crf=_opt_int("X264_CRF_LONG", 23, 0),
        audio_bitrate_k=_opt_int("AUDIO_BITRATE_K", 192, 32),
        long_clips_folder=Path(_opt("LONG_CLIPS_FOLDER", r"C:\ReplayTrove\long_clips")),
        long_clips_trigger=Path(trig_lc) if trig_lc else None,
        encoder_log_file=log_file,
        long_record_min_bytes=_opt_int("LONG_RECORD_MIN_BYTES", 256 * 1024, 1024),
        long_record_verify_stable_seconds=_opt_float(
            "LONG_RECORD_VERIFY_STABLE_SECONDS", 3.0, 0.5
        ),
        hotkey_start_long=_hotkey_combo("ENCODER_HOTKEY_START_LONG", "ctrl+shift+r"),
        hotkey_stop_long=_hotkey_combo("ENCODER_HOTKEY_STOP_LONG", "ctrl+shift+s"),
        hotkey_restart_app=_hotkey_combo(
            "ENCODER_HOTKEY_RESTART_APP", "q+p+backspace"
        ),
        long_output_width=_opt_int("LONG_OUTPUT_WIDTH", 1920, 16),
        long_output_height=_opt_int("LONG_OUTPUT_HEIGHT", 1080, 16),
        long_output_fps=_opt_int("LONG_OUTPUT_FPS", 30, 1),
        long_record_max_seconds=_opt_int("LONG_RECORD_MAX_SECONDS", 1200, 1),
        long_record_rtbufsize=_opt("LONG_RECORD_RTBUFSIZE", "512M"),
        long_record_input_fps=_opt("LONG_RECORD_INPUT_FRAMERATE", "60"),
        long_record_output_fps=_opt("LONG_RECORD_OUTPUT_FRAMERATE", "30"),
        long_record_encode_width=_opt_int("LONG_RECORD_ENCODE_WIDTH", 0, 0),
        long_record_encode_height=_opt_int("LONG_RECORD_ENCODE_HEIGHT", 0, 0),
        long_record_keyint_seconds=_opt_int("LONG_RECORD_KEYINT_SECONDS", 2, 1),
        long_record_video_codec=_opt("LONG_RECORD_VIDEO_CODEC", "libx264"),
        long_record_video_preset=_opt("LONG_RECORD_VIDEO_PRESET", "superfast"),
        long_record_video_crf=_opt_int("LONG_RECORD_VIDEO_CRF", 23, 0),
        long_record_pix_fmt=_opt("LONG_RECORD_PIX_FMT", "yuv420p"),
        long_record_libx264_tune=_opt("LONG_RECORD_LIBX264_TUNE", ""),
        long_record_nvenc_preset=_opt("LONG_RECORD_NVENC_PRESET", "p1"),
        long_record_nvenc_tune=_opt("LONG_RECORD_NVENC_TUNE", "ll"),
        long_record_audio_codec=_opt("LONG_RECORD_AUDIO_CODEC", "aac"),
        long_record_audio_bitrate=_opt("LONG_RECORD_AUDIO_BITRATE", "128k"),
        long_record_audio_aresample_async_max=_opt_int(
            "LONG_RECORD_AUDIO_ARESAMPLE_ASYNC_MAX",
            1000,
            0,
        ),
        long_record_thread_queue_size=_opt_int(
            "LONG_RECORD_THREAD_QUEUE_SIZE",
            2048,
            0,
        ),
        long_record_max_muxing_queue_size=_opt_int(
            "LONG_RECORD_MAX_MUXING_QUEUE_SIZE",
            4096,
            0,
        ),
        long_record_use_wallclock_timestamps=(
            _opt(
                "LONG_RECORD_USE_WALLCLOCK_TIMESTAMPS",
                "1" if sys.platform == "win32" else "0",
            ).lower()
            in ("1", "true", "yes", "on")
        ),
        long_record_dshow_split_audio=(
            _opt("LONG_RECORD_DSHOW_SPLIT_AUDIO", "0").lower()
            in ("1", "true", "yes", "on")
        ),
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
        encoder_app_restart_exit_code=_opt_int(
            "ENCODER_APP_RESTART_EXIT_CODE", 75, 1
        ),
        ffmpeg_child_graceful_wait_seconds=_opt_float(
            "FFMPEG_CHILD_GRACEFUL_WAIT_SECONDS", 2.0, 0.1
        ),
        ffmpeg_child_terminate_wait_seconds=_opt_float(
            "FFMPEG_CHILD_TERMINATE_WAIT_SECONDS", 4.0, 0.1
        ),
        long_record_stall_threshold_seconds=_opt_float(
            "LONG_RECORD_STALL_THRESHOLD_SECONDS", 12.0, 3.0
        ),
        long_record_ffprobe_verify=(
            _opt("LONG_RECORD_FFPROBE_VERIFY", "1").lower()
            in ("1", "true", "yes", "on")
        ),
        long_record_ffprobe_min_duration_seconds=_opt_float(
            "LONG_RECORD_FFPROBE_MIN_DURATION_SECONDS", 0.5, 0.5
        ),
    )
