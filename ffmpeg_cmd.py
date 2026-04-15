"""
Build ffmpeg argument lists for UVC capture → rolling HLS buffer and long recordings.
"""

from __future__ import annotations

from pathlib import Path

from settings import EncoderSettings


def video_scale_fps_filter(width: int, height: int, fps: int) -> str:
    return f"scale={width}:{height}:flags=bicubic,fps={fps}"


def uvc_input_args(s: EncoderSettings) -> list[str]:
    """Open UVC / capture device (DirectShow on Windows, Video4Linux2 elsewhere)."""
    if not s.uvc_video_device.strip():
        raise ValueError(
            "UVC_VIDEO_DEVICE is required (see list_uvc_devices.py / ffmpeg device list)."
        )
    args: list[str] = ["-hide_banner", "-loglevel", "info", "-y"]
    args += ["-thread_queue_size", "512"]

    be = s.uvc_capture_backend
    if be == "dshow":
        if s.uvc_rtbufsize.strip():
            args += ["-rtbufsize", s.uvc_rtbufsize.strip()]
        v = s.uvc_video_device.strip()
        if s.uvc_audio_device.strip():
            spec = f"video={v}:audio={s.uvc_audio_device.strip()}"
        else:
            spec = f"video={v}"
        args += ["-f", "dshow"]
        if s.uvc_dshow_video_size.strip():
            args += ["-video_size", s.uvc_dshow_video_size.strip()]
        if s.uvc_dshow_framerate > 0:
            args += ["-framerate", str(s.uvc_dshow_framerate)]
        args += ["-i", spec]
    elif be == "v4l2":
        args += ["-f", "v4l2"]
        if s.uvc_v4l2_input_format.strip():
            args += ["-input_format", s.uvc_v4l2_input_format.strip()]
        args += [
            "-framerate",
            str(s.uvc_v4l2_framerate),
            "-video_size",
            s.uvc_v4l2_video_size.strip(),
            "-i",
            s.uvc_video_device.strip(),
        ]
        if s.uvc_audio_device.strip():
            args += ["-f", "alsa", "-i", s.uvc_audio_device.strip()]
    else:
        raise ValueError(f"Unsupported UVC_CAPTURE_BACKEND: {be!r} (use dshow or v4l2)")

    return args


def uvc_encode_maps(s: EncoderSettings) -> list[str]:
    """Stream maps for x264+aac encoding (handles optional / second-device audio)."""
    if s.uvc_capture_backend == "v4l2" and s.uvc_audio_device.strip():
        return ["-map", "0:v:0", "-map", "1:a:0"]
    return ["-map", "0:v:0", "-map", "0:a?"]


def uvc_probe_decode_args(s: EncoderSettings) -> list[str]:
    """Decode ~0.5s of video to verify the device opens (video stream only)."""
    vf = video_scale_fps_filter(
        s.buffer_output_width,
        s.buffer_output_height,
        s.buffer_output_fps,
    )
    cmd: list[str] = uvc_input_args(s)
    cmd += [
        "-map",
        "0:v:0",
        "-vf",
        vf,
        "-t",
        "0.5",
        "-f",
        "null",
        "-",
    ]
    return cmd


def buffer_hls_args(s: EncoderSettings) -> list[str]:
    """Encode capture to rolling HLS (video + audio when present) under buffer_dir."""
    buf = s.buffer_dir.resolve()
    buf.mkdir(parents=True, exist_ok=True)
    seg_pattern = str(buf / "seg_%05d.ts")
    playlist = str(buf / "live.m3u8")

    vf = video_scale_fps_filter(
        s.buffer_output_width,
        s.buffer_output_height,
        s.buffer_output_fps,
    )
    gop = s.hls_segment_seconds * s.buffer_output_fps

    cmd: list[str] = uvc_input_args(s)
    cmd += uvc_encode_maps(s)
    cmd += [
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
    ]

    cmd += [
        "-preset",
        s.buffer_preset,
        "-crf",
        str(s.buffer_crf),
        "-g",
        str(gop),
        "-keyint_min",
        str(gop),
        "-sc_threshold",
        "0",
    ]

    cmd += [
        "-c:a",
        "aac",
        "-ar",
        "48000",
        "-b:a",
        f"{s.audio_bitrate_k}k",
        "-f",
        "hls",
        "-hls_time",
        str(s.hls_segment_seconds),
        "-hls_list_size",
        str(s.hls_list_size),
        "-hls_flags",
        "delete_segments+append_list+omit_endlist+program_date_time",
        "-hls_segment_filename",
        seg_pattern,
        playlist,
    ]
    return cmd


def long_record_args(s: EncoderSettings, output_file: Path) -> list[str]:
    """Encode capture to Matroska; duration capped at long_record_max_seconds (-t)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    vf = video_scale_fps_filter(
        s.long_output_width,
        s.long_output_height,
        s.long_output_fps,
    )
    long_gop = max(1, s.long_output_fps * s.hls_segment_seconds)

    cmd: list[str] = uvc_input_args(s)
    cmd += uvc_encode_maps(s)
    cmd += [
        "-t",
        str(s.long_record_max_seconds),
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        s.long_preset,
        "-crf",
        str(s.long_crf),
        "-g",
        str(long_gop),
        "-keyint_min",
        str(long_gop),
        "-sc_threshold",
        "0",
    ]

    cmd += [
        "-c:a",
        "aac",
        "-ar",
        "48000",
        "-b:a",
        f"{s.audio_bitrate_k}k",
        str(output_file.resolve()),
    ]
    return cmd


def concat_recent_segments_args(
    _settings: EncoderSettings,
    concat_list: Path,
    output_mkv: Path,
) -> list[str]:
    """Remux TS segments listed in concat_list (ffmpeg demuxer format) to MKV."""
    return [
        "-hide_banner",
        "-loglevel",
        "warning",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_list.resolve()),
        "-c",
        "copy",
        str(output_mkv.resolve()),
    ]
