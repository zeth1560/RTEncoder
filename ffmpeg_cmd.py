"""
Build ffmpeg argument lists for NDI → rolling HLS buffer and long recordings.
"""

from __future__ import annotations

from pathlib import Path

from settings import EncoderSettings


def ndi_input_args(s: EncoderSettings) -> list[str]:
    if not s.ndi_source_name.strip():
        raise ValueError("NDI_SOURCE_NAME is required (exact NDI source name).")
    args: list[str] = ["-hide_banner", "-loglevel", "info", "-y"]
    args += ["-f", "libndi_newtek"]
    if s.ndi_extra_ips.strip():
        args += ["-extra_ips", s.ndi_extra_ips.strip()]
    args += ["-i", s.ndi_source_name.strip()]
    return args


def video_filter_1080p60() -> str:
    return "scale=1920:1080:flags=bicubic,fps=60"


def buffer_hls_args(s: EncoderSettings) -> list[str]:
    """Encode NDI to rolling HLS (video + audio) under buffer_dir."""
    buf = s.buffer_dir.resolve()
    buf.mkdir(parents=True, exist_ok=True)
    seg_pattern = str(buf / "seg_%05d.ts")
    playlist = str(buf / "live.m3u8")

    vf = video_filter_1080p60()

    cmd: list[str] = ndi_input_args(s)
    cmd += [
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
    ]

    gop = s.hls_segment_seconds * 60
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
    """Encode NDI to a single MP4 (1080p60, video + audio)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    vf = video_filter_1080p60()

    cmd: list[str] = ndi_input_args(s)
    cmd += [
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
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
    ]

    cmd += [
        "-c:a",
        "aac",
        "-ar",
        "48000",
        "-b:a",
        f"{s.audio_bitrate_k}k",
        "-movflags",
        "+faststart",
        str(output_file.resolve()),
    ]
    return cmd


def concat_recent_segments_args(
    s: EncoderSettings,
    concat_list: Path,
    output_mp4: Path,
) -> list[str]:
    """Remux TS segments listed in concat_list (ffmpeg demuxer format) to MP4."""
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
        str(output_mp4.resolve()),
    ]
