"""
Build ffmpeg argument lists for UVC capture → long recordings.
"""

from __future__ import annotations

from pathlib import Path

from settings import EncoderSettings


def effective_uvc_input_framerate(s: EncoderSettings) -> str:
    """The capture ``-framerate`` string used for ffmpeg (matches ``uvc_input_args`` / long-record dshow)."""
    be = s.uvc_capture_backend
    if be == "dshow":
        if s.uvc_dshow_framerate > 0:
            return str(s.uvc_dshow_framerate)
        return s.long_record_input_fps.strip()
    if be == "v4l2":
        return str(s.uvc_v4l2_framerate)
    raise ValueError(f"Unsupported UVC_CAPTURE_BACKEND: {be!r}")


def video_scale_fps_filter(width: int, height: int, fps: int) -> str:
    return f"scale={width}:{height}:flags=bicubic,fps={fps}"


def _dshow_input_framerate_args(s: EncoderSettings, fallback_fps: str) -> list[str]:
    """DirectShow needs -framerate to match the real capture rate or video runs at wrong speed vs audio."""
    if s.uvc_dshow_framerate > 0:
        return ["-framerate", str(s.uvc_dshow_framerate)]
    return ["-framerate", fallback_fps.strip()]


def _dshow_device_label(name: str) -> str:
    """Sanitize a DirectShow friendly name for ``-i video=…`` / ``audio=…`` (single argv token).

    Do not wrap in extra ``"…"`` around the label: ffmpeg 6.2+ on Windows then fails with
    ``Could not find video device with name […]`` even when the device exists.
    """
    return name.strip().replace("\\", "\\\\").replace('"', '\\"')


def _dshow_i_video(device: str) -> str:
    return f"video={_dshow_device_label(device)}"


def _dshow_i_audio(device: str) -> str:
    return f"audio={_dshow_device_label(device)}"


def _dshow_i_combined(video_dev: str, audio_dev: str) -> str:
    return f"{_dshow_i_video(video_dev)}:{_dshow_i_audio(audio_dev)}"


def _long_record_audio_filter(s: EncoderSettings) -> str | None:
    """Optional audio filter chain for long-record (aresample drift + fixed sync offset)."""
    parts: list[str] = []
    if s.uvc_capture_backend == "dshow" and s.long_record_audio_aresample_async_max > 0:
        osr = int(s.long_record_audio_sample_rate)
        parts.append(
            f"aresample=async={s.long_record_audio_aresample_async_max}:first_pts=0:osr={osr}"
        )
    if s.long_record_audio_sync_offset_ms != 0:
        sec = s.long_record_audio_sync_offset_ms / 1000.0
        # Positive ms → advance audio (subtract PTS) when sound lags behind video.
        parts.append(f"asetpts=PTS-({sec})/TB")
    return ",".join(parts) if parts else None


def _long_record_dshow_vf(s: EncoderSettings) -> str | None:
    """
    Downscale + fps for long-record dshow encode. Capture can stay 1080p60; this reduces
    encoder load. If width or height is <= 0, omit filter (native resolution; still uses -r for CFR).
    """
    w, h = s.long_record_encode_width, s.long_record_encode_height
    if w <= 0 or h <= 0:
        return None
    fps = s.long_record_output_fps.strip()
    if not fps:
        raise ValueError("LONG_RECORD_OUTPUT_FRAMERATE must be non-empty")
    return f"scale={w}:{h}:flags=bicubic,fps={fps}"


def _long_record_dshow_video_encode_args(s: EncoderSettings) -> list[str]:
    """Video encoder flags for long-record dshow (libx264 vs NVENC use different rate-control options)."""
    vc = s.long_record_video_codec.strip()
    vlow = vc.lower()
    cq = str(s.long_record_video_crf)
    preset = s.long_record_video_preset.strip()

    if vlow in ("h264_nvenc", "hevc_nvenc"):
        npreset = s.long_record_nvenc_preset.strip() or "p1"
        out: list[str] = ["-c:v", vc, "-preset", npreset]
        ntune = s.long_record_nvenc_tune.strip()
        if ntune:
            out += ["-tune", ntune]
        # Matches common NVENC CQ/VBR usage; LONG_RECORD_VIDEO_CRF maps to -cq.
        out += ["-rc", "vbr", "-cq", cq, "-b:v", "0"]
        return out

    if vlow in ("libx264", "libx265"):
        out = ["-c:v", vc, "-preset", preset, "-crf", cq]
        tune = s.long_record_libx264_tune.strip()
        if tune and vlow == "libx264":
            out += ["-tune", tune]
        return out

    return ["-c:v", vc, "-preset", preset, "-crf", cq]


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
        # Video-only here (quoted name: commas in friendly names must not break the option parser).
        spec = _dshow_i_video(v)
        args += ["-f", "dshow"]
        if s.uvc_dshow_video_size.strip():
            args += ["-video_size", s.uvc_dshow_video_size.strip()]
        args += _dshow_input_framerate_args(s, s.long_record_input_fps)
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
        s.long_output_width,
        s.long_output_height,
        s.long_output_fps,
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


def long_record_config_messages(s: EncoderSettings, output_file: Path) -> list[str]:
    """Resolved long-record options to log before spawning ffmpeg (UI + file logger)."""
    out = str(output_file.resolve())
    if s.uvc_capture_backend == "dshow":
        mode = (
            "split dshow inputs (video, then mic)"
            if s.long_record_dshow_split_audio
            else "single dshow input (video:audio)"
        )
        return [
            f"Long record (dshow): starting ffmpeg with ({mode}):",
            f"  video device: {s.uvc_video_device.strip()}",
            f"  audio device: {s.uvc_audio_device.strip()}",
            f"  capture fps: {effective_uvc_input_framerate(s)}",
            f"  output fps: {s.long_record_output_fps.strip()}",
            (
                f"  encode frame: {s.long_record_encode_width}x{s.long_record_encode_height} "
                f"(0x0 = native capture size)"
                if s.long_record_encode_width > 0 and s.long_record_encode_height > 0
                else "  encode frame: native (LONG_RECORD_ENCODE_WIDTH/HEIGHT 0 = no scale)"
            ),
            f"  rtbufsize: {s.long_record_rtbufsize.strip()}",
            "  video: codec="
            f"{s.long_record_video_codec.strip()} "
            + (
                f"preset={s.long_record_nvenc_preset.strip() or 'p1'} "
                f"tune={s.long_record_nvenc_tune.strip() or '(none)'} cq={s.long_record_video_crf}"
                if s.long_record_video_codec.strip().lower() in ("h264_nvenc", "hevc_nvenc")
                else f"preset={s.long_record_video_preset.strip()} "
                f"crf={s.long_record_video_crf} "
                f"libx264_tune={s.long_record_libx264_tune.strip() or '(none)'}"
            ),
            f"  pix_fmt: {s.long_record_pix_fmt.strip()}",
            f"  audio: codec={s.long_record_audio_codec.strip()} "
            f"bitrate={s.long_record_audio_bitrate.strip()} "
            f"sample_rate_hz={s.long_record_audio_sample_rate}",
            f"  audio sync offset ms (+ = advance audio): {s.long_record_audio_sync_offset_ms}",
            f"  audio aresample async (0=off): {s.long_record_audio_aresample_async_max}",
            f"  thread_queue_size (0=default): {s.long_record_thread_queue_size}",
            f"  max_muxing_queue_size (0=default): {s.long_record_max_muxing_queue_size}",
            f"  use_wallclock_as_timestamps: {int(s.long_record_use_wallclock_timestamps)}",
            f"  dshow_split_audio: {int(s.long_record_dshow_split_audio)}",
            f"  max seconds: {s.long_record_max_seconds}",
            f"  output path: {out}",
        ]
    return [
        "Long record (v4l2): starting ffmpeg with:",
        f"  video device: {s.uvc_video_device.strip()}",
        f"  audio device: {s.uvc_audio_device.strip() or '(none)'}",
        f"  output size/fps: {s.long_output_width}x{s.long_output_height} @ {s.long_output_fps}",
        f"  video: preset={s.long_preset} crf={s.long_crf}",
        f"  audio: aac {s.audio_bitrate_k}k @ {s.long_record_audio_sample_rate} Hz "
        f"(sync offset ms: {s.long_record_audio_sync_offset_ms})",
        f"  max seconds: {s.long_record_max_seconds}",
        f"  output path: {out}",
    ]


def _long_record_args_v4l2(s: EncoderSettings, output_file: Path) -> list[str]:
    vf = video_scale_fps_filter(
        s.long_output_width,
        s.long_output_height,
        s.long_output_fps,
    )
    long_gop = max(1, s.long_output_fps * s.long_record_keyint_seconds)

    cmd: list[str] = uvc_input_args(s)
    cmd += uvc_encode_maps(s)
    af = _long_record_audio_filter(s)
    if af:
        cmd += ["-af", af]
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
        str(s.long_record_audio_sample_rate),
        "-b:a",
        f"{s.audio_bitrate_k}k",
        str(output_file.resolve()),
    ]
    return cmd


def _long_record_args_dshow(s: EncoderSettings, output_file: Path) -> list[str]:
    v = s.uvc_video_device.strip()
    a = s.uvc_audio_device.strip()
    if not v or not a:
        raise ValueError(
            "UVC_VIDEO_DEVICE and UVC_AUDIO_DEVICE are required for long recording (dshow)."
        )
    rs = s.long_record_rtbufsize.strip()
    if not rs:
        raise ValueError("LONG_RECORD_RTBUFSIZE must be non-empty for long recording (dshow).")
    out_fps = s.long_record_output_fps.strip()
    out_gop = max(1, _round_fps_for_gop(out_fps) * s.long_record_keyint_seconds)
    # -y: overwrite output without blocking on an interactive prompt when the path exists.
    # -vsync cfr: CFR output (pairs with -r) so muxed duration tracks audio if the device rate drifts.
    # -thread_queue_size: bounded queue before decode — too small → dropped video frames while audio
    #   keeps running → shorter video duration vs audio in the mux (common “video ends before audio”).
    # -use_wallclock_as_timestamps: map dshow packet timing to wall clock so A/V share one timeline.
    # -rtbufsize: larger DirectShow real-time buffer to reduce "real-time buffer overflow" drops.
    # -framerate must match the real capture rate (see UVC_DSHOW_FRAMERATE / LONG_RECORD_INPUT_*);
    # -r output: encode timeline (e.g. 60 capture → 30 output).
    cmd: list[str] = ["-y", "-vsync", "cfr"]
    if s.long_record_thread_queue_size > 0:
        cmd += ["-thread_queue_size", str(s.long_record_thread_queue_size)]
    cmd += ["-rtbufsize", rs, "-f", "dshow"]
    cmd += _dshow_input_framerate_args(s, s.long_record_input_fps)
    if s.long_record_use_wallclock_timestamps:
        cmd += ["-use_wallclock_as_timestamps", "1"]
    if s.long_record_dshow_split_audio:
        cmd += ["-i", _dshow_i_video(v)]
        if s.long_record_thread_queue_size > 0:
            cmd += ["-thread_queue_size", str(s.long_record_thread_queue_size)]
        if s.long_record_use_wallclock_timestamps:
            cmd += ["-use_wallclock_as_timestamps", "1"]
        cmd += ["-f", "dshow", "-i", _dshow_i_audio(a)]
        cmd += ["-map", "0:v:0", "-map", "1:a:0"]
    else:
        cmd += ["-i", _dshow_i_combined(v, a)]
        cmd += ["-map", "0:v:0", "-map", "0:a:0"]
    vf = _long_record_dshow_vf(s)
    if vf is not None:
        cmd += ["-vf", vf]
    cmd += _long_record_dshow_video_encode_args(s)
    cmd += [
        "-pix_fmt",
        s.long_record_pix_fmt.strip(),
        "-g",
        str(out_gop),
        "-keyint_min",
        str(out_gop),
        "-sc_threshold",
        "0",
        "-r",
        out_fps,
    ]
    # Stretch/trim audio to track video PTS when capture-card vs mic clocks drift (growing A/V skew).
    # osr matches output -ar to avoid redundant resample when the device rate matches LONG_RECORD_AUDIO_SAMPLE_RATE.
    # Optional asetpts offset advances or delays audio vs video (LONG_RECORD_AUDIO_SYNC_OFFSET_MS).
    af = _long_record_audio_filter(s)
    if af:
        cmd += ["-af", af]
    cmd += [
        "-ar",
        str(s.long_record_audio_sample_rate),
        "-ac",
        "2",
        "-c:a",
        s.long_record_audio_codec.strip(),
        "-b:a",
        s.long_record_audio_bitrate.strip(),
    ]
    # Larger mux queue reduces forced flushes when one stream is bursty (helps interleave).
    if s.long_record_max_muxing_queue_size > 0:
        cmd += ["-max_muxing_queue_size", str(s.long_record_max_muxing_queue_size)]
    cmd += ["-t", str(s.long_record_max_seconds), str(output_file.resolve())]
    return cmd


def _round_fps_for_gop(fps: str) -> int:
    t = fps.strip()
    if "/" in t:
        num, den = t.split("/", 1)
        try:
            return max(1, round(float(num) / float(den)))
        except ValueError:
            return 30
    try:
        return max(1, round(float(t)))
    except ValueError:
        return 30


def long_record_args(s: EncoderSettings, output_file: Path) -> list[str]:
    """Encode capture to Matroska; duration capped at long_record_max_seconds (-t)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    if s.uvc_capture_backend == "dshow":
        return _long_record_args_dshow(s, output_file)
    return _long_record_args_v4l2(s, output_file)
