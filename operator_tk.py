"""
ReplayTrove UVC encoder operator: rolling HLS buffer, instant replay export, long recordings.

Replay / long-record actions use explicit state (see operator_state); no queued replay saves.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import queue
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Callable
import tkinter as tk
from tkinter import messagebox, scrolledtext

from app_logging import setup_encoder_logging
from buffer_health import BufferHealthSnapshot, snapshot_buffer_health
from buffer_purge import purge_stale_hls_artifacts
from encoder_state import (
    STATE_BLOCKED,
    STATE_DEGRADED,
    STATE_READY,
    STATE_RECORDING,
    STATE_RESTARTING,
    STATE_SHUTTING_DOWN,
    STATE_STARTING,
    STATE_UNAVAILABLE,
    publish_encoder_state,
)
from ffmpeg_cmd import (
    buffer_hls_args,
    concat_recent_segments_args,
    long_record_args,
    long_record_config_messages,
)
from operator_state import (
    LongRecordState,
    ReplayControlState,
    ScoreboardEncoderSnapshot,
    read_scoreboard_replay_for_encoder,
)
from replay_export import build_replay_plan
from replay_verify import verify_replay_mkv
from settings import EncoderSettings, load_encoder_settings
from startup_validate import validate_startup
from subprocess_win import no_console_creationflags

logger = logging.getLogger("replaytrove.encoder")


def _long_record_fault_summary(exit_code: int, stderr_tail: str) -> str:
    """Human-facing summary for scoreboard/operators when long-record ffmpeg fails."""
    if exit_code == -1:
        return f"LONG RECORD: failed to launch ffmpeg. {stderr_tail}"
    if exit_code == -2:
        return f"LONG RECORD: configuration error — {stderr_tail}"

    low = stderr_tail.lower()
    if any(
        x in low
        for x in (
            "device or resource busy",
            "resource busy",
            "in use by another",
            "being used by another",
            "cannot start capture",
            "already in use",
        )
    ):
        return (
            "LONG RECORD: capture device is busy or in use. "
            "Another process (often the rolling buffer) may already own the camera; "
            "stop long record from competing with the same UVC device."
        )
    if any(
        x in low
        for x in (
            "could not find device",
            "could not open",
            "cannot open",
            "i/o error",
            "no such device",
            "device not found",
        )
    ):
        return (
            "LONG RECORD: could not open the video device "
            "(check UVC_VIDEO_DEVICE name, USB, and drivers)."
        )
    if "could not run graph" in low or "could not set video" in low:
        return (
            "LONG RECORD: DirectShow could not configure the device "
            "(try UVC_DSHOW_VIDEO_SIZE / UVC_DSHOW_FRAMERATE or match the device’s native mode)."
        )
    if "permission denied" in low:
        return "LONG RECORD: permission denied opening capture device."
    return (
        f"LONG RECORD failed (ffmpeg exit {exit_code}). "
        "The rolling buffer may be using the only capture handle — see encoder log for ffmpeg stderr."
    )


def _pretty_hotkey_combo(combo: str) -> str:
    return "+".join(p.strip().capitalize() for p in combo.split("+"))


def _touch(path: Path | None) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def _flush_logger_handlers() -> None:
    log = logging.getLogger("replaytrove.encoder")
    for h in log.handlers:
        try:
            h.flush()
        except OSError:
            pass


def _hardened_stop_ffmpeg_process(
    proc: subprocess.Popen[bytes],
    *,
    settings: EncoderSettings,
    reaper_thread: threading.Thread | None,
    label: str,
    log_line: Callable[[str], None],
) -> tuple[str, bool]:
    """
    Send 'q', wait, terminate, kill if needed.
    Returns (mode, used_kill) where mode is already_dead|graceful|terminated|killed.
    """
    if proc.poll() is not None:
        if reaper_thread is not None and reaper_thread.is_alive():
            reaper_thread.join(timeout=5.0)
        return ("already_dead", False)

    if proc.stdin and not proc.stdin.closed:
        try:
            proc.stdin.write(b"q\n")
            proc.stdin.flush()
            proc.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    graceful = settings.ffmpeg_child_graceful_wait_seconds
    termwait = settings.ffmpeg_child_terminate_wait_seconds
    t0 = time.monotonic()
    while proc.poll() is None and (time.monotonic() - t0) < graceful:
        time.sleep(0.05)

    if proc.poll() is not None:
        if reaper_thread is not None:
            reaper_thread.join(timeout=5.0)
        log_line(f"{label}: ffmpeg stopped (graceful after 'q').")
        logger.info("%s: ffmpeg stopped (graceful after 'q').", label)
        return ("graceful", False)

    log_line(f"{label}: graceful quit timed out ({graceful}s); sending terminate.")
    logger.warning(
        "%s: graceful quit timed out (%ss); sending terminate.", label, graceful
    )
    try:
        proc.terminate()
    except OSError as e:
        logger.warning("%s: terminate() failed: %s", label, e)

    t1 = time.monotonic()
    while proc.poll() is None and (time.monotonic() - t1) < termwait:
        time.sleep(0.05)

    if proc.poll() is not None:
        if reaper_thread is not None:
            reaper_thread.join(timeout=5.0)
        log_line(f"{label}: ffmpeg stopped after terminate.")
        logger.info("%s: ffmpeg stopped after terminate.", label)
        return ("terminated", False)

    log_line(f"{label}: terminate timed out ({termwait}s); killing process.")
    logger.error(
        "%s: terminate timed out (%ss); killing process.", label, termwait
    )
    try:
        proc.kill()
    except OSError as e:
        logger.warning("%s: kill() failed: %s", label, e)

    if reaper_thread is not None:
        reaper_thread.join(timeout=45.0)
    log_line(f"{label}: ffmpeg stop completed (kill).")
    logger.error("%s: ffmpeg stop completed (kill).", label)
    return ("killed", True)


class BufferProcess:
    def __init__(
        self,
        settings: EncoderSettings,
        log_q: queue.Queue[str],
        on_exit: Callable[[int, bool], None] | None = None,
    ) -> None:
        self.settings = settings
        self.log_q = log_q
        self.on_exit = on_exit
        self.proc: subprocess.Popen[bytes] | None = None
        self._stderr_thread: threading.Thread | None = None
        self._reaper_thread: threading.Thread | None = None
        self._intentional_stop = False
        self._stderr_tail: deque[str] = deque(maxlen=100)
        self.dropped_line_hits = 0

    def _emit_log(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def stderr_tail_recent(self, max_lines: int = 50) -> str:
        lines = list(self._stderr_tail)
        if max_lines <= 0:
            return ""
        return "\n".join(lines[-max_lines:])

    def start(self) -> bool:
        self.stop()
        try:
            purge_stale_hls_artifacts(self.settings.buffer_dir)
            args = [str(self.settings.ffmpeg_path)] + buffer_hls_args(self.settings)
        except ValueError as e:
            self._emit_log(f"Buffer config error: {e}")
            return False

        buf = self.settings.buffer_dir.resolve()
        playlist_target = buf / "live.m3u8"
        segment_pattern = buf / "seg_%05d.ts"
        self._emit_log(
            f"Buffer start: dir={buf} dir_exists={buf.is_dir()} "
            f"playlist_target={playlist_target} "
            f"hls_segment_pattern={segment_pattern}"
        )
        logger.info(
            "Buffer start: dir=%s dir_exists=%s playlist=%s segments=%s",
            buf,
            buf.is_dir(),
            playlist_target,
            segment_pattern,
        )

        self._intentional_stop = False
        try:
            self.proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=False,
                **no_console_creationflags(),
            )
        except OSError as e:
            self._emit_log(f"Failed to start buffer ffmpeg: {e}")
            return False

        pid = self.proc.pid
        self._emit_log(
            f"Buffer ffmpeg started (PID {pid}); HLS output → {playlist_target}"
        )

        proc_ref = self.proc

        def drain_stderr() -> None:
            assert proc_ref.stderr
            for raw in proc_ref.stderr:
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                self._stderr_tail.append(line)
                low = line.lower()
                if "drop" in low and "frame" in low:
                    self.dropped_line_hits += 1
                if "error" in low or "failed" in low:
                    logger.warning("[buf stderr] %s", line)
                else:
                    logger.debug("[buf stderr] %s", line)

        def reaper() -> None:
            code = proc_ref.wait()
            intentional = self._intentional_stop
            self._emit_log(
                f"Buffer ffmpeg process ended (exit={code}, intentional_stop={intentional})."
            )
            if self.on_exit:
                try:
                    self.on_exit(code, intentional)
                except Exception:
                    logger.exception("buffer on_exit callback failed")

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()
        return True

    def stop(self) -> tuple[str, bool]:
        """Hardened stop; returns (stop_mode, used_kill)."""
        p = self.proc
        if p is None:
            return ("noop", False)
        self._intentional_stop = True
        mode, killed = _hardened_stop_ffmpeg_process(
            p,
            settings=self.settings,
            reaper_thread=self._reaper_thread,
            label="Buffer",
            log_line=self._emit_log,
        )
        self.proc = None
        self._reaper_thread = None
        self._stderr_thread = None
        self._emit_log("Buffer ffmpeg stop completed.")
        return (mode, killed)

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def pid(self) -> int | None:
        p = self.proc
        return p.pid if p and p.poll() is None else None

    def stderr_tail_text(self) -> str:
        return "\n".join(self._stderr_tail)


class LongRecordProcess:
    def __init__(
        self,
        settings: EncoderSettings,
        log_q: queue.Queue[str],
        *,
        on_ffmpeg_exit: Callable[[int, bool, str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.log_q = log_q
        self._on_ffmpeg_exit = on_ffmpeg_exit
        self.proc: subprocess.Popen[bytes] | None = None
        self.output_path: Path | None = None
        self._stderr_thread: threading.Thread | None = None
        self._reaper_thread: threading.Thread | None = None
        self._intentional_stop = False
        self._stderr_tail: deque[str] = deque(maxlen=250)
        self.dropped_line_hits = 0

    def _emit_log(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def _stderr_blob(self) -> str:
        return "\n".join(self._stderr_tail)

    def start(self) -> bool:
        self.stop()
        ts = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        out = self.settings.long_clips_folder / f"{ts}.mkv"
        try:
            args = [str(self.settings.ffmpeg_path)] + long_record_args(self.settings, out)
        except ValueError as e:
            self._emit_log(f"Long record config error: {e}")
            if self._on_ffmpeg_exit:
                self._on_ffmpeg_exit(-2, False, str(e))
            return False

        for line in long_record_config_messages(self.settings, out):
            self._emit_log(line)

        self._intentional_stop = False
        try:
            self.proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=False,
                **no_console_creationflags(),
            )
        except OSError as e:
            self._emit_log(f"Failed to start long record: {e}")
            if self._on_ffmpeg_exit:
                self._on_ffmpeg_exit(-1, False, str(e))
            return False

        self.output_path = out
        self._emit_log(f"Long recording started (PID {self.proc.pid}) → {out}")
        proc_ref = self.proc

        def drain_stderr() -> None:
            assert proc_ref.stderr
            for raw in proc_ref.stderr:
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                self._stderr_tail.append(line)
                low = line.lower()
                if "drop" in low and "frame" in low:
                    self.dropped_line_hits += 1
                if "error" in low or "failed" in low:
                    logger.warning("[long stderr] %s", line)
                else:
                    logger.debug("[long stderr] %s", line)

        def reaper() -> None:
            code = proc_ref.wait()
            self._emit_log(f"Long record ffmpeg ended (exit={code}).")
            fn = self._on_ffmpeg_exit
            if fn:
                fn(code, self._intentional_stop, self._stderr_blob())

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()

        pid_tag = self.proc.pid

        def early_death_watch() -> None:
            time.sleep(3.0)
            if self._intentional_stop:
                return
            p = self.proc
            if p is None or p.pid != pid_tag:
                return
            c = p.poll()
            if c is not None and c != 0 and self._on_ffmpeg_exit:
                self._on_ffmpeg_exit(c, False, self._stderr_blob())

        threading.Thread(
            target=early_death_watch, daemon=True, name="long-record-early-fail"
        ).start()
        return True

    def stop(
        self, *, on_verified: Callable[[Path | None, str], None] | None = None
    ) -> tuple[str, bool]:
        """Hardened stop; returns (stop_mode, used_kill)."""
        p = self.proc
        out = self.output_path
        if p is None:
            if on_verified:
                on_verified(None, "no active recording")
            return ("noop", False)
        self._intentional_stop = True
        mode, killed = _hardened_stop_ffmpeg_process(
            p,
            settings=self.settings,
            reaper_thread=self._reaper_thread,
            label="Long record",
            log_line=self._emit_log,
        )
        self.proc = None
        self._reaper_thread = None
        self._stderr_thread = None
        self._emit_log("Long record ffmpeg stop joined; starting file verification…")

        def verify() -> None:
            msg, path_ok = self._verify_long_output(out)
            if path_ok:
                logger.info("Long record verification ok: %s", msg)
                self._emit_log(f"Long record verified: {msg}")
                _touch(self.settings.long_clips_trigger)
                self._emit_log("LONG_CLIPS_TRIGGER_FILE touched after verification.")
            else:
                logger.error("Long record verification failed: %s", msg)
                self._emit_log(f"Long record verification FAILED: {msg}")
            if on_verified:
                on_verified(out if path_ok else None, msg)

        threading.Thread(target=verify, daemon=True).start()
        return (mode, killed)

    def _verify_long_output(self, out: Path | None) -> tuple[str, bool]:
        if out is None:
            return "no output path", False
        if not out.exists():
            return f"missing file {out}", False
        try:
            sz = out.stat().st_size
        except OSError as e:
            return f"stat failed: {e}", False
        if sz < self.settings.long_record_min_bytes:
            return f"too small ({sz} < {self.settings.long_record_min_bytes} bytes)", False

        stable_need = self.settings.long_record_verify_stable_seconds
        prev = (-1, -1.0)
        stable_rounds = 0
        needed = max(2, int(stable_need / 0.5) if stable_need > 0 else 2)
        for _ in range(needed + 5):
            try:
                st = out.stat()
                sig = (st.st_size, st.st_mtime)
            except OSError as e:
                return f"stat unstable: {e}", False
            if sig == prev:
                stable_rounds += 1
            else:
                stable_rounds = 0
            prev = sig
            if stable_rounds >= 2 and stable_need <= 0.01:
                break
            if stable_rounds >= 2 and (time.time() - st.st_mtime) >= stable_need:
                break
            time.sleep(0.5)

        if stable_rounds < 2:
            return "file still changing after stop", False

        prob = self.settings.ffmpeg_path.with_name(
            "ffprobe.exe" if self.settings.ffmpeg_path.suffix.lower() == ".exe" else "ffprobe"
        )
        if not prob.is_file():
            prob = self.settings.ffmpeg_path.parent / "ffprobe.exe"
        if prob.is_file():
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
                        str(out),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    **no_console_creationflags(),
                )
                if r.returncode != 0:
                    return f"ffprobe exit {r.returncode}: {(r.stderr or '')[-400:]}", False
                dur = float((r.stdout or "").strip() or "0")
                if dur <= 0.5:
                    return f"ffprobe duration suspicious: {dur}", False
            except (OSError, ValueError, subprocess.TimeoutExpired) as e:
                return f"ffprobe failed: {e}", False

        return f"size={sz} bytes, stable ok", True

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def pid(self) -> int | None:
        p = self.proc
        return p.pid if p and p.poll() is None else None


class OperatorApp:
    def __init__(self, root: tk.Tk, settings: EncoderSettings) -> None:
        self.root = root
        self.settings = settings
        self.log_q: queue.Queue[str] = queue.Queue()
        self.exit_code: int = 0
        self._shutting_down = False
        self._app_restart_requested = False
        self._full_restart_perform_started = False
        self._app_start_monotonic = time.monotonic()
        self._replay_lock = threading.Lock()
        self._replay_export_in_progress = False
        self._replay_export_state = tk.StringVar(value="Replay: IDLE")
        self._buffer_state = tk.StringVar(value="Buffer: …")
        self._health_summary = tk.StringVar(value="")
        self._last_replay_ts: str = "—"
        self._last_error_ts: str = "—"
        self._last_error_msg: str = "—"
        self._auto_restart_count = 0
        self._consecutive_buffer_restart_failures = 0
        self._replay_export_consecutive_failures = 0
        self._unhealthy_since: float | None = None
        self._unhealthy_escalate_sent = False
        self._ffmpeg_kill_stop_count = 0
        self._backoff_idx = 0
        self._backoffs = (2, 5, 10)
        self._buffer_intentional_stop = False
        self._last_segment_ts: str = "—"
        self._stall_ticks = 0
        self._startup_blocked = False
        self._restart_pending = False
        self._replay_gating_log_key: object | None = None
        self._long_recording_last_fault: str = ""
        self._long_record_fail_reported = False
        self._long_record_exit_lock = threading.Lock()
        self._buffer_start_monotonic: float | None = None
        self._buffer_has_produced_hls: bool = False
        self._buffer_startup_failure_logged: bool = False

        setup_encoder_logging(settings.encoder_log_file, ui_queue=self.log_q)
        logger.info("Encoder operator starting")

        self.buffer = BufferProcess(settings, self.log_q, on_exit=self._on_buffer_exit)
        self.long_rec = LongRecordProcess(
            settings,
            self.log_q,
            on_ffmpeg_exit=self._on_long_record_ffmpeg_exit,
        )

        root.title("ReplayTrove UVC Encoder")
        root.geometry("820x640")

        self.status_long = tk.StringVar(value="Long: idle")

        info = tk.Frame(root)
        info.pack(fill=tk.X, padx=8, pady=6)
        uvc_line = settings.uvc_video_device or "(set UVC_VIDEO_DEVICE)"
        if settings.uvc_capture_backend == "dshow":
            src_lbl = f"UVC (DirectShow): {uvc_line}"
        else:
            src_lbl = f"UVC (v4l2): {uvc_line}"
        tk.Label(
            info,
            text=src_lbl,
            anchor="w",
        ).pack(fill=tk.X)
        tk.Label(
            info,
            text=f"Encoder: libx264  |  Buffer: {settings.buffer_dir}",
            anchor="w",
        ).pack(fill=tk.X)
        tk.Label(
            info,
            text=f"Instant replay file: {settings.instant_replay_path}",
            anchor="w",
        ).pack(fill=tk.X)

        tk.Label(info, textvariable=self._buffer_state, anchor="w", fg="darkgreen").pack(
            fill=tk.X
        )
        tk.Label(info, textvariable=self.status_long, anchor="w").pack(fill=tk.X)
        tk.Label(info, textvariable=self._replay_export_state, anchor="w").pack(fill=tk.X)
        tk.Label(
            info,
            textvariable=self._health_summary,
            anchor="w",
            justify=tk.LEFT,
            font=("Consolas", 9),
        ).pack(fill=tk.X)

        btns = tk.Frame(root)
        btns.pack(fill=tk.X, padx=8, pady=4)

        self.btn_restart = tk.Button(btns, text="Restart buffer", command=self.on_restart_buffer)
        self.btn_restart.pack(side=tk.LEFT, padx=2)
        self.btn_save = tk.Button(btns, text="Save instant replay", command=self.request_replay_save)
        self.btn_save.pack(side=tk.LEFT, padx=2)
        self.btn_start_long = tk.Button(
            btns, text="Start long recording", command=self.request_start_long
        )
        self.btn_start_long.pack(side=tk.LEFT, padx=2)
        self.btn_stop_long = tk.Button(
            btns, text="Stop long recording", command=self.request_stop_long
        )
        self.btn_stop_long.pack(side=tk.LEFT, padx=2)
        self.btn_copy_log = tk.Button(btns, text="Copy log", command=self._copy_log_to_clipboard)
        self.btn_copy_log.pack(side=tk.LEFT, padx=2)

        self.log_widget = scrolledtext.ScrolledText(
            root,
            height=20,
            state=tk.NORMAL,
            font=("Consolas", 9),
            wrap=tk.WORD,
        )
        self.log_widget.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)
        self.log_widget.bind("<Key>", self._log_readonly_key)
        self.log_widget.bind("<<Paste>>", lambda e: "break")
        self.log_widget.bind("<Button-1>", self._log_focus_on_click)

        hk_line = (
            f"{_pretty_hotkey_combo(settings.hotkey_save_replay)} → Save instant replay | "
            f"{_pretty_hotkey_combo(settings.hotkey_start_long)} → Start long recording | "
            f"{_pretty_hotkey_combo(settings.hotkey_stop_long)} → Stop long recording | "
            f"{_pretty_hotkey_combo(settings.hotkey_restart_app)} → Restart encoder app"
        )
        if sys.platform == "win32":
            footer_txt = "Global hotkeys (Windows): " + hk_line
        else:
            footer_txt = "Global hotkeys: Windows only — " + hk_line
        tk.Label(
            root,
            text=footer_txt,
            anchor="w",
            fg="gray30",
            font=("Segoe UI", 9),
            wraplength=780,
            justify=tk.LEFT,
        ).pack(fill=tk.X, padx=8, pady=(0, 6))

        self.root.protocol("WM_DELETE_WINDOW", self.on_quit)
        self._poll_log()
        self._watchdog_tick()

        errs, warns = validate_startup(settings)
        for w in warns:
            logger.warning("Startup check: %s", w)
            self._append_log(f"[startup] {w}\n")
        for e in errs:
            logger.error("Startup check FAILED: %s", e)
            self._append_log(f"[startup ERROR] {e}\n")

        snap0 = snapshot_buffer_health(settings)
        if errs:
            self._startup_blocked = True
            self._publish_encoder_state(snap0, override_last_error="; ".join(errs))
            messagebox.showerror(
                "Startup validation failed",
                "Buffer will not start until these are fixed:\n\n" + "\n".join(errs),
            )
            self._buffer_state.set("Buffer: BLOCKED (see log)")
            self.btn_save.configure(state=tk.DISABLED)
        else:
            self._publish_encoder_state(snap0)
            self._buffer_state.set("Buffer: STARTING")
            ok = self.buffer.start()
            if ok:
                self._mark_buffer_started()
                self._buffer_state.set("Buffer: RUNNING")
                self._backoff_idx = 0
                self._consecutive_buffer_restart_failures = 0
            else:
                self._buffer_state.set("Buffer: FAILED TO START")
                self._last_error_msg = "buffer failed to start"
                self._consecutive_buffer_restart_failures += 1
            self._publish_encoder_state(self._buffer_health_snapshot())

        self._sync_button_states()
        self._refresh_replay_status_label()
        self._register_global_hotkeys()

    def _seconds_since_buffer_start(self) -> float | None:
        if self._buffer_start_monotonic is None or not self.buffer.running():
            return None
        return time.monotonic() - self._buffer_start_monotonic

    def _buffer_health_snapshot(self) -> BufferHealthSnapshot:
        return snapshot_buffer_health(
            self.settings,
            buffer_running=self.buffer.running(),
            seconds_since_buffer_start=self._seconds_since_buffer_start(),
            buffer_had_successful_hls=self._buffer_has_produced_hls,
        )

    def _mark_buffer_started(self) -> None:
        self._buffer_start_monotonic = time.monotonic()
        self._buffer_has_produced_hls = False
        self._buffer_startup_failure_logged = False

    @property
    def long_record_state(self) -> LongRecordState:
        return (
            LongRecordState.RECORDING
            if self.long_rec.running()
            else LongRecordState.NOT_RECORDING
        )

    def _on_long_record_ffmpeg_exit(
        self, exit_code: int, intentional: bool, stderr_tail: str
    ) -> None:
        if intentional or self._shutting_down:
            return
        if exit_code == 0:
            return
        with self._long_record_exit_lock:
            if self._long_record_fail_reported:
                return
            self._long_record_fail_reported = True

        fault = _long_record_fault_summary(exit_code, stderr_tail)
        self._long_recording_last_fault = fault
        self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
        self._last_error_msg = fault
        logger.error(
            "LONG RECORD FAILURE (exit=%s)\n%s\n--- ffmpeg stderr (tail) ---\n%s",
            exit_code,
            fault,
            stderr_tail[-4000:],
        )

        def _alert() -> None:
            messagebox.showerror(
                "Long recording — capture failed",
                fault + "\n\nFull stderr is in the encoder log.",
            )

        self.root.after(0, _alert)
        self.root.after(0, lambda: self._emit_ui(f"CRITICAL: {fault}"))
        self.root.after(
            0,
            lambda: self._publish_encoder_state(
                self._buffer_health_snapshot()
            ),
        )

    def _primary_encoder_state(
        self, snap: BufferHealthSnapshot, buf_running: bool
    ) -> str:
        if self._shutting_down:
            return STATE_SHUTTING_DOWN
        if self._app_restart_requested:
            return STATE_RESTARTING
        if self._startup_blocked:
            return STATE_BLOCKED
        if self.long_rec.running():
            return STATE_RECORDING
        if self._restart_pending:
            return STATE_RESTARTING
        if not buf_running:
            grace = 20.0
            if (time.monotonic() - self._app_start_monotonic) < grace:
                return STATE_STARTING
            return STATE_UNAVAILABLE
        if buf_running and snap.in_startup_grace:
            return STATE_STARTING
        if (
            buf_running
            and not snap.healthy
            and "startup failed" in snap.detail.lower()
        ):
            return STATE_DEGRADED
        if not snap.healthy:
            return STATE_DEGRADED
        return STATE_READY

    def _status_text_for_state(self, state: str) -> str:
        return {
            STATE_STARTING: "Recorder Starting",
            STATE_READY: "Recorder Ready",
            STATE_DEGRADED: "Recorder Degraded",
            STATE_UNAVAILABLE: "Recorder Unavailable",
            STATE_RECORDING: "Recorder Recording",
            STATE_RESTARTING: "Recorder Restarting",
            STATE_BLOCKED: "Recorder Blocked",
            STATE_SHUTTING_DOWN: "Recorder Shutting Down",
        }.get(state, "Recorder Unknown")

    def _publish_encoder_state(
        self,
        snap: BufferHealthSnapshot,
        *,
        override_last_error: str | None = None,
    ) -> None:
        buf_running = self.buffer.running()
        state = self._primary_encoder_state(snap, buf_running)
        status_text = self._status_text_for_state(state)
        restart_pending = self._restart_pending or self._app_restart_requested
        healthy = snap.healthy if buf_running else False
        allow_timer = (
            not restart_pending
            and healthy
            and buf_running
            and state
            not in (
                STATE_UNAVAILABLE,
                STATE_BLOCKED,
                STATE_SHUTTING_DOWN,
                STATE_RESTARTING,
                STATE_STARTING,
            )
        )

        long_ok = (
            bool(self.settings.uvc_video_device.strip())
            and buf_running
            and healthy
            and not self._startup_blocked
            and not restart_pending
            and not self._shutting_down
            and not self.long_rec.running()
            and state not in (STATE_BLOCKED, STATE_SHUTTING_DOWN)
        )

        encoder_ready = (
            state in (STATE_READY, STATE_DEGRADED, STATE_RECORDING)
            and not self._startup_blocked
        )

        degraded = state == STATE_DEGRADED or (
            buf_running
            and not healthy
            and state not in (STATE_SHUTTING_DOWN, STATE_STARTING)
        )

        last_err = override_last_error if override_last_error else self._last_error_msg

        try:
            publish_encoder_state(
                self.settings.encoder_state_path,
                {
                    "state": state,
                    "status_text": status_text,
                    "encoder_ready": encoder_ready,
                    "buffer_running": buf_running,
                    "buffer_healthy": healthy,
                    "long_recording_active": self.long_rec.running(),
                    "long_recording_available": long_ok,
                    "allow_record_timer_overlay": allow_timer,
                    "restart_pending": restart_pending,
                    "degraded": degraded,
                    "auto_restart_count": self._auto_restart_count,
                    "last_error": last_err,
                    "buffer_detail": snap.detail,
                    "buffer_in_startup_grace": snap.in_startup_grace,
                    "buffer_stall_restart_eligible": snap.stall_restart_eligible,
                    "consecutive_buffer_restart_failures": self._consecutive_buffer_restart_failures,
                    "replay_export_consecutive_failures": self._replay_export_consecutive_failures,
                    "long_recording_last_fault": self._long_recording_last_fault,
                },
            )
        except OSError as e:
            logger.warning("encoder state publish failed: %s", e)

    def _request_full_app_restart(self, reason: str) -> None:
        if self._app_restart_requested or self._shutting_down:
            return
        self._app_restart_requested = True
        self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
        self._last_error_msg = f"app restart: {reason}"
        logger.error("Full app restart requested: %s", reason)
        self._emit_ui(f"Full app restart: {reason}")
        snap = self._buffer_health_snapshot()
        self._publish_encoder_state(snap, override_last_error=self._last_error_msg)
        self.root.after(0, self._perform_full_app_restart)

    def _perform_full_app_restart(self) -> None:
        if self._full_restart_perform_started:
            return
        self._full_restart_perform_started = True
        self._shutting_down = True
        self.exit_code = self.settings.encoder_app_restart_exit_code
        try:
            snap = self._buffer_health_snapshot()
            self._publish_encoder_state(
                snap, override_last_error=self._last_error_msg
            )
        except OSError:
            pass
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.long_rec.stop()
        self.buffer.stop()
        _flush_logger_handlers()
        logging.shutdown()
        try:
            self.root.destroy()
        except tk.TclError:
            pass

    def _maybe_escalate(self, reason: str) -> None:
        if self._app_restart_requested or self._shutting_down:
            return
        self.root.after(0, lambda r=reason: self._request_full_app_restart(r))

    def _note_ffmpeg_kill(self) -> None:
        if self._shutting_down or self._app_restart_requested:
            return
        self._ffmpeg_kill_stop_count += 1
        if self._ffmpeg_kill_stop_count >= 3:
            self._maybe_escalate("repeated ffmpeg processes required kill to stop")

    def _bump_replay_export_failure(self) -> None:
        self._replay_export_consecutive_failures += 1
        if (
            self._replay_export_consecutive_failures
            >= self.settings.encoder_max_replay_export_failures
        ):
            self._maybe_escalate("replay export failures exceeded threshold")

    def _reset_replay_export_failures(self) -> None:
        self._replay_export_consecutive_failures = 0

    def _scoreboard_snapshot(self) -> ScoreboardEncoderSnapshot:
        return read_scoreboard_replay_for_encoder(
            self.settings.scoreboard_state_path,
            max_age_seconds=self.settings.scoreboard_state_max_age_seconds,
        )

    def _replay_control_state_from_snap(
        self, snap: ScoreboardEncoderSnapshot
    ) -> ReplayControlState:
        if snap.blocks_replay_save:
            return ReplayControlState.REPLAY_ACTIVE
        with self._replay_lock:
            if self._replay_export_in_progress:
                return ReplayControlState.SAVING_REPLAY
        return ReplayControlState.IDLE

    def _derive_replay_control_state(self) -> ReplayControlState:
        return self._replay_control_state_from_snap(self._scoreboard_snapshot())

    def _update_replay_gating_logs(
        self,
        st: ReplayControlState,
        snap: ScoreboardEncoderSnapshot,
    ) -> None:
        key: object = (
            st,
            snap.blocks_replay_save,
            snap.stale,
            snap.replay_display_active_raw,
        )
        if key == self._replay_gating_log_key:
            return
        self._replay_gating_log_key = key

        if st == ReplayControlState.REPLAY_ACTIVE:
            logger.info(
                "Replay gating: scoreboard state file (replay_display_active=true, age=%.1fs, max_age=%.0fs)",
                snap.age_seconds if snap.age_seconds is not None else -1.0,
                self.settings.scoreboard_state_max_age_seconds,
            )
        elif st == ReplayControlState.SAVING_REPLAY:
            logger.info("Replay gating: encoder replay export in progress")
        elif snap.stale and snap.replay_display_active_raw:
            logger.warning(
                "Scoreboard state stale: replay_display_active=true but age %.1fs > %.0fs — not blocking replay saves",
                snap.age_seconds if snap.age_seconds is not None else -1.0,
                self.settings.scoreboard_state_max_age_seconds,
            )

    def _refresh_replay_status_label(self) -> None:
        snap = self._scoreboard_snapshot()
        st = self._replay_control_state_from_snap(snap)
        self._update_replay_gating_logs(st, snap)
        if st == ReplayControlState.SAVING_REPLAY:
            self._replay_export_state.set("Replay: SAVING (encoder export)")
        elif st == ReplayControlState.REPLAY_ACTIVE:
            self._replay_export_state.set("Replay: ACTIVE (scoreboard)")
        elif snap.stale and snap.replay_display_active_raw:
            self._replay_export_state.set(
                "Replay: IDLE — stale scoreboard state (not blocking)"
            )
        else:
            self._replay_export_state.set("Replay: IDLE")

    def request_replay_save(self) -> None:
        """Single entry for Save instant replay (button, hotkey, etc.). No queuing."""
        if not self.buffer.running():
            logger.info("Replay save ignored: buffer not running")
            self._emit_ui("Replay save ignored: buffer not running")
            self._refresh_replay_status_label()
            return
        snap = self._scoreboard_snapshot()
        if snap.blocks_replay_save:
            logger.info(
                "Replay save ignored: replay mode already active (scoreboard state file, age=%.1fs)",
                snap.age_seconds if snap.age_seconds is not None else -1.0,
            )
            self._emit_ui(
                "Replay save ignored: replay mode already active (scoreboard)"
            )
            self._refresh_replay_status_label()
            return
        with self._replay_lock:
            if self._replay_export_in_progress:
                duplicate_export = True
            else:
                self._replay_export_in_progress = True
                duplicate_export = False
        if duplicate_export:
            logger.info(
                "Replay save ignored: export already in progress (encoder replay export)"
            )
            self._emit_ui(
                "Replay save ignored: export already in progress (encoder)"
            )
            self._refresh_replay_status_label()
            return

        logger.info("Replay save accepted")
        self._emit_ui("Replay save accepted")
        self._refresh_replay_status_label()
        self._sync_button_states()
        threading.Thread(target=self._replay_export_worker, daemon=True).start()

    def request_start_long(self) -> None:
        """Single entry for Start long recording (button, hotkey, etc.)."""
        if self.long_record_state != LongRecordState.NOT_RECORDING:
            logger.info("Long recording start ignored (already recording)")
            self._emit_ui("Long recording start ignored (already recording)")
            return
        if self._restart_pending or self._app_restart_requested:
            logger.info("Long recording start ignored (restart pending)")
            self._emit_ui("Long recording start ignored (restart pending)")
            return
        if not self.settings.uvc_video_device.strip():
            logger.info("Long recording start ignored (UVC_VIDEO_DEVICE not set)")
            self._emit_ui("Long recording start ignored (UVC_VIDEO_DEVICE not set)")
            return
        if not self.buffer.running():
            logger.info("Long recording start ignored (buffer not running)")
            self._emit_ui("Long recording start ignored (buffer not running)")
            return
        hs = self._buffer_health_snapshot()
        if not hs.healthy:
            logger.info("Long recording start ignored (buffer unhealthy)")
            self._emit_ui("Long recording start ignored (buffer unhealthy)")
            return
        self._long_record_fail_reported = False
        self._long_recording_last_fault = ""
        logger.info("Long recording start accepted")
        self._emit_ui("Long recording start accepted")
        if not self.long_rec.start():
            self._sync_button_states()
            self._publish_encoder_state(self._buffer_health_snapshot())
            return
        self._sync_button_states()
        self._publish_encoder_state(self._buffer_health_snapshot())

    def request_stop_long(self) -> None:
        """Single entry for Stop long recording (button, hotkey, etc.)."""
        if self.long_record_state != LongRecordState.RECORDING:
            logger.info("Long recording stop ignored (not recording)")
            self._emit_ui("Long recording stop ignored (not recording)")
            return
        logger.info("Long recording stop accepted")
        self._emit_ui("Long recording stop accepted")
        _, killed = self.long_rec.stop()
        if killed:
            self._note_ffmpeg_kill()
        self._sync_button_states()
        self._publish_encoder_state(self._buffer_health_snapshot())

    def _register_global_hotkeys(self) -> None:
        if sys.platform != "win32":
            logger.info("Global hotkeys skipped (not Windows)")
            return

        from global_hotkeys import register_global_hotkeys_win

        def on_done() -> None:
            self._emit_ui(
                "Global hotkey registration finished (if hooks failed, install: pip install keyboard)."
            )

        register_global_hotkeys_win(
            self.root,
            [
                (self.settings.hotkey_save_replay, self._hotkey_save_replay),
                (self.settings.hotkey_start_long, self._hotkey_start_long),
                (self.settings.hotkey_stop_long, self._hotkey_stop_long),
                (self.settings.hotkey_restart_app, self._hotkey_restart_app),
            ],
            on_done=on_done,
        )

    def _hotkey_save_replay(self) -> None:
        if self._shutting_down:
            return
        self.request_replay_save()

    def _hotkey_start_long(self) -> None:
        if self._shutting_down:
            return
        self.request_start_long()

    def _hotkey_stop_long(self) -> None:
        if self._shutting_down:
            return
        self.request_stop_long()

    def _hotkey_restart_app(self) -> None:
        if self._shutting_down:
            return
        self._request_full_app_restart("manual hotkey (restart app)")

    def _on_buffer_exit(self, code: int, intentional: bool) -> None:
        if self._shutting_down:
            return
        if intentional:
            self._buffer_intentional_stop = True
            return
        logger.error(
            "Buffer ffmpeg unexpected exit code=%s (stderr tail below)\n%s",
            code,
            self.buffer.stderr_tail_text()[-2000:],
        )
        self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
        self._last_error_msg = f"buffer exit {code}"

    def _log_readonly_key(self, event: tk.Event) -> str | None:
        """Block edits; allow navigation and Ctrl+C / Ctrl+A / Ctrl+Insert."""
        ctrl = (event.state & 0x4) != 0
        keysym = event.keysym
        if ctrl and keysym.lower() in ("c", "a", "insert"):
            return None
        if keysym in (
            "Left",
            "Right",
            "Up",
            "Down",
            "Home",
            "End",
            "Prior",
            "Next",
            "Shift_L",
            "Shift_R",
            "Control_L",
            "Control_R",
            "Alt_L",
            "Alt_R",
        ):
            return None
        return "break"

    def _log_focus_on_click(self, event: tk.Event) -> None:
        self.log_widget.focus_set()

    def _copy_log_to_clipboard(self) -> None:
        text = self.log_widget.get("1.0", tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(text.rstrip("\n"))
        self.root.update_idletasks()

    def _append_log(self, text: str) -> None:
        self.log_widget.insert(tk.END, text)
        self.log_widget.see(tk.END)

    def _poll_log(self) -> None:
        try:
            while True:
                line = self.log_q.get_nowait()
                self._append_log(line)
        except queue.Empty:
            pass
        self.root.after(200, self._poll_log)

    def _sync_button_states(self) -> None:
        buf_ok = self.buffer.running()
        hs = self._buffer_health_snapshot()
        buf_healthy = hs.healthy if buf_ok else False
        replay_ok = self._derive_replay_control_state() == ReplayControlState.IDLE
        long_st = self.long_record_state
        blocked = "BLOCKED" in self._buffer_state.get()

        self.btn_save.configure(
            state=tk.DISABLED
            if (not buf_ok or not replay_ok or blocked)
            else tk.NORMAL
        )
        self.btn_start_long.configure(
            state=tk.DISABLED
            if (
                long_st != LongRecordState.NOT_RECORDING
                or blocked
                or not buf_ok
                or not buf_healthy
                or self._restart_pending
                or self._app_restart_requested
            )
            else tk.NORMAL
        )
        self.btn_stop_long.configure(
            state=tk.DISABLED
            if long_st != LongRecordState.RECORDING
            else tk.NORMAL
        )

    def _watchdog_tick(self) -> None:
        running = self.buffer.running()
        if not running:
            self._buffer_start_monotonic = None

        snap = self._buffer_health_snapshot()
        if snap.observed_hls_output:
            self._buffer_has_produced_hls = True

        if not self._shutting_down and not self._startup_blocked:

            if snap.newest_segment_mtime:
                self._last_segment_ts = dt.datetime.fromtimestamp(
                    snap.newest_segment_mtime
                ).strftime("%H:%M:%S")

            if running and snap.healthy:
                self._ffmpeg_kill_stop_count = 0

            if running and not snap.healthy and snap.stall_restart_eligible:
                self._stall_ticks += 1
                logger.warning("Buffer health (stall-eligible): %s", snap.detail)
                if self._unhealthy_since is None:
                    self._unhealthy_since = time.monotonic()
                elif (
                    not self._unhealthy_escalate_sent
                    and time.monotonic() - self._unhealthy_since
                    >= self.settings.encoder_unhealthy_window_seconds
                ):
                    self._unhealthy_escalate_sent = True
                    self._maybe_escalate(
                        f"buffer unhealthy for "
                        f"{self.settings.encoder_unhealthy_window_seconds:.0f}s+ "
                        f"({snap.detail})"
                    )
            elif running and snap.healthy:
                self._stall_ticks = 0
                self._unhealthy_since = None
                self._unhealthy_escalate_sent = False
            else:
                self._stall_ticks = 0
                self._unhealthy_since = None
                self._unhealthy_escalate_sent = False

            if (
                running
                and not snap.healthy
                and not snap.stall_restart_eligible
                and not snap.in_startup_grace
                and "startup failed" in snap.detail.lower()
                and not self._buffer_startup_failure_logged
            ):
                self._buffer_startup_failure_logged = True
                tail = self.buffer.stderr_tail_recent(50)
                logger.error(
                    "Buffer HLS startup failed after grace (%s). "
                    "Last ffmpeg stderr lines:\n%s",
                    snap.detail,
                    tail,
                )
                self._emit_ui(
                    f"Buffer startup failed: {snap.detail}. "
                    "See log for ffmpeg stderr (last 50 lines)."
                )
                self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
                self._last_error_msg = snap.detail

            stalled = (
                running
                and not snap.healthy
                and snap.stall_restart_eligible
                and self._stall_ticks >= 2
            )

            if stalled and not self._restart_pending:
                self._buffer_state.set("Buffer: STALLED (restarting)")
                logger.error("Buffer stalled: %s — restarting", snap.detail)
                self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
                self._last_error_msg = f"stalled: {snap.detail}"
                self._auto_restart_count += 1
                _, killed = self.buffer.stop()
                if killed:
                    self._note_ffmpeg_kill()
                self._schedule_buffer_restart()

            elif (
                not running
                and not self._buffer_intentional_stop
                and not self._restart_pending
            ):
                self._buffer_state.set("Buffer: RESTARTING")
                logger.warning("Buffer not running; scheduling auto-restart")
                self._auto_restart_count += 1
                self._schedule_buffer_restart()
            elif running:
                self._buffer_intentional_stop = False
                st = self._buffer_state.get()
                if "STALLED" not in st and "RESTARTING" not in st and "BLOCKED" not in st:
                    if snap.in_startup_grace:
                        self._buffer_state.set("Buffer: STARTING (HLS)")
                    else:
                        self._buffer_state.set("Buffer: RUNNING")
                if self._backoff_idx > 0 and snap.healthy:
                    self._backoff_idx = 0

            if (
                self._buffer_intentional_stop
                and not running
                and not self._restart_pending
            ):
                st = self._buffer_state.get()
                if "STALLED" not in st and "RESTARTING" not in st and "BLOCKED" not in st:
                    self._buffer_state.set("Buffer: STOPPED")

        if self.long_record_state == LongRecordState.RECORDING:
            self.status_long.set(f"Long: RECORDING → {self.long_rec.output_path}")
        else:
            self.status_long.set("Long: NOT_RECORDING")

        self._refresh_replay_status_label()

        self._health_summary.set(
            "Health | "
            f"buf PID: {self.buffer.pid() or '—'} | "
            f"long PID: {self.long_rec.pid() or '—'} | "
            f"last seg: {self._last_segment_ts} | "
            f"last replay: {self._last_replay_ts} | "
            f"auto-restarts: {self._auto_restart_count} | "
            f"buf drops(log lines): {self.buffer.dropped_line_hits} | "
            f"long drops: {self.long_rec.dropped_line_hits}\n"
            f"Last error ({self._last_error_ts}): {self._last_error_msg}"
        )

        try:
            self._publish_encoder_state(snap)
        except OSError:
            pass

        self._sync_button_states()
        self.root.after(1000, self._watchdog_tick)

    def _schedule_buffer_restart(self) -> None:
        if self._startup_blocked or self._shutting_down or self._restart_pending:
            return
        self._restart_pending = True
        delay = self._backoffs[min(self._backoff_idx, len(self._backoffs) - 1)]
        self._backoff_idx = min(self._backoff_idx + 1, len(self._backoffs) - 1)
        logger.info(
            "Scheduling buffer restart in %ss (backoff step %s)",
            delay,
            self._backoff_idx,
        )

        def go() -> None:
            self._restart_pending = False
            if self._shutting_down or self._startup_blocked:
                return
            self._buffer_intentional_stop = False
            if self.buffer.start():
                self._mark_buffer_started()
                self._buffer_state.set("Buffer: RUNNING")
                logger.info("Buffer auto-restart succeeded")
                self._consecutive_buffer_restart_failures = 0
            else:
                self._buffer_state.set("Buffer: RESTART FAILED")
                logger.error("Buffer auto-restart failed")
                self._consecutive_buffer_restart_failures += 1
                if (
                    self._consecutive_buffer_restart_failures
                    >= self.settings.encoder_max_auto_restarts_before_app_restart
                ):
                    self._maybe_escalate("consecutive buffer auto-restart failures")

            self._publish_encoder_state(self._buffer_health_snapshot())

        self.root.after(int(delay * 1000), go)

    def on_restart_buffer(self) -> None:
        logger.info("Operator: restart buffer requested")
        self._restart_pending = False
        self._buffer_intentional_stop = True
        self._buffer_state.set("Buffer: RESTARTING (manual)")
        _, killed = self.buffer.stop()
        if killed:
            self._note_ffmpeg_kill()
        self._buffer_intentional_stop = False
        self._backoff_idx = 0
        if self.buffer.start():
            self._mark_buffer_started()
            self._buffer_state.set("Buffer: RUNNING")
            self._consecutive_buffer_restart_failures = 0
        else:
            self._buffer_state.set("Buffer: FAILED")
            self._consecutive_buffer_restart_failures += 1
            if (
                self._consecutive_buffer_restart_failures
                >= self.settings.encoder_max_auto_restarts_before_app_restart
            ):
                self._maybe_escalate("consecutive manual/auto buffer restarts failed")
        self._publish_encoder_state(self._buffer_health_snapshot())

    def _emit_ui(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def _replay_export_worker(self) -> None:
        try:
            self._run_one_replay_export()
        finally:
            with self._replay_lock:
                self._replay_export_in_progress = False
            self.root.after(0, self._refresh_replay_status_label)
            self.root.after(0, self._sync_button_states)

    def _run_one_replay_export(self) -> None:
        plan = build_replay_plan(self.settings)
        if plan is None:
            self._emit_ui("Save replay: no HLS playlist/segments yet.")
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = "replay: no plan"
            return

        seg_list = ", ".join(p.name for p in plan.segments)
        self._emit_ui(f"Replay segments ({len(plan.segments)}): {seg_list}")
        self._emit_ui(plan.parse_note)

        final = self.settings.instant_replay_path.resolve()
        final.parent.mkdir(parents=True, exist_ok=True)
        temp = final.with_suffix(".copying" + final.suffix)

        args = [str(self.settings.ffmpeg_path)] + concat_recent_segments_args(
            self.settings, plan.concat_list_path, temp
        )
        cwd = self.settings.buffer_dir.resolve()
        try:
            r = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=120,
                cwd=str(cwd),
                **no_console_creationflags(),
            )
        except (OSError, subprocess.TimeoutExpired) as e:
            self._emit_ui(f"Save replay ffmpeg failed: {e}")
            logger.exception("replay export")
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = str(e)
            self.root.after(0, self._bump_replay_export_failure)
            return

        if r.returncode != 0:
            tail = (r.stderr or "")[-1200:]
            self._emit_ui(f"Save replay ffmpeg exit {r.returncode}: {tail}")
            logger.error("replay ffmpeg stderr: %s", tail)
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = f"replay ffmpeg {r.returncode}"
            self.root.after(0, self._bump_replay_export_failure)
            return

        ok_v, vmsg = verify_replay_mkv(temp, self.settings)
        if not ok_v:
            self._emit_ui(vmsg)
            logger.error("%s", vmsg)
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = vmsg
            self.root.after(0, self._bump_replay_export_failure)
            return

        try:
            temp.replace(final)
        except OSError as e:
            self._emit_ui(f"Could not finalize replay file: {e}")
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = str(e)
            self.root.after(0, self._bump_replay_export_failure)
            return

        self._emit_ui(f"Instant replay written: {final}")
        logger.info("Replay export success: %s", final)
        _touch(self.settings.instant_replay_trigger)
        self._last_replay_ts = dt.datetime.now().strftime("%H:%M:%S")
        self.root.after(0, self._reset_replay_export_failures)

    def on_quit(self) -> None:
        self._shutting_down = True
        self.exit_code = 0
        self._buffer_intentional_stop = True
        logger.info("Operator shutdown")
        try:
            self._publish_encoder_state(self._buffer_health_snapshot())
        except OSError:
            pass
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.long_rec.stop()
        self.buffer.stop()
        _flush_logger_handlers()
        self.root.destroy()


def main() -> None:
    try:
        settings = load_encoder_settings()
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    root = tk.Tk()
    app = OperatorApp(root, settings)
    root.mainloop()
    code = app.exit_code
    if code == settings.encoder_app_restart_exit_code and settings.encoder_self_restart_enabled:
        try:
            os.execv(sys.executable, [sys.executable, *sys.argv])
        except OSError as exc:
            print(f"Encoder re-exec failed: {exc}", file=sys.stderr)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
