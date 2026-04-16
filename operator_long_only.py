"""
ReplayTrove long-record-only operator.

No rolling HLS buffer and no instant replay export.
This mode records long clips only (using LONG_RECORD_* settings).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import queue
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from collections import deque
from pathlib import Path
from tkinter import messagebox, scrolledtext
from typing import Any

from app_logging import setup_encoder_logging
from encoder_state import (
    STATE_BLOCKED,
    STATE_READY,
    STATE_RECORDING,
    STATE_SHUTTING_DOWN,
    STATE_UNAVAILABLE,
    publish_encoder_state,
)
from ffmpeg_cmd import long_record_args, long_record_config_messages
from flight_recorder import (
    FlightJsonlEmitter,
    ffprobe_video_report,
    new_encoder_run_id,
    parse_ffmpeg_input_stream,
    redact_argv,
    resolve_ffprobe_path,
)
from settings import EncoderSettings, load_encoder_settings
from startup_validate import validate_startup_detailed
from subprocess_win import no_console_creationflags

logger = logging.getLogger("replaytrove.encoder")

# Encoder state constant -> flight-recorder app_state string
STATE_TO_APP: dict[str, str] = {
    "starting": "starting",
    STATE_BLOCKED: "blocked",
    STATE_READY: "ready",
    STATE_RECORDING: "recording",
    STATE_UNAVAILABLE: "unavailable",
    STATE_SHUTTING_DOWN: "shutting_down",
}


def _pretty_hotkey_combo(combo: str) -> str:
    return "+".join(p.strip().capitalize() for p in combo.split("+"))


def _flush_logger_handlers() -> None:
    log = logging.getLogger("replaytrove.encoder")
    for h in log.handlers:
        try:
            h.flush()
        except OSError:
            pass


def _state_payload_signature(payload: dict[str, Any]) -> str:
    trimmed = {k: v for k, v in payload.items() if k != "updated_at"}
    return json.dumps(trimmed, sort_keys=True, default=str)


def _infer_transition_reason(prev: str | None, new: str) -> str:
    if prev is None:
        if new == "blocked":
            return "startup_failed"
        if new == "ready":
            return "startup_complete"
        return "initial"
    if new == "ready" and prev == "starting":
        return "startup_complete"
    if new == "blocked":
        return "startup_failed"
    if new == "recording" and prev == "ready":
        return "recording_started"
    if new == "ready" and prev == "recording":
        return "recording_stopped"
    if new == "shutting_down":
        return "shutdown"
    return "state_update"


class LongOnlyRecorder:
    _PROGRESS_RE = re.compile(
        r"fps=\s*(?P<fps>[0-9.]+).*?bitrate=\s*(?P<bitrate>[0-9.]+)kbits/s.*?speed=\s*(?P<speed>[0-9.]+)x"
    )

    def __init__(
        self,
        settings: EncoderSettings,
        log_q: queue.Queue[str],
        events: FlightJsonlEmitter,
    ) -> None:
        self.settings = settings
        self.log_q = log_q
        self.events = events
        self.proc: subprocess.Popen[bytes] | None = None
        self.output_path: Path | None = None
        self._stderr_thread: threading.Thread | None = None
        self._reaper_thread: threading.Thread | None = None
        self._intentional_stop = False
        self._start_monotonic: float | None = None
        self._stop_reason = "operator_request"
        self._stop_method: str = "graceful_q"
        self._last_fps: float | None = None
        self._last_speed: float | None = None
        self._last_bitrate_kbps: float | None = None
        self._stderr_tail: deque[str] = deque(maxlen=80)
        self._input_opened = False
        self._output_opened = False
        self._session_pid: int | None = None
        self._last_completed_session_pid: int | None = None
        self.last_record_fault: str = ""
        self._last_exit_data: dict[str, Any] | None = None
        self._stop_trigger_source: str = "operator"

    def _emit(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def progress_snapshot(self) -> dict[str, Any]:
        size = 0
        if self.output_path is not None and self.output_path.exists():
            try:
                size = self.output_path.stat().st_size
            except OSError:
                size = 0
        elapsed = 0.0
        if self._start_monotonic is not None and self.running():
            elapsed = max(0.0, time.monotonic() - self._start_monotonic)
        return {
            "record_elapsed_seconds": round(elapsed, 3),
            "output_file_size_bytes": size,
            "last_ffmpeg_progress_fps": self._last_fps,
            "last_ffmpeg_progress_speed": self._last_speed,
            "last_ffmpeg_progress_bitrate_kbps": self._last_bitrate_kbps,
        }

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, *, trigger_source: str) -> bool:
        self.stop(reason="operator_request", stop_trigger_source="preempt_new_session")
        self._session_pid = None
        self._last_completed_session_pid = None
        self._last_exit_data = None
        self.events.emit(
            "LONG_RECORD_START_REQUESTED",
            message="Long recording start requested.",
            data={"trigger_source": trigger_source},
        )
        ts = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        out = self.settings.long_clips_folder / f"{ts}.mkv"
        try:
            args = [str(self.settings.ffmpeg_path)] + long_record_args(self.settings, out)
        except ValueError as e:
            self._emit(f"Long record config error: {e}")
            self.last_record_fault = str(e)
            self.events.emit(
                "LONG_RECORD_FAILED",
                level="ERROR",
                message="Long record config error.",
                data={
                    "error": {"kind": "config_error", "detail": str(e)},
                    "pid": None,
                    "output_path": str(out),
                    "stop_reason": "error",
                    "stop_method": "graceful_q",
                },
            )
            return False

        req_size = (
            self.settings.uvc_dshow_video_size.strip()
            if self.settings.uvc_capture_backend == "dshow"
            else self.settings.uvc_v4l2_video_size.strip()
        )
        req_fps = (
            float(self.settings.long_record_input_fps)
            if self.settings.uvc_capture_backend == "dshow"
            else float(self.settings.uvc_v4l2_framerate)
        )
        self._intentional_stop = False
        self._stop_reason = "operator_request"
        self._stop_method = "graceful_q"
        self._last_fps = None
        self._last_speed = None
        self._last_bitrate_kbps = None
        self._stderr_tail.clear()
        self._input_opened = False
        self._output_opened = False
        for line in long_record_config_messages(self.settings, out):
            self._emit(line)
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
            self._emit(f"Failed to start long record: {e}")
            self.last_record_fault = str(e)
            self.events.emit(
                "LONG_RECORD_FAILED",
                level="ERROR",
                message="Long recording failed to start.",
                data={
                    "error": {"kind": "spawn_failed", "detail": str(e)},
                    "pid": None,
                    "output_path": str(out),
                    "stop_reason": "error",
                    "stop_method": "graceful_q",
                },
            )
            return False

        self.output_path = out
        self._session_pid = self.proc.pid
        self._start_monotonic = time.monotonic()
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.touch(exist_ok=True)
        except OSError:
            pass
        self._emit(f"Long recording started (PID {self.proc.pid}) → {out}")
        self.events.emit(
            "LONG_RECORD_START_ACCEPTED",
            message="Long recording request accepted.",
            data={"output_path": str(out), "pid": self._session_pid, "trigger_source": trigger_source},
        )
        argv_r = redact_argv(args)
        self.events.emit(
            "FFMPEG_CHILD_LAUNCHED",
            message="ffmpeg child launched.",
            data={
                "pid": self._session_pid,
                "backend": self.settings.uvc_capture_backend,
                "device_name": self.settings.uvc_video_device,
                "requested_video_size": req_size or None,
                "requested_fps": req_fps,
                "output_path": str(out),
                "argv_redacted": argv_r,
            },
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
                if "error" in low or "failed" in low:
                    logger.warning("[long stderr] %s", line)
                else:
                    logger.debug("[long stderr] %s", line)
                if not self._input_opened and "input #0" in low:
                    self._input_opened = True
                    blob = parse_ffmpeg_input_stream("\n".join(self._stderr_tail))
                    self.events.emit(
                        "FFMPEG_CHILD_STDERR_SUMMARY",
                        message="ffmpeg input opened.",
                        data={
                            "phase": "input_opened",
                            "input_format": blob.get("input_format"),
                            "device_name": blob.get("device_name"),
                            "detected_codec": blob.get("detected_codec"),
                            "detected_resolution": blob.get("detected_resolution"),
                            "detected_fps": blob.get("detected_fps"),
                        },
                    )
                if not self._output_opened and "output #0" in low:
                    self._output_opened = True
                    self.events.emit(
                        "FFMPEG_CHILD_STDERR_SUMMARY",
                        message="ffmpeg output initialized.",
                        data={"phase": "output_initialized", "stderr_line": line[:500]},
                    )
                m = self._PROGRESS_RE.search(line)
                if m:
                    try:
                        self._last_fps = float(m.group("fps"))
                        self._last_bitrate_kbps = float(m.group("bitrate"))
                        self._last_speed = float(m.group("speed"))
                    except ValueError:
                        pass

        def reaper() -> None:
            child_pid = proc_ref.pid
            out_p = str(self.output_path) if self.output_path else None
            code = proc_ref.wait()
            self._emit(f"Long record ffmpeg ended (exit={code}).")
            if code != 0:
                self._stop_reason = "error"
            elif not self._intentional_stop:
                self._stop_reason = "auto_stop_max_duration"
                self._stop_method = "graceful_q"

            if code == 0:
                self.last_record_fault = ""
            else:
                self.last_record_fault = f"ffmpeg exited {code}"

            data = {
                "exit_code": code,
                "stop_reason": self._stop_reason,
                "stop_method": self._stop_method,
                "pid": child_pid,
                "output_path": out_p,
            }
            if code != 0:
                tail_lines = list(self._stderr_tail)[-20:]
                self.events.emit(
                    "FFMPEG_CHILD_STDERR_SUMMARY",
                    level="ERROR",
                    message="ffmpeg fatal / error tail.",
                    data={
                        "phase": "fatal_error_tail",
                        "stderr_tail": tail_lines,
                        "error": {"kind": "ffmpeg_exit_error", "exit_code": code},
                    },
                )
                self.events.emit(
                    "LONG_RECORD_FAILED",
                    level="ERROR",
                    message="Long recording process exited with error.",
                    data=data,
                )
            self.events.emit(
                "FFMPEG_CHILD_EXITED",
                level="INFO" if code == 0 else "ERROR",
                message="ffmpeg child exited.",
                data=data,
            )
            if out_p is not None:
                self.events.emit(
                    "OUTPUT_FILE_FINALIZED",
                    message="Output file finalized.",
                    data=data,
                )
            self._last_completed_session_pid = child_pid
            self._last_exit_data = dict(data)

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()
        self.last_record_fault = ""
        return True

    def stop(self, *, reason: str, stop_trigger_source: str = "operator") -> None:
        p = self.proc
        if p is None:
            return
        self._stop_trigger_source = stop_trigger_source
        stop_pid = p.pid
        out_p = str(self.output_path) if self.output_path else None
        self.events.emit(
            "LONG_RECORD_STOP_REQUESTED",
            message="Long recording stop requested.",
            data={
                "trigger_source": stop_trigger_source,
                "pid": stop_pid,
                "output_path": out_p,
            },
        )
        self.events.emit(
            "FFMPEG_CHILD_STOP_REQUESTED",
            message="ffmpeg child stop requested.",
            data={
                "pid": stop_pid,
                "output_path": out_p,
                "intended_method": "graceful_q",
            },
        )
        self._stop_reason = reason
        self._stop_method = "graceful_q"
        self._intentional_stop = True
        if p.poll() is None and p.stdin:
            try:
                p.stdin.write(b"q\n")
                p.stdin.flush()
                p.stdin.close()
            except (BrokenPipeError, OSError):
                pass

        t0 = time.monotonic()
        while p.poll() is None and (time.monotonic() - t0) < self.settings.ffmpeg_child_graceful_wait_seconds:
            time.sleep(0.05)
        if p.poll() is None:
            try:
                p.terminate()
                self._stop_method = "terminate"
                self.events.emit(
                    "FFMPEG_CHILD_STOP_TERMINATED",
                    level="WARNING",
                    message="ffmpeg child terminate sent after graceful timeout.",
                    data={
                        "stop_reason": reason,
                        "stop_method": "terminate",
                        "pid": stop_pid,
                        "output_path": out_p,
                    },
                )
            except OSError:
                pass
        else:
            self.events.emit(
                "FFMPEG_CHILD_STOP_GRACEFUL",
                message="ffmpeg child stopped via graceful q.",
                data={
                    "stop_reason": reason,
                    "stop_method": "graceful_q",
                    "pid": stop_pid,
                    "output_path": out_p,
                },
            )
        t1 = time.monotonic()
        while p.poll() is None and (time.monotonic() - t1) < self.settings.ffmpeg_child_terminate_wait_seconds:
            time.sleep(0.05)
        if p.poll() is None:
            try:
                p.kill()
                self._stop_method = "kill"
                self.events.emit(
                    "FFMPEG_CHILD_STOP_KILLED",
                    level="ERROR",
                    message="ffmpeg child killed after terminate timeout.",
                    data={
                        "stop_reason": reason,
                        "stop_method": "kill",
                        "pid": stop_pid,
                        "output_path": out_p,
                    },
                )
            except OSError:
                pass

        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=15)
        self.proc = None
        self._stderr_thread = None
        self._reaper_thread = None
        self._session_pid = None
        self._emit("Long record stop completed.")


class LongOnlyApp:
    def __init__(self, root: tk.Tk, settings: EncoderSettings, run_id: str) -> None:
        self.root = root
        self.settings = settings
        self.run_id = run_id
        self.log_q: queue.Queue[str] = queue.Queue()
        self._shutting_down = False
        self._startup_blocked = False
        self._restart_pending = False
        self._degraded = False
        self._last_error = "—"
        self._app_state = "starting"
        self._prev_app_state: str | None = None
        self._transition_reason_override: str | None = None
        self._last_health_check_mono = 0.0
        self._health_interval_seconds = 12.0
        self._last_health_unavailable_mono = 0.0
        self._state_log_heartbeat_seconds = 45.0
        self._last_state_log_sig: str | None = None
        self._last_state_log_mono = 0.0
        self._last_record_size_bytes = 0
        self._last_record_size_change_monotonic = time.monotonic()
        self._max_duration_event_emitted = False

        setup_encoder_logging(settings.encoder_log_file, ui_queue=self.log_q)
        self.events = FlightJsonlEmitter(
            run_id=run_id,
            mode="long_only",
            state_provider=self._state_for_event,
        )
        logger.info("Long-only operator starting")
        self.events.emit("APP_START", message="Long-only operator app start.")

        self.rec = LongOnlyRecorder(settings, self.log_q, self.events)

        root.title("ReplayTrove Long Recorder")
        root.geometry("760x560")

        info = tk.Frame(root)
        info.pack(fill=tk.X, padx=8, pady=6)
        tk.Label(info, text=f"UVC: {settings.uvc_video_device or '(set UVC_VIDEO_DEVICE)'}", anchor="w").pack(fill=tk.X)
        tk.Label(
            info,
            text=f"Long clips folder: {settings.long_clips_folder}",
            anchor="w",
        ).pack(fill=tk.X)
        self.status = tk.StringVar(value="Long: NOT_RECORDING")
        tk.Label(info, textvariable=self.status, anchor="w").pack(fill=tk.X)

        btns = tk.Frame(root)
        btns.pack(fill=tk.X, padx=8, pady=4)
        self.btn_start = tk.Button(btns, text="Start long recording", command=lambda: self.start_long("ui_start_button"))
        self.btn_start.pack(side=tk.LEFT, padx=2)
        self.btn_stop = tk.Button(btns, text="Stop long recording", command=lambda: self.stop_long("ui_stop_button"))
        self.btn_stop.pack(side=tk.LEFT, padx=2)
        self.btn_copy = tk.Button(btns, text="Copy log", command=self._copy_log_to_clipboard)
        self.btn_copy.pack(side=tk.LEFT, padx=2)

        self.log_widget = scrolledtext.ScrolledText(
            root,
            height=20,
            state=tk.NORMAL,
            font=("Consolas", 9),
            wrap=tk.WORD,
        )
        self.log_widget.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)
        self.log_widget.bind("<Key>", lambda _e: "break")

        hk_line = (
            f"{_pretty_hotkey_combo(settings.hotkey_start_long)} -> Start long recording | "
            f"{_pretty_hotkey_combo(settings.hotkey_stop_long)} -> Stop long recording | "
            f"{_pretty_hotkey_combo(settings.hotkey_restart_app)} -> Restart app"
        )
        if sys.platform == "win32":
            footer_txt = "Global hotkeys (Windows): " + hk_line
        else:
            footer_txt = "Global hotkeys: Windows only - " + hk_line
        tk.Label(
            root,
            text=footer_txt,
            anchor="w",
            fg="gray30",
            font=("Segoe UI", 9),
            wraplength=740,
            justify=tk.LEFT,
        ).pack(fill=tk.X, padx=8, pady=(0, 6))

        self._log_startup_config_snapshot()
        self._run_startup_probe()
        self.root.protocol("WM_DELETE_WINDOW", self.on_quit)
        self._publish_state()
        self._poll_log()
        self._tick()
        self._register_global_hotkeys()
        self.events.emit("APP_READY", message="Long-only operator app ready.")

    def _emit_ui(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def _state_for_event(self) -> dict[str, Any]:
        rec = getattr(self, "rec", None)
        recording_active = bool(rec.running()) if rec is not None else False
        return {
            "app_state": self._app_state,
            "recording_active": recording_active,
            "recording_available": (not self._startup_blocked and not recording_active),
            "restart_pending": self._restart_pending,
            "degraded": self._degraded,
        }

    def _log_startup_config_snapshot(self) -> None:
        ffprobe = resolve_ffprobe_path(self.settings)
        req_size = (
            self.settings.uvc_dshow_video_size.strip()
            if self.settings.uvc_capture_backend == "dshow"
            else self.settings.uvc_v4l2_video_size.strip()
        )
        req_fps = (
            self.settings.uvc_dshow_framerate
            if self.settings.uvc_capture_backend == "dshow"
            else self.settings.uvc_v4l2_framerate
        )
        be = self.settings.uvc_capture_backend
        if be == "dshow":
            source_kind = "dshow"
        elif be == "v4l2":
            source_kind = "v4l2"
        else:
            source_kind = str(be)
        self.events.emit(
            "STARTUP_CONFIG_SNAPSHOT",
            message="Startup configuration snapshot.",
            data={
                "ffmpeg_path": str(self.settings.ffmpeg_path),
                "ffprobe_path": str(ffprobe) if ffprobe else None,
                "backend": self.settings.uvc_capture_backend,
                "device_name": self.settings.uvc_video_device,
                "source_kind": source_kind,
                "requested_video_size": req_size or None,
                "requested_fps": req_fps,
                "output_width": self.settings.long_output_width,
                "output_height": self.settings.long_output_height,
                "output_fps": self.settings.long_output_fps,
                "output_codec": "libx264",
                "container": "matroska",
                "max_duration_seconds": self.settings.long_record_max_seconds,
                "output_folder": str(self.settings.long_clips_folder),
                "state_file": str(self.settings.encoder_state_path),
            },
        )

    def _run_startup_probe(self) -> None:
        self.events.emit("STARTUP_PROBE_STARTED", message="Startup probe started.")
        self.events.emit(
            "SOURCE_OPEN_REQUESTED",
            message="Startup source probe requested.",
            data={"device_name": self.settings.uvc_video_device},
        )
        errors, warnings, probe = validate_startup_detailed(self.settings)
        if errors:
            self._startup_blocked = True
            self._last_error = "; ".join(errors)
            self._last_health_unavailable_mono = -1e9
            err_payload: dict[str, Any] = {"errors": errors, "warnings": warnings}
            if probe is not None:
                err_payload["probe"] = {
                    "exit_code": probe.exit_code,
                    "error_kind": probe.error_kind,
                    "stderr_tail": (probe.stderr[-800:] if probe.stderr else ""),
                }
            self.events.emit(
                "STARTUP_PROBE_FAILED",
                level="ERROR",
                message="Startup probe failed.",
                data=err_payload,
            )
            if probe is not None and not probe.ok:
                self.events.emit(
                    "SOURCE_OPEN_FAILED",
                    level="ERROR",
                    message="Startup source probe failed.",
                    data={
                        "error": {
                            "kind": probe.error_kind or "probe_failed",
                            "exit_code": probe.exit_code,
                            "stderr_tail": (probe.stderr[-1200:] if probe.stderr else []),
                        }
                    },
                )
            messagebox.showerror("Startup validation failed", "\n".join(errors))
        else:
            ok_data: dict[str, Any] = {"warnings": warnings}
            if probe is not None and probe.ok:
                ok_data["detected_resolution"] = probe.detected_resolution
                ok_data["detected_fps"] = probe.detected_fps
                ok_data["detected_codec"] = probe.detected_codec
                ok_data["probe_duration_seconds"] = round(probe.probe_duration_seconds, 3)
            self.events.emit(
                "STARTUP_PROBE_SUCCEEDED",
                message="Startup probe succeeded.",
                data=ok_data,
            )
            self.events.emit(
                "SOURCE_OPEN_SUCCEEDED",
                message="Startup source probe succeeded.",
                data={
                    "device_name": self.settings.uvc_video_device,
                    "detected_resolution": probe.detected_resolution if probe else None,
                    "detected_fps": probe.detected_fps if probe else None,
                    "detected_codec": probe.detected_codec if probe else None,
                    "probe_duration_seconds": round(probe.probe_duration_seconds, 3) if probe else None,
                },
            )

    def _register_global_hotkeys(self) -> None:
        if sys.platform != "win32":
            logger.info("Global hotkeys skipped (not Windows)")
            return

        try:
            import keyboard  # noqa: F401
        except ImportError:
            msg = (
                "Global hotkeys unavailable: Python package 'keyboard' is not installed.\n"
                "Install it in this interpreter with:\n"
                "  python -m pip install keyboard"
            )
            logger.error(msg)
            self._emit_ui(msg)
            self.events.emit(
                "HOTKEY_REGISTRATION_FAILED",
                level="ERROR",
                message="Keyboard package missing; hotkeys unavailable.",
                data={"error": msg},
            )
            messagebox.showerror("Global hotkeys unavailable", msg)
            return

        from global_hotkeys import register_global_hotkeys_win

        def on_done() -> None:
            self._emit_ui(
                "Global hotkey registration finished (if hooks failed, install: pip install keyboard)."
            )
            self.events.emit("HOTKEYS_ARMED", message="Global hotkeys armed.")

        def on_registered(combo: str) -> None:
            self.events.emit(
                "HOTKEY_REGISTERED",
                message="Global hotkey registered.",
                data={"hotkey": combo},
            )

        def on_registration_failed(combo: str, err: str) -> None:
            self.events.emit(
                "HOTKEY_REGISTRATION_FAILED",
                level="ERROR",
                message="Global hotkey registration failed.",
                data={"hotkey": combo, "error": err},
            )

        register_global_hotkeys_win(
            self.root,
            [
                (self.settings.hotkey_start_long, self._hotkey_start_long),
                (self.settings.hotkey_stop_long, self._hotkey_stop_long),
                (self.settings.hotkey_restart_app, self._hotkey_restart_app),
            ],
            on_done=on_done,
            on_registered=on_registered,
            on_registration_failed=on_registration_failed,
        )
        self._emit_ui(
            "Hotkeys armed: "
            f"{self.settings.hotkey_start_long} (start), "
            f"{self.settings.hotkey_stop_long} (stop), "
            f"{self.settings.hotkey_restart_app} (restart app)."
        )

    def _hotkey_start_long(self) -> None:
        if self._shutting_down:
            return
        self.events.emit(
            "HOTKEY_TRIGGERED",
            message="Start long hotkey triggered.",
            data={"hotkey": self.settings.hotkey_start_long, "action": "start_long"},
        )
        self.start_long("hotkey_start_long")

    def _hotkey_stop_long(self) -> None:
        if self._shutting_down:
            return
        self.events.emit(
            "HOTKEY_TRIGGERED",
            message="Stop long hotkey triggered.",
            data={"hotkey": self.settings.hotkey_stop_long, "action": "stop_long"},
        )
        self.stop_long("hotkey_stop_long")

    def _hotkey_restart_app(self) -> None:
        if self._shutting_down:
            return
        self._emit_ui("Manual restart requested via global hotkey.")
        self.events.emit(
            "HOTKEY_TRIGGERED",
            message="Restart app hotkey triggered.",
            data={"hotkey": self.settings.hotkey_restart_app, "action": "restart_app"},
        )
        self.events.emit(
            "APP_RESTART_REQUESTED",
            message="App restart requested via hotkey.",
            data={"reason": "operator_hotkey"},
        )
        self._shutting_down = True
        self._restart_pending = True
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.events.emit("HOTKEYS_UNREGISTERED", message="Global hotkeys unregistered.")
        self.rec.stop(reason="restart", stop_trigger_source="hotkey_restart_app")
        self._publish_state()
        self.events.emit(
            "APP_RESTART_EXITING",
            message="App exiting for restart.",
            data={"reason": "restart"},
        )
        _flush_logger_handlers()
        self.root.destroy()
        try:
            os.execv(sys.executable, [sys.executable, *sys.argv])
        except OSError as exc:
            logger.exception("App restart failed: %s", exc)

    def _publish_state(self) -> None:
        if self._shutting_down:
            st = STATE_SHUTTING_DOWN
            txt = "Recorder Shutting Down"
        elif self._startup_blocked:
            st = STATE_BLOCKED
            txt = "Recorder Blocked"
        elif self.rec.running():
            st = STATE_RECORDING
            txt = "Recorder Recording"
        elif not self.settings.uvc_video_device.strip():
            st = STATE_UNAVAILABLE
            txt = "Recorder Unavailable"
        else:
            st = STATE_READY
            txt = "Recorder Ready (Long-Only)"

        new_app = STATE_TO_APP.get(st, st)
        if self._prev_app_state != new_app:
            reason = self._transition_reason_override or _infer_transition_reason(
                self._prev_app_state, new_app
            )
            self.events.emit(
                "STATE_TRANSITION",
                message=f"State transition {self._prev_app_state} -> {new_app}",
                data={
                    "prev_state": self._prev_app_state or "none",
                    "new_state": new_app,
                    "reason": reason,
                },
            )
            self._prev_app_state = new_app
            self._transition_reason_override = None
        self._app_state = new_app

        payload = {
            "state": st,
            "status_text": txt,
            "encoder_ready": st in (STATE_READY, STATE_RECORDING),
            "rolling_buffer_applicable": False,
            "long_recording_active": self.rec.running(),
            "long_recording_available": (not self._startup_blocked and not self.rec.running()),
            "restart_pending": self._restart_pending,
            "degraded": self._degraded,
            "auto_restart_count": 0,
            "last_error": self._last_error,
            "long_recording_last_fault": self.rec.last_record_fault,
            "mode": "long_only",
        }
        publish_encoder_state(
            self.settings.encoder_state_path,
            payload,
            on_written=self._on_state_file_written,
        )

    def _on_state_file_written(self, path: Path, payload: dict[str, Any]) -> None:
        now = time.monotonic()
        sig = _state_payload_signature(payload)
        changed = self._last_state_log_sig is None or sig != self._last_state_log_sig
        due_hb = (now - self._last_state_log_mono) >= self._state_log_heartbeat_seconds
        if not changed and not due_hb:
            return
        reason = "change" if changed else "heartbeat"
        self._last_state_log_sig = sig
        self._last_state_log_mono = now
        self.events.emit(
            "STATE_SNAPSHOT",
            message="State snapshot (JSONL).",
            data={"state_log_reason": reason, "summary": {k: payload[k] for k in ("state", "status_text", "long_recording_active", "encoder_ready", "degraded") if k in payload}},
        )
        self.events.emit(
            "STATE_FILE_WRITTEN",
            message="Encoder state file written.",
            data={"path": str(path), "payload": payload, "state_log_reason": reason},
        )

    def start_long(self, trigger_source: str = "ui_start_button") -> None:
        if self._startup_blocked:
            self.rec.last_record_fault = self._last_error
            self.events.emit(
                "LONG_RECORD_FAILED",
                level="ERROR",
                message="Long recording blocked by startup validation.",
                data={
                    "error": {"kind": "startup_blocked", "detail": self._last_error},
                    "pid": None,
                    "output_path": None,
                    "stop_reason": "error",
                    "stop_method": "graceful_q",
                    "trigger_source": trigger_source,
                },
            )
            return
        if self.rec.running():
            return
        self._max_duration_event_emitted = False
        self._last_record_size_bytes = 0
        self._last_record_size_change_monotonic = time.monotonic()
        if not self.rec.start(trigger_source=trigger_source):
            self._last_error = "long record failed to start"
            self.rec.last_record_fault = self._last_error
            self._publish_state()
            return
        self._transition_reason_override = "recording_started"
        self._publish_state()
        self.events.emit(
            "LONG_RECORD_STARTED",
            message="Long recording session active.",
            data={
                "pid": self.rec._session_pid,
                "output_path": str(self.rec.output_path) if self.rec.output_path else None,
                "trigger_source": trigger_source,
            },
        )

    def stop_long(self, trigger_source: str = "ui_stop_button") -> None:
        if not self.rec.running():
            return
        self.rec.stop(reason="operator_request", stop_trigger_source=trigger_source)
        self._finalize_stop_chain(trigger_source)

    def _finalize_stop_chain(self, trigger_source: str) -> None:
        self._verify_last_output()
        if self.rec._last_exit_data:
            self.events.emit(
                "LONG_RECORD_STOPPED",
                message="Long recording session complete.",
                data={**self.rec._last_exit_data, "stop_trigger_source": trigger_source},
            )
            self.rec._last_exit_data = None
        self._transition_reason_override = "recording_stopped"
        self._publish_state()

    def _verify_last_output(self) -> None:
        out = self.rec.output_path
        if out is None:
            return
        sess_pid = self.rec._last_completed_session_pid
        base_stop = {
            "pid": sess_pid,
            "output_path": str(out),
            "stop_reason": self.rec._stop_reason,
            "stop_method": self.rec._stop_method,
        }
        self.events.emit(
            "LONG_RECORD_VERIFICATION_STARTED",
            message="Long record verification started.",
            data=dict(base_stop),
        )
        try:
            size = out.stat().st_size
        except OSError as e:
            self.rec.last_record_fault = str(e)
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                level="ERROR",
                message="Long record verification failed (stat).",
                data={
                    **base_stop,
                    "error": {"kind": "stat_failed", "detail": str(e)},
                },
            )
            self.events.emit(
                "LONG_RECORD_FAILED",
                level="ERROR",
                message="Long record output missing after stop.",
                data={**base_stop, "stop_reason": "error"},
            )
            return
        if size < self.settings.long_record_min_bytes:
            msg = (
                f"output too small ({size} < {self.settings.long_record_min_bytes} bytes)"
            )
            self.rec.last_record_fault = msg
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                level="ERROR",
                message="Long record output too small.",
                data={
                    **base_stop,
                    "error": {
                        "kind": "file_too_small",
                        "expected_min_bytes": self.settings.long_record_min_bytes,
                        "actual_size_bytes": size,
                    },
                },
            )
            self.events.emit(
                "LONG_RECORD_FAILED",
                level="ERROR",
                message="Long record output failed minimum size verification.",
                data={**base_stop, "stop_reason": "error"},
            )
            return

        ffprobe = resolve_ffprobe_path(self.settings)
        if ffprobe is None:
            self.rec.last_record_fault = ""
            self.events.emit(
                "LONG_RECORD_VERIFICATION_PASSED",
                message="Long record verification passed (size only; ffprobe missing).",
                data={
                    **base_stop,
                    "file_size_bytes": size,
                    "duration_seconds": None,
                    "video_codec": None,
                    "width": None,
                    "height": None,
                    "avg_frame_rate": None,
                },
            )
            return

        rep = ffprobe_video_report(out, ffprobe)
        if rep.error or rep.duration_seconds is None:
            self.rec.last_record_fault = rep.error or "ffprobe incomplete"
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                level="ERROR",
                message="Long record ffprobe verification failed.",
                data={
                    **base_stop,
                    "file_size_bytes": size,
                    "error": {"kind": "ffprobe_failed", "detail": rep.error},
                    "actual_duration_seconds": rep.duration_seconds,
                },
            )
            return
        min_dur = max(0.5, float(self.settings.replay_export_min_duration_seconds))
        if rep.duration_seconds < min_dur:
            self.rec.last_record_fault = f"duration {rep.duration_seconds}s < {min_dur}s"
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                level="ERROR",
                message="Long record duration below threshold.",
                data={
                    **base_stop,
                    "file_size_bytes": size,
                    "error": {"kind": "duration_too_short"},
                    "expected_min_duration_seconds": min_dur,
                    "actual_duration_seconds": rep.duration_seconds,
                },
            )
            return

        self.rec.last_record_fault = ""
        self.events.emit(
            "LONG_RECORD_VERIFICATION_PASSED",
            message="Long record verification passed.",
            data={
                **base_stop,
                "output_path": str(out),
                "file_size_bytes": size,
                "duration_seconds": rep.duration_seconds,
                "video_codec": rep.video_codec,
                "width": rep.width,
                "height": rep.height,
                "avg_frame_rate": rep.avg_frame_rate,
            },
        )

    def _copy_log_to_clipboard(self) -> None:
        text = self.log_widget.get("1.0", tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(text.rstrip("\n"))
        self.root.update_idletasks()

    def _poll_log(self) -> None:
        try:
            while True:
                self.log_widget.insert(tk.END, self.log_q.get_nowait())
                self.log_widget.see(tk.END)
        except queue.Empty:
            pass
        self.root.after(200, self._poll_log)

    def _tick(self) -> None:
        if self.rec.running():
            self.status.set(f"Long: RECORDING → {self.rec.output_path}")
        else:
            self.status.set("Long: NOT_RECORDING")
        now = time.monotonic()
        if self.rec.running() and now - self._last_health_check_mono >= self._health_interval_seconds:
            progress = self.rec.progress_snapshot()
            hc_data = {
                **progress,
                "pid": self.rec._session_pid,
                "output_path": str(self.rec.output_path) if self.rec.output_path else None,
                "degraded": self._degraded,
            }
            self.events.emit(
                "HEALTH_CHECK",
                message="Recording health check.",
                data=hc_data,
            )
            size = int(progress.get("output_file_size_bytes") or 0)
            if size > self._last_record_size_bytes:
                self._last_record_size_bytes = size
                self._last_record_size_change_monotonic = now
                if self._degraded:
                    self._degraded = False
                    self.events.emit(
                        "HEALTH_RECOVERED",
                        message="Recording health recovered.",
                        data=hc_data,
                    )
            elif (now - self._last_record_size_change_monotonic) > self.settings.buffer_stall_threshold_seconds:
                if not self._degraded:
                    self._degraded = True
                    self._last_error = "recording progress stalled"
                    self.events.emit(
                        "HEALTH_DEGRADED",
                        level="WARNING",
                        message="Recording appears stalled.",
                        data=hc_data,
                    )
            self._last_health_check_mono = now
            if (
                progress["record_elapsed_seconds"] >= self.settings.long_record_max_seconds
                and not self._max_duration_event_emitted
            ):
                self._max_duration_event_emitted = True
                self.events.emit(
                    "WATCHDOG_ACTION",
                    message="Watchdog: max duration observed (ffmpeg should exit).",
                    data={
                        "action": "observe_auto_stop",
                        "stop_reason": "auto_stop_max_duration",
                        "pid": self.rec._session_pid,
                        "output_path": str(self.rec.output_path) if self.rec.output_path else None,
                    },
                )
        if self._startup_blocked and (
            now - self._last_health_unavailable_mono >= self._state_log_heartbeat_seconds
        ):
            self.events.emit(
                "HEALTH_UNAVAILABLE",
                level="WARNING",
                message="Recording unavailable due to startup block.",
                data={"last_error": self._last_error},
            )
            self._last_health_unavailable_mono = now
        self.btn_start.configure(state=tk.DISABLED if self.rec.running() or self._startup_blocked else tk.NORMAL)
        self.btn_stop.configure(state=tk.NORMAL if self.rec.running() else tk.DISABLED)
        self._publish_state()
        self.root.after(1000, self._tick)

    def on_quit(self) -> None:
        self.events.emit(
            "APP_SHUTDOWN_REQUESTED",
            message="App shutdown requested.",
            data={"reason": "operator_request"},
        )
        self._shutting_down = True
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.events.emit("HOTKEYS_UNREGISTERED", message="Global hotkeys unregistered.")
        was_running = self.rec.running()
        self.rec.stop(reason="operator_request", stop_trigger_source="window_close")
        if was_running:
            self._finalize_stop_chain("window_close")
        else:
            self._publish_state()
        self.events.emit("APP_SHUTDOWN_COMPLETED", message="App shutdown completed.")
        _flush_logger_handlers()
        self.root.destroy()


def main() -> None:
    run_id = new_encoder_run_id()
    try:
        settings = load_encoder_settings()
    except ValueError as e:
        print(e, file=sys.stderr)
        raise SystemExit(1)

    root = tk.Tk()
    LongOnlyApp(root, settings, run_id)
    root.mainloop()


if __name__ == "__main__":
    main()
