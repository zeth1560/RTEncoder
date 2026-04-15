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
import uuid
from collections import deque
from pathlib import Path
from tkinter import messagebox, scrolledtext
from typing import Any, Callable

from app_logging import setup_encoder_logging, utc_now_iso
from encoder_state import (
    STATE_BLOCKED,
    STATE_READY,
    STATE_RECORDING,
    STATE_SHUTTING_DOWN,
    STATE_UNAVAILABLE,
    publish_encoder_state,
)
from ffmpeg_cmd import long_record_args
from settings import EncoderSettings, load_encoder_settings
from startup_validate import validate_startup

logger = logging.getLogger("replaytrove.encoder")
json_logger = logging.getLogger("replaytrove.encoder.jsonl")


class StructuredEventEmitter:
    def __init__(
        self,
        *,
        run_id: str,
        mode: str,
        state_provider: Callable[[], dict[str, Any]],
    ) -> None:
        self.run_id = run_id
        self.mode = mode
        self._state_provider = state_provider

    def emit(
        self,
        event: str,
        *,
        component: str,
        message: str,
        level: str = "INFO",
        data: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "ts": utc_now_iso(),
            "level": level.upper(),
            "logger": "replaytrove.encoder",
            "event": event,
            "run_id": self.run_id,
            "component": component,
            "mode": self.mode,
            "message": message,
            "state": self._state_provider(),
        }
        if data:
            payload.update(data)
        json_logger.info(json.dumps(payload, ensure_ascii=True))


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


class LongOnlyRecorder:
    _PROGRESS_RE = re.compile(
        r"fps=\s*(?P<fps>[0-9.]+).*?bitrate=\s*(?P<bitrate>[0-9.]+)kbits/s.*?speed=\s*(?P<speed>[0-9.]+)x"
    )

    def __init__(
        self,
        settings: EncoderSettings,
        log_q: queue.Queue[str],
        events: StructuredEventEmitter,
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
        self._stop_method = "graceful_q"
        self._last_fps: float | None = None
        self._last_speed: float | None = None
        self._last_bitrate_kbps: float | None = None
        self._stderr_tail: deque[str] = deque(maxlen=50)
        self._input_opened = False
        self._output_opened = False

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

    def start(self) -> bool:
        self.stop()
        self.events.emit(
            "LONG_RECORD_START_REQUESTED",
            component="recorder",
            message="Long recording start requested.",
        )
        ts = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        out = self.settings.long_clips_folder / f"{ts}.mkv"
        try:
            args = [str(self.settings.ffmpeg_path)] + long_record_args(self.settings, out)
        except ValueError as e:
            self._emit(f"Long record config error: {e}")
            self.events.emit(
                "LONG_RECORD_FAILED",
                component="recorder",
                level="ERROR",
                message="Long record config error.",
                data={"error": str(e)},
            )
            return False

        self.events.emit(
            "SOURCE_OPEN_REQUESTED",
            component="ffmpeg",
            message="Requesting ffmpeg to open UVC source.",
            data={"output_path": str(out), "argv": args},
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
        try:
            self.proc = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=False,
            )
        except OSError as e:
            self._emit(f"Failed to start long record: {e}")
            self.events.emit(
                "SOURCE_OPEN_FAILED",
                component="ffmpeg",
                level="ERROR",
                message="Failed to launch ffmpeg process.",
                data={"error": str(e)},
            )
            self.events.emit(
                "LONG_RECORD_FAILED",
                component="recorder",
                level="ERROR",
                message="Long recording failed to start.",
                data={"error": str(e)},
            )
            return False

        self.output_path = out
        self._start_monotonic = time.monotonic()
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.touch(exist_ok=True)
            self.events.emit(
                "OUTPUT_FILE_CREATED",
                component="recorder",
                message="Output file path created.",
                data={"output_path": str(out.resolve())},
            )
        except OSError:
            pass
        self._emit(f"Long recording started (PID {self.proc.pid}) → {out}")
        self.events.emit(
            "LONG_RECORD_START_ACCEPTED",
            component="recorder",
            message="Long recording request accepted.",
            data={"output_path": str(out), "pid": self.proc.pid},
        )
        self.events.emit(
            "FFMPEG_CHILD_LAUNCHED",
            component="ffmpeg",
            message="ffmpeg child launched.",
            data={"pid": self.proc.pid, "output_path": str(out)},
        )
        self.events.emit(
            "LONG_RECORD_STARTED",
            component="recorder",
            message="Long recording started.",
            data={"output_path": str(out), "pid": self.proc.pid},
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
                    self.events.emit(
                        "SOURCE_OPEN_SUCCEEDED",
                        component="ffmpeg",
                        message="ffmpeg opened input source.",
                        data={"stderr_line": line},
                    )
                    self.events.emit(
                        "FFMPEG_CHILD_STDERR_SUMMARY",
                        component="ffmpeg",
                        message="ffmpeg input initialized.",
                        data={"summary_type": "input_opened", "stderr_line": line},
                    )
                if not self._output_opened and "output #0" in low:
                    self._output_opened = True
                    self.events.emit(
                        "FFMPEG_CHILD_STDERR_SUMMARY",
                        component="ffmpeg",
                        message="ffmpeg output initialized.",
                        data={"summary_type": "output_initialized", "stderr_line": line},
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
            code = proc_ref.wait()
            self._emit(f"Long record ffmpeg ended (exit={code}).")
            data = {
                "exit_code": code,
                "stop_reason": self._stop_reason,
                "stop_method": self._stop_method,
            }
            if code != 0:
                data["fatal_stderr_tail"] = list(self._stderr_tail)[-20:]
                self.events.emit(
                    "FFMPEG_CHILD_STDERR_SUMMARY",
                    component="ffmpeg",
                    level="ERROR",
                    message="ffmpeg fatal stderr tail captured.",
                    data={"summary_type": "fatal_stderr_tail", **data},
                )
                self.events.emit(
                    "LONG_RECORD_FAILED",
                    component="recorder",
                    level="ERROR",
                    message="Long recording process exited with error.",
                    data=data,
                )
            self.events.emit(
                "FFMPEG_CHILD_EXITED",
                component="ffmpeg",
                level="INFO" if code == 0 else "ERROR",
                message="ffmpeg child exited.",
                data=data,
            )

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()
        return True

    def stop(self, *, reason: str = "operator_request") -> None:
        p = self.proc
        if p is None:
            return
        self.events.emit(
            "LONG_RECORD_STOP_REQUESTED",
            component="recorder",
            message="Long recording stop requested.",
            data={"stop_reason": reason},
        )
        self.events.emit(
            "FFMPEG_CHILD_STOP_REQUESTED",
            component="ffmpeg",
            message="ffmpeg child stop requested.",
            data={"stop_reason": reason},
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
                    component="ffmpeg",
                    level="WARNING",
                    message="ffmpeg child terminate sent after graceful timeout.",
                    data={"stop_reason": reason},
                )
            except OSError:
                pass
        else:
            self.events.emit(
                "FFMPEG_CHILD_STOP_GRACEFUL",
                component="ffmpeg",
                message="ffmpeg child stopped via graceful q.",
                data={"stop_reason": reason},
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
                    component="ffmpeg",
                    level="ERROR",
                    message="ffmpeg child killed after terminate timeout.",
                    data={"stop_reason": reason},
                )
            except OSError:
                pass

        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=15)
        self.proc = None
        self._stderr_thread = None
        self._reaper_thread = None
        if self.output_path is not None:
            self.events.emit(
                "LONG_RECORD_STOPPED",
                component="recorder",
                message="Long record stop completed.",
                data={
                    "output_path": str(self.output_path),
                    "stop_reason": self._stop_reason,
                    "stop_method": self._stop_method,
                },
            )
            self.events.emit(
                "OUTPUT_FILE_FINALIZED",
                component="recorder",
                message="Output file finalized.",
                data={
                    "output_path": str(self.output_path),
                    "stop_reason": self._stop_reason,
                    "stop_method": self._stop_method,
                },
            )
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
        self._prev_state: str | None = None
        self._last_health_emit_monotonic = 0.0
        self._health_interval_seconds = 12.0
        self._last_watchdog_emit_monotonic = 0.0
        self._watchdog_interval_seconds = 5.0
        self._last_record_size_bytes = 0
        self._last_record_size_change_monotonic = time.monotonic()
        self._max_duration_event_emitted = False

        setup_encoder_logging(settings.encoder_log_file, ui_queue=self.log_q)
        self.events = StructuredEventEmitter(
            run_id=run_id,
            mode="long_only",
            state_provider=self._state_for_event,
        )
        logger.info("Long-only operator starting")
        self.events.emit(
            "APP_START",
            component="app",
            message="Long-only operator app start.",
        )

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
        self.btn_start = tk.Button(btns, text="Start long recording", command=self.start_long)
        self.btn_start.pack(side=tk.LEFT, padx=2)
        self.btn_stop = tk.Button(btns, text="Stop long recording", command=self.stop_long)
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
        self.events.emit(
            "APP_READY",
            component="app",
            message="Long-only operator app ready.",
        )

    def _emit_ui(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def _state_for_event(self) -> dict[str, Any]:
        return {
            "app_state": self._app_state,
            "recording_active": self.rec.running(),
            "recording_available": (not self._startup_blocked and not self.rec.running()),
            "restart_pending": self._restart_pending,
            "degraded": self._degraded,
        }

    def _log_startup_config_snapshot(self) -> None:
        requested_size = (
            self.settings.uvc_dshow_video_size.strip()
            if self.settings.uvc_capture_backend == "dshow"
            else self.settings.uvc_v4l2_video_size.strip()
        )
        requested_fps = (
            self.settings.uvc_dshow_framerate
            if self.settings.uvc_capture_backend == "dshow"
            else self.settings.uvc_v4l2_framerate
        )
        self.events.emit(
            "STARTUP_CONFIG_SNAPSHOT",
            component="startup",
            message="Startup configuration snapshot.",
            data={
                "ffmpeg_path": str(self.settings.ffmpeg_path),
                "backend": self.settings.uvc_capture_backend,
                "device_name": self.settings.uvc_video_device,
                "requested_video_size": requested_size,
                "requested_video_fps": requested_fps,
                "output_video_size": (
                    f"{self.settings.long_output_width}x{self.settings.long_output_height}"
                ),
                "output_video_fps": self.settings.long_output_fps,
                "output_path": str(self.settings.long_clips_folder),
                "max_duration_seconds": self.settings.long_record_max_seconds,
            },
        )

    def _run_startup_probe(self) -> None:
        self.events.emit(
            "STARTUP_PROBE_STARTED",
            component="startup",
            message="Startup probe started.",
        )
        self.events.emit(
            "SOURCE_OPEN_REQUESTED",
            component="startup",
            message="Startup source probe requested.",
            data={"device_name": self.settings.uvc_video_device},
        )
        errors, warnings = validate_startup(self.settings)
        if errors:
            self._startup_blocked = True
            self._last_error = "; ".join(errors)
            self.events.emit(
                "STARTUP_PROBE_FAILED",
                component="startup",
                level="ERROR",
                message="Startup probe failed.",
                data={"errors": errors, "warnings": warnings},
            )
            self.events.emit(
                "SOURCE_OPEN_FAILED",
                component="startup",
                level="ERROR",
                message="Startup source probe failed.",
                data={"errors": errors},
            )
            messagebox.showerror("Startup validation failed", "\n".join(errors))
        else:
            self.events.emit(
                "STARTUP_PROBE_SUCCEEDED",
                component="startup",
                message="Startup probe succeeded.",
                data={"warnings": warnings},
            )
            self.events.emit(
                "SOURCE_OPEN_SUCCEEDED",
                component="startup",
                message="Startup source probe succeeded.",
                data={"device_name": self.settings.uvc_video_device},
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
                component="hotkeys",
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
            self.events.emit(
                "HOTKEYS_ARMED",
                component="hotkeys",
                message="Global hotkeys armed.",
            )

        def on_registered(combo: str) -> None:
            self.events.emit(
                "HOTKEY_REGISTERED",
                component="hotkeys",
                message="Global hotkey registered.",
                data={"hotkey": combo},
            )

        def on_registration_failed(combo: str, err: str) -> None:
            self.events.emit(
                "HOTKEY_REGISTRATION_FAILED",
                component="hotkeys",
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
            component="hotkeys",
            message="Start long hotkey triggered.",
            data={"hotkey": self.settings.hotkey_start_long, "action": "start_long"},
        )
        self.start_long()

    def _hotkey_stop_long(self) -> None:
        if self._shutting_down:
            return
        self.events.emit(
            "HOTKEY_TRIGGERED",
            component="hotkeys",
            message="Stop long hotkey triggered.",
            data={"hotkey": self.settings.hotkey_stop_long, "action": "stop_long"},
        )
        self.stop_long()

    def _hotkey_restart_app(self) -> None:
        if self._shutting_down:
            return
        self._emit_ui("Manual restart requested via global hotkey.")
        self.events.emit(
            "HOTKEY_TRIGGERED",
            component="hotkeys",
            message="Restart app hotkey triggered.",
            data={"hotkey": self.settings.hotkey_restart_app, "action": "restart_app"},
        )
        self.events.emit(
            "APP_RESTART_REQUESTED",
            component="app",
            message="App restart requested via hotkey.",
            data={"stop_reason": "restart"},
        )
        self._shutting_down = True
        self._restart_pending = True
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.events.emit(
            "HOTKEYS_UNREGISTERED",
            component="hotkeys",
            message="Global hotkeys unregistered.",
        )
        self.rec.stop(reason="restart")
        self._publish_state()
        self.events.emit(
            "APP_RESTART_EXITING",
            component="app",
            message="App exiting for restart.",
            data={"stop_reason": "restart"},
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

        self._app_state = st
        if self._prev_state != st:
            self.events.emit(
                "STATE_TRANSITION",
                component="state",
                message="State transition.",
                data={"from_state": self._prev_state, "to_state": st},
            )
            self._prev_state = st

        payload = {
            "state": st,
            "status_text": txt,
            "encoder_ready": st in (STATE_READY, STATE_RECORDING),
            "buffer_running": False,
            "buffer_healthy": False,
            "long_recording_active": self.rec.running(),
            "long_recording_available": (not self._startup_blocked and not self.rec.running()),
            "allow_record_timer_overlay": (st == STATE_RECORDING),
            "restart_pending": self._restart_pending,
            "degraded": self._degraded,
            "auto_restart_count": 0,
            "last_error": self._last_error,
            "mode": "long_only",
        }
        publish_encoder_state(
            self.settings.encoder_state_path,
            payload,
            on_written=self._on_state_file_written,
        )
        self.events.emit(
            "STATE_SNAPSHOT",
            component="state",
            message="State snapshot published.",
            data={"state_payload": payload},
        )

    def _on_state_file_written(self, path: Path, payload: dict[str, Any]) -> None:
        self.events.emit(
            "STATE_FILE_WRITTEN",
            component="state",
            message="Encoder state file written.",
            data={"state_file_path": str(path), "payload": payload},
        )

    def start_long(self) -> None:
        if self._startup_blocked:
            self.events.emit(
                "LONG_RECORD_FAILED",
                component="recorder",
                level="ERROR",
                message="Long recording blocked by startup validation.",
                data={"error": self._last_error},
            )
            return
        if self.rec.running():
            return
        self._max_duration_event_emitted = False
        self._last_record_size_bytes = 0
        self._last_record_size_change_monotonic = time.monotonic()
        if not self.rec.start():
            self._last_error = "long record failed to start"
        self._publish_state()

    def stop_long(self) -> None:
        self.rec.stop(reason="operator_request")
        self._verify_last_output()
        self._publish_state()

    def _verify_last_output(self) -> None:
        out = self.rec.output_path
        if out is None:
            return
        self.events.emit(
            "LONG_RECORD_VERIFICATION_STARTED",
            component="recorder",
            message="Long record verification started.",
            data={"output_path": str(out)},
        )
        try:
            size = out.stat().st_size
        except OSError as e:
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                component="recorder",
                level="ERROR",
                message="Long record verification failed (stat).",
                data={"output_path": str(out), "error": str(e)},
            )
            self.events.emit(
                "LONG_RECORD_FAILED",
                component="recorder",
                level="ERROR",
                message="Long record output missing after stop.",
                data={"output_path": str(out), "error": str(e)},
            )
            return
        if size < self.settings.long_record_min_bytes:
            self.events.emit(
                "LONG_RECORD_VERIFICATION_FAILED",
                component="recorder",
                level="ERROR",
                message="Long record output too small.",
                data={
                    "output_path": str(out),
                    "size_bytes": size,
                    "min_bytes": self.settings.long_record_min_bytes,
                },
            )
            self.events.emit(
                "LONG_RECORD_FAILED",
                component="recorder",
                level="ERROR",
                message="Long record output failed minimum size verification.",
                data={"output_path": str(out), "size_bytes": size},
            )
            return
        self.events.emit(
            "LONG_RECORD_VERIFICATION_PASSED",
            component="recorder",
            message="Long record verification passed.",
            data={"output_path": str(out), "size_bytes": size},
        )
        self.events.emit(
            "OUTPUT_FILE_VERIFIED",
            component="recorder",
            message="Output file verified.",
            data={"output_path": str(out), "size_bytes": size},
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
        if now - self._last_watchdog_emit_monotonic >= self._watchdog_interval_seconds:
            self.events.emit(
                "WATCHDOG_TICK",
                component="watchdog",
                message="Watchdog tick.",
                data={"recording_active": self.rec.running()},
            )
            self._last_watchdog_emit_monotonic = now
        if self.rec.running() and now - self._last_health_emit_monotonic >= self._health_interval_seconds:
            progress = self.rec.progress_snapshot()
            self.events.emit(
                "HEALTH_CHECK",
                component="health",
                message="Recording health check.",
                data=progress,
            )
            size = int(progress.get("output_file_size_bytes") or 0)
            if size > self._last_record_size_bytes:
                self._last_record_size_bytes = size
                self._last_record_size_change_monotonic = now
                if self._degraded:
                    self._degraded = False
                    self.events.emit(
                        "HEALTH_RECOVERED",
                        component="health",
                        message="Recording health recovered.",
                        data=progress,
                    )
            elif (now - self._last_record_size_change_monotonic) > self.settings.buffer_stall_threshold_seconds:
                if not self._degraded:
                    self._degraded = True
                    self._last_error = "recording progress stalled"
                    self.events.emit(
                        "HEALTH_DEGRADED",
                        component="health",
                        level="WARNING",
                        message="Recording appears stalled.",
                        data=progress,
                    )
            self._last_health_emit_monotonic = now
            if (
                progress["record_elapsed_seconds"] >= self.settings.long_record_max_seconds
                and not self._max_duration_event_emitted
            ):
                self._max_duration_event_emitted = True
                self.events.emit(
                    "LONG_RECORD_AUTO_STOP_TRIGGERED",
                    component="watchdog",
                    message="Long recording reached max duration.",
                    data={"stop_reason": "auto_stop_max_duration", **progress},
                )
                self.events.emit(
                    "WATCHDOG_ACTION",
                    component="watchdog",
                    message="Watchdog detected auto-stop max duration.",
                    data={"action": "observe_auto_stop", "stop_reason": "auto_stop_max_duration"},
                )
        if self._startup_blocked and now - self._last_health_emit_monotonic >= self._health_interval_seconds:
            self.events.emit(
                "HEALTH_UNAVAILABLE",
                component="health",
                level="WARNING",
                message="Recording unavailable due to startup block.",
                data={"last_error": self._last_error},
            )
            self._last_health_emit_monotonic = now
        self.btn_start.configure(state=tk.DISABLED if self.rec.running() or self._startup_blocked else tk.NORMAL)
        self.btn_stop.configure(state=tk.NORMAL if self.rec.running() else tk.DISABLED)
        self._publish_state()
        self.root.after(1000, self._tick)

    def on_quit(self) -> None:
        self.events.emit(
            "APP_SHUTDOWN_REQUESTED",
            component="app",
            message="App shutdown requested.",
            data={"stop_reason": "operator_request"},
        )
        self._shutting_down = True
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.events.emit(
            "HOTKEYS_UNREGISTERED",
            component="hotkeys",
            message="Global hotkeys unregistered.",
        )
        self.rec.stop(reason="operator_request")
        self._verify_last_output()
        self._publish_state()
        self.events.emit(
            "APP_SHUTDOWN_COMPLETED",
            component="app",
            message="App shutdown completed.",
        )
        _flush_logger_handlers()
        self.root.destroy()


def main() -> None:
    run_id = uuid.uuid4().hex
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
