"""
ReplayTrove long-record-only operator.

No rolling HLS buffer and no instant replay export.
This mode records long clips only (using LONG_RECORD_* settings).
"""

from __future__ import annotations

import datetime as dt
import logging
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, scrolledtext

from app_logging import setup_encoder_logging
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

logger = logging.getLogger("replaytrove.encoder")


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
    def __init__(self, settings: EncoderSettings, log_q: queue.Queue[str]) -> None:
        self.settings = settings
        self.log_q = log_q
        self.proc: subprocess.Popen[bytes] | None = None
        self.output_path: Path | None = None
        self._stderr_thread: threading.Thread | None = None
        self._reaper_thread: threading.Thread | None = None
        self._intentional_stop = False

    def _emit(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self) -> bool:
        self.stop()
        ts = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        out = self.settings.long_clips_folder / f"{ts}.mkv"
        try:
            args = [str(self.settings.ffmpeg_path)] + long_record_args(self.settings, out)
        except ValueError as e:
            self._emit(f"Long record config error: {e}")
            return False

        self._intentional_stop = False
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
            return False

        self.output_path = out
        self._emit(f"Long recording started (PID {self.proc.pid}) → {out}")
        proc_ref = self.proc

        def drain_stderr() -> None:
            assert proc_ref.stderr
            for raw in proc_ref.stderr:
                line = raw.decode(errors="replace").rstrip()
                if not line:
                    continue
                low = line.lower()
                if "error" in low or "failed" in low:
                    logger.warning("[long stderr] %s", line)
                else:
                    logger.debug("[long stderr] %s", line)

        def reaper() -> None:
            code = proc_ref.wait()
            self._emit(f"Long record ffmpeg ended (exit={code}).")

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()
        return True

    def stop(self) -> None:
        p = self.proc
        if p is None:
            return
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
            except OSError:
                pass
        t1 = time.monotonic()
        while p.poll() is None and (time.monotonic() - t1) < self.settings.ffmpeg_child_terminate_wait_seconds:
            time.sleep(0.05)
        if p.poll() is None:
            try:
                p.kill()
            except OSError:
                pass

        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=15)
        self.proc = None
        self._stderr_thread = None
        self._reaper_thread = None
        self._emit("Long record stop completed.")


class LongOnlyApp:
    def __init__(self, root: tk.Tk, settings: EncoderSettings) -> None:
        self.root = root
        self.settings = settings
        self.log_q: queue.Queue[str] = queue.Queue()
        self._shutting_down = False
        self._startup_blocked = False
        self._last_error = "—"

        setup_encoder_logging(settings.encoder_log_file, ui_queue=self.log_q)
        logger.info("Long-only operator starting")

        self.rec = LongOnlyRecorder(settings, self.log_q)

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

        self.root.protocol("WM_DELETE_WINDOW", self.on_quit)
        self._publish_state()
        self._poll_log()
        self._tick()

        if not settings.uvc_video_device.strip():
            self._startup_blocked = True
            self._last_error = "UVC_VIDEO_DEVICE is empty"
            messagebox.showerror(
                "Startup validation failed",
                "UVC_VIDEO_DEVICE is empty.\nSet it in environment or .env.",
            )

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

        publish_encoder_state(
            self.settings.encoder_state_path,
            {
                "state": st,
                "status_text": txt,
                "encoder_ready": st in (STATE_READY, STATE_RECORDING),
                "buffer_running": False,
                "buffer_healthy": False,
                "long_recording_active": self.rec.running(),
                "long_recording_available": (not self._startup_blocked and not self.rec.running()),
                "allow_record_timer_overlay": (st == STATE_RECORDING),
                "restart_pending": False,
                "degraded": False,
                "auto_restart_count": 0,
                "last_error": self._last_error,
                "mode": "long_only",
            },
        )

    def start_long(self) -> None:
        if self._startup_blocked:
            return
        if self.rec.running():
            return
        if not self.rec.start():
            self._last_error = "long record failed to start"
        self._publish_state()

    def stop_long(self) -> None:
        self.rec.stop()
        self._publish_state()

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
        self.btn_start.configure(state=tk.DISABLED if self.rec.running() or self._startup_blocked else tk.NORMAL)
        self.btn_stop.configure(state=tk.NORMAL if self.rec.running() else tk.DISABLED)
        self._publish_state()
        self.root.after(1000, self._tick)

    def on_quit(self) -> None:
        self._shutting_down = True
        self.rec.stop()
        self._publish_state()
        _flush_logger_handlers()
        self.root.destroy()


def main() -> None:
    try:
        settings = load_encoder_settings()
    except ValueError as e:
        print(e, file=sys.stderr)
        raise SystemExit(1)

    root = tk.Tk()
    LongOnlyApp(root, settings)
    root.mainloop()


if __name__ == "__main__":
    main()
