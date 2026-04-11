"""
ReplayTrove NDI encoder operator: rolling HLS buffer, instant replay export, long recordings.

Replay / long-record actions use explicit state (see operator_state); no queued replay saves.
"""

from __future__ import annotations

import datetime as dt
import logging
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
from buffer_health import snapshot_buffer_health
from ffmpeg_cmd import buffer_hls_args, concat_recent_segments_args, long_record_args
from operator_state import (
    LongRecordState,
    ReplayControlState,
    ScoreboardEncoderSnapshot,
    read_scoreboard_replay_for_encoder,
)
from replay_export import build_replay_plan
from settings import EncoderSettings, load_encoder_settings
from startup_validate import validate_startup

logger = logging.getLogger("replaytrove.encoder")


def _pretty_hotkey_combo(combo: str) -> str:
    return "+".join(p.strip().capitalize() for p in combo.split("+"))


def _touch(path: Path | None) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


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
        self._stderr_tail: deque[str] = deque(maxlen=120)
        self.dropped_line_hits = 0

    def _emit_log(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def start(self) -> bool:
        self.stop()
        try:
            args = [str(self.settings.ffmpeg_path)] + buffer_hls_args(self.settings)
        except ValueError as e:
            self._emit_log(f"Buffer config error: {e}")
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
            self._emit_log(f"Failed to start buffer ffmpeg: {e}")
            return False

        pid = self.proc.pid
        self._emit_log(f"Buffer ffmpeg started (PID {pid}, rolling HLS).")

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
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=15)
        self.proc = None
        self._reaper_thread = None
        self._stderr_thread = None
        self._emit_log("Buffer ffmpeg stop completed.")

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def pid(self) -> int | None:
        p = self.proc
        return p.pid if p and p.poll() is None else None

    def stderr_tail_text(self) -> str:
        return "\n".join(self._stderr_tail)


class LongRecordProcess:
    def __init__(self, settings: EncoderSettings, log_q: queue.Queue[str]) -> None:
        self.settings = settings
        self.log_q = log_q
        self.proc: subprocess.Popen[bytes] | None = None
        self.output_path: Path | None = None
        self._stderr_thread: threading.Thread | None = None
        self._reaper_thread: threading.Thread | None = None
        self._intentional_stop = False
        self._stderr_tail: deque[str] = deque(maxlen=120)
        self.dropped_line_hits = 0

    def _emit_log(self, msg: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        self.log_q.put(f"[{ts}] {msg}\n")
        logger.info(msg)

    def start(self) -> bool:
        self.stop()
        ts = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        out = self.settings.long_clips_folder / f"{ts}.mp4"
        try:
            args = [str(self.settings.ffmpeg_path)] + long_record_args(self.settings, out)
        except ValueError as e:
            self._emit_log(f"Long record config error: {e}")
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
            self._emit_log(f"Failed to start long record: {e}")
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

        self._stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        self._stderr_thread.start()
        self._reaper_thread = threading.Thread(target=reaper, daemon=True)
        self._reaper_thread.start()
        return True

    def stop(self, *, on_verified: Callable[[Path | None, str], None] | None = None) -> None:
        p = self.proc
        out = self.output_path
        if p is None:
            if on_verified:
                on_verified(None, "no active recording")
            return
        self._intentional_stop = True
        if p.poll() is None and p.stdin:
            try:
                p.stdin.write(b"q\n")
                p.stdin.flush()
                p.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=180)
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
        self._shutting_down = False
        self._replay_lock = threading.Lock()
        self._replay_export_in_progress = False
        self._replay_export_state = tk.StringVar(value="Replay: IDLE")
        self._buffer_state = tk.StringVar(value="Buffer: …")
        self._health_summary = tk.StringVar(value="")
        self._last_replay_ts: str = "—"
        self._last_error_ts: str = "—"
        self._last_error_msg: str = "—"
        self._auto_restart_count = 0
        self._backoff_idx = 0
        self._backoffs = (2, 5, 10)
        self._buffer_intentional_stop = False
        self._last_segment_ts: str = "—"
        self._stall_ticks = 0
        self._startup_blocked = False
        self._restart_pending = False
        self._replay_gating_log_key: object | None = None

        setup_encoder_logging(settings.encoder_log_file, ui_queue=self.log_q)
        logger.info("Encoder operator starting")

        self.buffer = BufferProcess(settings, self.log_q, on_exit=self._on_buffer_exit)
        self.long_rec = LongRecordProcess(settings, self.log_q)

        root.title("ReplayTrove NDI Encoder")
        root.geometry("820x640")

        self.status_long = tk.StringVar(value="Long: idle")

        info = tk.Frame(root)
        info.pack(fill=tk.X, padx=8, pady=6)
        tk.Label(
            info,
            text=f"NDI: {settings.ndi_source_name or '(set NDI_SOURCE_NAME)'}",
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

        self.log_widget = scrolledtext.ScrolledText(root, height=20, state=tk.DISABLED)
        self.log_widget.pack(fill=tk.BOTH, expand=True, padx=8, pady=6)

        hk_line = (
            f"{_pretty_hotkey_combo(settings.hotkey_save_replay)} → Save instant replay | "
            f"{_pretty_hotkey_combo(settings.hotkey_start_long)} → Start long recording | "
            f"{_pretty_hotkey_combo(settings.hotkey_stop_long)} → Stop long recording"
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

        if errs:
            self._startup_blocked = True
            messagebox.showerror(
                "Startup validation failed",
                "Buffer will not start until these are fixed:\n\n" + "\n".join(errs),
            )
            self._buffer_state.set("Buffer: BLOCKED (see log)")
            self.btn_save.configure(state=tk.DISABLED)
        else:
            self._buffer_state.set("Buffer: STARTING")
            ok = self.buffer.start()
            if ok:
                self._buffer_state.set("Buffer: RUNNING")
                self._backoff_idx = 0
            else:
                self._buffer_state.set("Buffer: FAILED TO START")

        self._sync_button_states()
        self._refresh_replay_status_label()
        self._register_global_hotkeys()

    @property
    def long_record_state(self) -> LongRecordState:
        return (
            LongRecordState.RECORDING
            if self.long_rec.running()
            else LongRecordState.NOT_RECORDING
        )

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
        if not self.settings.ndi_source_name.strip():
            logger.info("Long recording start ignored (NDI_SOURCE_NAME not set)")
            self._emit_ui("Long recording start ignored (NDI_SOURCE_NAME not set)")
            return
        logger.info("Long recording start accepted")
        self._emit_ui("Long recording start accepted")
        self.long_rec.start()
        self._sync_button_states()

    def request_stop_long(self) -> None:
        """Single entry for Stop long recording (button, hotkey, etc.)."""
        if self.long_record_state != LongRecordState.RECORDING:
            logger.info("Long recording stop ignored (not recording)")
            self._emit_ui("Long recording stop ignored (not recording)")
            return
        logger.info("Long recording stop accepted")
        self._emit_ui("Long recording stop accepted")
        self.long_rec.stop()
        self._sync_button_states()

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

    def _append_log(self, text: str) -> None:
        self.log_widget.configure(state=tk.NORMAL)
        self.log_widget.insert(tk.END, text)
        self.log_widget.see(tk.END)
        self.log_widget.configure(state=tk.DISABLED)

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
            )
            else tk.NORMAL
        )
        self.btn_stop_long.configure(
            state=tk.DISABLED
            if long_st != LongRecordState.RECORDING
            else tk.NORMAL
        )

    def _watchdog_tick(self) -> None:
        if not self._shutting_down and not self._startup_blocked:
            running = self.buffer.running()
            snap = snapshot_buffer_health(self.settings)

            if snap.newest_segment_mtime:
                self._last_segment_ts = dt.datetime.fromtimestamp(
                    snap.newest_segment_mtime
                ).strftime("%H:%M:%S")

            if running and not snap.healthy:
                self._stall_ticks += 1
                logger.warning("Buffer health: %s", snap.detail)
            else:
                self._stall_ticks = 0

            stalled = running and (not snap.healthy and self._stall_ticks >= 2)

            if stalled and not self._restart_pending:
                self._buffer_state.set("Buffer: STALLED (restarting)")
                logger.error("Buffer stalled: %s — restarting", snap.detail)
                self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
                self._last_error_msg = f"stalled: {snap.detail}"
                self._auto_restart_count += 1
                self.buffer.stop()
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
                self._buffer_state.set("Buffer: RUNNING")
                logger.info("Buffer auto-restart succeeded")
            else:
                self._buffer_state.set("Buffer: RESTART FAILED")
                logger.error("Buffer auto-restart failed")

        self.root.after(int(delay * 1000), go)

    def on_restart_buffer(self) -> None:
        logger.info("Operator: restart buffer requested")
        self._restart_pending = False
        self._buffer_intentional_stop = True
        self._buffer_state.set("Buffer: RESTARTING (manual)")
        self.buffer.stop()
        self._buffer_intentional_stop = False
        self._backoff_idx = 0
        if self.buffer.start():
            self._buffer_state.set("Buffer: RUNNING")
        else:
            self._buffer_state.set("Buffer: FAILED")

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
        run_kw: dict = {}
        if sys.platform == "win32" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            run_kw["creationflags"] = subprocess.CREATE_NO_WINDOW  # type: ignore[attr-defined]

        cwd = self.settings.buffer_dir.resolve()
        try:
            r = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=120,
                cwd=str(cwd),
                **run_kw,
            )
        except (OSError, subprocess.TimeoutExpired) as e:
            self._emit_ui(f"Save replay ffmpeg failed: {e}")
            logger.exception("replay export")
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = str(e)
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
            return

        try:
            temp.replace(final)
        except OSError as e:
            self._emit_ui(f"Could not finalize replay file: {e}")
            self._last_error_ts = dt.datetime.now().strftime("%H:%M:%S")
            self._last_error_msg = str(e)
            return

        self._emit_ui(f"Instant replay written: {final}")
        logger.info("Replay export success: %s", final)
        _touch(self.settings.instant_replay_trigger)
        self._last_replay_ts = dt.datetime.now().strftime("%H:%M:%S")

    def on_quit(self) -> None:
        self._shutting_down = True
        self._buffer_intentional_stop = True
        logger.info("Operator shutdown")
        if sys.platform == "win32":
            from global_hotkeys import unregister_all_global_hotkeys_win

            unregister_all_global_hotkeys_win()
        self.long_rec.stop()
        self.buffer.stop()
        self.root.destroy()


def main() -> None:
    try:
        settings = load_encoder_settings()
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    root = tk.Tk()
    OperatorApp(root, settings)
    root.mainloop()


if __name__ == "__main__":
    main()
