"""
Supervise the long-record operator process and restart it when encoder_state.json
reports unhealthy status (blocked, degraded, or stale updates).

Run this instead of starting operator_long_only.py directly, e.g.:
  python encoder_watchdog.py

Environment (optional, after loading .env):
  ENCODER_STATE_PATH — same as operator (default: C:\\ReplayTrove\\scoreboard\\encoder_state.json)
  WATCHDOG_OPERATOR_SCRIPT — path to operator_long_only.py (default: alongside this file)
  WATCHDOG_POLL_INTERVAL — seconds between checks (default: 2)
  WATCHDOG_STARTUP_GRACE_SECONDS — ignore bad state until this long after each spawn (default: 45)
  WATCHDOG_STALE_STATE_SECONDS — if updated_at is older than this while child is alive, restart (default: 90)
  WATCHDOG_MAX_RESTARTS — exit watchdog after this many restarts in the sliding window (default: 15)
  WATCHDOG_RESTART_WINDOW_SECONDS — window for max restarts (default: 600)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from settings import load_dotenv_if_present
from subprocess_win import no_console_creationflags


def _opt_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return float(str(raw).strip())


def _opt_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return int(str(raw).strip())


def _state_path() -> Path:
    return Path(
        os.environ.get(
            "ENCODER_STATE_PATH",
            r"C:\ReplayTrove\scoreboard\encoder_state.json",
        )
    ).resolve()


def _operator_argv() -> list[str]:
    override = os.environ.get("WATCHDOG_OPERATOR_SCRIPT", "").strip()
    if override:
        script = Path(override).resolve()
    else:
        script = Path(__file__).resolve().parent / "operator_long_only.py"
    if not script.exists():
        raise FileNotFoundError(f"Operator script not found: {script}")
    return [sys.executable, str(script)]


def _parse_updated_at(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        s = raw.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _state_requires_restart(data: dict[str, Any]) -> tuple[bool, str]:
    st = data.get("state")
    if st == "shutting_down":
        return False, ""
    if data.get("degraded") is True:
        return True, "degraded"
    if st == "blocked":
        return True, "blocked"
    if st == "unavailable":
        return False, ""
    return False, ""


def _stale_seconds(path: Path, stale_after: float) -> tuple[bool, str]:
    data = _read_state(path)
    if data is None:
        return True, "missing_or_unreadable_state_file"
    ts = _parse_updated_at(data.get("updated_at"))
    if ts is None:
        return True, "invalid_or_missing_updated_at"
    age = (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds()
    if age > stale_after:
        return True, f"stale_state age={age:.1f}s"
    return False, ""


def _kill_process_tree(pid: int, log: logging.Logger) -> None:
    if pid <= 0:
        return
    if sys.platform == "win32":
        r = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=45,
            **no_console_creationflags(),
        )
        if r.returncode != 0:
            log.warning(
                "taskkill exit %s: %s",
                r.returncode,
                (r.stderr or r.stdout or "").strip()[-500:],
            )
    else:
        try:
            os.kill(pid, 15)
        except OSError as e:
            log.warning("kill(%s) failed: %s", pid, e)


def main() -> None:
    load_dotenv_if_present()
    state_path = _state_path()
    poll = _opt_float("WATCHDOG_POLL_INTERVAL", 2.0)
    grace = _opt_float("WATCHDOG_STARTUP_GRACE_SECONDS", 45.0)
    stale_after = _opt_float("WATCHDOG_STALE_STATE_SECONDS", 90.0)
    max_restarts = _opt_int("WATCHDOG_MAX_RESTARTS", 15)
    window_sec = _opt_float("WATCHDOG_RESTART_WINDOW_SECONDS", 600.0)
    restart_on_zero = os.environ.get("WATCHDOG_RESTART_ON_ZERO_EXIT", "0").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    log_dir = Path(os.environ.get("ENCODER_LOG_DIR", r"C:\ReplayTrove\logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "encoder_watchdog.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger("replaytrove.encoder.watchdog")

    argv = _operator_argv()
    log.info("Watchdog starting; state=%s operator=%s", state_path, argv)

    restart_times: deque[float] = deque()
    child: subprocess.Popen | None = None
    spawn_mono: float = 0.0

    def bump_restart_counter() -> None:
        restart_times.append(time.monotonic())
        if len(restart_times) >= max_restarts:
            log.critical(
                "Reached %s restarts in %.0fs (limit=%s); exiting watchdog.",
                len(restart_times),
                window_sec,
                max_restarts,
            )
            raise SystemExit(2)

    try:
        while True:
            now = time.monotonic()
            while restart_times and now - restart_times[0] > window_sec:
                restart_times.popleft()

            if child is None or child.poll() is not None:
                if child is not None and child.poll() is not None:
                    code = child.returncode
                    if code == 0 and not restart_on_zero:
                        log.info("Operator exited normally; watchdog stopping.")
                        return
                    log.error("Operator exited (code=%s); restarting.", code)
                    bump_restart_counter()

                log.info("Spawning operator: %s", argv)
                child = subprocess.Popen(argv, **no_console_creationflags())
                spawn_mono = time.monotonic()
                time.sleep(min(poll, 1.0))
                continue

            in_grace = (time.monotonic() - spawn_mono) < grace

            if not in_grace:
                bad_stale, stale_reason = _stale_seconds(state_path, stale_after)
                if bad_stale:
                    log.warning("Restarting operator: %s", stale_reason)
                    _kill_process_tree(child.pid, log)
                    try:
                        child.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        log.warning("Child wait timeout after taskkill.")
                    child = None
                    bump_restart_counter()
                    time.sleep(1.0)
                    continue

                data = _read_state(state_path)
                if data:
                    need, reason = _state_requires_restart(data)
                    if need:
                        log.warning(
                            "Restarting operator: %s (state=%s last_error=%s)",
                            reason,
                            data.get("state"),
                            data.get("last_error"),
                        )
                        _kill_process_tree(child.pid, log)
                        try:
                            child.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            log.warning("Child wait timeout after taskkill.")
                        child = None
                        bump_restart_counter()
                        time.sleep(1.0)
                        continue

            time.sleep(poll)
    finally:
        if child is not None and child.poll() is None:
            log.info("Watchdog stopping; terminating operator (pid=%s).", child.pid)
            _kill_process_tree(child.pid, log)
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Watchdog interrupted.", file=sys.stderr)
        raise SystemExit(130)
