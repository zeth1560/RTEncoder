"""
Publish encoder appliance state to a shared JSON file (atomic replace).

Scoreboard integration contract
-------------------------------
The scoreboard MUST read recorder status only from this file (``ENCODER_STATE_PATH``).
Do not infer readiness from triggers, timers, or side effects.

Typical overlay logic (``encoder_status_overlay.py``): terminal ``state`` values force an
unavailable-style display regardless of booleans. If ``encoder_ready``,
``long_recording_available``, or ``rolling_buffer_applicable`` is present and **any** is
true, the appliance is treated as ready for UI purposes; if all three are present and **all**
are false, unavailable — **except** you should special-case ``state: "starting"`` so boot
does not flash unavailable. Legacy fallback: ``state`` in ready / recording / idle.

Keep ``updated_at`` fresh while the process runs (the operator publishes at least once per
second on the Tk tick). If reads are less frequent, raise the overlay’s stale threshold
(e.g. ``ENCODER_STATUS_STALE_SECONDS`` > 45).

* ``allow_record_timer_overlay`` — when false, hide/disable record timer UI.
* ``long_recording_available`` — when true, operator may start a long recording (false  during ``starting``, ``blocked``, ``recording``, empty device, or ``shutting_down``).
* ``state`` / ``status_text`` — primary appliance state for display.
  ``starting`` is the boot window before startup validation; encoder booleans are false on purpose.
  ``unavailable`` means no video device configured (empty ``UVC_VIDEO_DEVICE``).
* ``encoder_ready`` — true only for ``ready`` and ``recording`` (appliance can capture).
* ``long_recording_session_seq`` — increments each time a long recording **starts** (edge-friendly
  for polled scoreboards). ``long_recording_started_at`` — UTC ISO timestamp while a session is
  active, else null. Pair with ``long_recording_active`` / ``state: recording`` for on-air UI.
* ``degraded`` — independent health hint (e.g. recording output stalled); may be true while
  ``state`` is still ``recording``. Scoreboards may style this separately from ``state``.
* ``last_error`` — most recent fault description for operators.
* ``long_recording_last_fault`` — last long-record capture failure (empty when none).

An optional ``encoder_watchdog.py`` process may restart the operator when ``state``
is ``blocked``, ``degraded`` is true, or ``updated_at`` is stale (see that script).

The operator sets ``rolling_buffer_applicable`` to false so scoreboards that still
read this field do not expect a rolling HLS buffer on this appliance.

The file is replaced atomically so readers never see a partial JSON object.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

SCHEMA_VERSION = 1

# Primary state values written to the JSON `state` field.
STATE_STARTING = "starting"
STATE_READY = "ready"
STATE_DEGRADED = "degraded"
STATE_UNAVAILABLE = "unavailable"
STATE_RECORDING = "recording"
STATE_RESTARTING = "restarting"
STATE_BLOCKED = "blocked"
STATE_SHUTTING_DOWN = "shutting_down"


def encoder_state_payload_starting() -> dict[str, Any]:
    """Minimal state while the operator process is booting (before Tk / probe)."""
    return {
        "state": STATE_STARTING,
        "status_text": "Recorder starting…",
        "encoder_ready": False,
        "allow_record_timer_overlay": False,
        "rolling_buffer_applicable": False,
        "long_recording_active": False,
        "long_recording_session_seq": 0,
        "long_recording_started_at": None,
        "long_recording_available": False,
        "restart_pending": False,
        "degraded": False,
        "auto_restart_count": 0,
        "last_error": "—",
        "long_recording_last_fault": "",
        "mode": "long_only",
    }


def publish_encoder_state(
    path: Path,
    payload: dict[str, Any],
    *,
    on_written: Callable[[Path, dict[str, Any]], None] | None = None,
) -> None:
    """Write JSON atomically (temp + os.replace)."""
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    out = dict(payload)
    out.setdefault("schema_version", SCHEMA_VERSION)
    out["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_name(path.name + ".tmp")
    text = json.dumps(out, indent=2, sort_keys=False) + "\n"
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    if on_written is not None:
        on_written(path, out)
