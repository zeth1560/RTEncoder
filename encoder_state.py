"""
Publish encoder appliance state to a shared JSON file (atomic replace).

Scoreboard integration contract
-------------------------------
The scoreboard MUST read recorder status only from this file (``ENCODER_STATE_PATH``).
Do not infer readiness from triggers, timers, or side effects.

* ``allow_record_timer_overlay`` — when false, hide/disable record timer UI.
* ``long_recording_available`` — when true, operator may start a long recording.
* ``state`` / ``status_text`` — primary appliance state for display.
* ``encoder_ready`` — high-level “ok to rely on encoder” (excludes blocked/unavailable).
* ``last_error`` — most recent fault description for operators.
* ``long_recording_last_fault`` — last long-record capture failure (empty when none).

The file is replaced atomically so readers never see a partial JSON object.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def publish_encoder_state(path: Path, payload: dict[str, Any]) -> None:
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
