"""
Explicit operator state for replay and long recording (ReplayTrove appliance semantics).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path

logger = logging.getLogger("replaytrove.encoder")


class ReplayControlState(Enum):
    """Derived replay control state for gating save commands."""

    IDLE = auto()
    SAVING_REPLAY = auto()
    REPLAY_ACTIVE = auto()


class LongRecordState(Enum):
    NOT_RECORDING = auto()
    RECORDING = auto()


def _parse_iso_utc(s: str) -> datetime | None:
    s = str(s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class ScoreboardEncoderSnapshot:
    """Parsed scoreboard state.json for encoder replay gating."""

    replay_display_active_raw: bool
    """Value from JSON (may be stale)."""

    stale: bool
    """True when replay_display_active_raw is true but updated_at/mtime is too old."""

    blocks_replay_save: bool
    """True only when scoreboard reports active and snapshot is fresh."""

    age_seconds: float | None
    """Seconds since updated_at (or file mtime fallback); None if unknown."""


def read_scoreboard_replay_for_encoder(
    path: Path | None,
    *,
    max_age_seconds: float,
) -> ScoreboardEncoderSnapshot:
    """
    Read scoreboard JSON. If ``replay_display_active`` is true but the file has not been
    updated within ``max_age_seconds``, treat as **stale** and do not block encoder replay saves.
    """
    if path is None:
        return ScoreboardEncoderSnapshot(False, False, False, None)

    try:
        if not path.is_file():
            return ScoreboardEncoderSnapshot(False, False, False, None)
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError, TypeError):
        logger.debug("Could not read scoreboard state from %s", path, exc_info=True)
        return ScoreboardEncoderSnapshot(False, False, False, None)

    active = bool(data.get("replay_display_active", False))
    updated_at_str = data.get("updated_at")

    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = None

    last_dt: datetime | None = None
    if isinstance(updated_at_str, str):
        last_dt = _parse_iso_utc(updated_at_str)
    if last_dt is None and mtime is not None:
        last_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)

    now = datetime.now(timezone.utc)
    if last_dt is None:
        age: float | None = None
        stale = active
    else:
        age = max(0.0, (now - last_dt).total_seconds())
        stale = active and age > max_age_seconds

    blocks = active and not stale
    return ScoreboardEncoderSnapshot(
        replay_display_active_raw=active,
        stale=stale,
        blocks_replay_save=blocks,
        age_seconds=age,
    )
