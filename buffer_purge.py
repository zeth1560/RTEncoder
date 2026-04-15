"""Remove stale rolling-HLS artifacts before starting a fresh buffer process."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("replaytrove.encoder")


def purge_stale_hls_artifacts(buffer_dir: Path) -> None:
    """
    Delete segment files, playlist, concat list, and common temp names under buffer_dir.
    """
    buf = buffer_dir.resolve()
    if not buf.is_dir():
        return
    try:
        names = list(buf.iterdir())
    except OSError as e:
        logger.warning("buffer purge: cannot list %s: %s", buf, e)
        return

    for p in names:
        if not p.is_file():
            continue
        name = p.name.lower()
        try:
            if name.endswith(".ts"):
                p.unlink()
            elif name in ("live.m3u8", "_replay_concat.txt", "live.m3u8.tmp"):
                p.unlink()
        except OSError as e:
            logger.warning("buffer purge: could not remove %s: %s", p, e)
