"""Windows helpers for spawning subprocesses without a flashing console window."""

from __future__ import annotations

import subprocess
import sys
from typing import Any


def no_console_creationflags() -> dict[str, Any]:
    """Keyword args for subprocess.Popen / subprocess.run on Windows GUI apps."""
    if sys.platform == "win32" and hasattr(subprocess, "CREATE_NO_WINDOW"):
        return {"creationflags": subprocess.CREATE_NO_WINDOW}  # type: ignore[attr-defined]
    return {}
