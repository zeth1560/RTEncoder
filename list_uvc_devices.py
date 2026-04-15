"""Print UVC / capture devices (DirectShow on Windows, v4l2 on Linux)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from settings import load_dotenv_if_present


def main() -> None:
    load_dotenv_if_present()
    ff = Path(os.environ.get("FFMPEG_PATH", r"C:\ffmpeg\bin\ffmpeg.exe"))
    if not ff.exists():
        w = shutil.which("ffmpeg")
        if w:
            ff = Path(w)

    if sys.platform == "win32":
        args = [str(ff), "-hide_banner", "-f", "dshow", "-list_devices", "true", "-i", "dummy"]
    else:
        args = [str(ff), "-hide_banner", "-f", "v4l2", "-list_devices", "true", "-i", "dummy"]

    r = subprocess.run(args)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
