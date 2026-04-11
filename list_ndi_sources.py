"""Print available NDI sources (requires ffmpeg with libndi_newtek)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> None:
    ff = Path(os.environ.get("FFMPEG_PATH", r"C:\ffmpeg\bin\ffmpeg.exe"))
    if not ff.exists():
        w = shutil.which("ffmpeg")
        if w:
            ff = Path(w)
    ips = os.environ.get("NDI_EXTRA_IPS", "").strip()
    args = [str(ff), "-hide_banner", "-f", "libndi_newtek"]
    if ips:
        args += ["-extra_ips", ips]
    args += ["-find_sources", "1", "-i", "dummy"]
    r = subprocess.run(args)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
