"""
Backward-compatible entry point: runs the long-record-only operator.

Rolling HLS buffer and instant replay export were removed from this codebase.
"""

from __future__ import annotations

from operator_long_only import main

if __name__ == "__main__":
    main()
