"""
Windows global hotkeys using the lightweight ``keyboard`` package.
Callbacks from the hook thread must marshal to Tk via ``root.after(0, ...)``.
"""

from __future__ import annotations

import logging
import sys
import threading
from typing import Callable

logger = logging.getLogger("replaytrove.encoder")


def register_global_hotkeys_win(
    root: object,
    bindings: list[tuple[str, Callable[[], None]]],
    *,
    on_done: Callable[[], None] | None = None,
    on_registered: Callable[[str], None] | None = None,
    on_registration_failed: Callable[[str, str], None] | None = None,
) -> None:
    """
    Register each ``(combo, handler)`` in a short-lived background thread.
    ``handler`` is scheduled on the Tk main thread (pass bound methods that are safe on Tk).
    """

    def make_hook(h: Callable[[], None]) -> Callable[[], None]:
        return lambda: root.after(0, h)

    def worker() -> None:
        try:
            import keyboard
        except ImportError:
            logger.exception("Install the keyboard package: pip install keyboard")
            if on_done:
                root.after(0, on_done)
            return

        try:
            for combo, handler in bindings:
                c = combo.strip().lower()
                try:
                    keyboard.add_hotkey(c, make_hook(handler))
                    logger.info("Registered global hotkey: %s", c)
                    if on_registered:
                        root.after(0, lambda combo_name=c: on_registered(combo_name))
                except Exception as e:
                    logger.exception("Failed to register global hotkey: %s", c)
                    if on_registration_failed:
                        root.after(
                            0,
                            lambda combo_name=c, err=str(e): on_registration_failed(
                                combo_name, err
                            ),
                        )
        except Exception:
            logger.exception("Failed to register global hotkeys")
        if on_done:
            root.after(0, on_done)

    threading.Thread(target=worker, daemon=True, name="encoder-hotkeys").start()


def unregister_all_global_hotkeys_win() -> None:
    if sys.platform != "win32":
        return
    try:
        import keyboard

        keyboard.unhook_all()
        logger.info("Global hotkeys unregistered (unhook_all)")
    except ImportError:
        pass
    except Exception:
        logger.exception("Failed to unregister global hotkeys")
