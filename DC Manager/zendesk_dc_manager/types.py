"""
Type definitions and data structures for Zendesk DC Manager.
"""

import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Callable

from zendesk_dc_manager.config import logger


@dataclass
class TranslationStats:
    """Statistics from a translation run."""

    total: int = 0
    translated: int = 0
    from_cache: int = 0
    failed: int = 0
    skipped: int = 0

    @property
    def success_rate(self) -> float:
        # Each item generates 2 operations (EN + ES), so use actual operations
        # attempted rather than item count to keep the rate in 0-100% range.
        operations = self.translated + self.from_cache + self.failed
        if operations == 0:
            return 0.0
        return (self.translated + self.from_cache) / operations * 100

    def __str__(self) -> str:
        return (
            f"TranslationStats(total={self.total}, "
            f"translated={self.translated}, "
            f"from_cache={self.from_cache}, failed={self.failed}, "
            f"success_rate={self.success_rate:.1f}%)"
        )


class AppState(Enum):
    """Application state enumeration."""

    IDLE = auto()
    CONNECTING = auto()
    SCANNING = auto()
    TRANSLATING = auto()
    APPLYING = auto()
    ROLLING_BACK = auto()
    LOADING_BACKUP = auto()
    CLEARING_CACHE = auto()

    def is_busy(self) -> bool:
        return self != AppState.IDLE

    @property
    def display_name(self) -> str:
        names = {
            AppState.IDLE: "Ready",
            AppState.CONNECTING: "Connecting...",
            AppState.SCANNING: "Scanning...",
            AppState.TRANSLATING: "Translating...",
            AppState.APPLYING: "Applying Changes...",
            AppState.ROLLING_BACK: "Rolling Back...",
            AppState.LOADING_BACKUP: "Loading Backup...",
            AppState.CLEARING_CACHE: "Clearing Cache...",
        }
        return names.get(self, self.name)


class StateManager:
    """Thread-safe application state manager."""

    def __init__(
        self,
        ui_callback: Optional[Callable[[AppState], None]] = None
    ):
        self._state = AppState.IDLE
        self._lock = threading.RLock()
        self._ui_callback = ui_callback

    @property
    def state(self) -> AppState:
        with self._lock:
            return self._state

    @property
    def is_busy(self) -> bool:
        return self.state != AppState.IDLE

    @property
    def is_idle(self) -> bool:
        return self.state == AppState.IDLE

    def _notify_ui(self, state: AppState):
        if self._ui_callback:
            try:
                self._ui_callback(state)
            except Exception as e:
                logger.warning(f"UI callback error: {e}")

    @contextmanager
    def transition(self, new_state: AppState):
        with self._lock:
            if self._state != AppState.IDLE:
                raise RuntimeError(
                    f"Cannot start {new_state.name} "
                    f"while in {self._state.name}"
                )
            self._state = new_state

        self._notify_ui(new_state)

        try:
            yield
        finally:
            with self._lock:
                self._state = AppState.IDLE
            self._notify_ui(AppState.IDLE)

    def force_reset(self):
        with self._lock:
            self._state = AppState.IDLE
        self._notify_ui(AppState.IDLE)

    def try_transition(self, new_state: AppState) -> bool:
        with self._lock:
            if self._state != AppState.IDLE:
                return False
            self._state = new_state

        self._notify_ui(new_state)
        return True
