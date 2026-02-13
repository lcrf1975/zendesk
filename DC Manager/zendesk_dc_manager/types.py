"""
Type definitions and data structures for Zendesk DC Manager.
"""

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Dict, List, Any, Callable, TypeVar, Generic

from zendesk_dc_manager.config import (
    SOURCE_NEW,
    SOURCE_ZENDESK_DC,
    SOURCE_TRANSLATED,
    SOURCE_CACHE,
    SOURCE_FAILED,
    logger,
)


T = TypeVar('T')


@dataclass
class Result(Generic[T]):
    """A Result type for operations that can fail."""

    success: bool
    value: Optional[T] = None
    error: Optional[str] = None
    error_code: Optional[int] = None
    details: Optional[Dict[str, Any]] = None

    @classmethod
    def ok(cls, value: T) -> 'Result[T]':
        return cls(success=True, value=value)

    @classmethod
    def fail(
        cls,
        error: str,
        error_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> 'Result[T]':
        return cls(
            success=False,
            error=error,
            error_code=error_code,
            details=details
        )

    def __bool__(self) -> bool:
        return self.success

    def unwrap(self) -> T:
        if not self.success:
            raise ValueError(f"Unwrap called on failed Result: {self.error}")
        return self.value

    def unwrap_or(self, default: T) -> T:
        return self.value if self.success else default


@dataclass
class TranslationResult:
    """Result of a translation operation for a single item."""

    en: str = ""
    es: str = ""
    en_source: str = SOURCE_NEW
    es_source: str = SOURCE_NEW
    en_failed: bool = False
    es_failed: bool = False


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
        if self.total == 0:
            return 0.0
        return (self.translated + self.from_cache) / self.total * 100

    def __str__(self) -> str:
        return (
            f"TranslationStats(total={self.total}, "
            f"translated={self.translated}, "
            f"from_cache={self.from_cache}, failed={self.failed}, "
            f"success_rate={self.success_rate:.1f}%)"
        )


@dataclass
class ScanStats:
    """Statistics from a scan operation."""

    # Ticket-related
    valid_fields: int = 0
    valid_forms: int = 0

    # User/Org fields
    valid_user_fields: int = 0
    valid_org_fields: int = 0

    # Business rules
    valid_macros: int = 0
    valid_triggers: int = 0
    valid_automations: int = 0
    valid_views: int = 0
    valid_sla_policies: int = 0

    # Custom statuses
    valid_custom_statuses: int = 0

    # Groups
    valid_groups: int = 0

    # Help Center
    valid_cats: int = 0
    valid_sects: int = 0
    valid_arts: int = 0

    # Exclusions
    ignored: int = 0
    already_dc: int = 0
    system_excluded: int = 0

    @property
    def total_valid(self) -> int:
        return (
            self.valid_fields + self.valid_forms +
            self.valid_user_fields + self.valid_org_fields +
            self.valid_macros + self.valid_triggers +
            self.valid_automations + self.valid_views +
            self.valid_sla_policies + self.valid_custom_statuses +
            self.valid_groups +
            self.valid_cats + self.valid_sects + self.valid_arts
        )

    def to_dict(self) -> Dict[str, int]:
        return {
            'valid_fields': self.valid_fields,
            'valid_forms': self.valid_forms,
            'valid_user_fields': self.valid_user_fields,
            'valid_org_fields': self.valid_org_fields,
            'valid_macros': self.valid_macros,
            'valid_triggers': self.valid_triggers,
            'valid_automations': self.valid_automations,
            'valid_views': self.valid_views,
            'valid_sla_policies': self.valid_sla_policies,
            'valid_custom_statuses': self.valid_custom_statuses,
            'valid_groups': self.valid_groups,
            'valid_cats': self.valid_cats,
            'valid_sects': self.valid_sects,
            'valid_arts': self.valid_arts,
            'ignored': self.ignored,
            'already_dc': self.already_dc,
            'system_excluded': self.system_excluded,
        }


@dataclass
class WorkItem:
    """Validated work item for DC operations."""

    id: int
    type: str
    context: str
    dc_name: str
    placeholder: str
    pt: str
    en: str = ""
    es: str = ""
    en_source: str = SOURCE_NEW
    es_source: str = SOURCE_NEW
    pt_source: str = SOURCE_NEW
    action: str = "CREATE"
    dc_id: Optional[int] = None
    is_option: bool = False
    parent_id: Optional[int] = None
    tags: str = ""
    force_update: bool = False
    source: str = SOURCE_NEW
    is_system: bool = False

    def __post_init__(self):
        if not self.dc_name:
            raise ValueError("dc_name is required")
        if not self.pt:
            raise ValueError("pt (original text) is required")

    @property
    def is_complete(self) -> bool:
        return bool(self.en and self.es)

    @property
    def needs_translation(self) -> bool:
        return not self.en or not self.es

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'type': self.type,
            'context': self.context,
            'dc_name': self.dc_name,
            'placeholder': self.placeholder,
            'pt': self.pt,
            'en': self.en,
            'es': self.es,
            'en_source': self.en_source,
            'es_source': self.es_source,
            'pt_source': self.pt_source,
            'action': self.action,
            'dc_id': self.dc_id,
            'is_option': self.is_option,
            'parent_id': self.parent_id,
            'tags': self.tags,
            'force_update': self.force_update,
            'source': self.source,
            'is_system': self.is_system,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkItem':
        return cls(
            id=data.get('id', 0),
            type=data.get('type', ''),
            context=data.get('context', 'Unknown'),
            dc_name=data.get('dc_name', ''),
            placeholder=data.get('placeholder', ''),
            pt=data.get('pt', ''),
            en=data.get('en', ''),
            es=data.get('es', ''),
            en_source=data.get('en_source', SOURCE_NEW),
            es_source=data.get('es_source', SOURCE_NEW),
            pt_source=data.get('pt_source', SOURCE_NEW),
            action=data.get('action', 'CREATE'),
            dc_id=data.get('dc_id'),
            is_option=data.get('is_option', False),
            parent_id=data.get('parent_id'),
            tags=data.get('tags', ''),
            force_update=data.get('force_update', False),
            source=data.get('source', SOURCE_NEW),
            is_system=data.get('is_system', False),
        )


@dataclass
class ExecutionResults:
    """Results from an apply/execute operation."""

    success: List[str] = field(default_factory=list)
    failed: List[Dict[str, Any]] = field(default_factory=list)
    backup_file: str = ""

    @property
    def success_count(self) -> int:
        return len(self.success)

    @property
    def failed_count(self) -> int:
        return len(self.failed)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'failed': self.failed,
            'backup_file': self.backup_file
        }


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