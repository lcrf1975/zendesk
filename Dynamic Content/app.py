"""
Zendesk Dynamic Content Manager

A PyQt6 desktop application for managing Zendesk Dynamic Content
with automated translation capabilities.

Version: 42.0 (Bug Fixes & Enhancements)

Changes from v41:
- FIX: Sidebar buttons no longer steal focus from checkboxes
- FIX: Table/data index mismatch validation in run_apply
- FIX: Translation loss prevention on force retranslate failure
- FIX: JSON error handling in pagination
- FIX: Safer cleanup with timeout
- FIX: Improved closeEvent with shorter timeouts
- ENHANCEMENT: Added read-only work items access for performance
- ENHANCEMENT: Better error messages throughout
"""

import sys
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import re
import unicodedata
import json
import os
import random
import html
import sqlite3
import hashlib
import threading
import logging
import copy
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from enum import Enum, auto
from queue import Queue, Empty, Full
from typing import (
    Optional,
    Dict,
    List,
    Tuple,
    Any,
    Callable,
    Generator,
    TypeVar,
    Generic,
    Set,
)

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURATION: MACOS + PYENV + QT6 COMPATIBILITY
# ==============================================================================

if sys.platform == 'darwin':
    os.environ['QT_MAC_WANTS_LAYER'] = '1'
    os.environ['QT_DEBUG_PLUGINS'] = '0'
    os.environ['QT_FILESYSTEMMODEL_WATCH_FILES'] = '0'
    os.environ['QT_ENABLE_HIGHDPI_SCALING'] = '1'

os.environ['QT_QUICK_BACKEND'] = 'software'

from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QProgressBar,
    QTextEdit,
    QMessageBox,
    QFrame,
    QSplitter,
    QAbstractItemView,
    QComboBox,
    QCheckBox,
    QFileDialog,
    QSpinBox,
    QSizePolicy,
    QGridLayout,
    QGroupBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import (
    QColor,
    QBrush,
    QFontDatabase,
    QPalette,
    QFont,
    QTextCharFormat,
)


# ==============================================================================
# RESULT TYPE FOR ERROR HANDLING
# ==============================================================================

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


# ==============================================================================
# CONFIGURATION CLASSES
# ==============================================================================

@dataclass(frozen=True)
class APIConfig:
    """API configuration constants."""
    
    TIMEOUT_SHORT: int = 15
    TIMEOUT_DEFAULT: int = 30
    TIMEOUT_LONG: int = 45
    
    RETRY_COUNT: int = 3
    RETRY_BASE_DELAY: float = 1.0
    RETRY_MAX_DELAY: float = 30.0
    RETRY_BACKOFF_FACTOR: float = 2.0
    
    RATE_LIMIT_INITIAL_WAIT: int = 2
    RATE_LIMIT_MAX_WAIT: int = 60
    RATE_LIMIT_BACKOFF_FACTOR: float = 2.0
    
    THREAD_POOL_SIZE: int = 5
    THREAD_POOL_SIZE_VARIANTS: int = 8
    
    MAX_PAGINATION_PAGES: int = 1000


@dataclass(frozen=True)
class TranslationConfig:
    """Translation-specific configuration."""
    
    DELAY_MIN: float = 0.3
    DELAY_MAX: float = 0.8
    MIN_TEXT_FOR_PADDING: int = 15
    MIN_TEXT_FOR_PADDING_LOWER: int = 3
    DEFAULT_CACHE_EXPIRY_DAYS: int = 30


@dataclass(frozen=True)
class UIConfig:
    """UI-specific configuration."""
    
    WORKER_STOP_TIMEOUT_MS: int = 3000
    WORKER_STOP_INTERVALS: Tuple[int, ...] = (500, 1000, 2000)
    LOG_INTERVAL: int = 100
    STATUS_UPDATE_INTERVAL_SEC: float = 10.0
    SIDEBAR_WIDTH: int = 200
    STATUS_BAR_HEIGHT: int = 55
    MIN_WINDOW_WIDTH: int = 1100
    MIN_WINDOW_HEIGHT: int = 700
    CARD_MARGIN: int = 20
    CARD_SPACING: int = 12
    SPLITTER_TOP_SIZE: int = 450
    SPLITTER_LOG_SIZE: int = 350
    INPUT_MIN_HEIGHT: int = 36
    COMBO_MIN_HEIGHT: int = 38
    SECTION_SPACING: int = 20
    FORM_ROW_SPACING: int = 12
    LABEL_WIDTH: int = 100


# Global configuration instances
API_CONFIG = APIConfig()
TRANSLATION_CONFIG = TranslationConfig()
UI_CONFIG = UIConfig()


# ==============================================================================
# CONSTANTS
# ==============================================================================

CREDENTIALS_FILE = "credentials.json"

SOURCE_NEW = "New"
SOURCE_ZENDESK_DC = "Zendesk DC"
SOURCE_TRANSLATED = "Translated"
SOURCE_CACHE = "Cache"
SOURCE_FAILED = "Failed"

SYSTEM_FIELD_IDENTIFIERS = frozenset({
    'subject', 'description', 'status', 'tickettype', 'ticket_type',
    'priority', 'group', 'assignee', 'brand', 'satisfaction_rating',
    'custom_status', 'lookup', 'email', 'name', 'time_zone', 'locale_id',
    'organization_id', 'role', 'custom_role_id', 'details', 'notes',
    'phone', 'mobile', 'whatsapp', 'facebook', 'twitter', 'google',
    'photo', 'authenticity_token', 'active', 'alias', 'signature',
    'shared_phone_number', 'domain_names', 'tags', 'shared_tickets',
    'shared_comments', 'external_id', 'problem_id', 'created_at',
    'updated_at', 'via_id', 'recipient', 'submitter', 'requester',
    'due_date'
})

SYSTEM_FIELD_NAMES = frozenset({
    'intent', 'intent confidence', 'sentiment', 'sentiment confidence',
    'language', 'language confidence', 'summary', 'resolution type',
    'approval status', 'suggestion', 'recommendation', 'ticket status',
    'shared with', 'confidence', 'summary agent id',
    'summary date and time', 'summary locale',
    'id do agente do resumo', 'localidade do resumo', 'resumo',
    'data e hora do resumo', 'status de aprovação', 'intenção',
    'confiança da intenção', 'confiança do sentimento', 'idioma',
    'confiança do idioma'
})

COMMON_SHORT_WORDS = frozenset({
    'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL',
    'CAN', 'HAD', 'HER', 'WAS', 'ONE', 'OUR', 'OUT', 'DAY',
    'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'MAN', 'NEW', 'NOW',
    'OLD', 'SEE', 'WAY', 'WHO', 'BOY', 'DID', 'ITS', 'LET',
    'SAY', 'SHE', 'TOO', 'USE', 'MAY', 'SAT', 'RAN', 'RUN',
    'SET', 'SIT', 'TRY', 'ASK', 'BIG', 'END', 'FAR', 'FEW',
    'GOT', 'OWN', 'PUT', 'RED', 'TOP', 'YES', 'YET', 'AGO',
    'ADD', 'AGE', 'AID', 'AIM', 'AIR', 'ART', 'BAD', 'BAG',
    'BED', 'BIT', 'BOX', 'BUS', 'BUY', 'CAR', 'CUT', 'DOG',
    'DUE', 'EAR', 'EAT', 'EGG', 'EYE', 'FAN', 'FAT', 'FIT',
    'FLY', 'FUN', 'GAS', 'GUN', 'GUY', 'HAT', 'HIT', 'HOT',
    'ICE', 'ILL', 'JOB', 'KEY', 'KID', 'LAW', 'LAY', 'LED',
    'LEG', 'LIE', 'LOT', 'LOW', 'MAP', 'MEN', 'MET', 'MIX',
    'MOM', 'NET', 'NOR', 'NUT', 'ODD', 'OFF', 'OIL', 'PAY',
    'PEN', 'PET', 'PIE', 'PIN', 'PIT', 'POP', 'POT', 'RAW',
    'RIB', 'RID', 'ROW', 'RUB', 'SAD', 'SAN', 'SEA', 'SIR',
    'SIX', 'SKI', 'SKY', 'SON', 'SUM', 'SUN', 'TAN', 'TAX',
    'TEA', 'TEN', 'TIE', 'TIP', 'TOE', 'TON', 'TOY', 'TWO',
    'VAN', 'VIA', 'WAR', 'WEB', 'WET', 'WON',
    'EL', 'LA', 'DE', 'EN', 'ES', 'UN', 'QUE', 'POR', 'CON',
    'LOS', 'LAS', 'DEL', 'AL', 'SU', 'SE', 'NO', 'MAS', 'MUY',
    'YA', 'SOY', 'ERA', 'SER', 'VER', 'DAR', 'HAY', 'SIN',
    'SOL', 'MAR', 'LUZ', 'PAZ', 'REY', 'LEY', 'VOZ', 'VEZ',
    'DOS', 'MIL', 'HOY', 'AUN', 'TAL', 'FIN', 'SUS',
    'UM', 'UMA', 'OS', 'AS', 'NA', 'DO', 'DA', 'AO', 'OU',
    'EU', 'TU', 'ELE', 'ELA', 'NOS', 'VOS', 'JA', 'NAO',
    'SIM', 'MAS', 'COM', 'SEM', 'SOB', 'LUA', 'CHA', 'PAI',
    'MAE', 'AVE', 'REI', 'DEZ', 'CEM', 'ANO', 'DIA',
})

# Colors for UI
COLOR_SOURCE_NEW = QColor("#FEF3C7")
COLOR_SOURCE_DC = QColor("#DCFCE7")
COLOR_SOURCE_TRANSLATED = QColor("#DBEAFE")
COLOR_SOURCE_CACHE = QColor("#E0E7FF")
COLOR_SOURCE_FAILED = QColor("#FECACA")

COLOR_TEXT_FROM_DC = QColor("#166534")
COLOR_TEXT_TRANSLATED = QColor("#1E40AF")
COLOR_TEXT_NEW = QColor("#92400E")
COLOR_TEXT_DEFAULT = QColor("#111827")
COLOR_TEXT_FAILED = QColor("#991B1B")

# Log colors
LOG_BACKGROUND_COLOR = "#0D1117"
LOG_TEXT_COLOR = "#10B981"
LOG_BORDER_COLOR = "#30363D"
LOG_SELECTION_BG = "#1F6FEB"

# Validation pattern - pre-compiled
SUBDOMAIN_PATTERN = re.compile(r'^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]?$')


# ==============================================================================
# GLOBAL ATEXIT REGISTRY - Prevents duplicate handlers
# ==============================================================================

_atexit_registered_caches: Set[int] = set()
_atexit_lock = threading.Lock()


def _register_cache_cleanup(cache_instance: 'PersistentCache'):
    """Register cache cleanup with deduplication."""
    import atexit
    instance_id = id(cache_instance)
    
    with _atexit_lock:
        if instance_id in _atexit_registered_caches:
            return
        _atexit_registered_caches.add(instance_id)
    
    weak_cache = weakref.ref(cache_instance)
    
    def cleanup_handler():
        cache = weak_cache()
        if cache is not None:
            try:
                cache.cleanup()
            except Exception:
                pass
        with _atexit_lock:
            _atexit_registered_caches.discard(instance_id)
    
    atexit.register(cleanup_handler)


# ==============================================================================
# PLATFORM-SPECIFIC FONT CONFIGURATION
# ==============================================================================


def get_system_font_family() -> str:
    """Get the appropriate system font family for the current platform."""
    if sys.platform == 'darwin':
        preferred_fonts = [
            'SF Pro Display', 'SF Pro Text', 'Helvetica Neue',
            'Helvetica', 'Arial'
        ]
    elif sys.platform == 'win32':
        preferred_fonts = [
            'Segoe UI', 'Tahoma', 'Microsoft Sans Serif', 'Arial'
        ]
    else:
        preferred_fonts = [
            'Ubuntu', 'Cantarell', 'DejaVu Sans',
            'Liberation Sans', 'FreeSans', 'Arial'
        ]

    available_fonts = set(QFontDatabase.families())
    for font in preferred_fonts:
        if font in available_fonts:
            return font
    return QApplication.font().family()


def get_monospace_font_family() -> str:
    """Get the appropriate monospace font family for the current platform."""
    if sys.platform == 'darwin':
        preferred_fonts = ['SF Mono', 'Menlo', 'Monaco', 'Courier New']
    elif sys.platform == 'win32':
        preferred_fonts = [
            'Cascadia Code', 'Cascadia Mono', 'Consolas', 'Courier New'
        ]
    else:
        preferred_fonts = [
            'Ubuntu Mono', 'DejaVu Sans Mono', 'Liberation Mono', 'Courier New'
        ]

    available_fonts = set(QFontDatabase.families())
    for font in preferred_fonts:
        if font in available_fonts:
            return font
    return 'monospace'


# ==============================================================================
# STYLESHEET GENERATOR
# ==============================================================================


def generate_stylesheet() -> str:
    """Generate the application stylesheet with platform-appropriate fonts."""
    system_font = get_system_font_family()

    return f"""
/* ============================================= */
/* MAIN WINDOW                                   */
/* ============================================= */

QMainWindow {{
    background-color: #F3F4F6;
}}

QWidget {{
    font-family: "{system_font}";
    font-size: 13px;
    color: #111827;
}}

/* ============================================= */
/* SIDEBAR                                       */
/* ============================================= */

QFrame#Sidebar {{
    background-color: #FFFFFF;
    border-right: 1px solid #D1D5DB;
}}

QPushButton#StepBtn {{
    text-align: left;
    padding: 10px 12px;
    border: 1px solid transparent;
    border-radius: 5px;
    margin: 2px 8px;
    background-color: transparent;
    color: #374151;
    font-weight: 500;
    font-size: 12px;
}}

QPushButton#StepBtn:hover {{
    background-color: #F3F4F6;
    color: #000000;
}}

QPushButton#StepBtn:checked {{
    background-color: #E0E7FF;
    color: #1E40AF;
    font-weight: 700;
    border: 1px solid #C7D2FE;
    border-left: 4px solid #1E40AF;
}}

QPushButton#StepBtn:disabled {{
    color: #9CA3AF;
    background-color: transparent;
}}

/* ============================================= */
/* CARDS                                         */
/* ============================================= */

QFrame#Card {{
    background-color: #FFFFFF;
    border: 1px solid #D1D5DB;
    border-radius: 6px;
}}

QLabel#Title {{
    font-size: 18px;
    font-weight: 700;
    color: #111827;
    padding-bottom: 4px;
}}

QLabel#Subtitle {{
    font-size: 12px;
    color: #6B7280;
    padding-bottom: 8px;
}}

QLabel#SummaryText {{
    font-weight: 600;
    color: #4B5563;
    font-size: 12px;
}}

QLabel#FieldLabel {{
    font-weight: 500;
    color: #374151;
    font-size: 13px;
}}

QLabel#SectionHeader {{
    font-weight: 700;
    color: #1F2937;
    font-size: 14px;
    padding: 8px 0px 4px 0px;
}}

QLabel#LegendText {{
    font-size: 10px;
    color: #6B7280;
    padding: 6px;
    background-color: #F9FAFB;
    border: 1px solid #E5E7EB;
    border-radius: 4px;
}}

/* ============================================= */
/* SECTION FRAME                                 */
/* ============================================= */

QFrame#SectionFrame {{
    background-color: #F9FAFB;
    border: 1px solid #E5E7EB;
    border-radius: 6px;
}}

/* ============================================= */
/* INPUTS                                        */
/* ============================================= */

QLineEdit {{
    padding: 10px 12px;
    border: 1px solid #D1D5DB;
    border-radius: 5px;
    background-color: #FFFFFF;
    color: #000000;
    font-size: 13px;
    min-height: 18px;
}}

QLineEdit:focus {{
    border: 2px solid #2563EB;
    padding: 9px 11px;
}}

QComboBox {{
    padding: 10px 12px;
    border: 1px solid #D1D5DB;
    border-radius: 5px;
    background-color: #FFFFFF;
    color: #000000;
    font-size: 13px;
    min-height: 18px;
}}

QComboBox:focus {{
    border: 2px solid #2563EB;
    padding: 9px 11px;
}}

QComboBox::drop-down {{
    border: none;
    padding-right: 10px;
}}

QComboBox::down-arrow {{
    width: 12px;
    height: 12px;
}}

QComboBox QAbstractItemView {{
    background-color: #FFFFFF;
    border: 1px solid #D1D5DB;
    selection-background-color: #E0E7FF;
    selection-color: #1E40AF;
    padding: 4px;
}}

QComboBox QAbstractItemView::item {{
    padding: 8px 12px;
    min-height: 24px;
}}

QSpinBox {{
    padding: 10px 12px;
    border: 1px solid #D1D5DB;
    border-radius: 5px;
    background-color: #FFFFFF;
    color: #000000;
    font-size: 13px;
    min-height: 18px;
}}

QSpinBox:focus {{
    border: 2px solid #2563EB;
    padding: 9px 11px;
}}

QCheckBox {{
    spacing: 8px;
    font-size: 13px;
    padding: 8px 0px;
}}

QCheckBox::indicator {{
    width: 18px;
    height: 18px;
    border-radius: 3px;
    border: 1px solid #D1D5DB;
    background-color: #FFFFFF;
}}

QCheckBox::indicator:checked {{
    background-color: #2563EB;
    border-color: #2563EB;
}}

QCheckBox::indicator:hover {{
    border-color: #9CA3AF;
}}

/* ============================================= */
/* BUTTONS                                       */
/* ============================================= */

QPushButton#PrimaryBtn {{
    background: #BFDBFE;
    color: #1E3A8A;
    border: 1px solid #60A5FA;
    padding: 10px 20px;
    border-radius: 5px;
    font-weight: 700;
    font-size: 13px;
}}

QPushButton#PrimaryBtn:hover {{
    background: #93C5FD;
    border: 1px solid #3B82F6;
}}

QPushButton#PrimaryBtn:pressed {{
    background: #60A5FA;
}}

QPushButton#DangerBtn {{
    background: #FECACA;
    color: #991B1B;
    border: 1px solid #F87171;
    padding: 10px 20px;
    border-radius: 5px;
    font-weight: 700;
    font-size: 13px;
}}

QPushButton#DangerBtn:hover {{
    background: #FCA5A5;
    border: 1px solid #EF4444;
}}

QPushButton#DangerBtn:disabled {{
    background: #F3F4F6;
    color: #9CA3AF;
    border: 1px solid #D1D5DB;
}}

QPushButton#SecondaryBtn {{
    background: #FFFFFF;
    color: #374151;
    border: 1px solid #D1D5DB;
    padding: 8px 14px;
    border-radius: 5px;
    font-weight: 600;
    font-size: 12px;
}}

QPushButton#SecondaryBtn:hover {{
    background: #F9FAFB;
    border: 1px solid #9CA3AF;
}}

QPushButton#SmallBtn {{
    background: #FFFFFF;
    color: #374151;
    border: 1px solid #D1D5DB;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    padding: 4px 10px;
}}

QPushButton#SmallBtn:hover {{
    background: #F9FAFB;
}}

/* ============================================= */
/* LOG PANEL                                     */
/* ============================================= */

QFrame#LogFrame {{
    background-color: {LOG_BACKGROUND_COLOR};
    border: 1px solid {LOG_BORDER_COLOR};
    border-radius: 6px;
}}

QLabel#LogTitle {{
    font-size: 12px;
    font-weight: 600;
    color: #374151;
}}

/* ============================================= */
/* INFO BOX                                      */
/* ============================================= */

QTextEdit#InfoBox {{
    background-color: #F0FDFA;
    border: 1px solid #99F6E4;
    color: #134E4A;
    font-family: "{system_font}";
    font-size: 12px;
    padding: 8px;
    border-radius: 5px;
}}

/* ============================================= */
/* STATUS BAR                                    */
/* ============================================= */

QFrame#StatusBar {{
    background-color: #F9FAFB;
    border-top: 1px solid #E5E7EB;
}}

QLabel#StatusText {{
    font-weight: 600;
    color: #2563EB;
    font-size: 13px;
}}

QLabel#StatsText {{
    font-weight: 500;
    color: #4B5563;
    font-size: 12px;
}}

QLabel#ElapsedText {{
    font-weight: 600;
    color: #059669;
    font-size: 12px;
}}

/* ============================================= */
/* SUMMARY FRAMES                                */
/* ============================================= */

QFrame#CompactSummary {{
    background-color: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 5px;
}}

QLabel#CompactLabel {{
    font-weight: 600;
    color: #1E40AF;
    font-size: 11px;
    padding: 0 6px;
}}

QFrame#TransSummary {{
    background-color: #FEF3C7;
    border: 1px solid #FCD34D;
    border-radius: 5px;
}}

QLabel#TransSummaryLabel {{
    font-weight: 500;
    color: #92400E;
    font-size: 12px;
}}

QLabel#FilterLabel {{
    font-weight: 600;
    color: #374151;
    font-size: 12px;
}}

/* ============================================= */
/* DIVIDER                                       */
/* ============================================= */

QFrame#Divider {{
    background-color: #E5E7EB;
    max-height: 1px;
    min-height: 1px;
}}

/* ============================================= */
/* GROUP BOX                                     */
/* ============================================= */

QGroupBox {{
    font-weight: 600;
    font-size: 12px;
    color: #374151;
    border: 1px solid #E5E7EB;
    border-radius: 5px;
    margin-top: 14px;
    padding: 16px;
    padding-top: 28px;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 8px;
    background-color: #FFFFFF;
}}

/* ============================================= */
/* TABLE                                         */
/* ============================================= */

QTableWidget {{
    background-color: #FFFFFF;
    gridline-color: #E5E7EB;
    border: 1px solid #D1D5DB;
    border-radius: 4px;
    font-size: 12px;
}}

QTableWidget::item {{
    padding: 4px 6px;
}}

QHeaderView::section {{
    background-color: #F9FAFB;
    border: none;
    border-bottom: 1px solid #E5E7EB;
    border-right: 1px solid #E5E7EB;
    padding: 6px 8px;
    font-weight: 600;
    font-size: 11px;
}}

/* ============================================= */
/* SCROLLBAR                                     */
/* ============================================= */

QScrollBar:vertical {{
    background-color: #F3F4F6;
    width: 10px;
    border-radius: 5px;
}}

QScrollBar::handle:vertical {{
    background-color: #D1D5DB;
    border-radius: 5px;
    min-height: 20px;
}}

QScrollBar::handle:vertical:hover {{
    background-color: #9CA3AF;
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}

QScrollBar:horizontal {{
    background-color: #F3F4F6;
    height: 10px;
    border-radius: 5px;
}}

QScrollBar::handle:horizontal {{
    background-color: #D1D5DB;
    border-radius: 5px;
    min-width: 20px;
}}

QScrollBar::handle:horizontal:hover {{
    background-color: #9CA3AF;
}}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
    width: 0px;
}}
"""


# ==============================================================================
# THREAD-SAFE ATOMIC COUNTER
# ==============================================================================


class AtomicCounter:
    """Thread-safe counter for tracking operations."""
    
    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = threading.Lock()
    
    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value
    
    def decrement(self) -> int:
        with self._lock:
            self._value -= 1
            return self._value
    
    def add(self, amount: int) -> int:
        with self._lock:
            self._value += amount
            return self._value
    
    def reset(self, value: int = 0) -> None:
        with self._lock:
            self._value = value
    
    @property
    def value(self) -> int:
        with self._lock:
            return self._value


# ==============================================================================
# DATA CLASSES
# ==============================================================================


@dataclass
class TranslationResult:
    """Result of a translation operation."""
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

    def __post_init__(self):
        if not self.dc_name:
            raise ValueError("dc_name is required")
        if not self.pt:
            raise ValueError("pt (original text) is required")

    @property
    def is_complete(self) -> bool:
        return bool(self.en and self.es)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id, 'type': self.type, 'context': self.context,
            'dc_name': self.dc_name, 'placeholder': self.placeholder,
            'pt': self.pt, 'en': self.en, 'es': self.es,
            'en_source': self.en_source, 'es_source': self.es_source,
            'pt_source': self.pt_source, 'action': self.action,
            'dc_id': self.dc_id, 'is_option': self.is_option,
            'parent_id': self.parent_id, 'tags': self.tags,
            'force_update': self.force_update, 'source': self.source
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkItem':
        return cls(
            id=data.get('id', 0), type=data.get('type', ''),
            context=data.get('context', 'Unknown'),
            dc_name=data.get('dc_name', ''),
            placeholder=data.get('placeholder', ''),
            pt=data.get('pt', ''), en=data.get('en', ''),
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
            source=data.get('source', SOURCE_NEW)
        )


# ==============================================================================
# APPLICATION STATE MANAGEMENT
# ==============================================================================


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


class StateManager:
    """Thread-safe application state manager."""

    def __init__(self, ui_callback: Optional[Callable[[AppState], None]] = None):
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

    @contextmanager
    def transition(self, new_state: AppState):
        with self._lock:
            if self._state != AppState.IDLE:
                raise RuntimeError(
                    f"Cannot start {new_state.name} while in {self._state.name}"
                )
            self._state = new_state

        if self._ui_callback:
            try:
                self._ui_callback(new_state)
            except Exception as e:
                logger.warning(f"UI callback error: {e}")
        try:
            yield
        finally:
            with self._lock:
                self._state = AppState.IDLE
            if self._ui_callback:
                try:
                    self._ui_callback(AppState.IDLE)
                except Exception as e:
                    logger.warning(f"UI callback error: {e}")

    def force_reset(self):
        with self._lock:
            self._state = AppState.IDLE


# ==============================================================================
# CUSTOM EXCEPTION FOR API ERRORS
# ==============================================================================


class ZendeskAPIError(Exception):
    """Custom exception that includes detailed API error information."""

    def __init__(
        self, message: str, status_code: int = 0,
        error_type: str = "", details: str = ""
    ):
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        self.details = details
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        parts = []
        if self.status_code:
            parts.append(f"HTTP {self.status_code}")
        if self.error_type:
            parts.append(self.error_type)
        if self.message:
            parts.append(self.message)
        if self.details:
            parts.append(f"Details: {self.details}")
        return " | ".join(parts) if parts else "Unknown API Error"


def parse_zendesk_error(response: requests.Response) -> ZendeskAPIError:
    """Parse Zendesk API error response into a detailed exception."""
    status_code = response.status_code
    error_type = ""
    message = ""
    details = ""

    try:
        data = response.json()
        if 'error' in data:
            error_type = data.get('error', '')
            if isinstance(error_type, dict):
                error_type = error_type.get('title', str(error_type))
        if 'description' in data:
            message = data.get('description', '')
        elif 'message' in data:
            message = data.get('message', '')
        if 'details' in data:
            detail_data = data['details']
            if isinstance(detail_data, dict):
                detail_parts = []
                for field_name, errors in detail_data.items():
                    if isinstance(errors, list):
                        for err in errors:
                            if isinstance(err, dict):
                                desc = err.get('description', str(err))
                                detail_parts.append(f"{field_name}: {desc}")
                            else:
                                detail_parts.append(f"{field_name}: {err}")
                    else:
                        detail_parts.append(f"{field_name}: {errors}")
                details = "; ".join(detail_parts)
            else:
                details = str(detail_data)
        if 'errors' in data:
            errors = data['errors']
            if isinstance(errors, list):
                error_msgs = [
                    err.get('message', str(err)) if isinstance(err, dict)
                    else str(err)
                    for err in errors
                ]
                if error_msgs:
                    message = "; ".join(error_msgs)
    except (json.JSONDecodeError, ValueError):
        message = response.text[:200] if response.text else "No response body"

    if not message and not error_type:
        message = f"Request failed with status {status_code}"

    return ZendeskAPIError(
        message=message, status_code=status_code,
        error_type=error_type, details=details
    )


# ==============================================================================
# ACRONYM PROTECTOR - Pre-compiled patterns for performance
# ==============================================================================


class AcronymProtector:
    """Thread-safe acronym protection for translation services."""

    CONTEXT_PREFIX = "The term is: "
    CONTEXT_SUFFIX = "."
    PLACEHOLDER_PREFIX = "ZZPHOLD"

    # Pre-compiled patterns
    PLACEHOLDER_PATTERN = re.compile(
        r'\b[ZS]Z?P?H?O?L?D?\d+[a-f0-9]{4}\b', re.IGNORECASE
    )
    PLACEHOLDER_PATTERN_LOOSE = re.compile(
        r'\b[ZS][xzXZ]?[pqPQ]?[vwVW]?\d+[a-f0-9]{2,6}\b', re.IGNORECASE
    )
    LEGACY_PLACEHOLDER_PATTERN = re.compile(
        r'\b[ZS][xX][qQ][vVwWbB]\d*[a-f0-9]*\b', re.IGNORECASE
    )
    MULTI_SPACE_PATTERN = re.compile(r'\s{2,}')
    
    # Pre-compiled context removal patterns
    CONTEXT_PATTERNS = [
        re.compile(r'^The term is\s*:\s*', re.IGNORECASE),
        re.compile(r'^The term is\s+', re.IGNORECASE),
        re.compile(r'^Term is\s*:\s*', re.IGNORECASE),
        re.compile(r'^Term\s*:\s*', re.IGNORECASE),
        re.compile(r'^O termo [eé]\s*:\s*', re.IGNORECASE),
        re.compile(r'^O termo [eé]\s+', re.IGNORECASE),
        re.compile(r'^Termo [eé]\s*:\s*', re.IGNORECASE),
        re.compile(r'^Termo\s*:\s*', re.IGNORECASE),
        re.compile(r'^El t[eé]rmino es\s*:\s*', re.IGNORECASE),
        re.compile(r'^El t[eé]rmino es\s+', re.IGNORECASE),
        re.compile(r'^T[eé]rmino es\s*:\s*', re.IGNORECASE),
        re.compile(r'^T[eé]rmino\s*:\s*', re.IGNORECASE),
        re.compile(r'^Le terme est\s*:\s*', re.IGNORECASE),
        re.compile(r'^Terme est\s*:\s*', re.IGNORECASE),
        re.compile(r'^Der Begriff ist\s*:\s*', re.IGNORECASE),
        re.compile(r'^Begriff ist\s*:\s*', re.IGNORECASE),
        re.compile(r'^Il termine [eè]\s*:\s*', re.IGNORECASE),
        re.compile(r'^Termine [eè]\s*:\s*', re.IGNORECASE),
        re.compile(r'^[A-Za-zÀ-ÿ]+\s+[A-Za-zÀ-ÿ]+\s*:\s*', re.IGNORECASE),
    ]
    
    # Acronym detection patterns
    ACRONYM_PATTERNS = [
        re.compile(r'\b[A-Z]{2,}[A-Z0-9]*\b'),
        re.compile(r'\b[A-Z0-9]*[A-Z]{2,}[A-Z0-9]*\b'),
        re.compile(r'\b(?:[A-Z]\.){2,}[A-Z]?\.?\b'),
    ]

    @staticmethod
    def should_skip_translation(text: str) -> bool:
        if not text:
            return True
        text = text.strip()
        if not text:
            return True
        if len(text) <= 3:
            return True
        if text.isdigit():
            return True
        if len(text) <= 5 and re.match(r'^[A-Za-z0-9\-_]+$', text):
            if not text.isalpha() or text.isupper():
                return True
        if AcronymProtector.is_likely_acronym(text):
            return True
        return False

    @staticmethod
    def is_likely_acronym(text: str) -> bool:
        if not text:
            return False
        text = text.strip()
        if len(text) < 2 or len(text) > 6:
            return False
        if not re.match(r'^[A-Z][A-Z0-9]+$', text):
            return False
        return True

    @staticmethod
    def _make_safe_placeholder(index: int, acronym: str) -> str:
        hash_val = hashlib.md5(acronym.encode()).hexdigest()[:4]
        return f"{AcronymProtector.PLACEHOLDER_PREFIX}{index}{hash_val}"

    @staticmethod
    def cleanup_placeholders(text: str) -> str:
        if not text:
            return ""
        result = text
        result = AcronymProtector.PLACEHOLDER_PATTERN.sub('', result)
        result = AcronymProtector.PLACEHOLDER_PATTERN_LOOSE.sub('', result)
        result = AcronymProtector.LEGACY_PLACEHOLDER_PATTERN.sub('', result)
        result = AcronymProtector.MULTI_SPACE_PATTERN.sub(' ', result)
        return result.strip()

    @staticmethod
    def has_placeholders(text: str) -> bool:
        if not text:
            return False
        if AcronymProtector.PLACEHOLDER_PATTERN.search(text):
            return True
        if AcronymProtector.PLACEHOLDER_PATTERN_LOOSE.search(text):
            return True
        if AcronymProtector.LEGACY_PLACEHOLDER_PATTERN.search(text):
            return True
        return False

    @staticmethod
    def protect(text: str) -> Tuple[str, Dict[str, str], bool]:
        if not text:
            return "", {}, False
        original_text = text.strip()
        if AcronymProtector.should_skip_translation(original_text):
            return original_text, {"__SKIP__": original_text}, True
        if text.isupper() and len(text.strip()) > 6:
            return text, {}, False

        all_matches: Set[str] = set()
        for pattern in AcronymProtector.ACRONYM_PATTERNS:
            matches = pattern.findall(text)
            all_matches.update(matches)
        all_matches = {m for m in all_matches if m not in COMMON_SHORT_WORDS}
        if not all_matches:
            return text, {}, False

        protected_text = text
        acronym_map: Dict[str, str] = {}
        sorted_matches = sorted(all_matches, key=len, reverse=True)
        for i, acronym in enumerate(sorted_matches):
            placeholder = AcronymProtector._make_safe_placeholder(i, acronym)
            acronym_map[placeholder] = acronym
            protected_text = re.sub(
                r'\b' + re.escape(acronym) + r'\b', placeholder, protected_text
            )
        return protected_text, acronym_map, False

    @staticmethod
    def add_context_padding(text: str) -> str:
        return f"{AcronymProtector.CONTEXT_PREFIX}{text}{AcronymProtector.CONTEXT_SUFFIX}"

    @staticmethod
    def remove_context_padding(text: str, original_text: str = "") -> str:
        if not text:
            return ""
        result = text.strip()
        original_len = len(original_text) if original_text else 0

        for pattern in AcronymProtector.CONTEXT_PATTERNS:
            new_result = pattern.sub('', result)
            if new_result != result:
                result = new_result
                break
        result = result.rstrip('.').strip()

        if original_len > 0 and len(result) > original_len * 3:
            quoted_match = re.search(r'"([^"]+)"', result)
            if quoted_match:
                result = quoted_match.group(1)
            else:
                colon_match = re.search(r':\s*(.+?)\.?$', result)
                if colon_match:
                    result = colon_match.group(1).strip()
        return result

    @staticmethod
    def restore(translated_text: str, acronym_map: Dict[str, str]) -> str:
        if not translated_text:
            return ""
        if not acronym_map:
            return translated_text
        if "__SKIP__" in acronym_map:
            return acronym_map["__SKIP__"]

        result = translated_text
        sorted_items = sorted(
            acronym_map.items(), key=lambda x: len(x[0]), reverse=True
        )
        for placeholder, acronym in sorted_items:
            if placeholder in result:
                result = result.replace(placeholder, acronym)
                continue
            pattern = re.compile(re.escape(placeholder), re.IGNORECASE)
            if pattern.search(result):
                result = pattern.sub(acronym, result)
                continue
            result = AcronymProtector._restore_corrupted(
                result, placeholder, acronym
            )
        return result

    @staticmethod
    def _restore_corrupted(text: str, placeholder: str, acronym: str) -> str:
        substitutions = [
            ('Z', 'S'), ('z', 's'), ('Z', 'C'), ('z', 'c'),
            ('x', 's'), ('x', 'j'), ('q', 'g'), ('q', 'c'),
            ('v', 'b'), ('v', 'w'),
        ]
        for old_char, new_char in substitutions:
            modified = placeholder.replace(old_char, new_char)
            if modified.lower() in text.lower():
                pattern = re.compile(re.escape(modified), re.IGNORECASE)
                return pattern.sub(acronym, text)
        if len(placeholder) >= 6:
            for length in range(len(placeholder), 4, -1):
                partial = placeholder[:length]
                if partial.lower() in text.lower():
                    idx = text.lower().find(partial.lower())
                    if idx != -1:
                        end_idx = idx + len(partial)
                        while end_idx < len(text) and (
                            text[end_idx].isalnum() or text[end_idx] in '._'
                        ):
                            end_idx += 1
                        return text[:idx] + acronym + text[end_idx:]
        return text

    @staticmethod
    def verify_and_fix(
        original: str, translated: str, acronym_map: Dict[str, str]
    ) -> Tuple[str, List[str]]:
        issues: List[str] = []
        if not original or not translated:
            return translated or "", issues
        if acronym_map and "__SKIP__" in acronym_map:
            original_value = acronym_map["__SKIP__"]
            if translated.strip() != original_value:
                issues.append("Short text modified - restoring original")
                return original_value, issues
            return translated, issues
        if not acronym_map:
            return translated, issues

        fixed = translated
        expected_acronyms = {v for k, v in acronym_map.items() if k != "__SKIP__"}
        for acronym in expected_acronyms:
            try:
                if re.search(r'\b' + re.escape(acronym) + r'\b', fixed):
                    continue
            except re.error:
                if acronym in fixed:
                    continue
            issues.append(f"Acronym '{acronym}' was restored")
            placeholder = None
            for ph, acr in acronym_map.items():
                if acr == acronym and ph != "__SKIP__":
                    placeholder = ph
                    break
            if placeholder:
                fixed = AcronymProtector._restore_corrupted(
                    fixed, placeholder, acronym
                )

        if AcronymProtector.has_placeholders(fixed):
            issues.append("Cleaning up remaining placeholder artifacts")
            fixed = AcronymProtector.cleanup_placeholders(fixed)
        return fixed, issues


# ==============================================================================
# PERSISTENT CACHE (SQLITE)
# ==============================================================================


class PersistentCache:
    """Thread-safe SQLite cache for translations with connection pooling."""

    def __init__(self, db_path: str = "translation_cache.db", pool_size: int = 5):
        self.db_path = db_path
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.RLock()
        self._pool_size = pool_size
        self._initialized = False
        self._closed = False
        self._init_pool()
        self._init_db()
        _register_cache_cleanup(self)

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path, timeout=30.0, check_same_thread=False,
            isolation_level=None
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        return conn

    def _init_pool(self):
        for _ in range(self._pool_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except Exception as e:
                logger.error(f"Error creating database connection: {e}")

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        if self._closed:
            raise RuntimeError("Cache has been closed")
        
        conn: Optional[sqlite3.Connection] = None
        from_pool = False
        
        try:
            try:
                conn = self._pool.get(timeout=30.0)
                from_pool = True
            except Empty:
                conn = self._create_connection()
                from_pool = False
            
            yield conn
            
        except Exception:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if from_pool:
                try:
                    new_conn = self._create_connection()
                    self._pool.put_nowait(new_conn)
                except Exception:
                    pass
            raise
        else:
            if conn is not None:
                if from_pool:
                    try:
                        self._pool.put_nowait(conn)
                    except Full:
                        try:
                            conn.close()
                        except Exception:
                            pass
                else:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _init_db(self):
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS translations (
                            id TEXT PRIMARY KEY, original TEXT,
                            target_lang TEXT, translated_text TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_translations_created
                        ON translations(created_at)
                    """)
                    conn.commit()
                    self._initialized = True
            except Exception as e:
                logger.error(f"Cache initialization error: {e}")

    def _generate_id(self, text: str, lang: str) -> str:
        content = f"{text}\x00{lang}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def get_with_age(self, text: str, lang: str) -> Optional[Tuple[str, int]]:
        if not text or not lang:
            return None
        key = self._generate_id(text, lang)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT translated_text, created_at FROM translations "
                    "WHERE id = ?",
                    (key,)
                )
                result = cursor.fetchone()
                if result:
                    trans_text, created_at_str = result
                    try:
                        created_dt = datetime.strptime(
                            created_at_str, "%Y-%m-%d %H:%M:%S"
                        )
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                        now_utc = datetime.now(timezone.utc)
                        delta = now_utc - created_dt
                        age = max(0, delta.days)
                    except Exception:
                        age = 0
                    return trans_text, age
                return None
        except Exception as e:
            logger.error(f"Cache read error: {e}")
            return None

    def get(self, text: str, lang: str) -> Optional[str]:
        result = self.get_with_age(text, lang)
        return result[0] if result else None

    def set(self, text: str, lang: str, translation: str) -> bool:
        if not text or not translation or not lang:
            return False
        key = self._generate_id(text, lang)
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO translations
                    (id, original, target_lang, translated_text, created_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (key, text, lang, translation))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Cache write error: {e}")
            return False

    def clear(self) -> bool:
        with self._lock:
            try:
                self.cleanup()
                if os.path.exists(self.db_path):
                    os.remove(self.db_path)
                wal_path = f"{self.db_path}-wal"
                shm_path = f"{self.db_path}-shm"
                if os.path.exists(wal_path):
                    os.remove(wal_path)
                if os.path.exists(shm_path):
                    os.remove(shm_path)
                self._closed = False
                self._init_pool()
                self._init_db()
                return True
            except Exception as e:
                logger.error(f"Cache clear error: {e}")
                return False

    def cleanup(self):
        with self._lock:
            self._closed = True
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except Exception:
                    pass


# ==============================================================================
# RATE LIMITER
# ==============================================================================


class RateLimiter:
    """Thread-safe rate limiter for API calls."""

    def __init__(self):
        self._lock = threading.RLock()
        self._rate_limit_until: float = 0
        self._rate_limit_count: int = 0

    def handle_rate_limit(
        self, response: requests.Response,
        log_callback: Optional[Callable[[str], None]] = None
    ) -> float:
        """Handle rate limit from response with optional logging."""
        with self._lock:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    wait_time = int(retry_after)
                except ValueError:
                    wait_time = API_CONFIG.RATE_LIMIT_INITIAL_WAIT
            else:
                self._rate_limit_count += 1
                wait_time = min(
                    API_CONFIG.RATE_LIMIT_INITIAL_WAIT * (
                        API_CONFIG.RATE_LIMIT_BACKOFF_FACTOR **
                        self._rate_limit_count
                    ),
                    API_CONFIG.RATE_LIMIT_MAX_WAIT
                )
            self._rate_limit_until = time.time() + wait_time
        if log_callback:
            log_callback(f"[RATE LIMIT] API throttled - waiting {wait_time}s...")
        return wait_time

    def handle_rate_limit_simple(self) -> float:
        """Handle rate limit without response (for retry scenarios).
        
        Returns the wait time in seconds.
        """
        with self._lock:
            self._rate_limit_count += 1
            wait_time = min(
                API_CONFIG.RATE_LIMIT_INITIAL_WAIT * (
                    API_CONFIG.RATE_LIMIT_BACKOFF_FACTOR **
                    self._rate_limit_count
                ),
                API_CONFIG.RATE_LIMIT_MAX_WAIT
            )
            self._rate_limit_until = time.time() + wait_time
        return wait_time

    def wait_if_needed(self) -> float:
        with self._lock:
            if self._rate_limit_until <= time.time():
                return 0
            wait_time = self._rate_limit_until - time.time()
        if wait_time > 0:
            time.sleep(wait_time)
            return wait_time
        return 0

    def decrease_count(self):
        with self._lock:
            self._rate_limit_count = max(0, self._rate_limit_count - 1)

    def reset(self):
        with self._lock:
            self._rate_limit_until = 0
            self._rate_limit_count = 0


# ==============================================================================
# INPUT VALIDATION
# ==============================================================================


def validate_subdomain(subdomain: str) -> str:
    if not subdomain:
        raise ValueError("Subdomain is required")
    cleaned = subdomain.lower().strip()
    for prefix in ("https://", "http://"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
    for suffix in (".zendesk.com", "/"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)]
    cleaned = cleaned.strip("/")
    if not cleaned:
        raise ValueError("Subdomain is required")
    if len(cleaned) > 63:
        raise ValueError("Subdomain too long (max 63 characters)")
    if not SUBDOMAIN_PATTERN.match(cleaned):
        raise ValueError(f"Invalid subdomain format: '{cleaned}'")
    return cleaned


def validate_email(email: str) -> str:
    if not email:
        raise ValueError("Email is required")
    email = email.strip()
    if not email:
        raise ValueError("Email is required")
    if '@' not in email or '.' not in email.split('@')[-1]:
        raise ValueError("Invalid email format")
    return email


def validate_token(token: str) -> str:
    if not token:
        raise ValueError("API Token is required")
    token = token.strip()
    if not token:
        raise ValueError("API Token is required")
    return token


def escape_html(text: str) -> str:
    """Safely escape HTML special characters."""
    if not text:
        return ""
    return html.escape(str(text))


# ==============================================================================
# CONTROLLER
# ==============================================================================


class ZendeskController:
    """Main controller for Zendesk Dynamic Content operations."""

    def __init__(self, log_callback: Optional[Callable[[str], None]] = None):
        self.creds: Dict[str, str] = {}
        self.session: Optional[requests.Session] = None
        self._init_session()
        self._work_items: List[Dict[str, Any]] = []
        self._work_items_lock = threading.RLock()
        self.cache: Optional[PersistentCache] = None
        self.trans_provider = "Google Web (Free)"
        self.trans_api_key = ""
        self.protect_acronyms = True
        self.cache_expiry_days = TRANSLATION_CONFIG.DEFAULT_CACHE_EXPIRY_DAYS
        self.last_execution_results: Dict[str, Any] = {
            'success': [], 'failed': [], 'backup_file': ''
        }
        self._stop_lock = threading.Lock()
        self._stop_requested = False
        self.backup_folder = ""
        self.instance_default_locale = "en-US"
        self.scan_stats: Dict[str, int] = {}
        self._existing_dc_variants: Dict[int, Dict[str, str]] = {}
        self._variant_fetch_lock = threading.Lock()
        self._rate_limiter = RateLimiter()
        self._log_callback = log_callback
        
        # Thread-safe locale map with lock
        self._locale_id_map_lock = threading.RLock()
        self._locale_id_map: Dict[int, str] = self._get_standard_locale_map()
        
        self._translation_failures = AtomicCounter(0)
        self._last_translation_stats: Optional[TranslationStats] = None

    def _get_standard_locale_map(self) -> Dict[int, str]:
        """Initialize with standard Zendesk locale mappings."""
        return {
            1: 'en-US', 2: 'es', 3: 'de', 4: 'fr', 5: 'it', 6: 'nl',
            7: 'pl', 8: 'pt-BR', 9: 'zh-CN', 10: 'ja', 11: 'ko', 12: 'ru',
            13: 'sv', 14: 'no', 15: 'da', 16: 'fi', 17: 'ar', 18: 'he',
            19: 'tr', 20: 'cs', 21: 'hu', 22: 'th', 23: 'id', 24: 'uk',
            25: 'vi', 26: 'pt', 27: 'zh-TW', 28: 'ms', 29: 'ca', 30: 'sk',
            31: 'el', 32: 'bg', 33: 'ro', 34: 'hr', 35: 'sl', 36: 'lt',
            37: 'lv', 38: 'et', 1000: 'en', 1001: 'en-GB', 1002: 'en-AU',
            1003: 'en-CA', 1004: 'es-ES', 1005: 'es-MX', 1006: 'es-419',
            1007: 'fr-CA', 1008: 'fr-FR', 1009: 'de-AT', 1010: 'de-CH',
            1011: 'nl-BE', 1012: 'pt-PT', 1176: 'pt-br',
        }

    def _get_locale_string(self, locale_id: int) -> str:
        """Thread-safe access to locale map."""
        with self._locale_id_map_lock:
            return self._locale_id_map.get(locale_id, '')

    def _set_locale_mapping(self, locale_id: int, locale_str: str):
        """Thread-safe update of locale map."""
        with self._locale_id_map_lock:
            self._locale_id_map[locale_id] = locale_str

    def _log(self, message: str, level: int = logging.INFO):
        logger.log(level, message)
        if self._log_callback:
            try:
                self._log_callback(message)
            except Exception:
                pass

    def _init_session(self):
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ZendeskDCManager/42.0',
            'Content-Type': 'application/json'
        })
        retry_strategy = Retry(
            total=API_CONFIG.RETRY_COUNT,
            backoff_factor=API_CONFIG.RETRY_BASE_DELAY,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=[
                "HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"
            ]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    @property
    def stop_requested(self) -> bool:
        with self._stop_lock:
            return self._stop_requested

    @stop_requested.setter
    def stop_requested(self, value: bool):
        with self._stop_lock:
            self._stop_requested = value

    @property
    def work_items(self) -> List[Dict[str, Any]]:
        with self._work_items_lock:
            return copy.deepcopy(self._work_items)

    @work_items.setter
    def work_items(self, value: List[Dict[str, Any]]):
        with self._work_items_lock:
            self._work_items = copy.deepcopy(value)

    def get_work_items_count(self) -> int:
        """Get count without deep copy for performance."""
        with self._work_items_lock:
            return len(self._work_items)

    def get_work_item(self, index: int) -> Optional[Dict[str, Any]]:
        with self._work_items_lock:
            if 0 <= index < len(self._work_items):
                return copy.deepcopy(self._work_items[index])
            return None

    def update_work_item(self, index: int, updates: Dict[str, Any]):
        with self._work_items_lock:
            if 0 <= index < len(self._work_items):
                self._work_items[index].update(updates)

    def update_work_item_by_match(
        self, match_criteria: Dict[str, Any], updates: Dict[str, Any],
        update_all: bool = False
    ):
        """Update work items matching criteria."""
        with self._work_items_lock:
            for item in self._work_items:
                if all(item.get(k) == v for k, v in match_criteria.items()):
                    item.update(updates)
                    if not update_all:
                        break

    @contextmanager
    def work_items_readonly(self) -> Generator[List[Dict[str, Any]], None, None]:
        """Context manager for read-only access without copying.
        
        Warning: Caller must NOT modify the returned list or its contents.
        """
        with self._work_items_lock:
            yield self._work_items

    def work_items_snapshot(self) -> List[Dict[str, Any]]:
        """Get a shallow copy (faster than deep copy) for iteration."""
        with self._work_items_lock:
            return list(self._work_items)

    def get_work_items_safe(self) -> List[Dict[str, Any]]:
        """Get work items with validation."""
        with self._work_items_lock:
            return [item.copy() for item in self._work_items if item is not None]

    def cleanup(self):
        """Clean up resources. Safe to call multiple times."""
        # Close session first (no lock needed)
        session = self.session
        self.session = None
        if session:
            try:
                session.close()
            except Exception:
                pass
        
        # Clean up cache with timeout to avoid blocking
        cache = self.cache
        self.cache = None
        if cache:
            try:
                cleanup_thread = threading.Thread(
                    target=cache.cleanup, daemon=True
                )
                cleanup_thread.start()
                cleanup_thread.join(timeout=5.0)
            except Exception:
                pass

    def _sanitize(self, text: str) -> str:
        if not text:
            return "unknown"
        t = unicodedata.normalize('NFKD', str(text)).encode(
            'ASCII', 'ignore'
        ).decode('utf-8')
        sanitized = re.sub(r'[^a-zA-Z0-9_]+', '_', t)
        sanitized = sanitized.strip('_').lower()
        if not sanitized:
            return f"item_{hashlib.md5(text.encode()).hexdigest()[:8]}"
        return sanitized

    def _format_elapsed(self, seconds: float) -> str:
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        else:
            return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    def _calc_eta(self, start_time: float, processed: int, total: int) -> str:
        if processed <= 0 or total <= 0:
            return "Calculating..."
        elapsed = time.time() - start_time
        if elapsed <= 0:
            return "..."
        remaining = total - processed
        if remaining <= 0:
            return "Complete"
        rate = processed / elapsed
        if rate <= 0:
            return "..."
        return self._format_elapsed(remaining / rate)

    def _is_dc_string(self, text: str) -> bool:
        if not text:
            return False
        text = text.strip()
        return text.startswith("{{") and text.endswith("}}") and "dc." in text

    def _normalize_locale(self, locale_str: str) -> Optional[str]:
        if not locale_str:
            return None
        locale_lower = locale_str.lower()
        if locale_lower.startswith('en'):
            return 'en'
        elif locale_lower.startswith('es'):
            return 'es'
        elif locale_lower.startswith('pt'):
            return 'pt'
        return None

    def save_profile(
        self, filepath: str, sub: str, email: str, token: str,
        backup_folder: str, api_key: str = "",
        protect_acronyms: bool = True, expiry_days: int = 30
    ) -> bool:
        """Save profile with credentials in plaintext JSON."""
        data = {
            "subdomain": sub,
            "email": email,
            "token": token,
            "backup_path": backup_folder,
            "google_api_key": api_key,
            "protect_acronyms": protect_acronyms,
            "cache_expiry_days": expiry_days
        }
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            return True
        except Exception as e:
            logger.error(f"Error saving profile: {e}")
            return False

    def load_profile(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Load profile from plaintext JSON."""
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if 'rollback_file' in data and 'backup_path' not in data:
                data['backup_path'] = os.path.dirname(data['rollback_file'])
            return data
        except Exception as e:
            logger.error(f"Error loading profile: {e}")
            return None

    def connect(
        self, subdomain: str, email: str, token: str,
        backup_folder: str, log_callback: Optional[pyqtSignal] = None
    ) -> str:
        try:
            clean_sub = validate_subdomain(subdomain)
            clean_email = validate_email(email)
            clean_token = validate_token(token)
        except ValueError as e:
            raise Exception(str(e))

        self.creds = {
            'subdomain': clean_sub, 'email': clean_email, 'token': clean_token
        }
        self.backup_folder = backup_folder

        self._init_session()
        self.session.auth = (f"{clean_email}/token", clean_token)
        self._rate_limiter.reset()

        if self.backup_folder:
            if not os.path.exists(self.backup_folder):
                try:
                    os.makedirs(self.backup_folder)
                except Exception as e:
                    if log_callback:
                        log_callback.emit(
                            f"Warning: Could not create backup folder: {e}"
                        )
                    self.backup_folder = os.getcwd()
        else:
            self.backup_folder = os.getcwd()

        cache_db_path = os.path.join(self.backup_folder, "translation_cache.db")
        if self.cache:
            self.cache.cleanup()
        self.cache = PersistentCache(cache_db_path)

        if log_callback:
            log_callback.emit(f"Cache initialized at: {cache_db_path}")

        target = f"https://{clean_sub}.zendesk.com/api/v2/users/me.json"
        if log_callback:
            log_callback.emit(f"Connecting to: {clean_sub}.zendesk.com")

        try:
            resp = self.session.get(target, timeout=API_CONFIG.TIMEOUT_SHORT)
            if log_callback:
                log_callback.emit(f"Response Code: {resp.status_code}")

            if resp.status_code == 200:
                data = resp.json()
                if 'user' not in data:
                    raise Exception("Auth Failed: Incorrect Credentials")
                name = data['user'].get('name', 'Unknown')
                role = data['user'].get('role', 'unknown')
                if role == 'end-user':
                    raise Exception(
                        "Authentication Failed: Check Your Credentials"
                    )
                if role not in ['admin', 'agent']:
                    raise Exception(f"Auth Failed: Role '{role}' insufficient.")
                if log_callback:
                    log_callback.emit(f"Authenticated: {name} ({role})")
                self._fetch_instance_locale(log_callback)
                self._fetch_locale_mapping(log_callback)
                return f"Connected as {name}"
            elif resp.status_code == 401:
                raise Exception("401 Unauthorized - Check your credentials")
            elif resp.status_code == 403:
                raise Exception("403 Forbidden - Insufficient permissions")
            else:
                raise Exception(f"Connection Failed: HTTP {resp.status_code}")
        except requests.exceptions.RequestException as e:
            if log_callback:
                log_callback.emit(f"Network Error: {str(e)}")
            raise Exception(f"Network Error: {str(e)}")

    def _fetch_instance_locale(self, log_callback: Optional[pyqtSignal] = None):
        try:
            url = (
                f"https://{self.creds['subdomain']}.zendesk.com"
                f"/api/v2/account/settings.json"
            )
            resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_SHORT)
            if resp.status_code == 200:
                settings = resp.json().get('settings', {})
                self.instance_default_locale = settings.get(
                    'localization', {}
                ).get('locale', 'en-US')
                if log_callback:
                    log_callback.emit(
                        f"Instance Locale: {self.instance_default_locale}"
                    )
        except Exception as e:
            if log_callback:
                log_callback.emit(
                    f"Warning: Could not fetch locale settings: {e}"
                )

    def _fetch_locale_mapping(self, log_callback: Optional[pyqtSignal] = None):
        try:
            url = (
                f"https://{self.creds['subdomain']}.zendesk.com"
                f"/api/v2/locales.json"
            )
            resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_SHORT)
            if resp.status_code == 200:
                locales = resp.json().get('locales', [])
                for loc in locales:
                    loc_id = loc.get('id')
                    loc_str = loc.get('locale')
                    if loc_id and loc_str:
                        self._set_locale_mapping(loc_id, loc_str)
        except Exception as e:
            if log_callback:
                log_callback.emit(f"Warning: Could not fetch locales: {e}")

    def stop(self):
        self.stop_requested = True

    def reset_stop(self):
        self.stop_requested = False

    def _clean_variant_content(self, content: str) -> str:
        if not content:
            return ""
        if AcronymProtector.has_placeholders(content):
            return AcronymProtector.cleanup_placeholders(content)
        return content

    def _fetch_dc_variants(self, dc_id: int) -> Dict[str, str]:
        self._rate_limiter.wait_if_needed()
        max_retries = API_CONFIG.RETRY_COUNT
        for attempt in range(max_retries):
            try:
                url = (
                    f"https://{self.creds['subdomain']}.zendesk.com"
                    f"/api/v2/dynamic_content/items/{dc_id}.json"
                )
                resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_DEFAULT)
                if resp.status_code == 200:
                    self._rate_limiter.decrease_count()
                    data = resp.json()
                    item = data.get('item', {})
                    variants = item.get('variants', [])
                    locale_map: Dict[str, str] = {}
                    for variant in variants:
                        locale_id = variant.get('locale_id')
                        content = variant.get('content', '')
                        content = self._clean_variant_content(content)
                        if locale_id is None or not content:
                            continue
                        locale_str = self._get_locale_string(locale_id)
                        if locale_str:
                            normalized = self._normalize_locale(locale_str)
                            if normalized:
                                locale_map[normalized] = content
                    return locale_map
                elif resp.status_code == 429:
                    wait_time = self._rate_limiter.handle_rate_limit(resp)
                    time.sleep(wait_time)
                    continue
                elif resp.status_code == 404:
                    return {}
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    time.sleep(API_CONFIG.RETRY_BASE_DELAY * (attempt + 1))
                    continue
            except Exception as e:
                logger.debug(f"Error fetching DC {dc_id}: {e}")
        return {}

    def _fetch_dc_variants_batch(
        self, dc_ids: List[int], progress_callback: pyqtSignal,
        log_callback: pyqtSignal
    ) -> Dict[int, Dict[str, str]]:
        total = len(dc_ids)
        if total == 0:
            return {}
        results: Dict[int, Dict[str, str]] = {}
        results_lock = threading.Lock()
        processed = AtomicCounter(0)
        errors = AtomicCounter(0)
        empty_responses = AtomicCounter(0)
        start_time = time.time()
        max_workers = min(API_CONFIG.THREAD_POOL_SIZE_VARIANTS, total)
        log_callback.emit(
            f"Loading {total} DC variants ({max_workers} parallel)..."
        )
        batch_size = max_workers * 2

        for batch_start in range(0, total, batch_size):
            if self.stop_requested:
                raise Exception("Operation Canceled")
            batch_end = min(batch_start + batch_size, total)
            batch_ids = dc_ids[batch_start:batch_end]

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_dc_id = {
                    executor.submit(self._fetch_dc_variants, dc_id): dc_id
                    for dc_id in batch_ids
                }
                
                try:
                    for future in concurrent.futures.as_completed(
                        future_to_dc_id
                    ):
                        if self.stop_requested:
                            raise Exception("Operation Canceled")
                        
                        dc_id = future_to_dc_id[future]
                        current_processed = processed.increment()
                        try:
                            variants = future.result()
                            if variants:
                                with results_lock:
                                    results[dc_id] = variants
                            else:
                                empty_responses.increment()
                        except concurrent.futures.CancelledError:
                            pass
                        except Exception:
                            errors.increment()

                        eta = self._calc_eta(
                            start_time, current_processed, total
                        )
                        with results_lock:
                            found_count = len(results)
                        progress_callback.emit(
                            current_processed, total,
                            f"Loading: {current_processed}/{total} | "
                            f"Found: {found_count} | ETA: {eta}"
                        )
                except Exception as e:
                    for f in future_to_dc_id.keys():
                        f.cancel()
                    raise
            
            if batch_end < total:
                time.sleep(0.1)

        elapsed_str = self._format_elapsed(time.time() - start_time)
        with results_lock:
            final_count = len(results)
        log_callback.emit(
            f"Variant loading: {final_count} found, "
            f"{empty_responses.value} empty, "
            f"{errors.value} errors, {elapsed_str}"
        )
        return results

    def scan_and_analyze(
        self, progress_callback: pyqtSignal, log_callback: pyqtSignal,
        scan_config: Dict[str, bool]
    ) -> Dict[str, int]:
        if not self.creds:
            raise Exception("Credentials not found - please connect first")

        progress_callback.emit(0, 0, "Initializing...")
        self.reset_stop()
        self._rate_limiter.reset()
        with self._variant_fetch_lock:
            self._existing_dc_variants = {}

        log_callback.emit("Fetching existing Dynamic Content...")
        existing_dc: Dict[str, int] = {}
        existing_dc_set: Set[str] = set()
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/dynamic_content/items.json"
        )
        page_counter = 0
        seen_urls: Set[str] = set()

        while url and page_counter < API_CONFIG.MAX_PAGINATION_PAGES:
            if self.stop_requested:
                raise Exception("Operation Canceled")
            if url in seen_urls:
                log_callback.emit("Warning: Pagination loop detected, stopping.")
                break
            seen_urls.add(url)
            
            try:
                resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_LONG)
                if resp.status_code == 429:
                    wait_time = self._rate_limiter.handle_rate_limit(
                        resp, lambda m: log_callback.emit(m)
                    )
                    time.sleep(wait_time)
                    seen_urls.discard(url)
                    continue
                if resp.status_code != 200:
                    break
                data = resp.json()
                for item in data.get('items', []):
                    item_name = item.get('name', '')
                    if item_name and item_name not in existing_dc_set:
                        existing_dc[item_name] = item['id']
                        existing_dc_set.add(item_name)
                page_counter += 1
                progress_callback.emit(
                    0, 0,
                    f"DC list: Page {page_counter} ({len(existing_dc)} items)"
                )
                url = data.get('next_page')
            except Exception as e:
                log_callback.emit(f"Warning: Error fetching DC: {e}")
                break

        if page_counter >= API_CONFIG.MAX_PAGINATION_PAGES:
            log_callback.emit(
                f"Warning: Reached max pagination limit "
                f"({API_CONFIG.MAX_PAGINATION_PAGES})"
            )

        log_callback.emit(f"Found {len(existing_dc)} existing DC items")
        log_callback.emit("Scanning Zendesk objects...")

        raw_items: List[Dict[str, Any]] = []
        self.scan_stats = {
            'valid_fields': 0, 'valid_forms': 0, 'valid_cats': 0,
            'valid_sects': 0, 'valid_arts': 0, 'ignored': 0, 'already_dc': 0
        }

        tasks: List[Callable] = []
        if scan_config.get('fields'):
            tasks.append(self._scan_fields)
        if scan_config.get('forms'):
            tasks.append(self._scan_forms)
        if scan_config.get('cats'):
            tasks.append(self._scan_categories)
        if scan_config.get('sects'):
            tasks.append(self._scan_sections)
        if scan_config.get('arts'):
            tasks.append(self._scan_articles)

        if not tasks:
            raise Exception("No scan tasks selected")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=API_CONFIG.THREAD_POOL_SIZE
        ) as executor:
            futures = {
                executor.submit(task, log_callback): task.__name__
                for task in tasks
            }
            try:
                for future in concurrent.futures.as_completed(futures):
                    if self.stop_requested:
                        raise Exception("Scan Canceled")
                    task_name = futures[future]
                    try:
                        result_items = future.result()
                        raw_items.extend(result_items)
                        progress_callback.emit(
                            0, 0, f"Found {len(raw_items)} items..."
                        )
                    except concurrent.futures.CancelledError:
                        pass
                    except Exception as e:
                        log_callback.emit(
                            f"Scan Error in {task_name}: {str(e)}"
                        )
            except Exception as e:
                for f in futures.keys():
                    f.cancel()
                raise

        log_callback.emit(f"Analyzing {len(raw_items)} items...")
        progress_callback.emit(0, 0, "Building tasks...")

        new_work_items: List[Dict[str, Any]] = []
        seen_dc_names: Set[str] = set()
        dc_ids_to_fetch: List[int] = []
        dc_ids_to_fetch_set: Set[int] = set()
        dc_id_to_items: Dict[int, List[Dict[str, Any]]] = {}

        for item in raw_items:
            if item['type'] == 'field':
                self.scan_stats['valid_fields'] += 1
            elif item['type'] == 'form':
                self.scan_stats['valid_forms'] += 1
            elif item['type'] == 'category':
                self.scan_stats['valid_cats'] += 1
            elif item['type'] == 'section':
                self.scan_stats['valid_sects'] += 1
            elif item['type'] == 'article':
                self.scan_stats['valid_arts'] += 1

            if not item['is_parent_dc']:
                action = "CREATE"
                dc_id = None
                source = SOURCE_NEW
                if item['dc_name'] in existing_dc:
                    action = "LINK"
                    dc_id = existing_dc[item['dc_name']]
                    source = SOURCE_ZENDESK_DC
                    if dc_id not in dc_ids_to_fetch_set:
                        dc_ids_to_fetch.append(dc_id)
                        dc_ids_to_fetch_set.add(dc_id)
                elif item['dc_name'] in seen_dc_names:
                    action = "LINK"
                else:
                    seen_dc_names.add(item['dc_name'])

                work_item = {
                    'id': item['id'], 'type': item['type'],
                    'context': item['context'],
                    'dc_name': item['dc_name'],
                    'placeholder': f"{{{{dc.{item['dc_name']}}}}}",
                    'pt': item['title'], 'en': "", 'es': "",
                    'en_source': SOURCE_NEW, 'es_source': SOURCE_NEW,
                    'pt_source': source,
                    'action': action, 'dc_id': dc_id, 'is_option': False,
                    'parent_id': None,
                    'tags': ",".join(item['tags']) if item['tags'] else "",
                    'force_update': False, 'source': source
                }
                new_work_items.append(work_item)
                if dc_id:
                    if dc_id not in dc_id_to_items:
                        dc_id_to_items[dc_id] = []
                    dc_id_to_items[dc_id].append(work_item)

            for opt in item['options']:
                if self._is_dc_string(opt.get('name', '')):
                    self.scan_stats['already_dc'] += 1
                    continue
                complex_dc_key = f"{item['dc_name']}::{opt['name']}"
                sanitized_key = self._sanitize(complex_dc_key)
                opt_action = "CREATE"
                opt_dc_id = None
                opt_source = SOURCE_NEW
                if sanitized_key in existing_dc:
                    opt_action = "LINK"
                    opt_dc_id = existing_dc[sanitized_key]
                    opt_source = SOURCE_ZENDESK_DC
                    if opt_dc_id not in dc_ids_to_fetch_set:
                        dc_ids_to_fetch.append(opt_dc_id)
                        dc_ids_to_fetch_set.add(opt_dc_id)
                elif sanitized_key in seen_dc_names:
                    opt_action = "LINK"
                else:
                    seen_dc_names.add(sanitized_key)

                opt_work_item = {
                    'id': opt.get('id'), 'type': 'option',
                    'context': 'Ticket',
                    'dc_name': sanitized_key,
                    'placeholder': f"{{{{dc.{sanitized_key}}}}}",
                    'pt': opt['name'], 'en': "", 'es': "",
                    'en_source': SOURCE_NEW, 'es_source': SOURCE_NEW,
                    'pt_source': opt_source,
                    'action': opt_action, 'dc_id': opt_dc_id,
                    'is_option': True,
                    'parent_id': item['id'], 'tags': opt.get('value', ''),
                    'force_update': False, 'source': opt_source
                }
                new_work_items.append(opt_work_item)
                if opt_dc_id:
                    if opt_dc_id not in dc_id_to_items:
                        dc_id_to_items[opt_dc_id] = []
                    dc_id_to_items[opt_dc_id].append(opt_work_item)

        if dc_ids_to_fetch:
            log_callback.emit(
                f"Loading {len(dc_ids_to_fetch)} DC translations..."
            )
            try:
                fetched_variants = self._fetch_dc_variants_batch(
                    dc_ids_to_fetch, progress_callback, log_callback
                )
                with self._variant_fetch_lock:
                    self._existing_dc_variants = fetched_variants

                updated_count = 0
                for dc_id, variants in fetched_variants.items():
                    if dc_id in dc_id_to_items:
                        en_val = variants.get('en', '')
                        es_val = variants.get('es', '')
                        pt_val = variants.get('pt', '')
                        for work_item in dc_id_to_items[dc_id]:
                            if en_val:
                                work_item['en'] = en_val
                                work_item['en_source'] = SOURCE_ZENDESK_DC
                            if es_val:
                                work_item['es'] = es_val
                                work_item['es_source'] = SOURCE_ZENDESK_DC
                            if pt_val:
                                work_item['existing_pt'] = pt_val
                                work_item['pt_source'] = SOURCE_ZENDESK_DC
                            updated_count += 1
                log_callback.emit(f"Applied DC data to {updated_count} items")
            except Exception as e:
                if "Canceled" in str(e):
                    raise
                log_callback.emit(f"Warning: Error fetching variants: {e}")

        self.work_items = new_work_items
        progress_callback.emit(0, 0, "Scan complete!")
        return self.scan_stats

    def _paginate_api(
        self, base_url: str, items_key: str, log_callback: pyqtSignal
    ) -> Generator[Dict[str, Any], None, None]:
        """Generic pagination helper with safety limits."""
        url = base_url
        page_counter = 0
        seen_urls: Set[str] = set()
        
        while url and page_counter < API_CONFIG.MAX_PAGINATION_PAGES:
            if self.stop_requested:
                return
            if url in seen_urls:
                log_callback.emit("Warning: Pagination loop detected")
                return
            seen_urls.add(url)
            
            try:
                resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_DEFAULT)
                if resp.status_code == 429:
                    wait_time = self._rate_limiter.handle_rate_limit(
                        resp, lambda m: log_callback.emit(m)
                    )
                    time.sleep(wait_time)
                    seen_urls.discard(url)
                    continue
                if resp.status_code != 200:
                    log_callback.emit(
                        f"API returned status {resp.status_code}"
                    )
                    return
                
                try:
                    data = resp.json()
                except json.JSONDecodeError as e:
                    log_callback.emit(f"Invalid JSON response: {e}")
                    return
                    
                for item in data.get(items_key, []):
                    yield item
                page_counter += 1
                url = data.get('next_page')
                
            except requests.exceptions.Timeout:
                log_callback.emit(f"Timeout fetching {url}")
                return
            except requests.exceptions.RequestException as e:
                log_callback.emit(f"Request error: {e}")
                return
            except Exception as e:
                log_callback.emit(f"Pagination error: {e}")
                return

    def _process_generic_obj(
        self, obj: Dict[str, Any], obj_type: str,
        tags: Optional[List[str]] = None,
        options: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Dict[str, Any]]:
        if obj is None:
            return None
            
        tags = tags or []
        options = options or []
        title = ""
        context = "Unknown"

        # Validate ID exists
        obj_id = obj.get('id')
        if obj_id is None:
            return None

        if obj_type == 'field':
            title = obj.get('title', '')
            context = "Ticket"
        elif obj_type == 'form':
            title = obj.get('display_name') or obj.get('name', '')
            context = "Ticket"
        elif obj_type in ['category', 'section', 'article']:
            title = obj.get('name') or obj.get('title', '')
            context = "Help Center"

        if not title:
            return None
        title = title.strip()
        if not title:
            return None
        if obj_type in ['field', 'form'] and not obj.get('active'):
            return None
        if obj_type == 'field':
            if obj.get('type') in SYSTEM_FIELD_IDENTIFIERS:
                return None
            if title.lower() in SYSTEM_FIELD_NAMES:
                return None
            if not obj.get('removable'):
                return None

        dc_name = self._sanitize(title)
        if obj_type == 'form':
            dc_name = f"form_{dc_name}"
        elif obj_type == 'category':
            dc_name = f"hc_cat_{dc_name}"
        elif obj_type == 'section':
            dc_name = f"hc_sec_{dc_name}"
        elif obj_type == 'article':
            dc_name = f"hc_art_{dc_name}"

        is_parent_dc = self._is_dc_string(title)
        pending_opts = [
            o for o in options if not self._is_dc_string(o.get('name', ''))
        ]
        if is_parent_dc and len(pending_opts) == 0:
            return None

        return {
            'id': obj_id, 'type': obj_type, 'title': title,
            'dc_name': dc_name, 'is_parent_dc': is_parent_dc,
            'tags': tags, 'options': options, 'context': context
        }

    def _scan_fields(self, log_cb: pyqtSignal) -> List[Dict[str, Any]]:
        log_cb.emit("Scanning Ticket Fields...")
        results = []
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/ticket_fields.json"
        )
        
        for field in self._paginate_api(url, 'ticket_fields', log_cb):
            item = self._process_generic_obj(
                field, 'field', field.get('tags', []),
                field.get('custom_field_options', [])
            )
            if item:
                results.append(item)
        
        log_cb.emit(f"  Found {len(results)} fields")
        return results

    def _scan_forms(self, log_cb: pyqtSignal) -> List[Dict[str, Any]]:
        log_cb.emit("Scanning Ticket Forms...")
        results = []
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/ticket_forms.json"
        )
        
        for form in self._paginate_api(url, 'ticket_forms', log_cb):
            item = self._process_generic_obj(form, 'form')
            if item:
                results.append(item)
        
        log_cb.emit(f"  Found {len(results)} forms")
        return results

    def _scan_categories(self, log_cb: pyqtSignal) -> List[Dict[str, Any]]:
        log_cb.emit("Scanning Help Center Categories...")
        results = []
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/categories.json"
        )
        
        for cat in self._paginate_api(url, 'categories', log_cb):
            item = self._process_generic_obj(cat, 'category')
            if item:
                results.append(item)
        
        log_cb.emit(f"  Found {len(results)} categories")
        return results

    def _scan_sections(self, log_cb: pyqtSignal) -> List[Dict[str, Any]]:
        log_cb.emit("Scanning Help Center Sections...")
        results = []
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/sections.json"
        )
        
        for sect in self._paginate_api(url, 'sections', log_cb):
            item = self._process_generic_obj(sect, 'section')
            if item:
                results.append(item)
        
        log_cb.emit(f"  Found {len(results)} sections")
        return results

    def _scan_articles(self, log_cb: pyqtSignal) -> List[Dict[str, Any]]:
        log_cb.emit("Scanning Help Center Articles...")
        results = []
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/articles.json"
        )
        
        for art in self._paginate_api(url, 'articles', log_cb):
            item = self._process_generic_obj(art, 'article')
            if item:
                results.append(item)
        
        log_cb.emit(f"  Found {len(results)} articles")
        return results

    def set_translation_config(
        self, provider: str, api_key: str, protect_acronyms: bool,
        expiry_days: int
    ):
        self.trans_provider = provider
        self.trans_api_key = api_key.strip() if api_key else ""
        self.protect_acronyms = protect_acronyms
        self.cache_expiry_days = max(1, expiry_days)

    def get_translation_summary(self) -> Dict[str, int]:
        with self._work_items_lock:
            work_items = self._work_items
            total = len(work_items)
            missing_en = sum(1 for i in work_items if not i.get('en'))
            missing_es = sum(1 for i in work_items if not i.get('es'))
            missing_both = sum(
                1 for i in work_items
                if not i.get('en') and not i.get('es')
            )
            has_existing = sum(
                1 for i in work_items
                if i.get('en') and i.get('es')
            )
            from_dc = sum(
                1 for i in work_items
                if i.get('source') == SOURCE_ZENDESK_DC
            )
            needs_translation = sum(
                1 for i in work_items
                if not i.get('en') or not i.get('es')
            )
            failed = sum(
                1 for i in work_items
                if i.get('en_source') == SOURCE_FAILED
                or i.get('es_source') == SOURCE_FAILED
            )
        
        return {
            'total': total, 'missing_en': missing_en, 'missing_es': missing_es,
            'missing_both': missing_both, 'has_existing': has_existing,
            'from_dc': from_dc, 'needs_translation': needs_translation,
            'failed': failed
        }

    def perform_translation(
        self, progress_callback: pyqtSignal, log_callback: pyqtSignal,
        force_retranslate: bool = False
    ) -> TranslationStats:
        """Perform translation and return statistics."""
        self.reset_stop()
        self._rate_limiter.reset()
        self._translation_failures.reset()
        
        stats = TranslationStats()
        
        if "Google Cloud" in self.trans_provider and not self.trans_api_key:
            raise Exception("Missing Google Cloud API Key.")

        work_items_snapshot = self.work_items
        if force_retranslate:
            to_translate = work_items_snapshot
            log_callback.emit("[FORCE] Retranslating ALL items")
        else:
            to_translate = [
                i for i in work_items_snapshot
                if not i.get('en') or not i.get('es')
            ]

        stats.total = len(to_translate)
        log_callback.emit(f"Translating {stats.total} items...")
        
        if stats.total == 0:
            log_callback.emit("No items need translation.")
            self._last_translation_stats = stats
            return stats

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=API_CONFIG.THREAD_POOL_SIZE
        ) as executor:
            future_map = {
                executor.submit(
                    self._fetch_trans, item, log_callback, force_retranslate
                ): idx
                for idx, item in enumerate(to_translate)
            }
            count = 0
            
            try:
                for future in concurrent.futures.as_completed(future_map):
                    if self.stop_requested:
                        raise Exception("Operation Canceled")
                    
                    idx = future_map[future]
                    item = to_translate[idx]
                    count += 1
                    
                    try:
                        result = future.result()
                        match_criteria = {
                            'id': item['id'], 'type': item['type'],
                            'dc_name': item['dc_name']
                        }
                        updates = {
                            'en': result.en, 'es': result.es,
                            'en_source': result.en_source,
                            'es_source': result.es_source
                        }
                        self.update_work_item_by_match(match_criteria, updates)
                        
                        # Update stats
                        if result.en_failed or result.es_failed:
                            stats.failed += 1
                        else:
                            stats.translated += 1
                        
                        eta = self._calc_eta(start_time, count, stats.total)
                        progress_callback.emit(
                            count, stats.total,
                            f"Translated: {count}/{stats.total} | ETA: {eta}"
                        )
                    except concurrent.futures.CancelledError:
                        pass
                    except Exception as e:
                        stats.failed += 1
                        log_callback.emit(
                            f"Translation Error for "
                            f"'{item.get('dc_name', 'unknown')}': {e}"
                        )
            except Exception as e:
                for f in future_map.keys():
                    f.cancel()
                raise
        
        failures = self._translation_failures.value
        if failures > 0:
            log_callback.emit(
                f"Warning: {failures} translation(s) failed and used fallback"
            )
        
        self._last_translation_stats = stats
        return stats

    def _fetch_trans(
        self, item: Dict[str, Any], log_callback: pyqtSignal,
        force_retranslate: bool = False
    ) -> TranslationResult:
        text = item.get('pt', '')
        if not text:
            return TranslationResult()

        original_text = text.strip()
        
        # Always preserve existing translations as fallback
        existing_en = item.get('en', '')
        existing_es = item.get('es', '')
        existing_en_source = item.get('en_source', SOURCE_NEW)
        existing_es_source = item.get('es_source', SOURCE_NEW)
        
        # Determine if we need to translate
        need_en = force_retranslate or not existing_en
        need_es = force_retranslate or not existing_es

        result = TranslationResult()

        if self.protect_acronyms and AcronymProtector.should_skip_translation(
            original_text
        ):
            result.en = original_text
            result.es = original_text
            result.en_source = SOURCE_NEW
            result.es_source = SOURCE_NEW
            return result

        if self.protect_acronyms:
            protected_text, acr_map, should_skip = AcronymProtector.protect(
                text
            )
            if should_skip and "__SKIP__" in acr_map:
                result.en = original_text
                result.es = original_text
                result.en_source = SOURCE_NEW
                result.es_source = SOURCE_NEW
                return result
        else:
            protected_text = text
            acr_map = {}

        text_length = len(protected_text.strip())
        needs_padding = (
            TRANSLATION_CONFIG.MIN_TEXT_FOR_PADDING_LOWER < text_length <
            TRANSLATION_CONFIG.MIN_TEXT_FOR_PADDING
        )
        if needs_padding:
            padded_text = AcronymProtector.add_context_padding(protected_text)
        else:
            padded_text = protected_text

        # Translate English
        en_failed = False
        if need_en:
            en_raw, en_failed = self._trans_with_status(
                padded_text, 'en', original_text
            )
            if en_failed and existing_en:
                # Fall back to existing translation on failure
                en_raw = existing_en
                en_source = existing_en_source
                en_failed = False
            else:
                en_source = SOURCE_FAILED if en_failed else SOURCE_TRANSLATED
        else:
            en_raw = existing_en
            en_source = existing_en_source

        # Translate Spanish
        es_failed = False
        if need_es:
            es_raw, es_failed = self._trans_with_status(
                padded_text, 'es', original_text
            )
            if es_failed and existing_es:
                # Fall back to existing translation on failure
                es_raw = existing_es
                es_source = existing_es_source
                es_failed = False
            else:
                es_source = SOURCE_FAILED if es_failed else SOURCE_TRANSLATED
        else:
            es_raw = existing_es
            es_source = existing_es_source

        if needs_padding:
            if en_source == SOURCE_TRANSLATED:
                en_raw = AcronymProtector.remove_context_padding(
                    en_raw, original_text
                )
            if es_source == SOURCE_TRANSLATED:
                es_raw = AcronymProtector.remove_context_padding(
                    es_raw, original_text
                )

        if self.protect_acronyms and acr_map and "__SKIP__" not in acr_map:
            if en_source in (SOURCE_TRANSLATED, SOURCE_FAILED):
                en_restored = AcronymProtector.restore(en_raw, acr_map)
            else:
                en_restored = en_raw
            if es_source in (SOURCE_TRANSLATED, SOURCE_FAILED):
                es_restored = AcronymProtector.restore(es_raw, acr_map)
            else:
                es_restored = es_raw
            en_final, _ = AcronymProtector.verify_and_fix(
                original_text, en_restored, acr_map
            )
            es_final, _ = AcronymProtector.verify_and_fix(
                original_text, es_restored, acr_map
            )
            if AcronymProtector.has_placeholders(en_final):
                en_final = (
                    AcronymProtector.cleanup_placeholders(en_final)
                    or original_text
                )
            if AcronymProtector.has_placeholders(es_final):
                es_final = (
                    AcronymProtector.cleanup_placeholders(es_final)
                    or original_text
                )
            
            result.en = en_final
            result.es = es_final
            result.en_source = en_source
            result.es_source = es_source
            result.en_failed = en_failed
            result.es_failed = es_failed
            return result

        if not en_raw:
            en_raw = existing_en if existing_en else original_text
            en_failed = not existing_en
            en_source = existing_en_source if existing_en else SOURCE_FAILED
        if not es_raw:
            es_raw = existing_es if existing_es else original_text
            es_failed = not existing_es
            es_source = existing_es_source if existing_es else SOURCE_FAILED
            
        if AcronymProtector.has_placeholders(en_raw):
            en_raw = AcronymProtector.cleanup_placeholders(en_raw)
        if AcronymProtector.has_placeholders(es_raw):
            es_raw = AcronymProtector.cleanup_placeholders(es_raw)
        
        result.en = en_raw
        result.es = es_raw
        result.en_source = en_source
        result.es_source = es_source
        result.en_failed = en_failed
        result.es_failed = es_failed
        return result

    def _trans_with_status(
        self, text: str, target: str, original_text: str
    ) -> Tuple[str, bool]:
        """Translate text and return (result, failed_flag)."""
        if not text:
            return "", True
        
        cache_key = original_text or text

        if self.cache:
            cached_result = self.cache.get_with_age(cache_key, target)
            if cached_result:
                trans_text, age_days = cached_result
                if (not AcronymProtector.has_placeholders(trans_text)
                        and age_days < self.cache_expiry_days):
                    return trans_text, False

        translation = ""
        failed = False
        
        try:
            if self.trans_provider == "Google Cloud Translation API":
                translation = self._translate_google_cloud(text, target)
            else:
                translation = self._translate_google_web(text, target)
        except Exception as e:
            logger.warning(
                f"Translation failed for '{original_text[:30]}...' "
                f"to {target}: {e}"
            )
            failed = True

        if not translation:
            failed = True
            self._translation_failures.increment()
            translation = original_text or text

        if translation and self.cache and not failed:
            if not AcronymProtector.has_placeholders(translation):
                self.cache.set(cache_key, target, translation)

        return translation, failed

    def _translate_google_cloud(self, text: str, target: str) -> str:
        if not self.trans_api_key:
            raise Exception("API Key is missing.")
        url = "https://translation.googleapis.com/language/translate/v2"
        params = {
            "q": text, "target": target,
            "key": self.trans_api_key, "format": "text"
        }
        resp = self.session.post(
            url, params=params, timeout=API_CONFIG.TIMEOUT_SHORT
        )
        if resp.status_code == 200:
            data = resp.json()
            try:
                res = data['data']['translations'][0]['translatedText']
                return html.unescape(res)
            except (KeyError, IndexError) as e:
                raise Exception(f"Unexpected API response: {e}")
        else:
            raise Exception(
                f"Google API Error ({resp.status_code}): {resp.text[:200]}"
            )

    def _translate_google_web(self, text: str, target: str) -> str:
        last_error = None
        for attempt in range(API_CONFIG.RETRY_COUNT):
            time.sleep(
                random.uniform(
                    TRANSLATION_CONFIG.DELAY_MIN,
                    TRANSLATION_CONFIG.DELAY_MAX
                )
            )
            params = {
                "client": "gtx", "sl": "auto",
                "tl": target, "dt": "t", "q": text
            }
            try:
                resp = requests.get(
                    "https://translate.googleapis.com/translate_a/single",
                    params=params, timeout=10
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data and data[0]:
                        return "".join(
                            [x[0] for x in data[0] if x and x[0]]
                        )
                if resp.status_code == 429:
                    last_error = "Rate limited"
                    time.sleep(2 * (attempt + 1))
                else:
                    last_error = f"HTTP {resp.status_code}"
            except requests.exceptions.Timeout:
                last_error = "Timeout"
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                last_error = str(e)
                time.sleep(1)
        
        if last_error:
            logger.warning(
                f"Google Web translation failed after "
                f"{API_CONFIG.RETRY_COUNT} attempts: {last_error}"
            )
        return ""

    def clear_cache(self) -> bool:
        return self.cache.clear() if self.cache else True

    def generate_backup_file(
        self, items_to_process: List[Dict[str, Any]], log_callback: pyqtSignal
    ) -> Optional[str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.json"
        backup_dir = self.backup_folder
        if (not backup_dir or not os.path.exists(backup_dir)
                or not os.access(backup_dir, os.W_OK)):
            backup_dir = os.path.expanduser("~/Documents")
            if not os.path.exists(backup_dir):
                backup_dir = os.getcwd()
        full_path = os.path.join(backup_dir, backup_filename)
        backup_data = {
            "timestamp": timestamp,
            "subdomain": self.creds.get('subdomain', 'unknown'),
            "items": []
        }
        for item in items_to_process:
            backup_data["items"].append({
                "id": item.get('id'), "type": item.get('type', ''),
                "context": item.get('context', 'Unknown'),
                "original_text": item.get('pt', ''),
                "en": item.get('en', ''),
                "es": item.get('es', ''),
                "placeholder": item.get('placeholder', ''),
                "parent_id": item.get('parent_id'),
                "dc_name": item.get('dc_name', '')
            })
        try:
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=4, ensure_ascii=False)
            log_callback.emit(f"Backup created: {full_path}")
            return os.path.basename(full_path)
        except Exception as e:
            log_callback.emit(f"Failed to create backup: {e}")
            return None

    def execute_changes(
        self, items_to_process: List[Dict[str, Any]],
        progress_callback: pyqtSignal, log_callback: pyqtSignal
    ) -> Dict[str, Any]:
        self.reset_stop()
        self._rate_limiter.reset()
        log_callback.emit("Creating Backup...")
        created_backup = self.generate_backup_file(
            items_to_process, log_callback
        )
        self.last_execution_results = {
            'success': [], 'failed': [], 'backup_file': created_backup or ''
        }

        if not items_to_process:
            log_callback.emit("No items to process")
            return self.last_execution_results

        log_callback.emit(f"Processing {len(items_to_process)} items...")
        start_time = time.time()
        l_map = self._fetch_locale_map_for_apply(log_callback)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=API_CONFIG.THREAD_POOL_SIZE
        ) as executor:
            futures = {
                executor.submit(
                    self._apply_single_with_retry, item, l_map, log_callback
                ): item
                for item in items_to_process
            }
            
            try:
                for i, future in enumerate(
                    concurrent.futures.as_completed(futures)
                ):
                    if self.stop_requested:
                        raise Exception("Operation Canceled")
                    
                    item = futures[future]
                    try:
                        res = future.result()
                        log_callback.emit(res)
                        self.last_execution_results['success'].append(res)
                        eta = self._calc_eta(
                            start_time, i + 1, len(items_to_process)
                        )
                        progress_callback.emit(
                            i + 1, len(items_to_process),
                            f"Applied: {i + 1}/{len(items_to_process)} | "
                            f"ETA: {eta}"
                        )
                    except concurrent.futures.CancelledError:
                        pass
                    except ZendeskAPIError as e:
                        error_info = {
                            'item': item.get('dc_name', 'unknown'),
                            'type': item.get('type', 'unknown'),
                            'error': str(e), 'status_code': e.status_code,
                            'error_type': e.error_type, 'details': e.details
                        }
                        self.last_execution_results['failed'].append(error_info)
                        log_callback.emit(
                            f"FAILED: {item.get('dc_name', 'unknown')} - {e}"
                        )
                    except Exception as e:
                        error_info = {
                            'item': item.get('dc_name', 'unknown'),
                            'type': item.get('type', 'unknown'),
                            'error': str(e), 'status_code': 0,
                            'error_type': 'Unknown', 'details': ''
                        }
                        self.last_execution_results['failed'].append(error_info)
                        log_callback.emit(
                            f"FAILED: {item.get('dc_name', 'unknown')} - {e}"
                        )
            except Exception as e:
                for f in futures.keys():
                    f.cancel()
                raise
                
        return self.last_execution_results

    def _fetch_locale_map_for_apply(
        self, log_callback: pyqtSignal
    ) -> Dict[str, int]:
        try:
            url = (
                f"https://{self.creds['subdomain']}.zendesk.com"
                f"/api/v2/locales.json"
            )
            resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_SHORT)
            if resp.status_code == 200:
                locs = resp.json().get('locales', [])
                return {loc['locale']: loc['id'] for loc in locs}
        except Exception as e:
            log_callback.emit(f"Warning: Could not fetch locales: {e}")
        return {}

    def _apply_single_with_retry(
        self, item: Dict[str, Any], l_map: Dict[str, int],
        log_callback: pyqtSignal = None
    ) -> str:
        """Apply single item with retry logic."""
        last_error: Optional[Exception] = None
        for attempt in range(API_CONFIG.RETRY_COUNT):
            try:
                return self._apply_single(item, l_map, log_callback)
            except ZendeskAPIError as e:
                if e.status_code == 429:
                    wait_time = self._rate_limiter.handle_rate_limit_simple()
                    time.sleep(wait_time)
                    last_error = e
                    continue
                elif e.status_code >= 500:
                    time.sleep(API_CONFIG.RETRY_BASE_DELAY * (attempt + 1))
                    last_error = e
                    continue
                else:
                    raise
            except requests.exceptions.Timeout:
                time.sleep(API_CONFIG.RETRY_BASE_DELAY * (attempt + 1))
                last_error = ZendeskAPIError("Request timeout", 408)
                continue
        
        if last_error:
            raise last_error
        raise ZendeskAPIError("Unknown error after retries", 0)

    def _apply_single(
        self, item: Dict[str, Any], l_map: Dict[str, int],
        log_callback: pyqtSignal = None
    ) -> str:
        dc_id = item.get('dc_id')
        pt_id = l_map.get('pt-BR') or l_map.get('pt') or 1
        en_id = l_map.get('en-US') or l_map.get('en') or 1
        es_id = l_map.get('es') or l_map.get('es-ES') or 2

        en_content = item.get('en', '') or item.get('pt', '')
        es_content = item.get('es', '') or item.get('pt', '')
        is_en_default = (self.instance_default_locale == 'en-US')

        variants = [
            {
                'locale_id': pt_id, 'default': not is_en_default,
                'content': item.get('pt', '')
            },
            {
                'locale_id': en_id, 'default': is_en_default,
                'content': en_content
            },
            {'locale_id': es_id, 'default': False, 'content': es_content}
        ]

        if item.get('force_update') and dc_id:
            url = (
                f"https://{self.creds['subdomain']}.zendesk.com"
                f"/api/v2/dynamic_content/items/{dc_id}.json"
            )
            resp = self.session.put(
                url, json={"item": {"variants": variants}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
        elif not dc_id and item.get('action') == 'CREATE':
            def_loc_id = en_id if is_en_default else pt_id
            payload = {
                "item": {
                    "name": item.get('dc_name', ''),
                    "default_locale_id": def_loc_id,
                    "variants": variants
                }
            }
            url = (
                f"https://{self.creds['subdomain']}.zendesk.com"
                f"/api/v2/dynamic_content/items.json"
            )
            resp = self.session.post(
                url, json=payload, timeout=API_CONFIG.TIMEOUT_LONG
            )
            if resp.status_code == 201:
                dc_id = resp.json()['item']['id']
            elif resp.status_code != 422:
                raise parse_zendesk_error(resp)

        ph = item.get('placeholder', '')
        item_type = item.get('type', '')
        
        if item_type == 'field':
            return self._update_field(item, ph)
        elif item_type == 'form':
            return self._update_form(item, ph)
        elif item_type == 'category':
            return self._update_category(item, ph)
        elif item_type == 'section':
            return self._update_section(item, ph)
        elif item_type == 'article':
            return self._update_article(item, ph)
        elif item_type == 'option':
            return self._update_option(item, ph)
        return f"SKIPPED: {item.get('dc_name', 'unknown')}"

    def _update_field(self, item: Dict[str, Any], ph: str) -> str:
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/ticket_fields/{item.get('id')}.json"
        )
        resp = self.session.put(
            url, json={"ticket_field": {"title": ph}},
            timeout=API_CONFIG.TIMEOUT_LONG
        )
        if not resp.ok:
            raise parse_zendesk_error(resp)
        return f"SUCCESS: Field {item.get('dc_name', '')}"

    def _update_form(self, item: Dict[str, Any], ph: str) -> str:
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/ticket_forms/{item.get('id')}.json"
        )
        resp = self.session.put(
            url, json={"ticket_form": {"display_name": ph}},
            timeout=API_CONFIG.TIMEOUT_LONG
        )
        if not resp.ok:
            raise parse_zendesk_error(resp)
        return f"SUCCESS: Form {item.get('dc_name', '')}"

    def _update_category(self, item: Dict[str, Any], ph: str) -> str:
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/categories/{item.get('id')}.json"
        )
        resp = self.session.put(
            url, json={"category": {"name": ph}},
            timeout=API_CONFIG.TIMEOUT_LONG
        )
        if not resp.ok:
            raise parse_zendesk_error(resp)
        return f"SUCCESS: Category {item.get('dc_name', '')}"

    def _update_section(self, item: Dict[str, Any], ph: str) -> str:
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/sections/{item.get('id')}.json"
        )
        resp = self.session.put(
            url, json={"section": {"name": ph}},
            timeout=API_CONFIG.TIMEOUT_LONG
        )
        if not resp.ok:
            raise parse_zendesk_error(resp)
        return f"SUCCESS: Section {item.get('dc_name', '')}"

    def _update_article(self, item: Dict[str, Any], ph: str) -> str:
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/help_center/articles/{item.get('id')}.json"
        )
        resp = self.session.put(
            url, json={"article": {"title": ph}},
            timeout=API_CONFIG.TIMEOUT_LONG
        )
        if not resp.ok:
            raise parse_zendesk_error(resp)
        return f"SUCCESS: Article {item.get('dc_name', '')}"

    def _update_option(self, item: Dict[str, Any], ph: str) -> str:
        parent_id = item.get('parent_id')
        if not parent_id:
            return f"SKIPPED: Option {item.get('dc_name', '')} - no parent_id"
        
        url = (
            f"https://{self.creds['subdomain']}.zendesk.com"
            f"/api/v2/ticket_fields/{parent_id}.json"
        )
        resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_LONG)
        if not resp.ok:
            raise parse_zendesk_error(resp)
        fresh_options = resp.json().get(
            'ticket_field', {}
        ).get('custom_field_options', [])
        found = False
        item_id = item.get('id')
        for opt in fresh_options:
            if str(opt.get('id')) == str(item_id):
                opt['name'] = ph
                found = True
                break
        if found:
            resp = self.session.put(
                url,
                json={"ticket_field": {"custom_field_options": fresh_options}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"SUCCESS: Option {item.get('dc_name', '')}"
        return f"SKIPPED: Option {item.get('dc_name', '')} not found"

    def load_backup_thread(
        self, progress_callback: pyqtSignal, log_callback: pyqtSignal,
        filepath: str
    ) -> List[Dict[str, Any]]:
        progress_callback.emit(0, 0, "Reading file...")
        if not os.path.exists(filepath):
            raise Exception(f"Backup file not found: {filepath}")
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                data = json.load(file)
            items = data.get('items', [])
            log_callback.emit(f"Backup loaded: {len(items)} items found.")
            return items
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON: {e}")
        except Exception as e:
            raise Exception(f"Failed to load backup: {e}")

    def perform_restore_from_data(
        self, items: List[Dict[str, Any]], progress_callback: pyqtSignal,
        log_callback: pyqtSignal
    ) -> str:
        self.reset_stop()
        self._rate_limiter.reset()
        total = len(items)
        if total == 0:
            return "No items to restore"

        log_callback.emit(f"Restoring {total} items...")
        start_time = time.time()
        success_count = 0
        error_count = 0

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=API_CONFIG.THREAD_POOL_SIZE
        ) as executor:
            futures = {
                executor.submit(self._restore_single_with_retry, item): item
                for item in items
            }
            
            try:
                for i, future in enumerate(
                    concurrent.futures.as_completed(futures)
                ):
                    if self.stop_requested:
                        raise Exception("Operation Canceled")
                    
                    item = futures[future]
                    try:
                        res = future.result()
                        log_callback.emit(res)
                        if res.startswith("Restored"):
                            success_count += 1
                        eta = self._calc_eta(start_time, i + 1, total)
                        progress_callback.emit(
                            i + 1, total,
                            f"Restored: {i + 1}/{total} | ETA: {eta}"
                        )
                    except concurrent.futures.CancelledError:
                        pass
                    except Exception as e:
                        error_count += 1
                        log_callback.emit(f"FAILED: {e}")
            except Exception as e:
                for f in futures.keys():
                    f.cancel()
                raise
                
        return f"Restore Complete. Success: {success_count}, Errors: {error_count}"

    def _restore_single_with_retry(self, item: Dict[str, Any]) -> str:
        """Restore single item with retry logic."""
        last_error: Optional[Exception] = None
        for attempt in range(API_CONFIG.RETRY_COUNT):
            try:
                return self._restore_single(item)
            except ZendeskAPIError as e:
                if e.status_code == 429 or e.status_code >= 500:
                    time.sleep(API_CONFIG.RETRY_BASE_DELAY * (attempt + 1))
                    last_error = e
                    continue
                else:
                    raise
            except requests.exceptions.Timeout:
                time.sleep(API_CONFIG.RETRY_BASE_DELAY * (attempt + 1))
                last_error = ZendeskAPIError("Request timeout", 408)
                continue
        
        if last_error:
            raise last_error
        raise ZendeskAPIError("Unknown error after retries", 0)

    def _restore_single(self, item: Dict[str, Any]) -> str:
        if item is None:
            return "Skipped: Invalid item"
        
        orig = str(item.get('original_text', ''))
        if not orig:
            return f"Skipped: {item.get('id', 'unknown')} - no original text"

        item_type = item.get('type', '')
        item_id = item.get('id')
        if not item_id:
            return "Skipped: no item ID"
        
        sub = self.creds.get('subdomain', '')
        if not sub:
            return "Skipped: not connected"

        if item_type == 'field':
            url = f"https://{sub}.zendesk.com/api/v2/ticket_fields/{item_id}.json"
            resp = self.session.put(
                url, json={"ticket_field": {"title": orig}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"Restored Field: {orig[:50]}"
        elif item_type == 'form':
            url = f"https://{sub}.zendesk.com/api/v2/ticket_forms/{item_id}.json"
            resp = self.session.put(
                url, json={"ticket_form": {"display_name": orig}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"Restored Form: {orig[:50]}"
        elif item_type == 'category':
            url = (
                f"https://{sub}.zendesk.com"
                f"/api/v2/help_center/categories/{item_id}.json"
            )
            resp = self.session.put(
                url, json={"category": {"name": orig}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"Restored Category: {orig[:50]}"
        elif item_type == 'section':
            url = (
                f"https://{sub}.zendesk.com"
                f"/api/v2/help_center/sections/{item_id}.json"
            )
            resp = self.session.put(
                url, json={"section": {"name": orig}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"Restored Section: {orig[:50]}"
        elif item_type == 'article':
            url = (
                f"https://{sub}.zendesk.com"
                f"/api/v2/help_center/articles/{item_id}.json"
            )
            resp = self.session.put(
                url, json={"article": {"title": orig}},
                timeout=API_CONFIG.TIMEOUT_LONG
            )
            if not resp.ok:
                raise parse_zendesk_error(resp)
            return f"Restored Article: {orig[:50]}"
        elif item_type == 'option':
            parent_id = item.get('parent_id')
            if not parent_id:
                return f"Skipped Option: {item_id} - no parent_id"
            url = (
                f"https://{sub}.zendesk.com"
                f"/api/v2/ticket_fields/{parent_id}.json"
            )
            resp = self.session.get(url, timeout=API_CONFIG.TIMEOUT_LONG)
            if resp.ok:
                opts = resp.json().get(
                    'ticket_field', {}
                ).get('custom_field_options', [])
                found = False
                for o in opts:
                    if str(o.get('id')) == str(item_id):
                        o['name'] = orig
                        found = True
                        break
                if found:
                    resp = self.session.put(
                        url,
                        json={
                            "ticket_field": {"custom_field_options": opts}
                        },
                        timeout=API_CONFIG.TIMEOUT_LONG
                    )
                    if not resp.ok:
                        raise parse_zendesk_error(resp)
                    return f"Restored Option: {orig[:50]}"
                return f"Skipped Option: {item_id} - not found"
            else:
                raise parse_zendesk_error(resp)
        return f"Skipped: {item_id} - unknown type '{item_type}'"


# ==============================================================================
# UI COMPONENTS
# ==============================================================================


class StepWorker(QThread):
    """Background worker thread for long-running operations."""

    progress = pyqtSignal(int, int, str)
    log = pyqtSignal(str)
    result = pyqtSignal(bool, object)

    def __init__(self, func: Callable, *args):
        super().__init__()
        self.func = func
        self.args = args
        self._is_cancelled = False
        self._cancel_lock = threading.Lock()

    def run(self):
        try:
            res = self.func(self.progress, self.log, *self.args)
            with self._cancel_lock:
                if not self._is_cancelled:
                    self.result.emit(True, res)
        except Exception as e:
            with self._cancel_lock:
                if not self._is_cancelled:
                    self.result.emit(False, str(e))

    def cancel(self):
        with self._cancel_lock:
            self._is_cancelled = True

    @property
    def is_cancelled(self) -> bool:
        with self._cancel_lock:
            return self._is_cancelled


class ModernSidebar(QFrame):
    """Sidebar navigation component."""

    def __init__(self, parent_wiz: 'ZendeskWizard'):
        super().__init__()
        self.setObjectName("Sidebar")
        self.parent_wiz = parent_wiz
        self.setFixedWidth(UI_CONFIG.SIDEBAR_WIDTH)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(8, 20, 8, 20)
        self.layout.setSpacing(4)
        self.btns: List[QPushButton] = []

        labels = [
            "Start", "Scan Data", "Translate", "Preview", "Apply", "Rollback"
        ]
        for i, txt in enumerate(labels):
            btn = QPushButton(f"{i + 1}. {txt}")
            btn.setObjectName("StepBtn")
            btn.setCheckable(True)
            btn.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            if i > 0:
                btn.setEnabled(False)
            btn.clicked.connect(lambda _, x=i: self.parent_wiz.goto(x))
            self.layout.addWidget(btn)
            self.btns.append(btn)
        self.layout.addStretch()

    def set_active(self, index: int):
        for i, btn in enumerate(self.btns):
            btn.setChecked(i == index)

    def unlock(self, index: int):
        if 0 <= index < len(self.btns):
            self.btns[index].setEnabled(True)

    def set_locked(self, locked: bool):
        for btn in self.btns:
            btn.setDisabled(locked)
        self.setEnabled(not locked)


class WizardPage(QFrame):
    """Base card component for wizard steps - fills available space."""

    def __init__(self, title: str, subtitle: str):
        super().__init__()
        self.setObjectName("Card")

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(
            UI_CONFIG.CARD_MARGIN, UI_CONFIG.CARD_MARGIN,
            UI_CONFIG.CARD_MARGIN, UI_CONFIG.CARD_MARGIN
        )
        self.main_layout.setSpacing(UI_CONFIG.CARD_SPACING)

        # Header
        header = QVBoxLayout()
        header.setSpacing(2)

        t = QLabel(title)
        t.setObjectName("Title")
        header.addWidget(t)

        s = QLabel(subtitle)
        s.setObjectName("Subtitle")
        s.setWordWrap(True)
        header.addWidget(s)

        self.main_layout.addLayout(header)

        # Content area
        self.content = QVBoxLayout()
        self.content.setSpacing(UI_CONFIG.CARD_SPACING)
        self.main_layout.addLayout(self.content, 1)

    def add_widget(self, w: QWidget, stretch: int = 0):
        self.content.addWidget(w, stretch)

    def add_layout(self, layout, stretch: int = 0):
        self.content.addLayout(layout, stretch)

    def add_stretch(self, factor: int = 1):
        self.content.addStretch(factor)


class EmbeddedStatusBar(QFrame):
    """Status bar component with progress indication."""

    def __init__(self):
        super().__init__()
        self.setObjectName("StatusBar")
        self.setFixedHeight(UI_CONFIG.STATUS_BAR_HEIGHT)
        self.setVisible(False)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 0, 15, 0)
        layout.setSpacing(15)

        self.status_lbl = QLabel("Ready")
        self.status_lbl.setObjectName("StatusText")

        self.elapsed_lbl = QLabel("")
        self.elapsed_lbl.setObjectName("ElapsedText")
        self.elapsed_lbl.setMinimumWidth(80)

        self.stats_lbl = QLabel("")
        self.stats_lbl.setObjectName("StatsText")
        self.stats_lbl.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )

        self.p_bar = QProgressBar()
        self.p_bar.setFixedWidth(180)
        self.p_bar.setTextVisible(False)

        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setObjectName("DangerBtn")
        self.btn_stop.setFixedSize(70, 32)
        self.btn_stop.setEnabled(False)

        layout.addWidget(self.status_lbl)
        layout.addWidget(self.elapsed_lbl)
        layout.addStretch()
        layout.addWidget(self.stats_lbl)
        layout.addWidget(self.p_bar)
        layout.addWidget(self.btn_stop)

        self._start_time: float = 0
        self._elapsed_timer = QTimer()
        self._elapsed_timer.timeout.connect(self._update_elapsed)

    def _format_elapsed(self, seconds: float) -> str:
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m {int(seconds % 60)}s"
        else:
            return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}m"

    def _update_elapsed(self):
        if self._start_time > 0:
            elapsed = time.time() - self._start_time
            self.elapsed_lbl.setText(f"⏱ {self._format_elapsed(elapsed)}")

    def show_progress(
        self, current: int, total: int, msg_main: str, msg_detail: str
    ):
        self.setVisible(True)
        self.status_lbl.setText(msg_main)
        self.stats_lbl.setText(msg_detail)
        self.btn_stop.setEnabled(True)
        if self._start_time == 0:
            self._start_time = time.time()
            self._elapsed_timer.start(1000)
            self._update_elapsed()
        if total == 0:
            self.p_bar.setRange(0, 0)
        else:
            self.p_bar.setRange(0, total)
            self.p_bar.setValue(current)

    def reset_ui(self):
        self.btn_stop.setEnabled(False)
        self.p_bar.setRange(0, 100)
        if self.p_bar.value() == -1 or self.p_bar.maximum() == 0:
            self.p_bar.setValue(0)
        self._elapsed_timer.stop()
        self._start_time = 0
        self.elapsed_lbl.setText("")

    def finish(self, message: str, success: bool = True):
        self.status_lbl.setText(message)
        self.stats_lbl.setText("")
        self.btn_stop.setEnabled(False)
        self._elapsed_timer.stop()
        self.p_bar.setRange(0, 100)
        self.p_bar.setValue(100 if success else 0)
    
    def stop_timer(self):
        """Stop the elapsed timer - call on window close."""
        self._elapsed_timer.stop()


def get_source_color(source: str) -> QColor:
    if source == SOURCE_ZENDESK_DC:
        return COLOR_SOURCE_DC
    elif source == SOURCE_TRANSLATED:
        return COLOR_SOURCE_TRANSLATED
    elif source == SOURCE_CACHE:
        return COLOR_SOURCE_CACHE
    elif source == SOURCE_FAILED:
        return COLOR_SOURCE_FAILED
    else:
        return COLOR_SOURCE_NEW


def get_text_color(source: str) -> QColor:
    if source == SOURCE_ZENDESK_DC:
        return COLOR_TEXT_FROM_DC
    elif source == SOURCE_TRANSLATED:
        return COLOR_TEXT_TRANSLATED
    elif source == SOURCE_NEW:
        return COLOR_TEXT_NEW
    elif source == SOURCE_FAILED:
        return COLOR_TEXT_FAILED
    else:
        return COLOR_TEXT_DEFAULT


# ==============================================================================
# DARK CONSOLE WIDGET
# ==============================================================================


class DarkConsoleWidget(QTextEdit):
    """Custom QTextEdit for console-style logging with dark background."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DarkConsole")
        self.setReadOnly(True)
        mono_font = get_monospace_font_family()
        self._setup_dark_theme(mono_font)

    def _setup_dark_theme(self, mono_font: str):
        bg_color = QColor(LOG_BACKGROUND_COLOR)
        text_color = QColor(LOG_TEXT_COLOR)
        selection_bg = QColor(LOG_SELECTION_BG)

        self.setStyleSheet(f"""
            QTextEdit#DarkConsole {{
                background-color: {LOG_BACKGROUND_COLOR};
                color: {LOG_TEXT_COLOR};
                border: none;
                border-radius: 4px;
                padding: 8px;
                font-family: "{mono_font}";
                font-size: 11px;
                selection-background-color: {LOG_SELECTION_BG};
                selection-color: {LOG_TEXT_COLOR};
            }}
        """)

        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Base, bg_color)
        palette.setColor(QPalette.ColorRole.Window, bg_color)
        palette.setColor(QPalette.ColorRole.Text, text_color)
        palette.setColor(QPalette.ColorRole.Highlight, selection_bg)
        palette.setColor(QPalette.ColorRole.HighlightedText, text_color)
        self.setPalette(palette)
        self.setAutoFillBackground(True)

        font = QFont(mono_font, 11)
        self.setFont(font)

        char_format = QTextCharFormat()
        char_format.setForeground(QBrush(text_color))
        self.setCurrentCharFormat(char_format)

    def showEvent(self, event):
        super().showEvent(event)
        viewport = self.viewport()
        if viewport:
            bg_color = QColor(LOG_BACKGROUND_COLOR)
            viewport_palette = viewport.palette()
            viewport_palette.setColor(QPalette.ColorRole.Base, bg_color)
            viewport_palette.setColor(QPalette.ColorRole.Window, bg_color)
            viewport.setPalette(viewport_palette)
            viewport.setAutoFillBackground(True)

    def append(self, text: str):
        char_format = QTextCharFormat()
        char_format.setForeground(QBrush(QColor(LOG_TEXT_COLOR)))
        cursor = self.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(text + "\n", char_format)
        self.verticalScrollBar().setValue(self.verticalScrollBar().maximum())


# ==============================================================================
# HELPER: CREATE FORM ROW
# ==============================================================================


def create_form_row(
    label_text: str,
    widget: QWidget,
    label_width: int = UI_CONFIG.LABEL_WIDTH
) -> QHBoxLayout:
    """Create a consistent form row with label and widget."""
    row = QHBoxLayout()
    row.setSpacing(12)
    
    label = QLabel(label_text)
    label.setObjectName("FieldLabel")
    label.setFixedWidth(label_width)
    label.setAlignment(
        Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
    )
    
    row.addWidget(label)
    row.addWidget(widget, 1)
    
    return row


def create_section_header(text: str) -> QLabel:
    """Create a section header label."""
    label = QLabel(text)
    label.setObjectName("SectionHeader")
    return label


def create_divider() -> QFrame:
    """Create a horizontal divider line."""
    divider = QFrame()
    divider.setObjectName("Divider")
    divider.setFrameShape(QFrame.Shape.HLine)
    divider.setFixedHeight(1)
    return divider


# ==============================================================================
# MAIN WINDOW
# ==============================================================================


class ZendeskWizard(QMainWindow):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Zendesk Dynamic Content Manager")
        self.setMinimumSize(UI_CONFIG.MIN_WINDOW_WIDTH, UI_CONFIG.MIN_WINDOW_HEIGHT)

        self.controller = ZendeskController()
        self.backup_candidates: List[Dict[str, Any]] = []
        self.worker: Optional[StepWorker] = None
        self.state_manager = StateManager(self._on_state_change)
        self._worker_lock = threading.Lock()

        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self.sidebar = ModernSidebar(self)
        root.addWidget(self.sidebar)

        # Main content area with splitter
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.setHandleWidth(3)
        self.splitter.setChildrenCollapsible(False)
        root.addWidget(self.splitter, 1)

        # Top pane - wizard pages
        self.top_pane = QWidget()
        self.top_pane.setStyleSheet("background-color: #f3f4f6;")
        top_layout = QVBoxLayout(self.top_pane)
        top_layout.setContentsMargins(15, 15, 15, 10)
        top_layout.setSpacing(8)

        self.stack = QStackedWidget()
        top_layout.addWidget(self.stack, 1)

        self.status_bar = EmbeddedStatusBar()
        self.status_bar.btn_stop.clicked.connect(self.stop_process)
        top_layout.addWidget(self.status_bar)

        self.splitter.addWidget(self.top_pane)

        # Log pane
        self.log_pane = QWidget()
        self.log_pane.setStyleSheet("background-color: #f3f4f6;")
        log_layout = QVBoxLayout(self.log_pane)
        log_layout.setContentsMargins(15, 8, 15, 15)
        log_layout.setSpacing(6)

        log_header = QHBoxLayout()
        lbl_log = QLabel("Activity Log")
        lbl_log.setObjectName("LogTitle")
        self.btn_clear_log = QPushButton("Clear")
        self.btn_clear_log.setObjectName("SmallBtn")
        self.btn_clear_log.setFixedSize(60, 22)
        self.btn_clear_log.clicked.connect(self.run_clear_log)
        log_header.addWidget(lbl_log)
        log_header.addStretch()
        log_header.addWidget(self.btn_clear_log)
        log_layout.addLayout(log_header)

        self.log_frame = QFrame()
        self.log_frame.setObjectName("LogFrame")
        self.log_frame.setStyleSheet(f"""
            QFrame#LogFrame {{
                background-color: {LOG_BACKGROUND_COLOR};
                border: 1px solid {LOG_BORDER_COLOR};
                border-radius: 6px;
            }}
        """)
        self.log_frame.setAutoFillBackground(True)

        log_frame_layout = QVBoxLayout(self.log_frame)
        log_frame_layout.setContentsMargins(2, 2, 2, 2)
        log_frame_layout.setSpacing(0)

        self.console = DarkConsoleWidget()
        log_frame_layout.addWidget(self.console)
        log_layout.addWidget(self.log_frame, 1)

        self.splitter.addWidget(self.log_pane)

        # Set splitter sizes
        self.splitter.setSizes([
            UI_CONFIG.SPLITTER_TOP_SIZE,
            UI_CONFIG.SPLITTER_LOG_SIZE
        ])

        self.init_pages()

    def _on_state_change(self, new_state: AppState):
        pass

    def closeEvent(self, event):
        """Handle window close - clean up resources properly."""
        # Stop the status bar timer
        self.status_bar.stop_timer()
        
        # Clean up worker
        with self._worker_lock:
            if self.worker is not None:
                self.controller.stop()
                self.worker.cancel()
                self.worker.quit()
                # Short timeout to avoid blocking UI
                if not self.worker.wait(500):
                    self.worker.terminate()
                    self.worker.wait(200)
        
        # Clean up controller resources
        self.controller.cleanup()
        event.accept()

    def log_msg(self, msg: str):
        """Thread-safe logging to console."""
        ts = datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{ts}] {msg}"
        
        # Append to console (thread-safe via Qt's signal mechanism)
        self.console.append(formatted_msg)
        
        # Write to log file
        try:
            folder = (
                self.controller.backup_folder
                if self.controller.backup_folder else os.getcwd()
            )
            path = os.path.join(folder, "process_log.txt")
            with open(path, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
                )
        except Exception as e:
            logger.debug(f"Could not write to log file: {e}")

    def goto(self, idx: int):
        if (0 <= idx < len(self.sidebar.btns)
                and self.sidebar.btns[idx].isEnabled()):
            self.stack.setCurrentIndex(idx)
            self.sidebar.set_active(idx)
            if idx == 2:
                self.update_translation_summary()
            elif idx == 3:
                self.populate_preview()

    def lock_ui(self, locked: bool):
        self.sidebar.set_locked(locked)
        self.btn_connect.setEnabled(not locked)
        self.btn_scan.setEnabled(not locked)
        self.btn_trans.setEnabled(not locked)
        self.btn_preview.setEnabled(not locked)
        self.btn_apply.setEnabled(not locked)
        self.btn_load_backup.setEnabled(not locked)
        self.btn_clear_log.setEnabled(not locked)
        self.btn_clear_cache.setEnabled(not locked)
        self.btn_execute_rollback.setEnabled(
            not locked and len(self.backup_candidates) > 0
        )

    def stop_process(self):
        self.controller.stop()
        self.log_msg("Stopping...")
        self.status_bar.status_lbl.setText("Stopping...")

    def _cleanup_worker(self):
        """Safely clean up the current worker thread."""
        with self._worker_lock:
            if self.worker is None:
                return
            
            worker = self.worker
            self.worker = None
        
        # Operations outside the lock
        if not worker.isRunning():
            self._safe_disconnect_worker(worker)
            return
        
        self.controller.stop()
        worker.cancel()
        worker.quit()
        
        for interval in UI_CONFIG.WORKER_STOP_INTERVALS:
            if worker.wait(interval):
                break
        else:
            worker.terminate()
            worker.wait(2000)
        
        self._safe_disconnect_worker(worker)

    def _safe_disconnect_worker(self, worker: StepWorker):
        """Safely disconnect and delete a worker."""
        if worker is None:
            return
        
        # Disconnect specific connections we made
        try:
            worker.progress.disconnect()
        except (TypeError, RuntimeError):
            pass
        try:
            worker.log.disconnect()
        except (TypeError, RuntimeError):
            pass
        try:
            worker.result.disconnect()
        except (TypeError, RuntimeError):
            pass
        try:
            worker.finished.disconnect()
        except (TypeError, RuntimeError):
            pass
        
        try:
            worker.deleteLater()
        except RuntimeError:
            pass

    def init_pages(self):
        self._init_page_config()
        self._init_page_scan()
        self._init_page_translate()
        self._init_page_preview()
        self._init_page_apply()
        self._init_page_rollback()

    def _init_page_config(self):
        page = WizardPage(
            "Configuration",
            "Setup your Zendesk credentials and preferences."
        )

        # Subdomain row
        self.in_sub = QLineEdit()
        self.in_sub.setPlaceholderText("your-company")
        self.in_sub.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        page.add_layout(create_form_row("Subdomain", self.in_sub))

        # Email row
        self.in_email = QLineEdit()
        self.in_email.setPlaceholderText("admin@company.com")
        self.in_email.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        page.add_layout(create_form_row("Email", self.in_email))

        # Token row
        self.in_tok = QLineEdit()
        self.in_tok.setEchoMode(QLineEdit.EchoMode.Password)
        self.in_tok.setPlaceholderText("Your Zendesk API Token")
        self.in_tok.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        page.add_layout(create_form_row("API Token", self.in_tok))

        # Backup folder row
        self.in_rollback = QLineEdit()
        self.in_rollback.setText(os.getcwd())
        self.in_rollback.setPlaceholderText("Folder for backups & cache")
        self.in_rollback.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        page.add_layout(create_form_row("Data Folder", self.in_rollback))

        # Cache row
        cache_widget = QWidget()
        cache_layout = QHBoxLayout(cache_widget)
        cache_layout.setContentsMargins(0, 0, 0, 0)
        cache_layout.setSpacing(10)
        
        self.spin_cache = QSpinBox()
        self.spin_cache.setRange(1, 365)
        self.spin_cache.setValue(30)
        self.spin_cache.setSuffix(" days")
        self.spin_cache.setFixedWidth(110)
        self.spin_cache.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        cache_layout.addWidget(self.spin_cache)

        self.btn_clear_cache = QPushButton("Clear Cache")
        self.btn_clear_cache.setObjectName("SecondaryBtn")
        self.btn_clear_cache.clicked.connect(self.run_clear_cache)
        cache_layout.addWidget(self.btn_clear_cache)
        cache_layout.addStretch()

        page.add_layout(create_form_row("Cache Validity", cache_widget))

        # Buttons row
        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)

        btn_save = QPushButton("Save Profile")
        btn_save.setObjectName("SecondaryBtn")
        btn_save.clicked.connect(self.save_creds)
        btn_row.addWidget(btn_save)

        btn_load = QPushButton("Load Profile")
        btn_load.setObjectName("SecondaryBtn")
        btn_load.clicked.connect(self.load_creds)
        btn_row.addWidget(btn_load)

        btn_row.addStretch()

        self.btn_connect = QPushButton("Connect")
        self.btn_connect.setObjectName("PrimaryBtn")
        self.btn_connect.clicked.connect(self.run_connect)
        btn_row.addWidget(self.btn_connect)

        page.add_stretch()
        page.add_layout(btn_row)
        self.stack.addWidget(page)

    def _init_page_scan(self):
        page = WizardPage(
            "Scan Data",
            "Select what to scan from your Zendesk instance."
        )

        # Options in a horizontal group
        options_group = QGroupBox("Scan Options")
        options_layout = QGridLayout(options_group)
        options_layout.setSpacing(15)
        options_layout.setContentsMargins(15, 24, 15, 15)

        self.chk_scan_fields = QCheckBox("Ticket Fields")
        self.chk_scan_fields.setChecked(True)
        self.chk_scan_forms = QCheckBox("Ticket Forms")
        self.chk_scan_forms.setChecked(True)
        self.chk_scan_cats = QCheckBox("HC Categories")
        self.chk_scan_sects = QCheckBox("HC Sections")
        self.chk_scan_arts = QCheckBox("HC Articles")

        options_layout.addWidget(self.chk_scan_fields, 0, 0)
        options_layout.addWidget(self.chk_scan_forms, 0, 1)
        options_layout.addWidget(self.chk_scan_cats, 0, 2)
        options_layout.addWidget(self.chk_scan_sects, 1, 0)
        options_layout.addWidget(self.chk_scan_arts, 1, 1)

        page.add_widget(options_group)

        # Results summary
        self.scan_summary_box = QTextEdit()
        self.scan_summary_box.setReadOnly(True)
        self.scan_summary_box.setObjectName("InfoBox")
        self.scan_summary_box.setMinimumHeight(120)
        self.scan_summary_box.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        page.add_widget(self.scan_summary_box, 1)

        # Button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_scan = QPushButton("Start Scan")
        self.btn_scan.setObjectName("PrimaryBtn")
        self.btn_scan.clicked.connect(self.run_scan)
        btn_row.addWidget(self.btn_scan)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_translate(self):
        """Initialize the redesigned Translation page."""
        page = WizardPage(
            "Translate",
            "Configure translation settings and run the translation."
        )

        # Status summary banner
        self.trans_summary_frame = QFrame()
        self.trans_summary_frame.setObjectName("TransSummary")
        self.trans_summary_frame.setMinimumHeight(50)
        trans_summary_layout = QHBoxLayout(self.trans_summary_frame)
        trans_summary_layout.setContentsMargins(16, 12, 16, 12)
        
        self.lbl_trans_summary = QLabel(
            "No scan data loaded. Run a scan first."
        )
        self.lbl_trans_summary.setObjectName("TransSummaryLabel")
        self.lbl_trans_summary.setWordWrap(True)
        trans_summary_layout.addWidget(self.lbl_trans_summary)
        
        page.add_widget(self.trans_summary_frame)

        # Spacer
        page.add_widget(QWidget(), 0)

        # Translation provider section
        page.add_widget(create_section_header("Translation Provider"))
        
        # Provider selection
        self.combo_provider = QComboBox()
        self.combo_provider.addItems([
            "Google Web (Free)", "Google Cloud Translation API"
        ])
        self.combo_provider.setMinimumHeight(UI_CONFIG.COMBO_MIN_HEIGHT)
        page.add_layout(create_form_row("Provider", self.combo_provider))

        # API Key
        self.in_api_key = QLineEdit()
        self.in_api_key.setPlaceholderText(
            "Required only for Google Cloud Translation API"
        )
        self.in_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        self.in_api_key.setMinimumHeight(UI_CONFIG.INPUT_MIN_HEIGHT)
        page.add_layout(create_form_row("API Key", self.in_api_key))

        # Divider
        page.add_widget(create_divider())

        # Options section
        page.add_widget(create_section_header("Options"))

        # Options container with left padding
        options_container = QWidget()
        options_layout = QVBoxLayout(options_container)
        options_layout.setContentsMargins(UI_CONFIG.LABEL_WIDTH + 12, 0, 0, 0)
        options_layout.setSpacing(8)

        self.chk_protect_acronyms = QCheckBox(
            "Protect Acronyms (API, ID, CST, SKU, etc.)"
        )
        self.chk_protect_acronyms.setChecked(True)
        options_layout.addWidget(self.chk_protect_acronyms)

        self.chk_force_retranslate = QCheckBox(
            "Force Retranslate All (ignore existing translations)"
        )
        options_layout.addWidget(self.chk_force_retranslate)

        page.add_widget(options_container)

        # Spacer and button
        page.add_stretch()

        # Button row
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        
        self.btn_trans = QPushButton("Run Translation")
        self.btn_trans.setObjectName("PrimaryBtn")
        self.btn_trans.setMinimumWidth(150)
        self.btn_trans.clicked.connect(self.run_trans)
        btn_row.addWidget(self.btn_trans)
        
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_preview(self):
        page = WizardPage("Preview", "Review changes before applying.")

        # Compact summary bar
        summary_row = QHBoxLayout()
        summary_row.setSpacing(15)

        self.lbl_sum_create = QLabel("Create: 0")
        self.lbl_sum_create.setObjectName("CompactLabel")
        self.lbl_sum_link = QLabel("Link: 0")
        self.lbl_sum_link.setObjectName("CompactLabel")
        self.lbl_sum_from_dc = QLabel("From DC: 0")
        self.lbl_sum_from_dc.setObjectName("CompactLabel")
        self.lbl_sum_translated = QLabel("Translated: 0")
        self.lbl_sum_translated.setObjectName("CompactLabel")

        summary_row.addWidget(self.lbl_sum_create)
        summary_row.addWidget(QLabel("|"))
        summary_row.addWidget(self.lbl_sum_link)
        summary_row.addWidget(QLabel("|"))
        summary_row.addWidget(self.lbl_sum_from_dc)
        summary_row.addWidget(QLabel("|"))
        summary_row.addWidget(self.lbl_sum_translated)
        summary_row.addStretch()

        # Filters
        lbl_filter = QLabel("Filter:")
        lbl_filter.setObjectName("FilterLabel")
        summary_row.addWidget(lbl_filter)

        self.chk_filter_ticket = QCheckBox("Ticket")
        self.chk_filter_ticket.setChecked(True)
        self.chk_filter_ticket.stateChanged.connect(self.apply_table_filter)
        summary_row.addWidget(self.chk_filter_ticket)

        self.chk_filter_hc = QCheckBox("Help Center")
        self.chk_filter_hc.setChecked(True)
        self.chk_filter_hc.stateChanged.connect(self.apply_table_filter)
        summary_row.addWidget(self.chk_filter_hc)

        page.add_layout(summary_row)

        # Table
        cols = ["Action", "Update", "Context", "Type", "Name", "PT", "EN", "ES"]
        self.table = QTableWidget(0, len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        self.table.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )

        h = self.table.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(7, QHeaderView.ResizeMode.Stretch)

        self.table.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        self.table.setAlternatingRowColors(True)
        page.add_widget(self.table, 1)

        # Legend and button
        bottom_row = QHBoxLayout()
        self.lbl_legend = QLabel(
            "<span style='background:#DCFCE7;padding:2px 4px;'>Green</span>=DC  "
            "<span style='background:#DBEAFE;padding:2px 4px;'>Blue</span>=Translated  "
            "<span style='background:#FEF3C7;padding:2px 4px;'>Yellow</span>=New  "
            "<span style='background:#FECACA;padding:2px 4px;'>Red</span>=Failed"
        )
        self.lbl_legend.setObjectName("LegendText")
        bottom_row.addWidget(self.lbl_legend)
        bottom_row.addStretch()

        self.btn_preview = QPushButton("Refresh")
        self.btn_preview.setObjectName("SecondaryBtn")
        self.btn_preview.clicked.connect(self.populate_preview)
        bottom_row.addWidget(self.btn_preview)

        page.add_layout(bottom_row)
        self.stack.addWidget(page)

    def _init_page_apply(self):
        page = WizardPage("Apply Changes", "Execute the changes to Zendesk.")

        # Options row
        options_row = QHBoxLayout()
        options_row.setSpacing(30)

        self.chk_create = QCheckBox("Create New DC Items")
        self.chk_create.setChecked(True)
        options_row.addWidget(self.chk_create)

        self.chk_link = QCheckBox("Link Existing DC Items")
        self.chk_link.setChecked(True)
        options_row.addWidget(self.chk_link)

        options_row.addStretch()

        self.lbl_apply_summary = QLabel("Ready to apply changes.")
        self.lbl_apply_summary.setObjectName("SummaryText")
        options_row.addWidget(self.lbl_apply_summary)

        page.add_layout(options_row)

        # Results box
        self.result_box = QTextEdit()
        self.result_box.setObjectName("InfoBox")
        self.result_box.setReadOnly(True)
        self.result_box.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        page.add_widget(self.result_box, 1)

        # Button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_apply = QPushButton("Apply Changes")
        self.btn_apply.setObjectName("DangerBtn")
        self.btn_apply.clicked.connect(self.run_apply)
        btn_row.addWidget(self.btn_apply)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_rollback(self):
        page = WizardPage(
            "Rollback",
            "Restore original values from a backup file."
        )

        # Load button
        load_row = QHBoxLayout()
        self.btn_load_backup = QPushButton("Load Backup File")
        self.btn_load_backup.setObjectName("PrimaryBtn")
        self.btn_load_backup.clicked.connect(self.load_backup_file)
        load_row.addWidget(self.btn_load_backup)
        load_row.addStretch()
        page.add_layout(load_row)

        # Backup table
        cols = ["Context", "Type", "Name", "Original Text", "EN", "ES"]
        self.backup_table = QTableWidget(0, len(cols))
        self.backup_table.setHorizontalHeaderLabels(cols)
        self.backup_table.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )

        h = self.backup_table.horizontalHeader()
        h.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)

        page.add_widget(self.backup_table, 1)

        # Execute button
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_execute_rollback = QPushButton("Execute Rollback")
        self.btn_execute_rollback.setObjectName("DangerBtn")
        self.btn_execute_rollback.setEnabled(False)
        self.btn_execute_rollback.clicked.connect(self.run_rollback)
        btn_row.addWidget(self.btn_execute_rollback)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def update_translation_summary(self):
        summary = self.controller.get_translation_summary()
        if summary['total'] == 0:
            self.lbl_trans_summary.setText(
                "No scan data loaded. Go to Step 2 (Scan Data) to scan "
                "your Zendesk instance."
            )
            return
        
        # Use HTML escaping for safety
        text = (
            f"<b>Total Items:</b> {escape_html(str(summary['total']))} "
            f"&nbsp;│&nbsp; "
            f"<b>From Zendesk DC:</b> {escape_html(str(summary['from_dc']))} "
            f"&nbsp;│&nbsp; "
            f"<b>Need Translation:</b> "
            f"{escape_html(str(summary['needs_translation']))} &nbsp;│&nbsp; "
            f"<b>Already Complete:</b> "
            f"{escape_html(str(summary['has_existing']))}"
        )
        if summary.get('failed', 0) > 0:
            text += (
                f" &nbsp;│&nbsp; <b style='color:#991B1B;'>Failed:</b> "
                f"{escape_html(str(summary['failed']))}"
            )
        
        self.lbl_trans_summary.setText(text)

    def save_creds(self):
        f, _ = QFileDialog.getSaveFileName(
            self, "Save Profile", "", "JSON (*.json)",
            options=QFileDialog.Option.DontUseNativeDialog
        )
        if f:
            success = self.controller.save_profile(
                f, self.in_sub.text(), self.in_email.text(),
                self.in_tok.text(), self.in_rollback.text(),
                self.in_api_key.text(), self.chk_protect_acronyms.isChecked(),
                self.spin_cache.value()
            )
            if success:
                self.log_msg(f"Profile saved: {f}")
            else:
                QMessageBox.warning(self, "Error", "Failed to save profile")

    def load_creds(self):
        f, _ = QFileDialog.getOpenFileName(
            self, "Load Profile", "", "JSON (*.json)",
            options=QFileDialog.Option.DontUseNativeDialog
        )
        if f:
            d = self.controller.load_profile(f)
            if d:
                self.in_sub.setText(d.get('subdomain', ''))
                self.in_email.setText(d.get('email', ''))
                self.in_tok.setText(d.get('token', ''))
                self.in_rollback.setText(d.get('backup_path', ''))
                self.in_api_key.setText(d.get('google_api_key', ''))
                self.chk_protect_acronyms.setChecked(
                    d.get('protect_acronyms', True)
                )
                self.spin_cache.setValue(d.get('cache_expiry_days', 30))
                self.log_msg(f"Profile loaded: {f}")
            else:
                QMessageBox.warning(self, "Error", "Failed to load profile")

    def run_connect(self):
        if self.state_manager.is_busy:
            return
        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Connecting...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.connect(
                    self.in_sub.text(), self.in_email.text(),
                    self.in_tok.text(), self.in_rollback.text(), l
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_connect_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_connect_finished(self, success: bool, msg: object):
        self.state_manager.force_reset()
        self.lock_ui(False)
        self.status_bar.finish("Ready")
        if success:
            self.log_msg(f"[SUCCESS] {msg}")
            QMessageBox.information(self, "Success", str(msg))
            self.sidebar.unlock(1)
            self.sidebar.unlock(5)
            self.goto(1)
        else:
            self.log_msg(f"[ERROR] {msg}")
            QMessageBox.critical(self, "Error", str(msg))

    def run_clear_cache(self):
        if self.state_manager.is_busy:
            return
        if QMessageBox.warning(
            self, "Confirm", "Clear the translation cache?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.No:
            return
        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Clearing Cache...", "")
        self.lock_ui(True)
        
        with self._worker_lock:
            self.worker = StepWorker(lambda p, l: self.controller.clear_cache())
            self.worker.result.connect(self.on_clear_cache_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_clear_cache_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)
        self.status_bar.finish(
            "Cache Cleared" if success else "Failed", success
        )
        if success:
            self.log_msg("Cache cleared.")
        else:
            QMessageBox.critical(self, "Error", "Failed to clear cache.")

    def run_clear_log(self):
        if self.state_manager.is_busy:
            return
        self.console.clear()
        self.log_msg("Log cleared.")

    def run_scan(self):
        if self.state_manager.is_busy:
            return
        config = {
            'fields': self.chk_scan_fields.isChecked(),
            'forms': self.chk_scan_forms.isChecked(),
            'cats': self.chk_scan_cats.isChecked(),
            'sects': self.chk_scan_sects.isChecked(),
            'arts': self.chk_scan_arts.isChecked()
        }
        if not any(config.values()):
            QMessageBox.warning(
                self, "Warning", "Select at least one item to scan."
            )
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Scanning...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.scan_and_analyze(p, l, config)
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Scanning...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_scan_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_scan_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            if "Canceled" in str(result):
                self.status_bar.finish("Canceled", False)
            else:
                self.status_bar.finish("Failed", False)
                QMessageBox.critical(self, "Error", str(result))
            return

        self.status_bar.finish("Scan Complete", True)
        stats = result
        work_items = self.controller.work_items
        from_dc = sum(
            1 for i in work_items if i.get('source') == SOURCE_ZENDESK_DC
        )
        has_trans = sum(1 for i in work_items if i.get('en') and i.get('es'))

        report = f"""
        <h3>Scan Complete</h3>
        <table>
        <tr><td><b>Fields:</b></td>
            <td>{escape_html(str(stats.get('valid_fields', 0)))}</td></tr>
        <tr><td><b>Forms:</b></td>
            <td>{escape_html(str(stats.get('valid_forms', 0)))}</td></tr>
        <tr><td><b>HC Items:</b></td>
            <td>{escape_html(str(
                stats.get('valid_cats', 0) +
                stats.get('valid_sects', 0) +
                stats.get('valid_arts', 0)
            ))}</td></tr>
        <tr><td><b>From DC:</b></td>
            <td>{escape_html(str(from_dc))}</td></tr>
        <tr><td><b>With EN+ES:</b></td>
            <td>{escape_html(str(has_trans))}</td></tr>
        <tr><td><b>Total Tasks:</b></td>
            <td><b>{escape_html(str(len(work_items)))}</b></td></tr>
        </table>
        """
        self.scan_summary_box.setHtml(report)
        self.sidebar.unlock(2)
        self.sidebar.unlock(3)
        self.populate_preview()
        self.update_translation_summary()
        self.goto(2)

    def populate_preview(self):
        self.table.setUpdatesEnabled(False)
        self.table.blockSignals(True)
        try:
            self.table.setRowCount(0)
            rows = self.controller.work_items
            self.table.setRowCount(len(rows))

            items_from_dc = 0
            items_translated = 0
            items_failed = 0

            for r, item in enumerate(rows):
                source = item.get('source', SOURCE_NEW)
                pt_source = item.get('pt_source', source)
                en_source = item.get('en_source', SOURCE_NEW)
                es_source = item.get('es_source', SOURCE_NEW)

                if source == SOURCE_ZENDESK_DC:
                    items_from_dc += 1
                if (en_source == SOURCE_TRANSLATED
                        or es_source == SOURCE_TRANSLATED):
                    items_translated += 1
                if en_source == SOURCE_FAILED or es_source == SOURCE_FAILED:
                    items_failed += 1

                self.table.setItem(
                    r, 0, QTableWidgetItem(item.get('action', ''))
                )

                chk_widget = QWidget()
                chk_layout = QHBoxLayout(chk_widget)
                chk_layout.setContentsMargins(0, 0, 0, 0)
                chk_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                chk = QCheckBox()
                is_link = (
                    item.get('action') == 'LINK' and bool(item.get('dc_id'))
                )
                chk.setChecked(is_link)
                chk.setEnabled(is_link)
                chk_layout.addWidget(chk)
                self.table.setCellWidget(r, 1, chk_widget)

                self.table.setItem(
                    r, 2, QTableWidgetItem(item.get('context', ''))
                )
                self.table.setItem(
                    r, 3, QTableWidgetItem(item.get('type', ''))
                )
                self.table.setItem(
                    r, 4, QTableWidgetItem(item.get('dc_name', ''))
                )

                pt_item = QTableWidgetItem(item.get('pt', ''))
                pt_item.setBackground(QBrush(get_source_color(pt_source)))
                pt_item.setForeground(QBrush(get_text_color(pt_source)))
                self.table.setItem(r, 5, pt_item)

                en_item = QTableWidgetItem(item.get('en', ''))
                en_item.setBackground(QBrush(get_source_color(en_source)))
                en_item.setForeground(QBrush(get_text_color(en_source)))
                self.table.setItem(r, 6, en_item)

                es_item = QTableWidgetItem(item.get('es', ''))
                es_item.setBackground(QBrush(get_source_color(es_source)))
                es_item.setForeground(QBrush(get_text_color(es_source)))
                self.table.setItem(r, 7, es_item)

            create_count = sum(1 for x in rows if x.get('action') == 'CREATE')
            link_count = sum(1 for x in rows if x.get('action') == 'LINK')

            self.lbl_sum_create.setText(f"Create: {create_count}")
            self.lbl_sum_link.setText(f"Link: {link_count}")
            self.lbl_sum_from_dc.setText(f"From DC: {items_from_dc}")
            self.lbl_sum_translated.setText(f"Translated: {items_translated}")
            self.lbl_apply_summary.setText(f"Ready: {len(rows)} items")

            self.apply_table_filter()
        finally:
            self.table.blockSignals(False)
            self.table.setUpdatesEnabled(True)

    def apply_table_filter(self):
        show_ticket = self.chk_filter_ticket.isChecked()
        show_hc = self.chk_filter_hc.isChecked()
        for r in range(self.table.rowCount()):
            ctx_item = self.table.item(r, 2)
            ctx = ctx_item.text() if ctx_item else ""
            visible = (
                (ctx == "Ticket" and show_ticket)
                or (ctx == "Help Center" and show_hc)
                or ctx == "Unknown"
            )
            self.table.setRowHidden(r, not visible)

    def run_trans(self):
        if self.state_manager.is_busy:
            return

        provider = self.combo_provider.currentText()
        key = self.in_api_key.text().strip()

        if "Google Cloud" in provider and not key:
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setWindowTitle("Missing API Key")
            msg.setText("Google Cloud API requires an API key.")
            btn_web = msg.addButton(
                "Use Free Web", QMessageBox.ButtonRole.AcceptRole
            )
            msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            msg.exec()
            if msg.clickedButton() == btn_web:
                self.combo_provider.setCurrentIndex(0)
                provider = "Google Web (Free)"
            else:
                return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Translating...", "")
        self.lock_ui(True)

        self.controller.set_translation_config(
            provider, key, self.chk_protect_acronyms.isChecked(),
            self.spin_cache.value()
        )

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.perform_translation(
                    p, l, self.chk_force_retranslate.isChecked()
                )
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Translating...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(
                lambda s, m: self.finish_step(
                    s, m if not s else "Translation Done", 4
                )
            )
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def run_apply(self):
        if self.state_manager.is_busy:
            return
        if QMessageBox.warning(
            self, "Confirm", "Apply changes to Zendesk?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.No:
            return

        self._cleanup_worker()
        self.result_box.setText("Processing...")
        self.status_bar.show_progress(0, 0, "Applying...", "")
        self.lock_ui(True)

        items: List[Dict[str, Any]] = []
        do_create = self.chk_create.isChecked()
        do_link = self.chk_link.isChecked()
        work_items = self.controller.work_items

        # Validate table matches work_items
        if self.table.rowCount() != len(work_items):
            self.log_msg("Warning: Table out of sync, refreshing preview...")
            self.populate_preview()
            work_items = self.controller.work_items

        for r, item in enumerate(work_items):
            if r >= self.table.rowCount():
                self.log_msg(f"Warning: Skipping item {r} - table row missing")
                continue
            item_copy = item.copy()
            chk_widget = self.table.cellWidget(r, 1)
            if chk_widget:
                layout = chk_widget.layout()
                if layout and layout.count() > 0:
                    chk = layout.itemAt(0).widget()
                    if isinstance(chk, QCheckBox):
                        item_copy['force_update'] = chk.isChecked()
            is_create = item_copy.get('action') == 'CREATE'
            if (is_create and do_create) or (not is_create and do_link):
                items.append(item_copy)

        if not items:
            self.log_msg("No items selected for processing")
            self.lock_ui(False)
            self.status_bar.finish("No items to process", False)
            return

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.execute_changes(items, p, l)
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Applying...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_apply_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_apply_finished(self, success: bool, results: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            if "Canceled" in str(results):
                self.status_bar.finish("Canceled", False)
                return
            self.status_bar.finish("Error", False)
            QMessageBox.critical(self, "Error", "Execution failed.")
            return

        self.status_bar.finish("Done", True)
        if isinstance(results, dict):
            s, f = results.get('success', []), results.get('failed', [])
            backup = results.get('backup_file', '')
        else:
            s, f, backup = [], [], ''

        report = f"RESULTS:\nSuccess: {len(s)}\nFailed: {len(f)}"
        if f:
            report += "\n\nFailed Items:"
            for err in f[:10]:
                if isinstance(err, dict):
                    report += (
                        f"\n• {escape_html(str(err.get('item', '')))}: "
                        f"{escape_html(str(err.get('error', '')))}"
                    )
            if len(f) > 10:
                report += f"\n... and {len(f) - 10} more"
        report += f"\n\nBackup: {backup}"
        self.result_box.setText(report)

        if f:
            QMessageBox.warning(
                self, "Completed with Errors",
                f"Success: {len(s)}, Failed: {len(f)}"
            )
        else:
            QMessageBox.information(
                self, "Success", "All changes applied successfully."
            )

    def load_backup_file(self):
        if self.state_manager.is_busy:
            return
        f, _ = QFileDialog.getOpenFileName(
            self, "Load Backup", "", "JSON (*.json)",
            options=QFileDialog.Option.DontUseNativeDialog
        )
        if not f:
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Loading Backup...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(self.controller.load_backup_thread, f)
            self.worker.result.connect(self.on_backup_loaded)
            self.worker.log.connect(self.log_msg)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_backup_loaded(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            self.status_bar.finish("Load Failed", False)
            QMessageBox.critical(self, "Error", str(result))
            return

        if not isinstance(result, list):
            self.status_bar.finish("Load Failed", False)
            QMessageBox.critical(self, "Error", "Invalid backup format")
            return

        self.backup_candidates = result
        self.backup_table.setRowCount(len(self.backup_candidates))

        for r, item in enumerate(self.backup_candidates):
            self.backup_table.setItem(
                r, 0, QTableWidgetItem(item.get('context', ''))
            )
            self.backup_table.setItem(
                r, 1, QTableWidgetItem(item.get('type', ''))
            )
            self.backup_table.setItem(
                r, 2, QTableWidgetItem(item.get('dc_name', ''))
            )
            self.backup_table.setItem(
                r, 3, QTableWidgetItem(item.get('original_text', ''))
            )
            self.backup_table.setItem(
                r, 4, QTableWidgetItem(item.get('en', ''))
            )
            self.backup_table.setItem(
                r, 5, QTableWidgetItem(item.get('es', ''))
            )

        self.btn_execute_rollback.setEnabled(True)
        self.status_bar.finish(
            f"Loaded {len(self.backup_candidates)} items", True
        )
        self.log_msg(
            f"Loaded {len(self.backup_candidates)} items from backup."
        )

    def run_rollback(self):
        if self.state_manager.is_busy:
            return
        if QMessageBox.warning(
            self, "Confirm", "Restore original values?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.No:
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Restoring...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.perform_restore_from_data(
                    self.backup_candidates, p, l
                )
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Restoring...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(
                lambda s, m: self.finish_step(
                    s, m if not s else "Rollback Complete", 0
                )
            )
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def finish_step(self, success: bool, msg: object, next_idx: int):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success and "Canceled" in str(msg):
            self.status_bar.finish("Canceled", False)
            return

        self.status_bar.finish(str(msg), success)
        if success:
            self.log_msg(f"[SUCCESS] {msg}")
            QMessageBox.information(self, "Success", str(msg))
            if next_idx > 0:
                self.sidebar.unlock(next_idx)
            if next_idx == 4:
                self.populate_preview()
                self.update_translation_summary()
                self.goto(3)
            elif next_idx > 0:
                self.goto(next_idx)
        else:
            self.log_msg(f"[ERROR] {msg}")
            QMessageBox.critical(self, "Error", str(msg))


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================


def main():
    app = QApplication(sys.argv)
    app.setStyleSheet(generate_stylesheet())

    win = ZendeskWizard()

    screen = app.primaryScreen()
    if screen:
        geom = screen.availableGeometry()
        w = int(geom.width() * 0.85)
        h = int(geom.height() * 0.85)
        w = max(w, UI_CONFIG.MIN_WINDOW_WIDTH)
        h = max(h, UI_CONFIG.MIN_WINDOW_HEIGHT)
        x = geom.x() + (geom.width() - w) // 2
        y = geom.y() + (geom.height() - h) // 2
        win.setGeometry(x, y, w, h)

    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()