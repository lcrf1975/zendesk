"""
Configuration management for Zendesk DC Manager.

This module provides:
- Frozen dataclass configurations for API, Translation, and UI settings
- Consolidated color schemes for the application
- Word lists and patterns for filtering system content
- Locale mappings for Zendesk API
- YAML configuration file support
- Logging configuration
"""

import os
import sys
import logging
from dataclasses import dataclass
from typing import Dict, FrozenSet, Tuple, Optional
from pathlib import Path


# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    json_format: bool = False
) -> logging.Logger:
    """Configure application logging."""
    logger = logging.getLogger("zendesk_dc_manager")
    logger.setLevel(level)
    logger.handlers.clear()

    if json_format:
        try:
            import json

            class JsonFormatter(logging.Formatter):
                def format(self, record):
                    log_data = {
                        "timestamp": self.formatTime(record),
                        "level": record.levelname,
                        "logger": record.name,
                        "message": record.getMessage(),
                        "module": record.module,
                        "function": record.funcName,
                        "line": record.lineno,
                    }
                    if record.exc_info:
                        log_data["exception"] = self.formatException(
                            record.exc_info
                        )
                    return json.dumps(log_data)

            formatter = JsonFormatter()
        except Exception:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create log file: {e}")

    return logger


logger = setup_logging()


# ==============================================================================
# MACOS + PYENV + QT6 COMPATIBILITY
# ==============================================================================


def configure_qt_environment():
    """Configure Qt environment variables for cross-platform compatibility."""
    if sys.platform == 'darwin':
        os.environ['QT_MAC_WANTS_LAYER'] = '1'
        os.environ['QT_DEBUG_PLUGINS'] = '0'
        os.environ['QT_FILESYSTEMMODEL_WATCH_FILES'] = '0'
        os.environ['QT_ENABLE_HIGHDPI_SCALING'] = '1'

    os.environ['QT_QUICK_BACKEND'] = 'software'



# ==============================================================================
# CONFIGURATION DATACLASSES
# ==============================================================================


@dataclass(frozen=True)
class APIConfig:
    """API-related configuration constants."""

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
    """Translation-related configuration constants."""

    DELAY_MIN: float = 0.3
    DELAY_MAX: float = 0.8
    MIN_TEXT_FOR_PADDING: int = 15
    MIN_TEXT_FOR_PADDING_LOWER: int = 3
    DEFAULT_CACHE_EXPIRY_DAYS: int = 30


@dataclass(frozen=True)
class UIConfig:
    """UI-related configuration constants."""

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
    TABLE_BATCH_SIZE: int = 100
    TABLE_INSERT_BATCH: int = 20
    TABLE_INSERT_INTERVAL_MS: int = 10
    SCREEN_RATIO: float = 1.0
    TABLE_ROW_HEIGHT: int = 32


# ==============================================================================
# SINGLETON CONFIG INSTANCES
# ==============================================================================


API_CONFIG = APIConfig()
TRANSLATION_CONFIG = TranslationConfig()
UI_CONFIG = UIConfig()


# ==============================================================================
# CONSTANTS
# ==============================================================================


VERSION = "49.0"

CREDENTIALS_FILE = "credentials.json"

# Translation source constants
SOURCE_NEW = "New"
SOURCE_ZENDESK_DC = "Zendesk DC"
SOURCE_TRANSLATED = "Translated"
SOURCE_CACHE = "Cache"
SOURCE_FAILED = "Failed"
SOURCE_MANUAL = "Manual"
SOURCE_ATTENTION = "Attention"
SOURCE_RESERVED = "Reserved"


# ==============================================================================
# CONSOLIDATED COLOR DEFINITIONS (Single Source of Truth)
# ==============================================================================


# Background colors for translation source types
SOURCE_COLORS: Dict[str, str] = {
    SOURCE_NEW: "#FEF9C3",        # Yellow - pending/new
    SOURCE_ZENDESK_DC: "#BBF7D0",  # Green - from DC
    SOURCE_TRANSLATED: "#BFDBFE",  # Blue - translated
    SOURCE_CACHE: "#C7D2FE",      # Indigo - from cache
    SOURCE_FAILED: "#FECACA",     # Red - failed
    SOURCE_MANUAL: "#DDD6FE",     # Purple - manual edit
    SOURCE_ATTENTION: "#FED7AA",  # Orange - needs attention
    SOURCE_RESERVED: "#D1D5DB",   # Gray - reserved/system
}

# Text colors for translation source types
TEXT_COLORS: Dict[str, str] = {
    SOURCE_NEW: "#854D0E",        # Dark yellow/brown
    SOURCE_ZENDESK_DC: "#166534",  # Dark green
    SOURCE_TRANSLATED: "#1E40AF",  # Dark blue
    SOURCE_CACHE: "#3730A3",      # Dark indigo
    SOURCE_FAILED: "#991B1B",     # Dark red
    SOURCE_MANUAL: "#6B21A8",     # Dark purple
    SOURCE_ATTENTION: "#C2410C",  # Dark orange
    SOURCE_RESERVED: "#4B5563",   # Dark gray
}

# Placeholder source colors
PLACEHOLDER_COLORS: Dict[str, str] = {
    'existing': '#CFFAFE',   # Cyan - existing DC from Zendesk
    'proposed': '#F1F5F9',   # Slate - proposed/will be created
}

# Placeholder text colors
PLACEHOLDER_TEXT_COLORS: Dict[str, str] = {
    'existing': '#0E7490',   # Dark cyan
    'proposed': '#475569',   # Dark slate
}

# Log console colors
LOG_COLORS: Dict[str, str] = {
    'background': '#0D1117',
    'text': '#10B981',
    'border': '#30363D',
    'selection_bg': '#1F6FEB',
}


# ==============================================================================
# COMMON SHORT WORDS (to avoid false positives in acronym detection)
# ==============================================================================


COMMON_SHORT_WORDS: FrozenSet[str] = frozenset({
    'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YES', 'NO',
    'ALL', 'ANY', 'CAN', 'HAD', 'HER', 'WAS', 'ONE', 'OUR',
    'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS',
    'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'BOY',
    'DID', 'OWN', 'SAY', 'SHE', 'TOO', 'USE',
})


# ==============================================================================
# TRANSLATABLE SHORT WORDS (short words that SHOULD be translated)
# ==============================================================================


TRANSLATABLE_SHORT_WORDS: FrozenSet[str] = frozenset({
    'YES', 'NO', 'OK', 'HI', 'BYE',
    'SIM', 'NÃO',
    'OUI', 'NON',
    'SÍ',
    'JA', 'NEIN',
})


# ==============================================================================
# LOCALE MAPPINGS (Zendesk locale ID to locale string)
# ==============================================================================


# Locale IDs used for Dynamic Content variant creation.
# pt-BR = 1176 (verified via Zendesk API).
# en-US = 1, es = 2 (standard Zendesk IDs).
LOCALE_ID_PT_BR: int = 1176
LOCALE_ID_EN_US: int = 1
LOCALE_ID_ES: int = 2

LOCALE_ID_MAP: Dict[int, str] = {
    1: 'en-US', 2: 'es', 3: 'de', 4: 'fr', 5: 'it',
    6: 'nl', 7: 'pl', 8: 'pt-BR', 9: 'zh-CN', 10: 'ja',
    11: 'ko', 12: 'ru', 13: 'sv', 14: 'no', 15: 'da',
    16: 'fr', 17: 'ar', 18: 'he', 19: 'tr', 20: 'cs',
    21: 'hu', 22: 'th', 23: 'id', 24: 'uk', 25: 'vi',
    26: 'pt', 27: 'zh-TW', 28: 'ms', 29: 'ca', 30: 'sk',
    31: 'el', 32: 'bg', 33: 'ro', 34: 'hr', 35: 'sl',
    36: 'lt', 37: 'lv', 38: 'et',
    1000: 'en', 1001: 'en-GB', 1002: 'en-AU', 1003: 'en-CA',
    1004: 'es-ES', 1005: 'es-MX', 1006: 'es-419',
    1007: 'fr-CA', 1008: 'fr-FR', 1009: 'de-AT', 1010: 'de-CH',
    1011: 'nl-BE', 1012: 'pt-PT', 1176: 'pt-BR',
    1013: 'en-NZ', 1014: 'en-IE', 1015: 'en-ZA',
    1016: 'es-AR', 1017: 'es-CL', 1018: 'es-CO',
    1019: 'fr-BE', 1020: 'fr-CH', 1021: 'de-DE',
    1022: 'it-CH', 1023: 'nl-NL',
}