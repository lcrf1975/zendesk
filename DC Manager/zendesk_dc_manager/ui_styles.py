"""
UI styling constants and functions for Zendesk DC Manager.

This module provides:
- Centralized color definitions (imports from config.py)
- Platform-specific font detection
- Main application stylesheet
"""

import sys

from PyQt6.QtGui import QColor

from zendesk_dc_manager.config import (
    SOURCE_NEW,
    SOURCE_ZENDESK_DC,
    SOURCE_TRANSLATED,
    SOURCE_CACHE,
    SOURCE_FAILED,
    SOURCE_MANUAL,
    SOURCE_ATTENTION,
    SOURCE_RESERVED,
    SOURCE_COLORS,
    TEXT_COLORS,
    PLACEHOLDER_COLORS,
    PLACEHOLDER_TEXT_COLORS,
    LOG_COLORS,
)


# ==============================================================================
# FONT HELPERS
# ==============================================================================


def get_platform_font() -> str:
    """Get the appropriate font family for the current platform."""
    if sys.platform == 'darwin':
        return 'Helvetica Neue'
    elif sys.platform == 'win32':
        return 'Segoe UI'
    else:
        return 'DejaVu Sans'


def get_monospace_font() -> str:
    """Get the appropriate monospace font for the current platform."""
    if sys.platform == 'darwin':
        return 'Menlo'
    elif sys.platform == 'win32':
        return 'Consolas'
    else:
        return 'DejaVu Sans Mono'


# ==============================================================================
# COLOR HELPERS
# ==============================================================================


def get_source_color(source: str) -> QColor:
    """Get background color for a source type."""
    hex_color = SOURCE_COLORS.get(source, SOURCE_COLORS[SOURCE_NEW])
    return QColor(hex_color)


def get_text_color(source: str) -> QColor:
    """Get text color for a source type."""
    hex_color = TEXT_COLORS.get(source, TEXT_COLORS[SOURCE_NEW])
    return QColor(hex_color)


def get_placeholder_color(source: str) -> QColor:
    """Get background color for a placeholder source type."""
    hex_color = PLACEHOLDER_COLORS.get(source, PLACEHOLDER_COLORS['proposed'])
    return QColor(hex_color)


def get_placeholder_text_color(source: str) -> QColor:
    """Get text color for a placeholder source type."""
    hex_color = PLACEHOLDER_TEXT_COLORS.get(
        source, PLACEHOLDER_TEXT_COLORS['proposed']
    )
    return QColor(hex_color)


# ==============================================================================
# MAIN STYLESHEET
# ==============================================================================


def get_main_stylesheet() -> str:
    """Get main application stylesheet."""
    font_family = get_platform_font()

    return f"""
        QMainWindow {{
            background-color: #F9FAFB;
        }}

        QWidget {{
            font-family: "{font_family}";
            font-size: 13px;
        }}

        QGroupBox {{
            font-weight: 600;
            border: 1px solid #E5E7EB;
            border-radius: 8px;
            margin-top: 12px;
            padding: 16px;
            background-color: #FFFFFF;
        }}

        QGroupBox::title {{
            subcontrol-origin: margin;
            left: 12px;
            padding: 0 8px;
            color: #374151;
        }}

        QLineEdit, QTextEdit, QSpinBox, QComboBox {{
            border: 1px solid #D1D5DB;
            border-radius: 6px;
            padding: 8px 12px;
            background-color: #FFFFFF;
            selection-background-color: #3B82F6;
        }}

        QLineEdit:focus, QTextEdit:focus, QSpinBox:focus, QComboBox:focus {{
            border-color: #3B82F6;
            outline: none;
        }}

        QLineEdit:disabled, QTextEdit:disabled {{
            background-color: #F3F4F6;
            color: #9CA3AF;
        }}

        QPushButton {{
            background-color: #FFFFFF;
            border: 1px solid #D1D5DB;
            border-radius: 6px;
            padding: 8px 16px;
            font-weight: 500;
            color: #374151;
        }}

        QPushButton:hover {{
            background-color: #F9FAFB;
            border-color: #9CA3AF;
        }}

        QPushButton:pressed {{
            background-color: #F3F4F6;
        }}

        QPushButton:disabled {{
            background-color: #F3F4F6;
            color: #9CA3AF;
            border-color: #E5E7EB;
        }}

        QCheckBox {{
            spacing: 8px;
            color: #374151;
        }}

        QCheckBox::indicator {{
            width: 18px;
            height: 18px;
            border-radius: 4px;
            border: 1px solid #D1D5DB;
            background-color: #FFFFFF;
        }}

        QCheckBox::indicator:checked {{
            background-color: #3B82F6;
            border-color: #3B82F6;
            image: url(data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxMiIgaGVpZ2h0PSIxMiIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjMiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCI+PHBvbHlsaW5lIHBvaW50cz0iMjAgNiA5IDE3IDQgMTIiPjwvcG9seWxpbmU+PC9zdmc+);
        }}

        QCheckBox::indicator:hover {{
            border-color: #9CA3AF;
        }}

        QComboBox::drop-down {{
            border: none;
            width: 30px;
        }}

        QComboBox::down-arrow {{
            image: none;
            border-left: 5px solid transparent;
            border-right: 5px solid transparent;
            border-top: 6px solid #6B7280;
            margin-right: 10px;
        }}

        QComboBox QAbstractItemView {{
            border: 1px solid #D1D5DB;
            border-radius: 6px;
            background-color: #FFFFFF;
            selection-background-color: #EFF6FF;
            selection-color: #1E40AF;
        }}

        QSpinBox::up-button, QSpinBox::down-button {{
            width: 20px;
            border: none;
            background-color: #F3F4F6;
        }}

        QSpinBox::up-button:hover, QSpinBox::down-button:hover {{
            background-color: #E5E7EB;
        }}

        QSplitter::handle {{
            background-color: #E5E7EB;
            height: 2px;
        }}

        QSplitter::handle:hover {{
            background-color: #D1D5DB;
        }}

        QTableWidget {{
            gridline-color: #E5E7EB;
            background-color: #FFFFFF;
            alternate-background-color: #F9FAFB;
            selection-background-color: #EFF6FF;
            selection-color: #1E40AF;
            border: 1px solid #E5E7EB;
            border-radius: 8px;
        }}

        QTableWidget::item {{
            padding: 8px;
            border-bottom: 1px solid #F3F4F6;
        }}

        QTableWidget::item:selected {{
            background-color: #EFF6FF;
            color: #1E40AF;
        }}

        QHeaderView::section {{
            background-color: #F9FAFB;
            color: #374151;
            font-weight: 600;
            padding: 10px 8px;
            border: none;
            border-bottom: 2px solid #E5E7EB;
            border-right: 1px solid #E5E7EB;
        }}

        QHeaderView::section:last {{
            border-right: none;
        }}

        QScrollBar:vertical {{
            background-color: #F3F4F6;
            width: 12px;
            border-radius: 6px;
            margin: 2px;
        }}

        QScrollBar::handle:vertical {{
            background-color: #D1D5DB;
            border-radius: 5px;
            min-height: 30px;
        }}

        QScrollBar::handle:vertical:hover {{
            background-color: #9CA3AF;
        }}

        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            height: 0px;
        }}

        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
            background: none;
        }}

        QScrollBar:horizontal {{
            background-color: #F3F4F6;
            height: 12px;
            border-radius: 6px;
            margin: 2px;
        }}

        QScrollBar::handle:horizontal {{
            background-color: #D1D5DB;
            border-radius: 5px;
            min-width: 30px;
        }}

        QScrollBar::handle:horizontal:hover {{
            background-color: #9CA3AF;
        }}

        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
            width: 0px;
        }}

        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{
            background: none;
        }}

        QToolTip {{
            background-color: #1F2937;
            color: #F9FAFB;
            border: none;
            border-radius: 4px;
            padding: 6px 10px;
            font-size: 12px;
        }}

        QLabel {{
            color: #374151;
        }}
    """


# Alias for backward compatibility
def load_application_stylesheet() -> str:
    """Alias for get_main_stylesheet() for backward compatibility."""
    return get_main_stylesheet()