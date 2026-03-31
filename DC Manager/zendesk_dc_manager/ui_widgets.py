"""
Custom widgets for Zendesk DC Manager UI.

This module provides:
- StepWorker: Thread worker for long-running operations
- DarkConsoleWidget: Log output console with dark theme
- EmbeddedStatusBar: Progress bar with cancel button and ETA
- TableContainer: Container for table widget
- PreviewTableWidget: Async-loading table for preview data
"""

import threading
from typing import Optional, List, Dict, Any

from PyQt6.QtCore import (
    Qt, QTimer, pyqtSignal, QThread, QElapsedTimer,
)
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QProgressBar, QTableWidget,
    QTableWidgetItem, QHeaderView, QCheckBox,
    QAbstractItemView, QTextEdit, QStyledItemDelegate,
    QStyleOptionViewItem, QStyle,
)
from PyQt6.QtGui import QBrush, QFontMetrics

from zendesk_dc_manager.config import (
    UI_CONFIG,
    LOG_COLORS,
    SOURCE_NEW,
    SOURCE_ZENDESK_DC,
    SOURCE_TRANSLATED,
    SOURCE_CACHE,
    SOURCE_FAILED,
    SOURCE_MANUAL,
    SOURCE_ATTENTION,
    SOURCE_RESERVED,
)
from zendesk_dc_manager.ui_styles import (
    get_monospace_font,
    get_source_color as _get_source_color,
    get_text_color as _get_text_color,
    get_placeholder_color as _get_placeholder_color,
    get_placeholder_text_color as _get_placeholder_text_color,
)


# ==============================================================================
# WORKER THREAD
# ==============================================================================


class StepWorker(QThread):
    """Worker thread for long-running operations."""

    progress = pyqtSignal(int, int, str)
    log = pyqtSignal(str)
    result = pyqtSignal(bool, object)

    def __init__(self, task_func, parent=None):
        super().__init__(parent)
        self._task_func = task_func
        self._canceled = False
        self._cancel_lock = threading.Lock()

    def run(self):
        try:
            result = self._task_func(self.progress, self.log)
            if not self._is_canceled():
                self.result.emit(True, result)
        except Exception as e:
            if not self._is_canceled():
                self.result.emit(False, str(e))

    def cancel(self):
        with self._cancel_lock:
            self._canceled = True

    def _is_canceled(self) -> bool:
        with self._cancel_lock:
            return self._canceled


# ==============================================================================
# DARK CONSOLE WIDGET
# ==============================================================================


class DarkConsoleWidget(QWidget):
    """Console widget with dark background and clear button."""

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)

        title = QLabel("Log Output")
        title.setStyleSheet("font-weight: bold; color: #374151;")
        header.addWidget(title)

        header.addStretch()

        self.btn_clear = QPushButton("Clear Log")
        self.btn_clear.setMinimumWidth(90)
        self.btn_clear.setFixedHeight(26)
        self.btn_clear.setStyleSheet("""
            QPushButton {
                background-color: #4B5563;
                color: white;
                border: none;
                border-radius: 4px;
                font-size: 11px;
                padding: 4px 12px;
            }
            QPushButton:hover {
                background-color: #6B7280;
            }
        """)
        self.btn_clear.clicked.connect(self.clear)
        header.addWidget(self.btn_clear)

        layout.addLayout(header)

        monospace_font = get_monospace_font()

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setStyleSheet(f"""
            QTextEdit {{
                background-color: {LOG_COLORS['background']};
                color: {LOG_COLORS['text']};
                font-family: "{monospace_font}";
                font-size: 11px;
                border: 1px solid {LOG_COLORS['border']};
                border-radius: 4px;
                padding: 8px;
            }}
            QScrollBar:vertical {{
                background-color: #2a2a2a;
                width: 14px;
                border: none;
            }}
            QScrollBar::handle:vertical {{
                background-color: #555;
                min-height: 30px;
                border-radius: 4px;
                margin: 2px;
            }}
            QScrollBar::handle:vertical:hover {{
                background-color: #666;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0px;
            }}
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
                background-color: #2a2a2a;
            }}
        """)
        layout.addWidget(self.text_edit)

    def append(self, text: str):
        """Append text to the console and scroll to bottom."""
        self.text_edit.append(text)
        scrollbar = self.text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def clear(self):
        """Clear all text from the console."""
        self.text_edit.clear()

    def toPlainText(self) -> str:
        """Get all text from the console."""
        return self.text_edit.toPlainText()


# ==============================================================================
# EMBEDDED STATUS BAR
# ==============================================================================


class EmbeddedStatusBar(QWidget):
    """Custom status bar with progress and cancel button."""

    cancel_clicked = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("embeddedStatusBar")
        self.setFixedHeight(UI_CONFIG.STATUS_BAR_HEIGHT)

        self.setStyleSheet("""
            QWidget#embeddedStatusBar {
                background-color: #F3F4F6;
                border-top: 1px solid #E5E7EB;
            }
        """)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 8, 16, 8)
        layout.setSpacing(12)

        self.lbl_status = QLabel("Ready")
        self.lbl_status.setStyleSheet(
            "background: transparent; color: #374151; font-weight: 500;"
        )
        layout.addWidget(self.lbl_status)

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedWidth(200)
        self.progress_bar.setFixedHeight(8)
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: none;
                border-radius: 4px;
                background-color: #E5E7EB;
            }
            QProgressBar::chunk {
                background-color: #3B82F6;
                border-radius: 4px;
            }
        """)
        layout.addWidget(self.progress_bar)

        self.lbl_progress_detail = QLabel("")
        self.lbl_progress_detail.setStyleSheet(
            "background: transparent; color: #6B7280; font-size: 12px;"
        )
        layout.addWidget(self.lbl_progress_detail)

        self.lbl_detail = QLabel("")
        self.lbl_detail.setStyleSheet(
            "background: transparent; color: #6B7280; font-size: 12px;"
        )
        layout.addWidget(self.lbl_detail)

        layout.addStretch()

        self.lbl_timer = QLabel("")
        self.lbl_timer.setStyleSheet(
            "background: transparent; color: #6B7280; font-size: 12px;"
        )
        layout.addWidget(self.lbl_timer)

        self.lbl_eta = QLabel("")
        self.lbl_eta.setStyleSheet(
            "background: transparent; color: #059669; "
            "font-size: 12px; font-weight: 500;"
        )
        layout.addWidget(self.lbl_eta)

        self.btn_cancel = QPushButton("Cancel")
        self.btn_cancel.setFixedWidth(80)
        self.btn_cancel.setVisible(False)
        self.btn_cancel.setStyleSheet("""
            QPushButton {
                background-color: #EF4444;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #DC2626;
            }
        """)
        self.btn_cancel.clicked.connect(self.cancel_clicked.emit)
        layout.addWidget(self.btn_cancel)

        self._elapsed_timer = QElapsedTimer()
        self._update_timer = QTimer()
        self._update_timer.timeout.connect(self._update_elapsed)

        self._last_current = 0
        self._last_total = 0

    def show_progress(
        self, current: int, total: int, status: str, detail: str
    ):
        """Show progress information."""
        self.lbl_status.setText(status)
        self.lbl_detail.setText(detail)

        self._last_current = current
        self._last_total = total

        if total > 0:
            self.progress_bar.setMaximum(total)
            self.progress_bar.setValue(current)
            self.progress_bar.setVisible(True)
            self.lbl_progress_detail.setText(f"{current} / {total}")
            self._update_eta(current, total)
        else:
            self.progress_bar.setMaximum(0)
            self.progress_bar.setVisible(True)
            self.lbl_progress_detail.setText("")
            self.lbl_eta.setText("")

        self.btn_cancel.setVisible(True)

        if not self._elapsed_timer.isValid():
            self._elapsed_timer.start()
            self._update_timer.start(1000)

    def _update_elapsed(self):
        """Update elapsed time display."""
        if self._elapsed_timer.isValid():
            elapsed_ms = self._elapsed_timer.elapsed()
            self.lbl_timer.setText(f"Elapsed: {self._format_time(elapsed_ms)}")

            if self._last_total > 0:
                self._update_eta(self._last_current, self._last_total)

    def _update_eta(self, current: int, total: int):
        """Calculate and update ETA display."""
        if current <= 0 or not self._elapsed_timer.isValid():
            self.lbl_eta.setText("")
            return

        elapsed_ms = self._elapsed_timer.elapsed()
        if elapsed_ms <= 0:
            self.lbl_eta.setText("")
            return

        rate = current / (elapsed_ms / 1000.0)
        remaining = total - current

        if rate > 0 and remaining > 0:
            eta_seconds = remaining / rate
            eta_ms = int(eta_seconds * 1000)
            self.lbl_eta.setText(f"ETA: {self._format_time(eta_ms)}")
        elif remaining <= 0:
            self.lbl_eta.setText("Finishing...")
        else:
            self.lbl_eta.setText("")

    def _format_time(self, milliseconds: int) -> str:
        """Format milliseconds as MM:SS or HH:MM:SS."""
        seconds = milliseconds // 1000
        minutes = seconds // 60
        hours = minutes // 60

        seconds = seconds % 60
        minutes = minutes % 60

        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"

    def finish(self, message: str, success: bool = True):
        """Show completion message."""
        self.lbl_status.setText(message)
        self.lbl_detail.setText("")
        self.lbl_progress_detail.setText("")
        self.lbl_eta.setText("")
        self.progress_bar.setVisible(False)
        self.btn_cancel.setVisible(False)

        color = "#059669" if success else "#DC2626"
        self.lbl_status.setStyleSheet(
            f"background: transparent; color: {color}; font-weight: 500;"
        )

        self._update_timer.stop()
        if self._elapsed_timer.isValid():
            elapsed_ms = self._elapsed_timer.elapsed()
            self.lbl_timer.setText(
                f"Completed in: {self._format_time(elapsed_ms)}"
            )
        self._elapsed_timer.invalidate()

        self._last_current = 0
        self._last_total = 0

    def reset_ui(self):
        """Reset to initial state."""
        self.lbl_status.setText("Ready")
        self.lbl_status.setStyleSheet(
            "background: transparent; color: #374151; font-weight: 500;"
        )
        self.lbl_detail.setText("")
        self.lbl_progress_detail.setText("")
        self.lbl_timer.setText("")
        self.lbl_eta.setText("")
        self.progress_bar.setVisible(False)
        self.btn_cancel.setVisible(False)

        self._update_timer.stop()
        self._elapsed_timer.invalidate()

        self._last_current = 0
        self._last_total = 0

    def stop_timer(self):
        """Stop the elapsed timer."""
        self._update_timer.stop()


# ==============================================================================
# TABLE CONTAINER
# ==============================================================================


class TableContainer(QWidget):
    """Container widget for the table with loading indicator."""

    def __init__(self, table: QTableWidget, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.table = table
        layout.addWidget(table)


# ==============================================================================
# PREVIEW TABLE WIDGET
# ==============================================================================


class _CellColorDelegate(QStyledItemDelegate):
    """Forces Qt to honour setBackground()/setForeground() on table cells
    even when a QSS ::item rule is active (which normally suppresses them)."""

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        bg = index.data(Qt.ItemDataRole.BackgroundRole)
        if bg is not None:
            option.backgroundBrush = (
                bg if isinstance(bg, QBrush) else QBrush(bg)
            )
        fg = index.data(Qt.ItemDataRole.ForegroundRole)
        if fg is not None:
            palette = option.palette
            palette.setBrush(
                palette.ColorRole.Text,
                fg if isinstance(fg, QBrush) else QBrush(fg),
            )
            option.palette = palette

    def paint(self, painter, option, index):
        bg = index.data(Qt.ItemDataRole.BackgroundRole)
        if bg is None:
            super().paint(painter, option, index)
            return

        # Qt6's QStyleSheetStyle repaints the background during drawControl
        # even after initStyleOption sets backgroundBrush, overriding it with
        # QSS/alternating colors. We bypass this by drawing manually.
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)

        painter.save()

        is_selected = bool(opt.state & QStyle.StateFlag.State_Selected)

        if is_selected:
            painter.fillRect(opt.rect, opt.palette.highlight())
            text_color = opt.palette.highlightedText().color()
        else:
            brush = bg if isinstance(bg, QBrush) else QBrush(bg)
            painter.fillRect(opt.rect, brush)
            text_color = opt.palette.text().color()

        text = index.data(Qt.ItemDataRole.DisplayRole) or ''
        if text:
            painter.setPen(text_color)
            text_rect = opt.rect.adjusted(8, 0, -8, 0)
            fm = QFontMetrics(opt.font)
            elided = fm.elidedText(
                text, Qt.TextElideMode.ElideRight, text_rect.width()
            )
            painter.drawText(
                text_rect,
                Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft,
                elided,
            )

        painter.restore()


class PreviewTableWidget(QTableWidget):
    """Table widget for preview data with async loading."""

    stats_updated = pyqtSignal(dict)
    cell_edited = pyqtSignal(int, str, str, str)
    selection_changed = pyqtSignal(int)
    loading_finished = pyqtSignal()

    COLUMNS = [
        ("select", "✓", 40),
        ("context", "Context", 100),
        ("type", "Type", 120),
        ("obj_id", "ID", 80),
        ("placeholder", "Placeholder", 180),
        ("pt", "Portuguese (PT)", 200),
        ("en", "English (EN)", 200),
        ("es", "Spanish (ES)", 200),
        ("action", "Action", 80),
    ]

    def __init__(self, parent=None):
        super().__init__(parent)

        self._data: List[Dict[str, Any]] = []
        self._loading = False
        self._load_timer: Optional[QTimer] = None
        self._load_index = 0
        self._batch_size = UI_CONFIG.TABLE_BATCH_SIZE

        self._selection_state: Dict[int, bool] = {}
        self._search_cache: Dict[int, str] = {}  # data_index -> lowercased searchable text

        # Filter settings
        self._filter_fields = True
        self._filter_forms = True
        self._filter_statuses = True
        self._filter_user_fields = True
        self._filter_org_fields = True
        self._filter_groups = True
        self._filter_macros = True
        self._filter_triggers = True
        self._filter_automations = True
        self._filter_views = True
        self._filter_sla = True
        self._filter_hc_cats = True
        self._filter_hc_sects = True
        self._filter_hc_arts = True
        self._filter_reserved = True

        self._setup_table()

    def _setup_table(self):
        """Initialize table structure and styling."""
        self.setColumnCount(len(self.COLUMNS))
        self.setHorizontalHeaderLabels([col[1] for col in self.COLUMNS])

        header = self.horizontalHeader()
        for i, (_, _, width) in enumerate(self.COLUMNS):
            if width > 0:
                self.setColumnWidth(i, width)

        header.setStretchLastSection(False)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.Stretch)

        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows
        )
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.verticalHeader().setVisible(False)
        self.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked)

        self.cellChanged.connect(self._on_cell_changed)

        # Apply delegate so setBackground()/setForeground() show through QSS
        self.setItemDelegate(_CellColorDelegate(self))

    def set_filter_settings(
        self,
        show_fields: bool = True,
        show_forms: bool = True,
        show_statuses: bool = True,
        show_user_fields: bool = True,
        show_org_fields: bool = True,
        show_groups: bool = True,
        show_macros: bool = True,
        show_triggers: bool = True,
        show_automations: bool = True,
        show_views: bool = True,
        show_sla: bool = True,
        show_hc_cats: bool = True,
        show_hc_sects: bool = True,
        show_hc_arts: bool = True,
        show_reserved: bool = True,
    ):
        """Set filter settings before loading data."""
        self._filter_fields = show_fields
        self._filter_forms = show_forms
        self._filter_statuses = show_statuses
        self._filter_user_fields = show_user_fields
        self._filter_org_fields = show_org_fields
        self._filter_groups = show_groups
        self._filter_macros = show_macros
        self._filter_triggers = show_triggers
        self._filter_automations = show_automations
        self._filter_views = show_views
        self._filter_sla = show_sla
        self._filter_hc_cats = show_hc_cats
        self._filter_hc_sects = show_hc_sects
        self._filter_hc_arts = show_hc_arts
        self._filter_reserved = show_reserved

    def start_async_load(
        self,
        data: List[Dict[str, Any]],
        mode: str = "preview",
        batch_size: int = None,
        selection_state: Dict[int, bool] = None
    ):
        """Start loading data asynchronously."""
        self.cancel_loading()

        self._data = data
        self._load_index = 0
        self._batch_size = batch_size or UI_CONFIG.TABLE_BATCH_SIZE
        self._loading = True

        if selection_state is not None:
            self._selection_state = selection_state.copy()
        else:
            self._selection_state = {}

        self._search_cache = {}
        self.setRowCount(0)
        self.blockSignals(True)

        self._load_timer = QTimer()
        self._load_timer.timeout.connect(self._load_batch)
        self._load_timer.start(UI_CONFIG.TABLE_INSERT_INTERVAL_MS)

    def _load_batch(self):
        """Load a batch of rows."""
        if not self._loading or self._load_index >= len(self._data):
            self._finish_loading()
            return

        end_index = min(
            self._load_index + UI_CONFIG.TABLE_INSERT_BATCH,
            len(self._data)
        )

        for i in range(self._load_index, end_index):
            self._add_row(i, self._data[i])

        self._load_index = end_index

    def _build_search_string(self, item: Dict[str, Any]) -> str:
        """Build a lowercased searchable string for a work item."""
        return '|'.join([
            item.get('context', ''),
            item.get('type_display', item.get('type', '')),
            str(item.get('obj_id', '')),
            item.get('dc_placeholder', '') or '',
            item.get('pt', '') or '',
            item.get('en', '') or '',
            item.get('es', '') or '',
        ]).lower()

    def _add_row(self, data_index: int, item: Dict[str, Any]):
        """Add a single row to the table."""
        row = self.rowCount()
        self.insertRow(row)
        self.setRowHeight(row, UI_CONFIG.TABLE_ROW_HEIGHT)

        is_reserved = (
            item.get('is_reserved', False) or item.get('is_system', False)
        )

        # Column 0: Selection checkbox
        chk_widget = QWidget()
        chk_layout = QHBoxLayout(chk_widget)
        chk_layout.setContentsMargins(0, 0, 0, 0)
        chk_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        chk = QCheckBox()
        chk.setChecked(self._selection_state.get(data_index, False))
        chk.stateChanged.connect(
            lambda state, r=row: self._on_selection_changed(r, state)
        )
        if is_reserved:
            chk.setEnabled(False)
            chk.setToolTip("System/reserved items cannot be modified")
        chk_layout.addWidget(chk)
        self.setCellWidget(row, 0, chk_widget)

        # Column 1: Context
        context_item = QTableWidgetItem(item.get('context', ''))
        context_item.setFlags(
            context_item.flags() & ~Qt.ItemFlag.ItemIsEditable
        )
        self.setItem(row, 1, context_item)

        # Column 2: Type
        type_item = QTableWidgetItem(
            item.get('type_display', item.get('type', ''))
        )
        type_item.setFlags(type_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.setItem(row, 2, type_item)

        # Column 3: ID
        id_item = QTableWidgetItem(str(item.get('obj_id', '')))
        id_item.setFlags(id_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.setItem(row, 3, id_item)

        # Column 4: Placeholder
        placeholder_text = item.get('dc_placeholder', '') or ''
        placeholder_source = item.get('placeholder_source', 'proposed')
        placeholder_item = QTableWidgetItem(placeholder_text)
        placeholder_item.setFlags(
            placeholder_item.flags() & ~Qt.ItemFlag.ItemIsEditable
        )

        if placeholder_text:
            placeholder_item.setBackground(
                QBrush(_get_placeholder_color(placeholder_source))
            )
            placeholder_item.setForeground(
                QBrush(_get_placeholder_text_color(placeholder_source))
            )

            if placeholder_source == 'existing':
                placeholder_item.setToolTip(f"Existing DC: {placeholder_text}")
            else:
                placeholder_item.setToolTip(f"Proposed: {placeholder_text}")

        self.setItem(row, 4, placeholder_item)

        # Determine source colors
        if is_reserved:
            pt_source = SOURCE_RESERVED
            en_source = SOURCE_RESERVED
            es_source = SOURCE_RESERVED
        else:
            pt_source = item.get('pt_source', SOURCE_NEW)
            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

        # Column 5: Portuguese (PT)
        pt_text = item.get('pt', '')
        pt_item = QTableWidgetItem(pt_text)
        pt_item.setBackground(QBrush(_get_source_color(pt_source)))
        pt_item.setForeground(QBrush(_get_text_color(pt_source)))
        if is_reserved:
            pt_item.setFlags(pt_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            pt_item.setToolTip("System/reserved item - cannot be modified")
        self.setItem(row, 5, pt_item)

        # Column 6: English (EN)
        en_text = item.get('en', '')
        en_item = QTableWidgetItem(en_text)
        en_item.setBackground(QBrush(_get_source_color(en_source)))
        en_item.setForeground(QBrush(_get_text_color(en_source)))
        if is_reserved:
            en_item.setFlags(en_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            en_item.setToolTip("System/reserved item - cannot be modified")
        self.setItem(row, 6, en_item)

        # Column 7: Spanish (ES)
        es_text = item.get('es', '')
        es_item = QTableWidgetItem(es_text)
        es_item.setBackground(QBrush(_get_source_color(es_source)))
        es_item.setForeground(QBrush(_get_text_color(es_source)))
        if is_reserved:
            es_item.setFlags(es_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            es_item.setToolTip("System/reserved item - cannot be modified")
        self.setItem(row, 7, es_item)

        # Column 8: Action
        action_text = "SYSTEM" if is_reserved else item.get('action', '')
        action_item = QTableWidgetItem(action_text)
        action_item.setFlags(action_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        if is_reserved:
            action_item.setForeground(QBrush(_get_text_color(SOURCE_RESERVED)))
        self.setItem(row, 8, action_item)

        # Store data index in type column for reference
        if self.item(row, 2):
            self.item(row, 2).setData(Qt.ItemDataRole.UserRole, data_index)

        self._search_cache[data_index] = self._build_search_string(item)

    def _finish_loading(self):
        """Finish the loading process."""
        self._loading = False
        if self._load_timer:
            self._load_timer.stop()
            self._load_timer = None

        self.blockSignals(False)
        self._update_stats()
        self.loading_finished.emit()

    def cancel_loading(self):
        """Cancel ongoing loading."""
        self._loading = False
        if self._load_timer:
            self._load_timer.stop()
            self._load_timer = None
        self.blockSignals(False)

    def _on_cell_changed(self, row: int, column: int):
        """Handle cell edit."""
        if self._loading:
            return

        field_map = {5: 'pt', 6: 'en', 7: 'es'}
        if column not in field_map:
            return

        field = field_map[column]
        item = self.item(row, column)
        if not item:
            return

        value = item.text()

        item.setBackground(QBrush(_get_source_color(SOURCE_MANUAL)))
        item.setForeground(QBrush(_get_text_color(SOURCE_MANUAL)))

        type_item = self.item(row, 2)
        if type_item:
            data_index = type_item.data(Qt.ItemDataRole.UserRole)
            if data_index is not None:
                self.cell_edited.emit(data_index, field, value, SOURCE_MANUAL)
                if data_index < len(self._data):
                    self._search_cache[data_index] = self._build_search_string(
                        self._data[data_index]
                    )

    def _on_selection_changed(self, row: int, state: int):
        """Handle selection checkbox change."""
        type_item = self.item(row, 2)
        if type_item:
            data_index = type_item.data(Qt.ItemDataRole.UserRole)
            if data_index is not None:
                self._selection_state[data_index] = (
                    state == Qt.CheckState.Checked.value
                )

        self.selection_changed.emit(self.get_selected_count())

    def _update_stats(self):
        """Update and emit statistics."""
        stats = {
            'total': len(self._data),
            'items_from_dc': 0,
            'items_translated': 0,
            'items_failed': 0,
            'items_attention': 0,
            'items_reserved': 0,
            'items_pending': 0,
            'selected_count': self.get_selected_count(),
        }

        # Define which sources count as "translated" (have a result)
        translated_sources = frozenset({
            SOURCE_TRANSLATED,
            SOURCE_CACHE,
            SOURCE_MANUAL,
            SOURCE_ATTENTION,
        })

        for item in self._data:
            is_reserved = (
                item.get('is_reserved', False) or item.get('is_system', False)
            )

            if is_reserved:
                stats['items_reserved'] += 1
                continue

            source = item.get('source', SOURCE_NEW)
            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

            # Check if item came from existing DC (already has translations)
            if source == SOURCE_ZENDESK_DC:
                stats['items_from_dc'] += 1
            # Check for any translation failures
            elif en_source == SOURCE_FAILED or es_source == SOURCE_FAILED:
                stats['items_failed'] += 1
            # Check if any translation result is identical to PT (needs review)
            elif en_source == SOURCE_ATTENTION or es_source == SOURCE_ATTENTION:
                stats['items_attention'] += 1
            # Check if both EN and ES are translated
            elif (en_source in translated_sources and
                  es_source in translated_sources):
                stats['items_translated'] += 1
            # Otherwise it's still pending
            else:
                stats['items_pending'] += 1

        self.stats_updated.emit(stats)

    def apply_filters(
        self,
        show_fields: bool = True,
        show_forms: bool = True,
        show_statuses: bool = True,
        show_user_fields: bool = True,
        show_org_fields: bool = True,
        show_groups: bool = True,
        show_macros: bool = True,
        show_triggers: bool = True,
        show_automations: bool = True,
        show_views: bool = True,
        show_sla: bool = True,
        show_hc_cats: bool = True,
        show_hc_sects: bool = True,
        show_hc_arts: bool = True,
        show_reserved: bool = True,
        search_text: str = '',
    ):
        """Apply visibility filters to rows based on object type and search text."""
        self._filter_fields = show_fields
        self._filter_forms = show_forms
        self._filter_statuses = show_statuses
        self._filter_user_fields = show_user_fields
        self._filter_org_fields = show_org_fields
        self._filter_groups = show_groups
        self._filter_macros = show_macros
        self._filter_triggers = show_triggers
        self._filter_automations = show_automations
        self._filter_views = show_views
        self._filter_sla = show_sla
        self._filter_hc_cats = show_hc_cats
        self._filter_hc_sects = show_hc_sects
        self._filter_hc_arts = show_hc_arts
        self._filter_reserved = show_reserved

        TYPE_VISIBLE = {
            'ticket_field': show_fields,
            'ticket_field_option': show_fields,
            'ticket_form': show_forms,
            'custom_status': show_statuses,
            'user_field': show_user_fields,
            'user_field_option': show_user_fields,
            'organization_field': show_org_fields,
            'organization_field_option': show_org_fields,
            'group': show_groups,
            'macro': show_macros,
            'trigger': show_triggers,
            'automation': show_automations,
            'view': show_views,
            'sla_policy': show_sla,
            'category': show_hc_cats,
            'section': show_hc_sects,
            'article': show_hc_arts,
        }

        search = search_text.strip().lower()

        for row in range(self.rowCount()):
            type_item = self.item(row, 2)
            if not type_item:
                continue

            data_index = type_item.data(Qt.ItemDataRole.UserRole)
            if data_index is None or data_index >= len(self._data):
                continue

            item = self._data[data_index]
            is_reserved = (
                item.get('is_reserved', False) or item.get('is_system', False)
            )

            if is_reserved:
                type_visible = show_reserved
            else:
                obj_type = item.get('type', '')
                type_visible = TYPE_VISIBLE.get(obj_type, True)

            if not type_visible:
                self.setRowHidden(row, True)
                continue

            if search:
                searchable = self._search_cache.get(data_index, '')
                self.setRowHidden(row, search not in searchable)
            else:
                self.setRowHidden(row, False)

    def get_selected_count(self) -> int:
        """Get count of selected items."""
        return sum(1 for v in self._selection_state.values() if v)

    def get_selected_rows(self) -> List[int]:
        """Get list of selected data indices."""
        return [
            idx for idx, selected in self._selection_state.items() if selected
        ]

    def get_selection_state(self) -> Dict[int, bool]:
        """Get copy of current selection state."""
        return self._selection_state.copy()

    def select_all_visible(self):
        """Select all visible rows (excluding reserved items)."""
        for row in range(self.rowCount()):
            if self.isRowHidden(row):
                continue

            type_item = self.item(row, 2)
            if type_item:
                data_index = type_item.data(Qt.ItemDataRole.UserRole)
                if data_index is not None:
                    item = self._data[data_index]
                    if item.get('is_reserved') or item.get('is_system'):
                        continue
                    self._selection_state[data_index] = True

            widget = self.cellWidget(row, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isEnabled():
                    chk.blockSignals(True)
                    chk.setChecked(True)
                    chk.blockSignals(False)

        self.selection_changed.emit(self.get_selected_count())

    def deselect_all_visible(self):
        """Deselect all visible rows."""
        for row in range(self.rowCount()):
            if self.isRowHidden(row):
                continue

            type_item = self.item(row, 2)
            if type_item:
                data_index = type_item.data(Qt.ItemDataRole.UserRole)
                if data_index is not None:
                    self._selection_state[data_index] = False

            widget = self.cellWidget(row, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk:
                    chk.blockSignals(True)
                    chk.setChecked(False)
                    chk.blockSignals(False)

        self.selection_changed.emit(self.get_selected_count())

    def invert_selection_visible(self):
        """Invert selection for all visible rows (excluding reserved items)."""
        for row in range(self.rowCount()):
            if self.isRowHidden(row):
                continue

            type_item = self.item(row, 2)
            if type_item:
                data_index = type_item.data(Qt.ItemDataRole.UserRole)
                if data_index is not None:
                    item = self._data[data_index]
                    if item.get('is_reserved') or item.get('is_system'):
                        continue
                    current = self._selection_state.get(data_index, False)
                    self._selection_state[data_index] = not current

            widget = self.cellWidget(row, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isEnabled():
                    chk.blockSignals(True)
                    chk.setChecked(not chk.isChecked())
                    chk.blockSignals(False)

        self.selection_changed.emit(self.get_selected_count())

    def update_row_colors(self, row: int, item: Dict[str, Any]):
        """Update colors for a specific row based on item data."""
        is_reserved = (
            item.get('is_reserved', False) or item.get('is_system', False)
        )

        if is_reserved:
            pt_source = SOURCE_RESERVED
            en_source = SOURCE_RESERVED
            es_source = SOURCE_RESERVED
        else:
            pt_source = item.get('pt_source', SOURCE_NEW)
            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

        pt_item = self.item(row, 5)
        if pt_item:
            pt_item.setBackground(QBrush(_get_source_color(pt_source)))
            pt_item.setForeground(QBrush(_get_text_color(pt_source)))

        en_item = self.item(row, 6)
        if en_item:
            en_item.setBackground(QBrush(_get_source_color(en_source)))
            en_item.setForeground(QBrush(_get_text_color(en_source)))

        es_item = self.item(row, 7)
        if es_item:
            es_item.setBackground(QBrush(_get_source_color(es_source)))
            es_item.setForeground(QBrush(_get_text_color(es_source)))

        placeholder_source = item.get('placeholder_source', 'proposed')
        placeholder_item = self.item(row, 4)
        if placeholder_item and placeholder_item.text():
            placeholder_item.setBackground(
                QBrush(_get_placeholder_color(placeholder_source))
            )
            placeholder_item.setForeground(
                QBrush(_get_placeholder_text_color(placeholder_source))
            )

    def update_row_text(self, row: int, item: Dict[str, Any]):
        """Update text for a specific row based on item data."""
        pt_item = self.item(row, 5)
        if pt_item:
            pt_item.setText(item.get('pt', ''))

        en_item = self.item(row, 6)
        if en_item:
            en_item.setText(item.get('en', ''))

        es_item = self.item(row, 7)
        if es_item:
            es_item.setText(item.get('es', ''))

        placeholder_item = self.item(row, 4)
        if placeholder_item:
            placeholder_text = item.get('dc_placeholder', '') or ''
            placeholder_item.setText(placeholder_text)
            if placeholder_text:
                placeholder_source = item.get('placeholder_source', 'proposed')
                if placeholder_source == 'existing':
                    placeholder_item.setToolTip(
                        f"Existing DC: {placeholder_text}"
                    )
                else:
                    placeholder_item.setToolTip(
                        f"Proposed: {placeholder_text}"
                    )

    def get_row_for_data_index(self, data_index: int) -> int:
        """Get the table row for a given data index."""
        for row in range(self.rowCount()):
            type_item = self.item(row, 2)
            if type_item:
                row_data_index = type_item.data(Qt.ItemDataRole.UserRole)
                if row_data_index == data_index:
                    return row
        return -1

    def refresh_row(self, data_index: int, item: Dict[str, Any]):
        """Refresh a row with new item data."""
        row = self.get_row_for_data_index(data_index)
        if row >= 0:
            self.blockSignals(True)
            self.update_row_text(row, item)
            self.update_row_colors(row, item)
            self.blockSignals(False)
            self._search_cache[data_index] = self._build_search_string(item)

    def get_visible_data_indices(self) -> List[int]:
        """Get list of visible (not hidden) data indices."""
        indices = []
        for row in range(self.rowCount()):
            if not self.isRowHidden(row):
                type_item = self.item(row, 2)
                if type_item:
                    data_index = type_item.data(Qt.ItemDataRole.UserRole)
                    if data_index is not None:
                        indices.append(data_index)
        return indices

    def get_visible_selected_indices(self) -> List[int]:
        """Get list of visible AND selected data indices."""
        visible = set(self.get_visible_data_indices())
        selected = set(self.get_selected_rows())
        return list(visible & selected)
