"""
Custom widgets for Zendesk DC Manager UI.

This module provides:
- StepWorker: Thread worker for long-running operations
- DarkConsoleWidget: Log output console with dark theme
- EmbeddedStatusBar: Progress bar with cancel button and ETA
- TableContainer: Container for table widget
- WorkItemTableModel: QAbstractTableModel backing PreviewTableWidget
- PreviewTableWidget: QTableView for preview data
"""

import threading
from typing import Optional, List, Dict, Any

from PyQt6.QtCore import (
    Qt, QTimer, pyqtSignal, QThread, QElapsedTimer,
    QAbstractTableModel, QModelIndex,
)
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QProgressBar, QTableView,
    QHeaderView, QAbstractItemView, QTextEdit,
    QStyledItemDelegate, QStyleOptionViewItem, QStyle,
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

    def __init__(self, table: QWidget, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.table = table
        layout.addWidget(table)


# ==============================================================================
# CELL COLOR DELEGATE
# ==============================================================================


class _CellColorDelegate(QStyledItemDelegate):
    """Forces Qt to honour BackgroundRole/ForegroundRole on table cells
    even when a QSS ::item rule is active (which normally suppresses them).
    Column 0 (checkbox) is always painted by the default delegate."""

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        if index.column() == 0:
            return
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
        # Always let Qt render the checkbox column natively
        if index.column() == 0:
            super().paint(painter, option, index)
            return

        bg = index.data(Qt.ItemDataRole.BackgroundRole)
        if bg is None:
            super().paint(painter, option, index)
            return

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


# ==============================================================================
# WORK ITEM TABLE MODEL
# ==============================================================================


class WorkItemTableModel(QAbstractTableModel):
    """Model backing PreviewTableWidget.

    Row index == data index into the underlying list.
    Colors and tooltips are derived from item data on demand,
    so no QTableWidgetItem allocations are needed.
    """

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

    COL_SELECT = 0
    COL_CONTEXT = 1
    COL_TYPE = 2
    COL_ID = 3
    COL_PLACEHOLDER = 4
    COL_PT = 5
    COL_EN = 6
    COL_ES = 7
    COL_ACTION = 8

    _SRC_KEY = {COL_PT: 'pt_source', COL_EN: 'en_source', COL_ES: 'es_source'}
    _FIELD_KEY = {COL_PT: 'pt', COL_EN: 'en', COL_ES: 'es'}

    # Emitted when a user edits a cell: (data_index, field, value, source)
    cell_edited = pyqtSignal(int, str, str, str)
    # Emitted when a checkbox is toggled
    selection_toggled = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._data: List[Dict[str, Any]] = []
        self._selection: Dict[int, bool] = {}
        self._search_cache: Dict[int, str] = {}

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_data(
        self,
        data: List[Dict[str, Any]],
        selection_state: Dict[int, bool] = None,
    ):
        self.beginResetModel()
        self._data = data
        self._selection = selection_state.copy() if selection_state else {}
        self._search_cache = {
            i: self._build_search_string(item)
            for i, item in enumerate(data)
        }
        self.endResetModel()

    def update_item(self, data_index: int, item: Dict[str, Any] = None):
        """Refresh a single row (update search cache + notify view)."""
        if not (0 <= data_index < len(self._data)):
            return
        if item is not None and item is not self._data[data_index]:
            self._data[data_index].update(item)
        self._search_cache[data_index] = self._build_search_string(
            self._data[data_index]
        )
        top_left = self.index(data_index, 0)
        bottom_right = self.index(data_index, len(self.COLUMNS) - 1)
        self.dataChanged.emit(top_left, bottom_right)

    # ------------------------------------------------------------------
    # QAbstractTableModel interface
    # ------------------------------------------------------------------

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.COLUMNS)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if (
            orientation == Qt.Orientation.Horizontal
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return self.COLUMNS[section][1]
        return None

    def flags(self, index):
        if not index.isValid() or index.row() >= len(self._data):
            return Qt.ItemFlag.NoItemFlags
        col = index.column()
        item = self._data[index.row()]
        is_system = item.get('is_system', False)

        base = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

        if col == self.COL_SELECT:
            if is_system:
                return base  # visible but not checkable
            return base | Qt.ItemFlag.ItemIsUserCheckable

        if col in (self.COL_PT, self.COL_EN, self.COL_ES) and not is_system:
            return base | Qt.ItemFlag.ItemIsEditable

        return base

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or index.row() >= len(self._data):
            return None
        row = index.row()
        col = index.column()
        item = self._data[row]
        is_system = item.get('is_system', False)

        if role == Qt.ItemDataRole.DisplayRole:
            return self._display(col, item, is_system)

        if role == Qt.ItemDataRole.CheckStateRole and col == self.COL_SELECT:
            checked = self._selection.get(row, False)
            return Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked

        if role == Qt.ItemDataRole.BackgroundRole:
            return self._background(col, item, is_system)

        if role == Qt.ItemDataRole.ForegroundRole:
            return self._foreground(col, item, is_system)

        if role == Qt.ItemDataRole.ToolTipRole:
            return self._tooltip(col, item, is_system)

        return None

    def setData(self, index, value, role=Qt.ItemDataRole.EditRole):
        if not index.isValid() or index.row() >= len(self._data):
            return False
        row = index.row()
        col = index.column()
        item = self._data[row]

        if role == Qt.ItemDataRole.CheckStateRole and col == self.COL_SELECT:
            if item.get('is_system', False):
                return False
            checked = (
                value == Qt.CheckState.Checked
                or (isinstance(value, int) and value == Qt.CheckState.Checked.value)
            )
            self._selection[row] = checked
            self.dataChanged.emit(index, index, [Qt.ItemDataRole.CheckStateRole])
            self.selection_toggled.emit()
            return True

        if role == Qt.ItemDataRole.EditRole and col in self._FIELD_KEY:
            if item.get('is_system', False):
                return False
            field = self._FIELD_KEY[col]
            item[field] = value
            item[field + '_source'] = SOURCE_MANUAL
            self._search_cache[row] = self._build_search_string(item)
            self.dataChanged.emit(
                index, index,
                [role, Qt.ItemDataRole.BackgroundRole, Qt.ItemDataRole.ForegroundRole],
            )
            self.cell_edited.emit(row, field, value, SOURCE_MANUAL)
            return True

        return False

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    def _display(self, col, item, is_system):
        if col == self.COL_SELECT:
            return None
        if col == self.COL_CONTEXT:
            return item.get('context', '')
        if col == self.COL_TYPE:
            return item.get('type_display', item.get('type', ''))
        if col == self.COL_ID:
            return str(item.get('obj_id', ''))
        if col == self.COL_PLACEHOLDER:
            return item.get('dc_placeholder', '') or ''
        if col == self.COL_PT:
            return item.get('pt', '')
        if col == self.COL_EN:
            return item.get('en', '')
        if col == self.COL_ES:
            return item.get('es', '')
        if col == self.COL_ACTION:
            return "SYSTEM" if is_system else item.get('action', '')
        return None

    def _background(self, col, item, is_system):
        if col in (self.COL_PT, self.COL_EN, self.COL_ES):
            src = SOURCE_RESERVED if is_system else item.get(
                self._SRC_KEY[col], SOURCE_NEW
            )
            return _get_source_color(src)
        if col == self.COL_PLACEHOLDER:
            ph = item.get('dc_placeholder', '') or ''
            if ph:
                return _get_placeholder_color(
                    item.get('placeholder_source', 'proposed')
                )
        return None

    def _foreground(self, col, item, is_system):
        if col in (self.COL_PT, self.COL_EN, self.COL_ES):
            src = SOURCE_RESERVED if is_system else item.get(
                self._SRC_KEY[col], SOURCE_NEW
            )
            return _get_text_color(src)
        if col == self.COL_PLACEHOLDER:
            ph = item.get('dc_placeholder', '') or ''
            if ph:
                return _get_placeholder_text_color(
                    item.get('placeholder_source', 'proposed')
                )
        if col == self.COL_ACTION and is_system:
            return _get_text_color(SOURCE_RESERVED)
        return None

    def _tooltip(self, col, item, is_system):
        if is_system and col in (
            self.COL_SELECT, self.COL_PT, self.COL_EN, self.COL_ES
        ):
            return "System/reserved item - cannot be modified"
        if col == self.COL_PLACEHOLDER:
            ph = item.get('dc_placeholder', '') or ''
            if ph:
                src = item.get('placeholder_source', 'proposed')
                return (
                    f"Existing DC: {ph}"
                    if src == 'existing'
                    else f"Proposed: {ph}"
                )
        if col == self.COL_ACTION and item.get('needs_locale_fix'):
            return (
                "Warning: PT-BR content was read from locale ID 16 (French).\n"
                "This DC was created with an incorrect locale assignment.\n"
                "Applying will re-save variants with the correct locale IDs."
            )
        return None

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def _build_search_string(self, item: Dict[str, Any]) -> str:
        return '|'.join([
            item.get('context', ''),
            item.get('type_display', item.get('type', '')),
            str(item.get('obj_id', '')),
            item.get('dc_placeholder', '') or '',
            item.get('pt', '') or '',
            item.get('en', '') or '',
            item.get('es', '') or '',
        ]).lower()

    # ------------------------------------------------------------------
    # Selection helpers (used by PreviewTableWidget)
    # ------------------------------------------------------------------

    def get_selected_count(self) -> int:
        return sum(1 for v in self._selection.values() if v)

    def get_selected_rows(self) -> List[int]:
        return [idx for idx, sel in self._selection.items() if sel]

    def get_selection_state(self) -> Dict[int, bool]:
        return self._selection.copy()

    def _emit_selection_range(self, indices: List[int]):
        """Emit dataChanged in contiguous blocks to avoid invalidating rows
        that were not actually changed (e.g. sparse filtered selections)."""
        if not indices:
            return
        sorted_idx = sorted(indices)
        role = [Qt.ItemDataRole.CheckStateRole]
        block_start = sorted_idx[0]
        prev = sorted_idx[0]
        for i in sorted_idx[1:]:
            if i != prev + 1:
                self.dataChanged.emit(
                    self.index(block_start, self.COL_SELECT),
                    self.index(prev, self.COL_SELECT),
                    role,
                )
                block_start = i
            prev = i
        self.dataChanged.emit(
            self.index(block_start, self.COL_SELECT),
            self.index(prev, self.COL_SELECT),
            role,
        )
        self.selection_toggled.emit()


# ==============================================================================
# PREVIEW TABLE WIDGET  (QTableView backed by WorkItemTableModel)
# ==============================================================================


class PreviewTableWidget(QTableView):
    """Table view for preview data, backed by WorkItemTableModel.

    Public API is intentionally identical to the previous QTableWidget
    subclass so that callers in ui_main.py need no changes.
    """

    stats_updated = pyqtSignal(dict)
    cell_edited = pyqtSignal(int, str, str, str)
    selection_changed = pyqtSignal(int)
    loading_finished = pyqtSignal()

    COLUMNS = WorkItemTableModel.COLUMNS

    def __init__(self, parent=None):
        super().__init__(parent)

        self._model = WorkItemTableModel(self)
        self._model.cell_edited.connect(self.cell_edited)
        self._model.selection_toggled.connect(
            lambda: self.selection_changed.emit(self._model.get_selected_count())
        )
        self.setModel(self._model)
        self._setup_view()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _setup_view(self):
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
        self.verticalHeader().setDefaultSectionSize(UI_CONFIG.TABLE_ROW_HEIGHT)
        self.setEditTriggers(QAbstractItemView.EditTrigger.DoubleClicked)
        self.setItemDelegate(_CellColorDelegate(self))

    # ------------------------------------------------------------------
    # Compatibility shim: expose _data so ui_main.py can do len(table._data)
    # ------------------------------------------------------------------

    @property
    def _data(self) -> List[Dict[str, Any]]:
        return self._model._data

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def start_async_load(
        self,
        data: List[Dict[str, Any]],
        mode: str = "preview",
        batch_size: int = None,
        selection_state: Dict[int, bool] = None,
    ):
        """Load data into the model.  QTableView virtualises rendering so
        no timer-based batching is needed."""
        self._model.load_data(data, selection_state)
        self._update_stats()
        # Defer loading_finished so callers can connect after this call
        QTimer.singleShot(0, self.loading_finished.emit)

    def cancel_loading(self):
        pass  # nothing to cancel

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def _update_stats(self):
        data = self._model._data
        stats = {
            'total': len(data),
            'items_from_dc': 0,
            'items_translated': 0,
            'items_failed': 0,
            'items_attention': 0,
            'items_reserved': 0,
            'items_pending': 0,
            'selected_count': self._model.get_selected_count(),
        }

        translated_sources = frozenset({
            SOURCE_TRANSLATED, SOURCE_CACHE, SOURCE_MANUAL, SOURCE_ATTENTION,
        })

        for item in data:
            if item.get('is_system', False):
                stats['items_reserved'] += 1
                continue

            source = item.get('source', SOURCE_NEW)
            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

            if source == SOURCE_ZENDESK_DC:
                stats['items_from_dc'] += 1
            elif en_source == SOURCE_FAILED or es_source == SOURCE_FAILED:
                stats['items_failed'] += 1
            elif en_source == SOURCE_ATTENTION or es_source == SOURCE_ATTENTION:
                stats['items_attention'] += 1
            elif (
                en_source in translated_sources
                and es_source in translated_sources
            ):
                stats['items_translated'] += 1
            else:
                stats['items_pending'] += 1

        self.stats_updated.emit(stats)

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

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

        for row in range(self._model.rowCount()):
            item = self._model._data[row]
            is_system = item.get('is_system', False)

            if is_system:
                type_visible = show_reserved
            else:
                type_visible = TYPE_VISIBLE.get(item.get('type', ''), True)

            if not type_visible:
                self.setRowHidden(row, True)
                continue

            if search:
                searchable = self._model._search_cache.get(row, '')
                self.setRowHidden(row, search not in searchable)
            else:
                self.setRowHidden(row, False)

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------

    def get_selected_count(self) -> int:
        return self._model.get_selected_count()

    def get_selected_rows(self) -> List[int]:
        return self._model.get_selected_rows()

    def get_selection_state(self) -> Dict[int, bool]:
        return self._model.get_selection_state()

    def select_all_visible(self):
        changed = []
        for row in range(self._model.rowCount()):
            if self.isRowHidden(row):
                continue
            item = self._model._data[row]
            if item.get('is_system', False):
                continue
            self._model._selection[row] = True
            changed.append(row)
        self._model._emit_selection_range(changed)

    def deselect_all_visible(self):
        changed = []
        for row in range(self._model.rowCount()):
            if self.isRowHidden(row):
                continue
            self._model._selection[row] = False
            changed.append(row)
        self._model._emit_selection_range(changed)

    def invert_selection_visible(self):
        changed = []
        for row in range(self._model.rowCount()):
            if self.isRowHidden(row):
                continue
            item = self._model._data[row]
            if item.get('is_system', False):
                continue
            self._model._selection[row] = not self._model._selection.get(
                row, False
            )
            changed.append(row)
        self._model._emit_selection_range(changed)

    # ------------------------------------------------------------------
    # Row access
    # ------------------------------------------------------------------

    def get_row_for_data_index(self, data_index: int) -> int:
        """O(1) lookup: row index == data index in this model."""
        if 0 <= data_index < self._model.rowCount():
            return data_index
        return -1

    def refresh_row(self, data_index: int, item: Dict[str, Any]):
        """Refresh a row after external data change."""
        self._model.update_item(data_index, item)
        self._update_stats()

    def update_row_colors(self, row: int, item: Dict[str, Any]):
        self._model.update_item(row, item)

    def update_row_text(self, row: int, item: Dict[str, Any]):
        self._model.update_item(row, item)

    def get_visible_data_indices(self) -> List[int]:
        return [
            row for row in range(self._model.rowCount())
            if not self.isRowHidden(row)
        ]

    def get_visible_selected_indices(self) -> List[int]:
        visible = set(self.get_visible_data_indices())
        selected = set(self._model.get_selected_rows())
        return list(visible & selected)
