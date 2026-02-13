"""
Main UI module for Zendesk DC Manager.
"""

import threading
from typing import Optional, List, Dict, Any

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QStackedWidget, QLabel, QPushButton, QLineEdit,
    QTextEdit, QGroupBox, QCheckBox, QComboBox,
    QSpinBox, QFileDialog, QMessageBox, QSplitter,
    QGridLayout, QApplication, QFrame,
)

from zendesk_dc_manager.config import (
    UI_CONFIG,
    CREDENTIALS_FILE,
    logger,
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
)
from zendesk_dc_manager.types import AppState, StateManager
from zendesk_dc_manager.controller import ZendeskController
from zendesk_dc_manager.ui_styles import get_main_stylesheet
from zendesk_dc_manager.ui_widgets import (
    StepWorker,
    PreviewTableWidget,
    DarkConsoleWidget,
    EmbeddedStatusBar,
    TableContainer,
)


class SidebarWidget(QWidget):
    """Sidebar navigation widget."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("sidebar")
        self.setFixedWidth(UI_CONFIG.SIDEBAR_WIDTH)
        self.setStyleSheet("""
            QWidget#sidebar {
                background-color: #1F2937;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 16, 8, 16)
        layout.setSpacing(4)

        title = QLabel("DC Manager")
        title.setStyleSheet(
            "color: #FFFFFF; font-size: 18px; font-weight: bold; "
            "padding: 8px; margin-bottom: 16px;"
        )
        layout.addWidget(title)

        self.buttons: List[QPushButton] = []
        self.pages = [
            ("🔗", "Connect"),
            ("🔍", "Scan"),
            ("📋", "Preview"),
            ("✅", "Apply"),
            ("↩️", "Rollback"),
            ("⚙️", "Config"),
        ]

        for icon, name in self.pages:
            btn = QPushButton(f"  {icon}  {name}")
            btn.setCheckable(True)
            btn.setEnabled(name in ["Connect", "Config"])
            btn.setStyleSheet("""
                QPushButton {
                    background-color: transparent;
                    color: #D1D5DB;
                    border: none;
                    border-radius: 6px;
                    padding: 12px 16px;
                    text-align: left;
                    font-size: 13px;
                    font-weight: 500;
                }
                QPushButton:hover {
                    background-color: #374151;
                    color: #F9FAFB;
                }
                QPushButton:checked {
                    background-color: #3B82F6;
                    color: #FFFFFF;
                }
                QPushButton:disabled {
                    color: #6B7280;
                }
            """)
            self.buttons.append(btn)
            layout.addWidget(btn)

        layout.addStretch()

        for i, btn in enumerate(self.buttons):
            btn.clicked.connect(
                lambda checked, idx=i: self._on_button_clicked(idx)
            )

        self._selected_index = 0
        self._page_selected_callback = None

    def set_page_selected_callback(self, callback):
        self._page_selected_callback = callback

    def _on_button_clicked(self, index: int):
        self.select(index)
        if self._page_selected_callback:
            self._page_selected_callback(index)

    def select(self, index: int):
        self._selected_index = index
        for i, btn in enumerate(self.buttons):
            btn.setChecked(i == index)

    def unlock(self, index: int):
        if 0 <= index < len(self.buttons):
            self.buttons[index].setEnabled(True)

    def lock(self, index: int):
        if 0 <= index < len(self.buttons):
            self.buttons[index].setEnabled(False)


class WizardPage(QWidget):
    """Base class for wizard pages."""

    def __init__(self, title: str, subtitle: str = "", parent=None):
        super().__init__(parent)

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(24, 24, 24, 24)
        self.main_layout.setSpacing(16)

        header = QVBoxLayout()
        header.setSpacing(4)

        title_label = QLabel(title)
        title_label.setStyleSheet(
            "font-size: 20px; font-weight: 600; color: #1F2937;"
        )
        header.addWidget(title_label)

        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setStyleSheet(
                "font-size: 14px; color: #6B7280;"
            )
            header.addWidget(subtitle_label)

        self.main_layout.addLayout(header)

    def add_widget(self, widget: QWidget, stretch: int = 0):
        self.main_layout.addWidget(widget, stretch)

    def add_layout(self, layout):
        self.main_layout.addLayout(layout)

    def add_stretch(self):
        self.main_layout.addStretch()

    def add_spacing(self, size: int):
        self.main_layout.addSpacing(size)


class CompactWizardPage(QWidget):
    """Wizard page with compact spacing for data-heavy views."""

    def __init__(self, title: str, subtitle: str = "", parent=None):
        super().__init__(parent)

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(16, 12, 16, 12)
        self.main_layout.setSpacing(8)

        header = QHBoxLayout()
        header.setSpacing(12)

        title_label = QLabel(title)
        title_label.setStyleSheet(
            "font-size: 18px; font-weight: 600; color: #1F2937;"
        )
        header.addWidget(title_label)

        if subtitle:
            subtitle_label = QLabel(f"— {subtitle}")
            subtitle_label.setStyleSheet(
                "font-size: 13px; color: #6B7280;"
            )
            header.addWidget(subtitle_label)

        header.addStretch()

        self.main_layout.addLayout(header)

    def add_widget(self, widget: QWidget, stretch: int = 0):
        self.main_layout.addWidget(widget, stretch)

    def add_layout(self, layout):
        self.main_layout.addLayout(layout)

    def add_stretch(self):
        self.main_layout.addStretch()

    def add_spacing(self, size: int):
        self.main_layout.addSpacing(size)


class ZendeskWizard(QMainWindow):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Zendesk Dynamic Content Manager")
        self.setMinimumSize(
            UI_CONFIG.MIN_WINDOW_WIDTH,
            UI_CONFIG.MIN_WINDOW_HEIGHT
        )

        self.controller = ZendeskController()
        self.state_manager = StateManager(self._on_state_change)

        self.worker: Optional[StepWorker] = None
        self._worker_lock = threading.Lock()

        self._pending_backup_items: List[Dict[str, Any]] = []
        self._work_items_cache: List[Dict[str, Any]] = []
        self._cached_stats: Dict[str, int] = {}
        self._is_connected = False
        self._has_scan_data = False

        self._init_ui()
        self._load_saved_credentials()
        self._center_window()
        self._update_sidebar_state()

    def _center_window(self):
        """Set window to use all available screen space."""
        screen = QApplication.primaryScreen()
        if screen:
            available = screen.availableGeometry()
            self.move(available.topLeft())
            self.resize(available.size())

    def _update_sidebar_state(self):
        """Update sidebar buttons based on current application state."""
        self.sidebar.buttons[0].setEnabled(True)
        self.sidebar.buttons[5].setEnabled(True)
        self.sidebar.buttons[1].setEnabled(self._is_connected)
        self.sidebar.buttons[2].setEnabled(self._has_scan_data)
        self.sidebar.buttons[3].setEnabled(self._has_scan_data)
        self.sidebar.buttons[4].setEnabled(self._is_connected)

    def _init_ui(self):
        self.setStyleSheet(get_main_stylesheet())

        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.sidebar = SidebarWidget()
        self.sidebar.set_page_selected_callback(self.goto)
        main_layout.addWidget(self.sidebar)

        right_container = QWidget()
        right_layout = QVBoxLayout(right_container)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)

        self.splitter = QSplitter(Qt.Orientation.Vertical)

        self.stack = QStackedWidget()
        self.splitter.addWidget(self.stack)

        self.log_panel = DarkConsoleWidget()
        self.splitter.addWidget(self.log_panel)

        self.splitter.setSizes([
            UI_CONFIG.SPLITTER_TOP_SIZE,
            UI_CONFIG.SPLITTER_LOG_SIZE
        ])
        self.splitter.setStretchFactor(0, 1)
        self.splitter.setStretchFactor(1, 0)

        right_layout.addWidget(self.splitter, 1)

        self.status_bar = EmbeddedStatusBar()
        self.status_bar.cancel_clicked.connect(self._on_cancel_clicked)
        right_layout.addWidget(self.status_bar)

        main_layout.addWidget(right_container, 1)

        self._init_page_connect()
        self._init_page_scan()
        self._init_page_preview()
        self._init_page_apply()
        self._init_page_rollback()
        self._init_page_config()

        self.sidebar.select(0)

    def _init_page_connect(self):
        page = WizardPage(
            "Connect to Zendesk",
            "Enter your Zendesk credentials to get started."
        )

        info_label = QLabel(
            "ℹ️ You need Admin or Agent role to use this tool. "
            "API tokens can be found in Admin Center → Apps and "
            "integrations → APIs → Zendesk API."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet(
            "background-color: #EFF6FF; color: #1E40AF; "
            "padding: 12px; border-radius: 6px; font-size: 12px;"
        )
        page.add_widget(info_label)

        form_group = QGroupBox("Credentials")
        form_layout = QGridLayout(form_group)
        form_layout.setColumnStretch(1, 1)
        form_layout.setVerticalSpacing(12)
        form_layout.setHorizontalSpacing(12)

        form_layout.addWidget(QLabel("Subdomain:"), 0, 0)
        subdomain_row = QHBoxLayout()
        subdomain_row.setSpacing(8)
        self.txt_subdomain = QLineEdit()
        self.txt_subdomain.setPlaceholderText("your-company")
        subdomain_row.addWidget(self.txt_subdomain)
        subdomain_suffix = QLabel(".zendesk.com")
        subdomain_suffix.setStyleSheet("color: #6B7280;")
        subdomain_suffix.setFixedWidth(100)
        subdomain_row.addWidget(subdomain_suffix)
        form_layout.addLayout(subdomain_row, 0, 1)

        form_layout.addWidget(QLabel("Email:"), 1, 0)
        self.txt_email = QLineEdit()
        self.txt_email.setPlaceholderText("admin@company.com")
        form_layout.addWidget(self.txt_email, 1, 1)

        form_layout.addWidget(QLabel("API Token:"), 2, 0)
        token_row = QHBoxLayout()
        token_row.setSpacing(8)
        self.txt_token = QLineEdit()
        self.txt_token.setEchoMode(QLineEdit.EchoMode.Password)
        self.txt_token.setPlaceholderText("Your Zendesk API token")
        token_row.addWidget(self.txt_token)
        self.btn_toggle_token = QPushButton("Show")
        self.btn_toggle_token.setMinimumWidth(70)
        self.btn_toggle_token.setCheckable(True)
        self.btn_toggle_token.clicked.connect(self._toggle_token_visibility)
        self.btn_toggle_token.setStyleSheet("""
            QPushButton {
                background-color: #E5E7EB;
                border: 1px solid #D1D5DB;
                border-radius: 4px;
                padding: 6px 12px;
            }
            QPushButton:checked {
                background-color: #DBEAFE;
                border-color: #3B82F6;
            }
            QPushButton:hover {
                background-color: #D1D5DB;
            }
        """)
        token_row.addWidget(self.btn_toggle_token)
        form_layout.addLayout(token_row, 2, 1)

        form_layout.addWidget(QLabel("Backup Folder:"), 3, 0)
        backup_row = QHBoxLayout()
        backup_row.setSpacing(8)
        self.txt_backup = QLineEdit()
        self.txt_backup.setPlaceholderText("Select folder for backups...")
        backup_row.addWidget(self.txt_backup)
        self.btn_browse = QPushButton("Browse...")
        self.btn_browse.setMinimumWidth(100)
        self.btn_browse.clicked.connect(self._browse_backup)
        backup_row.addWidget(self.btn_browse)
        form_layout.addLayout(backup_row, 3, 1)

        page.add_widget(form_group)

        options_group = QGroupBox("Options")
        options_layout = QVBoxLayout(options_group)

        self.chk_save_credentials = QCheckBox("Save credentials for next session")
        self.chk_save_credentials.setChecked(True)
        options_layout.addWidget(self.chk_save_credentials)

        page.add_widget(options_group)
        page.add_stretch()

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self.btn_connect = QPushButton("Connect")
        self.btn_connect.setMinimumWidth(120)
        self.btn_connect.setStyleSheet("""
            QPushButton {
                background-color: #3B82F6;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 24px;
                font-weight: 600;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #2563EB;
            }
            QPushButton:pressed {
                background-color: #1D4ED8;
            }
        """)
        self.btn_connect.clicked.connect(self.run_connect)
        btn_row.addWidget(self.btn_connect)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _toggle_token_visibility(self, checked: bool):
        if checked:
            self.txt_token.setEchoMode(QLineEdit.EchoMode.Normal)
            self.btn_toggle_token.setText("Hide")
        else:
            self.txt_token.setEchoMode(QLineEdit.EchoMode.Password)
            self.btn_toggle_token.setText("Show")

    def _init_page_scan(self):
        page = WizardPage(
            "Scan Data",
            "Select what Zendesk objects to scan for Dynamic Content."
        )

        ticket_group = QGroupBox("Ticket Fields && Forms")
        ticket_layout = QGridLayout(ticket_group)
        ticket_layout.setSpacing(10)
        ticket_layout.setColumnStretch(0, 1)
        ticket_layout.setColumnStretch(1, 1)
        ticket_layout.setColumnStretch(2, 1)

        self.chk_scan_fields = QCheckBox("Ticket Fields")
        self.chk_scan_fields.setChecked(True)
        self.chk_scan_forms = QCheckBox("Ticket Forms")
        self.chk_scan_forms.setChecked(True)

        ticket_layout.addWidget(self.chk_scan_fields, 0, 0)
        ticket_layout.addWidget(self.chk_scan_forms, 0, 1)

        page.add_widget(ticket_group)

        status_group = QGroupBox("Ticket Statuses")
        status_layout = QGridLayout(status_group)
        status_layout.setSpacing(10)
        status_layout.setColumnStretch(0, 1)
        status_layout.setColumnStretch(1, 1)
        status_layout.setColumnStretch(2, 1)

        self.chk_scan_custom_statuses = QCheckBox("Custom Statuses")
        self.chk_scan_custom_statuses.setChecked(False)

        status_layout.addWidget(self.chk_scan_custom_statuses, 0, 0)

        page.add_widget(status_group)

        user_org_group = QGroupBox("User && Organization")
        user_org_layout = QGridLayout(user_org_group)
        user_org_layout.setSpacing(10)
        user_org_layout.setColumnStretch(0, 1)
        user_org_layout.setColumnStretch(1, 1)
        user_org_layout.setColumnStretch(2, 1)

        self.chk_scan_user_fields = QCheckBox("User Fields")
        self.chk_scan_user_fields.setChecked(False)
        self.chk_scan_org_fields = QCheckBox("Organization Fields")
        self.chk_scan_org_fields.setChecked(False)
        self.chk_scan_groups = QCheckBox("Groups")
        self.chk_scan_groups.setChecked(False)

        user_org_layout.addWidget(self.chk_scan_user_fields, 0, 0)
        user_org_layout.addWidget(self.chk_scan_org_fields, 0, 1)
        user_org_layout.addWidget(self.chk_scan_groups, 0, 2)

        page.add_widget(user_org_group)

        rules_group = QGroupBox("Business Rules")
        rules_layout = QGridLayout(rules_group)
        rules_layout.setSpacing(10)
        rules_layout.setColumnStretch(0, 1)
        rules_layout.setColumnStretch(1, 1)
        rules_layout.setColumnStretch(2, 1)

        self.chk_scan_macros = QCheckBox("Macros")
        self.chk_scan_macros.setChecked(False)
        self.chk_scan_triggers = QCheckBox("Triggers")
        self.chk_scan_triggers.setChecked(False)
        self.chk_scan_automations = QCheckBox("Automations")
        self.chk_scan_automations.setChecked(False)
        self.chk_scan_views = QCheckBox("Views")
        self.chk_scan_views.setChecked(False)
        self.chk_scan_sla_policies = QCheckBox("SLA Policies")
        self.chk_scan_sla_policies.setChecked(False)

        rules_layout.addWidget(self.chk_scan_macros, 0, 0)
        rules_layout.addWidget(self.chk_scan_triggers, 0, 1)
        rules_layout.addWidget(self.chk_scan_automations, 0, 2)
        rules_layout.addWidget(self.chk_scan_views, 1, 0)
        rules_layout.addWidget(self.chk_scan_sla_policies, 1, 1)

        page.add_widget(rules_group)

        hc_group = QGroupBox("Help Center")
        hc_layout = QGridLayout(hc_group)
        hc_layout.setSpacing(10)
        hc_layout.setColumnStretch(0, 1)
        hc_layout.setColumnStretch(1, 1)
        hc_layout.setColumnStretch(2, 1)

        self.chk_scan_cats = QCheckBox("Categories")
        self.chk_scan_cats.setChecked(False)
        self.chk_scan_sects = QCheckBox("Sections")
        self.chk_scan_sects.setChecked(False)
        self.chk_scan_arts = QCheckBox("Articles")
        self.chk_scan_arts.setChecked(False)

        hc_layout.addWidget(self.chk_scan_cats, 0, 0)
        hc_layout.addWidget(self.chk_scan_sects, 0, 1)
        hc_layout.addWidget(self.chk_scan_arts, 0, 2)

        page.add_widget(hc_group)
        page.add_stretch()

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_scan = QPushButton("Start Scan")
        self.btn_scan.setMinimumWidth(120)
        self.btn_scan.setStyleSheet("""
            QPushButton {
                background-color: #3B82F6;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 24px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #2563EB;
            }
        """)
        self.btn_scan.clicked.connect(self.run_scan)
        btn_row.addWidget(self.btn_scan)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_preview(self):
        page = CompactWizardPage(
            "Preview & Translate",
            "Review scanned items and run translations"
        )

        # Filter row with all checkboxes including Reserved
        filter_row1 = QHBoxLayout()
        filter_row1.setSpacing(12)

        filter_label = QLabel("Show:")
        filter_label.setStyleSheet("font-weight: 500;")
        filter_row1.addWidget(filter_label)

        self.chk_filter_ticket = QCheckBox("Ticket")
        self.chk_filter_ticket.setChecked(True)
        self.chk_filter_ticket.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_ticket)

        self.chk_filter_status = QCheckBox("Status")
        self.chk_filter_status.setChecked(True)
        self.chk_filter_status.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_status)

        self.chk_filter_user = QCheckBox("User")
        self.chk_filter_user.setChecked(True)
        self.chk_filter_user.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_user)

        self.chk_filter_org = QCheckBox("Organization")
        self.chk_filter_org.setChecked(True)
        self.chk_filter_org.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_org)

        self.chk_filter_rules = QCheckBox("Business Rules")
        self.chk_filter_rules.setChecked(True)
        self.chk_filter_rules.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_rules)

        self.chk_filter_admin = QCheckBox("Admin")
        self.chk_filter_admin.setChecked(True)
        self.chk_filter_admin.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_admin)

        self.chk_filter_hc = QCheckBox("Help Center")
        self.chk_filter_hc.setChecked(True)
        self.chk_filter_hc.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_hc)

        self.chk_filter_reserved = QCheckBox("System")
        self.chk_filter_reserved.setChecked(True)
        self.chk_filter_reserved.setToolTip("Show system/reserved fields")
        self.chk_filter_reserved.stateChanged.connect(self.apply_table_filter)
        filter_row1.addWidget(self.chk_filter_reserved)

        filter_row1.addStretch()

        page.add_layout(filter_row1)

        # Selection buttons row
        filter_row2 = QHBoxLayout()
        filter_row2.setSpacing(8)

        self.btn_select_all = QPushButton("Select All")
        self.btn_select_all.setMinimumWidth(100)
        self.btn_select_all.clicked.connect(self._select_all_visible)
        filter_row2.addWidget(self.btn_select_all)

        self.btn_deselect_all = QPushButton("Deselect All")
        self.btn_deselect_all.setMinimumWidth(100)
        self.btn_deselect_all.clicked.connect(self._deselect_all_visible)
        filter_row2.addWidget(self.btn_deselect_all)

        self.btn_invert_selection = QPushButton("Invert Selection")
        self.btn_invert_selection.setMinimumWidth(120)
        self.btn_invert_selection.clicked.connect(self._invert_selection)
        filter_row2.addWidget(self.btn_invert_selection)

        filter_row2.addStretch()

        page.add_layout(filter_row2)

        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setStyleSheet("background-color: #E5E7EB;")
        separator.setFixedHeight(1)
        page.add_widget(separator)

        # Table
        self.preview_table = PreviewTableWidget()
        self.preview_table.stats_updated.connect(self._on_table_stats_updated)
        self.preview_table.cell_edited.connect(self._on_cell_edited)
        self.preview_table.selection_changed.connect(self._on_selection_changed)
        self.preview_table.loading_finished.connect(
            self._on_table_loading_finished
        )

        self.table_container = TableContainer(self.preview_table)
        page.add_widget(self.table_container, 1)

        # Bottom section - Combined summary and info row
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        bottom_layout.setContentsMargins(0, 6, 0, 0)
        bottom_layout.setSpacing(6)

        # Summary row with translation info on the right
        summary_row = QHBoxLayout()
        summary_row.setSpacing(12)

        self.lbl_sum_total = QLabel("Total: 0")
        self.lbl_sum_total.setStyleSheet("font-weight: 600; font-size: 11px;")
        summary_row.addWidget(self.lbl_sum_total)

        self.lbl_sum_selected = QLabel("Selected: 0")
        self.lbl_sum_selected.setStyleSheet(
            "color: #3B82F6; font-weight: 600; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_selected)

        summary_row.addWidget(self._create_separator_label())

        self.lbl_sum_from_dc = QLabel("From DC: 0")
        self.lbl_sum_from_dc.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_ZENDESK_DC]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_from_dc)

        self.lbl_sum_translated = QLabel("Translated: 0")
        self.lbl_sum_translated.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_TRANSLATED]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_translated)

        self.lbl_sum_pending = QLabel("Pending: 0")
        self.lbl_sum_pending.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_NEW]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_pending)

        self.lbl_sum_failed = QLabel("Failed: 0")
        self.lbl_sum_failed.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_FAILED]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_failed)

        self.lbl_sum_attention = QLabel("Attention: 0")
        self.lbl_sum_attention.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_ATTENTION]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_attention)

        self.lbl_sum_reserved = QLabel("System: 0")
        self.lbl_sum_reserved.setStyleSheet(
            f"color: {TEXT_COLORS[SOURCE_RESERVED]}; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_sum_reserved)

        summary_row.addStretch()

        # Translation info label on the right side of summary row
        self.lbl_will_translate = QLabel("ℹ️ Select items to translate")
        self.lbl_will_translate.setStyleSheet(
            "background-color: #F3F4F6; color: #6B7280; "
            "padding: 4px 10px; border-radius: 4px; font-size: 11px;"
        )
        summary_row.addWidget(self.lbl_will_translate)

        bottom_layout.addLayout(summary_row)

        # Legend and buttons row
        legend_button_row = QHBoxLayout()
        legend_button_row.setSpacing(5)

        # Translation status legend
        translation_legend_items = [
            (SOURCE_NEW, "Pending", "Not yet translated"),
            (SOURCE_ZENDESK_DC, "From DC", "Has existing DC translation"),
            (SOURCE_TRANSLATED, "Translated", "Newly translated"),
            (SOURCE_CACHE, "Cached", "From translation cache"),
            (SOURCE_FAILED, "Failed", "Translation failed"),
            (SOURCE_MANUAL, "Manual", "Manually edited"),
            (SOURCE_ATTENTION, "Attention", "PT=EN=ES (review needed)"),
            (SOURCE_RESERVED, "System", "System field (read-only)"),
        ]

        for source, label_text, tooltip in translation_legend_items:
            bg = SOURCE_COLORS.get(source, SOURCE_COLORS[SOURCE_NEW])
            fg = TEXT_COLORS.get(source, TEXT_COLORS[SOURCE_NEW])

            item_label = QLabel(label_text)
            item_label.setToolTip(tooltip)
            item_label.setStyleSheet(f"""
                QLabel {{
                    background-color: {bg};
                    color: {fg};
                    padding: 2px 5px;
                    border-radius: 3px;
                    font-weight: 500;
                    font-size: 9px;
                }}
            """)
            legend_button_row.addWidget(item_label)

        # Separator
        separator_label = QLabel("|")
        separator_label.setStyleSheet("color: #9CA3AF; padding: 0 3px;")
        legend_button_row.addWidget(separator_label)

        # Placeholder legend
        placeholder_legend_items = [
            ('existing', "DC Linked", "Already using DC placeholder"),
            ('proposed', "DC New", "Will create new DC"),
        ]

        for source, label_text, tooltip in placeholder_legend_items:
            bg = PLACEHOLDER_COLORS.get(source, PLACEHOLDER_COLORS['proposed'])
            fg = PLACEHOLDER_TEXT_COLORS.get(
                source, PLACEHOLDER_TEXT_COLORS['proposed']
            )

            item_label = QLabel(label_text)
            item_label.setToolTip(tooltip)
            item_label.setStyleSheet(f"""
                QLabel {{
                    background-color: {bg};
                    color: {fg};
                    padding: 2px 5px;
                    border-radius: 3px;
                    font-weight: 500;
                    font-size: 9px;
                }}
            """)
            legend_button_row.addWidget(item_label)

        legend_button_row.addStretch()

        # Re-translate checkbox
        self.chk_force_translate = QCheckBox("Re-translate")
        self.chk_force_translate.setChecked(False)
        self.chk_force_translate.setToolTip(
            "When checked: Re-translate ALL selected items\n"
            "(even if already translated)\n\n"
            "When unchecked: Only translate items that\n"
            "are still 'Pending'"
        )
        self.chk_force_translate.setStyleSheet(
            "font-size: 11px; font-weight: 500;"
        )
        self.chk_force_translate.stateChanged.connect(
            self._update_will_translate_label
        )
        legend_button_row.addWidget(self.chk_force_translate)

        legend_button_row.addSpacing(8)

        # Translate button
        self.btn_translate = QPushButton("Translate Selected")
        self.btn_translate.setMinimumWidth(140)
        self.btn_translate.setStyleSheet("""
            QPushButton {
                background-color: #059669;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 6px 16px;
                font-weight: 600;
                font-size: 12px;
            }
            QPushButton:hover {
                background-color: #047857;
            }
        """)
        self.btn_translate.clicked.connect(self.run_translation)
        legend_button_row.addWidget(self.btn_translate)

        bottom_layout.addLayout(legend_button_row)

        page.add_widget(bottom_widget)

        self.stack.addWidget(page)

    def _create_separator_label(self) -> QLabel:
        """Create a separator label for summary row."""
        sep = QLabel("|")
        sep.setStyleSheet("color: #D1D5DB; font-size: 11px;")
        return sep

    def _init_page_apply(self):
        page = WizardPage(
            "Apply Changes",
            "Apply translations to Zendesk and create Dynamic Content."
        )

        warning_label = QLabel(
            "⚠️ This will modify your Zendesk instance. "
            "A backup will be created automatically before changes are applied."
        )
        warning_label.setWordWrap(True)
        warning_label.setStyleSheet(
            "background-color: #FEF3C7; color: #92400E; "
            "padding: 12px; border-radius: 6px; font-size: 12px;"
        )
        page.add_widget(warning_label)

        info_label = QLabel(
            "Select items in the Preview tab, then click "
            "'Apply Selected Changes' to apply them to Zendesk. "
            "System items are automatically excluded."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet(
            "color: #6B7280; font-size: 13px; padding: 8px 0;"
        )
        page.add_widget(info_label)

        options_group = QGroupBox("Apply Options")
        options_layout = QVBoxLayout(options_group)

        self.chk_update_existing_dc = QCheckBox(
            "Update translations for items already linked to Dynamic Content"
        )
        self.chk_update_existing_dc.setChecked(False)
        self.chk_update_existing_dc.setToolTip(
            "When checked, items that are already linked to a DC placeholder\n"
            "will have their DC translations updated with the current values.\n"
            "\nWhen unchecked, linked items will be skipped."
        )
        options_layout.addWidget(self.chk_update_existing_dc)

        page.add_widget(options_group)

        page.add_stretch()

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        self.btn_apply = QPushButton("Apply Selected Changes")
        self.btn_apply.setMinimumWidth(200)
        self.btn_apply.setStyleSheet("""
            QPushButton {
                background-color: #DC2626;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 24px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #B91C1C;
            }
        """)
        self.btn_apply.clicked.connect(self.run_apply)
        btn_row.addWidget(self.btn_apply)
        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_rollback(self):
        page = WizardPage(
            "Rollback",
            "Restore original values from a backup file."
        )

        file_group = QGroupBox("Select Backup File")
        file_layout = QHBoxLayout(file_group)
        file_layout.setSpacing(8)

        self.txt_rollback_file = QLineEdit()
        self.txt_rollback_file.setPlaceholderText("Select a backup file...")
        file_layout.addWidget(self.txt_rollback_file)

        self.btn_browse_rollback = QPushButton("Browse...")
        self.btn_browse_rollback.setMinimumWidth(100)
        self.btn_browse_rollback.clicked.connect(self._browse_rollback_file)
        file_layout.addWidget(self.btn_browse_rollback)

        page.add_widget(file_group)

        info_group = QGroupBox("Backup Contents")
        info_layout = QVBoxLayout(info_group)
        self.rollback_info = QTextEdit()
        self.rollback_info.setReadOnly(True)
        self.rollback_info.setPlaceholderText(
            "Backup file contents will appear here after loading..."
        )
        info_layout.addWidget(self.rollback_info)
        page.add_widget(info_group, 1)

        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self.btn_load_backup = QPushButton("Load Backup")
        self.btn_load_backup.setMinimumWidth(130)
        self.btn_load_backup.clicked.connect(self.run_load_backup)
        btn_row.addWidget(self.btn_load_backup)

        self.btn_rollback = QPushButton("Restore Backup")
        self.btn_rollback.setMinimumWidth(140)
        self.btn_rollback.setEnabled(False)
        self.btn_rollback.setStyleSheet("""
            QPushButton {
                background-color: #DC2626;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 10px 24px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #B91C1C;
            }
            QPushButton:disabled {
                background-color: #9CA3AF;
            }
        """)
        self.btn_rollback.clicked.connect(self.run_rollback)
        btn_row.addWidget(self.btn_rollback)

        page.add_layout(btn_row)

        self.stack.addWidget(page)

    def _init_page_config(self):
        page = WizardPage(
            "Configuration",
            "Translation and cache settings."
        )

        trans_group = QGroupBox("Translation Settings")
        trans_layout = QGridLayout(trans_group)
        trans_layout.setColumnStretch(1, 1)
        trans_layout.setVerticalSpacing(12)

        trans_layout.addWidget(QLabel("Provider:"), 0, 0)
        self.cmb_provider = QComboBox()
        self.cmb_provider.addItems([
            "Google Web (Free)",
            "Google Cloud (API Key)"
        ])
        trans_layout.addWidget(self.cmb_provider, 0, 1)

        trans_layout.addWidget(QLabel("API Key:"), 1, 0)
        api_key_row = QHBoxLayout()
        api_key_row.setSpacing(8)
        self.txt_api_key = QLineEdit()
        self.txt_api_key.setPlaceholderText("Required for Google Cloud only")
        self.txt_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        api_key_row.addWidget(self.txt_api_key)
        self.btn_toggle_api_key = QPushButton("Show")
        self.btn_toggle_api_key.setMinimumWidth(70)
        self.btn_toggle_api_key.setCheckable(True)
        self.btn_toggle_api_key.clicked.connect(self._toggle_api_key_visibility)
        self.btn_toggle_api_key.setStyleSheet("""
            QPushButton {
                background-color: #E5E7EB;
                border: 1px solid #D1D5DB;
                border-radius: 4px;
                padding: 6px 12px;
            }
            QPushButton:checked {
                background-color: #DBEAFE;
                border-color: #3B82F6;
            }
            QPushButton:hover {
                background-color: #D1D5DB;
            }
        """)
        api_key_row.addWidget(self.btn_toggle_api_key)
        trans_layout.addLayout(api_key_row, 1, 1)

        self.chk_protect_acronyms = QCheckBox("Protect Acronyms (recommended)")
        self.chk_protect_acronyms.setChecked(True)
        trans_layout.addWidget(self.chk_protect_acronyms, 2, 0, 1, 2)

        page.add_widget(trans_group)

        cache_group = QGroupBox("Cache Settings")
        cache_layout = QGridLayout(cache_group)
        cache_layout.setColumnStretch(1, 1)
        cache_layout.setVerticalSpacing(12)

        cache_layout.addWidget(QLabel("Cache Expiry (days):"), 0, 0)
        self.spn_cache_days = QSpinBox()
        self.spn_cache_days.setRange(1, 365)
        self.spn_cache_days.setValue(30)
        cache_layout.addWidget(self.spn_cache_days, 0, 1)

        cache_info = QLabel("Translations are cached to reduce API calls.")
        cache_info.setStyleSheet("color: #6B7280; font-size: 12px;")
        cache_layout.addWidget(cache_info, 1, 0, 1, 2)

        self.btn_clear_cache = QPushButton("Clear Translation Cache")
        self.btn_clear_cache.setMinimumWidth(200)
        self.btn_clear_cache.clicked.connect(self.run_clear_cache)
        cache_layout.addWidget(self.btn_clear_cache, 2, 0, 1, 2)

        page.add_widget(cache_group)

        profile_group = QGroupBox("Profile")
        profile_layout = QHBoxLayout(profile_group)

        self.btn_save_profile = QPushButton("Save Profile")
        self.btn_save_profile.setMinimumWidth(130)
        self.btn_save_profile.clicked.connect(self._save_profile)
        profile_layout.addWidget(self.btn_save_profile)

        self.btn_load_profile = QPushButton("Load Profile")
        self.btn_load_profile.setMinimumWidth(130)
        self.btn_load_profile.clicked.connect(self._load_profile)
        profile_layout.addWidget(self.btn_load_profile)

        profile_layout.addStretch()

        page.add_widget(profile_group)
        page.add_stretch()

        self.stack.addWidget(page)

    def _toggle_api_key_visibility(self, checked: bool):
        if checked:
            self.txt_api_key.setEchoMode(QLineEdit.EchoMode.Normal)
            self.btn_toggle_api_key.setText("Hide")
        else:
            self.txt_api_key.setEchoMode(QLineEdit.EchoMode.Password)
            self.btn_toggle_api_key.setText("Show")

    def _browse_backup(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Backup Folder")
        if folder:
            self.txt_backup.setText(folder)

    def _browse_rollback_file(self):
        filepath, _ = QFileDialog.getOpenFileName(
            self,
            "Select Backup File",
            self.controller.backup_folder or "",
            "JSON Files (*.json)"
        )
        if filepath:
            self.txt_rollback_file.setText(filepath)

    def _load_saved_credentials(self):
        data = self.controller.load_profile(CREDENTIALS_FILE)
        if data:
            self.txt_subdomain.setText(data.get('subdomain', ''))
            self.txt_email.setText(data.get('email', ''))
            self.txt_token.setText(data.get('token', ''))
            self.txt_backup.setText(data.get('backup_path', ''))
            self.txt_api_key.setText(data.get('google_api_key', ''))
            self.chk_protect_acronyms.setChecked(
                data.get('protect_acronyms', True)
            )
            self.spn_cache_days.setValue(data.get('cache_expiry_days', 30))

    def _save_profile(self):
        filepath, _ = QFileDialog.getSaveFileName(
            self, "Save Profile", "", "JSON Files (*.json)"
        )
        if filepath:
            success = self.controller.save_profile(
                filepath,
                self.txt_subdomain.text(),
                self.txt_email.text(),
                self.txt_token.text(),
                self.txt_backup.text(),
                self.txt_api_key.text(),
                self.chk_protect_acronyms.isChecked(),
                self.spn_cache_days.value()
            )
            if success:
                QMessageBox.information(self, "Success", "Profile saved.")
            else:
                QMessageBox.warning(self, "Error", "Failed to save profile.")

    def _load_profile(self):
        filepath, _ = QFileDialog.getOpenFileName(
            self, "Load Profile", "", "JSON Files (*.json)"
        )
        if filepath:
            data = self.controller.load_profile(filepath)
            if data:
                self.txt_subdomain.setText(data.get('subdomain', ''))
                self.txt_email.setText(data.get('email', ''))
                self.txt_token.setText(data.get('token', ''))
                self.txt_backup.setText(data.get('backup_path', ''))
                self.txt_api_key.setText(data.get('google_api_key', ''))
                self.chk_protect_acronyms.setChecked(
                    data.get('protect_acronyms', True)
                )
                self.spn_cache_days.setValue(
                    data.get('cache_expiry_days', 30)
                )
                QMessageBox.information(self, "Success", "Profile loaded.")
            else:
                QMessageBox.warning(self, "Error", "Failed to load profile.")

    def goto(self, index: int):
        if index == 3:
            self._log_apply_summary()
        self.stack.setCurrentIndex(index)
        self.sidebar.select(index)

    def log_msg(self, message: str):
        self.log_panel.append(message)

    def lock_ui(self, locked: bool):
        self.sidebar.setEnabled(not locked)
        self.btn_connect.setEnabled(not locked)
        self.btn_scan.setEnabled(not locked)
        self.btn_translate.setEnabled(not locked)
        self.btn_apply.setEnabled(not locked)
        self.btn_rollback.setEnabled(not locked)
        self.btn_load_backup.setEnabled(not locked)
        self.btn_clear_cache.setEnabled(not locked)

    def _on_state_change(self, state: AppState):
        pass

    def _on_cancel_clicked(self):
        self.controller.stop()
        self.log_msg("Cancellation requested...")

    def _cleanup_worker(self):
        with self._worker_lock:
            if self.worker is not None:
                self.worker.cancel()
                if self.worker.isRunning():
                    self.worker.quit()
                    self.worker.wait(UI_CONFIG.WORKER_STOP_TIMEOUT_MS)
                self.worker = None

    def _on_table_stats_updated(self, stats: dict):
        """Handle table statistics update - FIXED to use items_pending."""
        total = stats.get('total', 0)
        from_dc = stats.get('items_from_dc', 0)
        translated = stats.get('items_translated', 0)
        failed = stats.get('items_failed', 0)
        attention = stats.get('items_attention', 0)
        reserved = stats.get('items_reserved', 0)
        pending = stats.get('items_pending', 0)
        selected = stats.get('selected_count', 0)

        self.lbl_sum_total.setText(f"Total: {total}")
        self.lbl_sum_from_dc.setText(f"From DC: {from_dc}")
        self.lbl_sum_translated.setText(f"Translated: {translated}")
        self.lbl_sum_failed.setText(f"Failed: {failed}")
        self.lbl_sum_attention.setText(f"Attention: {attention}")
        self.lbl_sum_reserved.setText(f"System: {reserved}")
        self.lbl_sum_selected.setText(f"Selected: {selected}")
        self.lbl_sum_pending.setText(f"Pending: {pending}")

        self._cached_stats = stats
        self._update_will_translate_label()

    def _on_cell_edited(self, row: int, field: str, value: str, source: str):
        if 0 <= row < len(self._work_items_cache):
            self._work_items_cache[row][field] = value
            self._work_items_cache[row][f'{field}_source'] = source
            self.controller.update_work_item(row, {
                field: value,
                f'{field}_source': source
            })

    def _on_selection_changed(self, count: int):
        self.lbl_sum_selected.setText(f"Selected: {count}")
        self._update_will_translate_label()

    def _on_table_loading_finished(self):
        self.apply_table_filter()

    def _update_will_translate_label(self):
        """Update the 'will translate' information label."""
        if not hasattr(self, 'preview_table') or not self.preview_table:
            return

        selected_rows = self.preview_table.get_selected_rows()
        selected_count = len(selected_rows)

        if selected_count == 0:
            self.lbl_will_translate.setText("ℹ️ Select items to translate")
            self.lbl_will_translate.setStyleSheet(
                "background-color: #F3F4F6; color: #6B7280; "
                "padding: 4px 10px; border-radius: 4px; font-size: 11px;"
            )
            return

        force_retranslate = self.chk_force_translate.isChecked()

        will_translate = 0
        already_done = 0
        system_skipped = 0

        # Define which sources count as "translated"
        translated_sources = frozenset({
            SOURCE_TRANSLATED,
            SOURCE_CACHE,
            SOURCE_MANUAL,
            SOURCE_ZENDESK_DC,
        })

        for row in selected_rows:
            if row < len(self._work_items_cache):
                item = self._work_items_cache[row]

                if (item.get('is_system', False) or
                        item.get('is_reserved', False)):
                    system_skipped += 1
                    continue

                en_source = item.get('en_source', SOURCE_NEW)
                es_source = item.get('es_source', SOURCE_NEW)

                is_translated = (
                    en_source in translated_sources and
                    es_source in translated_sources
                )

                if force_retranslate:
                    will_translate += 1
                elif not is_translated:
                    will_translate += 1
                else:
                    already_done += 1

        if force_retranslate:
            msg = f"🔄 RE-TRANSLATE: {will_translate}"
            if system_skipped > 0:
                msg += f" (skip {system_skipped} sys)"
            style = (
                "background-color: #FEF3C7; color: #92400E; "
                "padding: 4px 10px; border-radius: 4px; font-size: 11px; "
                "font-weight: 500;"
            )
        else:
            if will_translate > 0:
                msg = f"📝 Translate: {will_translate}"
                if already_done > 0:
                    msg += f" (skip {already_done} done)"
                if system_skipped > 0:
                    msg += f" ({system_skipped} sys)"
                style = (
                    "background-color: #EFF6FF; color: #1E40AF; "
                    "padding: 4px 10px; border-radius: 4px; font-size: 11px;"
                )
            else:
                msg = f"✅ All {already_done} translated"
                if system_skipped > 0:
                    msg += f" ({system_skipped} sys)"
                style = (
                    "background-color: #DCFCE7; color: #166534; "
                    "padding: 4px 10px; border-radius: 4px; font-size: 11px;"
                )

        self.lbl_will_translate.setText(msg)
        self.lbl_will_translate.setStyleSheet(style)

    def run_connect(self):
        if self.state_manager.is_busy:
            return

        subdomain = self.txt_subdomain.text().strip()
        email = self.txt_email.text().strip()
        token = self.txt_token.text().strip()
        backup = self.txt_backup.text().strip()

        if not subdomain or not email or not token:
            QMessageBox.warning(
                self, "Warning", "Please fill in all credential fields."
            )
            return

        if self.chk_save_credentials.isChecked():
            self.controller.save_profile(
                CREDENTIALS_FILE, subdomain, email, token, backup,
                self.txt_api_key.text(),
                self.chk_protect_acronyms.isChecked(),
                self.spn_cache_days.value()
            )

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Connecting...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.connect(
                    subdomain, email, token, backup, l
                )
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Connecting...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_connect_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_connect_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if success:
            self._is_connected = True
            self._update_sidebar_state()
            self.status_bar.finish("Connected", True)
            self.log_msg(str(result))
            self.goto(1)
        else:
            self._is_connected = False
            self._update_sidebar_state()
            self.status_bar.finish("Failed", False)
            QMessageBox.critical(self, "Connection Error", str(result))

    def run_scan(self):
        if self.state_manager.is_busy:
            return

        config = {
            'fields': self.chk_scan_fields.isChecked(),
            'forms': self.chk_scan_forms.isChecked(),
            'user_fields': self.chk_scan_user_fields.isChecked(),
            'org_fields': self.chk_scan_org_fields.isChecked(),
            'macros': self.chk_scan_macros.isChecked(),
            'triggers': self.chk_scan_triggers.isChecked(),
            'automations': self.chk_scan_automations.isChecked(),
            'views': self.chk_scan_views.isChecked(),
            'sla_policies': self.chk_scan_sla_policies.isChecked(),
            'custom_statuses': self.chk_scan_custom_statuses.isChecked(),
            'groups': self.chk_scan_groups.isChecked(),
            'cats': self.chk_scan_cats.isChecked(),
            'sects': self.chk_scan_sects.isChecked(),
            'arts': self.chk_scan_arts.isChecked()
        }

        if not any(config.values()):
            QMessageBox.warning(
                self, "Warning", "Select at least one item type to scan."
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
            self.status_bar.finish("Failed", False)
            if "Canceled" not in str(result):
                QMessageBox.critical(self, "Error", str(result))
            return

        self._has_scan_data = True
        self._update_sidebar_state()

        self.status_bar.finish("Scan Complete", True)
        stats = result
        self._work_items_cache = self.controller.work_items

        self.log_msg("=" * 50)
        self.log_msg("SCAN RESULTS")
        self.log_msg("=" * 50)
        self.log_msg(f"Ticket Fields: {stats.get('valid_fields', 0)}")
        self.log_msg(f"Ticket Forms: {stats.get('valid_forms', 0)}")
        self.log_msg(f"Custom Statuses: {stats.get('valid_custom_statuses', 0)}")
        self.log_msg(f"User Fields: {stats.get('valid_user_fields', 0)}")
        self.log_msg(f"Organization Fields: {stats.get('valid_org_fields', 0)}")
        self.log_msg(f"Groups: {stats.get('valid_groups', 0)}")
        self.log_msg(f"Macros: {stats.get('valid_macros', 0)}")
        self.log_msg(f"Triggers: {stats.get('valid_triggers', 0)}")
        self.log_msg(f"Automations: {stats.get('valid_automations', 0)}")
        self.log_msg(f"Views: {stats.get('valid_views', 0)}")
        self.log_msg(f"SLA Policies: {stats.get('valid_sla_policies', 0)}")
        self.log_msg(f"HC Categories: {stats.get('valid_cats', 0)}")
        self.log_msg(f"HC Sections: {stats.get('valid_sects', 0)}")
        self.log_msg(f"HC Articles: {stats.get('valid_arts', 0)}")
        self.log_msg("-" * 50)
        self.log_msg(f"TOTAL ITEMS: {len(self._work_items_cache)}")
        self.log_msg(f"System Fields: {stats.get('system_excluded', 0)}")
        self.log_msg("=" * 50)

        self.populate_preview(preserve_selection=False)
        self.goto(2)

    def populate_preview(self, preserve_selection: bool = True):
        self._work_items_cache = self.controller.work_items

        selection_state = None
        if preserve_selection:
            selection_state = self.preview_table.get_selection_state()

        self.preview_table.set_filter_settings(
            self.chk_filter_ticket.isChecked(),
            self.chk_filter_status.isChecked(),
            self.chk_filter_user.isChecked(),
            self.chk_filter_org.isChecked(),
            self.chk_filter_rules.isChecked(),
            self.chk_filter_admin.isChecked(),
            self.chk_filter_hc.isChecked(),
            self.chk_filter_reserved.isChecked()
        )

        self.preview_table.start_async_load(
            self._work_items_cache,
            "preview",
            batch_size=UI_CONFIG.TABLE_BATCH_SIZE,
            selection_state=selection_state
        )

    def apply_table_filter(self):
        self.preview_table.apply_filters(
            show_ticket=self.chk_filter_ticket.isChecked(),
            show_status=self.chk_filter_status.isChecked(),
            show_user=self.chk_filter_user.isChecked(),
            show_org=self.chk_filter_org.isChecked(),
            show_rules=self.chk_filter_rules.isChecked(),
            show_admin=self.chk_filter_admin.isChecked(),
            show_hc=self.chk_filter_hc.isChecked(),
            show_reserved=self.chk_filter_reserved.isChecked()
        )

        selected_count = self.preview_table.get_selected_count()
        self.lbl_sum_selected.setText(f"Selected: {selected_count}")
        self._update_will_translate_label()

    def _select_all_visible(self):
        self.preview_table.select_all_visible()
        selected_count = self.preview_table.get_selected_count()
        self.lbl_sum_selected.setText(f"Selected: {selected_count}")
        self._update_will_translate_label()

    def _deselect_all_visible(self):
        self.preview_table.deselect_all_visible()
        selected_count = self.preview_table.get_selected_count()
        self.lbl_sum_selected.setText(f"Selected: {selected_count}")
        self._update_will_translate_label()

    def _invert_selection(self):
        self.preview_table.invert_selection_visible()
        selected_count = self.preview_table.get_selected_count()
        self.lbl_sum_selected.setText(f"Selected: {selected_count}")
        self._update_will_translate_label()

    def _get_visible_selected_indices(self) -> List[int]:
        selected_rows = self.preview_table.get_selected_rows()
        return selected_rows

    def run_translation(self):
        if self.state_manager.is_busy:
            return

        selected_indices = self._get_visible_selected_indices()

        if not selected_indices:
            QMessageBox.warning(
                self, "Warning",
                "No items selected for translation.\n\n"
                "Please select items using:\n"
                "• Individual checkboxes in the table\n"
                "• 'Select All' button to select all visible items\n"
                "• 'Invert Selection' to toggle selection"
            )
            return

        self.controller.set_translation_config(
            self.cmb_provider.currentText(),
            self.txt_api_key.text(),
            self.chk_protect_acronyms.isChecked(),
            self.spn_cache_days.value()
        )

        force = self.chk_force_translate.isChecked()

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Translating...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.perform_translation_for_indices(
                    p, l, selected_indices, force
                )
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Translating...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_translation_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_translation_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            self.status_bar.finish("Failed", False)
            if "Canceled" not in str(result):
                QMessageBox.critical(self, "Error", str(result))
            return

        self.status_bar.finish("Translation Complete", True)
        stats = result

        self.log_msg("=" * 50)
        self.log_msg("TRANSLATION RESULTS")
        self.log_msg("=" * 50)
        self.log_msg(f"Total: {stats.total}")
        self.log_msg(f"Translated: {stats.translated}")
        self.log_msg(f"From Cache: {stats.from_cache}")
        self.log_msg(f"Failed: {stats.failed}")
        self.log_msg(f"Success Rate: {stats.success_rate:.1f}%")
        self.log_msg("=" * 50)

        self.populate_preview(preserve_selection=True)

    def _log_apply_summary(self):
        selected_rows = self.preview_table.get_selected_rows()

        selected_items = []
        for row in selected_rows:
            if row < len(self._work_items_cache):
                selected_items.append(self._work_items_cache[row])

        non_system_selected = [
            i for i in selected_items if not i.get('is_system', False)
        ]

        ready = sum(
            1 for i in non_system_selected if i.get('en') and i.get('es')
        )
        pending = len(non_system_selected) - ready

        self.log_msg("=" * 50)
        self.log_msg("APPLY SUMMARY")
        self.log_msg("=" * 50)
        self.log_msg(f"Selected: {len(non_system_selected)} items")
        self.log_msg(f"Ready to apply: {ready}")
        self.log_msg(f"Still need translation: {pending}")
        self.log_msg("=" * 50)

    def _get_selected_items(self) -> List[Dict[str, Any]]:
        """Get selected items with force_update flag."""
        selected_rows = self.preview_table.get_selected_rows()
        force_update = self.chk_update_existing_dc.isChecked()

        selected_items = []
        for row in selected_rows:
            if row < len(self._work_items_cache):
                item = self._work_items_cache[row].copy()
                item['force_update'] = force_update
                selected_items.append(item)

        return selected_items

    def run_apply(self):
        if self.state_manager.is_busy:
            return

        selected = self._get_selected_items()
        non_system_selected = [
            i for i in selected if not i.get('is_system', False)
        ]

        if not non_system_selected:
            QMessageBox.warning(
                self, "Warning",
                "No items selected. Select items in the Preview tab.\n\n"
                "Note: System items cannot be modified."
            )
            return

        not_ready = [
            i for i in non_system_selected
            if not i.get('en') or not i.get('es')
        ]
        if not_ready:
            reply = QMessageBox.question(
                self, "Incomplete Translations",
                f"{len(not_ready)} item(s) don't have complete translations.\n"
                f"Continue anyway?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                return

        update_existing = self.chk_update_existing_dc.isChecked()
        confirm_msg = (
            f"This will modify {len(non_system_selected)} items in Zendesk.\n"
        )
        if update_existing:
            confirm_msg += "Existing DC translations will be updated.\n"
        confirm_msg += "A backup will be created first.\n\nContinue?"

        reply = QMessageBox.question(
            self, "Confirm Apply",
            confirm_msg,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.No:
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Applying...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.execute_changes(
                    non_system_selected, p, l
                )
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

    def on_apply_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            self.status_bar.finish("Failed", False)
            if "Canceled" not in str(result):
                QMessageBox.critical(self, "Error", str(result))
            return

        self.status_bar.finish("Apply Complete", True)

        if isinstance(result, dict):
            success_count = len(result.get('success', []))
            failed_count = len(result.get('failed', []))
            backup_file = result.get('backup_file', '')

            self.log_msg("=" * 50)
            self.log_msg("APPLY RESULTS")
            self.log_msg("=" * 50)
            self.log_msg(f"Succeeded: {success_count}")
            self.log_msg(f"Failed: {failed_count}")
            if backup_file:
                self.log_msg(f"Backup: {backup_file}")
            self.log_msg("=" * 50)

            if failed_count > 0:
                QMessageBox.warning(
                    self, "Partial Success",
                    f"{success_count} items applied successfully.\n"
                    f"{failed_count} items failed.\n"
                    f"Check the log for details."
                )
            else:
                QMessageBox.information(
                    self, "Success",
                    f"All {success_count} items applied successfully.\n"
                    f"Backup: {backup_file}"
                )

    def run_load_backup(self):
        if self.state_manager.is_busy:
            return

        filepath = self.txt_rollback_file.text().strip()
        if not filepath:
            QMessageBox.warning(
                self, "Warning", "Please select a backup file first."
            )
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Loading backup...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.load_backup_thread(p, l, filepath)
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Loading...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_load_backup_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_load_backup_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            self.status_bar.finish("Failed", False)
            QMessageBox.critical(self, "Error", str(result))
            return

        self.status_bar.finish("Backup Loaded", True)

        items = result
        self._pending_backup_items = items

        type_counts: Dict[str, int] = {}
        for item in items:
            t = item.get('type', 'unknown')
            type_counts[t] = type_counts.get(t, 0) + 1

        self.log_msg("=" * 50)
        self.log_msg(f"BACKUP LOADED: {len(items)} items")
        self.log_msg("=" * 50)
        for t, count in sorted(type_counts.items()):
            self.log_msg(f"  {t}: {count}")
        self.log_msg("=" * 50)

        info_text = f"Loaded {len(items)} items:\n\n"
        for t, count in sorted(type_counts.items()):
            info_text += f"  {t}: {count}\n"
        self.rollback_info.setText(info_text)

        self.btn_rollback.setEnabled(True)

    def run_rollback(self):
        if self.state_manager.is_busy:
            return

        if not self._pending_backup_items:
            QMessageBox.warning(
                self, "Warning", "Please load a backup file first."
            )
            return

        reply = QMessageBox.question(
            self, "Confirm Rollback",
            f"This will restore {len(self._pending_backup_items)} items "
            f"to their original values.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.No:
            return

        self._cleanup_worker()
        self.status_bar.show_progress(0, 0, "Rolling back...", "")
        self.lock_ui(True)

        with self._worker_lock:
            self.worker = StepWorker(
                lambda p, l: self.controller.perform_restore_from_data(
                    self._pending_backup_items, p, l
                )
            )
            self.worker.progress.connect(
                lambda c, t, m: self.status_bar.show_progress(
                    c, t, "Rolling back...", m
                )
            )
            self.worker.log.connect(self.log_msg)
            self.worker.result.connect(self.on_rollback_finished)
            self.worker.finished.connect(self.status_bar.reset_ui)
            self.worker.start()

    def on_rollback_finished(self, success: bool, result: object):
        self.state_manager.force_reset()
        self.lock_ui(False)

        if not success:
            self.status_bar.finish("Failed", False)
            if "Canceled" not in str(result):
                QMessageBox.critical(self, "Error", str(result))
            return

        self.status_bar.finish("Rollback Complete", True)
        self.log_msg("=" * 50)
        self.log_msg("ROLLBACK COMPLETE")
        self.log_msg(str(result))
        self.log_msg("=" * 50)
        QMessageBox.information(self, "Success", str(result))

    def run_clear_cache(self):
        if self.state_manager.is_busy:
            return

        reply = QMessageBox.question(
            self, "Confirm Clear Cache",
            "This will delete all cached translations.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.No:
            return

        success = self.controller.clear_cache()
        if success:
            self.log_msg("Translation cache cleared.")
            QMessageBox.information(
                self, "Success", "Translation cache cleared."
            )
        else:
            QMessageBox.warning(self, "Error", "Failed to clear cache.")

    def closeEvent(self, event):
        self._cleanup_worker()
        self.preview_table.cancel_loading()
        self.controller.cleanup()
        self.status_bar.stop_timer()
        event.accept()