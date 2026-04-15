import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import requests
import json
import threading
import logging
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# --- CONFIGURATION & THEME ---
COLOR_BG = "#F0F2F5"
COLOR_CARD = "#FFFFFF"
COLOR_TEXT = "#1C1E21"
COLOR_SUBTEXT = "#606770"
COLOR_ACCENT = "#1877F2"
COLOR_DANGER = "#DC3545"
COLOR_BORDER = "#CCD0D5"

FONT_MAIN = ("Segoe UI", 10)
FONT_BOLD = ("Segoe UI", 10, "bold")
FONT_HEADER = ("Segoe UI", 11, "bold")

# --- ICONS ---
ICON_CHECKED = "☑"
ICON_UNCHECKED = "☐"

# --- SYSTEM FIELD BLOCKLIST (EXPANDED) ---
# Comprehensive list including Support, AI, WFM, Messaging, Approvals, and App keys.
SYSTEM_KEYS = {
    # 1. Standard Support Fields
    'subject', 'description', 'status', 'custom_status_id', 'ticket_type', 'tickettype', 'priority',
    'group', 'group_id', 'assignee', 'assignee_id', 'requester', 'requester_id',
    'submitter', 'submitter_id', 'organization', 'organization_id',
    'satisfaction_rating', 'satisfaction_probability',
    'created_at', 'updated_at', 'generated_timestamp', 'due_date',
    'tags', 'ticket_form_id', 'brand', 'brand_id', 'external_id',
    'problem_id', 'recipient', 'recipient_email', 'via_id',
    'followers', 'email_cc', 'allow_channelback', 'allow_attachments',
    'is_public', 'collaborator_ids', 'follower_ids', 'email_cc_ids',
    'resolution_type',

    # 2. Approvals & Workflows
    'approval_status', 'approval_status_id', 

    # 3. Zendesk Intelligent Triage & AI Summary
    'intent', 'intent_confidence',
    'sentiment', 'sentiment_confidence', 'sentiment_score',
    'language', 'language_confidence',
    'suggestion', 'suggestion_confidence',
    'summary', 'summary_is_public', 
    
    # 3a. AI Summary App Artifacts (Variations)
    'summary_data_and_time', 'summary_date_and_time', # Covers typo variations
    'summary_locate', 'summary_locale', 

    # 4. Zendesk AI Agents (Ultimate) & Messaging
    'platform_conversation_id', 'conversation_id', 'visitor_id',
    'bot_id', 'botid', 'dialogflow_conversation_id',
    'visitor_name', 'visitor_email', 'visitor_phone_number',
    'chat_id', 'chat_group_id', 'chat_engagement_id',

    # 5. Voice / Talk System Fields
    'call_duration', 'recording_url', 'transcription_text',
    'call_status', 'call_type', 'to_number', 'from_number',
    'call_sid', 'call_id',

    # 6. User & Organization System Fields
    'email', 'details', 'notes', 'phone', 'mobile',
    'time_zone', 'locale', 'photo', 'shared_phone_number',
    'authenticity_token', 'active', 'alias', 'signature',
    'role', 'custom_role_id', 'moderator', 'only_private_comments',
    'restricted_agent', 'suspended', 'two_factor_auth_enabled'
}

# --- SYSTEM TITLE BLOCKLIST ---
# Failsafe: Matches Display Titles (case-insensitive)
SYSTEM_TITLES = {
    "type", "priority", "status", "group", "assignee",
    "intent", "sentiment", "language", "satisfaction",
    "intent confidence", "sentiment confidence", "language confidence",
    "resolution type", "resolution",
    "approval status",
    "summary", "summary agent id", 
    "summary data and time", "summary date and time", # Covers both
    "summary locate", "summary locale" # Covers both
}

# --- API KEY MAPPING ---
KEY_MAP = {
    'ticket_fields': 'ticket_field',
    'user_fields': 'user_field',
    'organization_fields': 'organization_field',
    'ticket_forms': 'ticket_form',
    'macros': 'macro',
    'triggers': 'trigger',
    'automations': 'automation',
    'views': 'view',
    'dynamic_content/items': 'item'
}


class TextHandler(logging.Handler):
    """Redirects logging output to a Tkinter ScrolledText widget."""

    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        level = record.levelname  # 'INFO', 'WARNING', 'ERROR'

        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n', level)
            self.text_widget.see(tk.END)
            self.text_widget.configure(state='disabled')
        self.text_widget.after(0, append)


class ZendeskApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Zendesk Delete Tool (Beta)")
        self.root.configure(bg=COLOR_BG)

        self.setup_styles()
        self.setup_window()

        # Data & State
        self.subdomain_var = tk.StringVar()
        self.email_var = tk.StringVar()
        self.token_var = tk.StringVar()

        self.progress_var = tk.DoubleVar()
        self.status_var = tk.StringVar(value="Ready")
        self.time_info_var = tk.StringVar(value="")

        # Main Data Store
        self.items_map = {}
        self.visible_items = []
        self.is_working = False
        self.start_time = 0

        # Stop Signal
        self.stop_event = threading.Event()
        self._map_lock = threading.Lock()

        # HTTP Session (connection pooling + CA bundle resolved once)
        self._session = requests.Session()
        ca_bundle = os.path.expanduser('~/.nscacert_combined.pem')
        if os.path.exists(ca_bundle):
            self._session.verify = ca_bundle

        self.count_found_var = tk.StringVar(value="Found: 0")
        self.count_selected_var = tk.StringVar(value="Selected: 0")
        self.all_checked = False
        self.selected_count = 0

        # Tooltip state
        self._tooltip_win = None
        self._tooltip_after_id = None

        # Filter Vars
        self.filter_mode_var = tk.StringVar(value="Fields & Forms")
        self.filter_category_var = tk.StringVar(value="All")
        self.filter_status_var = tk.StringVar(value="All")
        self.filter_usage_var = tk.StringVar(value="All")
        self.filter_search_var = tk.StringVar()

        self.create_layout()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        logging.info("System Ready. Window maximized.")

    def setup_window(self):
        """Center and maximize window."""
        self.root.update_idletasks()
        self.root.minsize(1250, 800)

        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        target_w = int(screen_width * 0.95)
        target_h = int(screen_height * 0.90)
        pos_x = (screen_width - target_w) // 2
        pos_y = (screen_height - target_h) // 2

        self.root.geometry(f"{target_w}x{target_h}+{pos_x}+{pos_y}")
        try:
            if self.root.tk.call('tk', 'windowingsystem') == 'win32':
                self.root.state('zoomed')
            else:
                self.root.lift()
        except Exception:
            pass

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')

        style.configure(
            ".", background=COLOR_BG, foreground=COLOR_TEXT, font=FONT_MAIN
        )
        style.configure("TFrame", background=COLOR_BG)

        # Cards
        style.configure(
            "Card.TFrame",
            background=COLOR_CARD,
            relief="solid",
            borderwidth=1,
            bordercolor=COLOR_BORDER
        )
        style.configure(
            "Card.TLabel",
            background=COLOR_CARD,
            font=FONT_BOLD,
            foreground=COLOR_TEXT
        )

        # Inputs
        style.configure(
            "TEntry",
            fieldbackground="#FFFFFF",
            foreground="#000000",
            bordercolor=COLOR_BORDER
        )
        style.configure(
            "TCombobox",
            fieldbackground="#FFFFFF",
            foreground="#000000",
            arrowcolor=COLOR_TEXT
        )

        # Buttons
        style.configure(
            "TButton",
            background=COLOR_ACCENT,
            foreground="white",
            borderwidth=0,
            font=FONT_BOLD,
            padding=6
        )
        style.map(
            "TButton",
            background=[('active', '#1565C0')],
            relief=[('disabled', 'flat')]
        )

        style.configure(
            "Danger.TButton",
            background=COLOR_DANGER,
            foreground="white"
        )
        style.map(
            "Danger.TButton",
            background=[('active', '#C82333')]
        )

        # Treeview
        style.configure(
            "Treeview",
            background="#FFFFFF",
            fieldbackground="#FFFFFF",
            foreground=COLOR_TEXT,
            rowheight=35,
            font=FONT_MAIN
        )
        style.configure(
            "Treeview.Heading",
            background="#E4E6EB",
            foreground=COLOR_TEXT,
            font=FONT_HEADER
        )
        style.map(
            "Treeview",
            background=[('selected', '#E7F3FF')],
            foreground=[('selected', COLOR_TEXT)]
        )

        # Progress Bar
        style.configure(
            "Horizontal.TProgressbar",
            background=COLOR_ACCENT,
            troughcolor="#E4E6EB",
            bordercolor=COLOR_BG
        )

        # Status Bar
        style.configure("Status.TFrame", background=COLOR_CARD)
        style.configure(
            "Status.TLabel",
            background=COLOR_CARD,
            foreground=COLOR_SUBTEXT,
            font=("Segoe UI", 9)
        )
        style.configure(
            "StatusBold.TLabel",
            background=COLOR_CARD,
            foreground=COLOR_TEXT,
            font=("Segoe UI", 9, "bold")
        )

    def create_layout(self):
        main_container = ttk.Frame(self.root, padding=20)
        main_container.pack(fill="both", expand=True)

        # --- TOP: CONFIGURATION ---
        config_frame = ttk.Frame(
            main_container, style="Card.TFrame", padding=15
        )
        config_frame.pack(fill="x", pady=(0, 15))
        config_frame.columnconfigure(1, weight=1)
        config_frame.columnconfigure(3, weight=2)
        config_frame.columnconfigure(5, weight=2)

        ttk.Label(
            config_frame,
            text="CONFIGURATION",
            style="Card.TLabel",
            font=("Segoe UI", 12, "bold"),
            foreground=COLOR_ACCENT
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        grid_opts = {'padx': 5, 'pady': 5, 'sticky': 'w'}

        ttk.Label(
            config_frame, text="Subdomain:", style="Card.TLabel"
        ).grid(row=1, column=0, **grid_opts)
        self.entry_sub = ttk.Entry(
            config_frame, textvariable=self.subdomain_var, width=18
        )
        self.entry_sub.grid(row=1, column=1, padx=5, pady=5, sticky='ew')

        ttk.Label(
            config_frame, text="Email:", style="Card.TLabel"
        ).grid(row=1, column=2, **grid_opts)
        self.entry_email = ttk.Entry(
            config_frame, textvariable=self.email_var, width=22
        )
        self.entry_email.grid(row=1, column=3, padx=5, pady=5, sticky='ew')

        ttk.Label(
            config_frame, text="Token:", style="Card.TLabel"
        ).grid(row=1, column=4, **grid_opts)
        self.entry_token = ttk.Entry(
            config_frame,
            textvariable=self.token_var,
            width=22,
            show="●"
        )
        self.entry_token.grid(row=1, column=5, padx=5, pady=5, sticky='ew')

        btn_frame = ttk.Frame(config_frame, style="Card.TFrame", padding=0)
        btn_frame.configure(relief="flat", borderwidth=0)
        btn_frame.grid(row=1, column=6, padx=(10, 0))

        self.btn_load = ttk.Button(
            btn_frame, text="Load Config", command=self.load_config
        )
        self.btn_load.pack(side="left", padx=3)
        self.btn_save = ttk.Button(
            btn_frame, text="Save Config", command=self.save_config
        )
        self.btn_save.pack(side="left", padx=3)
        self.btn_fetch = ttk.Button(
            btn_frame, text="Fetch Data", command=self.start_fetch_thread
        )
        self.btn_fetch.pack(side="left", padx=(3, 0))

        # --- MIDDLE: CONTENT ---
        content_split = ttk.Frame(main_container)
        content_split.pack(fill="both", expand=True)

        # LEFT: FILTERS
        filter_panel = ttk.Frame(
            content_split, style="Card.TFrame", padding=15, width=280
        )
        filter_panel.pack(side="left", fill="y", padx=(0, 15))
        filter_panel.pack_propagate(False)

        ttk.Label(
            filter_panel,
            text="FILTERS",
            style="Card.TLabel",
            font=("Segoe UI", 12, "bold"),
            foreground=COLOR_ACCENT
        ).pack(anchor="w", pady=(0, 15))

        # 1. Primary Mode
        ttk.Label(
            filter_panel, text="Mode", style="Card.TLabel"
        ).pack(anchor="w", pady=(5, 0))
        self.combo_mode = ttk.Combobox(
            filter_panel,
            textvariable=self.filter_mode_var,
            state="readonly",
            values=("Fields & Forms", "Dynamic Content")
        )
        self.combo_mode.pack(fill="x", pady=5)
        self.combo_mode.bind(
            "<<ComboboxSelected>>", self.on_mode_change
        )

        # 1b. Text Search
        ttk.Label(
            filter_panel, text="Search", style="Card.TLabel"
        ).pack(anchor="w", pady=(10, 0))
        self.entry_search = ttk.Entry(
            filter_panel, textvariable=self.filter_search_var
        )
        self.entry_search.pack(fill="x", pady=5)
        self.filter_search_var.trace_add("write", lambda *_: self.apply_filters_only())

        # 2. Category (Context - Only for Fields)
        self.lbl_cat = ttk.Label(
            filter_panel, text="Category", style="Card.TLabel"
        )
        self.combo_category = ttk.Combobox(
            filter_panel,
            textvariable=self.filter_category_var,
            state="readonly",
            values=("All", "Ticket Fields", "Ticket Forms",
                    "User Fields", "Organization Fields")
        )
        self.lbl_cat.pack(anchor="w", pady=(10, 0))
        self.combo_category.pack(fill="x", pady=5)
        self.combo_category.bind(
            "<<ComboboxSelected>>", self.on_secondary_filter_change
        )

        # 3. Usage (Dependency - Only for DC)
        self.lbl_usage = ttk.Label(
            filter_panel, text="Usage", style="Card.TLabel"
        )
        self.combo_usage = ttk.Combobox(
            filter_panel,
            textvariable=self.filter_usage_var,
            state="readonly",
            values=(
                "All", "Unused", "Ticket Field", "Ticket Form",
                "Macro", "Trigger", "Automation", "View"
            )
        )
        self.combo_usage.bind("<<ComboboxSelected>>", self.on_secondary_filter_change)
        # Usage hidden by default

        # 4. Status
        ttk.Label(
            filter_panel, text="Status", style="Card.TLabel"
        ).pack(anchor="w", pady=(10, 0))
        self.combo_status = ttk.Combobox(
            filter_panel,
            textvariable=self.filter_status_var,
            state="readonly",
            values=("All", "Active", "Inactive")
        )
        self.combo_status.pack(fill="x", pady=5)
        self.combo_status.bind(
            "<<ComboboxSelected>>", self.on_secondary_filter_change
        )

        # Date Listbox
        ttk.Label(
            filter_panel, text="Created Date", style="Card.TLabel"
        ).pack(anchor="w", pady=(15, 5))
        ttk.Label(
            filter_panel,
            text="(Ctrl+Click for multiple)",
            font=("Segoe UI", 8),
            foreground=COLOR_SUBTEXT,
            style="Card.TLabel"
        ).pack(anchor="w")

        self.date_listbox = tk.Listbox(
            filter_panel,
            selectmode="multiple",
            height=15,
            bg="#FFFFFF",
            fg="#000000",
            selectbackground=COLOR_ACCENT,
            selectforeground="white",
            relief="solid",
            borderwidth=1
        )
        self.date_listbox.pack(fill="both", expand=True, pady=5)
        self.date_listbox.bind('<<ListboxSelect>>', self.apply_filters_only)

        # RIGHT: DATA TABLE
        right_panel = ttk.Frame(content_split)
        right_panel.pack(side="left", fill="both", expand=True)

        self.tree_frame = ttk.Frame(
            right_panel, style="Card.TFrame", padding=1
        )
        self.tree_frame.pack(fill="both", expand=True, pady=(0, 15))

        self.tree = ttk.Treeview(
            self.tree_frame, columns=("num",), show="headings", selectmode="none"
        )
        self.setup_tree_columns("Fields & Forms")

        sb = ttk.Scrollbar(
            self.tree_frame, orient="vertical", command=self.tree.yview
        )
        self.tree.configure(yscrollcommand=sb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        self.tree.bind('<Button-1>', self.on_tree_click)
        self.tree.bind('<Motion>', self._on_tree_motion)
        self.tree.bind('<Leave>', self._on_tree_leave)

        # --- BOTTOM: ACTIONS & LOGS ---
        bottom_frame = ttk.Frame(right_panel, style="Card.TFrame", padding=15)
        bottom_frame.pack(fill="x")

        # Action Bar
        action_bar = ttk.Frame(
            bottom_frame, style="Card.TFrame", relief="flat", borderwidth=0
        )
        action_bar.pack(fill="x", pady=(0, 10))

        lbl_found = ttk.Label(
            action_bar,
            textvariable=self.count_found_var,
            font=("Segoe UI", 11, "bold"),
            foreground=COLOR_ACCENT,
            style="Card.TLabel"
        )
        lbl_found.pack(side="left", padx=(0, 15))

        lbl_selected = ttk.Label(
            action_bar,
            textvariable=self.count_selected_var,
            font=("Segoe UI", 11, "bold"),
            foreground=COLOR_DANGER,
            style="Card.TLabel"
        )
        lbl_selected.pack(side="left")

        # Buttons Right
        btn_right_frame = ttk.Frame(action_bar, style="Card.TFrame", borderwidth=0)
        btn_right_frame.pack(side="right")

        self.btn_stop = ttk.Button(
            btn_right_frame,
            text="STOP",
            style="Danger.TButton",
            command=self.stop_operation,
            state='disabled'
        )
        self.btn_stop.pack(side="left", padx=(0, 10))

        self.btn_del = ttk.Button(
            btn_right_frame,
            text="DELETE SELECTED ITEMS",
            style="Danger.TButton",
            command=self.confirm_delete
        )
        self.btn_del.pack(side="left")

        # STATUS PANEL
        status_frame = ttk.Frame(bottom_frame, style="Status.TFrame")
        status_frame.pack(fill="x", pady=(0, 5))

        ttk.Label(
            status_frame,
            textvariable=self.status_var,
            style="StatusBold.TLabel"
        ).pack(side="left")

        ttk.Label(
            status_frame,
            textvariable=self.time_info_var,
            style="Status.TLabel"
        ).pack(side="right")

        self.progress_bar = ttk.Progressbar(
            bottom_frame,
            variable=self.progress_var,
            maximum=100,
            style="Horizontal.TProgressbar"
        )
        self.progress_bar.pack(fill="x", pady=(0, 10))

        # Log header
        log_header = ttk.Frame(bottom_frame, style="Card.TFrame")
        log_header.pack(fill="x", pady=(0, 2))
        ttk.Label(log_header, text="LOGS", style="Card.TLabel",
                  font=FONT_BOLD).pack(side="left", padx=(6, 0))
        ttk.Button(
            log_header, text="Clear",
            command=self._clear_log,
            style="TButton", width=6
        ).pack(side="right")

        # Log widget
        self.log_text = scrolledtext.ScrolledText(
            bottom_frame,
            height=6,
            state='disabled',
            bg="#F7F8FA",
            fg=COLOR_TEXT,
            font=("Consolas", 10),
            borderwidth=1,
            relief="solid"
        )
        self.log_text.pack(fill="both", expand=True)

        # Level colour tags
        self.log_text.tag_config('INFO',    foreground=COLOR_TEXT)
        self.log_text.tag_config('WARNING', foreground="#D4800A")
        self.log_text.tag_config('ERROR',   foreground=COLOR_DANGER)

        handler = TextHandler(self.log_text)
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s', datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if not any(isinstance(h, TextHandler) for h in logger.handlers):
            logger.addHandler(handler)

        # Initial Filter State
        self.on_mode_change(None)

    # --- HELPER: TREE COLUMN CONFIG ---
    def setup_tree_columns(self, mode):
        if mode == "Dynamic Content":
            # Added "num" as first column
            cols = ("num", "check", "usage", "date", "placeholder", "text", "id")
            self.tree.configure(columns=cols, displaycolumns=cols)

            self.tree.heading("num", text="#")
            self.tree.heading(
                "check",
                text=ICON_UNCHECKED,
                command=self.toggle_all_selection
            )
            self.tree.heading("usage", text="APPLIED IN (USAGE)")
            self.tree.heading("date", text="CREATED")
            self.tree.heading("placeholder", text="PLACEHOLDER")
            self.tree.heading("text", text="DEFAULT TEXT (FLATTEN TO)")
            self.tree.heading("id", text="ID")

            self.tree.column("num", width=40, anchor="center", stretch=False)
            self.tree.column("check", width=40, anchor="center", stretch=False)
            self.tree.column("usage", width=180, anchor="w")
            self.tree.column("date", width=100, anchor="center")
            self.tree.column("placeholder", width=200, anchor="w")
            self.tree.column("text", width=200, anchor="w")
            self.tree.column("id", width=90, anchor="center")

        else:  # Fields & Forms
            # Added "num" as first column
            cols = ("num", "check", "type", "status", "date", "title", "id")
            self.tree.configure(columns=cols, displaycolumns=cols)

            self.tree.heading("num", text="#")
            self.tree.heading(
                "check",
                text=ICON_UNCHECKED,
                command=self.toggle_all_selection
            )
            self.tree.heading("type", text="TYPE")
            self.tree.heading("status", text="STATUS")
            self.tree.heading("date", text="CREATED")
            self.tree.heading("title", text="TITLE")
            self.tree.heading("id", text="ID")

            self.tree.column("num", width=40, anchor="center", stretch=False)
            self.tree.column("check", width=40, anchor="center", stretch=False)
            self.tree.column("type", width=110, anchor="center")
            self.tree.column("status", width=70, anchor="center")
            self.tree.column("date", width=100, anchor="center")
            self.tree.column("title", width=300, anchor="w")
            self.tree.column("id", width=90, anchor="center")

    # --- UI HELPER: TOGGLE STATE ---
    def set_ui_state(self, enabled):
        state = 'normal' if enabled else 'disabled'
        stop_state = 'normal' if not enabled else 'disabled'

        self.btn_stop.configure(state=stop_state)
        self.btn_load.configure(state=state)
        self.btn_save.configure(state=state)
        self.btn_fetch.configure(state=state)
        self.btn_del.configure(state=state)
        self.entry_sub.configure(state=state)
        self.entry_email.configure(state=state)
        self.entry_token.configure(state=state)

        self.combo_mode.configure(state=state)
        self.combo_status.configure(state=state)
        self.combo_category.configure(state=state)

        if self.filter_mode_var.get() == "Dynamic Content":
            self.combo_usage.configure(state=state)
        else:
            self.combo_usage.configure(state='disabled')

        self.entry_search.configure(state=state)
        self.date_listbox.configure(state=state)
        if enabled:
            self.tree.bind('<Button-1>', self.on_tree_click)
            self.tree.bind('<Motion>', self._on_tree_motion)
            self.tree.bind('<Leave>', self._on_tree_leave)
        else:
            self.tree.unbind('<Button-1>')
            self.tree.unbind('<Motion>')
            self.tree.unbind('<Leave>')
            self._hide_tooltip()
            if self._tooltip_after_id:
                self.root.after_cancel(self._tooltip_after_id)
                self._tooltip_after_id = None

    def format_time(self, seconds):
        if seconds < 60:
            return f"{int(seconds)}s"
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"

    def update_clock(self):
        if self.is_working:
            elapsed = time.time() - self.start_time
            self.time_info_var.set(f"Elapsed: {self.format_time(elapsed)}")
            self.root.after(500, self.update_clock)

    def _update_status(self, status=None, progress=None):
        """Thread-safe status/progress update scheduled on the main thread."""
        def _do():
            if status is not None:
                self.status_var.set(status)
            if progress is not None:
                self.progress_var.set(progress)
        self.root.after(0, _do)

    # --- STOP LOGIC ---
    def stop_operation(self):
        if self.is_working:
            self.status_var.set("Stopping operation...")
            logging.warning(
                "User requested STOP. Please wait for current tasks..."
            )
            self.stop_event.set()

    # --- LOGIC ---
    def load_config(self):
        f = filedialog.askopenfilename(filetypes=[("JSON", "*.json")])
        if f:
            try:
                with open(f) as file:
                    d = json.load(file)
                    self.subdomain_var.set(d.get('subdomain', ''))
                    self.email_var.set(d.get('email', ''))
                    self.token_var.set(d.get('token', ''))
                logging.info("Configuration loaded.")
            except Exception as e:
                logging.error(f"Config Load Error: {e}")

    def save_config(self):
        f = filedialog.asksaveasfilename(
            defaultextension=".json", filetypes=[("JSON", "*.json")]
        )
        if f:
            if not messagebox.askyesno(
                "Security Notice",
                "Your API token will be saved in plain text.\n"
                "Keep this file secure and do not share it.\n\n"
                "Proceed with saving?",
                icon='warning'
            ):
                return
            try:
                data = {
                    "subdomain": self.subdomain_var.get(),
                    "email": self.email_var.get(),
                    "token": self.token_var.get()
                }
                with open(f, 'w') as file:
                    json.dump(data, file)
                logging.info("Configuration saved.")
            except Exception as e:
                logging.error(f"Config Save Error: {e}")

    def start_fetch_thread(self):
        sub = self.subdomain_var.get().strip()
        email = self.email_var.get().strip()
        token = self.token_var.get().strip()
        if not sub or not email or not token:
            messagebox.showwarning(
                "Missing Credentials",
                "Please enter subdomain, email, and token before fetching."
            )
            return

        self.stop_event.clear()
        self.set_ui_state(enabled=False)
        self.progress_var.set(0)
        self.status_var.set("Initializing connection...")
        self.is_working = True
        self.start_time = time.time()
        self.update_clock()

        auth = (f"{email}/token", token)

        threading.Thread(
            target=self.fetch_data_thread,
            args=(sub, auth),
            daemon=True
        ).start()

    def fetch_data_thread(self, sub, auth):
        logging.info(f"Connecting to {sub}.zendesk.com ...")
        temp_items_map = {}
        aux_data = {}
        base_url = f"https://{sub}.zendesk.com/api/v2"

        # Fetch DC category map upfront (gracefully skipped if unavailable)
        dc_cat_map = {}
        cat_resp = self.safe_request('GET', f"{base_url}/dynamic_content/item_categories.json", auth=auth)
        if cat_resp is not None and cat_resp.status_code == 200:
            for cat in cat_resp.json().get('item_categories', []):
                dc_cat_map[cat['id']] = cat['name']

        main_endpoints = [
            ("ticket_fields", "Ticket Field"),
            ("user_fields", "User Field"),
            ("organization_fields", "Organization Field"),
            ("ticket_forms", "Ticket Form"),
            ("dynamic_content/items", "Dynamic Content")
        ]

        safety_endpoints = [
            ("macros", "Macro"),
            ("triggers", "Trigger"),
            ("automations", "Automation"),
            ("views", "View")
        ]

        try:
            total_ops = len(main_endpoints) + len(safety_endpoints)
            current_op = 0

            # FETCH MAIN ITEMS
            for ep, label in main_endpoints:
                if self.stop_event.is_set():
                    break
                self._update_status(
                    status=f"Fetching {label}s...",
                    progress=(current_op / total_ops) * 80
                )
                current_op += 1
                ep_count = 0

                url = f"{base_url}/{ep}.json"
                page = 0
                while url:
                    if self.stop_event.is_set():
                        break
                    page += 1
                    if page > 500:
                        logging.warning(f"Pagination limit reached for {label}. Stopping.")
                        break
                    resp = self.safe_request('GET', url, auth=auth)
                    if resp is None or resp.status_code != 200:
                        logging.error(f"API Error {resp.status_code if resp is not None else 'Timeout'}: {label}")
                        break

                    data = resp.json()
                    json_key = "items" if "dynamic_content" in ep else ep
                    items_list = data.get(json_key, [])

                    for item in items_list:

                        # --- ENHANCED SYSTEM FILTERING ---

                        # 1. API Flag Checks
                        # Ticket Fields: 'removable' flag (API source of truth)
                        if 'removable' in item and not item['removable']:
                            continue

                        # User/Org/DC: 'system' flag
                        if item.get('system') is True:
                            continue

                        # Ticket Forms: 'default' forms are system
                        if ep == 'ticket_forms' and item.get('default') is True:
                            continue

                        # 2. Key Blocklist (Cross-Product Safety)
                        raw_key = item.get('key')
                        item_key = str(raw_key).lower() if raw_key else ""
                        if item_key in SYSTEM_KEYS:
                            continue

                        # 3. Title Failsafe (For fields with generated keys but standard titles)
                        raw_title = item.get('title', item.get('name', '')).lower().strip()
                        if raw_title in SYSTEM_TITLES:
                            continue

                        # --- END FILTERING ---

                        extra_data = {}
                        if label == "Dynamic Content":
                            placeholder = item.get('placeholder', '')
                            default_text = ""
                            default_loc = item.get('default_locale_id')
                            for variant in item.get('variants', []):
                                if variant.get('locale_id') == default_loc:
                                    default_text = variant.get('content', '')
                                    break
                            cat_id = item.get('category_id') or item.get('group_id')
                            if cat_id and dc_cat_map:
                                category = dc_cat_map.get(cat_id, '')
                            else:
                                name = item.get('name', '')
                                category = name.split('::')[0].strip() if '::' in name else ''
                            extra_data = {
                                'placeholder': placeholder,
                                'flatten_text': default_text,
                                'is_used': False,
                                'usage_list': [],
                                'category': category
                            }

                        iid = str(item['id'])
                        created_full = item.get('created_at', '')
                        c_date = created_full[:10] if created_full else ""
                        title = item.get(
                            'title',
                            item.get('name', item.get('key', 'No Title'))
                        )

                        temp_items_map[iid] = {
                            "id": iid,
                            "ep": ep,
                            "title": title,
                            "type": label,
                            "active": item.get('active', True),
                            "date": c_date,
                            "checked": False,
                            "extra": extra_data
                        }
                        ep_count += 1
                    url = data.get('next_page')
                if not self.stop_event.is_set():
                    logging.info(f"  {label}s: {ep_count} item(s) fetched")

            # FETCH SAFETY DATA
            if not self.stop_event.is_set():
                for ep, label in safety_endpoints:
                    if self.stop_event.is_set():
                        break
                    self._update_status(
                        status=f"Scanning {label}s for Safety...",
                        progress=(current_op / total_ops) * 80
                    )
                    current_op += 1
                    aux_count = 0

                    url = f"{base_url}/{ep}.json"
                    page = 0
                    while url:
                        if self.stop_event.is_set():
                            break
                        page += 1
                        if page > 500:
                            logging.warning(f"Pagination limit reached for {label}. Stopping.")
                            break
                        resp = self.safe_request('GET', url, auth=auth)
                        if resp is None or resp.status_code != 200:
                            break
                        data = resp.json()
                        items_list = data.get(ep, [])
                        for item in items_list:
                            str_dump = json.dumps(item)
                            aux_data[f"{label}_{item['id']}"] = {
                                "type": label,
                                "name": item.get('title', item.get('name', '')),
                                "content": str_dump
                            }
                            aux_count += 1
                        url = data.get('next_page')
                    if not self.stop_event.is_set():
                        logging.info(f"  {label}s: {aux_count} scanned for dependencies")

        except Exception as e:
            logging.error(f"Critical Fetch Error: {e}")
            self.root.after(0, lambda: self.set_ui_state(True))
            self.is_working = False
            return

        # --- DEPENDENCY ANALYSIS ---
        if not self.stop_event.is_set():
            self._update_status(
                status="Deep Safety Scan (Cross-Referencing)...",
                progress=85
            )

            # Maps field/form title -> (type, title) for DC placeholder lookups
            field_title_map = {
                v['title'].strip(): (v['type'], v['title'].strip())
                for v in temp_items_map.values()
                if v['type'] != "Dynamic Content"
            }

            # Collect unique placeholders to avoid O(n*m) inner loop
            unique_phs = {
                v['extra'].get('placeholder', '').strip()
                for v in temp_items_map.values()
                if v['type'] == "Dynamic Content" and v['extra'].get('placeholder')
            }

            # Single pass over aux_data: build ph -> {types, details} index
            ph_aux_usage = {ph: {'types': set(), 'details': set()} for ph in unique_phs}
            for aux_val in aux_data.values():
                if self.stop_event.is_set():
                    break
                for ph in unique_phs:
                    if ph in aux_val['content']:
                        t = aux_val['type']
                        n = aux_val.get('name', '').strip()
                        ph_aux_usage[ph]['types'].add(t)
                        ph_aux_usage[ph]['details'].add(f"{t}: {n}" if n else t)

            # Apply results to each DC item
            for v in temp_items_map.values():
                if self.stop_event.is_set():
                    break
                if v['type'] != "Dynamic Content":
                    continue
                ph = v['extra'].get('placeholder', '').strip()
                if not ph:
                    continue
                usage_types = set()
                usage_details = set()
                if ph in field_title_map:
                    ftype, fname = field_title_map[ph]
                    usage_types.add(ftype)
                    usage_details.add(f"{ftype}: {fname}")
                aux = ph_aux_usage.get(ph, {})
                usage_types.update(aux.get('types', set()))
                usage_details.update(aux.get('details', set()))
                if usage_types:
                    v['extra']['is_used'] = True
                    v['extra']['usage_list'] = sorted(usage_types)
                    v['extra']['usage_details'] = sorted(usage_details)

            dc_total = sum(1 for v in temp_items_map.values() if v['type'] == "Dynamic Content")
            dc_used  = sum(1 for v in temp_items_map.values()
                          if v['type'] == "Dynamic Content" and v['extra'].get('is_used'))
            if dc_total:
                logging.info(f"  DC dependency scan: {dc_used}/{dc_total} items in use")

        if self.stop_event.is_set():
            logging.warning("Fetch operation stopped by user.")

        self._update_status(status="Finalizing...", progress=95)
        self.root.after(0, lambda: self.finish_fetch_ui(temp_items_map))

    def finish_fetch_ui(self, new_data):
        self.status_var.set("Rendering Table...")
        self.root.update_idletasks()

        self.items_map = new_data
        self.selected_count = 0

        total_time = time.time() - self.start_time

        type_counts = Counter(v['type'] for v in self.items_map.values())
        breakdown = ", ".join(f"{t}: {c}" for t, c in sorted(type_counts.items()))
        logging.info(
            f"Fetch complete. {len(self.items_map)} items in "
            f"{self.format_time(total_time)}. [{breakdown or 'none'}]"
        )
        if type_counts.get("Dynamic Content", 0) == 0:
            logging.warning(
                "No Dynamic Content items were fetched. "
                "Check API permissions or inspect errors above."
            )

        self.progress_var.set(100)
        self.status_var.set("Ready")
        self.time_info_var.set(f"Last Fetch: {self.format_time(total_time)}")

        self.set_ui_state(enabled=True)
        self.is_working = False
        self.progress_var.set(0)

        # Refresh view preserving current mode and filters
        self.on_mode_change(None)

    # --- FILTERING LOGIC ---
    def on_mode_change(self, event):
        mode = self.filter_mode_var.get()

        active_state = 'disabled' if self.is_working else 'normal'

        if mode == "Fields & Forms":
            self.lbl_usage.pack_forget()
            self.combo_usage.pack_forget()
            self.combo_usage.configure(state='disabled')
            self.combo_category.configure(values=(
                "All", "Ticket Fields", "Ticket Forms", "User Fields", "Organization Fields"
            ))
            self.lbl_cat.pack(anchor="w", pady=(10, 0), after=self.combo_mode)
            self.combo_category.pack(fill="x", pady=5, after=self.lbl_cat)
            self.combo_category.configure(state=active_state)
            self.filter_category_var.set("All")
        else:
            self._refresh_dc_category_combo()
            self.lbl_cat.pack(anchor="w", pady=(10, 0), after=self.combo_mode)
            self.combo_category.pack(fill="x", pady=5, after=self.lbl_cat)
            self.combo_category.configure(state=active_state)
            self.lbl_usage.pack(anchor="w", pady=(5, 0), after=self.combo_category)
            self.combo_usage.pack(fill="x", pady=5, after=self.lbl_usage)
            self.combo_usage.configure(state=active_state)
            self.filter_usage_var.set("All")
            self.filter_category_var.set("All")

        self.setup_tree_columns(mode)
        self.update_date_listbox()
        self.apply_filters_only()

    def on_secondary_filter_change(self, event):
        self.update_date_listbox()
        self.apply_filters_only()

    def _refresh_dc_category_combo(self):
        """Populate the category combo with unique DC categories from items_map."""
        cats = sorted({
            v['extra'].get('category', '')
            for v in self.items_map.values()
            if v['type'] == "Dynamic Content" and v['extra'].get('category')
        })
        values = ["All"] + cats
        self.combo_category.configure(values=values)
        if self.filter_category_var.get() not in values:
            self.filter_category_var.set("All")

    def _iter_filtered_items(self, mode, cat, status, usage, selected_dates=None, search=""):
        """Yield (iid, data) pairs matching the current filter state."""
        search_lower = search.lower()
        for iid, data in self.items_map.items():
            if search_lower:
                title = data.get('title', '').lower()
                ph = data.get('extra', {}).get('placeholder', '').lower()
                if search_lower not in title and search_lower not in ph and search_lower not in iid:
                    continue
            if mode == "Dynamic Content":
                if data['type'] != "Dynamic Content":
                    continue
                if cat != "All" and data.get('extra', {}).get('category', '') != cat:
                    continue
            else:
                if data['type'] == "Dynamic Content":
                    continue
                if cat == "Ticket Fields" and data['type'] != "Ticket Field":
                    continue
                if cat == "Ticket Forms" and data['type'] != "Ticket Form":
                    continue
                if cat == "User Fields" and data['type'] != "User Field":
                    continue
                if cat == "Organization Fields" and data['type'] != "Organization Field":
                    continue

            status_str = "Active" if data['active'] else "Inactive"
            if status != "All" and status_str != status:
                continue

            if mode == "Dynamic Content" and usage != "All":
                is_used = data.get('extra', {}).get('is_used', False)
                usage_list = data.get('extra', {}).get('usage_list', [])
                if usage == "Unused" and is_used:
                    continue
                if usage != "Unused":
                    if not is_used:
                        continue
                    if usage not in usage_list:
                        continue

            if selected_dates and data['date'] not in selected_dates:
                continue

            yield iid, data

    def update_date_listbox(self):
        mode = self.filter_mode_var.get()
        cat = self.filter_category_var.get()
        status = self.filter_status_var.get()
        usage = self.filter_usage_var.get()
        search = self.filter_search_var.get()

        valid_dates = {
            data['date']
            for _, data in self._iter_filtered_items(mode, cat, status, usage, search=search)
        }

        self.date_listbox.delete(0, tk.END)
        for d in sorted(valid_dates, reverse=True):
            self.date_listbox.insert(tk.END, d)

    def apply_filters_only(self, event=None):
        self.tree.delete(*self.tree.get_children())
        self.visible_items = []

        mode = self.filter_mode_var.get()
        cat = self.filter_category_var.get()
        status = self.filter_status_var.get()
        usage = self.filter_usage_var.get()
        search = self.filter_search_var.get()

        selected_indices = self.date_listbox.curselection()
        selected_dates = [self.date_listbox.get(i) for i in selected_indices] or None

        for iid, data in self._iter_filtered_items(mode, cat, status, usage, selected_dates, search=search):
            self.visible_items.append(iid)
            icon = ICON_CHECKED if data['checked'] else ICON_UNCHECKED
            row_num = len(self.visible_items)
            status_str = "Active" if data['active'] else "Inactive"

            if mode == "Dynamic Content":
                ph = data['extra'].get('placeholder', '')
                txt = data['extra'].get('flatten_text', '')
                usage_details = data.get('extra', {}).get('usage_details', [])
                usage_display = ", ".join(usage_details) if usage_details else ""
                self.tree.insert(
                    "", "end", iid=iid,
                    values=(row_num, icon, usage_display, data['date'], ph, txt, iid)
                )
            else:
                self.tree.insert(
                    "", "end", iid=iid,
                    values=(row_num, icon, data['type'], status_str, data['date'], data['title'], iid)
                )

        self.update_counter()
        self.count_found_var.set(f"Found: {len(self.visible_items)}")

    def on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region == "cell":
            col = self.tree.identify_column(event.x)
            # Column #2 is now the Checkbox (index 1) because #1 is Row Number
            if col == "#2":
                iid = self.tree.identify_row(event.y)
                if iid:
                    self.toggle_check(iid)

    def _on_tree_motion(self, event):
        # Cancel any pending tooltip
        if self._tooltip_after_id:
            self.root.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None

        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self._hide_tooltip()
            return

        col = self.tree.identify_column(event.x)
        # Skip row-number and checkbox columns
        if col in ("#1", "#2"):
            self._hide_tooltip()
            return

        iid = self.tree.identify_row(event.y)
        if not iid:
            self._hide_tooltip()
            return

        col_index = int(col.lstrip("#")) - 1
        values = self.tree.item(iid, "values")
        if col_index >= len(values):
            self._hide_tooltip()
            return

        text = str(values[col_index]).strip()
        if not text:
            self._hide_tooltip()
            return

        x, y = event.x_root + 12, event.y_root + 16
        self._tooltip_after_id = self.root.after(
            450, lambda: self._show_tooltip(text, x, y)
        )

    def _on_tree_leave(self, _event):
        if self._tooltip_after_id:
            self.root.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None
        self._hide_tooltip()

    def _show_tooltip(self, text, x, y):
        self._hide_tooltip()
        win = tk.Toplevel(self.root)
        win.wm_overrideredirect(True)
        win.wm_geometry("+0+0")  # off-screen placeholder so we can measure
        lbl = tk.Label(
            win, text=text, justify="left",
            background="#ffffe0", foreground="#000000",
            relief="solid", borderwidth=1,
            font=("Segoe UI", 10),
            wraplength=500, padx=6, pady=4
        )
        lbl.pack()
        win.update_idletasks()  # force geometry calculation

        tw = win.winfo_reqwidth()
        th = win.winfo_reqheight()
        sw = win.winfo_screenwidth()
        sh = win.winfo_screenheight()

        if x + tw > sw:
            x = sw - tw - 4
        if y + th > sh:
            y = y - th - 28  # flip above the cursor

        win.wm_geometry(f"+{x}+{y}")
        self._tooltip_win = win

    def _hide_tooltip(self):
        if self._tooltip_win:
            self._tooltip_win.destroy()
            self._tooltip_win = None

    def _clear_log(self):
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', tk.END)
        self.log_text.configure(state='disabled')

    def _on_close(self):
        if self._tooltip_after_id:
            self.root.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None
        self._hide_tooltip()
        self.root.destroy()

    def toggle_check(self, iid):
        was_checked = self.items_map[iid]['checked']
        self.items_map[iid]['checked'] = not was_checked
        self.selected_count += -1 if was_checked else 1
        icon = ICON_CHECKED if self.items_map[iid]['checked'] else ICON_UNCHECKED
        vals = list(self.tree.item(iid, "values"))
        # Update index 1 (Checkbox) not 0
        vals[1] = icon
        self.tree.item(iid, values=vals)
        self.update_counter()

    def toggle_all_selection(self):
        all_visible_checked = bool(self.visible_items) and all(
            self.items_map[iid]['checked'] for iid in self.visible_items
        )
        self.all_checked = not all_visible_checked
        icon = ICON_CHECKED if self.all_checked else ICON_UNCHECKED
        for iid in self.visible_items:
            if self.items_map[iid]['checked'] != self.all_checked:
                self.selected_count += 1 if self.all_checked else -1
            self.items_map[iid]['checked'] = self.all_checked
            vals = list(self.tree.item(iid, "values"))
            vals[1] = icon
            self.tree.item(iid, values=vals)
        self.tree.heading("check", text=icon)
        self.update_counter()

    def update_counter(self):
        self.count_selected_var.set(f"Selected: {self.selected_count}")

    def confirm_delete(self):
        to_delete = [v for k, v in self.items_map.items() if v['checked']]
        count = len(to_delete)
        if count == 0:
            messagebox.showinfo("Wait", "No items selected.")
            return

        risky_items = [
            i for i in to_delete
            if "Macro" in i.get('extra', {}).get('usage_list', []) or
            "Trigger" in i.get('extra', {}).get('usage_list', [])
        ]
        if risky_items:
            msg = (
                f"DANGER: {len(risky_items)} items are used in Macros/Triggers!\n"
                "Deleting them may break your workflows.\n\n"
                "Proceed anyway?"
            )
            if not messagebox.askyesno("CRITICAL WARNING", msg, icon='error'):
                return

        msg = (
            f"WARNING: You are about to PERMANENTLY DELETE {count} items.\n\n"
            "First 5 items:\n"
        )
        for item in to_delete[:5]:
            msg += f"- {item['title']}\n"
        if count > 5:
            msg += "...and others."

        if messagebox.askyesno("CONFIRM DELETION", msg, icon='warning'):
            self.set_ui_state(False)
            self.status_var.set("Preparing to delete...")
            self.is_working = True
            self.start_time = time.time()
            self.update_clock()
            self.stop_event.clear()

            sub = self.subdomain_var.get().strip()
            email = self.email_var.get().strip()
            token = self.token_var.get().strip()
            auth = (f"{email}/token", token)

            threading.Thread(
                target=self.run_delete,
                args=(to_delete, sub, auth),
                daemon=True
            ).start()

    def safe_request(self, method, url, use_session=True, **kwargs):
        """Retries request on 429 (Rate Limit) errors.

        Set use_session=False for thread-safe calls from worker threads.
        """
        kwargs.setdefault('timeout', 30)
        if use_session:
            kwargs.setdefault('verify', self._session.verify)
        else:
            ca_bundle = getattr(self._session, 'verify', True)
            kwargs.setdefault('verify', ca_bundle)
        retries = 3
        for i in range(retries):
            try:
                if use_session:
                    response = self._session.request(method, url, **kwargs)
                else:
                    response = requests.request(method, url, **kwargs)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logging.warning(
                        f"Rate limited (429). Retrying in {retry_after}s..."
                    )
                    time.sleep(retry_after + 1)
                    continue
                return response
            except requests.RequestException as e:
                logging.warning(f"Request Exception (attempt {i + 1}/{retries}): {e}")
                if i < retries - 1:
                    time.sleep(2)
        return None

    def process_single_item(self, item, sub, auth):
        if self.stop_event.is_set():
            return (False, item['id'], f"Skipped (Stopped): {item['title']}")

        if item['type'] == "Dynamic Content":
            base_url = f"https://{sub}.zendesk.com/api/v2/dynamic_content/items/{item['id']}.json"
        else:
            base_url = f"https://{sub}.zendesk.com/api/v2/{item['ep']}/{item['id']}.json"

        try:
            # --- FLATTEN LOGIC (Dynamic Content) ---
            if item['type'] == "Dynamic Content":
                dc_ph = item['extra'].get('placeholder')
                dc_tx = item['extra'].get('flatten_text')
                if dc_ph and dc_tx:
                    with self._map_lock:
                        targets = [
                            v for v in self.items_map.values()
                            if v['title'].strip() == dc_ph.strip()
                        ]
                    if targets:
                        logging.info(
                            f"  Flatten: updating {len(targets)} field(s) "
                            f"using '{item['title']}'"
                        )
                    for t in targets:
                        if self.stop_event.is_set():
                            return (False, item['id'], "Stopped during flatten")
                        upd_url = f"https://{sub}.zendesk.com/api/v2/{t['ep']}/{t['id']}.json"

                        # Use strictly mapped key
                        json_key = KEY_MAP.get(t['ep'])

                        if json_key:
                            payload = {'title': dc_tx}
                            if t['ep'] == 'ticket_forms':
                                payload = {'name': dc_tx}

                            fr = self.safe_request(
                                'PUT', upd_url, use_session=False,
                                json={json_key: payload}, auth=auth
                            )
                            if fr is None or fr.status_code not in [200, 201]:
                                logging.warning(
                                    f"Flatten failed for {t['type']} '{t['title']}' "
                                    f"(HTTP {fr.status_code if fr else 'Timeout'})"
                                )

            # --- DEACTIVATE ---
            if item['active'] and item['type'] != "Dynamic Content":
                if self.stop_event.is_set():
                    return (False, item['id'], "Stopped during deactivate")

                # STRICT KEY MAPPING
                json_key = KEY_MAP.get(item['ep'])

                if json_key:
                    logging.info(f"  Deactivating: {item['type']} '{item['title']}'")
                    r = self.safe_request(
                        'PUT',
                        base_url,
                        use_session=False,
                        json={json_key: {'active': False}},
                        auth=auth
                    )
                    if r is None or r.status_code not in [200, 201]:
                        return (False, item['id'], "Failed to deactivate")

            # --- DELETE ---
            if self.stop_event.is_set():
                return (False, item['id'], "Stopped before delete")

            r = self.safe_request('DELETE', base_url, use_session=False, auth=auth)

            if r is not None and r.status_code in [204, 200]:
                return (True, item['id'], f"Deleted: {item['title']}")
            else:
                if r is None:
                    return (False, item['id'], f"Failed: {item['title']} (Timeout)")
                try:
                    err = r.json().get('error', 'Unknown')
                    return (False, item['id'], f"Failed: {item['title']} - {err}")
                except Exception:
                    return (False, item['id'], f"Failed: {item['title']} ({r.status_code})")

        except Exception as e:
            return (False, item['id'], f"Error: {item['title']} - {str(e)}")

    def run_delete(self, items, sub, auth):
        breakdown = Counter(i['type'] for i in items)
        bd_str = ", ".join(f"{c}x {t}" for t, c in sorted(breakdown.items()))
        logging.info(f"--- DELETE STARTED: {len(items)} item(s) — {bd_str} ---")
        success = 0
        total = len(items)
        self._update_status(progress=0)

        # Using 5 workers to be safe, but now backed by safe_request rate limiting
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_item = {
                executor.submit(self.process_single_item, item, sub, auth): item
                for item in items
            }

            for i, future in enumerate(as_completed(future_to_item)):
                item = future_to_item[future]

                try:
                    result_ok, item_id, msg = future.result()

                    if result_ok:
                        success += 1
                        logging.info(f"✔ {msg}")
                        with self._map_lock:
                            if item_id in self.items_map:
                                self.items_map[item_id]['deleted'] = True
                    else:
                        if "Stopped" in msg:
                            logging.warning(msg)
                        else:
                            logging.error(f"✘ {msg}")
                except Exception as e:
                    logging.error(f"Thread Error on {item['title']}: {e}")

                # Update UI *after* completion to prevent lag
                self._update_status(
                    status=f"Processed {i + 1}/{total} items...",
                    progress=((i + 1) / total) * 100
                )

        self.root.after(0, lambda: self.finish_delete_ui(success, total))

    def finish_delete_ui(self, success, total):
        keys = [k for k, v in self.items_map.items() if v.get('deleted')]
        for k in keys:
            del self.items_map[k]
        self.selected_count = sum(1 for v in self.items_map.values() if v.get('checked'))

        total_time = time.time() - self.start_time
        logging.info(
            f"--- FINISHED: Deleted {success}/{total} in "
            f"{self.format_time(total_time)} ---"
        )

        self.on_mode_change(None)
        self.set_ui_state(True)
        self.status_var.set("Ready")
        self.time_info_var.set(f"Last Run: {self.format_time(total_time)}")
        self.progress_var.set(0)
        self.is_working = False


if __name__ == "__main__":
    root = tk.Tk()
    app = ZendeskApp(root)
    root.mainloop()