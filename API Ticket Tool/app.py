import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import requests
import json
import logging
import platform
import subprocess
import threading
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor


class APILogHandler(logging.Handler):
    """Handler customizado para enviar logs para a interface gráfica"""

    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        tag = getattr(record, "tag", None) or (
            "debug"   if record.levelno <= logging.DEBUG   else
            "warning" if record.levelno == logging.WARNING else
            "error"   if record.levelno >= logging.ERROR   else
            "info"
        )
        def _append():
            self.text_widget.configure(state="normal")
            self.text_widget.insert(tk.END, msg + "\n", tag)
            self.text_widget.configure(state="disabled")
            self.text_widget.yview(tk.END)
        self.text_widget.after(0, _append)


class ZendeskApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Zendesk - Criar Tickets por API")
        self._maximize_to_usable_area()

        self.session = requests.Session()
        self.base_url = ""
        self.ticket_forms = []
        self.ticket_fields = []
        self.groups = []
        self._co_cache = {}
        self.form_widgets = {}
        self._form_generation = 0
        self._condition_map = {}
        self._conditional_child_ids = set()
        self._current_form_fields = []

        self._setup_ui()
        self._setup_logging()

    def _maximize_to_usable_area(self):
        self.root.update_idletasks()
        if platform.system() == "Darwin":
            try:
                script = "tell application \"Finder\" to get bounds of window of desktop"
                out = subprocess.check_output(["osascript", "-e", script], timeout=2).decode().strip()
                left, top, right, bottom = map(int, out.split(", "))
                w, h = right - left, bottom - top
                self.root.geometry(f"{w}x{h}+{left}+{top}")
                return
            except Exception:
                pass
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        self.root.geometry(f"{sw}x{sh}+0+0")

    def _setup_logging(self):
        self.logger = logging.getLogger("ZendeskAPI")
        self.logger.setLevel(logging.DEBUG)
        handler = APILogHandler(self.log_text)
        formatter = logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.log_text.tag_configure("debug",      foreground="#7a7a7a")
        self.log_text.tag_configure("info",       foreground="#7ec8a0")
        self.log_text.tag_configure("warning",    foreground="#e8a04e")
        self.log_text.tag_configure("error",      foreground="#e06c75")
        self.log_text.tag_configure("conditions", foreground="#61afef")

    def _setup_ui(self):
        # Painel de log fixo na base (empacotado antes do notebook para reservar espaço)
        log_frame = ttk.LabelFrame(self.root, text="Logs da API", padding=5)
        log_frame.pack(side="bottom", fill="x", padx=10, pady=(0, 10))

        log_controls = ttk.Frame(log_frame)
        log_controls.pack(fill="x", pady=(0, 4))
        ttk.Button(log_controls, text="Limpar", command=self._clear_log, width=8).pack(side="left")
        ttk.Button(log_controls, text="+", width=3, command=self._grow_log).pack(side="right", padx=1)
        ttk.Button(log_controls, text="−", width=3, command=self._shrink_log).pack(side="right", padx=1)
        ttk.Label(log_controls, text="Altura:").pack(side="right", padx=(8, 2))

        self.log_text = scrolledtext.ScrolledText(
            log_frame, state="disabled", bg="#1a1a1a", fg="#c8c8c8",
            font=("Consolas", 10), height=8, relief="flat", borderwidth=0,
        )
        self.log_text.pack(fill="both", expand=True)

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(expand=True, fill="both")

        # Abas
        self.tab_config = ttk.Frame(self.notebook)
        self.tab_ticket = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_config, text="1. Configuração")
        self.notebook.add(self.tab_ticket, text="2. Criar Ticket")

        self._build_config_tab()
        self._build_ticket_tab()

    # ==========================================
    # ABA 1: CONFIGURAÇÃO
    # ==========================================
    def _build_config_tab(self):
        frame = ttk.LabelFrame(self.tab_config, text="Credenciais da API Zendesk", padding=20)
        frame.pack(padx=20, pady=20, fill="x")
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Subdomínio (ex: d3v-meu_subdominio):").grid(row=0, column=0, sticky="w", pady=5)
        self.entry_subdomain = ttk.Entry(frame, width=40)
        self.entry_subdomain.grid(row=0, column=1, sticky="ew", pady=5)

        ttk.Label(frame, text="Email do Agente:").grid(row=1, column=0, sticky="w", pady=5)
        self.entry_email = ttk.Entry(frame, width=40)
        self.entry_email.grid(row=1, column=1, sticky="ew", pady=5)

        ttk.Label(frame, text="Token da API:").grid(row=2, column=0, sticky="w", pady=5)
        self.entry_token = ttk.Entry(frame, width=40, show="*")
        self.entry_token.grid(row=2, column=1, sticky="ew", pady=5)

        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=3, column=0, columnspan=2, pady=15)

        ttk.Button(btn_frame, text="Salvar Config", command=self.save_config).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Carregar Config", command=self.load_config).pack(side="left", padx=5)
        self.btn_connect = ttk.Button(btn_frame, text="Conectar e Buscar Formulários", command=self.connect)
        self.btn_connect.pack(side="left", padx=5)

        self.progress_config = ttk.Progressbar(self.tab_config, mode="indeterminate")

    def save_config(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json")],
            initialfile="zendesk_config.json",
            title="Salvar configuração",
        )
        if not path:
            return
        config = {
            "subdomain": self.entry_subdomain.get(),
            "email": self.entry_email.get(),
            "token": self.entry_token.get(),
        }
        with open(path, "w") as f:
            json.dump(config, f)

    def load_config(self):
        path = filedialog.askopenfilename(
            filetypes=[("JSON", "*.json")],
            title="Carregar configuração",
        )
        if not path:
            return
        try:
            with open(path, "r") as f:
                config = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            messagebox.showerror("Erro", f"Erro ao ler configuração: {e}")
            return
        self.entry_subdomain.delete(0, tk.END)
        self.entry_subdomain.insert(0, config.get("subdomain", ""))
        self.entry_email.delete(0, tk.END)
        self.entry_email.insert(0, config.get("email", ""))
        self.entry_token.delete(0, tk.END)
        self.entry_token.insert(0, config.get("token", ""))

    def connect(self):
        subdomain = self.entry_subdomain.get()
        email = self.entry_email.get()
        token = self.entry_token.get()

        if not all([subdomain, email, token]):
            messagebox.showerror("Erro", "Preencha todos os campos.")
            return

        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.session.auth = HTTPBasicAuth(f"{email}/token", token)
        self._co_cache.clear()
        if hasattr(self, "_agents_cache"):
            del self._agents_cache

        self.btn_connect.configure(state="disabled")
        self.notebook.tab(self.tab_ticket, state="disabled")
        self.progress_config.pack(fill="x", padx=20, pady=(0, 10))
        self.progress_config.start()
        threading.Thread(target=self._connect_worker, daemon=True).start()

    def _connect_worker(self):
        self.logger.info("Tentando conectar à API do Zendesk...")
        try:
            resp = self.session.get(f"{self.base_url}/users/me.json")
            self.logger.info(f"Conexão: [{resp.status_code}] {resp.text[:100]}")
            resp.raise_for_status()

            with ThreadPoolExecutor(max_workers=3) as ex:
                f_forms  = ex.submit(self._fetch_paginated, "ticket_forms", "ticket_forms")
                f_fields = ex.submit(self._fetch_paginated, "ticket_fields", "ticket_fields")
                f_groups = ex.submit(self._fetch_paginated, "groups", "groups")
            self.ticket_forms  = f_forms.result()
            self.ticket_fields = f_fields.result()
            self.groups = [g for g in f_groups.result() if not g.get("deleted")]

            self.root.after(0, self._on_connect_success)
        except Exception as e:
            err_msg = str(e)
            self.logger.error(f"Falha na conexão: {err_msg}")
            self.root.after(0, lambda: self._hide_progress(self.progress_config))
            self.root.after(0, lambda: self.btn_connect.configure(state="normal"))
            self.root.after(0, lambda: self.notebook.tab(self.tab_ticket, state="normal"))
            self.root.after(0, lambda: messagebox.showerror("Erro de Conexão", f"Falha ao conectar: {err_msg}"))

    def _on_connect_success(self):
        self._hide_progress(self.progress_config)
        self.btn_connect.configure(state="normal")
        self.notebook.tab(self.tab_ticket, state="normal")
        form_names = [f["name"] for f in self.ticket_forms if f["active"]]
        self.combo_forms["values"] = form_names
        if form_names:
            self.combo_forms.current(0)
            self.on_form_select(None)
        self.notebook.select(self.tab_ticket)

    # ==========================================
    # ABA 2: CRIAÇÃO DE TICKET
    # ==========================================
    def _build_ticket_tab(self):
        top_frame = ttk.Frame(self.tab_ticket, padding=10)
        top_frame.pack(fill="x")

        ttk.Label(top_frame, text="Selecione o Formulário:").pack(side="left", padx=5)
        self.combo_forms = ttk.Combobox(top_frame, state="disabled", width=50)
        self.combo_forms.pack(side="left", padx=5)
        self.combo_forms.bind("<<ComboboxSelected>>", self.on_form_select)

        self.var_conditions = tk.BooleanVar(value=True)
        self.chk_conditions = ttk.Checkbutton(
            top_frame, text="Aplicar Condições",
            variable=self.var_conditions,
            command=self._on_conditions_toggle,
            state="disabled",
        )
        self.chk_conditions.pack(side="left", padx=(15, 5))

        self.btn_submit = ttk.Button(top_frame, text="Criar Ticket", command=self.submit_ticket, state="disabled")
        self.btn_submit.pack(side="right", padx=5)
        self._status_options = {
            "Novo": "new",
            "Aberto": "open",
            "Pendente": "pending",
            "Em espera": "hold",
            "Resolvido": "solved",
            "Encerrado": "closed",
        }
        self.combo_status = ttk.Combobox(
            top_frame, values=list(self._status_options.keys()),
            state="disabled", width=15,
        )
        self.combo_status.set("Novo")
        self.combo_status.pack(side="right", padx=5)
        self.combo_status.bind("<<ComboboxSelected>>", self._on_status_select)
        ttk.Label(top_frame, text="Status:").pack(side="right", padx=(20, 0))
        self.btn_clear = ttk.Button(top_frame, text="Limpar", command=self._clear_form, state="disabled")
        self.btn_clear.pack(side="right", padx=5)

        fixed_frame = ttk.Frame(self.tab_ticket, padding=(10, 5, 10, 5))
        fixed_frame.pack(fill="x")
        fixed_frame.columnconfigure(1, weight=1)

        lbl_subj = ttk.Frame(fixed_frame)
        lbl_subj.grid(row=0, column=0, sticky="w", padx=(0, 10), pady=4)
        ttk.Label(lbl_subj, text="Assunto:").pack(side="left")
        tk.Label(lbl_subj, text=" *", fg="red", font=("TkDefaultFont", 12, "bold")).pack(side="left")
        self.entry_subject = ttk.Entry(fixed_frame)
        self.entry_subject.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(fixed_frame, text="Descrição:").grid(row=1, column=0, sticky="nw", padx=(0, 10), pady=4)
        self.text_description = tk.Text(fixed_frame, height=4, width=1)
        self.text_description.grid(row=1, column=1, sticky="ew", pady=4)

        tags_inner = ttk.Frame(fixed_frame)
        tags_inner.grid(row=2, column=1, sticky="ew", pady=4)
        tags_inner.columnconfigure(0, weight=1)
        ttk.Label(fixed_frame, text="Tags:").grid(row=2, column=0, sticky="w", padx=(0, 10), pady=4)
        self.entry_tags = ttk.Entry(tags_inner)
        self.entry_tags.grid(row=0, column=0, sticky="ew")
        ttk.Label(tags_inner, text="(separadas por espaço ou vírgula)", foreground="gray").grid(row=0, column=1, padx=(8, 0))

        self.progress_ticket = ttk.Progressbar(self.tab_ticket, mode="indeterminate")

        # Canvas para scroll dos campos dinâmicos
        self.canvas = tk.Canvas(self.tab_ticket, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.tab_ticket, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))

        self._canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.bind("<Configure>", lambda e: self.canvas.itemconfig(self._canvas_window, width=e.width))

        self.canvas.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        self.scrollbar.pack(side="right", fill="y")
        self.canvas.bind("<MouseWheel>", lambda e: self.canvas.yview_scroll(-1 * (e.delta // 120 or e.delta), "units"))

        self.scrollable_frame.columnconfigure(0, weight=1)
        self.scrollable_frame.columnconfigure(1, weight=1)

        # Layout dividido: Visíveis vs Outros
        self.frame_visible = ttk.LabelFrame(self.scrollable_frame, text="Campos Visíveis ao Usuário", padding=10)
        self.frame_visible.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        self.frame_others = ttk.LabelFrame(self.scrollable_frame, text="Outros Campos (Uso Interno/Agente)", padding=10)
        self.frame_others.grid(row=0, column=1, sticky="nsew", padx=10, pady=10)

    def on_form_select(self, event):
        self.combo_forms.configure(state="disabled")
        self.btn_submit.configure(state="disabled")
        self.combo_status.configure(state="disabled")
        self.btn_clear.configure(state="disabled")
        self.chk_conditions.configure(state="disabled")
        self.notebook.tab(self.tab_config, state="disabled")
        self.progress_ticket.pack(fill="x", padx=10, before=self.canvas)
        self.progress_ticket.start()

        for widget in self.frame_visible.winfo_children():
            widget.destroy()
        for widget in self.frame_others.winfo_children():
            widget.destroy()
        self.form_widgets = {}

        selected_name = self.combo_forms.get()
        selected_form = next((f for f in self.ticket_forms if f["name"] == selected_name), None)

        if not selected_form:
            self.combo_forms.configure(state="readonly")
            self.notebook.tab(self.tab_config, state="normal")
            self._hide_progress(self.progress_ticket)
            return

        self._condition_map = {}
        self._conditional_child_ids = set()
        self._current_form_fields = []
        for cond in selected_form.get("agent_conditions", []):
            pid = cond["parent_field_id"]
            self._condition_map.setdefault(pid, []).append({
                "value": str(cond.get("value", "")),
                "child_fields": cond.get("child_fields", []),
            })
        for cond_list in self._condition_map.values():
            for c in cond_list:
                for cf in c["child_fields"]:
                    self._conditional_child_ids.add(cf["id"])
        self._form_generation += 1
        gen = self._form_generation

        field_ids = selected_form.get("ticket_field_ids", [])
        field_map = {f["id"]: f for f in self.ticket_fields if f["active"]}
        form_fields = [field_map[fid] for fid in field_ids if fid in field_map and field_map[fid]["type"] != "status"]

        if self._condition_map:
            def _field_title(fid):
                return field_map[fid]["title"] if fid in field_map else f"ID:{fid}"
            lines = [f"Form Conditions ({len(self._condition_map)} pai / {len(self._conditional_child_ids)} filho):"]
            for parent_id, cond_list in self._condition_map.items():
                lines.append(f"  [{_field_title(parent_id)}]")
                for cond in cond_list:
                    children = ", ".join(
                        f"{_field_title(cf['id'])}{'*' if cf.get('is_required') else ''}"
                        for cf in cond["child_fields"]
                    )
                    lines.append(f"    valor={cond['value']!r:20s} → {children}")
            self.logger.info("\n".join(lines), extra={"tag": "conditions"})


        def _prefetch_and_build():
            try:
                tasks = {}
                for field in form_fields:
                    if field["type"] == "lookup":
                        target = field.get("relationship_target_type", "")
                        if target.startswith("zen:custom_object:"):
                            co_key = target.split(":")[-1]
                            filters = self._build_co_filters(field)
                            cache_key = self._co_cache_key(co_key, filters)
                            if cache_key not in self._co_cache:
                                tasks[cache_key] = lambda k=co_key, f=filters, ck=cache_key: self._fetch_co_records(k, f, ck)
                    elif field["type"] == "assignee" and not hasattr(self, "_agents_cache"):
                        tasks["__agents__"] = self._fetch_agents
                if tasks:
                    with ThreadPoolExecutor(max_workers=min(len(tasks), 5)) as ex:
                        futures = [ex.submit(fn) for fn in tasks.values()]
                    for f in futures:
                        f.result()
                if gen != self._form_generation:
                    self.logger.info("Form switch detectado, descartando build anterior")
                    return
                self.root.after(0, lambda: self._build_form_widgets(form_fields, gen))
            except Exception as e:
                err_msg = str(e)
                self.logger.error(f"Erro ao carregar dados do formulário: {err_msg}")
                self.root.after(0, lambda: self._hide_progress(self.progress_ticket))
                self.root.after(0, lambda: self.combo_forms.configure(state="readonly"))
                self.root.after(0, lambda: self.notebook.tab(self.tab_config, state="normal"))

        threading.Thread(target=_prefetch_and_build, daemon=True).start()

    def _build_form_widgets(self, form_fields, gen=None):
        if gen is not None and gen != self._form_generation:
            return
        self._current_form_fields = form_fields
        row_vis = 0
        row_oth = 0
        for field in form_fields:
            if field["type"] in ["subject", "description"]:
                continue
            if field.get("visible_in_portal"):
                parent = self.frame_visible
                row = row_vis
                row_vis += 1
            else:
                parent = self.frame_others
                row = row_oth
                row_oth += 1
            self._create_field_widget(parent, field, row)
        self.combo_forms.configure(state="readonly")
        self.btn_submit.configure(state="normal")
        self.combo_status.configure(state="readonly")
        self.btn_clear.configure(state="normal")
        self.notebook.tab(self.tab_config, state="normal")
        self._hide_progress(self.progress_ticket)
        self._bind_mousewheel(self.scrollable_frame)
        self._apply_conditions()
        self.chk_conditions.configure(state="normal" if self._condition_map else "disabled")
        self.logger.info("Dados carregados com sucesso!")

    def _create_field_widget(self, parent, field, row):
        parent.columnconfigure(1, weight=1)
        label_frame = ttk.Frame(parent)
        label_frame.grid(row=row, column=0, sticky="nw", pady=5)
        ttk.Label(label_frame, text=field["title"], wraplength=350).pack(side="left")
        if field.get("required"):
            tk.Label(label_frame, text=" *", fg="red", font=("TkDefaultFont", 12, "bold")).pack(side="left")

        widget = None
        # Analisando o tipo do campo
        if field["type"] in ["text", "subject", "integer", "decimal"]:
            widget = ttk.Entry(parent, width=40)
            widget.grid(row=row, column=1, sticky="ew", pady=5)

        elif field["type"] in ["description", "textarea"]:
            widget = tk.Text(parent, width=40, height=4)
            widget.grid(row=row, column=1, sticky="ew", pady=5)

        elif field["type"] in ["tagger", "dropdown"]:
            options_map = {opt["name"]: opt["value"] for opt in field.get("custom_field_options", [])}
            widget = self._make_combo(parent, options_map, row)

        elif field["type"] == "group":
            options_map = {g["name"]: g["id"] for g in self.groups}
            widget = self._make_combo(parent, options_map, row)

        elif field["type"] == "assignee":
            agents = self._fetch_agents()
            options_map = {f"{a['name']} ({self._display_email(a['email'])})": a["id"] for a in agents}
            widget = self._make_combo(parent, options_map, row)

        elif field["type"] in ["priority", "status", "tickettype"]:
            options_map = {opt["name"]: opt["value"] for opt in field.get("system_field_options", [])}
            widget = self._make_combo(parent, options_map, row)

        elif field["type"] == "lookup":
            target = field.get("relationship_target_type", "")
            if target.startswith("zen:custom_object:"):
                co_key = target.split(":")[-1]
                filters = self._build_co_filters(field)
                cache_key = self._co_cache_key(co_key, filters)
                records = self._co_cache.get(cache_key, [])
                options_map = {f"{r['name']} (ID: {r['id']})": r["id"] for r in records}
                widget = self._make_combo(parent, options_map, row)
            else:
                widget = ttk.Entry(parent, width=40)
                widget.grid(row=row, column=1, sticky="ew", pady=5)

        elif field["type"] == "date":
            date_frame = ttk.Frame(parent)
            date_frame.grid(row=row, column=1, sticky="ew", pady=5)
            date_frame.columnconfigure(0, weight=1)
            widget = ttk.Entry(date_frame, width=37)
            widget.grid(row=0, column=0, sticky="ew")
            ttk.Button(date_frame, text="...", width=3,
                       command=lambda w=widget: self._open_calendar(w)).grid(row=0, column=1, padx=(2, 0))
            setattr(widget, "_grid_widget", date_frame)

        else:
            # Fallback para checkbox e outros
            widget = ttk.Entry(parent, width=40)
            widget.grid(row=row, column=1, sticky="ew", pady=5)

        if widget:
            self.form_widgets[field["id"]] = {
                "widget": widget,
                "grid_widget": getattr(widget, "_grid_widget", widget),
                "label_frame": label_frame,
                "type": field["type"],
                "required": field.get("required", False),
                "title": field["title"],
            }

    _SEARCH_THRESHOLD = 10

    def _make_combo(self, parent, options_map, row):
        """Usa busca se houver mais de _SEARCH_THRESHOLD opções, readonly caso contrário."""
        if len(options_map) > self._SEARCH_THRESHOLD:
            return self._make_searchable_list(parent, options_map, row)
        widget = ttk.Combobox(parent, values=list(options_map.keys()), state="readonly", width=37)
        widget.grid(row=row, column=1, sticky="ew", pady=5)
        setattr(widget, "options_map", options_map)
        return widget

    def _make_searchable_list(self, parent, options_map, row):
        """Entry + Listbox com filtro por digitação para listas longas."""
        all_options = list(options_map.keys())

        frame = ttk.Frame(parent)
        frame.grid(row=row, column=1, sticky="ew", pady=5)
        frame.columnconfigure(0, weight=1)

        search_entry = ttk.Entry(frame)
        search_entry.grid(row=0, column=0, sticky="ew")

        list_frame = ttk.Frame(frame)
        list_frame.grid(row=1, column=0, sticky="ew")
        list_frame.columnconfigure(0, weight=1)

        listbox = tk.Listbox(list_frame, height=8, exportselection=False)
        listbox.grid(row=0, column=0, sticky="ew")
        sb = ttk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        listbox.configure(yscrollcommand=sb.set)

        for opt in all_options:
            listbox.insert(tk.END, opt)

        count_var = tk.StringVar(value=f"{len(all_options)} itens")
        ttk.Label(frame, textvariable=count_var, foreground="gray").grid(row=2, column=0, sticky="w")

        selected_value = tk.StringVar()
        _change_callbacks = []

        def _filter(*_args):
            typed = search_entry.get().strip().lower()
            filtered = [o for o in all_options if typed in o.lower()] if typed else all_options
            listbox.delete(0, tk.END)
            for opt in filtered:
                listbox.insert(tk.END, opt)
            count_var.set(f"{len(filtered)} de {len(all_options)} itens")
            if selected_value.get() and selected_value.get() not in filtered:
                selected_value.set("")

        def _on_select(*_args):
            sel = listbox.curselection()
            if sel:
                selected_value.set(listbox.get(sel[0]))
                for cb in _change_callbacks:
                    cb()

        search_entry.bind("<KeyRelease>", _filter)
        listbox.bind("<<ListboxSelect>>", _on_select)

        # Widget proxy que expõe get/set compatíveis com o resto do app
        proxy = ttk.Frame(frame)
        setattr(proxy, "options_map", options_map)
        setattr(proxy, "get", lambda: selected_value.get())
        setattr(proxy, "set", lambda v: selected_value.set(v))
        setattr(proxy, "_grid_widget", frame)
        setattr(proxy, "_on_change_callbacks", _change_callbacks)
        # Para _clear_form: isinstance check de ttk.Entry/Combobox não vai bater,
        # então adicionamos delete que limpa tudo
        def _clear(start=None, end=None):
            selected_value.set("")
            search_entry.delete(0, tk.END)
            _filter()
            listbox.selection_clear(0, tk.END)
        setattr(proxy, "delete", _clear)

        return proxy

    def _display_email(self, email):
        if not email:
            return ""
        if email.endswith("@example.com"):
            local = email[:-len("@example.com")]
            if "+" in local:
                user, domain = local.split("+", 1)
                return f"{user}@{domain}"
        return email

    def _open_calendar(self, entry_widget):
        from tkcalendar import Calendar
        top = tk.Toplevel(self.root)
        top.title("Selecionar Data")
        top.grab_set()
        cal = Calendar(top, date_pattern="yyyy-mm-dd",
                       foreground="black", background="white",
                       headersforeground="black", headersbackground="#d9d9d9")
        cal.pack(padx=10, pady=10)

        def _select():
            entry_widget.delete(0, tk.END)
            entry_widget.insert(0, cal.get_date())
            top.destroy()

        btn_frame = ttk.Frame(top)
        btn_frame.pack(pady=(0, 10))
        ttk.Button(btn_frame, text="Selecionar", command=_select).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Limpar", command=lambda: [entry_widget.delete(0, tk.END), top.destroy()]).pack(side="left", padx=5)

    def _hide_progress(self, bar):
        bar.stop()
        bar.pack_forget()

    def _bind_mousewheel(self, widget):
        if isinstance(widget, (tk.Listbox, tk.Text)):
            return
        scroll = lambda e: self.canvas.yview_scroll(-1 * (e.delta // 120 or e.delta), "units")
        widget.bind("<MouseWheel>", scroll)
        for child in widget.winfo_children():
            self._bind_mousewheel(child)

    def _on_status_select(self, _=None):
        if self.combo_status.get() == "Encerrado":
            self.combo_status.configure(foreground="red")
        else:
            self.combo_status.configure(foreground="")

    def _clear_form(self):
        self.combo_status.set("Novo")
        self.combo_status.configure(foreground="")
        self.entry_subject.delete(0, tk.END)
        self.text_description.delete("1.0", tk.END)
        self.entry_tags.delete(0, tk.END)
        for info in self.form_widgets.values():
            widget = info["widget"]
            if isinstance(widget, tk.Text):
                widget.delete("1.0", tk.END)
            elif isinstance(widget, ttk.Combobox):
                widget.set("")
            elif isinstance(widget, ttk.Entry):
                widget.delete(0, tk.END)
            elif hasattr(widget, "delete"):
                widget.delete()

    def _fetch_paginated(self, endpoint, key):
        results = []
        url = f"{self.base_url}/{endpoint}.json"
        while url:
            resp = self.session.get(url)
            self.logger.info(f"Buscando {endpoint}: [{resp.status_code}]")
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get(key, []))
            url = data.get("next_page")
        return results

    def _build_co_filters(self, field):
        params = {}
        rf = field.get("relationship_filter", {})
        if rf.get("any"):
            self.logger.warning(
                f"Campo '{field.get('title', '')}' possui condições 'any' (OR) "
                "que não são totalmente suportáveis via API de filtros. "
                "Serão aplicadas como AND.")
        for group in ("all", "any"):
            for condition in rf.get(group, []):
                fld = condition.get("field", "")
                op = condition.get("operator", "")
                val = condition.get("value", "")
                if not fld or val == "":
                    continue
                if op and op != "is":
                    params[f"filter[{fld}][{op}]"] = val
                else:
                    params[f"filter[{fld}]"] = val
        self.logger.debug(f"Filtros do campo '{field.get('title', '')}': {rf} -> {params}")
        return params

    def _co_cache_key(self, co_key, filters):
        if not filters:
            return co_key
        suffix = "&".join(f"{k}={v}" for k, v in sorted(filters.items()))
        return f"{co_key}?{suffix}"

    def _fetch_co_records(self, co_key, filters=None, cache_key=None):
        ck = cache_key or co_key
        if ck in self._co_cache:
            return self._co_cache[ck]
        try:
            records = []
            params = {"sort": "name"}
            if filters:
                params.update(filters)
                self.logger.info(f"Custom Object '{co_key}' com filtros: {filters}")
            url = f"{self.base_url}/custom_objects/{co_key}/records.json"
            first_request = True
            while url:
                if first_request:
                    resp = self.session.get(url, params=params)
                    first_request = False
                else:
                    resp = self.session.get(url)
                self.logger.info(f"Buscando Custom Object '{co_key}': [{resp.status_code}]")
                resp.raise_for_status()
                data = resp.json()
                records.extend(data.get("custom_object_records", []))
                url = data.get("links", {}).get("next")
            total = len(records)
            records = [r for r in records if self._is_co_record_active(r)]
            filtered_out = total - len(records)
            if filtered_out:
                self.logger.info(f"Custom Object '{co_key}': {filtered_out} registros inativos ocultados de {total}")
            self._co_cache[ck] = records
            return records
        except Exception as e:
            self.logger.error(f"Erro ao buscar objeto personalizado {co_key}: {str(e)}")
            return []

    _INACTIVE_FIELD_KEYS = {"status", "state", "estado",
                            "active", "ativo", "activo",
                            "inactive", "inativo", "inactivo",
                            "enabled", "habilitado",
                            "is_active", "es_activo"}

    _INACTIVE_VALUES = {"inactive", "inativo", "inactivo",
                        "disabled", "desabilitado", "deshabilitado",
                        "false", "0", "no", "não", "nao",
                        "closed", "fechado", "cerrado",
                        "archived", "arquivado", "archivado",
                        "deleted", "excluido", "excluído", "eliminado",
                        "cancelled", "cancelado", "canceled",
                        "suspended", "suspenso", "suspendido",
                        "blocked", "bloqueado", "bloqueado",
                        "expired", "expirado", "vencido",
                        "deprecated", "descontinuado", "obsoleto"}

    def _is_co_record_active(self, record):
        fields = record.get("custom_object_fields", {})
        if not fields:
            return True
        for key, val in fields.items():
            key_lower = key.lower()
            matched_key = False
            for ik in self._INACTIVE_FIELD_KEYS:
                if ik in key_lower:
                    matched_key = True
                    break
            if not matched_key:
                continue
            val_lower = str(val).strip().lower()
            if val_lower in self._INACTIVE_VALUES:
                return False
        return True

    def _fetch_agents(self):
        if hasattr(self, "_agents_cache"):
            return self._agents_cache
        try:
            agents = []
            url = f"{self.base_url}/users.json?role=agent"
            while url:
                resp = self.session.get(url)
                self.logger.info(f"Buscando Agentes: [{resp.status_code}]")
                resp.raise_for_status()
                data = resp.json()
                agents.extend(data.get("users", []))
                url = data.get("next_page")
            self._agents_cache = agents
            return agents
        except Exception as e:
            self.logger.error(f"Erro ao buscar agentes: {str(e)}")
            return []

    def submit_ticket(self):
        selected_name = self.combo_forms.get()
        selected_form = next((f for f in self.ticket_forms if f["name"] == selected_name), None)

        if not selected_form:
            messagebox.showerror("Erro", "Nenhum formulário selecionado.")
            return

        payload = {"ticket": {"ticket_form_id": selected_form["id"], "custom_fields": []}}

        status_val = self._status_options.get(self.combo_status.get())
        if status_val:
            payload["ticket"]["status"] = status_val

        subject_val = self.entry_subject.get().strip()
        if subject_val:
            payload["ticket"]["subject"] = subject_val

        desc_val = self.text_description.get("1.0", tk.END).strip()
        if desc_val:
            payload["ticket"]["comment"] = {"body": desc_val}

        # Coleta os valores do formulário
        for field_id, info in self.form_widgets.items():
            widget = info["widget"]
            field_type = info["type"]

            if isinstance(widget, tk.Text):
                value = widget.get("1.0", tk.END).strip()
            else:
                value = widget.get().strip()

            if not value:
                continue

            # Lida com mapeamento de combobox (Tag values e Lookup IDs)
            options_map = getattr(widget, "options_map", None)
            if options_map is not None and value in options_map:
                value = options_map[value]

            # Campos padrões vs Custom Fields
            if field_type == "subject":
                payload["ticket"]["subject"] = value
            elif field_type == "description":
                payload["ticket"]["comment"] = {"body": value}
            elif field_type == "group":
                payload["ticket"]["group_id"] = value
            elif field_type == "assignee":
                payload["ticket"]["assignee_id"] = value
            elif field_type in ["priority", "status"]:
                payload["ticket"][field_type] = value
            elif field_type == "tickettype":
                payload["ticket"]["type"] = value
            else:
                payload["ticket"]["custom_fields"].append({"id": field_id, "value": value})

        raw_tags = self.entry_tags.get().strip()
        if raw_tags:
            tags = [t for t in raw_tags.replace(",", " ").split() if t]
            if tags:
                payload["ticket"]["tags"] = tags

        if "comment" not in payload["ticket"]:
            payload["ticket"]["comment"] = {"body": ""}

        # Validação mínima: subject é sempre obrigatório pela API
        if "subject" not in payload["ticket"]:
            messagebox.showerror("Campo obrigatório", "O campo 'Assunto' é obrigatório.")
            return

        use_import = payload["ticket"].get("status") == "closed"
        endpoint = "imports/tickets.json" if use_import else "tickets.json"
        status_label = self.combo_status.get()
        form_name = selected_form["name"]

        self.btn_submit.configure(state="disabled")
        self.combo_status.configure(state="disabled")
        self.logger.info(f"Formulário: '{form_name}' | Status: {status_label} | Endpoint: {endpoint}")
        self.logger.debug(f"Payload: {json.dumps(payload, ensure_ascii=False)}")
        threading.Thread(target=self._submit_worker, args=(payload, use_import), daemon=True).start()

    def _submit_worker(self, payload, use_import):
        try:
            url = f"{self.base_url}/{'imports/tickets.json' if use_import else 'tickets.json'}"
            resp = self.session.post(url, json=payload)

            try:
                resp_data = resp.json()
            except Exception:
                resp_data = {}

            if not resp.ok:
                api_error = resp_data.get("error", "")
                api_description = resp_data.get("description", "")
                details = resp_data.get("details", {})
                detail_lines = []
                for field, errs in details.items():
                    for e in errs:
                        detail_lines.append(f"  • {field}: {e.get('description', e)}")
                parts = []
                if api_error:
                    parts.append(f"Erro: {api_error}")
                if api_description:
                    parts.append(f"Descrição: {api_description}")
                if detail_lines:
                    parts.append("Detalhes:\n" + "\n".join(detail_lines))
                msg = "\n".join(parts) if parts else f"HTTP {resp.status_code}"
                self.logger.error(f"Falha ao criar ticket [{resp.status_code}]: {msg.replace(chr(10), ' | ')}")
                self.root.after(0, lambda m=msg: self._show_after_log(
                    lambda: messagebox.showerror("Erro ao criar ticket", m)))
                return

            ticket_id = resp_data["ticket"]["id"]
            ticket_url = f"https://{self.base_url.split('/')[2]}/agent/tickets/{ticket_id}"
            self.logger.info(f"Ticket #{ticket_id} criado com sucesso! URL: {ticket_url}")
            self.root.after(0, lambda: self._show_after_log(
                lambda: messagebox.showinfo("Ticket criado", f"Ticket #{ticket_id} criado com sucesso!\n\n{ticket_url}")))
        except Exception as e:
            msg = f"Erro de conexão: {e}"
            self.logger.error(msg)
            self.root.after(0, lambda m=msg: self._show_after_log(
                lambda: messagebox.showerror("Erro de conexão", m)))
        finally:
            self.root.after(0, lambda: self.btn_submit.configure(state="normal"))
            self.root.after(0, lambda: self.combo_status.configure(state="readonly"))

    def _apply_conditions(self):
        if self.var_conditions.get() and self._condition_map:
            for fid in self._conditional_child_ids:
                self._set_field_visible(fid, False)
            for parent_id in self._condition_map:
                if parent_id in self.form_widgets:
                    self._bind_condition_trigger(parent_id)
        else:
            for fid in self._conditional_child_ids:
                self._set_field_visible(fid, True)

    def _bind_condition_trigger(self, parent_id):
        widget = self.form_widgets[parent_id]["widget"]
        cb = lambda *_: self._evaluate_all_conditions()
        if isinstance(widget, ttk.Combobox):
            widget.bind("<<ComboboxSelected>>", cb)
        elif hasattr(widget, "_on_change_callbacks"):
            widget._on_change_callbacks.append(lambda: self._evaluate_all_conditions())
        else:
            widget.bind("<KeyRelease>", cb)

    def _evaluate_all_conditions(self):
        for child_id in self._conditional_child_ids:
            self._set_field_visible(child_id, self._is_child_condition_met(child_id))

    def _is_child_condition_met(self, child_id):
        for parent_id, cond_list in self._condition_map.items():
            parent_info = self.form_widgets.get(parent_id)
            if not parent_info:
                continue
            current_val = parent_info["widget"].get().strip()
            options_map = getattr(parent_info["widget"], "options_map", None)
            if options_map and current_val in options_map:
                current_val = str(options_map[current_val])
            for cond in cond_list:
                child_ids = [cf["id"] for cf in cond.get("child_fields", [])]
                if child_id in child_ids and str(cond["value"]) == current_val:
                    return True
        return False

    def _set_field_visible(self, fid, visible):
        info = self.form_widgets.get(fid)
        if not info:
            return
        gw = info["grid_widget"]
        lf = info["label_frame"]
        if visible:
            lf.grid()
            gw.grid()
        else:
            lf.grid_remove()
            gw.grid_remove()

    def _on_conditions_toggle(self):
        if not self._current_form_fields:
            return
        for w in self.frame_visible.winfo_children():
            w.destroy()
        for w in self.frame_others.winfo_children():
            w.destroy()
        self.form_widgets = {}
        self._build_form_widgets(self._current_form_fields)

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")

    def _grow_log(self):
        self.log_text.configure(height=self.log_text.cget("height") + 2)

    def _shrink_log(self):
        h = self.log_text.cget("height")
        if h > 3:
            self.log_text.configure(height=h - 2)

    def _show_after_log(self, dialog_fn):
        self.root.update_idletasks()
        dialog_fn()



if __name__ == "__main__":
    root = tk.Tk()
    app = ZendeskApp(root)
    root.mainloop()
