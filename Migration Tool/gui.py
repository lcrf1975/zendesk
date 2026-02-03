import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
import requests
import json
import csv
import time
import os
from datetime import datetime
import queue

# Fix for blurry text on Mac Retina displays
try:
    from ctypes import cdll
    cdll.LoadLibrary("libtk8.6.dylib")
except:
    pass

# ==========================================
# 1. API CLIENT
# ==========================================

class ZendeskClient:
    def __init__(self, subdomain, email, token, logger_func, verbose=False):
        self.subdomain = subdomain
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.auth = (f"{email}/token", token)
        self.headers = {'Content-Type': 'application/json'}
        self.logger = logger_func
        self.verbose = verbose

    def _request(self, method, endpoint, payload=None):
        url = f"{self.base_url}/{endpoint}"
        max_retries = 10
        attempt = 0
        
        while attempt < max_retries:
            try:
                if self.verbose:
                    self.logger(f"[Debug] {method} {url}")
                    if payload:
                        self.logger(f"[Debug] Payload: {json.dumps(payload)}")

                if method == 'GET':
                    response = requests.get(url, auth=self.auth, timeout=30)
                elif method == 'POST':
                    response = requests.post(url, auth=self.auth, headers=self.headers, json=payload, timeout=30)
                elif method == 'PUT':
                    response = requests.put(url, auth=self.auth, headers=self.headers, json=payload, timeout=30)
                elif method == 'DELETE':
                    response = requests.delete(url, auth=self.auth, timeout=30)
                
                if self.verbose:
                    self.logger(f"[Debug] Status: {response.status_code}")

                # Handle Rate Limiting
                if response.status_code == 429:
                    wait = int(response.headers.get('Retry-After', 60))
                    self.logger(f"[!] Rate Limit on {self.subdomain}. Sleeping {wait}s...")
                    time.sleep(wait + 1)
                    attempt += 1
                    continue
                
                # Server Errors (5xx) - Retry
                if 500 <= response.status_code < 600:
                    self.logger(f"[!] Server Error {response.status_code}. Retrying...")
                    time.sleep(2)
                    attempt += 1
                    continue
                
                return response

            except requests.exceptions.Timeout:
                self.logger(f"[Timeout] Server {self.subdomain} took too long to respond.")
                attempt += 1
            except requests.exceptions.RequestException as e:
                self.logger(f"[Connection Error] {e}")
                attempt += 1
            
            time.sleep(1)

        self.logger(f"[Error] Max retries reached for {endpoint}")
        return None

    def get_all(self, endpoint, key):
        items = []
        url = endpoint
        self.logger(f"Fetching {key} from {self.subdomain}...")
        
        while url:
            response = self._request('GET', url)
            if not response or response.status_code != 200:
                self.logger(f"[Error] Failed to fetch {url}. Status: {response.status_code if response else 'None'}")
                if self.verbose and response:
                    self.logger(f"[Debug] Raw Response: {response.text[:200]}...")
                break
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger(f"[Error] Invalid JSON received from {url}")
                if self.verbose:
                    self.logger(f"[Debug] Raw Text: {response.text}")
                break
                
            items.extend(data.get(key, []))
            
            next_url = data.get('next_page')
            if next_url:
                url = next_url.replace(self.base_url + "/", "")
            else:
                url = None
        return items

    def create_field_safe(self, endpoint, payload, object_type_key):
        response = self._request('POST', endpoint, payload=payload)
        if not response: return None
        
        try:
            data = response.json()
        except:
            data = {}

        if response.status_code == 201: 
            return data

        if response.status_code == 422:
            error_text = response.text.lower()
            if ("tag" in error_text or "key" in error_text) and ("taken" in error_text or "used" in error_text):
                field_data = payload.get(object_type_key)
                if field_data:
                    old_val = field_data.get('key') or field_data.get('tag', 'unknown')
                    new_val = f"{old_val}_migrated"
                    
                    if field_data.get('key'): field_data['key'] = new_val
                    if field_data.get('tag'): field_data['tag'] = new_val
                    
                    self.logger(f"[Info] Collision detected ({old_val}). Renaming to {new_val} and retrying...")
                    return self.create_field_safe(endpoint, payload, object_type_key)
            
            self.logger(f"[Error 422] Failed to create {object_type_key}. API Message: {response.text}")
            return None
        
        self.logger(f"[Error {response.status_code}] {response.text}")
        return None

    def update_object(self, endpoint, obj_id, payload):
        url = f"{endpoint}/{obj_id}.json"
        return self._request('PUT', url, payload=payload)

    def delete_item(self, endpoint, item_id):
        url = f"{endpoint}/{item_id}.json"
        response = self._request('DELETE', url)
        if response and response.status_code in [204, 404]:
            return True
        self.logger(f"[Error] Delete failed for {item_id}: {response.text if response else 'No Response'}")
        return False

# ==========================================
# 2. LOGIC CONTROLLER
# ==========================================

class MigrationLogic:
    SYSTEM_FIELDS = {
        'subject', 'description', 'status', 'tickettype', 'priority', 'group', 'assignee',
        'brand', 'satisfaction_rating', 'custom_status', 'email', 'name', 'time_zone', 
        'locale_id', 'organization_id', 'role', 'phone', 'mobile', 'whatsapp', 'facebook', 
        'twitter', 'google', 'photo', 'authenticity_token', 'active', 'alias', 'signature',
        'shared_phone_number', 'domain_names', 'tags', 'shared_tickets', 'shared_comments'
    }

    ALLOWED_TYPES = {
        'text', 'textarea', 'checkbox', 'date', 'integer', 'decimal', 
        'regexp', 'tagger', 'multiselect', 'lookup' 
    }

    def __init__(self, logger_func, update_progress_func):
        self.log = logger_func
        self.progress = update_progress_func
        # Initial default, will be overridden by UI/Config
        self.rollback_file = os.path.join(os.path.expanduser("~"), "Downloads", "rollback_log.csv")

    def log_rollback(self, item_type, item_id, item_name):
        try:
            file_exists = os.path.isfile(self.rollback_file)
            
            with open(self.rollback_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(['type', 'id', 'name', 'created_at'])
                writer.writerow([item_type, item_id, item_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        except Exception as e:
            self.log(f"[Warning] Could not write to rollback log ({self.rollback_file}): {e}")

    def is_system_field(self, field):
        ftype = field.get('type', 'unknown')
        if ftype not in self.ALLOWED_TYPES: return True
        if field.get('removable') is False: return True
        if ftype in self.SYSTEM_FIELDS or field.get('key') in self.SYSTEM_FIELDS: return True
        if field.get('creator_user_id') == -1: return True
        return False

    def prepare_payload(self, field, object_key):
        try:
            pos = int(field.get('position', 0))
        except (ValueError, TypeError):
            pos = 0

        field_data = {
            'type': field.get('type'),
            'title': field.get('title'),
            'description': field.get('description', ''),
            'position': pos,
            'active': field.get('active', True),
        }

        if field.get('custom_field_options'):
            field_data['custom_field_options'] = [
                {'name': o['name'], 'value': o['value'], 'default': o.get('default', False)} 
                for o in field['custom_field_options']
            ]

        if object_key == 'ticket_field':
            if field.get('tag'):
                field_data['tag'] = field.get('tag')
            for attr in ['required', 'required_in_portal', 'visible_in_portal', 
                         'editable_in_portal', 'title_in_portal', 'agent_description', 'regexp_for_validation']:
                if attr in field:
                    field_data[attr] = field[attr]

        elif object_key in ['user_field', 'organization_field']:
            if field.get('key'):
                field_data['key'] = field.get('key')
            if 'regexp_for_validation' in field:
                field_data['regexp_for_validation'] = field['regexp_for_validation']

        return {object_key: field_data}

# ==========================================
# 3. GUI APPLICATION
# ==========================================

class ZendeskMigratorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Zendesk Migrator Suite")
        
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        self.root.geometry(f"{screen_width}x{screen_height}")
        
        self.s_data = {}
        self.t_data = {}
        self.analysis_results = None
        self.logic = None 
        self.show_tokens = False 
        self.verbose_var = tk.BooleanVar(value=False)
        
        self.setup_ui()
        self.init_config_path() 

    def setup_ui(self):
        style = ttk.Style()
        style.configure("Bold.TLabel", font=("Segoe UI", 9, "bold"))
        
        # --- CREDENTIALS FRAME ---
        self.creds_frame = ttk.LabelFrame(self.root, text=" Credentials Configuration ", padding=10)
        self.creds_frame.pack(fill='x', padx=10, pady=5, side='top')
        
        # Source
        ttk.Label(self.creds_frame, text="Source Domain:").grid(row=0, column=0, sticky='e')
        self.src_domain = ttk.Entry(self.creds_frame, width=25)
        self.src_domain.grid(row=0, column=1, padx=5, pady=2)
        
        ttk.Label(self.creds_frame, text="Source Email:").grid(row=0, column=2, sticky='e')
        self.src_email = ttk.Entry(self.creds_frame, width=25)
        self.src_email.grid(row=0, column=3, padx=5, pady=2)
        
        ttk.Label(self.creds_frame, text="Source Token:").grid(row=0, column=4, sticky='e')
        self.src_token = ttk.Entry(self.creds_frame, width=25, show="*")
        self.src_token.grid(row=0, column=5, padx=5, pady=2)

        # Target
        ttk.Label(self.creds_frame, text="Target Domain:").grid(row=1, column=0, sticky='e')
        self.tgt_domain = ttk.Entry(self.creds_frame, width=25)
        self.tgt_domain.grid(row=1, column=1, padx=5, pady=2)
        
        ttk.Label(self.creds_frame, text="Target Email:").grid(row=1, column=2, sticky='e')
        self.tgt_email = ttk.Entry(self.creds_frame, width=25)
        self.tgt_email.grid(row=1, column=3, padx=5, pady=2)
        
        ttk.Label(self.creds_frame, text="Target Token:").grid(row=1, column=4, sticky='e')
        self.tgt_token = ttk.Entry(self.creds_frame, width=25, show="*")
        self.tgt_token.grid(row=1, column=5, padx=5, pady=2)

        self.toggle_btn = ttk.Button(self.creds_frame, text="👁 Show Tokens", command=self.toggle_token_visibility, width=12)
        self.toggle_btn.grid(row=0, column=6, rowspan=2, padx=10)

        # --- CONFIG ROW ---
        cfg_row_frame = ttk.Frame(self.creds_frame)
        cfg_row_frame.grid(row=2, column=0, columnspan=7, pady=(15, 5), sticky='ew')
        ttk.Label(cfg_row_frame, text="Config File:").pack(side='left')
        self.config_path_var = tk.StringVar()
        self.config_entry = ttk.Entry(cfg_row_frame, textvariable=self.config_path_var)
        self.config_entry.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(cfg_row_frame, text="Browse...", command=self.browse_config_file).pack(side='left')

        # --- BUTTONS ---
        btn_frame = ttk.Frame(self.creds_frame)
        btn_frame.grid(row=3, column=0, columnspan=7, pady=5, sticky='w')
        ttk.Button(btn_frame, text="Load Config", command=self.load_config).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Save Config", command=self.save_config).pack(side='left', padx=5)
        ttk.Checkbutton(btn_frame, text="Verbose Logging (Debug)", variable=self.verbose_var).pack(side='left', padx=20)

        # --- TABS ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='x', expand=False, padx=10, pady=5)
        
        self.tab_migrate = ttk.Frame(self.notebook)
        self.tab_import = ttk.Frame(self.notebook)
        self.tab_rollback = ttk.Frame(self.notebook)
        
        self.notebook.add(self.tab_migrate, text=" Analysis & Export ")
        self.notebook.add(self.tab_import, text=" Import & Execute ")
        self.notebook.add(self.tab_rollback, text=" Rollback ")

        # --- TAB 1 ---
        mig_ctrl_frame = ttk.Frame(self.tab_migrate, padding=15)
        mig_ctrl_frame.pack(fill='x')
        ttk.Label(mig_ctrl_frame, text="Step 1:", font=("Segoe UI", 9, "bold")).pack(side='left')
        ttk.Button(mig_ctrl_frame, text="Analyze Differences", command=self.start_analysis).pack(side='left', padx=(5, 20))
        ttk.Label(mig_ctrl_frame, text="Step 2:", font=("Segoe UI", 9, "bold")).pack(side='left')
        ttk.Button(mig_ctrl_frame, text="Export Result to CSV", command=self.export_csv).pack(side='left', padx=5)

        # --- TAB 2 ---
        imp_ctrl_frame = ttk.Frame(self.tab_import, padding=15)
        imp_ctrl_frame.pack(fill='x')
        ttk.Label(imp_ctrl_frame, text="Step 3: Select CSV File").pack(anchor='w')
        file_sel_frame = ttk.Frame(imp_ctrl_frame)
        file_sel_frame.pack(fill='x', pady=5)
        self.csv_path_var = tk.StringVar()
        ttk.Entry(file_sel_frame, textvariable=self.csv_path_var).pack(side='left', fill='x', expand=True, padx=(0, 5))
        ttk.Button(file_sel_frame, text="Browse...", command=self.browse_csv).pack(side='left')
        
        opt_frame = ttk.Frame(imp_ctrl_frame)
        opt_frame.pack(fill='x', pady=10)
        ttk.Label(opt_frame, text="Step 4: Choose Strategy").pack(side='left')
        self.imp_strategy_var = tk.StringVar(value="skip")
        ttk.Radiobutton(opt_frame, text="Skip Existing (Safe)", variable=self.imp_strategy_var, value="skip").pack(side='left', padx=10)
        ttk.Radiobutton(opt_frame, text="Update (Overwrite)", variable=self.imp_strategy_var, value="update").pack(side='left', padx=10)
        ttk.Radiobutton(opt_frame, text="Clone (Duplicate)", variable=self.imp_strategy_var, value="clone").pack(side='left', padx=10)
        ttk.Button(opt_frame, text="RUN IMPORT", command=self.start_import).pack(side='right', padx=5)

        # --- TAB 3 ---
        rb_ctrl_frame = ttk.Frame(self.tab_rollback, padding=15)
        rb_ctrl_frame.pack(fill='x')
        
        # Default visual path
        default_rb_path = os.path.join(os.path.expanduser("~"), "Downloads", "rollback_log.csv")
        
        ttk.Label(rb_ctrl_frame, text="Rollback Log File:").pack(side='left')
        self.rb_path_var = tk.StringVar(value=default_rb_path)
        ttk.Entry(rb_ctrl_frame, textvariable=self.rb_path_var, width=40).pack(side='left', padx=5)
        ttk.Button(rb_ctrl_frame, text="Browse...", command=self.browse_rb).pack(side='left')
        ttk.Button(rb_ctrl_frame, text="DELETE ITEMS (UNDO)", command=self.start_rollback).pack(side='right', padx=5)
        
        # --- LOG ---
        log_frame = ttk.LabelFrame(self.root, text=" System Log ", padding=5)
        log_frame.pack(fill='both', expand=True, padx=10, pady=5)
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, state='disabled', font=("Consolas", 10))
        self.log_text.pack(fill='both', expand=True)
        
        # --- PROGRESS ---
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.root, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill='x', padx=10, pady=(0, 5))

        # --- INIT ---
        self.log_queue = queue.Queue()
        self.logic = MigrationLogic(self.log_queue_put, self.update_progress)
        self.root.after(100, self.process_log_queue)

    # --- HELPERS ---
    def count_options(self, fields):
        count = 0
        for f in fields:
            opts = f.get('custom_field_options', [])
            if opts: count += len(opts)
        return count
    
    def toggle_token_visibility(self):
        if self.show_tokens:
            self.src_token.config(show="*")
            self.tgt_token.config(show="*")
            self.toggle_btn.config(text="👁 Show Tokens")
            self.show_tokens = False
        else:
            self.src_token.config(show="")
            self.tgt_token.config(show="")
            self.toggle_btn.config(text="👁 Hide Tokens")
            self.show_tokens = True

    def log_queue_put(self, msg):
        self.log_queue.put(msg)

    def process_log_queue(self):
        while not self.log_queue.empty():
            msg = self.log_queue.get()
            if isinstance(msg, tuple) and msg[0] == "POPUP":
                self.log_text.update_idletasks() 
                messagebox.showinfo(msg[1], msg[2])
            else:
                self.log_text.configure(state='normal')
                self.log_text.insert(tk.END, str(msg) + "\n")
                self.log_text.see(tk.END)
                self.log_text.configure(state='disabled')
        self.root.after(100, self.process_log_queue)

    def update_progress(self, current, total):
        if total > 0:
            pct = (current / total) * 100
            self.progress_var.set(pct)

    def browse_csv(self):
        f = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if f: self.csv_path_var.set(f)

    def browse_rb(self):
        f = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if f: self.rb_path_var.set(f)
    
    def browse_config_file(self):
        f = filedialog.askopenfilename(filetypes=[("JSON Files", "*.json")])
        if f: self.config_path_var.set(f)

    def get_creds(self, target_only=False):
        t = {
            'subdomain': self.tgt_domain.get(),
            'email': self.tgt_email.get(),
            'token': self.tgt_token.get()
        }
        if target_only: return t
        s = {
            'subdomain': self.src_domain.get(),
            'email': self.src_email.get(),
            'token': self.src_token.get()
        }
        if not all(s.values()) or not all(t.values()):
            messagebox.showerror("Error", "Please fill in all credential fields.")
            return None, None
        return s, t

    # --- CONFIG ---
    def init_config_path(self):
        current_dir_path = os.path.abspath('config.json')
        user_home_path = os.path.join(os.path.expanduser("~"), 'config.json')
        if os.path.exists(current_dir_path):
            self.config_path_var.set(current_dir_path)
            self.load_config()
        elif os.path.exists(user_home_path):
            self.config_path_var.set(user_home_path)
            self.load_config()
        else:
            self.config_path_var.set(user_home_path)
            self.log_queue_put("[Info] No config.json found. Defaulting path to User Home.")

    def load_config(self):
        path = self.config_path_var.get()
        if not os.path.exists(path):
            messagebox.showerror("Error", f"File not found:\n{path}")
            return
        try:
            with open(path, 'r') as f:
                c = json.load(f)
                s = c.get('source_creds', {})
                t = c.get('target_creds', {})
                self.src_domain.delete(0, tk.END); self.src_domain.insert(0, s.get('subdomain', ''))
                self.src_email.delete(0, tk.END); self.src_email.insert(0, s.get('email', ''))
                self.src_token.delete(0, tk.END); self.src_token.insert(0, s.get('token', ''))
                self.tgt_domain.delete(0, tk.END); self.tgt_domain.insert(0, t.get('subdomain', ''))
                self.tgt_email.delete(0, tk.END); self.tgt_email.insert(0, t.get('email', ''))
                self.tgt_token.delete(0, tk.END); self.tgt_token.insert(0, t.get('token', ''))
                
                rb_config = c.get('rollback_filename')
                if rb_config:
                    self.rb_path_var.set(rb_config)
                    
                self.log_queue_put(f"[Info] Loaded config from {path}")
        except Exception as e:
            self.log_queue_put(f"[Error] Config load failed: {e}")
            messagebox.showerror("Error", f"Failed to parse config file: {e}")

    def save_config(self):
        path = self.config_path_var.get()
        if not path:
             messagebox.showerror("Error", "Please define a path for the config file.")
             return
        data = {
            "source_creds": {"subdomain": self.src_domain.get(), "email": self.src_email.get(), "token": self.src_token.get()},
            "target_creds": {"subdomain": self.tgt_domain.get(), "email": self.tgt_email.get(), "token": self.tgt_token.get()},
            "rollback_filename": self.rb_path_var.get()
        }
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=4)
            self.log_queue_put(f"[Info] Saved config to {path}")
            messagebox.showinfo("Saved", f"Configuration saved successfully to:\n{path}")
        except Exception as e:
            self.log_queue_put(f"[Error] Save failed: {e}")
            messagebox.showerror("Error", f"Could not save file: {e}")

    # ==========================================
    # --- LOGIC THREADS ---
    # ==========================================
    
    def start_analysis(self):
        s_creds, t_creds = self.get_creds()
        if not s_creds: return
        threading.Thread(target=self.run_analysis_thread, args=(s_creds, t_creds, self.verbose_var.get()), daemon=True).start()

    def run_analysis_thread(self, s_creds, t_creds, verbose):
        try:
            self.log_queue_put("--- STARTING ANALYSIS ---")
            source = ZendeskClient(s_creds['subdomain'], s_creds['email'], s_creds['token'], self.log_queue_put, verbose)
            target = ZendeskClient(t_creds['subdomain'], t_creds['email'], t_creds['token'], self.log_queue_put, verbose)

            self.s_data = {
                'ticket_fields': source.get_all("ticket_fields.json", "ticket_fields"),
                'user_fields': source.get_all("user_fields.json", "user_fields"),
                'organization_fields': source.get_all("organization_fields.json", "organization_fields"),
                'ticket_forms': source.get_all("ticket_forms.json", "ticket_forms")
            }
            
            self.t_data = {
                'ticket_fields': target.get_all("ticket_fields.json", "ticket_fields"),
                'user_fields': target.get_all("user_fields.json", "user_fields"),
                'organization_fields': target.get_all("organization_fields.json", "organization_fields"),
                'ticket_forms': target.get_all("ticket_forms.json", "ticket_forms")
            }

            self.analysis_results = {'new': [], 'exist': [], 'forms_new': [], 'forms_exist': []}
            
            t_maps = {
                'ticket_fields': {(f['title'].lower(), f['type']): f['id'] for f in self.t_data['ticket_fields']},
                'user_fields': {(f['title'].lower(), f['type']): f['id'] for f in self.t_data['user_fields']},
                'organization_fields': {(f['title'].lower(), f['type']): f['id'] for f in self.t_data['organization_fields']},
                'ticket_forms': {f['name'].lower(): f['id'] for f in self.t_data['ticket_forms']}
            }

            for type_key in ['ticket_fields', 'user_fields', 'organization_fields']:
                for f in self.s_data[type_key]:
                    if self.logic.is_system_field(f): continue
                    key = (f['title'].lower(), f['type'])
                    obj_type_singular = type_key[:-1]
                    if key in t_maps[type_key]:
                        self.analysis_results['exist'].append({'source': f, 'target_id': t_maps[type_key][key], 'type': obj_type_singular, 'list_key': type_key})
                    else:
                        self.analysis_results['new'].append({'source': f, 'type': obj_type_singular, 'list_key': type_key})

            for f in self.s_data['ticket_forms']:
                if f.get('name') == "Default Ticket Form": continue
                if f['name'].lower() in t_maps['ticket_forms']:
                    self.analysis_results['forms_exist'].append({'source': f, 'target_id': t_maps['ticket_forms'][f['name'].lower()]})
                else:
                    self.analysis_results['forms_new'].append({'source': f})

            mig_ticket = [x['source'] for x in self.analysis_results['new'] if x['list_key'] == 'ticket_fields']
            mig_user   = [x['source'] for x in self.analysis_results['new'] if x['list_key'] == 'user_fields']
            mig_org    = [x['source'] for x in self.analysis_results['new'] if x['list_key'] == 'organization_fields']
            exist_ticket = [x for x in self.analysis_results['exist'] if x['list_key'] == 'ticket_fields']
            exist_user   = [x for x in self.analysis_results['exist'] if x['list_key'] == 'user_fields']
            exist_org    = [x for x in self.analysis_results['exist'] if x['list_key'] == 'organization_fields']

            self.log_queue_put("\n" + "="*85)
            self.log_queue_put(f"{'MIGRATION BLUEPRINT':^85}")
            self.log_queue_put("="*85)
            self.log_queue_put(f"FROM: {s_creds['subdomain']}")
            self.log_queue_put(f"TO:   {t_creds['subdomain']}")
            self.log_queue_put("-" * 85)
            self.log_queue_put(f"{'OBJECT TYPE':<15} | {'NEW':<5} | {'EXISTING':<8} | {'DROPDOWN OPTIONS'}")
            self.log_queue_put("-" * 85)
            self.log_queue_put(f"{'Ticket Fields':<15} | {len(mig_ticket):<5} | {len(exist_ticket):<8} | {self.count_options(mig_ticket)} tags new")
            self.log_queue_put(f"{'User Fields':<15} | {len(mig_user):<5} | {len(exist_user):<8} | {self.count_options(mig_user)} tags new")
            self.log_queue_put(f"{'Org Fields':<15} | {len(mig_org):<5} | {len(exist_org):<8} | {self.count_options(mig_org)} tags new")
            self.log_queue_put(f"{'Ticket Forms':<15} | {len(self.analysis_results['forms_new']):<5} | {len(self.analysis_results['forms_exist']):<8} | -")
            self.log_queue_put("-" * 85)
            
            self.log_queue_put(("POPUP", "Analysis Done", "Fetch and analysis complete. You can now Export CSV."))
        except Exception as e:
            self.log_queue_put(f"[CRITICAL ERROR] Analysis Thread Crashed: {e}")
            import traceback
            self.log_queue_put(traceback.format_exc())

    def export_csv(self):
        if not self.analysis_results:
            messagebox.showwarning("No Data", "Please run Analysis first.")
            return
        f = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not f: return
        all_fields = [x['source'] for x in self.analysis_results['new']] + [x['source'] for x in self.analysis_results['exist']]
        all_forms = [x['source'] for x in self.analysis_results['forms_new']] + [x['source'] for x in self.analysis_results['forms_exist']]
        field_to_forms = {}
        for form in all_forms:
            for fid in form.get('ticket_field_ids', []):
                if fid not in field_to_forms: field_to_forms[fid] = []
                field_to_forms[fid].append(form.get('name'))

        try:
            with open(f, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([
                    'Type', 'Object', 'Name', 'Title (Customer)', 'Tag', 'Description (Customer)', 'Agent Description',
                    'Agent Required (Solved)', 'End-User Required', 'End-User Visible', 'End-User Editable', 'RegEx', 'Default', 'Active'
                ])
                for frm in all_forms:
                    writer.writerow(['ticket_form', 'root', frm['name'], frm.get('display_name', ''), '', '', '', '', '', frm.get('end_user_visible', False), '', '', '', frm.get('active', True)])
                for field in all_fields:
                    ftype = field['type']
                    if ftype in self.logic.ALLOWED_TYPES:
                        ctx = "Global"
                        if 'ticket' in str(field.get('url', '')): 
                            f_forms = field_to_forms.get(field['id'], [])
                            ctx = " | ".join(f_forms) if f_forms else "Ticket"
                        elif 'user' in str(field.get('url', '')): ctx = "(User) Global"
                        elif 'organization' in str(field.get('url', '')): ctx = "(Org) Global"
                        writer.writerow([
                            ftype, ctx, field['title'], field.get('title_in_portal', ''), field.get('tag', field.get('key', '')),
                            (field.get('description') or '').replace('\n', ' '), (field.get('agent_description') or '').replace('\n', ' '),
                            field.get('required', False), field.get('required_in_portal', False), field.get('visible_in_portal', False),
                            field.get('editable_in_portal', False), field.get('regexp_for_validation', ''), '', field.get('active', True)
                        ])
                        for o in field.get('custom_field_options', []):
                             writer.writerow(['option', field['title'], o['name'], '', o['value'], '', '', '', '', '', '', '', o.get('default', False), ''])
            self.log_queue_put(f"[Success] CSV Exported to {f}")
        except Exception as e:
            self.log_queue_put(f"[Error] CSV Export failed: {e}")

    def start_import(self):
        csv_p = self.csv_path_var.get()
        if not os.path.exists(csv_p):
             messagebox.showerror("Error", "CSV file not found")
             return
        t_creds = self.get_creds(target_only=True)
        if not t_creds: return
        
        rb_path = self.rb_path_var.get()
        threading.Thread(target=self.run_import_thread, args=(t_creds, csv_p, self.imp_strategy_var.get(), self.verbose_var.get(), rb_path), daemon=True).start()

    def run_import_thread(self, creds, csv_path, strategy, verbose, rb_path):
        try:
            # CHECK AND REDIRECT PATH IF NEEDED
            rb_dir = os.path.dirname(rb_path)
            if rb_dir and not os.path.exists(rb_dir):
                new_path = os.path.join(os.path.expanduser("~"), "Downloads", "rollback_log.csv")
                self.log_queue_put(f"[Warning] Path not found: {rb_dir}")
                self.log_queue_put(f"[Info] Redirecting rollback log to: {new_path}")
                rb_path = new_path
            
            self.logic.rollback_file = rb_path
            
            self.log_queue_put("--- STARTING CSV IMPORT ---")
            client = ZendeskClient(creds['subdomain'], creds['email'], creds['token'], self.log_queue_put, verbose)
            
            fields = []
            forms = []
            current_field = None
            
            try:
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    
                    required_headers = ['Type', 'Name']
                    if not all(h in reader.fieldnames for h in required_headers):
                        self.log_queue_put(f"[Error] CSV missing required headers. Found: {reader.fieldnames}")
                        return

                    for row in reader:
                        row = {k.strip(): v for k, v in row.items() if k}
                        rtype = row['Type'].strip().lower()
                        
                        if rtype == 'ticket_form':
                            forms.append({
                                'name': row['Name'], 
                                'display_name': row.get('Title (Customer)', ''),
                                'active': row.get('Active', 'true').lower() == 'true',
                                'end_user_visible': row.get('End-User Visible', 'false').lower() == 'true'
                            })
                            current_field = None
                        elif rtype in self.logic.ALLOWED_TYPES:
                            obj_type = 'ticket_field'
                            obj_val = row.get('Object', '')
                            if '(User)' in obj_val: obj_type = 'user_field'
                            elif '(Org)' in obj_val: obj_type = 'organization_field'
                            
                            field_tag_or_key = row.get('Tag', '')
                            f_key = None
                            f_tag = None
                            
                            if obj_type == 'ticket_field':
                                f_tag = field_tag_or_key
                            else:
                                f_key = field_tag_or_key 
                                f_tag = field_tag_or_key 
                            
                            current_field = {
                                'type': rtype, 
                                'title': row['Name'], 
                                'title_in_portal': row.get('Title (Customer)', ''),
                                'description': row.get('Description (Customer)', ''), 
                                'agent_description': row.get('Agent Description', ''),
                                'active': row.get('Active', 'true').lower() == 'true', 
                                'tag': f_tag,
                                'key': f_key, 
                                'regexp_for_validation': row.get('RegEx', ''),
                                'required': row.get('Agent Required (Solved)', 'false').lower() == 'true',
                                'required_in_portal': row.get('End-User Required', 'false').lower() == 'true',
                                'visible_in_portal': row.get('End-User Visible', 'false').lower() == 'true',
                                'editable_in_portal': row.get('End-User Editable', 'false').lower() == 'true',
                                'custom_field_options': [], 
                                'system_object_type': obj_type,
                                'associated_forms': [x.strip() for x in obj_val.split('|')] if obj_type == 'ticket_field' else []
                            }
                            fields.append(current_field)
                        elif rtype == 'option' and current_field:
                             current_field['custom_field_options'].append({
                                 'name': row['Name'], 
                                 'value': row.get('Tag', ''), 
                                 'default': row.get('Default', 'False').lower() == 'true'
                             })
                        else:
                            if verbose:
                                self.log_queue_put(f"[Debug] Skipping CSV row: Type='{row['Type']}' (normalized='{rtype}')")

            except Exception as e:
                self.log_queue_put(f"[Error] CSV Parse Failed: {e}")
                return

            t_data = {
                'ticket_field': client.get_all("ticket_fields.json", "ticket_fields"),
                'user_field': client.get_all("user_fields.json", "user_fields"),
                'organization_field': client.get_all("organization_fields.json", "organization_fields"),
                'ticket_form': client.get_all("ticket_forms.json", "ticket_forms")
            }
            
            maps = {}
            for k, v in t_data.items():
                if k == 'ticket_form':
                     maps[k] = {x['name'].lower(): x['id'] for x in v}
                else:
                     maps[k] = {(x['title'].lower(), x['type']): x['id'] for x in v}

            name_to_id_map = {} 
            total = len(fields) + len(forms)
            count = 0
            
            stats = {
                'ticket_field': {'new': 0, 'exist': 0},
                'user_field': {'new': 0, 'exist': 0},
                'organization_field': {'new': 0, 'exist': 0},
                'ticket_form': {'new': 0, 'exist': 0}
            }
            new_fields_for_options = []

            for f in fields:
                ftype = f['system_object_type']
                key = (f['title'].lower(), f['type'])
                exists = key in maps[ftype]
                
                if verbose:
                    self.log_queue_put(f"--- Processing Field: {f['title']} ---")
                    self.log_queue_put(f"   > Type: {f['type']} | Object: {ftype}")
                    self.log_queue_put(f"   > Match in target? {'YES' if exists else 'NO'}")

                payload = self.logic.prepare_payload(f, ftype)
                target_id = None
                
                if exists:
                    target_id = maps[ftype][key]
                    if strategy == 'clone':
                        if verbose: self.log_queue_put("   > Action: CLONING (Creating duplicate)")
                        res = client.create_field_safe(f"{ftype}s.json", payload, ftype)
                        if res: 
                            target_id = res[ftype]['id']
                            self.logic.log_rollback(ftype, target_id, f['title'])
                            stats[ftype]['new'] += 1
                            new_fields_for_options.append(f)
                    elif strategy == 'update':
                        if verbose: self.log_queue_put(f"   > Action: UPDATING existing ID {target_id}")
                        client.update_object(f"{ftype}s", target_id, payload)
                        stats[ftype]['exist'] += 1
                    else: 
                        if verbose: self.log_queue_put("   > Action: SKIPPING (Strategy=Skip)")
                        stats[ftype]['exist'] += 1
                else:
                    if verbose: self.log_queue_put("   > Action: CREATING new field")
                    res = client.create_field_safe(f"{ftype}s.json", payload, ftype)
                    if res:
                        target_id = res[ftype]['id']
                        self.logic.log_rollback(ftype, target_id, f['title'])
                        stats[ftype]['new'] += 1
                        new_fields_for_options.append(f)
                
                if target_id:
                    name_to_id_map[f['title']] = target_id
                
                count += 1
                self.update_progress(count, total)
                time.sleep(0.2)

            for frm in forms:
                if verbose: self.log_queue_put(f"--- Processing Form: {frm['name']} ---")
                
                exists = frm['name'].lower() in maps['ticket_form']
                target_id = maps['ticket_form'].get(frm['name'].lower())
                
                f_ids = []
                for f in fields:
                     if 'associated_forms' in f and frm['name'] in f['associated_forms']:
                         if f['title'] in name_to_id_map:
                             f_ids.append(name_to_id_map[f['title']])
                
                payload = {'ticket_form': {
                    'name': frm['name'], 
                    'display_name': frm['display_name'],
                    'active': frm['active'], 
                    'end_user_visible': frm['end_user_visible'],
                    'ticket_field_ids': f_ids
                }}
                
                if exists:
                    if strategy == 'clone':
                        if verbose: self.log_queue_put("   > Action: CLONING form")
                        res = client._request('POST', 'ticket_forms.json', payload)
                        if res and res.status_code==201:
                             self.logic.log_rollback('ticket_form', res.json()['ticket_form']['id'], frm['name'])
                             stats['ticket_form']['new'] += 1
                    elif strategy == 'update':
                        if verbose: self.log_queue_put(f"   > Action: UPDATING form ID {target_id}")
                        client.update_object('ticket_forms', target_id, payload)
                        stats['ticket_form']['exist'] += 1
                    else:
                        if verbose: self.log_queue_put("   > Action: SKIPPING form")
                        stats['ticket_form']['exist'] += 1
                else:
                    if verbose: self.log_queue_put("   > Action: CREATING form")
                    res = client._request('POST', 'ticket_forms.json', payload)
                    if res and res.status_code==201:
                         self.logic.log_rollback('ticket_form', res.json()['ticket_form']['id'], frm['name'])
                         stats['ticket_form']['new'] += 1
                
                count += 1
                self.update_progress(count, total)
                time.sleep(0.2)
                
            self.log_queue_put("\n" + "="*85)
            self.log_queue_put(f"{'IMPORT EXECUTION REPORT':^85}")
            self.log_queue_put("="*85)
            self.log_queue_put(f"TARGET: {creds['subdomain']}")
            self.log_queue_put("-" * 85)
            self.log_queue_put(f"{'OBJECT TYPE':<15} | {'CREATED':<7} | {'UPDATED/SKIPPED':<15} | {'DROPDOWN OPTIONS'}")
            self.log_queue_put("-" * 85)
            
            def count_new_options(ftype_filter):
                subset = [f for f in new_fields_for_options if f['system_object_type'] == ftype_filter]
                return self.count_options(subset)

            self.log_queue_put(f"{'Ticket Fields':<15} | {stats['ticket_field']['new']:<7} | {stats['ticket_field']['exist']:<15} | {count_new_options('ticket_field')} tags added")
            self.log_queue_put(f"{'User Fields':<15} | {stats['user_field']['new']:<7} | {stats['user_field']['exist']:<15} | {count_new_options('user_field')} tags added")
            self.log_queue_put(f"{'Org Fields':<15} | {stats['organization_field']['new']:<7} | {stats['organization_field']['exist']:<15} | {count_new_options('organization_field')} tags added")
            self.log_queue_put(f"{'Ticket Forms':<15} | {stats['ticket_form']['new']:<7} | {stats['ticket_form']['exist']:<15} | -")
            self.log_queue_put("-" * 85)

            self.log_queue_put("Import Complete.")
            self.log_queue_put(("POPUP", "Success", "Import execution finished."))
        except Exception as e:
            self.log_queue_put(f"[CRITICAL ERROR] Import Thread Crashed: {e}")
            import traceback
            self.log_queue_put(traceback.format_exc())

    def start_rollback(self):
        rb_file = self.rb_path_var.get()
        if not os.path.exists(rb_file):
            messagebox.showerror("Error", "Rollback file not found.")
            return
        t_creds = self.get_creds(target_only=True)
        if not t_creds: return
        confirm = messagebox.askstring("Danger Zone", "Type 'DELETE' to permanently delete items listed in the rollback file:")
        if confirm != 'DELETE': return
        threading.Thread(target=self.run_rollback_thread, args=(t_creds, rb_file, self.verbose_var.get()), daemon=True).start()

    def run_rollback_thread(self, creds, filepath, verbose):
        try:
            self.log_queue_put("--- STARTING ROLLBACK ---")
            client = ZendeskClient(creds['subdomain'], creds['email'], creds['token'], self.log_queue_put, verbose)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    reader = list(csv.DictReader(f))
            except Exception as e:
                self.log_queue_put(f"[Error] Read CSV failed: {e}")
                return
            reader.reverse()
            total = len(reader)
            for i, row in enumerate(reader):
                endpoint = None
                if row['type'] == 'ticket_form': endpoint = "ticket_forms"
                elif row['type'] == 'ticket_field': endpoint = "ticket_fields"
                elif row['type'] == 'user_field': endpoint = "user_fields"
                elif row['type'] == 'organization_field': endpoint = "organization_fields"
                if endpoint:
                    client.delete_item(endpoint, row['id'])
                    self.log_queue_put(f"Deleted {row['type']} ID: {row['id']}")
                self.update_progress(i+1, total)
                time.sleep(0.3)
            self.log_queue_put("Rollback Complete.")
            self.log_queue_put(("POPUP", "Rollback", "Rollback execution finished."))
        except Exception as e:
            self.log_queue_put(f"[CRITICAL ERROR] Rollback Thread Crashed: {e}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ZendeskMigratorApp(root)
    root.mainloop()