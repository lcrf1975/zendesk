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
import socket
import html
from datetime import datetime
from datetime import timedelta
from datetime import timezone

# ==============================================================================
# CONFIGURATION: MACOS + PYENV + QT6 COMPATIBILITY
# ==============================================================================
if sys.platform == 'darwin':  # macOS
    os.environ['QT_MAC_WANTS_LAYER'] = '1'
    os.environ['QT_DEBUG_PLUGINS'] = '0'
    os.environ['QT_FILESYSTEMMODEL_WATCH_FILES'] = '0'

os.environ['QT_QUICK_BACKEND'] = 'software'

from PyQt6.QtWidgets import QApplication
from PyQt6.QtWidgets import QMainWindow
from PyQt6.QtWidgets import QWidget
from PyQt6.QtWidgets import QVBoxLayout
from PyQt6.QtWidgets import QHBoxLayout
from PyQt6.QtWidgets import QLabel
from PyQt6.QtWidgets import QLineEdit
from PyQt6.QtWidgets import QPushButton
from PyQt6.QtWidgets import QStackedWidget
from PyQt6.QtWidgets import QTableWidget
from PyQt6.QtWidgets import QTableWidgetItem
from PyQt6.QtWidgets import QHeaderView
from PyQt6.QtWidgets import QProgressBar
from PyQt6.QtWidgets import QTextEdit
from PyQt6.QtWidgets import QMessageBox
from PyQt6.QtWidgets import QFrame
from PyQt6.QtWidgets import QGraphicsDropShadowEffect
from PyQt6.QtWidgets import QGroupBox
from PyQt6.QtWidgets import QSizePolicy
from PyQt6.QtWidgets import QSplitter
from PyQt6.QtWidgets import QAbstractItemView
from PyQt6.QtWidgets import QComboBox
from PyQt6.QtWidgets import QCheckBox
from PyQt6.QtWidgets import QFileDialog
from PyQt6.QtWidgets import QSpacerItem

from PyQt6.QtCore import Qt
from PyQt6.QtCore import QThread
from PyQt6.QtCore import pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtGui import QColor
from PyQt6.QtGui import QPalette

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

CREDENTIALS_FILE = "credentials.json"

SYSTEM_FIELD_IDENTIFIERS = {
    'subject', 'description', 'status', 'tickettype', 'ticket_type', 'priority', 
    'group', 'assignee', 'brand', 'satisfaction_rating', 'custom_status', 'lookup',
    'email', 'name', 'time_zone', 'locale_id', 'organization_id', 'role', 
    'custom_role_id', 'details', 'notes', 'phone', 'mobile', 'whatsapp', 'facebook', 
    'twitter', 'google', 'photo', 'authenticity_token', 'active', 'alias', 
    'signature', 'shared_phone_number', 'domain_names', 'tags', 'shared_tickets', 
    'shared_comments', 'external_id', 'problem_id', 'created_at', 'updated_at', 
    'via_id', 'recipient', 'submitter', 'requester', 'due_date'
}

SYSTEM_FIELD_NAMES = {
    'intent', 'intent confidence', 'sentiment', 'sentiment confidence', 'language', 
    'language confidence', 'summary', 'resolution type', 'approval status',
    'suggestion', 'recommendation', 'ticket status', 'shared with', 'confidence', 
    'summary agent id', 'summary date and time', 'summary locale', 
    'id do agente do resumo', 'localidade do resumo', 'resumo', 'data e hora do resumo', 
    'status de aprovação', 'intenção', 'confiança da intenção', 'confiança do sentimento', 
    'idioma', 'confiança do idioma'
}

# ==============================================================================
# STYLESHEET
# ==============================================================================
STYLESHEET = """
QMainWindow { background-color: #F3F4F6; }
QWidget { font-family: '.AppleSystemUIFont', 'Helvetica Neue', Arial, sans-serif; font-size: 14px; color: #111827; }
QFrame#Sidebar { background-color: #FFFFFF; border-right: 1px solid #D1D5DB; }
QPushButton#StepBtn { text-align: left; padding: 12px 16px; border: 1px solid transparent; border-radius: 6px; margin: 4px 12px; background-color: transparent; color: #374151; font-weight: 500; }
QPushButton#StepBtn:hover { background-color: #F3F4F6; color: #000000; }
QPushButton#StepBtn:checked { background-color: #E0E7FF; color: #1E40AF; font-weight: 800; border: 1px solid #C7D2FE; border-left: 5px solid #1E40AF; }
QPushButton#StepBtn:disabled { color: #9CA3AF; background-color: transparent; }
QFrame#Card, QFrame#CardFull { background-color: #FFFFFF; border: 1px solid #D1D5DB; border-radius: 8px; }
QLabel#Title { font-size: 22px; font-weight: 800; color: #111827; margin-bottom: 8px; }
QLabel#Subtitle { font-size: 14px; color: #4B5563; margin-bottom: 24px; line-height: 1.4; }
QLabel#SummaryText { font-weight: 600; color: #4B5563; margin-bottom: 10px; }
QLabel#NoteText { font-size: 12px; color: #6B7280; font-style: italic; margin-top: 5px; }
QLineEdit, QComboBox { padding: 10px; border: 1px solid #9CA3AF; border-radius: 6px; background-color: #FFFFFF; color: #000000; }
QLineEdit:focus, QComboBox:focus { border: 2px solid #2563EB; }
QPushButton#PrimaryBtn { background: #BFDBFE; color: #1E3A8A; border: 1px solid #60A5FA; padding: 12px 24px; border-radius: 6px; font-weight: 800; font-size: 14px; }
QPushButton#PrimaryBtn:hover { background: #93C5FD; border: 1px solid #3B82F6; }
QPushButton#PrimaryBtn:pressed { background: #60A5FA; }
QPushButton#DangerBtn { background: #FECACA; color: #991B1B; border: 1px solid #F87171; padding: 12px 24px; border-radius: 6px; font-weight: 800; }
QPushButton#DangerBtn:hover { background: #FCA5A5; border: 1px solid #EF4444; }
QPushButton#DangerBtn:disabled { background: #F3F4F6; color: #9CA3AF; border: 1px solid #D1D5DB; }
QPushButton#SecondaryBtn { background: #FFFFFF; color: #374151; border: 1px solid #D1D5DB; padding: 8px 16px; border-radius: 6px; font-weight: 600; }
QPushButton#SecondaryBtn:hover { background: #F9FAFB; border: 1px solid #9CA3AF; }
QTextEdit#LogBox { background-color: #111827; border: none; color: #10B981; font-family: 'Menlo', 'Courier New', monospace; font-size: 12px; padding: 10px; }
QTextEdit#InfoBox { background-color: #F0FDFA; border: 1px solid #2DD4BF; color: #134E4A; font-family: '.AppleSystemUIFont', 'Helvetica Neue', 'Segoe UI', Arial, sans-serif; font-size: 13px; padding: 12px; border-radius: 6px; font-weight: 600; }
QFrame#StatusBar { background-color: #F9FAFB; border-top: 1px solid #D1D5DB; }
QLabel#StatusText { font-weight: 700; color: #2563EB; font-size: 16px; }
QLabel#StatsText { font-weight: 500; color: #4B5563; font-size: 13px; margin-right: 10px; }
QFrame#CompactSummary { background-color: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 6px; max-height: 50px; }
QLabel#CompactLabel { font-weight: 600; color: #1E40AF; padding: 0 10px; }
QLabel#FilterLabel { font-weight: bold; color: #374151; }
"""

# ==============================================================================
# CONTROLLER
# ==============================================================================
class ZendeskController:
    def __init__(self):
        self.creds = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ZendeskDCManager/14.0',
            'Content-Type': 'application/json'
        })
        
        retry_strategy = Retry(
            total=5, 
            backoff_factor=1, 
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        
        self.work_items = [] 
        self.translation_cache = {}
        self.trans_provider = "Google Web (Free)"
        self.trans_api_key = ""
        self.last_execution_results = {
            'success': [], 
            'failed': [],
            'backup_file': ''
        }
        self.rollback_log_file = ""
        self.stop_requested = False
        self.backup_folder = ""

    def _clean_subdomain(self, sub):
        cleaned = sub.lower()
        cleaned = cleaned.replace("https://", "")
        cleaned = cleaned.replace(".zendesk.com", "")
        cleaned = cleaned.replace("/", "")
        return cleaned.strip()

    def save_profile(self, filepath, sub, email, token, backup_folder, api_key=""):
        data = {
            "subdomain": sub,
            "email": email,
            "token": token,
            "backup_path": backup_folder,
            "google_api_key": api_key
        }
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            return True
        except Exception as e:
            print(e)
            return False

    def load_profile(self, filepath):
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
                if 'rollback_file' in data and 'backup_path' not in data:
                    data['backup_path'] = os.path.dirname(data['rollback_file'])
                return data
        except:
            return None

    def connect(self, subdomain, email, token, backup_folder, log_callback=None):
        clean_sub = self._clean_subdomain(subdomain)
        self.creds = {
            'subdomain': clean_sub, 
            'email': email, 
            'token': token
        }
        self.backup_folder = backup_folder
        self.session.auth = (f"{email}/token", token)
        
        target = f"https://{clean_sub}.zendesk.com/api/v2/users/me.json"
        
        if log_callback:
            log_callback.emit(f"Connecting to: {target}")
        
        try:
            resp = self.session.get(target, timeout=10)
            
            if log_callback:
                log_callback.emit(f"Response Code: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                if 'user' not in data:
                    raise Exception("Auth Failed: Incorrect Credentials")
                
                name = data['user'].get('name', 'Unknown')
                role = data['user'].get('role', 'unknown')
                
                if role == 'end-user':
                    raise Exception("Authentication Failed: Check Your Credentials")
                
                if role not in ['admin', 'agent']:
                    raise Exception(f"Auth Failed: Role '{role}' insufficient.")
                
                if log_callback:
                    log_callback.emit(f"Authenticated: {name} ({role})")
                
                return f"Connected as {name}"
                
            elif resp.status_code == 401:
                raise Exception("401 Unauthorized.")
            elif resp.status_code == 403:
                raise Exception("403 Forbidden.")
            else:
                raise Exception(f"Connection Failed: {resp.status_code}")
                
        except Exception as e:
            if log_callback:
                log_callback.emit(f"Error: {str(e)}")
            raise e

    def stop(self):
        self.stop_requested = True

    def reset_stop(self):
        self.stop_requested = False

    def _is_dc_string(self, text):
        if not text:
            return False
        text = text.strip()
        if text.startswith("{{") and text.endswith("}}") and "dc." in text:
            return True
        return False

    def _sanitize(self, text):
        t = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8')
        sanitized = re.sub(r'[^a-zA-Z0-9_]+', '_', t)
        return sanitized.strip('_').lower()

    def _calc_eta(self, start_time, processed, total):
        if processed == 0: 
            return "Calculating..."
        
        elapsed = time.time() - start_time
        
        if elapsed == 0: 
            return "..."
            
        rate = processed / elapsed
        remaining_items = total - processed
        eta_seconds = remaining_items / rate
        
        if eta_seconds < 60:
            return f"{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            return f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
        else:
            return f"{int(eta_seconds // 3600)}h {int((eta_seconds % 3600) // 60)}m"

    # --- PARALLEL SCAN AND ANALYZE ---
    def scan_and_analyze(self, progress_callback, log_callback, scan_config):
        if not self.creds:
            raise Exception("Credentials not found")
            
        progress_callback.emit(0, 0, "Initializing Parallel Scan...")
        self.reset_stop()
        
        log_callback.emit("Fetching existing Dynamic Content...")
        existing_dc = {} 
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/dynamic_content/items.json"
        
        while url:
            if self.stop_requested:
                raise Exception("Operation Canceled")
            
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200:
                break
            
            for item in resp.json().get('items', []):
                existing_dc[item['name']] = item['id']
            
            url = resp.json().get('next_page')
            
        log_callback.emit("Launching parallel scan threads...")
        
        raw_items = []
        self.scan_stats = {
            'valid_fields': 0, 
            'valid_forms': 0, 
            'valid_cats': 0, 
            'valid_sects': 0, 
            'valid_arts': 0, 
            'ignored': 0, 
            'already_dc': 0
        }
        
        tasks = []
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(task, log_callback) for task in tasks]
            
            for future in concurrent.futures.as_completed(futures):
                if self.stop_requested:
                    raise Exception("Scan Canceled")
                
                try:
                    result_items = future.result()
                    raw_items.extend(result_items)
                    count = len(raw_items)
                    progress_callback.emit(0, 0, f"Found {count} items so far...")
                except Exception as e:
                    log_callback.emit(f"Scan Error in thread: {str(e)}")

        log_callback.emit("Analyzing items and building tasks...")
        self.work_items = []
        seen_dc_names = set()
        
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
                
                if item['dc_name'] in existing_dc:
                    action = "LINK"
                    dc_id = existing_dc[item['dc_name']]
                elif item['dc_name'] in seen_dc_names:
                    action = "LINK" 
                else:
                    seen_dc_names.add(item['dc_name'])
                
                self.work_items.append({
                    'id': item['id'],
                    'type': item['type'],
                    'context': item['context'],
                    'dc_name': item['dc_name'],
                    'placeholder': f"{{{{dc.{item['dc_name']}}}}}",
                    'pt': item['title'],
                    'en': "(Pending Translation)" if action == "CREATE" else "(Existing DC)",
                    'es': "(Pending Translation)" if action == "CREATE" else "(Existing DC)",
                    'action': action,
                    'dc_id': dc_id,
                    'is_option': False,
                    'parent_id': None,
                    'tags': ",".join(item['tags']) if item['tags'] else ""
                })
                
            for opt in item['options']:
                if self._is_dc_string(opt['name']):
                    continue 
                
                complex_dc_key = f"{item['dc_name']}::{opt['name']}"
                sanitized_key = self._sanitize(complex_dc_key)
                
                opt_action = "CREATE"
                opt_dc_id = None
                
                if sanitized_key in existing_dc:
                    opt_action = "LINK"
                    opt_dc_id = existing_dc[sanitized_key]
                elif sanitized_key in seen_dc_names:
                    opt_action = "LINK"
                else:
                    seen_dc_names.add(sanitized_key)
                    
                self.work_items.append({
                    'id': opt['id'],
                    'type': 'option',
                    'context': 'Ticket',
                    'dc_name': sanitized_key,
                    'placeholder': f"{{{{dc.{sanitized_key}}}}}",
                    'pt': opt['name'],
                    'en': "(Pending Translation)" if opt_action == "CREATE" else "(Existing DC)",
                    'es': "(Pending Translation)" if opt_action == "CREATE" else "(Existing DC)",
                    'action': opt_action,
                    'dc_id': opt_dc_id,
                    'is_option': True,
                    'parent_id': item['id'],
                    'tags': opt.get('value', '')
                })

        return self.scan_stats

    # --- HELPER FUNCTIONS FOR SCANNING ---
    def _process_generic_obj(self, obj, obj_type, tags=[], options=[]):
        title = ""
        context = "Unknown"

        if obj_type == 'field':
            title = obj.get('title')
            context = "Ticket"
        elif obj_type == 'form':
            title = obj.get('display_name') or obj.get('name')
            context = "Ticket"
        elif obj_type in ['category', 'section', 'article']:
            title = obj.get('name') or obj.get('title')
            context = "Help Center"

        if not title:
            title = "Unknown"
        title = title.strip()

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
        
        pending_opts = []
        for o in options:
            if not self._is_dc_string(o['name']):
                pending_opts.append(o)
        
        if is_parent_dc and len(pending_opts) == 0:
            return None 

        return {
            'id': obj['id'],
            'type': obj_type,
            'title': title,
            'dc_name': dc_name,
            'is_parent_dc': is_parent_dc,
            'tags': tags,
            'options': options,
            'context': context
        }

    def _scan_fields(self, log_cb):
        log_cb.emit("Scanning Fields...")
        results = []
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_fields.json"
        
        while url and not self.stop_requested:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: 
                break
            data = resp.json()
            for f in data.get('ticket_fields', []):
                item = self._process_generic_obj(f, 'field', f.get('tags', []), f.get('custom_field_options', []))
                if item:
                    results.append(item)
            url = data.get('next_page')
        return results

    def _scan_forms(self, log_cb):
        log_cb.emit("Scanning Forms...")
        results = []
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_forms.json"
        
        while url and not self.stop_requested:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: 
                break
            data = resp.json()
            for f in data.get('ticket_forms', []):
                item = self._process_generic_obj(f, 'form')
                if item:
                    results.append(item)
            url = data.get('next_page')
        return results

    def _scan_categories(self, log_cb):
        log_cb.emit("Scanning Categories...")
        results = []
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/categories.json"
        
        while url and not self.stop_requested:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: 
                break
            data = resp.json()
            for i in data.get('categories', []):
                item = self._process_generic_obj(i, 'category')
                if item:
                    results.append(item)
            url = data.get('next_page')
        return results

    def _scan_sections(self, log_cb):
        log_cb.emit("Scanning Sections...")
        results = []
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/sections.json"
        
        while url and not self.stop_requested:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: 
                break
            data = resp.json()
            for i in data.get('sections', []):
                item = self._process_generic_obj(i, 'section')
                if item:
                    results.append(item)
            url = data.get('next_page')
        return results

    def _scan_articles(self, log_cb):
        log_cb.emit("Scanning Articles...")
        results = []
        url = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/articles.json"
        
        while url and not self.stop_requested:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: 
                break
            data = resp.json()
            for i in data.get('articles', []):
                item = self._process_generic_obj(i, 'article')
                if item:
                    results.append(item)
            url = data.get('next_page')
        return results

    # --- TRANSLATE ---
    def set_translation_config(self, provider, api_key):
        self.trans_provider = provider
        self.trans_api_key = api_key

    def perform_translation(self, progress_callback, log_callback):
        self.reset_stop()
        
        if "Google Cloud" in self.trans_provider and not self.trans_api_key:
             raise Exception("Missing Google Cloud API Key. Operation aborted.")

        # --- FIXED: Allow both CREATE and LINK items to be translated ---
        to_translate = []
        for i in self.work_items:
            if i['action'] in ['CREATE', 'LINK']:
                to_translate.append(i)
                
        total = len(to_translate)
        log_callback.emit(f"Translating {total} items...")
        
        if total == 0:
            return
            
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_map = {
                executor.submit(self._fetch_trans, item, log_callback): item 
                for item in to_translate
            }
            
            count = 0
            for future in concurrent.futures.as_completed(future_map):
                if self.stop_requested:
                    for f in future_map:
                        f.cancel()
                    raise Exception("Operation Canceled")
                
                item = future_map[future]
                count += 1
                try:
                    en, es = future.result()
                    item['en'] = en
                    item['es'] = es
                    
                    eta = self._calc_eta(start_time, count, total)
                    status_str = f"Processed: {count}/{total} | Rem: {total - count} | ETA: {eta}"
                    progress_callback.emit(count, total, status_str)
                    
                except Exception as e:
                    log_callback.emit(f"Translation Error for {item['dc_name']}: {e}")
                    progress_callback.emit(count, total, "Error on item")

    def _fetch_trans(self, item, log_callback):
        text = item['pt']
        en = self._trans(text, 'en')
        es = self._trans(text, 'es')
        return en, es

    def _trans(self, text, target):
        if not text:
            return ""
        
        key = f"{text}|{target}"
        if key in self.translation_cache:
            return self.translation_cache[key]
        
        if self.trans_provider == "Google Cloud Translation API":
            if not self.trans_api_key:
                raise Exception("API Key is missing.")
            
            url = "https://translation.googleapis.com/language/translate/v2"
            params = {
                "q": text,
                "target": target,
                "key": self.trans_api_key,
                "format": "text"
            }
            
            resp = self.session.post(url, params=params, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                try:
                    res = data['data']['translations'][0]['translatedText']
                    res = html.unescape(res)
                    self.translation_cache[key] = res
                    return res
                except (KeyError, IndexError):
                    raise Exception(f"Unexpected API response format: {data}")
            else:
                raise Exception(f"Google API Error ({resp.status_code}): {resp.text}")

        else:
            # --- GOOGLE WEB FALLBACK ---
            for _ in range(2):
                time.sleep(random.uniform(0.3, 0.8)) 
                params = {
                    "client": "gtx", 
                    "sl": "auto", 
                    "tl": target, 
                    "dt": "t", 
                    "q": text
                }
                try:
                    resp = requests.get(
                        "https://translate.googleapis.com/translate_a/single", 
                        params=params, 
                        timeout=5
                    )
                    
                    if resp.status_code == 200:
                        res = "".join([x[0] for x in resp.json()[0]])
                        self.translation_cache[key] = res
                        return res
                    
                    if resp.status_code == 429:
                        time.sleep(2)
                except:
                    pass
        
        return f"[{target}] {text}"

    # --- EXECUTE ---
    def generate_backup_file(self, items_to_process, log_callback):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{timestamp}.json"
        
        backup_dir = self.backup_folder
        
        if not backup_dir or not os.path.exists(backup_dir) or not os.access(backup_dir, os.W_OK):
            backup_dir = os.path.expanduser("~/Documents")
            
        full_path = os.path.join(backup_dir, backup_filename)
        
        backup_data = {
            "timestamp": timestamp,
            "items": []
        }
        
        for item in items_to_process:
            backup_data["items"].append({
                "id": item['id'],
                "type": item['type'],
                "context": item.get('context', 'Unknown'),
                "original_text": item['pt'],
                "en": item.get('en', ''),
                "es": item.get('es', ''),
                "placeholder": item.get('placeholder', ''),
                "parent_id": item['parent_id'],
                "dc_name": item['dc_name']
            })
            
        try:
            with open(full_path, 'w') as f:
                json.dump(backup_data, f, indent=4)
            log_callback.emit(f"Backup created: {full_path}")
            return os.path.basename(full_path)
        except Exception as e:
            log_callback.emit(f"Failed to create backup: {e}")
            return None

    def execute_changes(self, items_to_process, progress_callback, log_callback):
        self.reset_stop()
        log_callback.emit("Creating Backup...")
        
        created_backup = self.generate_backup_file(items_to_process, log_callback)
        self.last_execution_results = {
            'success': [], 
            'failed': [], 
            'backup_file': created_backup
        }
        
        log_callback.emit(f"Processing {len(items_to_process)} items...")
        start_time = time.time()
        
        try: 
            url_loc = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/locales.json"
            locs = self.session.get(url_loc, timeout=10).json()['locales']
            l_map = {l['locale']: l['id'] for l in locs}
        except: 
            l_map = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._apply_single, item, l_map): item 
                for item in items_to_process
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                if self.stop_requested:
                    for f in futures:
                        f.cancel()
                    raise Exception("Operation Canceled")
                
                item = futures[future]
                try: 
                    res = future.result()
                    log_callback.emit(res)
                    self.last_execution_results['success'].append(res)
                    
                    eta = self._calc_eta(start_time, i+1, len(items_to_process))
                    status_str = f"Processed: {i+1}/{len(items_to_process)} | Rem: {len(items_to_process) - (i+1)} | ETA: {eta}"
                    progress_callback.emit(i+1, len(items_to_process), status_str)
                    
                except Exception as e: 
                    log_callback.emit(f"ERROR: {str(e)}")
                    self.last_execution_results['failed'].append(str(e))
        
        return self.last_execution_results

    def _apply_single(self, item, l_map):
        dc_id = item.get('dc_id')
        
        pt_id = l_map.get('pt-BR', 1)
        en_id = l_map.get('en-US', 1)
        es_id = l_map.get('es', 2)
        
        en_content = item.get('en', '')
        if not en_content or en_content == "(Pending Translation)":
            en_content = item['pt']
            
        es_content = item.get('es', '')
        if not es_content or es_content == "(Pending Translation)":
            es_content = item['pt']
        
        vars = [
            {'locale_id': pt_id, 'default': True, 'content': item['pt']}, 
            {'locale_id': en_id, 'default': False, 'content': en_content}, 
            {'locale_id': es_id, 'default': False, 'content': es_content}
        ]
        
        if item.get('force_update') and dc_id:
            self.session.put(
                f"https://{self.creds['subdomain']}.zendesk.com/api/v2/dynamic_content/items/{dc_id}.json", 
                json={"item": {"variants": vars}}, 
                timeout=20
            ).raise_for_status()
            
        elif not dc_id and item['action'] == 'CREATE':
            payload = {
                "item": {
                    "name": item['dc_name'], 
                    "default_locale_id": pt_id, 
                    "variants": vars
                }
            }
            resp = self.session.post(
                f"https://{self.creds['subdomain']}.zendesk.com/api/v2/dynamic_content/items.json", 
                json=payload, 
                timeout=20
            )
            
            if resp.status_code == 201:
                dc_id = resp.json()['item']['id']
            elif resp.status_code != 422:
                raise Exception(f"DC Create Failed: {resp.status_code}")

        ph = item['placeholder']
        
        if item['type'] == 'field': 
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_fields/{item['id']}.json"
            self.session.put(u, json={"ticket_field": {"title": ph}}, timeout=20).raise_for_status()
            return f"SUCCESS: Field {item['dc_name']}"
        
        elif item['type'] == 'form':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_forms/{item['id']}.json"
            self.session.put(u, json={"ticket_form": {"display_name": ph}}, timeout=20).raise_for_status()
            return f"SUCCESS: Form {item['dc_name']}"
        
        elif item['type'] == 'category':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/categories/{item['id']}.json"
            self.session.put(u, json={"category": {"name": ph}}, timeout=20).raise_for_status()
            return f"SUCCESS: Category {item['dc_name']}"
        
        elif item['type'] == 'section':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/sections/{item['id']}.json"
            self.session.put(u, json={"section": {"name": ph}}, timeout=20).raise_for_status()
            return f"SUCCESS: Section {item['dc_name']}"
        
        elif item['type'] == 'article':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/articles/{item['id']}.json"
            self.session.put(u, json={"article": {"title": ph}}, timeout=20).raise_for_status()
            return f"SUCCESS: Article {item['dc_name']}"
        
        elif item['type'] == 'option':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_fields/{item['parent_id']}.json"
            fresh = self.session.get(u, timeout=20).json()['ticket_field']['custom_field_options']
            found = False
            for o in fresh:
                if str(o['id']) == str(item['id']):
                    o['name'] = ph
                    found = True
                    break
            
            if found:
                self.session.put(u, json={"ticket_field": {"custom_field_options": fresh}}, timeout=20).raise_for_status()
                return f"SUCCESS: Option {item['dc_name']}"
            else:
                return "SKIPPED: Option not found"
        
        return "SKIPPED"

    # --- ROLLBACK & BACKUP LOADING ---
    def load_backup_thread(self, progress_callback, log_callback, filepath):
        progress_callback.emit(0, 0, "Reading file...")
        try:
            with open(filepath, 'r') as file:
                data = json.load(file)
            items = data.get('items', [])
            log_callback.emit(f"Backup loaded: {len(items)} items found.")
            return items
        except Exception as e:
            raise Exception(f"Failed to load backup: {e}")

    def perform_restore_from_data(self, items, progress_callback, log_callback):
        self.reset_stop()
        total = len(items)
        log_callback.emit(f"Restoring {total} items from backup...")
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._restore_single, item): item 
                for item in items
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                if self.stop_requested:
                    for f in futures:
                        f.cancel()
                    raise Exception("Operation Canceled")
                
                try:
                    res = future.result()
                    log_callback.emit(res)
                    
                    eta = self._calc_eta(start_time, i+1, total)
                    status_str = f"Restored: {i+1}/{total} | Rem: {total - (i+1)} | ETA: {eta}"
                    progress_callback.emit(i+1, total, status_str)
                    
                except Exception as e:
                    log_callback.emit(f"Error restoring item: {e}")

        return "Restore from Backup Complete."

    def _restore_single(self, item):
        orig = str(item['original_text'])
        
        if item['type'] == 'field':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_fields/{item['id']}.json"
            self.session.put(u, json={"ticket_field": {"title": orig}}, timeout=20).raise_for_status()
            return f"Restored Field: {orig}"
            
        elif item['type'] == 'form':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_forms/{item['id']}.json"
            self.session.put(u, json={"ticket_form": {"display_name": orig}}, timeout=20).raise_for_status()
            return f"Restored Form: {orig}"
        
        elif item['type'] == 'category':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/categories/{item['id']}.json"
            self.session.put(u, json={"category": {"name": orig}}, timeout=20).raise_for_status()
            return f"Restored Category: {orig}"

        elif item['type'] == 'section':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/sections/{item['id']}.json"
            self.session.put(u, json={"section": {"name": orig}}, timeout=20).raise_for_status()
            return f"Restored Section: {orig}"
        
        elif item['type'] == 'article':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/help_center/articles/{item['id']}.json"
            self.session.put(u, json={"article": {"title": orig}}, timeout=20).raise_for_status()
            return f"Restored Article: {orig}"

        elif item['type'] == 'option':
            u = f"https://{self.creds['subdomain']}.zendesk.com/api/v2/ticket_fields/{item['parent_id']}.json"
            resp = self.session.get(u, timeout=20)
            if resp.status_code == 200:
                opts = resp.json()['ticket_field']['custom_field_options']
                found = False
                for o in opts:
                    if str(o['id']) == str(item['id']):
                        o['name'] = orig
                        found = True
                        break
                if found:
                    self.session.put(u, json={"ticket_field": {"custom_field_options": opts}}, timeout=20).raise_for_status()
                    return f"Restored Option: {orig}"
        
        return f"Skipped/Failed: {item['id']}"


# ==============================================================================
# UI COMPONENTS
# ==============================================================================
class StepWorker(QThread):
    progress = pyqtSignal(int, int, str) 
    log = pyqtSignal(str)
    result = pyqtSignal(bool, object)
    
    def __init__(self, func, *args):
        super().__init__()
        self.func = func
        self.args = args
        
    def run(self):
        try:
            res = self.func(self.progress, self.log, *self.args)
            self.result.emit(True, res)
        except Exception as e:
            self.result.emit(False, str(e))


class ModernSidebar(QFrame):
    def __init__(self, parent_wiz):
        super().__init__()
        self.setObjectName("Sidebar")
        self.parent_wiz = parent_wiz
        self.setFixedWidth(240)
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(15, 30, 15, 30)
        self.layout.setSpacing(8)
        self.btns = []
        
        labels = ["Connect", "Scan Data", "Translate", "Preview", "Apply", "Rollback"]
        for i, txt in enumerate(labels):
            btn = QPushButton(f"{i+1}. {txt}")
            btn.setObjectName("StepBtn")
            btn.setCheckable(True)
            
            if i > 0:
                btn.setEnabled(False)
            
            btn.clicked.connect(lambda _, x=i: self.parent_wiz.goto(x))
            self.layout.addWidget(btn)
            self.btns.append(btn)
            
        self.layout.addStretch()
        
    def set_active(self, index):
        for i, btn in enumerate(self.btns):
            btn.setChecked(i == index)
            
    def unlock(self, index):
        if index < len(self.btns):
            self.btns[index].setEnabled(True)
            
    def set_locked(self, locked):
        for btn in self.btns:
            btn.setDisabled(locked)
        self.setEnabled(not locked)


class WizardCard(QFrame):
    def __init__(self, title, subtitle):
        super().__init__()
        self.setObjectName("Card")
        
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(25)
        shadow.setColor(QColor(0, 0, 0, 10))
        shadow.setOffset(0, 4)
        self.setGraphicsEffect(shadow)
        
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(40, 40, 40, 40)
        
        t = QLabel(title)
        t.setObjectName("Title")
        self.layout.addWidget(t)
        
        s = QLabel(subtitle)
        s.setObjectName("Subtitle")
        s.setWordWrap(True)
        self.layout.addWidget(s)
        
        self.content = QVBoxLayout()
        self.layout.addLayout(self.content)
        self.layout.addStretch()
        
    def add_widget(self, w):
        self.content.addWidget(w)
        
    def add_layout(self, l):
        self.content.addLayout(l)


class WizardCardFull(QFrame):
    def __init__(self, title, subtitle):
        super().__init__()
        self.setObjectName("CardFull")
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(10, 15, 10, 10)
        
        t = QLabel(title)
        t.setObjectName("Title")
        self.layout.addWidget(t)
        
        s = QLabel(subtitle)
        s.setObjectName("Subtitle")
        self.layout.addWidget(s)
        
        self.content = QVBoxLayout()
        self.layout.addLayout(self.content)
        
    def add_widget(self, w):
        self.content.addWidget(w)
        
    def add_layout(self, l):
        self.content.addLayout(l)


class EmbeddedStatusBar(QFrame):
    def __init__(self):
        super().__init__()
        self.setObjectName("StatusBar")
        self.setFixedHeight(65)
        self.setVisible(False)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(20, 0, 20, 0)
        
        self.status_lbl = QLabel("Ready")
        self.status_lbl.setObjectName("StatusText")
        
        self.stats_lbl = QLabel("")
        self.stats_lbl.setObjectName("StatsText")
        self.stats_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        
        self.p_bar = QProgressBar()
        self.p_bar.setFixedWidth(200)
        self.p_bar.setTextVisible(False)
        
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setObjectName("DangerBtn")
        self.btn_stop.setFixedSize(80, 36)
        self.btn_stop.setEnabled(False)
        
        layout.addWidget(self.status_lbl)
        layout.addStretch()
        layout.addWidget(self.stats_lbl)
        layout.addWidget(self.p_bar)
        layout.addWidget(self.btn_stop)
        
    def show_progress(self, current, total, msg_main, msg_detail):
        self.setVisible(True)
        self.status_lbl.setText(msg_main)
        self.stats_lbl.setText(msg_detail)
        self.btn_stop.setEnabled(True)
        
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

    def finish(self, message, success=True):
        self.status_lbl.setText(message)
        self.stats_lbl.setText("")
        self.btn_stop.setEnabled(False)
        
        self.p_bar.setRange(0, 100)
        
        if success:
            self.p_bar.setValue(100)
        else:
            self.p_bar.setValue(0)
             
        if "Updated" in message:
            self.p_bar.setValue(0)


# ==============================================================================
# MAIN WINDOW
# ==============================================================================
class ZendeskWizard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Zendesk Dynamic Content Manager")
        self.resize(1200, 800)
        self.controller = ZendeskController()
        self.backup_candidates = [] 
        self.worker = None
        self.is_busy = False # LOGIC GATE TO PREVENT DOUBLE CLICKS
        
        central = QWidget()
        self.setCentralWidget(central)
        root = QHBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        
        self.sidebar = ModernSidebar(self)
        root.addWidget(self.sidebar)
        
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.setHandleWidth(1)
        root.addWidget(self.splitter)
        
        self.top_pane = QWidget()
        self.top_pane.setStyleSheet("background-color: #f3f4f6;")
        
        top_layout = QVBoxLayout(self.top_pane)
        top_layout.setContentsMargins(30, 30, 30, 15)
        top_layout.setSpacing(10)
        
        self.stack = QStackedWidget()
        top_layout.addWidget(self.stack)
        
        self.status_bar = EmbeddedStatusBar()
        self.status_bar.btn_stop.clicked.connect(self.stop_process)
        top_layout.addWidget(self.status_bar)
        
        self.splitter.addWidget(self.top_pane)

        self.log_pane = QWidget()
        self.log_pane.setStyleSheet("background-color: #f3f4f6;")
        
        log_layout = QVBoxLayout(self.log_pane)
        log_layout.setContentsMargins(20, 20, 20, 20)
        
        lbl_log = QLabel("Activity Log")
        lbl_log.setObjectName("LogTitle")
        log_layout.addWidget(lbl_log)
        
        self.log_frame = QFrame()
        self.log_frame.setObjectName("LogFrame")
        
        inner_layout = QVBoxLayout(self.log_frame)
        inner_layout.setContentsMargins(0, 0, 0, 0)
        
        self.console = QTextEdit()
        self.console.setObjectName("LogBox")
        self.console.setReadOnly(True)
        self.console.setStyleSheet("background-color: #111827; color: #10B981; font-family: 'Courier New', monospace; font-size: 12px; border: none;")
        
        inner_layout.addWidget(self.console)
        log_layout.addWidget(self.log_frame)
        
        self.splitter.addWidget(self.log_pane)
        self.splitter.setSizes([500, 300])

        self.init_pages()

    def log_msg(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        self.console.append(f"[{ts}] {msg}")
        self.console.verticalScrollBar().setValue(self.console.verticalScrollBar().maximum())

    def goto(self, idx):
        if self.sidebar.btns[idx].isEnabled():
            self.stack.setCurrentIndex(idx)
            self.sidebar.set_active(idx)
        else:
            self.log_msg(f"Step {idx+1} is strictly locked. Complete previous steps first.")

    def lock_ui(self, locked):
        self.sidebar.set_locked(locked)
        
        # Explicitly disable action buttons
        self.btn_connect.setEnabled(not locked)
        self.btn_scan.setEnabled(not locked)
        self.btn_trans.setEnabled(not locked)
        self.btn_preview.setEnabled(not locked)
        self.btn_apply.setEnabled(not locked)
        self.btn_load_backup.setEnabled(not locked)
        
        if locked:
            self.btn_execute_rollback.setEnabled(False)
        else:
            # Only enable execute if data is loaded
            self.btn_execute_rollback.setEnabled(len(self.backup_candidates) > 0)

    def stop_process(self):
        self.controller.stop()
        self.log_msg("Stopping process...")
        self.status_bar.status_lbl.setText("Stopping...")

    def on_connect_finished(self, success, msg):
        self.is_busy = False # Reset busy flag
        self.status_bar.finish("Ready") 
        self.lock_ui(False)
        
        if success:
            self.log_msg(f"[SUCCESS] {msg}")
            QMessageBox.information(self, "Success", str(msg))
            self.sidebar.unlock(1)
            self.sidebar.unlock(5)
            self.goto(1)
        else:
            self.log_msg(f"[ERROR] {msg}")
            QMessageBox.critical(self, "Error", str(msg))

    def init_pages(self):
        # 1. Connect
        p1 = WizardCard("Connect Instance", "Enter your Zendesk API credentials.")
        self.in_sub = QLineEdit()
        self.in_sub.setPlaceholderText("Subdomain")
        self.in_email = QLineEdit()
        self.in_email.setPlaceholderText("Email")
        self.in_tok = QLineEdit()
        self.in_tok.setEchoMode(QLineEdit.EchoMode.Password)
        
        self.in_rollback = QLineEdit()
        
        current_dir = os.getcwd()
        self.in_rollback.setText(current_dir)
        self.in_rollback.setPlaceholderText("Backup Folder Path")
        
        h_creds = QHBoxLayout()
        btn_save = QPushButton("Save Profile")
        btn_save.setObjectName("SecondaryBtn")
        btn_save.clicked.connect(self.save_creds)
        
        btn_load = QPushButton("Load Profile")
        btn_load.setObjectName("SecondaryBtn")
        btn_load.clicked.connect(self.load_creds)
        
        h_creds.addWidget(btn_save)
        h_creds.addWidget(btn_load)
        h_creds.addStretch()
        
        self.btn_connect = QPushButton("Connect")
        self.btn_connect.setObjectName("PrimaryBtn")
        self.btn_connect.clicked.connect(self.run_connect)
        
        f = QVBoxLayout()
        f.setSpacing(15)
        f.addWidget(QLabel("Subdomain"))
        f.addWidget(self.in_sub)
        f.addWidget(QLabel("Email"))
        f.addWidget(self.in_email)
        f.addWidget(QLabel("API Token"))
        f.addWidget(self.in_tok)
        f.addWidget(QLabel("Backup Folder Path"))
        f.addWidget(self.in_rollback)
        
        p1.add_layout(f)
        p1.add_layout(h_creds)
        p1.add_widget(self.btn_connect)
        self.stack.addWidget(p1)

        # 2. Scan
        p2 = WizardCard("Scan Data", "Select what you want to scan and translate.")
        self.scan_summary_box = QTextEdit()
        self.scan_summary_box.setReadOnly(True)
        self.scan_summary_box.setFixedHeight(300)
        self.scan_summary_box.setObjectName("InfoBox")
        
        self.chk_scan_fields = QCheckBox("Ticket Fields (Titles && Options)")
        self.chk_scan_fields.setChecked(True)
        self.chk_scan_forms = QCheckBox("Ticket Forms (Names)")
        self.chk_scan_forms.setChecked(True)
        self.chk_scan_cats = QCheckBox("Help Center: Categories")
        self.chk_scan_cats.setChecked(False)
        self.chk_scan_sects = QCheckBox("Help Center: Sections")
        self.chk_scan_sects.setChecked(False)
        self.chk_scan_arts = QCheckBox("Help Center: Articles (Titles Only)")
        self.chk_scan_arts.setChecked(False)
        
        chk_layout = QVBoxLayout()
        chk_layout.addWidget(self.chk_scan_fields)
        chk_layout.addWidget(self.chk_scan_forms)
        chk_layout.addWidget(self.chk_scan_cats)
        chk_layout.addWidget(self.chk_scan_sects)
        chk_layout.addWidget(self.chk_scan_arts)
        
        self.btn_scan = QPushButton("Start Scan")
        self.btn_scan.setObjectName("PrimaryBtn")
        self.btn_scan.clicked.connect(self.run_scan)
        
        p2.add_layout(chk_layout)
        p2.add_widget(self.scan_summary_box)
        p2.add_widget(self.btn_scan)
        
        self.stack.addWidget(p2)

        # 3. Translate
        p3 = WizardCard("Translate", "Choose your translation engine.")
        form = QVBoxLayout()
        form.addWidget(QLabel("Translation Provider:"))
        self.combo_provider = QComboBox()
        self.combo_provider.addItems(["Google Web (Free)", "Google Cloud Translation API"])
        form.addWidget(self.combo_provider)
        
        form.addWidget(QLabel("API Key (Optional for Web, Required for Cloud):"))
        self.in_api_key = QLineEdit()
        self.in_api_key.setPlaceholderText("Enter API Key here...")
        self.in_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        form.addWidget(self.in_api_key)
        
        self.btn_trans = QPushButton("Run Translation")
        self.btn_trans.setObjectName("PrimaryBtn")
        self.btn_trans.clicked.connect(self.run_trans)
        
        p3.add_layout(form)
        p3.add_widget(self.btn_trans)
        self.stack.addWidget(p3)

        # 4. Preview
        p4 = WizardCardFull("Review Plan", "Review data before applying.")
        self.sum_box = QFrame()
        self.sum_box.setObjectName("CompactSummary")
        
        sum_layout = QHBoxLayout(self.sum_box)
        sum_layout.setContentsMargins(10, 5, 10, 5)
        
        self.lbl_sum_create = QLabel("Create: 0")
        self.lbl_sum_create.setObjectName("CompactLabel")
        self.lbl_sum_link = QLabel("Link: 0")
        self.lbl_sum_link.setObjectName("CompactLabel")
        self.lbl_sum_time = QLabel("Time: 0s")
        self.lbl_sum_time.setObjectName("CompactLabel")
        
        sum_layout.addWidget(self.lbl_sum_create)
        sum_layout.addWidget(QLabel("|"))
        sum_layout.addWidget(self.lbl_sum_link)
        sum_layout.addWidget(QLabel("|"))
        sum_layout.addWidget(self.lbl_sum_time)
        sum_layout.addStretch()
        
        filter_bar = QHBoxLayout()
        lbl_filter = QLabel("Filter View:")
        lbl_filter.setObjectName("FilterLabel")
        filter_bar.addWidget(lbl_filter)
        
        self.chk_filter_ticket = QCheckBox("Ticket")
        self.chk_filter_ticket.setChecked(True)
        self.chk_filter_ticket.stateChanged.connect(self.apply_table_filter)
        
        self.chk_filter_hc = QCheckBox("Help Center")
        self.chk_filter_hc.setChecked(True)
        self.chk_filter_hc.stateChanged.connect(self.apply_table_filter)

        filter_bar.addWidget(self.chk_filter_ticket)
        filter_bar.addWidget(self.chk_filter_hc)
        filter_bar.addStretch()

        cols = ["Action", "Update DC?", "Context", "Type", "Name", "Placeholder", "PT *", "EN", "ES"]
        self.table = QTableWidget(0, len(cols))
        self.table.setHorizontalHeaderLabels(cols)
        h = self.table.horizontalHeader()
        for i in [0, 1, 2, 3]:
            h.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        for i in [4, 5, 6, 7, 8]:
            h.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        
        self.btn_preview = QPushButton("Generate Preview")
        self.btn_preview.setObjectName("PrimaryBtn")
        self.btn_preview.clicked.connect(self.populate_preview)
        
        lbl_note = QLabel("* Original text from Zendesk")
        lbl_note.setObjectName("NoteText")
        
        p4.add_widget(self.sum_box)
        p4.add_layout(filter_bar)
        p4.add_widget(self.table)
        p4.add_widget(lbl_note)
        p4.add_widget(self.btn_preview)
        self.stack.addWidget(p4)

        # 5. Apply
        p5 = WizardCardFull("Execute", "Apply changes to Zendesk.")
        self.lbl_apply_summary = QLabel("No plan generated yet. Go to Step 4.")
        self.lbl_apply_summary.setObjectName("SummaryText")
        h_opts = QHBoxLayout()
        self.chk_create = QCheckBox("Create New Dynamic Content")
        self.chk_create.setChecked(True)
        self.chk_link = QCheckBox("Link Existing Dynamic Content")
        self.chk_link.setChecked(True)
        h_opts.addWidget(self.chk_create)
        h_opts.addWidget(self.chk_link)
        h_opts.addStretch()
        self.result_box = QTextEdit()
        self.result_box.setObjectName("InfoBox")
        self.result_box.setReadOnly(True)
        self.result_box.setFixedHeight(300) 
        self.btn_apply = QPushButton("Apply Changes")
        self.btn_apply.setObjectName("DangerBtn")
        self.btn_apply.clicked.connect(self.run_apply)
        p5.add_widget(self.lbl_apply_summary)
        p5.add_layout(h_opts)
        p5.add_widget(self.result_box)
        p5.add_widget(self.btn_apply)
        p5.content.addStretch()
        self.stack.addWidget(p5)

        # 6. Rollback
        p6 = WizardCardFull("Rollback", "Restore original text using a backup file.")
        
        self.btn_load_backup = QPushButton("Load Backup File")
        self.btn_load_backup.setObjectName("PrimaryBtn")
        self.btn_load_backup.clicked.connect(self.load_backup_file)
        
        cols = ["Context", "Type", "Name", "Original Text (Restoring)", "Placeholder", "EN (Ref)", "ES (Ref)"]
        self.backup_table = QTableWidget(0, len(cols))
        self.backup_table.setHorizontalHeaderLabels(cols)
        h = self.backup_table.horizontalHeader()
        for i in [0, 1]:
            h.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        for i in [2, 3, 4, 5, 6]:
            h.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
        
        self.btn_execute_rollback = QPushButton("Execute Rollback")
        self.btn_execute_rollback.setObjectName("DangerBtn")
        self.btn_execute_rollback.setEnabled(False)
        self.btn_execute_rollback.clicked.connect(self.run_rollback)
        
        p6.add_widget(QLabel("Select a backup file generated during the Apply step."))
        p6.add_widget(self.btn_load_backup)
        p6.add_widget(self.backup_table)
        p6.add_widget(self.btn_execute_rollback)
        
        self.stack.addWidget(p6)

    # ACTIONS
    def save_creds(self):
        f, _ = QFileDialog.getSaveFileName(self, "Save", "", "JSON (*.json)", options=QFileDialog.Option.DontUseNativeDialog)
        if f: 
            self.controller.save_profile(f, self.in_sub.text(), self.in_email.text(), self.in_tok.text(), self.in_rollback.text(), self.in_api_key.text())

    def load_creds(self):
        f, _ = QFileDialog.getOpenFileName(self, "Load", "", "JSON (*.json)", options=QFileDialog.Option.DontUseNativeDialog)
        if f:
            d = self.controller.load_profile(f)
            if d: 
                self.in_sub.setText(d.get('subdomain', ''))
                self.in_email.setText(d.get('email', ''))
                self.in_tok.setText(d.get('token', ''))
                self.in_rollback.setText(d.get('backup_path', ''))
                self.in_api_key.setText(d.get('google_api_key', ''))

    def run_connect(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        self.is_busy = True
        
        self.status_bar.show_progress(0, 0, "Connecting...", "")
        self.lock_ui(True)
        self.worker = StepWorker(lambda p, l: self.controller.connect(self.in_sub.text(), self.in_email.text(), self.in_tok.text(), self.in_rollback.text(), l))
        self.worker.log.connect(self.log_msg)
        self.worker.result.connect(self.on_connect_finished)
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def run_scan(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        
        config = {
            'fields': self.chk_scan_fields.isChecked(),
            'forms': self.chk_scan_forms.isChecked(),
            'cats': self.chk_scan_cats.isChecked(),
            'sects': self.chk_scan_sects.isChecked(),
            'arts': self.chk_scan_arts.isChecked()
        }
        
        if not any(config.values()):
            QMessageBox.warning(self, "Warning", "Please select at least one item to scan.")
            return

        self.is_busy = True # Set busy only after validation passes
        self.status_bar.show_progress(0, 0, "Scanning...", "Initializing...")
        self.lock_ui(True)
        self.worker = StepWorker(lambda p, l: self.controller.scan_and_analyze(p, l, config))
        self.worker.progress.connect(lambda c, t, m: self.status_bar.show_progress(c, t, "Scanning...", m)) 
        self.worker.log.connect(self.log_msg)
        self.worker.result.connect(self.on_scan_finished)
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def on_scan_finished(self, success, result):
        self.is_busy = False # Reset busy flag
        self.lock_ui(False)
        if not success: 
            if "Canceled" in str(result):
                self.status_bar.finish("Operation Canceled", False)
                self.log_msg("[INFO] Scan canceled by user.")
            else:
                self.status_bar.finish("Scan Failed", False)
                QMessageBox.critical(self, "Error", str(result))
            return
            
        self.status_bar.finish("Scan Complete", True)
        
        stats = result
        
        report = f"""
        <h3 style="margin-top:0;">Scan Complete</h3>
        <table width="100%" cellpadding="4" cellspacing="0" style="color: #134E4A;">
            <tr>
                <td><b>Ticket Fields:</b></td>
                <td align="left">{stats['valid_fields']}</td>
            </tr>
            <tr>
                <td><b>Ticket Forms:</b></td>
                <td align="left">{stats['valid_forms']}</td>
            </tr>
            <tr>
                <td><b>Help Center Items:</b></td>
                <td align="left">{stats['valid_cats'] + stats['valid_sects'] + stats['valid_arts']}</td>
            </tr>
            <tr>
                <td colspan="2"><hr style="border: 1px solid #2DD4BF;"></td>
            </tr>
            <tr>
                <td><i>Ignored (System/Hidden):</i></td>
                <td align="left">{stats['ignored']}</td>
            </tr>
            <tr>
                <td><i>Already Dynamic Content:</i></td>
                <td align="left">{stats['already_dc']}</td>
            </tr>
            <tr>
                <td colspan="2"><hr style="border: 1px solid #2DD4BF;"></td>
            </tr>
            <tr>
                <td><b>Total Tasks Generated:</b></td>
                <td align="left" style="font-size: 14px;"><b>{len(self.controller.work_items)}</b></td>
            </tr>
        </table>
        """
        self.scan_summary_box.setHtml(report)
        
        self.sidebar.unlock(2)
        self.sidebar.unlock(3)
        self.populate_preview()
        self.goto(2) 

    def populate_preview(self):
        self.table.setRowCount(0)
        rows = self.controller.work_items
        self.table.setRowCount(len(rows))
        
        for r, item in enumerate(rows):
            self.table.setItem(r, 0, QTableWidgetItem(item['action']))
            
            chk_widget = QWidget()
            chk_layout = QHBoxLayout(chk_widget)
            chk_layout.setContentsMargins(0, 0, 0, 0)
            chk_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            chk = QCheckBox()
            is_link = item['action'] == 'LINK' and bool(item.get('dc_id'))
            chk.setChecked(is_link)
            chk.setEnabled(is_link)
            chk_layout.addWidget(chk)
            self.table.setCellWidget(r, 1, chk_widget)

            self.table.setItem(r, 2, QTableWidgetItem(item.get('context', 'Unknown')))
            self.table.setItem(r, 3, QTableWidgetItem(item['type']))
            self.table.setItem(r, 4, QTableWidgetItem(item['dc_name']))
            self.table.setItem(r, 5, QTableWidgetItem(item['placeholder']))
            self.table.setItem(r, 6, QTableWidgetItem(item['pt']))
            self.table.setItem(r, 7, QTableWidgetItem(item['en']))
            self.table.setItem(r, 8, QTableWidgetItem(item['es']))
        
        c = len([x for x in rows if x['action'] == 'CREATE'])
        l = len([x for x in rows if x['action'] == 'LINK'])
        self.lbl_sum_create.setText(f"Create: {c}")
        self.lbl_sum_link.setText(f"Link: {l}")
        self.lbl_sum_time.setText(f"Time: {(c * 1.2) / 60:.1f}m")
        self.lbl_apply_summary.setText(f"Ready to apply {len(rows)} changes.")
        
        self.apply_table_filter()
        self.status_bar.finish("Preview Updated", True)

    def apply_table_filter(self):
        show_ticket = self.chk_filter_ticket.isChecked()
        show_hc = self.chk_filter_hc.isChecked()
        
        for r in range(self.table.rowCount()):
            ctx = self.table.item(r, 2).text() if self.table.item(r, 2) else ""
            
            visible = False
            if ctx == "Ticket" and show_ticket:
                visible = True
            elif ctx == "Help Center" and show_hc:
                visible = True
            elif ctx == "Unknown":
                visible = True
                
            self.table.setRowHidden(r, not visible)

    def run_trans(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        
        provider = self.combo_provider.currentText()
        key = self.in_api_key.text().strip()
        
        if "Google Cloud" in provider and not key:
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Icon.Warning)
            msg.setWindowTitle("Missing API Key")
            msg.setText("You selected 'Google Cloud Translation API' but provided no API Key.")
            msg.setInformativeText("How would you like to proceed?")
            
            btn_web = msg.addButton("Use Free Web Method", QMessageBox.ButtonRole.AcceptRole)
            btn_cancel = msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
            
            msg.exec()
            
            if msg.clickedButton() == btn_web:
                self.combo_provider.setCurrentIndex(0)
                provider = "Google Web (Free)"
            else:
                return

        self.is_busy = True
        self.status_bar.show_progress(0, 0, "Translating...", "Starting...")
        self.lock_ui(True)
        self.controller.set_translation_config(provider, key)
        self.worker = StepWorker(self.controller.perform_translation)
        self.worker.progress.connect(lambda c, t, m: self.status_bar.show_progress(c, t, "Translating...", m)) 
        self.worker.log.connect(self.log_msg)
        self.worker.result.connect(lambda s, m: self.finish_step(s, m if not s else "Translation Done.", 4)) 
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def run_apply(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        
        if QMessageBox.warning(self, "Confirm", "Proceed?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No: 
            return
            
        self.is_busy = True
        self.result_box.setText("Processing...")
        self.status_bar.show_progress(0, 0, "Applying...", "Starting...")
        self.lock_ui(True)
        
        items = []
        do_create = self.chk_create.isChecked()
        do_link = self.chk_link.isChecked()
        
        for r in range(self.table.rowCount()):
            if r >= len(self.controller.work_items):
                continue
                
            item = self.controller.work_items[r]
            
            chk_widget = self.table.cellWidget(r, 1)
            if chk_widget:
                chk_layout = chk_widget.layout()
                if chk_layout and chk_layout.count() > 0:
                    chk = chk_layout.itemAt(0).widget()
                    if isinstance(chk, QCheckBox):
                        item['force_update'] = chk.isChecked()
            
            is_create = item['action'] == 'CREATE'
            
            if (is_create and do_create):
                items.append(item)
            elif (not is_create and do_link):
                items.append(item)
        
        self.worker = StepWorker(lambda p, l: self.controller.execute_changes(items, p, l))
        self.worker.progress.connect(lambda c, t, m: self.status_bar.show_progress(c, t, "Applying...", m)) 
        self.worker.log.connect(self.log_msg)
        self.worker.result.connect(self.on_apply_finished)
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def on_apply_finished(self, success, results):
        self.is_busy = False # Reset busy flag
        self.lock_ui(False)
        if not success:
            if "Canceled" in str(results):
                self.status_bar.finish("Operation Canceled", False)
                self.log_msg("[INFO] Application canceled by user.")
                return
            
            self.status_bar.finish("Error", False)
            QMessageBox.critical(self, "Error", "Execution finished with errors.")
            return

        self.status_bar.finish("Done", True)
        s, f = results.get('success', []), results.get('failed', [])
        report = f"EXECUTION REPORT:\nTotal:   {len(s)+len(f)}\nSuccess: {len(s)}\nFailed:  {len(f)}"
        if f: 
            report += f"\n(Check Log for details)"
            
        backup_name = results.get('backup_file', 'Unknown')
        report += f"\n\nBackup Saved: {backup_name}"
        
        self.result_box.setText(report)
        
        QMessageBox.information(self, "Success", "Application Complete.")

    def run_cleanup_scan(self):
        self.log_msg("Switching to File Restore mode...")

    # ROLLBACK UI (MULTITHREADED LOADING)
    def load_backup_file(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        
        f, _ = QFileDialog.getOpenFileName(self, "Load Backup", "", "JSON (*.json)", options=QFileDialog.Option.DontUseNativeDialog)
        if not f: 
            return
        
        self.is_busy = True
        self.status_bar.show_progress(0, 0, "Loading Backup...", "Reading file...")
        self.lock_ui(True)
        
        # Correctly pass arguments (progress, log, filepath)
        self.worker = StepWorker(self.controller.load_backup_thread, f)
        self.worker.result.connect(self.on_backup_loaded)
        self.worker.log.connect(self.log_msg)
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def on_backup_loaded(self, success, result):
        self.is_busy = False # Reset busy flag
        self.lock_ui(False)
        
        if not success:
            self.status_bar.finish("Load Failed", False)
            QMessageBox.critical(self, "Error", str(result))
            return
            
        self.backup_candidates = result
        self.backup_table.setRowCount(len(self.backup_candidates))
        
        for r, item in enumerate(self.backup_candidates):
            self.backup_table.setItem(r, 0, QTableWidgetItem(item.get('context', 'Unknown')))
            self.backup_table.setItem(r, 1, QTableWidgetItem(item.get('type', 'unknown')))
            self.backup_table.setItem(r, 2, QTableWidgetItem(item.get('dc_name', '')))
            self.backup_table.setItem(r, 3, QTableWidgetItem(item.get('original_text', '')))
            self.backup_table.setItem(r, 4, QTableWidgetItem(item.get('placeholder', '')))
            self.backup_table.setItem(r, 5, QTableWidgetItem(item.get('en', '')))
            self.backup_table.setItem(r, 6, QTableWidgetItem(item.get('es', '')))
            
        self.btn_execute_rollback.setEnabled(True)
        self.status_bar.finish(f"Loaded {len(self.backup_candidates)} items", True)
        self.log_msg(f"Loaded {len(self.backup_candidates)} items from backup.")

    def run_rollback(self):
        # LOGIC GATE: Prevent double clicks
        if self.is_busy: return
        
        if QMessageBox.warning(self, "Confirm", "Restore fields and delete DC items?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.No: 
            return
        
        self.is_busy = True
        self.lock_ui(True)
        self.worker = StepWorker(lambda p, l: self.controller.perform_restore_from_data(self.backup_candidates, p, l))
        self.worker.progress.connect(lambda c, t, m: self.status_bar.show_progress(c, t, "Restoring...", m))
        self.worker.log.connect(self.log_msg)
        self.worker.result.connect(lambda s, m: self.finish_step(s, m if not s else "Rollback Finished", 0))
        self.worker.finished.connect(self.status_bar.reset_ui)
        self.worker.start()

    def finish_step(self, success, msg, next_idx):
        self.is_busy = False # Reset busy flag
        self.lock_ui(False)
        if not success and "Canceled" in str(msg):
            self.status_bar.finish("Operation Canceled", False)
            self.log_msg("[INFO] Operation canceled by user.")
            return

        self.status_bar.finish(str(msg), True)
        
        if success: 
            self.log_msg(f"[SUCCESS] {msg}")
            QMessageBox.information(self, "Success", str(msg))
            
            if next_idx > 0:
                self.sidebar.unlock(next_idx)
            
            if next_idx == 4:
                self.populate_preview()
                self.goto(3)
            elif next_idx > 0:
                self.goto(next_idx)
        else: 
            self.log_msg(f"[ERROR] {msg}")
            QMessageBox.critical(self, "Error", str(msg))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(STYLESHEET)
    win = ZendeskWizard()
    win.showMaximized()
    sys.exit(app.exec())