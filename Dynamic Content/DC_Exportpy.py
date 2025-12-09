import requests
import csv
import time
import os
import re
import json
import unicodedata
import concurrent.futures
import random  # <--- NEW IMPORT for jitter
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
ZENDESK_SUBDOMAIN = "z3n-xxx" # Zendesk subdomain Ex. zen-mydomain
ZENDESK_EMAIL = "xxxx@mycompany.com" # Your login email at Zendesk plataform
ZENDESK_API_TOKEN = "xxxxxyyyyyzzzzzwwwww" # Your Zendesk API Token

# OPTIONAL: DeepL API Key for better quality
DEEPL_API_KEY = "" 

# Output Configuration
OUTPUT_FOLDER = '/tmp'
CACHE_FILE = os.path.join(OUTPUT_FOLDER, 'translation_cache.json')

# RATE LIMITING CONFIGURATION (NEW)
# Time in seconds to wait between translation calls per thread.
# 0.5s with 5 threads = ~10 requests/second max. Increase if you still get 429s.
TRANSLATION_REQUEST_DELAY = 0.5 

# DEFINITION OF SYSTEM FIELD TYPES
SYSTEM_FIELD_TYPES = {
    'subject', 'description', 'status', 'tickettype', 'priority', 'group', 'assignee',
    'brand', 'satisfaction_rating'
}

# DEFINITION OF SYSTEM TAGS (Case-insensitive)
SYSTEM_TAGS = {
    'standard field', 'campo padrão', 'campo estándar', 'system'
}

# DEFINITION OF SYSTEM FIELD NAMES (Case-insensitive Blocklist)
SYSTEM_FIELD_NAMES = {
    # English Names
    'intent', 'intent confidence', 
    'sentiment', 'sentiment confidence', 
    'language', 'language confidence', 
    'summary', 'summary agent id', 'summarized by agent id', 
    'summary date and time', 'summary generated at', 'summary locale',
    'resolution type',
    'approval status',
    
    # Portuguese Names
    'id do agente do resumo',
    'localidade do resumo',
    'resumo',
    'data e hora do resumo',
    'status de aprovação'
}

# ==============================================================================
# IMPORTS & SETUP
# ==============================================================================
try:
    from deep_translator import GoogleTranslator
except ImportError as e:
    print(f"\nCRITICAL ERROR: Could not import 'deep_translator'.")
    print(f"Specific Python Error: {e}")
    exit(1)

try:
    from deep_translator import DeepL
except ImportError:
    DeepL = None
    if DEEPL_API_KEY:
        print("Warning: DeepL library could not be imported. Falling back to Google Translate.")

BASE_URL = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/ticket_fields.json'

def get_headers():
    return {'Content-Type': 'application/json'}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: Could not save cache: {e}")

TRANSLATION_CACHE = load_cache()

def get_translator(target_lang):
    source_lang = 'pt'
    if DEEPL_API_KEY and DeepL:
        if target_lang == 'en': target_lang = 'EN-US'
        return DeepL(api_key=DEEPL_API_KEY, source=source_lang, target=target_lang, use_free_api=True)
    else:
        return GoogleTranslator(source=source_lang, target=target_lang)

def safe_translate(translator, text, target_lang):
    if not text: return ""
    text = str(text).strip()
    
    # Check Cache
    cache_key = f"{text}|{target_lang}"
    if cache_key in TRANSLATION_CACHE:
        return TRANSLATION_CACHE[cache_key]

    max_retries = 5
    
    for attempt in range(max_retries):
        try:
            # --- IMPROVEMENT: PROACTIVE RATE LIMITING ---
            # Wait a small amount before making the request to smooth out traffic
            time.sleep(TRANSLATION_REQUEST_DELAY)
            
            translated_text = translator.translate(text)
            TRANSLATION_CACHE[cache_key] = translated_text
            return translated_text
            
        except Exception as e:
            error_msg = str(e).lower()
            is_rate_limit = "429" in error_msg or "too many requests" in error_msg
            
            if attempt == max_retries - 1:
                print(f"\nWarning: Translation failed for '{text[:15]}...' ({target_lang}) after {max_retries} attempts. Error: {e}")
                return text
            
            # --- IMPROVEMENT: EXPONENTIAL BACKOFF WITH JITTER ---
            # Base wait time: 2^attempt (2s, 4s, 8s, 16s...)
            # Jitter: Adds random 0-1s to prevent threads from retrying in sync
            sleep_time = (2 ** attempt) + random.uniform(0, 1)
            
            # If it's a confirmed Rate Limit (429), verify significantly longer
            if is_rate_limit:
                sleep_time += 10 
                print(f" [Rate Limit Hit] Cooling down for {int(sleep_time)}s...")

            time.sleep(sleep_time)

def generate_dc_placeholder(text):
    if not text: return ""
    text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8')
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '_', text)
    text = text.strip('_')
    if not text: return ""
    return f"{{{{dc.{text}}}}}"

def process_field_and_options(field, unique_field_name):
    """
    Worker function to process a single field AND its options (if any).
    """
    rows = []
    translator_en = get_translator('en')
    translator_es = get_translator('es')

    # --- 1. Process the Main Field ---
    title_original = field.get('title')
    field_id = field.get('id')
    field_type = field.get('type')
    portal_title_original = field.get('title_in_portal')
    description_original = field.get('description', '')
    
    dc_name = unique_field_name
    dc_placeholder = generate_dc_placeholder(dc_name)

    title_en = safe_translate(translator_en, title_original, 'en')
    title_es = safe_translate(translator_es, title_original, 'es')
    portal_title_en = safe_translate(translator_en, portal_title_original, 'en')
    portal_title_es = safe_translate(translator_es, portal_title_original, 'es')

    rows.append({
        'id': field_id,
        'parent_field': '', 
        'type': field_type,
        'option_tag_value': '', 
        'dc_name': dc_name,
        'dc_description': description_original,
        'dc_placeholder': dc_placeholder,
        'title_pt': title_original,
        'title_es': title_es,
        'title_en': title_en,
        'title_in_portal_pt': portal_title_original,
        'title_in_portal_es': portal_title_es,
        'title_in_portal_en': portal_title_en
    })

    # --- 2. Process Field Options (Dropdown values) ---
    options = field.get('custom_field_options', [])
    
    if options:
        for opt in options:
            opt_name = opt.get('name')
            opt_value = opt.get('value') 
            
            if not opt_name: continue

            opt_dc_name = f"{unique_field_name}::{opt_name}"
            opt_placeholder = generate_dc_placeholder(f"{unique_field_name}_{opt_value}")

            opt_en = safe_translate(translator_en, opt_name, 'en')
            opt_es = safe_translate(translator_es, opt_name, 'es')

            rows.append({
                'id': opt.get('id'),
                'parent_field': field_id,
                'type': 'option',
                'option_tag_value': opt_value,
                'dc_name': opt_dc_name,
                'dc_description': f"Option for field {field_id} ({field_type})",
                'dc_placeholder': opt_placeholder,
                'title_pt': opt_name,
                'title_es': opt_es,
                'title_en': opt_en,
                'title_in_portal_pt': opt_name,
                'title_in_portal_es': opt_es,
                'title_in_portal_en': opt_en
            })

    return rows

def _print_progress(current, total, start_time, prefix="Progress"):
    percent = current / total if total > 0 else 0
    bar_length = 30
    filled_length = int(bar_length * percent)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    
    elapsed_time = time.time() - start_time
    if current > 0:
        estimated_total = elapsed_time / percent
        remaining = estimated_total - elapsed_time
        eta_str = time.strftime("%H:%M:%S", time.gmtime(remaining))
    else:
        eta_str = "--:--:--"

    print(f'\r{prefix}: |{bar}| {int(percent * 100)}% ({current}/{total}) [ETA: {eta_str}]', end='', flush=True)

def get_user_confirmation(prompt):
    while True:
        response = input(f"{prompt} (y/n): ").strip().lower()
        if response in ['y', 'yes']: return True
        if response in ['n', 'no', '']: return False
        print("Please enter 'y' or 'n'.")

# ==============================================================================
# MAIN EXPORT FUNCTION
# ==============================================================================
def export_ticket_fields():
    print("\n--- Export Options ---")
    include_inactive = get_user_confirmation("Do you want to export INACTIVE ticket fields?")
    include_system = get_user_confirmation("Do you want to export SYSTEM ticket fields?")
    reset_cache = get_user_confirmation("Do you want to RESET the translation cache?")
    print("----------------------\n")

    if reset_cache:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
        TRANSLATION_CACHE.clear()
        print("Cache cleared.")

    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    output_file = os.path.join(OUTPUT_FOLDER, f'{ZENDESK_SUBDOMAIN}_{timestamp}_ticket_fields.csv')

    print(f"Starting export from {ZENDESK_SUBDOMAIN}...")
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    session = requests.Session()
    session.auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
    session.headers.update(get_headers())
    
    # --- PHASE 1: SCANNING ---
    print(f"Scanning Zendesk ({ZENDESK_SUBDOMAIN}) for fields...")
    
    all_raw_fields = []
    url = BASE_URL
    
    while url:
        try:
            response = session.get(url)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                print(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            if response.status_code != 200:
                print(f"Error fetching fields: {response.status_code}")
                return
            
            data = response.json()
            fields = data.get('ticket_fields', [])
            all_raw_fields.extend(fields)
            print(f"\rFetched {len(all_raw_fields)} fields...", end='', flush=True)
            url = data.get('next_page')
        except Exception as e:
            print(f"\nNetwork Error during scan: {e}")
            return

    print("\nScan complete. Analyzing...")

    # Analyze and Filter
    final_tasks = []
    title_counts = {}
    
    stats = {
        'total_scanned': len(all_raw_fields),
        'skipped_inactive': 0,
        'skipped_system': 0,
        'fields_to_export': 0,
        'options_to_export': 0
    }

    for field in all_raw_fields:
        # 1. FILTER: Inactive Fields
        if not include_inactive and not field.get('active'):
            stats['skipped_inactive'] += 1
            continue
        
        # 2. FILTER: System Fields
        field_type = field.get('type')
        is_system_type = field_type in SYSTEM_FIELD_TYPES
        is_removable = field.get('removable', True)
        
        field_tags = [t.lower() for t in field.get('tags', [])] if field.get('tags') else []
        
        # Check explicit names (case-insensitive)
        field_title_lower = field.get('title', '').lower()
        is_blocked_name = field_title_lower in SYSTEM_FIELD_NAMES

        # Combined check
        is_system = (
            is_system_type or 
            (not is_removable) or 
            is_blocked_name or 
            any(t in SYSTEM_TAGS for t in field_tags)
        )
        
        if not include_system and is_system:
            stats['skipped_system'] += 1
            continue

        # Prepare task data
        title_original = field.get('title', 'Untitled')
        if title_original in title_counts:
            count = title_counts[title_original]
            unique_dc_name = f"{title_original}_{count}"
            title_counts[title_original] += 1
        else:
            unique_dc_name = title_original
            title_counts[title_original] = 1
        
        # Count options for stats
        options_count = len(field.get('custom_field_options', []))
        stats['options_to_export'] += options_count
        
        final_tasks.append((field, unique_dc_name))

    stats['fields_to_export'] = len(final_tasks)
    total_rows_expected = stats['fields_to_export'] + stats['options_to_export']

    # --- SHOW RESUME ---
    print("\n" + "="*40)
    print(" EXPORT RESUME")
    print("="*40)
    print(f"Total Fields Scanned:   {stats['total_scanned']}")
    print(f"Skipped (Inactive):     {stats['skipped_inactive']}")
    print(f"Skipped (System):       {stats['skipped_system']}")
    print("-" * 40)
    print(f"Fields to Export:       {stats['fields_to_export']}")
    print(f"Options to Export:      {stats['options_to_export']}")
    print("-" * 40)
    print(f"TOTAL ITEMS TO PROCESS: {total_rows_expected}")
    print("="*40 + "\n")

    if total_rows_expected == 0:
        print("Nothing to export based on current filters.")
        return

    if not get_user_confirmation("Do you want to proceed with the export and translation?"):
        print("Operation cancelled.")
        return

    # --- PHASE 2: PROCESSING ---
    print(f"\nStarting processing with { 'DeepL' if DEEPL_API_KEY and DeepL else 'Google Translate' }...")
    
    all_rows = []
    MAX_WORKERS = 5 
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_field = {
            executor.submit(process_field_and_options, field, unique_name): field 
            for field, unique_name in final_tasks
        }
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_field):
            completed += 1
            try:
                rows_list = future.result()
                all_rows.extend(rows_list)
                _print_progress(completed, len(final_tasks), start_time, prefix="Translating")
            except Exception as exc:
                print(f"\nException: {exc}")
                
    print() # Newline
    save_cache(TRANSLATION_CACHE)

    # Write CSV
    if all_rows:
        try:
            # Sort logic
            all_rows.sort(key=lambda x: (
                int(x['parent_field']) if x['parent_field'] else int(x['id']), 
                1 if x['parent_field'] else 0,
                int(x['id'])
            ))
            
            with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
                fieldnames = [
                    'id', 'parent_field', 'type', 'option_tag_value',
                    'dc_name', 'dc_description', 'dc_placeholder',
                    'title_pt', 'title_es', 'title_en',
                    'title_in_portal_pt', 'title_in_portal_es', 'title_in_portal_en'
                ]
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_rows)
                
            print(f"\nSuccessfully exported {len(all_rows)} items to:\n{output_file}")
        except IOError as e:
            print(f"Error writing file: {e}")

if __name__ == '__main__':
    export_ticket_fields()