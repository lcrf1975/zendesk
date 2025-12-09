import requests
import csv
import time
import os
import sys
import concurrent.futures
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
ZENDESK_SUBDOMAIN = "z3n-xxx" # Zendesk subdomain Ex. zen-mydomain
ZENDESK_EMAIL = "xxxx@mycompany.com" # Your login email at Zendesk plataform
ZENDESK_API_TOKEN = "xxxxxyyyyyzzzzzwwwww" # Your Zendesk API Token

# Input File - The CSV with the DC Placeholders
INPUT_CSV_FILE = '/tmp/ticket_fields.csv'

# Folder for Backups (Use /tmp to avoid permission errors)
BACKUP_FOLDER = '/tmp'

# API Endpoints
BASE_URL = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/ticket_fields.json'
FIELD_URL_TEMPLATE = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/ticket_fields/{{id}}.json'
OPTION_URL_TEMPLATE = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/ticket_fields/{{field_id}}/options/{{option_id}}.json'

def get_headers():
    return {
        'Content-Type': 'application/json',
    }

class ZendeskFieldUpdater:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
        self.session.headers.update(get_headers())

    def _print_progress(self, current, total, start_time, prefix="Progress"):
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

    # --- FETCHING CURRENT VALUES (FOR BACKUP & ROLLBACK CHECK) ---
    def get_current_value(self, row_data):
        field_id = row_data['id'] if row_data.get('type') != 'option' else row_data['parent_field']
        option_id = row_data['id'] if row_data.get('type') == 'option' else None
        
        if option_id:
             url = OPTION_URL_TEMPLATE.format(field_id=field_id, option_id=option_id)
             key = "custom_field_option"
             attr = "name"
        else:
             url = FIELD_URL_TEMPLATE.format(id=field_id)
             key = "ticket_field"
             attr = "title"
             
        response = self.session.get(url)
        while response.status_code == 429:
             time.sleep(int(response.headers.get('Retry-After', 5)))
             response = self.session.get(url)

        if response.status_code == 200:
            current_val = response.json().get(key, {}).get(attr, "")
            return {
                'id': row_data['id'],
                'parent_field': row_data.get('parent_field', ''),
                'type': row_data.get('type', ''),
                'current_value': current_val 
            }
        return None

    # --- UPDATE FUNCTIONS ---
    def update_item_api(self, row_data, simulate):
        if simulate:
            time.sleep(0.005)
            return

        row_type = row_data.get('type')
        placeholder = row_data.get('dc_placeholder')
        if not placeholder: return

        if row_type == 'option':
             self._update_option(row_data['parent_field'], row_data['id'], placeholder)
        else:
             self._update_field(row_data['id'], placeholder)

    def restore_item_api(self, row_data, simulate):
        original_val = row_data.get('original_value')
        if not original_val: return

        current_state = self.get_current_value(row_data)
        if not current_state: return

        current_val = current_state.get('current_value', '')
        if current_val == original_val: return

        if simulate: return

        row_type = row_data.get('type')
        if row_type == 'option':
             self._update_option(row_data['parent_field'], row_data['id'], original_val)
        else:
             self._update_field(row_data['id'], original_val)

    def _update_field(self, field_id, new_title):
        url = FIELD_URL_TEMPLATE.format(id=field_id)
        payload = {"ticket_field": {"title": new_title}}
        self._send_put(url, payload, f"Field {field_id}")

    def _update_option(self, field_id, option_id, new_name):
        url = OPTION_URL_TEMPLATE.format(field_id=field_id, option_id=option_id)
        payload = {"custom_field_option": {"name": new_name}}
        self._send_put(url, payload, f"Option {option_id}")

    def _send_put(self, url, payload, item_name):
        response = self.session.put(url, json=payload)
        while response.status_code == 429:
            time.sleep(int(response.headers.get('Retry-After', 5)))
            response = self.session.put(url, json=payload)
        
        if response.status_code not in [200, 201]:
             raise Exception(f"Failed to update {item_name}: {response.text}")

    # --- BACKUP LOGIC ---
    def create_backup(self, tasks, backup_file):
        full_path = os.path.join(BACKUP_FOLDER, backup_file)
        print(f"\n[BACKUP] Creating backup of {len(tasks)} items before modifying...")
        print(f"[BACKUP] Target File: {full_path}")
        
        backup_data = []
        total = len(tasks)
        start_t = time.time()
        MAX_WORKERS = 5 
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {executor.submit(self.get_current_value, row): row for row in tasks}
            completed = 0
            for future in concurrent.futures.as_completed(future_to_row):
                completed += 1
                self._print_progress(completed, total, start_t, prefix="Backing Up")
                try:
                    result = future.result()
                    if result:
                        result['original_value'] = result.pop('current_value')
                        backup_data.append(result)
                except Exception as e:
                    print(f"\nError backing up item: {e}")

        # Sort for readability
        backup_data.sort(key=lambda x: (
            int(x['parent_field']) if x['parent_field'] else int(x['id']), 
            1 if x['parent_field'] else 0,
            int(x['id'])
        ))

        print(f"\n\n[BACKUP] Saving {len(backup_data)} records to disk...")
        try:
            with open(full_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['id', 'parent_field', 'type', 'original_value'])
                writer.writeheader()
                writer.writerows(backup_data)
            print(f"[BACKUP] Success! Saved to {full_path}\n")
            return True
        except IOError as e:
            print(f"[BACKUP] FAILED to write file: {e}")
            return False

    # --- ROLLBACK LOGIC ---
    def run_rollback(self):
        backup_path = input("\nEnter the FULL path of the BACKUP CSV file: ").strip()
        if not os.path.exists(backup_path):
            print("File not found.")
            return

        with open(backup_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        print(f"\nLoaded {len(rows)} items to RESTORE.")
        
        simulate = False
        sim_resp = input("Do you want to SIMULATE the rollback first? (y/n): ").strip().lower()
        if sim_resp in ['y', 'yes']:
            simulate = True
            print("\n*** RUNNING ROLLBACK SIMULATION (Checking Diffs Only) ***")
        else:
            print("\n   [WARNING] This will REVERT field titles to their backup values.")
            if input("Are you sure you want to proceed with LIVE ROLLBACK? (y/n): ").lower() not in ['y', 'yes']:
                print("Cancelled.")
                return

        start_time = time.time()
        total_tasks = len(rows)
        MAX_WORKERS = 5
        prefix = "Simulating Rollback" if simulate else "Rolling Back"
        
        print(f"\nStarting {prefix} with {MAX_WORKERS} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {executor.submit(self.restore_item_api, row, simulate): row for row in rows}
            completed = 0
            for future in concurrent.futures.as_completed(future_to_row):
                completed += 1
                self._print_progress(completed, total_tasks, start_time, prefix=prefix)
                try:
                    future.result()
                except Exception as e:
                    print(f"\nError restoring: {e}")

        print(f"\n\n{prefix} Complete.")

    # --- MAIN RUN ---
    def run(self, csv_path):
        if not os.path.exists(csv_path):
            print(f"Error: Input File {csv_path} not found.")
            return

        print(f"\n--- Zendesk Field Update Tool ---")
        print(f"Reading CSV: {csv_path}...")
        
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Analyze for Resume
        tasks = []
        stats = {'fields': 0, 'options': 0, 'skipped': 0}
        for row in rows:
            placeholder = row.get('dc_placeholder')
            row_type = row.get('type')
            if placeholder:
                tasks.append(row)
                if row_type == 'option':
                    stats['options'] += 1
                else:
                    stats['fields'] += 1
            else:
                stats['skipped'] += 1

        # SHOW RESUME FIRST
        print("\n" + "="*40)
        print(" FILE ANALYSIS RESUME")
        print("="*40)
        print(f"Fields to Update:       {stats['fields']}")
        print(f"Options to Update:      {stats['options']}")
        print("-" * 40)
        print(f"TOTAL POTENTIAL UPDATES: {len(tasks)}")
        print(f"Skipped (No Placeholder):{stats['skipped']}")
        print("="*40 + "\n")

        if len(tasks) == 0:
            print("No valid items found to update.")
            return

        print("Select Mode:")
        print("1. UPDATE Fields with DC Placeholders (from this file)")
        print("2. ROLLBACK (Restore from a backup file)")
        print("3. CREATE BACKUP ONLY (of items in this file)")
        
        mode = input("Enter 1, 2, or 3: ").strip()
        
        if mode == '2':
            self.run_rollback()
            return

        if mode == '3':
            timestamp = datetime.now().strftime("%Y%m%d%H%M")
            backup_file = f"backup_fields_{timestamp}.csv"
            if self.create_backup(tasks, backup_file):
                print(f"Backup created successfully.")
            return

        # Mode 1: Update
        simulate = False
        sim_resp = input("\nDo you want to SIMULATE the update first? (y/n): ").strip().lower()
        if sim_resp in ['y', 'yes']:
            simulate = True
            print("\n*** RUNNING IN SIMULATION MODE - NO CHANGES WILL BE MADE ***")
        
        if not simulate:
            print("\n   [WARNING] This action modifies your LIVE ticket fields.")
            if input("Are you sure you want to proceed with LIVE UPDATES? (y/n): ").lower() not in ['y', 'yes']:
                print("Cancelled.")
                return
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M")
            backup_file = f"backup_fields_{timestamp}.csv"
            if not self.create_backup(tasks, backup_file):
                print("Backup failed. Aborting update for safety.")
                return

        total_tasks = len(tasks)
        start_time = time.time()
        MAX_WORKERS = 10 if simulate else 5
        prefix = "Simulating" if simulate else "Updating"
        
        print(f"\nStarting {prefix} with {MAX_WORKERS} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {executor.submit(self.update_item_api, row, simulate): row for row in tasks}
            completed = 0
            for future in concurrent.futures.as_completed(future_to_row):
                completed += 1
                self._print_progress(completed, total_tasks, start_time, prefix=prefix)
                try:
                    future.result()
                except Exception as e:
                    row = future_to_row[future]
                    print(f"\n[ERROR] Item {row.get('id')}: {e}")

        print(f"\n\n{prefix} Complete.")

if __name__ == '__main__':
    updater = ZendeskFieldUpdater()
    csv_file = sys.argv[1] if len(sys.argv) > 1 else INPUT_CSV_FILE
    updater.run(csv_file)