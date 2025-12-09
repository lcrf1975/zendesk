import requests
import time
import sys
import os
import concurrent.futures
from datetime import datetime, timedelta, timezone

# Configuration
ZENDESK_SUBDOMAIN = "z3n-xxx" # Zendesk subdomain Ex. zen-mydomain
ZENDESK_EMAIL = "xxxx@mycompany.com" # Your login email at Zendesk plataform
ZENDESK_API_TOKEN = "xxxxxyyyyyzzzzzwwwww" # Your Zendesk API Token
# API Endpoints
BASE_URL = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/dynamic_content/items.json'
DELETE_URL_TEMPLATE = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/dynamic_content/items/{{id}}.json'

def get_headers():
    return {
        'Content-Type': 'application/json',
    }

class ZendeskDCCleanup:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (f'{ZENDESK_EMAIL}/token', ZENDESK_API_TOKEN)
        self.session.headers.update(get_headers())

    def parse_zendesk_date(self, date_str):
        """
        Parses Zendesk ISO 8601 date string to a timezone-aware datetime object.
        """
        date_str = date_str.replace('Z', '+00:00')
        return datetime.fromisoformat(date_str)

    def _print_progress(self, current, total, start_time, prefix="Progress", suffix=""):
        percent = current / total if total > 0 else 0
        bar_length = 30
        filled_length = int(bar_length * percent)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        
        # Calculate ETA
        elapsed_time = time.time() - start_time
        if current > 0:
            estimated_total = elapsed_time / percent
            remaining = estimated_total - elapsed_time
            eta_str = time.strftime("%H:%M:%S", time.gmtime(remaining))
        else:
            eta_str = "--:--:--"

        print(f'\r{prefix}: |{bar}| {int(percent * 100)}% ({current}/{total}) [ETA: {eta_str}] {suffix}', end='', flush=True)

    def get_cleanup_window(self):
        """
        Asks the user for the time window in hours.
        """
        while True:
            try:
                val = input("\nEnter the cleanup window in hours (e.g., 1 for last hour, 0.5 for 30 mins): ").strip()
                hours = float(val)
                if hours <= 0:
                    print("Please enter a positive number.")
                    continue
                return hours
            except ValueError:
                print("Invalid input. Please enter a number (e.g. 1, 2.5).")

    def find_recent_items(self, hours):
        """
        Fetches all items and filters those created within the last 'hours'.
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        print(f"\nScanning for items created AFTER: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S UTC')}...\n")
        
        candidates = []
        url = BASE_URL
        page_count = 0
        
        while url:
            response = self.session.get(url)
            if response.status_code == 429:
                wait = int(response.headers.get('Retry-After', 10))
                print(f"Rate limit hit. Waiting {wait}s...")
                time.sleep(wait)
                continue
            
            if response.status_code != 200:
                print(f"Error fetching items: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            items = data.get('items', [])
            
            for item in items:
                created_at = self.parse_zendesk_date(item['created_at'])
                
                if created_at > cutoff_time:
                    candidates.append({
                        'id': item['id'],
                        'name': item['name'],
                        'created_at': created_at,
                        'placeholder': item['placeholder']
                    })
            
            page_count += 1
            # Simple loading indicator
            print(f"\rScanning page {page_count}...", end='', flush=True)
            url = data.get('next_page')
            
        print(f"\nScan complete. Found {len(candidates)} matching items.")
        return candidates

    def delete_item_api(self, item):
        """
        Worker function: Deletes a single item.
        Isolated logic for threading.
        """
        url = DELETE_URL_TEMPLATE.format(id=item['id'])
        response = self.session.delete(url)
        
        # Handle Rate Limiting inside the thread
        while response.status_code == 429:
            wait = int(response.headers.get('Retry-After', 5))
            time.sleep(wait)
            response = self.session.delete(url)
            
        if response.status_code not in [200, 204]:
            raise Exception(f"Status {response.status_code}: {response.text}")

    def delete_items(self, items):
        """
        Deletes the list of items using Multi-threading.
        """
        print("\nStarting deletion...")
        total = len(items)
        MAX_WORKERS = 5
        
        start_time = time.time() # Initialize timer
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Map each item to a future task
            future_to_item = {executor.submit(self.delete_item_api, item): item for item in items}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                completed += 1
                self._print_progress(completed, total, start_time, prefix="Deleting")
                
                try:
                    future.result()
                except Exception as e:
                    # Print newline to escape progress bar
                    print(f"\n[ERROR] Failed to delete '{item['name']}' (ID: {item['id']}): {e}")

        print("\n\nDeletion Complete.")

    def run(self):
        print("--- Zendesk Dynamic Content Cleanup Tool ---")
        
        # 1. Ask User for Hours
        hours_back = self.get_cleanup_window()

        # 2. Find Candidates
        candidates = self.find_recent_items(hours_back)
        
        if not candidates:
            print("No items found created in that time window.")
            return

        # 3. Calculate Category Breakdown
        category_counts = {}
        for item in candidates:
            name = item['name']
            # Extract category prefix (e.g., "Text" from "Text::Description")
            if '::' in name:
                category = name.split('::')[0]
            else:
                category = "(Uncategorized)"
            
            category_counts[category] = category_counts.get(category, 0) + 1

        # 4. Print Preview Summary
        print("\n" + "="*60)
        print(f" PREVIEW: ITEMS TO DELETE (Created in last {hours_back} hour(s))")
        print("="*60)
        print(f"{'CATEGORY':<40} | {'COUNT'}")
        print("-" * 60)
        
        for category, count in sorted(category_counts.items()):
            print(f"{category:<40} | {count}")
            
        print("-" * 60)
        print(f"{'TOTAL':<40} | {len(candidates)}")
        print("="*60)

        # 5. Confirm
        confirm = input("\nAre you sure you want to PERMANENTLY DELETE these items? (y/n): ").strip().lower()
        
        if confirm in ['y', 'yes']:
            self.delete_items(candidates)
        else:
            print("Operation cancelled.")

if __name__ == '__main__':
    cleaner = ZendeskDCCleanup()
    cleaner.run()