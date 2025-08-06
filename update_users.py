# --- Init ---
# If you don't have request lib installed run in the terminal the cmd: pip install requests before execute the script
import requests
import json
import time

# --- Configuration ---
ZENDESK_SUBDOMAIN = "z3n-xxx" # Zendesk subdomain Ex. zen-mydomain
ZENDESK_EMAIL = "xxxx@mycompany.com" # Your login email at Zendesk plataform
ZENDESK_API_TOKEN = "xxxxxyyyyyzzzzzwwwww" # Your Zendesk API Token

# The new alias you want to set
NEW_ALIAS = "Suporte MedSenior"

# A list of dictionaries, where each dictionary defines a valid role/role_type combination.
# The script will process users that match ANY of these conditions.
TARGET_USER_CONDITIONS = [
    # Condition 1: Admins with role_type 4
    {"role": "admin", "role_type": 4},
    # Condition 2: Agents with role_type 0
    {"role": "agent", "role_type": 0}
]

# --- API Endpoints ---
BASE_URL = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"
USERS_ENDPOINT = f"{BASE_URL}/users.json"

# --- Functions ---
def get_all_zendesk_users():
    """Fetches ALL users from Zendesk, handling pagination."""
    all_users = []
    next_page_url = USERS_ENDPOINT

    while next_page_url:
        try:
            response = requests.get(next_page_url, auth=(ZENDESK_EMAIL + "/token", ZENDESK_API_TOKEN))
            response.raise_for_status()
            data = response.json()
            all_users.extend(data.get('users', []))
            next_page_url = data.get('next_page')

        except requests.exceptions.RequestException as e:
            print(f"Error fetching users: {e}")
            return None
    return all_users

def update_user_alias(user_id, new_alias_value):
    """Updates the alias for a specific user."""
    url = f"{BASE_URL}/users/{user_id}.json"
    payload = {"user": {"alias": new_alias_value}}
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.put(url, auth=(ZENDESK_EMAIL + "/token", ZENDESK_API_TOKEN), headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        print(f"Successfully updated alias for user ID {user_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating alias for user ID {user_id}: {e}")
        if response is not None:
            print(f"Response status code: {response.status_code}")
            print(f"Response content: {response.text}")
        return False

# --- Main Script ---
if __name__ == "__main__":
    print("Fetching all users from Zendesk...")
    users = get_all_zendesk_users()

    if users is None:
        print("Failed to retrieve users. Exiting.")
        exit()

    users_to_process = []
    for user in users:
        # Check if the user matches any of the defined conditions
        is_match = any(
            user.get('role') == condition.get('role') and user.get('role_type') == condition.get('role_type')
            for condition in TARGET_USER_CONDITIONS
        )

        if is_match:
            users_to_process.append(user)

    print(f"Found {len(users_to_process)} users matching the specified criteria to potentially update.")
    print(f"The new alias to be set is: '{NEW_ALIAS}'")

    if not users_to_process:
        print("No users found matching the specified criteria. Exiting.")
        exit()

    for user in users_to_process:
        user_id = user.get('id')
        current_alias = user.get('alias')

        if user_id and current_alias != NEW_ALIAS:
            print(f"Updating alias for '{user.get('name')}' (ID: {user_id}, Role: {user.get('role')}, Role Type: {user.get('role_type')}). Current: '{current_alias}'")
            update_user_alias(user_id, NEW_ALIAS)
            # Optional: Add a small delay between updates to respect rate limits
            # time.sleep(0.5)
        elif user_id:
            print(f"Skipping '{user.get('name')}' (ID: {user_id}, Role: {user.get('role')}, Role Type: {user.get('role_type')}) as alias is already set to '{NEW_ALIAS}'.")

    print("Script finished.")