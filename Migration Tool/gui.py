import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import threading
import requests
import json
import csv
import time
import os
import sys
import copy
import re
from datetime import datetime
import queue
import random
import traceback
import hashlib
from typing import Optional, Dict, List, Any, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum, IntEnum

# Fix for blurry text on Mac Retina displays
try:
    from ctypes import cdll
    cdll.LoadLibrary("libtk8.6.dylib")
except OSError:
    pass
except Exception:
    pass


# ==========================================
# ENUMS AND DATA CLASSES
# ==========================================

class ImportStrategy(Enum):
    """Defines how to handle existing fields during import."""
    SKIP = "skip"
    UPDATE = "update"
    CLONE = "clone"


class FieldType(Enum):
    """Supported Zendesk field types."""
    TEXT = "text"
    TEXTAREA = "textarea"
    CHECKBOX = "checkbox"
    DATE = "date"
    INTEGER = "integer"
    DECIMAL = "decimal"
    REGEXP = "regexp"
    TAGGER = "tagger"
    MULTISELECT = "multiselect"
    LOOKUP = "lookup"


class ProcessState(Enum):
    """Current state of background processing."""
    IDLE = "idle"
    RUNNING = "running"
    STOPPING = "stopping"


class LogLevel(IntEnum):
    """
    Log message severity levels.

    Using IntEnum allows comparison operators for filtering.
    Higher values = more severe.
    """
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    ALWAYS = 100

    @classmethod
    def get_display_name(cls, level: "LogLevel") -> str:
        """Get human-readable name for display."""
        names = {
            cls.DEBUG: "DEBUG - All messages",
            cls.INFO: "INFO - Operations & above",
            cls.WARNING: "WARNING - Problems & above",
            cls.ERROR: "ERROR - Errors & above",
            cls.CRITICAL: "CRITICAL - Critical only",
        }
        return names.get(level, level.name)

    @classmethod
    def get_filter_levels(cls) -> List["LogLevel"]:
        """Get levels available for filtering (excludes ALWAYS)."""
        return [cls.DEBUG, cls.INFO, cls.WARNING, cls.ERROR, cls.CRITICAL]

    @classmethod
    def from_string(cls, name: str) -> "LogLevel":
        """Convert string name to LogLevel."""
        name_upper = name.upper().split(" ")[0]
        for level in cls:
            if level.name == name_upper:
                return level
        return cls.INFO


@dataclass
class Credentials:
    """Stores API credentials for a Zendesk instance."""
    subdomain: str = ""
    email: str = ""
    token: str = ""

    def is_valid(self) -> bool:
        """Check if all credential fields are filled."""
        return bool(self.subdomain and self.email and self.token)

    def to_dict(self) -> Dict[str, str]:
        """Convert credentials to dictionary."""
        return {
            "subdomain": self.subdomain,
            "email": self.email,
            "token": self.token
        }

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "Credentials":
        """Create credentials from dictionary."""
        return cls(
            subdomain=data.get("subdomain", ""),
            email=data.get("email", ""),
            token=data.get("token", "")
        )


@dataclass
class ImportReport:
    """Tracks import operation results."""
    created: List[str] = field(default_factory=list)
    updated: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def total_processed(self) -> int:
        """Return total number of items processed."""
        return (
            len(self.created) +
            len(self.updated) +
            len(self.skipped) +
            len(self.errors)
        )


@dataclass
class AnalysisResults:
    """Stores results from analysis operation."""
    new_fields: List[Dict] = field(default_factory=list)
    existing_fields: List[Dict] = field(default_factory=list)
    changed_fields: List[Dict] = field(default_factory=list)
    new_forms: List[Dict] = field(default_factory=list)
    existing_forms: List[Dict] = field(default_factory=list)
    changed_forms: List[Dict] = field(default_factory=list)

    def has_data(self) -> bool:
        """Check if analysis has any results."""
        return bool(
            self.new_fields or
            self.existing_fields or
            self.changed_fields or
            self.new_forms or
            self.existing_forms or
            self.changed_forms
        )


# ==========================================
# 1. API CLIENT
# ==========================================

class ZendeskClient:
    """
    HTTP client for Zendesk REST API.

    Handles authentication, rate limiting, retries, and collision resolution
    for field/form creation and updates.
    """

    SYSTEM_FIELD_KEYS = frozenset({
        'subject', 'description', 'status', 'tickettype', 'priority',
        'group', 'assignee', 'brand', 'satisfaction_rating', 'custom_status',
        'email', 'name', 'time_zone', 'locale_id', 'organization_id', 'role',
        'phone', 'mobile', 'whatsapp', 'facebook', 'twitter', 'google',
        'photo', 'authenticity_token', 'active', 'alias', 'signature',
        'shared_phone_number', 'domain_names', 'tags', 'shared_tickets',
        'shared_comments'
    })

    ALLOWED_FIELD_TYPES = frozenset({ft.value for ft in FieldType})

    SUPPORTED_METHODS = frozenset({'GET', 'POST', 'PUT', 'DELETE'})

    # Reserved tag prefixes that cannot be migrated as-is
    RESERVED_TAG_PREFIXES = (
        'zd_',
        'zendesk_',
    )

    # Valid system lookup targets that exist in all Zendesk instances
    SYSTEM_LOOKUP_TARGETS = frozenset({
        'zen:user',
        'zen:organization',
        'zen:ticket',
    })

    MAX_RETRIES = 10
    REQUEST_TIMEOUT = 30
    RATE_LIMIT_BUFFER = 1
    RETRY_DELAY = 2
    MIN_DELAY_BETWEEN_REQUESTS = 0.1

    def __init__(
        self,
        subdomain: str,
        email: str,
        token: str,
        logger_func: Callable[[str, LogLevel], None],
        verbose: bool = False,
        stop_check: Optional[Callable[[], bool]] = None
    ):
        """
        Initialize Zendesk API client.

        Args:
            subdomain: Zendesk instance subdomain (e.g., 'mycompany')
            email: Admin email address
            token: API token
            logger_func: Callback for log messages with level
            verbose: Enable debug logging (for backward compatibility)
            stop_check: Optional callback that returns True if operation should stop
        """
        self.subdomain = self._sanitize_subdomain(subdomain)
        self.base_url = f"https://{self.subdomain}.zendesk.com/api/v2"
        self.auth = (f"{email}/token", token)
        self.headers = {'Content-Type': 'application/json'}
        self.logger = logger_func
        self.verbose = verbose
        self.stop_check = stop_check

    def _should_stop(self) -> bool:
        """Check if operation should be stopped."""
        if self.stop_check:
            return self.stop_check()
        return False

    @staticmethod
    def _sanitize_subdomain(subdomain: str) -> str:
        """Sanitize subdomain input to prevent injection."""
        subdomain = subdomain.lower().strip()
        subdomain = re.sub(r'^https?://', '', subdomain)
        subdomain = re.sub(r'\.zendesk\.com.*$', '', subdomain)
        subdomain = re.sub(r'[^a-z0-9\-]', '', subdomain)
        return subdomain

    def _log(self, message: str, level: LogLevel = LogLevel.INFO) -> None:
        """Log message with specified level."""
        self.logger(message, level)

    def _log_debug(self, message: str) -> None:
        """Log debug message."""
        self._log(f"[DEBUG] {message}", LogLevel.DEBUG)

    def _log_info(self, message: str) -> None:
        """Log info message."""
        self._log(f"[INFO] {message}", LogLevel.INFO)

    def _log_warning(self, message: str) -> None:
        """Log warning message."""
        self._log(f"[WARNING] {message}", LogLevel.WARNING)

    def _log_error(self, message: str) -> None:
        """Log error message."""
        self._log(f"[ERROR] {message}", LogLevel.ERROR)

    def _log_critical(self, message: str) -> None:
        """Log critical message."""
        self._log(f"[CRITICAL] {message}", LogLevel.CRITICAL)

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict] = None
    ) -> Optional[requests.Response]:
        """Execute HTTP request with retry logic and error handling."""
        if self._should_stop():
            self._log_info("Operation cancelled by user")
            return None

        method = method.upper()
        if method not in self.SUPPORTED_METHODS:
            self._log_error(f"Unsupported HTTP method: {method}")
            return None

        url = f"{self.base_url}/{endpoint}"
        attempt = 0
        response = None

        while attempt < self.MAX_RETRIES:
            if self._should_stop():
                self._log_info("Operation cancelled by user")
                return None

            try:
                self._log_debug(f"{method} {url}")
                if payload:
                    try:
                        self._log_debug(f"Payload: {json.dumps(payload, indent=2)}")
                    except (TypeError, ValueError):
                        self._log_debug("Payload: (Complex Data - cannot serialize)")

                response = self._execute_request(method, url, payload)

                if response is None:
                    attempt += 1
                    continue

                self._log_debug(f"Response Status: {response.status_code}")

                if response.status_code == 422:
                    self._log_warning(
                        f"Validation/Collision (422) | Body: {response.text[:500]}"
                    )
                elif response.status_code >= 400 and response.status_code != 429:
                    self._log_error(
                        f"HTTP {response.status_code} | Body: {response.text[:500]}"
                    )

                if response.status_code == 429:
                    wait_time = int(
                        response.headers.get('Retry-After', 60)
                    ) + self.RATE_LIMIT_BUFFER
                    self._log_warning(
                        f"Rate limit on {self.subdomain}. Waiting {wait_time}s..."
                    )
                    for _ in range(wait_time):
                        if self._should_stop():
                            self._log_info("Operation cancelled during rate limit wait")
                            return None
                        time.sleep(1)
                    attempt += 1
                    continue

                if 500 <= response.status_code < 600:
                    self._log_warning(
                        f"Server error {response.status_code}. Retrying..."
                    )
                    time.sleep(self.RETRY_DELAY)
                    attempt += 1
                    continue

                return response

            except requests.exceptions.Timeout:
                self._log_warning(
                    f"Timeout - server {self.subdomain} took too long to respond"
                )
                attempt += 1

            except requests.exceptions.ConnectionError as e:
                self._log_error(f"Connection error: {e}")
                attempt += 1

            except requests.exceptions.RequestException as e:
                self._log_error(f"Request error: {e}")
                attempt += 1

            time.sleep(self.MIN_DELAY_BETWEEN_REQUESTS)

        self._log_error(f"Max retries ({self.MAX_RETRIES}) reached for {endpoint}")
        return None

    def _execute_request(
        self,
        method: str,
        url: str,
        payload: Optional[Dict]
    ) -> Optional[requests.Response]:
        """Execute the actual HTTP request."""
        request_kwargs = {
            'auth': self.auth,
            'timeout': self.REQUEST_TIMEOUT
        }

        if method == 'GET':
            return requests.get(url, **request_kwargs)
        elif method == 'POST':
            return requests.post(
                url,
                headers=self.headers,
                json=payload,
                **request_kwargs
            )
        elif method == 'PUT':
            return requests.put(
                url,
                headers=self.headers,
                json=payload,
                **request_kwargs
            )
        elif method == 'DELETE':
            return requests.delete(url, **request_kwargs)

        return None

    def get_all(self, endpoint: str, key: str) -> List[Dict]:
        """Fetch all items from a paginated endpoint."""
        items = []
        url = endpoint

        self._log_debug(f"Fetching {key} from {self.subdomain}...")

        while url:
            if self._should_stop():
                self._log_info("Fetch operation cancelled")
                break

            response = self._request('GET', url)

            if response is None:
                self._log_error(f"Failed to fetch {url} - no response")
                break

            if response.status_code != 200:
                self._log_error(
                    f"Failed to fetch {url}. Status: {response.status_code}"
                )
                break

            try:
                data = response.json()
            except json.JSONDecodeError as e:
                self._log_error(f"Invalid JSON from {url}: {e}")
                break

            items.extend(data.get(key, []))

            next_url = data.get('next_page')
            if next_url:
                url = next_url.replace(self.base_url + "/", "")
            else:
                url = None

        self._log_debug(f"Fetched {len(items)} {key}")
        return items

    def _generate_unique_suffix(self) -> str:
        """Generate a unique suffix for collision resolution."""
        timestamp = int(time.time() * 1000) % 100000
        random_part = random.randint(100, 999)
        return f"_mig{timestamp}{random_part}"

    def _has_reserved_tags(self, payload: Dict, object_type_key: str) -> List[str]:
        """
        Check if payload contains reserved Zendesk tags.

        Returns list of reserved tags found.
        """
        reserved_found = []
        field_data = payload.get(object_type_key, {})

        # Check main tag
        tag = field_data.get('tag', '')
        if tag:
            for prefix in self.RESERVED_TAG_PREFIXES:
                if tag.lower().startswith(prefix):
                    reserved_found.append(tag)
                    break

        # Check custom field options
        for opt in field_data.get('custom_field_options', []):
            opt_value = opt.get('value', '')
            if opt_value:
                for prefix in self.RESERVED_TAG_PREFIXES:
                    if opt_value.lower().startswith(prefix):
                        reserved_found.append(opt_value)
                        break

        return reserved_found

    def _rename_reserved_tags(self, payload: Dict, object_type_key: str) -> Dict:
        """
        Rename reserved tags to allow migration.

        Replaces 'zd_' prefix with 'mig_' to avoid conflicts.
        """
        new_payload = copy.deepcopy(payload)
        field_data = new_payload.get(object_type_key, {})

        # Rename main tag
        tag = field_data.get('tag', '')
        if tag:
            for prefix in self.RESERVED_TAG_PREFIXES:
                if tag.lower().startswith(prefix):
                    new_tag = 'mig_' + tag[len(prefix):]
                    field_data['tag'] = new_tag
                    self._log_warning(f"Renamed reserved tag: {tag} -> {new_tag}")
                    break

        # Rename custom field option tags
        for opt in field_data.get('custom_field_options', []):
            opt_value = opt.get('value', '')
            if opt_value:
                for prefix in self.RESERVED_TAG_PREFIXES:
                    if opt_value.lower().startswith(prefix):
                        new_value = 'mig_' + opt_value[len(prefix):]
                        self._log_warning(
                            f"Renamed reserved option tag: {opt_value} -> {new_value}"
                        )
                        opt['value'] = new_value
                        break

        return new_payload

    def _resolve_collision(
        self,
        payload: Dict,
        object_type_key: str,
        attempt: int = 1
    ) -> Dict:
        """Resolve field collision by renaming keys/tags."""
        new_payload = copy.deepcopy(payload)
        field_data = new_payload.get(object_type_key)

        if not field_data:
            return new_payload

        suffix = self._generate_unique_suffix()
        self._log_debug(f"Auto-fix attempt {attempt}: Using suffix {suffix}")

        if 'key' in field_data:
            old_key = field_data['key']
            field_data['key'] = f"{old_key}{suffix}"
            self._log_debug(f"Renamed key: {old_key} -> {field_data['key']}")

        if 'tag' in field_data:
            old_tag = field_data['tag']
            field_data['tag'] = f"{old_tag}{suffix}"
            self._log_debug(f"Renamed tag: {old_tag} -> {field_data['tag']}")

        if 'custom_field_options' in field_data:
            renamed_count = 0
            for opt in field_data['custom_field_options']:
                if 'value' in opt:
                    old_val = opt['value']
                    opt['value'] = f"{old_val}{suffix}"
                    renamed_count += 1

            if renamed_count > 0:
                self._log_debug(f"Renamed {renamed_count} option tags")

        return new_payload

    def _is_collision_error(self, response_text: str) -> bool:
        """Determine if error response indicates a collision/duplicate."""
        if not response_text:
            return False

        error_lower = response_text.lower()

        # Non-collision indicators - actual validation errors
        non_collision_indicators = [
            "blankvalue",
            "em branco",
            "blank",
            "is required",
            "can't be blank",
            "cannot be blank",
            "must be present",
            "parameter missing",
            "parametermissing",
            "relationship_target_type",
            "is not a valid",
        ]

        for indicator in non_collision_indicators:
            if indicator in error_lower:
                return False

        # Collision indicators - tag/key already exists
        collision_indicators = [
            "em uso",
            "taken",
            "already exists",
            "duplicate",
            "já estão",
            "not unique",
            "has already been taken",
            "must be unique",
            "key is in use",
            "already used in a custom field",
            "already used in",
            "is already used",
        ]

        for indicator in collision_indicators:
            if indicator in error_lower:
                return True

        if "recordinvalid" in error_lower:
            uniqueness_context = [
                "unique", "duplicate", "taken", "exists", "already used"
            ]
            return any(ctx in error_lower for ctx in uniqueness_context)

        return False

    def _is_reserved_tag_error(self, response_text: str) -> bool:
        """Check if error is due to reserved tags."""
        if not response_text:
            return False

        error_lower = response_text.lower()

        # Check for reserved tag patterns
        for prefix in self.RESERVED_TAG_PREFIXES:
            if prefix in error_lower and "already used" in error_lower:
                return True

        return False

    def create_field_safe(
        self,
        endpoint: str,
        payload: Dict,
        object_type_key: str,
        max_collision_retries: int = 3
    ) -> Optional[Dict]:
        """Create a field with automatic collision resolution."""
        if self._should_stop():
            return None

        # Check for reserved tags before first attempt
        reserved_tags = self._has_reserved_tags(payload, object_type_key)
        if reserved_tags:
            self._log_warning(
                f"Found reserved tags: {', '.join(reserved_tags)}. "
                "Renaming to avoid conflicts..."
            )
            payload = self._rename_reserved_tags(payload, object_type_key)

        response = self._request('POST', endpoint, payload=payload)

        if response is None:
            return None

        if response.status_code == 201:
            try:
                return response.json()
            except json.JSONDecodeError:
                self._log_error("Failed to parse creation response")
                return None

        if response.status_code == 422:
            # Check if it's a reserved tag error
            if self._is_reserved_tag_error(response.text):
                self._log_warning("Reserved tag conflict detected. Renaming tags...")
                payload = self._rename_reserved_tags(payload, object_type_key)

                retry_response = self._request('POST', endpoint, payload=payload)
                if retry_response and retry_response.status_code == 201:
                    self._log_info("Reserved tag rename successful!")
                    try:
                        return retry_response.json()
                    except json.JSONDecodeError:
                        self._log_error("Failed to parse retry response")
                        return None

            if self._is_collision_error(response.text):
                self._log_info("Collision detected. Attempting auto-fix...")

                for attempt in range(1, max_collision_retries + 1):
                    if self._should_stop():
                        return None

                    new_payload = self._resolve_collision(
                        payload, object_type_key, attempt
                    )

                    retry_response = self._request(
                        'POST', endpoint, payload=new_payload
                    )

                    if retry_response and retry_response.status_code == 201:
                        self._log_info("Auto-fix successful!")
                        try:
                            return retry_response.json()
                        except json.JSONDecodeError:
                            self._log_error("Failed to parse retry response")
                            return None

                    elif retry_response and retry_response.status_code == 422:
                        if not self._is_collision_error(retry_response.text):
                            self._log_error(
                                f"Auto-fix failed with different error: "
                                f"{retry_response.text[:200]}"
                            )
                            break

                self._log_error(
                    f"Auto-fix failed after {max_collision_retries} attempts"
                )
            else:
                self._log_error(
                    f"Validation error (not collision): {response.text[:200]}"
                )

        return None

    def update_object_safe(
        self,
        endpoint: str,
        obj_id: int,
        payload: Dict,
        object_type_key: str,
        max_collision_retries: int = 3
    ) -> Optional[Dict]:
        """Update an object with automatic collision resolution."""
        if self._should_stop():
            return None

        url = f"{endpoint}/{obj_id}.json"

        # Check for reserved tags before update
        reserved_tags = self._has_reserved_tags(payload, object_type_key)
        if reserved_tags:
            self._log_warning(
                f"Found reserved tags in update: {', '.join(reserved_tags)}. "
                "Renaming to avoid conflicts..."
            )
            payload = self._rename_reserved_tags(payload, object_type_key)

        response = self._request('PUT', url, payload=payload)

        if response is None:
            self._log_error(f"Network failure updating object {obj_id}")
            return None

        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError:
                self._log_error("Failed to parse update response")
                return None

        if response.status_code == 422:
            self._log_debug("422 error during update. Checking if collision...")

            if self._is_collision_error(response.text):
                self._log_info("Tag conflict confirmed. Attempting auto-fix...")

                for attempt in range(1, max_collision_retries + 1):
                    if self._should_stop():
                        return None

                    new_payload = self._resolve_collision(
                        payload, object_type_key, attempt
                    )

                    time.sleep(0.5)

                    retry_response = self._request('PUT', url, payload=new_payload)

                    if retry_response and retry_response.status_code == 200:
                        self._log_info("Auto-fix update successful!")
                        try:
                            return retry_response.json()
                        except json.JSONDecodeError:
                            self._log_error("Failed to parse retry response")
                            return None

                self._log_error(
                    f"Auto-fix update failed after {max_collision_retries} attempts"
                )
            else:
                self._log_warning(
                    "422 error was NOT a collision. Skipping auto-fix."
                )

        return None

    def delete_item(self, endpoint: str, item_id: int) -> bool:
        """Delete an item by ID."""
        if self._should_stop():
            return False

        url = f"{endpoint}/{item_id}.json"
        response = self._request('DELETE', url)

        if response and response.status_code in [200, 204, 404]:
            return True

        self._log_error(f"Delete failed for item {item_id}")
        return False


# ==========================================
# 2. LOGIC CONTROLLER
# ==========================================

class MigrationLogic:
    """Business logic for Zendesk migration operations."""

    def __init__(
        self,
        logger_func: Callable[[str, LogLevel], None],
        update_progress_func: Callable[[int, int], None]
    ):
        """Initialize migration logic."""
        self.log = logger_func
        self.progress = update_progress_func
        self.rollback_file = os.path.join(
            os.path.expanduser("~"), "rollback_log.csv"
        )
        self._rollback_lock = threading.Lock()

    def _log_debug(self, message: str) -> None:
        """Log debug message."""
        self.log(f"[DEBUG] {message}", LogLevel.DEBUG)

    def _log_info(self, message: str) -> None:
        """Log info message."""
        self.log(f"[INFO] {message}", LogLevel.INFO)

    def _log_warning(self, message: str) -> None:
        """Log warning message."""
        self.log(f"[WARNING] {message}", LogLevel.WARNING)

    def _log_error(self, message: str) -> None:
        """Log error message."""
        self.log(f"[ERROR] {message}", LogLevel.ERROR)

    def log_rollback(
        self,
        item_type: str,
        item_id: int,
        item_name: str
    ) -> None:
        """Log created item for potential rollback."""
        with self._rollback_lock:
            try:
                directory = os.path.dirname(self.rollback_file)
                if directory and not os.path.exists(directory):
                    os.makedirs(directory)

                file_exists = os.path.isfile(self.rollback_file)

                with open(
                    self.rollback_file,
                    mode='a',
                    newline='',
                    encoding='utf-8'
                ) as file:
                    writer = csv.writer(file)
                    if not file_exists:
                        writer.writerow(['type', 'id', 'name', 'created_at'])
                    writer.writerow([
                        item_type,
                        item_id,
                        item_name,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ])

            except IOError as e:
                self._log_warning(f"Could not write to rollback log: {e}")
            except Exception as e:
                self._log_warning(f"Rollback logging error: {e}")

    def is_system_field(self, field: Dict) -> bool:
        """Determine if a field is a system field that should not be migrated."""
        field_type = field.get('type', 'unknown')

        if field_type not in ZendeskClient.ALLOWED_FIELD_TYPES:
            return True

        if field.get('removable') is False:
            return True

        field_key = field.get('key', '')
        if field_key and field_key in ZendeskClient.SYSTEM_FIELD_KEYS:
            return True

        if field.get('creator_user_id') == -1:
            return True

        field_tag = field.get('tag', '')
        if field_tag and field_tag in ZendeskClient.SYSTEM_FIELD_KEYS:
            return True

        return False

    def should_skip_field(self, field: Dict) -> Tuple[bool, str]:
        """
        Check if field should be skipped with reason.

        Returns (should_skip, reason)
        """
        field_type = field.get('type', '')

        # Check lookup fields
        if field_type == 'lookup':
            relationship_type = field.get('relationship_target_type', '')

            # No relationship type specified
            if not relationship_type:
                return True, "Lookup field missing relationship_target_type"

            # Check if it's a custom object lookup
            if relationship_type.startswith('zen:custom_object:'):
                custom_object_key = relationship_type.replace(
                    'zen:custom_object:', ''
                )
                return True, (
                    f"Lookup field references custom object '{custom_object_key}'. "
                    "Custom objects must be created in target instance first."
                )

            # Check if it's a valid system lookup
            if relationship_type not in ZendeskClient.SYSTEM_LOOKUP_TARGETS:
                return True, (
                    f"Lookup field has unknown relationship type: {relationship_type}"
                )

        return False, ""

    def get_lookup_field_info(self, field: Dict) -> Dict[str, Any]:
        """
        Get detailed information about a lookup field.

        Returns dict with:
            - is_lookup: bool
            - target_type: str (user, organization, ticket, custom_object)
            - custom_object_key: str or None
            - is_migratable: bool
            - reason: str
        """
        info = {
            'is_lookup': False,
            'target_type': None,
            'custom_object_key': None,
            'is_migratable': True,
            'reason': ''
        }

        if field.get('type') != 'lookup':
            return info

        info['is_lookup'] = True
        relationship_type = field.get('relationship_target_type', '')

        if not relationship_type:
            info['is_migratable'] = False
            info['reason'] = 'Missing relationship_target_type'
            return info

        if relationship_type == 'zen:user':
            info['target_type'] = 'user'
        elif relationship_type == 'zen:organization':
            info['target_type'] = 'organization'
        elif relationship_type == 'zen:ticket':
            info['target_type'] = 'ticket'
        elif relationship_type.startswith('zen:custom_object:'):
            info['target_type'] = 'custom_object'
            info['custom_object_key'] = relationship_type.replace(
                'zen:custom_object:', ''
            )
            info['is_migratable'] = False
            info['reason'] = (
                f"References custom object '{info['custom_object_key']}' "
                "which may not exist in target"
            )
        else:
            info['target_type'] = 'unknown'
            info['is_migratable'] = False
            info['reason'] = f"Unknown relationship type: {relationship_type}"

        return info

    def prepare_payload(self, field: Dict, object_key: str) -> Dict:
        """Prepare API payload from field data."""
        field_data = {
            'type': field.get('type'),
            'title': field.get('title'),
            'description': field.get('description', ''),
            'active': field.get('active', True),
        }

        if field.get('custom_field_options'):
            field_data['custom_field_options'] = [
                {
                    'name': opt['name'],
                    'value': opt['value'],
                    'default': opt.get('default', False)
                }
                for opt in field['custom_field_options']
            ]

        # Handle lookup fields - only add if it's a system lookup
        if field.get('type') == 'lookup':
            relationship_type = field.get('relationship_target_type', '')
            if relationship_type in ZendeskClient.SYSTEM_LOOKUP_TARGETS:
                field_data['relationship_target_type'] = relationship_type
            else:
                self._log_warning(
                    f"Lookup field '{field.get('title')}' has non-system "
                    f"relationship type: {relationship_type}"
                )

        if object_key == 'ticket_field':
            self._add_ticket_field_attributes(field, field_data)
        elif object_key in ('user_field', 'organization_field'):
            self._add_user_org_field_attributes(field, field_data)

        return {object_key: field_data}

    def _add_ticket_field_attributes(
        self,
        source: Dict,
        target: Dict
    ) -> None:
        """Add ticket field specific attributes to payload."""
        if source.get('tag'):
            target['tag'] = source['tag']

        optional_attrs = [
            'required',
            'required_in_portal',
            'visible_in_portal',
            'editable_in_portal',
            'title_in_portal',
            'agent_description',
            'regexp_for_validation'
        ]

        for attr in optional_attrs:
            if attr in source:
                value = source[attr]

                if isinstance(value, str):
                    value = value.strip()

                if attr in ('regexp_for_validation', 'agent_description'):
                    if not value:
                        continue

                target[attr] = value

    def _add_user_org_field_attributes(
        self,
        source: Dict,
        target: Dict
    ) -> None:
        """Add user/organization field specific attributes to payload."""
        if source.get('key'):
            target['key'] = source['key']
        else:
            title = source.get('title', '')
            safe_key = self._generate_safe_key(title)
            target['key'] = safe_key
            self._log_warning(
                f"Missing key for '{title}', auto-generated: {safe_key}"
            )

        if 'regexp_for_validation' in source:
            value = source['regexp_for_validation']
            if isinstance(value, str):
                value = value.strip()
            if value:
                target['regexp_for_validation'] = value

    def _generate_safe_key(self, title: str) -> str:
        """Generate a safe, unique key from a title."""
        base_key = re.sub(r'[^a-z0-9]', '_', title.lower())
        base_key = re.sub(r'_+', '_', base_key)
        base_key = base_key.strip('_')

        hash_suffix = hashlib.md5(
            f"{title}{time.time()}".encode()
        ).hexdigest()[:6]

        return f"{base_key}_{hash_suffix}"


# ==========================================
# 3. LOG FILE MANAGER
# ==========================================

class LogFileManager:
    """Manages writing log messages to a file."""

    def __init__(self, filepath: str = ""):
        """Initialize log file manager."""
        self._filepath = filepath
        self._lock = threading.Lock()
        self._file_handle = None
        self._enabled = False

    @property
    def filepath(self) -> str:
        """Get current log file path."""
        return self._filepath

    @filepath.setter
    def filepath(self, value: str) -> None:
        """Set log file path and reinitialize."""
        with self._lock:
            self._close_file()
            self._filepath = value
            self._enabled = bool(value)

    def _open_file(self) -> bool:
        """Open log file for appending."""
        if not self._filepath:
            return False

        try:
            directory = os.path.dirname(self._filepath)
            if directory and not os.path.exists(directory):
                os.makedirs(directory)

            self._file_handle = open(
                self._filepath,
                mode='a',
                encoding='utf-8'
            )
            return True

        except IOError as e:
            print(f"Failed to open log file: {e}")
            return False

    def _close_file(self) -> None:
        """Close log file handle."""
        if self._file_handle:
            try:
                self._file_handle.close()
            except IOError:
                pass
            self._file_handle = None

    def write(self, message: str, level: LogLevel = LogLevel.INFO) -> bool:
        """Write message to log file with level prefix."""
        if not self._enabled or not self._filepath:
            return False

        with self._lock:
            try:
                if not self._file_handle:
                    if not self._open_file():
                        return False

                assert self._file_handle is not None
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                level_name = level.name if level != LogLevel.ALWAYS else "NOTICE"
                self._file_handle.write(
                    f"[{timestamp}] [{level_name}] {message}\n"
                )
                self._file_handle.flush()
                return True

            except IOError as e:
                print(f"Failed to write to log file: {e}")
                self._close_file()
                return False

    def close(self) -> None:
        """Close log file and cleanup."""
        with self._lock:
            self._close_file()

    def __del__(self):
        """Destructor to ensure file is closed."""
        self.close()


# ==========================================
# 4. GUI APPLICATION
# ==========================================

class ZendeskMigratorApp:
    """Main application class for Zendesk Migration Tool."""

    VERSION = "1.2.0"

    # High contrast color coding for log levels
    LOG_COLORS = {
        LogLevel.DEBUG: "#6B8E23",      # Olive Green
        LogLevel.INFO: "#0000CD",       # Medium Blue
        LogLevel.WARNING: "#FF8C00",    # Dark Orange
        LogLevel.ERROR: "#DC143C",      # Crimson Red
        LogLevel.CRITICAL: "#8B008B",   # Dark Magenta/Purple
        LogLevel.ALWAYS: "#2F4F4F",     # Dark Slate Gray
    }

    # Legend display names (full names)
    LOG_LEGEND = {
        LogLevel.DEBUG: "DEBUG",
        LogLevel.INFO: "INFO",
        LogLevel.WARNING: "WARNING",
        LogLevel.ERROR: "ERROR",
        LogLevel.CRITICAL: "CRITICAL",
    }

    def __init__(self, root: tk.Tk):
        """Initialize the application."""
        self.root = root
        self.root.title(f"Zendesk Migration Tool [v{self.VERSION}]")

        self.source_data: Dict[str, List] = {}
        self.target_data: Dict[str, List] = {}
        self.analysis_results: Optional[AnalysisResults] = None
        self._analysis_lock = threading.Lock()

        # Process control
        self._process_state = ProcessState.IDLE
        self._stop_event = threading.Event()
        self._current_thread: Optional[threading.Thread] = None

        # Log file manager
        self.log_file_manager = LogFileManager()

        # UI state
        self.show_tokens = False
        self.log_level_var = tk.StringVar(
            value=LogLevel.get_display_name(LogLevel.INFO)
        )
        self.auto_load_var = tk.BooleanVar(value=True)

        self.setup_ui()
        self.log_queue: queue.Queue = queue.Queue()
        self.logic = MigrationLogic(self.log_with_level, self.update_progress)

        # Center window AFTER UI is built
        self._center_window()

        self.root.after(100, self.process_log_queue)
        self.init_config_path()
        self.log_with_level(
            f"[SYSTEM] App Ready (v{self.VERSION})",
            LogLevel.ALWAYS
        )

    def _center_window(self) -> None:
        """Center the application window on the screen using 90% of space."""
        # Update to get accurate screen info
        self.root.update_idletasks()

        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()

        # Use 90% of screen space
        window_width = int(screen_width * 0.9)
        window_height = int(screen_height * 0.9)

        # Calculate center position
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2

        # Set minimum size
        self.root.minsize(800, 600)

        # Set geometry: WIDTHxHEIGHT+X+Y
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")

    def setup_ui(self) -> None:
        """Setup all UI components."""
        style = ttk.Style()
        style.configure("Bold.TLabel", font=("Segoe UI", 9, "bold"))
        style.configure("Stop.TButton", foreground="red")

        self._setup_credentials_frame()
        self._setup_notebook_tabs()
        self._setup_log_frame()
        self._setup_progress_frame()

    def _setup_credentials_frame(self) -> None:
        """Setup the credentials input frame."""
        self.creds_frame = ttk.LabelFrame(
            self.root, text=" Credentials ", padding=10
        )
        self.creds_frame.pack(fill='x', padx=10, pady=5, side='top')

        ttk.Label(self.creds_frame, text="Source Domain:").grid(
            row=0, column=0, sticky='e'
        )
        self.src_domain = ttk.Entry(self.creds_frame, width=25)
        self.src_domain.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(self.creds_frame, text="Source Email:").grid(
            row=0, column=2, sticky='e'
        )
        self.src_email = ttk.Entry(self.creds_frame, width=25)
        self.src_email.grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(self.creds_frame, text="Source Token:").grid(
            row=0, column=4, sticky='e'
        )
        self.src_token = ttk.Entry(self.creds_frame, width=25, show="*")
        self.src_token.grid(row=0, column=5, padx=5, pady=2)

        ttk.Label(self.creds_frame, text="Target Domain:").grid(
            row=1, column=0, sticky='e'
        )
        self.tgt_domain = ttk.Entry(self.creds_frame, width=25)
        self.tgt_domain.grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(self.creds_frame, text="Target Email:").grid(
            row=1, column=2, sticky='e'
        )
        self.tgt_email = ttk.Entry(self.creds_frame, width=25)
        self.tgt_email.grid(row=1, column=3, padx=5, pady=2)

        ttk.Label(self.creds_frame, text="Target Token:").grid(
            row=1, column=4, sticky='e'
        )
        self.tgt_token = ttk.Entry(self.creds_frame, width=25, show="*")
        self.tgt_token.grid(row=1, column=5, padx=5, pady=2)

        self.toggle_btn = ttk.Button(
            self.creds_frame,
            text="👁 Show Tokens",
            command=self.toggle_token_visibility,
            width=12
        )
        self.toggle_btn.grid(row=0, column=6, rowspan=2, padx=10)

        # Config file row
        cfg_row_frame = ttk.Frame(self.creds_frame)
        cfg_row_frame.grid(
            row=2, column=0, columnspan=7, pady=(15, 5), sticky='ew'
        )

        ttk.Label(cfg_row_frame, text="Config File:").pack(side='left')
        self.config_path_var = tk.StringVar()
        self.config_entry = ttk.Entry(
            cfg_row_frame, textvariable=self.config_path_var
        )
        self.config_entry.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(
            cfg_row_frame, text="Browse", command=self.browse_config_file
        ).pack(side='left')

        # Log file row
        log_row_frame = ttk.Frame(self.creds_frame)
        log_row_frame.grid(
            row=3, column=0, columnspan=7, pady=(5, 5), sticky='ew'
        )

        ttk.Label(log_row_frame, text="Log File:").pack(side='left')
        self.log_file_path_var = tk.StringVar()
        self.log_file_path_var.trace_add('write', self._on_log_path_changed)
        self.log_file_entry = ttk.Entry(
            log_row_frame, textvariable=self.log_file_path_var
        )
        self.log_file_entry.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(
            log_row_frame, text="Browse", command=self.browse_log_file
        ).pack(side='left')
        ttk.Label(
            log_row_frame,
            text="(Leave empty to disable file logging)",
            foreground="gray"
        ).pack(side='left', padx=10)

        # Button frame
        btn_frame = ttk.Frame(self.creds_frame)
        btn_frame.grid(row=4, column=0, columnspan=7, pady=5, sticky='w')

        ttk.Button(
            btn_frame, text="Load Config", command=self.load_config
        ).pack(side='left', padx=5)

        ttk.Button(
            btn_frame, text="Save Config", command=self.save_config
        ).pack(side='left', padx=5)

        ttk.Separator(btn_frame, orient='vertical').pack(
            side='left', fill='y', padx=15
        )

        ttk.Checkbutton(
            btn_frame,
            text="Auto-load config on startup",
            variable=self.auto_load_var
        ).pack(side='left', padx=5)

    def _setup_notebook_tabs(self) -> None:
        """Setup the tabbed interface."""
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='x', expand=False, padx=10, pady=5)

        self.tab_migrate = ttk.Frame(self.notebook)
        self.tab_import = ttk.Frame(self.notebook)
        self.tab_rollback = ttk.Frame(self.notebook)
        self.tab_diff = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_migrate, text=" Analysis & Export ")
        self.notebook.add(self.tab_import, text=" Import & Execute ")
        self.notebook.add(self.tab_rollback, text=" Rollback ")
        self.notebook.add(self.tab_diff, text=" Diff Viewer ")

        self._setup_migrate_tab()
        self._setup_import_tab()
        self._setup_rollback_tab()
        self._setup_diff_tab()

    def _setup_migrate_tab(self) -> None:
        """Setup the Analysis & Export tab."""
        mig_ctrl_frame = ttk.Frame(self.tab_migrate, padding=15)
        mig_ctrl_frame.pack(fill='x')

        ttk.Button(
            mig_ctrl_frame,
            text="Step 1: Analyze Differences",
            command=self.start_analysis
        ).pack(side='left', padx=(5, 20))

        ttk.Button(
            mig_ctrl_frame,
            text="Step 2: Export to CSV",
            command=self.export_csv
        ).pack(side='left', padx=5)

    def _setup_import_tab(self) -> None:
        """Setup the Import & Execute tab."""
        imp_ctrl_frame = ttk.Frame(self.tab_import, padding=15)
        imp_ctrl_frame.pack(fill='x')

        ttk.Label(imp_ctrl_frame, text="Step 3: Select CSV").pack(anchor='w')

        file_sel_frame = ttk.Frame(imp_ctrl_frame)
        file_sel_frame.pack(fill='x', pady=5)

        self.csv_path_var = tk.StringVar()
        ttk.Entry(
            file_sel_frame, textvariable=self.csv_path_var
        ).pack(side='left', fill='x', expand=True, padx=(0, 5))

        ttk.Button(
            file_sel_frame, text="Browse", command=self.browse_csv
        ).pack(side='left')

        opt_frame = ttk.Frame(imp_ctrl_frame)
        opt_frame.pack(fill='x', pady=10)

        ttk.Label(opt_frame, text="Step 4: Strategy").pack(side='left')

        self.imp_strategy_var = tk.StringVar(value=ImportStrategy.SKIP.value)

        ttk.Radiobutton(
            opt_frame,
            text="Skip (Safe)",
            variable=self.imp_strategy_var,
            value=ImportStrategy.SKIP.value
        ).pack(side='left', padx=10)

        ttk.Radiobutton(
            opt_frame,
            text="Update (Overwrite)",
            variable=self.imp_strategy_var,
            value=ImportStrategy.UPDATE.value
        ).pack(side='left', padx=10)

        ttk.Radiobutton(
            opt_frame,
            text="Clone (Duplicate)",
            variable=self.imp_strategy_var,
            value=ImportStrategy.CLONE.value
        ).pack(side='left', padx=10)

        ttk.Button(
            opt_frame, text="RUN IMPORT", command=self.start_import
        ).pack(side='right', padx=5)

    def _setup_rollback_tab(self) -> None:
        """Setup the Rollback tab."""
        rb_ctrl_frame = ttk.Frame(self.tab_rollback, padding=15)
        rb_ctrl_frame.pack(fill='x')

        default_rb_path = os.path.join(
            os.path.expanduser("~"), "Downloads", "rollback_log.csv"
        )

        ttk.Label(rb_ctrl_frame, text="Log File:").pack(side='left')

        self.rb_path_var = tk.StringVar(value=default_rb_path)
        ttk.Entry(
            rb_ctrl_frame, textvariable=self.rb_path_var, width=40
        ).pack(side='left', padx=5)

        ttk.Button(
            rb_ctrl_frame, text="Browse", command=self.browse_rb
        ).pack(side='left')

        ttk.Button(
            rb_ctrl_frame, text="DELETE (UNDO)", command=self.start_rollback
        ).pack(side='right', padx=5)

    def _setup_diff_tab(self) -> None:
        """Setup the Diff Viewer tab."""
        # --- Filter toolbar ---
        filter_frame = ttk.Frame(self.tab_diff, padding=(10, 8))
        filter_frame.pack(fill='x')

        ttk.Label(filter_frame, text="Status:").pack(side='left')
        self.diff_status_var = tk.StringVar(value='All')
        ttk.Combobox(
            filter_frame,
            textvariable=self.diff_status_var,
            values=['All', 'New', 'Changed', 'Unchanged'],
            state='readonly',
            width=12,
        ).pack(side='left', padx=(4, 14))
        self.diff_status_var.trace_add('write', lambda *_: self._apply_diff_filter())

        ttk.Label(filter_frame, text="Type:").pack(side='left')
        self.diff_type_var = tk.StringVar(value='All')
        ttk.Combobox(
            filter_frame,
            textvariable=self.diff_type_var,
            values=['All', 'Ticket Fields', 'User Fields', 'Org Fields', 'Forms'],
            state='readonly',
            width=15,
        ).pack(side='left', padx=(4, 14))
        self.diff_type_var.trace_add('write', lambda *_: self._apply_diff_filter())

        ttk.Button(
            filter_frame,
            text='Refresh',
            command=self._populate_diff_viewer,
        ).pack(side='left', padx=8)

        ttk.Button(
            filter_frame,
            text='Export to CSV',
            command=self.export_diff_csv,
        ).pack(side='left', padx=(0, 8))

        self.diff_summary_var = tk.StringVar(
            value='Run Step 1: Analyze Differences to populate this view.'
        )
        ttk.Label(
            filter_frame,
            textvariable=self.diff_summary_var,
            foreground='gray',
        ).pack(side='right', padx=10)

        # --- Treeview + scrollbars ---
        tree_frame = ttk.Frame(self.tab_diff)
        tree_frame.pack(fill='both', expand=True, padx=10, pady=(0, 8))

        cols = ('obj_type', 'name', 'status', 'attribute', 'target_val', 'source_val')
        self.diff_tree = ttk.Treeview(
            tree_frame,
            columns=cols,
            show='headings',
            height=12,
            selectmode='browse',
        )
        self.diff_tree.heading('obj_type', text='Type')
        self.diff_tree.heading('name', text='Name')
        self.diff_tree.heading('status', text='Status')
        self.diff_tree.heading('attribute', text='Attribute')
        self.diff_tree.heading('target_val', text='Source')
        self.diff_tree.heading('source_val', text='Target')

        self.diff_tree.column('obj_type', width=120, minwidth=80, anchor='w')
        self.diff_tree.column('name', width=200, minwidth=120, anchor='w')
        self.diff_tree.column('status', width=90, minwidth=70, anchor='center')
        self.diff_tree.column('attribute', width=160, minwidth=100, anchor='w')
        self.diff_tree.column('target_val', width=220, minwidth=120, anchor='w')
        self.diff_tree.column('source_val', width=220, minwidth=120, anchor='w')

        self.diff_tree.tag_configure('new', background='#f8d7da', foreground='#721c24')
        self.diff_tree.tag_configure('attr_diff', background='#fff3cd', foreground='#856404')
        self.diff_tree.tag_configure('unchanged', background='#d4edda', foreground='#155724')

        vsb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.diff_tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.diff_tree.xview)
        self.diff_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.diff_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

    def _setup_log_frame(self) -> None:
        """Setup the log output frame."""
        log_frame = ttk.LabelFrame(self.root, text=" Log ", padding=5)
        log_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # Toolbar for log
        log_toolbar = ttk.Frame(log_frame)
        log_toolbar.pack(fill='x', pady=(0, 5))

        ttk.Button(
            log_toolbar,
            text="Clear Log",
            command=self.clear_log
        ).pack(side='left', padx=2)

        ttk.Button(
            log_toolbar,
            text="Save Log As...",
            command=self.save_log_as
        ).pack(side='left', padx=2)

        # Log level filter
        ttk.Separator(log_toolbar, orient='vertical').pack(
            side='left', fill='y', padx=10
        )

        ttk.Label(log_toolbar, text="Show Level:").pack(side='left', padx=(0, 5))

        level_values = [
            LogLevel.get_display_name(level)
            for level in LogLevel.get_filter_levels()
        ]

        self.log_level_combo = ttk.Combobox(
            log_toolbar,
            textvariable=self.log_level_var,
            values=level_values,
            state='readonly',
            width=25
        )
        self.log_level_combo.pack(side='left')
        self.log_level_combo.bind(
            '<<ComboboxSelected>>', self._on_log_level_changed
        )

        # Legend
        ttk.Separator(log_toolbar, orient='vertical').pack(
            side='left', fill='y', padx=10
        )

        legend_frame = ttk.Frame(log_toolbar)
        legend_frame.pack(side='left')

        ttk.Label(
            legend_frame, text="Legend:", font=("Segoe UI", 8, "bold")
        ).pack(side='left', padx=(0, 5))

        for level, display_name in self.LOG_LEGEND.items():
            color = self.LOG_COLORS[level]
            lbl = tk.Label(
                legend_frame,
                text=f" {display_name} ",
                font=("Consolas", 9, "bold"),
                fg=color,
                bg="#F0F0F0",
                relief="groove",
                padx=3,
                pady=1
            )
            lbl.pack(side='left', padx=2)

        # Log text area
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            height=15,
            state='disabled',
            font=("Consolas", 10),
            bg="#FFFFFF"
        )
        self.log_text.pack(fill='both', expand=True)

        # Configure text tags for colors
        for level, color in self.LOG_COLORS.items():
            self.log_text.tag_configure(
                f"level_{level.name}",
                foreground=color
            )

    def _on_log_level_changed(self, event=None) -> None:
        """Handle log level filter change."""
        level = self._get_current_log_level()
        self.log_with_level(
            f"[SYSTEM] Log level filter changed to: {level.name}",
            LogLevel.ALWAYS
        )

    def _get_current_log_level(self) -> LogLevel:
        """Get currently selected log level from combo box."""
        display_name = self.log_level_var.get()
        return LogLevel.from_string(display_name)

    def _setup_progress_frame(self) -> None:
        """Setup the progress bar and stop button."""
        progress_frame = ttk.Frame(self.root)
        progress_frame.pack(fill='x', padx=10, pady=(0, 5))

        self.stop_btn = ttk.Button(
            progress_frame,
            text="⏹ Stop",
            command=self.stop_process,
            width=10,
            state='disabled'
        )
        self.stop_btn.pack(side='right')

        self.status_label = ttk.Label(
            progress_frame,
            text="Ready",
            foreground="gray"
        )
        self.status_label.pack(side='right', padx=10)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame, variable=self.progress_var, maximum=100
        )
        self.progress_bar.pack(side='left', fill='x', expand=True, padx=(0, 10))

    def _on_log_path_changed(self, *args) -> None:
        """Handle log file path changes."""
        new_path = self.log_file_path_var.get()
        self.log_file_manager.filepath = new_path

    def toggle_token_visibility(self) -> None:
        """Toggle visibility of token fields."""
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

    def _should_show_message(self, level: LogLevel) -> bool:
        """Determine if a message should be displayed based on level filter."""
        if level == LogLevel.ALWAYS:
            return True

        filter_level = self._get_current_log_level()
        return level >= filter_level

    def log_with_level(
        self, message: str, level: LogLevel = LogLevel.INFO
    ) -> None:
        """Add message to log queue with level information."""
        self.log_queue.put((message, level))

    def log_queue_put(self, msg: Any) -> None:
        """Add message to log queue (for backward compatibility)."""
        if isinstance(msg, tuple) and len(msg) >= 3 and msg[0] == "POPUP":
            self.log_queue.put(msg)
        else:
            level = self._detect_log_level(str(msg))
            self.log_queue.put((str(msg), level))

    def _detect_log_level(self, message: str) -> LogLevel:
        """Detect log level from message content."""
        message_upper = message.upper()

        if '[CRITICAL]' in message_upper or '[FATAL]' in message_upper:
            return LogLevel.CRITICAL
        elif '[ERROR]' in message_upper or '[!]' in message or '[✗]' in message:
            return LogLevel.ERROR
        elif '[WARNING]' in message_upper or '[WARN]' in message_upper:
            return LogLevel.WARNING
        elif '[DEBUG]' in message_upper:
            return LogLevel.DEBUG
        elif '[INFO]' in message_upper:
            return LogLevel.INFO
        elif '[SYSTEM]' in message_upper or '====' in message or '----' in message:
            return LogLevel.ALWAYS

        return LogLevel.INFO

    def process_log_queue(self) -> None:
        """Process pending log messages from queue."""
        try:
            while not self.log_queue.empty():
                item = self.log_queue.get_nowait()

                # Handle special control messages
                if isinstance(item, tuple) and item[0] == "REFRESH_DIFF":
                    self.root.after(0, self._populate_diff_viewer)
                    continue

                if isinstance(item, tuple) and len(item) >= 3 and item[0] == "POPUP":
                    self.root.after(
                        0, lambda m=item: messagebox.showinfo(m[1], m[2])
                    )
                    continue

                # Handle regular log messages with level
                if isinstance(item, tuple) and len(item) == 2:
                    message, level = item
                else:
                    message = str(item)
                    level = self._detect_log_level(message)

                # Check if message should be shown in GUI
                if self._should_show_message(level):
                    self._write_to_log(message, level)

                # Always write to file regardless of filter
                self.log_file_manager.write(message, level)

        except queue.Empty:
            pass
        except Exception as e:
            print(f"Log queue processing error: {e}")

        self.root.after(100, self.process_log_queue)

    def _write_to_log(
        self, message: str, level: LogLevel = LogLevel.INFO
    ) -> None:
        """Write message to log text widget with color coding."""
        try:
            self.log_text.configure(state='normal')

            tag = f"level_{level.name}"
            self.log_text.insert(tk.END, message + "\n", tag)

            self.log_text.see(tk.END)
            self.log_text.configure(state='disabled')
        except tk.TclError as e:
            print(f"GUI log write error: {e}")

    def clear_log(self) -> None:
        """Clear the log text widget."""
        try:
            self.log_text.configure(state='normal')
            self.log_text.delete(1.0, tk.END)
            self.log_text.configure(state='disabled')
        except tk.TclError:
            pass

    def save_log_as(self) -> None:
        """Save current log content to a file."""
        filepath = filedialog.asksaveasfilename(
            title="Save Log As",
            defaultextension=".txt",
            filetypes=[
                ("Text Files", "*.txt"),
                ("Log Files", "*.log"),
                ("All Files", "*.*")
            ]
        )

        if not filepath:
            return

        try:
            log_content = self.log_text.get(1.0, tk.END)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(log_content)
            messagebox.showinfo("Success", f"Log saved to:\n{filepath}")
        except IOError as e:
            messagebox.showerror("Error", f"Failed to save log: {e}")

    def update_progress(self, current: int, total: int) -> None:
        """Update progress bar (thread-safe)."""
        if total > 0:
            pct = (current / total) * 100
            self.root.after(0, lambda: self.progress_var.set(pct))

    def reset_progress(self) -> None:
        """Reset progress bar to zero."""
        self.root.after(0, lambda: self.progress_var.set(0))

    def _set_process_state(
        self, state: ProcessState, status_text: str = ""
    ) -> None:
        """Update process state and UI accordingly."""
        self._process_state = state

        def update_ui():
            if state == ProcessState.IDLE:
                self.stop_btn.config(state='disabled')
                self.status_label.config(
                    text=status_text or "Ready",
                    foreground="gray"
                )
            elif state == ProcessState.RUNNING:
                self.stop_btn.config(state='normal')
                self.status_label.config(
                    text=status_text or "Running...",
                    foreground="blue"
                )
            elif state == ProcessState.STOPPING:
                self.stop_btn.config(state='disabled')
                self.status_label.config(text="Stopping...", foreground="orange")

        self.root.after(0, update_ui)

    def stop_process(self) -> None:
        """Request stop of current running process."""
        if self._process_state == ProcessState.RUNNING:
            self._set_process_state(ProcessState.STOPPING)
            self._stop_event.set()
            self.log_with_level(
                "[SYSTEM] Stop requested. Waiting for current operation...",
                LogLevel.ALWAYS
            )

    def _is_stop_requested(self) -> bool:
        """Check if stop has been requested."""
        return self._stop_event.is_set()

    def _start_background_task(
        self,
        target: Callable,
        args: tuple,
        task_name: str
    ) -> bool:
        """Start a background task with proper state management."""
        if self._process_state != ProcessState.IDLE:
            messagebox.showwarning(
                "Busy",
                "Another operation is in progress. Please wait or stop it first."
            )
            return False

        self._stop_event.clear()
        self.reset_progress()
        self._set_process_state(ProcessState.RUNNING, task_name)

        self._current_thread = threading.Thread(
            target=self._task_wrapper,
            args=(target, args, task_name),
            daemon=True
        )
        self._current_thread.start()
        return True

    def _task_wrapper(
        self,
        target: Callable,
        args: tuple,
        task_name: str
    ) -> None:
        """Wrapper for background tasks to handle completion."""
        try:
            target(*args)
        finally:
            was_stopped = self._stop_event.is_set()
            self._stop_event.clear()

            if was_stopped:
                status = f"{task_name} - Stopped"
                self.log_with_level(
                    f"[SYSTEM] {task_name} was stopped by user",
                    LogLevel.ALWAYS
                )
            else:
                status = "Ready"

            self._set_process_state(ProcessState.IDLE, status)

    def browse_csv(self) -> None:
        """Browse for CSV file."""
        filepath = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if filepath:
            if not filepath.lower().endswith('.csv'):
                messagebox.showwarning(
                    "Warning",
                    "Selected file does not have .csv extension"
                )
            self.csv_path_var.set(filepath)

    def browse_rb(self) -> None:
        """Browse for rollback log file."""
        filepath = filedialog.askopenfilename(
            title="Select Rollback Log",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        if filepath:
            self.rb_path_var.set(filepath)

    def browse_config_file(self) -> None:
        """Browse for config file."""
        filepath = filedialog.askopenfilename(
            title="Select Config File",
            filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")]
        )
        if filepath:
            self.config_path_var.set(filepath)

    def browse_log_file(self) -> None:
        """Browse for log file location."""
        filepath = filedialog.asksaveasfilename(
            title="Select Log File Location",
            defaultextension=".log",
            filetypes=[
                ("Log Files", "*.log"),
                ("Text Files", "*.txt"),
                ("All Files", "*.*")
            ]
        )
        if filepath:
            self.log_file_path_var.set(filepath)

    def _check_auto_load_setting(self, config_path: str) -> bool:
        """
        Check if auto-load is enabled in a config file without fully loading it.

        Args:
            config_path: Path to the config file

        Returns:
            True if auto-load is enabled (or not specified), False otherwise
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('auto_load_config', True)
        except (IOError, json.JSONDecodeError, KeyError):
            return True  # Default to True if can't read

    @staticmethod
    def _get_app_config_path() -> str:
        """Return the default config.json path next to the app/executable."""
        if getattr(sys, 'frozen', False):
            app_dir = os.path.dirname(sys.executable)
        else:
            app_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(app_dir, 'config.json')

    def init_config_path(self) -> None:
        """Initialize config file path, optionally loading if auto-load is enabled."""
        current_dir_path = self._get_app_config_path()
        user_home_path = os.path.join(os.path.expanduser("~"), 'config.json')

        config_path = None

        # Find config file
        if os.path.exists(current_dir_path):
            config_path = current_dir_path
        elif os.path.exists(user_home_path):
            config_path = user_home_path

        if config_path:
            self.config_path_var.set(config_path)

            # Check if auto-load is enabled
            if self._check_auto_load_setting(config_path):
                self.load_config()
                self.log_with_level(
                    f"[SYSTEM] Auto-loaded config from: {config_path}",
                    LogLevel.ALWAYS
                )
            else:
                self.log_with_level(
                    f"[SYSTEM] Config found but auto-load disabled: {config_path}",
                    LogLevel.INFO
                )
                self.log_with_level(
                    "[SYSTEM] Click 'Load Config' to load manually.",
                    LogLevel.INFO
                )
        else:
            self.config_path_var.set(user_home_path)
            self.log_with_level(
                "[SYSTEM] No config file found. Using defaults.",
                LogLevel.INFO
            )

    def load_config(self) -> None:
        """Load configuration from file (plain text JSON)."""
        path = self.config_path_var.get()

        if not path or not os.path.exists(path):
            self.log_with_level(
                f"[WARNING] Config file not found: {path}",
                LogLevel.WARNING
            )
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            source_data = data.get('source_creds', {})
            target_data = data.get('target_creds', {})

            self._set_entry_value(
                self.src_domain, source_data.get('subdomain', '')
            )
            self._set_entry_value(
                self.src_email, source_data.get('email', '')
            )
            self._set_entry_value(
                self.src_token, source_data.get('token', '')
            )

            self._set_entry_value(
                self.tgt_domain, target_data.get('subdomain', '')
            )
            self._set_entry_value(
                self.tgt_email, target_data.get('email', '')
            )
            self._set_entry_value(
                self.tgt_token, target_data.get('token', '')
            )

            rollback_path = data.get('rollback_filename', '')
            if rollback_path:
                self.rb_path_var.set(rollback_path)

            log_file_path = data.get('log_filename', '')
            self.log_file_path_var.set(log_file_path)

            log_level_name = data.get('log_level', 'INFO')
            level = LogLevel.from_string(log_level_name)
            self.log_level_var.set(LogLevel.get_display_name(level))

            # Load auto-load setting
            auto_load = data.get('auto_load_config', True)
            self.auto_load_var.set(auto_load)

            self.log_with_level(
                f"[INFO] Config loaded from {path}",
                LogLevel.INFO
            )

            # If this file is not the default config.json, write the loaded
            # settings back to config.json so the next startup picks them up.
            app_config = self._get_app_config_path()
            if os.path.normpath(path) != os.path.normpath(app_config):
                try:
                    with open(app_config, 'w', encoding='utf-8') as fw:
                        json.dump(data, fw, indent=4)
                    self.config_path_var.set(app_config)
                    self.log_with_level(
                        f"[INFO] Default config updated: {app_config}",
                        LogLevel.INFO
                    )
                except IOError as write_err:
                    self.log_with_level(
                        f"[WARNING] Could not update default config: {write_err}",
                        LogLevel.WARNING
                    )

        except json.JSONDecodeError as e:
            self.log_with_level(
                f"[ERROR] Invalid JSON in config: {e}",
                LogLevel.ERROR
            )
        except KeyError as e:
            self.log_with_level(
                f"[ERROR] Missing config key: {e}",
                LogLevel.ERROR
            )
        except Exception as e:
            self.log_with_level(
                f"[ERROR] Config load failed: {e}",
                LogLevel.ERROR
            )

    @staticmethod
    def _set_entry_value(entry: ttk.Entry, value: str) -> None:
        """Set entry widget value."""
        entry.delete(0, tk.END)
        entry.insert(0, value)

    def save_config(self) -> None:
        """Save configuration to file (plain text JSON)."""
        path = self.config_path_var.get()

        if not path:
            messagebox.showerror("Error", "No config file path specified")
            return

        try:
            current_level = self._get_current_log_level()

            data = {
                "source_creds": {
                    "subdomain": self.src_domain.get(),
                    "email": self.src_email.get(),
                    "token": self.src_token.get()
                },
                "target_creds": {
                    "subdomain": self.tgt_domain.get(),
                    "email": self.tgt_email.get(),
                    "token": self.tgt_token.get()
                },
                "rollback_filename": self.rb_path_var.get(),
                "log_filename": self.log_file_path_var.get(),
                "log_level": current_level.name,
                "auto_load_config": self.auto_load_var.get()
            }

            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)

            self.log_with_level(
                f"[INFO] Config saved to {path}",
                LogLevel.INFO
            )
            messagebox.showinfo("Saved", "Configuration saved successfully.")

        except IOError as e:
            messagebox.showerror("Error", f"Failed to write config: {e}")
        except Exception as e:
            messagebox.showerror("Error", f"Save failed: {e}")

    def get_credentials(
        self,
        target_only: bool = False
    ) -> Optional[Tuple[Optional[Credentials], Credentials]]:
        """Get credentials from UI fields."""
        target_creds = Credentials(
            subdomain=self.tgt_domain.get(),
            email=self.tgt_email.get(),
            token=self.tgt_token.get()
        )

        if target_only:
            if not target_creds.is_valid():
                messagebox.showerror("Error", "Missing target credentials")
                return None
            return None, target_creds

        source_creds = Credentials(
            subdomain=self.src_domain.get(),
            email=self.src_email.get(),
            token=self.src_token.get()
        )

        if not source_creds.is_valid() or not target_creds.is_valid():
            messagebox.showerror("Error", "Missing credentials")
            return None

        return source_creds, target_creds

    # ==========================================
    # THREADED OPERATIONS
    # ==========================================

    def start_analysis(self) -> None:
        """Start analysis in background thread."""
        creds = self.get_credentials()
        if not creds:
            return

        source_creds, target_creds = creds

        self._start_background_task(
            self._run_analysis_thread,
            (source_creds, target_creds),
            "Analysis"
        )

    def _run_analysis_thread(
        self,
        source_creds: Credentials,
        target_creds: Credentials
    ) -> None:
        """Execute analysis operation."""
        try:
            self.log_with_level("=" * 60, LogLevel.ALWAYS)
            self.log_with_level("STARTING ANALYSIS", LogLevel.ALWAYS)
            self.log_with_level("=" * 60, LogLevel.ALWAYS)

            source_client = ZendeskClient(
                source_creds.subdomain,
                source_creds.email,
                source_creds.token,
                self.log_with_level,
                verbose=True,
                stop_check=self._is_stop_requested
            )

            target_client = ZendeskClient(
                target_creds.subdomain,
                target_creds.email,
                target_creds.token,
                self.log_with_level,
                verbose=True,
                stop_check=self._is_stop_requested
            )

            if self._is_stop_requested():
                return

            self.log_with_level(
                "[INFO] Fetching source data...",
                LogLevel.INFO
            )
            self.source_data = {
                'ticket_fields': source_client.get_all(
                    "ticket_fields.json", "ticket_fields"
                ),
                'user_fields': source_client.get_all(
                    "user_fields.json", "user_fields"
                ),
                'organization_fields': source_client.get_all(
                    "organization_fields.json", "organization_fields"
                ),
                'ticket_forms': source_client.get_all(
                    "ticket_forms.json", "ticket_forms"
                )
            }

            if self._is_stop_requested():
                return

            self.log_with_level(
                "[INFO] Fetching target data...",
                LogLevel.INFO
            )
            self.target_data = {
                'ticket_fields': target_client.get_all(
                    "ticket_fields.json", "ticket_fields"
                ),
                'user_fields': target_client.get_all(
                    "user_fields.json", "user_fields"
                ),
                'organization_fields': target_client.get_all(
                    "organization_fields.json", "organization_fields"
                ),
                'ticket_forms': target_client.get_all(
                    "ticket_forms.json", "ticket_forms"
                )
            }

            if self._is_stop_requested():
                return

            target_maps = self._build_target_maps()

            with self._analysis_lock:
                self.analysis_results = self._analyze_differences(target_maps)

            self.log_with_level("-" * 60, LogLevel.ALWAYS)
            self.log_with_level("ANALYSIS RESULTS:", LogLevel.ALWAYS)
            self.log_with_level(
                f"  New Fields:       {len(self.analysis_results.new_fields)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"  Unchanged Fields: {len(self.analysis_results.existing_fields)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"  Changed Fields:   {len(self.analysis_results.changed_fields)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"  New Forms:        {len(self.analysis_results.new_forms)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"  Unchanged Forms:  {len(self.analysis_results.existing_forms)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"  Changed Forms:    {len(self.analysis_results.changed_forms)}",
                LogLevel.ALWAYS
            )
            self.log_with_level("-" * 60, LogLevel.ALWAYS)

            for entry in self.analysis_results.changed_fields:
                field_title = entry['source'].get('title', '?')
                field_type = entry['type']
                self.log_with_level(
                    f"[INFO] Field '{field_title}' ({field_type}) has differences:",
                    LogLevel.INFO
                )
                for attr, diff in entry['diffs'].items():
                    if attr == 'custom_field_options':
                        opts = diff
                        self.log_with_level(
                            f"[INFO]   - options: "
                            f"{len(opts['added'])} added, "
                            f"{len(opts['removed'])} removed, "
                            f"{len(opts['renamed'])} renamed",
                            LogLevel.INFO
                        )
                    else:
                        src_val, tgt_val = diff
                        self.log_with_level(
                            f"[INFO]   - {attr}: {tgt_val!r} → {src_val!r}",
                            LogLevel.INFO
                        )

            for entry in self.analysis_results.changed_forms:
                form_name = entry['source'].get('name', '?')
                self.log_with_level(
                    f"[INFO] Form '{form_name}' has differences:",
                    LogLevel.INFO
                )
                for attr, (src_val, tgt_val) in entry['diffs'].items():
                    self.log_with_level(
                        f"[INFO]   - {attr}: {tgt_val!r} → {src_val!r}",
                        LogLevel.INFO
                    )

            if not self._is_stop_requested():
                self.log_queue.put(("REFRESH_DIFF",))
                self.log_queue.put(
                    ("POPUP", "Complete", "Analysis finished successfully.")
                )

        except requests.exceptions.RequestException as e:
            self.log_with_level(
                f"[ERROR] Network error during analysis: {e}",
                LogLevel.ERROR
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )

        except Exception as e:
            self.log_with_level(
                f"[CRITICAL] Unexpected error: {e}",
                LogLevel.CRITICAL
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )

    def _build_target_maps(self) -> Dict[str, Dict]:
        """Build lookup maps for target instance data."""
        maps = {}

        for field_type in ['ticket_fields', 'user_fields', 'organization_fields']:
            field_map = {}
            for tgt_obj in self.target_data.get(field_type, []):
                key = (tgt_obj.get('title', '').lower(), tgt_obj.get('type', ''))
                if key not in field_map:
                    field_map[key] = tgt_obj['id']
            maps[field_type] = field_map

        maps['ticket_forms'] = {
            form.get('name', '').lower(): form['id']
            for form in self.target_data.get('ticket_forms', [])
        }

        return maps

    def _analyze_differences(
        self,
        target_maps: Dict[str, Dict]
    ) -> AnalysisResults:
        """Analyze differences between source and target."""
        results = AnalysisResults()

        # Build id → full-object maps so we can do deep attribute comparison.
        target_objects: Dict[str, Dict[int, Dict]] = {}
        for type_key in ['ticket_fields', 'user_fields', 'organization_fields']:
            target_objects[type_key] = {
                obj['id']: obj
                for obj in self.target_data.get(type_key, [])
            }
        target_objects['ticket_forms'] = {
            obj['id']: obj
            for obj in self.target_data.get('ticket_forms', [])
        }

        for type_key in ['ticket_fields', 'user_fields', 'organization_fields']:
            for src_field in self.source_data.get(type_key, []):
                if self.logic.is_system_field(src_field):
                    continue

                lookup_key = (
                    src_field.get('title', '').lower(),
                    src_field.get('type', '')
                )

                if lookup_key in target_maps[type_key]:
                    target_id = target_maps[type_key][lookup_key]
                    target_field = target_objects[type_key].get(target_id, {})
                    diffs = self._diff_field(src_field, target_field, type_key)
                    entry = {
                        'source': src_field,
                        'target_id': target_id,
                        'type': type_key[:-1],
                        'list_key': type_key,
                        'diffs': diffs,
                    }
                    if diffs:
                        results.changed_fields.append(entry)
                    else:
                        results.existing_fields.append(entry)
                else:
                    results.new_fields.append({
                        'source': src_field,
                        'type': type_key[:-1],
                        'list_key': type_key
                    })

        for form in self.source_data.get('ticket_forms', []):
            if form.get('name') == "Default Ticket Form":
                continue

            form_name_lower = form.get('name', '').lower()

            if form_name_lower in target_maps['ticket_forms']:
                target_id = target_maps['ticket_forms'][form_name_lower]
                target_form = target_objects['ticket_forms'].get(target_id, {})
                diffs = self._diff_form(form, target_form)
                entry = {
                    'source': form,
                    'target_id': target_id,
                    'diffs': diffs,
                }
                if diffs:
                    results.changed_forms.append(entry)
                else:
                    results.existing_forms.append(entry)
            else:
                results.new_forms.append({
                    'source': form
                })

        return results

    @staticmethod
    def _diff_field(
        source: Dict,
        target: Dict,
        field_type: str
    ) -> Dict[str, Any]:
        """Compare source and target field dicts.

        Returns a dict of {attr: (source_val, target_val)} for every attribute
        that differs between the two objects.  Options (tagger/multiselect) are
        reported as {added, removed, renamed} sets of tag values.
        """
        # String attributes where None and "" are semantically identical.
        string_attrs = frozenset({
            'description', 'agent_description', 'title_in_portal',
            'regexp_for_validation', 'tag', 'key', 'relationship_target_type',
        })

        def _cmp(val: Any, attr: str) -> Any:
            """Normalise None → '' for string attributes before comparing."""
            return '' if (val is None and attr in string_attrs) else val

        diffs: Dict[str, Any] = {}

        for attr in ('description', 'active'):
            src_val = source.get(attr)
            tgt_val = target.get(attr)
            if _cmp(src_val, attr) != _cmp(tgt_val, attr):
                diffs[attr] = (src_val, tgt_val)

        if field_type == 'ticket_fields':
            for attr in (
                'required', 'required_in_portal', 'visible_in_portal',
                'editable_in_portal', 'title_in_portal', 'agent_description',
                'regexp_for_validation', 'tag',
            ):
                src_val = source.get(attr)
                tgt_val = target.get(attr)
                if _cmp(src_val, attr) != _cmp(tgt_val, attr):
                    diffs[attr] = (src_val, tgt_val)

        elif field_type in ('user_fields', 'organization_fields'):
            for attr in ('key', 'regexp_for_validation'):
                src_val = source.get(attr)
                tgt_val = target.get(attr)
                if _cmp(src_val, attr) != _cmp(tgt_val, attr):
                    diffs[attr] = (src_val, tgt_val)

        if source.get('type') == 'lookup':
            src_val = source.get('relationship_target_type')
            tgt_val = target.get('relationship_target_type')
            if _cmp(src_val, 'relationship_target_type') != _cmp(tgt_val, 'relationship_target_type'):
                diffs['relationship_target_type'] = (src_val, tgt_val)

        if source.get('type') in ('tagger', 'multiselect'):
            src_opts = {
                o['value']: o['name']
                for o in source.get('custom_field_options', [])
            }
            tgt_opts = {
                o['value']: o['name']
                for o in target.get('custom_field_options', [])
            }
            added = [v for v in src_opts if v not in tgt_opts]
            removed = [v for v in tgt_opts if v not in src_opts]
            renamed = [
                v for v in src_opts
                if v in tgt_opts and src_opts[v] != tgt_opts[v]
            ]
            if added or removed or renamed:
                diffs['custom_field_options'] = {
                    'added': added,
                    'removed': removed,
                    'renamed': renamed,
                }

        return diffs

    @staticmethod
    def _diff_form(source: Dict, target: Dict) -> Dict[str, Any]:
        """Compare source and target form dicts.

        Returns a dict of {attr: (source_val, target_val)} for every attribute
        that differs.
        """
        diffs: Dict[str, Any] = {}
        for attr in ('display_name', 'end_user_visible', 'active'):
            src_val = source.get(attr)
            tgt_val = target.get(attr)
            # Treat None and "" as equivalent for string attributes.
            src_cmp = '' if (src_val is None and attr == 'display_name') else src_val
            tgt_cmp = '' if (tgt_val is None and attr == 'display_name') else tgt_val
            if src_cmp != tgt_cmp:
                diffs[attr] = (src_val, tgt_val)
        return diffs

    @staticmethod
    def _humanize_conditions(
        conditions: List[Dict],
        field_id_to_title: Dict[int, str]
    ) -> List[Dict]:
        """Replace numeric field IDs with titles in a conditions list.

        Conditions whose parent or all child fields cannot be resolved to a
        title are silently dropped — they would be unremappable on import.
        """
        result = []
        for cond in conditions:
            parent_title = field_id_to_title.get(cond.get('parent_field_id', 0))
            if not parent_title:
                continue

            child_fields = []
            for cf in cond.get('child_fields', []):
                child_title = field_id_to_title.get(cf.get('id'))
                if child_title:
                    child_fields.append({
                        'title': child_title,
                        'is_required': cf.get('is_required', False)
                    })

            if child_fields:
                result.append({
                    'parent_field_title': parent_title,
                    'value': cond.get('value', ''),
                    'child_fields': child_fields
                })
        return result

    def _serialize_form_conditions(
        self,
        form: Dict,
        field_id_to_title: Dict[int, str]
    ) -> str:
        """Serialise a form's agent/end_user conditions as a JSON string.

        Returns an empty string when the form has no conditions so that the
        CSV cell is blank and backward-compatible with old import flows.
        """
        agent = self._humanize_conditions(
            form.get('agent_conditions', []), field_id_to_title
        )
        end_user = self._humanize_conditions(
            form.get('end_user_conditions', []), field_id_to_title
        )
        if not agent and not end_user:
            return ''
        return json.dumps(
            {'agent': agent, 'end_user': end_user}, ensure_ascii=False
        )

    def export_csv(self) -> None:
        """Export analysis results to CSV file."""
        with self._analysis_lock:
            if not self.analysis_results or not self.analysis_results.has_data():
                messagebox.showwarning(
                    "No Data",
                    "Run Analysis first to generate data for export."
                )
                return

            results = copy.deepcopy(self.analysis_results)

        filepath = filedialog.asksaveasfilename(
            title="Save Export CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")]
        )

        if not filepath:
            return

        try:
            self._write_export_csv(filepath, results)
            self.log_with_level(
                f"[INFO] Exported to {filepath}",
                LogLevel.INFO
            )
            messagebox.showinfo("Success", f"Data exported to:\n{filepath}")

        except IOError as e:
            self.log_with_level(
                f"[ERROR] Export failed: {e}",
                LogLevel.ERROR
            )
            messagebox.showerror("Error", f"Export failed: {e}")

        except Exception as e:
            self.log_with_level(
                f"[ERROR] Export failed: {e}",
                LogLevel.ERROR
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )

    def export_diff_csv(self) -> None:
        """Export only new and changed objects to a focused diff CSV."""
        with self._analysis_lock:
            if not self.analysis_results or not self.analysis_results.has_data():
                messagebox.showwarning(
                    "No Data",
                    "Run Analysis first to generate diff data for export."
                )
                return
            results = copy.deepcopy(self.analysis_results)

        filepath = filedialog.asksaveasfilename(
            title="Save Diff CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not filepath:
            return

        try:
            self._write_diff_csv(filepath, results)
            self.log_with_level(
                f"[INFO] Diff exported to {filepath}", LogLevel.INFO
            )
            messagebox.showinfo("Success", f"Diff exported to:\n{filepath}")
        except IOError as e:
            self.log_with_level(
                f"[ERROR] Diff export failed: {e}", LogLevel.ERROR
            )
            messagebox.showerror("Error", f"Diff export failed: {e}")
        except Exception as e:
            self.log_with_level(
                f"[ERROR] Diff export failed: {e}", LogLevel.ERROR
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}", LogLevel.DEBUG
            )

    def _write_diff_csv(self, filepath: str, results: AnalysisResults) -> None:
        """Write a diff CSV containing new, changed, and unchanged objects."""
        with open(filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Object Type', 'Name', 'Status',
                'Attribute', 'Source Value', 'Target Value'
            ])

            for entry in results.new_fields:
                writer.writerow([
                    entry.get('type', ''), entry['source'].get('title', ''),
                    'NEW', '', '', ''
                ])

            for entry in results.changed_fields:
                obj_type = entry.get('type', '')
                name = entry['source'].get('title', '')
                for attr, diff_val in entry['diffs'].items():
                    if attr == 'custom_field_options':
                        tgt_str, src_str = self._format_options_diff_display(diff_val)
                    else:
                        tgt_str = str(diff_val[1]) if diff_val[1] is not None else ''
                        src_str = str(diff_val[0]) if diff_val[0] is not None else ''
                    writer.writerow([obj_type, name, 'CHANGED', attr, src_str, tgt_str])

            for entry in results.existing_fields:
                writer.writerow([
                    entry.get('type', ''), entry['source'].get('title', ''),
                    'UNCHANGED', '', '', ''
                ])

            for entry in results.new_forms:
                writer.writerow([
                    'ticket_form', entry['source'].get('name', ''),
                    'NEW', '', '', ''
                ])

            for entry in results.changed_forms:
                name = entry['source'].get('name', '')
                for attr, (src_val, tgt_val) in entry['diffs'].items():
                    writer.writerow([
                        'ticket_form', name, 'CHANGED',
                        attr, str(src_val), str(tgt_val)
                    ])

            for entry in results.existing_forms:
                writer.writerow([
                    'ticket_form', entry['source'].get('name', ''),
                    'UNCHANGED', '', '', ''
                ])

    def _write_export_csv(
        self,
        filepath: str,
        results: AnalysisResults
    ) -> None:
        """Write analysis results to CSV file."""
        all_fields = (
            [x['source'] for x in results.new_fields] +
            [x['source'] for x in results.existing_fields] +
            [x['source'] for x in results.changed_fields]
        )

        all_forms = (
            [x['source'] for x in results.new_forms] +
            [x['source'] for x in results.existing_forms] +
            [x['source'] for x in results.changed_forms]
        )

        # Build field ID → title map from ALL source ticket fields (custom +
        # system) so that conditions referencing system fields can be exported.
        field_id_to_title: Dict[int, str] = {}
        for f in self.source_data.get('ticket_fields', []):
            fid = f.get('id')
            title = f.get('title', '')
            if fid and title:
                field_id_to_title[fid] = title

        field_to_forms: Dict[int, List[str]] = {}
        for form in all_forms:
            for field_id in form.get('ticket_field_ids', []):
                if field_id not in field_to_forms:
                    field_to_forms[field_id] = []
                field_to_forms[field_id].append(form.get('name', ''))

        with open(filepath, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            writer.writerow([
                'Type', 'Object', 'Name', 'Title (Customer)', 'Tag',
                'Description (Customer)', 'Agent Description',
                'Agent Required (Solved)', 'End-User Required',
                'End-User Visible', 'End-User Editable', 'RegEx',
                'Default', 'Active', 'Relationship Target Type',
                'Conditions (JSON)'
            ])

            for form in all_forms:
                conditions_json = self._serialize_form_conditions(
                    form, field_id_to_title
                )
                writer.writerow([
                    'ticket_form',
                    'root',
                    form.get('name', ''),
                    form.get('display_name', ''),
                    '',
                    '',
                    '',
                    '',
                    '',
                    form.get('end_user_visible', False),
                    '',
                    '',
                    '',
                    form.get('active', True),
                    '',
                    conditions_json
                ])

            for fld in all_fields:
                field_type = fld.get('type', '')

                if field_type not in ZendeskClient.ALLOWED_FIELD_TYPES:
                    continue

                context = self._determine_field_context(fld, field_to_forms)

                description = (fld.get('description') or '').replace('\n', ' ')
                agent_desc = (
                    fld.get('agent_description') or ''
                ).replace('\n', ' ')

                # Get lookup field info for logging
                if field_type == 'lookup':
                    lookup_info = self.logic.get_lookup_field_info(fld)
                    if not lookup_info['is_migratable']:
                        self.log_with_level(
                            f"[WARNING] Lookup field '{fld.get('title')}' "
                            f"may not migrate: {lookup_info['reason']}",
                            LogLevel.WARNING
                        )

                writer.writerow([
                    field_type,
                    context,
                    fld.get('title', ''),
                    fld.get('title_in_portal', ''),
                    fld.get('tag', fld.get('key', '')),
                    description,
                    agent_desc,
                    fld.get('required', False),
                    fld.get('required_in_portal', False),
                    fld.get('visible_in_portal', False),
                    fld.get('editable_in_portal', False),
                    fld.get('regexp_for_validation', ''),
                    '',
                    fld.get('active', True),
                    fld.get('relationship_target_type', ''),
                    ''
                ])

                for option in fld.get('custom_field_options', []):
                    writer.writerow([
                        'option',
                        fld.get('title', ''),
                        option.get('name', ''),
                        '',
                        option.get('value', ''),
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        option.get('default', False),
                        '',
                        '',
                        ''
                    ])

    # -----------------------------------------------------------------------
    # Diff Viewer
    # -----------------------------------------------------------------------

    @staticmethod
    def _format_options_diff_display(diff_detail: Dict) -> Tuple[str, str]:
        """Return (target_label, source_label) strings for an options diff."""
        added = diff_detail.get('added', [])
        removed = diff_detail.get('removed', [])
        renamed = diff_detail.get('renamed', [])

        def _sample(lst: list) -> str:
            sample = ', '.join(lst[:2])
            return f"{sample}{'…' if len(lst) > 2 else ''}"

        tgt_parts = []
        if removed:
            tgt_parts.append(f"-{len(removed)} ({_sample(removed)})")
        if renamed:
            tgt_parts.append(f"~{len(renamed)} name change(s)")

        src_parts = []
        if added:
            src_parts.append(f"+{len(added)} ({_sample(added)})")
        if renamed:
            src_parts.append(f"~{len(renamed)} renamed")

        return (
            ', '.join(tgt_parts) if tgt_parts else '',
            ', '.join(src_parts) if src_parts else '',
        )

    def _populate_diff_viewer(self) -> None:
        """Populate the diff treeview from current analysis results."""
        with self._analysis_lock:
            results = copy.deepcopy(self.analysis_results) if self.analysis_results else None

        for item in self.diff_tree.get_children():
            self.diff_tree.delete(item)

        if not results or not results.has_data():
            self.diff_summary_var.set(
                'No analysis data — run Step 1: Analyze Differences first.'
            )
            return

        status_filter = self.diff_status_var.get()
        type_filter = self.diff_type_var.get()

        type_label_map = {
            'ticket_field': 'Ticket Field',
            'user_field': 'User Field',
            'organization_field': 'Org Field',
        }
        type_filter_map = {
            'Ticket Fields': 'ticket_field',
            'User Fields': 'user_field',
            'Org Fields': 'organization_field',
            'Forms': 'ticket_form',
        }

        def _want(obj_type: str, status: str) -> bool:
            if status_filter != 'All' and status_filter != status:
                return False
            if type_filter != 'All':
                if obj_type != type_filter_map.get(type_filter, ''):
                    return False
            return True

        rows = 0

        # New fields
        for entry in results.new_fields:
            obj_type = entry.get('type', '')
            if not _want(obj_type, 'New'):
                continue
            self.diff_tree.insert(
                '', 'end',
                values=(
                    type_label_map.get(obj_type, obj_type),
                    entry['source'].get('title', '?'),
                    'NEW', '', '', '',
                ),
                tags=('new',),
            )
            rows += 1

        # Changed fields — one flat row per attribute diff
        for entry in results.changed_fields:
            obj_type = entry.get('type', '')
            if not _want(obj_type, 'Changed'):
                continue
            name = entry['source'].get('title', '?')
            type_label = type_label_map.get(obj_type, obj_type)
            inserted = 0
            for attr, diff_val in entry['diffs'].items():
                if attr == 'custom_field_options':
                    tgt_str, src_str = self._format_options_diff_display(diff_val)
                else:
                    tgt_str = str(diff_val[1]) if diff_val[1] not in (None, '') else ''
                    src_str = str(diff_val[0]) if diff_val[0] not in (None, '') else ''
                if src_str == tgt_str:
                    continue
                self.diff_tree.insert(
                    '', 'end',
                    values=(type_label, name, 'CHANGED', attr, src_str, tgt_str),
                    tags=('attr_diff',),
                )
                inserted += 1
            if inserted > 0:
                rows += 1

        # Unchanged fields
        for entry in results.existing_fields:
            obj_type = entry.get('type', '')
            if not _want(obj_type, 'Unchanged'):
                continue
            self.diff_tree.insert(
                '', 'end',
                values=(
                    type_label_map.get(obj_type, obj_type),
                    entry['source'].get('title', '?'),
                    'UNCHANGED', '', '', '',
                ),
                tags=('unchanged',),
            )
            rows += 1

        # New forms
        for entry in results.new_forms:
            if not _want('ticket_form', 'New'):
                continue
            self.diff_tree.insert(
                '', 'end',
                values=('Form', entry['source'].get('name', '?'), 'NEW', '', '', ''),
                tags=('new',),
            )
            rows += 1

        # Changed forms — one flat row per attribute diff
        for entry in results.changed_forms:
            if not _want('ticket_form', 'Changed'):
                continue
            name = entry['source'].get('name', '?')
            inserted = 0
            for attr, (src_val, tgt_val) in entry['diffs'].items():
                src_str = str(src_val) if src_val not in (None, '') else ''
                tgt_str = str(tgt_val) if tgt_val not in (None, '') else ''
                if src_str == tgt_str:
                    continue
                self.diff_tree.insert(
                    '', 'end',
                    values=('Form', name, 'CHANGED', attr, src_str, tgt_str),
                    tags=('attr_diff',),
                )
                inserted += 1
            if inserted > 0:
                rows += 1

        # Unchanged forms
        for entry in results.existing_forms:
            if not _want('ticket_form', 'Unchanged'):
                continue
            self.diff_tree.insert(
                '', 'end',
                values=('Form', entry['source'].get('name', '?'), 'UNCHANGED', '', '', ''),
                tags=('unchanged',),
            )
            rows += 1

        total_new = len(results.new_fields) + len(results.new_forms)
        total_changed = len(results.changed_fields) + len(results.changed_forms)
        total_unchanged = len(results.existing_fields) + len(results.existing_forms)
        summary = (
            f"New: {total_new}  |  Changed: {total_changed}  |  Unchanged: {total_unchanged}"
        )
        if rows != total_new + total_changed + total_unchanged:
            summary += f"  |  Showing: {rows}"
        self.diff_summary_var.set(summary)

        # Update tab title to signal data is ready
        changed_label = f" ({total_changed} changes)" if total_changed else ""
        self.notebook.tab(self.tab_diff, text=f" Diff Viewer{changed_label} ")

    def _apply_diff_filter(self) -> None:
        """Repopulate the diff viewer with the current filter applied."""
        if self.analysis_results:
            self._populate_diff_viewer()

    @staticmethod
    def _determine_field_context(
        field: Dict,
        field_to_forms: Dict[int, List[str]]
    ) -> str:
        """Determine the context/scope of a field."""
        url = str(field.get('url', ''))

        if 'user' in url:
            return "(User) Global"
        elif 'organization' in url:
            return "(Org) Global"
        else:
            forms = field_to_forms.get(field.get('id', 0), [])
            if forms:
                return " | ".join(forms)
            return "Ticket"

    def start_import(self) -> None:
        """Start import operation in background thread."""
        csv_path = self.csv_path_var.get()

        if not csv_path or not os.path.exists(csv_path):
            messagebox.showerror("Error", "CSV file not found")
            return

        creds = self.get_credentials(target_only=True)
        if not creds:
            return

        _, target_creds = creds

        self._start_background_task(
            self._run_import_thread,
            (
                target_creds,
                csv_path,
                self.imp_strategy_var.get(),
                self.rb_path_var.get()
            ),
            "Import"
        )

    def _run_import_thread(
        self,
        creds: Credentials,
        csv_path: str,
        strategy: str,
        rollback_path: str
    ) -> None:
        """Execute import operation."""
        try:
            self.logic.rollback_file = rollback_path

            self.log_with_level("=" * 60, LogLevel.ALWAYS)
            self.log_with_level(
                f"STARTING IMPORT (Strategy: {strategy})",
                LogLevel.ALWAYS
            )
            self.log_with_level("=" * 60, LogLevel.ALWAYS)

            client = ZendeskClient(
                creds.subdomain,
                creds.email,
                creds.token,
                self.log_with_level,
                verbose=True,
                stop_check=self._is_stop_requested
            )

            if self._is_stop_requested():
                return

            fields, forms = self._parse_import_csv(csv_path)
            self.log_with_level(
                f"[INFO] Parsed {len(fields)} fields and "
                f"{len(forms)} forms from CSV",
                LogLevel.INFO
            )

            if self._is_stop_requested():
                return

            target_data = self._fetch_target_state(client)

            if self._is_stop_requested():
                return

            target_maps = self._build_import_maps(target_data)

            reports = {
                'ticket_field': ImportReport(),
                'user_field': ImportReport(),
                'organization_field': ImportReport(),
                'ticket_form': ImportReport()
            }

            name_to_id_map: Dict[str, int] = {}

            # Pre-populate with target system field titles so that conditions
            # referencing system fields (status, priority, etc.) can be remapped.
            # Custom fields added during migration below will overwrite these
            # entries only if a custom field happens to share the same title,
            # which is an edge case and acceptable behaviour.
            for _sf in target_data.get('ticket_field', []):
                if self.logic.is_system_field(_sf):
                    _title = _sf.get('title', '')
                    _fid = _sf.get('id')
                    if _title and _fid:
                        name_to_id_map[_title] = _fid

            # Track skipped lookup fields for summary
            skipped_lookups: List[Dict[str, str]] = []

            total = len(fields) + len(forms)
            count = 0

            for fld in fields:
                if self._is_stop_requested():
                    self.log_with_level(
                        "[INFO] Import stopped during field processing",
                        LogLevel.INFO
                    )
                    break

                # Check if field should be skipped
                should_skip, skip_reason = self.logic.should_skip_field(fld)
                if should_skip:
                    field_type = fld['system_object_type']
                    reports[field_type].skipped.append(fld['title'])

                    # Track lookup field skips separately
                    if fld.get('type') == 'lookup':
                        skipped_lookups.append({
                            'title': fld['title'],
                            'reason': skip_reason
                        })

                    self.log_with_level(
                        f"[WARNING] Skipping '{fld['title']}': {skip_reason}",
                        LogLevel.WARNING
                    )
                    count += 1
                    self.update_progress(count, total)
                    continue

                result = self._process_field_import(
                    client, fld, target_data, target_maps, strategy
                )

                field_type = fld['system_object_type']
                report = reports[field_type]

                if result['status'] == 'created':
                    report.created.append(fld['title'])
                    if result.get('id'):
                        name_to_id_map[fld['title']] = result['id']
                        self.logic.log_rollback(
                            field_type, result['id'], fld['title']
                        )
                    self.log_with_level(
                        f"[INFO] [+] Created: {fld['title']}",
                        LogLevel.INFO
                    )

                elif result['status'] == 'updated':
                    report.updated.append(fld['title'])
                    if result.get('id'):
                        name_to_id_map[fld['title']] = result['id']
                    self.log_with_level(
                        f"[INFO] [~] Updated: {fld['title']}",
                        LogLevel.INFO
                    )

                elif result['status'] == 'skipped':
                    report.skipped.append(fld['title'])
                    if result.get('id'):
                        name_to_id_map[fld['title']] = result['id']
                    self.log_with_level(
                        f"[DEBUG] [-] Skipped: {fld['title']}",
                        LogLevel.DEBUG
                    )

                elif result['status'] == 'error':
                    report.errors.append(fld['title'])
                    self.log_with_level(
                        f"[ERROR] [!] Failed: {fld['title']}",
                        LogLevel.ERROR
                    )

                count += 1
                self.update_progress(count, total)
                time.sleep(0.2)

            for form in forms:
                if self._is_stop_requested():
                    self.log_with_level(
                        "[INFO] Import stopped during form processing",
                        LogLevel.INFO
                    )
                    break

                result = self._process_form_import(
                    client, form, fields, name_to_id_map,
                    target_maps, target_data, strategy
                )

                report = reports['ticket_form']

                if result['status'] == 'created':
                    report.created.append(form['name'])
                    if result.get('id'):
                        self.logic.log_rollback(
                            'ticket_form', result['id'], form['name']
                        )
                    self.log_with_level(
                        f"[INFO] [+] Created Form: {form['name']}",
                        LogLevel.INFO
                    )

                elif result['status'] == 'updated':
                    report.updated.append(form['name'])
                    self.log_with_level(
                        f"[INFO] [~] Updated Form: {form['name']}",
                        LogLevel.INFO
                    )

                elif result['status'] == 'skipped':
                    report.skipped.append(form['name'])
                    self.log_with_level(
                        f"[DEBUG] [-] Skipped Form: {form['name']}",
                        LogLevel.DEBUG
                    )

                elif result['status'] == 'error':
                    report.errors.append(form['name'])
                    self.log_with_level(
                        f"[ERROR] [!] Failed Form: {form['name']}",
                        LogLevel.ERROR
                    )

                count += 1
                self.update_progress(count, total)
                time.sleep(0.2)

            self._log_import_report(reports, skipped_lookups)

            if not self._is_stop_requested():
                self.log_queue.put(
                    ("POPUP", "Complete", "Import finished successfully.")
                )

        except csv.Error as e:
            self.log_with_level(
                f"[ERROR] CSV parsing error: {e}",
                LogLevel.ERROR
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )

        except Exception as e:
            self.log_with_level(
                f"[CRITICAL] Unexpected error: {e}",
                LogLevel.CRITICAL
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )

    def _parse_import_csv(
        self,
        csv_path: str
    ) -> Tuple[List[Dict], List[Dict]]:
        """Parse import CSV file."""
        fields = []
        forms = []
        current_field = None

        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [name.strip() for name in (reader.fieldnames or [])]

            for row in reader:
                row = {
                    k.strip(): (v.strip() if v else '')
                    for k, v in row.items()
                    if k
                }

                row_type = row.get('Type', '').lower()

                if row_type == 'ticket_form':
                    forms.append({
                        'name': row.get('Name', ''),
                        'display_name': row.get('Title (Customer)', ''),
                        'active': row.get('Active', 'true').lower() == 'true',
                        'end_user_visible': (
                            row.get('End-User Visible', 'false').lower() == 'true'
                        ),
                        'conditions_json': row.get('Conditions (JSON)', '')
                    })
                    current_field = None

                elif row_type in ZendeskClient.ALLOWED_FIELD_TYPES:
                    obj_type = self._determine_object_type(
                        row.get('Object', '')
                    )

                    tag_value = row.get('Tag', '')

                    current_field = {
                        'type': row_type,
                        'title': row.get('Name', ''),
                        'title_in_portal': row.get('Title (Customer)', ''),
                        'description': row.get('Description (Customer)', ''),
                        'agent_description': row.get('Agent Description', ''),
                        'active': row.get('Active', 'true').lower() == 'true',
                        'tag': tag_value,
                        'key': tag_value,
                        'regexp_for_validation': row.get('RegEx', ''),
                        'required': (
                            row.get(
                                'Agent Required (Solved)', 'false'
                            ).lower() == 'true'
                        ),
                        'required_in_portal': (
                            row.get('End-User Required', 'false').lower() == 'true'
                        ),
                        'visible_in_portal': (
                            row.get('End-User Visible', 'false').lower() == 'true'
                        ),
                        'editable_in_portal': (
                            row.get('End-User Editable', 'false').lower() == 'true'
                        ),
                        'custom_field_options': [],
                        'system_object_type': obj_type,
                        'associated_forms': self._parse_associated_forms(
                            row.get('Object', ''), obj_type
                        ),
                        'relationship_target_type': row.get(
                            'Relationship Target Type', ''
                        )
                    }
                    fields.append(current_field)

                elif row_type == 'option' and current_field is not None:
                    opts = current_field.get('custom_field_options', [])  # pylint: disable=no-member
                    opts.append({
                        'name': row.get('Name', ''),
                        'value': row.get('Tag', ''),
                        'default': row.get('Default', 'false').lower() == 'true'
                    })

        return fields, forms

    @staticmethod
    def _determine_object_type(object_value: str) -> str:
        """Determine object type from CSV Object column."""
        if '(User)' in object_value:
            return 'user_field'
        elif '(Org)' in object_value:
            return 'organization_field'
        return 'ticket_field'

    @staticmethod
    def _parse_associated_forms(
        object_value: str,
        object_type: str
    ) -> List[str]:
        """Parse associated forms from Object column."""
        if object_type != 'ticket_field':
            return []

        return [
            x.strip()
            for x in object_value.split('|')
            if x.strip() and '(' not in x
        ]

    def _fetch_target_state(self, client: ZendeskClient) -> Dict[str, List]:
        """Fetch current state from target instance."""
        return {
            'ticket_field': client.get_all(
                "ticket_fields.json", "ticket_fields"
            ),
            'user_field': client.get_all(
                "user_fields.json", "user_fields"
            ),
            'organization_field': client.get_all(
                "organization_fields.json", "organization_fields"
            ),
            'ticket_form': client.get_all(
                "ticket_forms.json", "ticket_forms"
            )
        }

    @staticmethod
    def _build_import_maps(target_data: Dict[str, List]) -> Dict[str, Dict]:
        """Build lookup maps for import comparison."""
        maps = {}

        for key in ['ticket_field', 'user_field', 'organization_field']:
            maps[key] = {
                (x.get('title', '').lower(), x.get('type', '')): x['id']
                for x in target_data.get(key, [])
            }

        maps['ticket_form'] = {
            x.get('name', '').lower(): x['id']
            for x in target_data.get('ticket_form', [])
        }

        return maps

    def _process_field_import(
        self,
        client: ZendeskClient,
        field: Dict,
        target_data: Dict[str, List],
        target_maps: Dict[str, Dict],
        strategy: str
    ) -> Dict[str, Any]:
        """Process single field import."""
        field_type = field['system_object_type']
        lookup_key = (field['title'].lower(), field['type'])
        exists = lookup_key in target_maps[field_type]

        self.log_with_level(
            f"[DEBUG] Processing {field_type}: {field['title']}",
            LogLevel.DEBUG
        )

        payload = self.logic.prepare_payload(field, field_type)
        target_id = target_maps[field_type].get(lookup_key)

        if exists:
            assert target_id is not None
            if strategy == ImportStrategy.CLONE.value:
                result = client.create_field_safe(
                    f"{field_type}s.json", payload, field_type
                )
                if result:
                    return {
                        'status': 'created',
                        'id': result[field_type]['id']
                    }
                return {'status': 'error'}

            elif strategy == ImportStrategy.UPDATE.value:
                # key is immutable after creation on user/org fields.
                # Sending a different key in PUT causes a 422. Drop it so
                # Zendesk keeps whatever key the target field already has.
                if field_type in ('user_field', 'organization_field'):
                    payload[field_type].pop('key', None)

                option_warnings = self._map_existing_options(
                    field_type, target_id, target_data, payload
                )
                for warning in option_warnings:
                    self.log_with_level(
                        f"[WARNING] '{field['title']}': {warning}",
                        LogLevel.WARNING
                    )

                result = client.update_object_safe(
                    f"{field_type}s", target_id, payload, field_type
                )
                if result:
                    return {'status': 'updated', 'id': target_id}
                return {'status': 'error'}

            else:
                return {'status': 'skipped', 'id': target_id}

        else:
            result = client.create_field_safe(
                f"{field_type}s.json", payload, field_type
            )
            if result:
                return {
                    'status': 'created',
                    'id': result[field_type]['id']
                }
            return {'status': 'error'}

    @staticmethod
    def _map_existing_options(
        field_type: str,
        target_id: int,
        target_data: Dict[str, List],
        payload: Dict
    ) -> List[str]:
        """Map existing option IDs for update operations.

        Matches options by value (tag) only. Name-only matches are skipped
        and returned as warnings to avoid silently changing option tags, which
        would break any triggers, automations, or views referencing the old tag.

        Returns:
            List of warning messages for options that could not be safely mapped.
        """
        warnings = []
        existing_obj = next(
            (x for x in target_data.get(field_type, []) if x['id'] == target_id),
            None
        )

        if not existing_obj or 'custom_field_options' not in existing_obj:
            return warnings

        value_map = {
            o['value']: o['id']
            for o in existing_obj['custom_field_options']
        }
        name_to_value = {
            o['name']: o['value']
            for o in existing_obj['custom_field_options']
        }

        for opt in payload[field_type].get('custom_field_options', []):
            if opt['value'] in value_map:
                opt['id'] = value_map[opt['value']]
            elif opt['name'] in name_to_value:
                existing_value = name_to_value[opt['name']]
                warnings.append(
                    f"Option '{opt['name']}': CSV tag '{opt['value']}' differs "
                    f"from target tag '{existing_value}'. Tag NOT changed to "
                    "avoid breaking existing references. Update manually if intended."
                )

        return warnings

    def _process_form_import(
        self,
        client: ZendeskClient,
        form: Dict,
        fields: List[Dict],
        name_to_id_map: Dict[str, int],
        target_maps: Dict[str, Dict],
        target_data: Dict[str, List],
        strategy: str
    ) -> Dict[str, Any]:
        """Process single form import."""
        exists = form['name'].lower() in target_maps['ticket_form']
        target_id = target_maps['ticket_form'].get(form['name'].lower())

        # Collect custom field IDs for this form from the current migration session.
        custom_field_ids = []
        for fld in fields:
            if form['name'] in fld.get('associated_forms', []):
                if fld['title'] in name_to_id_map:
                    custom_field_ids.append(name_to_id_map[fld['title']])

        # For UPDATE: preserve system fields already on the existing form so
        # they are not silently dropped from ticket_field_ids.
        if exists and strategy == ImportStrategy.UPDATE.value:
            system_field_ids = {
                f['id'] for f in target_data.get('ticket_field', [])
                if self.logic.is_system_field(f)
            }
            existing_form = next(
                (f for f in target_data.get('ticket_form', [])
                 if f['id'] == target_id),
                None
            )
            preserved = []
            if existing_form:
                preserved = [
                    fid for fid in existing_form.get('ticket_field_ids', [])
                    if fid in system_field_ids
                ]
            field_ids = preserved + custom_field_ids
        else:
            field_ids = custom_field_ids

        # Remap conditions from humanised titles back to target field IDs.
        agent_conditions = []
        end_user_conditions = []
        conditions_json_str = form.get('conditions_json', '')
        if conditions_json_str:
            try:
                conditions_data = json.loads(conditions_json_str)
                agent_conditions = self._remap_conditions(
                    conditions_data.get('agent', []),
                    name_to_id_map,
                    form['name']
                )
                end_user_conditions = self._remap_conditions(
                    conditions_data.get('end_user', []),
                    name_to_id_map,
                    form['name']
                )
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                self.log_with_level(
                    f"[WARNING] Could not parse conditions for form "
                    f"'{form['name']}': {e}",
                    LogLevel.WARNING
                )

        payload: Dict[str, Any] = {
            'ticket_form': {
                'name': form['name'],
                'display_name': form['display_name'],
                'active': form['active'],
                'end_user_visible': form['end_user_visible'],
                'ticket_field_ids': field_ids
            }
        }
        if agent_conditions:
            payload['ticket_form']['agent_conditions'] = agent_conditions
        if end_user_conditions:
            payload['ticket_form']['end_user_conditions'] = end_user_conditions

        if exists:
            assert target_id is not None
            if strategy == ImportStrategy.CLONE.value:
                response = client._request('POST', 'ticket_forms.json', payload)
                if response and response.status_code == 201:
                    try:
                        return {
                            'status': 'created',
                            'id': response.json()['ticket_form']['id']
                        }
                    except (json.JSONDecodeError, KeyError):
                        return {'status': 'error'}
                return {'status': 'error'}

            elif strategy == ImportStrategy.UPDATE.value:
                result = client.update_object_safe(
                    'ticket_forms', target_id, payload, 'ticket_form'
                )
                if result:
                    return {'status': 'updated', 'id': target_id}
                return {'status': 'error'}

            else:
                return {'status': 'skipped'}

        else:
            response = client._request('POST', 'ticket_forms.json', payload)
            if response and response.status_code == 201:
                try:
                    return {
                        'status': 'created',
                        'id': response.json()['ticket_form']['id']
                    }
                except (json.JSONDecodeError, KeyError):
                    return {'status': 'error'}
            return {'status': 'error'}

    def _remap_conditions(
        self,
        conditions: List[Dict],
        name_to_id_map: Dict[str, int],
        form_name: str
    ) -> List[Dict]:
        """Remap humanised condition titles back to target field IDs.

        Conditions exported to CSV have field IDs replaced with titles.
        This reverses that substitution using name_to_id_map, which contains
        both migrated custom fields and pre-populated system fields.

        Conditions that reference fields not present in name_to_id_map are
        skipped with a warning rather than failing the whole form import.
        """
        remapped = []
        for cond in conditions:
            parent_title = cond.get('parent_field_title', '')
            parent_id = name_to_id_map.get(parent_title)
            if not parent_id:
                self.log_with_level(
                    f"[WARNING] Form '{form_name}': condition parent field "
                    f"'{parent_title}' not found in target — condition skipped.",
                    LogLevel.WARNING
                )
                continue

            child_fields = []
            for cf in cond.get('child_fields', []):
                child_title = cf.get('title', '')
                child_id = name_to_id_map.get(child_title)
                if child_id:
                    child_fields.append({
                        'id': child_id,
                        'is_required': cf.get('is_required', False)
                    })
                else:
                    self.log_with_level(
                        f"[WARNING] Form '{form_name}': condition child field "
                        f"'{child_title}' not found in target — skipped from "
                        "condition.",
                        LogLevel.WARNING
                    )

            if child_fields:
                remapped.append({
                    'parent_field_id': parent_id,
                    'value': cond.get('value', ''),
                    'child_fields': child_fields
                })

        return remapped

    def _log_import_report(
        self,
        reports: Dict[str, ImportReport],
        skipped_lookups: List[Dict[str, str]]
    ) -> None:
        """Log final import summary report."""
        self.log_with_level("\n" + "=" * 60, LogLevel.ALWAYS)
        self.log_with_level(f"{'IMPORT SUMMARY REPORT':^60}", LogLevel.ALWAYS)
        self.log_with_level("=" * 60, LogLevel.ALWAYS)

        categories = [
            ('Ticket Fields', 'ticket_field'),
            ('User Fields', 'user_field'),
            ('Organization Fields', 'organization_field'),
            ('Ticket Forms', 'ticket_form')
        ]

        totals = ImportReport()

        for label, key in categories:
            report = reports[key]

            self.log_with_level(f"\n[{label}]", LogLevel.ALWAYS)
            self.log_with_level(
                f"   + Created: {len(report.created)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"   ~ Updated: {len(report.updated)}",
                LogLevel.ALWAYS
            )
            self.log_with_level(
                f"   - Skipped: {len(report.skipped)}",
                LogLevel.ALWAYS
            )

            if report.errors:
                self.log_with_level(
                    f"   ! Errors:  {len(report.errors)}",
                    LogLevel.ALWAYS
                )
                for error_item in report.errors[:5]:
                    self.log_with_level(
                        f"       - {error_item}",
                        LogLevel.ALWAYS
                    )
                if len(report.errors) > 5:
                    self.log_with_level(
                        f"       ... and {len(report.errors) - 5} more",
                        LogLevel.ALWAYS
                    )

            totals.created.extend(report.created)
            totals.updated.extend(report.updated)
            totals.skipped.extend(report.skipped)
            totals.errors.extend(report.errors)

        # Log skipped lookup fields summary
        if skipped_lookups:
            self.log_with_level("\n[Skipped Lookup Fields]", LogLevel.ALWAYS)
            self.log_with_level(
                f"   Total: {len(skipped_lookups)} (Custom Object references)",
                LogLevel.ALWAYS
            )
            for item in skipped_lookups[:5]:
                self.log_with_level(
                    f"   - {item['title']}: {item['reason']}",
                    LogLevel.WARNING
                )
            if len(skipped_lookups) > 5:
                self.log_with_level(
                    f"   ... and {len(skipped_lookups) - 5} more",
                    LogLevel.ALWAYS
                )
            self.log_with_level(
                "\n   NOTE: To migrate these fields, first create the "
                "Custom Objects in the target instance.",
                LogLevel.ALWAYS
            )

        self.log_with_level("\n" + "-" * 60, LogLevel.ALWAYS)
        self.log_with_level(f"{'TOTALS':^60}", LogLevel.ALWAYS)
        self.log_with_level("-" * 60, LogLevel.ALWAYS)
        self.log_with_level(
            f"   + Total Created: {len(totals.created)}",
            LogLevel.ALWAYS
        )
        self.log_with_level(
            f"   ~ Total Updated: {len(totals.updated)}",
            LogLevel.ALWAYS
        )
        self.log_with_level(
            f"   - Total Skipped: {len(totals.skipped)}",
            LogLevel.ALWAYS
        )
        self.log_with_level(
            f"   ! Total Errors:  {len(totals.errors)}",
            LogLevel.ALWAYS
        )
        self.log_with_level("=" * 60, LogLevel.ALWAYS)

    def start_rollback(self) -> None:
        """Start rollback operation in background thread."""
        creds = self.get_credentials(target_only=True)
        if not creds:
            return

        _, target_creds = creds

        rollback_path = self.rb_path_var.get()
        if not rollback_path or not os.path.exists(rollback_path):
            messagebox.showerror("Error", "Rollback log file not found")
            return

        confirm = messagebox.askyesno(
            "Confirm Rollback",
            "This will DELETE all items in the rollback log.\n\n"
            "Are you sure you want to continue?"
        )

        if not confirm:
            return

        self._start_background_task(
            self._run_rollback_thread,
            (target_creds, rollback_path),
            "Rollback"
        )

    def _run_rollback_thread(
        self,
        creds: Credentials,
        filepath: str
    ) -> None:
        """Execute rollback operation."""
        try:
            self.log_with_level("=" * 60, LogLevel.ALWAYS)
            self.log_with_level("STARTING ROLLBACK", LogLevel.ALWAYS)
            self.log_with_level("=" * 60, LogLevel.ALWAYS)

            client = ZendeskClient(
                creds.subdomain,
                creds.email,
                creds.token,
                self.log_with_level,
                verbose=True,
                stop_check=self._is_stop_requested
            )

            with open(filepath, 'r', encoding='utf-8') as f:
                reader = list(csv.DictReader(f))

            reader.reverse()
            total = len(reader)

            deleted_count = 0
            error_count = 0

            endpoint_map = {
                'ticket_form': 'ticket_forms',
                'ticket_field': 'ticket_fields',
                'user_field': 'user_fields',
                'organization_field': 'organization_fields'
            }

            for i, row in enumerate(reader):
                if self._is_stop_requested():
                    self.log_with_level(
                        "[INFO] Rollback stopped by user",
                        LogLevel.INFO
                    )
                    break

                item_type = row.get('type', '')
                item_id = row.get('id', '')
                item_name = row.get('name', '')

                endpoint = endpoint_map.get(item_type)

                if endpoint and item_id:
                    success = client.delete_item(endpoint, int(item_id))

                    if success:
                        deleted_count += 1
                        self.log_with_level(
                            f"[INFO] [✓] Deleted {item_type}: {item_name} "
                            f"(ID: {item_id})",
                            LogLevel.INFO
                        )
                    else:
                        error_count += 1
                        self.log_with_level(
                            f"[ERROR] [✗] Failed to delete {item_type}: "
                            f"{item_name} (ID: {item_id})",
                            LogLevel.ERROR
                        )
                else:
                    self.log_with_level(
                        f"[WARNING] Skipped invalid row: {row}",
                        LogLevel.WARNING
                    )

                self.update_progress(i + 1, total)
                time.sleep(0.2)

            self.log_with_level("\n" + "-" * 60, LogLevel.ALWAYS)
            self.log_with_level("ROLLBACK COMPLETE", LogLevel.ALWAYS)
            self.log_with_level(f"   Deleted: {deleted_count}", LogLevel.ALWAYS)
            self.log_with_level(f"   Errors:  {error_count}", LogLevel.ALWAYS)
            self.log_with_level("-" * 60, LogLevel.ALWAYS)

            if not self._is_stop_requested():
                self.log_queue.put(("POPUP", "Complete", "Rollback finished."))

        except csv.Error as e:
            self.log_with_level(
                f"[ERROR] Failed to read rollback log: {e}",
                LogLevel.ERROR
            )

        except Exception as e:
            self.log_with_level(
                f"[CRITICAL] Unexpected error: {e}",
                LogLevel.CRITICAL
            )
            self.log_with_level(
                f"[DEBUG] Stack Trace:\n{traceback.format_exc()}",
                LogLevel.DEBUG
            )


# ==========================================
# MAIN ENTRY POINT
# ==========================================

def main():
    """Application entry point."""
    root = tk.Tk()

    app = ZendeskMigratorApp(root)

    def on_closing():
        app.log_file_manager.close()

        if app._process_state != ProcessState.IDLE:
            if messagebox.askokcancel(
                "Quit",
                "A process is still running. Are you sure you want to quit?"
            ):
                app._stop_event.set()
                root.destroy()
        else:
            if messagebox.askokcancel("Quit", "Do you want to quit?"):
                root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    root.mainloop()


if __name__ == "__main__":
    main()
