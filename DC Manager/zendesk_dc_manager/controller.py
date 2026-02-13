"""
Controller module for Zendesk DC Manager.
Orchestrates API calls, translation, and data processing.
"""

import json
import os
import re
import threading
import unicodedata
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from zendesk_dc_manager.config import (
    logger,
    SOURCE_NEW,
    SOURCE_ZENDESK_DC,
    SOURCE_TRANSLATED,
    SOURCE_CACHE,
    SOURCE_FAILED,
    SOURCE_MANUAL,
)
from zendesk_dc_manager.api import ZendeskAPI
from zendesk_dc_manager.translator import TranslationService
from zendesk_dc_manager.types import TranslationStats


DC_PLACEHOLDER_PATTERN = re.compile(r'\{\{dc\.([^}]+)\}\}')

SYSTEM_FIELD_TYPES = frozenset([
    'subject', 'description', 'status', 'tickettype', 'priority',
    'group', 'assignee', 'custom_status'
])

CONTEXT_MAP = {
    'ticket_field': 'Ticket',
    'ticket_field_option': 'Ticket',
    'ticket_form': 'Ticket',
    'custom_status': 'Status',
    'user_field': 'User',
    'user_field_option': 'User',
    'organization_field': 'Organization',
    'organization_field_option': 'Organization',
    'group': 'Admin',
    'macro': 'Business Rules',
    'trigger': 'Business Rules',
    'automation': 'Business Rules',
    'view': 'Business Rules',
    'sla_policy': 'Business Rules',
    'category': 'Help Center',
    'section': 'Help Center',
    'article': 'Help Center',
}

TYPE_DISPLAY_MAP = {
    'ticket_field': 'Ticket Field',
    'ticket_field_option': 'Ticket Field Option',
    'ticket_form': 'Ticket Form',
    'custom_status': 'Custom Status',
    'user_field': 'User Field',
    'user_field_option': 'User Field Option',
    'organization_field': 'Organization Field',
    'organization_field_option': 'Organization Field Option',
    'group': 'Group',
    'macro': 'Macro',
    'trigger': 'Trigger',
    'automation': 'Automation',
    'view': 'View',
    'sla_policy': 'SLA Policy',
    'category': 'Category',
    'section': 'Section',
    'article': 'Article',
}


def generate_dc_name(text: str, max_length: int = 50) -> str:
    """
    Generate a valid DC name from text.
    Converts to lowercase, removes accents, replaces spaces with underscores.
    Returns empty string if text is already a DC placeholder.
    """
    if not text:
        return ""

    if is_dc_placeholder(text):
        return ""

    normalized = unicodedata.normalize('NFKD', text)
    ascii_text = normalized.encode('ASCII', 'ignore').decode('ASCII')
    lower_text = ascii_text.lower()
    cleaned = re.sub(r'[^a-z0-9]+', '_', lower_text)
    cleaned = cleaned.strip('_')
    cleaned = re.sub(r'_+', '_', cleaned)

    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip('_')

    return cleaned


def generate_dc_placeholder(text: str) -> str:
    """Generate a proposed DC placeholder from text."""
    if is_dc_placeholder(text):
        return text

    dc_name = generate_dc_name(text)
    if dc_name:
        return f"{{{{dc.{dc_name}}}}}"
    return ""


def is_dc_placeholder(text: str) -> bool:
    """Check if text is already a DC placeholder."""
    if not text:
        return False
    return bool(DC_PLACEHOLDER_PATTERN.match(str(text).strip()))


def extract_dc_name_from_placeholder(placeholder: str) -> str:
    """Extract the DC name from a placeholder like {{dc.name}}."""
    match = DC_PLACEHOLDER_PATTERN.search(str(placeholder))
    if match:
        return match.group(1)
    return ""


class ZendeskController:
    """Main controller for Zendesk operations."""

    def __init__(self):
        self.api: Optional[ZendeskAPI] = None
        self.translator: Optional[TranslationService] = None
        self.work_items: List[Dict[str, Any]] = []
        self.dc_map: Dict[str, Dict[str, Any]] = {}
        self.dc_name_map: Dict[str, str] = {}
        self.dc_by_name: Dict[str, Dict[str, Any]] = {}
        self.backup_folder: str = ""
        self.default_locale: Dict[str, Any] = {}

        self._stop_flag = False
        self._stop_lock = threading.Lock()

    def stop(self):
        """Signal to stop current operation."""
        with self._stop_lock:
            self._stop_flag = True
        if self.api:
            self.api.stop()

    def _should_stop(self) -> bool:
        """Check if operation should stop."""
        with self._stop_lock:
            return self._stop_flag

    def _reset_stop(self):
        """Reset stop flag."""
        with self._stop_lock:
            self._stop_flag = False
        if self.api:
            self.api.reset_stop()

    def connect(
        self,
        subdomain: str,
        email: str,
        token: str,
        backup_folder: str,
        log_signal
    ) -> str:
        """Connect to Zendesk API and verify credentials."""
        self._reset_stop()

        self.api = ZendeskAPI(subdomain, email, token)
        self.backup_folder = backup_folder

        if backup_folder and not os.path.exists(backup_folder):
            try:
                os.makedirs(backup_folder)
            except OSError as e:
                logger.warning(f"Could not create backup folder: {e}")

        log_signal.emit("Connecting to Zendesk...")

        try:
            user_info = self.api.get_current_user()
            user_name = user_info.get('name', 'Unknown')
            user_email = user_info.get('email', email)
            user_role = user_info.get('role', 'Unknown')

            log_signal.emit(f"Connected as: {user_name} ({user_email})")
            log_signal.emit(f"Role: {user_role}")

            try:
                self.default_locale = self.api.get_default_locale()
                locale_name = self.default_locale.get('name', 'Unknown')
                locale_code = self.default_locale.get('locale', 'Unknown')
                log_signal.emit(
                    f"Default Locale: {locale_name} ({locale_code})"
                )
            except Exception as e:
                log_signal.emit(
                    f"Warning: Could not fetch default locale: {e}"
                )
                self.default_locale = {}

            return f"Connected as {user_name} ({user_role})"

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def set_translation_config(
        self,
        provider: str,
        api_key: str,
        protect_acronyms: bool,
        cache_days: int
    ):
        """Configure translation service."""
        use_google_cloud = "Cloud" in provider
        self.translator = TranslationService(
            use_google_cloud=use_google_cloud,
            google_api_key=api_key if use_google_cloud else None,
            protect_acronyms=protect_acronyms,
            cache_expiry_days=cache_days
        )

    def scan_and_analyze(
        self,
        progress_signal,
        log_signal,
        config: Dict[str, bool]
    ) -> Dict[str, int]:
        """Scan Zendesk objects and analyze for DC opportunities."""
        self._reset_stop()
        self.work_items = []
        self.dc_map = {}
        self.dc_name_map = {}
        self.dc_by_name = {}

        stats = {
            'valid_fields': 0,
            'valid_forms': 0,
            'valid_custom_statuses': 0,
            'valid_user_fields': 0,
            'valid_org_fields': 0,
            'valid_groups': 0,
            'valid_macros': 0,
            'valid_triggers': 0,
            'valid_automations': 0,
            'valid_views': 0,
            'valid_sla_policies': 0,
            'valid_cats': 0,
            'valid_sects': 0,
            'valid_arts': 0,
            'system_excluded': 0,
        }

        log_signal.emit("Fetching existing Dynamic Content...")
        progress_signal.emit(0, 100, "Fetching DC items...")

        try:
            dc_items = self.api.get_dynamic_content_items()
            self._build_dc_cache(dc_items)
            log_signal.emit(f"Found {len(self.dc_map)} existing DC items")

        except Exception as e:
            log_signal.emit(f"Warning: Could not fetch DC items: {e}")

        if self._should_stop():
            raise Exception("Canceled by user")

        step = 0
        total_steps = sum(1 for v in config.values() if v)

        if config.get('fields'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning ticket fields..."
            )
            log_signal.emit("Scanning ticket fields...")
            count, system_count = self._scan_ticket_fields(log_signal)
            stats['valid_fields'] = count
            stats['system_excluded'] += system_count

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('forms'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning ticket forms..."
            )
            log_signal.emit("Scanning ticket forms...")
            stats['valid_forms'] = self._scan_ticket_forms(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('custom_statuses'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning custom statuses..."
            )
            log_signal.emit("Scanning custom statuses...")
            stats['valid_custom_statuses'] = self._scan_custom_statuses(
                log_signal
            )

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('user_fields'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning user fields..."
            )
            log_signal.emit("Scanning user fields...")
            stats['valid_user_fields'] = self._scan_user_fields(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('org_fields'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning organization fields..."
            )
            log_signal.emit("Scanning organization fields...")
            stats['valid_org_fields'] = self._scan_org_fields(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('groups'):
            step += 1
            progress_signal.emit(step, total_steps, "Scanning groups...")
            log_signal.emit("Scanning groups...")
            stats['valid_groups'] = self._scan_groups(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('macros'):
            step += 1
            progress_signal.emit(step, total_steps, "Scanning macros...")
            log_signal.emit("Scanning macros...")
            stats['valid_macros'] = self._scan_macros(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('triggers'):
            step += 1
            progress_signal.emit(step, total_steps, "Scanning triggers...")
            log_signal.emit("Scanning triggers...")
            stats['valid_triggers'] = self._scan_triggers(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('automations'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning automations..."
            )
            log_signal.emit("Scanning automations...")
            stats['valid_automations'] = self._scan_automations(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('views'):
            step += 1
            progress_signal.emit(step, total_steps, "Scanning views...")
            log_signal.emit("Scanning views...")
            stats['valid_views'] = self._scan_views(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('sla_policies'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning SLA policies..."
            )
            log_signal.emit("Scanning SLA policies...")
            stats['valid_sla_policies'] = self._scan_sla_policies(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('cats'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning HC categories..."
            )
            log_signal.emit("Scanning Help Center categories...")
            stats['valid_cats'] = self._scan_hc_categories(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('sects'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning HC sections..."
            )
            log_signal.emit("Scanning Help Center sections...")
            stats['valid_sects'] = self._scan_hc_sections(log_signal)

        if self._should_stop():
            raise Exception("Canceled by user")

        if config.get('arts'):
            step += 1
            progress_signal.emit(
                step, total_steps, "Scanning HC articles..."
            )
            log_signal.emit("Scanning Help Center articles...")
            stats['valid_arts'] = self._scan_hc_articles(log_signal)

        log_signal.emit(f"Scan complete. Found {len(self.work_items)} items.")
        return stats

    def _build_dc_cache(self, dc_items: List[Dict]):
        """Build DC lookup caches from list of DC items."""
        self.dc_map = {}
        self.dc_name_map = {}
        self.dc_by_name = {}

        for dc in dc_items:
            dc_id = dc.get('id')
            dc_name = dc.get('name', '')
            placeholder = dc.get('placeholder', '')

            variants = {}
            for v in dc.get('variants', []):
                locale = v.get('locale_id')
                content = v.get('content', '')
                variant_id = v.get('id')
                default = v.get('default', False)
                variants[locale] = {
                    'content': content,
                    'variant_id': variant_id,
                    'default': default
                }

            dc_info = {
                'id': dc_id,
                'name': dc_name,
                'placeholder': placeholder,
                'variants': variants
            }

            self.dc_map[placeholder] = dc_info

            dc_name_lower = dc_name.lower()
            self.dc_name_map[dc_name_lower] = placeholder
            self.dc_by_name[dc_name_lower] = dc_info

            extracted_name = extract_dc_name_from_placeholder(placeholder)
            if extracted_name:
                self.dc_name_map[extracted_name.lower()] = placeholder
                self.dc_by_name[extracted_name.lower()] = dc_info

    def _find_dc_by_placeholder(self, placeholder: str) -> Optional[Dict]:
        """Find DC info by placeholder."""
        if placeholder in self.dc_map:
            return self.dc_map[placeholder]

        dc_name = extract_dc_name_from_placeholder(placeholder)
        if dc_name:
            full_placeholder = f"{{{{dc.{dc_name}}}}}"
            if full_placeholder in self.dc_map:
                return self.dc_map[full_placeholder]

            dc_name_lower = dc_name.lower()
            if dc_name_lower in self.dc_name_map:
                found_placeholder = self.dc_name_map[dc_name_lower]
                if found_placeholder in self.dc_map:
                    return self.dc_map[found_placeholder]

        return None

    def _find_dc_by_name(self, name: str) -> Optional[Dict]:
        """Find DC info by name (case-insensitive)."""
        if not name:
            return None

        name_lower = name.lower()
        if name_lower in self.dc_by_name:
            return self.dc_by_name[name_lower]

        generated_name = generate_dc_name(name)
        if generated_name:
            generated_lower = generated_name.lower()
            if generated_lower in self.dc_by_name:
                return self.dc_by_name[generated_lower]

        return None

    def _scan_ticket_fields(self, log_signal) -> Tuple[int, int]:
        """Scan ticket fields and their options."""
        count = 0
        system_count = 0

        # Field keys that are system-managed (Zendesk protects these)
        SYSTEM_FIELD_KEYS = frozenset([
            'approval_status',
            'zd_approval_status',
            'resolution_type',
            'zd_resolution_type',
            'zd_automated_resolution',
        ])

        # Patterns in field keys that indicate system fields
        SYSTEM_KEY_PATTERNS = [
            'zd_es_approval',
            'zd_automated',
            'zd_resolution',
        ]

        try:
            fields = self.api.get_ticket_fields()

            for field in fields:
                if self._should_stop():
                    break

                field_id = field.get('id')
                field_type = field.get('type', '')
                field_key = field.get('key', '')
                title = field.get('title', '')
                raw_title = field.get('raw_title', title)

                # Check if system field by type
                is_system = field_type in SYSTEM_FIELD_TYPES

                # Check if system field by key
                if not is_system and field_key:
                    field_key_lower = field_key.lower()
                    if field_key_lower in SYSTEM_FIELD_KEYS:
                        is_system = True
                    else:
                        for pattern in SYSTEM_KEY_PATTERNS:
                            if pattern in field_key_lower:
                                is_system = True
                                break

                # Check if system field by title patterns
                if not is_system:
                    title_lower = title.lower()
                    if any(p in title_lower for p in [
                        'approval status', 'status de aprovação',
                        'resolution type', 'tipo de resolução',
                        'tipo de resolucao'
                    ]):
                        is_system = True

                if is_system:
                    system_count += 1
                    logger.debug(
                        f"System field: {field_id} - {title} "
                        f"(type={field_type}, key={field_key})"
                    )

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='ticket_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info,
                        is_system=is_system,
                        extra={'field_type': field_type, 'field_key': field_key}
                    )
                else:
                    self._add_work_item(
                        obj_type='ticket_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        is_system=is_system,
                        extra={'field_type': field_type, 'field_key': field_key}
                    )

                count += 1

                # Process field options - inherit system flag from parent
                options = field.get('custom_field_options', [])
                for opt in options:
                    opt_id = opt.get('id')
                    opt_name = opt.get('name', '')
                    opt_raw = opt.get('raw_name', opt_name)
                    opt_value = opt.get('value', '')

                    logger.debug(
                        f"Option: id={opt_id}, name={opt_name}, "
                        f"value={opt_value}"
                    )

                    dc_match = DC_PLACEHOLDER_PATTERN.search(str(opt_raw))

                    if dc_match:
                        placeholder = dc_match.group(0)
                        dc_info = self._find_dc_by_placeholder(placeholder)

                        self._add_work_item(
                            obj_type='ticket_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            dc_placeholder=placeholder,
                            dc_info=dc_info,
                            parent_id=field_id,
                            is_system=is_system,
                            extra={
                                'field_type': field_type,
                                'field_key': field_key,
                                'option_value': opt_value
                            }
                        )
                    else:
                        self._add_work_item(
                            obj_type='ticket_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            parent_id=field_id,
                            is_system=is_system,
                            extra={
                                'field_type': field_type,
                                'field_key': field_key,
                                'option_value': opt_value
                            }
                        )

            log_signal.emit(
                f"  Found {count} ticket fields ({system_count} system)"
            )

        except Exception as e:
            log_signal.emit(f"  Error scanning ticket fields: {e}")
            logger.error(f"Error scanning ticket fields: {e}")

        return count, system_count

    def _scan_ticket_forms(self, log_signal) -> int:
        """Scan ticket forms."""
        count = 0

        try:
            forms = self.api.get_ticket_forms()

            for form in forms:
                if self._should_stop():
                    break

                form_id = form.get('id')
                name = form.get('name', '')
                raw_name = form.get('raw_name', name)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_name))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='ticket_form',
                        obj_id=form_id,
                        field_name='name',
                        current_value=name,
                        raw_value=raw_name,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='ticket_form',
                        obj_id=form_id,
                        field_name='name',
                        current_value=name,
                        raw_value=raw_name
                    )

                count += 1

            log_signal.emit(f"  Found {count} ticket forms")

        except Exception as e:
            log_signal.emit(f"  Error scanning ticket forms: {e}")
            logger.error(f"Error scanning ticket forms: {e}")

        return count

    def _scan_custom_statuses(self, log_signal) -> int:
        """Scan custom ticket statuses."""
        count = 0

        # Status categories that are system-protected
        PROTECTED_STATUS_CATEGORIES = frozenset([
            'hold',  # "Em espera" / "On-hold" - protected by Zendesk
        ])

        # Status category names that indicate default/system statuses
        SYSTEM_STATUS_NAMES = frozenset([
            'new', 'open', 'pending', 'hold', 'solved', 'closed',
            'novo', 'aberto', 'pendente', 'em espera', 'resolvido',
            'nuevo', 'abierto', 'pendiente', 'en espera', 'resuelto',
        ])

        try:
            statuses = self.api.get_custom_statuses()

            for status in statuses:
                if self._should_stop():
                    break

                status_id = status.get('id')
                agent_label = status.get('agent_label', '')
                raw_agent_label = status.get('raw_agent_label', agent_label)
                status_category = status.get('status_category', '')
                is_default = status.get('default', False)

                # Determine if this is a system/protected status
                is_system = False

                # Check if it's in a protected category
                if status_category.lower() in PROTECTED_STATUS_CATEGORIES:
                    is_system = True
                    logger.debug(
                        f"Protected status category: {status_id} - "
                        f"{agent_label} (category={status_category})"
                    )

                # Check if it's a default status with system-like name
                if is_default:
                    agent_label_lower = agent_label.lower().strip()
                    if agent_label_lower in SYSTEM_STATUS_NAMES:
                        is_system = True
                        logger.debug(
                            f"Default system status: {status_id} - "
                            f"{agent_label} (default={is_default})"
                        )

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_agent_label))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='custom_status',
                        obj_id=status_id,
                        field_name='agent_label',
                        current_value=agent_label,
                        raw_value=raw_agent_label,
                        dc_placeholder=placeholder,
                        dc_info=dc_info,
                        is_system=is_system,
                        extra={
                            'status_category': status_category,
                            'is_default': is_default
                        }
                    )
                else:
                    self._add_work_item(
                        obj_type='custom_status',
                        obj_id=status_id,
                        field_name='agent_label',
                        current_value=agent_label,
                        raw_value=raw_agent_label,
                        is_system=is_system,
                        extra={
                            'status_category': status_category,
                            'is_default': is_default
                        }
                    )

                count += 1

            log_signal.emit(f"  Found {count} custom statuses")

        except Exception as e:
            log_signal.emit(f"  Error scanning custom statuses: {e}")
            logger.error(f"Error scanning custom statuses: {e}")

        return count

    def _scan_user_fields(self, log_signal) -> int:
        """Scan user fields and their options."""
        count = 0

        try:
            fields = self.api.get_user_fields()

            for field in fields:
                if self._should_stop():
                    break

                field_id = field.get('id')
                title = field.get('title', '')
                raw_title = field.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='user_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='user_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

                options = field.get('custom_field_options', [])
                for opt in options:
                    opt_id = opt.get('id')
                    opt_name = opt.get('name', '')
                    opt_raw = opt.get('raw_name', opt_name)
                    opt_value = opt.get('value', '')

                    dc_match = DC_PLACEHOLDER_PATTERN.search(str(opt_raw))

                    if dc_match:
                        placeholder = dc_match.group(0)
                        dc_info = self._find_dc_by_placeholder(placeholder)

                        self._add_work_item(
                            obj_type='user_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            dc_placeholder=placeholder,
                            dc_info=dc_info,
                            parent_id=field_id,
                            extra={'option_value': opt_value}
                        )
                    else:
                        self._add_work_item(
                            obj_type='user_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            parent_id=field_id,
                            extra={'option_value': opt_value}
                        )

            log_signal.emit(f"  Found {count} user fields")

        except Exception as e:
            log_signal.emit(f"  Error scanning user fields: {e}")
            logger.error(f"Error scanning user fields: {e}")

        return count

    def _scan_org_fields(self, log_signal) -> int:
        """Scan organization fields and their options."""
        count = 0

        try:
            fields = self.api.get_organization_fields()

            for field in fields:
                if self._should_stop():
                    break

                field_id = field.get('id')
                title = field.get('title', '')
                raw_title = field.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='organization_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='organization_field',
                        obj_id=field_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

                options = field.get('custom_field_options', [])
                for opt in options:
                    opt_id = opt.get('id')
                    opt_name = opt.get('name', '')
                    opt_raw = opt.get('raw_name', opt_name)
                    opt_value = opt.get('value', '')

                    dc_match = DC_PLACEHOLDER_PATTERN.search(str(opt_raw))

                    if dc_match:
                        placeholder = dc_match.group(0)
                        dc_info = self._find_dc_by_placeholder(placeholder)

                        self._add_work_item(
                            obj_type='organization_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            dc_placeholder=placeholder,
                            dc_info=dc_info,
                            parent_id=field_id,
                            extra={'option_value': opt_value}
                        )
                    else:
                        self._add_work_item(
                            obj_type='organization_field_option',
                            obj_id=opt_id,
                            field_name='name',
                            current_value=opt_name,
                            raw_value=opt_raw,
                            parent_id=field_id,
                            extra={'option_value': opt_value}
                        )

            log_signal.emit(f"  Found {count} organization fields")

        except Exception as e:
            log_signal.emit(f"  Error scanning organization fields: {e}")
            logger.error(f"Error scanning organization fields: {e}")

        return count

    def _scan_groups(self, log_signal) -> int:
        """Scan groups."""
        count = 0

        try:
            groups = self.api.get_groups()

            for group in groups:
                if self._should_stop():
                    break

                group_id = group.get('id')
                name = group.get('name', '')

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(name))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='group',
                        obj_id=group_id,
                        field_name='name',
                        current_value=name,
                        raw_value=name,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='group',
                        obj_id=group_id,
                        field_name='name',
                        current_value=name,
                        raw_value=name
                    )

                count += 1

            log_signal.emit(f"  Found {count} groups")

        except Exception as e:
            log_signal.emit(f"  Error scanning groups: {e}")
            logger.error(f"Error scanning groups: {e}")

        return count

    def _scan_macros(self, log_signal) -> int:
        """Scan macros."""
        count = 0

        try:
            macros = self.api.get_macros()

            for macro in macros:
                if self._should_stop():
                    break

                macro_id = macro.get('id')
                title = macro.get('title', '')
                raw_title = macro.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='macro',
                        obj_id=macro_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='macro',
                        obj_id=macro_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

            log_signal.emit(f"  Found {count} macros")

        except Exception as e:
            log_signal.emit(f"  Error scanning macros: {e}")
            logger.error(f"Error scanning macros: {e}")

        return count

    def _scan_triggers(self, log_signal) -> int:
        """Scan triggers."""
        count = 0

        try:
            triggers = self.api.get_triggers()

            for trigger in triggers:
                if self._should_stop():
                    break

                trigger_id = trigger.get('id')
                title = trigger.get('title', '')
                raw_title = trigger.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='trigger',
                        obj_id=trigger_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='trigger',
                        obj_id=trigger_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

            log_signal.emit(f"  Found {count} triggers")

        except Exception as e:
            log_signal.emit(f"  Error scanning triggers: {e}")
            logger.error(f"Error scanning triggers: {e}")

        return count

    def _scan_automations(self, log_signal) -> int:
        """Scan automations."""
        count = 0

        try:
            automations = self.api.get_automations()

            for automation in automations:
                if self._should_stop():
                    break

                automation_id = automation.get('id')
                title = automation.get('title', '')
                raw_title = automation.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='automation',
                        obj_id=automation_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='automation',
                        obj_id=automation_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

            log_signal.emit(f"  Found {count} automations")

        except Exception as e:
            log_signal.emit(f"  Error scanning automations: {e}")
            logger.error(f"Error scanning automations: {e}")

        return count

    def _scan_views(self, log_signal) -> int:
        """Scan views."""
        count = 0

        try:
            views = self.api.get_views()

            for view in views:
                if self._should_stop():
                    break

                view_id = view.get('id')
                title = view.get('title', '')
                raw_title = view.get('raw_title', title)

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='view',
                        obj_id=view_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='view',
                        obj_id=view_id,
                        field_name='title',
                        current_value=title,
                        raw_value=raw_title
                    )

                count += 1

            log_signal.emit(f"  Found {count} views")

        except Exception as e:
            log_signal.emit(f"  Error scanning views: {e}")
            logger.error(f"Error scanning views: {e}")

        return count

    def _scan_sla_policies(self, log_signal) -> int:
        """Scan SLA policies."""
        count = 0

        try:
            policies = self.api.get_sla_policies()

            for policy in policies:
                if self._should_stop():
                    break

                policy_id = policy.get('id')
                title = policy.get('title', '')

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(title))

                if dc_match:
                    placeholder = dc_match.group(0)
                    dc_info = self._find_dc_by_placeholder(placeholder)

                    self._add_work_item(
                        obj_type='sla_policy',
                        obj_id=policy_id,
                        field_name='title',
                        current_value=title,
                        raw_value=title,
                        dc_placeholder=placeholder,
                        dc_info=dc_info
                    )
                else:
                    self._add_work_item(
                        obj_type='sla_policy',
                        obj_id=policy_id,
                        field_name='title',
                        current_value=title,
                        raw_value=title
                    )

                count += 1

            log_signal.emit(f"  Found {count} SLA policies")

        except Exception as e:
            log_signal.emit(f"  Error scanning SLA policies: {e}")
            logger.error(f"Error scanning SLA policies: {e}")

        return count

    def _scan_hc_categories(self, log_signal) -> int:
        """Scan Help Center categories."""
        count = 0

        try:
            categories = self.api.get_hc_categories()

            for cat in categories:
                if self._should_stop():
                    break

                cat_id = cat.get('id')
                name = cat.get('name', '')

                self._add_work_item(
                    obj_type='category',
                    obj_id=cat_id,
                    field_name='name',
                    current_value=name,
                    raw_value=name
                )

                count += 1

            log_signal.emit(f"  Found {count} HC categories")

        except Exception as e:
            log_signal.emit(f"  Error scanning HC categories: {e}")
            logger.error(f"Error scanning HC categories: {e}")

        return count

    def _scan_hc_sections(self, log_signal) -> int:
        """Scan Help Center sections."""
        count = 0

        try:
            sections = self.api.get_hc_sections()

            for sect in sections:
                if self._should_stop():
                    break

                sect_id = sect.get('id')
                name = sect.get('name', '')

                self._add_work_item(
                    obj_type='section',
                    obj_id=sect_id,
                    field_name='name',
                    current_value=name,
                    raw_value=name
                )

                count += 1

            log_signal.emit(f"  Found {count} HC sections")

        except Exception as e:
            log_signal.emit(f"  Error scanning HC sections: {e}")
            logger.error(f"Error scanning HC sections: {e}")

        return count

    def _scan_hc_articles(self, log_signal) -> int:
        """Scan Help Center articles."""
        count = 0

        try:
            articles = self.api.get_hc_articles()

            for art in articles:
                if self._should_stop():
                    break

                art_id = art.get('id')
                title = art.get('title', '')

                self._add_work_item(
                    obj_type='article',
                    obj_id=art_id,
                    field_name='title',
                    current_value=title,
                    raw_value=title
                )

                count += 1

            log_signal.emit(f"  Found {count} HC articles")

        except Exception as e:
            log_signal.emit(f"  Error scanning HC articles: {e}")
            logger.error(f"Error scanning HC articles: {e}")

        return count

    def _add_work_item(
        self,
        obj_type: str,
        obj_id: int,
        field_name: str,
        current_value: str,
        raw_value: str,
        dc_placeholder: str = None,
        dc_info: Dict = None,
        parent_id: int = None,
        is_system: bool = False,
        extra: Dict = None
    ):
        """Add a work item to the list."""
        action = 'CREATE'
        dc_id = None
        pt_text = current_value
        en_text = ''
        es_text = ''
        source = SOURCE_NEW
        placeholder_source = 'proposed'
        already_linked = False

        raw_is_dc = is_dc_placeholder(raw_value)

        if raw_is_dc and dc_info:
            # Field is linked to DC and we found the DC in our cache
            action = 'LINK'
            dc_id = dc_info.get('id')
            source = SOURCE_ZENDESK_DC
            placeholder_source = 'existing'
            already_linked = True
            dc_placeholder = dc_info.get('placeholder', dc_placeholder)

            variants = dc_info.get('variants', {})
            if 16 in variants:
                pt_text = variants[16].get('content', current_value)
            if 1 in variants:
                en_text = variants[1].get('content', '')
            if 2 in variants:
                es_text = variants[2].get('content', '')

        elif raw_is_dc:
            # Field is linked to DC but we couldn't find it in our cache
            # This is OK - the field is still properly linked in Zendesk
            action = 'LINK'
            source = SOURCE_ZENDESK_DC
            placeholder_source = 'existing'
            already_linked = True
            pt_text = current_value
            dc_placeholder = raw_value.strip()

            # Log at DEBUG level - this is informational, not an error
            logger.debug(
                f"DC reference not in cache: {raw_value} for {obj_type} "
                f"{obj_id}. This is normal if the DC exists with a different "
                f"name format."
            )

        else:
            # Field is not linked to DC yet - check if a matching DC exists
            existing_dc = self._find_dc_by_name(current_value)
            if existing_dc:
                action = 'LINK'
                dc_id = existing_dc.get('id')
                dc_placeholder = existing_dc.get('placeholder', '')
                source = SOURCE_ZENDESK_DC
                placeholder_source = 'existing'
                already_linked = False

                variants = existing_dc.get('variants', {})
                if 16 in variants:
                    pt_text = variants[16].get('content', current_value)
                if 1 in variants:
                    en_text = variants[1].get('content', '')
                if 2 in variants:
                    es_text = variants[2].get('content', '')
            else:
                dc_placeholder = generate_dc_placeholder(current_value)

        context = CONTEXT_MAP.get(obj_type, 'Other')
        type_name = TYPE_DISPLAY_MAP.get(
            obj_type, obj_type.replace('_', ' ').title()
        )

        item = {
            'type': obj_type,
            'type_display': type_name,
            'obj_id': obj_id,
            'field_name': field_name,
            'current_value': current_value,
            'raw_value': raw_value,
            'action': action,
            'dc_placeholder': dc_placeholder,
            'dc_id': dc_id,
            'context': context,
            'pt': pt_text,
            'en': en_text,
            'es': es_text,
            'source': source,
            'pt_source': source,
            'en_source': SOURCE_ZENDESK_DC if en_text else SOURCE_NEW,
            'es_source': SOURCE_ZENDESK_DC if es_text else SOURCE_NEW,
            'is_system': is_system,
            'is_reserved': is_system,
            'parent_id': parent_id,
            'placeholder_source': placeholder_source,
            'already_linked': already_linked,
        }

        if extra:
            item.update(extra)

        self.work_items.append(item)

    def update_work_item(self, index: int, updates: Dict[str, Any]):
        """Update a work item at the given index."""
        if 0 <= index < len(self.work_items):
            self.work_items[index].update(updates)

    def perform_translation(
        self,
        progress_signal,
        log_signal,
        force: bool = False
    ) -> TranslationStats:
        """Perform translation for all work items."""
        self._reset_stop()

        if not self.translator:
            self.translator = TranslationService()

        stats = TranslationStats()
        total = len(self.work_items)
        stats.total = total

        log_signal.emit(f"Starting translation of {total} items...")

        for i, item in enumerate(self.work_items):
            if self._should_stop():
                log_signal.emit("Translation canceled by user")
                raise Exception("Canceled by user")

            progress_signal.emit(
                i + 1, total, f"Translating {i + 1}/{total}..."
            )

            if item.get('is_system') or item.get('is_reserved'):
                continue

            pt_text = item.get('pt', '')

            if not pt_text:
                continue

            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

            needs_en = force or en_source == SOURCE_NEW or not item.get('en')
            needs_es = force or es_source == SOURCE_NEW or not item.get('es')

            if not needs_en and not needs_es:
                continue

            try:
                if needs_en:
                    en_result, en_from_cache = self.translator.translate(
                        pt_text, 'pt', 'en'
                    )
                    if en_result:
                        item['en'] = en_result
                        item['en_source'] = (
                            SOURCE_CACHE if en_from_cache else SOURCE_TRANSLATED
                        )
                        if en_from_cache:
                            stats.from_cache += 1
                        else:
                            stats.translated += 1
                    else:
                        item['en_source'] = SOURCE_FAILED
                        stats.failed += 1

                if needs_es:
                    es_result, es_from_cache = self.translator.translate(
                        pt_text, 'pt', 'es'
                    )
                    if es_result:
                        item['es'] = es_result
                        item['es_source'] = (
                            SOURCE_CACHE if es_from_cache else SOURCE_TRANSLATED
                        )
                        if es_from_cache:
                            stats.from_cache += 1
                        else:
                            stats.translated += 1
                    else:
                        item['es_source'] = SOURCE_FAILED
                        stats.failed += 1

            except Exception as e:
                logger.error(f"Translation error for item {i}: {e}")
                item['en_source'] = SOURCE_FAILED
                item['es_source'] = SOURCE_FAILED
                stats.failed += 1

        log_signal.emit(
            f"Translation complete: {stats.translated} translated, "
            f"{stats.from_cache} from cache, {stats.failed} failed"
        )

        return stats

    def perform_translation_for_indices(
        self,
        progress_signal,
        log_signal,
        selected_indices: List[int],
        force_retranslate: bool = False
    ) -> TranslationStats:
        """Translate only the items at the specified indices."""
        self._reset_stop()

        if not self.translator:
            self.translator = TranslationService()

        stats = TranslationStats()

        items_to_consider = []
        for idx in selected_indices:
            if idx < len(self.work_items):
                item = self.work_items[idx]
                if (not item.get('is_system', False) and
                        not item.get('is_reserved', False)):
                    items_to_consider.append((idx, item))

        system_skipped = len(selected_indices) - len(items_to_consider)
        if system_skipped > 0:
            log_signal.emit(f"Skipping {system_skipped} system/reserved items")

        if force_retranslate:
            to_translate = items_to_consider
            log_signal.emit(
                f"[RE-TRANSLATE] Processing {len(to_translate)} selected items"
            )
        else:
            to_translate = []
            for idx, item in items_to_consider:
                en_text = item.get('en', '')
                es_text = item.get('es', '')
                en_source = item.get('en_source', SOURCE_NEW)
                es_source = item.get('es_source', SOURCE_NEW)

                needs_en = not en_text or en_source == SOURCE_NEW
                needs_es = not es_text or es_source == SOURCE_NEW

                if needs_en or needs_es:
                    to_translate.append((idx, item))

            already_done = len(items_to_consider) - len(to_translate)
            if already_done > 0:
                log_signal.emit(
                    f"Skipping {already_done} items that already have "
                    f"translations"
                )

        total = len(to_translate)
        stats.total = total

        log_signal.emit(f"Translating {total} items...")

        if total == 0:
            log_signal.emit("No items need translation.")
            return stats

        for i, (idx, item) in enumerate(to_translate):
            if self._should_stop():
                log_signal.emit("Translation canceled by user")
                raise Exception("Canceled by user")

            progress_signal.emit(
                i + 1, total, f"Translating {i + 1}/{total}..."
            )

            pt_text = item.get('pt', '')

            if not pt_text:
                continue

            en_source = item.get('en_source', SOURCE_NEW)
            es_source = item.get('es_source', SOURCE_NEW)

            if force_retranslate:
                needs_en = True
                needs_es = True
            else:
                needs_en = en_source == SOURCE_NEW or not item.get('en')
                needs_es = es_source == SOURCE_NEW or not item.get('es')

            if not needs_en and not needs_es:
                continue

            try:
                if needs_en:
                    en_result, en_from_cache = self.translator.translate(
                        pt_text, 'pt', 'en'
                    )
                    if en_result:
                        self.work_items[idx]['en'] = en_result
                        self.work_items[idx]['en_source'] = (
                            SOURCE_CACHE if en_from_cache else SOURCE_TRANSLATED
                        )
                        if en_from_cache:
                            stats.from_cache += 1
                        else:
                            stats.translated += 1
                    else:
                        self.work_items[idx]['en_source'] = SOURCE_FAILED
                        stats.failed += 1

                if needs_es:
                    es_result, es_from_cache = self.translator.translate(
                        pt_text, 'pt', 'es'
                    )
                    if es_result:
                        self.work_items[idx]['es'] = es_result
                        self.work_items[idx]['es_source'] = (
                            SOURCE_CACHE if es_from_cache else SOURCE_TRANSLATED
                        )
                        if es_from_cache:
                            stats.from_cache += 1
                        else:
                            stats.translated += 1
                    else:
                        self.work_items[idx]['es_source'] = SOURCE_FAILED
                        stats.failed += 1

            except Exception as e:
                logger.error(f"Translation error for item {idx}: {e}")
                self.work_items[idx]['en_source'] = SOURCE_FAILED
                self.work_items[idx]['es_source'] = SOURCE_FAILED
                stats.failed += 1

        log_signal.emit(
            f"Translation complete: {stats.translated} translated, "
            f"{stats.from_cache} from cache, {stats.failed} failed"
        )

        return stats

    def execute_changes(
        self,
        items: List[Dict[str, Any]],
        progress_signal,
        log_signal
    ) -> Dict[str, Any]:
        """Execute changes to Zendesk."""
        self._reset_stop()

        result = {
            'success': [],
            'failed': [],
            'skipped': [],
            'backup_file': ''
        }

        if self.backup_folder:
            backup_file = self._create_backup(items, log_signal)
            result['backup_file'] = backup_file

        log_signal.emit("Refreshing Dynamic Content list...")
        self._refresh_dc_cache()

        sorted_items = self._sort_items_for_apply(items)

        total = len(sorted_items)
        log_signal.emit(f"Applying {total} changes to Zendesk...")

        for i, item in enumerate(sorted_items):
            if self._should_stop():
                log_signal.emit("Apply canceled by user")
                raise Exception("Canceled by user")

            progress_signal.emit(i + 1, total, f"Applying {i + 1}/{total}...")

            try:
                success, skipped, msg = self._apply_single_item(
                    item, log_signal
                )
                if skipped:
                    result['skipped'].append(item)
                elif success:
                    result['success'].append(item)
                else:
                    result['failed'].append(item)
            except Exception as e:
                logger.error(f"Failed to apply item: {e}")
                result['failed'].append(item)
                log_signal.emit(f"    Error: {e}")

        skipped_count = len(result['skipped'])
        if skipped_count > 0:
            log_signal.emit(f"Skipped {skipped_count} already-linked items")

        log_signal.emit(
            f"Apply complete: {len(result['success'])} succeeded, "
            f"{len(result['failed'])} failed, {skipped_count} skipped"
        )

        return result

    def _refresh_dc_cache(self):
        """Refresh the DC cache from Zendesk."""
        try:
            dc_items = self.api.get_dynamic_content_items()
            self._build_dc_cache(dc_items)
        except Exception as e:
            logger.error(f"Error refreshing DC cache: {e}")

    def _sort_items_for_apply(
        self,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Sort items for optimal apply order."""
        already_linked = []
        options = []
        parents = []

        for item in items:
            if item.get('already_linked', False):
                already_linked.append(item)
            elif '_option' in item.get('type', ''):
                options.append(item)
            else:
                parents.append(item)

        return already_linked + options + parents

    def _apply_single_item(
        self,
        item: Dict[str, Any],
        log_signal
    ) -> Tuple[bool, bool, str]:
        """Apply a single item change to Zendesk."""
        obj_type = item.get('type')
        obj_id = item.get('obj_id')
        pt_text = item.get('pt', '')
        en_text = item.get('en', '')
        es_text = item.get('es', '')
        force_update = item.get('force_update', False)
        current_value = item.get('current_value', pt_text)
        raw_value = item.get('raw_value', '')
        parent_id = item.get('parent_id')
        option_value = item.get('option_value', '')

        log_signal.emit(
            f"  Processing {obj_type} {obj_id}: {current_value[:50]}..."
        )

        # Debug logging for options
        if '_option' in obj_type:
            logger.debug(
                f"Option details: parent_id={parent_id}, "
                f"option_value='{option_value}', obj_id={obj_id}"
            )

        try:
            # Check if raw_value is already a DC placeholder
            if is_dc_placeholder(raw_value):
                dc_id = item.get('dc_id')

                if dc_id and force_update:
                    self.api.update_dynamic_content_variants(
                        dc_id,
                        [
                            {'locale_id': 16, 'content': pt_text},
                            {'locale_id': 1, 'content': en_text},
                            {'locale_id': 2, 'content': es_text},
                        ]
                    )
                    log_signal.emit("    Updated DC translations")
                    return True, False, "Updated DC"
                else:
                    log_signal.emit("    Already linked to DC (skipped)")
                    return True, True, "Already linked"

            # Not linked yet - need to create or find DC
            safe_dc_name = generate_dc_name(current_value)
            if not safe_dc_name:
                log_signal.emit("    Cannot generate DC name (skipped)")
                return False, True, "Cannot generate DC name"

            # Check if DC with this name already exists
            existing_dc = self._find_dc_by_name(current_value)

            placeholder = None
            dc_id = None

            if existing_dc:
                dc_id = existing_dc.get('id')
                placeholder = existing_dc.get('placeholder')

                if force_update:
                    self.api.update_dynamic_content_variants(
                        dc_id,
                        [
                            {'locale_id': 16, 'content': pt_text},
                            {'locale_id': 1, 'content': en_text},
                            {'locale_id': 2, 'content': es_text},
                        ]
                    )
                    log_signal.emit(f"    Updated existing DC: {placeholder}")
            else:
                # Create new DC
                try:
                    dc_item = self.api.create_dynamic_content(
                        name=safe_dc_name,
                        default_locale_id=16,
                        variants=[
                            {
                                'locale_id': 16,
                                'content': pt_text,
                                'default': True
                            },
                            {'locale_id': 1, 'content': en_text},
                            {'locale_id': 2, 'content': es_text},
                        ]
                    )

                    if dc_item:
                        placeholder = dc_item.get('placeholder', '')
                        dc_id = dc_item.get('id')

                        dc_info = {
                            'id': dc_id,
                            'name': safe_dc_name,
                            'placeholder': placeholder,
                            'variants': {}
                        }
                        self.dc_map[placeholder] = dc_info
                        self.dc_by_name[safe_dc_name.lower()] = dc_info
                        self.dc_name_map[safe_dc_name.lower()] = placeholder

                        log_signal.emit(f"    Created DC: {placeholder}")

                except Exception as e:
                    error_str = str(e)

                    if '422' in error_str:
                        self._refresh_dc_cache()
                        existing_dc = self._find_dc_by_name(current_value)

                        if existing_dc:
                            dc_id = existing_dc.get('id')
                            placeholder = existing_dc.get('placeholder')
                            log_signal.emit(
                                f"    Using existing DC: {placeholder}"
                            )
                        else:
                            log_signal.emit(f"    Error creating DC: {e}")
                            return False, False, str(e)
                    else:
                        log_signal.emit(f"    Error creating DC: {e}")
                        return False, False, str(e)

            # Now update the object to use the DC placeholder
            if placeholder:
                try:
                    self._update_object_with_dc(
                        obj_type, obj_id, item.get('field_name'),
                        placeholder, parent_id, option_value, log_signal
                    )

                    item['dc_id'] = dc_id
                    item['dc_placeholder'] = placeholder
                    item['placeholder_source'] = 'existing'
                    item['action'] = 'LINK'
                    item['already_linked'] = True

                    return True, False, "Linked to DC"

                except Exception as e:
                    log_signal.emit(f"    Warning: {e}")
                    return False, False, str(e)

            return False, False, "Unknown error"

        except Exception as e:
            logger.error(f"Error applying item {obj_type} {obj_id}: {e}")
            log_signal.emit(f"    Error: {e}")
            return False, False, str(e)

    def _update_object_with_dc(
        self,
        obj_type: str,
        obj_id: int,
        field_name: str,
        placeholder: str,
        parent_id: int,
        option_value: str,
        log_signal
    ):
        """Update a Zendesk object to use a DC placeholder."""
        try:
            if obj_type == 'ticket_field':
                self.api.update_ticket_field(obj_id, {field_name: placeholder})

            elif obj_type == 'ticket_field_option':
                if not parent_id:
                    raise Exception("No parent_id for ticket_field_option")

                if not option_value:
                    raise Exception("No option_value for ticket_field_option")

                # Use field-based update (more reliable)
                log_signal.emit(
                    f"    Updating via field, value='{option_value}'"
                )
                self.api.update_ticket_field_option_via_field(
                    parent_id, option_value, placeholder
                )

            elif obj_type == 'ticket_form':
                self.api.update_ticket_form(obj_id, {field_name: placeholder})

            elif obj_type == 'custom_status':
                self.api.update_custom_status(obj_id, {field_name: placeholder})

            elif obj_type == 'user_field':
                self.api.update_user_field(obj_id, {field_name: placeholder})

            elif obj_type == 'user_field_option':
                if not parent_id:
                    raise Exception("No parent_id for user_field_option")

                if not option_value:
                    raise Exception("No option_value for user_field_option")

                self.api.update_user_field_option_via_field(
                    parent_id, option_value, placeholder
                )

            elif obj_type == 'organization_field':
                self.api.update_organization_field(
                    obj_id, {field_name: placeholder}
                )

            elif obj_type == 'organization_field_option':
                if not parent_id:
                    raise Exception(
                        "No parent_id for organization_field_option"
                    )

                if not option_value:
                    raise Exception(
                        "No option_value for organization_field_option"
                    )

                self.api.update_organization_field_option_via_field(
                    parent_id, option_value, placeholder
                )

            elif obj_type == 'group':
                self.api.update_group(obj_id, {field_name: placeholder})

            elif obj_type == 'macro':
                self.api.update_macro(obj_id, {field_name: placeholder})

            elif obj_type == 'trigger':
                self.api.update_trigger(obj_id, {field_name: placeholder})

            elif obj_type == 'automation':
                self.api.update_automation(obj_id, {field_name: placeholder})

            elif obj_type == 'view':
                self.api.update_view(obj_id, {field_name: placeholder})

            elif obj_type == 'sla_policy':
                self.api.update_sla_policy(obj_id, {field_name: placeholder})

        except Exception as e:
            log_signal.emit(f"    Warning: Could not update {obj_type}: {e}")
            raise

    def _create_backup(
        self,
        items: List[Dict[str, Any]],
        log_signal
    ) -> str:
        """Create a backup file for the items being modified."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"backup_{timestamp}.json"
        filepath = os.path.join(self.backup_folder, filename)

        backup_data = {
            'timestamp': timestamp,
            'items': items
        }

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            log_signal.emit(f"Backup created: {filepath}")
            return filepath
        except Exception as e:
            log_signal.emit(f"Warning: Could not create backup: {e}")
            return ""

    def load_backup_thread(
        self,
        progress_signal,
        log_signal,
        filepath: str
    ) -> List[Dict[str, Any]]:
        """Load backup file in a thread."""
        log_signal.emit(f"Loading backup: {filepath}")

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            items = data.get('items', [])
            log_signal.emit(f"Loaded {len(items)} items from backup")
            return items

        except Exception as e:
            logger.error(f"Error loading backup: {e}")
            raise

    def perform_restore_from_data(
        self,
        items: List[Dict[str, Any]],
        progress_signal,
        log_signal
    ) -> str:
        """Restore items from backup data."""
        self._reset_stop()

        total = len(items)
        success_count = 0
        failed_count = 0

        log_signal.emit(f"Restoring {total} items...")

        for i, item in enumerate(items):
            if self._should_stop():
                log_signal.emit("Restore canceled by user")
                raise Exception("Canceled by user")

            progress_signal.emit(i + 1, total, f"Restoring {i + 1}/{total}...")

            try:
                obj_type = item.get('type')
                obj_id = item.get('obj_id')
                field_name = item.get('field_name')
                original_value = item.get('current_value', '')
                option_value = item.get('option_value', '')

                log_signal.emit(f"  Restoring {obj_type} {obj_id}...")

                self._update_object_with_dc(
                    obj_type, obj_id, field_name,
                    original_value, item.get('parent_id'),
                    option_value, log_signal
                )

                success_count += 1

            except Exception as e:
                logger.error(f"Failed to restore item: {e}")
                log_signal.emit(f"    Error: {e}")
                failed_count += 1

        return f"Restored {success_count} items, {failed_count} failed"

    def clear_cache(self) -> bool:
        """Clear translation cache."""
        if self.translator:
            return self.translator.clear_cache()
        return True

    def save_profile(
        self,
        filepath: str,
        subdomain: str,
        email: str,
        token: str,
        backup_path: str,
        google_api_key: str = "",
        protect_acronyms: bool = True,
        cache_expiry_days: int = 30
    ) -> bool:
        """Save profile to file."""
        try:
            data = {
                'subdomain': subdomain,
                'email': email,
                'token': token,
                'backup_path': backup_path,
                'google_api_key': google_api_key,
                'protect_acronyms': protect_acronyms,
                'cache_expiry_days': cache_expiry_days
            }

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            return True

        except Exception as e:
            logger.error(f"Error saving profile: {e}")
            return False

    def load_profile(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Load profile from file."""
        try:
            if not os.path.exists(filepath):
                return None

            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Error loading profile: {e}")
            return None

    def cleanup(self):
        """Cleanup resources."""
        self.stop()
        if self.translator:
            self.translator.save_cache()