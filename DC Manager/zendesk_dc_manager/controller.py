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
    LOCALE_ID_PT_BR,
    LOCALE_ID_EN_US,
    LOCALE_ID_ES,
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
        self.pt_locale_id: int = LOCALE_ID_PT_BR
        self.en_locale_id: int = LOCALE_ID_EN_US
        self.es_locale_id: int = LOCALE_ID_ES
        # When set to a dict, _update_object_with_dc queues option updates
        # instead of applying them immediately; execute_changes flushes them
        # as batched GET+PUT calls (one per parent field, not one per option).
        self._deferred_option_updates: Optional[Dict] = None

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
                    f"Agent Locale: {locale_name} ({locale_code})"
                )
            except Exception as e:
                log_signal.emit(
                    f"Warning: Could not fetch default locale: {e}"
                )
                self.default_locale = {}

            try:
                locales = self.api.get_locales()
                by_code = {loc['locale'].lower(): loc['id'] for loc in locales}
                self.pt_locale_id = (
                    by_code.get('pt-br') or by_code.get('pt') or LOCALE_ID_PT_BR
                )
                self.en_locale_id = (
                    by_code.get('en-us') or by_code.get('en') or LOCALE_ID_EN_US
                )
                self.es_locale_id = by_code.get('es') or LOCALE_ID_ES
                log_signal.emit(
                    f"Locale IDs resolved: PT={self.pt_locale_id}, "
                    f"EN={self.en_locale_id}, ES={self.es_locale_id}"
                )
            except Exception as e:
                log_signal.emit(f"Warning: Could not resolve locale IDs: {e}")

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
        progress_signal.emit(0, 0, "Fetching DC items...")

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

    # =========================================================================
    # GENERIC SCAN HELPERS
    # =========================================================================

    def _scan_named_objects(
        self,
        log_signal,
        api_method,
        obj_type: str,
        name_field: str = 'title',
        raw_field: str = None,
        extra_func=None,
        is_system_func=None,
    ) -> int:
        """Generic scanner for simple named objects without sub-items."""
        raw_field = raw_field or f'raw_{name_field}'
        count = 0
        try:
            items = api_method()
            for item in items:
                if self._should_stop():
                    break

                obj_id = item.get('id')
                name = item.get(name_field, '')
                raw = item.get(raw_field, name)
                is_system = is_system_func(item) if is_system_func else False
                extra = extra_func(item) if extra_func else None

                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw))
                kwargs = dict(
                    obj_type=obj_type,
                    obj_id=obj_id,
                    field_name=name_field,
                    current_value=name,
                    raw_value=raw,
                    is_system=is_system,
                )
                if extra:
                    kwargs['extra'] = extra
                if dc_match:
                    placeholder = dc_match.group(0)
                    kwargs['dc_placeholder'] = placeholder
                    kwargs['dc_info'] = self._find_dc_by_placeholder(
                        placeholder
                    )
                self._add_work_item(**kwargs)
                count += 1
        except Exception as e:
            log_signal.emit(f"  Error scanning {obj_type}: {e}")
            logger.error(f"Error scanning {obj_type}: {e}")
        return count

    def _scan_fields_with_options(
        self,
        log_signal,
        api_method,
        field_type: str,
        option_type: str,
        field_extra_func=None,
        option_extra_func=None,
        is_system_func=None,
    ) -> Tuple[int, int]:
        """Generic scanner for fields with custom_field_options sub-items.

        Returns:
            Tuple of (field_count, system_field_count)
        """
        count = 0
        system_count = 0
        try:
            fields = api_method()
            for field in fields:
                if self._should_stop():
                    break

                field_id = field.get('id')
                title = field.get('title', '')
                raw_title = field.get('raw_title', title)
                is_system = is_system_func(field) if is_system_func else False
                if is_system:
                    system_count += 1

                field_extra = (
                    field_extra_func(field) if field_extra_func else None
                )
                dc_match = DC_PLACEHOLDER_PATTERN.search(str(raw_title))
                kwargs = dict(
                    obj_type=field_type,
                    obj_id=field_id,
                    field_name='title',
                    current_value=title,
                    raw_value=raw_title,
                    is_system=is_system,
                )
                if field_extra:
                    kwargs['extra'] = field_extra
                if dc_match:
                    placeholder = dc_match.group(0)
                    kwargs['dc_placeholder'] = placeholder
                    kwargs['dc_info'] = self._find_dc_by_placeholder(
                        placeholder
                    )
                self._add_work_item(**kwargs)
                count += 1

                for opt in field.get('custom_field_options', []):
                    if self._should_stop():
                        break

                    opt_id = opt.get('id')
                    opt_name = opt.get('name', '')
                    opt_raw = opt.get('raw_name', opt_name)
                    opt_value = opt.get('value', '')

                    logger.debug(
                        f"Option: id={opt_id}, name={opt_name}, "
                        f"value={opt_value}"
                    )

                    if option_extra_func:
                        opt_extra = option_extra_func(field, opt)
                    else:
                        opt_extra = {'option_value': opt_value}
                    # Store parent field title so _apply_single_item can build
                    # a namespaced DC name: {parent_name}_{option_name}
                    opt_extra['parent_name'] = title

                    dc_match = DC_PLACEHOLDER_PATTERN.search(str(opt_raw))
                    opt_kwargs = dict(
                        obj_type=option_type,
                        obj_id=opt_id,
                        field_name='name',
                        current_value=opt_name,
                        raw_value=opt_raw,
                        parent_id=field_id,
                        is_system=is_system,
                        extra=opt_extra,
                    )
                    if dc_match:
                        placeholder = dc_match.group(0)
                        opt_kwargs['dc_placeholder'] = placeholder
                        opt_kwargs['dc_info'] = self._find_dc_by_placeholder(
                            placeholder
                        )
                    self._add_work_item(**opt_kwargs)

        except Exception as e:
            log_signal.emit(f"  Error scanning {field_type}: {e}")
            logger.error(f"Error scanning {field_type}: {e}")
        return count, system_count

    # =========================================================================
    # SCAN METHODS
    # =========================================================================

    def _scan_ticket_fields(self, log_signal) -> Tuple[int, int]:
        """Scan ticket fields and their options."""
        SYSTEM_FIELD_KEYS = frozenset([
            'approval_status',
            'zd_approval_status',
            'resolution_type',
            'zd_resolution_type',
            'zd_automated_resolution',
        ])
        SYSTEM_KEY_PATTERNS = [
            'zd_es_approval',
            'zd_automated',
            'zd_resolution',
        ]
        SYSTEM_TITLE_PATTERNS = [
            'approval status', 'status de aprovação',
            'resolution type', 'tipo de resolução', 'tipo de resolucao',
        ]

        def is_system(field):
            field_type = field.get('type', '')
            field_key = field.get('key', '')
            title = field.get('title', '')
            if field_type in SYSTEM_FIELD_TYPES:
                logger.debug(
                    f"System field: {field.get('id')} - {title} "
                    f"(type={field_type}, key={field_key})"
                )
                return True
            if field_key:
                fk = field_key.lower()
                if fk in SYSTEM_FIELD_KEYS:
                    logger.debug(
                        f"System field: {field.get('id')} - {title} "
                        f"(type={field_type}, key={field_key})"
                    )
                    return True
                if any(p in fk for p in SYSTEM_KEY_PATTERNS):
                    logger.debug(
                        f"System field: {field.get('id')} - {title} "
                        f"(type={field_type}, key={field_key})"
                    )
                    return True
            if any(p in title.lower() for p in SYSTEM_TITLE_PATTERNS):
                logger.debug(
                    f"System field: {field.get('id')} - {title} "
                    f"(type={field_type}, key={field_key})"
                )
                return True
            return False

        def field_extra(field):
            return {
                'field_type': field.get('type', ''),
                'field_key': field.get('key', ''),
            }

        def option_extra(field, opt):
            return {
                'field_type': field.get('type', ''),
                'field_key': field.get('key', ''),
                'option_value': opt.get('value', ''),
            }

        count, system_count = self._scan_fields_with_options(
            log_signal,
            self.api.get_ticket_fields,
            'ticket_field',
            'ticket_field_option',
            field_extra_func=field_extra,
            option_extra_func=option_extra,
            is_system_func=is_system,
        )
        log_signal.emit(
            f"  Found {count} ticket fields ({system_count} system)"
        )
        return count, system_count

    def _scan_ticket_forms(self, log_signal) -> int:
        """Scan ticket forms."""
        count = self._scan_named_objects(
            log_signal, self.api.get_ticket_forms, 'ticket_form',
            name_field='name', raw_field='raw_name',
        )
        log_signal.emit(f"  Found {count} ticket forms")
        return count

    def _scan_custom_statuses(self, log_signal) -> int:
        """Scan custom ticket statuses."""
        PROTECTED_STATUS_CATEGORIES = frozenset(['hold'])
        SYSTEM_STATUS_NAMES = frozenset([
            'new', 'open', 'pending', 'hold', 'on-hold', 'on hold',
            'solved', 'closed', 'deleted',
            'novo', 'aberto', 'pendente', 'em espera', 'resolvido',
            'nuevo', 'abierto', 'pendiente', 'en espera', 'resuelto',
        ])

        def is_system(status):
            category = status.get('status_category', '').lower()
            if category in PROTECTED_STATUS_CATEGORIES:
                logger.debug(
                    f"Protected status category: {status.get('id')} - "
                    f"{status.get('agent_label')} (category={category})"
                )
                return True
            if status.get('default', False):
                label = status.get('agent_label', '').lower().strip()
                if label in SYSTEM_STATUS_NAMES:
                    logger.debug(
                        f"Default system status: {status.get('id')} - "
                        f"{status.get('agent_label')} (default=True)"
                    )
                    return True
            return False

        def status_extra(status):
            return {
                'status_category': status.get('status_category', ''),
                'is_default': status.get('default', False),
            }

        count = self._scan_named_objects(
            log_signal, self.api.get_custom_statuses, 'custom_status',
            name_field='agent_label', raw_field='raw_agent_label',
            extra_func=status_extra, is_system_func=is_system,
        )
        log_signal.emit(f"  Found {count} custom statuses")
        return count

    def _scan_user_fields(self, log_signal) -> int:
        """Scan user fields and their options."""
        count, _ = self._scan_fields_with_options(
            log_signal, self.api.get_user_fields,
            'user_field', 'user_field_option',
        )
        log_signal.emit(f"  Found {count} user fields")
        return count

    def _scan_org_fields(self, log_signal) -> int:
        """Scan organization fields and their options."""
        count, _ = self._scan_fields_with_options(
            log_signal, self.api.get_organization_fields,
            'organization_field', 'organization_field_option',
        )
        log_signal.emit(f"  Found {count} organization fields")
        return count

    def _scan_groups(self, log_signal) -> int:
        """Scan groups."""
        count = self._scan_named_objects(
            log_signal, self.api.get_groups, 'group', name_field='name',
        )
        log_signal.emit(f"  Found {count} groups")
        return count

    def _scan_macros(self, log_signal) -> int:
        """Scan macros."""
        count = self._scan_named_objects(
            log_signal, self.api.get_macros, 'macro',
        )
        log_signal.emit(f"  Found {count} macros")
        return count

    def _scan_triggers(self, log_signal) -> int:
        """Scan triggers."""
        count = self._scan_named_objects(
            log_signal, self.api.get_triggers, 'trigger',
        )
        log_signal.emit(f"  Found {count} triggers")
        return count

    def _scan_automations(self, log_signal) -> int:
        """Scan automations."""
        count = self._scan_named_objects(
            log_signal, self.api.get_automations, 'automation',
        )
        log_signal.emit(f"  Found {count} automations")
        return count

    def _scan_views(self, log_signal) -> int:
        """Scan views."""
        count = self._scan_named_objects(
            log_signal, self.api.get_views, 'view',
        )
        log_signal.emit(f"  Found {count} views")
        return count

    def _scan_sla_policies(self, log_signal) -> int:
        """Scan SLA policies."""
        count = self._scan_named_objects(
            log_signal, self.api.get_sla_policies, 'sla_policy',
        )
        log_signal.emit(f"  Found {count} SLA policies")
        return count

    def _scan_hc_categories(self, log_signal) -> int:
        """Scan Help Center categories."""
        count = self._scan_named_objects(
            log_signal, self.api.get_hc_categories, 'category',
            name_field='name',
        )
        log_signal.emit(f"  Found {count} HC categories")
        return count

    def _scan_hc_sections(self, log_signal) -> int:
        """Scan Help Center sections."""
        count = self._scan_named_objects(
            log_signal, self.api.get_hc_sections, 'section',
            name_field='name',
        )
        log_signal.emit(f"  Found {count} HC sections")
        return count

    def _scan_hc_articles(self, log_signal) -> int:
        """Scan Help Center articles."""
        count = self._scan_named_objects(
            log_signal, self.api.get_hc_articles, 'article',
        )
        log_signal.emit(f"  Found {count} HC articles")
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
            # Try correct pt-BR locale (1176) first; fall back to 16 for DCs
            # created before the locale ID bug was fixed (16 = French, not PT).
            pt_variant = variants.get(self.pt_locale_id) or variants.get(16)
            if pt_variant:
                pt_text = pt_variant.get('content', current_value)
            if self.en_locale_id in variants:
                en_text = variants[self.en_locale_id].get('content', '')
            if self.es_locale_id in variants:
                es_text = variants[self.es_locale_id].get('content', '')

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
                pt_variant = variants.get(self.pt_locale_id) or variants.get(16)
                if pt_variant:
                    pt_text = pt_variant.get('content', current_value)
                if self.en_locale_id in variants:
                    en_text = variants[self.en_locale_id].get('content', '')
                if self.es_locale_id in variants:
                    es_text = variants[self.es_locale_id].get('content', '')
            else:
                # For field options, use parent-prefixed DC name so the
                # preview shows the same placeholder that will be created.
                if '_option' in obj_type:
                    parent_name = (extra or {}).get('parent_name', '')
                    parent_dc = generate_dc_name(parent_name) if parent_name else ''
                    option_dc = generate_dc_name(current_value)
                    if parent_dc and option_dc:
                        dc_placeholder = f"{{{{dc.{parent_dc}_{option_dc}}}}}"
                    else:
                        dc_placeholder = generate_dc_placeholder(current_value)
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

    # =========================================================================
    # TRANSLATION
    # =========================================================================

    def _translate_items_list(
        self,
        log_signal,
        progress_signal,
        items_to_translate: List[Tuple[int, Dict]],
        force_retranslate: bool = False,
    ) -> TranslationStats:
        """Translate a list of (index, item) pairs. Updates work_items in-place."""
        stats = TranslationStats()
        total = len(items_to_translate)
        stats.total = total

        if total == 0:
            log_signal.emit("No items need translation.")
            return stats

        log_signal.emit(f"Translating {total} items...")

        for i, (idx, item) in enumerate(items_to_translate):
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
            needs_en = (
                force_retranslate or en_source == SOURCE_NEW
                or not item.get('en')
            )
            needs_es = (
                force_retranslate or es_source == SOURCE_NEW
                or not item.get('es')
            )

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
                            SOURCE_CACHE if en_from_cache
                            else SOURCE_TRANSLATED
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
                            SOURCE_CACHE if es_from_cache
                            else SOURCE_TRANSLATED
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

        total = len(self.work_items)
        log_signal.emit(f"Starting translation of {total} items...")

        items_to_translate = [
            (i, item) for i, item in enumerate(self.work_items)
            if not item.get('is_system') and not item.get('is_reserved')
            and item.get('pt')
            and (
                force
                or item.get('en_source') == SOURCE_NEW or not item.get('en')
                or item.get('es_source') == SOURCE_NEW or not item.get('es')
            )
        ]

        stats = self._translate_items_list(
            log_signal, progress_signal, items_to_translate,
            force_retranslate=force,
        )
        stats.total = total
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

        items_to_consider = [
            (idx, self.work_items[idx])
            for idx in selected_indices
            if idx < len(self.work_items)
            and not self.work_items[idx].get('is_system', False)
            and not self.work_items[idx].get('is_reserved', False)
        ]

        system_skipped = len(selected_indices) - len(items_to_consider)
        if system_skipped > 0:
            log_signal.emit(f"Skipping {system_skipped} system/reserved items")

        if force_retranslate:
            to_translate = items_to_consider
            log_signal.emit(
                f"[RE-TRANSLATE] Processing {len(to_translate)} selected items"
            )
        else:
            to_translate = [
                (idx, item) for idx, item in items_to_consider
                if (not item.get('en') or item.get('en_source') == SOURCE_NEW)
                or (not item.get('es') or item.get('es_source') == SOURCE_NEW)
            ]
            already_done = len(items_to_consider) - len(to_translate)
            if already_done > 0:
                log_signal.emit(
                    f"Skipping {already_done} items that already have "
                    f"translations"
                )

        return self._translate_items_list(
            log_signal, progress_signal, to_translate,
            force_retranslate=force_retranslate,
        )

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

        self._deferred_option_updates = {}
        try:
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

            self._flush_deferred_option_updates(log_signal, result)
        finally:
            self._deferred_option_updates = None

        skipped_count = len(result['skipped'])
        if skipped_count > 0:
            log_signal.emit(f"Skipped {skipped_count} already-linked items")

        log_signal.emit(
            f"Apply complete: {len(result['success'])} succeeded, "
            f"{len(result['failed'])} failed, {skipped_count} skipped"
        )

        return result

    def _flush_deferred_option_updates(
        self,
        log_signal,
        result: Dict[str, Any]
    ) -> None:
        """Batch-apply all queued field option updates (one GET+PUT per field)."""
        if not self._deferred_option_updates:
            return
        for (field_type, parent_id), updates in self._deferred_option_updates.items():
            if self._should_stop():
                break
            try:
                log_signal.emit(
                    f"  Batch updating {len(updates)} options "
                    f"for {field_type} {parent_id}..."
                )
                if field_type == 'ticket_field':
                    self.api.batch_update_ticket_field_options(parent_id, updates)
                elif field_type == 'user_field':
                    self.api.batch_update_user_field_options(parent_id, updates)
                elif field_type == 'organization_field':
                    self.api.batch_update_organization_field_options(parent_id, updates)
            except Exception as e:
                logger.error(f"Batch option update failed for {field_type} {parent_id}: {e}")
                log_signal.emit(
                    f"  Error batching options for {field_type} {parent_id}: {e}"
                )

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
        """Sort items for optimal apply order.

        already_linked items are skipped first, then parent fields are created
        before their child options to avoid ordering issues.
        """
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

        return already_linked + parents + options

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
                    ok = self.api.update_dynamic_content_variants(
                        dc_id,
                        [
                            {'locale_id': self.pt_locale_id, 'content': pt_text},
                            {'locale_id': self.en_locale_id, 'content': en_text},
                            {'locale_id': self.es_locale_id, 'content': es_text},
                        ],
                        default_locale_id=self.pt_locale_id,
                    )
                    if not ok:
                        log_signal.emit(
                            f"    Warning: pt-BR variant not found on DC {dc_id}; "
                            "could not set as default"
                        )
                    log_signal.emit("    Updated DC translations")
                    return True, False, "Updated DC"
                else:
                    log_signal.emit("    Already linked to DC (skipped)")
                    return True, True, "Already linked"

            # Not linked yet - need to create or find DC
            # For field options, prefix with the parent field name so the DC
            # name is unique and self-documenting: {field}_{option}
            if '_option' in obj_type:
                parent_name = item.get('parent_name', '')
                parent_dc = generate_dc_name(parent_name) if parent_name else ''
                option_dc = generate_dc_name(current_value)
                safe_dc_name = (
                    f"{parent_dc}_{option_dc}"
                    if parent_dc and option_dc
                    else option_dc
                )
            else:
                safe_dc_name = generate_dc_name(current_value)

            if not safe_dc_name:
                log_signal.emit("    Cannot generate DC name (skipped)")
                return False, True, "Cannot generate DC name"

            # Check if DC with this name already exists
            existing_dc = self._find_dc_by_name(safe_dc_name)

            placeholder = None
            dc_id = None

            if existing_dc:
                dc_id = existing_dc.get('id')
                placeholder = existing_dc.get('placeholder')

                if force_update:
                    ok = self.api.update_dynamic_content_variants(
                        dc_id,
                        [
                            {'locale_id': self.pt_locale_id, 'content': pt_text},
                            {'locale_id': self.en_locale_id, 'content': en_text},
                            {'locale_id': self.es_locale_id, 'content': es_text},
                        ],
                        default_locale_id=self.pt_locale_id,
                    )
                    if not ok:
                        log_signal.emit(
                            f"    Warning: pt-BR variant not found on DC {dc_id}; "
                            "could not set as default"
                        )
                    log_signal.emit(f"    Updated existing DC: {placeholder}")
            else:
                # Create new DC
                try:
                    dc_item = self.api.create_dynamic_content(
                        name=safe_dc_name,
                        default_locale_id=self.pt_locale_id,
                        variants=[
                            {
                                'locale_id': self.pt_locale_id,
                                'content': pt_text,
                                'default': True
                            },
                            {'locale_id': self.en_locale_id, 'content': en_text},
                            {'locale_id': self.es_locale_id, 'content': es_text},
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
                if self._deferred_option_updates is not None:
                    key = ('ticket_field', parent_id)
                    self._deferred_option_updates.setdefault(key, {})[option_value] = placeholder
                    log_signal.emit(f"    Queued for batch update (field {parent_id})")
                else:
                    log_signal.emit(f"    Updating via field, value='{option_value}'")
                    self.api.update_ticket_field_option_via_field(
                        parent_id, option_value, placeholder
                    )

            elif obj_type == 'ticket_form':
                self.api.update_ticket_form(obj_id, {field_name: placeholder})

            elif obj_type == 'custom_status':
                self.api.update_custom_status(
                    obj_id, {field_name: placeholder}
                )

            elif obj_type == 'user_field':
                self.api.update_user_field(obj_id, {field_name: placeholder})

            elif obj_type == 'user_field_option':
                if not parent_id:
                    raise Exception("No parent_id for user_field_option")
                if not option_value:
                    raise Exception("No option_value for user_field_option")
                if self._deferred_option_updates is not None:
                    key = ('user_field', parent_id)
                    self._deferred_option_updates.setdefault(key, {})[option_value] = placeholder
                    log_signal.emit(f"    Queued for batch update (field {parent_id})")
                else:
                    self.api.update_user_field_option_via_field(
                        parent_id, option_value, placeholder
                    )

            elif obj_type == 'organization_field':
                self.api.update_organization_field(
                    obj_id, {field_name: placeholder}
                )

            elif obj_type == 'organization_field_option':
                if not parent_id:
                    raise Exception("No parent_id for organization_field_option")
                if not option_value:
                    raise Exception("No option_value for organization_field_option")
                if self._deferred_option_updates is not None:
                    key = ('organization_field', parent_id)
                    self._deferred_option_updates.setdefault(key, {})[option_value] = placeholder
                    log_signal.emit(f"    Queued for batch update (field {parent_id})")
                else:
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
