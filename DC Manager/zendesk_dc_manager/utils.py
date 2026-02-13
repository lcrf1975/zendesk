"""
Utility functions and classes for Zendesk DC Manager.

This module provides:
- Input validation functions
- Text processing utilities
- Acronym protection for translations
- Thread-safe atomic counter
"""

import re
import html
import hashlib
import unicodedata
import threading
from typing import Dict, List, Tuple, Set, Optional

from zendesk_dc_manager.config import (
    COMMON_SHORT_WORDS,
    TRANSLATABLE_SHORT_WORDS,
    logger,
)


# ==============================================================================
# VALIDATION PATTERNS
# ==============================================================================


SUBDOMAIN_PATTERN = re.compile(r'^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]?$')
EMAIL_PATTERN = re.compile(r'^[^@]+@[^@]+\.[^@]+$')


# ==============================================================================
# INPUT VALIDATION
# ==============================================================================


def validate_subdomain(subdomain: str) -> str:
    """
    Validate and clean Zendesk subdomain.

    Args:
        subdomain: The subdomain to validate (may include URL parts)

    Returns:
        Cleaned subdomain string

    Raises:
        ValueError: If subdomain is invalid
    """
    if not subdomain:
        raise ValueError("Subdomain is required")

    cleaned = subdomain.lower().strip()

    for prefix in ("https://", "http://"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]

    for suffix in (".zendesk.com", "/"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)]

    cleaned = cleaned.strip("/")

    if not cleaned:
        raise ValueError("Subdomain is required")

    if len(cleaned) > 63:
        raise ValueError("Subdomain too long (max 63 characters)")

    if not SUBDOMAIN_PATTERN.match(cleaned):
        raise ValueError(f"Invalid subdomain format: '{cleaned}'")

    return cleaned


def validate_email(email: str) -> str:
    """Validate email format."""
    if not email:
        raise ValueError("Email is required")

    email = email.strip()

    if not email:
        raise ValueError("Email is required")

    if not EMAIL_PATTERN.match(email):
        raise ValueError("Invalid email format")

    return email


def validate_token(token: str) -> str:
    """Validate API token."""
    if not token:
        raise ValueError("API Token is required")

    token = token.strip()

    if not token:
        raise ValueError("API Token is required")

    return token


# ==============================================================================
# TEXT PROCESSING
# ==============================================================================


def escape_html(text: str) -> str:
    """Safely escape HTML special characters."""
    if not text:
        return ""
    return html.escape(str(text))


def sanitize_for_dc_name(text: str) -> str:
    """Sanitize text for use as a Dynamic Content name."""
    if not text:
        return "unknown"

    normalized = unicodedata.normalize('NFKD', str(text))
    ascii_text = normalized.encode('ASCII', 'ignore').decode('utf-8')

    sanitized = re.sub(r'[^a-zA-Z0-9_]+', '_', ascii_text)
    sanitized = sanitized.strip('_').lower()

    if not sanitized:
        return f"item_{hashlib.md5(text.encode()).hexdigest()[:8]}"

    return sanitized


def is_dc_string(text: str) -> bool:
    """Check if text is already a Dynamic Content placeholder."""
    if not text:
        return False
    text = text.strip()
    return text.startswith("{{") and text.endswith("}}") and "dc." in text


def normalize_locale(locale_str: str) -> Optional[str]:
    """Normalize a locale string to a standard format."""
    if not locale_str:
        return None

    locale_lower = locale_str.lower()

    if locale_lower.startswith('en'):
        return 'en'
    elif locale_lower.startswith('es'):
        return 'es'
    elif locale_lower.startswith('pt'):
        return 'pt'

    return None


def format_elapsed_time(seconds: float) -> str:
    """Format elapsed time as a human-readable string."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def calculate_eta(start_time: float, processed: int, total: int) -> str:
    """Calculate estimated time of arrival."""
    import time

    if processed <= 0 or total <= 0:
        return "Calculating..."

    elapsed = time.time() - start_time

    if elapsed <= 0:
        return "..."

    remaining = total - processed

    if remaining <= 0:
        return "Complete"

    rate = processed / elapsed

    if rate <= 0:
        return "..."

    return format_elapsed_time(remaining / rate)


# ==============================================================================
# THREAD-SAFE ATOMIC COUNTER
# ==============================================================================


class AtomicCounter:
    """Thread-safe counter for tracking operations."""

    def __init__(self, initial: int = 0):
        self._value = initial
        self._lock = threading.Lock()

    def increment(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def decrement(self) -> int:
        with self._lock:
            self._value -= 1
            return self._value

    def add(self, amount: int) -> int:
        with self._lock:
            self._value += amount
            return self._value

    def reset(self, value: int = 0) -> None:
        with self._lock:
            self._value = value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def __repr__(self) -> str:
        return f"AtomicCounter({self.value})"


# ==============================================================================
# ACRONYM PROTECTOR
# ==============================================================================


class AcronymProtector:
    """Thread-safe acronym protection for translation services."""

    CONTEXT_PREFIX = "The term is: "
    CONTEXT_SUFFIX = "."
    PLACEHOLDER_PREFIX = "ZZPHOLD"

    PLACEHOLDER_PATTERN = re.compile(
        r'\b[ZS]Z?P?H?O?L?D?\d+[a-f0-9]{4}\b', re.IGNORECASE
    )
    PLACEHOLDER_PATTERN_LOOSE = re.compile(
        r'\b[ZS][xzXZ]?[pqPQ]?[vwVW]?\d+[a-f0-9]{2,6}\b', re.IGNORECASE
    )
    LEGACY_PLACEHOLDER_PATTERN = re.compile(
        r'\b[ZS][xX][qQ][vVwWbB]\d*[a-f0-9]*\b', re.IGNORECASE
    )
    MULTI_SPACE_PATTERN = re.compile(r'\s{2,}')

    CONTEXT_PATTERNS = [
        re.compile(r'^The term is\s*:\s*', re.IGNORECASE),
        re.compile(r'^The term is\s+', re.IGNORECASE),
        re.compile(r'^Term is\s*:\s*', re.IGNORECASE),
        re.compile(r'^Term\s*:\s*', re.IGNORECASE),
        re.compile(r'^O termo [eé]\s*:\s*', re.IGNORECASE),
        re.compile(r'^O termo [eé]\s+', re.IGNORECASE),
        re.compile(r'^Termo [eé]\s*:\s*', re.IGNORECASE),
        re.compile(r'^Termo\s*:\s*', re.IGNORECASE),
        re.compile(r'^El t[eé]rmino es\s*:\s*', re.IGNORECASE),
        re.compile(r'^El t[eé]rmino es\s+', re.IGNORECASE),
        re.compile(r'^T[eé]rmino es\s*:\s*', re.IGNORECASE),
        re.compile(r'^T[eé]rmino\s*:\s*', re.IGNORECASE),
        re.compile(r'^Le terme est\s*:\s*', re.IGNORECASE),
        re.compile(r'^Terme est\s*:\s*', re.IGNORECASE),
        re.compile(r'^Der Begriff ist\s*:\s*', re.IGNORECASE),
        re.compile(r'^Begriff ist\s*:\s*', re.IGNORECASE),
        re.compile(r'^Il termine [eè]\s*:\s*', re.IGNORECASE),
        re.compile(r'^Termine [eè]\s*:\s*', re.IGNORECASE),
        re.compile(r'^[A-Za-zÀ-ÿ]+\s+[A-Za-zÀ-ÿ]+\s*:\s*', re.IGNORECASE),
    ]

    ACRONYM_PATTERNS = [
        re.compile(r'\b[A-Z]{2,}[A-Z0-9]*\b'),
        re.compile(r'\b[A-Z0-9]*[A-Z]{2,}[A-Z0-9]*\b'),
        re.compile(r'\b(?:[A-Z]\.){2,}[A-Z]?\.?\b'),
    ]

    @staticmethod
    def should_skip_translation(text: str) -> bool:
        """Determine if text should be skipped entirely."""
        if not text:
            return True

        text = text.strip()
        if not text:
            return True

        if text.isdigit():
            return True

        text_upper = text.upper()

        if text_upper in TRANSLATABLE_SHORT_WORDS:
            return False

        if text_upper in COMMON_SHORT_WORDS:
            return False

        if len(text) <= 1:
            return True

        if len(text) <= 3:
            vowel_pattern = (
                r'[aeiouáéíóúàèìòùâêîôûãõäëïöü'
                r'AEIOUÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÄËÏÖÜ]'
            )
            if re.search(vowel_pattern, text):
                return False
            return True

        if AcronymProtector.is_likely_acronym(text):
            return True

        return False

    @staticmethod
    def is_likely_acronym(text: str) -> bool:
        """Check if text is likely an acronym."""
        if not text:
            return False

        text = text.strip()

        if len(text) < 2 or len(text) > 6:
            return False

        if not re.match(r'^[A-Z][A-Z0-9]+$', text):
            return False

        if text in COMMON_SHORT_WORDS:
            return False

        if text in TRANSLATABLE_SHORT_WORDS:
            return False

        return True

    @staticmethod
    def _make_safe_placeholder(index: int, acronym: str) -> str:
        hash_val = hashlib.md5(acronym.encode()).hexdigest()[:4]
        return f"{AcronymProtector.PLACEHOLDER_PREFIX}{index}{hash_val}"

    @staticmethod
    def cleanup_placeholders(text: str) -> str:
        """Remove any remaining placeholder artifacts from text."""
        if not text:
            return ""

        result = text
        result = AcronymProtector.PLACEHOLDER_PATTERN.sub('', result)
        result = AcronymProtector.PLACEHOLDER_PATTERN_LOOSE.sub('', result)
        result = AcronymProtector.LEGACY_PLACEHOLDER_PATTERN.sub('', result)
        result = AcronymProtector.MULTI_SPACE_PATTERN.sub(' ', result)

        return result.strip()

    @staticmethod
    def has_placeholders(text: str) -> bool:
        """Check if text contains any placeholder patterns."""
        if not text:
            return False

        if AcronymProtector.PLACEHOLDER_PATTERN.search(text):
            return True
        if AcronymProtector.PLACEHOLDER_PATTERN_LOOSE.search(text):
            return True
        if AcronymProtector.LEGACY_PLACEHOLDER_PATTERN.search(text):
            return True

        return False

    @staticmethod
    def protect(text: str) -> Tuple[str, Dict[str, str], bool]:
        """Protect acronyms in text by replacing with placeholders."""
        if not text:
            return "", {}, False

        original_text = text.strip()

        if AcronymProtector.should_skip_translation(original_text):
            return original_text, {"__SKIP__": original_text}, True

        if text.isupper() and len(text.strip()) > 6:
            return text, {}, False

        all_matches: Set[str] = set()
        for pattern in AcronymProtector.ACRONYM_PATTERNS:
            matches = pattern.findall(text)
            all_matches.update(matches)

        all_matches = {
            m for m in all_matches
            if m not in COMMON_SHORT_WORDS and m not in TRANSLATABLE_SHORT_WORDS
        }

        if not all_matches:
            return text, {}, False

        protected_text = text
        acronym_map: Dict[str, str] = {}

        sorted_matches = sorted(all_matches, key=len, reverse=True)

        for i, acronym in enumerate(sorted_matches):
            placeholder = AcronymProtector._make_safe_placeholder(i, acronym)
            acronym_map[placeholder] = acronym
            protected_text = re.sub(
                r'\b' + re.escape(acronym) + r'\b',
                placeholder,
                protected_text
            )

        return protected_text, acronym_map, False

    @staticmethod
    def add_context_padding(text: str) -> str:
        """Add context padding to help translation services."""
        return (
            f"{AcronymProtector.CONTEXT_PREFIX}{text}"
            f"{AcronymProtector.CONTEXT_SUFFIX}"
        )

    @staticmethod
    def remove_context_padding(text: str, original_text: str = "") -> str:
        """Remove context padding from translated text."""
        if not text:
            return ""

        result = text.strip()
        original_len = len(original_text) if original_text else 0

        for pattern in AcronymProtector.CONTEXT_PATTERNS:
            new_result = pattern.sub('', result)
            if new_result != result:
                result = new_result
                break

        result = result.rstrip('.').strip()

        if original_len > 0 and len(result) > original_len * 3:
            quoted_match = re.search(r'"([^"]+)"', result)
            if quoted_match:
                result = quoted_match.group(1)
            else:
                colon_match = re.search(r':\s*(.+?)\.?$', result)
                if colon_match:
                    result = colon_match.group(1).strip()

        return result

    @staticmethod
    def restore(translated_text: str, acronym_map: Dict[str, str]) -> str:
        """Restore acronyms in translated text."""
        if not translated_text:
            return ""

        if not acronym_map:
            return translated_text

        if "__SKIP__" in acronym_map:
            return acronym_map["__SKIP__"]

        result = translated_text

        sorted_items = sorted(
            acronym_map.items(),
            key=lambda x: len(x[0]),
            reverse=True
        )

        for placeholder, acronym in sorted_items:
            if placeholder in result:
                result = result.replace(placeholder, acronym)
                continue

            pattern = re.compile(re.escape(placeholder), re.IGNORECASE)
            if pattern.search(result):
                result = pattern.sub(acronym, result)
                continue

            result = AcronymProtector._restore_corrupted(
                result, placeholder, acronym
            )

        return result

    @staticmethod
    def _restore_corrupted(text: str, placeholder: str, acronym: str) -> str:
        """Attempt to restore corrupted placeholder."""
        substitutions = [
            ('Z', 'S'), ('z', 's'), ('Z', 'C'), ('z', 'c'),
            ('x', 's'), ('x', 'j'), ('q', 'g'), ('q', 'c'),
            ('v', 'b'), ('v', 'w'),
        ]

        for old_char, new_char in substitutions:
            modified = placeholder.replace(old_char, new_char)
            if modified.lower() in text.lower():
                pattern = re.compile(re.escape(modified), re.IGNORECASE)
                return pattern.sub(acronym, text)

        if len(placeholder) >= 6:
            for length in range(len(placeholder), 4, -1):
                partial = placeholder[:length]
                if partial.lower() in text.lower():
                    idx = text.lower().find(partial.lower())
                    if idx != -1:
                        end_idx = idx + len(partial)
                        while end_idx < len(text) and (
                            text[end_idx].isalnum() or text[end_idx] in '._'
                        ):
                            end_idx += 1
                        return text[:idx] + acronym + text[end_idx:]

        return text

    @staticmethod
    def verify_and_fix(
        original: str,
        translated: str,
        acronym_map: Dict[str, str]
    ) -> Tuple[str, List[str]]:
        """Verify translation and fix any issues with acronyms."""
        issues: List[str] = []

        if not original or not translated:
            return translated or "", issues

        if acronym_map and "__SKIP__" in acronym_map:
            original_value = acronym_map["__SKIP__"]
            if translated.strip() != original_value:
                issues.append("Short text modified - restoring original")
                return original_value, issues
            return translated, issues

        if not acronym_map:
            return translated, issues

        fixed = translated

        expected_acronyms = {
            v for k, v in acronym_map.items() if k != "__SKIP__"
        }

        for acronym in expected_acronyms:
            try:
                if re.search(r'\b' + re.escape(acronym) + r'\b', fixed):
                    continue
            except re.error:
                if acronym in fixed:
                    continue

            issues.append(f"Acronym '{acronym}' was restored")

            placeholder = None
            for ph, acr in acronym_map.items():
                if acr == acronym and ph != "__SKIP__":
                    placeholder = ph
                    break

            if placeholder:
                fixed = AcronymProtector._restore_corrupted(
                    fixed, placeholder, acronym
                )

        if AcronymProtector.has_placeholders(fixed):
            issues.append("Cleaning up remaining placeholder artifacts")
            fixed = AcronymProtector.cleanup_placeholders(fixed)

        return fixed, issues