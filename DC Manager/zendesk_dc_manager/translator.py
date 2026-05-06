"""
Translation service module for Zendesk DC Manager.
Handles translation via DeepL, Google Cloud, or Google Web (free, unofficial).

Provider priority: DeepL (if deepl_api_key) > Google Cloud (if google_api_key) > Google Web.

This module uses:
- PersistentCache from cache.py for SQLite-based caching
- AcronymProtector from utils.py for acronym protection
"""

import time
import random
import threading
from pathlib import Path
from typing import Optional, Dict, Tuple

import requests

from zendesk_dc_manager.config import (
    logger,
    TRANSLATION_CONFIG,
)
from zendesk_dc_manager.cache import PersistentCache
from zendesk_dc_manager.utils import AcronymProtector
from zendesk_dc_manager.types import TranslationStats


class TranslationService:
    """Translation service. Provider priority: DeepL > Google Cloud > Google Web."""

    GOOGLE_WEB_URL = "https://translate.googleapis.com/translate_a/single"
    GOOGLE_CLOUD_URL = "https://translation.googleapis.com/language/translate/v2"
    DEEPL_FREE_URL = "https://api-free.deepl.com/v2/translate"
    DEEPL_PRO_URL = "https://api.deepl.com/v2/translate"

    # DeepL language codes differ from Google's for some languages
    _DEEPL_LANG_MAP = {
        'pt': 'PT',
        'en': 'EN-US',
        'es': 'ES',
    }

    def __init__(
        self,
        use_google_cloud: bool = False,
        google_api_key: Optional[str] = None,
        deepl_api_key: Optional[str] = None,
        protect_acronyms: bool = True,
        cache_expiry_days: int = 30,
        cache_file: Optional[str] = None
    ):
        self.deepl_api_key = deepl_api_key or None
        # DeepL free keys end with ':fx'
        self._deepl_url = (
            self.DEEPL_FREE_URL
            if (self.deepl_api_key and self.deepl_api_key.endswith(':fx'))
            else self.DEEPL_PRO_URL
        )
        self.use_google_cloud = (not self.deepl_api_key) and use_google_cloud and google_api_key
        self.google_api_key = google_api_key
        self.protect_acronyms = protect_acronyms
        self.cache_expiry_days = cache_expiry_days

        # Setup cache using SQLite-based PersistentCache
        if cache_file is None:
            cache_dir = Path.home() / ".zendesk_dc_manager"
            cache_dir.mkdir(exist_ok=True)
            cache_file = str(cache_dir / "translation_cache.db")

        self.cache = PersistentCache(db_path=cache_file)

        # Rate limiting
        self._last_request_time = 0.0
        self._request_lock = threading.Lock()

        # Session for requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36'
            )
        })

    def _rate_limit(self):
        """Enforce rate limiting between translation requests."""
        with self._request_lock:
            delay = random.uniform(
                TRANSLATION_CONFIG.DELAY_MIN,
                TRANSLATION_CONFIG.DELAY_MAX
            )
            elapsed = time.time() - self._last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
            self._last_request_time = time.time()

    def translate(
        self,
        text: str,
        source_lang: str,
        target_lang: str
    ) -> Tuple[Optional[str], bool, bool]:
        """
        Translate text from source language to target language.

        Returns:
            Tuple of (translated_text, from_cache, needs_attention)
            - translated_text: The translated text or None if failed
            - from_cache: True if result came from cache
            - needs_attention: True if acronym(s) were lost — human review needed
        """
        if not text or not text.strip():
            return text, False, False

        text = text.strip()

        # Check cache first
        cache_result = self.cache.get_with_age(text, target_lang)
        if cache_result:
            cached_text, age_days = cache_result
            if age_days <= self.cache_expiry_days:
                return cached_text, True, False

        # Check if text needs translation
        if not self._needs_translation(text):
            return text, False, False

        # Protect acronyms if enabled
        protected_text = text
        acronym_map: Dict[str, str] = {}
        skip_translation = False

        if self.protect_acronyms:
            protected_text, acronym_map, skip_translation = (
                AcronymProtector.protect(text)
            )

        # If text should be skipped (e.g., pure acronym)
        if skip_translation and "__SKIP__" in acronym_map:
            return acronym_map["__SKIP__"], False, False

        # Perform translation
        try:
            if self.deepl_api_key:
                translated = self._translate_deepl(
                    protected_text, source_lang, target_lang
                )
            elif self.use_google_cloud:
                translated = self._translate_google_cloud(
                    protected_text, source_lang, target_lang
                )
            else:
                translated = self._translate_google_web(
                    protected_text, source_lang, target_lang
                )

            if translated:
                needs_attention = False
                if acronym_map:
                    translated, placeholder_lost = AcronymProtector.restore(
                        translated, acronym_map
                    )
                    translated, acronym_missing = AcronymProtector.verify_and_fix(
                        text, translated, acronym_map
                    )
                    needs_attention = placeholder_lost or acronym_missing

                self.cache.set(text, target_lang, translated)
                return translated, False, needs_attention

            return None, False, False

        except Exception as e:
            logger.error(f"Translation error: {e}")
            return None, False, False

    def _needs_translation(self, text: str) -> bool:
        """Check if text actually needs translation."""
        # Empty or whitespace only
        if not text or not text.strip():
            return False

        # Single character
        if len(text.strip()) <= 1:
            return False

        # Numbers only
        clean_text = text.strip().replace(',', '').replace('.', '').replace('-', '')
        if clean_text.isdigit():
            return False

        # URL or email
        url_patterns = ['http://', 'https://', 'www.', '@', '://']
        if any(p in text.lower() for p in url_patterns):
            return False

        # Placeholder patterns
        placeholder_patterns = [
            '{{', '}}', '${', '%s', '%d', '{0}', '{1}',
            '<%', '%>', '[[', ']]'
        ]
        if any(p in text for p in placeholder_patterns):
            # Only skip if entire text is a placeholder
            if text.startswith('{{') and text.endswith('}}'):
                return False

        return True

    def _translate_deepl(
        self,
        text: str,
        source_lang: str,
        target_lang: str
    ) -> Optional[str]:
        """Translate using DeepL API (free or pro)."""
        self._rate_limit()

        src = self._DEEPL_LANG_MAP.get(source_lang, source_lang.upper())
        tgt = self._DEEPL_LANG_MAP.get(target_lang, target_lang.upper())

        headers = {
            'Authorization': f'DeepL-Auth-Key {self.deepl_api_key}',
            'Content-Type': 'application/json',
        }
        data = {
            'text': [text],
            'source_lang': src,
            'target_lang': tgt,
            'tag_handling': 'text',
        }

        try:
            response = self.session.post(
                self._deepl_url,
                headers=headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            translations = result.get('translations', [])
            if translations:
                return translations[0].get('text')
            return None
        except Exception as e:
            logger.error(f"DeepL translation failed: {e}")
            return None

    def _translate_google_web(
        self,
        text: str,
        source_lang: str,
        target_lang: str
    ) -> Optional[str]:
        """Translate using Google's free web API."""
        self._rate_limit()

        params = {
            'client': 'gtx',
            'sl': source_lang,
            'tl': target_lang,
            'dt': 't',
            'q': text
        }

        try:
            response = self.session.get(
                self.GOOGLE_WEB_URL,
                params=params,
                timeout=30
            )
            response.raise_for_status()

            # Parse the response
            result = response.json()

            if result and isinstance(result, list) and len(result) > 0:
                if isinstance(result[0], list):
                    # Combine all translated segments
                    translated_parts = []
                    for segment in result[0]:
                        if isinstance(segment, list) and len(segment) > 0:
                            translated_parts.append(str(segment[0]))
                    return ''.join(translated_parts)

            return None

        except Exception as e:
            logger.error(f"Google Web translation failed: {e}")
            return None

    def _translate_google_cloud(
        self,
        text: str,
        source_lang: str,
        target_lang: str
    ) -> Optional[str]:
        """Translate using Google Cloud Translation API."""
        self._rate_limit()

        params = {
            'key': self.google_api_key
        }

        data = {
            'q': text,
            'source': source_lang,
            'target': target_lang,
            'format': 'text'
        }

        try:
            response = self.session.post(
                self.GOOGLE_CLOUD_URL,
                params=params,
                json=data,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()

            if 'data' in result and 'translations' in result['data']:
                translations = result['data']['translations']
                if translations and len(translations) > 0:
                    return translations[0].get('translatedText')

            return None

        except Exception as e:
            logger.error(f"Google Cloud translation failed: {e}")
            return None

    def clear_cache(self) -> bool:
        """Clear the translation cache."""
        return self.cache.clear()

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        stats = self.cache.get_stats()
        return {
            'total_entries': stats.get('entries', 0),
            'valid_entries': stats.get('entries', 0),
            'expired_entries': 0,
            'db_path': stats.get('db_path', ''),
        }