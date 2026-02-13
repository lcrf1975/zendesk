"""
Persistent cache implementation for Zendesk DC Manager.

This module provides a thread-safe SQLite-based cache with connection pooling
for storing translation results.
"""

import os
import sqlite3
import hashlib
import threading
import weakref
import atexit
from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Queue, Empty, Full
from typing import Optional, Tuple, Set, Generator

from zendesk_dc_manager.config import logger


# ==============================================================================
# GLOBAL CLEANUP REGISTRY
# ==============================================================================


_atexit_registered_caches: Set[int] = set()
_atexit_lock = threading.Lock()


def _register_cache_cleanup(cache_instance: 'PersistentCache'):
    """Register cache cleanup with deduplication."""
    instance_id = id(cache_instance)

    with _atexit_lock:
        if instance_id in _atexit_registered_caches:
            return
        _atexit_registered_caches.add(instance_id)

    weak_cache = weakref.ref(cache_instance)

    def cleanup_handler():
        cache = weak_cache()
        if cache is not None:
            try:
                cache.cleanup()
            except Exception:
                pass
        with _atexit_lock:
            _atexit_registered_caches.discard(instance_id)

    atexit.register(cleanup_handler)


# ==============================================================================
# PERSISTENT CACHE
# ==============================================================================


class PersistentCache:
    """Thread-safe SQLite cache for translations with connection pooling."""

    def __init__(self, db_path: str = "translation_cache.db", pool_size: int = 5):
        self.db_path = db_path
        self._pool: Queue = Queue(maxsize=pool_size)
        self._lock = threading.RLock()
        self._pool_size = pool_size
        self._initialized = False
        self._closed = False

        self._init_pool()
        self._init_db()
        _register_cache_cleanup(self)

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection with optimized settings."""
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        return conn

    def _init_pool(self):
        """Initialize the connection pool."""
        for _ in range(self._pool_size):
            try:
                conn = self._create_connection()
                self._pool.put(conn)
            except Exception as e:
                logger.error(f"Error creating database connection: {e}")

    @contextmanager
    def _get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager to get a connection from the pool."""
        if self._closed:
            raise RuntimeError("Cache has been closed")

        conn: Optional[sqlite3.Connection] = None
        from_pool = False

        try:
            try:
                conn = self._pool.get(timeout=30.0)
                from_pool = True
            except Empty:
                conn = self._create_connection()
                from_pool = False

            yield conn

        except Exception:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            if from_pool:
                try:
                    new_conn = self._create_connection()
                    self._pool.put_nowait(new_conn)
                except Exception:
                    pass
            raise
        else:
            if conn is not None:
                if from_pool:
                    try:
                        self._pool.put_nowait(conn)
                    except Full:
                        try:
                            conn.close()
                        except Exception:
                            pass
                else:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _init_db(self):
        """Initialize the database schema."""
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS translations (
                            id TEXT PRIMARY KEY,
                            original TEXT,
                            target_lang TEXT,
                            translated_text TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_translations_created
                        ON translations(created_at)
                    """)
                    conn.commit()
                    self._initialized = True
            except Exception as e:
                logger.error(f"Cache initialization error: {e}")

    def _generate_id(self, text: str, lang: str) -> str:
        """Generate a unique ID for a text/language pair."""
        content = f"{text}\x00{lang}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()

    def get_with_age(self, text: str, lang: str) -> Optional[Tuple[str, int]]:
        """Get cached translation with age information."""
        if not text or not lang:
            return None

        key = self._generate_id(text, lang)

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT translated_text, created_at FROM translations "
                    "WHERE id = ?",
                    (key,)
                )
                result = cursor.fetchone()

                if result:
                    trans_text, created_at_str = result
                    try:
                        created_dt = datetime.strptime(
                            created_at_str, "%Y-%m-%d %H:%M:%S"
                        )
                        created_dt = created_dt.replace(tzinfo=timezone.utc)
                        now_utc = datetime.now(timezone.utc)
                        delta = now_utc - created_dt
                        age = max(0, delta.days)
                    except Exception:
                        age = 0
                    return trans_text, age
                return None
        except Exception as e:
            logger.error(f"Cache read error: {e}")
            return None

    def get(self, text: str, lang: str) -> Optional[str]:
        """Get cached translation."""
        result = self.get_with_age(text, lang)
        return result[0] if result else None

    def set(self, text: str, lang: str, translation: str) -> bool:
        """Store a translation in the cache."""
        if not text or not translation or not lang:
            return False

        key = self._generate_id(text, lang)

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO translations
                    (id, original, target_lang, translated_text, created_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (key, text, lang, translation))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Cache write error: {e}")
            return False

    def delete(self, text: str, lang: str) -> bool:
        """Delete a specific entry from the cache."""
        if not text or not lang:
            return False

        key = self._generate_id(text, lang)

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM translations WHERE id = ?", (key,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False

    def clear(self) -> bool:
        """Clear all entries from the cache."""
        with self._lock:
            try:
                self.cleanup()

                if os.path.exists(self.db_path):
                    os.remove(self.db_path)

                wal_path = f"{self.db_path}-wal"
                shm_path = f"{self.db_path}-shm"

                if os.path.exists(wal_path):
                    os.remove(wal_path)
                if os.path.exists(shm_path):
                    os.remove(shm_path)

                self._closed = False
                self._init_pool()
                self._init_db()

                return True
            except Exception as e:
                logger.error(f"Cache clear error: {e}")
                return False

    def get_stats(self) -> dict:
        """Get cache statistics."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM translations")
                count = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT MIN(created_at), MAX(created_at) FROM translations"
                )
                oldest, newest = cursor.fetchone()

                return {
                    'entries': count,
                    'oldest': oldest,
                    'newest': newest,
                    'db_path': self.db_path,
                }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'entries': 0, 'error': str(e)}

    def cleanup(self):
        """Close all connections and clean up resources."""
        with self._lock:
            self._closed = True
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except Exception:
                    pass

    def __del__(self):
        try:
            self.cleanup()
        except Exception:
            pass