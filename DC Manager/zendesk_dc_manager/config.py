"""
Configuration management for Zendesk DC Manager.

This module provides:
- Frozen dataclass configurations for API, Translation, and UI settings
- Consolidated color schemes for the application
- Word lists and patterns for filtering system content
- Locale mappings for Zendesk API
- YAML configuration file support
- Logging configuration
"""

import os
import sys
import logging
from dataclasses import dataclass, asdict
from typing import Dict, FrozenSet, Tuple, Optional, Any
from pathlib import Path

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    json_format: bool = False
) -> logging.Logger:
    """Configure application logging."""
    logger = logging.getLogger("zendesk_dc_manager")
    logger.setLevel(level)
    logger.handlers.clear()

    if json_format:
        try:
            import json

            class JsonFormatter(logging.Formatter):
                def format(self, record):
                    log_data = {
                        "timestamp": self.formatTime(record),
                        "level": record.levelname,
                        "logger": record.name,
                        "message": record.getMessage(),
                        "module": record.module,
                        "function": record.funcName,
                        "line": record.lineno,
                    }
                    if record.exc_info:
                        log_data["exception"] = self.formatException(
                            record.exc_info
                        )
                    return json.dumps(log_data)

            formatter = JsonFormatter()
        except Exception:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create log file: {e}")

    return logger


logger = setup_logging()


# ==============================================================================
# MACOS + PYENV + QT6 COMPATIBILITY
# ==============================================================================


def configure_qt_environment():
    """Configure Qt environment variables for cross-platform compatibility."""
    if sys.platform == 'darwin':
        os.environ['QT_MAC_WANTS_LAYER'] = '1'
        os.environ['QT_DEBUG_PLUGINS'] = '0'
        os.environ['QT_FILESYSTEMMODEL_WATCH_FILES'] = '0'
        os.environ['QT_ENABLE_HIGHDPI_SCALING'] = '1'

    os.environ['QT_QUICK_BACKEND'] = 'software'


configure_qt_environment()


# ==============================================================================
# CONFIGURATION DATACLASSES
# ==============================================================================


@dataclass(frozen=True)
class APIConfig:
    """API-related configuration constants."""

    TIMEOUT_SHORT: int = 15
    TIMEOUT_DEFAULT: int = 30
    TIMEOUT_LONG: int = 45
    RETRY_COUNT: int = 3
    RETRY_BASE_DELAY: float = 1.0
    RETRY_MAX_DELAY: float = 30.0
    RETRY_BACKOFF_FACTOR: float = 2.0
    RATE_LIMIT_INITIAL_WAIT: int = 2
    RATE_LIMIT_MAX_WAIT: int = 60
    RATE_LIMIT_BACKOFF_FACTOR: float = 2.0
    THREAD_POOL_SIZE: int = 5
    THREAD_POOL_SIZE_VARIANTS: int = 8
    MAX_PAGINATION_PAGES: int = 1000


@dataclass(frozen=True)
class TranslationConfig:
    """Translation-related configuration constants."""

    DELAY_MIN: float = 0.3
    DELAY_MAX: float = 0.8
    MIN_TEXT_FOR_PADDING: int = 15
    MIN_TEXT_FOR_PADDING_LOWER: int = 3
    DEFAULT_CACHE_EXPIRY_DAYS: int = 30


@dataclass(frozen=True)
class UIConfig:
    """UI-related configuration constants."""

    WORKER_STOP_TIMEOUT_MS: int = 3000
    WORKER_STOP_INTERVALS: Tuple[int, ...] = (500, 1000, 2000)
    LOG_INTERVAL: int = 100
    STATUS_UPDATE_INTERVAL_SEC: float = 10.0
    SIDEBAR_WIDTH: int = 200
    STATUS_BAR_HEIGHT: int = 55
    MIN_WINDOW_WIDTH: int = 1100
    MIN_WINDOW_HEIGHT: int = 700
    CARD_MARGIN: int = 20
    CARD_SPACING: int = 12
    SPLITTER_TOP_SIZE: int = 450
    SPLITTER_LOG_SIZE: int = 350
    INPUT_MIN_HEIGHT: int = 36
    COMBO_MIN_HEIGHT: int = 38
    SECTION_SPACING: int = 20
    FORM_ROW_SPACING: int = 12
    LABEL_WIDTH: int = 100
    TABLE_BATCH_SIZE: int = 100
    TABLE_INSERT_BATCH: int = 20
    TABLE_INSERT_INTERVAL_MS: int = 10
    SCREEN_RATIO: float = 1.0
    TABLE_ROW_HEIGHT: int = 32


@dataclass
class AppConfig:
    """Main application configuration."""

    subdomain: str = ""
    email: str = ""
    backup_folder: str = ""
    translation_provider: str = "Google Web (Free)"
    protect_acronyms: bool = True
    cache_expiry_days: int = 30
    window_width: int = 1100
    window_height: int = 700
    splitter_sizes: Tuple[int, int] = (450, 350)
    scan_fields: bool = True
    scan_forms: bool = True
    scan_categories: bool = False
    scan_sections: bool = False
    scan_articles: bool = False
    log_level: str = "INFO"
    json_logging: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered_data)

    def save_to_yaml(self, filepath: str) -> bool:
        if not YAML_AVAILABLE:
            logger.warning("PyYAML not installed, cannot save YAML config")
            return False
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                yaml.dump(self.to_dict(), f, default_flow_style=False)
            return True
        except Exception as e:
            logger.error(f"Failed to save config to {filepath}: {e}")
            return False

    @classmethod
    def load_from_yaml(cls, filepath: str) -> Optional['AppConfig']:
        if not YAML_AVAILABLE:
            logger.warning("PyYAML not installed, cannot load YAML config")
            return None
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            return cls.from_dict(data) if data else None
        except Exception as e:
            logger.error(f"Failed to load config from {filepath}: {e}")
            return None


# ==============================================================================
# SINGLETON CONFIG INSTANCES
# ==============================================================================


API_CONFIG = APIConfig()
TRANSLATION_CONFIG = TranslationConfig()
UI_CONFIG = UIConfig()


def get_default_config_path() -> str:
    if sys.platform == 'darwin':
        config_dir = (
            Path.home() / "Library" / "Application Support" / "ZendeskDCManager"
        )
    elif sys.platform == 'win32':
        config_dir = Path(os.environ.get('APPDATA', '')) / "ZendeskDCManager"
    else:
        config_dir = Path.home() / ".config" / "zendesk_dc_manager"
    config_dir.mkdir(parents=True, exist_ok=True)
    return str(config_dir / "config.yaml")


# ==============================================================================
# CONSTANTS
# ==============================================================================


VERSION = "49.0"

CREDENTIALS_FILE = "credentials.json"

# Translation source constants
SOURCE_NEW = "New"
SOURCE_ZENDESK_DC = "Zendesk DC"
SOURCE_TRANSLATED = "Translated"
SOURCE_CACHE = "Cache"
SOURCE_FAILED = "Failed"
SOURCE_MANUAL = "Manual"
SOURCE_ATTENTION = "Attention"
SOURCE_RESERVED = "Reserved"


# ==============================================================================
# CONSOLIDATED COLOR DEFINITIONS (Single Source of Truth)
# ==============================================================================


# Background colors for translation source types
SOURCE_COLORS: Dict[str, str] = {
    SOURCE_NEW: "#FEF9C3",        # Yellow - pending/new
    SOURCE_ZENDESK_DC: "#BBF7D0",  # Green - from DC
    SOURCE_TRANSLATED: "#BFDBFE",  # Blue - translated
    SOURCE_CACHE: "#C7D2FE",      # Indigo - from cache
    SOURCE_FAILED: "#FECACA",     # Red - failed
    SOURCE_MANUAL: "#DDD6FE",     # Purple - manual edit
    SOURCE_ATTENTION: "#FED7AA",  # Orange - needs attention
    SOURCE_RESERVED: "#D1D5DB",   # Gray - reserved/system
}

# Text colors for translation source types
TEXT_COLORS: Dict[str, str] = {
    SOURCE_NEW: "#854D0E",        # Dark yellow/brown
    SOURCE_ZENDESK_DC: "#166534",  # Dark green
    SOURCE_TRANSLATED: "#1E40AF",  # Dark blue
    SOURCE_CACHE: "#3730A3",      # Dark indigo
    SOURCE_FAILED: "#991B1B",     # Dark red
    SOURCE_MANUAL: "#6B21A8",     # Dark purple
    SOURCE_ATTENTION: "#C2410C",  # Dark orange
    SOURCE_RESERVED: "#4B5563",   # Dark gray
}

# Placeholder source colors
PLACEHOLDER_COLORS: Dict[str, str] = {
    'existing': '#CFFAFE',   # Cyan - existing DC from Zendesk
    'proposed': '#F1F5F9',   # Slate - proposed/will be created
}

# Placeholder text colors
PLACEHOLDER_TEXT_COLORS: Dict[str, str] = {
    'existing': '#0E7490',   # Dark cyan
    'proposed': '#475569',   # Dark slate
}

# Log console colors
LOG_COLORS: Dict[str, str] = {
    'background': '#0D1117',
    'text': '#10B981',
    'border': '#30363D',
    'selection_bg': '#1F6FEB',
}


# ==============================================================================
# SYSTEM FIELD IDENTIFIERS (field types from Zendesk API)
# ==============================================================================


SYSTEM_FIELD_IDENTIFIERS: FrozenSet[str] = frozenset({
    'subject', 'description', 'status', 'tickettype', 'ticket_type',
    'priority', 'group', 'assignee', 'brand', 'tags',
    'satisfaction_rating', 'satisfaction_reason', 'custom_status',
    'lookup', 'lookup_relationship', 'email', 'name',
    'due_date', 'due_at', 'organization', 'requester', 'submitter',
    'collaborator', 'follower', 'cc', 'ccs',
    'via', 'via_id', 'recipient', 'channel',
    'problem', 'incident', 'problem_id',
    'followup', 'followup_source_id',
    'sla', 'sla_policy', 'skill', 'skills', 'agent_workspace',
    'sharing', 'shared', 'sharing_agreement_ids',
    'ticket_form_id', 'ticket_form', 'external_id',
    'macro_id', 'macro_ids', 'answer_bot',
    'suggested_articles', 'suggested_macros',
    'messaging', 'messaging_channel', 'conversation', 'conversation_id',
    'talk', 'call', 'voicemail', 'callback',
    'chat', 'chat_id', 'twitter', 'facebook', 'instagram',
    'attachments', 'attachment', 'comment', 'comments',
    'metric', 'metrics', 'audit', 'audits',
    'sunshine', 'unified_conversation',
})


# ==============================================================================
# SYSTEM FIELD NAMES (exact lowercase matches)
# ==============================================================================


SYSTEM_FIELD_NAMES: FrozenSet[str] = frozenset({
    'intent', 'sentiment', 'language', 'summary', 'confidence',
    'approval status', 'approval_status',
    'subject', 'description', 'status', 'type', 'priority',
    'group', 'assignee', 'requester', 'submitter', 'organization',
    'tags', 'brand', 'satisfaction', 'satisfaction rating', 'csat',
    'created', 'created at', 'updated', 'updated at',
    'due date', 'due at', 'solved at', 'closed at',
    'sla', 'sla policy', 'first reply time', 'next reply time',
    'resolution time', 'channel', 'via', 'source', 'recipient',
    'cc', 'ccs', 'followers', 'collaborators',
    'problem', 'incident', 'linked problem', 'linked incident',
    'ticket id', 'id',
    'intent confidence', 'sentiment confidence', 'language confidence',
    'detected intent', 'detected sentiment', 'detected language',
    'auto-detected language', 'predicted intent', 'predicted sentiment',
    'ai summary', 'ai-generated', 'auto-reply', 'suggested reply',
    'suggested response', 'suggested macro', 'suggested article',
    'answer suggestion', 'answer bot',
    'status category', 'status state', 'custom status',
    'shared', 'sharing', 'shared ticket', 'shared with',
    'form', 'ticket form',
    'skill', 'skills', 'routing', 'agent skill', 'required skills',
    'capacity', 'workload', 'agent capacity', 'agent workload',
    'call', 'callback', 'voicemail', 'phone', 'talk',
    'call duration', 'wait time', 'hold time',
    'chat', 'chat rating', 'chat duration',
    'messaging', 'conversation', 'messaging channel',
    'twitter', 'facebook', 'instagram', 'whatsapp', 'social channel',
    'agent', 'agent name', 'agent id', 'handled by', 'assigned to',
    'assigned at', 'initially assigned at', 'first response at',
    'full resolution at', 'requester updated at', 'assignee updated at',
    'status updated at', 'replies', 'reply count', 'reopens',
    'reopen count', 'group stations', 'assignee stations',
})


# ==============================================================================
# SYSTEM FIELD TITLE WORDS (partial matches)
# ==============================================================================


SYSTEM_FIELD_TITLE_WORDS: FrozenSet[str] = frozenset({
    'confidence', 'sentiment', 'intent', 'summary',
    'prediction', 'predicted', 'suggested', 'suggestion',
    'recommended', 'recommendation', 'auto-reply', 'autoreply',
    'auto reply', 'macro suggestion', 'article suggestion', 'entity',
    'automated', 'automation', 'trigger', 'routing', 'routed',
    'assignment', 'auto-assign', 'autoassign',
    'resolution', 'resolved', 'resolver',
    'score', 'scoring', 'rating', 'approval',
    'sla', 'breach', 'first reply', 'next reply', 'resolution time',
    'wait time', 'work time', 'response time', 'handle time',
    'system', 'internal', 'zendesk', '_internal', '_system',
    'messaging', 'conversation', 'chat', 'talk', 'voice', 'call', 'social',
    'skill', 'capacity', 'workload',
    'satisfaction', 'csat', 'nps', 'survey', 'feedback',
    'answer bot', 'answerbot', 'ai-generated', 'ai generated',
    'machine learning', 'ml-', 'auto-',
    'triage', 'intelligent triage', 'auto-triage',
    'status category', 'status state',
    'metric', 'analytics', 'explore', 'report', 'dashboard',
    'sunshine', 'unified',
    'timestamp', 'datetime', 'date time', '_at', '_date', '_time',
    '_id', '_ids', 'identifier',
    '_count', 'count of', 'number of', 'total ',
    'channel type', 'via channel', 'source channel',
    'assignee', 'assigned', 'handler', 'handled',
    'locale', 'detected language', 'user language',
    'spam', 'suspended', 'blocked', 'security',
    'archived', 'deleted', 'closed',
})


# ==============================================================================
# SYSTEM TITLE PATTERNS (substring matches in titles)
# ==============================================================================


SYSTEM_TITLE_PATTERNS: FrozenSet[str] = frozenset({
    'ticket id', 'created at', 'updated at', 'solved at', 'closed at',
    'due at', 'due date',
    'ai ', ' ai', 'ai-', '-ai', 'ml ', ' ml',
    'system ', ' system', 'system_', '_system',
    'agent id', 'agent workspace', 'agent status',
    'date and time', 'timestamp',
    'locate', 'language confidence', 'sentiment confidence',
    'intent confidence',
    'summary agent', 'summary date', 'summary locate', 'summary language',
    'conversation summary', 'ticket summary',
    'approval status', 'approval required', 'requires approval',
    'pending approval', 'approval workflow',
    'sla policy', 'sla target', 'sla breach',
    'first reply time', 'next reply time', 'resolution time',
    'requester wait', 'agent work time',
    'channel type', 'source channel', 'via channel',
    'routing attribute', 'skill routing', 'omnichannel',
    'messaging channel', 'conversation id', 'conversation status',
    'messaging status',
    'integration', 'external id', 'sync status',
    'zendesk_', '_zendesk', 'zd_', '_zd',
    'answer bot', 'answerbot', 'suggested article', 'suggested macro',
    'auto reply', 'auto-reply',
    'intent:', 'sentiment:', 'language:', 'triage:',
    'auto-detected', 'predicted:',
    'status:', 'status category', 'custom status',
    'call id', 'call duration', 'callback', 'voicemail', 'recording',
    'talk:',
    'chat id', 'chat duration', 'chat rating', 'pre-chat', 'post-chat',
    'sunshine:', 'unified conversation', 'conversation:',
    'metric:', 'count:', 'total:', 'average:', 'median:',
    'twitter:', 'facebook:', 'instagram:', 'social:', 'dm:',
    'direct message',
    'assigned to', 'assigned at', 'assignee:', 'handled by',
    'group:', 'team:', 'requester:', 'customer:', 'end user:',
    'audit:', 'history:', 'change:', 'log:',
    'spam:', 'suspended:', 'blocked:', 'flagged:',
    'shared:', 'sharing:', 'shared with',
})


# ==============================================================================
# SYSTEM OPTION VALUES (dropdown values that are system-defined)
# ==============================================================================


SYSTEM_OPTION_VALUES: FrozenSet[str] = frozenset({
    'low', 'normal', 'medium', 'high', 'urgent',
    'very low', 'very high', 'critical',
    'new', 'open', 'pending', 'hold', 'on-hold', 'on hold',
    'solved', 'closed', 'deleted',
    'question', 'incident', 'problem', 'task',
    'good', 'bad', 'offered', 'unoffered', 'not offered',
    'satisfied', 'dissatisfied', 'not rated',
    'none', 'unknown', 'other', 'default', 'n/a',
    'not set', 'not available', 'not applicable',
    'unassigned', 'unspecified',
    'approved', 'rejected', 'withdrawn', 'pending',
    'pending approval', 'awaiting approval', 'not requested',
    'requested', 'in review', 'in progress',
    'automated', 'manual', 'auto-resolved', 'auto resolved',
    'auto-closed', 'auto closed',
    'positive', 'negative', 'neutral',
    'yes', 'no', 'true', 'false', 'enabled', 'disabled',
    'active', 'inactive',
    '-', '--', '---', '—', '–', '...',
    '(none)', '[none]', '(empty)', '[empty]', '(blank)', '[blank]',
    '(select)', '[select]', '-- select --', '- select -',
    'select...', 'select one', 'please select', 'choose...', 'choose one',
    'within sla', 'breached', 'paused', 'achieved', 'missed',
    'email', 'web', 'api', 'chat', 'phone',
    'twitter', 'facebook', 'instagram', 'whatsapp', 'sms',
    'voice', 'messaging', 'mobile', 'mobile sdk',
    'web widget', 'web form', 'help center',
    'closed ticket', 'side conversation', 'any channel',
    'agent', 'requester', 'agent_action', 'requester_action',
    'inbound', 'outbound', 'missed call', 'voicemail', 'callback',
    'transferred', 'abandoned', 'answered', 'unanswered',
    'online', 'offline', 'away', 'invisible', 'serving', 'not serving',
    'high confidence', 'medium confidence', 'low confidence',
    'not detected', 'detected',
    'shared', 'not shared', 'private', 'public',
    'default form', 'available', 'busy', 'transfers only',
    'any', 'all', 'all channels',
    'never', 'always', 'immediately', 'asap',
    'small', 'large', 'unlimited', 'limited',
    'en', 'en-us', 'en-gb', 'es', 'es-es', 'es-mx',
    'pt', 'pt-br', 'pt-pt', 'fr', 'fr-fr', 'de', 'de-de',
    'it', 'ja', 'ko', 'zh', 'zh-cn', 'zh-tw', 'ru', 'nl', 'pl',
    'ar', 'he', 'tr',
})


# ==============================================================================
# COMMON SHORT WORDS (to avoid false positives in acronym detection)
# ==============================================================================


COMMON_SHORT_WORDS: FrozenSet[str] = frozenset({
    'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YES', 'NO',
    'ALL', 'ANY', 'CAN', 'HAD', 'HER', 'WAS', 'ONE', 'OUR',
    'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS',
    'MAY', 'NEW', 'NOW', 'OLD', 'SEE', 'WAY', 'WHO', 'BOY',
    'DID', 'OWN', 'SAY', 'SHE', 'TOO', 'USE',
})


# ==============================================================================
# TRANSLATABLE SHORT WORDS (short words that SHOULD be translated)
# ==============================================================================


TRANSLATABLE_SHORT_WORDS: FrozenSet[str] = frozenset({
    'YES', 'NO', 'OK', 'HI', 'BYE',
    'SIM', 'NÃO',
    'OUI', 'NON',
    'SÍ',
    'JA', 'NEIN',
})


# ==============================================================================
# HELP CENTER SAMPLE ARTICLE PATTERNS
# ==============================================================================


HC_SAMPLE_ARTICLE_PATTERNS: FrozenSet[str] = frozenset({
    'how do i customize my help center',
    'what are these sections and articles doing here',
    'how can agents leverage knowledge to help customers',
    'how do i publish my content in other languages',
    'welcome to your help center',
    'sample article',
    'stellar skyonomy',
    'this is a sample article',
    'getting started with',
    'como personalizo minha central de ajuda',
    'como personalizo a minha central de ajuda',
    'como personalizou o meu centro de ajuda',
    'como personalizou a minha central de ajuda',
    'como eu personalizo minha central de ajuda',
    'como eu personalizo a minha central de ajuda',
    'o que estas seções e artigos estão fazendo aqui',
    'o que essas seções e artigos estão fazendo aqui',
    'o que essas seções e esses artigos estão fazendo aqui',
    'o que estas secções e artigos estão a fazer aqui',
    'como os agentes podem aproveitar o conhecimento',
    'como os agentes podem aproveitar o conhecimento para ajudar',
    'como os agentes podem aproveitar o conhecimento para ajudar os clientes',
    'como os agentes podem usar o conhecimento',
    'como agentes podem aproveitar o conhecimento',
    'como publico meu conteúdo em outros idiomas',
    'como publico o meu conteúdo em outros idiomas',
    'como eu publico meu conteúdo em outros idiomas',
    'como faço para publicar meu conteúdo em outros idiomas',
    'bem-vindo à sua central de ajuda',
    'bem-vindo ao seu centro de ajuda',
    'bem vindo à sua central de ajuda',
    'bem vindo ao seu centro de ajuda',
    'artigo de exemplo',
    'políticas de reembolso',
    'stellar skyonomy refund',
    'cómo personalizo mi centro de ayuda',
    'como personalizo mi centro de ayuda',
    'qué hacen aquí estas secciones y artículos',
    'que hacen aqui estas secciones y articulos',
    'cómo pueden los agentes aprovechar el conocimiento',
    'como pueden los agentes aprovechar el conocimiento',
    'cómo publico mi contenido en otros idiomas',
    'como publico mi contenido en otros idiomas',
    'bienvenido a tu centro de ayuda',
    'artículo de ejemplo',
    'articulo de ejemplo',
    'wie passe ich mein help center an',
    'was machen diese abschnitte und beiträge hier',
    'was machen diese abschnitte und beitrage hier',
    'willkommen in ihrem help center',
    'comment personnaliser mon centre d\'aide',
    'comment personnaliser mon centre d aide',
    'que font ces sections et ces articles ici',
    'bienvenue dans votre centre d\'aide',
    'bienvenue dans votre centre d aide',
    'come posso personalizzare il mio centro assistenza',
    'benvenuto nel tuo centro assistenza',
    'hoe pas ik mijn helpcenter aan',
    'welkom bij uw helpcentrum',
    'help center no customize',
    'placeholder article',
    'test article',
    'demo article',
    'example article',
    'template article',
})


# ==============================================================================
# HELP CENTER SAMPLE CATEGORY/SECTION NAMES
# ==============================================================================


HC_SAMPLE_CATEGORY_SECTION_NAMES: FrozenSet[str] = frozenset({
    'general', 'announcements', 'faq', 'faqs', 'frequently asked questions',
    'getting started', 'sample category', 'sample section',
    'knowledge base', 'documentation', 'guides', 'tutorials',
    'how-to', 'how to', 'support', 'help', 'resources',
    'community', 'discussions', 'feedback', 'ideas',
    'feature requests', 'bug reports', 'q&a', 'questions and answers',
    'geral', 'anúncios', 'anuncios', 'perguntas frequentes',
    'primeiros passos', 'começando', 'comecando',
    'categoria de exemplo', 'seção de exemplo', 'secao de exemplo',
    'base de conhecimento', 'documentação', 'guias', 'tutoriais',
    'como fazer', 'suporte', 'ajuda', 'recursos',
    'comunidade', 'discussões', 'discussoes',
    'preguntas frecuentes', 'primeros pasos', 'empezando',
    'categoría de ejemplo', 'categoria de ejemplo',
    'sección de ejemplo', 'seccion de ejemplo',
    'base de conocimiento', 'documentación', 'documentacion',
    'guías', 'guias', 'tutoriales', 'cómo hacer', 'como hacer',
    'soporte', 'ayuda',
    'allgemein', 'ankündigungen', 'ankundigungen',
    'häufig gestellte fragen', 'haufig gestellte fragen', 'erste schritte',
    'général', 'general', 'annonces', 'questions fréquentes',
    'questions frequentes', 'foire aux questions', 'commencer', 'premiers pas',
    'generale', 'annunci', 'domande frequenti', 'per iniziare',
    'algemeen', 'aankondigingen', 'veelgestelde vragen', 'aan de slag',
})


# ==============================================================================
# HELP CENTER SYSTEM PATTERNS
# ==============================================================================


HC_SYSTEM_PATTERNS: FrozenSet[str] = frozenset({
    '[archived]', '[draft]', '[do not edit]', '[do not translate]',
    '[system]', '[internal]', '[sample]', '[template]', '[placeholder]',
    '[test]', '[demo]', '[wip]', '[work in progress]',
    '[deprecated]', '[obsolete]', '[hidden]',
    '[exemplo]', '[rascunho]', '[arquivado]', '[interno]', '[modelo]', '[teste]',
    '[ejemplo]', '[borrador]', '[archivado]', '[plantilla]', '[prueba]',
    '(archived)', '(draft)', '(sample)', '(test)', '(internal)',
    '(deprecated)', '(wip)', '(hidden)',
    'template:', 'test:', 'internal:', 'draft:', 'sample:',
    'demo:', 'wip:', 'todo:', 'fixme:',
    '_archived', '_draft', '_system', '_internal', '_template',
    '_test', '_sample', '_hidden', '_deprecated',
    'api_', 'dev_', 'staging_', 'qa_', 'uat_',
})


# ==============================================================================
# HELP CENTER EXCLUDED LABELS
# ==============================================================================


HC_EXCLUDED_LABELS: FrozenSet[str] = frozenset({
    'system', 'internal', 'internal-only', 'internal only',
    'staff-only', 'staff only', 'agents-only', 'agents only',
    'admin-only', 'admin only',
    'do-not-translate', 'do_not_translate', 'do not translate',
    'no-translate', 'no_translate', 'no translate',
    'skip-translation', 'skip_translation', 'skip translation',
    'english-only', 'english only',
    'archive', 'archived', 'draft', 'drafts', 'wip',
    'work-in-progress', 'work in progress',
    'deprecated', 'obsolete', 'outdated', 'old', 'legacy',
    'sample', 'samples', 'example', 'examples',
    'test', 'tests', 'testing', 'demo', 'demos',
    'template', 'templates', 'placeholder', 'dummy',
    'exemplo', 'exemplos', 'rascunho', 'rascunhos',
    'teste', 'testes', 'interno', 'arquivado', 'modelo', 'modelos',
    'ejemplo', 'ejemplos', 'borrador', 'borradores',
    'prueba', 'pruebas', 'plantilla', 'plantillas',
    'api', 'api-docs', 'api docs', 'developer', 'developers',
    'dev', 'technical', 'integration', 'webhook', 'webhooks',
    'hidden', 'private', 'confidential', 'restricted', 'secret',
})


# ==============================================================================
# LOCALE MAPPINGS (Zendesk locale ID to locale string)
# ==============================================================================


LOCALE_ID_MAP: Dict[int, str] = {
    1: 'en-US', 2: 'es', 3: 'de', 4: 'fr', 5: 'it',
    6: 'nl', 7: 'pl', 8: 'pt-BR', 9: 'zh-CN', 10: 'ja',
    11: 'ko', 12: 'ru', 13: 'sv', 14: 'no', 15: 'da',
    16: 'fi', 17: 'ar', 18: 'he', 19: 'tr', 20: 'cs',
    21: 'hu', 22: 'th', 23: 'id', 24: 'uk', 25: 'vi',
    26: 'pt', 27: 'zh-TW', 28: 'ms', 29: 'ca', 30: 'sk',
    31: 'el', 32: 'bg', 33: 'ro', 34: 'hr', 35: 'sl',
    36: 'lt', 37: 'lv', 38: 'et',
    1000: 'en', 1001: 'en-GB', 1002: 'en-AU', 1003: 'en-CA',
    1004: 'es-ES', 1005: 'es-MX', 1006: 'es-419',
    1007: 'fr-CA', 1008: 'fr-FR', 1009: 'de-AT', 1010: 'de-CH',
    1011: 'nl-BE', 1012: 'pt-PT', 1176: 'pt-br',
    1013: 'en-NZ', 1014: 'en-IE', 1015: 'en-ZA',
    1016: 'es-AR', 1017: 'es-CL', 1018: 'es-CO',
    1019: 'fr-BE', 1020: 'fr-CH', 1021: 'de-DE',
    1022: 'it-CH', 1023: 'nl-NL',
}