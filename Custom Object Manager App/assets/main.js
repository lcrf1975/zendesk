/* global ZAFClient, Tabulator, TomSelect */
// main.js is loaded as an ES module (see iframe.html). Helpers imported from ./modules/*;
// stateful UI code lives here so we don't paper over a natural module boundary with churn.
import { CONFIG } from './modules/config.js';
import { loadPrefs, savePrefs } from './modules/prefs.js';
import {
  escapeHtml, formatElapsed,
  normalizeForMatch, sanitizeSearchPhrase,
  normalizeForDuplicate, normalizeForSimilar, DUPLICATE_STOPWORDS,
  diffHighlight, csvEscape,
} from './modules/utils.js';

// Initialize the ZAF Client
const client = ZAFClient.init();

// ============================================================
// SHA256 / EXTERNAL ID HELPERS
// ============================================================
function fixMojibake(s) {
  s = s.replace(/Ã /g, "à");
  try {
    const bytes = new Uint8Array([...s].map(c => c.charCodeAt(0)));
    return new TextDecoder("utf-8").decode(bytes);
  } catch { return s; }
}
async function computeExternalId(newName, originalExternalId) {
  const clean = fixMojibake(newName);
  const encoded = new TextEncoder().encode(clean);
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoded);
  const sha = Array.from(new Uint8Array(hashBuffer)).map(b => b.toString(16).padStart(2, "0")).join("");
  const parts = (originalExternalId || "").split("_");
  if (parts.length === 2 && /^\d+$/.test(parts[1])) return `${sha}_${parts[1]}`;
  return sha;
}

// ============================================================
// ZAF REQUEST WRAPPER WITH 429/503 RETRY + BACKOFF
// ============================================================
// Zendesk rate limits to ~700 req/min per agent. Large Reverse Lookup scans or
// delete propagation can hit that ceiling. The wrapper retries transient errors
// (429 Too Many Requests, 503 Service Unavailable) honouring the Retry-After
// header when present, with exponential backoff otherwise. All other errors
// propagate unchanged so existing catch blocks still work.
async function zafRequest(options, retriesLeft = 3) {
  try {
    return await client.request(options);
  } catch (err) {
    const status = err?.status ?? err?.statusCode;
    const isRetryable = status === 429 || status === 503;
    if (!isRetryable || retriesLeft <= 0) throw err;

    let waitMs = 1000 * Math.pow(2, 3 - retriesLeft);
    const retryAfter = err?.responseHeaders?.['retry-after'] ?? err?.responseHeaders?.['Retry-After'];
    if (retryAfter) {
      const seconds = parseInt(retryAfter, 10);
      if (!isNaN(seconds) && seconds > 0) waitMs = Math.min(seconds * 1000, 30000);
    }
    waitMs = Math.min(waitMs, 10000);
    console.warn(`[zafRequest] ${status} — retrying in ${waitMs}ms (${retriesLeft} left)`);
    await new Promise(r => setTimeout(r, waitMs));
    return zafRequest(options, retriesLeft - 1);
  }
}

// Global App State
let currentCoKey = null;
let currentCoTitle = null; 
let currentSchema = null;
let tabulatorTable = null;
let columnSelectorTS = null; 
let objectSelectorTS = null; 
let cachedLookupFields       = null; // Caches the relation schema so we don't spam the API
let cachedTextTicketFieldIds = null; // Caches text/textarea ticket field IDs for ticket search
let cachedTextUserFieldKeys  = null; // Caches text/textarea user field keys for user search
let cachedTextOrgFieldKeys   = null; // Caches text/textarea org field keys for org search
let _rlCancelled             = false; // Cancellation flag for Reverse Lookup
let activeFilters = [];        // Active advanced filter conditions
let filterColumns = [];        // Columns available for filtering (set per table load)
let lastFilterCoKey = null;    // Tracks which CO the filter bar was built for
let currentLoadToken = null;   // Cancels stale background page loads when CO changes
let isBackgroundLoading = false;
// Set to true when the user clicks "Stop loading" in the summary bar. Distinct
// from currentLoadToken invalidation (which happens on CO switch) because Stop
// must keep what was already fetched and re-enable the toolbar.
let _bgLoadStopped = false;
let zendeskBaseUrl = '';       // Zendesk base URL for constructing item links
let formIsDirty = false;       // Tracks unsaved changes in the active form
let rowNumMap = null;          // id -> consecutive # built from dataFiltered/dataSorted rows
let _rnRedrawing = false;      // guard: prevents cascade if redraw(true) fires dataFiltered
let _loaderTimer = null;       // elapsed timer for the #loader view
let _preScanTimer = null;      // elapsed timer for "Discovering..." phase before fullReferenceScan

// ============================================================
// INTERNATIONALISATION
// ============================================================
// Translations are loaded from assets/i18n/{en,pt-BR,es}.js via <script> tags in
// iframe.html and attached to window.TRANSLATIONS before main.js runs.
const TRANSLATIONS = window.TRANSLATIONS || {};
let i18n = TRANSLATIONS.en || {};

// Returns the translated string for key, replacing {var} placeholders with vars
function t(key, vars = {}) {
  let str = i18n[key] ?? TRANSLATIONS.en?.[key] ?? key;
  Object.entries(vars).forEach(([k, v]) => {
    str = str.replace(new RegExp(`\\{${k}\\}`, 'g'), String(v));
  });
  return str;
}

// Detects the Zendesk user locale and sets the active translation set
async function initLocale() {
  let locale = 'en';
  try {
    const data = await client.get('currentUser');
    locale = (data.currentUser?.locale || 'en').toLowerCase();
  } catch (e) {
    console.warn('[i18n] Could not read Zendesk user locale, defaulting to English.', e);
  }
  if (locale.startsWith('pt'))      { i18n = TRANSLATIONS['pt-BR'] || TRANSLATIONS.en || {}; document.documentElement.lang = 'pt-BR'; }
  else if (locale.startsWith('es')) { i18n = TRANSLATIONS['es']    || TRANSLATIONS.en || {}; document.documentElement.lang = 'es';    }
  else                              { i18n = TRANSLATIONS['en']    || {};                    document.documentElement.lang = 'en';    }
}

// Applies translations to static HTML elements that carry data-i18n* attributes
function applyI18nToDOM() {
  document.title = t('app.title');
  document.querySelectorAll('[data-i18n]').forEach(el => {
    el.textContent = t(el.getAttribute('data-i18n'));
  });
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    el.placeholder = t(el.getAttribute('data-i18n-placeholder'));
  });
}

// DOM Elements — cached at module load time (modules are deferred so DOM is ready)
const views = {
  loader: document.getElementById('loader'),
  selector: document.getElementById('selector-view'),
  table: document.getElementById('table-view'),
  form: document.getElementById('form-view')
};
// Cached separately because it's queried on every row-selection-change event
const btnBulkDelete = document.getElementById('btn-bulk-delete');

// Initialize the app when the DOM is ready.
// Note: this module is loaded with `type="module"` which implies `defer`, so it may
// execute AFTER DOMContentLoaded has already fired. Check readyState and run the init
// immediately in that case — addEventListener alone would silently never trigger.
const bootApp = async () => {
  await initLocale();
  applyI18nToDOM();
  startApp();

  // Toolbar Listeners
  document.getElementById('btn-new-record').addEventListener('click', () => showForm());
  document.getElementById('btn-bulk-delete').addEventListener('click', () => {
    if (!tabulatorTable) return;
    const selected = tabulatorTable.getSelectedData();
    if (selected.length > 0) showBulkDeleteModal(selected);
  });
  document.getElementById('btn-back-selector').addEventListener('click', startApp);
  document.getElementById('btn-refresh').addEventListener('click', async () => {
    if (!currentCoKey || isBackgroundLoading) return;
    // If a form is open with unsaved edits, confirm discard before reload wipes them.
    if (formIsDirty) {
      const leave = await showUnsavedChangesModal();
      if (!leave) return;
      formIsDirty = false;
    }
    // Invalidate cached lookup schema so Usage & Impact picks up any new fields.
    cachedLookupFields = null;
    loadTable(currentCoKey);
  });
  document.getElementById('btn-refresh').title = t('table.refresh');
  
  // Tabulator Global Search (now delegates to unified filter).
  // Debounced so we don't re-run the filter predicate on every keystroke — noticeable
  // on large datasets where applyTableFilters iterates every row.
  let _searchDebounceTimer = null;
  document.getElementById('table-search').addEventListener('input', function() {
    if (_searchDebounceTimer) clearTimeout(_searchDebounceTimer);
    _searchDebounceTimer = setTimeout(() => {
      _searchDebounceTimer = null;
      applyTableFilters();
    }, CONFIG.SEARCH_DEBOUNCE_MS);
  });

  // Export CSV
  document.getElementById('btn-export-csv').addEventListener('click', showExportModal);
  document.getElementById('btn-reverse-lookup').addEventListener('click', () => {
    if (!tabulatorTable) return;
    showReverseLookupModal();
  });
  document.getElementById('btn-find-duplicates').addEventListener('click', () => {
    if (!tabulatorTable) return;
    showFindDuplicatesModal();
  });

  // Advanced Filter toggle
  document.getElementById('btn-advanced-filter').addEventListener('click', () => {
    const filterBar = document.getElementById('filter-bar');
    const btn = document.getElementById('btn-advanced-filter');
    const isVisible = filterBar.style.display !== 'none';
    if (!isVisible) {
      filterBar.style.display = 'block';
      btn.classList.add('active');
      // Auto-add a first empty row if the bar is being opened fresh
      if (document.getElementById('filter-rows').children.length === 0) {
        addFilterRow();
      }
    } else {
      filterBar.style.display = 'none';
      btn.classList.remove('active');
    }
  });

  // Restore Columns Button Listener
  document.getElementById('btn-restore-columns').addEventListener('click', () => {
    if (!currentSchema || !columnSelectorTS || !tabulatorTable) return;
    const defaultCols = computeDefaultVisibleCols();
    columnSelectorTS.setValue(defaultCols);
    savePrefs(currentCoKey, { visibleColumns: defaultCols });
  });

  // Global keyboard shortcuts:
  //   Esc     — close topmost open modal (or collapse filter bar if it's the only thing open)
  //   Ctrl+K  — focus the global search input (also supports Cmd+K on macOS)
  //   /       — focus the global search input (like GitHub, when not already typing)
  const MODAL_OVERLAY_IDS = [
    'delete-modal-overlay',
    'bulk-delete-overlay',
    'unsaved-modal-overlay',
    'reverse-lookup-overlay',
    'find-duplicates-overlay',
    'export-modal-overlay',
  ];
  const isModalOpen = () => MODAL_OVERLAY_IDS.some(id => {
    const el = document.getElementById(id);
    return el && el.style.display && el.style.display !== 'none';
  });
  const isTypingTarget = (target) => {
    const tag = (target?.tagName || '').toLowerCase();
    return tag === 'input' || tag === 'textarea' || tag === 'select' || target?.isContentEditable;
  };

  document.addEventListener('keydown', (e) => {
    // Escape always runs first so modals can be dismissed even while typing.
    if (e.key === 'Escape') {
      // Close the topmost open overlay. Order matters: last-opened should close first.
      for (const id of MODAL_OVERLAY_IDS) {
        const ov = document.getElementById(id);
        if (ov && ov.style.display !== 'none' && ov.style.display !== '') {
          // For the unsaved-changes modal, "Stay" is the safe default on Esc.
          if (id === 'unsaved-modal-overlay') {
            document.getElementById('unsaved-modal-stay')?.click();
          } else {
            ov.style.display = 'none';
          }
          e.preventDefault();
          return;
        }
      }
      // No modal open — collapse the advanced filter bar if it's showing.
      // Note: Esc hides the bar UI but intentionally keeps activeFilters intact so
      // the filter remains applied (badge stays on button). This matches the same
      // behaviour as clicking the Advanced Filter button a second time to close it.
      const filterBar = document.getElementById('filter-bar');
      if (filterBar && filterBar.style.display === 'block') {
        filterBar.style.display = 'none';
        document.getElementById('btn-advanced-filter')?.classList.remove('active');
      }
      return;
    }

    // Ctrl+K / Cmd+K — focus global search. Blocked while a modal is open or the user is typing
    // in another field so it doesn't steal focus from forms, filter rows, or modal inputs.
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      if (isModalOpen() || isTypingTarget(e.target)) return;
      const searchEl = document.getElementById('table-search');
      if (searchEl && views.table.style.display !== 'none' && !searchEl.disabled) {
        e.preventDefault();
        searchEl.focus();
        searchEl.select();
      }
      return;
    }

    // "/" — same guards as Ctrl+K. Typing check is essential here since "/" is a valid character.
    if (e.key === '/' && !e.ctrlKey && !e.metaKey && !e.altKey) {
      if (isModalOpen() || isTypingTarget(e.target)) return;
      const searchEl = document.getElementById('table-search');
      if (searchEl && views.table.style.display !== 'none' && !searchEl.disabled) {
        e.preventDefault();
        searchEl.focus();
        searchEl.select();
      }
      return;
    }

  });

  // Live-resize: browser window or Zendesk workspace pane resize should recompute
  // iframe height and redraw Tabulator so columns refit the new width. Debounced
  // to collapse the 60+ resize events browsers fire per drag into one update.
  let _winResizeTimer = null;
  window.addEventListener('resize', () => {
    if (_winResizeTimer) clearTimeout(_winResizeTimer);
    _winResizeTimer = setTimeout(() => {
      _winResizeTimer = null;
      resizeIframe();
      if (tabulatorTable && views.table.style.display !== 'none') {
        tabulatorTable.redraw(true);
      }
    }, CONFIG.RESIZE_DEBOUNCE_MS);
  });
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bootApp);
} else {
  bootApp();
}

// Helper for Tabulator Search
function customFilter(data, filterParams) {
  const search = filterParams.toLowerCase();
  for (let key in data) {
    const val = data[key];
    if (val !== null && val !== undefined && typeof val !== 'object') {
      if (String(val).toLowerCase().includes(search)) {
        return true;
      }
    }
  }
  return false;
}

// Lightweight toast notifications.
// level: 'info' | 'success' | 'warning' | 'error'.
// Auto-dismiss after CONFIG.TOAST_DURATION_MS; click the toast to dismiss early.
function showToast(message, level = 'info') {
  const container = document.getElementById('toast-container');
  if (!container) return;
  const toast = document.createElement('div');
  toast.className = `toast toast-${level}`;
  toast.setAttribute('role', level === 'error' || level === 'warning' ? 'alert' : 'status');
  toast.textContent = message;
  const dismiss = () => {
    toast.classList.add('toast-fade-out');
    setTimeout(() => toast.remove(), 200);
  };
  toast.addEventListener('click', dismiss);
  container.appendChild(toast);
  setTimeout(dismiss, CONFIG.TOAST_DURATION_MS);
}

function startPreScanTimer() {
  stopPreScanTimer();
  let secs = 0;
  _preScanTimer = setInterval(() => {
    secs++;
    const el = document.getElementById('pre-scan-timer');
    if (el) el.textContent = formatElapsed(secs);
  }, 1000);
}

function stopPreScanTimer() {
  if (_preScanTimer) { clearInterval(_preScanTimer); _preScanTimer = null; }
}

let _resizeTimer = null;
function resizeIframe() {
  // Debounced: many call sites (switchView, dataFiltered, dataSorted,
  // tableBuilt, DOMContentLoaded) can fire in quick succession. Collapse
  // bursts into a single client.invoke('resize') call to avoid postMessage
  // chatter and intermediate-height flicker.
  if (_resizeTimer) clearTimeout(_resizeTimer);
  _resizeTimer = setTimeout(() => {
    _resizeTimer = null;
    // Cap at 75% of the available screen height so the panel never extends
    // off the bottom of the Zendesk workspace (which is ~75-80% of the screen
    // after browser chrome + Zendesk top bar are subtracted).
    // Content taller than this cap scrolls inside the body (overflow-y: auto).
    const maxH = Math.floor((window.screen.availHeight || 900) * CONFIG.IFRAME_MAX_HEIGHT_RATIO);
    const h = Math.min(document.body.scrollHeight + 16, maxH);
    client.invoke('resize', { width: '100%', height: `${h}px` });
  }, CONFIG.RESIZE_DEBOUNCE_MS);
}

function switchView(activeViewId) {
  if (_loaderTimer) { clearInterval(_loaderTimer); _loaderTimer = null; }
  const loaderTimerEl = document.getElementById('loader-timer');
  if (loaderTimerEl) loaderTimerEl.textContent = '';
  // Pre-scan timer lives inside the delete-confirm modal / Usage & Impact
  // loader; leaving the view orphans its DOM element, so stop the tick.
  stopPreScanTimer();

  Object.values(views).forEach(el => el.style.display = 'none');
  views[activeViewId].style.display = 'block';

  if (activeViewId === 'loader') {
    let secs = 0;
    _loaderTimer = setInterval(() => {
      secs++;
      const el = document.getElementById('loader-timer');
      if (el) el.textContent = formatElapsed(secs);
    }, 1000);
  }

  // When returning to the table view, force Tabulator to recalculate column
  // widths and re-render rows. fitColumns caches width=0 while the container
  // is hidden, so rows appear blank until a full redraw is triggered.
  if (activeViewId === 'table' && tabulatorTable) {
    requestAnimationFrame(() => { if (tabulatorTable) tabulatorTable.redraw(true); });
  }

  requestAnimationFrame(resizeIframe);
}

function updateLoaderText(text, { skeleton = false } = {}) {
  const loaderTextEl = document.getElementById('loader-text');
  if (loaderTextEl) loaderTextEl.innerText = text;
  const skeletonEl = document.getElementById('loader-skeleton');
  if (skeletonEl) skeletonEl.style.display = skeleton ? 'flex' : 'none';
}

// activeCount: when called from dataFiltered, pass rows.length directly because
// getData('active') may not have committed the new filter state yet in Tabulator 5.
function updateRecordSummary(activeCount) {
  const summaryEl = document.getElementById('record-summary');
  if (!summaryEl || !tabulatorTable) return;
  const total = tabulatorTable.getData().length;
  const active = activeCount !== undefined ? activeCount : (rowNumMap ? rowNumMap.size : tabulatorTable.getData().length);
  const loadingNote = isBackgroundLoading
    ? ` <span style="color:#1f73b7; font-size:12px; font-weight:600;">${t('summary.loadingMore')}</span>`
    : '';
  summaryEl.innerHTML = `${t('summary.showing')} <strong>${active}</strong> ${t('summary.of')} <strong>${total}</strong> ${t('summary.records')}${loadingNote}`;
}

// Renders a determinate progress bar into el, showing done/total and a status label
function renderScanProgress(el, done, total, label, elapsedSecs) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  const timerHtml = (elapsedSecs !== undefined && elapsedSecs >= 0)
    ? ` <span style="color:#68737d;">· ${formatElapsed(elapsedSecs)}</span>`
    : '';
  el.innerHTML = `
    <div style="padding: 20px; text-align: center;">
      <p style="color: #68737d; margin: 0 0 8px 0; font-size: 13px;">${escapeHtml(label)}</p>
      <div class="progress-container">
        <div class="progress-bar-determinate" style="width: ${pct}%;"></div>
      </div>
      <p style="color: #68737d; margin: 6px 0 0 0; font-size: 12px;">${t('delete.checksComplete', { done, total })}${timerHtml}</p>
    </div>
  `;
}

// ----------------------------------------------------
// VIEW 1: SELECTOR
// ----------------------------------------------------
// Enables or disables the search box and Advanced Filter button.
// Called when background loading starts/ends to prevent partial-data searches.
function setLoadingUIState(loading) {
  const searchEl = document.getElementById('table-search');
  const filterBtn = document.getElementById('btn-advanced-filter');
  if (searchEl) {
    searchEl.disabled = loading;
    searchEl.placeholder = loading ? t('table.searchLoading') : t('table.searchPlaceholder');
  }
  if (filterBtn) {
    filterBtn.disabled = loading;
    filterBtn.title = loading ? t('export.loadingWarning') : '';
  }
  const reverseLookupBtn = document.getElementById('btn-reverse-lookup');
  if (reverseLookupBtn) {
    reverseLookupBtn.disabled = loading;
    reverseLookupBtn.title = loading ? t('export.loadingWarning') : '';
  }
  const exportBtn = document.getElementById('btn-export-csv');
  if (exportBtn) {
    exportBtn.disabled = loading;
    exportBtn.title = loading ? t('export.loadingWarning') : '';
  }
  const findDupBtn = document.getElementById('btn-find-duplicates');
  if (findDupBtn) {
    findDupBtn.disabled = loading;
    findDupBtn.title = loading ? t('export.loadingWarning') : '';
  }
  const refreshBtn = document.getElementById('btn-refresh');
  if (refreshBtn) {
    refreshBtn.disabled = loading;
    refreshBtn.title = loading ? t('export.loadingWarning') : t('table.refresh');
  }
}

async function startApp() {
  updateLoaderText(t('loader.customObjects'));
  switchView('loader');
  // Fetch Zendesk base URL once for constructing item links
  if (!zendeskBaseUrl) {
    try {
      const ctx = await client.context();
      zendeskBaseUrl = `https://${ctx.account.subdomain}.zendesk.com`;
    } catch (e) {}
  }
  try {
    const response = await zafRequest('/api/v2/custom_objects');
    const customObjects = response.custom_objects;
    renderObjectSelector(customObjects);
    switchView('selector');
  } catch (error) {
    updateLoaderText(t('loader.error.customObjects'));
    console.error(error);
  }
}

function renderObjectSelector(objects) {
  let html = `<h2>${t('selector.title')}</h2>
              <div class="form-group">
                <select id="co-selector" placeholder="${t('selector.title')}">
                  <option value="">${t('selector.placeholder')}</option>`;
  objects.forEach(obj => {
    html += `<option value="${escapeHtml(obj.key)}">${escapeHtml(obj.title_pluralized)}</option>`;
  });
  html += `</select></div>
           <button class="btn" id="load-co-btn">${t('selector.button')}</button>`;
           
  views.selector.innerHTML = html;

  if (objectSelectorTS) {
    objectSelectorTS.destroy();
  }

  objectSelectorTS = new TomSelect('#co-selector', {
    create: false,
    sortField: { field: "text", direction: "asc" }
  });

  document.getElementById('load-co-btn').addEventListener('click', async () => {
    const selector = document.getElementById('co-selector');
    const selectedKey = selector.value;
    if (selectedKey) {
      currentCoKey = selectedKey;
      currentCoTitle = selector.options[selector.selectedIndex].text;
      cachedLookupFields       = null;
      cachedTextTicketFieldIds = null;
      cachedTextUserFieldKeys  = null;
      cachedTextOrgFieldKeys   = null;
      await loadTable(currentCoKey);
    }
  });
}

// Returns ordered list of field keys to show by default, prioritising compact and
// high-value columns. Uses currentSchema (global) so both loadTable and the
// Restore button share the same logic.
function computeDefaultVisibleCols() {
  if (!currentSchema) return [];
  const typeRank = (type) => {
    if (!type) return 5;                          // id and unknown
    if (type === 'checkbox' || type === 'dropdown' || type === 'date' || type === 'integer' || type === 'decimal') return 1;
    if (type === 'lookup' || type.startsWith('zen:')) return 2;
    if (type === 'text') return 3;
    if (type === 'textarea') return 4;
    return 3;
  };

  const containerWidth = document.getElementById('table-view').clientWidth || window.innerWidth;
  const fixedWidth = CONFIG.SELECTION_COL_WIDTH + CONFIG.ROWNUM_COL_WIDTH + CONFIG.TABLE_VIEW_HORIZONTAL_PADDING;
  const maxDataCols = Math.max(2, Math.floor((containerWidth - fixedWidth) / CONFIG.DEFAULT_COL_WIDTH));

  // Build candidate list: name first, then schema fields ranked by type, id last
  const candidates = [
    { field: 'name', rank: 0 },
    ...currentSchema.map(f => ({ field: f.key, rank: typeRank(f.type) })),
    { field: 'id', rank: 5 },
  ];
  candidates.sort((a, b) => a.rank - b.rank);

  const result = [];
  for (const c of candidates) {
    if (result.length >= maxDataCols) break;
    if (!result.includes(c.field)) result.push(c.field);
  }
  return result;
}

// ----------------------------------------------------
// VIEW 2: DATA TABLE (TABULATOR)
// ----------------------------------------------------
async function loadTable(coKey) {
  updateLoaderText(t('loader.schema'));
  switchView('loader');
  try {
    const myToken = Symbol();
    currentLoadToken = myToken;
    isBackgroundLoading = false;
    _bgLoadStopped = false;
    rowNumMap = null;

    // Paginate schema: CO field endpoint returns 100/page by default — without
    // fetchAllPages, objects with more than 100 fields would silently drop the rest.
    currentSchema = await fetchAllPages(`/api/v2/custom_objects/${coKey}/fields`, 'custom_object_fields');

    updateLoaderText(t('loader.records'), { skeleton: true });
    const firstResponse = await zafRequest(`/api/v2/custom_objects/${coKey}/records?page[size]=${CONFIG.CO_RECORDS_PAGE_SIZE}`);
    const firstPageRecords = firstResponse.custom_object_records || [];

    updateLoaderText(t('loader.rendering'), { skeleton: true });

    const tableData = firstPageRecords.map(record => ({
      id: record.id,
      name: record.name,
      external_id: record.external_id,
      ...record.custom_object_fields
    }));

    const hasMorePages = !!(firstResponse.meta?.has_more &&
      (firstResponse.links?.next || firstResponse.next_page));
    isBackgroundLoading = hasMorePages;
    setLoadingUIState(hasMorePages); // always sync UI state, not just when loading starts

    const titleEl = document.getElementById('table-title');
    if (titleEl) {
      titleEl.innerText = currentCoTitle || currentCoKey;
    }

    const columns = [
      {
        // Row-selection checkbox (Tabulator built-in formatter)
        formatter: "rowSelection",
        titleFormatter: "rowSelection",
        hozAlign: "center",
        headerHozAlign: "center",
        headerSort: false,
        width: 36,
        minWidth: 36,
        maxWidth: 36,
        resizable: false
      },
      {
        title: "#",
        field: "custom_rownum",
        headerSort: false,
        width: CONFIG.ROWNUM_COL_WIDTH,
        minWidth: 40,
        maxWidth: CONFIG.ROWNUM_COL_WIDTH,
        hozAlign: "center",
        resizable: false,
        formatter: function(cell) {
          return rowNumMap ? (rowNumMap.get(cell.getData().id) || '') : '';
        }
      },
      { title: t('col.id'), field: "id", width: 80, minWidth: 80, hozAlign: "center", headerHozAlign: "center" },
      { title: t('col.name'), field: "name", minWidth: CONFIG.DEFAULT_COL_WIDTH },
      { title: t('col.externalId'), field: "external_id", width: 140, minWidth: 100 }
    ];

    currentSchema.forEach((field) => {
      let colDef = { title: field.title, field: field.key };

      if (field.type === 'checkbox') {
        colDef.width          = 100;
        colDef.minWidth       = 80;
        colDef.hozAlign       = "center";
        colDef.headerHozAlign = "center";
        colDef.formatter      = "tickCross";
      } else if (field.type === 'date') {
        colDef.width          = 120;
        colDef.minWidth       = 100;
        colDef.hozAlign       = "center";
        colDef.headerHozAlign = "center";
      } else if (field.type === 'integer' || field.type === 'decimal') {
        colDef.width          = 110;
        colDef.minWidth       = 80;
        colDef.hozAlign       = "center";
        colDef.headerHozAlign = "center";
      } else {
        colDef.minWidth = CONFIG.DEFAULT_COL_WIDTH;
      }

      columns.push(colDef);
    });

    columns.push({
      title: t('col.actions'),
      field: "actions",
      formatter: function() {
        return `<div style="display:flex;align-items:center;justify-content:center;height:100%;gap:4px;">
          <button class="btn-edit" title="${t('row.edit')}"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg></button>
          <button class="btn-danger" title="${t('row.delete')}"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg></button>
        </div>`;
      },
      width: CONFIG.ACTIONS_COL_WIDTH,
      minWidth: CONFIG.ACTIONS_COL_WIDTH,
      maxWidth: CONFIG.ACTIONS_COL_WIDTH,
      frozen: true,
      headerSort: false,
      cellClick: function(e, cell) {
        const rowData = cell.getRow().getData();
        if (e.target.closest('.btn-edit')) {
          showForm(rowData);
        } else if (e.target.closest('.btn-danger')) {
          deleteRecord(rowData.id, rowData.name);
        }
      }
    });

    switchView('table');
    document.getElementById('table-search').value = '';

    // Restore saved column visibility if the user has loaded this CO before; fall
    // back to the width-based autofit when nothing is stored.
    const savedPrefs = loadPrefs(coKey);
    const savedVisibleCols = savedPrefs?.visibleColumns;
    const defaultVisibleCols = [];

    if (Array.isArray(savedVisibleCols) && savedVisibleCols.length > 0) {
      const savedSet = new Set(savedVisibleCols);
      columns.forEach(col => {
        if (!col.field || col.field === 'actions' || col.field === 'custom_rownum') {
          col.visible = true;
          return;
        }
        col.visible = savedSet.has(col.field);
        if (col.visible) defaultVisibleCols.push(col.field);
      });
    } else {
      const defaultCols = new Set(computeDefaultVisibleCols());
      columns.forEach(col => {
        if (!col.field || col.field === 'actions' || col.field === 'custom_rownum') {
          col.visible = true;
          return;
        }
        col.visible = defaultCols.has(col.field);
        if (col.visible) defaultVisibleCols.push(col.field);
      });
    }

    if(tabulatorTable) {
      tabulatorTable.destroy();
    }
    // destroy() does not clear externally-mounted pagination elements — wipe manually
    // to prevent duplicate pagination controls when loading a second CO.
    const paginationBar = document.getElementById('table-pagination-bar');
    if (paginationBar) paginationBar.innerHTML = '';

    const savedPageSize = savedPrefs?.pageSize && CONFIG.TABLE_PAGINATION_SIZES.includes(savedPrefs.pageSize)
      ? savedPrefs.pageSize
      : CONFIG.TABLE_PAGINATION_SIZE;

    tabulatorTable = new Tabulator("#records-table", {
      data: tableData,
      layout: "fitDataStretch",
      pagination: "local",
      paginationSize: savedPageSize,
      paginationSizeSelector: CONFIG.TABLE_PAGINATION_SIZES,
      paginationElement: document.getElementById("table-pagination-bar"),
      renderHorizontal: "virtual",    // virtualize wide column sets (many CO fields)
      selectable: true,               // multi-row selection; click = toggle, shift+click = range
      locale: true,
      langs: {
        "default": {
          "pagination": {
            "page_size": "",
            "first": t('pagination.first'),
            "first_title": t('pagination.firstTitle'),
            "last": t('pagination.last'),
            "last_title": t('pagination.lastTitle'),
            "prev": t('pagination.prev'),
            "prev_title": t('pagination.prevTitle'),
            "next": t('pagination.next'),
            "next_title": t('pagination.nextTitle'),
          },
        },
      },
      columns: columns,
    });

    // Reset button immediately — previous table may have had rows selected
    updateBulkDeleteButton(0);

    // data = array of selected row data objects; rows = array of Row components
    tabulatorTable.on("rowSelectionChanged", (data, rows) => {
      updateBulkDeleteButton(data.length);
    });

    // Capture the table reference so the setTimeout callback can verify it's
    // still the same Tabulator instance (CO switch destroys/recreates the table
    // — a stale redraw would hit the new table or a destroyed one).
    const _thisTable = tabulatorTable;

    tabulatorTable.on("dataFiltered", function(_filters, rows) {
      // rows is the authoritative list of filtered rows in display order.
      if (_rnRedrawing) { updateRecordSummary(rows.length); return; }
      rowNumMap = new Map(rows.map((row, i) => [row.getData().id, i + 1]));
      setTimeout(() => {
        if (tabulatorTable !== _thisTable) return;  // table swapped
        _rnRedrawing = true;
        try { tabulatorTable.redraw(true); } finally { _rnRedrawing = false; }
      }, 0);
      updateRecordSummary(rows.length);
    });

    tabulatorTable.on("dataSorted", function(_sorters, rows) {
      if (_rnRedrawing) return;
      rowNumMap = new Map(rows.map((row, i) => [row.getData().id, i + 1]));
      setTimeout(() => {
        if (tabulatorTable !== _thisTable) return;
        _rnRedrawing = true;
        try { tabulatorTable.redraw(true); } finally { _rnRedrawing = false; }
      }, 0);
    });

    tabulatorTable.on("pageSizeChanged", (pageSize) => {
      savePrefs(coKey, { pageSize });
    });

    tabulatorTable.on("tableBuilt", () => {

      // Build the initial row-number map only when no filter/search is active.
      // For same-CO reloads where applyTableFilters() was called before tableBuilt fires,
      // the dataFiltered event will build rowNumMap correctly — avoid overwriting it here
      // with getRows('active'), which does not reflect functional filters in Tabulator 5.5.0.
      const hasActiveSearch = !!(document.getElementById('table-search')?.value?.trim());
      if (!hasActiveSearch && activeFilters.length === 0) {
        const initRows = tabulatorTable.getRows();
        rowNumMap = new Map(initRows.map((row, i) => [row.getData().id, i + 1]));
      }
      updateRecordSummary();
      resizeIframe();
      if (hasMorePages) {
        const nextUrl = firstResponse.links?.next || firstResponse.next_page;
        loadRemainingPages(nextUrl, myToken);
      }
    });

    if (columnSelectorTS) {
      columnSelectorTS.destroy();
      columnSelectorTS = null;
    }

    const colSelectEl = document.getElementById('column-selector');
    let colOptionsHtml = '';
    columns.forEach(col => {
      if (col.field !== 'actions' && col.field !== 'custom_rownum') {
        const isSelected = col.visible ? 'selected' : '';
        colOptionsHtml += `<option value="${escapeHtml(col.field)}" ${isSelected}>${escapeHtml(col.title)}</option>`;
      }
    });
    colSelectEl.innerHTML = colOptionsHtml;

    columnSelectorTS = new TomSelect('#column-selector', {
      plugins: ['checkbox_options'],
      hidePlaceholder: false,
      closeAfterSelect: false,
      hideSelected: false,
      render: {
        option: function(data, escape) {
          return `<div class="option"><span>${escape(data.text)}</span></div>`;
        },
        item: function() { return '<span style="display:none"></span>'; },
        no_results: null,
      },
      onInitialize: function() {
        const updatePlaceholder = () => {
          const n = this.getValue().length;
          const input = this.control_input;
          if (input) input.placeholder = n > 0 ? t('table.columnsSelected', { n }) : (t('table.selectColumns') || 'Select columns…');
        };
        this.on('change', updatePlaceholder);
        updatePlaceholder();
      },
      onChange: function(values) {
        if(!tabulatorTable) return;

        const valArray = Array.isArray(values) ? values : (values ? values.split(',') : []);
        const allHidableFields = columns.map(c => c.field).filter(f => f !== 'actions' && f !== 'custom_rownum');

        allHidableFields.forEach(field => {
          if (valArray.includes(field)) {
            tabulatorTable.showColumn(field);
          } else {
            tabulatorTable.hideColumn(field);
          }
        });
        // Persist so the same layout is restored next time this CO loads
        savePrefs(coKey, { visibleColumns: valArray });
      }
    });

    renderFilterBar(columns, coKey);

  } catch (error) {
    console.error("Error loading table data:", error);
    updateLoaderText(t('loader.error.table'));
  }
}

// Fetches all remaining pages silently in memory, then does a single replaceData
// at the end - one render instead of one per page, eliminating UI freezes and flickering.
// token guards against stale loads: if the user switches CO the old load stops at the next check.
async function loadRemainingPages(nextUrl, token) {
  const allRemainingRows = [];
  let currentEndpoint = nextUrl;
  const loadStart = Date.now();
  const loadElapsed = () => Math.floor((Date.now() - loadStart) / 1000);

  // Single source of truth for the "loading more…" summary — used by both the
  // interval tick and per-page completion paths to keep the DOM write consistent.
  // Shows total fetched so far (first page + background) because the Tabulator
  // table still holds only the first page until the final replaceData.
  const renderLoadingSummary = () => {
    const summaryEl = document.getElementById('record-summary');
    if (!summaryEl || !tabulatorTable) return;
    const fetched = tabulatorTable.getData().length + allRemainingRows.length;
    summaryEl.innerHTML = `<strong>${fetched}</strong> ${t('summary.records')} <span style="color:#1f73b7; font-size:12px; font-weight:600;">${t('summary.loadingMore')}</span> <span style="color:#68737d; font-size:12px;">· ${formatElapsed(loadElapsed())}</span> <button type="button" id="btn-stop-bg-load" class="summary-stop-btn">${t('summary.stopLoading')}</button>`;
  };

  // Delegated click: button is re-rendered each tick, so a direct listener would detach.
  const summaryEl = document.getElementById('record-summary');
  const onStopClick = (e) => {
    if (e.target && e.target.id === 'btn-stop-bg-load') _bgLoadStopped = true;
  };
  if (summaryEl) summaryEl.addEventListener('click', onStopClick);

  // Tick every second so the elapsed counter updates even between page fetches
  const timerInterval = setInterval(() => {
    if (!isBackgroundLoading) return;
    renderLoadingSummary();
  }, 1000);

  try {

  while (currentEndpoint) {
    if (currentLoadToken !== token) return;
    if (_bgLoadStopped) break;
    try {
      const response = await zafRequest(currentEndpoint);
      if (currentLoadToken !== token) return;
      if (_bgLoadStopped) break;

      (response.custom_object_records || []).forEach(record => {
        allRemainingRows.push({ id: record.id, name: record.name, external_id: record.external_id, ...record.custom_object_fields });
      });

      renderLoadingSummary();

      currentEndpoint = (response.meta?.has_more && (response.links?.next || response.next_page))
        ? (response.links?.next || response.next_page)
        : null;
    } catch (error) {
      console.error('Error loading background page:', error);
      currentEndpoint = null;
      // Show a visible error in the summary so the user knows loading stopped early
      const errSummaryEl = document.getElementById('record-summary');
      if (errSummaryEl && tabulatorTable) {
        const loaded = tabulatorTable.getData().length + allRemainingRows.length;
        errSummaryEl.innerHTML = `<strong>${loaded}</strong> ${t('summary.records')} <span style="color:#cc3340; font-size:12px; font-weight:600;">${t('summary.loadError')}</span>`;
      }
      showToast(t('toast.recordsTruncated'), 'error');
    }
  }

  if (currentLoadToken !== token) return;
  const wasStopped = _bgLoadStopped;
  isBackgroundLoading = false;
  setLoadingUIState(false);

  if (tabulatorTable && allRemainingRows.length > 0) {
    const existingData = tabulatorTable.getData();
    const currentPage = tabulatorTable.getPage() || 1;
    await tabulatorTable.replaceData([...existingData, ...allRemainingRows]);
    if (currentLoadToken !== token) return;
    if (currentPage > 1) tabulatorTable.setPage(currentPage);
    const hasActiveSearch = !!(document.getElementById('table-search')?.value?.trim());
    if (hasActiveSearch || activeFilters.length > 0) applyTableFilters();
  }

  updateRecordSummary();
  if (wasStopped) showToast(t('toast.bgLoadStopped'), 'warning');

  } finally {
    clearInterval(timerInterval);
    if (summaryEl) summaryEl.removeEventListener('click', onStopClick);
  }
}

// ============================================================
// BULK DELETE (selection-aware)
// ============================================================

// Toggles the toolbar button visibility + updates the count badge based on how
// many rows are currently selected in Tabulator.
function updateBulkDeleteButton(count) {
  if (!btnBulkDelete) return;
  if (count > 0) {
    btnBulkDelete.style.display = '';
    btnBulkDelete.textContent = t('table.bulkDeleteN', { n: count });
  } else {
    btnBulkDelete.style.display = 'none';
    btnBulkDelete.textContent = t('table.bulkDelete');
  }
}

// Shows the bulk-delete confirmation modal.  Unlike single delete we don't run a
// per-record Usage & Impact scan (could be hundreds of records — that's O(n)
// slow API calls); instead we let the user opt-in to propagation which uses the
// existing propagateDeleteLinkedReferences helper once per record.
async function showBulkDeleteModal(selectedRecords) {
  const overlay = document.getElementById('bulk-delete-overlay');
  const modal   = document.getElementById('bulk-delete-modal');
  const n = selectedRecords.length;

  const close = () => { overlay.style.display = 'none'; };

  // Preview list: first 10 names, then "... and X more"
  const preview = selectedRecords.slice(0, 10)
    .map(r => `<li>${escapeHtml(String(r.name || `#${r.id}`))} <span class="badge-id">ID: ${escapeHtml(String(r.id))}</span></li>`)
    .join('');
  const moreCount = n - Math.min(n, 10);

  modal.innerHTML = `
    <h3 style="margin:0 0 8px 0;">${t('bulkDelete.title', { n })}</h3>
    <p style="color:#68737d; font-size:13px; margin:0 0 12px 0;">${t('bulkDelete.description')}</p>
    <ul class="bulk-delete-preview">${preview}${moreCount > 0 ? `<li style="color:#68737d;">${t('bulkDelete.more', { n: moreCount })}</li>` : ''}</ul>

    <label class="delete-propagate-label">
      <input type="checkbox" id="chk-bulk-propagate" />
      ${t('bulkDelete.propagateLabel')}
    </label>
    <p class="delete-propagate-hint">${t('bulkDelete.propagateHint')}</p>

    <div id="bulk-delete-progress" style="display:none; margin-top:14px;">
      <p id="bulk-delete-status" style="color:#68737d; font-size:13px; margin:0 0 8px 0;"></p>
      <div class="progress-container"><div class="progress-bar-determinate" id="bulk-delete-bar" style="width:0%"></div></div>
    </div>

    <div class="modal-footer">
      <div class="modal-footer-actions">
        <button id="bulk-delete-cancel" class="btn btn-secondary">${t('form.cancel')}</button>
        <button id="bulk-delete-confirm" class="btn btn-danger">${t('bulkDelete.confirm', { n })}</button>
      </div>
    </div>
  `;

  const cancelBtn  = document.getElementById('bulk-delete-cancel');
  const confirmBtn = document.getElementById('bulk-delete-confirm');
  cancelBtn.onclick = close;
  overlay.onclick = (e) => { if (e.target === overlay) close(); };

  confirmBtn.onclick = async () => {
    const shouldPropagate = document.getElementById('chk-bulk-propagate').checked;
    confirmBtn.disabled = true;
    cancelBtn.disabled  = true;
    overlay.onclick = null;

    const progressEl = document.getElementById('bulk-delete-progress');
    const statusEl   = document.getElementById('bulk-delete-status');
    const barEl      = document.getElementById('bulk-delete-bar');
    progressEl.style.display = 'block';

    let done = 0;
    let deleted = 0;
    let failed  = 0;
    const failedIds = [];

    // Fetch lookup fields once if propagating (same for all records in the CO)
    const lookupFields = shouldPropagate ? await getLookupFieldsForCurrentCo() : [];

    for (const record of selectedRecords) {
      done++;
      statusEl.textContent = t('bulkDelete.progress', {
        done, total: n, name: String(record.name || record.id).slice(0, 40)
      });
      barEl.style.width = `${Math.round((done / n) * 100)}%`;

      try {
        if (shouldPropagate && lookupFields.length > 0) {
          await propagateDeleteLinkedReferences(record.id, lookupFields, null);
        }
        await zafRequest({
          url: `/api/v2/custom_objects/${currentCoKey}/records/${record.id}`,
          type: 'DELETE'
        });
        if (tabulatorTable) tabulatorTable.deleteRow(record.id);
        deleted++;
      } catch (err) {
        console.error('[bulk-delete] failed for', record.id, err);
        failed++;
        failedIds.push(record.id);
      }
    }

    if (tabulatorTable) {
      // deselectRow() without args clears all — but in Tabulator 5.5.0 the safest
      // form is iterating selected rows directly to avoid version-specific behaviour.
      tabulatorTable.getSelectedRows().forEach(r => r.deselect());
      updateRecordSummary();
    }

    if (failed === 0) {
      showToast(t('bulkDelete.success', { n: deleted }), 'success');
    } else {
      showToast(t('bulkDelete.partial', { deleted, failed }), 'warning');
    }
    close();
  };

  overlay.style.display = 'flex';
}

async function deleteRecord(recordId, recordName) {
  const overlay = document.getElementById('delete-modal-overlay');
  const titleEl = document.getElementById('delete-modal-title');
  const bodyEl = document.getElementById('delete-modal-body');
  const confirmBtn = document.getElementById('delete-modal-confirm');
  const cancelBtn = document.getElementById('delete-modal-cancel');

  const close = () => { overlay.style.display = 'none'; };

  // Show modal in loading state immediately
  titleEl.innerText = t('delete.scanning');
  bodyEl.innerHTML = `<div style="text-align: center; padding: 20px;">
                        <p style="color: #68737d; margin-bottom: 12px; font-size: 13px;">${t('delete.discovering')}</p>
                        <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
                        <p id="pre-scan-timer" style="margin: 8px 0 0 0; font-size: 12px; color: #68737d;"></p>
                      </div>`;
  startPreScanTimer();
  confirmBtn.disabled = true;
  cancelBtn.onclick = close;
  overlay.onclick = (e) => { if (e.target === overlay) close(); };
  overlay.style.display = 'flex';

  // Full scan: relationship fields + triggers/automations/views/SLA, with live progress
  const _zeroRuleCounts = () => ({ triggers: 0, automations: 0, views: 0, sla: 0 });
  let scanResult = { relationshipHtml: '', rulesHtml: '', possibleRulesHtml: '', totalRelationships: 0, totalRules: 0, totalPossible: 0, totalFound: 0, relationshipCounts: [], ruleCounts: _zeroRuleCounts(), possibleCounts: _zeroRuleCounts() };
  try {
    scanResult = await fullReferenceScan(
      recordId, recordName,
      (done, total, label, secs) => renderScanProgress(bodyEl, done, total, label, secs)
    );
  } catch (err) {
    console.warn('Could not complete reference scan', err);
  }

  const { totalFound, totalPossible, relationshipCounts, ruleCounts, possibleCounts, lookupFields = [] } = scanResult;

  // Populate modal based on findings
  if (totalFound > 0 || totalPossible > 0) {
    const relParts = relationshipCounts.map(r => `${r.count} ${escapeHtml(r.label)}`);
    const ruleParts = [];
    if (ruleCounts.triggers    > 0) ruleParts.push(`${ruleCounts.triggers} ${t('rules.triggers')}`);
    if (ruleCounts.automations > 0) ruleParts.push(`${ruleCounts.automations} ${t('rules.automations')}`);
    if (ruleCounts.views       > 0) ruleParts.push(`${ruleCounts.views} ${t('rules.views')}`);
    if (ruleCounts.sla         > 0) ruleParts.push(`${ruleCounts.sla} ${t('rules.sla')}`);

    const possibleParts = [];
    if (possibleCounts.triggers    > 0) possibleParts.push(`${possibleCounts.triggers} ${t('rules.triggers')}`);
    if (possibleCounts.automations > 0) possibleParts.push(`${possibleCounts.automations} ${t('rules.automations')}`);
    if (possibleCounts.views       > 0) possibleParts.push(`${possibleCounts.views} ${t('rules.views')}`);
    if (possibleCounts.sla         > 0) possibleParts.push(`${possibleCounts.sla} ${t('rules.sla')}`);

    const summaryRows = [];
    if (relParts.length > 0)      summaryRows.push(`<tr><td style="color:#68737d; padding:3px 16px 3px 0; white-space:nowrap; font-size:13px;">${t('delete.linkedData')}</td><td style="font-size:13px;">${relParts.join(' &nbsp;·&nbsp; ')}</td></tr>`);
    if (ruleParts.length > 0)     summaryRows.push(`<tr><td style="color:#68737d; padding:3px 16px 3px 0; white-space:nowrap; font-size:13px;">${t('delete.ruleConditions')}</td><td style="font-size:13px;">${ruleParts.join(' &nbsp;·&nbsp; ')}</td></tr>`);
    if (possibleParts.length > 0) summaryRows.push(`<tr><td style="color:#b45309; padding:3px 16px 3px 0; white-space:nowrap; font-size:13px;">${t('delete.possibleMatches')}</td><td style="font-size:13px; color:#b45309;">${possibleParts.join(' &nbsp;·&nbsp; ')}</td></tr>`);

    titleEl.innerText = t('delete.warningTitle', { name: recordName || recordId });
    bodyEl.innerHTML = `
      <div class="delete-warning">
        ${totalFound > 0 ? `<p style="margin:0 0 10px 0;">${t('delete.warningBody')}</p>` : ''}
        <table style="border-collapse:collapse;">${summaryRows.join('')}</table>
      </div>
      <p style="margin: 16px 0 4px 0;"><button class="btn btn-secondary" id="btn-view-usage-impact">${t('delete.viewUsage')}</button></p>
      ${scanResult.totalRelationships > 0 ? `
      <label class="delete-propagate-label">
        <input type="checkbox" id="chk-propagate-delete" />
        ${t('delete.propagateLabel', { n: `${scanResult.totalRelationships}${scanResult.relationshipsHasMore ? '+' : ''}` })}
      </label>
      <p class="delete-propagate-hint">${t('delete.propagateHint')}</p>
      ` : ''}
      <p style="margin-top: 16px; font-weight: 600; color: #2f3941;">${t('delete.confirmQuestion')}</p>
    `;

    document.getElementById('btn-view-usage-impact').addEventListener('click', () => {
      close();
      const rowData = tabulatorTable?.getData().find(r => r.id === recordId);
      if (rowData) showForm(rowData, 'related');
    });
  } else {
    titleEl.innerText = t('delete.confirmTitle');
    bodyEl.innerHTML = `<p>${t('delete.confirmBody', { name: escapeHtml(String(recordName || recordId)) })}</p>`;
  }
  // Reset title if only possible matches were found (no confirmed ones)
  if (totalFound === 0 && totalPossible > 0) {
    titleEl.innerText = t('delete.confirmTitle');
  }

  confirmBtn.disabled = false;
  confirmBtn.onclick = async () => {
    confirmBtn.disabled = true;
    cancelBtn.disabled = true;
    // Block backdrop-click dismissal while work is in flight. The handler set during
    // the scan phase (line above) would otherwise hide the modal mid-propagation
    // while the bulk-update loop keeps running invisibly.
    overlay.onclick = null;

    const shouldPropagate = document.getElementById('chk-propagate-delete')?.checked === true;

    try {
      // Step 1 (optional): clear lookup references on all linked items before deleting
      if (shouldPropagate && lookupFields.length > 0) {
        confirmBtn.textContent = t('delete.propagating');
        bodyEl.innerHTML = `
          <div style="text-align:center; padding:16px 0;">
            <p style="color:#68737d; font-size:13px; margin:0 0 12px 0;" id="propagate-status">${t('delete.propagatingStatus')}</p>
            <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
          </div>`;

        const { totalCleared, totalFailed, errors } = await propagateDeleteLinkedReferences(
          recordId, lookupFields,
          (fieldLabel, done, total) => {
            const statusEl = document.getElementById('propagate-status');
            if (statusEl) statusEl.textContent = t('delete.propagatingField', { field: fieldLabel, done, total });
          }
        );

        if (totalFailed > 0) {
          // Partial failure: warn but continue to delete the record.
          // Dedupe — two CO types may share title_pluralized (e.g. two COs both labelled "Partners").
          const uniqueErrors = [...new Set(errors)];
          showToast(t('delete.propagatePartialError', { n: totalFailed, fields: uniqueErrors.join(', ') }), 'warning');
        }
      }

      // Step 2: delete the record itself
      confirmBtn.textContent = t('delete.deleting');
      await zafRequest({
        url: `/api/v2/custom_objects/${currentCoKey}/records/${recordId}`,
        type: 'DELETE'
      });

      if (shouldPropagate && lookupFields.length > 0) {
        showToast(t('delete.propagateSuccess'), 'success');
      }

      if (tabulatorTable) {
        tabulatorTable.deleteRow(recordId);
        updateRecordSummary();
      }
      close();
    } catch (error) {
      console.error('Error deleting record:', error);
      confirmBtn.disabled = false;
      cancelBtn.disabled = false;
      // Restore backdrop-click dismissal now that nothing is in flight.
      overlay.onclick = (e) => { if (e.target === overlay) close(); };
      confirmBtn.textContent = t('row.delete');
      const actionsEl = document.getElementById('delete-modal-actions');
      const prevErr = actionsEl?.querySelector('.delete-error-msg');
      if (prevErr) prevErr.remove();
      const errEl = document.createElement('p');
      errEl.className = 'delete-error-msg';
      errEl.style.cssText = 'color: #cc3340; font-size: 13px; margin: 0; flex-basis: 100%; text-align: right;';
      errEl.textContent = t('delete.error');
      actionsEl?.prepend(errEl);
    }
  };
}

// ----------------------------------------------------
// SHA256 MODAL
// ----------------------------------------------------
const SHA256_PATTERN = /^[0-9a-f]{64}(_\d+)?$/;

// targetFieldName: the form field [name] to apply the result to.
// Pass null to open in global mode (copy-only, no Apply button).
function openSha256Modal(currentFieldValue, targetFieldName) {
  const overlay = document.getElementById('sha256-modal-overlay');
  const inputEl = document.getElementById('sha256-input');
  const outputEl = document.getElementById('sha256-output');
  const suffixNote = document.getElementById('sha256-suffix-note');
  const applyBtn = document.getElementById('sha256-apply');

  inputEl.value = '';
  outputEl.value = '';

  const parts = (currentFieldValue || '').split('_');
  const hasSuffix = parts.length === 2 && /^\d+$/.test(parts[1]);
  suffixNote.textContent = hasSuffix ? t('sha256.suffixNote', { suffix: parts[1] }) : '';
  suffixNote.style.display = hasSuffix ? 'block' : 'none';

  // Show Apply only when wired to a specific field
  applyBtn.style.display = targetFieldName ? '' : 'none';

  overlay.style.display = 'flex';
  inputEl.focus();

  async function recalculate() {
    const val = inputEl.value;
    if (!val.trim()) { outputEl.value = ''; return; }
    outputEl.value = await computeExternalId(val, currentFieldValue);
  }

  inputEl.oninput = recalculate;

  document.getElementById('sha256-copy-input').onclick = () => {
    if (!inputEl.value) return;
    navigator.clipboard.writeText(inputEl.value).then(() => showToast(t('sha256.copied'), 'success'));
  };

  document.getElementById('sha256-copy-output').onclick = () => {
    if (!outputEl.value) return;
    navigator.clipboard.writeText(outputEl.value).then(() => showToast(t('sha256.copied'), 'success'));
  };

  applyBtn.onclick = () => {
    if (!outputEl.value || !targetFieldName) return;
    const target = document.querySelector(`[name="${targetFieldName}"]`);
    if (target) {
      target.value = outputEl.value;
      formIsDirty = true;
    }
    overlay.style.display = 'none';
    showToast(t('sha256.applied'), 'success');
  };

  document.getElementById('sha256-close').onclick = () => { overlay.style.display = 'none'; };
  overlay.onclick = (e) => { if (e.target === overlay) overlay.style.display = 'none'; };
}

// ----------------------------------------------------
// VIEW 3: DYNAMIC FORM & RELATED RECORDS TAB
// ----------------------------------------------------
async function showForm(existingRecord = null, initialTab = 'details') {
  // Reset stale scan flag: if a previous form's Usage & Impact scan is still running,
  // opening a new form must not block the new tab from loading.
  _relatedScanActive = false;
  updateLoaderText(t('loader.form'));
  switchView('loader');
  
  const isEdit = existingRecord !== null;
  const formTitle = isEdit ? t('form.editTitle', { id: escapeHtml(String(existingRecord.id)) }) : t('form.createTitle', { key: escapeHtml(currentCoKey) });

  try {
    let existingName = '';
    if (isEdit && existingRecord.name !== null && existingRecord.name !== undefined) {
      existingName = existingRecord.name;
    }

    let formHtml = `<h2>${formTitle}</h2>
                    <div id="form-msg"></div>`;

    if (isEdit) {
      formHtml += `
        <div class="tabs">
          <button type="button" class="tab-btn active" id="tab-details">${t('form.tabDetails')}</button>
          <button type="button" class="tab-btn" id="tab-related">${t('form.tabUsage')}</button>
        </div>
      `;
    }

    const extVal = isEdit && existingRecord.external_id ? existingRecord.external_id : '';
    formHtml += `<div id="tab-content-details">
                   <form id="dynamic-form">
                     <input type="hidden" name="record_id" value="${isEdit ? escapeHtml(existingRecord.id) : ''}" />

                     <div class="form-actions-top">
                       <button type="submit" class="btn">${isEdit ? t('form.updateButton') : t('form.saveButton')}</button>
                       <button type="button" class="btn btn-secondary" id="btn-cancel-form">${t('form.cancel')}</button>
                       <button type="button" class="btn btn-secondary label-action-btn sha256-global-btn" id="btn-sha256-global" title="${t('sha256.tooltip')}">${t('sha256.trigger')}</button>
                     </div>

                     <div class="form-group">
                       <label>${t('form.recordName')}</label>
                       <input type="text" name="name" value="${escapeHtml(existingName)}" required />
                     </div>

                     <div class="form-group">
                       <label class="form-label-with-action">
                         ${t('form.externalId')}
                         ${SHA256_PATTERN.test(extVal) ? `<button type="button" class="label-action-btn sha256-field-btn" data-field="external_id" title="${t('sha256.tooltip')}">${t('sha256.trigger')}</button>` : ''}
                       </label>
                       <input type="text" name="external_id" value="${escapeHtml(extVal)}" />
                     </div>`;

    const lookupFieldIds = [];

    // Resolve all lookup field labels in parallel before building HTML
    const lookupLabelMap = {};
    if (isEdit) {
      const lookupFields = currentSchema.filter(f => f.type === 'lookup' && existingRecord[f.key]);
      const labels = await Promise.all(
        lookupFields.map(f => fetchSingleRecordName(f.relationship_target_type, existingRecord[f.key]))
      );
      lookupFields.forEach((f, i) => { lookupLabelMap[f.key] = labels[i]; });
    }

    for (const field of currentSchema) {
      let fieldValue = '';
      if (isEdit && existingRecord[field.key] !== null && existingRecord[field.key] !== undefined) {
        fieldValue = existingRecord[field.key];
      }

      formHtml += `<div class="form-group">`;

      if (field.type === 'text') {
        const showSha = SHA256_PATTERN.test(String(fieldValue));
        formHtml += `<label class="form-label-with-action">
                       ${escapeHtml(field.title)}
                       ${showSha ? `<button type="button" class="label-action-btn sha256-field-btn" data-field="${escapeHtml(field.key)}" title="${t('sha256.tooltip')}">${t('sha256.trigger')}</button>` : ''}
                     </label>
                     <input type="text" name="${escapeHtml(field.key)}" value="${escapeHtml(fieldValue)}" ${field.required ? 'required' : ''} />`;
      }
      else if (field.type === 'textarea') {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <textarea name="${escapeHtml(field.key)}" ${field.required ? 'required' : ''} rows="3">${escapeHtml(fieldValue)}</textarea>`;
      }
      else if (field.type === 'integer') {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <input type="number" step="1" name="${escapeHtml(field.key)}" value="${escapeHtml(fieldValue)}" ${field.required ? 'required' : ''} />`;
      }
      else if (field.type === 'decimal') {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <input type="number" step="any" name="${escapeHtml(field.key)}" value="${escapeHtml(fieldValue)}" ${field.required ? 'required' : ''} />`;
      }
      else if (field.type === 'date') {
        const dateVal = fieldValue ? String(fieldValue).substring(0, 10) : '';
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <input type="date" name="${escapeHtml(field.key)}" value="${escapeHtml(dateVal)}" ${field.required ? 'required' : ''} />`;
      }
      else if (field.type === 'checkbox') {
        const isChecked = (fieldValue === true || fieldValue === 'true') ? 'checked' : '';
        formHtml += `<label>
                       <input type="checkbox" name="${escapeHtml(field.key)}" value="true" ${isChecked} />
                       <span class="checkbox-label">${escapeHtml(field.title)}</span>
                     </label>`;
      }
      else if (field.type === 'dropdown') {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <select name="${escapeHtml(field.key)}" ${field.required ? 'required' : ''}>
                       <option value="">${t('form.selectPlaceholder', { field: escapeHtml(field.title) })}</option>`;
        if (field.custom_field_options) {
          field.custom_field_options.forEach(opt => {
            if (opt.active !== false || fieldValue === opt.value) {
              const selected = (fieldValue === opt.value) ? 'selected' : '';
              formHtml += `<option value="${escapeHtml(opt.value)}" ${selected}>${escapeHtml(opt.name)}</option>`;
            }
          });
        }
        formHtml += `</select>`;
      }
      else if (field.type === 'lookup') {
        formHtml += `<label>${escapeHtml(field.title)}</label>`;
        const selectId = `lookup-${field.key}`;

        const initialLabel = lookupLabelMap[field.key] || t('form.recordFallback', { id: fieldValue });

        lookupFieldIds.push({
           id: selectId,
           targetType: field.relationship_target_type
        });

        formHtml += `<select id="${selectId}" name="${escapeHtml(field.key)}" ${field.required ? 'required' : ''} placeholder="${t('form.lookupSearch', { field: escapeHtml(field.title) })}">`;
        if (fieldValue) {
            formHtml += `<option value="${escapeHtml(fieldValue)}" selected>${escapeHtml(initialLabel)}</option>`;
        } else {
            formHtml += `<option value="">${t('form.lookupPlaceholder')}</option>`;
        }
        formHtml += `</select>`;
      }
      else {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <input type="text" name="${escapeHtml(field.key)}" value="${escapeHtml(fieldValue)}" placeholder="${escapeHtml(field.type)}" />`;
      }
      formHtml += `</div>`;
    }

    formHtml += `</form></div>`; 

    if (isEdit) {
      formHtml += `<div id="tab-content-related" style="display: none;"></div>`;
    }

    // Destroy any TomSelect instances on the current form before replacing the DOM.
    // TomSelect attaches global document-level listeners that are only removed via .destroy().
    views.form.querySelectorAll('select').forEach(el => { if (el.tomselect) el.tomselect.destroy(); });

    views.form.innerHTML = formHtml;
    switchView('form');

    lookupFieldIds.forEach(lookup => {
      new TomSelect(`#${lookup.id}`, {
        valueField: 'id',
        labelField: 'label',
        searchField: 'label',
        maxOptions: 50,
        onChange: () => { formIsDirty = true; },
        load: function(query, callback) {
          if (!query.length) return callback();
          searchLookupData(lookup.targetType, query)
            .then(results => callback(results))
            .catch(() => callback());
        }
      });
    });

    formIsDirty = false;
    // 'input' fires for every mutation (typing, select change, checkbox toggle),
    // so one listener covers all cases — no separate 'change' needed.
    document.getElementById('dynamic-form').addEventListener('input', () => { formIsDirty = true; });

    document.getElementById('btn-cancel-form').addEventListener('click', async () => {
      if (formIsDirty) {
        const leave = await showUnsavedChangesModal();
        if (!leave) return;
      }
      formIsDirty = false;
      switchView('table');
    });
    document.getElementById('dynamic-form').addEventListener('submit', handleFormSubmit);

    // Global SHA256 button — no target field, copy-only mode
    document.getElementById('btn-sha256-global').addEventListener('click', () => {
      openSha256Modal('', null);
    });

    // Inline SHA256 buttons on specific fields (auto-detected by value pattern)
    document.querySelectorAll('.sha256-field-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const fieldName = btn.dataset.field;
        const currentVal = document.querySelector(`[name="${fieldName}"]`)?.value || '';
        openSha256Modal(currentVal, fieldName);
      });
    });

    if (isEdit) {
      document.getElementById('tab-details').addEventListener('click', () => {
        document.getElementById('tab-details').classList.add('active');
        document.getElementById('tab-related').classList.remove('active');
        document.getElementById('tab-content-details').style.display = 'block';
        document.getElementById('tab-content-related').style.display = 'none';
      });

      document.getElementById('tab-related').addEventListener('click', () => {
        document.getElementById('tab-related').classList.add('active');
        document.getElementById('tab-details').classList.remove('active');
        document.getElementById('tab-content-related').style.display = 'block';
        document.getElementById('tab-content-details').style.display = 'none';

        const relatedContent = document.getElementById('tab-content-related');
        if (relatedContent.innerHTML.trim() === '') {
           loadRelatedRecords(existingRecord.id, existingRecord.name);
        }
      });

      if (initialTab === 'related') {
        document.getElementById('tab-related').click();
      }
    }

  } catch (error) {
    views.form.innerHTML = `<p>${t('form.loadError')}</p>`;
    console.error(error);
  }
}

// ----------------------------------------------------
// RELATED RECORDS DISCOVERY ENGINE
// ----------------------------------------------------

// Scans Zendesk to find ALL fields that point to this Custom Object
// Uses Promise.all to run all top-level fetches in parallel, then CO field fetches in parallel
async function getLookupFieldsForCurrentCo() {
  if (cachedLookupFields) return cachedLookupFields;
  const target = `zen:custom_object:${currentCoKey}`;
  const fields = [];

  // Fetch ticket fields, user fields, org fields, and the CO list all in parallel
  const [ticketFields, userFields, orgFields, customObjects] = await Promise.all([
    fetchAllPages('/api/v2/ticket_fields.json', 'ticket_fields').catch(e => { console.warn("Could not load ticket fields", e); showToast(t('toast.fieldsLoadFailed', { entity: 'ticket' }), 'warning'); return []; }),
    fetchAllPages('/api/v2/user_fields.json', 'user_fields').catch(e => { console.warn("Could not load user fields", e); showToast(t('toast.fieldsLoadFailed', { entity: 'user' }), 'warning'); return []; }),
    fetchAllPages('/api/v2/organization_fields.json', 'organization_fields').catch(e => { console.warn("Could not load org fields", e); showToast(t('toast.fieldsLoadFailed', { entity: 'organization' }), 'warning'); return []; }),
    fetchAllPages('/api/v2/custom_objects.json', 'custom_objects').catch(e => { console.warn("Could not load COs", e); showToast(t('toast.fieldsLoadFailed', { entity: 'custom object' }), 'warning'); return []; }),
  ]);

  ticketFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      // key = field.key (string slug used in custom_fields updates), id = numeric field id
      fields.push({ id: f.id, key: f.key, title: f.title, type: 'zen:ticket', label: t('rel.tickets') });
  });
  userFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      fields.push({ id: f.id, key: f.key, title: f.title, type: 'zen:user', label: t('rel.users') });
  });
  orgFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      fields.push({ id: f.id, key: f.key, title: f.title, type: 'zen:organization', label: t('rel.organizations') });
  });

  // Reuse already-fetched user/org fields to populate text-field caches,
  // avoiding duplicate API calls when Usage & Impact runs the text searches.
  if (cachedTextUserFieldKeys === null)
    cachedTextUserFieldKeys = userFields.filter(f => ['text', 'textarea'].includes(f.type)).map(f => f.key);
  if (cachedTextOrgFieldKeys === null)
    cachedTextOrgFieldKeys = orgFields.filter(f => ['text', 'textarea'].includes(f.type)).map(f => f.key);

  // Fetch all CO fields in parallel
  await Promise.all(customObjects.map(async obj => {
    try {
      const coFields = await fetchAllPages(`/api/v2/custom_objects/${obj.key}/fields.json`, 'custom_object_fields');
      coFields.forEach(f => {
        if (f.type === 'lookup' && f.relationship_target_type === target) {
          // Note: Custom object fields use their string 'key' rather than an integer 'id'
          fields.push({ id: f.key, title: f.title, type: `zen:custom_object:${obj.key}`, label: obj.title_pluralized });
        }
      });
    } catch(e) { console.warn(`Could not load fields for CO ${obj.key}`, e); }
  }));

  cachedLookupFields = fields;
  return fields;
}


// Returns true if any condition value contains the normalised record name.
// Requires name to be at least 3 chars to avoid trivial false positives.
function checkConditionsForName(conditions, normalizedName) {
  if (!Array.isArray(conditions) || normalizedName.length < 3) return false;
  return conditions.some(c => {
    const val = normalizeForMatch(c.value);
    return val && val.includes(normalizedName);
  });
}

// Returns true if any condition references one of the given ticket field IDs with the exact record ID.
// Matches both raw numeric ID and "custom_fields_{id}" formats.
function checkConditionsForRecord(conditions, ticketFieldIds, recordId) {
  if (!Array.isArray(conditions)) return false;
  return conditions.some(c => {
    const fieldStr = String(c.field || '');
    return ticketFieldIds.some(id =>
      (fieldStr === String(id) || fieldStr === `custom_fields_${id}`) &&
      String(c.value) === String(recordId)
    );
  });
}

// Fetches triggers, automations, views, and SLA policies in parallel.
// For each item checks BOTH exact lookup-field ID match AND name-based fuzzy match.
// exact matches go into result.triggers/automations/views/sla
// name-based possible matches (not already in exact) go into result.possibleTriggers etc.
async function scanRuleReferences(recordId, ticketFieldIds, recordName, onAdvance) {
  const result = {
    triggers: [], automations: [], views: [], sla: [],
    possibleTriggers: [], possibleAutomations: [], possibleViews: [], possibleSla: [],
  };

  const normalizedName = normalizeForMatch(recordName);
  const nameEnabled = normalizedName.length >= CONFIG.MIN_CHARS_FOR_RULE_MATCH;

  const classify = (conds, exactKey, possibleKey, item) => {
    if (checkConditionsForRecord(conds, ticketFieldIds, recordId)) {
      result[exactKey].push(item);
    } else if (nameEnabled && checkConditionsForName(conds, normalizedName)) {
      result[possibleKey].push(item);
    }
  };

  if (ticketFieldIds.length === 0 && !nameEnabled) {
    onAdvance(t('delete.scanTriggers'));
    onAdvance(t('delete.scanAutomations'));
    onAdvance(t('delete.scanViews'));
    onAdvance(t('delete.scanSla'));
    return result;
  }

  await Promise.all([
    fetchAllPages('/api/v2/triggers', 'triggers')
      .catch(e => { console.warn('Could not load triggers', e); return []; })
      .then(items => {
        items.forEach(item => {
          const conds = [
            ...(item.conditions?.all || []),
            ...(item.conditions?.any || []),
            ...(item.actions         || []), // actions can set lookup fields to a CO record
          ];
          classify(conds, 'triggers', 'possibleTriggers', item);
        });
        onAdvance(t('delete.scanTriggers'));
      }),

    fetchAllPages('/api/v2/automations', 'automations')
      .catch(e => { console.warn('Could not load automations', e); return []; })
      .then(items => {
        items.forEach(item => {
          const conds = [
            ...(item.conditions?.all || []),
            ...(item.conditions?.any || []),
            ...(item.actions         || []), // actions can set lookup fields to a CO record
          ];
          classify(conds, 'automations', 'possibleAutomations', item);
        });
        onAdvance(t('delete.scanAutomations'));
      }),

    fetchAllPages('/api/v2/views', 'views')
      .catch(e => { console.warn('Could not load views', e); return []; })
      .then(items => {
        items.forEach(item => {
          const conds = [...(item.conditions?.all || []), ...(item.conditions?.any || [])];
          classify(conds, 'views', 'possibleViews', item);
        });
        onAdvance(t('delete.scanViews'));
      }),

    fetchAllPages('/api/v2/sla_policies', 'sla_policies')
      .catch(e => { console.warn('Could not load SLA policies', e); return []; })
      .then(items => {
        items.forEach(item => {
          const conds = [...(item.filter?.all || []), ...(item.filter?.any || [])];
          classify(conds, 'sla', 'possibleSla', item);
        });
        onAdvance(t('delete.scanSla'));
      }),
  ]);

  return result;
}

// Builds the HTML block and item count for rule-based references
function buildRulesHtml(ruleResults) {
  let html = '';
  let total = 0;
  const sections = [
    { key: 'triggers',    label: t('rules.triggers')    },
    { key: 'automations', label: t('rules.automations') },
    { key: 'views',       label: t('rules.views')       },
    { key: 'sla',         label: t('rules.sla')         },
  ];
  sections.forEach(({ key, label }) => {
    const items = ruleResults[key];
    if (!items || items.length === 0) return;
    total += items.length;
    html += `<details class="related-section">
               <summary>
                 <span>${escapeHtml(label)} <span class="badge-id">${items.length}</span></span>
                 <span class="section-meta">${t('usage.conditionRef')}</span>
                 <span class="section-toggle">▸</span>
               </summary>
               <ul class="related-list">`;
    items.forEach(item => {
      const name = item.title || item.name || `ID ${item.id}`;
      const ruleUrl = getRuleUrl(key, item.id);
      html += `<li>
                 <span>${linkWrap(name, ruleUrl)}</span>
                 <span class="badge-id">ID: ${escapeHtml(String(item.id))}</span>
               </li>`;
    });
    html += `</ul></details>`;
  });
  return { html, total };
}

// Builds the HTML block for name-based possible matches, visually distinct from confirmed matches.
function buildPossibleRulesHtml(ruleResults) {
  let html = '';
  let total = 0;
  const sections = [
    { key: 'possibleTriggers',    ruleKey: 'triggers',    label: t('rules.triggers')    },
    { key: 'possibleAutomations', ruleKey: 'automations', label: t('rules.automations') },
    { key: 'possibleViews',       ruleKey: 'views',       label: t('rules.views')       },
    { key: 'possibleSla',         ruleKey: 'sla',         label: t('rules.sla')         },
  ];
  sections.forEach(({ key, ruleKey, label }) => {
    const items = ruleResults[key];
    if (!items || items.length === 0) return;
    total += items.length;
    html += `<details class="related-section">
               <summary>
                 <span>${escapeHtml(label)} <span class="badge-id badge-possible">${items.length}</span></span>
                 <span class="section-meta">${t('usage.possibleCondRef')}</span>
                 <span class="section-toggle">▸</span>
               </summary>
               <ul class="related-list">`;
    items.forEach(item => {
      const name = item.title || item.name || `ID ${item.id}`;
      const ruleUrl = getRuleUrl(ruleKey, item.id);
      html += `<li>
                 <span>${linkWrap(name, ruleUrl)}</span>
                 <span class="badge-id">ID: ${escapeHtml(String(item.id))}</span>
               </li>`;
    });
    html += `</ul></details>`;
  });
  if (!html) return { html: '', total: 0 };
  return {
    total,
    html: `<div class="possible-matches-divider">${t('usage.possibleMatches')} <span style="font-weight:normal; font-size:11px;">&nbsp;·&nbsp; ${t('usage.possibleMatchesHint')}</span></div>${html}`,
  };
}

// Maps a Zendesk relationship field type to its payload array key and the
// property holding a display label. Single source of truth for all code paths
// that read /relationship_fields/{id}/{type} responses.
function getRelationshipDataKey(fieldType) {
  if (fieldType === 'zen:ticket')                      return { dataKey: 'tickets',              displayField: 'subject' };
  if (fieldType === 'zen:user')                        return { dataKey: 'users',                displayField: 'name'    };
  if (fieldType === 'zen:organization')                return { dataKey: 'organizations',        displayField: 'name'    };
  if (fieldType && fieldType.startsWith('zen:custom_object:')) return { dataKey: 'custom_object_records', displayField: 'name' };
  return { dataKey: '', displayField: 'name' };
}

// Returns a Zendesk URL for a related data item, or null if not linkable
function getZendeskItemUrl(type, id) {
  if (!zendeskBaseUrl) return null;
  if (type === 'zen:ticket')       return `${zendeskBaseUrl}/agent/tickets/${id}`;
  if (type === 'zen:user')         return `${zendeskBaseUrl}/agent/users/${id}/tickets`;
  if (type === 'zen:organization') return `${zendeskBaseUrl}/agent/organizations/${id}/tickets`;
  return null; // custom object records have no standard URL
}

// Returns a Zendesk admin URL for a rule item, or null if not linkable
function getRuleUrl(key, id) {
  if (!zendeskBaseUrl) return null;
  // Modern Zendesk Admin Center paths (legacy /admin/triggers/ etc. show "Nothing here")
  if (key === 'triggers')    return `${zendeskBaseUrl}/admin/objects-rules/rules/triggers/${id}`;
  if (key === 'automations') return `${zendeskBaseUrl}/admin/objects-rules/rules/automations/${id}`;
  if (key === 'views')       return `${zendeskBaseUrl}/admin/workspaces/agent-workspace/views/${id}`;
  if (key === 'sla')         return `${zendeskBaseUrl}/admin/objects-rules/rules/sla-policies`;
  return null;
}

// Wraps nameText in a link that opens in a new tab, or returns plain text if no URL.
// escapeHtml on url as defense-in-depth: URLs are built from trusted sources
// (Zendesk subdomain + numeric IDs) but attribute escaping is cheap insurance.
function linkWrap(nameText, url) {
  if (!url) return escapeHtml(nameText);
  return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" title="${t('usage.openLink')}">${escapeHtml(nameText)}</a>`;
}

// Runs the full scan (relationship fields + triggers/automations/views/SLA) with live progress.
// onProgress(done, total, label) is called after each step completes.
async function fullReferenceScan(recordId, recordName, onProgress) {
  // Start elapsed time from here so the timer is continuous with the pre-scan phase
  const scanStart = Date.now();
  stopPreScanTimer();

  const fields = await getLookupFieldsForCurrentCo();
  const ticketFieldIds = fields.filter(f => f.type === 'zen:ticket').map(f => f.id);
  const total = fields.length + 4;
  let done = 0;

  // Timer: wrap onProgress to inject elapsed seconds, and tick every second
  let _lastDone = 0, _lastTotal = total, _lastLabel = t('delete.starting');
  const elapsed = () => Math.floor((Date.now() - scanStart) / 1000);
  const progress = (d, t2, label) => {
    _lastDone = d; _lastTotal = t2; _lastLabel = label;
    onProgress(d, t2, label, elapsed());
  };
  const timerInterval = setInterval(() => {
    onProgress(_lastDone, _lastTotal, _lastLabel, elapsed());
  }, 1000);

  progress(0, total, t('delete.starting'));

  let relationshipHtml = '';
  let totalRelationships = 0;
  let relationshipsHasMore = false;   // any field returned meta.has_more = true
  const relationshipCounts = [];

  const [, ruleResults] = await Promise.all([
    // Relationship fields - sequential so progress advances per field
    (async () => {
      for (const field of fields) {
        const endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${recordId}/relationship_fields/${field.id}/${field.type}`;
        try {
          const response = await zafRequest(endpoint);
          const { dataKey, displayField } = getRelationshipDataKey(field.type);
          const records = response[dataKey] || [];
          if (records.length > 0) {
            totalRelationships += records.length;
            const hasMore = response.meta && response.meta.has_more;
            if (hasMore) relationshipsHasMore = true;
            relationshipCounts.push({ label: field.label, count: records.length + (hasMore ? '+' : '') });
            const countBadge = `${records.length}${hasMore ? '+' : ''}`;
            relationshipHtml += `<details class="related-section">
                                   <summary>
                                     <span>${escapeHtml(field.label)} <span class="badge-id">${countBadge}</span></span>
                                     <span class="section-meta">${t('usage.viaField', { title: escapeHtml(field.title) })}</span>
                                     <span class="section-toggle">▸</span>
                                   </summary>
                                   <ul class="related-list">`;
            records.forEach(r => {
              let nameText = r[displayField] || r.title || `Record #${r.id}`;
              if (nameText.trim() === '') nameText = `[No Name] Record #${r.id}`;
              relationshipHtml += `<li>
                                     <span>${linkWrap(nameText, getZendeskItemUrl(field.type, r.id))}</span>
                                     <span class="badge-id">ID: ${escapeHtml(String(r.id))}</span>
                                   </li>`;
            });
            if (hasMore) {
              relationshipHtml += `<li><span style="color:#1f73b7; font-size:12px;">${t('usage.moreRecords')}</span></li>`;
            }
            relationshipHtml += `</ul></details>`;
          }
        } catch (err) {
          console.warn(`Failed to fetch related records for field ${field.id}`, err);
        }
        done++;
        progress(done, total, t('delete.checked', { label: field.label }));
      }
    })(),

    // Rule references - all 4 types in parallel, each advances independently
    scanRuleReferences(recordId, ticketFieldIds, recordName, (label) => {
      done++;
      progress(done, total, label);
    }),
  ]);

  clearInterval(timerInterval);

  const { html: rulesHtml, total: totalRules } = buildRulesHtml(ruleResults);
  const { html: possibleRulesHtml, total: totalPossible } = buildPossibleRulesHtml(ruleResults);
  const ruleCounts = {
    triggers:    ruleResults.triggers.length,
    automations: ruleResults.automations.length,
    views:       ruleResults.views.length,
    sla:         ruleResults.sla.length,
  };
  const possibleCounts = {
    triggers:    ruleResults.possibleTriggers.length,
    automations: ruleResults.possibleAutomations.length,
    views:       ruleResults.possibleViews.length,
    sla:         ruleResults.possibleSla.length,
  };
  return { relationshipHtml, rulesHtml, possibleRulesHtml, totalRelationships, relationshipsHasMore, totalRules, totalPossible, totalFound: totalRelationships + totalRules, relationshipCounts, ruleCounts, possibleCounts, lookupFields: fields };
}

// ============================================================
// DELETE PROPAGATION HELPERS
// ============================================================

// Fetches ALL item IDs linked to recordId via a specific relationship field,
// handling Zendesk cursor pagination.  Returns array of numeric/string IDs.
async function fetchLinkedItemIds(recordId, field) {
  const ids = [];
  const { dataKey } = getRelationshipDataKey(field.type);
  if (!dataKey) return ids;

  let endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${recordId}/relationship_fields/${field.id}/${field.type}`;
  while (endpoint) {
    try {
      const resp = await zafRequest(endpoint);
      (resp[dataKey] || []).forEach(r => ids.push(r.id));
      endpoint = (resp.meta?.has_more && resp.links?.next) ? resp.links.next : null;
    } catch (e) {
      console.warn('[propagate] fetchLinkedItemIds failed', field.id, e);
      endpoint = null;
    }
  }
  return ids;
}

// Waits for a Zendesk async job (returned by *_many bulk endpoints) to finish.
// Polls /api/v2/job_statuses/{id}.json until status is completed/failed/killed.
// Times out after ~30s so the user isn't blocked forever on a stuck job.
async function waitForJobStatus(jobUrl, maxWaitMs = 30000) {
  if (!jobUrl) return { status: 'unknown' };
  // Zendesk bulk update endpoints return absolute URLs in job_status.url
  // (e.g. https://account.zendesk.com/api/v2/job_statuses/xxx.json).
  // ZAF client.request requires a relative path, so strip the origin.
  let pollPath = jobUrl;
  try {
    const parsed = new URL(jobUrl);
    pollPath = parsed.pathname + parsed.search;
  } catch (_) { /* jobUrl was already relative — use as-is */ }

  const start = Date.now();
  let delay = 500;
  while (Date.now() - start < maxWaitMs) {
    try {
      const resp = await zafRequest({ url: pollPath, type: 'GET' });
      const status = resp.job_status?.status || resp.status;
      if (status === 'completed' || status === 'failed' || status === 'killed') {
        return resp.job_status || resp;
      }
    } catch (e) {
      console.warn('[propagate] job_status poll failed', e);
      return { status: 'unknown' };
    }
    await new Promise(r => setTimeout(r, delay));
    delay = Math.min(delay * 1.5, 3000);
  }
  return { status: 'timeout' };
}

// Nulls out a lookup field on a batch of items using the Zendesk bulk-update APIs.
// Returns { cleared, failed } counts.
//
// Bulk endpoints (tickets/users/organizations update_many) are ASYNCHRONOUS on Zendesk:
// they return 202 Accepted with a job_status URL. We poll that URL until the job is
// complete so the caller knows the updates are actually applied before proceeding
// with the DELETE. Without the poll, we'd have a race window where the record is
// deleted while lookup fields still reference it.
async function clearFieldOnItems(field, ids, onProgress) {
  const BULK = 100;
  let cleared = 0;
  let failed  = 0;

  const runBulk = async (url, payload) => {
    const resp = await zafRequest({
      url, type: 'PUT', contentType: 'application/json', data: JSON.stringify(payload)
    });
    const jobUrl = resp.job_status?.url;
    const result = await waitForJobStatus(jobUrl);
    return result;
  };

  for (let i = 0; i < ids.length; i += BULK) {
    const chunk = ids.slice(i, i + BULK);
    if (onProgress) onProgress(Math.min(i + BULK, ids.length), ids.length);

    // If the Zendesk bulk job didn't complete (timeout/unknown), we can't tell which
    // items succeeded. Count the whole chunk as failed so the user sees a warning
    // rather than a silent "cleared" that may be incorrect.
    const countJobOutcome = (job) => {
      if (job?.status === 'timeout' || job?.status === 'unknown' || !job) {
        return { ok: 0, fail: chunk.length };
      }
      const jobFailed = (job.results || []).filter(r => r.status !== 'Updated' && r.success !== true).length;
      return { ok: chunk.length - jobFailed, fail: jobFailed };
    };

    try {
      if (field.type === 'zen:ticket') {
        // Ticket custom fields use numeric field id
        const ticketsPayload = chunk.map(id => ({
          id, custom_fields: [{ id: field.id, value: null }]
        }));
        const job = await runBulk('/api/v2/tickets/update_many.json', { tickets: ticketsPayload });
        const { ok, fail } = countJobOutcome(job);
        cleared += ok; failed += fail;

      } else if (field.type === 'zen:user') {
        // User custom fields use string key
        const usersPayload = chunk.map(id => ({
          id, user_fields: { [field.key]: null }
        }));
        const job = await runBulk('/api/v2/users/update_many.json', { users: usersPayload });
        const { ok, fail } = countJobOutcome(job);
        cleared += ok; failed += fail;

      } else if (field.type === 'zen:organization') {
        // Org custom fields use string key
        const orgsPayload = chunk.map(id => ({
          id, organization_fields: { [field.key]: null }
        }));
        const job = await runBulk('/api/v2/organizations/update_many.json', { organizations: orgsPayload });
        const { ok, fail } = countJobOutcome(job);
        cleared += ok; failed += fail;

      } else if (field.type.startsWith('zen:custom_object:')) {
        // CO records must be updated one at a time (no bulk endpoint).
        // Promise.allSettled lets partial failures coexist without throwing,
        // so we can count successes and failures explicitly from the results.
        const coKey = field.type.replace('zen:custom_object:', '');
        const results = await Promise.allSettled(chunk.map(id =>
          zafRequest({
            url: `/api/v2/custom_objects/${coKey}/records/${id}`,
            type: 'PATCH',
            contentType: 'application/json',
            data: JSON.stringify({ custom_object_record: { custom_object_fields: { [field.id]: null } } })
          })
        ));
        const ok = results.filter(r => r.status === 'fulfilled').length;
        cleared += ok;
        failed  += chunk.length - ok;
        results.forEach((r, idx) => {
          if (r.status === 'rejected') console.warn('[propagate] CO record update failed', chunk[idx], r.reason);
        });
      }
    } catch (e) {
      console.warn('[propagate] bulk update failed for field', field.id, e);
      failed += chunk.length;
    }
  }
  return { cleared, failed };
}

// Orchestrator: for each lookup field that has linked items, fetch all item IDs
// and clear the field.  Calls onProgress(fieldLabel, clearedSoFar, total) after each field.
// Returns { totalCleared, totalFailed, errors[] }.
async function propagateDeleteLinkedReferences(recordId, lookupFields, onProgress) {
  let totalCleared = 0;
  let totalFailed  = 0;
  const errors     = [];

  for (const field of lookupFields) {
    let ids;
    try {
      ids = await fetchLinkedItemIds(recordId, field);
    } catch (e) {
      errors.push(field.label);
      continue;
    }
    if (ids.length === 0) continue;

    const { cleared, failed } = await clearFieldOnItems(field, ids, (done, total) => {
      if (onProgress) onProgress(field.label, done, total);
    });
    totalCleared += cleared;
    totalFailed  += failed;
    if (failed > 0) errors.push(field.label);
  }
  return { totalCleared, totalFailed, errors };
}

// ============================================================

let _relatedScanActive = false;

async function loadRelatedRecords(recordId, recordName) {
  if (_relatedScanActive) return; // prevent concurrent scans
  _relatedScanActive = true;

  const container = document.getElementById('tab-content-related');

  // Show indeterminate bar while fields are discovered (instant if cached, slower on first call)
  container.innerHTML = `<div style="text-align: center; padding: 40px;">
                           <p style="color: #68737d; margin-bottom: 15px; font-size: 13px;">${t('usage.discovering')}</p>
                           <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
                           <p id="pre-scan-timer" style="margin: 8px 0 0 0; font-size: 12px; color: #68737d;"></p>
                         </div>`;
  startPreScanTimer();

  try {
    const { relationshipHtml, rulesHtml, possibleRulesHtml } = await fullReferenceScan(
      recordId, recordName,
      (done, total, label, secs) => renderScanProgress(container, done, total, label, secs)
    );

    // Text-field search across tickets, users and organizations — all three run in parallel.
    // Only runs when the record name produces a non-empty phrase with at least
    // MIN_CHARS_FOR_TEXT_SEARCH meaningful characters after sanitization.
    let ticketTextHtml = '', userTextHtml = '', orgTextHtml = '';
    const phrase = sanitizeSearchPhrase(recordName || '');
    const phraseNorm = normalizeForMatch(phrase);
    if (phrase.length >= CONFIG.MIN_CHARS_FOR_TEXT_SEARCH && phraseNorm.length >= CONFIG.MIN_CHARS_FOR_TEXT_SEARCH) {

      // Fetch field lists in parallel, then fire all three searches in parallel
      const [ticketFieldIds, userFieldKeys, orgFieldKeys] = await Promise.all([
        getTextTicketFieldIds(), getTextUserFieldKeys(), getTextOrgFieldKeys(),
      ]);

      await Promise.all([
        // Tickets
        (async () => {
          const ids = ticketFieldIds.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
          if (!ids.length) return;
          const q = ids.map(id => `custom_fields_${id}:"${phrase}"`).join(' OR ');
          try {
            const resp = await zafRequest({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:ticket (${q})`)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) ticketTextHtml = buildEntitySearchHtml(items, t('reverseLookup.ticketFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/tickets/${item.id}` : null, item => item.subject);
          } catch (e) {
            const status = e?.status ?? e?.statusCode;
            console.warn(`Usage & Impact ticket text search failed (HTTP ${status}):`, e);
            // 403/404 = permission or Search API not enabled — don't toast, just skip silently.
            // Other errors (500, network) = warn the user.
            if (status !== 403 && status !== 404) showToast(t('toast.usageTicketFailed'), 'warning');
          }
        })(),
        // Users
        (async () => {
          const keys = userFieldKeys.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
          if (!keys.length) return;
          const q = keys.map(k => `user_fields.${k}:"${phrase}"`).join(' OR ');
          try {
            const resp = await zafRequest({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:user (${q})`)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) userTextHtml = buildEntitySearchHtml(items, t('reverseLookup.userFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/users/${item.id}/tickets` : null, item => item.name || item.email);
          } catch (e) {
            const status = e?.status ?? e?.statusCode;
            console.warn(`Usage & Impact user text search failed (HTTP ${status}):`, e);
            if (status !== 403 && status !== 404) showToast(t('toast.usageUserFailed'), 'warning');
          }
        })(),
        // Organizations
        (async () => {
          const keys = orgFieldKeys.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
          if (!keys.length) return;
          const q = keys.map(k => `organization_fields.${k}:"${phrase}"`).join(' OR ');
          try {
            const resp = await zafRequest({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:organization (${q})`)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) orgTextHtml = buildEntitySearchHtml(items, t('reverseLookup.orgFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/organizations/${item.id}/tickets` : null, item => item.name);
          } catch (e) {
            const status = e?.status ?? e?.statusCode;
            console.warn(`Usage & Impact org text search failed (HTTP ${status}):`, e);
            if (status !== 403 && status !== 404) showToast(t('toast.usageOrgFailed'), 'warning');
          }
        })(),
      ]);
    }

    const confirmedHtml = relationshipHtml + rulesHtml;
    const extraHtml     = ticketTextHtml + userTextHtml + orgTextHtml;
    container.innerHTML = (confirmedHtml || possibleRulesHtml || extraHtml)
      ? (confirmedHtml + possibleRulesHtml + extraHtml)
      : `<p style="color: #68737d; padding: 20px;">${t('usage.noItems')}</p>`;

    const refreshBtn = document.createElement('button');
    refreshBtn.className = 'btn btn-secondary';
    refreshBtn.style.cssText = 'margin: 12px 0 4px 0; font-size: 12px;';
    refreshBtn.textContent = t('usage.refresh');
    refreshBtn.onclick = () => { _relatedScanActive = false; loadRelatedRecords(recordId, recordName); };
    container.appendChild(refreshBtn);
  } finally {
    _relatedScanActive = false;
  }
}

async function handleFormSubmit(event) {
  event.preventDefault();
  const formMsg = document.getElementById('form-msg');
  const submitBtn = event.target.querySelector('button[type="submit"]');
  
  formMsg.innerHTML = `<span style='color: #1f73b7; font-weight: bold;'>${t('form.saving')}</span>`;
  submitBtn.disabled = true;

  const formData = new FormData(event.target);
  const customObjectFields = {};
  
  let recordName = formData.get('name') || "New Record";
  let recordId = formData.get('record_id');
  // external_id is a native top-level field on CO records (not part of the schema).
  // Empty string must be sent as null so Zendesk clears it instead of storing "".
  const externalIdRaw = formData.get('external_id');
  const externalId = (externalIdRaw === null || externalIdRaw === '') ? null : externalIdRaw;

  currentSchema.forEach(field => {
    if (field.type === 'checkbox') {
      const checkboxEl = event.target.querySelector(`input[name="${field.key}"]`);
      customObjectFields[field.key] = checkboxEl ? checkboxEl.checked : false;
    } else {
      const val = formData.get(field.key);
      if (val !== null) {
        customObjectFields[field.key] = val;
      }
    }
  });

  const payload = {
    custom_object_record: {
      name: recordName,
      external_id: externalId,
      custom_object_fields: customObjectFields
    }
  };

  try {
    const isEdit = recordId !== '';
    const url = isEdit
      ? `/api/v2/custom_objects/${currentCoKey}/records/${recordId}`
      : `/api/v2/custom_objects/${currentCoKey}/records`;
    const method = isEdit ? 'PATCH' : 'POST';

    const result = await zafRequest({
      url: url,
      type: method,
      contentType: 'application/json',
      data: JSON.stringify(payload)
    });

    if (tabulatorTable) {
      const record = result.custom_object_record;
      const rowData = { id: record.id, name: record.name, external_id: record.external_id, ...record.custom_object_fields };
      if (isEdit) {
        tabulatorTable.updateRow(record.id, rowData);
      } else {
        tabulatorTable.addRow(rowData, true);
      }
      formIsDirty = false;
      updateRecordSummary();
      switchView('table');
    } else {
      formIsDirty = false;
      await loadTable(currentCoKey);
    }

  } catch (error) {
    formMsg.innerHTML = `<span style='color: red;'>${t('form.saveError')}</span>`;
    submitBtn.disabled = false;
    console.error("Save Error:", error);
  }
}

// ----------------------------------------------------
// ADVANCED FILTER
// ----------------------------------------------------

// Operators that don't require a value input
const NO_VALUE_OPS = new Set(['empty', 'notempty', 'true', 'false']);

// Evaluates a single filter condition against a cell value
function evaluateFilter(cellValue, operator, pattern) {
  const strVal = String(cellValue ?? '').trim();
  const isEmpty = cellValue === null || cellValue === undefined || strVal === '';

  switch (operator) {
    case 'empty':    return isEmpty;
    case 'notempty': return !isEmpty;
    case 'true':     return cellValue === true || strVal.toLowerCase() === 'true' || strVal === '1';
    case 'false':    return cellValue === false || strVal.toLowerCase() === 'false' || strVal === '0' || isEmpty;
    case 'gt':
    case 'lt':
    case 'gte':
    case 'lte': {
      const numA = parseFloat(cellValue);
      const numB = parseFloat(pattern);
      // Prefer numeric comparison; fall back to lexicographic (handles ISO dates)
      const a = !isNaN(numA) && !isNaN(numB) ? numA : strVal;
      const b = !isNaN(numA) && !isNaN(numB) ? numB : String(pattern);
      if (operator === 'gt')  return a > b;
      if (operator === 'lt')  return a < b;
      if (operator === 'gte') return a >= b;
      if (operator === 'lte') return a <= b;
      return false;
    }
    case 'eq':
    case 'neq':
    default: {
      // Wildcard matching: *suffix, prefix*, *contains*, or exact
      if (!pattern) return true;
      const val = strVal.toLowerCase();
      const pat = String(pattern).toLowerCase();
      const startsWild = pat.startsWith('*');
      const endsWild   = pat.endsWith('*');
      const core = pat.replace(/^\*|\*$/g, '');
      let match;
      if (startsWild && endsWild) { match = val.includes(core); }
      else if (startsWild)        { match = val.endsWith(core); }
      else if (endsWild)          { match = val.startsWith(core); }
      else                        { match = val === pat; }
      return operator === 'neq' ? !match : match;
    }
  }
}

// Unified filter: combines global search (always AND) + advanced conditions (AND or OR)
function applyTableFilters() {
  if (!tabulatorTable) return;
  // While background loading is in progress only the first page is in the table.
  // Filtering now would show partial/misleading results. Skip and let
  // loadRemainingPages re-apply the filter once the full dataset is available.
  if (isBackgroundLoading) return;
  const searchTerm = (document.getElementById('table-search')?.value || '').trim();
  const logic = document.querySelector('input[name="filter-logic"]:checked')?.value || 'and';

  // Always use setFilter(fn): clearFilter() does not reliably clear a previously
  // set functional filter in Tabulator 5, leaving rows stuck in the filtered state.
  // When nothing is active the function returns true for every row (= show all).
  tabulatorTable.setFilter(function(data) {
    if (searchTerm && !customFilter(data, searchTerm)) return false;
    if (activeFilters.length === 0) return true;
    if (logic === 'or') {
      return activeFilters.some(f => evaluateFilter(data[f.field], f.operator, f.value));
    }
    return activeFilters.every(f => evaluateFilter(data[f.field], f.operator, f.value));
  });
}

// Initialises the filter bar for a freshly loaded table
function renderFilterBar(columns, coKey) {
  filterColumns = columns.filter(c => c.field !== 'actions' && c.field !== 'custom_rownum');

  if (coKey !== lastFilterCoKey) {
    // Different CO: clear in-memory filter state, then try to rehydrate from prefs
    activeFilters = [];
    document.getElementById('filter-rows').innerHTML = '';
    lastFilterCoKey = coKey;

    const savedFilters = loadPrefs(coKey)?.filters;
    if (savedFilters?.rows?.length > 0) {
      // Restore the logic radio
      const logicRadio = document.querySelector(`input[name="filter-logic"][value="${savedFilters.logic || 'and'}"]`);
      if (logicRadio) logicRadio.checked = true;
      // Rebuild each row from saved values — validate field still exists in current schema
      const validFields = new Set(filterColumns.map(c => c.field));
      savedFilters.rows.filter(r => validFields.has(r.field)).forEach(r => addFilterRow(r));
      collectFiltersFromDOM();
      applyTableFilters();
    }
    updateFilterBadge();
  } else {
    // Same CO reloaded (after save/delete): re-apply existing filters to the new table instance
    applyTableFilters();
  }

  document.getElementById('btn-add-filter-row').onclick = () => addFilterRow();

  document.getElementById('btn-apply-filters').onclick = () => {
    collectFiltersFromDOM();
    applyTableFilters();
    updateFilterBadge();
    persistActiveFilters(coKey);
  };

  document.getElementById('btn-clear-filters').onclick = () => {
    activeFilters = [];
    document.getElementById('filter-rows').innerHTML = '';
    applyTableFilters();
    updateFilterBadge();
    persistActiveFilters(coKey);
  };
}

// Serialises the current filter state (logic radio + activeFilters) into prefs,
// so re-opening the same CO restores the same conditions.
function persistActiveFilters(coKey) {
  if (!coKey) return;
  const logic = document.querySelector('input[name="filter-logic"]:checked')?.value || 'and';
  savePrefs(coKey, { filters: { logic, rows: activeFilters.slice() } });
}

function addFilterRow(initial) {
  const colOptions = filterColumns.map(c =>
    `<option value="${escapeHtml(c.field)}">${escapeHtml(c.title)}</option>`
  ).join('');

  const rowEl = document.createElement('div');
  rowEl.className = 'filter-row';
  rowEl.innerHTML = `
    <select class="filter-field">${colOptions}</select>
    <select class="filter-operator">
      <option value="eq">${t('op.eq')}</option>
      <option value="neq">${t('op.neq')}</option>
      <option value="empty">${t('op.empty')}</option>
      <option value="notempty">${t('op.notempty')}</option>
      <option value="true">${t('op.true')}</option>
      <option value="false">${t('op.false')}</option>
      <option value="gt">${t('op.gt')}</option>
      <option value="lt">${t('op.lt')}</option>
      <option value="gte">${t('op.gte')}</option>
      <option value="lte">${t('op.lte')}</option>
    </select>
    <input type="text" class="filter-value" placeholder="${t('filter.valuePlaceholder')}" />
    <button type="button" class="filter-row-remove" title="${t('filter.rowRemove')}">×</button>
  `;

  const fieldEl    = rowEl.querySelector('.filter-field');
  const operatorEl = rowEl.querySelector('.filter-operator');
  const valueEl    = rowEl.querySelector('.filter-value');

  // Rehydrate if initial values supplied (filter-bar restore from prefs)
  if (initial) {
    if (initial.field)    fieldEl.value    = initial.field;
    if (initial.operator) operatorEl.value = initial.operator;
    if (initial.value)    valueEl.value    = initial.value;
  }

  operatorEl.addEventListener('change', () => {
    valueEl.style.visibility = NO_VALUE_OPS.has(operatorEl.value) ? 'hidden' : 'visible';
  });
  // Apply initial no-value-op visibility rule
  if (NO_VALUE_OPS.has(operatorEl.value)) valueEl.style.visibility = 'hidden';

  rowEl.querySelector('.filter-row-remove').onclick = () => {
    rowEl.remove();
    collectFiltersFromDOM();
    applyTableFilters();
    updateFilterBadge();
    persistActiveFilters(lastFilterCoKey);
  };
  document.getElementById('filter-rows').appendChild(rowEl);
}

function collectFiltersFromDOM() {
  activeFilters = [];
  document.querySelectorAll('.filter-row').forEach(row => {
    const field    = row.querySelector('.filter-field').value;
    const operator = row.querySelector('.filter-operator').value;
    const value    = row.querySelector('.filter-value').value.trim();
    // No-value operators are valid without a value; others require one
    if (field && (NO_VALUE_OPS.has(operator) || value)) {
      activeFilters.push({ field, operator, value });
    }
  });
}

function updateFilterBadge() {
  const btn = document.getElementById('btn-advanced-filter');
  if (!btn) return;
  const existing = btn.querySelector('.filter-active-badge');
  if (existing) existing.remove();
  if (activeFilters.length > 0) {
    const badge = document.createElement('span');
    badge.className = 'filter-active-badge';
    badge.innerText = activeFilters.length;
    btn.appendChild(badge);
  }
}

// ----------------------------------------------------
// ----------------------------------------------------
// REVERSE LOOKUP
// ----------------------------------------------------

// Well-known standard Zendesk ticket field names
const STANDARD_FIELD_NAMES = {
  'subject': 'Subject', 'status': 'Status', 'priority': 'Priority',
  'type': 'Type', 'group_id': 'Group', 'assignee_id': 'Assignee',
  'requester_id': 'Requester', 'organization_id': 'Organization',
  'tags': 'Tags', 'description': 'Description',
  'via_id': 'Channel', 'ticket_form_id': 'Ticket Form',
  'custom_status_id': 'Ticket Status', 'brand_id': 'Brand',
};

// Formats a Zendesk condition object into a readable tooltip string.
// fieldMap (optional): Map<"custom_fields_ID" → "Field Title"> built from the ticket fields API.
// e.g. { field: "custom_fields_12345", operator: "contains", value: "Ar Cond" }
//   → 'Customer Field contains "Ar Cond"'
function formatConditionForTooltip(cond, fieldMap) {
  if (!cond) return '';
  const ops = {
    'is': 'is', 'is_not': 'is not', 'contains': 'contains',
    'not_contains': 'does not contain', 'starts_with': 'starts with',
    'ends_with': 'ends with', 'includes': 'includes', 'excludes': 'excludes',
    'greater_than': '>', 'less_than': '<',
    'greater_or_equal_to': '>=', 'less_or_equal_to': '<=',
    'before': 'before', 'after': 'after',
  };
  const op    = ops[cond.operator] || cond.operator || '';
  const val   = cond.value != null ? `"${String(cond.value)}"` : '';
  const raw   = cond.field || '';
  const label = (fieldMap && fieldMap.get(raw))
    || STANDARD_FIELD_NAMES[raw]
    || raw;
  return `${label} ${op} ${val}`.trim();
}

// Builds the grouped results HTML for the Reverse Lookup scan.
function buildReverseLookupResultsHtml(results) {
  if (!results || results.size === 0) return '';
  let html = '';
  results.forEach(({ record, items }) => {
    const exactCount   = items.filter(i => !i.isNameMatch).length;
    const nameCount    = items.filter(i =>  i.isNameMatch).length;
    const metaParts    = [];
    if (exactCount > 0) metaParts.push(`${exactCount} ${t('reverseLookup.exactMatch')}`);
    if (nameCount  > 0) metaParts.push(`${nameCount} ${t('reverseLookup.nameMatch')}`);

    html += `<details class="related-section">
               <summary>
                 <span><a href="#" class="rl-record-link" data-rl-record-id="${escapeHtml(String(record.id))}">${escapeHtml(record.name || `ID ${record.id}`)}</a> <span class="badge-id">${items.length}</span></span>
                 <span class="section-meta">${metaParts.join(' · ')}</span>
                 <span class="section-toggle">▸</span>
               </summary>
               <ul class="related-list">`;
    items.forEach(item => {
      const ruleLink   = linkWrap(item.ruleLabel, item.ruleUrl);
      const matchBadge = item.isNameMatch
        ? ` <span class="badge-id badge-possible" title="${t('reverseLookup.nameMatch')}">~</span>`
        : '';
      html += `<li>
                 <span>${escapeHtml(item.typeLabel)}: ${ruleLink}${matchBadge}</span>
                 <span class="badge-id">ID: ${escapeHtml(String(item.ruleId))}</span>
               </li>`;
    });
    html += `</ul></details>`;
  });
  return html;
}

// Returns IDs of ticket fields with type text or textarea (cached after first call).
async function getTextTicketFieldIds() {
  if (cachedTextTicketFieldIds !== null) return cachedTextTicketFieldIds;
  try {
    const fields = await fetchAllPages('/api/v2/ticket_fields.json', 'ticket_fields');
    cachedTextTicketFieldIds = fields
      .filter(f => ['text', 'textarea'].includes(f.type))
      .map(f => f.id);
  } catch (e) {
    console.warn('Could not fetch text ticket field IDs:', e);
    cachedTextTicketFieldIds = [];
  }
  return cachedTextTicketFieldIds;
}

// Returns keys of user fields with type text or textarea (cached after first call).
async function getTextUserFieldKeys() {
  if (cachedTextUserFieldKeys !== null) return cachedTextUserFieldKeys;
  try {
    const fields = await fetchAllPages('/api/v2/user_fields.json', 'user_fields');
    cachedTextUserFieldKeys = fields
      .filter(f => ['text', 'textarea'].includes(f.type))
      .map(f => f.key);
  } catch (e) {
    console.warn('Could not fetch text user field keys:', e);
    cachedTextUserFieldKeys = [];
  }
  return cachedTextUserFieldKeys;
}

// Returns keys of organization fields with type text or textarea (cached after first call).
async function getTextOrgFieldKeys() {
  if (cachedTextOrgFieldKeys !== null) return cachedTextOrgFieldKeys;
  try {
    const fields = await fetchAllPages('/api/v2/organization_fields.json', 'organization_fields');
    cachedTextOrgFieldKeys = fields
      .filter(f => ['text', 'textarea'].includes(f.type))
      .map(f => f.key);
  } catch (e) {
    console.warn('Could not fetch text org field keys:', e);
    cachedTextOrgFieldKeys = [];
  }
  return cachedTextOrgFieldKeys;
}

// Generic: builds a collapsible possible-match section for any entity type.
// urlFn(item) → URL string or null; labelFn(item) → display string.
function buildEntitySearchHtml(items, sectionLabel, urlFn, labelFn) {
  const hasMore = items.length >= CONFIG.SEARCH_RESULTS_PAGE_SIZE;
  let html = `<details class="related-section">
    <summary>
      <span>${escapeHtml(sectionLabel)} <span class="badge-id badge-possible">${items.length}${hasMore ? '+' : ''}</span></span>
      <span class="section-meta">${escapeHtml(t('usage.possibleCondRef'))}</span>
      <span class="section-toggle">▸</span>
    </summary>
    <ul class="related-list">`;
  items.forEach(item => {
    const label = escapeHtml(labelFn(item) || `#${item.id}`);
    const url   = urlFn(item);
    // escapeHtml on the url as defense-in-depth: Zendesk IDs are numeric in practice,
    // but the url itself is built as a template string and could in theory embed
    // attacker-controlled content if the API ever returned malformed IDs.
    const link  = url
      ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" title="${t('usage.openLink')}">${label}</a>`
      : label;
    html += `<li>
      <span>${link}</span>
      <span class="badge-id">ID: ${escapeHtml(String(item.id))}</span>
    </li>`;
  });
  html += `</ul></details>`;
  return html;
}

// for exact record ID matches and optional name-based matches.
async function runReverseLookup(selectedTypes, includeNames, useFilteredOnly, wholeWord, modal, showSelection) {
  _rlCancelled = false;

  modal.innerHTML = `
    <h3 style="margin:0 0 16px 0; font-size:16px;">${t('reverseLookup.title')}</h3>
    <div style="text-align:center; padding:20px 0;">
      <p id="rl-status" style="color:#68737d; margin:0 0 12px 0; font-size:13px;">${t('reverseLookup.scanning')}</p>
      <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
      <p id="rl-timer" style="margin:8px 0 0 0; font-size:12px; color:#68737d;"></p>
      <button id="rl-btn-stop" class="btn btn-secondary" style="margin-top:14px; font-size:12px;">${t('reverseLookup.stop')}</button>
    </div>`;

  document.getElementById('rl-btn-stop').onclick = () => {
    _rlCancelled = true;
    const btn = document.getElementById('rl-btn-stop');
    if (btn) { btn.disabled = true; btn.textContent = t('reverseLookup.stopping'); }
  };

  let secs = 0;
  const timerInterval = setInterval(() => {
    secs++;
    const el = document.getElementById('rl-timer');
    if (el) el.textContent = formatElapsed(secs);
  }, 1000);

  const updateStatus = (msg) => {
    const el = document.getElementById('rl-status');
    if (el) el.textContent = msg;
  };

  try {
    const allRecords   = tabulatorTable ? tabulatorTable.getData() : [];
    // When "visible records only" is selected, restrict to the IDs in rowNumMap
    // (built from dataFiltered rows — the most accurate reflection of active filter/search).
    let tableRecords = allRecords;
    if (useFilteredOnly && rowNumMap && rowNumMap.size > 0) {
      const filteredIds = new Set(rowNumMap.keys());
      tableRecords = allRecords.filter(r => filteredIds.has(r.id));
    }
    const recordById  = new Map(tableRecords.map(r => [String(r.id), r]));
    // Normalize each name once, then filter — avoids calling normalizeForMatch twice per record
    const normRecords = includeNames
      ? tableRecords
          .map(r => ({ record: r, normName: r.name ? normalizeForMatch(r.name) : '' }))
          .filter(nr => nr.normName.length >= CONFIG.MIN_CHARS_FOR_RULE_MATCH)
      : [];

    // results: Map<recordId, {record, items[]}>
    const results = new Map();
    const seen    = new Set();

    const addResult = (record, typeLabel, ruleLabel, ruleId, ruleUrl, isNameMatch, matchedCond) => {
      const key = `${typeLabel}:${ruleId}:${String(record.id)}`;
      if (seen.has(key)) return;
      seen.add(key);
      if (!results.has(record.id)) results.set(record.id, { record, items: [] });
      results.get(record.id).items.push({ typeLabel, ruleLabel, ruleId, ruleUrl, isNameMatch, matchedCond });
    };

    const scanConditions = (conds, typeLabel, ruleLabel, ruleId, ruleUrl) => {
      // Two-pass approach: exact ID matches first, name matches second.
      // This prevents a name match from blocking a subsequent exact match for
      // the same record when both conditions exist in the same rule.
      const exactMatchedIds = new Set();

      // Pass 1: exact ID matches — pass the matching condition as tooltip source
      conds.forEach(cond => {
        if (cond.value == null) return;
        const exactRecord = recordById.get(String(cond.value));
        if (exactRecord) {
          addResult(exactRecord, typeLabel, ruleLabel, ruleId, ruleUrl, false, cond);
          exactMatchedIds.add(String(exactRecord.id));
        }
      });

      // Pass 2: name matches — only for records not already matched exactly
      if (normRecords.length > 0) {
        // Pre-compute normalised values once per condition to avoid repeating the work
        const normVals = conds.map(cond =>
          cond.value != null ? normalizeForMatch(String(cond.value)) : ''
        );
        // When wholeWord is on, a hit at position P requires the char at P-1 and at P+len
        // to be a non-alphanumeric separator (or out-of-bounds). This blocks false positives
        // like "PDF" matching inside "uploadPDFdoc".
        const matchesWord = (haystack, needle) => {
          const idx = haystack.indexOf(needle);
          if (idx < 0) return false;
          if (!wholeWord) return true;
          const isAlnum = (c) => /[a-z0-9]/i.test(c);
          const before = idx > 0 ? haystack[idx - 1] : '';
          const after  = idx + needle.length < haystack.length ? haystack[idx + needle.length] : '';
          return !isAlnum(before) && !isAlnum(after);
        };
        normRecords.forEach(nr => {
          if (exactMatchedIds.has(String(nr.record.id))) return; // already exact
          const matchIdx = normVals.findIndex(v => v.length >= CONFIG.MIN_CHARS_FOR_RULE_MATCH && matchesWord(v, nr.normName));
          if (matchIdx >= 0) {
            addResult(nr.record, typeLabel, ruleLabel, ruleId, ruleUrl, true, conds[matchIdx]);
          }
        });
      }
    };

    // Build a field-name map: "custom_fields_ID" → "Field Title"
    // Fetched in parallel with the rule types so it adds no extra wait time.
    const fieldMap = new Map();
    Object.entries(STANDARD_FIELD_NAMES).forEach(([k, v]) => fieldMap.set(k, v));

    const tasks = [
      fetchAllPages('/api/v2/ticket_fields.json', 'ticket_fields').catch(() => []).then(fields => {
        fields.forEach(f => fieldMap.set(`custom_fields_${f.id}`, f.title));
        // Reuse this fetch to populate the text-field cache if not already set,
        // avoiding a duplicate API call when ticketFields scan is also selected.
        if (cachedTextTicketFieldIds === null) {
          cachedTextTicketFieldIds = fields
            .filter(f => ['text', 'textarea'].includes(f.type))
            .map(f => f.id);
        }
      }),
    ];
    const add = (apiPath, dataKey, typeKey, ruleKeyFn, condsGetter) =>
      tasks.push(
        fetchAllPages(apiPath, dataKey).catch(() => []).then(items => {
          updateStatus(`${t(typeKey)}...`);
          items.forEach(item => {
            const conds = condsGetter(item);
            scanConditions(conds, t(typeKey), item.title || item.name || `ID ${item.id}`, item.id, ruleKeyFn(item.id));
          });
        })
      );

    if (selectedTypes.includes('triggers'))
      add('/api/v2/triggers', 'triggers', 'rules.triggers', id => getRuleUrl('triggers', id),
          // Scan conditions AND actions — actions can set lookup fields to a CO record ID
          item => [
            ...(item.conditions?.all || []),
            ...(item.conditions?.any || []),
            ...(item.actions         || []),
          ]);

    if (selectedTypes.includes('automations'))
      add('/api/v2/automations', 'automations', 'rules.automations', id => getRuleUrl('automations', id),
          // Scan conditions AND actions — same reason as triggers above
          item => [
            ...(item.conditions?.all || []),
            ...(item.conditions?.any || []),
            ...(item.actions         || []),
          ]);

    if (selectedTypes.includes('views'))
      add('/api/v2/views', 'views', 'rules.views', id => getRuleUrl('views', id),
          item => [...(item.conditions?.all || []), ...(item.conditions?.any || [])]);

    if (selectedTypes.includes('sla'))
      add('/api/v2/sla_policies', 'sla_policies', 'rules.sla', id => getRuleUrl('sla', id),
          item => [...(item.filter?.all || []), ...(item.filter?.any || [])]);

    await Promise.all(tasks);

    // Pre-compute once — reused by all three text-field search loops below
    const searchable = tableRecords.filter(r => r.name && normalizeForMatch(r.name).length >= CONFIG.MIN_CHARS_FOR_TEXT_SEARCH);

    // Accumulated over the three section loops, reported as a single toast at the end
    // so a full outage doesn't flood the user with 3 simultaneous warnings.
    const erroredSections = [];

    // Linked records via lookup fields — uses the same relationship_fields endpoint as
    // Usage & Impact. Deterministic (no fuzzy matching). Surfaces tickets/users/orgs/COs
    // that reference each record through a lookup field.
    if (selectedTypes.includes('linkedRecords') && !_rlCancelled) {
      const lookupFields = await getLookupFieldsForCurrentCo();
      if (lookupFields.length > 0) {
        const BATCH = CONFIG.RL_RECORDS_PER_BATCH;
        let sectionErrored = false;
        for (let i = 0; i < tableRecords.length && !_rlCancelled; i += BATCH) {
          const batch = tableRecords.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.linkedRecords')}: ${Math.min(i + BATCH, tableRecords.length)}/${tableRecords.length}`);
          await Promise.all(batch.map(async record => {
            for (const field of lookupFields) {
              if (_rlCancelled) return;
              const { dataKey, displayField } = getRelationshipDataKey(field.type);
              if (!dataKey) continue;
              // Walk pagination: relationships can span many pages for popular records,
              // and the first page alone would miss results.
              let endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${record.id}/relationship_fields/${field.id}/${field.type}`;
              try {
                while (endpoint && !_rlCancelled) {
                  const resp = await zafRequest(endpoint);
                  (resp[dataKey] || []).forEach(item => {
                    const label = item[displayField] || item.title || `#${item.id}`;
                    addResult(record, field.label, label, item.id, getZendeskItemUrl(field.type, item.id), false);
                  });
                  endpoint = (resp.meta?.has_more && resp.links?.next) ? resp.links.next : null;
                }
              } catch (e) { console.warn('[RL] linked records fetch failed for', record.id, field.id, e); sectionErrored = true; }
            }
          }));
        }
        if (sectionErrored) erroredSections.push(t('reverseLookup.linkedRecords'));
      }
    }

    // Batched parallel ticket text-field search (custom fields only, not subject/description/comments)
    // Tracks fields skipped by the RL_MAX_FIELDS_PER_QUERY cap across all 3 text-field sections.
    const truncatedSections = [];

    if (selectedTypes.includes('ticketFields') && !_rlCancelled) {
      const textFieldIds = await getTextTicketFieldIds();
      if (textFieldIds.length > 0) {
        // Cap fields per query to keep the URL within Zendesk's length limit
        const queryFieldIds = textFieldIds.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
        if (textFieldIds.length > queryFieldIds.length) {
          truncatedSections.push({ section: t('reverseLookup.ticketFields'), searched: queryFieldIds.length, total: textFieldIds.length });
        }
        const BATCH = CONFIG.RL_RECORDS_PER_BATCH;
        let sectionErrored = false;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.ticketFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase    = sanitizeSearchPhrase(record.name);
            const rawQuery  = `type:ticket (${queryFieldIds.map(id => `custom_fields_${id}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await zafRequest({
                url:  `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`,
                type: 'GET'
              });
              (resp.results || []).forEach(ticket => {
                addResult(
                  record,
                  t('reverseLookup.ticketLabel'),
                  ticket.subject || `#${ticket.id}`,
                  ticket.id,
                  zendeskBaseUrl ? `${zendeskBaseUrl}/agent/tickets/${ticket.id}` : null,
                  true
                );
              });
            } catch (e) { console.warn('[RL] ticket field search failed for', record.name, e); sectionErrored = true; }
          }));
        }
        if (sectionErrored) erroredSections.push(t('reverseLookup.ticketFields'));
      }
    }

    // Batched parallel user text-field search
    if (selectedTypes.includes('userFields') && !_rlCancelled) {
      const userFieldKeys = await getTextUserFieldKeys();
      if (userFieldKeys.length > 0) {
        const queryKeys = userFieldKeys.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
        if (userFieldKeys.length > queryKeys.length) {
          truncatedSections.push({ section: t('reverseLookup.userFields'), searched: queryKeys.length, total: userFieldKeys.length });
        }
        const BATCH = CONFIG.RL_RECORDS_PER_BATCH;
        let sectionErrored = false;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.userFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase   = sanitizeSearchPhrase(record.name);
            const rawQuery = `type:user (${queryKeys.map(k => `user_fields.${k}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await zafRequest({ url: `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`, type: 'GET' });
              (resp.results || []).forEach(user => {
                addResult(record, t('reverseLookup.userFields'), user.name || user.email || `#${user.id}`,
                  user.id, zendeskBaseUrl ? `${zendeskBaseUrl}/agent/users/${user.id}/tickets` : null, true);
              });
            } catch (e) { console.warn('[RL] user field search failed for', record.name, e); sectionErrored = true; }
          }));
        }
        if (sectionErrored) erroredSections.push(t('reverseLookup.userFields'));
      }
    }

    // Batched parallel organization text-field search
    if (selectedTypes.includes('orgFields') && !_rlCancelled) {
      const orgFieldKeys = await getTextOrgFieldKeys();
      if (orgFieldKeys.length > 0) {
        const queryKeys = orgFieldKeys.slice(0, CONFIG.RL_MAX_FIELDS_PER_QUERY);
        if (orgFieldKeys.length > queryKeys.length) {
          truncatedSections.push({ section: t('reverseLookup.orgFields'), searched: queryKeys.length, total: orgFieldKeys.length });
        }
        const BATCH = CONFIG.RL_RECORDS_PER_BATCH;
        let sectionErrored = false;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.orgFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase   = sanitizeSearchPhrase(record.name);
            const rawQuery = `type:organization (${queryKeys.map(k => `organization_fields.${k}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await zafRequest({ url: `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&per_page=${CONFIG.SEARCH_RESULTS_PAGE_SIZE}`, type: 'GET' });
              (resp.results || []).forEach(org => {
                addResult(record, t('reverseLookup.orgFields'), org.name || `#${org.id}`,
                  org.id, zendeskBaseUrl ? `${zendeskBaseUrl}/agent/organizations/${org.id}/tickets` : null, true);
              });
            } catch (e) { console.warn('[RL] org field search failed for', record.name, e); sectionErrored = true; }
          }));
        }
        if (sectionErrored) erroredSections.push(t('reverseLookup.orgFields'));
      }
    }

    // Single summarized toast for any failures across the three loops
    if (erroredSections.length > 0) {
      showToast(t('toast.rlPartialFailure', { sections: erroredSections.join(', ') }), 'warning');
    }

    const wasStopped = _rlCancelled;
    _rlCancelled = false;
    clearInterval(timerInterval);

    // Build results HTML
    const resultsHtml = buildReverseLookupResultsHtml(results);
    // Only show the false-positive note when name matches actually exist in the results
    const hasNameMatches = includeNames && [...results.values()].some(({ items }) => items.some(i => i.isNameMatch));
    const nameMatchNote = hasNameMatches
      ? `<p style="font-size:12px; color:#b45309; margin:0 0 12px 0; padding:6px 10px; background:#fffbeb; border:1px solid #f59e0b; border-radius:4px;">${t('reverseLookup.nameMatchNote')}</p>`
      : '';
    const stoppedNote = wasStopped
      ? `<p style="font-size:12px; color:#cc3340; margin:0 0 12px 0; padding:6px 10px; background:#fff0ee; border:1px solid #f97583; border-radius:4px;">${t('reverseLookup.stopped')}</p>`
      : '';
    // Surfaced when RL_MAX_FIELDS_PER_QUERY truncated one or more text-field sections
    const truncatedNote = truncatedSections.length > 0
      ? `<p style="font-size:12px; color:#b45309; margin:0 0 12px 0; padding:6px 10px; background:#fffbeb; border:1px solid #f59e0b; border-radius:4px;">${truncatedSections.map(s => t('reverseLookup.truncatedNote', { section: escapeHtml(s.section), searched: s.searched, total: s.total })).join('<br>')}</p>`
      : '';

    modal.innerHTML = `
      <h3 style="margin:0 0 8px 0; font-size:16px;">${t('reverseLookup.title')}</h3>
      <p style="font-size:13px; color:#68737d; margin:0 0 14px 0;">${t('reverseLookup.found', { n: results.size })}</p>
      ${stoppedNote}${truncatedNote}${nameMatchNote}
      <div id="rl-results-container">${resultsHtml || `<p style="color:#68737d;">${t('reverseLookup.noResults')}</p>`}</div>
      <div class="modal-footer">
        <div class="modal-footer-actions">
          <button id="rl-btn-again" class="btn btn-secondary">${t('reverseLookup.runAgain')}</button>
          <button id="rl-btn-close" class="btn btn-secondary">${t('form.cancel')}</button>
        </div>
      </div>`;

    document.getElementById('rl-btn-again').onclick = showSelection;
    document.getElementById('rl-btn-close').onclick = () => {
      document.getElementById('reverse-lookup-overlay').style.display = 'none';
    };

    // Clicking a record name closes the modal and opens that record's Usage & Impact tab
    const rlContainer = document.getElementById('rl-results-container');
    if (rlContainer) {
      rlContainer.addEventListener('click', (e) => {
        const link = e.target.closest('[data-rl-record-id]');
        if (!link) return;
        e.preventDefault();
        e.stopPropagation(); // prevent <details> from toggling
        const rid = link.getAttribute('data-rl-record-id');
        const rowData = tabulatorTable?.getData().find(r => String(r.id) === rid);
        if (rowData) {
          document.getElementById('reverse-lookup-overlay').style.display = 'none';
          showForm(rowData, 'related');
        }
      });
    }

  } catch (err) {
    _rlCancelled = false;
    clearInterval(timerInterval);
    console.error('Reverse lookup failed:', err);
    modal.innerHTML = `
      <h3 style="margin:0 0 12px 0; font-size:16px;">${t('reverseLookup.title')}</h3>
      <p style="color:#cc3340; font-size:13px; margin:0 0 16px 0;">${escapeHtml(err.message || String(err))}</p>
      <div class="modal-footer">
        <div class="modal-footer-actions">
          <button id="rl-err-again" class="btn btn-secondary">${t('reverseLookup.runAgain')}</button>
          <button id="rl-err-close" class="btn btn-secondary">${t('form.cancel')}</button>
        </div>
      </div>`;
    document.getElementById('rl-err-again').onclick = showSelection;
    document.getElementById('rl-err-close').onclick = () => {
      document.getElementById('reverse-lookup-overlay').style.display = 'none';
    };
  }
}

// Opens the Reverse Lookup modal in selection phase.
function showReverseLookupModal() {
  const overlay = document.getElementById('reverse-lookup-overlay');
  const modal   = document.getElementById('reverse-lookup-modal');

  overlay.onclick = (e) => { if (e.target === overlay) overlay.style.display = 'none'; };

  const showSelection = () => {
    const totalRecords  = tabulatorTable ? tabulatorTable.getData().length : 0;
    const estimatedReqs = Math.ceil(totalRecords / 5); // 5 records per batch
    const activeCount   = (rowNumMap && rowNumMap.size > 0) ? rowNumMap.size : totalRecords;
    const hasFilter     = activeCount < totalRecords;
    const scopeHtml     = `
      <div style="margin-top:12px; border-top:1px solid #e9ebed; padding-top:12px;">
        <p style="font-weight:600; font-size:13px; margin:0 0 8px 0;">${t('reverseLookup.scope')}</p>
        <label class="lookup-type-label"><input type="radio" name="rl-scope" value="all" id="rl-scope-all" checked> ${t('reverseLookup.scopeAll', { n: totalRecords })}</label>
        <label class="lookup-type-label" style="${!hasFilter ? 'opacity:0.45;' : ''}">
          <input type="radio" name="rl-scope" value="filtered" id="rl-scope-filtered" ${!hasFilter ? 'disabled title="' + t('reverseLookup.scopeNoFilter') + '"' : ''}>
          ${hasFilter ? t('reverseLookup.scopeFiltered', { n: activeCount }) : t('reverseLookup.scopeNoFilter')}
        </label>
      </div>`;

    modal.innerHTML = `
      <h3 style="margin:0 0 8px 0; font-size:16px;">${t('reverseLookup.title')}</h3>
      <p style="font-size:13px; color:#68737d; margin:0 0 14px 0;">${t('reverseLookup.description')}</p>
      <p style="font-weight:600; font-size:13px; margin:0 0 10px 0;">${t('reverseLookup.selectTypes')}</p>
      <div class="lookup-type-grid">
        <label class="lookup-type-label"><input type="checkbox" value="triggers" checked> ${t('rules.triggers')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="automations" checked> ${t('rules.automations')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="views" checked> ${t('rules.views')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="sla" checked> ${t('rules.sla')}</label>
      </div>
      <div style="margin-top:12px; border-top:1px solid #e9ebed; padding-top:12px;">
        <label class="lookup-type-label"><input type="checkbox" value="linkedRecords" id="rl-include-linked" checked> ${t('reverseLookup.linkedRecords')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="ticketFields" id="rl-include-tickets"> ${t('reverseLookup.ticketFields')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="userFields"   id="rl-include-users">   ${t('reverseLookup.userFields')}</label>
        <label class="lookup-type-label"><input type="checkbox" value="orgFields"    id="rl-include-orgs">    ${t('reverseLookup.orgFields')}</label>
        <p id="rl-text-warning" style="display:none; font-size:11px; color:#b45309; margin:8px 0 0 4px; padding:5px 8px; background:#fffbeb; border:1px solid #f59e0b; border-radius:4px;">${t('reverseLookup.textFieldWarning', { n: estimatedReqs })}</p>
      </div>
      ${scopeHtml}
      <label class="lookup-type-label" style="margin-top:12px; border-top:1px solid #e9ebed; padding-top:12px;">
        <input type="checkbox" id="rl-include-names" checked>
        ${t('reverseLookup.includeNames')}
      </label>
      <label class="lookup-type-label" id="rl-whole-word-label" style="margin-left:24px; opacity:0.9;">
        <input type="checkbox" id="rl-whole-word">
        ${t('reverseLookup.wholeWord')}
      </label>
      <div class="modal-footer">
        <div class="modal-footer-actions">
          <button id="rl-btn-cancel" class="btn btn-secondary">${t('form.cancel')}</button>
          <button id="rl-btn-run" class="btn">${t('reverseLookup.run')}</button>
        </div>
      </div>`;

    // Show/hide and update the text-field performance warning whenever a text type checkbox
    // or the scope radio changes. Re-reads the scope each time so the count is always accurate.
    const textCheckIds = ['rl-include-tickets', 'rl-include-users', 'rl-include-orgs'];
    const updateTextWarning = () => {
      const checkedCount = textCheckIds.filter(id => document.getElementById(id)?.checked).length;
      const warningEl    = document.getElementById('rl-text-warning');
      if (!warningEl) return;
      if (checkedCount === 0) {
        warningEl.style.display = 'none';
      } else {
        const useFiltered = document.getElementById('rl-scope-filtered')?.checked ?? false;
        const scopeCount  = useFiltered ? activeCount : totalRecords;
        const reqs        = Math.ceil(scopeCount / 5) * checkedCount;
        warningEl.textContent  = t('reverseLookup.textFieldWarning', { n: reqs });
        warningEl.style.display = 'block';
      }
    };
    textCheckIds.forEach(id => document.getElementById(id)?.addEventListener('change', updateTextWarning));
    // Also update the warning when the scope radio changes
    ['rl-scope-all', 'rl-scope-filtered'].forEach(id => document.getElementById(id)?.addEventListener('change', updateTextWarning));

    // wholeWord only makes sense with name match — gray out + uncheck when names disabled
    const syncWholeWordState = () => {
      const namesOn = document.getElementById('rl-include-names')?.checked ?? false;
      const wwInput = document.getElementById('rl-whole-word');
      const wwLabel = document.getElementById('rl-whole-word-label');
      if (!wwInput || !wwLabel) return;
      wwInput.disabled = !namesOn;
      wwLabel.style.opacity = namesOn ? '0.9' : '0.45';
      if (!namesOn) wwInput.checked = false;
    };
    document.getElementById('rl-include-names')?.addEventListener('change', syncWholeWordState);
    syncWholeWordState();

    document.getElementById('rl-btn-cancel').onclick = () => { overlay.style.display = 'none'; };
    document.getElementById('rl-btn-run').onclick = () => {
      const selectedTypes    = [...modal.querySelectorAll('.lookup-type-grid input:checked')].map(cb => cb.value);
      if (document.getElementById('rl-include-linked')?.checked)  selectedTypes.push('linkedRecords');
      if (document.getElementById('rl-include-tickets')?.checked) selectedTypes.push('ticketFields');
      if (document.getElementById('rl-include-users')?.checked)   selectedTypes.push('userFields');
      if (document.getElementById('rl-include-orgs')?.checked)    selectedTypes.push('orgFields');
      if (selectedTypes.length === 0) return;
      const includeNames     = document.getElementById('rl-include-names')?.checked ?? true;
      const wholeWord        = document.getElementById('rl-whole-word')?.checked ?? false;
      const useFilteredOnly  = document.getElementById('rl-scope-filtered')?.checked ?? false;
      runReverseLookup(selectedTypes, includeNames, useFilteredOnly, wholeWord, modal, showSelection);
    };
  };

  showSelection();
  overlay.style.display = 'flex';
}

// CSV EXPORT
// ----------------------------------------------------

// Returns a Promise that resolves true (leave) or false (stay)
function showUnsavedChangesModal() {
  return new Promise(resolve => {
    const overlay = document.getElementById('unsaved-modal-overlay');
    const close = (result) => { overlay.style.display = 'none'; resolve(result); };
    document.getElementById('unsaved-modal-stay').onclick  = () => close(false);
    document.getElementById('unsaved-modal-leave').onclick = () => close(true);
    overlay.onclick = (e) => { if (e.target === overlay) close(false); };
    overlay.style.display = 'flex';
  });
}

function showExportModal() {
  if (!tabulatorTable) return;

  const overlay    = document.getElementById('export-modal-overlay');
  const infoEl     = document.getElementById('export-modal-info');
  const totalRows  = tabulatorTable.getData().length;
  const activeRows = rowNumMap ? rowNumMap.size : totalRows;
  const isFiltered = activeRows < totalRows;
  const infoKey = isFiltered
    ? (activeRows === 1 ? 'export.rowSingularFiltered' : 'export.rowPluralFiltered')
    : (activeRows === 1 ? 'export.rowSingular'         : 'export.rowPlural');
  infoEl.innerHTML = t(infoKey, { n: `<strong>${activeRows}</strong>`, total: totalRows });

  const close = () => { overlay.style.display = 'none'; };
  document.getElementById('export-modal-cancel').onclick = close;
  overlay.onclick = (e) => { if (e.target === overlay) close(); };

  document.getElementById('export-btn-visible').onclick = () => { close(); buildAndDownloadCSV(false); };
  document.getElementById('export-btn-all').onclick     = () => { close(); buildAndDownloadCSV(true);  };

  overlay.style.display = 'flex';
}

function buildAndDownloadCSV(allColumns) {
  try {
    // getData('active') does not reflect functional filters in Tabulator 5.5.0 — use
    // rowNumMap instead, which is built from the authoritative dataFiltered rows param.
    // rowNumMap keys are in insertion order == sorted+filtered display order.
    const allData = tabulatorTable.getData();
    const allDataById = new Map(allData.map(r => [r.id, r]));
    const data = rowNumMap
      ? [...rowNumMap.keys()].map(id => allDataById.get(id)).filter(Boolean)
      : allData;

    const cols = tabulatorTable.getColumns().filter(col => {
      const field = col.getField();
      if (!field || field === 'actions' || field === 'custom_rownum') return false;
      return allColumns || col.isVisible();
    });

    const headers = cols.map(col => csvEscape(col.getDefinition().title));
    const rows    = data.map(row =>
      cols.map(col => csvEscape(String(row[col.getField()] ?? '')))
    );

    // Prepend UTF-8 BOM so Excel opens the file with correct encoding
    const csv  = '\ufeff' + [headers.join(','), ...rows.map(r => r.join(','))].join('\r\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `${currentCoKey}_export.csv`;
    a.click();
    URL.revokeObjectURL(url);
  } catch (e) {
    console.error('CSV export failed:', e);
    alert('Export failed. The dataset may be too large for your browser.');
  }
}

// -------------------------------------------------------
// FIND DUPLICATES
// -------------------------------------------------------

function showFindDuplicatesModal() {
  if (!tabulatorTable) return;
  const overlay = document.getElementById('find-duplicates-overlay');
  const modal   = document.getElementById('find-duplicates-modal');

  overlay.onclick = (e) => { if (e.target === overlay) overlay.style.display = 'none'; };

  // ── helpers ─────────────────────────────────────────

  function computeDuplicates() {
    const allRecords = tabulatorTable.getData();

    // Pass 1 — exact duplicates (same normalizeForDuplicate key)
    const exactMap    = new Map();
    const recExactKey = new Map(); // recId → exact key (or undefined for singletons)
    for (const rec of allRecords) {
      const key = normalizeForDuplicate(rec.name || '');
      if (!key) continue;
      if (!exactMap.has(key)) exactMap.set(key, []);
      exactMap.get(key).push(rec);
      recExactKey.set(rec.id, key);
    }
    const exactDuplicates = [...exactMap.entries()]
      .filter(([, r]) => r.length >= 2)
      .sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]));

    // Pass 2 — similar names (same normalizeForSimilar key, spans ≥2 distinct exact identities)
    const simMap = new Map();
    for (const rec of allRecords) {
      const key = normalizeForSimilar(rec.name || '');
      if (!key) continue;
      if (!simMap.has(key)) simMap.set(key, []);
      simMap.get(key).push(rec);
    }
    const similarDuplicates = [...simMap.entries()]
      .filter(([, recs]) => {
        if (recs.length < 2) return false;
        const eKeys = new Set(recs.map(r => recExactKey.get(r.id) || `__${r.id}`));
        return eKeys.size >= 2; // at least two distinct "identities" → not already grouped
      })
      .sort((a, b) => b[1].length - a[1].length || a[0].localeCompare(b[0]));

    return { exactDuplicates, similarDuplicates };
  }

  function buildGroupsHtml(duplicates, startIndex = 0, isSimilar = false) {
    return duplicates.map(([, recs], gi) => {
      const groupIdx = startIndex + gi;
      const refName  = recs[0].name || '';
      const itemsHtml = recs.map((r, ri) => {
        // First record: diff against the second so it also shows its own differences.
        // Other records: diff against the first (reference).
        const nameHtml = ri === 0
          ? diffHighlight(recs[1].name || String(recs[1].id), r.name || String(r.id))
          : diffHighlight(refName, r.name || String(r.id));
        return `<li style="display:flex; align-items:center; gap:8px; padding:7px 0; border-bottom:1px solid #f0f2f4;">
          <input type="checkbox" class="fd-check" data-id="${escapeHtml(String(r.id))}" data-name="${escapeHtml(r.name || '')}" data-group="${groupIdx}" style="width:auto; flex-shrink:0; cursor:pointer;">
          <a class="fd-record-link" data-fd-record-id="${escapeHtml(String(r.id))}" href="#" style="flex:1; font-size:13px;">${nameHtml}</a>
          <span class="badge-id" style="flex-shrink:0;">${escapeHtml(String(r.id))}</span>
        </li>`;
      }).join('');
      const cls = isSimilar ? 'related-section fd-similar-group' : 'related-section';
      return `<details class="${cls}" open>
        <summary>
          <input type="checkbox" class="fd-group-check" data-group="${groupIdx}" style="width:auto; margin:0 6px 0 0; flex-shrink:0; cursor:pointer;" title="${t('findDuplicates.selectAll')}">
          <span>${escapeHtml(refName || String(recs[0].id))}</span>
          <span class="section-meta">${t('findDuplicates.groupCount', { n: recs.length })}</span>
          <span class="section-toggle">&#9654;</span>
        </summary>
        <ul style="list-style:none; padding:0 14px; margin:0;">${itemsHtml}</ul>
      </details>`;
    }).join('');
  }

  // ── phases ───────────────────────────────────────────

  function renderSelection({ exactDuplicates, similarDuplicates }, notice = '') {
    const hasExact   = exactDuplicates.length > 0;
    const hasSimilar = similarDuplicates.length > 0;

    if (!hasExact && !hasSimilar) {
      modal.innerHTML = `
        <h3 style="margin:0 0 8px 0; font-size:16px;">${t('findDuplicates.title')}</h3>
        ${notice ? `<div style="padding:10px 14px; background:#edf7ed; border:1px solid #5c9e6e; border-radius:4px; margin-bottom:14px; font-size:13px; color:#1e5631; font-weight:600;">${notice}</div>` : ''}
        <p style="font-size:14px; color:#2f3941; margin:0;">${t('findDuplicates.noResults')}</p>
        <div class="modal-footer">
          <div class="modal-footer-actions">
            <button id="fd-close" class="btn btn-secondary">${t('form.cancel')}</button>
          </div>
        </div>`;
      document.getElementById('fd-close').onclick = () => { overlay.style.display = 'none'; };
      return;
    }

    const exactTotal = exactDuplicates.reduce((s, [, r]) => s + r.length, 0);
    const simTotal   = similarDuplicates.reduce((s, [, r]) => s + r.length, 0);
    const simOffset  = exactDuplicates.length; // group-index offset so similar indices don't clash

    const exactSection = `
      <div class="fd-section-header fd-section-exact">${t('findDuplicates.exactTitle')}${hasExact ? ` &mdash; ${t('findDuplicates.found', { n: exactDuplicates.length, total: exactTotal })}` : ''}</div>
      <p style="font-size:11px; color:#68737d; margin:0 0 8px 4px;">${t('findDuplicates.exactHint')}</p>
      ${hasExact ? buildGroupsHtml(exactDuplicates, 0, false) : `<p style="font-size:13px; color:#68737d; padding:4px;">${t('findDuplicates.noExact')}</p>`}`;

    const similarSection = hasSimilar ? `
      <div class="fd-section-header fd-section-similar">${t('findDuplicates.similarTitle')} &mdash; ${t('findDuplicates.found', { n: similarDuplicates.length, total: simTotal })}</div>
      <p style="font-size:11px; color:#92400e; margin:0 0 8px 4px;">${t('findDuplicates.similarHint')}</p>
      ${buildGroupsHtml(similarDuplicates, simOffset, true)}` : '';

    modal.innerHTML = `
      <h3 style="margin:0 0 6px 0; font-size:16px;">${t('findDuplicates.title')}</h3>
      ${notice ? `<div style="padding:10px 14px; background:#edf7ed; border:1px solid #5c9e6e; border-radius:4px; margin-bottom:10px; font-size:13px; color:#1e5631; font-weight:600;">${notice}</div>` : ''}
      <p style="font-size:12px; color:#028484; margin:0 0 10px 0;">${t('findDuplicates.diffHint')}</p>
      <div id="fd-groups-container">${exactSection}${similarSection}</div>
      <div class="modal-footer">
        <span id="fd-sel-count" style="font-size:13px; color:#68737d; min-width:0;">${t('findDuplicates.noneSelected')}</span>
        <div class="modal-footer-actions">
          <button id="fd-close" class="btn btn-secondary">${t('form.cancel')}</button>
          <button id="fd-delete-btn" class="btn btn-danger" disabled>${t('findDuplicates.deleteSelected')}</button>
        </div>
      </div>`;

    document.getElementById('fd-close').onclick = () => { overlay.style.display = 'none'; };

    const updateFooter = () => {
      const n = modal.querySelectorAll('.fd-check:checked').length;
      document.getElementById('fd-sel-count').textContent = n > 0
        ? t('findDuplicates.selectedCount', { n })
        : t('findDuplicates.noneSelected');
      const btn = document.getElementById('fd-delete-btn');
      btn.disabled = n === 0;
      btn.textContent = n > 0
        ? t('findDuplicates.deleteSelectedN', { n })
        : t('findDuplicates.deleteSelected');
    };

    const container = document.getElementById('fd-groups-container');

    container.addEventListener('change', (e) => {
      const el = e.target;
      if (el.classList.contains('fd-group-check')) {
        const gi = el.getAttribute('data-group');
        modal.querySelectorAll(`.fd-check[data-group="${gi}"]`).forEach(cb => { cb.checked = el.checked; });
      } else if (el.classList.contains('fd-check')) {
        const gi = el.getAttribute('data-group');
        const all = [...modal.querySelectorAll(`.fd-check[data-group="${gi}"]`)];
        const hdr = modal.querySelector(`.fd-group-check[data-group="${gi}"]`);
        if (hdr) {
          hdr.checked       = all.every(c => c.checked);
          hdr.indeterminate = !hdr.checked && all.some(c => c.checked);
        }
      }
      updateFooter();
    });

    container.addEventListener('click', (e) => {
      // Prevent checkbox inside <summary> from toggling <details>
      if (e.target.classList.contains('fd-group-check')) { e.stopPropagation(); return; }
      const link = e.target.closest('[data-fd-record-id]');
      if (!link) return;
      e.preventDefault();
      e.stopPropagation();
      const rid = link.getAttribute('data-fd-record-id');
      const rowData = tabulatorTable?.getData().find(r => String(r.id) === rid);
      if (rowData) { overlay.style.display = 'none'; showForm(rowData); }
    });

    document.getElementById('fd-delete-btn').onclick = () => {
      const toDelete = [...modal.querySelectorAll('.fd-check:checked')].map(cb => ({
        id:   cb.getAttribute('data-id'),
        name: cb.getAttribute('data-name'),
      }));
      if (toDelete.length === 0) return;
      scanAndConfirm(toDelete, { exactDuplicates, similarDuplicates });
    };
  }

  // Runs fullReferenceScan on each selected record sequentially, then shows review
  async function scanAndConfirm(toDelete, duplicates) {
    let scanned = 0;
    const total = toDelete.length;

    const showProgress = () => {
      const pct = Math.round((scanned / total) * 100);
      modal.innerHTML = `
        <h3 style="margin:0 0 12px 0; font-size:16px;">${t('findDuplicates.title')}</h3>
        <p style="font-size:14px; color:#2f3941; margin:0 0 10px 0;">${t('findDuplicates.scanning', { done: scanned, total })}</p>
        <div class="progress-container">
          <div class="progress-bar-determinate" style="width:${pct}%"></div>
        </div>`;
    };

    showProgress();
    const results = [];
    for (const rec of toDelete) {
      let scan = { totalFound: 0, totalPossible: 0, totalRelationships: 0, totalRules: 0,
                   ruleCounts: { triggers:0, automations:0, views:0, sla:0 },
                   possibleCounts: { triggers:0, automations:0, views:0, sla:0 } };
      try { scan = await fullReferenceScan(rec.id, rec.name, () => {}); }
      catch (e) { console.warn(`Scan failed for record ${rec.id}`, e); }
      results.push({ rec, scan });
      scanned++;
      showProgress();
    }

    renderReview(results, duplicates);
  }

  function renderReview(results, duplicates) {
    const hasWarnings = results.some(r => r.scan.totalFound > 0);

    const rowsHtml = results.map(({ rec, scan }) => {
      const ruleTotal = scan.ruleCounts.triggers + scan.ruleCounts.automations +
                        scan.ruleCounts.views    + scan.ruleCounts.sla;
      const possTotal = scan.possibleCounts.triggers + scan.possibleCounts.automations +
                        scan.possibleCounts.views    + scan.possibleCounts.sla;
      let statusParts = [];
      if (scan.totalRelationships > 0) statusParts.push(`<span style="color:#cc3340; font-weight:600;">${t('findDuplicates.hasLinked', { n: scan.totalRelationships })}</span>`);
      if (ruleTotal > 0)               statusParts.push(`<span style="color:#cc3340; font-weight:600;">${t('findDuplicates.hasRules',  { n: ruleTotal })}</span>`);
      if (statusParts.length === 0 && possTotal > 0) statusParts.push(`<span style="color:#b45309;">${t('findDuplicates.hasPossible', { n: possTotal })}</span>`);
      if (statusParts.length === 0)    statusParts.push(`<span style="color:#038153;">✓ ${t('findDuplicates.noDeps')}</span>`);

      const rowBg = scan.totalFound > 0 ? 'background:#fff8f8;' : '';
      return `<tr style="border-bottom:1px solid #e9ebed; ${rowBg}">
        <td style="padding:8px 16px 8px 0; font-size:13px;">${escapeHtml(rec.name || rec.id)} <span class="badge-id">${escapeHtml(rec.id)}</span></td>
        <td style="padding:8px 0; font-size:12px; white-space:nowrap;">${statusParts.join('&nbsp;·&nbsp;')}</td>
      </tr>`;
    }).join('');

    modal.innerHTML = `
      <h3 style="margin:0 0 10px 0; font-size:16px;">${t('findDuplicates.title')}</h3>
      ${hasWarnings ? `<div style="padding:10px 14px; background:#fff0ee; border:1px solid #cc3340; border-radius:4px; margin-bottom:12px; font-size:13px; color:#cc3340;">${t('delete.warningBody')}</div>` : ''}
      <p style="font-size:13px; font-weight:600; color:#2f3941; margin:0 0 10px 0;">${t('findDuplicates.scanComplete')}</p>
      <div style="overflow-x:auto;">
        <table style="width:100%; border-collapse:collapse; margin-bottom:4px;">
          <thead><tr style="border-bottom:2px solid #e9ebed;">
            <th style="text-align:left; padding:6px 16px 6px 0; font-size:12px; color:#68737d; font-weight:600;">${t('col.name')}</th>
            <th style="text-align:left; padding:6px 0; font-size:12px; color:#68737d; font-weight:600;">${t('findDuplicates.dependencies')}</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
      <p style="font-size:12px; color:#68737d; margin:8px 0 0 0;">${t('findDuplicates.deleteWarning')}</p>
      <div class="modal-footer">
        <div class="modal-footer-actions">
          <button id="fd-back" class="btn btn-secondary">${t('form.cancel')}</button>
          <button id="fd-confirm" class="btn btn-danger">${t('findDuplicates.confirmDeleteBtn', { n: results.length })}</button>
        </div>
      </div>`;

    document.getElementById('fd-back').onclick    = () => renderSelection(duplicates);
    document.getElementById('fd-confirm').onclick = () => runDeletion(results.map(r => r.rec));
  }

  async function runDeletion(toDelete) {
    let done = 0;
    const errors = [];

    const showProgress = () => {
      modal.innerHTML = `
        <h3 style="margin:0 0 16px 0; font-size:16px;">${t('findDuplicates.title')}</h3>
        <p style="font-size:14px; color:#2f3941; margin:0 0 10px 0;">${t('findDuplicates.deleting', { done, total: toDelete.length })}</p>
        <div class="progress-container">
          <div class="progress-bar-determinate" style="width:${Math.round((done / toDelete.length) * 100)}%"></div>
        </div>`;
    };

    showProgress();
    for (const rec of toDelete) {
      try {
        await zafRequest({ url: `/api/v2/custom_objects/${currentCoKey}/records/${rec.id}`, type: 'DELETE' });
        tabulatorTable?.deleteRow(rec.id);
      } catch (e) {
        console.error(`Failed to delete record ${rec.id}`, e);
        errors.push(rec.name || rec.id);
      }
      done++;
      showProgress();
    }
    updateRecordSummary();

    const successCount = toDelete.length - errors.length;
    const notice = t('findDuplicates.deleteSuccess', { n: successCount })
      + (errors.length > 0 ? `  ${t('findDuplicates.deleteErrors', { n: errors.length })}: ${errors.map(escapeHtml).join(', ')}` : '');

    renderSelection(computeDuplicates(), notice);
  }

  // ── entry point ──────────────────────────────────────
  renderSelection(computeDuplicates());
  overlay.style.display = 'flex';
}

// ----------------------------------------------------
// UTILITIES: PAGINATION & SERVER-SIDE SEARCH
// ----------------------------------------------------

async function fetchAllPages(initialEndpoint, dataKey, progressCallback) {
  const allRecords = [];
  let currentEndpoint = initialEndpoint;
  let truncated = false;
  let lastError = null;

  while (currentEndpoint) {
    try {
      const response = await zafRequest(currentEndpoint);
      if (response[dataKey]) {
        for (const r of response[dataKey]) allRecords.push(r);
        if (progressCallback) {
          progressCallback(allRecords.length);
        }
      }
      if (response.meta && response.meta.has_more && response.links && response.links.next) {
        currentEndpoint = response.links.next;
      } else if (response.next_page) {
        currentEndpoint = response.next_page;
      } else {
        currentEndpoint = null;
      }
    } catch (error) {
      console.error(`Error fetching paginated data from ${currentEndpoint}:`, error);
      truncated = true;
      lastError = error;
      currentEndpoint = null;
    }
  }
  if (truncated) {
    // Callers that care can inspect result.truncated / result.error; existing
    // callers treat the array normally (partial data, same as before).
    allRecords.truncated = true;
    allRecords.error = lastError;
  }
  return allRecords;
}

async function searchLookupData(targetType, query) {
  let endpoint = '';
  let dataKey = '';
  let labelField = 'name'; 
  
  if (targetType.startsWith('zen:custom_object:')) {
    const relatedCoKey = targetType.replace('zen:custom_object:', '');
    endpoint = `/api/v2/custom_objects/${relatedCoKey}/records/autocomplete?name=${encodeURIComponent(query)}`;
    dataKey = 'custom_object_records';
  } else if (targetType === 'zen:user') {
    endpoint = `/api/v2/users/autocomplete.json?name=${encodeURIComponent(query)}`;
    dataKey = 'users';
  } else if (targetType === 'zen:ticket') {
    endpoint = `/api/v2/search.json?query=${encodeURIComponent(`type:ticket ${query}`)}`;
    dataKey = 'results';
    labelField = 'subject'; 
  } else if (targetType === 'zen:organization') {
    endpoint = `/api/v2/organizations/autocomplete.json?name=${encodeURIComponent(query)}`;
    dataKey = 'organizations';
  } else {
    return [];
  }

  try {
    const response = await zafRequest(endpoint);
    const rawRecords = response[dataKey] || [];
    
    const filteredRecords = rawRecords.filter(record => {
      if (record.suspended === true) return false;
      if (record.active === false) return false;
      if (record.custom_object_fields && record.custom_object_fields.active === false) return false;
      return true;
    });

    return filteredRecords.map(record => ({
      id: record.id || record.external_id,
      label: record[labelField] || record.title || `Record ${record.id}`
    }));
  } catch (error) {
    console.error("Lookup Search Error:", error);
    return [];
  }
}

async function fetchSingleRecordName(targetType, id) {
  let endpoint = '';
  let dataKey = '';
  let labelField = 'name'; 

  if (targetType.startsWith('zen:custom_object:')) {
    const relatedCoKey = targetType.replace('zen:custom_object:', '');
    endpoint = `/api/v2/custom_objects/${relatedCoKey}/records/${id}`;
    dataKey = 'custom_object_record';
  } else if (targetType === 'zen:user') {
    endpoint = `/api/v2/users/${id}.json`;
    dataKey = 'user';
  } else if (targetType === 'zen:ticket') {
    endpoint = `/api/v2/tickets/${id}.json`;
    dataKey = 'ticket';
    labelField = 'subject'; 
  } else if (targetType === 'zen:organization') {
    endpoint = `/api/v2/organizations/${id}.json`;
    dataKey = 'organization';
  } else {
    return t('form.recordFallback', { id });
  }

  try {
    const response = await zafRequest(endpoint);
    const record = response[dataKey];
    return record[labelField] || record.title || t('form.recordFallback', { id: record.id });
  } catch (e) {
    return t('form.recordFallback', { id });
  }
}
