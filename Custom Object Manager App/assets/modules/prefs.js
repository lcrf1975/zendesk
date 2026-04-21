// Per-CO preferences persisted to localStorage (column visibility, saved filters).
// Silently no-ops on storage errors (private mode, quota exceeded) — prefs are best-effort.
const PREFS_VERSION = 1;
const prefsKey = (coKey) => `co_manager:prefs:${coKey}`;

export function loadPrefs(coKey) {
  if (!coKey) return null;
  try {
    const raw = localStorage.getItem(prefsKey(coKey));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed?.version !== PREFS_VERSION) return null;
    return parsed;
  } catch (e) {
    console.warn('[prefs] failed to load', e);
    return null;
  }
}

export function savePrefs(coKey, patch) {
  if (!coKey) return;
  try {
    const current = loadPrefs(coKey) || { version: PREFS_VERSION };
    const merged = { ...current, ...patch, version: PREFS_VERSION };
    localStorage.setItem(prefsKey(coKey), JSON.stringify(merged));
  } catch (e) {
    console.warn('[prefs] failed to save', e);
  }
}
