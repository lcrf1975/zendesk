// Pure helpers — no DOM, no global state. Safe to import anywhere.

export function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Formats a seconds count as h:mm:ss (hours omitted when zero), e.g. "1:04" or "1:02:34"
export function formatElapsed(secs) {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
  return `${m}:${String(s).padStart(2,'0')}`;
}

// Normalises a string for name-based fuzzy matching:
// lowercase, remove diacritics, strip special characters, collapse spaces.
// Preserves separators (< > - / | \) as distinct characters so "CST > X"
// doesn't match "CST - X".
export function normalizeForMatch(str) {
  if (!str) return '';
  return String(str)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')          // strip diacritics
    .replace(/[^a-z0-9\s<>\-\/|\\]/g, '')    // keep only: letters, digits, spaces, and separators
    .replace(/\s+/g, ' ')
    .trim();
}

// Strips chars with syntactic meaning in the Zendesk Search API so a record
// name can be embedded as a quoted phrase without producing malformed queries.
export function sanitizeSearchPhrase(str) {
  if (!str) return '';
  return String(str).replace(/[<>\/|\\":()]+/g, ' ').replace(/\s+/g, ' ').trim();
}

// Common words to ignore when computing similarity keys (pt-BR / es / en)
export const DUPLICATE_STOPWORDS = new Set([
  'de','da','do','dos','das','em','na','no','nas','nos','para','com','por','ou', // pt
  'del','el','la','los','las','en','al','con','un','una','y',                    // es
  'the','of','in','and','or','an','for','to','by','on','with',                   // en
]);

// Normalises a string for duplicate-name detection:
// case-insensitive, strips diacritics, treats _ - . as spaces, collapses whitespace.
// "Ar Condicionado" === "Ar_Condicionado" === "ar-condicionado" after this.
export function normalizeForDuplicate(str) {
  if (!str) return '';
  return String(str)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')   // strip diacritics
    .replace(/[_\-.]/g, ' ')           // treat _ - . as spaces
    .replace(/[^a-z0-9\s]/g, '')       // strip remaining special chars
    .replace(/\s+/g, ' ')
    .trim();
}

// Normalises for similarity matching: removes stopwords and sorts tokens alphabetically so
// word-order and missing articles/prepositions don't matter.
// "Gestão de Terceiros" === "Gestão Terceiros" === "Terceiros Gestão" after this.
// Uses its own base normalization (not normalizeForDuplicate) so that abbreviations like
// "D&O" split into separate tokens ("d" + "o") instead of merging into the stopword "do".
// Returns '' when fewer than 2 meaningful tokens remain (avoids trivial false positives).
export function normalizeForSimilar(str) {
  const base = String(str)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')  // strip diacritics
    .replace(/[&+]/g, ' ')            // split abbreviations like D&O into separate tokens
    .replace(/[_\-.]/g, ' ')          // treat _ - . as spaces
    .replace(/[^a-z0-9\s]/g, '')      // strip remaining special chars
    .replace(/\s+/g, ' ')
    .trim();

  const tokens = base
    .split(' ')
    .filter(t => t.length > 0 && !DUPLICATE_STOPWORDS.has(t))
    .sort();
  return tokens.length >= 2 ? tokens.join(' ') : '';
}

// Returns HTML for `target` with characters highlighted where they differ from `reference`.
// Uses case-insensitive LCS for alignment; highlights exact-char mismatches and insertions.
export function diffHighlight(reference, target) {
  if (!reference || reference === target) return escapeHtml(target);
  const a = reference, b = target;
  const m = a.length, n = b.length;
  if (m * n > 40000) return escapeHtml(target); // guard for very long strings

  const dp = Array.from({ length: m + 1 }, () => new Uint16Array(n + 1));
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i-1].toLowerCase() === b[j-1].toLowerCase()
        ? dp[i-1][j-1] + 1
        : Math.max(dp[i-1][j], dp[i][j-1]);
    }
  }

  const parts = [];
  let i = m, j = n;
  while (j > 0) {
    if (i > 0 && a[i-1].toLowerCase() === b[j-1].toLowerCase()) {
      parts.unshift({ ch: b[j-1], d: a[i-1] !== b[j-1] });
      i--; j--;
    } else if (i > 0 && dp[i-1][j] >= dp[i][j-1]) {
      i--;
    } else {
      parts.unshift({ ch: b[j-1], d: true });
      j--;
    }
  }

  let html = '', inMark = false;
  for (const p of parts) {
    if ( p.d && !inMark) { html += '<mark class="fd-diff">'; inMark = true; }
    if (!p.d &&  inMark) { html += '</mark>';                inMark = false; }
    html += escapeHtml(p.ch);
  }
  return inMark ? html + '</mark>' : html;
}

// CSV cell escape — wraps in quotes, doubles internal quotes, replaces newlines with spaces.
export function csvEscape(val) {
  return `"${String(val).replace(/"/g, '""').replace(/\r?\n/g, ' ')}"`;
}
