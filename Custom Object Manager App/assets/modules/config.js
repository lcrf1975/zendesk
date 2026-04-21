// Central configuration for tuning knobs (page sizes, timings, layout constants).
// All magic numbers that might need adjustment live here.
export const CONFIG = {
  // API pagination
  CO_RECORDS_PAGE_SIZE: 100,          // records fetched per page from /custom_objects/*/records
  SEARCH_RESULTS_PAGE_SIZE: 25,       // results per Zendesk Search API call

  // Reverse Lookup / Usage & Impact
  RL_MAX_FIELDS_PER_QUERY: 15,        // cap on text fields per OR-joined search query (URL length limit)
  RL_RECORDS_PER_BATCH: 5,            // records processed per parallel batch in reverse lookup

  // UI layout
  TABLE_PAGINATION_SIZE: 25,          // rows shown per Tabulator page (default)
  TABLE_PAGINATION_SIZES: [10, 25, 50, 100], // options for the rows-per-page selector
  IFRAME_MAX_HEIGHT_RATIO: 0.75,      // cap iframe at % of screen height (Zendesk workspace visible area)
  ACTIONS_COL_WIDTH: 72,              // width of the Edit/Delete column (icon buttons)
  ROWNUM_COL_WIDTH: 50,               // width of the # column
  SELECTION_COL_WIDTH: 36,            // width of the row-selection checkbox column
  DEFAULT_COL_WIDTH: 150,             // fallback minWidth for text columns
  TABLE_VIEW_HORIZONTAL_PADDING: 40,  // subtracted from containerWidth when computing default visible columns

  // Timing
  RESIZE_DEBOUNCE_MS: 100,            // debounce for resizeIframe bursts
  SEARCH_DEBOUNCE_MS: 200,            // debounce for global search input
  TOAST_DURATION_MS: 5000,            // how long a toast stays on screen

  // Matching thresholds
  MIN_CHARS_FOR_RULE_MATCH: 3,        // normalizeForMatch minimum length for rule name fuzzy matching
  MIN_CHARS_FOR_TEXT_SEARCH: 5,       // minimum length for entity text-field search
};
