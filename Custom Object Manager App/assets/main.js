// Initialize the ZAF Client
const client = ZAFClient.init();

// Global App State
let currentCoKey = null;
let currentCoTitle = null; 
let currentSchema = null;
let tabulatorTable = null;
let columnSelectorTS = null; 
let objectSelectorTS = null; 
let cachedLookupFields = null; // Caches the relation schema so we don't spam the API
let activeFilters = [];        // Active advanced filter conditions
let filterColumns = [];        // Columns available for filtering (set per table load)
let lastFilterCoKey = null;    // Tracks which CO the filter bar was built for

// DOM Elements
const views = {
  loader: document.getElementById('loader'),
  selector: document.getElementById('selector-view'),
  table: document.getElementById('table-view'),
  form: document.getElementById('form-view')
};

// Initialize the app when the DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  client.invoke('resize', { width: '100%', height: '800px' });
  startApp();

  // Toolbar Listeners
  document.getElementById('btn-new-record').addEventListener('click', () => showForm());
  document.getElementById('btn-back-selector').addEventListener('click', startApp);
  
  // Tabulator Global Search (now delegates to unified filter)
  document.getElementById('table-search').addEventListener('input', function() {
    applyTableFilters();
  });

  // Export CSV
  document.getElementById('btn-export-csv').addEventListener('click', showExportModal);

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
    
    const containerWidth = document.getElementById('table-view').clientWidth || window.innerWidth;
    const availableWidth = containerWidth - 40; 
    let usedWidth = 190; 
    const defaultVisible = [];

    const tableCols = tabulatorTable.getColumnDefinitions();

    tableCols.forEach(col => {
      if (col.field === 'actions' || col.field === 'custom_rownum') return;
      
      let colW = col.width || col.minWidth || 150;
      if (usedWidth + colW <= availableWidth || defaultVisible.length < 2) {
         defaultVisible.push(col.field);
         usedWidth += colW;
      }
    });

    columnSelectorTS.setValue(defaultVisible);
  });
});

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

function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function switchView(activeViewId) {
  Object.values(views).forEach(el => el.style.display = 'none');
  views[activeViewId].style.display = 'block';
}

function updateLoaderText(text) {
  const loaderTextEl = document.getElementById('loader-text');
  if (loaderTextEl) loaderTextEl.innerText = text;
}

// ----------------------------------------------------
// VIEW 1: SELECTOR
// ----------------------------------------------------
async function startApp() {
  updateLoaderText("Loading Custom Objects...");
  switchView('loader');
  try {
    const response = await client.request('/api/v2/custom_objects');
    const customObjects = response.custom_objects;
    renderObjectSelector(customObjects);
    switchView('selector');
  } catch (error) {
    updateLoaderText("Error loading Custom Objects. Check your permissions.");
    console.error(error);
  }
}

function renderObjectSelector(objects) {
  let html = `<h2>Select a Custom Object</h2>
              <div class="form-group">
                <select id="co-selector" placeholder="Search for an Object...">
                  <option value="">-- Choose an Object --</option>`;
  objects.forEach(obj => {
    html += `<option value="${escapeHtml(obj.key)}">${escapeHtml(obj.title_pluralized)}</option>`;
  });
  html += `</select></div>
           <button class="btn" id="load-co-btn">Load Object Dashboard</button>`;
           
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
      cachedLookupFields = null; 
      await loadTable(currentCoKey);
    }
  });
}

// ----------------------------------------------------
// VIEW 2: DATA TABLE (TABULATOR)
// ----------------------------------------------------
async function loadTable(coKey) {
  updateLoaderText("Loading schema...");
  switchView('loader');
  try {
    const schemaResponse = await client.request(`/api/v2/custom_objects/${coKey}/fields`);
    currentSchema = schemaResponse.custom_object_fields;
    
    const records = await fetchAllPages(
      `/api/v2/custom_objects/${coKey}/records?page[size]=100`, 
      'custom_object_records',
      (currentCount) => {
        updateLoaderText(`Fetching data... ${currentCount} records loaded`);
      }
    );
    
    updateLoaderText("Rendering table...");

    const tableData = records.map(record => ({
      id: record.id,
      name: record.name,
      ...record.custom_object_fields 
    }));

    const titleEl = document.getElementById('table-title');
    if (titleEl) {
      titleEl.innerText = currentCoTitle || currentCoKey;
    }

    const summaryEl = document.getElementById('record-summary');
    if (summaryEl) {
      summaryEl.innerHTML = `Showing <strong>${tableData.length}</strong> of <strong>${tableData.length}</strong> records`;
    }

    const columns = [
      { 
        title: "#", 
        field: "custom_rownum", 
        headerSort: false, 
        width: 50, 
        hozAlign: "center", 
        resizable: false,
        formatter: function(cell) {
          const allRows = cell.getTable().getData("active");
          const rowId = cell.getData().id;
          const index = allRows.findIndex(r => r.id === rowId);
          return index >= 0 ? index + 1 : '';
        }
      },
      { title: "ID", field: "id", width: 80 },
      { title: "Record Name", field: "name", minWidth: 150 }
    ];

    currentSchema.forEach((field) => {
      let colDef = { title: field.title, field: field.key };

      if (field.type === 'checkbox') {
        colDef.width = 100; 
        colDef.hozAlign = "center";
        colDef.formatter = "tickCross"; 
      } else if (field.type === 'date') {
        colDef.width = 120; 
      } else if (field.type === 'integer' || field.type === 'decimal') {
        colDef.width = 110; 
        colDef.hozAlign = "right";
      } else {
        colDef.minWidth = 150; 
      }

      columns.push(colDef);
    });

    columns.push({
      title: "Actions",
      field: "actions", 
      formatter: function() {
        return `<button class="btn-edit">Edit</button><button class="btn-danger">Delete</button>`;
      },
      width: 140, 
      headerSort: false,
      cellClick: function(e, cell) {
        const rowData = cell.getRow().getData();
        if(e.target.classList.contains('btn-edit')) {
          showForm(rowData);
        } else if (e.target.classList.contains('btn-danger')) {
          deleteRecord(rowData.id);
        }
      }
    });

    switchView('table');
    document.getElementById('table-search').value = ''; 

    const containerWidth = document.getElementById('table-view').clientWidth || window.innerWidth;
    const availableWidth = containerWidth - 40; 
    let usedWidth = 190; 
    
    const defaultVisibleCols = [];

    columns.forEach(col => {
      if (col.field === 'actions' || col.field === 'custom_rownum') {
        col.visible = true;
        return;
      }

      let colW = col.width || col.minWidth || 150;
      
      if (usedWidth + colW <= availableWidth || defaultVisibleCols.length < 2) {
         col.visible = true;
         defaultVisibleCols.push(col.field);
         usedWidth += colW;
      } else {
         col.visible = false;
      }
    });

    if(tabulatorTable) {
      tabulatorTable.destroy(); 
    }

    tabulatorTable = new Tabulator("#records-table", {
      data: tableData,
      layout: "fitColumns", 
      pagination: "local",
      paginationSize: 15,
      columns: columns,
    });

    tabulatorTable.on("dataFiltered", function(filters, rows) {
      const liveSummaryEl = document.getElementById('record-summary');
      if (liveSummaryEl) {
        liveSummaryEl.innerHTML = `Showing <strong>${rows.length}</strong> of <strong>${tableData.length}</strong> records`;
      }
      setTimeout(() => {
        if (tabulatorTable) tabulatorTable.redraw(true);
      }, 10);
    });

    tabulatorTable.on("dataSorted", function(sorters, rows) {
      setTimeout(() => {
        if (tabulatorTable) tabulatorTable.redraw(true);
      }, 10);
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
      plugins: ['remove_button'],
      hidePlaceholder: true,
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
      }
    });

    renderFilterBar(columns, coKey);

  } catch (error) {
    console.error("Error loading table data:", error);
    updateLoaderText("Error loading table data.");
  }
}

async function deleteRecord(recordId) {
  const overlay = document.getElementById('delete-modal-overlay');
  const titleEl = document.getElementById('delete-modal-title');
  const bodyEl = document.getElementById('delete-modal-body');
  const confirmBtn = document.getElementById('delete-modal-confirm');
  const cancelBtn = document.getElementById('delete-modal-cancel');

  const close = () => { overlay.style.display = 'none'; };

  // Show modal in loading state immediately
  titleEl.innerText = 'Checking for related records...';
  bodyEl.innerHTML = `<div style="text-align: center; padding: 20px;">
                        <p style="color: #68737d; margin-bottom: 12px;">Scanning for linked data...</p>
                        <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
                      </div>`;
  confirmBtn.disabled = true;
  cancelBtn.onclick = close;
  overlay.onclick = (e) => { if (e.target === overlay) close(); };
  overlay.style.display = 'flex';

  // Scan for related records using the existing relationship engine
  let totalRelated = 0;
  let relatedHtml = '';
  try {
    const fields = await getLookupFieldsForCurrentCo();
    for (const field of fields) {
      const endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${recordId}/relationship_fields/${field.id}/${field.type}`;
      try {
        const response = await client.request(endpoint);
        let dataKey = '';
        let displayField = 'name';
        if (field.type === 'zen:ticket') { dataKey = 'tickets'; displayField = 'subject'; }
        else if (field.type === 'zen:user') { dataKey = 'users'; displayField = 'name'; }
        else if (field.type === 'zen:organization') { dataKey = 'organizations'; displayField = 'name'; }
        else if (field.type.startsWith('zen:custom_object:')) { dataKey = 'custom_object_records'; displayField = 'name'; }

        const records = response[dataKey] || [];
        if (records.length > 0) {
          totalRelated += records.length;
          relatedHtml += `<div class="related-section">
                            <h4>${escapeHtml(field.label)} <span>(via field: ${escapeHtml(field.title)})</span></h4>
                            <ul class="related-list">`;
          records.forEach(r => {
            let nameText = r[displayField] || r.title || `Record #${r.id}`;
            if (nameText.trim() === '') nameText = `[No Name] Record #${r.id}`;
            relatedHtml += `<li>
                               <span>${escapeHtml(nameText)}</span>
                               <span class="badge-id">ID: ${escapeHtml(r.id)}</span>
                             </li>`;
          });
          if (response.meta && response.meta.has_more) {
            relatedHtml += `<li><span style="color:#1f73b7; font-size:12px;">+ More records exist (not all shown)</span></li>`;
          }
          relatedHtml += `</ul></div>`;
        }
      } catch (err) {
        console.warn(`Could not check relationships for field ${field.id}`, err);
      }
    }
  } catch (err) {
    console.warn('Could not scan for related records', err);
  }

  // Populate modal based on findings
  if (totalRelated > 0) {
    titleEl.innerText = `Warning: ${totalRelated} linked item${totalRelated !== 1 ? 's' : ''} will be affected`;
    bodyEl.innerHTML = `
      <div class="delete-warning">
        <p>Deleting this record will remove its reference from <strong>${totalRelated} linked item${totalRelated !== 1 ? 's' : ''}</strong> in Zendesk. Those items will not be deleted, but they will lose their link to this record.</p>
      </div>
      ${relatedHtml}
      <p style="margin-top: 16px; font-weight: 600; color: #2f3941;">Are you sure you want to permanently delete this record?</p>
    `;
  } else {
    titleEl.innerText = 'Confirm Deletion';
    bodyEl.innerHTML = `<p>Are you sure you want to delete record <strong>${escapeHtml(String(recordId))}</strong>? This cannot be undone.</p>`;
  }

  confirmBtn.disabled = false;
  confirmBtn.onclick = async () => {
    close();
    updateLoaderText(`Deleting record ${recordId}...`);
    switchView('loader');
    try {
      await client.request({
        url: `/api/v2/custom_objects/${currentCoKey}/records/${recordId}`,
        type: 'DELETE'
      });
      await loadTable(currentCoKey);
    } catch (error) {
      console.error('Error deleting record:', error);
      alert('Failed to delete record.');
      switchView('table');
    }
  };
}

// ----------------------------------------------------
// VIEW 3: DYNAMIC FORM & RELATED RECORDS TAB
// ----------------------------------------------------
async function showForm(existingRecord = null) {
  updateLoaderText("Building form...");
  switchView('loader');
  
  const isEdit = existingRecord !== null;
  const formTitle = isEdit ? `Edit Record ${escapeHtml(existingRecord.id)}` : `Create new ${escapeHtml(currentCoKey)} record`;

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
          <button type="button" class="tab-btn active" id="tab-details">Details</button>
          <button type="button" class="tab-btn" id="tab-related">Related Records</button>
        </div>
      `;
    }

    formHtml += `<div id="tab-content-details">
                   <form id="dynamic-form">
                     <input type="hidden" name="record_id" value="${isEdit ? escapeHtml(existingRecord.id) : ''}" />
                     
                     <div class="form-actions-top">
                       <button type="submit" class="btn">${isEdit ? 'Update Record' : 'Save Record'}</button>
                       <button type="button" class="btn btn-secondary" id="btn-cancel-form">Cancel</button>
                     </div>

                     <div class="form-group">
                       <label>Record Name</label>
                       <input type="text" name="name" value="${escapeHtml(existingName)}" required />
                     </div>`;

    const lookupFieldIds = [];

    for (const field of currentSchema) {
      let fieldValue = '';
      if (isEdit && existingRecord[field.key] !== null && existingRecord[field.key] !== undefined) {
        fieldValue = existingRecord[field.key];
      }
      
      formHtml += `<div class="form-group">`;

      if (field.type === 'text') {
        formHtml += `<label>${escapeHtml(field.title)}</label>
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
                       <option value="">-- Select ${escapeHtml(field.title)} --</option>`;
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

        let initialLabel = `Record ${fieldValue}`;
        if (fieldValue) {
           initialLabel = await fetchSingleRecordName(field.relationship_target_type, fieldValue);
        }

        lookupFieldIds.push({
           id: selectId,
           targetType: field.relationship_target_type
        });

        formHtml += `<select id="${selectId}" name="${escapeHtml(field.key)}" ${field.required ? 'required' : ''} placeholder="Type to search ${escapeHtml(field.title)}...">`;
        if (fieldValue) {
            formHtml += `<option value="${escapeHtml(fieldValue)}" selected>${escapeHtml(initialLabel)}</option>`;
        } else {
            formHtml += `<option value="">-- Type to search --</option>`;
        }
        formHtml += `</select>`;
      }
      else {
        formHtml += `<label>${escapeHtml(field.title)}</label>
                     <input type="text" name="${escapeHtml(field.key)}" value="${escapeHtml(fieldValue)}" placeholder="Type: ${escapeHtml(field.type)}" />`;
      }
      formHtml += `</div>`;
    }

    formHtml += `</form></div>`; 

    if (isEdit) {
      formHtml += `<div id="tab-content-related" style="display: none;"></div>`;
    }

    views.form.innerHTML = formHtml;
    switchView('form');

    lookupFieldIds.forEach(lookup => {
      new TomSelect(`#${lookup.id}`, {
        valueField: 'id',
        labelField: 'label',
        searchField: 'label',
        maxOptions: 50, 
        load: function(query, callback) {
          if (!query.length) return callback();
          searchLookupData(lookup.targetType, query)
            .then(results => callback(results))
            .catch(() => callback());
        }
      });
    });

    document.getElementById('btn-cancel-form').addEventListener('click', () => switchView('table'));
    document.getElementById('dynamic-form').addEventListener('submit', handleFormSubmit);

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
           loadRelatedRecords(existingRecord.id);
        }
      });
    }

  } catch (error) {
    views.form.innerHTML = `<p>Error loading form.</p>`;
    console.error(error);
  }
}

// ----------------------------------------------------
// NEW: RELATED RECORDS DISCOVERY ENGINE
// ----------------------------------------------------

// Scans Zendesk to find ALL fields that point to this Custom Object
// UPDATED: Now uses fetchAllPages to prevent silent pagination limits from hiding fields
async function getLookupFieldsForCurrentCo() {
  if (cachedLookupFields) return cachedLookupFields;
  const target = `zen:custom_object:${currentCoKey}`;
  const fields = [];

  try {
    const ticketFields = await fetchAllPages('/api/v2/ticket_fields.json', 'ticket_fields');
    ticketFields.forEach(f => {
      if (f.type === 'lookup' && f.relationship_target_type === target) {
        fields.push({ id: f.id, title: f.title, type: 'zen:ticket', label: 'Tickets' });
      }
    });
  } catch(e) { console.warn("Could not load ticket fields", e); }

  try {
    const userFields = await fetchAllPages('/api/v2/user_fields.json', 'user_fields');
    userFields.forEach(f => {
      if (f.type === 'lookup' && f.relationship_target_type === target) {
        fields.push({ id: f.id, title: f.title, type: 'zen:user', label: 'Users' });
      }
    });
  } catch(e) { console.warn("Could not load user fields", e); }

  try {
    const orgFields = await fetchAllPages('/api/v2/organization_fields.json', 'organization_fields');
    orgFields.forEach(f => {
      if (f.type === 'lookup' && f.relationship_target_type === target) {
        fields.push({ id: f.id, title: f.title, type: 'zen:organization', label: 'Organizations' });
      }
    });
  } catch(e) { console.warn("Could not load org fields", e); }

  try {
    const customObjects = await fetchAllPages('/api/v2/custom_objects.json', 'custom_objects');
    for (const obj of customObjects) {
      try {
        const coFields = await fetchAllPages(`/api/v2/custom_objects/${obj.key}/fields.json`, 'custom_object_fields');
        coFields.forEach(f => {
          if (f.type === 'lookup' && f.relationship_target_type === target) {
            // Note: Custom object fields use their string 'key' rather than an integer 'id'
            fields.push({ id: f.key, title: f.title, type: `zen:custom_object:${obj.key}`, label: obj.title_pluralized });
          }
        });
      } catch(e) { console.warn(`Could not load fields for CO ${obj.key}`, e); }
    }
  } catch(e) { console.warn("Could not load COs", e); }

  cachedLookupFields = fields;
  return fields;
}

async function loadRelatedRecords(recordId) {
  const container = document.getElementById('tab-content-related');
  
  container.innerHTML = `<div style="text-align: center; padding: 40px;">
                           <p style="color: #68737d; margin-bottom: 15px;">Scanning Zendesk for relationships...</p>
                           <div class="progress-container"><div class="progress-bar-indeterminate"></div></div>
                         </div>`;

  const fields = await getLookupFieldsForCurrentCo();
  
  if (fields.length === 0) {
    container.innerHTML = '<p style="color: #68737d; padding: 20px;">There are no lookup fields in this Zendesk account pointing to this Custom Object.</p>';
    return;
  }

  let html = '';
  let foundAny = false;

  for (const field of fields) {
    const endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${recordId}/relationship_fields/${field.id}/${field.type}`;
    try {
      const response = await client.request(endpoint);
      
      let dataKey = '';
      let displayField = 'name';

      if (field.type === 'zen:ticket') { dataKey = 'tickets'; displayField = 'subject'; }
      else if (field.type === 'zen:user') { dataKey = 'users'; displayField = 'name'; }
      else if (field.type === 'zen:organization') { dataKey = 'organizations'; displayField = 'name'; }
      else if (field.type.startsWith('zen:custom_object:')) { dataKey = 'custom_object_records'; displayField = 'name'; }

      const records = response[dataKey] || [];
      if (records.length > 0) {
        foundAny = true;
        html += `<div class="related-section">
                   <h4>${escapeHtml(field.label)} <span>(via field: ${escapeHtml(field.title)})</span></h4>
                   <ul class="related-list">`;
        
        records.forEach(r => {
          let nameText = r[displayField] || r.title || `Record #${r.id}`;
          if (nameText.trim() === '') nameText = `[No Name] Record #${r.id}`;
          
          html += `<li>
                     <span>${escapeHtml(nameText)}</span>
                     <span class="badge-id">ID: ${escapeHtml(r.id)}</span>
                   </li>`;
        });

        if (response.meta && response.meta.has_more) {
           html += `<li><span style="color:#1f73b7; font-size:12px;">+ More records exist (First 100 shown)</span></li>`;
        }

        html += `</ul></div>`;
      }
    } catch (err) {
      console.warn(`Failed to fetch related records for field ${field.id}`, err);
    }
  }

  if (!foundAny) {
    html = `<p style="color: #68737d; padding: 20px;">No active records are currently linked to this item.</p>`;
  }

  container.innerHTML = html;
}

async function handleFormSubmit(event) {
  event.preventDefault();
  const formMsg = document.getElementById('form-msg');
  const submitBtn = event.target.querySelector('button[type="submit"]');
  
  formMsg.innerHTML = "<span style='color: #1f73b7; font-weight: bold;'>Saving...</span>";
  submitBtn.disabled = true;

  const formData = new FormData(event.target);
  const customObjectFields = {};
  
  let recordName = formData.get('name') || "New Record";
  let recordId = formData.get('record_id');

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
      custom_object_fields: customObjectFields
    }
  };

  try {
    const isEdit = recordId !== '';
    const url = isEdit 
      ? `/api/v2/custom_objects/${currentCoKey}/records/${recordId}` 
      : `/api/v2/custom_objects/${currentCoKey}/records`;
    const method = isEdit ? 'PATCH' : 'POST';

    await client.request({
      url: url,
      type: method,
      contentType: 'application/json',
      data: JSON.stringify(payload)
    });
    
    await loadTable(currentCoKey);

  } catch (error) {
    formMsg.innerHTML = "<span style='color: red;'>Failed to save record. Check console.</span>";
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
  const searchTerm = (document.getElementById('table-search')?.value || '').trim();
  const logic = document.querySelector('input[name="filter-logic"]:checked')?.value || 'and';

  if (!searchTerm && activeFilters.length === 0) {
    tabulatorTable.clearFilter();
    return;
  }
  tabulatorTable.setFilter(function(data) {
    // Global search always narrows results (AND with advanced filters)
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
    // Different CO: clear filter state and DOM rows
    activeFilters = [];
    document.getElementById('filter-rows').innerHTML = '';
    lastFilterCoKey = coKey;
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
  };

  document.getElementById('btn-clear-filters').onclick = () => {
    activeFilters = [];
    document.getElementById('filter-rows').innerHTML = '';
    applyTableFilters();
    updateFilterBadge();
  };
}

function addFilterRow() {
  const colOptions = filterColumns.map(c =>
    `<option value="${escapeHtml(c.field)}">${escapeHtml(c.title)}</option>`
  ).join('');

  const rowEl = document.createElement('div');
  rowEl.className = 'filter-row';
  rowEl.innerHTML = `
    <select class="filter-field">${colOptions}</select>
    <select class="filter-operator">
      <option value="eq">= equals</option>
      <option value="neq">≠ not equals</option>
      <option value="empty">is empty / null</option>
      <option value="notempty">is not empty</option>
      <option value="true">is true / yes</option>
      <option value="false">is false / no / null</option>
      <option value="gt">&gt; greater than</option>
      <option value="lt">&lt; less than</option>
      <option value="gte">≥ greater or equal</option>
      <option value="lte">≤ less or equal</option>
    </select>
    <input type="text" class="filter-value" placeholder="value, prefix*, *suffix, *contains*" />
    <button type="button" class="filter-row-remove" title="Remove condition">×</button>
  `;

  const operatorEl = rowEl.querySelector('.filter-operator');
  const valueEl    = rowEl.querySelector('.filter-value');
  operatorEl.addEventListener('change', () => {
    valueEl.style.visibility = NO_VALUE_OPS.has(operatorEl.value) ? 'hidden' : 'visible';
  });

  rowEl.querySelector('.filter-row-remove').onclick = () => {
    rowEl.remove();
    collectFiltersFromDOM();
    applyTableFilters();
    updateFilterBadge();
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
// CSV EXPORT
// ----------------------------------------------------

function showExportModal() {
  if (!tabulatorTable) return;

  const overlay    = document.getElementById('export-modal-overlay');
  const infoEl     = document.getElementById('export-modal-info');
  const activeRows = tabulatorTable.getData('active').length;
  const totalRows  = tabulatorTable.getData().length;
  const note       = activeRows < totalRows ? ` (filtered from ${totalRows} total)` : '';

  infoEl.innerHTML = `<strong>${activeRows} row${activeRows !== 1 ? 's' : ''}</strong> will be exported${note}.`;

  const close = () => { overlay.style.display = 'none'; };
  document.getElementById('export-modal-cancel').onclick = close;
  overlay.onclick = (e) => { if (e.target === overlay) close(); };

  document.getElementById('export-btn-visible').onclick = () => { close(); buildAndDownloadCSV(false); };
  document.getElementById('export-btn-all').onclick     = () => { close(); buildAndDownloadCSV(true);  };

  overlay.style.display = 'flex';
}

function buildAndDownloadCSV(allColumns) {
  const data = tabulatorTable.getData('active');

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
}

function csvEscape(val) {
  return `"${String(val).replace(/"/g, '""').replace(/\r?\n/g, ' ')}"`;
}

// ----------------------------------------------------
// UTILITIES: PAGINATION & SERVER-SIDE SEARCH
// ----------------------------------------------------

async function fetchAllPages(initialEndpoint, dataKey, progressCallback) {
  let allRecords = [];
  let currentEndpoint = initialEndpoint;

  while (currentEndpoint) {
    try {
      const response = await client.request(currentEndpoint);
      if (response[dataKey]) {
        allRecords = allRecords.concat(response[dataKey]);
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
      currentEndpoint = null; 
    }
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
    endpoint = `/api/v2/search.json?query=type:ticket ${encodeURIComponent(query)}`;
    dataKey = 'results';
    labelField = 'subject'; 
  } else if (targetType === 'zen:organization') {
    endpoint = `/api/v2/organizations/autocomplete.json?name=${encodeURIComponent(query)}`;
    dataKey = 'organizations';
  } else {
    return [];
  }

  try {
    const response = await client.request(endpoint);
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
    return `Record ${id}`;
  }

  try {
    const response = await client.request(endpoint);
    const record = response[dataKey];
    return record[labelField] || record.title || `Record ${record.id}`;
  } catch (e) {
    return `Record ${id}`;
  }
}