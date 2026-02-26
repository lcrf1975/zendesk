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
  
  // Tabulator Global Search
  document.getElementById('table-search').addEventListener('input', function(e) {
    if(tabulatorTable) {
      const searchTerm = e.target.value.trim();
      if (searchTerm === "") {
        tabulatorTable.clearFilter(); 
      } else {
        tabulatorTable.setFilter(customFilter, searchTerm);
      }
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
  let match = false;
  for (let key in data) {
    if (String(data[key]).toLowerCase().includes(filterParams.toLowerCase())) {
      match = true;
    }
  }
  return match;
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
    html += `<option value="${obj.key}">${obj.title_pluralized}</option>`;
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
          const activeData = cell.getTable().getData("active"); 
          const rowId = cell.getData().id;
          const index = activeData.findIndex(d => d.id === rowId);
          return index >= 0 ? index + 1 : "";
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
    colSelectEl.innerHTML = '';
    columns.forEach(col => {
      if (col.field !== 'actions' && col.field !== 'custom_rownum') {
        const isSelected = col.visible ? 'selected' : '';
        colSelectEl.innerHTML += `<option value="${col.field}" ${isSelected}>${col.title}</option>`;
      }
    });

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

  } catch (error) {
    console.error("Error loading table data:", error);
    updateLoaderText("Error loading table data.");
  }
}

async function deleteRecord(recordId) {
  if (confirm(`Are you sure you want to delete record ${recordId}? This cannot be undone.`)) {
    updateLoaderText(`Deleting record ${recordId}...`);
    switchView('loader');
    try {
      await client.request({
        url: `/api/v2/custom_objects/${currentCoKey}/records/${recordId}`,
        type: 'DELETE'
      });
      await loadTable(currentCoKey); 
    } catch (error) {
      console.error("Error deleting record:", error);
      alert("Failed to delete record.");
      switchView('table');
    }
  }
}

// ----------------------------------------------------
// VIEW 3: DYNAMIC FORM & RELATED RECORDS TAB
// ----------------------------------------------------
async function showForm(existingRecord = null) {
  updateLoaderText("Building form...");
  switchView('loader');
  
  const isEdit = existingRecord !== null;
  const formTitle = isEdit ? `Edit Record ${existingRecord.id}` : `Create new ${currentCoKey} record`;

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
                     <input type="hidden" name="record_id" value="${isEdit ? existingRecord.id : ''}" />
                     
                     <div class="form-actions-top">
                       <button type="submit" class="btn">${isEdit ? 'Update Record' : 'Save Record'}</button>
                       <button type="button" class="btn btn-secondary" id="btn-cancel-form">Cancel</button>
                     </div>

                     <div class="form-group">
                       <label>Record Name</label>
                       <input type="text" name="name" value="${existingName}" required />
                     </div>`;

    const lookupFieldIds = [];

    for (const field of currentSchema) {
      let fieldValue = '';
      if (isEdit && existingRecord[field.key] !== null && existingRecord[field.key] !== undefined) {
        fieldValue = existingRecord[field.key];
      }
      
      formHtml += `<div class="form-group">`;

      if (field.type === 'text') {
        formHtml += `<label>${field.title}</label>
                     <input type="text" name="${field.key}" value="${fieldValue}" required="${field.required}" />`;
      } 
      else if (field.type === 'textarea') {
        formHtml += `<label>${field.title}</label>
                     <textarea name="${field.key}" required="${field.required}" rows="3">${fieldValue}</textarea>`;
      }
      else if (field.type === 'integer') {
        formHtml += `<label>${field.title}</label>
                     <input type="number" step="1" name="${field.key}" value="${fieldValue}" required="${field.required}" />`;
      }
      else if (field.type === 'decimal') {
        formHtml += `<label>${field.title}</label>
                     <input type="number" step="any" name="${field.key}" value="${fieldValue}" required="${field.required}" />`;
      }
      else if (field.type === 'date') {
        const dateVal = fieldValue ? String(fieldValue).substring(0, 10) : '';
        formHtml += `<label>${field.title}</label>
                     <input type="date" name="${field.key}" value="${dateVal}" required="${field.required}" />`;
      }
      else if (field.type === 'checkbox') {
        const isChecked = (fieldValue === true || fieldValue === 'true') ? 'checked' : '';
        formHtml += `<label>
                       <input type="checkbox" name="${field.key}" value="true" ${isChecked} />
                       <span class="checkbox-label">${field.title}</span>
                     </label>`;
      }
      else if (field.type === 'dropdown') {
        formHtml += `<label>${field.title}</label>
                     <select name="${field.key}" required="${field.required}">
                       <option value="">-- Select ${field.title} --</option>`;
        if (field.custom_field_options) {
          field.custom_field_options.forEach(opt => {
            if (opt.active !== false || fieldValue === opt.value) {
              const selected = (fieldValue === opt.value) ? 'selected' : '';
              formHtml += `<option value="${opt.value}" ${selected}>${opt.name}</option>`;
            }
          });
        }
        formHtml += `</select>`;
      }
      else if (field.type === 'lookup') {
        formHtml += `<label>${field.title}</label>`;
        const selectId = `lookup-${field.key}`;
        
        let initialLabel = `Record ${fieldValue}`;
        if (fieldValue) {
           initialLabel = await fetchSingleRecordName(field.relationship_target_type, fieldValue);
        }

        lookupFieldIds.push({
           id: selectId,
           targetType: field.relationship_target_type
        });

        formHtml += `<select id="${selectId}" name="${field.key}" required="${field.required}" placeholder="Type to search ${field.title}...">`;
        if (fieldValue) {
            formHtml += `<option value="${fieldValue}" selected>${initialLabel}</option>`;
        } else {
            formHtml += `<option value="">-- Type to search --</option>`;
        }
        formHtml += `</select>`;
      } 
      else {
        formHtml += `<label>${field.title}</label>
                     <input type="text" name="${field.key}" value="${fieldValue}" placeholder="Type: ${field.type}" />`;
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
                   <h4>${field.label} <span>(via field: ${field.title})</span></h4>
                   <ul class="related-list">`;
        
        records.forEach(r => {
          let nameText = r[displayField] || r.title || `Record #${r.id}`;
          if (nameText.trim() === '') nameText = `[No Name] Record #${r.id}`;
          
          html += `<li>
                     <span>${nameText}</span>
                     <span class="badge-id">ID: ${r.id}</span>
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