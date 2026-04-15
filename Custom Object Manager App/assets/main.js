/* global ZAFClient */
// Initialize the ZAF Client
const client = ZAFClient.init();

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
let zendeskBaseUrl = '';       // Zendesk base URL for constructing item links
let formIsDirty = false;       // Tracks unsaved changes in the active form
let rowNumMap = null;          // id -> consecutive # built from dataFiltered/dataSorted rows
let _rnRedrawing = false;      // guard: prevents cascade if redraw(true) fires dataFiltered
let _loaderTimer = null;       // elapsed timer for the #loader view
let _preScanTimer = null;      // elapsed timer for "Discovering..." phase before fullReferenceScan

// ============================================================
// INTERNATIONALISATION
// ============================================================
const TRANSLATIONS = {
  en: {
    'loader.customObjects': 'Loading Custom Objects...',
    'loader.schema': 'Loading schema...', 'loader.records': 'Loading records...',
    'loader.rendering': 'Rendering table...', 'loader.form': 'Building form...',
    'loader.error.customObjects': 'Error loading Custom Objects. Check your permissions.',
    'loader.error.table': 'Error loading table data.',
    'selector.title': 'Select a Custom Object', 'selector.placeholder': '-- Choose an Object --',
    'selector.button': 'Load Object Dashboard',
    'table.addRecord': 'Add New Record', 'table.advancedFilter': 'Advanced Filter',
    'table.exportCsv': 'Export CSV', 'table.changeObject': 'Change Object',
    'table.searchPlaceholder': 'Search all fields...', 'table.restoreColumns': 'Restore Columns',
    'table.selectColumns': 'Select visible columns...',
    'summary.showing': 'Showing', 'summary.of': 'of', 'summary.records': 'records',
    'summary.loadingMore': '(only the first 100 records are shown while loading)',
    'col.id': 'ID', 'col.name': 'Record Name', 'col.actions': 'Actions',
    'row.edit': 'Edit', 'row.delete': 'Delete',
    'filter.title': 'Advanced Filters', 'filter.match': 'Match:',
    'filter.and': 'ALL conditions (AND)', 'filter.or': 'ANY condition (OR)',
    'filter.hint': '* wildcard: abc* starts with  ·  *xyz ends with  ·  *text* contains  ·  Numeric/date operators use exact values',
    'filter.addCondition': '+ Add Condition', 'filter.apply': 'Apply Filter', 'filter.clearAll': 'Clear All',
    'filter.valuePlaceholder': 'value, prefix*, *suffix, *contains*',
    'op.eq': '= equals', 'op.neq': '≠ not equals', 'op.empty': 'is empty / null',
    'op.notempty': 'is not empty', 'op.true': 'is true / yes', 'op.false': 'is false / no / null',
    'op.gt': '> greater than', 'op.lt': '< less than', 'op.gte': '≥ greater or equal', 'op.lte': '≤ less or equal',
    'form.editTitle': 'Edit Record {id}', 'form.createTitle': 'Create new {key} record',
    'form.tabDetails': 'Details', 'form.tabUsage': 'Usage & Impact',
    'form.updateButton': 'Update Record', 'form.saveButton': 'Save Record', 'form.cancel': 'Cancel',
    'form.recordName': 'Record Name', 'form.selectPlaceholder': '-- Select {field} --',
    'form.lookupPlaceholder': '-- Type to search --', 'form.lookupSearch': 'Type to search {field}...',
    'form.saving': 'Saving...', 'form.saveError': 'Failed to save record. Check console.',
    'form.loadError': 'Error loading form.', 'form.recordFallback': 'Record {id}',
    'delete.scanning': 'Scanning for linked data...', 'delete.discovering': 'Discovering lookup fields...',
    'delete.starting': 'Starting scan...', 'delete.checked': 'Checked: {label}',
    'delete.scanTriggers': 'Scanning triggers', 'delete.scanAutomations': 'Scanning automations',
    'delete.scanViews': 'Scanning views', 'delete.scanSla': 'Scanning SLA policies',
    'delete.warningTitle': 'Warning: "{name}" is actively used',
    'delete.warningBody': 'Deleting it will clear linked references and may break rule conditions.',
    'delete.linkedData': 'Linked data', 'delete.ruleConditions': 'Rule conditions',
    'delete.viewUsage': 'View Usage & Impact',
    'delete.confirmQuestion': 'Are you sure you want to permanently delete this record?',
    'delete.confirmTitle': 'Confirm Deletion',
    'delete.confirmBody': 'Are you sure you want to delete <strong>{name}</strong>? This cannot be undone.',
    'delete.error': 'Failed to delete record. Please try again.', 'delete.deleting': 'Deleting...',
    'delete.linkedItem': 'linked item', 'delete.linkedItems': 'linked items',
    'delete.ruleWord': 'rule', 'delete.rulesWord': 'rules', 'delete.ruleTypes': '(trigger/automation/view/SLA)',
    'delete.linkedItemsLoseRef': 'Linked items will lose their reference to this record.',
    'delete.rulesMayBreak': 'Rules with a condition referencing this record may behave unexpectedly.',
    'delete.checksComplete': '{done} / {total} checks complete',
    'usage.discovering': 'Discovering lookup fields...', 'usage.viaField': 'via field: {title}',
    'usage.conditionRef': 'condition references this record',
    'usage.noItems': 'No active records, rules, or configurations are currently linked to this item.',
    'usage.moreRecords': '+ More records exist (First 100 shown)',
    'export.title': 'Export to CSV',
    'export.rowSingular': '{n} row will be exported', 'export.rowPlural': '{n} rows will be exported',
    'export.rowSingularFiltered': '{n} row will be exported (filtered from {total} total)',
    'export.rowPluralFiltered': '{n} rows will be exported (filtered from {total} total)',
    'export.question': 'Which columns should be included?',
    'export.cancel': 'Cancel', 'export.visible': 'Visible Columns Only', 'export.all': 'All Fields',
    'rules.triggers': 'Triggers', 'rules.automations': 'Automations',
    'rules.views': 'Views', 'rules.sla': 'SLA Policies',
    'table.reverseLookup': 'Reverse Lookup',
    'reverseLookup.title': 'Reverse Lookup',
    'reverseLookup.description': 'Scan rule conditions to find which records of this type are referenced.',
    'reverseLookup.selectTypes': 'Select types to scan:',
    'reverseLookup.includeNames': 'Include name-based matches (may include false positives)',
    'reverseLookup.run': 'Run Lookup',
    'reverseLookup.scanning': 'Scanning rules...',
    'reverseLookup.found': '{n} record(s) found with references',
    'reverseLookup.noResults': 'No records of this type were found referenced in the selected rules.',
    'reverseLookup.runAgain': 'New Search',
    'reverseLookup.exactMatch': 'exact',
    'reverseLookup.nameMatch': 'name match',
    'reverseLookup.nameMatchNote': 'Results marked "name match" are based on similarity and may include false positives.',
    'reverseLookup.ticketFields': 'Ticket text fields',
    'reverseLookup.ticketLabel':  'Ticket',
    'reverseLookup.userFields':   'User text fields',
    'reverseLookup.orgFields':    'Organization text fields',
    'reverseLookup.textFieldWarning': 'Text field searches make ~{n} API requests. This may take a while for large datasets.',
    'reverseLookup.scope':         'Records to scan:',
    'reverseLookup.scopeAll':      'All records ({n})',
    'reverseLookup.scopeFiltered': 'Visible records only ({n})',
    'reverseLookup.stop':         'Stop',
    'reverseLookup.stopping':     'Stopping...',
    'reverseLookup.stopped':      'Scan stopped — results shown may be incomplete.',
    'reverseLookup.scopeNoFilter': 'Filtered records (apply a search or filter first)',
    'app.title': 'Custom Object Record Manager (Build 202603-47)',
    'table.dashboard': 'Dashboard',
    'table.searchLoading': 'Available after all records are loaded',
    'export.loadingWarning': 'This feature will be available after all records are loaded.',
    'form.unsavedTitle': 'Unsaved Changes',
    'form.unsavedBody': 'You have unsaved changes that will be lost if you leave.',
    'form.unsavedStay': 'Stay',
    'form.unsavedLeave': 'Leave',
    'usage.refresh': 'Refresh',
    'summary.loadError': '· loading interrupted, some records may be missing',
    'usage.openLink': 'Open in Zendesk',
    'rel.tickets':       'Tickets',
    'rel.users':         'Users',
    'rel.organizations': 'Organizations',
    'usage.possibleMatches': 'Possible Matches',
    'usage.possibleMatchesHint': 'Rules where a condition value contains this record\'s name. Verify manually.',
    'usage.possibleCondRef': 'name found in condition value',
    'delete.possibleMatches': 'possible matches (by name)',
    'table.findDuplicates': 'Find Duplicates',
    'findDuplicates.title': 'Find Duplicate Records',
    'findDuplicates.description': 'Finds records with identical or similar names (case-insensitive; spaces, underscores, and hyphens are treated as equivalent).',
    'findDuplicates.diffHint': 'Highlighted characters show differences between records in each group.',
    'findDuplicates.noResults': 'No duplicate or similar names found among all loaded records.',
    'findDuplicates.found': '{n} group(s) with duplicate or similar names — {total} records affected',
    'findDuplicates.groupCount': '{n} records',
    'findDuplicates.selectAll': 'Select all in group',
    'findDuplicates.noneSelected': 'Select records to delete',
    'findDuplicates.selectedCount': '{n} record(s) selected',
    'findDuplicates.deleteSelected': 'Delete Selected',
    'findDuplicates.deleteSelectedN': 'Delete Selected ({n})',
    'findDuplicates.confirmTitle': 'Delete {n} record(s)?',
    'findDuplicates.deleteWarning': 'This cannot be undone. Use Edit → Usage & Impact to check for linked data before deleting.',
    'findDuplicates.confirmDeleteBtn': 'Confirm Delete ({n})',
    'findDuplicates.deleting': 'Deleting {done} of {total}...',
    'findDuplicates.deleteSuccess': '{n} record(s) deleted successfully.',
    'findDuplicates.deleteErrors': '{n} record(s) could not be deleted',
    'findDuplicates.scanning': 'Checking dependencies {done} of {total}...',
    'findDuplicates.scanComplete': 'Scan complete — review before confirming',
    'findDuplicates.dependencies': 'Dependencies',
    'findDuplicates.noDeps': 'No dependencies',
    'findDuplicates.hasLinked': '{n} linked item(s)',
    'findDuplicates.hasRules': '{n} rule condition(s)',
    'findDuplicates.hasPossible': '~{n} possible match(es)',
    'findDuplicates.exactTitle': 'Exact Duplicates',
    'findDuplicates.exactHint': 'Records that are identical or differ only in case, separators, or diacritics.',
    'findDuplicates.similarTitle': 'Similar Names',
    'findDuplicates.similarHint': 'Records with similar names — different word order, or missing/extra words like articles and prepositions. Verify manually before deleting.',
    'findDuplicates.noExact': 'No exact duplicates found.',
  },

  'pt-BR': {
    'loader.customObjects': 'Carregando Objetos Customizados...',
    'loader.schema': 'Carregando esquema...', 'loader.records': 'Carregando registros...',
    'loader.rendering': 'Renderizando tabela...', 'loader.form': 'Preparando formulário...',
    'loader.error.customObjects': 'Erro ao carregar Objetos Customizados. Verifique suas permissões.',
    'loader.error.table': 'Erro ao carregar dados da tabela.',
    'selector.title': 'Selecione um Objeto Customizado', 'selector.placeholder': '-- Escolha um Objeto --',
    'selector.button': 'Carregar Dashboard',
    'table.addRecord': 'Adicionar Registro', 'table.advancedFilter': 'Filtro Avançado',
    'table.exportCsv': 'Exportar CSV', 'table.changeObject': 'Trocar Objeto',
    'table.searchPlaceholder': 'Pesquisar em todos os campos...', 'table.restoreColumns': 'Restaurar Colunas',
    'table.selectColumns': 'Selecione as colunas visíveis...',
    'summary.showing': 'Exibindo', 'summary.of': 'de', 'summary.records': 'registros',
    'summary.loadingMore': '(apenas os 100 primeiros registros sao exibidos durante o carregamento)',
    'col.id': 'ID', 'col.name': 'Nome do Registro', 'col.actions': 'Ações',
    'row.edit': 'Editar', 'row.delete': 'Excluir',
    'filter.title': 'Filtros Avançados', 'filter.match': 'Corresponder:',
    'filter.and': 'TODAS as condições (E)', 'filter.or': 'QUALQUER condição (OU)',
    'filter.hint': '* curinga: abc* começa com  ·  *xyz termina com  ·  *texto* contém  ·  Operadores numéricos/data usam valores exatos',
    'filter.addCondition': '+ Adicionar Condição', 'filter.apply': 'Aplicar Filtro', 'filter.clearAll': 'Limpar Tudo',
    'filter.valuePlaceholder': 'valor, prefixo*, *sufixo, *contém*',
    'op.eq': '= igual a', 'op.neq': '≠ diferente de', 'op.empty': 'está vazio / nulo',
    'op.notempty': 'não está vazio', 'op.true': 'é verdadeiro / sim', 'op.false': 'é falso / não / nulo',
    'op.gt': '> maior que', 'op.lt': '< menor que', 'op.gte': '≥ maior ou igual', 'op.lte': '≤ menor ou igual',
    'form.editTitle': 'Editar Registro {id}', 'form.createTitle': 'Criar novo registro em {key}',
    'form.tabDetails': 'Detalhes', 'form.tabUsage': 'Uso e Impacto',
    'form.updateButton': 'Atualizar Registro', 'form.saveButton': 'Salvar Registro', 'form.cancel': 'Cancelar',
    'form.recordName': 'Nome do Registro', 'form.selectPlaceholder': '-- Selecione {field} --',
    'form.lookupPlaceholder': '-- Digite para pesquisar --', 'form.lookupSearch': 'Digite para pesquisar {field}...',
    'form.saving': 'Salvando...', 'form.saveError': 'Falha ao salvar registro. Verifique o console.',
    'form.loadError': 'Erro ao carregar formulário.', 'form.recordFallback': 'Registro {id}',
    'delete.scanning': 'Verificando dados vinculados...', 'delete.discovering': 'Descobrindo campos de lookup...',
    'delete.starting': 'Iniciando verificação...', 'delete.checked': 'Verificado: {label}',
    'delete.scanTriggers': 'Verificando gatilhos', 'delete.scanAutomations': 'Verificando automações',
    'delete.scanViews': 'Verificando visões', 'delete.scanSla': 'Verificando políticas de SLA',
    'delete.warningTitle': 'Atenção: "{name}" está em uso',
    'delete.warningBody': 'Excluí-lo limpará as referências vinculadas e pode quebrar condições de regras.',
    'delete.linkedData': 'Dados vinculados', 'delete.ruleConditions': 'Condições de regras',
    'delete.viewUsage': 'Ver Uso e Impacto',
    'delete.confirmQuestion': 'Tem certeza que deseja excluir permanentemente este registro?',
    'delete.confirmTitle': 'Confirmar Exclusão',
    'delete.confirmBody': 'Tem certeza que deseja excluir <strong>{name}</strong>? Esta ação não pode ser desfeita.',
    'delete.error': 'Falha ao excluir registro. Tente novamente.', 'delete.deleting': 'Excluindo...',
    'delete.linkedItem': 'item vinculado', 'delete.linkedItems': 'itens vinculados',
    'delete.ruleWord': 'regra', 'delete.rulesWord': 'regras', 'delete.ruleTypes': '(gatilho/automação/visão/SLA)',
    'delete.linkedItemsLoseRef': 'Os itens vinculados perderão sua referência a este registro.',
    'delete.rulesMayBreak': 'Regras com condição referenciando este registro podem se comportar de forma inesperada.',
    'delete.checksComplete': '{done} / {total} verificações concluídas',
    'usage.discovering': 'Descobrindo campos de lookup...', 'usage.viaField': 'via campo: {title}',
    'usage.conditionRef': 'condição referencia este registro',
    'usage.noItems': 'Nenhum registro, regra ou configuração está vinculada a este item.',
    'usage.moreRecords': '+ Mais registros existem (Primeiros 100 exibidos)',
    'export.title': 'Exportar para CSV',
    'export.rowSingular': '{n} linha será exportada', 'export.rowPlural': '{n} linhas serão exportadas',
    'export.rowSingularFiltered': '{n} linha será exportada (filtrada de {total} no total)',
    'export.rowPluralFiltered': '{n} linhas serão exportadas (filtradas de {total} no total)',
    'export.question': 'Quais colunas devem ser incluídas?',
    'export.cancel': 'Cancelar', 'export.visible': 'Apenas Colunas Visíveis', 'export.all': 'Todos os Campos',
    'rules.triggers': 'Gatilhos', 'rules.automations': 'Automações',
    'rules.views': 'Visões', 'rules.sla': 'Políticas de SLA',
    'table.reverseLookup': 'Pesquisa Reversa',
    'reverseLookup.title': 'Pesquisa Reversa',
    'reverseLookup.description': 'Verifica condições de regras para encontrar quais registros deste tipo são referenciados.',
    'reverseLookup.selectTypes': 'Selecione os tipos para verificar:',
    'reverseLookup.includeNames': 'Incluir correspondências por nome (pode incluir falsos positivos)',
    'reverseLookup.run': 'Executar Pesquisa',
    'reverseLookup.scanning': 'Verificando regras...',
    'reverseLookup.found': '{n} registro(s) encontrado(s) com referências',
    'reverseLookup.noResults': 'Nenhum registro deste tipo foi encontrado nas regras selecionadas.',
    'reverseLookup.runAgain': 'Nova Pesquisa',
    'reverseLookup.exactMatch': 'exato',
    'reverseLookup.nameMatch': 'correspondência por nome',
    'reverseLookup.nameMatchNote': 'Resultados marcados como "correspondência por nome" são baseados em similaridade e podem incluir falsos positivos.',
    'reverseLookup.ticketFields': 'Campos de texto de tickets',
    'reverseLookup.ticketLabel':  'Ticket',
    'reverseLookup.userFields':   'Campos de texto de usuários',
    'reverseLookup.orgFields':    'Campos de texto de organizações',
    'reverseLookup.textFieldWarning': 'Pesquisas em campos de texto fazem ~{n} requisições. Pode demorar para grandes volumes.',
    'reverseLookup.scope':         'Registros para pesquisar:',
    'reverseLookup.scopeAll':      'Todos os registros ({n})',
    'reverseLookup.scopeFiltered': 'Apenas registros visíveis ({n})',
    'reverseLookup.stop':         'Parar',
    'reverseLookup.stopping':     'Parando...',
    'reverseLookup.stopped':      'Varredura interrompida — resultados exibidos podem estar incompletos.',
    'reverseLookup.scopeNoFilter': 'Registros filtrados (aplique uma pesquisa ou filtro primeiro)',
    'app.title': 'Gerenciador de Registros de Objetos Customizados',
    'table.dashboard': 'Painel',
    'table.searchLoading': 'Disponível após o carregamento de todos os registros',
    'export.loadingWarning': 'Esta funcionalidade ficará disponível após o carregamento de todos os registros.',
    'form.unsavedTitle': 'Alterações não salvas',
    'form.unsavedBody': 'Você tem alterações não salvas que serão perdidas se sair.',
    'form.unsavedStay': 'Ficar',
    'form.unsavedLeave': 'Sair',
    'usage.refresh': 'Atualizar',
    'summary.loadError': '· carregamento interrompido, alguns registros podem estar faltando',
    'usage.openLink': 'Abrir no Zendesk',
    'rel.tickets':       'Tickets',
    'rel.users':         'Usuários',
    'rel.organizations': 'Organizações',
    'usage.possibleMatches': 'Correspondências Possíveis',
    'usage.possibleMatchesHint': 'Regras com valor de condição contendo o nome deste registro. Verifique manualmente.',
    'usage.possibleCondRef': 'nome encontrado no valor da condição',
    'delete.possibleMatches': 'correspondências possíveis (por nome)',
    'table.findDuplicates': 'Encontrar Duplicatas',
    'findDuplicates.title': 'Encontrar Registros Duplicados',
    'findDuplicates.description': 'Encontra registros com nomes idênticos ou similares (sem distinção de maiúsculas/minúsculas; espaços, underscores e hífens são equivalentes).',
    'findDuplicates.diffHint': 'Caracteres destacados mostram diferenças entre os registros do grupo.',
    'findDuplicates.noResults': 'Nenhum nome duplicado ou similar encontrado nos registros carregados.',
    'findDuplicates.found': '{n} grupo(s) com nomes duplicados ou similares — {total} registros afetados',
    'findDuplicates.groupCount': '{n} registros',
    'findDuplicates.selectAll': 'Selecionar todos no grupo',
    'findDuplicates.noneSelected': 'Selecione registros para excluir',
    'findDuplicates.selectedCount': '{n} registro(s) selecionado(s)',
    'findDuplicates.deleteSelected': 'Excluir Selecionados',
    'findDuplicates.deleteSelectedN': 'Excluir Selecionados ({n})',
    'findDuplicates.confirmTitle': 'Excluir {n} registro(s)?',
    'findDuplicates.deleteWarning': 'Esta ação não pode ser desfeita. Use Editar → Uso e Impacto para verificar dados vinculados antes de excluir.',
    'findDuplicates.confirmDeleteBtn': 'Confirmar Exclusão ({n})',
    'findDuplicates.deleting': 'Excluindo {done} de {total}...',
    'findDuplicates.deleteSuccess': '{n} registro(s) excluído(s) com sucesso.',
    'findDuplicates.deleteErrors': '{n} registro(s) não puderam ser excluídos',
    'findDuplicates.scanning': 'Verificando dependências {done} de {total}...',
    'findDuplicates.scanComplete': 'Verificação concluída — revise antes de confirmar',
    'findDuplicates.dependencies': 'Dependências',
    'findDuplicates.noDeps': 'Sem dependências',
    'findDuplicates.hasLinked': '{n} item(ns) vinculado(s)',
    'findDuplicates.hasRules': '{n} condição(ões) de regra',
    'findDuplicates.hasPossible': '~{n} correspondência(s) possível(is)',
    'findDuplicates.exactTitle': 'Duplicatas Exatas',
    'findDuplicates.exactHint': 'Registros idênticos ou que diferem apenas em maiúsculas/minúsculas, separadores ou diacríticos.',
    'findDuplicates.similarTitle': 'Nomes Similares',
    'findDuplicates.similarHint': 'Registros com nomes similares — ordem diferente ou palavras extras/faltantes como artigos e preposições. Verifique manualmente antes de excluir.',
    'findDuplicates.noExact': 'Nenhuma duplicata exata encontrada.',
  },

  es: {
    'loader.customObjects': 'Cargando Objetos Personalizados...',
    'loader.schema': 'Cargando esquema...', 'loader.records': 'Cargando registros...',
    'loader.rendering': 'Renderizando tabla...', 'loader.form': 'Preparando formulario...',
    'loader.error.customObjects': 'Error al cargar Objetos Personalizados. Verifique sus permisos.',
    'loader.error.table': 'Error al cargar datos de la tabla.',
    'selector.title': 'Seleccione un Objeto Personalizado', 'selector.placeholder': '-- Elija un Objeto --',
    'selector.button': 'Cargar Panel',
    'table.addRecord': 'Agregar Registro', 'table.advancedFilter': 'Filtro Avanzado',
    'table.exportCsv': 'Exportar CSV', 'table.changeObject': 'Cambiar Objeto',
    'table.searchPlaceholder': 'Buscar en todos los campos...', 'table.restoreColumns': 'Restaurar Columnas',
    'table.selectColumns': 'Seleccione las columnas visibles...',
    'summary.showing': 'Mostrando', 'summary.of': 'de', 'summary.records': 'registros',
    'summary.loadingMore': '(solo los primeros 100 registros se muestran mientras se carga)',
    'col.id': 'ID', 'col.name': 'Nombre del Registro', 'col.actions': 'Acciones',
    'row.edit': 'Editar', 'row.delete': 'Eliminar',
    'filter.title': 'Filtros Avanzados', 'filter.match': 'Coincidir:',
    'filter.and': 'TODAS las condiciones (Y)', 'filter.or': 'CUALQUIER condición (O)',
    'filter.hint': '* comodín: abc* comienza con  ·  *xyz termina con  ·  *texto* contiene  ·  Operadores numéricos/fecha usan valores exactos',
    'filter.addCondition': '+ Agregar Condición', 'filter.apply': 'Aplicar Filtro', 'filter.clearAll': 'Limpiar Todo',
    'filter.valuePlaceholder': 'valor, prefijo*, *sufijo, *contiene*',
    'op.eq': '= igual a', 'op.neq': '≠ diferente de', 'op.empty': 'está vacío / nulo',
    'op.notempty': 'no está vacío', 'op.true': 'es verdadero / sí', 'op.false': 'es falso / no / nulo',
    'op.gt': '> mayor que', 'op.lt': '< menor que', 'op.gte': '≥ mayor o igual', 'op.lte': '≤ menor o igual',
    'form.editTitle': 'Editar Registro {id}', 'form.createTitle': 'Crear nuevo registro en {key}',
    'form.tabDetails': 'Detalles', 'form.tabUsage': 'Uso e Impacto',
    'form.updateButton': 'Actualizar Registro', 'form.saveButton': 'Guardar Registro', 'form.cancel': 'Cancelar',
    'form.recordName': 'Nombre del Registro', 'form.selectPlaceholder': '-- Seleccione {field} --',
    'form.lookupPlaceholder': '-- Escriba para buscar --', 'form.lookupSearch': 'Escriba para buscar {field}...',
    'form.saving': 'Guardando...', 'form.saveError': 'Error al guardar el registro. Revise la consola.',
    'form.loadError': 'Error al cargar el formulario.', 'form.recordFallback': 'Registro {id}',
    'delete.scanning': 'Verificando datos vinculados...', 'delete.discovering': 'Descubriendo campos de búsqueda...',
    'delete.starting': 'Iniciando verificación...', 'delete.checked': 'Verificado: {label}',
    'delete.scanTriggers': 'Verificando disparadores', 'delete.scanAutomations': 'Verificando automatizaciones',
    'delete.scanViews': 'Verificando vistas', 'delete.scanSla': 'Verificando políticas de SLA',
    'delete.warningTitle': 'Advertencia: "{name}" está en uso',
    'delete.warningBody': 'Eliminarlo borrará las referencias vinculadas y puede romper condiciones de reglas.',
    'delete.linkedData': 'Datos vinculados', 'delete.ruleConditions': 'Condiciones de reglas',
    'delete.viewUsage': 'Ver Uso e Impacto',
    'delete.confirmQuestion': '¿Está seguro de que desea eliminar permanentemente este registro?',
    'delete.confirmTitle': 'Confirmar Eliminación',
    'delete.confirmBody': '¿Está seguro de que desea eliminar <strong>{name}</strong>? Esta acción no se puede deshacer.',
    'delete.error': 'Error al eliminar el registro. Intente nuevamente.', 'delete.deleting': 'Eliminando...',
    'delete.linkedItem': 'elemento vinculado', 'delete.linkedItems': 'elementos vinculados',
    'delete.ruleWord': 'regla', 'delete.rulesWord': 'reglas', 'delete.ruleTypes': '(disparador/automatización/vista/SLA)',
    'delete.linkedItemsLoseRef': 'Los elementos vinculados perderán su referencia a este registro.',
    'delete.rulesMayBreak': 'Las reglas con condición que referencia este registro pueden comportarse de forma inesperada.',
    'delete.checksComplete': '{done} / {total} verificaciones completadas',
    'usage.discovering': 'Descubriendo campos de búsqueda...', 'usage.viaField': 'vía campo: {title}',
    'usage.conditionRef': 'condición referencia este registro',
    'usage.noItems': 'Ningún registro, regla o configuración está vinculada a este elemento.',
    'usage.moreRecords': '+ Existen más registros (Primeros 100 mostrados)',
    'export.title': 'Exportar a CSV',
    'export.rowSingular': '{n} fila será exportada', 'export.rowPlural': '{n} filas serán exportadas',
    'export.rowSingularFiltered': '{n} fila será exportada (filtrada de {total} en total)',
    'export.rowPluralFiltered': '{n} filas serán exportadas (filtradas de {total} en total)',
    'export.question': '¿Qué columnas deben incluirse?',
    'export.cancel': 'Cancelar', 'export.visible': 'Solo Columnas Visibles', 'export.all': 'Todos los Campos',
    'rules.triggers': 'Disparadores', 'rules.automations': 'Automatizaciones',
    'rules.views': 'Vistas', 'rules.sla': 'Políticas de SLA',
    'table.reverseLookup': 'Búsqueda Inversa',
    'reverseLookup.title': 'Búsqueda Inversa',
    'reverseLookup.description': 'Analiza condiciones de reglas para encontrar qué registros de este tipo son referenciados.',
    'reverseLookup.selectTypes': 'Seleccione los tipos a analizar:',
    'reverseLookup.includeNames': 'Incluir coincidencias por nombre (puede incluir falsos positivos)',
    'reverseLookup.run': 'Ejecutar Búsqueda',
    'reverseLookup.scanning': 'Analizando reglas...',
    'reverseLookup.found': '{n} registro(s) encontrado(s) con referencias',
    'reverseLookup.noResults': 'No se encontraron registros de este tipo en las reglas seleccionadas.',
    'reverseLookup.runAgain': 'Nueva Búsqueda',
    'reverseLookup.exactMatch': 'exacto',
    'reverseLookup.nameMatch': 'coincidencia por nombre',
    'reverseLookup.nameMatchNote': 'Los resultados marcados como "coincidencia por nombre" se basan en similitud y pueden incluir falsos positivos.',
    'reverseLookup.ticketFields': 'Campos de texto de tickets',
    'reverseLookup.ticketLabel':  'Ticket',
    'reverseLookup.userFields':   'Campos de texto de usuarios',
    'reverseLookup.orgFields':    'Campos de texto de organizaciones',
    'reverseLookup.textFieldWarning': 'Las búsquedas de campos de texto realizan ~{n} solicitudes. Puede tardar en conjuntos grandes.',
    'reverseLookup.scope':         'Registros a buscar:',
    'reverseLookup.scopeAll':      'Todos los registros ({n})',
    'reverseLookup.scopeFiltered': 'Solo registros visibles ({n})',
    'reverseLookup.stop':         'Parar',
    'reverseLookup.stopping':     'Deteniendo...',
    'reverseLookup.stopped':      'Búsqueda detenida — los resultados mostrados pueden estar incompletos.',
    'reverseLookup.scopeNoFilter': 'Registros filtrados (aplique una búsqueda o filtro primero)',
    'app.title': 'Administrador de Registros de Objetos Personalizados',
    'table.dashboard': 'Panel',
    'table.searchLoading': 'Disponible después de cargar todos los registros',
    'export.loadingWarning': 'Esta funcionalidad estará disponible después de cargar todos los registros.',
    'form.unsavedTitle': 'Cambios sin guardar',
    'form.unsavedBody': 'Tiene cambios sin guardar que se perderan si sale.',
    'form.unsavedStay': 'Quedarse',
    'form.unsavedLeave': 'Salir',
    'usage.refresh': 'Actualizar',
    'summary.loadError': '· carga interrumpida, algunos registros pueden faltar',
    'usage.openLink': 'Abrir en Zendesk',
    'rel.tickets':       'Tickets',
    'rel.users':         'Usuarios',
    'rel.organizations': 'Organizaciones',
    'usage.possibleMatches': 'Coincidencias Posibles',
    'usage.possibleMatchesHint': 'Reglas con valor de condición que contiene el nombre de este registro. Verifique manualmente.',
    'usage.possibleCondRef': 'nombre encontrado en el valor de condición',
    'delete.possibleMatches': 'coincidencias posibles (por nombre)',
    'table.findDuplicates': 'Buscar Duplicados',
    'findDuplicates.title': 'Buscar Registros Duplicados',
    'findDuplicates.description': 'Encuentra registros con nombres idénticos o similares (sin distinción de mayúsculas/minúsculas; espacios, guiones bajos y guiones son equivalentes).',
    'findDuplicates.diffHint': 'Los caracteres resaltados muestran diferencias entre los registros de cada grupo.',
    'findDuplicates.noResults': 'No se encontraron nombres duplicados o similares entre los registros cargados.',
    'findDuplicates.found': '{n} grupo(s) con nombres duplicados o similares — {total} registros afectados',
    'findDuplicates.groupCount': '{n} registros',
    'findDuplicates.selectAll': 'Seleccionar todos en el grupo',
    'findDuplicates.noneSelected': 'Seleccione registros para eliminar',
    'findDuplicates.selectedCount': '{n} registro(s) seleccionado(s)',
    'findDuplicates.deleteSelected': 'Eliminar Seleccionados',
    'findDuplicates.deleteSelectedN': 'Eliminar Seleccionados ({n})',
    'findDuplicates.confirmTitle': '¿Eliminar {n} registro(s)?',
    'findDuplicates.deleteWarning': 'Esta acción no se puede deshacer. Use Editar → Uso e Impacto para verificar datos vinculados antes de eliminar.',
    'findDuplicates.confirmDeleteBtn': 'Confirmar Eliminación ({n})',
    'findDuplicates.deleting': 'Eliminando {done} de {total}...',
    'findDuplicates.deleteSuccess': '{n} registro(s) eliminado(s) correctamente.',
    'findDuplicates.deleteErrors': '{n} registro(s) no pudieron ser eliminados',
    'findDuplicates.scanning': 'Verificando dependencias {done} de {total}...',
    'findDuplicates.scanComplete': 'Verificación completa — revise antes de confirmar',
    'findDuplicates.dependencies': 'Dependencias',
    'findDuplicates.noDeps': 'Sin dependencias',
    'findDuplicates.hasLinked': '{n} elemento(s) vinculado(s)',
    'findDuplicates.hasRules': '{n} condición(es) de regla',
    'findDuplicates.hasPossible': '~{n} coincidencia(s) posible(s)',
    'findDuplicates.exactTitle': 'Duplicados Exactos',
    'findDuplicates.exactHint': 'Registros idénticos o que difieren solo en mayúsculas/minúsculas, separadores o diacríticos.',
    'findDuplicates.similarTitle': 'Nombres Similares',
    'findDuplicates.similarHint': 'Registros con nombres similares — diferente orden de palabras o palabras extras/faltantes como artículos y preposiciones. Verifique manualmente antes de eliminar.',
    'findDuplicates.noExact': 'No se encontraron duplicados exactos.',
  },
};

let i18n = TRANSLATIONS.en;

// Returns the translated string for key, replacing {var} placeholders with vars
function t(key, vars = {}) {
  let str = i18n[key] ?? TRANSLATIONS.en[key] ?? key;
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
  if (locale.startsWith('pt'))      { i18n = TRANSLATIONS['pt-BR']; document.documentElement.lang = 'pt-BR'; }
  else if (locale.startsWith('es')) { i18n = TRANSLATIONS['es'];    document.documentElement.lang = 'es';   }
  else                              { i18n = TRANSLATIONS['en'];    document.documentElement.lang = 'en';   }
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

// DOM Elements
const views = {
  loader: document.getElementById('loader'),
  selector: document.getElementById('selector-view'),
  table: document.getElementById('table-view'),
  form: document.getElementById('form-view')
};

// Initialize the app when the DOM is ready
document.addEventListener('DOMContentLoaded', async () => {
  await initLocale();
  applyI18nToDOM();
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
    
    const containerWidth = document.getElementById('table-view').clientWidth || window.innerWidth;
    const availableWidth = containerWidth - 40; 
    let usedWidth = 215; // 50 (# col) + 165 (actions col) — always-visible, not counted in the loop below
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

function startPreScanTimer() {
  if (_preScanTimer) { clearInterval(_preScanTimer); }
  let secs = 0;
  _preScanTimer = setInterval(() => {
    secs++;
    const el = document.getElementById('pre-scan-timer');
    if (el) el.textContent = `${secs}s`;
  }, 1000);
}

function resizeIframe() {
  // Cap at 75% of the available screen height so the panel never extends
  // off the bottom of the Zendesk workspace (which is ~75-80% of the screen
  // after browser chrome + Zendesk top bar are subtracted).
  // Content taller than this cap scrolls inside the body (overflow-y: auto).
  const maxH = Math.floor((window.screen.availHeight || 900) * 0.75);
  const h = Math.min(document.body.scrollHeight + 16, maxH);
  client.invoke('resize', { width: '100%', height: `${h}px` });
}

function switchView(activeViewId) {
  if (_loaderTimer) { clearInterval(_loaderTimer); _loaderTimer = null; }
  const loaderTimerEl = document.getElementById('loader-timer');
  if (loaderTimerEl) loaderTimerEl.textContent = '';

  Object.values(views).forEach(el => el.style.display = 'none');
  views[activeViewId].style.display = 'block';

  if (activeViewId === 'loader') {
    let secs = 0;
    _loaderTimer = setInterval(() => {
      secs++;
      const el = document.getElementById('loader-timer');
      if (el) el.textContent = `${secs}s`;
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

function updateLoaderText(text) {
  const loaderTextEl = document.getElementById('loader-text');
  if (loaderTextEl) loaderTextEl.innerText = text;
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
    ? ` <span style="color:#68737d;">· ${elapsedSecs}s</span>`
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
    const response = await client.request('/api/v2/custom_objects');
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
    rowNumMap = null;

    const schemaResponse = await client.request(`/api/v2/custom_objects/${coKey}/fields`);
    currentSchema = schemaResponse.custom_object_fields;

    updateLoaderText(t('loader.records'));
    const firstResponse = await client.request(`/api/v2/custom_objects/${coKey}/records?page[size]=100`);
    const firstPageRecords = firstResponse.custom_object_records || [];

    updateLoaderText(t('loader.rendering'));

    const tableData = firstPageRecords.map(record => ({
      id: record.id,
      name: record.name,
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
        title: "#",
        field: "custom_rownum",
        headerSort: false,
        width: 50,
        minWidth: 40,
        hozAlign: "center",
        resizable: false,
        formatter: function(cell) {
          return rowNumMap ? (rowNumMap.get(cell.getData().id) || '') : '';
        }
      },
      { title: t('col.id'), field: "id", width: 80, minWidth: 80 },
      { title: t('col.name'), field: "name", minWidth: 150 }
    ];

    currentSchema.forEach((field) => {
      let colDef = { title: field.title, field: field.key };

      if (field.type === 'checkbox') {
        colDef.width    = 100;
        colDef.minWidth = 80;
        colDef.hozAlign = "center";
        colDef.formatter = "tickCross";
      } else if (field.type === 'date') {
        colDef.width    = 120;
        colDef.minWidth = 100;
      } else if (field.type === 'integer' || field.type === 'decimal') {
        colDef.width    = 110;
        colDef.minWidth = 80;
        colDef.hozAlign = "right";
      } else {
        colDef.minWidth = 150;
      }

      columns.push(colDef);
    });

    columns.push({
      title: t('col.actions'),
      field: "actions",
      formatter: function() {
        return `<button class="btn-edit">${t('row.edit')}</button><button class="btn-danger">${t('row.delete')}</button>`;
      },
      width: 165,
      minWidth: 165,
      headerSort: false,
      cellClick: function(e, cell) {
        const rowData = cell.getRow().getData();
        if(e.target.classList.contains('btn-edit')) {
          showForm(rowData);
        } else if (e.target.classList.contains('btn-danger')) {
          deleteRecord(rowData.id, rowData.name);
        }
      }
    });

    switchView('table');
    document.getElementById('table-search').value = ''; 

    const containerWidth = document.getElementById('table-view').clientWidth || window.innerWidth;
    const availableWidth = containerWidth - 40; 
    let usedWidth = 215; // 50 (# col) + 165 (actions col) — always-visible, not counted in the loop below
    
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
      layout: "fitDataStretch",
      pagination: "local",
      paginationSize: 15,
      columns: columns,
    });

    tabulatorTable.on("dataFiltered", function(_filters, rows) {
      // rows is the authoritative list of filtered rows in display order.
      if (_rnRedrawing) { updateRecordSummary(rows.length); return; }
      rowNumMap = new Map(rows.map((row, i) => [row.getData().id, i + 1]));
      setTimeout(() => {
        if (!tabulatorTable) return;
        _rnRedrawing = true;
        try { tabulatorTable.redraw(true); } finally { _rnRedrawing = false; }
      }, 0);
      updateRecordSummary(rows.length);
    });

    tabulatorTable.on("dataSorted", function(_sorters, rows) {
      if (_rnRedrawing) return;
      rowNumMap = new Map(rows.map((row, i) => [row.getData().id, i + 1]));
      setTimeout(() => {
        if (!tabulatorTable) return;
        _rnRedrawing = true;
        try { tabulatorTable.redraw(true); } finally { _rnRedrawing = false; }
      }, 0);
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

  // Tick every second so the elapsed counter updates even between page fetches
  const timerInterval = setInterval(() => {
    if (!isBackgroundLoading) return;
    const summaryEl = document.getElementById('record-summary');
    if (summaryEl && tabulatorTable) {
      const fetched = tabulatorTable.getData().length + allRemainingRows.length;
      summaryEl.innerHTML = `<strong>${fetched}</strong> ${t('summary.records')} <span style="color:#1f73b7; font-size:12px; font-weight:600;">${t('summary.loadingMore')}</span> <span style="color:#68737d; font-size:12px;">· ${loadElapsed()}s</span>`;
    }
  }, 1000);

  try {

  while (currentEndpoint) {
    if (currentLoadToken !== token) return;
    try {
      const response = await client.request(currentEndpoint);
      if (currentLoadToken !== token) return;

      (response.custom_object_records || []).forEach(record => {
        allRemainingRows.push({ id: record.id, name: record.name, ...record.custom_object_fields });
      });

      // Update only the summary text, no table render.
      // During background fetch the table still holds only the first page so
      // getData('active') would show 100, which is confusing. Show total
      // fetched so far instead: "6357 registros · loading more records..."
      const summaryEl = document.getElementById('record-summary');
      if (summaryEl && tabulatorTable) {
        const fetched = tabulatorTable.getData().length + allRemainingRows.length;
        summaryEl.innerHTML = `<strong>${fetched}</strong> ${t('summary.records')} <span style="color:#1f73b7; font-size:12px; font-weight:600;">${t('summary.loadingMore')}</span> <span style="color:#68737d; font-size:12px;">· ${loadElapsed()}s</span>`;
      }

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
    }
  }

  if (currentLoadToken !== token) return;
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

  } finally {
    clearInterval(timerInterval);
  }
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

  const { totalFound, totalPossible, relationshipCounts, ruleCounts, possibleCounts } = scanResult;

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
    confirmBtn.textContent = t('delete.deleting');
    try {
      await client.request({
        url: `/api/v2/custom_objects/${currentCoKey}/records/${recordId}`,
        type: 'DELETE'
      });
      if (tabulatorTable) {
        tabulatorTable.deleteRow(recordId);
        updateRecordSummary();
      }
      close();
    } catch (error) {
      console.error('Error deleting record:', error);
      confirmBtn.disabled = false;
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

    formHtml += `<div id="tab-content-details">
                   <form id="dynamic-form">
                     <input type="hidden" name="record_id" value="${isEdit ? escapeHtml(existingRecord.id) : ''}" />
                     
                     <div class="form-actions-top">
                       <button type="submit" class="btn">${isEdit ? t('form.updateButton') : t('form.saveButton')}</button>
                       <button type="button" class="btn btn-secondary" id="btn-cancel-form">${t('form.cancel')}</button>
                     </div>

                     <div class="form-group">
                       <label>${t('form.recordName')}</label>
                       <input type="text" name="name" value="${escapeHtml(existingName)}" required />
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
    document.getElementById('dynamic-form').addEventListener('input',  () => { formIsDirty = true; });
    document.getElementById('dynamic-form').addEventListener('change', () => { formIsDirty = true; });

    document.getElementById('btn-cancel-form').addEventListener('click', async () => {
      if (formIsDirty) {
        const leave = await showUnsavedChangesModal();
        if (!leave) return;
      }
      formIsDirty = false;
      switchView('table');
    });
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
    fetchAllPages('/api/v2/ticket_fields.json', 'ticket_fields').catch(e => { console.warn("Could not load ticket fields", e); return []; }),
    fetchAllPages('/api/v2/user_fields.json', 'user_fields').catch(e => { console.warn("Could not load user fields", e); return []; }),
    fetchAllPages('/api/v2/organization_fields.json', 'organization_fields').catch(e => { console.warn("Could not load org fields", e); return []; }),
    fetchAllPages('/api/v2/custom_objects.json', 'custom_objects').catch(e => { console.warn("Could not load COs", e); return []; }),
  ]);

  ticketFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      fields.push({ id: f.id, title: f.title, type: 'zen:ticket', label: t('rel.tickets') });
  });
  userFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      fields.push({ id: f.id, title: f.title, type: 'zen:user', label: t('rel.users') });
  });
  orgFields.forEach(f => {
    if (f.type === 'lookup' && f.relationship_target_type === target)
      fields.push({ id: f.id, title: f.title, type: 'zen:organization', label: t('rel.organizations') });
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

// Normalises a string for name-based fuzzy matching:
// lowercase, remove diacritics, strip special characters, collapse spaces.
function normalizeForMatch(str) {
  if (!str) return '';
  return String(str)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')          // strip diacritics
    .replace(/[^a-z0-9\s<>\-\/|\\]/g, '')    // keep only: letters, digits, spaces, and separators < > - / | \
    .replace(/\s+/g, ' ')
    .trim();
}

// Common words to ignore when computing similarity keys (pt-BR / es / en)
const DUPLICATE_STOPWORDS = new Set([
  'de','da','do','dos','das','em','na','no','nas','nos','para','com','por','ou', // pt
  'del','el','la','los','las','en','al','con','un','una','y',                    // es
  'the','of','in','and','or','an','for','to','by','on','with',                   // en
]);

// Normalises a string for duplicate-name detection:
// case-insensitive, strips diacritics, treats _ - . as spaces, collapses whitespace.
// "Ar Condicionado" === "Ar_Condicionado" === "ar-condicionado" after this.
function normalizeForDuplicate(str) {
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
function normalizeForSimilar(str) {
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
    .filter(t => t.length > 0 && !DUPLICATE_STOPWORDS.has(t)) // no min-length: keep A,B,C,1,2…
    .sort();
  return tokens.length >= 2 ? tokens.join(' ') : '';
}

// Returns HTML for `target` with characters highlighted where they differ from `reference`.
// Uses case-insensitive LCS for alignment; highlights exact-char mismatches and insertions.
function diffHighlight(reference, target) {
  if (!reference || reference === target) return escapeHtml(target);
  const a = reference, b = target;
  const m = a.length, n = b.length;
  if (m * n > 40000) return escapeHtml(target); // guard for very long strings

  // Build LCS table (case-insensitive for alignment)
  const dp = Array.from({ length: m + 1 }, () => new Uint16Array(n + 1));
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] = a[i-1].toLowerCase() === b[j-1].toLowerCase()
        ? dp[i-1][j-1] + 1
        : Math.max(dp[i-1][j], dp[i][j-1]);
    }
  }

  // Traceback: collect chars from b, marking those that differ from reference
  const parts = [];
  let i = m, j = n;
  while (j > 0) {
    if (i > 0 && a[i-1].toLowerCase() === b[j-1].toLowerCase()) {
      parts.unshift({ ch: b[j-1], d: a[i-1] !== b[j-1] }); // highlight case mismatches
      i--; j--;
    } else if (i > 0 && dp[i-1][j] >= dp[i][j-1]) {
      i--;
    } else {
      parts.unshift({ ch: b[j-1], d: true }); // insertion in b (e.g. underscore, extra char)
      j--;
    }
  }

  // Render with grouped <mark> spans for consecutive diffs
  let html = '', inMark = false;
  for (const p of parts) {
    if ( p.d && !inMark) { html += '<mark class="fd-diff">'; inMark = true; }
    if (!p.d &&  inMark) { html += '</mark>';                inMark = false; }
    html += escapeHtml(p.ch);
  }
  return inMark ? html + '</mark>' : html;
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
  const nameEnabled = normalizedName.length >= 3;

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
function linkWrap(nameText, url) {
  if (!url) return escapeHtml(nameText);
  return `<a href="${url}" target="_blank" rel="noopener noreferrer" title="${t('usage.openLink')}">${escapeHtml(nameText)}</a>`;
}

// Runs the full scan (relationship fields + triggers/automations/views/SLA) with live progress.
// onProgress(done, total, label) is called after each step completes.
async function fullReferenceScan(recordId, recordName, onProgress) {
  // Start elapsed time from here so the timer is continuous with the pre-scan phase
  const scanStart = Date.now();
  if (_preScanTimer) { clearInterval(_preScanTimer); _preScanTimer = null; }

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
  const relationshipCounts = [];

  const [, ruleResults] = await Promise.all([
    // Relationship fields - sequential so progress advances per field
    (async () => {
      for (const field of fields) {
        const endpoint = `/api/v2/zen:custom_object:${currentCoKey}/${recordId}/relationship_fields/${field.id}/${field.type}`;
        try {
          const response = await client.request(endpoint);
          let dataKey = '';
          let displayField = 'name';
          if (field.type === 'zen:ticket')                    { dataKey = 'tickets';              displayField = 'subject'; }
          else if (field.type === 'zen:user')                 { dataKey = 'users';                displayField = 'name';    }
          else if (field.type === 'zen:organization')         { dataKey = 'organizations';        displayField = 'name';    }
          else if (field.type.startsWith('zen:custom_object:')) { dataKey = 'custom_object_records'; displayField = 'name'; }

          const records = response[dataKey] || [];
          if (records.length > 0) {
            totalRelationships += records.length;
            const hasMore = response.meta && response.meta.has_more;
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
  return { relationshipHtml, rulesHtml, possibleRulesHtml, totalRelationships, totalRules, totalPossible, totalFound: totalRelationships + totalRules, relationshipCounts, ruleCounts, possibleCounts };
}

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

    // Text-field search across tickets, users and organizations — all three run in parallel
    let ticketTextHtml = '', userTextHtml = '', orgTextHtml = '';
    if (recordName && normalizeForMatch(recordName).length >= 5) {
      const phrase = recordName.replace(/[<>\/|\\]/g, ' ').replace(/"/g, '').replace(/\s+/g, ' ').trim();

      // Fetch field lists in parallel, then fire all three searches in parallel
      const [ticketFieldIds, userFieldKeys, orgFieldKeys] = await Promise.all([
        getTextTicketFieldIds(), getTextUserFieldKeys(), getTextOrgFieldKeys(),
      ]);

      await Promise.all([
        // Tickets
        (async () => {
          if (!ticketFieldIds.length) return;
          const q = ticketFieldIds.slice(0, 15).map(id => `custom_fields_${id}:"${phrase}"`).join(' OR ');
          try {
            const resp = await client.request({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:ticket (${q})`)}&page[size]=25`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) ticketTextHtml = buildEntitySearchHtml(items, t('reverseLookup.ticketFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/tickets/${item.id}` : null, item => item.subject);
          } catch (e) { console.warn('Usage & Impact ticket text search failed:', e); }
        })(),
        // Users
        (async () => {
          if (!userFieldKeys.length) return;
          const q = userFieldKeys.slice(0, 15).map(k => `user_fields.${k}:"${phrase}"`).join(' OR ');
          try {
            const resp = await client.request({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:user (${q})`)}&page[size]=25`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) userTextHtml = buildEntitySearchHtml(items, t('reverseLookup.userFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/users/${item.id}/tickets` : null, item => item.name || item.email);
          } catch (e) { console.warn('Usage & Impact user text search failed:', e); }
        })(),
        // Organizations
        (async () => {
          if (!orgFieldKeys.length) return;
          const q = orgFieldKeys.slice(0, 15).map(k => `organization_fields.${k}:"${phrase}"`).join(' OR ');
          try {
            const resp = await client.request({ url: `/api/v2/search.json?query=${encodeURIComponent(`type:organization (${q})`)}&page[size]=25`, type: 'GET' });
            const items = resp.results || [];
            if (items.length > 0) orgTextHtml = buildEntitySearchHtml(items, t('reverseLookup.orgFields'),
              item => zendeskBaseUrl ? `${zendeskBaseUrl}/agent/organizations/${item.id}/tickets` : null, item => item.name);
          } catch (e) { console.warn('Usage & Impact org text search failed:', e); }
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

    const result = await client.request({
      url: url,
      type: method,
      contentType: 'application/json',
      data: JSON.stringify(payload)
    });

    if (tabulatorTable) {
      const record = result.custom_object_record;
      const rowData = { id: record.id, name: record.name, ...record.custom_object_fields };
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
  const hasMore = items.length >= 25;
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
    const link  = url
      ? `<a href="${url}" target="_blank" rel="noopener noreferrer" title="${t('usage.openLink')}">${label}</a>`
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
async function runReverseLookup(selectedTypes, includeNames, useFilteredOnly, modal, showSelection) {
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
    if (el) el.textContent = `${secs}s`;
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
          .filter(nr => nr.normName.length >= 3)
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
        normRecords.forEach(nr => {
          if (exactMatchedIds.has(String(nr.record.id))) return; // already exact
          const matchIdx = normVals.findIndex(v => v.length >= 3 && v.includes(nr.normName));
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
    const searchable = tableRecords.filter(r => r.name && normalizeForMatch(r.name).length >= 5);

    // Batched parallel ticket text-field search (custom fields only, not subject/description/comments)
    if (selectedTypes.includes('ticketFields') && !_rlCancelled) {
      const textFieldIds = await getTextTicketFieldIds();
      if (textFieldIds.length > 0) {
        // Cap at 15 fields to keep the query within Zendesk's URL length limit
        const queryFieldIds = textFieldIds.slice(0, 15);
        const BATCH = 5;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.ticketFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase    = record.name.replace(/[<>\/|\\]/g, ' ').replace(/"/g, '').replace(/\s+/g, ' ').trim();
            const rawQuery  = `type:ticket (${queryFieldIds.map(id => `custom_fields_${id}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await client.request({
                url:  `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&page[size]=25`,
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
            } catch (e) { console.warn('[RL] ticket field search failed for', record.name, e); }
          }));
        }
      }
    }

    // Batched parallel user text-field search
    if (selectedTypes.includes('userFields') && !_rlCancelled) {
      const userFieldKeys = await getTextUserFieldKeys();
      if (userFieldKeys.length > 0) {
        const queryKeys = userFieldKeys.slice(0, 15);
        const BATCH = 5;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.userFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase   = record.name.replace(/[<>\/|\\]/g, ' ').replace(/"/g, '').replace(/\s+/g, ' ').trim();
            const rawQuery = `type:user (${queryKeys.map(k => `user_fields.${k}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await client.request({ url: `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&page[size]=25`, type: 'GET' });
              (resp.results || []).forEach(user => {
                addResult(record, t('reverseLookup.userFields'), user.name || user.email || `#${user.id}`,
                  user.id, zendeskBaseUrl ? `${zendeskBaseUrl}/agent/users/${user.id}/tickets` : null, true);
              });
            } catch (e) { console.warn('[RL] user field search failed for', record.name, e); }
          }));
        }
      }
    }

    // Batched parallel organization text-field search
    if (selectedTypes.includes('orgFields') && !_rlCancelled) {
      const orgFieldKeys = await getTextOrgFieldKeys();
      if (orgFieldKeys.length > 0) {
        const queryKeys = orgFieldKeys.slice(0, 15);
        const BATCH = 5;
        for (let i = 0; i < searchable.length && !_rlCancelled; i += BATCH) {
          const batch = searchable.slice(i, i + BATCH);
          updateStatus(`${t('reverseLookup.orgFields')}: ${Math.min(i + BATCH, searchable.length)}/${searchable.length}`);
          await Promise.all(batch.map(async record => {
            const phrase   = record.name.replace(/[<>\/|\\]/g, ' ').replace(/"/g, '').replace(/\s+/g, ' ').trim();
            const rawQuery = `type:organization (${queryKeys.map(k => `organization_fields.${k}:"${phrase}"`).join(' OR ')})`;
            try {
              const resp = await client.request({ url: `/api/v2/search.json?query=${encodeURIComponent(rawQuery)}&page[size]=25`, type: 'GET' });
              (resp.results || []).forEach(org => {
                addResult(record, t('reverseLookup.orgFields'), org.name || `#${org.id}`,
                  org.id, zendeskBaseUrl ? `${zendeskBaseUrl}/agent/organizations/${org.id}/tickets` : null, true);
              });
            } catch (e) { console.warn('[RL] org field search failed for', record.name, e); }
          }));
        }
      }
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

    modal.innerHTML = `
      <h3 style="margin:0 0 8px 0; font-size:16px;">${t('reverseLookup.title')}</h3>
      <p style="font-size:13px; color:#68737d; margin:0 0 14px 0;">${t('reverseLookup.found', { n: results.size })}</p>
      ${stoppedNote}${nameMatchNote}
      <div id="rl-results-container">${resultsHtml || `<p style="color:#68737d;">${t('reverseLookup.noResults')}</p>`}</div>
      <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:20px; padding-top:16px; border-top:1px solid #e9ebed;">
        <button id="rl-btn-again" class="btn btn-secondary">${t('reverseLookup.runAgain')}</button>
        <button id="rl-btn-close" class="btn btn-secondary">${t('form.cancel')}</button>
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
      <div style="display:flex; justify-content:flex-end; gap:8px; padding-top:16px; border-top:1px solid #e9ebed;">
        <button id="rl-err-again" class="btn btn-secondary">${t('reverseLookup.runAgain')}</button>
        <button id="rl-err-close" class="btn btn-secondary">${t('form.cancel')}</button>
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
      <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:20px; padding-top:16px; border-top:1px solid #e9ebed;">
        <button id="rl-btn-cancel" class="btn btn-secondary">${t('form.cancel')}</button>
        <button id="rl-btn-run" class="btn">${t('reverseLookup.run')}</button>
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

    document.getElementById('rl-btn-cancel').onclick = () => { overlay.style.display = 'none'; };
    document.getElementById('rl-btn-run').onclick = () => {
      const selectedTypes    = [...modal.querySelectorAll('.lookup-type-grid input:checked')].map(cb => cb.value);
      if (document.getElementById('rl-include-tickets')?.checked) selectedTypes.push('ticketFields');
      if (document.getElementById('rl-include-users')?.checked)   selectedTypes.push('userFields');
      if (document.getElementById('rl-include-orgs')?.checked)    selectedTypes.push('orgFields');
      if (selectedTypes.length === 0) return;
      const includeNames     = document.getElementById('rl-include-names')?.checked ?? true;
      const useFilteredOnly  = document.getElementById('rl-scope-filtered')?.checked ?? false;
      runReverseLookup(selectedTypes, includeNames, useFilteredOnly, modal, showSelection);
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

function csvEscape(val) {
  return `"${String(val).replace(/"/g, '""').replace(/\r?\n/g, ' ')}"`;
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
        <div style="display:flex; justify-content:flex-end; padding-top:16px; border-top:1px solid #e9ebed; margin-top:16px;">
          <button id="fd-close" class="btn btn-secondary">${t('form.cancel')}</button>
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
      <div id="fd-groups-container" style="max-height:50vh; overflow-y:auto;">${exactSection}${similarSection}</div>
      <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; padding-top:14px; border-top:1px solid #e9ebed; margin-top:14px;">
        <span id="fd-sel-count" style="font-size:13px; color:#68737d;">${t('findDuplicates.noneSelected')}</span>
        <div style="display:flex; gap:8px;">
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
      <div style="display:flex; justify-content:flex-end; gap:8px; padding-top:14px; border-top:1px solid #e9ebed; margin-top:14px;">
        <button id="fd-back" class="btn btn-secondary">${t('form.cancel')}</button>
        <button id="fd-confirm" class="btn btn-danger">${t('findDuplicates.confirmDeleteBtn', { n: results.length })}</button>
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
        await client.request({ url: `/api/v2/custom_objects/${currentCoKey}/records/${rec.id}`, type: 'DELETE' });
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
    return t('form.recordFallback', { id });
  }

  try {
    const response = await client.request(endpoint);
    const record = response[dataKey];
    return record[labelField] || record.title || t('form.recordFallback', { id: record.id });
  } catch (e) {
    return t('form.recordFallback', { id });
  }
}