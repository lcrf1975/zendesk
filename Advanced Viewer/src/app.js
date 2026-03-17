import React, { useState, useEffect, useRef, useMemo } from 'react';
import { ThemeProvider, DEFAULT_THEME } from '@zendeskgarden/react-theming';
import { Grid } from '@zendeskgarden/react-grid';
import { Table } from '@zendeskgarden/react-tables';
import { Checkbox, Field } from '@zendeskgarden/react-forms';
import { Modal } from '@zendeskgarden/react-modals';
import { Button } from '@zendeskgarden/react-buttons';

const modernFont = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif';
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

const formatDate = (isoString) => {
  const date = new Date(isoString);
  if (isNaN(date.getTime())) return isoString;
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);
  if (diffMins < 1) return 'Just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  const opts = { month: 'short', day: 'numeric' };
  if (date.getFullYear() !== now.getFullYear()) opts.year = 'numeric';
  return date.toLocaleDateString('en-US', opts);
};

const getStatusColor = (category) => {
  switch (category) {
    // Exact Garden palette: bg = hue100/200, text = hue700/800
    case 'new':     return { bg: '#fff3e4', text: '#ac5918' };  // yellow100 / yellow700
    case 'open':    return { bg: '#fee3e5', text: '#cd3642' };  // red200    / red700
    case 'pending': return { bg: '#edf7ff', text: '#1f73b7' };  // blue100   / blue700
    case 'hold':    return { bg: '#f8f9f9', text: '#5c6970' };  // grey100   / grey700
    case 'solved':  return { bg: '#eef8f4', text: '#037f52' };  // green100  / green700
    case 'closed':  return { bg: '#d8dcde', text: '#39434b' };  // grey300   / grey800
    default:        return { bg: '#f8f9f9', text: '#5c6970' };
  }
};

const getPriorityColor = (priority) => {
  switch (String(priority).toLowerCase()) {
    case 'urgent': return { bg: '#fee2e2', text: '#dc2626' };
    case 'high':   return { bg: '#ffedd5', text: '#ea580c' };
    case 'normal': return { bg: '#fef3c7', text: '#d97706' };
    case 'low':    return { bg: '#f3f4f6', text: '#4b5563' };
    default:       return null;
  }
};

const App = ({ client }) => {
  // ── Data ────────────────────────────────────────────────────────────────────
  const [views, setViews] = useState([]);
  const [viewCounts, setViewCounts] = useState({});
  const [brands, setBrands] = useState({});
  const [forms, setForms] = useState({});
  const [organizations, setOrganizations] = useState({});
  const [customStatuses, setCustomStatuses] = useState({});
  const [macros, setMacros] = useState([]);
  const [groups, setGroups] = useState([]);
  const [agents, setAgents] = useState([]);
  const [subdomain, setSubdomain] = useState('');
  const [loadingViews, setLoadingViews] = useState(true);
  const [fetchError, setFetchError] = useState(false);

  // ── Active content ───────────────────────────────────────────────────────────
  const [selectedView, setSelectedView] = useState(null);
  // 'suspended' | 'deleted' | null — mutually exclusive with selectedView
  const [selectedSection, setSelectedSection] = useState(null);

  // ── Tickets ──────────────────────────────────────────────────────────────────
  const [columns, setColumns] = useState([]);
  const [rows, setRows] = useState([]);
  const [loadingTickets, setLoadingTickets] = useState(false);
  const [ticketsUrl, setTicketsUrl] = useState(null);
  const [nextPage, setNextPage] = useState(null);
  const [prevPage, setPrevPage] = useState(null);
  const [sortColumn, setSortColumn] = useState(null);
  const [sortDirection, setSortDirection] = useState('desc');
  const [refreshTrigger, setRefreshTrigger] = useState(0);

  // ── Bulk actions ─────────────────────────────────────────────────────────────
  const [selectedTickets, setSelectedTickets] = useState([]);
  const [isUpdatingBulk, setIsUpdatingBulk] = useState(false);
  const [bulkType, setBulkType] = useState('status');
  const [bulkValue, setBulkValue] = useState('open');
  const [confirmAction, setConfirmAction] = useState(null);
  const [mergeState, setMergeState] = useState({ open: false, targetId: null });
  const [cloneModal, setCloneModal] = useState({ open: false, title: '' });

  // ── Play mode ────────────────────────────────────────────────────────────────
  const [playIndex, setPlayIndex] = useState(null);

  // ── Filter ───────────────────────────────────────────────────────────────────
  const [filterText, setFilterText] = useState('');

  // ── Ticket preview ───────────────────────────────────────────────────────────
  const [previewEnabled, setPreviewEnabled] = useState(() =>
    localStorage.getItem('zd-preview-enabled') !== 'false'
  );
  const [previewTicketId, setPreviewTicketId] = useState(null);
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const previewTimerRef = useRef(null);
  // Keeps playIndex + rows accessible inside the storage event listener without stale closures
  const playStateRef = useRef({ index: null, rows: [] });

  // ── Preferences ──────────────────────────────────────────────────────────────
  const [groupBy, setGroupBy] = useState(() => localStorage.getItem('zendesk-custom-views-groupby') || 'None');
  const selectedTicketSet = useMemo(() => new Set(selectedTickets), [selectedTickets]);

  const filteredRows = useMemo(() => {
    if (!filterText.trim()) return rows;
    const q = filterText.toLowerCase();

    // Resolve each cell to the same text that would be displayed, so the filter
    // matches what the agent actually sees (e.g. "Open" not "12345").
    const toSearchText = (val, colId) => {
      if (val == null) return '';
      if (colId === 'status' || colId === 'custom_status_id') {
        return (customStatuses[val]?.label || String(val)).toLowerCase();
      }
      if (colId === 'priority' || colId === 'previous_state') {
        return String(val).toLowerCase();
      }
      if (Array.isArray(val)) return val.join(' ').toLowerCase();
      if (typeof val === 'string' && ISO_DATE_RE.test(val)) return formatDate(val).toLowerCase();
      if (typeof val === 'object') {
        return (val.title || val.name || Object.values(val).filter(v => typeof v === 'string').join(' ')).toLowerCase();
      }
      return String(val).toLowerCase();
    };

    return rows.filter(row =>
      columns.some(col => toSearchText(row[col.id], col.id).includes(q))
    );
  }, [rows, columns, filterText, customStatuses]);

  // ── Effects ──────────────────────────────────────────────────────────────────

  useEffect(() => {
    localStorage.setItem('zendesk-custom-views-groupby', groupBy);
  }, [groupBy]);

  useEffect(() => {
    localStorage.setItem('zd-preview-enabled', String(previewEnabled));
    if (!previewEnabled) {
      setPreviewTicketId(null);
      setPreviewData(null);
    }
  }, [previewEnabled]);

  useEffect(() => {
    const handleAppActivated = () => setRefreshTrigger(prev => prev + 1);
    client.on('app.activated', handleAppActivated);
    return () => client.off('app.activated', handleAppActivated);
  }, [client]);

  // 1. Initial data fetch
  useEffect(() => {
    const fetchInitialData = async () => {
      setFetchError(false);
      try {
        const [viewsRes, brandsRes, formsRes, orgsRes, statusesRes, macrosRes, groupsRes, agentsRes] = await Promise.all([
          client.request({ url: '/api/v2/views.json?active=true', cache: false }),
          client.request({ url: '/api/v2/brands.json', cache: false }),
          client.request({ url: '/api/v2/ticket_forms.json', cache: false }),
          client.request({ url: '/api/v2/organizations.json?per_page=100', cache: false }),
          client.request({ url: '/api/v2/custom_statuses.json', cache: false }).catch(() => ({ custom_statuses: [] })),
          client.request({ url: '/api/v2/macros.json?active=true&per_page=100', cache: false }).catch(() => ({ macros: [] })),
          client.request({ url: '/api/v2/groups.json', cache: false }).catch(() => ({ groups: [] })),
          client.request({ url: '/api/v2/users.json?role[]=agent&role[]=admin&per_page=100', cache: false }).catch(() => ({ users: [] }))
        ]);

        setViews(viewsRes.views || []);

        const brandMap = {};
        (brandsRes.brands || []).forEach(b => brandMap[b.id] = b.name);
        setBrands(brandMap);

        const formMap = {};
        (formsRes.ticket_forms || []).forEach(f => formMap[f.id] = f.name);
        setForms(formMap);

        const orgMap = {};
        (orgsRes.organizations || []).forEach(o => orgMap[o.id] = o.name);
        setOrganizations(orgMap);

        const statusMap = {};
        (statusesRes?.custom_statuses || []).forEach(cs => {
          statusMap[cs.id] = { label: cs.agent_label, category: cs.status_category };
        });
        setCustomStatuses(statusMap);

        if (macrosRes?.macros) setMacros(macrosRes.macros);
        if (groupsRes?.groups) setGroups(groupsRes.groups);
        if (agentsRes?.users) setAgents(agentsRes.users);
        setLoadingViews(false);

        client.get('currentAccount').then(d => setSubdomain(d?.currentAccount?.subdomain || '')).catch(() => {});

        // Fire-and-forget: counts must never block or risk triggering setFetchError
        (async (views) => {
          const ids = views.map(v => v.id);
          const chunks = [];
          for (let i = 0; i < ids.length; i += 100) chunks.push(ids.slice(i, i + 100).join(','));
          const countsMap = {};
          await Promise.all(chunks.map(async (chunk) => {
            try {
              const res = await client.request({ url: `/api/v2/views/count_many.json?ids=${chunk}`, cache: false });
              // value is null when Zendesk marks the count stale; fall back to pretty ("5", "100+")
              res.view_counts.forEach(vc => countsMap[vc.view_id] = vc.value ?? vc.pretty);
            } catch (err) { console.error('Failed to load view counts', err); }
          }));
          setViewCounts(prev => ({ ...prev, ...countsMap }));
        })(viewsRes.views || []);

      } catch (error) {
        console.error('Error fetching app data:', error);
        setFetchError(true);
        setLoadingViews(false);
      }
    };
    fetchInitialData();
  }, [client, refreshTrigger]);

  // 2. Build tickets URL for regular views
  useEffect(() => {
    if (!selectedView) return;
    const base = `/api/v2/views/${selectedView.id}/execute.json`;
    setTicketsUrl(sortColumn
      ? `${base}?sort_by=${sortColumn}&sort_order=${sortDirection}&refresh=${refreshTrigger}`
      : `${base}?refresh=${refreshTrigger}`
    );
  }, [sortColumn, sortDirection, selectedView, refreshTrigger]);

  // 3. Fetch regular view tickets
  useEffect(() => {
    if (!ticketsUrl) return;
    let cancelled = false;
    setLoadingTickets(true);
    setRows([]);
    setColumns([]);
    setSelectedTickets([]);
    setPlayIndex(null);

    client.request({ url: ticketsUrl, cache: false }).then((res) => {
      if (cancelled) return;
      if (res.columns?.length > 0) setColumns(res.columns);
      setRows(res.rows || []);
      setNextPage(res.next_page || null);
      setPrevPage(res.previous_page || null);
      setLoadingTickets(false);
    }).catch((err) => {
      if (cancelled) return;
      console.error('Error executing view:', err);
      setRows([{ _error: true }]);
      setLoadingTickets(false);
    });

    return () => { cancelled = true; };
  }, [ticketsUrl, client]);

  // 4. Fetch ticket preview data
  useEffect(() => {
    if (!previewTicketId) { setPreviewData(null); return; }
    let cancelled = false;
    setPreviewLoading(true);
    setPreviewData(null);

    client.request({ url: `/api/v2/tickets/${previewTicketId}.json?include=users`, cache: false })
      .then((res) => {
        if (cancelled) return;
        const users = res.users || [];
        setPreviewData({
          ticket: res.ticket,
          requester: users.find(u => u.id === res.ticket.requester_id) || null,
          assignee: users.find(u => u.id === res.ticket.assignee_id) || null,
        });
        setPreviewLoading(false);
      }).catch(() => { if (!cancelled) setPreviewLoading(false); });

    return () => { cancelled = true; };
  }, [previewTicketId, client]);

  // 5. Keep playStateRef in sync so the storage listener never has a stale closure
  useEffect(() => {
    playStateRef.current = { index: playIndex, rows };
  }, [playIndex, rows]);

  // 6. Auto-advance Play mode when ticket_sidebar signals a ticket was submitted
  useEffect(() => {
    const onStorage = (e) => {
      if (e.key !== 'zd-play-advance') return;
      const { index, rows: r } = playStateRef.current;
      if (index === null) return;
      const next = index + 1;
      if (next >= r.length) {
        setPlayIndex(null);
        client.invoke('notify', 'You have reached the end of this view.', 'notice');
      } else {
        setPlayIndex(next);
        client.invoke('routeTo', 'ticket', r[next].ticket.id);
      }
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, [client]);

  // ── Handlers ──────────────────────────────────────────────────────────────────

  const resetTicketState = () => {
    setSortColumn(null);
    setSortDirection('desc');
    setPlayIndex(null);
    setPreviewTicketId(null);
    setSelectedTickets([]);
    setFilterText('');
  };

  const handleSelectView = (view) => {
    resetTicketState();
    setSelectedSection(null);
    setSelectedView(view);
  };

  const handleSelectSection = (section) => {
    resetTicketState();
    setSelectedView(null);
    setTicketsUrl(null);
    setRows([]);
    setColumns([]);
    setNextPage(null);
    setPrevPage(null);
    setSelectedSection(section);
    setLoadingTickets(true);

    (async () => {
      try {
        if (section === 'suspended') {
          const res = await client.request({ url: '/api/v2/suspended_tickets.json', cache: false });
          const tickets = res.suspended_tickets || [];
          setColumns([
            { id: 'cause', title: 'Cause' },
            { id: 'subject', title: 'Subject' },
            { id: 'author', title: 'Author' },
            { id: 'created_at', title: 'Received' },
          ]);
          setRows(tickets.map(t => ({
            ticket: { id: t.id },
            cause: t.cause || 'Unknown',
            subject: t.subject || '(No subject)',
            author: t.author?.name || t.author?.email || '-',
            created_at: t.created_at,
          })));
          setNextPage(res.next_page || null);
          setPrevPage(res.previous_page || null);
        } else if (section === 'deleted') {
          const res = await client.request({ url: '/api/v2/deleted_tickets.json', cache: false });
          const tickets = res.deleted_tickets || [];
          setColumns([
            { id: 'id', title: 'ID' },
            { id: 'subject', title: 'Subject' },
            { id: 'previous_state', title: 'Previous Status' },
            { id: 'deleted_at', title: 'Deleted' },
          ]);
          setRows(tickets.map(t => ({
            ticket: { id: t.id },
            id: t.id,
            subject: t.subject || '(No subject)',
            previous_state: t.previous_state,
            deleted_at: t.deleted_at,
          })));
          setNextPage(res.next_page || null);
          setPrevPage(res.previous_page || null);
        }
      } catch (err) {
        console.error(`Error fetching ${section} tickets:`, err);
      } finally {
        setLoadingTickets(false);
      }
    })();
  };

  const handleSort = (columnId) => {
    if (sortColumn === columnId) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(columnId);
      setSortDirection('desc');
    }
  };

  const handleRefresh = () => {
    setRefreshTrigger(prev => prev + 1);
    // If a special section is active, reload its data too
    if (selectedSection) handleSelectSection(selectedSection);
  };

  const handleSelectAll = (e) => {
    if (e.target.checked) {
      const filteredIds = filteredRows.map(r => r.ticket.id);
      setSelectedTickets(prev => [...new Set([...prev, ...filteredIds])]);
    } else {
      const filteredIds = new Set(filteredRows.map(r => r.ticket.id));
      setSelectedTickets(prev => prev.filter(id => !filteredIds.has(id)));
    }
  };

  const handleSelectOne = (e, ticketId) => {
    e.stopPropagation();
    setSelectedTickets(selectedTicketSet.has(ticketId)
      ? selectedTickets.filter(id => id !== ticketId)
      : [...selectedTickets, ticketId]
    );
  };

  // ── Bulk update (regular views) ───────────────────────────────────────────────

  const handleBulkUpdate = async () => {
    if (selectedTickets.length === 0 || !bulkValue) return;
    const count = selectedTickets.length;
    setIsUpdatingBulk(true);
    try {
      const ids = selectedTickets.join(',');
      if (bulkType === 'status') {
        await client.request({ url: `/api/v2/tickets/update_many.json?ids=${ids}`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: { status: bulkValue } }) });
      } else if (bulkType === 'group') {
        await client.request({ url: `/api/v2/tickets/update_many.json?ids=${ids}`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: { group_id: parseInt(bulkValue, 10) } }) });
      } else if (bulkType === 'agent') {
        await client.request({ url: `/api/v2/tickets/update_many.json?ids=${ids}`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: { assignee_id: parseInt(bulkValue, 10) } }) });
      } else if (bulkType === 'macro') {
        await Promise.all(selectedTickets.map(async (ticketId) => {
          try {
            const applyRes = await client.request({ url: `/api/v2/tickets/${ticketId}/macros/${bulkValue}/apply.json`, type: 'GET' });
            await client.request({ url: `/api/v2/tickets/${ticketId}.json`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: applyRes.result.ticket }) });
          } catch (err) { console.error(`Macro failed for ticket ${ticketId}`, err); }
        }));
      } else if (bulkType === 'add_tags') {
        const tags = bulkValue.split(',').map(t => t.trim()).filter(Boolean);
        await client.request({ url: `/api/v2/tickets/update_many.json?ids=${ids}`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: { additional_tags: tags } }) });
      } else if (bulkType === 'remove_tags') {
        const tags = bulkValue.split(',').map(t => t.trim()).filter(Boolean);
        await client.request({ url: `/api/v2/tickets/update_many.json?ids=${ids}`, type: 'PUT', contentType: 'application/json', data: JSON.stringify({ ticket: { remove_tags: tags } }) });
      }
      setSelectedTickets([]);
      handleRefresh();
      client.invoke('notify', `Updated ${count} ticket${count > 1 ? 's' : ''}.`, 'success');
    } catch (err) {
      console.error('Bulk update failed', err);
      client.invoke('notify', 'Failed to update some tickets.', 'error');
    } finally {
      setIsUpdatingBulk(false);
    }
  };

  const handleBulkMerge = () => {
    if (selectedTickets.length < 2) return;
    setMergeState({ open: true, targetId: selectedTickets[0] });
  };

  const handleConfirmMerge = async () => {
    const { targetId } = mergeState;
    const sourceIds = selectedTickets.filter(id => id !== targetId);
    const count = selectedTickets.length;
    setMergeState({ open: false, targetId: null });
    setIsUpdatingBulk(true);
    try {
      await client.request({
        url: `/api/v2/tickets/${targetId}/merge.json`, type: 'POST', contentType: 'application/json',
        data: JSON.stringify({
          ids: sourceIds,
          target_comment: `Merged with ${sourceIds.length} other ticket${sourceIds.length > 1 ? 's' : ''}.`,
          source_comment: `Merged into ticket #${targetId}.`,
        })
      });
      setSelectedTickets([]);
      handleRefresh();
      client.invoke('notify', `Merged ${count} tickets into #${targetId}.`, 'success');
    } catch (err) {
      console.error('Merge failed', err);
      client.invoke('notify', 'Failed to merge tickets.', 'error');
    } finally {
      setIsUpdatingBulk(false);
    }
  };

  const handleBulkDelete = () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setConfirmAction({
      type: 'delete',
      message: `Permanently delete ${count} ticket${count > 1 ? 's' : ''}? This cannot be undone.`,
      onConfirm: async () => {
        setConfirmAction(null);
        setIsUpdatingBulk(true);
        try {
          await client.request({ url: `/api/v2/tickets/destroy_many.json?ids=${selectedTickets.join(',')}`, type: 'DELETE' });
          setSelectedTickets([]);
          handleRefresh();
          client.invoke('notify', `Deleted ${count} ticket${count > 1 ? 's' : ''}.`, 'success');
        } catch (err) {
          client.invoke('notify', 'Failed to delete tickets.', 'error');
        } finally {
          setIsUpdatingBulk(false);
        }
      }
    });
  };

  const handleBulkSpam = () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setConfirmAction({
      type: 'spam',
      message: `Mark ${count} ticket${count > 1 ? 's' : ''} as spam and suspend requesters?`,
      onConfirm: async () => {
        setConfirmAction(null);
        setIsUpdatingBulk(true);
        try {
          await client.request({ url: `/api/v2/tickets/mark_many_as_spam.json?ids=${selectedTickets.join(',')}`, type: 'PUT' });
          setSelectedTickets([]);
          handleRefresh();
          client.invoke('notify', `Marked ${count} ticket${count > 1 ? 's' : ''} as spam.`, 'success');
        } catch (err) {
          client.invoke('notify', 'Failed to mark as spam.', 'error');
        } finally {
          setIsUpdatingBulk(false);
        }
      }
    });
  };

  // ── Suspended ticket actions ──────────────────────────────────────────────────

  const handleSuspendedRecover = async () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setIsUpdatingBulk(true);
    try {
      await client.request({ url: `/api/v2/suspended_tickets/recover_many.json?ids=${selectedTickets.join(',')}`, type: 'PUT' });
      setSelectedTickets([]);
      handleSelectSection('suspended');
      client.invoke('notify', `Recovered ${count} ticket${count > 1 ? 's' : ''}.`, 'success');
    } catch (err) {
      client.invoke('notify', 'Failed to recover tickets.', 'error');
    } finally {
      setIsUpdatingBulk(false);
    }
  };

  const handleSuspendedDelete = () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setConfirmAction({
      type: 'delete',
      message: `Permanently delete ${count} suspended ticket${count > 1 ? 's' : ''}? This cannot be undone.`,
      onConfirm: async () => {
        setConfirmAction(null);
        setIsUpdatingBulk(true);
        try {
          await client.request({ url: `/api/v2/suspended_tickets/destroy_many.json?ids=${selectedTickets.join(',')}`, type: 'DELETE' });
          setSelectedTickets([]);
          handleSelectSection('suspended');
          client.invoke('notify', `Deleted ${count} suspended ticket${count > 1 ? 's' : ''}.`, 'success');
        } catch (err) {
          client.invoke('notify', 'Failed to delete suspended tickets.', 'error');
        } finally {
          setIsUpdatingBulk(false);
        }
      }
    });
  };

  // ── Deleted ticket actions ────────────────────────────────────────────────────

  const handleDeletedRestore = async () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setIsUpdatingBulk(true);
    try {
      await client.request({ url: `/api/v2/deleted_tickets/restore_many.json?ids=${selectedTickets.join(',')}`, type: 'PUT' });
      setSelectedTickets([]);
      handleSelectSection('deleted');
      client.invoke('notify', `Restored ${count} ticket${count > 1 ? 's' : ''}.`, 'success');
    } catch (err) {
      client.invoke('notify', 'Failed to restore tickets.', 'error');
    } finally {
      setIsUpdatingBulk(false);
    }
  };

  const handleDeletedPermanentDelete = () => {
    const count = selectedTickets.length;
    if (count === 0) return;
    setConfirmAction({
      type: 'delete',
      message: `Permanently destroy ${count} deleted ticket${count > 1 ? 's' : ''}? This cannot be undone.`,
      onConfirm: async () => {
        setConfirmAction(null);
        setIsUpdatingBulk(true);
        try {
          await Promise.all(selectedTickets.map(id =>
            client.request({ url: `/api/v2/deleted_tickets/${id}.json`, type: 'DELETE' })
          ));
          setSelectedTickets([]);
          handleSelectSection('deleted');
          client.invoke('notify', `Permanently deleted ${count} ticket${count > 1 ? 's' : ''}.`, 'success');
        } catch (err) {
          client.invoke('notify', 'Failed to permanently delete tickets.', 'error');
        } finally {
          setIsUpdatingBulk(false);
        }
      }
    });
  };

  // ── View management ───────────────────────────────────────────────────────────

  const handleCloneView = async () => {
    if (!selectedView) return;
    // Pre-fill the modal title then let the user confirm / rename before cloning
    setCloneModal({ open: true, title: `Copy of ${selectedView.title}` });
  };

  const handleConfirmClone = async () => {
    if (!selectedView || !cloneModal.title.trim()) return;
    const cloneTitle = cloneModal.title.trim();
    setCloneModal({ open: false, title: '' });
    try {
      const res = await client.request({ url: `/api/v2/views/${selectedView.id}.json`, cache: false });
      const v = res.view;
      const createRes = await client.request({
        url: '/api/v2/views.json', type: 'POST', contentType: 'application/json',
        data: JSON.stringify({
          view: {
            title: cloneTitle,
            active: v.active,
            conditions: v.conditions,
            execution: v.execution,
            ...(v.restriction && { restriction: v.restriction }),
          }
        })
      });
      handleRefresh();
      client.invoke('notify', `"${cloneTitle}" created. Opening edit page…`, 'success');
      if (subdomain && createRes.view?.id) {
        window.open(`https://${subdomain}.zendesk.com/agent/admin/views/${createRes.view.id}/edit`, '_blank');
      }
    } catch (err) {
      console.error('Clone view failed', err);
      client.invoke('notify', 'Failed to clone view.', 'error');
    }
  };

  // ── Export ───────────────────────────────────────────────────────────────────

  const cellToText = (value, colId) => {
    if (value === null || value === undefined) return '';
    if (colId === 'status' || colId === 'custom_status_id') {
      if (customStatuses[value]) return customStatuses[value].label;
      return String(value);
    }
    if (Array.isArray(value)) return value.join(', ');
    if (typeof value === 'string' && ISO_DATE_RE.test(value)) return formatDate(value);
    if (typeof value === 'object') return value.title || value.name || '';
    return String(value);
  };

  const handleExport = () => {
    if (rows.length === 0) return;
    const header = columns.map(col => `"${col.title}"`).join(',');
    const csvRows = rows.map(row =>
      columns.map(col => {
        const text = cellToText(row[col.id], col.id);
        return `"${String(text).replace(/"/g, '""')}"`;
      }).join(',')
    );
    const csv = [header, ...csvRows].join('\n');
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${(selectedView?.title || sectionTitle || 'tickets').replace(/[^a-z0-9]/gi, '_')}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleDeleteView = () => {
    if (!selectedView) return;
    setConfirmAction({
      type: 'delete_view',
      message: `Delete view "${selectedView.title}"? This cannot be undone.`,
      onConfirm: async () => {
        setConfirmAction(null);
        try {
          await client.request({ url: `/api/v2/views/${selectedView.id}.json`, type: 'DELETE' });
          setSelectedView(null);
          handleRefresh();
          client.invoke('notify', 'View deleted.', 'success');
        } catch (err) {
          console.error('Delete view failed', err);
          client.invoke('notify', 'Failed to delete view.', 'error');
        }
      }
    });
  };

  // ── Play mode ────────────────────────────────────────────────────────────────

  const handlePlay = () => {
    if (rows.length === 0) return;
    setPlayIndex(0);
    client.invoke('routeTo', 'ticket', rows[0].ticket.id);
  };

  const handlePlayNext = () => {
    if (playIndex === null) return;
    const next = playIndex + 1;
    if (next >= rows.length) {
      setPlayIndex(null);
      client.invoke('notify', 'You have reached the end of this view.', 'notice');
      return;
    }
    setPlayIndex(next);
    client.invoke('routeTo', 'ticket', rows[next].ticket.id);
  };

  const handlePlayPrev = () => {
    if (playIndex === null || playIndex === 0) return;
    const prev = playIndex - 1;
    setPlayIndex(prev);
    client.invoke('routeTo', 'ticket', rows[prev].ticket.id);
  };

  const handleStopPlay = () => setPlayIndex(null);

  // ── Render helpers ───────────────────────────────────────────────────────────

  const renderCellData = (value, colId) => {
    if (value === null || value === undefined) return '-';

    if (colId === 'id' || colId === 'ticket_id') {
      return <span style={{ color: '#6b7280', fontWeight: '500' }}>#{value}</span>;
    }

    if (colId === 'status' || colId === 'custom_status_id') {
      let label = String(value);
      let category = String(value).toLowerCase();
      if (customStatuses[value]) { label = customStatuses[value].label; category = customStatuses[value].category; }
      const colors = getStatusColor(category);
      return <span style={{ backgroundColor: colors.bg, color: colors.text, padding: '4px 10px', borderRadius: '12px', fontSize: '12px', fontWeight: '600', textTransform: 'capitalize' }}>{label}</span>;
    }

    if (colId === 'priority') {
      const colors = getPriorityColor(value);
      if (colors) return <span style={{ backgroundColor: colors.bg, color: colors.text, padding: '4px 10px', borderRadius: '12px', fontSize: '12px', fontWeight: '600', textTransform: 'capitalize' }}>{value}</span>;
    }

    if (colId === 'previous_state') {
      const colors = getStatusColor(String(value).toLowerCase());
      return <span style={{ backgroundColor: colors.bg, color: colors.text, padding: '4px 10px', borderRadius: '12px', fontSize: '12px', fontWeight: '600', textTransform: 'capitalize' }}>{value}</span>;
    }

    if (Array.isArray(value)) {
      if (value.length === 0) return '-';
      return (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
          {value.slice(0, 5).map((tag, i) => (
            <span key={i} style={{ backgroundColor: '#f3f4f6', color: '#374151', padding: '2px 6px', borderRadius: '4px', fontSize: '11px', fontWeight: '500' }}>{tag}</span>
          ))}
          {value.length > 5 && <span style={{ color: '#9ca3af', fontSize: '11px', alignSelf: 'center' }}>+{value.length - 5}</span>}
        </div>
      );
    }

    if (typeof value === 'string' && ISO_DATE_RE.test(value)) return formatDate(value);
    if (typeof value === 'object') return value.title || value.name || JSON.stringify(value);
    return String(value);
  };

  const groupedViews = useMemo(() => {
    if (groupBy === 'None') return { 'Flat List': views };
    return views.reduce((acc, view) => {
      const conditions = [...(view.conditions?.all || []), ...(view.conditions?.any || [])];
      if (groupBy === 'Brand') {
        const c = conditions.find(c => c.field === 'brand_id');
        if (c) { const g = brands[c.value] || `Brand ID: ${c.value}`; if (!acc[g]) acc[g] = []; acc[g].push(view); }
      } else if (groupBy === 'Form') {
        const c = conditions.find(c => c.field === 'ticket_form_id');
        if (c) { const g = forms[c.value] || `Form ID: ${c.value}`; if (!acc[g]) acc[g] = []; acc[g].push(view); }
      } else if (groupBy === 'Organization') {
        const c = conditions.find(c => c.field === 'organization_id');
        if (c) { const g = organizations[c.value] || `Org ID: ${c.value}`; if (!acc[g]) acc[g] = []; acc[g].push(view); }
      } else if (groupBy === 'Type') {
        const g = view.restriction ? 'Personal' : 'Shared';
        if (!acc[g]) acc[g] = [];
        acc[g].push(view);
      }
      return acc;
    }, {});
  }, [views, groupBy, brands, forms, organizations]);

  const hasViewsToDisplay = Object.keys(groupedViews).length > 0;

  const ghostBtnStyle = {
    padding: '4px 10px', borderRadius: '6px', border: '1px solid #e5e7eb',
    background: '#fff', color: '#374151', fontSize: '13px', fontWeight: '500',
    cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '4px',
  };

  const sideNavItemStyle = (isSelected) => ({
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    padding: '10px 12px', cursor: 'pointer', borderRadius: '8px', marginBottom: '4px',
    backgroundColor: isSelected ? '#eff6ff' : 'transparent', transition: 'background-color 0.15s ease',
  });

  const hasActiveContent = selectedView || selectedSection;
  const sectionTitle = selectedSection === 'suspended' ? 'Suspended Tickets' : 'Deleted Tickets';

  // ── JSX ──────────────────────────────────────────────────────────────────────

  return (
    <ThemeProvider theme={DEFAULT_THEME}>

      {/* ── Confirmation modal (delete / spam / delete view) ── */}
      {confirmAction && (
        <Modal onClose={() => setConfirmAction(null)}>
          <Modal.Header>
            {confirmAction.type === 'delete_view' ? 'Delete View' : confirmAction.type === 'delete' ? 'Delete Tickets' : 'Mark as Spam'}
          </Modal.Header>
          <Modal.Body>{confirmAction.message}</Modal.Body>
          <Modal.Footer>
            <Modal.FooterItem><Button isBasic onClick={() => setConfirmAction(null)}>Cancel</Button></Modal.FooterItem>
            <Modal.FooterItem>
              <Button isDanger isPrimary onClick={confirmAction.onConfirm}>
                {confirmAction.type === 'spam' ? 'Mark as Spam' : 'Delete'}
              </Button>
            </Modal.FooterItem>
          </Modal.Footer>
          <Modal.Close aria-label="Close" />
        </Modal>
      )}

      {/* ── Merge target selection modal ── */}
      {mergeState.open && (
        <Modal onClose={() => setMergeState({ open: false, targetId: null })}>
          <Modal.Header>Merge Tickets</Modal.Header>
          <Modal.Body>
            <p style={{ marginBottom: '16px', fontSize: '14px', color: '#374151' }}>
              Select the ticket to keep. All others will be closed and merged into it.
            </p>
            {selectedTickets.map(ticketId => {
              const ticketRow = rows.find(r => r.ticket.id === ticketId);
              const subject = ticketRow?.subject || `Ticket #${ticketId}`;
              const isTarget = mergeState.targetId === ticketId;
              return (
                <div key={ticketId} onClick={() => setMergeState(prev => ({ ...prev, targetId: ticketId }))}
                  style={{ padding: '12px', marginBottom: '8px', borderRadius: '8px', cursor: 'pointer', border: `2px solid ${isTarget ? '#3b82f6' : '#e5e7eb'}`, backgroundColor: isTarget ? '#eff6ff' : '#fff', display: 'flex', alignItems: 'center', gap: '10px', transition: 'border-color 0.15s' }}
                >
                  <span style={{ fontSize: '12px', color: '#6b7280', fontWeight: '600', flexShrink: 0 }}>#{ticketId}</span>
                  <span style={{ fontSize: '13px', color: '#111827', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{subject}</span>
                  {isTarget && <span style={{ fontSize: '11px', color: '#fff', fontWeight: '600', backgroundColor: '#3b82f6', padding: '2px 8px', borderRadius: '4px', flexShrink: 0 }}>KEEP</span>}
                </div>
              );
            })}
          </Modal.Body>
          <Modal.Footer>
            <Modal.FooterItem><Button isBasic onClick={() => setMergeState({ open: false, targetId: null })}>Cancel</Button></Modal.FooterItem>
            <Modal.FooterItem><Button isPrimary onClick={handleConfirmMerge}>Merge {selectedTickets.length} Tickets</Button></Modal.FooterItem>
          </Modal.Footer>
          <Modal.Close aria-label="Close merge modal" />
        </Modal>
      )}

      {/* ── Clone view modal ── */}
      {cloneModal.open && (
        <Modal onClose={() => setCloneModal({ open: false, title: '' })}>
          <Modal.Header>Clone View</Modal.Header>
          <Modal.Body>
            <p style={{ marginBottom: '12px', fontSize: '14px', color: '#374151' }}>
              Give the cloned view a name. You'll be taken to its edit page to review and adjust the conditions.
            </p>
            <input
              autoFocus
              value={cloneModal.title}
              onChange={(e) => setCloneModal(prev => ({ ...prev, title: e.target.value }))}
              onKeyDown={(e) => e.key === 'Enter' && handleConfirmClone()}
              placeholder="View name"
              style={{ width: '100%', padding: '8px 12px', borderRadius: '6px', border: '1px solid #d1d5db', fontSize: '14px', fontFamily: modernFont, boxSizing: 'border-box', outline: 'none' }}
            />
          </Modal.Body>
          <Modal.Footer>
            <Modal.FooterItem>
              <Button isBasic onClick={() => setCloneModal({ open: false, title: '' })}>Cancel</Button>
            </Modal.FooterItem>
            <Modal.FooterItem>
              <Button isPrimary disabled={!cloneModal.title.trim()} onClick={handleConfirmClone}>
                Clone &amp; Edit
              </Button>
            </Modal.FooterItem>
          </Modal.Footer>
          <Modal.Close aria-label="Close clone modal" />
        </Modal>
      )}

      <Grid fluid style={{ height: '100vh', padding: 0, backgroundColor: '#f3f4f6', fontFamily: modernFont, overflow: 'hidden' }}>
        <Grid.Row style={{ height: '100%' }}>

          {/* ════════════ SIDEBAR ════════════ */}
          <Grid.Col sm={3} style={{ display: 'flex', flexDirection: 'column', padding: '24px', borderRight: '1px solid #e5e7eb', height: '100%' }}>

            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px', flexShrink: 0 }}>
              <h2 style={{ fontSize: '20px', fontWeight: '600', color: '#111827', margin: 0 }}>My Views</h2>
              <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                {subdomain && (
                  <a href={`https://${subdomain}.zendesk.com/agent/admin/views/new`} target="_blank" rel="noreferrer" title="Create new view"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '20px', color: '#6b7280', padding: '2px 6px', borderRadius: '4px', textDecoration: 'none', lineHeight: 1, display: 'flex', alignItems: 'center' }}
                    onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#e5e7eb'}
                    onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                  >+</a>
                )}
                <button onClick={handleRefresh} title="Refresh"
                  style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '18px', color: '#6b7280', padding: '4px 8px', borderRadius: '4px', transition: 'background-color 0.2s' }}
                  onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#e5e7eb'}
                  onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                >↻</button>
              </div>
            </div>

            {/* Group by */}
            <div style={{ marginBottom: '24px', flexShrink: 0 }}>
              <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6b7280', marginBottom: '6px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Filter & Group By
              </label>
              <select value={groupBy} onChange={(e) => setGroupBy(e.target.value)}
                style={{ width: '100%', padding: '10px', borderRadius: '8px', border: '1px solid #d1d5db', backgroundColor: '#fff', fontSize: '14px', fontFamily: modernFont, cursor: 'pointer', boxShadow: '0 1px 2px 0 rgba(0,0,0,0.05)' }}
              >
                <option value="None">None (All Views)</option>
                <option value="Brand">Brands</option>
                <option value="Form">Ticket Forms</option>
                <option value="Organization">Organizations</option>
                <option value="Type">Shared &amp; Personal</option>
              </select>
            </div>

            {/* Views list + special sections */}
            <div style={{ overflowY: 'auto', flex: 1, paddingRight: '8px' }}>
              {loadingViews ? (
                <div style={{ color: '#6b7280', fontSize: '14px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span>↻</span> Loading views...
                </div>
              ) : fetchError ? (
                <div style={{ color: '#dc2626', fontSize: '14px', textAlign: 'center', marginTop: '20px', padding: '12px', backgroundColor: '#fee2e2', borderRadius: '8px' }}>
                  Failed to load views.{' '}
                  <button onClick={handleRefresh} style={{ background: 'none', border: 'none', color: '#dc2626', textDecoration: 'underline', cursor: 'pointer', fontSize: '14px', padding: 0 }}>Retry</button>
                </div>
              ) : !hasViewsToDisplay ? (
                <div style={{ color: '#6b7280', fontSize: '14px', textAlign: 'center', marginTop: '20px', padding: '10px', backgroundColor: '#f9fafb', borderRadius: '8px' }}>
                  No views found matching <strong>{groupBy}</strong>.
                </div>
              ) : (
                Object.entries(groupedViews)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([groupName, groupViews]) => (
                    <div key={groupName} style={{ marginBottom: '20px' }}>
                      {groupBy !== 'None' && (
                        <h4 style={{ fontSize: '12px', fontWeight: '700', color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '8px', marginTop: '0' }}>
                          {groupName}
                        </h4>
                      )}
                      <ul style={{ listStyleType: 'none', padding: 0, margin: 0 }}>
                        {groupViews.map((view) => {
                          const isSelected = selectedView?.id === view.id;
                          const countDisplay = viewCounts[view.id] != null ? viewCounts[view.id] : '-';
                          return (
                            <li key={view.id} onClick={() => handleSelectView(view)} style={sideNavItemStyle(isSelected)}>
                              <span style={{ fontSize: '14px', fontWeight: isSelected ? '600' : '500', color: isSelected ? '#1d4ed8' : '#374151', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', marginRight: '12px' }}>
                                {view.title}
                              </span>
                              <span style={{ backgroundColor: isSelected ? '#dbeafe' : '#f3f4f6', color: isSelected ? '#1d4ed8' : '#6b7280', fontSize: '12px', fontWeight: '600', padding: '2px 8px', borderRadius: '9999px', flexShrink: 0 }}>
                                {countDisplay}
                              </span>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  ))
              )}

            </div>

            {/* ── Management — pinned to bottom of sidebar ── */}
            {!loadingViews && !fetchError && (
              <div style={{ flexShrink: 0, borderTop: '1px solid #e5e7eb', paddingTop: '12px', marginTop: '8px' }}>
                <h4 style={{ fontSize: '12px', fontWeight: '700', color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '4px', marginTop: '0', padding: '0 4px' }}>
                  Management
                </h4>
                {[
                  { key: 'suspended', label: 'Suspended Tickets', icon: '⚠️' },
                  { key: 'deleted', label: 'Deleted Tickets', icon: '🗑️' },
                ].map(({ key, label, icon }) => {
                  const isSectionSelected = selectedSection === key;
                  return (
                    <div key={key} onClick={() => handleSelectSection(key)} style={sideNavItemStyle(isSectionSelected)}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{ fontSize: '14px' }}>{icon}</span>
                        <span style={{ fontSize: '14px', fontWeight: isSectionSelected ? '600' : '500', color: isSectionSelected ? '#1d4ed8' : '#374151' }}>
                          {label}
                        </span>
                      </div>
                    </div>
                  );
                })}
                {subdomain && (
                  <a href={`https://${subdomain}.zendesk.com/agent/admin/views`} target="_blank" rel="noreferrer"
                    style={{ display: 'flex', alignItems: 'center', gap: '8px', padding: '10px 12px', borderRadius: '8px', marginBottom: '4px', color: '#374151', textDecoration: 'none', fontSize: '14px', fontWeight: '500', transition: 'background-color 0.15s ease' }}
                    onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f3f4f6'}
                    onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
                  >
                    <span style={{ fontSize: '14px' }}>⚙️</span>
                    <span>Manage Views</span>
                  </a>
                )}
              </div>
            )}
          </Grid.Col>

          {/* ════════════ MAIN CONTENT ════════════ */}
          <Grid.Col sm={9} style={{ padding: '24px', position: 'relative', height: '100%', display: 'flex', flexDirection: 'column' }}>

            {/* ── Floating bulk action bar ── */}
            {selectedTickets.length > 0 && (
              <div style={{
                position: 'absolute', bottom: '40px', left: '50%', transform: 'translateX(-50%)', zIndex: 100,
                backgroundColor: '#1f2937', borderRadius: '12px', padding: '12px 24px',
                display: 'flex', alignItems: 'center', gap: '20px',
                boxShadow: '0 10px 25px -5px rgba(0,0,0,0.4)', width: 'max-content',
              }}>
                <span style={{ fontSize: '14px', fontWeight: '600', color: '#f9fafb' }}>
                  {selectedTickets.length} ticket{selectedTickets.length > 1 ? 's' : ''}
                </span>
                <div style={{ width: '1px', height: '24px', backgroundColor: '#4b5563' }} />

                {selectedSection === 'suspended' ? (
                  // Suspended actions
                  <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                    <button disabled={isUpdatingBulk} onClick={handleSuspendedRecover}
                      style={{ padding: '8px 16px', borderRadius: '6px', border: 'none', backgroundColor: '#16a34a', color: '#fff', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                    >↩ Recover</button>
                    <button disabled={isUpdatingBulk} onClick={handleSuspendedDelete}
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: 'transparent', color: '#ef4444', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#4b5563'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >🗑 Delete</button>
                  </div>
                ) : selectedSection === 'deleted' ? (
                  // Deleted actions
                  <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                    <button disabled={isUpdatingBulk} onClick={handleDeletedRestore}
                      style={{ padding: '8px 16px', borderRadius: '6px', border: 'none', backgroundColor: '#16a34a', color: '#fff', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                    >↩ Restore</button>
                    <button disabled={isUpdatingBulk} onClick={handleDeletedPermanentDelete}
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: 'transparent', color: '#ef4444', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#4b5563'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >🗑 Delete Permanently</button>
                  </div>
                ) : (
                  // Regular view actions
                  <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                    <select value={bulkType}
                      onChange={(e) => {
                        const t = e.target.value;
                        setBulkType(t);
                        if (t === 'status') setBulkValue('open');
                        else if (t === 'group') setBulkValue(groups[0]?.id || '');
                        else if (t === 'agent') setBulkValue(agents[0]?.id || '');
                        else if (t === 'macro') setBulkValue(macros[0]?.id || '');
                        else setBulkValue('');
                      }}
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: '#374151', color: '#fff', fontSize: '13px', outline: 'none' }}
                    >
                      <option value="status">Change Status</option>
                      <option value="group">Assign to Group</option>
                      <option value="agent">Assign to Agent</option>
                      <option value="macro">Apply Macro</option>
                      <option value="add_tags">Add Tags</option>
                      <option value="remove_tags">Remove Tags</option>
                    </select>

                    {bulkType === 'add_tags' || bulkType === 'remove_tags' ? (
                      <input value={bulkValue} onChange={(e) => setBulkValue(e.target.value)} placeholder="tag1, tag2, tag3"
                        style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: '#374151', color: '#fff', fontSize: '13px', width: '160px', outline: 'none' }}
                      />
                    ) : (
                      <select value={bulkValue} onChange={(e) => setBulkValue(e.target.value)}
                        style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: '#374151', color: '#fff', fontSize: '13px', maxWidth: '200px', outline: 'none' }}
                      >
                        {bulkType === 'status' && (<><option value="open">Open</option><option value="pending">Pending</option><option value="hold">On-hold</option><option value="solved">Solved</option></>)}
                        {bulkType === 'group' && groups.map(g => <option key={g.id} value={g.id}>{g.name}</option>)}
                        {bulkType === 'agent' && agents.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
                        {bulkType === 'macro' && macros.map(m => <option key={m.id} value={m.id}>{m.title}</option>)}
                      </select>
                    )}

                    <button disabled={isUpdatingBulk || !bulkValue} onClick={handleBulkUpdate}
                      style={{ padding: '8px 16px', borderRadius: '6px', border: 'none', backgroundColor: '#3b82f6', color: '#fff', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#2563eb'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#3b82f6'; }}
                    >{isUpdatingBulk ? 'Updating...' : 'Update'}</button>

                    <div style={{ width: '1px', height: '24px', backgroundColor: '#4b5563', margin: '0 4px' }} />

                    <button disabled={isUpdatingBulk || selectedTickets.length < 2} onClick={handleBulkMerge}
                      title={selectedTickets.length < 2 ? 'Select at least 2 tickets to merge' : 'Merge tickets'}
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: 'transparent', color: isUpdatingBulk || selectedTickets.length < 2 ? '#6b7280' : '#a5f3fc', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk || selectedTickets.length < 2 ? 'not-allowed' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk && selectedTickets.length >= 2) e.currentTarget.style.backgroundColor = '#4b5563'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk && selectedTickets.length >= 2) e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >⊕ Merge</button>

                    <button disabled={isUpdatingBulk} onClick={handleBulkSpam} title="Mark as Spam"
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: 'transparent', color: '#fca5a5', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#4b5563'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >⚠ Spam</button>

                    <button disabled={isUpdatingBulk} onClick={handleBulkDelete} title="Delete Tickets"
                      style={{ padding: '8px 12px', borderRadius: '6px', border: '1px solid #4b5563', backgroundColor: 'transparent', color: '#ef4444', fontSize: '13px', fontWeight: '600', cursor: isUpdatingBulk ? 'wait' : 'pointer' }}
                      onMouseEnter={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = '#4b5563'; }}
                      onMouseLeave={(e) => { if (!isUpdatingBulk) e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >🗑 Delete</button>
                  </div>
                )}
              </div>
            )}

            {hasActiveContent ? (
              <div style={{ backgroundColor: '#ffffff', borderRadius: '16px', boxShadow: '0 4px 6px -1px rgba(0,0,0,0.05), 0 2px 4px -1px rgba(0,0,0,0.03)', display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>

                {/* ── View / section header ── */}
                <div style={{ padding: '32px 32px 16px 32px', flexShrink: 0 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                    <h1 style={{ fontSize: '24px', fontWeight: '700', color: '#111827', margin: 0 }}>
                      {selectedSection ? sectionTitle : selectedView.title}
                    </h1>

                    {/* Actions — Export for all, rest only for regular views */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <button
                        onClick={() => setPreviewEnabled(v => !v)}
                        title={previewEnabled ? 'Disable ticket preview on hover' : 'Enable ticket preview on hover'}
                        style={{ ...ghostBtnStyle, color: previewEnabled ? '#1d4ed8' : '#9ca3af', borderColor: previewEnabled ? '#bfdbfe' : '#e5e7eb', backgroundColor: previewEnabled ? '#eff6ff' : '#fff' }}
                      >
                        👁 Preview
                      </button>
                      {rows.length > 0 && (
                        <button onClick={handleExport} title="Export visible tickets to CSV" style={ghostBtnStyle}>⬇ Export</button>
                      )}
                      {selectedView && (
                        <>
                        <button onClick={handleCloneView} title="Clone this view" style={ghostBtnStyle}>⎘ Clone</button>
                        <button onClick={handleDeleteView} title="Delete this view"
                          style={{ ...ghostBtnStyle, color: '#ef4444', borderColor: '#fca5a5' }}
                          onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#fef2f2'}
                          onMouseLeave={(e) => e.currentTarget.style.backgroundColor = '#fff'}
                        >🗑 Delete</button>
                        {subdomain && (
                          <a href={`https://${subdomain}.zendesk.com/agent/admin/views/${selectedView.id}/edit`} target="_blank" rel="noreferrer" title="Edit in Zendesk admin" style={{ ...ghostBtnStyle, textDecoration: 'none' }}>✏ Edit</a>
                        )}
                        {rows.length > 0 && (
                          playIndex !== null ? (
                            <div style={{ display: 'flex', gap: '6px', alignItems: 'center', padding: '4px 12px', backgroundColor: '#eff6ff', borderRadius: '8px', border: '1px solid #dbeafe' }}>
                              <span style={{ fontSize: '12px', color: '#1d4ed8', fontWeight: '600', marginRight: '4px' }}>{playIndex + 1} / {rows.length}</span>
                              <button onClick={handlePlayPrev} disabled={playIndex === 0} title="Previous"
                                style={{ background: 'none', border: 'none', cursor: playIndex === 0 ? 'not-allowed' : 'pointer', color: playIndex === 0 ? '#93c5fd' : '#3b82f6', fontSize: '14px', padding: '0 2px', lineHeight: 1 }}
                              >◀</button>
                              <button onClick={handlePlayNext} title="Next"
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#3b82f6', fontSize: '14px', padding: '0 2px', lineHeight: 1 }}
                              >▶</button>
                              <button onClick={handleStopPlay} title="Stop"
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#6b7280', fontSize: '12px', padding: '0 4px', fontWeight: '500' }}
                              >✕ Stop</button>
                            </div>
                          ) : (
                            <button onClick={handlePlay} title="Play — open tickets one by one" style={ghostBtnStyle}>▶ Play</button>
                          )
                        )}
                        </>
                      )}
                    </div>
                  </div>
                  <p style={{ fontSize: '14px', color: '#6b7280', margin: '0 0 12px 0' }}>
                    {selectedSection
                      ? `${rows.length} ticket${rows.length !== 1 ? 's' : ''}`
                      : `Total tickets in view: ${viewCounts[selectedView.id] != null ? viewCounts[selectedView.id] : rows.length}`
                    }
                    {filterText.trim() && (
                      <span style={{ marginLeft: '8px', color: '#1d4ed8', fontWeight: '500' }}>
                        — {filteredRows.length} match{filteredRows.length !== 1 ? 'es' : ''}
                      </span>
                    )}
                  </p>

                  {/* ── Inline filter ── */}
                  <div style={{ position: 'relative' }}>
                    <span style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: '#9ca3af', fontSize: '14px', pointerEvents: 'none' }}>🔍</span>
                    <input
                      type="text"
                      value={filterText}
                      onChange={(e) => setFilterText(e.target.value)}
                      placeholder="Filter tickets in this view…"
                      style={{
                        width: '100%', padding: '8px 32px 8px 32px', borderRadius: '8px',
                        border: '1px solid #d1d5db', fontSize: '14px', fontFamily: modernFont,
                        boxSizing: 'border-box', outline: 'none',
                        boxShadow: filterText ? '0 0 0 2px #bfdbfe' : 'none',
                        borderColor: filterText ? '#3b82f6' : '#d1d5db',
                      }}
                    />
                    {filterText && (
                      <button
                        onClick={() => setFilterText('')}
                        style={{ position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af', fontSize: '16px', lineHeight: 1, padding: 0 }}
                      >✕</button>
                    )}
                  </div>
                </div>

                {/* ── Table area + preview panel ── */}
                {loadingTickets ? (
                  <div style={{ color: '#6b7280', fontSize: '15px', flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <span style={{ padding: '10px 20px', backgroundColor: '#f3f4f6', borderRadius: '20px', fontWeight: '500' }}>Fetching tickets...</span>
                  </div>
                ) : (
                  <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }}>

                    {/* Table */}
                    <div style={{ flex: 1, overflowY: 'auto', padding: '0 32px' }}>
                      <Table style={{ width: '100%', marginBottom: '24px' }}>
                        <Table.Head>
                          <Table.HeaderRow style={{ borderBottom: '2px solid #e5e7eb' }}>
                            <Table.HeaderCell style={{ padding: '16px 8px', width: '40px' }}>
                              <Field>
                                <Checkbox
                                  checked={filteredRows.length > 0 && filteredRows.every(r => selectedTicketSet.has(r.ticket.id))}
                                  indeterminate={filteredRows.some(r => selectedTicketSet.has(r.ticket.id)) && !filteredRows.every(r => selectedTicketSet.has(r.ticket.id))}
                                  onChange={handleSelectAll}
                                >
                                  <Field.Label hidden>Select All</Field.Label>
                                </Checkbox>
                              </Field>
                            </Table.HeaderCell>
                            {columns.map((col) => {
                              const isSortable = !selectedSection && (col.sortable === true || col.id === 'status' || col.id === 'custom_status_id');
                              const effectiveSortCol = col.id === 'custom_status_id' ? 'status' : col.id;
                              return (
                                <Table.HeaderCell key={col.id} onClick={() => isSortable && handleSort(effectiveSortCol)}
                                  style={{ padding: '16px 8px', fontSize: '13px', fontWeight: '600', color: sortColumn === effectiveSortCol ? '#1d4ed8' : '#6b7280', textTransform: 'uppercase', letterSpacing: '0.05em', cursor: isSortable ? 'pointer' : 'default', transition: 'color 0.2s', userSelect: 'none' }}
                                >
                                  <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                    {col.title}
                                    {sortColumn === effectiveSortCol && <span style={{ fontSize: '14px' }}>{sortDirection === 'asc' ? '↑' : '↓'}</span>}
                                  </div>
                                </Table.HeaderCell>
                              );
                            })}
                          </Table.HeaderRow>
                        </Table.Head>
                        <Table.Body>
                          {filteredRows.length > 0 ? filteredRows.map((row) => {
                            const isRowSelected = selectedTicketSet.has(row.ticket.id);
                            const isPlaying = playIndex !== null && rows[playIndex]?.ticket.id === row.ticket.id;
                            return (
                              <Table.Row key={row.ticket.id}
                                style={{ cursor: 'pointer', borderBottom: '1px solid #f3f4f6', backgroundColor: isPlaying ? '#fefce8' : isRowSelected ? '#f0fdf4' : 'transparent', outline: isPlaying ? '2px solid #eab308' : 'none' }}
                                onClick={() => !selectedSection && client.invoke('routeTo', 'ticket', row.ticket.id)}
                                onMouseEnter={(e) => {
                                  if (!isRowSelected && !isPlaying) e.currentTarget.style.backgroundColor = '#f9fafb';
                                  if (previewEnabled) {
                                    if (previewTimerRef.current) clearTimeout(previewTimerRef.current);
                                    previewTimerRef.current = setTimeout(() => setPreviewTicketId(row.ticket.id), 600);
                                  }
                                }}
                                onMouseLeave={(e) => {
                                  if (!isRowSelected && !isPlaying) e.currentTarget.style.backgroundColor = isPlaying ? '#fefce8' : 'transparent';
                                  if (previewTimerRef.current) clearTimeout(previewTimerRef.current);
                                }}
                              >
                                <Table.Cell style={{ padding: '16px 8px' }} onClick={(e) => e.stopPropagation()}>
                                  <Field>
                                    <Checkbox checked={isRowSelected} onChange={(e) => handleSelectOne(e, row.ticket.id)}>
                                      <Field.Label hidden>Select</Field.Label>
                                    </Checkbox>
                                  </Field>
                                </Table.Cell>
                                {columns.map((col) => (
                                  <Table.Cell key={`${row.ticket.id}-${col.id}`} style={{ padding: '16px 8px', fontSize: '14px', color: '#374151' }}>
                                    {col.id === 'subject'
                                      ? <span style={{ fontWeight: '600', color: '#111827' }}>{renderCellData(row[col.id], col.id)}</span>
                                      : renderCellData(row[col.id], col.id)
                                    }
                                  </Table.Cell>
                                ))}
                              </Table.Row>
                            );
                          }) : (
                            <Table.Row>
                              <Table.Cell colSpan={columns.length + 1} style={{ textAlign: 'center', padding: '48px' }}>
                                {rows[0]?._error
                                  ? <span style={{ color: '#dc2626' }}>Failed to load tickets. <button onClick={handleRefresh} style={{ background: 'none', border: 'none', color: '#dc2626', textDecoration: 'underline', cursor: 'pointer', fontSize: '14px', padding: 0 }}>Retry</button></span>
                                  : <span style={{ color: '#6b7280' }}>No tickets found.</span>
                                }
                              </Table.Cell>
                            </Table.Row>
                          )}
                        </Table.Body>
                      </Table>
                    </div>

                    {/* ── Ticket preview panel ── */}
                    {previewTicketId && (
                      <div
                        style={{ width: '300px', flexShrink: 0, borderLeft: '1px solid #e5e7eb', overflowY: 'auto', backgroundColor: '#fafafa', padding: '20px', display: 'flex', flexDirection: 'column', gap: '14px' }}
                        onMouseEnter={() => { if (previewTimerRef.current) clearTimeout(previewTimerRef.current); }}
                      >
                        {/* Preview header */}
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <span style={{ fontSize: '11px', fontWeight: '700', color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Preview</span>
                          <button onClick={() => { setPreviewTicketId(null); setPreviewData(null); }}
                            style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '16px', color: '#9ca3af', padding: '0', lineHeight: 1 }}
                          >✕</button>
                        </div>

                        {previewLoading ? (
                          <div style={{ color: '#9ca3af', fontSize: '13px', textAlign: 'center', padding: '20px 0' }}>Loading…</div>
                        ) : previewData ? (
                          <>
                            <div>
                              <p style={{ margin: '0 0 4px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Subject</p>
                              <p style={{ margin: 0, fontSize: '14px', fontWeight: '600', color: '#111827', lineHeight: '1.4' }}>{previewData.ticket.subject}</p>
                            </div>

                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                              {renderCellData(previewData.ticket.status, 'status')}
                              {previewData.ticket.priority && renderCellData(previewData.ticket.priority, 'priority')}
                            </div>

                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                              <div>
                                <p style={{ margin: '0 0 2px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Requester</p>
                                <p style={{ margin: 0, fontSize: '12px', color: '#374151' }}>{previewData.requester?.name || `#${previewData.ticket.requester_id}`}</p>
                              </div>
                              <div>
                                <p style={{ margin: '0 0 2px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Assignee</p>
                                <p style={{ margin: 0, fontSize: '12px', color: '#374151' }}>{previewData.assignee?.name || '-'}</p>
                              </div>
                              <div>
                                <p style={{ margin: '0 0 2px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Created</p>
                                <p style={{ margin: 0, fontSize: '12px', color: '#374151' }}>{formatDate(previewData.ticket.created_at)}</p>
                              </div>
                              <div>
                                <p style={{ margin: '0 0 2px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Updated</p>
                                <p style={{ margin: 0, fontSize: '12px', color: '#374151' }}>{formatDate(previewData.ticket.updated_at)}</p>
                              </div>
                            </div>

                            {previewData.ticket.description && (
                              <div>
                                <p style={{ margin: '0 0 4px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Description</p>
                                <p style={{ margin: 0, fontSize: '12px', color: '#374151', lineHeight: '1.6', display: '-webkit-box', WebkitLineClamp: 6, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                                  {previewData.ticket.description}
                                </p>
                              </div>
                            )}

                            {previewData.ticket.tags?.length > 0 && (
                              <div>
                                <p style={{ margin: '0 0 4px 0', fontSize: '11px', color: '#9ca3af', fontWeight: '600', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Tags</p>
                                {renderCellData(previewData.ticket.tags, 'tags')}
                              </div>
                            )}

                            <button
                              onClick={() => { client.invoke('routeTo', 'ticket', previewData.ticket.id); setPreviewTicketId(null); setPreviewData(null); }}
                              style={{ marginTop: 'auto', padding: '8px 16px', borderRadius: '8px', border: 'none', backgroundColor: '#3b82f6', color: '#fff', fontSize: '13px', fontWeight: '600', cursor: 'pointer', width: '100%' }}
                            >
                              Open Ticket →
                            </button>
                          </>
                        ) : null}
                      </div>
                    )}
                  </div>
                )}

                {/* ── Pagination ── */}
                <div style={{ padding: '24px 32px', flexShrink: 0, borderTop: '1px solid #e5e7eb', display: 'flex', justifyContent: 'flex-end', gap: '12px' }}>
                  <button disabled={!prevPage}
                    onClick={() => selectedSection ? handleSelectSection(selectedSection) : setTicketsUrl(prevPage)}
                    style={{ padding: '8px 16px', borderRadius: '8px', border: '1px solid #d1d5db', backgroundColor: prevPage ? '#fff' : '#f3f4f6', color: prevPage ? '#374151' : '#9ca3af', cursor: prevPage ? 'pointer' : 'not-allowed', fontWeight: '600', fontSize: '14px' }}
                  >Previous Page</button>
                  <button disabled={!nextPage}
                    onClick={() => selectedSection ? handleSelectSection(selectedSection) : setTicketsUrl(nextPage)}
                    style={{ padding: '8px 16px', borderRadius: '8px', border: '1px solid #d1d5db', backgroundColor: nextPage ? '#fff' : '#f3f4f6', color: nextPage ? '#374151' : '#9ca3af', cursor: nextPage ? 'pointer' : 'not-allowed', fontWeight: '600', fontSize: '14px' }}
                  >Next Page</button>
                </div>

              </div>
            ) : (
              <div style={{ height: '100%', display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ width: '72px', height: '72px', backgroundColor: '#eff6ff', borderRadius: '20px', marginBottom: '24px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 18 18" fill="#1d4ed8" width="40" height="40">
                    <rect x="2.5" y="1.5" width="4" height="6" rx="2"/>
                    <rect x="11.5" y="1.5" width="4" height="6" rx="2"/>
                    <rect x="6.5" y="4" width="5" height="2.5" rx="1.25"/>
                    <path fillRule="evenodd" d="M4.5 13 m-4 0 a4 4 0 1 0 8 0 a4 4 0 1 0-8 0 Z M4.5 13 m-2.5 0 a2.5 2.5 0 1 0 5 0 a2.5 2.5 0 1 0-5 0 Z"/>
                    <path fillRule="evenodd" d="M13.5 13 m-4 0 a4 4 0 1 0 8 0 a4 4 0 1 0-8 0 Z M13.5 13 m-2.5 0 a2.5 2.5 0 1 0 5 0 a2.5 2.5 0 1 0-5 0 Z"/>
                  </svg>
                </div>
                <h2 style={{ fontSize: '24px', fontWeight: '600', color: '#374151', margin: '0 0 8px 0' }}>Select a view</h2>
                <p style={{ color: '#6b7280', fontSize: '15px' }}>Choose a view from the sidebar to get started.</p>
              </div>
            )}

          </Grid.Col>
        </Grid.Row>
      </Grid>
    </ThemeProvider>
  );
};

export default App;
