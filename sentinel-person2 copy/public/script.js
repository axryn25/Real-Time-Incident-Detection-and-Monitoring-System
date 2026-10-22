/**
 * Sentinel — Real-Time Incident Detection and Monitoring System
 * Pure Vanilla JavaScript Client
 * Communicates with Node.js/Express backend backed by RethinkDB
 */

// ==========================================
// 1. HELPER & UTILITY FUNCTIONS
// ==========================================

/**
 * Format ISO timestamp into a human-readable string.
 * @param {string|null} isoString
 * @returns {string}
 */
function formatDate(isoString) {
  if (!isoString) return 'Not available';
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return 'Invalid date';

  // Format: "Oct 21, 2026, 12:04:30 PM"
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

/**
 * Safely create a status badge element.
 * @param {string} status
 * @returns {HTMLSpanElement}
 */
function createStatusBadge(status) {
  const badge = document.createElement('span');
  const upper = String(status || 'UNKNOWN').toUpperCase();
  badge.className = 'badge';

  if (upper === 'OPEN') {
    badge.classList.add('badge-open');
  } else if (upper === 'ACKNOWLEDGED') {
    badge.classList.add('badge-acknowledged');
  } else if (upper === 'RESOLVED') {
    badge.classList.add('badge-resolved');
  } else {
    badge.style.backgroundColor = '#e2e8f0';
    badge.style.color = '#475569';
  }

  badge.textContent = upper;
  return badge;
}

/**
 * Safely create a severity badge element.
 * @param {string} severity
 * @returns {HTMLSpanElement}
 */
function createSeverityBadge(severity) {
  const badge = document.createElement('span');
  const upper = String(severity || 'MEDIUM').toUpperCase();
  badge.className = 'badge';

  if (upper === 'CRITICAL') {
    badge.classList.add('badge-critical');
  } else if (upper === 'HIGH') {
    badge.classList.add('badge-high');
  } else if (upper === 'MEDIUM') {
    badge.classList.add('badge-medium');
  } else {
    badge.classList.add('badge-low');
  }

  badge.textContent = upper;
  return badge;
}

/**
 * Wrapper for Fetch API with timeout, network error handling, and structured error extraction.
 * @param {string} url
 * @param {RequestInit} [options={}]
 * @returns {Promise<any>}
 */
async function apiFetch(url, options = {}) {
  const controller = new AbortController();
  const timeoutMs = 8000;
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      ...options,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        ...(options.headers || {})
      },
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    // Parse JSON safely
    let data = null;
    const contentType = res.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      data = await res.json().catch(() => null);
    }

    if (!res.ok) {
      const errorMsg = (data && data.error) ? data.error : `HTTP ${res.status}: ${res.statusText || 'Request failed'}`;
      const err = new Error(errorMsg);
      err.status = res.status;
      err.data = data;
      throw err;
    }

    return data;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new Error('Network request timed out. Please check if the server is responsive.');
    }
    if (error.message.includes('Failed to fetch') || error.name === 'TypeError') {
      throw new Error('Could not connect to the Sentinel backend server. Ensure Express is running on port 3000.');
    }
    throw error;
  }
}

/**
 * Connect to RethinkDB Server-Sent Events (SSE) stream.
 * Automatically updates connection pill in the header and invokes callback on change events.
 * @param {Function} onChangeCallback
 */
function initLiveStream(onChangeCallback) {
  const pill = document.getElementById('connection-pill');
  const pillText = document.getElementById('connection-text');

  if (typeof EventSource === 'undefined') {
    if (pill) {
      pill.className = 'connection-pill disconnected';
      if (pillText) pillText.textContent = 'SSE Not Supported';
    }
    return null;
  }

  let eventSource = null;
  try {
    eventSource = new EventSource('/api/incidents/stream');

    eventSource.onopen = () => {
      if (pill) {
        pill.className = 'connection-pill connected';
        if (pillText) pillText.textContent = 'Live Connected';
      }
    };

    eventSource.onmessage = (event) => {
      try {
        const change = JSON.parse(event.data);
        if (onChangeCallback && typeof onChangeCallback === 'function') {
          onChangeCallback(change);
        }
      } catch (err) {
        console.warn('Failed to parse SSE payload:', err);
      }
    };

    eventSource.onerror = () => {
      if (pill) {
        pill.className = 'connection-pill disconnected';
        if (pillText) pillText.textContent = 'Reconnecting...';
      }
    };
  } catch (e) {
    if (pill) {
      pill.className = 'connection-pill disconnected';
      if (pillText) pillText.textContent = 'Stream Offline';
    }
  }

  // Cleanup on unload
  window.addEventListener('beforeunload', () => {
    if (eventSource) eventSource.close();
  });

  return eventSource;
}

// ==========================================
// 2. PAGE A: DASHBOARD LOGIC (index.html)
// ==========================================

async function initDashboard() {
  const totalCountEl = document.getElementById('metric-total');
  const openCountEl = document.getElementById('metric-open');
  const ackCountEl = document.getElementById('metric-ack');
  const resCountEl = document.getElementById('metric-res');
  const recentTableBody = document.getElementById('recent-incidents-body');
  const dashboardAlert = document.getElementById('dashboard-alert');

  function showError(message) {
    if (!dashboardAlert) return;
    dashboardAlert.className = 'alert alert-error';
    dashboardAlert.textContent = message;
    dashboardAlert.style.display = 'flex';
  }

  function hideError() {
    if (dashboardAlert) dashboardAlert.style.display = 'none';
  }

  async function loadDashboardData() {
    try {
      hideError();
      const incidents = await apiFetch('/api/incidents');

      if (!Array.isArray(incidents)) {
        throw new Error('Unexpected data format received from server.');
      }

      // Compute statistics directly from backend data
      let total = incidents.length;
      let open = 0;
      let ack = 0;
      let res = 0;

      for (const inc of incidents) {
        const s = String(inc.status || '').toUpperCase();
        if (s === 'OPEN') open++;
        else if (s === 'ACKNOWLEDGED') ack++;
        else if (s === 'RESOLVED') res++;
      }

      if (totalCountEl) totalCountEl.textContent = total;
      if (openCountEl) openCountEl.textContent = open;
      if (ackCountEl) ackCountEl.textContent = ack;
      if (resCountEl) resCountEl.textContent = res;

      // Populate recent incidents table (newest 5)
      if (recentTableBody) {
        recentTableBody.innerHTML = '';
        const recent = incidents.slice(0, 5);

        if (recent.length === 0) {
          const row = document.createElement('tr');
          const td = document.createElement('td');
          td.colSpan = 6;
          td.style.textAlign = 'center';
          td.style.padding = '2rem';
          td.style.color = '#64748b';
          td.textContent = 'No incidents recorded yet. New failures sent to POST /api/events will appear here.';
          row.appendChild(td);
          recentTableBody.appendChild(row);
          return;
        }

        for (const inc of recent) {
          const row = document.createElement('tr');

          // Title / Type
          const tdTitle = document.createElement('td');
          tdTitle.style.fontWeight = '600';
          tdTitle.textContent = inc.type || inc.title || 'Unknown Incident';
          row.appendChild(tdTitle);

          // Service
          const tdService = document.createElement('td');
          tdService.textContent = inc.service || 'N/A';
          row.appendChild(tdService);

          // Severity
          const tdSev = document.createElement('td');
          tdSev.appendChild(createSeverityBadge(inc.severity));
          row.appendChild(tdSev);

          // Status
          const tdStatus = document.createElement('td');
          tdStatus.appendChild(createStatusBadge(inc.status));
          row.appendChild(tdStatus);

          // Last Seen
          const tdTime = document.createElement('td');
          tdTime.textContent = formatDate(inc.lastSeen || inc.updatedAt);
          row.appendChild(tdTime);

          // Action
          const tdAction = document.createElement('td');
          const link = document.createElement('a');
          link.href = `incident-details.html?id=${encodeURIComponent(inc.id)}`;
          link.className = 'btn btn-secondary btn-sm';
          link.textContent = 'View Details';
          tdAction.appendChild(link);
          row.appendChild(tdAction);

          recentTableBody.appendChild(row);
        }
      }
    } catch (err) {
      console.error('Error loading dashboard data:', err);
      showError(`Failed to load dashboard metrics: ${err.message}`);
      if (totalCountEl) totalCountEl.textContent = '--';
      if (openCountEl) openCountEl.textContent = '--';
      if (ackCountEl) ackCountEl.textContent = '--';
      if (resCountEl) resCountEl.textContent = '--';
    }
  }

  // Initial load
  await loadDashboardData();

  // Listen for live RethinkDB changefeed updates
  initLiveStream((_change) => {
    // Whenever an incident is inserted or updated in RethinkDB, refresh counts & recent table
    loadDashboardData();
  });
}

// ==========================================
// 3. PAGE B: ALL INCIDENTS LOGIC (incidents.html)
// ==========================================

async function initIncidentsList() {
  const tableBody = document.getElementById('incidents-table-body');
  const alertEl = document.getElementById('incidents-alert');
  const filterBtns = document.querySelectorAll('.filter-btn');

  let currentFilter = 'ALL';
  let cachedIncidents = [];

  function showAlert(message, type = 'error') {
    if (!alertEl) return;
    alertEl.className = `alert alert-${type}`;
    alertEl.textContent = message;
    alertEl.style.display = 'flex';
  }

  function hideAlert() {
    if (alertEl) alertEl.style.display = 'none';
  }

  function renderTableRows(incidents) {
    if (!tableBody) return;
    tableBody.innerHTML = '';

    const filtered = incidents.filter(inc => {
      if (currentFilter === 'ALL') return true;
      return String(inc.status || '').toUpperCase() === currentFilter;
    });

    if (filtered.length === 0) {
      const row = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 7;
      td.style.textAlign = 'center';
      td.style.padding = '2.5rem 1rem';
      td.style.color = '#64748b';

      if (incidents.length === 0) {
        td.textContent = 'No incidents exist in the database.';
      } else {
        td.textContent = `No incidents with status "${currentFilter}".`;
      }

      row.appendChild(td);
      tableBody.appendChild(row);
      return;
    }

    for (const inc of filtered) {
      const row = document.createElement('tr');

      // 1. Title / Type
      const tdTitle = document.createElement('td');
      const titleSpan = document.createElement('div');
      titleSpan.style.fontWeight = '600';
      titleSpan.textContent = inc.type || inc.title || 'Untitled Incident';
      tdTitle.appendChild(titleSpan);

      if (inc.lastMessage) {
        const msgSpan = document.createElement('div');
        msgSpan.style.fontSize = '0.8rem';
        msgSpan.style.color = '#64748b';
        msgSpan.style.marginTop = '0.2rem';
        msgSpan.textContent = inc.lastMessage;
        tdTitle.appendChild(msgSpan);
      }
      row.appendChild(tdTitle);

      // 2. Affected Service
      const tdService = document.createElement('td');
      tdService.textContent = inc.service || 'Unknown';
      row.appendChild(tdService);

      // 3. Severity
      const tdSev = document.createElement('td');
      tdSev.appendChild(createSeverityBadge(inc.severity));
      row.appendChild(tdSev);

      // 4. Status
      const tdStatus = document.createElement('td');
      tdStatus.appendChild(createStatusBadge(inc.status));
      row.appendChild(tdStatus);

      // 5. Grouped Events Count
      const tdCount = document.createElement('td');
      tdCount.style.fontWeight = '600';
      tdCount.textContent = inc.occurrenceCount || 1;
      row.appendChild(tdCount);

      // 6. Last Seen
      const tdLastSeen = document.createElement('td');
      tdLastSeen.textContent = formatDate(inc.lastSeen || inc.updatedAt);
      row.appendChild(tdLastSeen);

      // 7. Action
      const tdAction = document.createElement('td');
      const link = document.createElement('a');
      link.href = `incident-details.html?id=${encodeURIComponent(inc.id)}`;
      link.className = 'btn btn-secondary btn-sm';
      link.textContent = 'View Details';
      tdAction.appendChild(link);
      row.appendChild(tdAction);

      tableBody.appendChild(row);
    }
  }

  async function fetchIncidents() {
    try {
      hideAlert();
      if (tableBody) {
        tableBody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 2rem; color: #64748b;">Loading incidents from RethinkDB...</td></tr>';
      }

      const data = await apiFetch('/api/incidents');
      if (!Array.isArray(data)) {
        throw new Error('Received unexpected non-array format from /api/incidents.');
      }
      cachedIncidents = data;
      renderTableRows(cachedIncidents);
    } catch (err) {
      console.error('Error fetching incidents:', err);
      showAlert(`Failed to retrieve incidents: ${err.message}`, 'error');
      if (tableBody) {
        tableBody.innerHTML = `<tr><td colspan="7" style="text-align:center; padding: 2rem; color: #dc2626;">Error loading data. Check console or verify server is running.</td></tr>`;
      }
    }
  }

  // Setup filter button listeners
  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentFilter = btn.getAttribute('data-status') || 'ALL';
      renderTableRows(cachedIncidents);
    });
  });

  // Initial load
  await fetchIncidents();

  // Live updates via RethinkDB changefeed
  initLiveStream((change) => {
    if (!change) return;
    const { new_val, old_val } = change;

    if (!old_val && new_val) {
      // New incident created: prepend to cached list
      cachedIncidents = [new_val, ...cachedIncidents.filter(i => i.id !== new_val.id)];
    } else if (new_val && old_val) {
      // Incident updated: replace in cache
      cachedIncidents = cachedIncidents.map(i => i.id === new_val.id ? new_val : i);
    } else if (!new_val && old_val) {
      // Incident deleted
      cachedIncidents = cachedIncidents.filter(i => i.id !== old_val.id);
    }

    renderTableRows(cachedIncidents);
  });
}

// ==========================================
// 4. PAGE C: INCIDENT DETAILS LOGIC (incident-details.html)
// ==========================================

async function initIncidentDetails() {
  const container = document.getElementById('details-content');
  const alertEl = document.getElementById('details-alert');

  // DOM fields
  const titleEl = document.getElementById('inc-title');
  const idEl = document.getElementById('inc-id');
  const serviceEl = document.getElementById('inc-service');
  const statusEl = document.getElementById('inc-status');
  const severityEl = document.getElementById('inc-severity');
  const countEl = document.getElementById('inc-count');
  const firstSeenEl = document.getElementById('inc-first-seen');
  const lastSeenEl = document.getElementById('inc-last-seen');
  const resolvedAtEl = document.getElementById('inc-resolved-at');
  const firstMsgEl = document.getElementById('inc-first-msg');
  const lastMsgEl = document.getElementById('inc-last-msg');
  const recoveryMsgRow = document.getElementById('row-recovery-msg');
  const recoveryMsgEl = document.getElementById('inc-recovery-msg');

  // Action controls
  const btnAck = document.getElementById('btn-ack');
  const btnResolve = document.getElementById('btn-resolve');
  const lifecycleNotice = document.getElementById('lifecycle-notice');

  // Events table
  const eventsTableBody = document.getElementById('events-table-body');

  let currentIncident = null;
  let isUpdating = false;

  function showAlert(message, type = 'error') {
    if (!alertEl) return;
    alertEl.className = `alert alert-${type}`;
    alertEl.textContent = message;
    alertEl.style.display = 'flex';
  }

  function hideAlert() {
    if (alertEl) alertEl.style.display = 'none';
  }

  // Extract ID from query parameter: ?id=...
  const urlParams = new URLSearchParams(window.location.search);
  const incidentId = urlParams.get('id');

  if (!incidentId || !incidentId.trim()) {
    showAlert('No incident ID provided in the URL. Please return to the incident list and select an incident.', 'error');
    if (container) container.style.display = 'none';
    return;
  }

  /**
   * Update the UI fields with incident data.
   * @param {Object} inc
   */
  function renderIncidentData(inc) {
    currentIncident = inc;
    if (container) container.style.display = 'block';

    // Title / Type
    if (titleEl) titleEl.textContent = inc.type || inc.title || 'Untitled Incident';
    if (idEl) idEl.textContent = inc.id || 'N/A';
    if (serviceEl) serviceEl.textContent = inc.service || 'Not available';

    // Status & Severity badges
    if (statusEl) {
      statusEl.innerHTML = '';
      statusEl.appendChild(createStatusBadge(inc.status));
    }
    if (severityEl) {
      severityEl.innerHTML = '';
      severityEl.appendChild(createSeverityBadge(inc.severity));
    }

    // Counts and timestamps
    if (countEl) countEl.textContent = `${inc.occurrenceCount || 1} grouped failure(s)`;
    if (firstSeenEl) firstSeenEl.textContent = formatDate(inc.firstSeen || inc.createdAt);
    if (lastSeenEl) lastSeenEl.textContent = formatDate(inc.lastSeen || inc.updatedAt);

    if (resolvedAtEl) {
      resolvedAtEl.textContent = inc.resolvedAt ? formatDate(inc.resolvedAt) : 'Not resolved';
    }

    // Messages
    if (firstMsgEl) firstMsgEl.textContent = inc.firstMessage || 'No initial message recorded';
    if (lastMsgEl) lastMsgEl.textContent = inc.lastMessage || inc.firstMessage || 'No recent message';

    if (recoveryMsgRow && recoveryMsgEl) {
      if (inc.recoveryMessage) {
        recoveryMsgRow.style.display = 'flex';
        recoveryMsgEl.textContent = inc.recoveryMessage;
      } else {
        recoveryMsgRow.style.display = 'none';
      }
    }

    // Lifecycle Controls State
    updateLifecycleButtons(inc.status);

    // Render Events Breakdown
    renderEventsTable(inc);
  }

  /**
   * Configure Acknowledge & Resolve button states based on current status.
   * Allowed backend transitions:
   * OPEN -> ACKNOWLEDGED, RESOLVED
   * ACKNOWLEDGED -> OPEN, RESOLVED
   * RESOLVED -> terminal
   */
  function updateLifecycleButtons(status) {
    const s = String(status || '').toUpperCase();

    if (!btnAck || !btnResolve || !lifecycleNotice) return;

    if (s === 'OPEN') {
      btnAck.disabled = false;
      btnAck.style.display = 'inline-flex';
      btnAck.textContent = 'Acknowledge Incident';

      btnResolve.disabled = false;
      btnResolve.style.display = 'inline-flex';
      btnResolve.textContent = 'Resolve Incident';

      lifecycleNotice.textContent = 'Status is OPEN. You can acknowledge or resolve this incident.';
    } else if (s === 'ACKNOWLEDGED') {
      btnAck.disabled = true;
      btnAck.style.display = 'none'; // Acknowledged already

      btnResolve.disabled = false;
      btnResolve.style.display = 'inline-flex';
      btnResolve.textContent = 'Resolve Incident';

      lifecycleNotice.textContent = 'Incident is ACKNOWLEDGED. It can now be marked as RESOLVED.';
    } else if (s === 'RESOLVED') {
      btnAck.disabled = true;
      btnAck.style.display = 'none';

      btnResolve.disabled = true;
      btnResolve.style.display = 'none';

      lifecycleNotice.textContent = 'This incident is RESOLVED. Terminal state: no further status transitions permitted.';
    } else {
      btnAck.disabled = true;
      btnResolve.disabled = true;
      lifecycleNotice.textContent = `Unknown status: "${s}". Actions disabled.`;
    }
  }

  /**
   * Render the aggregated event milestones from actual backend document attributes.
   * Sentinel groups repeat failures into occurrenceCount, firstSeen, and lastSeen rather than storing individual rows.
   */
  function renderEventsTable(inc) {
    if (!eventsTableBody) return;
    eventsTableBody.innerHTML = '';

    const rowsData = [];

    // 1. Initial observed failure
    rowsData.push({
      milestone: 'Initial Failure Detected',
      timestamp: inc.firstSeen || inc.createdAt,
      count: '1st event (created incident)',
      message: inc.firstMessage || 'Initial failure event triggered this incident.'
    });

    // 2. Latest observed failure (if occurrenceCount > 1)
    if (inc.occurrenceCount > 1) {
      rowsData.push({
        milestone: 'Latest Failure Observed',
        timestamp: inc.lastSeen || inc.updatedAt,
        count: `${inc.occurrenceCount} total grouped events (aggregated into count)`,
        message: inc.lastMessage || 'Most recently observed failure message.'
      });
    }

    // 3. Resolution event (if resolved)
    if (inc.status === 'RESOLVED' && inc.resolvedAt) {
      rowsData.push({
        milestone: 'Resolution Event',
        timestamp: inc.resolvedAt,
        count: 'Lifecycle Resolution',
        message: inc.recoveryMessage || 'Incident marked as resolved.'
      });
    }

    for (const item of rowsData) {
      const tr = document.createElement('tr');

      const tdMilestone = document.createElement('td');
      tdMilestone.style.fontWeight = '600';
      tdMilestone.textContent = item.milestone;
      tr.appendChild(tdMilestone);

      const tdTime = document.createElement('td');
      tdTime.textContent = formatDate(item.timestamp);
      tr.appendChild(tdTime);

      const tdCount = document.createElement('td');
      tdCount.style.fontSize = '0.85rem';
      tdCount.style.color = '#475569';
      tdCount.textContent = item.count;
      tr.appendChild(tdCount);

      const tdMsg = document.createElement('td');
      tdMsg.textContent = item.message;
      tr.appendChild(tdMsg);

      eventsTableBody.appendChild(tr);
    }
  }

  /**
   * Perform status transition request to PATCH /api/incidents/:id/status
   * @param {string} nextStatus
   */
  async function handleStatusChange(nextStatus) {
    if (isUpdating) return;
    if (!currentIncident) return;

    isUpdating = true;
    hideAlert();

    // Disable buttons during in-flight request
    if (btnAck) btnAck.disabled = true;
    if (btnResolve) btnResolve.disabled = true;

    const originalAckText = btnAck ? btnAck.textContent : '';
    const originalResolveText = btnResolve ? btnResolve.textContent : '';

    if (nextStatus === 'ACKNOWLEDGED' && btnAck) {
      btnAck.textContent = 'Updating...';
    } else if (nextStatus === 'RESOLVED' && btnResolve) {
      btnResolve.textContent = 'Resolving...';
    }

    try {
      const updatedIncident = await apiFetch(`/api/incidents/${encodeURIComponent(incidentId)}/status`, {
        method: 'PATCH',
        body: JSON.stringify({ status: nextStatus })
      });

      showAlert(`Incident successfully updated to ${nextStatus}.`, 'success');
      renderIncidentData(updatedIncident);
    } catch (err) {
      console.error('Status transition failed:', err);
      showAlert(`Cannot update status: ${err.message}`, 'error');

      // Re-fetch latest incident state from server to prevent stale state
      await loadIncident();
    } finally {
      isUpdating = false;
      if (btnAck) btnAck.textContent = originalAckText;
      if (btnResolve) btnResolve.textContent = originalResolveText;
      if (currentIncident) updateLifecycleButtons(currentIncident.status);
    }
  }

  // Hook up button click listeners
  if (btnAck) {
    btnAck.addEventListener('click', () => handleStatusChange('ACKNOWLEDGED'));
  }
  if (btnResolve) {
    btnResolve.addEventListener('click', () => handleStatusChange('RESOLVED'));
  }

  /**
   * Fetch single incident from backend.
   */
  async function loadIncident() {
    try {
      hideAlert();
      // Use direct GET /api/incidents/:id endpoint
      const inc = await apiFetch(`/api/incidents/${encodeURIComponent(incidentId)}`);
      if (!inc) {
        throw new Error('Incident not found.');
      }
      renderIncidentData(inc);
    } catch (err) {
      console.error('Failed to load incident:', err);
      showAlert(`Incident error: ${err.message}`, 'error');
      if (container) container.style.display = 'none';
    }
  }

  // Initial load
  await loadIncident();

  // Real-time updates via RethinkDB changefeed
  initLiveStream((change) => {
    if (!change) return;
    const { new_val, old_val } = change;

    // Check if the change applies to this incident
    if (new_val && new_val.id === incidentId) {
      showAlert('Notice: This incident was just updated in real-time from RethinkDB changefeed.', 'info');
      renderIncidentData(new_val);
    } else if (!new_val && old_val && old_val.id === incidentId) {
      showAlert('Notice: This incident was deleted from the database.', 'error');
      if (container) container.style.display = 'none';
    }
  });
}

// ==========================================
// 5. AUTO-INITIALIZE ON PAGE LOAD
// ==========================================

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('metric-total')) {
    initDashboard();
  } else if (document.getElementById('incidents-table-body')) {
    initIncidentsList();
  } else if (document.getElementById('incident-details-container')) {
    initIncidentDetails();
  }
});
