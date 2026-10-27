const { r, getConnection } = require('./database');

const SEVERITIES = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'];
const VALID_STATUSES = ['OPEN', 'ACKNOWLEDGED', 'RESOLVED'];
const TRANSITIONS = {
  OPEN: ['ACKNOWLEDGED', 'RESOLVED'],
  ACKNOWLEDGED: ['OPEN', 'RESOLVED'],
  RESOLVED: []
};

// Serializes events with the same service/type inside this Node process.
// This prevents two simultaneous first events from creating duplicate incidents
// in our single-instance coursework prototype.
const pendingByKey = new Map();

function withKeyLock(key, task) {
  const previous = pendingByKey.get(key) || Promise.resolve();
  const current = previous.catch(() => {}).then(task);
  pendingByKey.set(key, current);
  return current.finally(() => {
    if (pendingByKey.get(key) === current) pendingByKey.delete(key);
  });
}

function validateEvent(event) {
  if (!event || typeof event !== 'object') throw new Error('Event body must be an object.');

  const service = typeof event.service === 'string' ? event.service.trim() : '';
  if (!service) throw new Error('service must be a non-empty string.');

  const type = typeof event.type === 'string' ? event.type.trim() : '';
  if (!type) throw new Error('type must be a non-empty string.');

  const severity = String(event.severity || 'MEDIUM').trim().toUpperCase();
  if (!SEVERITIES.includes(severity)) {
    throw new Error(`severity must be one of: ${SEVERITIES.join(', ')}.`);
  }

  const action = String(event.action || 'FAILURE').trim().toUpperCase();
  if (!['FAILURE', 'RECOVERY'].includes(action)) {
    throw new Error('action must be FAILURE or RECOVERY.');
  }

  const timestamp = event.timestamp ? new Date(event.timestamp) : new Date();
  if (Number.isNaN(timestamp.getTime())) throw new Error('timestamp must be a valid date.');

  return {
    service,
    type,
    severity,
    action,
    timestamp: timestamp.toISOString(),
    message: event.message ? String(event.message).trim() : ''
  };
}

async function findRecentActiveIncident(event, windowMinutes) {
  const connection = getConnection();
  const cutoff = new Date(Date.parse(event.timestamp) - windowMinutes * 60 * 1000).toISOString();

  // Use the service index first, then narrow by type, state, and time window.
  const cursor = await r.table('incidents')
    .getAll(event.service, { index: 'service' })
    .filter(function (incident) {
      return incident('type').eq(event.type)
        .and(incident('status').ne('RESOLVED'))
        .and(incident('lastSeen').ge(cutoff));
    })
    .orderBy(r.desc('lastSeen'))
    .limit(1)
    .run(connection);

  const matches = await cursor.toArray();
  return matches.length ? matches[0] : null;
}

async function resolveForRecovery(event) {
  const connection = getConnection();
  const incident = await findRecentActiveIncident(event, 50 * 365 * 24 * 60); // broad enough for this prototype
  if (!incident) {
    return { action: 'NO_ACTIVE_INCIDENT', incident: null };
  }

  const now = new Date().toISOString();
  await r.table('incidents').get(incident.id).update({
    status: 'RESOLVED',
    resolvedAt: now,
    lastSeen: event.timestamp,
    updatedAt: now,
    recoveryMessage: event.message || 'Recovery event received'
  }).run(connection);

  const updated = await r.table('incidents').get(incident.id).run(connection);
  return { action: 'RESOLVED_BY_RECOVERY', incident: updated };
}

async function processEvent(rawEvent) {
  const event = validateEvent(rawEvent);
  const connection = getConnection();
  const windowMinutes = Number(process.env.INCIDENT_WINDOW_MINUTES || 5);
  const lockKey = `${event.service}::${event.type}`;

  return withKeyLock(lockKey, async () => {
    if (event.action === 'RECOVERY') return resolveForRecovery(event);

    const existing = await findRecentActiveIncident(event, windowMinutes);
    const now = new Date().toISOString();

    if (existing) {
      // RethinkDB applies this update atomically to the existing incident.
      await r.table('incidents').get(existing.id).update(function (incident) {
        return {
          occurrenceCount: incident('occurrenceCount').add(1),
          lastSeen: r.max(incident('lastSeen'), event.timestamp),
          updatedAt: now,
          severity: event.severity,
          lastMessage: event.message
        };
      }).run(connection);

      const updated = await r.table('incidents').get(existing.id).run(connection);
      return { action: 'GROUPED_WITH_EXISTING', incident: updated };
    }

    const newIncident = {
      service: event.service,
      type: event.type,
      severity: event.severity,
      status: 'OPEN',
      occurrenceCount: 1,
      firstSeen: event.timestamp,
      lastSeen: event.timestamp,
      createdAt: now,
      updatedAt: now,
      resolvedAt: null,
      firstMessage: event.message,
      lastMessage: event.message
    };

    const result = await r.table('incidents').insert(newIncident).run(connection);
    const incident = await r.table('incidents').get(result.generated_keys[0]).run(connection);
    return { action: 'CREATED_NEW_INCIDENT', incident };
  });
}

async function updateIncidentStatus(id, nextStatus) {
  const connection = getConnection();
  const status = String(nextStatus || '').trim().toUpperCase();
  if (!VALID_STATUSES.includes(status)) {
    const error = new Error(`status must be one of: ${VALID_STATUSES.join(', ')}.`);
    error.statusCode = 400;
    throw error;
  }

  const incident = await r.table('incidents').get(id).run(connection);
  if (!incident) {
    const error = new Error('Incident not found.');
    error.statusCode = 404;
    throw error;
  }

  if (!TRANSITIONS[incident.status].includes(status)) {
    const error = new Error(`Invalid status transition: ${incident.status} -> ${status}.`);
    error.statusCode = 409;
    throw error;
  }

  const now = new Date().toISOString();
  const patch = { status, updatedAt: now };
  if (status === 'RESOLVED') patch.resolvedAt = now;
  if (status !== 'RESOLVED') patch.resolvedAt = null;

  // Safer status update: verify status hasn't changed concurrently
  const updateResult = await r.table('incidents')
    .filter(r.row('id').eq(id).and(r.row('status').eq(incident.status)))
    .update(patch)
    .run(connection);

  if (updateResult.replaced === 0) {
    const latest = await r.table('incidents').get(id).run(connection);
    if (!latest) {
      const error = new Error('Incident not found.');
      error.statusCode = 404;
      throw error;
    }
    const error = new Error(`Status update conflict: incident was concurrently updated to ${latest.status}.`);
    error.statusCode = 409;
    throw error;
  }

  return r.table('incidents').get(id).run(connection);
}

async function listIncidents(filters = {}) {
  const connection = getConnection();
  let query = r.table('incidents');

  if (filters.service) {
    query = query.getAll(String(filters.service).trim(), { index: 'service' });
    if (filters.status) {
      const status = String(filters.status).trim().toUpperCase();
      query = query.filter({ status });
    }
  } else if (filters.status) {
    // Utilize secondary index on status when filtering by status alone
    const status = String(filters.status).trim().toUpperCase();
    query = query.getAll(status, { index: 'status' });
  }

  if (filters.severity) {
    const severity = String(filters.severity).trim().toUpperCase();
    query = query.filter({ severity });
  }

  const cursor = await query.orderBy(r.desc('lastSeen')).run(connection);
  return cursor.toArray();
}

async function getIncidentById(id) {
  const connection = getConnection();
  const incident = await r.table('incidents').get(id).run(connection);
  return incident;
}

// ─── MULTI-TABLE JOIN & AGGREGATION QUERIES ─────────────────────────────────

/**
 * eqJoin: incidents joined with services table on incident.service == services.name.
 * Merges service metadata (owner, sla, region) into incident documents.
 * Equivalent SQL:
 *   SELECT i.*, s.owner, s.sla, s.region
 *   FROM incidents i LEFT JOIN services s ON i.service = s.name
 *   ORDER BY i.lastSeen DESC
 */
async function listIncidentsWithServiceInfo(filters = {}) {
  const connection = getConnection();
  let query = r.table('incidents');

  if (filters.status) {
    const status = String(filters.status).trim().toUpperCase();
    query = query.getAll(status, { index: 'status' });
  }

  if (filters.severity) {
    const severity = String(filters.severity).trim().toUpperCase();
    query = query.filter({ severity });
  }

  const cursor = await query
    .orderBy(r.desc('lastSeen'))
    .map(function (incident) {
      const svc = r.table('services').get(incident('service')).default({});
      return incident.merge({
        owner: svc('owner').default(null),
        sla: svc('sla').default(null),
        region: svc('region').default(null)
      });
    })
    .run(connection);

  return cursor.toArray();
}

/**
 * Direct demonstration of RethinkDB eqJoin() + zip() ReQL construct.
 */
async function getRawEqJoinDemo() {
  const connection = getConnection();
  const cursor = await r.table('incidents')
    .eqJoin('service', r.table('services'))
    .zip()
    .run(connection);
  return cursor.toArray();
}

/**
 * Computes service health using RethinkDB group-by aggregation and joined service metadata.
 * Directly powers GET /api/service-health so the frontend UI stays 100% unchanged.
 */
async function getServiceHealthFromDb() {
  const connection = getConnection();

  // Group incidents by service in RethinkDB and aggregate totals/unresolved counts
  const cursor = await r.table('incidents')
    .group('service')
    .ungroup()
    .map(function (row) {
      const openCount = row('reduction').filter(function (inc) {
        return inc('status').ne('RESOLVED');
      }).count();

      const criticalCount = row('reduction').filter(function (inc) {
        return inc('severity').eq('CRITICAL').and(inc('status').ne('RESOLVED'));
      }).count();

      return {
        service: row('group'),
        totalIncidents: row('reduction').count(),
        openIncidents: openCount,
        criticalIncidents: criticalCount
      };
    })
    .run(connection);

  const incidentSummary = await cursor.toArray();
  const summaryByService = new Map(incidentSummary.map(item => [item.service, item]));

  // Pull registered services from services table
  const servicesCursor = await r.table('services').run(connection);
  const registeredServices = await servicesCursor.toArray();

  const results = [];
  const handled = new Set();

  for (const svc of registeredServices) {
    handled.add(svc.name);
    const stats = summaryByService.get(svc.name) || {
      totalIncidents: 0,
      openIncidents: 0,
      criticalIncidents: 0
    };
    results.push({
      service: svc.name,
      owner: svc.owner || 'Engineering',
      sla: svc.sla || 'TIER_2',
      region: svc.region || 'us-east-1',
      totalIncidents: stats.totalIncidents,
      openIncidents: stats.openIncidents,
      criticalIncidents: stats.criticalIncidents,
      health: stats.openIncidents > 0 ? 'Needs Attention' : 'Healthy'
    });
  }

  // Also include any unregistered services present in incidents
  for (const [name, stats] of summaryByService.entries()) {
    if (!handled.has(name)) {
      results.push({
        service: name,
        owner: 'Unassigned',
        sla: 'TIER_3',
        region: 'N/A',
        totalIncidents: stats.totalIncidents,
        openIncidents: stats.openIncidents,
        criticalIncidents: stats.criticalIncidents,
        health: stats.openIncidents > 0 ? 'Needs Attention' : 'Healthy'
      });
    }
  }

  return results;
}

async function listServices() {
  const connection = getConnection();
  const cursor = await r.table('services').orderBy('name').run(connection);
  return cursor.toArray();
}

async function upsertService(data) {
  const name = typeof data.name === 'string' ? data.name.trim() : '';
  if (!name) {
    const error = new Error('name must be a non-empty string.');
    error.statusCode = 400;
    throw error;
  }
  const connection = getConnection();
  const doc = {
    name,
    owner: typeof data.owner === 'string' ? data.owner.trim() : 'Engineering',
    sla: typeof data.sla === 'string' ? data.sla.trim().toUpperCase() : 'TIER_2',
    region: typeof data.region === 'string' ? data.region.trim() : 'us-east-1',
    updatedAt: new Date().toISOString()
  };
  await r.table('services').insert(doc, { conflict: 'update' }).run(connection);
  return r.table('services').get(name).run(connection);
}

module.exports = {
  processEvent,
  updateIncidentStatus,
  listIncidents,
  validateEvent,
  getIncidentById,
  listIncidentsWithServiceInfo,
  getRawEqJoinDemo,
  getServiceHealthFromDb,
  listServices,
  upsertService
};
