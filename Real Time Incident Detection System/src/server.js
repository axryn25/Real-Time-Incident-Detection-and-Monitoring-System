require('dotenv').config();
const path = require('path');
const express = require('express');
const { ensureDatabase, closeConnection, getConnection, r } = require('./database');
const {
  processEvent,
  updateIncidentStatus,
  listIncidents,
  getIncidentById,
  getServiceHealthFromDb,
  listIncidentsWithServiceInfo,
  getRawEqJoinDemo,
  listServices,
  upsertService
} = require('./incidentService');

const app = express();
app.use(express.json());

// Serve static frontend files from public directory
app.use(express.static(path.join(__dirname, '../public')));

app.get('/api/health', (_req, res) => res.json({ ok: true, module: 'incident-management' }));

// Powered by RethinkDB group aggregation + services table join (frontend contract stays identical)
app.get('/api/service-health', async (_req, res) => {
  try {
    const result = await getServiceHealthFromDb();
    res.json(result);
  } catch (error) {
    console.error('Service health error:', error.message);
    res.status(500).json({
      error: 'Failed to retrieve service health.'
    });
  }
});

// ─── JOIN & SERVICE DEMONSTRATION ROUTES (Evaluator / API access) ─────────────

// GET /api/incidents/enriched — incidents joined with services metadata
app.get('/api/incidents/enriched', async (req, res) => {
  try {
    const data = await listIncidentsWithServiceInfo({
      status: req.query.status,
      severity: req.query.severity
    });
    res.json(data);
  } catch (error) {
    console.error('Enriched incidents error:', error.message);
    res.status(500).json({ error: 'Failed to retrieve enriched incidents.' });
  }
});

// GET /api/incidents/eqjoin — direct ReQL eqJoin demo
app.get('/api/incidents/eqjoin', async (_req, res) => {
  try {
    const data = await getRawEqJoinDemo();
    res.json(data);
  } catch (error) {
    console.error('Raw eqJoin error:', error.message);
    res.status(500).json({ error: 'Failed to run eqJoin.' });
  }
});

// GET /api/services — list all registered services
app.get('/api/services', async (_req, res) => {
  try {
    const services = await listServices();
    res.json(services);
  } catch (error) {
    console.error('List services error:', error.message);
    res.status(500).json({ error: 'Failed to list services.' });
  }
});

// POST /api/services — register or update a service
app.post('/api/services', async (req, res) => {
  try {
    const service = await upsertService(req.body || {});
    res.status(201).json(service);
  } catch (error) {
    if (error.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    console.error('Upsert service error:', error.message);
    res.status(500).json({ error: 'Failed to register service.' });
  }
});


app.post('/api/events', async (req, res) => {
  try {
    const result = await processEvent(req.body);
    res.status(result.action === 'CREATED_NEW_INCIDENT' ? 201 : 200).json(result);
  } catch (error) {
    if (error.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    if (error.message && (error.message.includes('must be') || error.message.includes('Event body'))) {
      return res.status(400).json({ error: error.message });
    }
    console.error('Process event error:', error.message);
    res.status(500).json({ error: 'Failed to process event due to an internal server error.' });
  }
});

app.get('/api/incidents', async (req, res) => {
  try {
    const incidents = await listIncidents({
      service: req.query.service,
      status: req.query.status,
      severity: req.query.severity
    });
    res.json(incidents);
  } catch (error) {
    console.error('List incidents error:', error.message);
    res.status(500).json({ error: 'Failed to retrieve incidents.' });
  }
});

// SSE endpoint placed BEFORE /api/incidents/:id to prevent Express treating 'stream' as an ID
app.get('/api/incidents/stream', async (_req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  let cursor;
  let isClosed = false;

  res.on('close', () => {
    isClosed = true;
    if (cursor) {
      cursor.close().catch(() => {});
    }
  });

  try {
    const connection = getConnection();
    cursor = await r.table('incidents').changes().run(connection);
    cursor.each((err, change) => {
      if (isClosed || res.writableEnded) {
        if (cursor) cursor.close().catch(() => {});
        return false;
      }
      if (err) {
        if (!isClosed && !res.writableEnded) {
          res.write(`event: error\ndata: ${JSON.stringify({ error: 'Stream error' })}\n\n`);
        }
        return false;
      }
      res.write(`data: ${JSON.stringify(change)}\n\n`);
    });
  } catch (error) {
    if (!isClosed && !res.writableEnded) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: 'Failed to establish changefeed' })}\n\n`);
      res.end();
    }
  }
});

app.get('/api/incidents/:id', async (req, res) => {
  try {
    const incident = await getIncidentById(req.params.id);
    if (!incident) {
      return res.status(404).json({ error: 'Incident not found.' });
    }
    res.json(incident);
  } catch (error) {
    console.error('Get incident error:', error.message);
    res.status(500).json({ error: 'Failed to retrieve incident.' });
  }
});

app.patch('/api/incidents/:id/status', async (req, res) => {
  try {
    const status = req.body ? req.body.status : undefined;
    const incident = await updateIncidentStatus(req.params.id, status);
    res.json(incident);
  } catch (error) {
    if (error.statusCode) {
      return res.status(error.statusCode).json({ error: error.message });
    }
    console.error('Update incident status error:', error.message);
    res.status(500).json({ error: 'Failed to update incident status.' });
  }
});

const port = Number(process.env.PORT || 3000);

ensureDatabase()
  .then(() => {
    const server = app.listen(port, () => console.log(`Sentinel incident module listening on http://localhost:${port}`));
    const shutdown = async () => {
      server.close();
      await closeConnection();
      process.exit(0);
    };
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  })
  .catch(error => {
    console.error('Could not connect to RethinkDB:', error.message);
    process.exit(1);
  });
