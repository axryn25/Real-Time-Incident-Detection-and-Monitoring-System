# Sentinel — Person 2: Incident Management & Data Consistency

This module groups repeated failure events into incidents and manages incident status in RethinkDB.

## Requirements
- Node.js 18+
- RethinkDB running locally on port 28015

## Run
```bash
npm install
cp .env.example .env
npm start
```

The server creates the `sentinel` database and `incidents` table if they do not exist.

## Event contract
Send `POST /api/events` with JSON:
```json
{
  "service": "payment-api",
  "type": "PAYMENT_TIMEOUT",
  "severity": "HIGH",
  "action": "FAILURE",
  "timestamp": "2026-10-21T12:00:00.000Z",
  "message": "Payment request timed out"
}
```
- `service`: service name
- `type`: stable event category; recovery must use the same type
- `severity`: `LOW`, `MEDIUM`, `HIGH`, or `CRITICAL`
- `action`: `FAILURE` (default) or `RECOVERY`
- `timestamp`: ISO date/time; optional, defaults to now
- `message`: optional explanation

Repeated failures for the same service/type within `INCIDENT_WINDOW_MINUTES` (default 5) increment the incident's `occurrenceCount` instead of creating another incident. A recovery event resolves the latest active incident for that service/type.

## Endpoints
- `GET /api/health`
- `POST /api/events`
- `GET /api/incidents`
- `GET /api/incidents?service=payment-api&status=OPEN&severity=HIGH`
- `PATCH /api/incidents/:id/status` with `{ "status": "ACKNOWLEDGED" }` or `{ "status": "RESOLVED" }`

Allowed transitions: `OPEN -> ACKNOWLEDGED`, `OPEN -> RESOLVED`, `ACKNOWLEDGED -> OPEN`, `ACKNOWLEDGED -> RESOLVED`. `RESOLVED` is terminal; a later failure creates a new incident.

## Try it
```bash
curl -X POST http://localhost:3000/api/events \
  -H 'Content-Type: application/json' \
  -d '{"service":"payment-api","type":"PAYMENT_TIMEOUT","severity":"HIGH","message":"First timeout"}'

curl -X POST http://localhost:3000/api/events \
  -H 'Content-Type: application/json' \
  -d '{"service":"payment-api","type":"PAYMENT_TIMEOUT","severity":"HIGH","message":"Second timeout"}'

curl 'http://localhost:3000/api/incidents?service=payment-api'
```

## Coursework consistency note
Updating an existing incident's count uses a RethinkDB atomic update. The in-process keyed queue prevents duplicate first-time incidents from racing within this single Node process. This is a coursework prototype, not a distributed multi-server locking mechanism.
