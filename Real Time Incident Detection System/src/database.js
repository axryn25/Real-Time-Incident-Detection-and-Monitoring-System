const r = require('rethinkdb');

let connection;

async function connect() {
  if (connection) return connection;

  connection = await r.connect({
    host: process.env.RETHINKDB_HOST || 'localhost',
    port: Number(process.env.RETHINKDB_PORT || 28015),
    db: process.env.RETHINKDB_DB || 'sentinel'
  });

  return connection;
}

async function ensureDatabase() {
  // Create the database first, if it does not exist.
  const dbName = process.env.RETHINKDB_DB || 'sentinel';
  const connectionWithoutDb = await r.connect({
    host: process.env.RETHINKDB_HOST || 'localhost',
    port: Number(process.env.RETHINKDB_PORT || 28015)
  });

  try {
    const databases = await r.dbList().run(connectionWithoutDb);
    if (!databases.includes(dbName)) {
      await r.dbCreate(dbName).run(connectionWithoutDb);
    }
  } finally {
    connectionWithoutDb.close();
  }

  connection = await r.connect({
    host: process.env.RETHINKDB_HOST || 'localhost',
    port: Number(process.env.RETHINKDB_PORT || 28015),
    db: dbName
  });

  const tables = await r.tableList().run(connection);
  if (!tables.includes('incidents')) {
    await r.tableCreate('incidents').run(connection);
  }
  // Secondary table: stores service metadata for multi-table joins (eqJoin target)
  if (!tables.includes('services')) {
    await r.tableCreate('services', { primaryKey: 'name' }).run(connection);
  }

  const indexes = await r.table('incidents').indexList().run(connection);
  if (!indexes.includes('service')) {
    await r.table('incidents').indexCreate('service').run(connection);
  }
  if (!indexes.includes('status')) {
    await r.table('incidents').indexCreate('status').run(connection);
  }
  await r.table('incidents').indexWait().run(connection);

  // Auto-seed default services if table is empty for joins
  const serviceCount = await r.table('services').count().run(connection);
  if (serviceCount === 0) {
    await r.table('services').insert([
      { name: 'payment-service', owner: 'Payments Engineering', sla: 'TIER_1', region: 'us-east-1', updatedAt: new Date().toISOString() },
      { name: 'payment-api', owner: 'Payments Engineering', sla: 'TIER_1', region: 'us-east-1', updatedAt: new Date().toISOString() },
      { name: 'inventory-api', owner: 'Catalog & Inventory Team', sla: 'TIER_2', region: 'us-east-2', updatedAt: new Date().toISOString() },
      { name: 'order-api', owner: 'Checkout & Orders Team', sla: 'TIER_1', region: 'us-west-1', updatedAt: new Date().toISOString() },
      { name: 'auth-api', owner: 'Identity & Security Team', sla: 'TIER_1', region: 'global', updatedAt: new Date().toISOString() },
      { name: 'notification-service', owner: 'Customer Comm Team', sla: 'TIER_3', region: 'eu-west-1', updatedAt: new Date().toISOString() }
    ]).run(connection);
  }

  return connection;
}

function getConnection() {
  if (!connection) throw new Error('Database is not connected. Call ensureDatabase() first.');
  return connection;
}

async function closeConnection() {
  if (connection) {
    await connection.close();
    connection = undefined;
  }
}

module.exports = { r, connect, ensureDatabase, getConnection, closeConnection };
