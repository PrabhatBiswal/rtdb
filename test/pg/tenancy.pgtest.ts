import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import pg from 'pg';
import {
  DEFAULT_CONTROL_SCHEMA,
  openSharedTenancy,
  PostgresStorage,
  sharedPoolSize,
} from '../../src/storage/postgres.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import { startGateway } from '../../src/gateway/server.ts';
import * as M from '../../src/gateway/metrics.ts';
import { waitUntil, wsUrl } from '../helpers.ts';
import { createDatabase } from './helper.ts';

/**
 * §5.22 Gate D shart (A), and Gate F's precondition — what a multi-tenant gateway is allowed to
 * OPEN, and what it is allowed to CREATE.
 *
 * Both are claims about Postgres itself rather than about this repo's objects, so both are asserted
 * from `pg_stat_activity` and `pg_namespace` through a client on ANOTHER database: a counter that
 * counts itself is not a count.
 */
const db = await createDatabase('gated');

/** Declared before anything boots — this is what an owner does in the console. */
const seed = db.make(undefined, 'public');
await seed.declareDatabase('alpha', 'console-rw-owner');
await seed.declareDatabase('beta', 'console-rw-owner');
await seed.close();

const shared = await openSharedTenancy({ url: db.url, schema: 'public' });

/**
 * `pg_namespace` is PER DATABASE, so the inspector has to sit on the one being inspected — and then
 * it is itself a backend in the count. It says so in `application_name` and the count excludes it,
 * which is the same trick `CommitListener` uses to be findable at all.
 */
const INSPECTOR = 'rtdb-test-inspector';
const admin = new pg.Client({ connectionString: db.url, application_name: INSPECTOR });
await admin.connect();

const backends = async (): Promise<{ total: number; listeners: number }> => {
  const { rows } = await admin.query<{ total: string; listeners: string }>(
    `SELECT count(*) AS total,
            count(*) FILTER (WHERE application_name = 'rtdb-commit-listener') AS listeners
       FROM pg_stat_activity WHERE datname = $1 AND application_name <> $2`,
    [db.name, INSPECTOR],
  );
  const r = rows[0] as { total: string; listeners: string };
  return { total: Number(r.total), listeners: Number(r.listeners) };
};

// In order, and the order is the point: `db.drop()` is a FORCE disconnect, so anything still
// holding a connection when it runs shows up as an uncaught "terminating connection" afterwards.
after(async () => {
  await shared.close();
  await admin.end();
  await db.drop();
});

test('the default tenant shares the pool and the ONE listener, it does not bring its own', async () => {
  // 2 declared + the default tenant = N of 3. The default tenant is a database this gateway serves
  // and is never in the registry, which is the whole of the `+ 1`.
  assert.deepEqual(shared.declared, ['alpha', 'beta']);
  assert.equal(shared.max, sharedPoolSize(3));

  // Drive all three: the pool opens connections lazily and the listener connects on its first
  // LISTEN, so a count taken before any of that would pass with nothing wired at all.
  const stores = [shared.storage, shared.tenantStorage('alpha'), shared.tenantStorage('beta')];
  for (const s of stores) {
    s.onCommit(() => undefined);
    await s.commitGroup([{ writeId: crypto.randomUUID(), path: 'x/y', op: 'put', value: 1 }]);
  }
  await waitUntil(async () => (await backends()).listeners > 0, 'the listener attached');

  const { total, listeners } = await backends();
  // THE tooth for (A): built the other way round — `storageFromEnv()` first, shared pool after —
  // the default tenant keeps a `CommitListener` of its own and this is 2. It is also the half that
  // cannot be argued with, because a second listener is a second connection whatever the pool does.
  assert.equal(listeners, 1, 'one LISTEN connection for the whole gateway (Gate C)');
  assert.ok(
    total <= shared.max + 1,
    `${total} backends, ceiling ${shared.max} pool + 1 listener — a private default pool adds 10`,
  );
});

test('an UNDECLARED database never becomes a schema, however the token is signed', async (t) => {
  // Gate F's precondition, on the storage that actually creates things: opening a tenant runs the
  // factory, and `PostgresStorage#init` is a `CREATE SCHEMA IF NOT EXISTS`. Before `tenantFor`
  // consulted the registry, any token signed with the shard secret could name a database nobody
  // declared and make it, at hello.
  const schemaExists = async (name: string): Promise<boolean> =>
    (await admin.query('SELECT 1 FROM pg_namespace WHERE nspname = $1', [name])).rowCount === 1;

  const gw = await startGateway({
    storage: shared.storage,
    tenantStorage: shared.tenantStorage,
    requireNs: true,
    db: 'public',
  });
  t.after(() => {
    gw.close();
    M.resetSources();
  });

  const hello = async (ns: string): Promise<{ code: number | null; frames: Record<string, unknown>[] }> => {
    const ws = new WebSocket(wsUrl(gw.port));
    await new Promise((r) => ws.addEventListener('open', r, { once: true }));
    const frames: Record<string, unknown>[] = [];
    let code: number | null = null;
    ws.addEventListener('message', (ev) =>
      frames.push(JSON.parse(String(ev.data)) as Record<string, unknown>),
    );
    ws.addEventListener('close', (ev) => (code = (ev as { code: number }).code), { once: true });
    ws.send(
      JSON.stringify({
        type: 'hello',
        proto: 1,
        token: signDevToken({ sub: 'app-x', exp: Math.floor(Date.now() / 1000) + 3600, ns }),
      }),
    );
    // Either answer settles this: a refusal closes, an acceptance sends helloAck. Waiting on the
    // close alone would turn the broken case into a file timeout instead of a failed assertion.
    await waitUntil(
      () => code !== null || frames.some((f) => f['type'] === 'helloAck'),
      `hello for ${ns} was answered`,
    );
    ws.close();
    return { code, frames };
  };

  assert.equal(await schemaExists('gatecrash'), false, 'the schema does not exist to begin with');
  const refused = await hello('gatecrash');
  assert.equal(refused.code, 4401);
  assert.equal(refused.frames.find((f) => f['type'] === 'err')?.['code'], 'AUTH');
  assert.equal(await schemaExists('gatecrash'), false, 'and hello did not create it');

  // The other arm, so this is not "the gateway refuses everything": a DECLARED name connects, and
  // the schema it names IS created — which is exactly what makes the first half worth having.
  const accepted = await hello('alpha');
  assert.equal(accepted.code, null, 'a declared database is served');
  assert.ok(accepted.frames.some((f) => f['type'] === 'helloAck'));
  assert.equal(await schemaExists('alpha'), true);
});

test('every tenant on the shard is sized in ONE catalogue query, not one per tenant', async (t) => {
  // §5.24 Gate A. A scrape must not turn into N round trips against the shard the panel exists to
  // protect — and under §5.22 a tenant is a SCHEMA, so "every database" is a `GROUP BY nspname`.
  // The query count is taken from the pool this test owns, not from anything the adapter reports.
  const stores = ['alpha', 'beta'].map((schema) => shared.tenantStorage(schema));
  for (const [i, s] of stores.entries()) {
    await s.commitGroup([{ writeId: crypto.randomUUID(), path: `t${i}/x`, op: 'put', value: 'z'.repeat(2000) }]);
  }

  let queries = 0;
  const real = shared.pool.query.bind(shared.pool);
  (shared.pool as { query: unknown }).query = (...args: unknown[]) => {
    queries++;
    return (real as (...a: unknown[]) => unknown)(...args);
  };
  t.after(() => { (shared.pool as { query: unknown }).query = real; });

  await shared.storage.storageBytes(); // warm `#init`, whose DDL is not what is being counted
  queries = 0;
  const sizes = await shared.storage.storageBytes();

  assert.equal(queries, 1, `three tenant schemas, ${queries} queries`);
  for (const db of ['public', 'alpha', 'beta']) {
    assert.ok((sizes[db] ?? 0) > 0, `${db} was sized (${JSON.stringify(sizes)})`);
  }
  // And it is the LIVE relation only: the oplog is §9 retention, not the client's data, so a
  // client is not billed for our durability window. `nodes` alone is what the query names.
  const { rows } = await admin.query<{ n: string }>(
    `SELECT count(*)::text AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'alpha' AND c.relkind = 'r'`,
  );
  assert.ok(Number((rows[0] as { n: string }).n) > 1, 'the schema really holds more relations than nodes');
});

test('the quota column is added to a registry that already exists, and NULL means the shard default', async (t) => {
  // §5.24 Gate C. `CREATE TABLE IF NOT EXISTS` does NOTHING to a table that already exists, so a
  // shard that has been declaring databases since §5.19 would never grow this column — the
  // override would read null forever on exactly the deployments that have data. `ALTER TABLE ... ADD
  // COLUMN IF NOT EXISTS`, inside the same advisory lock, is what makes an existing shard migrate.
  //
  /**
   * An OLD shard, built by hand: the §5.19 registry exactly as it shipped, with no quota column and
   * a database already declared in it. This is the only arrangement that can catch the mistake —
   * a fresh test database gets the column from `CREATE TABLE` whatever the migration does, so a
   * test that only ever runs against a new schema would pass with no migration at all. (It did:
   * this assertion was green against a folded-in column until the setup below existed.)
   */
  const old = 'oldctl';
  await admin.query(`CREATE SCHEMA IF NOT EXISTS ${old}`);
  await admin.query(`CREATE TABLE IF NOT EXISTS ${old}.databases (
    name TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), created_by TEXT NOT NULL)`);
  await admin.query(`INSERT INTO ${old}.databases (name, created_by) VALUES ('legacy', 'console-rw-owner')
                     ON CONFLICT DO NOTHING`);
  const hasColumn = async (schema: string): Promise<boolean> => {
    const { rows } = await admin.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'databases' AND column_name = 'quota_acq_per_sec'`,
      [schema],
    );
    return Number((rows[0] as { n: string }).n) === 1;
  };
  assert.equal(await hasColumn(old), false, 'the old shard really has no quota column');

  const upgraded = new PostgresStorage({ url: db.url, schema: 'legacy', controlSchema: old });
  // Closed BEFORE the file's `after` drops the database: `db.drop()` is a FORCE disconnect, and an
  // adapter still holding a pool when it runs leaves the drop to fail and the database behind — one
  // leftover `rtdb_gated_*` after a full battery, which is exactly what the leftover check is for.
  // `t.after` runs when this TEST ends, which is before the file's own hook, so the order is right.
  t.after(() => upgraded.close());
  await upgraded.listDeclared(); // any call runs `#init`, which is where the migration lives
  assert.equal(await hasColumn(old), true, 'starting against it adds the column');
  assert.deepEqual(await upgraded.describeDatabase('legacy'), { quotaAcqPerSec: null }, 'and the old row survives');

  assert.equal(await hasColumn(DEFAULT_CONTROL_SCHEMA), true, 'a fresh shard has it too');

  await shared.storage.declareDatabase('gamma', 'console-rw-owner', 120);
  assert.deepEqual(await shared.storage.describeDatabase('gamma'), { quotaAcqPerSec: 120 });
  // Declared without one: null, and null is a STATEMENT — "nobody decided", so it follows the
  // shard's number rather than freezing today's into the row.
  assert.deepEqual(await shared.storage.describeDatabase('alpha'), { quotaAcqPerSec: null });
  // A name that is not in the registry at all — the default tenant's case.
  assert.equal(await shared.storage.describeDatabase('public'), null);

  // Re-declaring must not reset it, on this storage as on the other (`ON CONFLICT DO NOTHING`).
  await shared.storage.declareDatabase('gamma', 'console-rw-someone-else');
  assert.deepEqual(await shared.storage.describeDatabase('gamma'), { quotaAcqPerSec: 120 });
});
