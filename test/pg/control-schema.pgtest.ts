import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { DEFAULT_CONTROL_SCHEMA, PostgresStorage } from '../../src/storage/postgres.ts';
import { createDatabase } from './helper.ts';

/**
 * §5.22 Gate A — the registry belongs to the SHARD, not to a tenant.
 *
 * `databases` used to be created by `schema.sql` into the adapter's own schema, which is invisible
 * while there is one tenant and wrong the moment there are two: one schema per tenant would have
 * put the list of ALL tenants inside ONE of them, and Gate D's map cannot resolve a name from a
 * list it has to already know a tenant to read.
 *
 * Everything here runs TWO adapters over one Postgres database — different tenant schemas, one
 * control schema — because that is the smallest arrangement in which the old shape is wrong. A
 * single-adapter test would have passed before this gate and after it.
 */
const db = await createDatabase('gatea');
const open: PostgresStorage[] = [];
after(async () => {
  await Promise.allSettled(open.map((s) => s.close()));
  await db.drop();
});

/**
 * An adapter with an EXPLICIT control schema. Each test gets its own, because `databases` is now
 * shared by construction and `deepEqual` on the sidebar is the assertion worth keeping — one
 * registry per test is what lets it stay `deepEqual` instead of degrading to `includes`.
 */
const mk = (schema: string, control: string): PostgresStorage => {
  const s = new PostgresStorage({ url: db.url, schema, controlSchema: control });
  open.push(s);
  return s;
};

test('two tenants share ONE registry, and keep their own derived names', async () => {
  const car = mk('ns_car', 'ctl_share');
  const chat = mk('ns_chat', 'ctl_share');

  // Declared through one adapter...
  await car.declareDatabase('car_race', 'console-rw-owner');
  // ...and visible through the other. THIS is the gate. Before it, `chat` read its own schema's
  // `databases` and saw nothing at all.
  assert.deepEqual(await chat.topNodes(), ['car_race']);

  // And the derived half stays per-tenant: data written into one tenant's `nodes` must not appear
  // in the other's sidebar, or the registry would have been hoisted at the cost of leaking rows.
  await car.commitGroup([{ writeId: crypto.randomUUID(), path: 'car_race/players/p1', op: 'put', value: 1 }]);
  await chat.commitGroup([{ writeId: crypto.randomUUID(), path: 'rooms/r1', op: 'put', value: 1 }]);

  assert.deepEqual(await car.topNodes(), ['car_race'], 'car sees its own data plus the shared registry');
  assert.deepEqual(await chat.topNodes(), ['car_race', 'rooms'], 'chat sees ITS data plus the same registry');
});

test('the registry table is in the control schema and NOWHERE in a tenant schema', async () => {
  const ns = mk('ns_where', 'ctl_where');
  await ns.head(); // applies both schemas

  const c = await db.client();
  const { rows } = await c.query<{ table_schema: string }>(
    `SELECT table_schema FROM information_schema.tables
      WHERE table_name = 'databases' AND table_schema IN ('ns_where', 'ctl_where')`,
  );
  assert.deepEqual(rows.map((r) => r.table_schema), ['ctl_where'], 'the registry is the shard\'s, not the tenant\'s');

  // The tenant schema holds the three tables that ARE a tenant's, and only those.
  const { rows: tenant } = await c.query<{ table_name: string }>(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = 'ns_where' ORDER BY table_name`,
  );
  assert.deepEqual(tenant.map((r) => r.table_name), ['nodes', 'oplog', 'rev_counter']);
});

test('a tenant may not own the control schema, and neither may take a name that is not an identifier', async () => {
  // Silent would be catastrophic here: a tenant schema equal to the control schema puts the list of
  // all tenants back inside one of them, which is the exact shape this gate undoes.
  assert.throws(
    () => new PostgresStorage({ url: db.url, schema: DEFAULT_CONTROL_SCHEMA }),
    /is the control schema/,
  );
  assert.throws(
    () => new PostgresStorage({ url: db.url, schema: 'ns_a', controlSchema: 'ctl; DROP SCHEMA public' }),
    /illegal control schema name/,
  );
  // The tenant guard that already existed still says which name it means.
  assert.throws(
    () => new PostgresStorage({ url: db.url, schema: 'public; DROP DATABASE x' }),
    /illegal schema name/,
  );
});

test('two shards can share one Postgres database without sharing a registry', async () => {
  // What `RTDB_CONTROL_SCHEMA` is for, and the reason it is a knob at all: `RTDB_PG_SCHEMA` already
  // lets two independent shards share one database, and a registry that ignored that would silently
  // merge them.
  const shardA = mk('sa_ns', 'sa_control');
  const shardB = mk('sb_ns', 'sb_control');
  await shardA.declareDatabase('only_on_a', 'console-rw-owner');
  await shardB.declareDatabase('only_on_b', 'console-rw-owner');
  assert.deepEqual(await shardA.topNodes(), ['only_on_a']);
  assert.deepEqual(await shardB.topNodes(), ['only_on_b']);
});

test('N adapters starting at once all get one registry, and none deadlocks', async () => {
  // `#init` now takes TWO advisory locks. Every adapter takes the SAME control lock and its OWN
  // tenant lock, so the order (control first, always) is what stops a cycle. Gate D is the reason
  // this matters: N adapters coming up together is its normal startup, not a stress case.
  const many = Array.from({ length: 8 }, (_, i) => mk(`race_${i}`, 'ctl_race'));
  await Promise.all(many.map((s) => s.head()));
  await many[0]?.declareDatabase('declared_under_contention', 'console-rw-owner');
  const seen = await Promise.all(many.map((s) => s.topNodes()));
  for (const names of seen) assert.deepEqual(names, ['declared_under_contention']);
});
