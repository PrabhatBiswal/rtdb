import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import pg from 'pg';
import v8 from 'node:v8';
import vm from 'node:vm';
import {
  createPool,
  DEFAULT_CONTROL_SCHEMA,
  NO_TENANT_SCHEMA,
  PostgresStorage,
  sharedPoolSize,
} from '../../src/storage/postgres.ts';
import { createDatabase } from './helper.ts';

/**
 * §5.22 Gate B — one pool, many tenants, and the leak that would not have announced itself.
 *
 * This gate's failure is SILENT: a transaction that does not claim its connection writes into
 * whichever tenant borrowed it last, returns `ok`, and the row is simply in the wrong table. No
 * error, no log line, nothing for a dashboard to show. A test is the only witness, so these run
 * every tenant through a pool of ONE — the arrangement in which every checkout is guaranteed to be
 * somebody else's leftover connection, which is the rare case in production and the only case here.
 */
const db = await createDatabase('gateb');
const open: PostgresStorage[] = [];
const pools: pg.Pool[] = [];
after(async () => {
  await Promise.allSettled(open.map((s) => s.close()));
  await Promise.allSettled(pools.map((p) => p.end()));
  await db.drop();
});

/**
 * The adapter's OWN pool factory, not a copy of it. That is the point: `createPool` is what
 * `PostgresStorage` uses when nobody hands it one, so everything asserted here about a shared pool
 * is also asserted about the private one every existing test and today's deployment runs on. A
 * hand-built twin here would have tested the twin.
 */
function sharedPool(max = 1): pg.Pool {
  const p = createPool(db.url, max);
  pools.push(p);
  return p;
}

/** `SHOW` names its own output column; it cannot be aliased. */
const searchPath = async (pool: pg.Pool): Promise<string> =>
  (await pool.query<{ search_path: string }>('SHOW search_path')).rows[0]?.search_path ?? '';

const tenant = (pool: pg.Pool, schema: string, control: string): PostgresStorage => {
  const s = new PostgresStorage({ url: db.url, schema, controlSchema: control, pool });
  open.push(s);
  return s;
};

test('the pool default names a schema that DOES NOT EXIST — the invariant, not the constant', async () => {
  // The tooth the rest of this file could not bite. Every other assertion here compares against
  // `NO_TENANT_SCHEMA` itself, so redefining that constant moves the goalposts with the ball:
  // set it to `public` and all six pass, because THIS database's `public` happens to be empty.
  //
  // The invariant is not "the default equals the constant", it is a fact about the WORLD: the
  // default must name a schema nothing can resolve into. Two ways it can stop being true, and both
  // are checked, because only the first is about this repo:
  const c = await db.client();

  //  (1) The name exists as a schema. Then an unclaimed connection resolves into it and the
  //      structural defence is gone — quietly, because every statement would simply start working.
  const { rows } = await c.query<{ n: string }>(
    'SELECT count(*) AS n FROM pg_namespace WHERE nspname = $1',
    [NO_TENANT_SCHEMA],
  );
  assert.equal(Number((rows[0] as { n: string }).n), 0, `${NO_TENANT_SCHEMA} must not be a real schema`);

  //  (2) The name is one a TENANT can hold. `public` is the default tenant schema in BOTH
  //      `main.ts` and `PostgresStorage`, so production's own tenant schema is `public` — a default
  //      pointing there would resolve into a live tenant, and check (1) would still pass on a test
  //      database whose `public` is empty. This is the half that catches it anywhere.
  assert.notEqual(NO_TENANT_SCHEMA, 'public', 'the default tenant schema is not a place to park a connection');
  assert.notEqual(NO_TENANT_SCHEMA, DEFAULT_CONTROL_SCHEMA, 'nor is the shard\'s own control schema');
});

test('two tenants on ONE pooled connection do not see each other', async () => {
  // max: 1 — there is literally one backend, and both tenants take turns on it.
  const pool = sharedPool(1);
  const a = tenant(pool, 'b_a', 'b_ctl');
  const b = tenant(pool, 'b_b', 'b_ctl');

  await a.commitGroup([{ writeId: crypto.randomUUID(), path: 'shared/key', op: 'put', value: 'from-a' }]);
  await b.commitGroup([{ writeId: crypto.randomUUID(), path: 'shared/key', op: 'put', value: 'from-b' }]);

  // Same path, same connection, different tenants. If the preamble leaked, one of these would hold
  // the other's value — and both writes would have returned a perfectly normal ack.
  assert.equal((await a.readSnapshot('shared/key')).value, 'from-a');
  assert.equal((await b.readSnapshot('shared/key')).value, 'from-b');

  // And each tenant's revs are its own: a leaked write would have advanced the wrong counter.
  assert.equal(await a.head(), 1);
  assert.equal(await b.head(), 1);
});

test('every connection in the pool really is one backend, or this file proves nothing', async () => {
  // The control for the test above. If `max: 1` did not force reuse, "no leak" would be vacuous.
  const pool = sharedPool(1);
  const pids = new Set<number>();
  for (let i = 0; i < 6; i++) {
    const { rows } = await pool.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    pids.add(Number((rows[0] as { pid: number }).pid));
  }
  assert.equal(pids.size, 1, 'six checkouts, one backend — so the tenants above really did share it');
});

test('a connection at rest is bound to NO tenant, so a forgotten claim cannot land anywhere', async () => {
  const pool = sharedPool(1);
  const a = tenant(pool, 'b_rest', 'b_rest_ctl');
  await a.head(); // apply the schema, then hand the connection back

  // This is the "somebody forgot the preamble" case, spelled out. It must not resolve to `b_rest`
  // and it must not resolve to anything else either.
  await assert.rejects(
    () => pool.query('SELECT count(*) FROM nodes'),
    /relation "nodes" does not exist/,
    'an unclaimed connection resolves nothing — that is the whole footgun defence',
  );
  assert.equal(await searchPath(pool), NO_TENANT_SCHEMA);
});

test('the claim is LOCAL: it does not outlive the transaction that made it', async () => {
  // The mechanism, asserted directly rather than inferred from the leak not happening. `SET` here
  // instead of `SET LOCAL` is exactly the silent bug, and this is what would catch it.
  const pool = sharedPool(1);
  const a = tenant(pool, 'b_local', 'b_local_ctl');
  await a.commitGroup([{ writeId: crypto.randomUUID(), path: 'x', op: 'put', value: 1 }]);

  assert.equal(
    await searchPath(pool),
    NO_TENANT_SCHEMA,
    'after a committed transaction the connection is back to belonging to nobody',
  );
});

test('a borrowed pool outlives the adapter that used it', async () => {
  // `close()` on one tenant must not take the shared connections down with it — under Gate D that
  // would be one tenant shutting down every other one.
  const pool = sharedPool(2);
  const a = tenant(pool, 'b_own_a', 'b_own_ctl');
  const b = tenant(pool, 'b_own_b', 'b_own_ctl');
  await a.head();
  await b.head();

  await a.close();
  assert.equal(await b.head(), 0, 'the surviving tenant still works');
  await b.commitGroup([{ writeId: crypto.randomUUID(), path: 'still/here', op: 'put', value: 1 }]);
  assert.equal(await b.head(), 1);
});

test('N tenants on one pool add ONE handler to it, whatever N is', async () => {
  // Each adapter used to hook the pool's `remove` event itself. At 14 tenants Node warns at the
  // eleventh — an EventEmitter used as a fan-out registry — which was the noisy half of the defect.
  const pool = sharedPool(4);
  const warnings: string[] = [];
  const onWarn = (w: Error): void => void warnings.push(w.name);
  process.on('warning', onWarn);
  try {
    const many = Array.from({ length: 14 }, (_, i) => tenant(pool, `hook_${i}`, 'hook_ctl'));
    await Promise.all(many.map((t) => t.head()));
    assert.equal(pool.listenerCount('remove'), 1, 'one handler for the pool, not one per tenant');
    assert.deepEqual(warnings.filter((w) => w === 'MaxListenersExceededWarning'), []);
    await Promise.all(many.map((t) => t.close()));
  } finally {
    process.off('warning', onWarn);
  }
});

/**
 * The QUIET half of the same defect, and `listenerCount` cannot see it: it reads 1 before `close()`
 * and 1 after, because what `close()` gives back is a membership in a module-private set. The
 * mentor removed the unhook entirely and this file stayed 8/8 green — a test that named
 * "give it back on close" in its title and asserted nothing of the kind.
 *
 * So the assertion is about the WORLD instead: after `close()`, is a closed adapter COLLECTABLE?
 * That is the thing the fix actually promises, it is true of any correct implementation, and no
 * value the mutation touches is read here.
 *
 * `--expose-gc` without changing the `test:pg` script: the flag can be set at runtime and the
 * function fetched out of a fresh context.
 */
v8.setFlagsFromString('--expose-gc');
const gc = vm.runInNewContext('gc') as () => void;

/**
 * Allocation lives in its OWN function, and this is not style — it is the trap that ate the first
 * two attempts at this measurement, mine and the mentor's independently. An async test frame keeps
 * its own locals alive across every `await` in it, so building the adapters in the test body reads
 * 14 of 14 SURVIVING even on a tree with no leak at all: a measurement that looks like a finding
 * and is the frame looking at itself.
 */
async function openAndClose(pool: pg.Pool, n: number): Promise<WeakRef<PostgresStorage>[]> {
  const refs: WeakRef<PostgresStorage>[] = [];
  const live: PostgresStorage[] = [];
  for (let i = 0; i < n; i++) {
    const s = new PostgresStorage({
      url: db.url,
      schema: `gcr_${i}`,
      controlSchema: 'gcr_ctl',
      pool,
    });
    live.push(s);
    refs.push(new WeakRef(s));
  }
  await Promise.all(live.map((s) => s.head()));
  await Promise.all(live.map((s) => s.close()));
  live.length = 0;
  return refs;
}

test('a closed adapter is collectable — the pool keeps nothing of it', async () => {
  const pool = sharedPool(4);
  const refs = await openAndClose(pool, 14);
  await new Promise((r) => setTimeout(r, 50));
  gc();
  gc();

  const alive = refs.map((r, i) => (r.deref() ? i : -1)).filter((i) => i >= 0);
  // The index matters as much as the count. With the handler built inline in `forgetPidsOnRemove`,
  // exactly ONE survives and it is always index 0 — the first adapter to touch the pool, pinned by
  // the closure context that handler shares with the unhook. Under Gate D that is the first tenant
  // on a gateway, alive for as long as the gateway is.
  assert.deepEqual(alive, [], `every closed adapter must be collectable; these were not: ${alive}`);
});

test('sharedPoolSize keeps the floor and the headroom as two different terms', () => {
  // §5.21: `N` is a structural floor (N serial chains, one connection each, for a whole
  // transaction); `reads` is queueing headroom (single statements, no transaction). Collapsing them
  // into one constant either wastes connections or hides the floor.
  assert.equal(sharedPoolSize(14), 19, '14 tenants: 14 + 4 reads + 1');
  assert.equal(sharedPoolSize(14, 0), 15, 'the floor alone is still N + 1, never 11');
  assert.ok(sharedPoolSize(14) < 14 * 11, 'and far below the 154 a pool-per-adapter would take');
  // The floor scales with tenants; the headroom does not.
  assert.equal(sharedPoolSize(28) - sharedPoolSize(14), 14);
});
