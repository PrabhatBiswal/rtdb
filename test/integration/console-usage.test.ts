import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test, { after } from 'node:test';
import { fileURLToPath } from 'node:url';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { startGateway } from '../../src/gateway/server.ts';
import { RtdbClient } from '../../harness/client.ts';
import { waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.24 Gate B — the usage tile, proved on the page's own code against a real gateway.
 *
 * The four lines are `/usage`'s answer, and `/usage` is five PromQL queries over the meters Gate A
 * added. Prometheus is not in this repo, so the test does what Prometheus does: reads the gateway's
 * own `/metrics`, filters by `db` exactly as the queries do, and hands the result to the PAGE'S OWN
 * `usageLines` — extracted verbatim from the shipped HTML, so the thing under test is what ships.
 *
 * What that arrangement can prove is the whole claim: that a database's numbers are ITS OWN. The
 * tooth is dropping the `db` filter, which is the one mistake that would make every tile on every
 * client's console show the same shard-wide number and look completely plausible doing it.
 */
const HTML = fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url));

function pageBlock<T>(id: string): T {
  const block = new RegExp(`<script id="${id}">([\\s\\S]*?)</script>`).exec(readFileSync(HTML, 'utf8'));
  assert.ok(block, `the console must keep this logic in a <script id="${id}"> block`);
  const mod = { exports: {} as T };
  new Function('module', block[1] as string)(mod);
  return mod.exports;
}

interface Lines {
  connections: string; storage: string; downloads: string;
  load: string; loadWarn: boolean; rejecting: boolean; shardLoad: string;
}
const wire = pageBlock<{
  usageLines: (d: unknown) => Lines;
  fmtBytes: (n: number) => string;
}>('rtdb-wire');

const { register } = await import('prom-client');

/** What `/usage`'s five queries do, against the numbers a scrape would have seen. */
async function usageFor(db: string | null): Promise<Record<string, { value: number }[]>> {
  const body = await register.metrics();
  /** `db=null` drops the filter — the TOOTH, and otherwise every series for every database. */
  const sum = (metric: string): { value: number }[] => {
    const re = new RegExp(`^${metric}\\{([^}]*)\\} (\\S+)$`, 'gm');
    let total = 0;
    let found = false;
    for (const m of body.matchAll(re)) {
      if (db !== null && !(m[1] as string).includes(`db="${db}"`)) continue;
      total += Number(m[2]);
      found = true;
    }
    return found ? [{ value: total }] : [];
  };
  const hold = Number(/^rtdb_lock_hold_ms (\S+)$/m.exec(body)?.[1] ?? 0);
  const acq = sum('rtdb_lock_acquisitions_total');
  return {
    connections: sum('rtdb_connections'),
    storageBytes: sum('rtdb_storage_bytes'),
    downloadsPerSec: sum('rtdb_wire_bytes_out_total'),
    // rho = acquisitions x hold. A rate over a test's seconds is noise, so this uses the totals —
    // the SHAPE of the arithmetic is what the tile does, and it is the shape that can be wrong.
    load: acq.length ? [{ value: (acq[0] as { value: number }).value * hold / 1000 }] : [],
    quotaRejectedPerSec: sum('rtdb_quota_rejected_total'),
    shardLoad: acq.length ? [{ value: (acq[0] as { value: number }).value * hold / 1000 }] : [],
  };
}

after(() => M.resetSources());

test('one database\'s tile shows ITS numbers, and the quiet one shows every line at zero', async (t) => {
  M.resetSources();
  const stores = new Map([
    ['busy', new MemoryStorage(DEFAULT_LIMITS, undefined, 'busy')],
    ['quiet', new MemoryStorage(DEFAULT_LIMITS, undefined, 'quiet')],
  ]);
  const base = new MemoryStorage(DEFAULT_LIMITS, undefined, 'public');
  for (const db of stores.keys()) await base.declareDatabase(db, 'console-rw-owner');
  const gw = await startGateway({
    storage: base,
    requireNs: true,
    db: 'public',
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
  });
  t.after(() => gw.close());
  /**
   * The Storage gauge is bound to ONE adapter, and under Postgres — the only multi-tenant
   * deployment there is, since `main.ts` refuses `RTDB_MULTI_TENANT` without it — that one adapter
   * sees every tenant, because a tenant is a schema in its database and the query is a `GROUP BY`
   * over the catalogue (asserted in `test/pg/tenancy.pgtest.ts`: three schemas, one query).
   *
   * `MemoryStorage` cannot do that: N tenants are N independent heaps and each knows only itself.
   * So this rig merges them, which is what the Postgres adapter does for free — the composition is
   * the test's scaffolding for a fact production gets from the database, and it is written out
   * rather than hidden so nobody reads this file as evidence that memory multi-tenancy sizes itself.
   */
  const everyStore = {
    listDeclared: () => base.listDeclared(),
    storageBytes: async (): Promise<Record<string, number>> => {
      const all = await Promise.all([base, ...stores.values()].map((s) => s.storageBytes()));
      return Object.assign({}, ...all) as Record<string, number>;
    },
  } as unknown as Parameters<typeof M.bindStorageBytes>[0];
  const unbind = M.bindStorageBytes(everyStore, 0);
  t.after(() => unbind());

  const connect = async (db: string): Promise<RtdbClient> => {
    const c = new RtdbClient({
      url: wsUrl(gw.port),
      token: signDevToken({ sub: `app-${db}`, exp: Math.floor(Date.now() / 1000) + 3600, ns: db }),
      pingIntervalMs: 600_000,
    });
    t.after(() => c.close());
    c.connect();
    await c.ready();
    return c;
  };

  // ONE database is used: a connection, a listener, and a hundred writes.
  const busy = await connect('busy');
  await new Promise<void>((r) => busy.listen('busy/room', () => r()));
  for (let i = 0; i < 100; i++) await busy.put(`busy/room/m${i}`, { text: `message ${i}`, at: Date.now() });
  await waitUntil(() => (busy.value('busy/room') as Record<string, unknown> | null) !== null, 'the writes landed');

  /**
   * The other database is DECLARED and NEVER TOUCHED — no connection, no data — which is exactly
   * §5.19's case: an owner hands a database to a team before anything is in it. Every line must be
   * present and every line must read zero.
   *
   * It is not merely "idle", and the difference is a measurement: a database with one idle
   * connection is NOT at zero downloads, because `helloAck` is bytes on the socket and Gate A
   * counts it (93 B for one connect, which is what a first draft of this test asserted away). The
   * tile telling a client "0 B/s" while their app is connected would be the panel lying quietly.
   */

  const busyLines = wire.usageLines(await usageFor('busy'));
  const quietLines = wire.usageLines(await usageFor('quiet'));

  // ---- the used one reports what it did.
  assert.equal(busyLines.connections, '1');
  assert.notEqual(busyLines.storage, '0 B', `100 messages have a size (${busyLines.storage})`);
  assert.notEqual(busyLines.downloads, '0 B/s', `a snapshot and 100 acks are bytes (${busyLines.downloads})`);
  assert.notEqual(busyLines.load, '0.0%', `100 writes take the lock (${busyLines.load})`);

  // ---- the quiet one reports zeros, and every line is PRESENT.
  assert.equal(quietLines.connections, '0', 'nobody has connected to it');
  assert.equal(quietLines.storage, '0 B', 'declared, empty, and present at zero — not absent');
  assert.equal(quietLines.downloads, '0 B/s');
  assert.equal(quietLines.load, '0.0%');
  assert.equal(quietLines.loadWarn, false);
  for (const [k, v] of Object.entries(quietLines)) {
    if (typeof v === 'string') assert.notEqual(v, '', `the ${k} line exists rather than being blank`);
  }

  // ---- and the two are not the same number, which is the tooth's target.
  assert.notEqual(busyLines.storage, quietLines.storage);
  assert.notEqual(busyLines.load, quietLines.load);

  // ---- a CONNECTION alone is not free, and the tile must not round it away: one more client on
  // the quiet database, no writes, no listen — and its downloads stop being zero, because
  // `helloAck` is bytes (§5.24 Gate A counts every frame, not just a subscription's).
  await connect('quiet');
  const nowConnected = wire.usageLines(await usageFor('quiet'));
  assert.equal(nowConnected.connections, '1');
  assert.notEqual(nowConnected.downloads, '0 B/s', `one helloAck is bytes (${nowConnected.downloads})`);
  assert.equal(nowConnected.storage, '0 B', 'and it still stored nothing');

  // ---- TOOTH, run in-line: drop the `db` filter and both tiles read the same shard-wide number,
  // which is exactly what a per-database panel must never do — and would look entirely plausible.
  const unfiltered = wire.usageLines(await usageFor(null));
  assert.notEqual(unfiltered.storage, quietLines.storage, 'unfiltered is not the quiet database');
  assert.notEqual(unfiltered.load, quietLines.load, 'unfiltered is not the quiet database');
  assert.ok(
    Number(unfiltered.connections) > Number(busyLines.connections),
    `unfiltered counts every connection (${unfiltered.connections} vs ${busyLines.connections})`,
  );
});

test('Load warns at 0.8, and a database at zero still reads as a number', () => {
  // The warn line is §5.23's: a database approaching 100% of the shard's lock is one about to start
  // seeing RATE. Asserted on the page's own function, because the threshold is the claim.
  const at = (load: number): Lines => wire.usageLines({ load: [{ value: load }] });
  assert.equal(at(0.79).loadWarn, false);
  assert.equal(at(0.8).loadWarn, true, 'the line is AT 0.8, not past it');
  assert.equal(at(1.2).loadWarn, true, 'and over 100% is still a warning, not an error');
  assert.equal(at(0.85).load, '85%');
  assert.equal(at(0.004).load, '0.4%', 'a small number keeps a digit rather than rounding to 0%');
  assert.equal(wire.usageLines({}).storage, '0 B', 'an answer with nothing in it reads as zero');
});
