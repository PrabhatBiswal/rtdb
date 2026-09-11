import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { startGateway } from '../../src/gateway/server.ts';
import { RtdbClient } from '../../harness/client.ts';
import { waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.23 Gate A — the quota UNIT, metered. No enforcement: nothing here may be refused.
 *
 * The counter has to equal what STORAGE actually saw, not what the gateway believes it sent, so
 * every tenant's store is wrapped and counts its own `commitGroup`/`commitCas` calls. That is the
 * two-sided shape §5.22 Gate B's ruling asks for: one side is the metric we write, the other is the
 * transactions the database really started, and a test comparing our number to our own number would
 * pass with the meter unplugged.
 */
const { register } = await import('prom-client');

const token = (sub: string, ns: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, ns });

/** Wraps a store so the test can count the lock acquisitions the WORLD saw. */
function counting(store: MemoryStorage): { store: MemoryStorage; acquisitions: () => number } {
  let n = 0;
  const group = store.commitGroup.bind(store);
  const cas = store.commitCas.bind(store);
  Object.assign(store, {
    commitGroup: (w: Parameters<typeof group>[0]) => { n++; return group(w); },
    commitCas: (w: Parameters<typeof cas>[0]) => { n++; return cas(w); },
  });
  return { store, acquisitions: () => n };
}

const sample = async (metric: string, db: string): Promise<number | null> => {
  const m = new RegExp(`^${metric}\\{db="${db}"\\} (\\S+)$`, 'm').exec(await register.metrics());
  return m ? Number(m[1]) : null;
};

after(() => M.resetSources());

test('every lock acquisition is counted, per database, and nothing is refused', async (t) => {
  M.resetSources();
  const car = counting(new MemoryStorage(DEFAULT_LIMITS));
  const chat = counting(new MemoryStorage(DEFAULT_LIMITS));
  const stores = new Map([['car', car.store], ['chat', chat.store]]);
  const base = new MemoryStorage(DEFAULT_LIMITS);
  for (const db of stores.keys()) await base.declareDatabase(db, 'console-rw-owner');

  const gw = await startGateway({
    storage: base,
    requireNs: true,
    db: 'public',
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
  });
  t.after(() => gw.close());

  // The DEFAULT tenant opens eagerly with the gateway, so its rejection series exists before a
  // single client has connected — registered on SERVE, not on the first refusal. A series that
  // appears only once it is non-zero reads as "no data" exactly when someone needs to know that
  // nothing was refused, and cannot be alerted on before the incident it would announce.
  assert.equal(await sample('rtdb_quota_rejected_total', 'public'), 0, 'registered, and zero');
  assert.equal(await sample('rtdb_quota_rejected_total', 'car'), null, 'a tenant nobody opened is not served');

  // Counters are process-global and other tests in this run have already moved them.
  const before = {
    car: (await sample('rtdb_lock_acquisitions_total', 'car')) ?? 0,
    chat: (await sample('rtdb_lock_acquisitions_total', 'chat')) ?? 0,
  };

  const connect = async (db: string): Promise<RtdbClient> => {
    const c = new RtdbClient({ url: wsUrl(gw.port), token: token(`app-${db}`, db), pingIntervalMs: 60_000 });
    t.after(() => c.close());
    c.connect();
    await c.ready();
    return c;
  };
  const carC = await connect('car');
  const chatC = await connect('chat');
  // Opened now, so registered now — the lazy tenant map is what decides when a database is served.
  assert.equal(await sample('rtdb_quota_rejected_total', 'car'), 0);
  assert.equal(await sample('rtdb_quota_rejected_total', 'chat'), 0);

  // A BURST on car: submitted together, so §4's group commit makes many writes one acquisition.
  await Promise.all(Array.from({ length: 40 }, (_, i) => carC.put(`car/burst/${i}`, i)));
  // A TRICKLE on chat: awaited one at a time, so each is its own acquisition — the 1.00-vs-N spread
  // the unit exists to price, on one gateway, at the same moment.
  for (let i = 0; i < 6; i++) await chatC.put(`chat/slow/${i}`, i);
  // And a CAS, which §4 commits SOLO: one acquisition for one write.
  const snap = await chatC.put('chat/cas', 0);
  assert.equal(snap.type, 'ack');
  await chatC.cas('chat/cas', (snap as { rev: number }).rev, 1);

  await waitUntil(
    async () => ((await sample('rtdb_lock_acquisitions_total', 'chat')) ?? 0) - before.chat === chat.acquisitions(),
    'the counter caught up with the transactions chat really started',
  );

  for (const [db, arm] of [['car', car], ['chat', chat]] as const) {
    const counted = ((await sample('rtdb_lock_acquisitions_total', db)) ?? 0) - before[db];
    assert.ok(arm.acquisitions() > 0, `${db} really committed something`);
    assert.equal(counted, arm.acquisitions(), `${db}: counter must equal the transactions storage saw`);
  }

  // The whole point of the unit, visible in one run: car did far more WRITES for far fewer
  // acquisitions than chat. A writes/second quota would price these two backwards.
  assert.ok(
    car.acquisitions() < chat.acquisitions(),
    `40 batched writes cost ${car.acquisitions()} acquisitions, 8 serial ones cost ${chat.acquisitions()}`,
  );

  // Gate A meters and does not enforce: nothing was refused, on either database.
  assert.equal(await sample('rtdb_quota_rejected_total', 'car'), 0, 'no enforcement in Gate A');
  assert.equal(await sample('rtdb_quota_rejected_total', 'chat'), 0);
});
