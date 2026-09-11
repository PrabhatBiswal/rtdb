import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { DEFAULT_LIMITS, makeLimits, type Limits } from '../../src/protocol/limits.ts';
import { AcquisitionQuota } from '../../src/pipeline/write.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { startGateway, type Gateway, type GatewayOptions } from '../../src/gateway/server.ts';
import { RtdbClient } from '../../harness/client.ts';
import { waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.23 Gate B — the per-DATABASE quota, in lock acquisitions.
 *
 * Every test here needs TWO tenants, because the claim is never "writes get refused" — it is that
 * the RIGHT tenant gets refused, for the right reason, while the other one does not notice. A
 * single-tenant gateway satisfies "something was rate-limited" trivially and says nothing.
 */
const { register } = await import('prom-client');

const token = (db: string): string =>
  signDevToken({ sub: `app-${db}`, exp: Math.floor(Date.now() / 1000) + 3600, ns: db });

const rejected = async (db: string): Promise<number> => {
  const m = new RegExp(`^rtdb_quota_rejected_total\\{db="${db}"\\} (\\S+)$`, 'm').exec(await register.metrics());
  return m ? Number(m[1]) : 0;
};

interface Rig {
  gw: Gateway;
  stores: Map<string, MemoryStorage>;
  connect: (db: string) => Promise<RtdbClient>;
}

after(() => M.resetSources());

async function rig(t: { after: (fn: () => unknown) => void }, limits: Limits, opts: Partial<GatewayOptions> = {}): Promise<Rig> {
  const stores = new Map([['slow', new MemoryStorage(limits)], ['fast', new MemoryStorage(limits)]]);
  const base = new MemoryStorage(limits);
  for (const db of stores.keys()) await base.declareDatabase(db, 'console-rw-owner');
  const gw = await startGateway({
    storage: base,
    limits,
    requireNs: true,
    db: 'public',
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
    ...opts,
  });
  t.after(() => gw.close());
  return {
    gw,
    stores,
    connect: async (db) => {
      const c = new RtdbClient({ url: wsUrl(gw.port), token: token(db), pingIntervalMs: 60_000 });
      t.after(() => c.close());
      c.connect();
      await c.ready();
      return c;
    },
  };
}

/**
 * A write's OUTCOME, refusal included. The client rejects an `err` with the frame itself (§4: an
 * err-rejected write surfaces and is never auto-retried), so a test that only awaits the happy path
 * would blow up on the very thing it is here to observe.
 */
interface Outcome { ok: boolean; msg: string | null }
const write = (c: RtdbClient, path: string, v: number): Promise<Outcome> =>
  c.put(path, v).then(
    () => ({ ok: true, msg: null }),
    (e: { msg?: string }) => ({ ok: false, msg: e.msg ?? null }),
  );

/** A tight quota, so a test does not have to spend a real second to cross it. */
const tight = (perSec: number, burst: number): Limits =>
  makeLimits({ QUOTA_ACQ_PER_SEC: perSec, QUOTA_ACQ_BURST: burst, WRITE_RATE_PER_SEC: 10_000, WRITE_RATE_BURST: 10_000 });

// ------------------------------------------------------------------------------------------ (i)

test('20x the writes for a fifth of the lock: the trickling tenant is refused, the bursty one is not', async (t) => {
  // The whole argument for the unit, on one gateway at one moment. `slow` awaits every write, so
  // each is its own acquisition and it spends its share one write at a time. `fast` submits without
  // awaiting, so §4's group commit turns hundreds of writes into a handful of acquisitions.
  //
  // A writes/second quota would have refused these two in exactly the opposite order.
  M.resetSources();
  const limits = tight(8, 8);
  const { connect } = await rig(t, limits);
  const slow = await connect('slow');
  const fast = await connect('fast');

  const slowResults: Outcome[] = [];
  for (let i = 0; i < 24; i++) slowResults.push(await write(slow, `slow/x/${i}`, i));

  // 480 writes on the other database, submitted together so §4 batches them, while the first
  // database was being refused for a twentieth of the writes.
  const fastResults = await Promise.all(
    Array.from({ length: 480 }, (_, i) => write(fast, `fast/y/${i}`, i)),
  );

  const refusals = slowResults.filter((r) => !r.ok);
  assert.ok(refusals.length > 0, `the trickling tenant was refused (${slowResults.filter((r) => r.ok).length}/24 acked)`);
  assert.deepEqual([...new Set(refusals.map((r) => r.msg))], ['database quota exceeded'], 'and told which limit');
  assert.equal(
    fastResults.filter((r) => !r.ok).length,
    0,
    `20x the writes, and not one refusal on the bursty database`,
  );
});

// ----------------------------------------------------------------------------------------- (ii)

test('one database in debt leaves the other untouched, and only its own counter moves', async (t) => {
  M.resetSources();
  const limits = tight(4, 4);
  const { connect, stores } = await rig(t, limits);
  const slow = await connect('slow');
  const fast = await connect('fast');
  const before = { slow: await rejected('slow'), fast: await rejected('fast') };

  for (let i = 0; i < 20; i++) await write(slow, `slow/x/${i}`, i);
  await waitUntil(async () => (await rejected('slow')) > before.slow, 'the over-quota database was counted');

  // The other database, on the same gateway, at the same moment, is completely unaffected —
  // submitted together, so §4 batches them and the six writes cost it about one acquisition.
  const fastResults = await Promise.all(Array.from({ length: 6 }, (_, i) => write(fast, `fast/y/${i}`, i)));
  assert.equal(fastResults.filter((r) => !r.ok).length, 0, 'the other database wrote normally throughout');
  assert.equal(await rejected('fast'), before.fast, 'and nothing was counted against it');
  assert.equal((await (stores.get('fast') as MemoryStorage).readSnapshot('fast/y/5')).value, 5);
});

// ---------------------------------------------------------------------------------------- (iii)

test('the per-CONNECTION limiter still refuses on its own terms, with its own message', async (t) => {
  // §9's older limit is a different question — one client flooding, not one database eating the
  // shard — and Gate B must not have quietly replaced it. Quota wide open, connection limit tiny.
  M.resetSources();
  const limits = makeLimits({ WRITE_RATE_PER_SEC: 1, WRITE_RATE_BURST: 3, QUOTA_ACQ_PER_SEC: 10_000, QUOTA_ACQ_BURST: 10_000 });
  const { connect } = await rig(t, limits);
  const slow = await connect('slow');
  const before = await rejected('slow');

  const msgs: string[] = [];
  for (let i = 0; i < 10; i++) {
    const r = await write(slow, `slow/x/${i}`, i);
    if (!r.ok) msgs.push(r.msg as string);
  }
  assert.ok(msgs.length > 0, 'the connection limiter still bites');
  assert.deepEqual([...new Set(msgs)], ['write rate exceeded'], 'and it says which limit it was');
  assert.equal(await rejected('slow'), before, 'a per-connection refusal is not a database rejection');
});

// ----------------------------------------------------------------------------------------- (iv)

test('a CAS is ONE acquisition, because §4 commits it solo', async (t) => {
  // The expensive end of the unit: a CAS cannot join a batch, so N of them cost N acquisitions
  // where N puts submitted together cost one. A writes/second quota cannot see that at all.
  M.resetSources();
  const limits = tight(1000, 1000);
  const { connect } = await rig(t, limits);
  const slow = await connect('slow');

  const acquisitions = async (): Promise<number> => {
    const m = /^rtdb_lock_acquisitions_total\{db="slow"\} (\S+)$/m.exec(await register.metrics());
    return m ? Number(m[1]) : 0;
  };

  const seed = await slow.put('slow/n', 0);
  assert.equal(seed.type, 'ack');
  const before = await acquisitions();

  let rev = (seed as { rev: number }).rev;
  for (let i = 0; i < 5; i++) {
    const r = await slow.cas('slow/n', rev, i + 1);
    assert.equal(r.type, 'ack', `cas ${i}`);
    rev = (r as { rev: number }).rev;
  }
  // Five solo commits, five acquisitions — on the gateway's own meter, which `quota-meter.test.ts`
  // already pins to what storage really saw. Five PUTS submitted together would have cost one.
  assert.equal((await acquisitions()) - before, 5, 'a CAS never joins a batch (§4)');

  // And the charge itself, on a frozen clock, so the arithmetic is not measured against a refill.
  const quota = new AcquisitionQuota(1000, 1000, 0);
  for (let i = 0; i < 5; i++) quota.charge(0);
  assert.equal(quota.balance, 1000 - 5);
});

// ------------------------------------------------------------------------------------------ (v)

test('the debt a charge-at-commit bucket runs up is bounded by one group-commit window', async (t) => {
  // The honest shape of a DEBIT, asserted rather than hidden: admission reads a balance that the
  // charge left behind, so writes already in flight when it hits zero still commit and still
  // charge. What must not happen is unbounded overshoot — the pipeline batches, so at most one
  // window's worth can be in flight, and the first refusal must arrive within that window.
  M.resetSources();
  const limits = tight(4, 4);
  const { connect } = await rig(t, limits);
  const slow = await connect('slow');

  const t0 = Date.now();
  let firstRefusalAt: number | null = null;
  for (let i = 0; i < 30; i++) {
    const r = await write(slow, `slow/x/${i}`, i);
    if (!r.ok && firstRefusalAt === null) firstRefusalAt = Date.now();
  }
  assert.ok(firstRefusalAt !== null, 'the bucket really ran out');
  // Four tokens at four per second: the fifth serial write is where it goes. Generous bound —
  // what is being asserted is that the refusal follows the exhaustion, not a whole refill period.
  assert.ok(
    (firstRefusalAt as number) - t0 < 1000,
    `refused ${(firstRefusalAt as number) - t0}ms in, not a refill period later`,
  );
});

// ----------------------------------------------------------------------------------------- (vi)

test('two gateways serve a shard, so each takes half the quota', async (t) => {
  // §5.23 faisla 5. A fixed division, not a shared bucket: Redis would make it exact and would put
  // a round trip on the write path. Asserted through the gateway option rather than on the class
  // alone, because the division is the wiring's job and the class would happily be told anything.
  M.resetSources();
  const limits = tight(2, 8);
  const solo = await rig(t, limits);
  const halved = await rig(t, limits, { gatewayCount: 2 });

  const spend = async (r: Rig): Promise<number> => {
    const c = await r.connect('slow');
    let acked = 0;
    for (let i = 0; i < 8; i++) if ((await write(c, `slow/x/${i}`, i)).ok) acked++;
    return acked;
  };
  const alone = await spend(solo);
  const shared = await spend(halved);
  assert.ok(alone > shared, `one gateway admitted ${alone}, two-gateway share admitted ${shared}`);

  // And the class's own arithmetic, since that is where the halving lands.
  const full = new AcquisitionQuota(8, 8, 0);
  const half = new AcquisitionQuota(8 / 2, 8 / 2, 0);
  for (let i = 0; i < 4; i++) { full.charge(0); half.charge(0); }
  assert.equal(full.allows(0), true, 'four of eight spent');
  assert.equal(half.allows(0), false, 'four of four spent');
});

// ------------------------------------------------------------ §5.24 Gate C: the per-database quota

test('a registry override holds one database to a rate the shard default would allow', async (t) => {
  // The override is read at tenant OPEN and the bucket cannot be resized afterwards — a quota that
  // moved under a running tenant would make the panel's number and the tenant's behaviour disagree
  // silently, which is the failure this whole phase is about.
  //
  // Both databases run on the SAME gateway with the SAME shard default. One has a row saying 4.
  M.resetSources();
  const limits = makeLimits({
    QUOTA_ACQ_PER_SEC: 64, QUOTA_ACQ_BURST: 128,
    WRITE_RATE_PER_SEC: 10_000, WRITE_RATE_BURST: 10_000,
  });
  const stores = new Map([['held', new MemoryStorage(limits)], ['free', new MemoryStorage(limits)]]);
  const base = new MemoryStorage(limits);
  await base.declareDatabase('held', 'console-rw-owner', 4);
  await base.declareDatabase('free', 'console-rw-owner');

  const gw = await startGateway({
    storage: base, limits, requireNs: true, db: 'public',
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
  });
  t.after(() => gw.close());

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
  const write = (c: RtdbClient, path: string, v: number): Promise<{ ok: boolean; msg: string | null }> =>
    c.put(path, v).then(() => ({ ok: true, msg: null }), (e: { msg?: string }) => ({ ok: false, msg: e.msg ?? null }));

  const held = await connect('held');
  const free = await connect('free');

  // 12 serial writes: each is its own acquisition, so this is 12 against a bucket of 8 (2x4) on one
  // database and 12 against 128 on the other.
  const heldResults = [];
  const freeResults = [];
  for (let i = 0; i < 12; i++) heldResults.push(await write(held, `held/x/${i}`, i));
  for (let i = 0; i < 12; i++) freeResults.push(await write(free, `free/x/${i}`, i));

  const refused = heldResults.filter((r) => !r.ok);
  assert.ok(refused.length > 0, `the overridden database was refused (${heldResults.filter((r) => r.ok).length}/12 acked)`);
  assert.deepEqual([...new Set(refused.map((r) => r.msg))], ['database quota exceeded']);
  assert.equal(
    freeResults.filter((r) => !r.ok).length,
    0,
    'and the database on the shard default took the same 12 writes without one refusal',
  );

  // The panel's own number, and WHICH statement it is making — "4 (set)" is a decision about this
  // database; "64 (default)" follows the shard if the shard's number ever changes.
  const body = await (await import('prom-client')).register.metrics();
  assert.match(body, /rtdb_quota_acq_per_sec\{db="held",source="override"\} 4\b/);
  assert.match(body, /rtdb_quota_acq_per_sec\{db="free",source="default"\} 64\b/);
});

test('re-declaring a database never resets a quota somebody set', async () => {
  // `ON CONFLICT DO NOTHING` on both storages, and it has to be both or they would disagree about
  // whether a second `POST /databases` with no quota field quietly undoes an override.
  const s = new MemoryStorage(DEFAULT_LIMITS);
  await s.declareDatabase('car', 'console-rw-owner', 120);
  await s.declareDatabase('car', 'console-rw-someone-else');
  assert.deepEqual(await s.describeDatabase('car'), { quotaAcqPerSec: 120 });
  // And a name nobody declared has no row at all — the DEFAULT tenant's case, which runs on the
  // shard default like any database nobody has decided about.
  assert.equal(await s.describeDatabase('public'), null);
});
