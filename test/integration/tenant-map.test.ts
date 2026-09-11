import assert from 'node:assert/strict';
import test, { after, type TestContext } from 'node:test';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import type { StorageAdapter } from '../../src/storage/adapter.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { startGateway, type Gateway, type GatewayOptions } from '../../src/gateway/server.ts';
import { RtdbClient } from '../../harness/client.ts';
import { waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.22 Gate D — one gateway, many databases.
 *
 * Every test here needs at least TWO tenants, because every claim is about isolation and a
 * single-tenant gateway satisfies all of them trivially. The single-tenant path is not re-tested
 * here: it is what the other 8 `startGateway` callers already assert, and this file would only
 * repeat them.
 */
const token = (sub: string, ns?: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, ...(ns ? { ns } : {}) });

interface Rig {
  gw: Gateway;
  /** The DEFAULT tenant's store, which is also the shard's registry — `tenantFor` reads it. */
  base: MemoryStorage;
  stores: Map<string, MemoryStorage>;
  built: string[];
  connect: (sub: string, ns?: string) => Promise<RtdbClient>;
}

const rigs: Rig[] = [];
after(() => M.resetSources());

async function rig(
  t: TestContext,
  opts: Partial<GatewayOptions> = {},
  declared: string[] = ['car', 'chat'],
): Promise<Rig> {
  const stores = new Map<string, MemoryStorage>();
  const built: string[] = [];
  // §5.22 Gate F's precondition: a name that is not in the registry never reaches the factory, so
  // every database these tests connect to has to be declared first — which is what an owner does
  // through the console before handing a database to anyone.
  const base = new MemoryStorage(DEFAULT_LIMITS);
  for (const db of declared) await base.declareDatabase(db, 'console-rw-owner');
  const gw = await startGateway({
    storage: base,
    requireNs: true,
    tenantStorage: (db) => {
      built.push(db);
      const s = new MemoryStorage(DEFAULT_LIMITS);
      stores.set(db, s);
      return s;
    },
    ...opts,
  });
  const r: Rig = {
    gw,
    base,
    stores,
    built,
    connect: async (sub, ns) => {
      const c = new RtdbClient({ url: wsUrl(gw.port), token: token(sub, ns), pingIntervalMs: 60_000 });
      t.after(() => c.close());
      c.connect();
      await c.ready();
      return c;
    },
  };
  rigs.push(r);
  t.after(() => gw.close());
  return r;
}

// ------------------------------------------------------------------------------------ (a)

test('two databases in one gateway keep separate revs and see nothing of each other', async (t) => {
  const { gw, stores, built, connect } = await rig(t);
  const car = await connect('app-car-web', 'car');
  const chat = await connect('app-chat-web', 'chat');

  await car.put('car/players/p1', { score: 1 });
  await car.put('car/players/p2', { score: 2 });
  await chat.put('chat/rooms/r1', 'hi');

  // Separate rev counters. A shared store would make these 3 and 3.
  assert.equal(await stores.get('car')?.head(), 2);
  assert.equal(await stores.get('chat')?.head(), 1);
  assert.equal((await stores.get('chat')?.readSnapshot('car/players/p1'))?.value, null);

  // The default tenant is never built by a connection that names a database, and `gw.storage` is
  // still its store — which is what 34 assertions elsewhere in the suite read.
  assert.deepEqual(built.sort(), ['car', 'chat'], 'only the databases that were connected to');
  assert.equal(await gw.storage.head(), 0, 'the default tenant stayed empty');
});

// ------------------------------------------------------------------------------------ (b)

test("one database's DELTAS reach only its own sockets", async (t) => {
  // Named for what it measures. It used to be called history loss, and it was not: `onHistoryLost`
  // only fires from the BUS (`redis.ts` `#fillFromOplog`) and this rig has no bus, so nothing here
  // could ever have called it — sharing `live` across tenants left this test GREEN. The two
  // properties that DO need `live` to be per-tenant, history loss and §10's kick, are witnessed
  // against a real Redis in `test/bus/tenant-shard.bustest.ts`.
  //
  // What is left is still worth its own test: a commit in one tenant's store must not be routed
  // into another tenant's subscriptions.
  const { stores, connect } = await rig(t);
  const car = await connect('app-car-web', 'car');
  const chat = await connect('app-chat-web', 'chat');

  const carValues: unknown[] = [];
  const chatValues: unknown[] = [];
  // A real wait, not `|| true`: the callback fires when the SNAPSHOT lands, including a null one.
  // `value() !== undefined` could not have been one — the mirror answers an unknown path with
  // `null`, so that condition was true before the first frame arrived.
  await Promise.all([
    new Promise<void>((r) => car.listen('car/players', (v) => (carValues.push(v), r()))),
    new Promise<void>((r) => chat.listen('chat/rooms', (v) => (chatValues.push(v), r()))),
  ]);
  const chatSeen = chatValues.length;

  await stores.get('car')?.commitGroup([
    { writeId: crypto.randomUUID(), path: 'car/players/p9', op: 'put', value: 1 },
  ]);
  await waitUntil(() => carValues.length > 1, 'car saw its own write');
  assert.equal(chatValues.length, chatSeen, "chat's subscription saw nothing of car's traffic");
});

// ------------------------------------------------------------------------------------ (d)

test('a tenant whose first open FAILS can be opened again', async (t) => {
  // R2: `opening` held the promise and deleted it only on success, so one transient — the factory
  // throwing, a Redis blip inside `bus.start()` — poisoned that database until the process
  // restarted. A transient made permanent, and invisible: every later connect just failed.
  let attempts = 0;
  const stores = new Map<string, MemoryStorage>();
  const base = new MemoryStorage(DEFAULT_LIMITS);
  await base.declareDatabase('car', 'console-rw-owner');
  const gw = await startGateway({
    storage: base,
    requireNs: true,
    tenantStorage: (db) => {
      attempts++;
      if (attempts === 1) throw new Error('storage unavailable, once');
      const s = new MemoryStorage(DEFAULT_LIMITS);
      stores.set(db, s);
      return s;
    },
  });
  t.after(() => gw.close());

  const c = new RtdbClient({
    url: wsUrl(gw.port),
    token: token('app-car-web', 'car'),
    pingIntervalMs: 60_000,
  });
  t.after(() => c.close());
  c.connect();

  // The client's own §6 backoff retries the failed connect — which is the RIGHT behaviour for a
  // transient, and it is what makes the bug's shape so quiet: with the rejected promise stuck in
  // `opening`, every reconnect is handed the SAME stale failure and `ready()` never resolves. So
  // the assertion is that the connection eventually comes up at all.
  await c.ready();
  assert.equal((await c.put('car/x', 1)).type, 'ack');
  assert.ok(attempts >= 2, `the factory was retried, not remembered as broken (attempts: ${attempts})`);
  assert.ok(stores.has('car'), 'and the second attempt really built the store');
});

// ------------------------------------------------------------------------------------ (e)

test('a frame arriving while the tenant is still opening never reaches a null sink', async (t) => {
  // TypeScript caught this window; no test could have, because every client in the suite waits for
  // helloAck before sending. Here a RAW socket sends a write immediately after hello, with a
  // factory that takes a real tick to build its store — so the frame lands while `tenantFor` is
  // still in flight and the sink does not exist.
  //
  // What is asserted is not which answer the early write gets. It is that hello still completes and
  // the gateway is still serving afterwards: before `userId`/`tenant`/`sink` were set together,
  // this frame read as authenticated and reached into null.
  const base = new MemoryStorage(DEFAULT_LIMITS);
  await base.declareDatabase('car', 'console-rw-owner');
  const gw = await startGateway({
    storage: base,
    requireNs: true,
    tenantStorage: () => new MemoryStorage(DEFAULT_LIMITS),
  });
  t.after(() => gw.close());

  const ws = new WebSocket(wsUrl(gw.port));
  await new Promise((r) => ws.addEventListener('open', r, { once: true }));
  const frames: Record<string, unknown>[] = [];
  ws.addEventListener('message', (ev) =>
    frames.push(JSON.parse(String(ev.data)) as Record<string, unknown>),
  );

  ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token('app-car-web', 'car') }));
  // Deliberately NOT awaiting helloAck — this races the tenant open, which is the whole point.
  ws.send(JSON.stringify({ type: 'put', writeId: crypto.randomUUID(), path: 'car/x', value: 1 }));

  await waitUntil(() => frames.some((f) => f['type'] === 'helloAck'), 'hello still completes');
  ws.close();

  // Still serving: a later client connects and writes normally on the same gateway.
  const after = new RtdbClient({
    url: wsUrl(gw.port),
    token: token('app-car-web', 'car'),
    pingIntervalMs: 60_000,
  });
  t.after(() => after.close());
  after.connect();
  await after.ready();
  assert.equal((await after.put('car/later', 1)).type, 'ack', 'the gateway survived the race');
});

// ------------------------------------------------------------------------------------ (f)

test("each database reports its own metrics, and they go when the gateway does", async (t) => {
  // Gate E's registry at Gate D's altitude: the labels now come from a real gateway rather than a
  // hand-made binding, and `close()` must take them with it.
  M.resetSources();
  const { connect, gw } = await rig(t);
  await connect('app-car-web', 'car');
  await connect('app-chat-web', 'chat');

  const scrape = async (): Promise<string> => {
    const { register } = await import('prom-client');
    return register.metrics();
  };
  const sample = (body: string, db: string): number | null => {
    const m = new RegExp(`^rtdb_connections\\{db="${db}"\\} (\\S+)$`, 'm').exec(body);
    return m ? Number(m[1]) : null;
  };

  const before = await scrape();
  assert.equal(sample(before, 'car'), 1);
  assert.equal(sample(before, 'chat'), 1);

  gw.close();
  const after = await scrape();
  assert.equal(sample(after, 'car'), null, 'a closed gateway leaves no labels behind');
  assert.equal(sample(after, 'chat'), null);
});

// (g) — the default tenant's bus keys — is NOT here. It needs a real Redis to observe the keys the
// gateway actually creates, and a version of it asserting `busKeys()` directly would be vacuous:
// the mutation is in `busShard`, which such a test never reads. It lives in
// `test/bus/tenant-shard.bustest.ts`. (Confirmed by breaking it: the `busKeys` form stayed green
// with `busShard` reverted.)

// ------------------------------------------------------------------------------------ (j)

test('a token naming an UNDECLARED database is refused, and no store is built for it', async (t) => {
  // §5.22 Gate F's precondition, and the hole was Gate D's own. Opening a tenant calls the factory,
  // and under Postgres the factory's `#init` is a `CREATE SCHEMA IF NOT EXISTS` — so before this,
  // any token signed with the shard secret could name a database nobody declared and MAKE it, at
  // hello, with no error anywhere. §5.19's "only an owner creates a database" was enforced at
  // `/app-token`'s mint and nowhere on the gateway.
  //
  // `built` is the witness at this altitude: the factory is the only thing that can create storage,
  // so a factory that was never called created nothing. The Postgres half — that `pg_namespace`
  // really has no such schema afterwards — is in `test/pg/tenancy.pgtest.ts`.
  //
  // On the WIRE, for the reason (h)(i) gives: §6 makes 4401 terminal, so a client-level assertion
  // would hang rather than fail.
  const { gw, built, connect } = await rig(t);
  const ws = new WebSocket(wsUrl(gw.port));
  await new Promise((r) => ws.addEventListener('open', r, { once: true }));
  const frames: Record<string, unknown>[] = [];
  ws.addEventListener('message', (ev) =>
    frames.push(JSON.parse(String(ev.data)) as Record<string, unknown>),
  );
  let closeCode: number | null = null;
  ws.addEventListener('close', (ev) => (closeCode = (ev as { code: number }).code), { once: true });

  ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token('app-x', 'nosuchdb') }));
  // `waitUntil` and not a close promise: with the refusal removed this hello SUCCEEDS, and awaiting
  // a close that never comes is a 20 s file timeout instead of a failed assertion.
  await waitUntil(() => closeCode !== null, 'the undeclared database was refused');
  assert.equal(closeCode, 4401, 'AUTH, and terminal — retrying will not make the database exist');
  const err = frames.find((f) => f['type'] === 'err');
  assert.equal(err?.['code'], 'AUTH');
  assert.match(String(err?.['msg']), /unknown database nosuchdb/);
  assert.deepEqual(built, [], 'and the factory was never called, so nothing was created');

  // The other arm, so this is not just "the gateway refuses everything": a DECLARED name still
  // connects — on THIS gateway, the one that just refused, not a second one built to succeed.
  const car = await connect('app-car-web', 'car');
  assert.equal((await car.put('car/x', 1)).type, 'ack');
  assert.deepEqual(built, ['car'], 'and the factory ran for the declared name only');
});

test('a database declared AFTER boot connects without a restart', async (t) => {
  // The mentor's tooth at Gate D, made a test. `tenantFor` asks the registry on every OPEN rather
  // than reading a snapshot at boot, and the reason is this exact sequence: an owner declares a
  // database in the console and hands it to their team a minute later. A boot-time list would
  // refuse them until someone restarted the gateway — and it would look exactly like a bad token.
  //
  // The claim was in a comment and nothing tested it. It is cheap because `tenants` already caches
  // an open tenant, so "on every open" is one registry read per database per process, not per hello.
  const { gw, base, built, connect } = await rig(t, {}, ['car']);

  const helloFor = async (ns: string): Promise<number> => {
    const ws = new WebSocket(wsUrl(gw.port));
    await new Promise((r) => ws.addEventListener('open', r, { once: true }));
    let code: number | null = null;
    let acked = false;
    ws.addEventListener('close', (ev) => (code = (ev as { code: number }).code), { once: true });
    ws.addEventListener('message', (ev) => {
      if ((JSON.parse(String(ev.data)) as { type?: string }).type === 'helloAck') acked = true;
    });
    ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token('app-x', ns) }));
    await waitUntil(() => code !== null || acked, `hello for ${ns} was answered`);
    ws.close();
    return code ?? 0;
  };

  assert.equal(await helloFor('chat'), 4401, 'undeclared at boot');
  assert.deepEqual(built, [], 'and no store was built for it');

  // Declared now, on the running gateway. No restart, no reconnect of anything else.
  await base.declareDatabase('chat', 'console-rw-owner');

  assert.equal(await helloFor('chat'), 0, 'the same gateway now serves it');
  assert.deepEqual(built, ['chat'], 'and the factory ran exactly once, for it');
  const chat = await connect('app-chat-web', 'chat');
  assert.equal((await chat.put('chat/x', 1)).type, 'ack');
  assert.deepEqual(built, ['chat'], 'a second connection reuses the open tenant');
});

// ------------------------------------------------------------------------------------ (h)(i)

test('a token naming a reserved database is refused at the door', async (t) => {
  // R4: `_default` and `_other` are synthetic metrics labels, and `validateSegment` accepted them —
  // so a token could name one and be handed to a tenant factory as if it were a database. The
  // refusal belongs at the token, which is why `auth.ts` now uses the same `validateDatabaseName`
  // the registry and the admin route use: past that door, `tenantFor`'s input is already trusted.
  //
  // Asserted on the WIRE rather than through the client, because §6 makes 4401 terminal — the
  // client stops and never settles `ready()`, so a client-level assertion would hang rather than
  // fail. The err frame is also the more precise claim: it says WHICH check refused it.
  const { gw, built } = await rig(t);
  for (const bad of ['_default', '_other']) {
    const ws = new WebSocket(wsUrl(gw.port));
    await new Promise((r) => ws.addEventListener('open', r, { once: true }));
    const first = new Promise<Record<string, unknown>>((r) =>
      ws.addEventListener('message', (ev) => r(JSON.parse(String(ev.data)) as Record<string, unknown>), {
        once: true,
      }),
    );
    ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token('app-x', bad) }));
    const frame = await first;
    assert.equal(frame['type'], 'err', bad);
    assert.equal(frame['code'], 'AUTH');
    assert.match(String(frame['msg']), /malformed token ns/);
    ws.close();
  }
  assert.deepEqual(built, [], 'and no factory was ever called for a reserved name');
});

test('head and tenantStorage together are refused, not guessed', async () => {
  // R5: `head` overrides ONE tenant's rev and what it means for N has no caller to derive from.
  await assert.rejects(
    () => startGateway({ head: () => 0, tenantStorage: () => new MemoryStorage(DEFAULT_LIMITS) }),
    /mutually exclusive/,
  );
});
