import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { DEFAULT_LIMITS, makeLimits } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import { busKeys, connectRedis } from '../../src/fanout/redis.ts';
import { startGateway } from '../../src/gateway/server.ts';
import { assertNoLeftovers, RedisProcess } from '../../harness/redis.ts';
import { RtdbClient } from '../../harness/client.ts';
import { CLOSE } from '../../src/protocol/frames.ts';
import { waitUntil, wsUrl } from '../helpers.ts';
import * as M from '../../src/gateway/metrics.ts';

after(() => {
  M.resetSources();
  assertNoLeftovers();
});

/**
 * §5.22 Gate D (R3) — the DEFAULT tenant's bus keys must not move when a tenant factory appears.
 *
 * This is a DEPLOY claim, not a naming one. `busKeys(shard)` names the stream, the leader lock, the
 * fence and the epoch, and the bus IS cross-gateway delivery. If the first multi-tenant deploy moved
 * the default tenant from `rtdb:0:*` to `rtdb:0:_default:*`, then during the rolling window the old
 * gateway would publish to one stream while the new one consumed another — every delta between two
 * gateways lost, silently, for the length of the deploy. Heavier than Gate C's channel rename, and
 * on the same kind of path.
 *
 * Asserted against the KEYS THAT APPEAR IN REDIS, not against `busKeys()`. A test calling `busKeys`
 * directly is vacuous here — the mutation lives in `startGateway`'s `busShard`, which such a test
 * never reads, and it stayed green when that function was reverted.
 */
const token = (sub: string, ns?: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, ...(ns ? { ns } : {}) });

test('the default tenant publishes to the same keys with and without a tenant factory', async (t) => {
  const redisProc = await RedisProcess.start();
  t.after(() => redisProc.stop());
  const probe = await connectRedis(redisProc.url);
  t.after(() => void probe.destroy());

  /** Every `rtdb:*` key currently in the server, sorted — the world, not our idea of it. */
  const keysNow = async (): Promise<string[]> => (await probe.keys('rtdb:*')).sort();

  /**
   * A client for a gateway to join the bus with, destroyed by this test rather than by the gateway:
   * `RedisBus.stop()` destroys the duplicates IT opened (reader, admin) and deliberately not the
   * connection it was handed, because the caller owns that one. Left open, the file passes every
   * assertion and then hangs on the handle — which is what it did before this helper existed.
   */
  const busClient = async (): Promise<Awaited<ReturnType<typeof connectRedis>>> => {
    const c = await connectRedis(redisProc.url);
    t.after(() => void c.destroy());
    return c;
  };

  // ---- arm 1: single-tenant, exactly as production runs today.
  const solo = await startGateway({
    redis: await busClient(),
    storage: new MemoryStorage(DEFAULT_LIMITS),
    shard: 0,
    db: 'public',
  });
  await solo.storage.commitGroup([
    { writeId: crypto.randomUUID(), path: 'public/x', op: 'put', value: 1 },
  ]);
  await new Promise((r) => setTimeout(r, 400));
  const soloKeys = await keysNow();
  solo.close();
  await new Promise((r) => setTimeout(r, 200));
  await probe.flushAll();

  // ---- arm 2: the SAME shard and default database, now with a factory present.
  const multi = await startGateway({
    redis: await busClient(),
    storage: new MemoryStorage(DEFAULT_LIMITS),
    shard: 0,
    db: 'public',
    requireNs: false,
    tenantStorage: () => new MemoryStorage(DEFAULT_LIMITS),
  });
  await multi.storage.commitGroup([
    { writeId: crypto.randomUUID(), path: 'public/x', op: 'put', value: 1 },
  ]);
  await new Promise((r) => setTimeout(r, 400));
  const multiKeys = await keysNow();
  multi.close();

  assert.ok(soloKeys.length > 0, 'the single-tenant arm really used the bus');
  assert.deepEqual(multiKeys, soloKeys, 'a factory must not move the default tenant off its keys');
  // And name what those keys are, so a failure says WHICH thing moved.
  const expected = busKeys(0);
  assert.ok(soloKeys.includes(expected.stream), `the stream is ${expected.stream}`);
  assert.equal(
    soloKeys.some((k) => k.includes('_default')),
    false,
    'nothing on the default tenant may carry the synthetic label',
  );
});

/**
 * §5.22 Gate D shart (C) — `onHistoryLost` and §10's KICK reach one database's sockets and no
 * others.
 *
 * Both walk a tenant's `live` set, and both are here rather than in `tenant-map.test.ts` because
 * neither can be reached without a real bus: `onHistoryLost` is called only from `RedisBus`'s
 * `#fillFromOplog`, and `onKick` only from its admin subscriber. The version of (b) that lived in
 * the integration file drove a tenant's store directly, so it measured delta isolation and stayed
 * GREEN with `live` shared across every tenant — the mentor's own tooth. This file is where the
 * property has a witness.
 */
test('history loss and a kick reach ONE database, on a real bus', async (t) => {
  const redisProc = await RedisProcess.start();
  t.after(() => redisProc.stop());
  const probe = await connectRedis(redisProc.url);
  t.after(() => void probe.destroy());

  // Retention of ONE rev, so three commits leave `prunedThrough` above the consumer's floor — which
  // is what history loss IS (§9 pruned past what the consumer still needs), not a simulation of it.
  const limits = makeLimits({ OPLOG_RETENTION_REVS: 1 });
  const stores = new Map([
    ['car', new MemoryStorage(limits)],
    ['chat', new MemoryStorage(limits)],
  ]);
  const base = new MemoryStorage(limits);
  for (const db of stores.keys()) await base.declareDatabase(db, 'console-rw-owner');

  /**
   * The test holds CAR's leader lock, so car's gateway consumes but never publishes. That is the
   * arrangement history loss actually needs and the reason it cannot happen in one lone process:
   * a leader that died before publishing leaves revs no XADD will announce, the pruner passes them,
   * and the consumer finds the gap on its idle reconcile. `busShard` puts a non-default tenant on
   * `<shard>:<db>`, which is why this key names car and leaves chat leading its own stream.
   */
  await probe.set(busKeys('0:car').lock, 'held-by-the-test', { PX: 60_000 });

  const gw = await startGateway({
    redis: await connectRedis(redisProc.url).then((c) => (t.after(() => void c.destroy()), c)),
    storage: base,
    limits,
    shard: 0,
    db: 'public',
    requireNs: true,
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
  });
  t.after(() => gw.close());

  // ONE user, on BOTH databases. §10 kicks by SUBJECT, so a shared `live` set would take both of
  // this user's sockets down for an incident on one of their databases.
  const client = async (db: string): Promise<RtdbClient> => {
    const c = new RtdbClient({
      url: wsUrl(gw.port),
      token: token('u1', db),
      pingIntervalMs: 60_000,
      autoReconnect: false,
    });
    t.after(() => c.close());
    c.connect();
    await c.ready();
    return c;
  };
  const car = await client('car');
  const chat = await client('chat');

  const resyncs = { car: 0, chat: 0 };
  const closes: { car: number | null; chat: number | null } = { car: null, chat: null };
  car.on('resync', () => resyncs.car++);
  chat.on('resync', () => resyncs.chat++);
  car.on('close', (e: unknown) => (closes.car = (e as { code: number }).code));
  chat.on('close', (e: unknown) => (closes.chat = (e as { code: number }).code));
  await Promise.all([
    new Promise<void>((r) => car.listen('car/players', () => r())),
    new Promise<void>((r) => chat.listen('chat/rooms', () => r())),
  ]);

  // ---- (b) history loss on CAR only.
  for (let i = 1; i <= 3; i++) {
    await (stores.get('car') as MemoryStorage).commitGroup([
      { writeId: crypto.randomUUID(), path: `car/players/p${i}`, op: 'put', value: i },
    ]);
  }
  assert.equal(await (stores.get('car') as MemoryStorage).prunedThroughRev(), 2, 'the oplog pruned past the floor');
  await waitUntil(() => resyncs.car > 0, "car's sockets were told to resync", 10_000);
  assert.equal(resyncs.chat, 0, 'and chat, whose bus lost nothing, was not');

  // ---- (c) §10 kick on CAR's admin channel only.
  await probe.publish(
    busKeys('0:car').kick,
    JSON.stringify({ type: 'kick', target: { userId: 'u1' }, reason: 'gate-d' }),
  );
  await waitUntil(() => closes.car !== null, "car's socket was closed by the kick");
  assert.equal(closes.car, CLOSE.KICK);
  assert.equal(closes.chat, null, "the same user's chat socket is untouched");
  assert.equal(chat.state, 'connected', 'and still serving');
});
