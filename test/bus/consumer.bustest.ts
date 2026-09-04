import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after, type TestContext } from 'node:test';
import { RtdbClient } from '../../harness/client.ts';
import { Proxy } from '../../harness/proxy.ts';
import { assertNoLeftovers, RedisProcess } from '../../harness/redis.ts';
import { GatewayProcess, waitUntil } from '../../harness/scenario.ts';
import { busKeys, connectRedis, type Redis } from '../../src/fanout/redis.ts';
import { startGateway, type Gateway } from '../../src/gateway/server.ts';
import type { Delta, ServerFrame } from '../../src/protocol/frames.ts';
import { makeLimits, type Limits } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { goodToken, wsUrl } from '../helpers.ts';

after(() => assertNoLeftovers());

const KEYS = busKeys();
const range = (a: number, b: number): number[] => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const destroy = (c: Redis): void => {
  try {
    c.destroy();
  } catch {
    /* already gone */
  }
};

/**
 * Two gateways over one shard and one Redis, which is the smallest rig that can break a CONSUMER
 * without breaking the publisher: A publishes on a direct connection, B consumes through a proxy the
 * test can blackhole. Faults are at the network, never a hook inside the server (WP1 Gate D rule).
 */
interface Cluster {
  redis: RedisProcess;
  storage: MemoryStorage;
  probe: Redis;
  proxy: Proxy;
  gwB: Gateway;
  /** A client on B — the gateway whose bus link the test is about to break. */
  client: RtdbClient;
  /** Every delta B put on that client's wire, in the order it arrived. */
  revs: number[];
  resyncs: number;
}

async function cluster(t: TestContext, overrides: Partial<Limits> = {}): Promise<Cluster> {
  const limits = makeLimits({ BACKOFF_CAP_MS: 40, ...overrides });
  const redis = await RedisProcess.start();
  const storage = new MemoryStorage(limits);
  const probe = await connectRedis(redis.url);

  const clientA = await connectRedis(redis.url);
  const gwA = await startGateway({ limits, storage, redis: clientA });
  // A campaigned and won inside its own start(), so B is deterministically the follower.
  assert.equal((await probe.get(KEYS.lock)) !== null, true, 'A holds the shard lock');

  const proxy = await Proxy.start(redis.port);
  const clientB = await connectRedis(`redis://127.0.0.1:${proxy.port}`);
  const gwB = await startGateway({ limits, storage, redis: clientB });

  const client = new RtdbClient({ url: wsUrl(gwB.port), token: goodToken(), limits, pingIntervalMs: 60_000 });
  const revs: number[] = [];
  let resyncs = 0;
  client.on('frame', (f: ServerFrame) => {
    if (f.type === 'delta') revs.push(f.rev);
    if (f.type === 'batch') for (const inner of f.frames) if (inner.type === 'delta') revs.push(inner.rev);
    if (f.type === 'resync') resyncs++;
  });
  client.connect();
  await client.ready();

  t.after(async () => {
    client.close();
    gwB.close();
    gwA.close();
    proxy.stop();
    destroy(clientA);
    destroy(clientB);
    destroy(probe);
    await redis.stop();
  });

  const self = { redis, storage, probe, proxy, gwB, client, revs, resyncs: 0 };
  Object.defineProperty(self, 'resyncs', { get: () => resyncs });
  return self as Cluster;
}

/** N writes through the client on B, one per rev, all under the listened path. */
async function write(c: Cluster, from: number, to: number): Promise<void> {
  for (const i of range(from, to)) await c.client.put(`p/k${i}`, i);
}

test('a cut consumer connection replays from its last id — order intact, nothing lost', async (t) => {
  const c = await cluster(t);
  c.client.listen('p');
  await write(c, 1, 10);
  await waitUntil(() => c.client.value('p/k10') === 10, 'first ten delivered');

  c.proxy.cut(); // B's bus link dies mid-flight; node-redis reconnects underneath it
  await write(c, 11, 20);

  await waitUntil(() => c.client.value('p/k20') === 20, 'delivery resumes after the cut', 10_000);
  assert.deepEqual(c.revs, range(1, 20), 'every rev exactly once, ascending — the XRANGE replay (§8)');
});

test('a stream trimmed past the consumer falls back to the oplog (§8)', async (t) => {
  const c = await cluster(t);
  c.client.listen('p');
  await write(c, 1, 10);
  await waitUntil(() => c.client.value('p/k10') === 10, 'first ten delivered');

  c.proxy.blackhole(); // B cannot reach Redis at all, and cannot reconnect either
  await write(c, 11, 20);
  await waitUntil(async () => (await c.probe.xLen(KEYS.stream)) === 20, 'A published them anyway');
  await c.probe.xTrim(KEYS.stream, 'MAXLEN', 1); // everything B has not read is gone from the bus
  assert.equal(await c.probe.xLen(KEYS.stream), 1);

  c.proxy.restore();
  await waitUntil(() => c.client.value('p/k20') === 20, 'delivery resumes', 15_000);
  assert.deepEqual(c.revs, range(1, 20), 'revs 11..19 came from the oplog, in order, exactly once');
  assert.equal(c.resyncs, 0, 'the oplog could still answer, so no subscription was declared stale');
});

test('past oplog retention the consumer stops guessing and resyncs (§3)', async (t) => {
  // Retention this small means the oplog itself forgets while B is away: neither the bus nor the
  // oplog can reconstruct what B missed, and §3's resync is the only honest repair left.
  const c = await cluster(t, { OPLOG_RETENTION_REVS: 5 });
  c.client.listen('p');
  await write(c, 1, 10);
  await waitUntil(() => c.client.value('p/k10') === 10, 'first ten delivered');

  c.proxy.blackhole();
  await write(c, 11, 30);
  await waitUntil(async () => (await c.storage.prunedThroughRev()) > 10, 'the oplog forgot B’s position');
  await c.probe.xTrim(KEYS.stream, 'MAXLEN', 1);

  c.proxy.restore();
  await waitUntil(() => c.resyncs > 0, 'the subscription is declared stale', 15_000);
  await waitUntil(() => c.client.value('p/k30') === 30, 'and a fresh snapshot repairs it', 15_000);
  assert.deepEqual(c.client.value('p/k1'), 1, 'the snapshot carries what the deltas could not');
});

test('history the bus lost and will never re-announce is reconciled from the oplog (§8)', async (t) => {
  const c = await cluster(t);
  c.client.listen('p');
  await write(c, 1, 10);
  await waitUntil(() => c.client.value('p/k10') === 10, 'first ten delivered');

  c.proxy.blackhole(); // B is away
  await write(c, 11, 20);
  await waitUntil(async () => (await c.probe.xLen(KEYS.stream)) === 20, 'A published them');
  // The bus loses its history while the leader keeps its place: A has already published through 20
  // and will never XADD those revs again, so nothing on the stream will ever announce 11..20 to B.
  // An XREAD-only consumer would sit in silence until the next unrelated write.
  await c.probe.del(KEYS.stream);

  c.proxy.restore();
  await waitUntil(() => c.client.value('p/k20') === 20, 'the idle reconcile fills it', 20_000);
  assert.deepEqual(c.revs, range(1, 20), 'in order, exactly once — never silence');
});

test('writes ack through a dead Redis, and the bus catches up when it returns', async (t) => {
  const c = await cluster(t);
  c.client.listen('p');
  await write(c, 1, 10);
  await waitUntil(() => c.client.value('p/k10') === 10, 'first ten delivered');

  // The bus dies. Writes still ack — the oplog is the truth, Redis is only live state.
  await c.redis.kill();
  await write(c, 11, 20);
  await c.redis.restart();

  await waitUntil(() => c.client.value('p/k20') === 20, 'fanout resumes', 20_000);
  assert.deepEqual(c.revs, range(1, 20), 'in order, exactly once — never silence');
});

test('a gateway joining a shard with history seeds its floor and cannot skip a range', async (t) => {
  // The hole a zero floor left: a gateway that has never delivered a delta has nothing to compare
  // the first stream entry against, so a bus-level SKIP is invisible to it and its clients' snapshots
  // strand below the skipped range. Everything here happens BEFORE that gateway's first delta.
  const limits = makeLimits({ BACKOFF_CAP_MS: 40 });
  const redis = await RedisProcess.start();
  const storage = new MemoryStorage(limits);
  const probe = await connectRedis(redis.url);
  const clientA = await connectRedis(redis.url);
  const gwA = await startGateway({ limits, storage, redis: clientA });

  const put = async (i: number): Promise<void> => {
    await storage.commitGroup([{ writeId: randomUUID(), path: `p/k${i}`, op: 'put', value: i }]);
  };
  for (const i of range(1, 10)) await put(i); // history that predates the new gateway entirely
  await waitUntil(async () => (await probe.xLen(KEYS.stream)) === 10, 'A published the history');

  // B joins now — fresh, floor seeded at the head it found.
  const proxy = await Proxy.start(redis.port);
  const clientB = await connectRedis(`redis://127.0.0.1:${proxy.port}`);
  const gwB = await startGateway({ limits, storage, redis: clientB });
  const client = new RtdbClient({ url: wsUrl(gwB.port), token: goodToken(), limits, pingIntervalMs: 60_000 });
  t.after(async () => {
    client.close();
    gwB.close();
    gwA.close();
    proxy.stop();
    for (const c of [clientA, clientB, probe]) destroy(c);
    await redis.stop();
  });
  client.connect();
  await client.ready();
  client.listen('p');
  await waitUntil(() => client.value('p/k10') === 10, 'B served a snapshot at rev 10');

  // The bus loses its history while the leader keeps its place, and B is away for all of it: the
  // revs published in between are gone from the stream and A will never re-announce them.
  proxy.blackhole();
  for (const i of range(11, 20)) await put(i);
  await probe.del(KEYS.stream);
  for (const i of range(21, 25)) await put(i);
  proxy.restore();

  await waitUntil(() => client.value('p/k25') === 25, 'the bus resumes', 20_000);
  assert.equal(client.value('p/k15'), 15, 'the skipped range reached a gateway that had never delivered one');
  assert.equal(client.value('p/k20'), 20);
});

test('a set-but-unreachable RTDB_REDIS_URL is a boot failure, not a silent island', async () => {
  const redis = await RedisProcess.start();
  const url = redis.url;
  await redis.stop(); // the port is now closed; the URL is set and wrong, which is the WP4 rule
  await assert.rejects(
    GatewayProcess.start({}, 0, undefined, { RTDB_REDIS_URL: url }),
    /exited before listening/,
    'the gateway must refuse to serve rather than serve its own connections as an island',
  );
});
