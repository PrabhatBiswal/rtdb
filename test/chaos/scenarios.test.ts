import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { after, type TestContext } from 'node:test';
import { RtdbClient, type ClientOptions } from '../../harness/client.ts';
import { Proxy } from '../../harness/proxy.ts';
import { assertNoLeftovers, RedisProcess } from '../../harness/redis.ts';
import { connectRedis, type Redis } from '../../src/fanout/redis.ts';
import { assertConverged, GatewayProcess, sleep, waitUntil } from '../../harness/scenario.ts';
import { startGateway, type Gateway } from '../../src/gateway/server.ts';
import type { Ack, CasFail, Delta, Json, ServerFrame } from '../../src/protocol/frames.ts';
import { makeLimits, type Limits } from '../../src/protocol/limits.ts';
import { goodToken, testStorage, wsUrl } from '../helpers.ts';

/**
 * WORKLOAD §6 Gate D. Every scenario ends the same way: every client mirror equals server state on
 * every path it subscribes to, and nothing was applied twice.
 *
 * Faults are injected at the network (harness/proxy.ts) or by killing the gateway process — never
 * by a test-only hook inside the server. The one exception is scenario 8, where WORKLOAD explicitly
 * calls for harness-level injection because the dispatcher itself never reorders.
 *
 * `RTDB_CHAOS_CLUSTER=1` (npm run chaos:cluster) runs this same battery over §8's bus: TWO gateways
 * on one store and one Redis, with `connect()` on gateway A and `connectDirect()` on gateway B, so
 * the writer and the reader are on opposite sides of the stream. Not one scenario body changes —
 * what changes is where the frames have to travel to satisfy it.
 */

const TOKEN = goodToken();
const CLUSTER = process.env['RTDB_CHAOS_CLUSTER'] === '1';

/** One redis-server for the whole file; a shard of its own per scenario, as the pg suites do. */
let redisProc: Promise<RedisProcess> | null = null;
let shardSeq = 0;
const redisUrl = async (): Promise<string> => {
  redisProc ??= RedisProcess.start();
  return (await redisProc).url;
};
after(async () => {
  if (!redisProc) return;
  await (await redisProc).stop();
  assertNoLeftovers();
});

/** A killable gateway process, on the bus when the battery is running in cluster mode. */
const chaosGateway = async (limits: Partial<Limits>, persist?: string): Promise<GatewayProcess> =>
  GatewayProcess.start(
    limits,
    0,
    persist,
    CLUSTER ? { RTDB_REDIS_URL: await redisUrl(), RTDB_SHARD: `c${++shardSeq}` } : {},
  );

interface Chaos {
  gw: Gateway;
  proxy: Proxy;
  /** Through the proxy: breakable. */
  connect: (o?: Partial<ClientOptions>) => Promise<RtdbClient>;
  /** Straight to the gateway: the other side of the story, never cut. */
  connectDirect: (o?: Partial<ClientOptions>) => Promise<RtdbClient>;
}

async function chaos(t: TestContext, overrides: Partial<Limits> = {}): Promise<Chaos> {
  const limits = makeLimits({ BACKOFF_CAP_MS: 40, ...overrides });
  const { storage, cleanup } = testStorage(limits);

  // Cluster mode: one store, one shard, TWO gateways — so a delta from the writer's gateway only
  // reaches the reader's by going out over the stream and back.
  const bus: Redis[] = [];
  const shard = `c${++shardSeq}`;
  const joinBus = async (): Promise<{ redis: Redis; shard: string }> => {
    const redis = await connectRedis(await redisUrl());
    bus.push(redis);
    return { redis, shard };
  };
  const gw = await startGateway({ limits, storage, ...(CLUSTER ? await joinBus() : {}) });
  const peer = CLUSTER ? await startGateway({ limits, storage, ...(await joinBus()) }) : gw;
  t.after(async () => {
    gw.close();
    peer.close();
    for (const r of bus) {
      try {
        r.destroy();
      } catch {
        /* already gone */
      }
    }
    await cleanup();
  });
  const proxy = await Proxy.start(gw.port);
  t.after(() => proxy.stop());

  const make = async (url: string, o: Partial<ClientOptions>): Promise<RtdbClient> => {
    const client = new RtdbClient({ url, token: TOKEN, limits, pingIntervalMs: 60_000, ...o });
    t.after(() => client.close());
    client.connect();
    await client.ready();
    return client;
  };
  return {
    gw,
    proxy,
    connect: (o = {}) => make(proxy.url, o),
    // The other side of the bus in cluster mode; the same gateway otherwise.
    connectDirect: (o = {}) => make(wsUrl(peer.port), o),
  };
}

const countFrames = (c: RtdbClient, type: ServerFrame['type']): (() => number) => {
  let n = 0;
  c.on('frame', (f: ServerFrame) => {
    if (f.type === type) n++;
  });
  return () => n;
};

// ---------------------------------------------------------------- 1

test('S1 lost ack: the ack dies after the commit; the replay returns the ORIGINAL rev', async (t) => {
  const { gw, proxy, connect } = await chaos(t);
  const c = await connect();
  c.listen('p');
  await waitUntil(async () => (await gw.storage.head()) === 0 && c.subscriptions.length === 1, 'listen');

  proxy.pauseDownstream(); // nothing the server sends can reach the client any more
  const inflight = c.put('p/score', 42);
  await waitUntil(async () => (await gw.storage.head()) === 1, 'server commit');
  proxy.cut(); // ...and now the connection dies, taking the ack with it
  proxy.resumeDownstream();

  const ack = (await inflight) as Ack;
  assert.equal(ack.rev, 1, 'the replay is acked with the rev the first commit got');
  assert.equal(await gw.storage.head(), 1, 'exactly-once: the replay burned no rev');
  assert.equal((await gw.storage.readSnapshot('p/score')).value, 42);
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 2

test('S2 reconnect catch-up: a short gap is served from the oplog, with no second snapshot', async (t) => {
  const { gw, proxy, connect, connectDirect } = await chaos(t);
  const writer = await connectDirect();
  await writer.put('room/seed', 'v'); // so the first snapshot carries a rev > 0 to resume from
  const c = await connect();
  const snapshots = countFrames(c, 'snapshot');
  c.listen('room');
  await waitUntil(() => snapshots() === 1, 'initial snapshot');

  proxy.blackhole(); // down, and staying down until every write has landed
  await once(c, 'state'); // -> waiting
  for (let i = 0; i < 3; i++) await writer.put(`room/m${i}`, i);
  proxy.restore();

  await c.ready();
  await waitUntil(() => c.value('room/m2') === 2, 'catch-up');
  assert.equal(snapshots(), 1, 'a retained lastRev is served with deltas only (§3)');
  assert.deepEqual(c.value('room'), { seed: 'v', m0: 0, m1: 1, m2: 2 });
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 3

test('S3 snapshot fallback: too far behind and the server re-snapshots instead', async (t) => {
  const { gw, proxy, connect, connectDirect } = await chaos(t, { CATCHUP_LIMIT: 2 });
  const writer = await connectDirect();
  await writer.put('room/seed', 'v');
  const c = await connect();
  const snapshots = countFrames(c, 'snapshot');
  c.listen('room');
  await waitUntil(() => snapshots() === 1, 'initial snapshot');

  proxy.blackhole();
  await once(c, 'state');
  for (let i = 0; i < 5; i++) await writer.put(`room/m${i}`, i); // > CATCHUP_LIMIT
  proxy.restore();

  await c.ready();
  await waitUntil(() => snapshots() === 2, 'fresh snapshot');
  assert.deepEqual(c.value('room'), { seed: 'v', m0: 0, m1: 1, m2: 2, m3: 3, m4: 4 });
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 4

test('S4 duplicate writeId storm: 5 sends across 2 connections, one commit, 5 identical acks', async (t) => {
  const { gw, connect, connectDirect } = await chaos(t);
  const [a, b] = await Promise.all([connect(), connectDirect()]);
  const acks: Ack[] = [];
  for (const c of [a, b]) {
    c.on('frame', (f: ServerFrame) => {
      if (f.type === 'ack') acks.push(f);
    });
  }

  const writeId = '0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6';
  const frame = { type: 'put', writeId, path: 'p', value: 'first' } as const;
  for (const c of [a, a, b, a, b]) c.send(frame);

  await waitUntil(() => acks.length === 5, 'five acks');
  await sleep(30);
  assert.deepEqual(acks, Array.from({ length: 5 }, () => ({ type: 'ack', writeId, rev: 1 })));
  assert.equal(await gw.storage.head(), 1, 'one commit');
  assert.equal((await gw.storage.readSnapshot('p')).value, 'first');
});

// ---------------------------------------------------------------- 5

test('S5 concurrent CAS: five racers, exactly one ack and four casFails with fresh state', async (t) => {
  const { gw, connectDirect } = await chaos(t);
  const clients = await Promise.all(Array.from({ length: 5 }, () => connectDirect()));
  const base = ((await clients[0]!.put('p/score', 0)) as Ack).rev;

  const results = await Promise.all(clients.map((c, i) => c.cas('p/score', base, i + 1)));
  const acks = results.filter((r): r is Ack => r.type === 'ack');
  const fails = results.filter((r): r is CasFail => r.type === 'casFail');

  assert.equal(acks.length, 1, 'exactly one CAS may win');
  assert.equal(fails.length, 4);
  const winner = (await gw.storage.readSnapshot('p/score')).value;
  for (const f of fails) {
    assert.equal(f.value, winner, 'a casFail carries the state that beat it');
    assert.equal(f.rev, acks[0]!.rev);
  }
  assert.equal(await gw.storage.head(), 2, 'four losing CAS attempts consume no revs (§1)');
});

// ---------------------------------------------------------------- 6

test('S6 slow consumer: the queue overflows, the server sends resync, the mirror converges', async (t) => {
  const { gw, proxy, connect, connectDirect } = await chaos(t, { SEND_QUEUE_MAX: 1024, DELTA_BATCH_MS: 5 });
  const writer = await connectDirect();
  const c = await connect();
  let resyncs = 0;
  c.on('resync', () => resyncs++);
  c.listen('room');
  await waitUntil(() => c.subscriptions[0]?.lastRev !== undefined, 'listen');
  await sleep(30);

  proxy.pauseDownstream(); // the client stops reading; backpressure is real, not simulated
  const blob = 'x'.repeat(64 * 1024);
  for (let i = 0; i < 40; i++) await writer.put(`room/big${i}`, `${i}:${blob}`);
  await sleep(100);
  proxy.resumeDownstream();

  await waitUntil(() => resyncs > 0, 'resync', 10_000);
  await waitUntil(async () => {
    const server = (await gw.storage.readSnapshot('room')).value;
    return JSON.stringify(c.value('room')) === JSON.stringify(server);
  }, 'convergence after resync', 10_000);
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 7

test('S7 subscribe/write race: a listen during a write storm leaves no gap and no double-apply', async (t) => {
  const { gw, connectDirect } = await chaos(t);
  const writer = await connectDirect();
  const N = 100;

  const storm = (async () => {
    for (let i = 0; i < N; i++) await writer.put(`race/k${i}`, i);
  })();
  await sleep(5); // subscribe mid-storm, which is the whole point
  const c = await connectDirect();
  c.listen('race');
  await storm;

  await waitUntil(() => c.value(`race/k${N - 1}`) === N - 1, 'the tail of the storm');
  const view = c.value('race') as { [k: string]: Json };
  assert.equal(Object.keys(view).length, N, 'every key exactly once — no gap, no duplicate');
  for (let i = 0; i < N; i++) assert.equal(view[`k${i}`], i);
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 8

test('S8 ancestor delete: a tombstone refuses to let a stale delta resurrect the subtree', async (t) => {
  const { gw, connectDirect } = await chaos(t);
  const writer = await connectDirect();
  const c = await connectDirect();
  c.listen('p');

  const created = ((await writer.put('p/a', { deep: 1, other: 2 })) as Ack).rev;
  await waitUntil(() => c.value('p/a/deep') === 1, 'creation');
  await writer.put('p/a', null); // delete the subtree
  await waitUntil(() => c.value('p') === null, 'deletion');

  // The dispatcher never reorders, so the stale delta is injected at the harness level — exactly
  // the case §7's tombstones exist to catch.
  const stale: Delta = { type: 'delta', rev: created, path: 'p/a/deep', op: 'put', value: 'zombie' };
  c.mirror.applyDelta(stale);
  c.mirror.applyDelta({ ...stale, path: 'p/a/never-seen' });
  c.mirror.applyDelta({ ...stale, path: 'p/a', value: { deep: 'zombie' } });

  assert.equal(c.value('p'), null, 'the deleted subtree stays deleted');
  await assertConverged([c], wsUrl(gw.port), TOKEN);

  // ...and a genuinely newer write still lands.
  await writer.put('p/a/deep', 'alive');
  await waitUntil(() => c.value('p/a/deep') === 'alive', 'a newer write is not blocked');
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 9

test('S9 pending overlay: unacked local writes and concurrent foreign deltas coexist', async (t) => {
  const { gw, proxy, connect, connectDirect } = await chaos(t);
  const writer = await connectDirect();
  const c = await connect();
  c.listen('shared');
  await sleep(30);

  proxy.pauseDownstream(); // no acks, no deltas — every local write stays pending
  const mine = Array.from({ length: 5 }, (_, i) => c.put(`shared/a${i}`, `mine-${i}`));
  const theirs = (async () => {
    for (let i = 0; i < 5; i++) await writer.put(`shared/b${i}`, `theirs-${i}`);
  })();

  // While pending: the overlay shows every local write, serverState shows none of them.
  for (let i = 0; i < 5; i++) {
    assert.equal(c.value(`shared/a${i}`), `mine-${i}`, 'view = serverState ⊕ overlay (§7)');
    assert.equal(c.mirror.serverValue(`shared/a${i}`), null, 'serverState is untouched until the echo');
  }
  assert.equal(c.pendingWriteIds.length, 5);

  await theirs;
  proxy.resumeDownstream();
  await Promise.all(mine);

  await waitUntil(() => c.value('shared/b4') === 'theirs-4', 'foreign writes arrive');
  await waitUntil(async () => {
    const server = (await gw.storage.readSnapshot('shared')).value;
    return JSON.stringify(c.mirror.serverValue('shared')) === JSON.stringify(server);
  }, 'convergence');
  assert.deepEqual(c.pendingWriteIds, [], 'every write settled');
  await assertConverged([c], wsUrl(gw.port), TOKEN);
});

// ---------------------------------------------------------------- 10

test('S10 gateway restart: SIGKILL mid-traffic, clients back off, resume and replay', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'rtdb-chaos-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const gw = await chaosGateway({ BACKOFF_CAP_MS: 40 }, join(dir, 'oplog.jsonl'));
  t.after(() => gw.stop());

  const limits = makeLimits({ BACKOFF_CAP_MS: 40 });
  const clients = await Promise.all(
    [0, 1].map(async () => {
      const c = new RtdbClient({ url: gw.url, token: TOKEN, limits, pingIntervalMs: 60_000 });
      t.after(() => c.close());
      c.connect();
      await c.ready();
      c.listen('room');
      return c;
    }),
  );
  const [a, b] = clients as [RtdbClient, RtdbClient];
  await a.put('room/before', 1);
  await waitUntil(() => b.value('room/before') === 1, 'pre-kill traffic');

  await gw.kill(); // SIGKILL: no close frames, the process simply stops
  // POLL the state, never await the next 'state' event: the socket can close (and the client can
  // transition) before kill() resolves, and a missed event here waits forever — the retry timer is
  // unref'd, so at that point nothing is left to keep the loop alive at all.
  await waitUntil(() => a.state !== 'connected', 'the client to notice the gateway is gone');

  const queued = a.put('room/during', 2); // issued while there is no server at all
  assert.equal(a.value('room/during'), 2, 'the overlay shows it immediately');

  await gw.restart();
  await Promise.all(clients.map((c) => c.ready()));
  const ack = (await queued) as Ack;
  assert.equal(ack.type, 'ack', 'the pending write replayed on reconnect (§6 step 3)');

  await a.put('room/after', 3);
  // BOTH clients, not just the peer. An ack means the write COMMITTED; §7 fills serverState from the
  // echo that follows, so the writer's own view is the last thing to converge, not the first — and on
  // the bus that echo is a round trip further away. Waiting only for the peer asserts the writer's
  // mirror one frame too early.
  await waitUntil(
    () => a.value('room/after') === 3 && b.value('room/after') === 3,
    'traffic resumes for everyone',
  );
  assert.deepEqual(a.value('room'), { before: 1, during: 2, after: 3 }, 'the oplog survived the kill');
  await assertConverged(clients, gw.url, TOKEN);
});

// ---------------------------------------------------------------- 11

test('S11 restart WITHOUT persistence: a new epoch drops the dead generation (§2 v1.5)', async (t) => {
  const gw = await chaosGateway({ BACKOFF_CAP_MS: 40 }); // no persist path: nothing survives
  t.after(() => gw.stop());

  const limits = makeLimits({ BACKOFF_CAP_MS: 40 });
  const c = new RtdbClient({ url: gw.url, token: TOKEN, limits, pingIntervalMs: 60_000 });
  t.after(() => c.close());
  c.connect();
  const first = await c.ready();
  c.listen('room');
  await c.put('room/before', 1);
  await waitUntil(() => c.value('room/before') === 1, 'pre-restart state');

  await gw.restart(); // head back to 0 — every rev this client holds is from a dead generation

  await waitUntil(() => c.epoch !== null && c.epoch !== first.epoch, 'a fresh epoch on helloAck');
  // Without the wholesale drop, §7's per-leaf LWW keeps `before` (rev 1) in preference to the
  // restored shard's rev-0 snapshot, and the client diverges silently until app restart.
  assert.equal(c.value('room'), null, 'mirrors, revs, tombstones and lastRevs all dropped');

  await c.put('room/after', 2);
  await waitUntil(() => c.value('room/after') === 2, 'writes resume against the new generation');
  assert.deepEqual(c.value('room'), { after: 2 }, 'nothing from the dead generation survived');
  await assertConverged([c], gw.url, TOKEN);
});
