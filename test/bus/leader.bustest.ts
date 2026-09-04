import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after, type TestContext } from 'node:test';
import { assertNoLeftovers, RedisProcess } from '../../harness/redis.ts';
import { RtdbClient } from '../../harness/client.ts';
import { GatewayProcess, waitUntil } from '../../harness/scenario.ts';
import { busKeys, connectRedis, publishEntry, RedisBus, type BusOptions, type Redis } from '../../src/fanout/redis.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { goodToken } from '../helpers.ts';

after(() => assertNoLeftovers());

const KEYS = busKeys();
const range = (a: number, b: number): number[] => Array.from({ length: b - a + 1 }, (_, i) => a + i);
const revsIn = async (c: Redis): Promise<number[]> =>
  (await c.xRange(KEYS.stream, '-', '+')).map((e) => Number(e.id.split('-')[0]));

/** N more writes on the shard. One group commit == N revs, assigned in arrival order (§4 step 2). */
const commit = (s: MemoryStorage, n: number): Promise<unknown> =>
  s.commitGroup(range(1, n).map((i) => ({ writeId: randomUUID(), path: `p/${i}`, op: 'put' as const, value: i })));

/** node-redis' destroy() is synchronous and throws on an already-dead client; teardown double-fires. */
const kill = (c: Redis): void => {
  try {
    c.destroy();
  } catch {
    /* already gone */
  }
};

interface Candidate {
  client: Redis;
  bus: RedisBus;
  prunes: number;
}

/** One Redis, one shard's storage, and however many gateways are contending for it. */
async function rig(t: TestContext, n: number, opts: BusOptions = {}) {
  const redis = await RedisProcess.start();
  const storage = new MemoryStorage();
  const cands: Candidate[] = [];
  for (let i = 0; i < n; i++) {
    const client = await connectRedis(redis.url);
    // Warmed BEFORE the election: WP4's false-green lesson — a "race" decided by who finished their
    // TCP handshake first is not a race, and passes just as happily with the mutual exclusion gone.
    await client.ping();
    const c: Candidate = { client, bus: null as unknown as RedisBus, prunes: 0 };
    c.bus = new RedisBus(client, storage, {
      ttlMs: 300,
      ...opts,
      ...(opts.prune ? { prune: { ...opts.prune, run: async () => void c.prunes++ } } : {}),
    });
    cands.push(c);
  }
  const probe = await connectRedis(redis.url);
  t.after(async () => {
    for (const c of cands) {
      await c.bus.stop().catch(() => undefined);
      kill(c.client);
    }
    kill(probe);
    await redis.stop();
  });
  return { redis, storage, cands, probe };
}

const leaderOf = (cands: Candidate[]): Candidate => {
  const leading = cands.filter((c) => c.bus.isLeader);
  assert.equal(leading.length, 1, `expected exactly one leader, got ${leading.length}`);
  return leading[0] as Candidate;
};

test('five warmed candidates campaign at once; exactly one leads (§8)', async (t) => {
  const { cands, probe } = await rig(t, 5);
  await Promise.all(cands.map((c) => c.bus.start()));

  // Teeth against a false green: the fencing counter proves all five actually reached SET NX. A
  // "race" where four candidates never campaigned would satisfy "exactly one leader" for free.
  await waitUntil(
    async () => Number(await probe.get(KEYS.fence)) >= 5 && cands.some((c) => c.bus.isLeader),
    'five campaigns and a leader',
  );
  const leader = leaderOf(cands);
  assert.equal(await probe.get(KEYS.lock), String(leader.bus.leadership.token));

  // Losers keep campaigning and keep losing for as long as the leader renews. Checked continuously:
  // a second leader appearing for one tick is the whole failure mode.
  await waitUntil(async () => {
    assert.equal(cands.filter((c) => c.bus.isLeader).length, 1, 'two publishers at once');
    return Number(await probe.get(KEYS.fence)) >= 12;
  }, 'losers keep campaigning');
  assert.equal(leaderOf(cands), leader, 'a live leader must not be displaced');
});

test('fencing tokens are strictly increasing across successive leaders', async (t) => {
  const { cands } = await rig(t, 2);
  await Promise.all(cands.map((c) => c.bus.start()));
  await waitUntil(() => cands.some((c) => c.bus.isLeader), 'first leader');
  const first = leaderOf(cands);
  const firstToken = first.bus.leadership.token as number;

  kill(first.client); // SIGKILL-shaped: never releases the lock, the TTL does
  const other = cands.find((c) => c !== first) as Candidate;
  await waitUntil(() => other.bus.isLeader, 'takeover');
  assert.ok(
    (other.bus.leadership.token as number) > firstToken,
    `token went backwards: ${other.bus.leadership.token} after ${firstToken}`,
  );
});

test('takeover on leader death: no gap, no duplicate, no reorder in the stream', async (t) => {
  const { storage, cands, probe } = await rig(t, 2);
  await Promise.all(cands.map((c) => c.bus.start()));
  await waitUntil(() => cands.some((c) => c.bus.isLeader), 'first leader');
  const leader = leaderOf(cands);
  const follower = cands.find((c) => c !== leader) as Candidate;

  await commit(storage, 20);
  await waitUntil(async () => (await revsIn(probe)).length === 20, 'the leader published the first 20');

  // The leader stops answering mid-stream and never gets to clean up after itself.
  kill(leader.client);
  // ...and the shard keeps taking writes while nobody is publishing. This is the gap under test.
  await commit(storage, 20);

  await waitUntil(() => follower.bus.isLeader, 'the follower takes over', 8000);
  await waitUntil(async () => (await revsIn(probe)).length === 40, 'the new leader catches the stream up', 8000);
  assert.deepEqual(await revsIn(probe), range(1, 40), 'every rev exactly once, ascending');
});

test('a fenced-out leader keeps publishing and Redis refuses every append (split brain)', async (t) => {
  // A long TTL widens the window that a crash-and-takeover closes in milliseconds: A still believes
  // it leads (its renewal is not due), B has legitimately acquired, and both are tailing one oplog.
  const { storage, cands, probe } = await rig(t, 2, { ttlMs: 60_000 });
  const [a, b] = cands as [Candidate, Candidate];
  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'A leads');
  await commit(storage, 20);
  await waitUntil(async () => (await revsIn(probe)).length === 20, 'A published 20');

  await b.bus.start(); // loses: A holds the lock
  await probe.del(KEYS.lock); // the lock expires under a leader too slow to notice
  await waitUntil(async () => {
    await b.bus.leadership.tick();
    return b.bus.isLeader;
  }, 'B acquires the vacant lock');
  assert.equal(a.bus.isLeader, true, 'A has not yet learned it was fenced out — the window under test');

  await commit(storage, 20);
  await waitUntil(async () => (await revsIn(probe)).length === 40, 'the shard caught up', 8000);
  // Two publishers, one stream: the id IS the rev, so Redis rejects whichever arrives second and
  // neither a duplicate nor a reorder can reach a consumer.
  assert.deepEqual(await revsIn(probe), range(1, 40));
});

test('a leader that lost the lock steps down on its next renewal and stops publishing', async (t) => {
  const { storage, cands, probe } = await rig(t, 1, { ttlMs: 60_000 });
  const [a] = cands as [Candidate];
  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'leader');
  await commit(storage, 5);
  await waitUntil(async () => (await revsIn(probe)).length === 5, 'published');

  await probe.set(KEYS.lock, '999999'); // the lock expired and somebody else took it
  await a.bus.leadership.tick();
  assert.equal(a.bus.isLeader, false, 'a holder that cannot prove it holds the lock does not hold it');

  await commit(storage, 5);
  await waitUntil(async () => (await storage.head()) === 10, 'the writes committed');
  await new Promise((r) => setTimeout(r, 50)); // room for any stray pump to publish
  assert.deepEqual(await revsIn(probe), range(1, 5), 'a demoted gateway publishes nothing');
});

test('an explicit stale append is refused, not swallowed', async (t) => {
  const { storage, cands, probe } = await rig(t, 1);
  const [a] = cands as [Candidate];
  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'leader');
  await commit(storage, 10);
  await waitUntil(async () => (await revsIn(probe)).length === 10, 'published');

  const delta = { type: 'delta' as const, rev: 0, path: 'p', op: 'put' as const, value: 'stale' };
  for (const rev of [1, 9, 10]) {
    assert.equal(await publishEntry(probe, KEYS.stream, rev, { ...delta, rev }, 1000), 'stale');
  }
  assert.equal(await publishEntry(probe, KEYS.stream, 11, { ...delta, rev: 11 }, 1000), 'ok');
  assert.deepEqual(await revsIn(probe), range(1, 11), 'refused appends left no trace');
});

test('the prune chore runs only while holding leadership (WP4 Gate D Q4)', async (t) => {
  const { cands } = await rig(t, 2, { prune: { intervalMs: 10, run: async () => undefined } });
  await Promise.all(cands.map((c) => c.bus.start()));
  await waitUntil(() => cands.some((c) => c.bus.isLeader), 'a leader');
  const leader = leaderOf(cands);
  const follower = cands.find((c) => c !== leader) as Candidate;

  await waitUntil(() => leader.prunes >= 3, 'the leader prunes');
  assert.equal(follower.prunes, 0, 'a follower must never prune');

  kill(leader.client);
  await waitUntil(() => follower.bus.isLeader, 'takeover', 8000);
  const frozen = leader.prunes;
  await waitUntil(() => follower.prunes >= 3, 'the new leader picks the chore up');
  assert.equal(leader.prunes, frozen, 'a demoted leader stops pruning');
});

test('a tail read that FAILED is not an empty stream: promotion aborts and retries', async (t) => {
  const { storage, cands, probe } = await rig(t, 1, { idleMs: 80 });
  const [a] = cands as [Candidate];
  await commit(storage, 10);
  // A bus that already knows its generation, so promotion goes straight to the tail read (§2's epoch
  // check would otherwise clear the key this test is about to corrupt).
  await probe.set(KEYS.epoch, String(await storage.epoch()));
  // Any read of the stream key now errors (WRONGTYPE). The lock and fence keys are untouched, so
  // the election still succeeds — which is precisely the dangerous shape: a leader that cannot see
  // the stream. Resuming from head() here would skip every rev the previous leader never published.
  await probe.set(KEYS.stream, 'not a stream');

  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'leadership is unaffected by an unreadable stream');
  assert.equal(a.bus.publishing, false, 'a tail read that failed must not be treated as empty');

  await probe.del(KEYS.stream); // the stream is readable again — and genuinely empty this time
  await waitUntil(() => a.bus.publishing, 'the retry picks it up');
  await commit(storage, 1);
  await waitUntil(async () => (await revsIn(probe)).length > 0, 'publishing resumed');
  assert.deepEqual(await revsIn(probe), [11]);
});

test('an empty stream resumes at the oplog head — the bus is live state, the oplog is the truth', async (t) => {
  const { storage, cands, probe } = await rig(t, 1);
  const [a] = cands as [Candidate];
  await commit(storage, 10); // history that predates the bus (or outlived a Redis restart)
  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'leader');

  await commit(storage, 1);
  await waitUntil(async () => (await revsIn(probe)).length > 0, 'the new write is published');
  assert.deepEqual(await revsIn(probe), [11], 'history is not replayed onto the bus');
});

test('MAXLEN trims the stream while the tail stays exact', async (t) => {
  const { storage, cands, probe } = await rig(t, 1, { maxLen: 50 });
  const [a] = cands as [Candidate];
  await a.bus.start();
  assert.equal(a.bus.isLeader, true, 'leader');
  await commit(storage, 500);

  await waitUntil(async () => (await revsIn(probe)).at(-1) === 500, 'all 500 published', 10_000);
  const revs = await revsIn(probe);
  assert.ok(revs.length < 500, `MAXLEN ~ never trimmed: ${revs.length} entries`);
  assert.deepEqual(revs, range(revs[0] as number, 500), 'what remains is a contiguous ascending tail');
});

test('a shard RESET clears the bus: the new generation publishes, it is not refused as stale (§2)', async (t) => {
  const redis = await RedisProcess.start();
  const probe = await connectRedis(redis.url);
  const open: Redis[] = [];
  t.after(async () => {
    for (const c of [...open, probe]) kill(c);
    await redis.stop();
  });
  const generation = async (storage: MemoryStorage, prefix: string, n: number): Promise<number[]> => {
    const client = await connectRedis(redis.url);
    open.push(client);
    const delivered: number[] = [];
    const bus = new RedisBus(client, storage, { ttlMs: 60_000, onDelta: (d) => delivered.push(d.rev) });
    await bus.start();
    await storage.commitGroup(
      range(1, n).map((i) => ({ writeId: randomUUID(), path: `${prefix}/k${i}`, op: 'put' as const, value: i })),
    );
    await waitUntil(async () => (await revsIn(probe)).length === n, `${prefix} reached the bus`, 10_000);
    await bus.stop();
    kill(client);
    return delivered;
  };

  const gen1 = new MemoryStorage();
  await generation(gen1, 'g1', 5);
  assert.deepEqual(await revsIn(probe), range(1, 5));

  // §2's other generation: a fresh store on the SAME shard — head back to 0, a new epoch. Its revs
  // 1..5 collide with the ids the dead generation already put on the stream, and Redis would refuse
  // every one of them as stale, leaving the bus carrying nothing but a shard that no longer exists.
  const gen2 = new MemoryStorage();
  assert.notEqual(await gen2.epoch(), await gen1.epoch(), 'a reset mints a new generation (§2)');
  await generation(gen2, 'g2', 5);

  assert.deepEqual(await revsIn(probe), range(1, 5), 'the new generation is on the bus, once each');
  const paths = (await probe.xRange(KEYS.stream, '-', '+')).map(
    (e) => (JSON.parse(e.message['d'] as string) as { path: string }).path,
  );
  assert.deepEqual(paths, range(1, 5).map((i) => `g2/k${i}`), 'no dead-generation entry survived');
  assert.equal(await probe.get(KEYS.epoch), String(await gen2.epoch()), 'the stream is stamped with its generation');
});

test('a gateway process told RTDB_REDIS_URL publishes its shard onto the bus', async (t) => {
  const redis = await RedisProcess.start();
  const probe = await connectRedis(redis.url);
  const gw = await GatewayProcess.start({}, 0, undefined, { RTDB_REDIS_URL: redis.url });
  t.after(async () => {
    await gw.stop();
    kill(probe);
    await redis.stop();
  });

  const client = new RtdbClient({ url: gw.url, token: goodToken(), pingIntervalMs: 60_000, autoReconnect: false });
  t.after(() => client.close());
  client.connect();
  await client.ready();
  await client.put('room/alpha', 1);
  await client.put('room/beta', 2);

  await waitUntil(async () => (await revsIn(probe)).length === 2, 'both writes reached the stream');
  const entries = await probe.xRange(KEYS.stream, '-', '+');
  assert.deepEqual(
    entries.map((e) => JSON.parse(e.message['d'] as string) as { rev: number; path: string; value: unknown }),
    [
      { type: 'delta', rev: 1, path: 'room/alpha', op: 'put', value: 1 },
      { type: 'delta', rev: 2, path: 'room/beta', op: 'put', value: 2 },
    ],
  );
});
