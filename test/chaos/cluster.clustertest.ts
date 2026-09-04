import assert from 'node:assert/strict';
import test, { after, type TestContext } from 'node:test';
import type { RtdbClient } from '../../harness/client.ts';
import { startCluster, type Cluster } from '../../harness/cluster.ts';
import { Proxy } from '../../harness/proxy.ts';
import { assertNoLeftovers } from '../../harness/redis.ts';
import { assertConverged, sleep, waitUntil } from '../../harness/scenario.ts';
import type { Ack, ServerFrame } from '../../src/protocol/frames.ts';
import { goodToken } from '../helpers.ts';

/**
 * WORKLOAD §6 Gate D, the half the single-process battery cannot reach: two gateway PROCESSES over
 * one shard, and the three deaths that only exist once there are two — the leader, the follower, and
 * the bus itself. Every one of them happens UNDER LOAD, because a fault on an idle system proves
 * nothing about a fault on a busy one.
 *
 * Every scenario ends the same way: every write acked, every client's wire strictly ascending with
 * no duplicate, every mirror equal to server state.
 */

after(() => assertNoLeftovers());

const TOKEN = goodToken();

/** A writer that never stops. `put()` resolves on the ack, so an outage stalls it rather than
 * failing it — which is the property under test: a fault must cost latency, never a write. */
function load(c: RtdbClient, path: string): { stop: () => Promise<{ count: number; bad: unknown[] }> } {
  let issued = 0;
  let stopping = false;
  const bad: unknown[] = [];
  const done = (async () => {
    while (!stopping) {
      const i = ++issued;
      try {
        const r = await c.put(`${path}/k${i}`, i);
        if (r.type !== 'ack') bad.push(r);
      } catch (e) {
        bad.push(e);
      }
    }
  })();
  return {
    stop: async () => {
      stopping = true;
      await done;
      return { count: issued, bad };
    },
  };
}

/** Every delta rev a client was shown, in arrival order. */
function watch(c: RtdbClient): number[] {
  const revs: number[] = [];
  c.on('frame', (f: ServerFrame) => {
    if (f.type === 'delta') revs.push(f.rev);
    if (f.type === 'batch') for (const inner of f.frames) if (inner.type === 'delta') revs.push(inner.rev);
  });
  return revs;
}

const ascendingUnique = (revs: number[], label: string): void => {
  for (let i = 1; i < revs.length; i++) {
    assert.ok(
      (revs[i] as number) > (revs[i - 1] as number),
      `${label}: rev ${revs[i]} arrived after ${revs[i - 1]} — §3 promises ascending, once each`,
    );
  }
};

async function cluster(t: TestContext): Promise<Cluster> {
  const rig = await startCluster();
  t.after(() => rig.stop());
  return rig;
}

// ---------------------------------------------------------------- C1

test('C1 kill the LEADER under load: the follower takes the shard, nothing is lost', async (t) => {
  const c = await cluster(t);
  const proxy = await Proxy.start(c.a.port);
  t.after(() => proxy.stop());
  const writer = await c.connect(proxy.url, { token: TOKEN }); // on the leader, so it dies with it
  const observer = await c.connect(c.b.url, { token: TOKEN });
  observer.listen('room');
  const seen = watch(observer);
  await waitUntil(() => observer.subscriptions.length === 1, 'observer subscribed');

  const running = load(writer, 'room');
  await waitUntil(() => seen.length >= 20, 'load is flowing across the bus', 20_000);

  await c.a.kill(); // SIGKILL the leader mid-write: no release, the TTL is the only clock
  proxy.retarget(c.b.port); // the NLB moves its clients, which is what production does
  const before = seen.length;
  await waitUntil(() => seen.length > before + 20, 'traffic resumes on the survivor', 30_000);

  const { count, bad } = await running.stop();
  assert.deepEqual(bad, [], 'a gateway death must cost latency, never a write');
  await waitUntil(() => seen.length === count, 'every committed rev reached the observer', 30_000);
  ascendingUnique(seen, 'observer on the survivor');
  assert.deepEqual(seen, Array.from({ length: count }, (_, i) => i + 1), 'contiguous: no gap');
  await waitUntil(async () => (await c.tail()) === count, 'the follower is publishing', 20_000);
  await assertConverged([writer, observer], c.b.url, TOKEN);
});

// ---------------------------------------------------------------- C2

test('C2 kill the FOLLOWER under load: the leader never notices, its clients move and resume', async (t) => {
  const c = await cluster(t);
  const proxy = await Proxy.start(c.b.port);
  t.after(() => proxy.stop());
  const writer = await c.connect(c.a.url, { token: TOKEN }); // on the leader, which survives
  const onLeader = await c.connect(c.a.url, { token: TOKEN });
  const moving = await c.connect(proxy.url, { token: TOKEN }); // on the follower, which dies
  onLeader.listen('room');
  moving.listen('room');
  const seenOnLeader = watch(onLeader);
  await waitUntil(() => onLeader.subscriptions.length === 1 && moving.subscriptions.length === 1, 'subscribed');

  const running = load(writer, 'room');
  await waitUntil(() => moving.value('room/k20') === 20, 'load is flowing to both', 20_000);

  await c.b.kill();
  proxy.retarget(c.a.port);
  await sleep(200); // writes land while the moved client is reconnecting

  const { count, bad } = await running.stop();
  assert.deepEqual(bad, [], 'the follower dying must not touch the leader’s writes');
  await waitUntil(() => moving.value(`room/k${count}`) === count, 'the moved client resumes by lastRev', 30_000);
  await waitUntil(() => seenOnLeader.length === count, 'the untouched client saw everything', 30_000);
  ascendingUnique(seenOnLeader, 'client on the surviving leader');
  assert.deepEqual(seenOnLeader, Array.from({ length: count }, (_, i) => i + 1), 'contiguous: no gap');
  await assertConverged([writer, onLeader, moving], c.a.url, TOKEN);
});

// ---------------------------------------------------------------- C3

test('C3 kill REDIS under load: Postgres is the truth, and the bus catches up', async (t) => {
  const c = await cluster(t);
  const writer = await c.connect(c.b.url, { token: TOKEN });
  const observer = await c.connect(c.a.url, { token: TOKEN }); // the far side of the dead bus
  observer.listen('room');
  const seen = watch(observer);
  await waitUntil(() => observer.subscriptions.length === 1, 'observer subscribed');

  const running = load(writer, 'room');
  await waitUntil(() => seen.length >= 20, 'load is flowing across the bus', 20_000);

  await c.redis.kill(); // the bus is gone; every write from here has nothing to publish it
  const during = (await writer.put('room/during', 'x')) as Ack;
  assert.equal(during.type, 'ack', 'writes commit with no bus at all — the oplog is the truth');
  await sleep(300);
  await c.redis.restart(); // ...and comes back with no history whatsoever

  const { count, bad } = await running.stop();
  assert.deepEqual(bad, [], 'a dead bus must not fail a write');
  await waitUntil(() => observer.value(`room/k${count}`) === count, 'fanout recovers', 40_000);
  ascendingUnique(seen, 'observer across a dead bus');
  assert.equal(observer.value('room/during'), 'x', 'including the write issued with the bus down');

  // Proof that PUBLISHING resumed, not merely the consumer's oplog reconcile: the restarted stream
  // is empty and the leader correctly resumes at the head, so only the next rev can show it.
  const after = (await writer.put('room/after', 'y')) as Ack;
  await waitUntil(async () => (await c.tail()) === after.rev, 'a live publisher is back', 20_000);
  await assertConverged([writer, observer], c.a.url, TOKEN);
});
