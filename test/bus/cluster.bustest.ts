import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import test, { after, type TestContext } from 'node:test';
import { promisify } from 'node:util';
import type { RtdbClient } from '../../harness/client.ts';
import { startCluster, type Cluster } from '../../harness/cluster.ts';
import { Proxy } from '../../harness/proxy.ts';
import { assertNoLeftovers } from '../../harness/redis.ts';
import { assertConverged, waitUntil } from '../../harness/scenario.ts';
import type { ServerFrame } from '../../src/protocol/frames.ts';
import { goodToken } from '../helpers.ts';

after(() => assertNoLeftovers());

const run = promisify(execFile);
const TOKEN = goodToken();
const range = (a: number, b: number): number[] => Array.from({ length: b - a + 1 }, (_, i) => a + i);

/** The rig lives in `harness/cluster.ts`; this adds the per-test teardown and a wire watcher. */
async function cluster(t: TestContext): Promise<Cluster & { watch(c: RtdbClient): number[] }> {
  const rig = await startCluster();
  t.after(() => rig.stop());
  return {
    ...rig,
    watch(c) {
      const revs: number[] = [];
      c.on('frame', (f: ServerFrame) => {
        if (f.type === 'delta') revs.push(f.rev);
        if (f.type === 'batch') for (const inner of f.frames) if (inner.type === 'delta') revs.push(inner.rev);
      });
      return revs;
    },
  };
}

const put = async (c: RtdbClient, from: number, to: number): Promise<void> => {
  for (const i of range(from, to)) await c.put(`room/k${i}`, i);
};

test('a client on A sees B’s writes, and both gateways converge', async (t) => {
  const c = await cluster(t);
  const ca = await c.connect(c.a.url, { token: TOKEN });
  const cb = await c.connect(c.b.url, { token: TOKEN });
  ca.listen('room');
  cb.listen('room');
  await waitUntil(() => ca.subscriptions.length === 1 && cb.subscriptions.length === 1, 'both subscribed');

  await cb.put('room/from_b', 1);
  await waitUntil(() => ca.value('room/from_b') === 1, 'A’s client saw B’s write — across the bus');
  await ca.put('room/from_a', 2);
  await waitUntil(() => cb.value('room/from_a') === 2, 'and back the other way');

  await assertConverged([ca], c.a.url, TOKEN);
  await assertConverged([cb], c.b.url, TOKEN);
});

test('a client moved from A to B resumes by lastRev — catch-up, not a fresh snapshot (§3)', async (t) => {
  const c = await cluster(t);
  const proxy = await Proxy.start(c.a.port);
  t.after(() => proxy.stop());
  const moving = await c.connect(proxy.url, { token: TOKEN });
  const onB = await c.connect(c.b.url, { token: TOKEN });
  moving.listen('room');
  await put(moving, 1, 3);
  await waitUntil(() => moving.value('room/k3') === 3, 'settled on A');

  // Away while the shard moves on, then the load balancer points the same URL at the other gateway.
  proxy.blackhole();
  await waitUntil(() => moving.state !== 'connected', 'the client noticed');
  await put(onB, 4, 5);

  const frames: string[] = [];
  moving.on('frame', (f: ServerFrame) => frames.push(f.type));
  proxy.retarget(c.b.port);
  proxy.restore();

  await waitUntil(() => moving.value('room/k5') === 5, 'B resumed the subscription', 15_000);
  assert.equal(frames.includes('snapshot'), false, 'B served the gap from the oplog, by lastRev');
  assert.deepEqual(frames.filter((f) => f === 'delta').length, 2, 'exactly the two revs it had missed');
  await assertConverged([moving], c.b.url, TOKEN);
});

test('SIGKILL the leader: the follower takes the shard and fanout resumes, in order', async (t) => {
  const c = await cluster(t);
  const proxy = await Proxy.start(c.a.port);
  t.after(() => proxy.stop());
  const onA = await c.connect(proxy.url, { token: TOKEN });
  const onB = await c.connect(c.b.url, { token: TOKEN });
  onA.listen('room');
  onB.listen('room');
  const revsB = c.watch(onB); // B is never disconnected: its wire is the clean ordering evidence
  await put(onB, 1, 10);
  await waitUntil(() => onA.value('room/k10') === 10 && onB.value('room/k10') === 10, 'both caught up');

  await c.a.kill(); // SIGKILL the leader — no release, no handover, the TTL is the only clock
  proxy.retarget(c.b.port); // ...and the NLB moves A's client, which is what happens in production
  await put(onB, 11, 20); // traffic through the leaderless window

  await waitUntil(
    () => onA.value('room/k20') === 20 && onB.value('room/k20') === 20,
    'the follower publishes what the dead leader never did',
    30_000,
  );
  assert.deepEqual(revsB, range(1, 20), 'no gap, no duplicate, no reorder on the surviving gateway');
  // Teeth on the LEADERSHIP, not just on delivery. The clients above converged in under two seconds
  // — BEFORE the lock's TTL could even expire — because the consumer's oplog reconcile fills what
  // the bus never carried. That is the system working, and it is also exactly the false green WP4
  // warned about: only a gateway that actually took the shard puts revs 11..20 on the stream.
  await waitUntil(async () => (await c.tail()) === 20, 'the follower takes the shard', 20_000);
  await assertConverged([onA, onB], c.b.url, TOKEN);
});

test('SIGKILL Redis: writes still ack, and fanout resumes when it comes back', async (t) => {
  const c = await cluster(t);
  const ca = await c.connect(c.a.url, { token: TOKEN });
  const cb = await c.connect(c.b.url, { token: TOKEN });
  ca.listen('room');
  cb.listen('room');
  const revsA = c.watch(ca);
  await put(cb, 1, 5);
  await waitUntil(() => ca.value('room/k5') === 5, 'fanout works before the fault');

  await c.redis.kill();
  // Postgres is the truth; Redis is live state. A write must not fail because the bus is down.
  for (const i of range(6, 15)) {
    const ack = await cb.put(`room/k${i}`, i);
    assert.equal(ack.type, 'ack', `write ${i} must still commit with the bus dead`);
  }
  await c.redis.restart();

  await waitUntil(
    () => ca.value('room/k15') === 15 && cb.value('room/k15') === 15,
    'fanout resumes — replay or clean resync, never silent loss',
    40_000,
  );
  assert.deepEqual(revsA, range(1, 15), 'in order, exactly once, across a dead bus');
  // The restarted Redis is empty and the leader correctly resumes at the head (Gate A Q1), so the
  // proof that PUBLISHING resumed — not merely the consumer's oplog reconcile — is the next write.
  await cb.put('room/k16', 16);
  await waitUntil(async () => (await c.tail()) === 16, 'a live publisher is back on the bus', 20_000);
  await waitUntil(() => ca.value('room/k16') === 16, 'and it reaches the other gateway');
  await assertConverged([ca], c.a.url, TOKEN);
  await assertConverged([cb], c.b.url, TOKEN);
});

test('§10 kick closes every connection a user holds, on EVERY gateway, with 4403', async (t) => {
  const c = await cluster(t);
  const victim = goodToken('u_victim');
  const closes: number[] = [];
  let authFailures = 0;
  const victims = await Promise.all([
    c.connect(c.a.url, { token: victim }),
    c.connect(c.b.url, { token: victim }),
    c.connect(c.b.url, { token: victim }), // two on one gateway: the loop must not stop at the first
  ]);
  for (const v of victims) {
    v.on('close', (e: { code: number }) => closes.push(e.code));
    v.on('authFailure', () => authFailures++);
  }
  const bystander = await c.connect(c.a.url, { token: goodToken('u_other') });
  bystander.listen('room');
  await waitUntil(() => bystander.subscriptions.length === 1, 'the bystander is live');

  const { stdout } = await run(process.execPath, [
    '--import', 'tsx', 'scripts/kick.ts',
    '--url', c.redis.url, '--user', 'u_victim', '--reason', 'ban',
  ]);
  assert.match(stdout, /kicked u_victim on 2 gateway\(s\)/, 'both gateways were listening');

  await waitUntil(() => closes.length === 3, 'all three connections closed', 10_000);
  assert.deepEqual(closes, [4403, 4403, 4403], '§10: close code and nothing else client-visible');
  assert.equal(authFailures, 0, '4403 is not 4401 — the SDK must not surface an auth failure');

  // §6: other close codes follow normal backoff. Revocation is enforced by the RE-AUTH on reconnect
  // (§3), not by keeping the socket shut — verified against the frozen spec, no client change.
  await waitUntil(() => victims.every((v) => v.state === 'connected'), 'they reconnect and re-auth', 10_000);
  assert.equal(bystander.state, 'connected', 'another user was never touched');
  assert.equal(bystander.value('room'), null);
});
