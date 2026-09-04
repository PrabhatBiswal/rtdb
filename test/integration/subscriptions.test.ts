import assert from 'node:assert/strict';
import { once } from 'node:events';
import test from 'node:test';
import { INFO_CONNECTED } from '../../harness/client.ts';
import { startGateway } from '../../src/gateway/server.ts';
import type { Ack, Delta, Err, Json } from '../../src/protocol/frames.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import type { StorageAdapter } from '../../src/storage/adapter.ts';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { collect, harness, rawConnected, sleep, testStorage, waitUntil } from '../helpers.ts';

/** §1: a writeId is a UUIDv4 — these are seeded straight into storage, bypassing the wire, and
 * `oplog.write_id` is a real `UUID` column (§8). Same shape as writes.test.ts's `W`. */
const W = (label: string): string => `00000000-0000-4000-8000-${label.padStart(12, '0')}`;

test('two clients see each other’s writes, in rev order', async (t) => {
  const { connect } = await harness(t);
  const [a, b] = await Promise.all([connect(), connect()]);

  const seen: Json[] = [];
  const revs: number[] = [];
  a.on('frame', (f: Delta) => {
    if (f.type === 'delta') revs.push(f.rev);
  });
  a.listen('room', (v) => seen.push(v));
  b.listen('room'); // b mirrors it too, so we can assert both converge
  await waitUntil(() => seen.length > 0, 'initial snapshot');

  await b.put('room/msg1', 'hi');
  await b.put('room/msg2', 'there');
  await b.merge('room', { msg1: 'edited', 'meta/count': 2 });

  await waitUntil(() => a.value('room/meta/count') === 2, 'a converges on b’s writes');
  assert.deepEqual(a.value('room'), { msg1: 'edited', msg2: 'there', meta: { count: 2 } });
  assert.deepEqual(revs, [...revs].sort((x, y) => x - y), 'deltas arrive in ascending rev order (§8)');
  await waitUntil(() => JSON.stringify(b.value('room')) === JSON.stringify(a.value('room')), 'convergence');
  assert.deepEqual(a.value('room'), b.value('room'), 'both mirrors agree');
});

test('ancestor and descendant subscriptions on one connection both see the change (§3)', async (t) => {
  const { connect } = await harness(t);
  const [a, b] = await Promise.all([connect(), connect()]);

  const parent: Json[] = [];
  const child: Json[] = [];
  a.listen('p', (v) => parent.push(v));
  a.listen('p/child', (v) => child.push(v));
  await waitUntil(() => parent.length > 0 && child.length > 0, 'both snapshots');

  await b.put('p/child/deep', 1); // below both subs
  await waitUntil(() => a.value('p/child/deep') === 1, 'descendant write');

  await b.put('p', { child: { deep: 2 }, sibling: 3 }); // above both subs
  await waitUntil(() => a.value('p/child/deep') === 2, 'ancestor write');
  assert.deepEqual(a.value('p'), { child: { deep: 2 }, sibling: 3 });
  assert.deepEqual(a.value('p/child'), { deep: 2 }, 'extracted at the sub’s relative path (§3)');

  await b.put('p', { sibling: 4 }); // the child disappears from the ancestor value
  await waitUntil(() => a.value('p/child') === null, 'extraction yields null when absent');
});

test('a re-listen with a retained lastRev is served from the oplog, with no snapshot (§3)', async (t) => {
  const { gw } = await harness(t);
  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const frames = collect(ws);

  await gw.storage.commitGroup([
    { writeId: W('1'), path: 'p/a', op: 'put', value: 1 },
    { writeId: W('2'), path: 'other', op: 'put', value: 2 },
    { writeId: W('3'), path: 'p/b', op: 'put', value: 3 },
  ]);
  ws.send(JSON.stringify({ type: 'listen', subId: 7, path: 'p', lastRev: 1 }));
  await waitUntil(() => frames.length > 0, 'catch-up');
  await sleep(50);

  assert.deepEqual(frames.map((f) => [f['type'], f['rev']]), [['delta', 3]], 'only what is relevant and newer');
});

test('too far behind falls back to a fresh snapshot (§3)', async (t) => {
  const { gw } = await harness(t, { limits: makeLimits({ CATCHUP_LIMIT: 2 }) });
  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const frames = collect(ws);

  await gw.storage.commitGroup(
    ['a', 'b', 'c', 'd'].map((k) => ({ writeId: W(k), path: `p/${k}`, op: 'put' as const, value: k })),
  );
  ws.send(JSON.stringify({ type: 'listen', subId: 7, path: 'p', lastRev: 0 }));
  await waitUntil(() => frames.length > 0, 'snapshot');
  await sleep(50);

  assert.deepEqual(frames, [
    { type: 'snapshot', subId: 7, path: 'p', value: { a: 'a', b: 'b', c: 'c', d: 'd' }, rev: 4 },
  ]);
});

test('a snapshot over SNAPSHOT_MAX is a sub-scoped TOOBIG and creates no subscription (§3)', async (t) => {
  const { gw, connect } = await harness(t, { limits: makeLimits({ SNAPSHOT_MAX: 300 }) });
  const c = await connect();
  await c.put('big', { blob: 'x'.repeat(600) });

  const errs: Err[] = [];
  c.on('subError', (e: Err) => errs.push(e));
  c.listen('big');
  await waitUntil(() => errs.length > 0, 'TOOBIG');
  assert.deepEqual({ ...errs[0], msg: undefined }, { type: 'err', subId: 1, code: 'TOOBIG', msg: undefined });
  assert.equal(await gw.storage.head(), 1);
});

test('a denied read is a sub-scoped err and no subscription (§3)', async (t) => {
  const { connect } = await harness(t, { rules: (ctx) => ctx.op !== 'read' || ctx.path !== 'secret' });
  const [a, b] = await Promise.all([connect(), connect()]);

  const errs: Err[] = [];
  a.on('subError', (e: Err) => errs.push(e));
  a.listen('secret');
  a.listen('open');
  await waitUntil(() => errs.length > 0, 'RULES err');
  assert.equal(errs[0]?.code, 'RULES');

  await b.put('secret/x', 1);
  await b.put('open/x', 2);
  await waitUntil(() => a.value('open/x') === 2, 'the allowed sub still works');
  assert.equal(a.value('secret/x'), null, 'and the denied one delivers nothing');
});

test('unlisten stops delivery (§3)', async (t) => {
  const { connect } = await harness(t);
  const [a, b] = await Promise.all([connect(), connect()]);
  const values: Json[] = [];
  const off = a.listen('p', (v) => values.push(v));
  await waitUntil(() => values.length > 0, 'snapshot');

  await b.put('p/x', 1);
  await waitUntil(() => a.value('p/x') === 1, 'first write');
  off();
  await b.put('p/x', 2);
  await sleep(60);
  assert.equal(a.value('p/x'), 1, 'the mirror stopped advancing for that path');
});

test('deltas that arrive together ship as one batch frame (§3)', async (t) => {
  const { gw, connect } = await harness(t);
  const writer = await connect();
  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const raw: Record<string, unknown>[] = [];
  ws.addEventListener('message', (ev) => raw.push(JSON.parse(String(ev.data)) as Record<string, unknown>));

  ws.send(JSON.stringify({ type: 'listen', subId: 1, path: 'p' }));
  await waitUntil(() => raw.length > 0, 'snapshot');
  await sleep(60); // let the batch window close so the connection is idle

  await Promise.all([writer.put('p/a', 1), writer.put('p/b', 2), writer.put('p/c', 3)]);
  await waitUntil(
    () => raw.flatMap((f) => (f['type'] === 'batch' ? (f['frames'] as unknown[]) : [f])).length >= 4,
    'three deltas',
  );
  assert.ok(raw.some((f) => f['type'] === 'batch'), 'a burst coalesces instead of sending three frames');
});

test('.info/connected is served locally and never reaches the wire (§7)', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ autoReconnect: false });

  const states: unknown[] = [];
  c.listen(INFO_CONNECTED, (v) => states.push(v));
  assert.deepEqual(states, [true], 'it fires immediately with the current state');

  const errs: Err[] = [];
  c.on('subError', (e: Err) => errs.push(e));
  gw.close();
  await once(c, 'state');
  await waitUntil(() => states.length === 2, 'disconnect');
  assert.deepEqual(states, [true, false]);
  assert.deepEqual(errs, [], '".info/connected" is not a legal wire path and was never sent');
});

test('a reconnect re-listens with lastRev and replays pending writes in order (§6)', async (t) => {
  const { gw, connect } = await harness(t);
  const writer = await connect();
  const c = await connect({ limits: makeLimits({ BACKOFF_CAP_MS: 30 }) });

  c.listen('room');
  await waitUntil(() => c.value('room') === null || c.value('room') !== undefined, 'snapshot');
  await writer.put('room/a', 1);
  await waitUntil(() => c.value('room/a') === 1, 'first delta');

  const port = gw.port;
  gw.close();
  await once(c, 'state'); // -> waiting

  const gw2 = await startGateway({ port, storage: gw.storage });
  t.after(() => gw2.close());
  await c.ready();

  await writer.connect(), await writer.ready();
  await writer.put('room/b', 2);
  await waitUntil(() => c.value('room/b') === 2, 'delivery resumes after the reconnect');
  assert.deepEqual(c.value('room'), { a: 1, b: 2 });
});

test('a write issued while disconnected is queued and replayed on reconnect (§6 step 3)', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ limits: makeLimits({ BACKOFF_CAP_MS: 30 }) });
  c.listen('p');
  await sleep(20);

  const port = gw.port;
  gw.close();
  await once(c, 'state');

  const offline = c.put('p/x', 'written while down');
  assert.equal(c.pendingWriteIds.length, 1);
  assert.equal(c.value('p/x'), 'written while down', 'the overlay shows it immediately (§7)');

  const gw2 = await startGateway({ port, storage: gw.storage });
  t.after(() => gw2.close());
  const ack = (await offline) as Ack;
  assert.equal(ack.type, 'ack');
  await waitUntil(() => c.mirror.serverValue('p/x') === 'written while down', 'converged');
  assert.deepEqual(c.pendingWriteIds, []);
});

// ------------------------------------------------- §5.11: a listen whose STORAGE read fails
//
// Before this, `server.ts` caught a rejected `listen` with `console.error` and nothing else: the
// client got no snapshot, no err, and an OPEN socket — on our own SDK, `addValueEventListener` that
// never fires, no timeout, no `onCancelled`. A permanent silent hang, and worse than the hello case
// because there was not even a close to act on.
//
// The repair is §3's `resync`, whose own definition in PROTOCOL.md names "internal error" among its
// reasons, plus a SPACED and BOUNDED retry. Not a sub-scoped `err` — those are spec'd to terminate
// the subscription (`RULES`/`BADPATH`/`TOOBIG` only), permanent for what is usually a failover. Not
// a connection close either — that re-listens every sub on the connection through the same starved
// pool, 3-4 acquisitions each.

/**
 * The real storage for this run (memory or pg — `testStorage` honours RTDB_STORAGE), with
 * `readSnapshot` rigged to reject the first `failures` calls for one path.
 *
 * A Proxy rather than `Object.create(inner)`: the prototype trick leaves `this` pointing at the
 * wrapper, and MemoryStorage reads private `#listeners` in `onCommit`, which then throws
 * "Cannot read private member ... from an object whose class did not declare it". Every forwarded
 * method is bound to the real instance.
 */
const flakyReads = (
  inner: StorageAdapter,
  path: string,
  failures: number,
): { storage: StorageAdapter; calls: () => number } => {
  let calls = 0;
  const storage = new Proxy(inner, {
    get(target, prop) {
      if (prop === 'readSnapshot') {
        return async (p: string) => {
          if (p === path) {
            calls += 1;
            if (calls <= failures) throw new Error('timeout exceeded when trying to connect');
          }
          return target.readSnapshot(p);
        };
      }
      const v = Reflect.get(target, prop, target) as unknown;
      return typeof v === 'function' ? v.bind(target) : v;
    },
  }) as StorageAdapter;
  return { storage, calls: () => calls };
};

test('§5.11: a transient read failure sends resync and repairs itself, silently and correctly', async (t) => {
  const real = testStorage(DEFAULT_LIMITS);
  t.after(() => real.cleanup());
  const { storage, calls } = flakyReads(real.storage, 'flaky', 1); // fails once, then works
  const { gw } = await harness(t, { storage });

  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const frames = collect(ws);
  ws.send(JSON.stringify({ type: 'listen', subId: 1, path: 'flaky' }));

  await waitUntil(() => frames.some((f) => f['type'] === 'snapshot'), 'the repaired snapshot arrives');
  assert.ok(
    frames.some((f) => f['type'] === 'resync' && f['subId'] === 1),
    'the client was TOLD — §3 resync, not silence',
  );
  const snap = frames.find((f) => f['type'] === 'snapshot');
  assert.equal(snap?.['subId'], 1, 'and the promised fresh snapshot followed it');
  assert.ok(calls() >= 2, 'which means the read was actually retried');
});

test('§5.11: a failing listen does not disturb the other subscription on that connection', async (t) => {
  const real = testStorage(DEFAULT_LIMITS);
  t.after(() => real.cleanup());
  const { storage } = flakyReads(real.storage, 'flaky', 99); // never recovers
  const lines: string[] = [];
  const { gw } = await harness(t, { storage, log: (l) => lines.push(l) });

  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const frames = collect(ws);
  ws.send(JSON.stringify({ type: 'listen', subId: 1, path: 'flaky' }));
  ws.send(JSON.stringify({ type: 'listen', subId: 2, path: 'healthy' }));

  // The neighbour is the point: one sub's storage failure must not cost the other its snapshot,
  // and must not close the connection out from under it.
  await waitUntil(
    () => frames.some((f) => f['type'] === 'snapshot' && f['subId'] === 2),
    'the healthy subscription is served regardless',
  );
  assert.ok(frames.some((f) => f['type'] === 'resync' && f['subId'] === 1), 'the doomed sub was told');
  assert.equal(ws.readyState, WebSocket.OPEN, 'and the connection stayed up');

  // Abandonment is bounded, and written down — a sub given up on without a line is the 2026-08-29
  // load test all over again at subscription granularity.
  await waitUntil(() => lines.some((l) => l.includes('listen-abandoned')), 'the abandonment is logged', 15_000);
  const line = JSON.parse(lines.find((l) => l.includes('listen-abandoned')) as string) as Record<string, unknown>;
  assert.equal(line['subId'], 1);
  assert.equal(line['path'], 'flaky');
  assert.equal(line['attempts'], 8, 'bounded, not retried forever into a starved pool');
  assert.ok(typeof line['connId'] === 'string' && line['connId'].length > 0, 'and it names the connection');

  // Still serving after the give-up: a new subscription on the same connection still works.
  ws.send(JSON.stringify({ type: 'listen', subId: 3, path: 'healthy/other' }));
  await waitUntil(
    () => frames.some((f) => f['type'] === 'snapshot' && f['subId'] === 3),
    'the connection is still usable after a subscription was abandoned',
  );
});
