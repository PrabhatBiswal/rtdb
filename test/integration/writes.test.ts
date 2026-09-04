import assert from 'node:assert/strict';
import test from 'node:test';
import type { Ack, CasFail, Err } from '../../src/protocol/frames.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import type { StorageAdapter } from '../../src/storage/adapter.ts';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { collect, harness, rawConnected, sleep, testStorage, waitUntil, wsUrl } from '../helpers.ts';
import { ClientClosedError, RtdbClient } from '../../harness/client.ts';
import { signDevToken } from '../../src/gateway/auth.ts';

const W = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

const failure = async (p: Promise<unknown>): Promise<Err> => {
  try {
    await p;
  } catch (e) {
    return e as Err;
  }
  throw new Error('expected the write to be rejected');
};

test('a put acks with its rev and lands in storage', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect();
  const ack = await c.put('MPK_1010/1474396', { name: 'Ravi', score: 42 });
  assert.equal(ack.type, 'ack');
  assert.equal((ack as Ack).rev, 1);
  assert.deepEqual((await gw.storage.readSnapshot('MPK_1010/1474396')).value, { name: 'Ravi', score: 42 });
});

test('writes arriving together group-commit into one rev range, in arrival order (§4 step 2)', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect();
  const acks = (await Promise.all([c.put('a', 1), c.put('b', 2), c.put('c', 3)])) as Ack[];
  assert.deepEqual(acks.map((a) => a.rev), [1, 2, 3]);
  assert.equal(await gw.storage.head(), 3, 'gap-free (§1)');
});

test('a replayed writeId acks with the ORIGINAL rev and applies once (§4 step 4)', async (t) => {
  const { gw, connect } = await harness(t);
  await connect();
  const ws = await rawConnected(gw.port);
  t.after(() => ws.close());
  const frames = collect(ws);

  const frame = JSON.stringify({ type: 'put', writeId: W(1), path: 'p', value: 'first' });
  ws.send(frame);
  await waitUntil(() => frames.length === 1, 'first ack');
  ws.send(JSON.stringify({ type: 'put', writeId: W(1), path: 'p', value: 'second' }));
  await waitUntil(() => frames.length === 2, 'replay ack');

  assert.deepEqual(frames, [
    { type: 'ack', writeId: W(1), rev: 1 },
    { type: 'ack', writeId: W(1), rev: 1 },
  ]);
  assert.equal(await gw.storage.head(), 1, 'a replay burns no rev');
  assert.equal((await gw.storage.readSnapshot('p')).value, 'first', 'and re-applies nothing');
});

test('a merge with deep keys commits atomically under ONE rev (§4)', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect();
  await c.put('p', { score: 1, tag: 'x', keep: true });
  const ack = (await c.merge('p', { score: 50, 'stats/wins': 3, tag: null })) as Ack;
  assert.equal(ack.rev, 2, 'one merge, one rev, however many keys');
  assert.deepEqual((await gw.storage.readSnapshot('p')).value, { score: 50, keep: true, stats: { wins: 3 } });
});

test('CAS commits on a match and casFails with fresh state on a miss (§4 step 3)', async (t) => {
  const { connect } = await harness(t);
  const c = await connect();
  const first = (await c.put('p/score', 50)) as Ack;

  const ok = (await c.cas('p/score', first.rev, 51)) as Ack;
  assert.deepEqual(ok, { type: 'ack', writeId: ok.writeId, rev: 2 });

  const stale = (await c.cas('p/score', first.rev, 99)) as CasFail;
  assert.deepEqual(stale, {
    type: 'casFail',
    writeId: stale.writeId,
    path: 'p/score',
    value: 51,
    rev: 2,
  });
});

test('two clients racing the same CAS: exactly one ack, one casFail (§4)', async (t) => {
  const { connect } = await harness(t);
  const [a, b] = await Promise.all([connect(), connect()]);
  const base = ((await a.put('p/score', 1)) as Ack).rev;

  const results = await Promise.all([a.cas('p/score', base, 10), b.cas('p/score', base, 20)]);
  const kinds = results.map((r) => r.type).sort();
  assert.deepEqual(kinds, ['ack', 'casFail'], 'a CAS check must be atomic with its rev assignment');
});

test('a CAS never overtakes the writes that arrived before it', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect();
  await c.put('p/score', 1);
  const [put, cas] = await Promise.all([c.put('p/score', 2), c.cas('p/score', 1, 3)]);
  assert.equal((put as Ack).rev, 2, 'the earlier put keeps the earlier rev');
  assert.equal(cas.type, 'casFail', 'and the CAS sees it');
  assert.equal((await gw.storage.readSnapshot('p/score')).value, 2);
});

test('rules are evaluated before the transaction; a denied write never enters a batch (§4 step 1)', async (t) => {
  const { gw, connect } = await harness(t, {
    rules: (ctx) => ctx.op === 'read' || !ctx.path.startsWith('locked'),
  });
  const c = await connect();
  const [denied, allowed] = await Promise.allSettled([c.put('locked/x', 1), c.put('open/y', 2)]);

  assert.equal(denied.status, 'rejected');
  assert.equal((denied.reason as Err).code, 'RULES');
  assert.equal(allowed.status, 'fulfilled', 'one bad write must not roll back its batch-mates');
  assert.equal(await gw.storage.head(), 1);
});

test('a write over MAX_LEAVES_PER_WRITE is TOOBIG (§9)', async (t) => {
  const { gw, connect } = await harness(t, { limits: makeLimits({ MAX_LEAVES_PER_WRITE: 3 }) });
  const c = await connect();
  const err = await failure(c.put('p', { a: 1, b: 2, c: 3, d: 4 }));
  assert.equal(err.code, 'TOOBIG');
  assert.equal(await gw.storage.head(), 0, 'nothing was committed');
  const ok = (await c.put('p', { a: 1, b: 2, c: 3 })) as Ack;
  assert.equal(ok.rev, 1);
});

test('a value key that is not a legal path segment is BADPATH', async (t) => {
  const { connect } = await harness(t);
  const c = await connect();
  assert.equal((await failure(c.put('p', { 'a.b': 1 }))).code, 'BADPATH');
  assert.equal((await failure(c.merge('p', { 'x/y.z': 1 }))).code, 'BADPATH');
});

test('a connection over its write burst gets RATE (§9)', async (t) => {
  const { connect } = await harness(t, {
    limits: makeLimits({ WRITE_RATE_PER_SEC: 0, WRITE_RATE_BURST: 3 }),
  });
  const c = await connect();
  const results = await Promise.allSettled([c.put('a', 1), c.put('b', 2), c.put('c', 3), c.put('d', 4)]);
  assert.deepEqual(results.map((r) => r.status), ['fulfilled', 'fulfilled', 'fulfilled', 'rejected']);
  assert.equal(((results[3] as PromiseRejectedResult).reason as Err).code, 'RATE');
});

test('the view is optimistic before the ack and converged after (§7)', async (t) => {
  const { connect } = await harness(t);
  const c = await connect();
  c.listen('p');
  await sleep(20);

  const inflight = c.put('p/score', 5);
  assert.equal(c.value('p/score'), 5, 'the overlay makes it visible immediately');
  assert.equal(c.mirror.serverValue('p/score'), null, 'serverState is untouched until the echo');

  await inflight;
  await waitUntil(() => c.mirror.serverValue('p/score') === 5, 'server echo');
  assert.equal(c.value('p/score'), 5);
  assert.deepEqual(c.pendingWriteIds, [], 'the ack emptied the pending queue');
});

// ------------------------------------------------------------------ P1 (load test 2026-08-29)

/**
 * A rejection that ARRIVES. Plain `assert.rejects` cannot tell "rejected" from "never settles",
 * which is the exact bug under test — without the guard the promise dangles and the assertion
 * would simply wait out the suite timeout. This makes the missing guard fail fast and by name.
 */
const rejection = async (p: Promise<unknown>, ms = 500): Promise<ClientClosedError> => {
  const never = Symbol('never settled');
  const outcome = await Promise.race([
    p.then(() => 'resolved' as const, (e: unknown) => e),
    sleep(ms).then(() => never),
  ]);
  assert.notEqual(outcome, never, `the write never settled in ${ms}ms — §4 has no such outcome`);
  assert.notEqual(outcome, 'resolved', 'expected the write to be rejected');
  return outcome as ClientClosedError;
};

test('P1 t1: a write issued on a closed client rejects at once, and leaves no trace (§4)', async (t) => {
  const { connect } = await harness(t);
  const c = await connect();
  c.listen('p');
  await sleep(20);
  c.close();
  assert.equal(c.state, 'closed');

  const e = await rejection(c.put('p/x', 1));
  assert.equal(e.name, 'ClientClosedError');
  assert.equal(e.type, 'closed');
  assert.deepEqual(c.pendingWriteIds, [], 'an abandoned write never joins the queue');
  assert.equal(c.value('p/x'), null, 'nor the §7 overlay — the view must not show what will never be sent');
});

test('P1 t2: close() rejects every pending write (§4 is ack-or-err, never silence)', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ limits: makeLimits({ BACKOFF_CAP_MS: 30 }) });
  c.listen('p');
  await sleep(20);

  gw.close(); // nothing can leave the client from here
  await waitUntil(() => c.state !== 'connected', 'the drop is seen');
  const queued = [c.put('p/a', 1), c.put('p/b', 2), c.put('p/c', 3)];
  assert.equal(c.pendingWriteIds.length, 3, 'queued for §6 step 3, as they should be while retrying');

  c.close(); // ...and now there is no step 3 coming
  for (const p of queued) assert.equal((await rejection(p)).name, 'ClientClosedError');
  assert.deepEqual(c.pendingWriteIds, [], 'the queue is empty, not merely unsent');
  assert.equal(c.value('p/a'), null, 'and the overlay went with it (§7)');
});

test('P1 t2b: a 4401 abandons the queue too — §6 v1.2 leaves no reconnect to replay it', async (t) => {
  const { gw } = await harness(t);
  const c = new RtdbClient({ url: wsUrl(gw.port), token: signDevToken({ sub: 'u_1' }, 'wrong-secret') });
  t.after(() => c.close());

  const queued = c.put('p/x', 1); // issued before the socket is even open, as an app may
  c.connect();

  assert.equal((await rejection(queued)).name, 'ClientClosedError');
  assert.equal(c.state, 'closed');
});

// ------------------------------------------------ §5.11: a commit that REJECTED (item 2)
//
// `#serialize` returns `run` while its handlers go on the derived `#chain`, and `submit()` `void`s
// it — so before this, a rejected `commitGroup` left every write in the batch with neither `ack`
// nor `err`. §4 gives a write exactly three outcomes and "never settles" is not among them; it is
// F5 arriving from the server side.
//
// The repair is a retry, not a report. A retry is safe by construction — the batch is ONE
// transaction, and §4 step 4's writeId dedup resolves the one indeterminate window (COMMIT itself
// rejecting) to the original rev. A report is not available: §4's err codes all blame the client,
// and a dishonest one settles the write `Failed`, turning a transient fault into permanent loss.

/** Storage whose `commitGroup` rejects the first `failures` calls. */
const flakyCommits = (
  inner: StorageAdapter,
  failures: number,
): { storage: StorageAdapter; calls: () => number } => {
  let calls = 0;
  const storage = new Proxy(inner, {
    get(target, prop) {
      if (prop === 'commitGroup') {
        return async (writes: Parameters<StorageAdapter['commitGroup']>[0]) => {
          calls += 1;
          if (calls <= failures) throw new Error('timeout exceeded when trying to connect');
          return target.commitGroup(writes);
        };
      }
      const v = Reflect.get(target, prop, target) as unknown;
      return typeof v === 'function' ? v.bind(target) : v;
    },
  }) as StorageAdapter;
  return { storage, calls: () => calls };
};

test('§5.11: a transient commit rejection is retried, and the write acks as if nothing happened', async (t) => {
  const real = testStorage(DEFAULT_LIMITS);
  t.after(() => real.cleanup());
  const { storage, calls } = flakyCommits(real.storage, 1); // rejects once, then commits
  const { connect } = await harness(t, { storage });
  const c = await connect();
  c.listen('room'); // so the assertion below reads real server state, not the optimistic overlay
  await waitUntil(() => c.value('room') !== undefined, 'initial snapshot');

  const ack = await c.put('room/msg', 'hello');
  assert.equal(ack.type, 'ack', '§4: the write settles, and settles as a normal commit');
  assert.ok(calls() >= 2, 'because the rejected commit was actually retried');
  await waitUntil(() => c.value('room/msg') === 'hello', 'the committed value fans out normally');
});

test('§5.11: a doomed batch is bounded and logged, and does not wedge later writes', async (t) => {
  const real = testStorage(DEFAULT_LIMITS);
  t.after(() => real.cleanup());
  // Reject enough to exhaust the first batch's three attempts, then recover.
  const { storage } = flakyCommits(real.storage, 3);
  const lines: string[] = [];
  const { connect } = await harness(t, { storage, log: (l) => lines.push(l) });
  const [a, b] = await Promise.all([connect(), connect()]);

  // Deliberately not awaited: this batch is abandoned, so the SERVER never settles it — that is
  // F5's shape, narrowed to a gateway that by now is also failing its own /healthz. Awaiting it
  // would hang. The catch is required rather than cosmetic: at teardown the client settles its own
  // pending writes with ClientClosedError (P1's guard doing its job), and without a handler that
  // lands after the test as an unhandledRejection.
  a.put('room/doomed', 1).catch(() => undefined);

  await waitUntil(() => lines.some((l) => l.includes('write-abandoned')), 'the give-up is written down');
  const line = JSON.parse(lines.find((l) => l.includes('write-abandoned')) as string) as Record<string, unknown>;
  assert.equal(line['attempts'], 3, 'bounded — head-of-line blocking on a shared chain, so fewer than a listen gets');
  assert.equal(line['writes'], 1);

  // The neighbour is the point, same as item 3: the chain recovers and a later write from a
  // DIFFERENT connection commits normally rather than being stuck behind the abandoned batch.
  b.listen('room');
  await waitUntil(() => b.value('room') !== undefined, 'initial snapshot');
  const ack = await b.put('room/after', 2);
  assert.equal(ack.type, 'ack', 'the pipeline is not wedged by the batch it gave up on');
  await waitUntil(() => b.value('room/after') === 2, 'and it really committed');
});
