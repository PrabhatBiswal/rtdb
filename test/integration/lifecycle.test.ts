import assert from 'node:assert/strict';
import { once } from 'node:events';
import test from 'node:test';
import { RtdbClient } from '../../harness/client.ts';
import { DevHs256Validator, signDevToken } from '../../src/gateway/auth.ts';
import { startGateway } from '../../src/gateway/server.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import {
  goodToken,
  harness,
  nextClose,
  nextFrame,
  openRaw,
  startDeafServer,
  sleep,
  waitForFrame,
  waitUntil,
  wsUrl,
} from '../helpers.ts';

interface LifecycleLine {
  ts: string;
  ev: string;
  connId: string;
  subId?: number;
  path?: string | null;
  code?: number;
  ms?: number;
  subs?: number;
}

const SECRET = 'test-secret';
const auth = new DevHs256Validator(SECRET);
const token = (userId = 'u_test'): string =>
  signDevToken({ sub: userId, exp: Math.floor(Date.now() / 1000) + 3600 }, SECRET);

test('happy connect: hello -> helloAck with head rev, region and session (§2)', async (t) => {
  const gw = await startGateway({ auth, region: 'ap-south-1', head: () => 184223 });
  t.after(() => gw.close());

  const client = new RtdbClient({ url: wsUrl(gw.port), token: token(), sdk: 'harness/1.0.0' });
  t.after(() => client.close());
  client.connect();

  const ack = await client.ready();
  assert.equal(ack.type, 'helloAck');
  assert.equal(ack.rev, 184223);
  assert.equal(ack.region, 'ap-south-1');
  assert.match(ack.session, /^s_[0-9a-f]{8}$/);
  assert.ok(Number.isInteger(ack.epoch) && ack.epoch >= 1, `helloAck carries the shard epoch (§2 v1.5), got ${ack.epoch}`);
  assert.equal(client.state, 'connected');
  assert.equal(client.session, ack.session);
});

test('bad token: err AUTH then close 4401 (§2)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  const frame = nextFrame<{ type: string; code: string }>(ws);
  const closed = nextClose(ws);
  ws.send(JSON.stringify({ type: 'hello', proto: 1, token: signDevToken({ sub: 'u_1' }, 'wrong-secret') }));

  assert.deepEqual({ ...(await frame), msg: undefined }, { type: 'err', code: 'AUTH', msg: undefined });
  assert.equal((await closed).code, 4401);
});

test('expired token is rejected (§2 validates at connect time)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  const frame = nextFrame<{ code: string; msg: string }>(ws);
  ws.send(
    JSON.stringify({
      type: 'hello',
      proto: 1,
      token: signDevToken({ sub: 'u_1', exp: Math.floor(Date.now() / 1000) - 1 }, SECRET),
    }),
  );
  const err = await frame;
  assert.equal(err.code, 'AUTH');
  assert.match(err.msg, /expired/);
  assert.equal((await nextClose(ws)).code, 4401);
});

test('any frame before hello closes 4400 (§2)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  for (const first of [
    { type: 'ping', t: 1 },
    { type: 'put', writeId: '0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6', path: 'a', value: 1 },
    { type: 'somethingUnknown' },
  ]) {
    const ws = await openRaw(gw.port);
    const closed = nextClose(ws);
    ws.send(JSON.stringify(first));
    assert.equal((await closed).code, 4400, `first frame ${first.type}`);
  }
});

test('garbage before hello also closes 4400', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  const closed = nextClose(ws);
  ws.send('not json at all');
  assert.equal((await closed).code, 4400);
});

test('unsupported hello.proto is rejected', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  const frame = nextFrame<{ code: string }>(ws);
  const closed = nextClose(ws);
  ws.send(JSON.stringify({ type: 'hello', proto: 2, token: token() }));
  assert.equal((await frame).code, 'BADFRAME');
  assert.equal((await closed).code, 4400);
});

test('ping is answered with pong echoing t verbatim (§5)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token() }));
  await nextFrame(ws); // helloAck
  const pong = nextFrame<{ type: string; t: number }>(ws);
  ws.send(JSON.stringify({ type: 'ping', t: 1756280000000 }));
  assert.deepEqual(await pong, { type: 'pong', t: 1756280000000 });
});

test('after hello: bad frames get a scoped err, unknown frames are ignored, socket stays open', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const client = new RtdbClient({ url: wsUrl(gw.port), token: token(), pingIntervalMs: 60_000 });
  t.after(() => client.close());
  client.connect();
  await client.ready();

  const frames: Record<string, unknown>[] = [];
  client.on('frame', (f: Record<string, unknown>) => frames.push(f));

  // BADPATH, scoped to the subId (§3)
  client.send({ type: 'listen', subId: 7, path: 'a//b' } as never);
  // unknown frame type -> ignored entirely (§Transport)
  client.send({ type: 'reauth', token: 'x' } as never);
  // sent last, so its pong is the barrier: everything sent before it has been answered by now
  client.send({ type: 'ping', t: 42 });
  await waitForFrame(client, (f) => f['type'] === 'pong' && f['t'] === 42);

  const errs = frames.filter((f) => f['type'] === 'err');
  assert.deepEqual(errs, [{ type: 'err', code: 'BADPATH', msg: 'empty path segment', subId: 7 }]);
  assert.equal(client.state, 'connected', 'a bad frame must not close the connection');
});

test('a frame over the 1 MiB limit gets TOOBIG, not a dropped connection (§9)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  ws.send(JSON.stringify({ type: 'hello', proto: 1, token: token() }));
  await nextFrame(ws); // helloAck

  const err = nextFrame<{ code: string }>(ws);
  ws.send(
    JSON.stringify({
      type: 'put',
      writeId: '0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6',
      path: 'a',
      value: 'x'.repeat(1024 * 1024),
    }),
  );
  assert.equal((await err).code, 'TOOBIG');
  assert.equal(ws.readyState, WebSocket.OPEN);
});

test('no pong within the timeout drops the connection and the FSM backs off (§5, §6)', async (t) => {
  const deaf = await startDeafServer();
  t.after(() => deaf.close());

  const client = new RtdbClient({
    url: wsUrl(deaf.port),
    token: goodToken(),
    pingIntervalMs: 10_000,
    pongTimeoutMs: 50,
    limits: makeLimits({ BACKOFF_CAP_MS: 40, BACKOFF_RESET_MS: 30_000 }),
  });
  t.after(() => client.close());

  const states: string[] = [];
  client.on('state', (s: string) => states.push(s));
  client.connect();
  await client.ready();

  await once(client, 'pongTimeout');
  await once(client, 'retry');
  assert.deepEqual(states.slice(0, 3), ['connecting', 'connected', 'waiting']);
  // and it really does come back up
  await client.ready();
  assert.equal(client.state, 'connected');
});

test('the client reconnects after the server drops it, without being told to (§6)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const client = new RtdbClient({
    url: wsUrl(gw.port),
    token: token(),
    pingIntervalMs: 60_000,
    limits: makeLimits({ BACKOFF_CAP_MS: 40 }),
  });
  t.after(() => client.close());
  client.connect();
  const first = await client.ready();

  const reconnected = once(client, 'helloAck');
  gw.close(); // ends every live connection
  const gw2 = await startGateway({ auth, port: gw.port });
  t.after(() => gw2.close());

  const [second] = (await reconnected) as [{ session: string }];
  assert.notEqual(second.session, first.session, 'a reconnect is a new session');
  assert.equal(client.state, 'connected');
});

test('autoReconnect:false stops the FSM at closed', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const client = new RtdbClient({ url: wsUrl(gw.port), token: token(), autoReconnect: false });
  t.after(() => client.close());
  client.connect();
  await client.ready();
  gw.close();
  await once(client, 'state'); // -> closed
  assert.equal(client.state, 'closed');
});

test('gateway.close() is idempotent (a double close used to segfault the process)', async () => {
  const gw = await startGateway({ auth });
  const client = new RtdbClient({ url: wsUrl(gw.port), token: token(), autoReconnect: false });
  client.connect();
  await client.ready();
  gw.close();
  gw.close();
  gw.close();
  client.close();
});

test('a 4401 close stops the FSM and surfaces authFailure — no retry with the same token (§6 v1.2)', async (t) => {
  const gw = await startGateway({ auth });
  t.after(() => gw.close());

  const client = new RtdbClient({
    url: wsUrl(gw.port),
    token: signDevToken({ sub: 'u_1' }, 'wrong-secret'),
    limits: makeLimits({ BACKOFF_CAP_MS: 20 }),
  });
  t.after(() => client.close());

  let retries = 0;
  client.on('retry', () => retries++);
  client.connect();

  const [failure] = (await once(client, 'authFailure')) as [{ code: number }];
  assert.equal(failure.code, 4401);
  assert.equal(client.state, 'closed');

  await sleep(150); // long enough for several capped-at-20ms backoffs, if any were scheduled
  assert.equal(retries, 0, 'a dead token must not be retried');

  // ...but a fresh token gets back in.
  client.connect(token());
  const ack = await client.ready();
  assert.equal(ack.type, 'helloAck');
});

test('a second hello while auth is in flight does not double-ack', async (t) => {
  const slow = {
    validate: async (tok: string) => {
      await sleep(30);
      return auth.validate(tok);
    },
  };
  const gw = await startGateway({ auth: slow });
  t.after(() => gw.close());

  const ws = await openRaw(gw.port);
  t.after(() => ws.close());
  const frames: Record<string, unknown>[] = [];
  ws.addEventListener('message', (ev) => frames.push(JSON.parse(String(ev.data))));

  const hello = JSON.stringify({ type: 'hello', proto: 1, token: token() });
  ws.send(hello);
  ws.send(hello);
  ws.send(hello);

  await sleep(200);
  assert.deepEqual(
    frames.filter((f) => f['type'] === 'helloAck').length,
    1,
    'exactly one helloAck, whatever the client does',
  );
  assert.equal(ws.readyState, WebSocket.OPEN);
});

// --------------------------------------------- hello-path crash containment (WORKLOAD §5.10)
//
// The point of every tooth below is the SECOND connection. "The failing socket got closed" is the
// easy half and it would pass just as well against a process that died and was restarted by
// `restart: unless-stopped`. "A DIFFERENT connection was still alive and still committing writes
// afterwards" is the half that distinguishes containment from a crash loop.
//
// REMOVAL SHAPE — measured, and it is NOT what you would guess. Take either `.catch()` out of
// `server.ts` and these do not fail with an assertion. The run ends `fail 0` with a NON-ZERO exit,
// `cancelled 1`, a truncated test count, a 20s `testTimeoutFailure`, and NO rejection trace printed
// anywhere. The cause is mundane: with no `failHello` the doomed socket is never closed, so the
// `nextClose()` await simply never resolves and the test hangs to its timeout.
//
// Note what this red does NOT prove. node:test installs its own unhandled-rejection handling at run
// time, so the production default — Node 22 terminating the process — is not in force under the
// runner. These teeth prove containment (that socket closes, others keep working); the process
// SURVIVAL half is covered separately, out of process, in test/unit/unhandled-rejection.test.ts.

test('§5.10: an auth-backend rejection kills that socket only, and the gateway keeps serving', async (t) => {
  const poison = token('u_poison');
  const lines: string[] = [];
  const flaky = {
    validate: (tok: string) =>
      tok === poison ? Promise.reject(new Error('auth backend down')) : auth.validate(tok),
  };
  const gw = await startGateway({ auth: flaky, log: (line) => lines.push(line) });
  t.after(() => gw.close());

  const doomed = await openRaw(gw.port);
  doomed.send(JSON.stringify({ type: 'hello', proto: 1, token: poison }));
  const closed = await nextClose(doomed);
  assert.equal(closed.code, 1011, 'RFC 6455 internal error — our fault, and retryable');

  // The whole tooth: a different connection, on the SAME process, still works end to end.
  const survivor = new RtdbClient({ url: wsUrl(gw.port), token: token('u_survivor') });
  t.after(() => survivor.close());
  survivor.connect();
  await survivor.ready();
  const ack = await survivor.put('room/after', 'still here');
  assert.equal(ack.type, 'ack', 'the survivor commits writes, not merely connects');

  const failed = lines.map((l) => JSON.parse(l) as { ev: string; where?: string });
  assert.equal(
    failed.filter((l) => l.ev === 'hello-failed' && l.where === 'auth').length,
    1,
    'and the incident is written down, named by which await failed',
  );
});

test('§5.10: a storage rejection kills that socket only, and the gateway keeps serving', async (t) => {
  let calls = 0;
  // Rejects the FIRST hello only — a gateway-wide head() that always rejects could never show a
  // second connection succeeding, which is the assertion that matters.
  const head = (): number | Promise<number> => {
    calls += 1;
    return calls === 1 ? Promise.reject(new Error('timeout exceeded when trying to connect')) : 184223;
  };
  const gw = await startGateway({ auth, head });
  t.after(() => gw.close());

  const doomed = await openRaw(gw.port);
  doomed.send(JSON.stringify({ type: 'hello', proto: 1, token: token() }));
  assert.equal((await nextClose(doomed)).code, 1011);

  const survivor = new RtdbClient({ url: wsUrl(gw.port), token: token('u_survivor') });
  t.after(() => survivor.close());
  survivor.connect();
  assert.equal((await survivor.ready()).rev, 184223, 'the next hello resolves normally');
  assert.equal((await survivor.put('room/after', 1)).type, 'ack');
});

test('§5.10: repeated hello rejections do not accumulate into process death', async (t) => {
  // Process survival stated as its own property: five independent rejections, each of which was a
  // process kill before this package, and the gateway is still answering afterwards.
  const gw = await startGateway({ auth, head: () => Promise.reject(new Error('pool exhausted')) });
  t.after(() => gw.close());

  for (let i = 0; i < 5; i += 1) {
    const doomed = await openRaw(gw.port);
    doomed.send(JSON.stringify({ type: 'hello', proto: 1, token: token(`u_${i}`) }));
    assert.equal((await nextClose(doomed)).code, 1011, `rejection ${i + 1} contained`);
  }

  // Still up, still upgrading sockets, still speaking §2 — on the same process that just absorbed
  // five rejections that each used to be fatal.
  const alive = await openRaw(gw.port);
  t.after(() => alive.close());
  assert.equal(alive.readyState, WebSocket.OPEN);
});

// ------------------------------------------------------------------ U3 (load test 2026-08-29)

test('U3: one structured line per connection-lifecycle event, and none per delta', async (t) => {
  const lines: string[] = [];
  const { connect } = await harness(t, { log: (line) => lines.push(line) });
  const c = await connect();
  const events = (): LifecycleLine[] => lines.map((l) => JSON.parse(l) as LifecycleLine);

  const stopQ = c.listen('q');
  c.listen('p');
  await waitUntil(() => events().filter((e) => e.ev === 'subscribe').length === 2, 'both subscribes');

  // The load the gateway actually carries. None of it is a lifecycle event, and that is the
  // property that makes always-on logging affordable at 100k deltas/s (§2: bounded volume).
  for (let i = 0; i < 25; i++) await c.put(`p/${i}`, i);
  await sleep(30);

  stopQ();
  await waitUntil(() => events().some((e) => e.ev === 'unsubscribe'), 'the unsubscribe line');
  c.close();
  await waitUntil(() => events().some((e) => e.ev === 'close'), 'the close line');

  const seen = events();
  assert.deepEqual(
    seen.map((e) => e.ev),
    ['open', 'subscribe', 'subscribe', 'unsubscribe', 'close'],
    '25 writes and their deltas wrote nothing; only the lifecycle did',
  );
  assert.equal(new Set(seen.map((e) => e.connId)).size, 1, 'every line names the same socket');

  const [open, subQ, subP, unsub, close] = seen as [LifecycleLine, LifecycleLine, LifecycleLine, LifecycleLine, LifecycleLine];
  assert.deepEqual(Object.keys(open), ['ts', 'ev', 'connId']);
  assert.deepEqual([subQ.path, subP.path], ['q', 'p']);
  assert.equal(unsub.path, 'q', 'the path is read before the sub is dropped, or it cannot be named');
  assert.equal(unsub.subId, subQ.subId);
  assert.equal(typeof close.code, 'number');
  assert.ok((close.ms ?? -1) >= 0, 'a close line carries how long the socket held');
  assert.equal(close.subs, 1, 'and how many subscriptions died with it — p, since q was dropped');
});
