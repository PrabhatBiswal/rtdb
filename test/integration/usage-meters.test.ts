import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { DEFAULT_LIMITS, makeLimits } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { ConnectionSink } from '../../src/fanout/subscriptions.ts';
import { SubscriptionRegistry } from '../../src/fanout/subscriptions.ts';
import { startGateway } from '../../src/gateway/server.ts';
import { RtdbClient } from '../../harness/client.ts';
import { waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.24 Gate A — the three meters the usage panel is built on, before any panel exists.
 *
 * The one that matters here is Downloads. `rtdb_bytes_out_total` had ONE caller, so every frame
 * that is not a subscription's — every `ack` above all — left the socket unmetered, and a batched
 * send never counted its own envelope. Both are asserted, and the ack one is the tooth that was RED
 * on the commit before this.
 */
const { register } = await import('prom-client');

const wire = async (db: string): Promise<number> => {
  const m = new RegExp(`^rtdb_wire_bytes_out_total\\{db="${db}"\\} (\\S+)$`, 'm').exec(await register.metrics());
  return m ? Number(m[1]) : 0;
};

after(() => M.resetSources());

test('an ACK moves the byte meter — the frames that never had one', async (t) => {
  // Before §5.24 this was zero: acks, casFails, errs, helloAck and pong all reached `ws.send`
  // through `server.ts`'s own `send()`, which no counter ever saw. On a write-heavy app that is
  // not an edge — the acks ARE the traffic.
  M.resetSources();
  const store = new MemoryStorage(DEFAULT_LIMITS);
  const gw = await startGateway({ storage: store, db: 'shopdb' });
  t.after(() => gw.close());

  const c = new RtdbClient({
    url: wsUrl(gw.port),
    token: signDevToken({ sub: 'u1', exp: Math.floor(Date.now() / 1000) + 3600 }),
    pingIntervalMs: 600_000,
  });
  t.after(() => c.close());
  c.connect();
  await c.ready();

  // helloAck is counted on the DATABASE, because `server.ts` sets the tenant before it answers —
  // by the time that frame exists the connection is on a database. `_none` is for the frames that
  // leave BEFORE any of that: a raw socket refused at the door.
  const noneBefore = await wire('_none');
  const raw = new WebSocket(wsUrl(gw.port));
  await new Promise((r) => raw.addEventListener('open', r, { once: true }));
  const errFrame = new Promise((r) => raw.addEventListener('message', (e) => r(String(e.data)), { once: true }));
  raw.send(JSON.stringify({ type: 'hello', proto: 2, token: 'anything' }));
  assert.match(String(await errFrame), /unsupported proto/, 'refused before it had a database');
  raw.close();
  assert.ok(await wire('_none') > noneBefore, 'and that refusal is metered, on no database');

  const before = await wire('shopdb');
  const acks = 20;
  for (let i = 0; i < acks; i++) assert.equal((await c.put(`shop/item/${i}`, { n: i })).type, 'ack');
  const after_ = await wire('shopdb');

  // An ack is ~60 bytes of JSON plus a 2-byte WebSocket header; 20 of them cannot be zero, and the
  // client never listened, so acks are the ONLY thing that could have moved this.
  assert.ok(after_ - before >= acks * 20, `20 acks moved the meter by ${after_ - before} bytes`);
});

test('a BATCHED send counts the envelope it actually wrote, not the frames inside it', async (t) => {
  // §3 batches deltas only when the send queue is non-empty, so a real socket on localhost never
  // reaches it — measured in §5.22, 56,160 deltas produced 56,160 separate frames. A stub whose
  // `bufferedAmount` is non-zero is the only way to hold the batching path still, and it is the
  // path where `rtdb_bytes_out_total` undercounts by the whole `{"type":"batch","frames":[…]}`
  // envelope. The wire meter is on the transport, so it counts what was WRITTEN.
  const written: string[] = [];
  const transport = {
    send: (text: string): boolean => {
      written.push(text);
      M.countWireBytes('batchdb', text);
      return true;
    },
    bufferedAmount: (): number => 1, // never drained: this is what engages §3's window
    closed: (): boolean => false,
  };
  const store = new MemoryStorage(DEFAULT_LIMITS);
  const registry = new SubscriptionRegistry();
  const limits = makeLimits({ DELTA_BATCH_MS: 5 });
  const sink = new ConnectionSink(transport, registry, store, limits);
  t.after(() => sink.close());

  const before = await wire('batchdb');
  // No `lastRev`: §3 serves a fresh SNAPSHOT. With `lastRev: 0` against an empty store this is a
  // catch-up of nothing and no frame is sent at all — which is correct, and would have made the
  // next assertion pass for the wrong reason.
  await sink.listen(1, 'batch/room');
  const afterSnapshot = await wire('batchdb');
  assert.ok(afterSnapshot > before, 'the snapshot itself is metered');

  // Straight into the sink, which is where `registry.route` lands anyway — five in one turn, so
  // the first opens §3's window and the other four are flushed together as one envelope. Driving
  // the store instead would need a dispatcher, and the dispatcher is not what is under test.
  for (let i = 0; i < 5; i++) {
    sink.enqueue({ type: 'delta', rev: i + 1, path: `batch/room/m${i}`, op: 'put', value: i });
  }
  await waitUntil(() => written.some((w) => w.includes('"type":"batch"')), 'the deltas were batched');
  await waitUntil(async () => (await wire('batchdb')) > afterSnapshot, 'and the batch was metered');

  // Every byte written, envelope included — asserted against the STRINGS the transport received,
  // which is the world, not our idea of it.
  const total = written.reduce((n, w) => n + Buffer.byteLength(w, 'utf8') + (Buffer.byteLength(w, 'utf8') < 126 ? 2 : 4), 0);
  assert.equal(await wire('batchdb'), before + total, 'the meter equals what the socket was handed');
  const envelope = written.find((w) => w.includes('"type":"batch"')) as string;
  assert.ok(envelope.startsWith('{"type":"batch","frames":['), 'and one of them really is an envelope');
});

test('the lock-hold gauge carries P2\'s constant, so the Load expression can be read', async () => {
  const m = /^rtdb_lock_hold_ms (\S+)$/m.exec(await register.metrics());
  assert.ok(m, 'the gauge is registered');
  assert.equal(Number(m?.[1]), 7.74, "P2's measured hold, and a deploy to change it");
});

test('storage bytes are reported per database, and a DECLARED empty one reads 0', async (t) => {
  // §5.19 (c): the registry exists so an owner can hand over an empty database. A usage line that
  // disappears when a database is empty says "no data" where the truth is "no bytes".
  M.resetSources();
  const base = new MemoryStorage(DEFAULT_LIMITS, undefined, 'public');
  await base.declareDatabase('empty_one', 'console-rw-owner');
  await base.commitGroup([{ writeId: crypto.randomUUID(), path: 'public/x', op: 'put', value: 'z'.repeat(500) }]);
  const unbind = M.bindStorageBytes(base, 0);
  t.after(() => unbind());

  const body = await register.metrics();
  const read = (db: string): number | null => {
    const m = new RegExp(`^rtdb_storage_bytes\\{db="${db}"\\} (\\S+)$`, 'm').exec(body);
    return m ? Number(m[1]) : null;
  };
  assert.equal(read('empty_one'), 0, 'declared, never written, and present at zero');
  assert.ok((read('public') ?? 0) > 500, `the store with data reports it (${read('public')})`);
});
