import assert from 'node:assert/strict';
import test from 'node:test';
import { toDelta } from '../../src/fanout/dispatcher.ts';
import { ConnectionSink, SubscriptionRegistry, type Transport } from '../../src/fanout/subscriptions.ts';
import type { Delta, ServerFrame } from '../../src/protocol/frames.ts';
import { makeLimits, type Limits } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';

class FakeSocket implements Transport {
  readonly sent: ServerFrame[] = [];
  pressure = 0;
  /** uWS drops what it cannot buffer; with this on, nothing handed to the socket arrives. */
  dropSends = false;
  #closed = false;
  send(text: string): boolean {
    if (this.dropSends) return false;
    this.sent.push(JSON.parse(text) as ServerFrame);
    return true;
  }
  bufferedAmount(): number {
    return this.pressure;
  }
  closed(): boolean {
    return this.#closed;
  }
  close(): void {
    this.#closed = true;
  }
  /** Flatten batches, so assertions read as "what the client actually processes" (§3). */
  get frames(): ServerFrame[] {
    return this.sent.flatMap((f) => (f.type === 'batch' ? f.frames : [f]));
  }
}

const setup = (limits: Limits = makeLimits({ DELTA_BATCH_MS: 5 })) => {
  const storage = new MemoryStorage(limits);
  const registry = new SubscriptionRegistry();
  const socket = new FakeSocket();
  const sink = new ConnectionSink(socket, registry, storage, limits);
  return { storage, registry, socket, sink, limits };
};

/** Let the micro-batch window close, so `socket.frames` shows everything that was queued. */
const drain = (ms = 40): Promise<void> => new Promise((r) => setTimeout(r, ms));

const put = (id: string, path: string, value: unknown) =>
  ({ writeId: id, path, op: 'put' as const, value: value as never });

test('listen with no lastRev sends a snapshot at the current head (§3)', async () => {
  const { storage, sink, socket } = setup();
  await storage.commitGroup([put('a', 'p', { score: 1 })]);
  await sink.listen(7, 'p');
  assert.deepEqual(socket.frames, [{ type: 'snapshot', subId: 7, path: 'p', value: { score: 1 }, rev: 1 }]);
});

test('a delta arriving mid-setup is buffered and flushed only if newer than the snapshot (§3)', async () => {
  const { storage, registry, sink, socket } = setup();
  await storage.commitGroup([put('a', 'p', { score: 1 })]); // rev 1

  // Interleave a delta between "join the topic" and "read the snapshot" by racing the two.
  const listening = sink.listen(7, 'p');
  registry.route(toDelta({ rev: 1, path: 'p', op: 'put', value: { score: 1 }, writeId: 'a', ts: 0 }));
  registry.route(toDelta({ rev: 2, path: 'p/score', op: 'put', value: 9, writeId: 'b', ts: 0 }));
  await listening;
  await drain();

  const kinds = socket.frames.map((f) => f.type);
  assert.equal(kinds[0], 'snapshot', 'the snapshot always comes first for a fresh sub');
  const revs = socket.frames.filter((f): f is Delta => f.type === 'delta').map((d) => d.rev);
  assert.ok(!revs.includes(1), 'a delta at or below the snapshot rev is discarded, not sent');
  assert.deepEqual(revs, [2], 'only the delta newer than the snapshot is flushed');
});

test('listen with a retained lastRev catches up with deltas instead of a snapshot (§3)', async () => {
  const { storage, sink, socket } = setup();
  await storage.commitGroup([put('a', 'p/x', 1), put('b', 'p/y', 2), put('c', 'other', 3)]);
  await sink.listen(7, 'p', 1);
  assert.deepEqual(
    socket.frames.map((f) => [f.type, (f as Delta).rev]),
    [['delta', 2]],
    'only relevant entries after lastRev, and no snapshot',
  );
});

test('more than CATCHUP_LIMIT relevant entries falls back to a fresh snapshot (§3)', async () => {
  const { storage, sink, socket } = setup(makeLimits({ CATCHUP_LIMIT: 2, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p/1', 1), put('b', 'p/2', 2), put('c', 'p/3', 3)]);
  await sink.listen(7, 'p', 0);
  assert.deepEqual(socket.frames.map((f) => f.type), ['snapshot']);
});

test('an unretained lastRev also falls back to a snapshot', async () => {
  const { storage, sink, socket } = setup(makeLimits({ OPLOG_RETENTION_REVS: 1, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p', 1)]);
  await storage.commitGroup([put('b', 'p', 2)]); // rev 1 is pruned
  await sink.listen(7, 'p', 0);
  assert.deepEqual(socket.frames.map((f) => f.type), ['snapshot']);
});

test('a snapshot over SNAPSHOT_MAX becomes a sub-scoped TOOBIG and no subscription (§3)', async () => {
  const { storage, sink, socket, registry } = setup(makeLimits({ SNAPSHOT_MAX: 200, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p', { blob: 'x'.repeat(500) })]);
  await sink.listen(7, 'p');
  assert.equal(socket.frames[0]?.type, 'err');
  assert.deepEqual({ ...socket.frames[0], msg: undefined }, {
    type: 'err',
    subId: 7,
    code: 'TOOBIG',
    msg: undefined,
  });
  assert.equal(sink.subs.size, 0, 'no subscription is created');
  registry.route(toDelta({ rev: 2, path: 'p', op: 'put', value: 1, writeId: 'z', ts: 0 }));
  assert.equal(socket.frames.filter((f) => f.type === 'delta').length, 0, 'and no deltas follow');
});

test('routing is ancestor-or-descendant, once per connection however many subs match (§3)', async () => {
  const { registry, sink, socket, storage } = setup();
  await storage.commitGroup([put('a', 'p/child', 1)]);
  await sink.listen(1, 'p');
  await sink.listen(2, 'p/child'); // overlapping subs on one connection
  socket.sent.length = 0;

  registry.route(toDelta({ rev: 2, path: 'p/child/deep', op: 'put', value: 5, writeId: 'b', ts: 0 }));
  registry.route(toDelta({ rev: 3, path: '', op: 'put', value: {}, writeId: 'c', ts: 0 }));
  registry.route(toDelta({ rev: 4, path: 'unrelated', op: 'put', value: 1, writeId: 'd', ts: 0 }));
  await drain();

  const revs = socket.frames.filter((f): f is Delta => f.type === 'delta').map((d) => d.rev);
  assert.deepEqual(revs, [2, 3], 'descendant and ancestor deltas arrive exactly once; unrelated ones never');
});

test('unlisten stops delivery', async () => {
  const { registry, sink, socket, storage } = setup();
  await storage.commitGroup([put('a', 'p', 1)]);
  await sink.listen(7, 'p');
  sink.unlisten(7);
  socket.sent.length = 0;
  registry.route(toDelta({ rev: 2, path: 'p', op: 'put', value: 2, writeId: 'b', ts: 0 }));
  await drain();
  assert.deepEqual(socket.frames, []);
});

test('the batch window is opened by the first frame and never delays an idle connection (§3)', async () => {
  const { registry, sink, socket, storage } = setup(makeLimits({ DELTA_BATCH_MS: 20 }));
  await storage.commitGroup([put('a', 'p', 1)]);
  await sink.listen(7, 'p');
  await drain(); // the snapshot's own window closes; the connection is idle again
  socket.sent.length = 0;

  for (const rev of [2, 3, 4]) {
    registry.route(toDelta({ rev, path: 'p', op: 'put', value: rev, writeId: `w${rev}`, ts: 0 }));
  }
  assert.equal(socket.sent.length, 1, 'the first delta goes out immediately');
  assert.equal(socket.sent[0]?.type, 'delta');

  await drain();
  assert.equal(socket.sent.length, 2);
  assert.equal(socket.sent[1]?.type, 'batch', 'the two that arrived inside the window ship as one batch');
  assert.deepEqual(socket.frames.map((f) => (f as Delta).rev), [2, 3, 4], 'in order, exactly once each');
});

test('send-queue overflow repairs the subscription with resync + a fresh snapshot (§3)', async () => {
  const { registry, sink, socket, storage } = setup(makeLimits({ SEND_QUEUE_MAX: 100, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p', { score: 1 })]);
  await sink.listen(7, 'p');
  socket.sent.length = 0;

  socket.pressure = 5000; // the consumer stopped reading
  await drain(); // idle first, so the overflow is caused by the delta and nothing else
  registry.route(toDelta({ rev: 2, path: 'p/score', op: 'put', value: 2, writeId: 'b', ts: 0 }));
  await drain();

  // The delta that revealed the pressure is already on the wire; the resync follows it.
  assert.deepEqual(socket.frames.map((f) => f.type), ['delta', 'resync'], 'the sub is declared stale');

  // Deltas arriving while the repair is outstanding are dropped, not queued into a full socket.
  registry.route(toDelta({ rev: 3, path: 'p/score', op: 'put', value: 3, writeId: 'c', ts: 0 }));
  await drain();
  assert.deepEqual(socket.frames.map((f) => f.type), ['delta', 'resync'], 'and further deltas are dropped');

  socket.pressure = 0; // the consumer caught up
  sink.onDrain();
  await drain();
  assert.deepEqual(
    socket.frames.map((f) => f.type),
    ['delta', 'resync', 'resync', 'snapshot'],
    'the fresh snapshot lands as soon as the socket can carry it, behind a re-announced resync',
  );
});

test('a resync the socket dropped is re-announced before the snapshot (§3)', async () => {
  const { registry, sink, socket, storage } = setup(makeLimits({ SEND_QUEUE_MAX: 100, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p', { score: 1 })]);
  await sink.listen(7, 'p');
  await drain();
  socket.sent.length = 0;

  // Over the backpressure limit, uWS refuses everything — including the tiny repair frame. This is
  // the WP2 Gate D finding: the resync was being sent into the very pressure that triggered it.
  socket.pressure = 5000;
  socket.dropSends = true;
  registry.route(toDelta({ rev: 2, path: 'p/score', op: 'put', value: 2, writeId: 'b', ts: 0 }));
  await drain();
  assert.deepEqual(
    socket.frames.map((f) => f.type),
    [],
    'the socket took neither the delta nor the resync',
  );

  socket.pressure = 0; // the consumer caught up
  socket.dropSends = false;
  sink.onDrain();
  await drain();
  assert.deepEqual(
    socket.frames.map((f) => f.type),
    ['resync', 'snapshot'],
    'the client is told its sub went stale before the replacement arrives, even though the first resync died',
  );
});

test('a frame the socket refuses declares the subscription stale on its own (§3)', async () => {
  // The buffer can read UNDER the limit while uWS is still refusing frames; the drop itself is the
  // signal, because §3 forbids the client from ever detecting the gap for itself.
  const { registry, sink, socket, storage } = setup(makeLimits({ SEND_QUEUE_MAX: 1_000_000, DELTA_BATCH_MS: 5 }));
  await storage.commitGroup([put('a', 'p', { score: 1 })]);
  await sink.listen(7, 'p');
  await drain();
  socket.sent.length = 0;

  socket.dropSends = true;
  socket.pressure = 0; // nothing about the buffer looks wrong
  registry.route(toDelta({ rev: 2, path: 'p/score', op: 'put', value: 2, writeId: 'b', ts: 0 }));
  await drain();
  socket.dropSends = false;
  sink.onDrain();
  await drain();

  assert.deepEqual(
    socket.frames.map((f) => f.type),
    ['resync', 'snapshot'],
    'a dropped delta is repaired, not silently forgotten',
  );
});
