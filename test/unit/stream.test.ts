import assert from 'node:assert/strict';
import test from 'node:test';
import { Dispatcher, toDelta } from '../../src/fanout/dispatcher.ts';
import { OrderedStream } from '../../src/fanout/stream.ts';
import type { Delta } from '../../src/protocol/frames.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';

const entry = (rev: number, path = 'p') =>
  ({ rev, path, op: 'put' as const, value: rev, writeId: `w${rev}`, ts: 0 });

test('the stream replays from an id, and says so when it has trimmed past it', () => {
  const s = new OrderedStream<Delta>(3);
  for (const rev of [1, 2, 3]) s.append(rev, toDelta(entry(rev)));
  assert.deepEqual(s.range(1)?.map((d) => d.rev), [2, 3]);
  assert.deepEqual(s.range(3), []);
  s.append(4, toDelta(entry(4))); // trims rev 1
  assert.equal(s.range(0), null, 'replay below the trim point is impossible — fall back to the oplog');
  assert.deepEqual(s.range(1)?.map((d) => d.rev), [2, 3, 4]);
  assert.equal(s.lastId, 4);
});

test('out-of-order appends throw — ordering is the invariant this class exists for (§8)', () => {
  const s = new OrderedStream<Delta>(10);
  s.append(5, toDelta(entry(5)));
  assert.throws(() => s.append(5, toDelta(entry(5))), /must ascend/);
  assert.throws(() => s.append(4, toDelta(entry(4))), /must ascend/);
});

test('subscribers see every item in append order', () => {
  const s = new OrderedStream<Delta>(10);
  const seen: number[] = [];
  const off = s.subscribe((d) => seen.push(d.rev));
  for (const rev of [1, 2, 3]) s.append(rev, toDelta(entry(rev)));
  off();
  s.append(4, toDelta(entry(4)));
  assert.deepEqual(seen, [1, 2, 3]);
});

test('the dispatcher publishes the oplog in strict rev order (§8)', async () => {
  const storage = new MemoryStorage();
  const stream = new OrderedStream<Delta>(1000);
  const published: number[] = [];
  stream.subscribe((d) => published.push(d.rev));
  const d = new Dispatcher(storage, stream);
  d.start();

  await storage.commitGroup([
    { writeId: 'a', path: 'x', op: 'put', value: 1 },
    { writeId: 'b', path: 'y', op: 'put', value: 2 },
  ]);
  await d.pump();
  await storage.commitCas({ writeId: 'c', path: 'z', expectedRev: 2, value: 3 });
  await d.pump();

  assert.deepEqual(published, [1, 2, 3]);
  assert.equal(d.lastPublishedRev, 3);
  d.stop();
});

test('concurrent pumps collapse instead of interleaving revs', async () => {
  const storage = new MemoryStorage();
  const stream = new OrderedStream<Delta>(1000);
  const published: number[] = [];
  stream.subscribe((d) => published.push(d.rev));
  const d = new Dispatcher(storage, stream);

  await storage.commitGroup(
    Array.from({ length: 20 }, (_, i) => ({ writeId: `w${i}`, path: `p${i}`, op: 'put' as const, value: i })),
  );
  // Five racing drains must still yield one ascending sequence — the stream would throw otherwise.
  await Promise.all([d.pump(), d.pump(), d.pump(), d.pump(), d.pump()]);
  assert.deepEqual(published, Array.from({ length: 20 }, (_, i) => i + 1));
});

test('the dispatcher drains an oplog longer than one read batch', async () => {
  const storage = new MemoryStorage(makeLimits({ OPLOG_RETENTION_REVS: 1000 }));
  const stream = new OrderedStream<Delta>(1000);
  const d = new Dispatcher(storage, stream, 3); // tiny batch on purpose
  await storage.commitGroup(
    Array.from({ length: 10 }, (_, i) => ({ writeId: `w${i}`, path: `p${i}`, op: 'put' as const, value: i })),
  );
  await d.pump();
  assert.equal(d.lastPublishedRev, 10);
  assert.deepEqual(stream.range(0)?.map((x) => x.rev), Array.from({ length: 10 }, (_, i) => i + 1));
});
