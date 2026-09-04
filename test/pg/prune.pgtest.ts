import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after } from 'node:test';
import { makeLimits } from '../../src/protocol/limits.ts';
import type { GroupWrite } from '../../src/storage/adapter.ts';
import { createDatabase } from './helper.ts';

const db = await createDatabase('prune');
after(() => db.drop());

const put = (path: string, value: unknown): GroupWrite =>
  ({ writeId: randomUUID(), path, op: 'put', value: value as never });

/** Backdates already-committed oplog rows — the honest way to test a 2h bound in a 2s test. */
const age = async (schema: string, revs: number[], hours: number): Promise<void> => {
  const c = await db.client(schema);
  await c.query(`UPDATE oplog SET ts = now() - make_interval(hours => $2) WHERE rev = ANY($1::bigint[])`, [
    revs,
    hours,
  ]);
};

test('§9 the TIME bound prunes what the count bound would have kept', async () => {
  const schema = 'time_bound';
  const s = db.make(makeLimits({ OPLOG_RETENTION_REVS: 500_000 }), schema); // count bound: keeps everything
  for (let i = 1; i <= 5; i++) await s.commitGroup([put(`p/${i}`, i)]);
  assert.equal(await s.prunedThroughRev(), 0, 'nothing is old enough or numerous enough yet');

  await age(schema, [1, 2, 3], 3); // older than the 2h default
  assert.equal(await s.prune(), 3, 'the three aged revs go, the two recent ones stay');
  assert.equal(await s.prunedThroughRev(), 3);
  assert.deepEqual((await s.readOplogSince(0, 100)).map((e) => e.rev), [4, 5]);
});

test('§9 the COUNT bound prunes what the time bound would have kept', async () => {
  const schema = 'count_bound';
  const s = db.make(makeLimits({ OPLOG_RETENTION_REVS: 2 }), schema);
  for (let i = 1; i <= 5; i++) await s.commitGroup([put(`p/${i}`, i)]);
  // Every row is seconds old, so the 2h bound would keep them all; the count bound keeps 2.
  assert.equal(await s.prunedThroughRev(), 3, 'the count bound applies inline, on every commit');
  assert.deepEqual((await s.readOplogSince(0, 100)).map((e) => e.rev), [4, 5]);
  assert.equal(await s.prune(), 3, 'and the timer agrees with what the write path already did');
});

test('§9 whichever bound prunes MORE wins', async () => {
  const schema = 'both_bounds';
  const s = db.make(makeLimits({ OPLOG_RETENTION_REVS: 4 }), schema);
  for (let i = 1; i <= 6; i++) await s.commitGroup([put(`p/${i}`, i)]);
  assert.equal(await s.prunedThroughRev(), 2, 'count bound alone: keep the last 4');

  // Now age rev 5 too. Time says "prune through 5", count says "prune through 2" — time prunes more.
  await age(schema, [3, 4, 5], 3);
  assert.equal(await s.prune(), 5, 'the more aggressive bound decides');
  assert.deepEqual((await s.readOplogSince(0, 100)).map((e) => e.rev), [6]);
});

test('pruning never touches `nodes` — it is the present, not history', async () => {
  const schema = 'nodes_kept';
  const s = db.make(makeLimits({ OPLOG_RETENTION_REVS: 1 }), schema);
  await s.commitGroup([put('room/a', 1)]);
  await s.commitGroup([put('room/b', 2)]);
  await s.commitGroup([put('room/c', 3)]);
  await s.prune();

  assert.ok((await s.prunedThroughRev()) >= 2, 'history is gone');
  assert.deepEqual(
    (await s.readSnapshot('room')).value,
    { a: 1, b: 2, c: 3 },
    'and every leaf ever written is still readable — a snapshot does not need the oplog',
  );
});

test('§4 a CAS below the watermark fails conservatively after a TIME prune', async () => {
  const schema = 'cas_below';
  const s = db.make(makeLimits({ OPLOG_RETENTION_REVS: 500_000 }), schema);
  await s.commitGroup([put('p/score', 1)]); // rev 1
  await s.commitGroup([put('other', 2)]); // rev 2
  await age(schema, [1, 2], 3);
  await s.prune();
  assert.equal(await s.prunedThroughRev(), 2);

  const stale = await s.commitCas({ writeId: randomUUID(), path: 'p/score', expectedRev: 1, value: 9 });
  assert.equal(stale.ok, false, 'we no longer hold the history that would decide it');
  assert.deepEqual(stale.ok === false ? stale.value : null, 1, 'and it carries fresh state');

  const ok = await s.commitCas({ writeId: randomUUID(), path: 'p/score', expectedRev: 2, value: 9 });
  assert.equal(ok.ok, true, 'at the watermark everything AFTER it is still retained');
});

test('§2 the watermark and the epoch both survive a restart', async () => {
  const schema = 'survives';
  const first = db.make(makeLimits({ OPLOG_RETENTION_REVS: 2 }), schema);
  for (let i = 1; i <= 5; i++) await first.commitGroup([put(`p/${i}`, i)]);
  const [epoch, watermark, head] = [await first.epoch(), await first.prunedThroughRev(), await first.head()];
  assert.equal(watermark, 3);
  await first.close();

  // A brand-new adapter over the same data — the gateway coming back after a kill.
  const restarted = db.make(makeLimits({ OPLOG_RETENTION_REVS: 2 }), schema);
  assert.equal(await restarted.epoch(), epoch, 'same data, same generation (§2)');
  assert.equal(await restarted.head(), head, 'the counter is where it was');
  assert.equal(await restarted.prunedThroughRev(), watermark, 'and so is the watermark');
});
