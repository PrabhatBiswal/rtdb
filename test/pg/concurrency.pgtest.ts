import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after } from 'node:test';
import { isRelevant } from '../../src/protocol/path.ts';
import type { AckResult, GroupWrite } from '../../src/storage/adapter.ts';
import { createDatabase, warmPool } from './helper.ts';

/**
 * WORKLOAD §5, concurrency honesty: memory.ts was atomic because it was synchronous, so the shared
 * conformance suite never interleaves two commits. Postgres genuinely can. Everything here runs real
 * parallel transactions on real separate connections against one database.
 */
const db = await createDatabase('concurrency');
after(() => db.drop());

const put = (path: string, value: unknown): GroupWrite =>
  ({ writeId: randomUUID(), path, op: 'put', value: value as never });

test('parallel group commits: revs stay gap-free, and each batch gets a contiguous range', async () => {
  const s = db.make();
  const BATCHES = 12;
  const PER_BATCH = 3;
  await warmPool(s, BATCHES);

  const acks = await Promise.all(
    Array.from({ length: BATCHES }, (_, b) =>
      s.commitGroup(Array.from({ length: PER_BATCH }, (_, i) => put(`w${b}/${i}`, i))),
    ),
  );

  const all = acks.flat().map((a) => a.rev).sort((x, y) => x - y);
  assert.deepEqual(
    all,
    Array.from({ length: BATCHES * PER_BATCH }, (_, i) => i + 1),
    'every rev from 1..N exactly once — no gaps, no duplicates, whatever the interleaving',
  );
  assert.equal(await s.head(), BATCHES * PER_BATCH);

  for (const [b, batch] of acks.entries()) {
    const revs = batch.map((a) => a.rev);
    assert.deepEqual(
      revs,
      Array.from({ length: PER_BATCH }, (_, i) => (revs[0] as number) + i),
      `batch ${b} must hold ONE contiguous range in arrival order — the counter is taken once (§4 step 2)`,
    );
  }

  // The oplog agrees with the acks: same revs, and the rows are really there.
  const oplog = await s.readOplogSince(0, 1000);
  assert.deepEqual(oplog.map((e) => e.rev), all);
});

test('concurrent CAS on one path, one expectedRev: exactly one winner (§4 step 3)', async () => {
  const s = db.make();
  await s.commitGroup([put('p/score', 0)]); // rev 1
  const RACERS = 12;
  await warmPool(s, RACERS);

  const results = await Promise.all(
    Array.from({ length: RACERS }, (_, i) =>
      s.commitCas({ writeId: randomUUID(), path: 'p/score', expectedRev: 1, value: 100 + i }),
    ),
  );

  const won = results.filter((r) => r.ok);
  assert.equal(won.length, 1, `exactly one CAS may commit against rev 1 — ${won.length} did`);
  assert.equal(await s.head(), 2, 'and the eleven losers must not consume revs — rev is gap-free (§1)');

  // §4: a mismatch carries FRESH state — and "fresh" means the value AS OF the rev it reports. A
  // loser that read before the winner committed must say (rev 1, 0); one that read after must say
  // (rev 2, the winner's value). Any other pair is a torn read across the winner's transaction.
  const winnerValue = 100 + results.findIndex((r) => r.ok);
  for (const [i, lost] of results.entries()) {
    if (lost.ok) continue;
    assert.deepEqual(
      { rev: lost.rev, value: lost.value },
      lost.rev === 1 ? { rev: 1, value: 0 } : { rev: 2, value: winnerValue },
      `loser ${i} reported state that never existed`,
    );
  }
});

test('CAS racing a put on the same path never commits over an unseen write', async () => {
  // The invariant is not "who wins" — it is that a CAS which DID commit at rev X saw nothing
  // relevant land between its expectedRev and X. Checked against the oplog the race actually wrote.
  for (let round = 0; round < 20; round++) {
    const s = db.make();
    await s.commitGroup([put('p/score', 0)]); // rev 1
    await warmPool(s, 2);

    const [cas] = await Promise.all([
      s.commitCas({ writeId: randomUUID(), path: 'p/score', expectedRev: 1, value: 99 }),
      s.commitGroup([put('p', { score: 1 })]), // an ANCESTOR write — relevant by §3
    ]);

    const oplog = await s.readOplogSince(1, 100);
    if (cas.ok) {
      const shadowed = oplog.filter((e) => e.rev < cas.rev && isRelevant(e.path, 'p/score'));
      assert.deepEqual(shadowed, [], `round ${round}: CAS committed at ${cas.rev} over ${shadowed.length} unseen relevant writes`);
    } else {
      const conflicting = oplog.filter((e) => isRelevant(e.path, 'p/score'));
      assert.ok(conflicting.length > 0, `round ${round}: casFail must be justified by a real relevant write`);
    }
    assert.equal(await s.head(), cas.ok ? 3 : 2, `round ${round}: no rev may be burned by the loser`);
  }
});

test('one writeId, two connections at once: one rev, identical acks (§4 step 4)', async () => {
  const s = db.make();
  await s.commitGroup([put('seed', 0)]); // rev 1
  const writeId = randomUUID();
  const RACERS = 8;
  await warmPool(s, RACERS);

  const acks: AckResult[] = (
    await Promise.all(
      Array.from({ length: RACERS }, () =>
        s.commitGroup([{ writeId, path: 'p/score', op: 'put', value: 42 }]),
      ),
    )
  ).map((batch) => batch[0] as AckResult);

  const revs = new Set(acks.map((a) => a.rev));
  assert.equal(revs.size, 1, `all ${RACERS} acks must carry the SAME rev — got ${[...revs]}`);
  assert.equal(acks.filter((a) => !a.duplicate).length, 1, 'exactly one of them was the first commit');
  assert.equal(await s.head(), 2, 'a replayed writeId must never burn a rev, however many arrive at once');
  assert.equal((await s.readSnapshot('p/score')).value, 42, 'and it must be applied exactly once');
});

test('a CAS interleaved with group commits keeps the oplog gap-free and ordered', async () => {
  const s = db.make();
  await s.commitGroup([put('p/score', 0)]); // rev 1
  await warmPool(s, 8);

  await Promise.all([
    ...Array.from({ length: 6 }, (_, i) => s.commitGroup([put(`bulk/${i}`, i), put(`bulk/${i}/x`, i)])),
    s.commitCas({ writeId: randomUUID(), path: 'p/score', expectedRev: 1, value: 7 }),
    s.commitCas({ writeId: randomUUID(), path: 'q', expectedRev: 1, value: 8 }),
  ]);

  const head = await s.head();
  const oplog = await s.readOplogSince(0, 1000);
  assert.deepEqual(
    oplog.map((e) => e.rev),
    Array.from({ length: head }, (_, i) => i + 1),
    'the oplog is exactly 1..head, ascending, with nothing missing',
  );
});
