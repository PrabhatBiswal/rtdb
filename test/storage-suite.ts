import assert from 'node:assert/strict';
import test from 'node:test';
import type { StorageAdapter } from '../src/storage/adapter.ts';
import { DEFAULT_LIMITS, makeLimits, type Limits } from '../src/protocol/limits.ts';

/**
 * StorageAdapter semantics, written against the INTERFACE so Phase 4's Postgres adapter reuses it
 * verbatim (WORKLOAD §6 Gate C). Nothing in here may touch an implementation detail.
 */
export function storageSemantics(name: string, make: (limits: Limits) => StorageAdapter): void {
  const fresh = (limits: Limits = DEFAULT_LIMITS): StorageAdapter => make(limits);
  let n = 0;
  const wid = (): string => `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`;
  const put = (path: string, value: unknown) =>
    ({ writeId: wid(), path, op: 'put' as const, value: value as never });

  test(`${name}: an empty store has head 0 and reads null everywhere`, async () => {
    const s = fresh();
    assert.equal(await s.head(), 0);
    assert.deepEqual(await s.readSnapshot(''), { value: null, rev: 0 });
    assert.deepEqual(await s.readSnapshot('a/b'), { value: null, rev: 0 });
  });

  test(`${name}: the epoch is a positive integer and does not move while the store lives (§2)`, async () => {
    const s = fresh();
    const epoch = await s.epoch();
    assert.ok(Number.isInteger(epoch) && epoch >= 1);
    await s.commitGroup([put('a', 1)]);
    assert.equal(await s.epoch(), epoch, 'writes never bump the epoch — only a broken rev promise does');
  });

  test(`${name}: revs are gap-free, ascending, and assigned in arrival order`, async () => {
    const s = fresh();
    const first = await s.commitGroup([put('a', 1), put('b', 2), put('c', 3)]);
    assert.deepEqual(first.map((r) => r.rev), [1, 2, 3]);
    const second = await s.commitGroup([put('d', 4)]);
    assert.equal(second[0]?.rev, 4);
    assert.equal(await s.head(), 4);
  });

  test(`${name}: a put stores a subtree that reads back whole and by part`, async () => {
    const s = fresh();
    await s.commitGroup([put('MPK_1010/1474396', { name: 'Ravi', score: 42, stats: { wins: 3 } })]);
    assert.deepEqual((await s.readSnapshot('MPK_1010/1474396')).value, {
      name: 'Ravi',
      score: 42,
      stats: { wins: 3 },
    });
    assert.equal((await s.readSnapshot('MPK_1010/1474396/score')).value, 42);
    assert.deepEqual((await s.readSnapshot('MPK_1010/1474396/stats')).value, { wins: 3 });
    assert.deepEqual((await s.readSnapshot('')).value, {
      MPK_1010: { 1474396: { name: 'Ravi', score: 42, stats: { wins: 3 } } },
    });
  });

  test(`${name}: null deletes the whole subtree; empty objects are never stored (§1)`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', { b: 1, c: { d: 2 } })]);
    await s.commitGroup([put('a/c', null)]);
    assert.deepEqual((await s.readSnapshot('a')).value, { b: 1 });
    await s.commitGroup([put('a', {})]);
    assert.equal((await s.readSnapshot('a')).value, null);
  });

  test(`${name}: arrays are opaque leaf values, Firebase-style (§1)`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', { list: [1, { x: 2 }, 3] })]);
    assert.deepEqual((await s.readSnapshot('a/list')).value, [1, { x: 2 }, 3]);
  });

  test(`${name}: the leaf set stays prefix-free in both directions`, async () => {
    const s = fresh();
    await s.commitGroup([put('a/b', 5)]);          // scalar leaf
    await s.commitGroup([put('a/b/c', 6)]);        // ...becomes an object
    assert.deepEqual((await s.readSnapshot('a')).value, { b: { c: 6 } });
    await s.commitGroup([put('a/b', 7)]);          // ...and back to a scalar
    assert.deepEqual((await s.readSnapshot('a')).value, { b: 7 });
  });

  test(`${name}: merge writes deep keys atomically under ONE rev and leaves siblings alone`, async () => {
    const s = fresh();
    await s.commitGroup([put('p', { score: 1, tag: 'x', keep: true })]);
    const [ack] = await s.commitGroup([
      { writeId: wid(), path: 'p', op: 'merge', value: { score: 50, 'stats/wins': 3, tag: null } },
    ]);
    assert.equal(ack?.rev, 2, 'one merge is one rev however many keys it carries');
    assert.deepEqual((await s.readSnapshot('p')).value, { score: 50, keep: true, stats: { wins: 3 } });
  });

  test(`${name}: a duplicate writeId returns the ORIGINAL rev and commits nothing (§4 step 4)`, async () => {
    const s = fresh();
    const w = put('a', 1);
    const [first] = await s.commitGroup([w]);
    const [again] = await s.commitGroup([{ ...w, value: 999 as never }]);
    assert.deepEqual(again, { writeId: w.writeId, rev: first?.rev as number, duplicate: true });
    assert.equal(await s.head(), 1, 'a replay must not burn a rev');
    assert.equal((await s.readSnapshot('a')).value, 1, 'a replay must not re-apply');
  });

  test(`${name}: duplicates inside one batch collapse to a single commit`, async () => {
    const s = fresh();
    const w = put('a', 1);
    const acks = await s.commitGroup([w, w, put('b', 2), w]);
    assert.deepEqual(acks.map((r) => r.rev), [1, 1, 2, 1]);
    assert.deepEqual(acks.map((r) => r.duplicate), [false, true, false, true]);
    assert.equal(await s.head(), 2, 'the counter is taken for new writes only — no gaps');
  });

  test(`${name}: CAS commits when nothing relevant landed after expectedRev (§4 step 3)`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 });
    assert.deepEqual(r, { ok: true, rev: 2, duplicate: false });
    assert.equal((await s.readSnapshot('p/score')).value, 51);
  });

  test(`${name}: CAS fails with fresh state when a relevant write landed`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    await s.commitGroup([put('p/score', 60)]);
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 });
    assert.deepEqual(r, { ok: false, rev: 2, value: 60 });
    assert.equal(await s.head(), 2, 'a failed CAS must not consume a rev — rev is gap-free (§1)');
  });

  test(`${name}: CAS relevance is ancestor-or-descendant, not exact path`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    await s.commitGroup([put('other', 1)]); // rev 2, irrelevant
    assert.equal((await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 })).ok, true);

    const s2 = fresh();
    await s2.commitGroup([put('p/score', 50)]);
    await s2.commitGroup([put('p', { score: 60 })]); // rev 2, an ANCESTOR write
    assert.equal((await s2.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 })).ok, false);
  });

  test(`${name}: CAS is defined on the oplog, so a delete still conflicts (§4)`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 42)]); // rev 1
    await s.commitGroup([put('p', null)]); // rev 2 — the leaf and its rev are gone from `nodes`
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 43 });
    assert.equal(r.ok, false, 'max(leaf revs) would have missed this; the oplog does not');
  });

  test(`${name}: a duplicate CAS writeId acks with its original rev`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    const w = { writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 as never };
    const first = await s.commitCas(w);
    assert.deepEqual(await s.commitCas(w), { ok: true, rev: (first as { rev: number }).rev, duplicate: true });
    assert.equal(await s.head(), 2);
  });

  test(`${name}: an expectedRev older than retention fails conservatively (§4)`, async () => {
    const s = fresh(makeLimits({ OPLOG_RETENTION_REVS: 2 }));
    await s.commitGroup([put('p/score', 1)]); // rev 1
    await s.commitGroup([put('other', 2)]); // rev 2
    await s.commitGroup([put('other', 3)]); // rev 3 -> rev 1 is pruned
    assert.equal(await s.prunedThroughRev(), 1);

    // expectedRev 0 would need the pruned rev 1 to decide -> we cannot prove it, so we must not commit.
    const stale = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 0, value: 9 });
    assert.equal(stale.ok, false, 'unprovable means casFail, never an optimistic commit');

    // expectedRev 1 is still decidable: everything after it (revs 2 and 3) is retained.
    const ok = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 9 });
    assert.equal(ok.ok, true, 'the boundary is "can I see everything AFTER it", not "is it itself retained"');
  });

  test(`${name}: catch-up returns relevant entries only, ascending, capped at the limit`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/a', 1), put('q/b', 2), put('p', { a: 3 }), put('p/a/deep', 4)]);
    const got = await s.readCatchup('p/a', 0, 100);
    assert.deepEqual(got.map((e) => e.rev), [1, 3, 4], 'self, ancestor and descendant are all relevant');
    assert.deepEqual((await s.readCatchup('p/a', 3, 100)).map((e) => e.rev), [4]);
    assert.equal((await s.readCatchup('', 0, 2)).length, 2, 'the limit is respected');
  });

  test(`${name}: the dispatcher tail returns every entry after a rev, ascending`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', 1), put('b', 2)]);
    await s.commitCas({ writeId: wid(), path: 'c', expectedRev: 2, value: 3 });
    assert.deepEqual((await s.readOplogSince(0, 100)).map((e) => [e.rev, e.path]), [
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ]);
    assert.deepEqual((await s.readOplogSince(2, 100)).map((e) => e.rev), [3]);
  });

  test(`${name}: commits notify, and unsubscribing stops it`, async () => {
    const s = fresh();
    let fired = 0;
    const off = s.onCommit(() => fired++);
    await s.commitGroup([put('a', 1), put('b', 2)]);
    assert.equal(fired, 1, 'one notify per transaction, not per write');
    await s.commitCas({ writeId: wid(), path: 'c', expectedRev: 2, value: 1 });
    assert.equal(fired, 2);
    off();
    await s.commitGroup([put('d', 1)]);
    assert.equal(fired, 2);
  });
}
