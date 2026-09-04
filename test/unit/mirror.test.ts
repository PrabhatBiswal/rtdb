import assert from 'node:assert/strict';
import test from 'node:test';
import { Mirror } from '../../harness/mirror.ts';
import type { Delta, Json } from '../../src/protocol/frames.ts';

const delta = (rev: number, path: string, value: Json, op: 'put' | 'merge' = 'put'): Delta => ({
  type: 'delta',
  rev,
  path,
  op,
  value,
});

test('a snapshot replaces the sub’s serverState (§3)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { name: 'Ravi', score: 42 }, 10);
  assert.deepEqual(m.view('p'), { name: 'Ravi', score: 42 });
  m.applySnapshot('p', { score: 7 }, 11);
  assert.deepEqual(m.view('p'), { score: 7 }, 'replace, not merge');
});

test('deltas apply by op: put replaces a subtree, merge writes children', () => {
  const m = new Mirror();
  m.applySnapshot('p', { score: 1, tag: 'x' }, 10);
  m.applyDelta(delta(11, 'p/score', 50));
  assert.deepEqual(m.view('p'), { score: 50, tag: 'x' });
  m.applyDelta(delta(12, 'p', { score: 60, 'stats/wins': 3, tag: null }, 'merge'));
  assert.deepEqual(m.view('p'), { score: 60, stats: { wins: 3 } });
});

test('an ancestor delta is extracted at the sub’s relative path, null when absent (§3)', () => {
  const m = new Mirror();
  m.applySnapshot('p/child', { a: 1 }, 10);
  m.applyDelta(delta(11, 'p', { child: { a: 2, b: 3 }, other: 9 }));
  assert.deepEqual(m.view('p/child'), { a: 2, b: 3 });
  m.applyDelta(delta(12, 'p', { other: 9 })); // child is gone from the ancestor value
  assert.equal(m.view('p/child'), null);
});

test('per-leaf rev LWW drops a stale delta for that leaf only (§7)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: 1, b: 1 }, 10);
  m.applyDelta(delta(12, 'p/a', 'new'));
  m.applyDelta(delta(11, 'p/a', 'stale')); // arrives late, older rev
  assert.deepEqual(m.view('p'), { a: 'new', b: 1 }, 'the stale delta is dropped, its sibling untouched');
  m.applyDelta(delta(13, 'p/b', 2));
  assert.deepEqual(m.view('p'), { a: 'new', b: 2 });
});

test('a delete leaves a rev-stamped tombstone that blocks resurrection (§7)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: { deep: 1 }, b: 2 }, 10);
  m.applyDelta(delta(12, 'p/a', null)); // delete the subtree
  assert.deepEqual(m.view('p'), { b: 2 });
  // A stale delta from before the delete must NOT bring the data back — this is the exact case the
  // tombstone exists for, including a leaf we no longer hold.
  m.applyDelta(delta(11, 'p/a/deep', 99));
  m.applyDelta(delta(11, 'p/a/never-seen', 99));
  assert.deepEqual(m.view('p'), { b: 2 });
  // ...but a NEWER write to the same place is applied normally.
  m.applyDelta(delta(13, 'p/a/deep', 5));
  assert.deepEqual(m.view('p'), { a: { deep: 5 }, b: 2 });
});

test('an ancestor put stamps every extracted leaf, not just the root (§7)', () => {
  const m = new Mirror();
  m.applySnapshot('', { p: { a: 1, b: 1 } }, 10);
  m.applyDelta(delta(20, 'p', { a: 2, b: 2 }));
  m.applyDelta(delta(15, 'p/a', 'stale')); // older than the ancestor put that produced p/a
  assert.deepEqual(m.view('p'), { a: 2, b: 2 });
});

test('view = serverState ⊕ overlay, applied in issue order, with no rollback (§7)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { score: 1, tag: 'x' }, 10);

  m.overlay.push({ op: 'put', path: 'p/score', value: 2 });
  assert.deepEqual(m.view('p'), { score: 2, tag: 'x' }, 'optimistic');
  assert.deepEqual(m.serverValue('p'), { score: 1, tag: 'x' }, 'serverState is untouched by the overlay');

  // A concurrent foreign delta lands while our write is still unacked: the view is still
  // serverState ⊕ overlay at every instant.
  m.applyDelta(delta(11, 'p/tag', 'y'));
  assert.deepEqual(m.view('p'), { score: 2, tag: 'y' });

  // ack: the entry leaves the overlay, its effect arrives via the server echo.
  m.applyDelta(delta(12, 'p/score', 2));
  m.overlay.length = 0;
  assert.deepEqual(m.view('p'), { score: 2, tag: 'y' });
  assert.deepEqual(m.serverValue('p'), m.view('p'), 'converged');
});

test('overlay ops compose in order, including deletes and ancestor writes', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: 1, b: 2 }, 10);
  m.overlay.push({ op: 'merge', path: 'p', value: { a: null, 'c/d': 3 } });
  assert.deepEqual(m.view('p'), { b: 2, c: { d: 3 } });
  m.overlay.push({ op: 'put', path: 'p', value: { z: 1 } }); // replaces everything above it
  assert.deepEqual(m.view('p'), { z: 1 });
  assert.deepEqual(m.view('p/z'), 1, 'the overlay is visible from a descendant view too');
});

test('empty subtrees read as null, never as {}', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: 1 }, 10);
  m.applyDelta(delta(11, 'p/a', null));
  assert.equal(m.view('p'), null);
  assert.equal(m.view('p/nothing/here'), null);
});

test('a snapshot older than a leaf we already hold does not roll it back (§7 v1.3)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { score: 1, tag: 'x' }, 10);

  // The wire sequence an overlapping sub's setup can produce: the live sub already applied rev 11
  // when the new sub's snapshot, read at rev 10, arrives — and rev 11 is flushed again after it.
  m.applyDelta(delta(11, 'p/score', 99));
  m.applySnapshot('p', { score: 1, tag: 'x' }, 10);
  assert.deepEqual(m.view('p'), { score: 99, tag: 'x' }, 'no visible rollback in between');

  m.applyDelta(delta(11, 'p/score', 99));
  assert.deepEqual(m.view('p'), { score: 99, tag: 'x' }, 'and re-applying the delta is idempotent');
});

test('a snapshot does not resurrect what was deleted after its rev (§7 v1.3)', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: 1, b: 2 }, 10);
  m.applyDelta(delta(12, 'p/a', null)); // deleted at rev 12
  m.applySnapshot('p', { a: 1, b: 2 }, 11); // a stale snapshot still carrying `a`
  assert.deepEqual(m.view('p'), { b: 2 }, 'the tombstone outranks the older snapshot');
});

test('a newer snapshot replaces everything, including leaves we thought were current', () => {
  const m = new Mirror();
  m.applySnapshot('p', { a: 1 }, 10);
  m.applyDelta(delta(11, 'p/b', 2));
  m.applySnapshot('p', { z: 9 }, 12);
  assert.deepEqual(m.view('p'), { z: 9 });
});

test('a delta below a scalar turns it into an object, as storage already did (§8 nodes)', () => {
  // The server deletes the ancestor leaf silently — it never sends a delta for it — so the client
  // has to infer it, or the mirror keeps reading the stale scalar forever.
  const m = new Mirror();
  m.applySnapshot('p', 5, 1);
  m.applyDelta(delta(2, 'p/x', 1));
  assert.deepEqual(m.view('p'), { x: 1 });
  m.applyDelta(delta(3, 'p', 7)); // ...and back to a scalar
  assert.equal(m.view('p'), 7);
});
