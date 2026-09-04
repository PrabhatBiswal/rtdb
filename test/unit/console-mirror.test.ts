import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

/**
 * The console is one static HTML file with no build step, so its §7 mirror cannot be imported. It is
 * EXTRACTED from the file and evaluated here instead — which means these tests exercise the exact
 * code the browser runs, not a copy of it that could drift away from it silently.
 */
const HTML = fileURLToPath(new URL('../../tools/console/rtdb-console.html', import.meta.url));

function loadMirror(): {
  Mirror: new () => {
    applySnapshot(p: string, v: unknown, rev: number): void;
    applyDelta(d: { rev: number; path: string; op: string; value: unknown }): void;
    dropServerState(): void;
    value(root: string): unknown;
    children(root: string): { key: string; path: string; leaf: boolean; value?: unknown }[];
  };
} {
  const src = readFileSync(HTML, 'utf8');
  const block = /<script id="rtdb-mirror">([\s\S]*?)<\/script>/.exec(src);
  assert.ok(block, 'the console must keep its mirror in a <script id="rtdb-mirror"> block');
  // Evaluated in THIS realm, not a vm context: objects the mirror builds must share the test's
  // Object.prototype or deepEqual rejects structurally identical values as cross-realm.
  const mod = { exports: {} as ReturnType<typeof loadMirror> };
  new Function('module', block[1] as string)(mod);
  return mod.exports;
}

const delta = (rev: number, path: string, value: unknown, op = 'put'): { rev: number; path: string; op: string; value: unknown } =>
  ({ type: 'delta', rev, path, op, value } as never);

test('snapshot then delta: the newer value wins and renders', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { name: 'Ravi', score: 42 }, 10);
  assert.deepEqual(m.value('room'), { name: 'Ravi', score: 42 });
  m.applyDelta(delta(11, 'room/score', 50));
  assert.deepEqual(m.value('room'), { name: 'Ravi', score: 50 });
});

// THE TOOTH. §7's per-leaf LWW is defence-in-depth: the dispatcher already guarantees order, so a
// broken port looks perfectly fine against ordered traffic and only corrupts data when a late frame
// arrives. Ordered tests cannot catch it; this one is deliberately out of order.
test('per-leaf LWW: a delta OLDER than the leaf it targets is dropped', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { score: 50 }, 20);
  m.applyDelta(delta(5, 'room/score', 1));                       // stale: rev 5 < 20
  assert.deepEqual(m.value('room'), { score: 50 }, 'a stale delta must not overwrite a newer leaf');
  m.applyDelta(delta(21, 'room/score', 51));                     // newer: applies
  assert.deepEqual(m.value('room'), { score: 51 });
});

// The tombstone half. Without rev-stamped tombstones the defence fails in exactly its target case:
// a late delta resurrecting deleted data.
test('tombstones: a late delta cannot resurrect a deleted subtree', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { a: 1, b: 2 }, 30);
  m.applyDelta(delta(31, 'room', null));                          // delete the subtree
  assert.equal(m.value('room'), null);
  m.applyDelta(delta(29, 'room/a', 1));                           // older than the delete
  assert.equal(m.value('room'), null, 'a delta older than the tombstone must not resurrect it');
  m.applyDelta(delta(32, 'room/a', 9));                           // newer than the delete
  assert.deepEqual(m.value('room'), { a: 9 });
});

test('snapshot LWW (v1.3): a leaf newer than the snapshot survives it', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applyDelta(delta(41, 'room/score', 99));
  m.applySnapshot('room', { score: 1, name: 'old' }, 40);         // reads OLDER than the delta
  assert.deepEqual(m.value('room'), { score: 99, name: 'old' },
    'the snapshot must not roll back a leaf the connection already applied at a higher rev');
});

test('ancestor scalar is cleared by a descendant write (§7, v1.4)', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', 'scalar', 50);
  assert.equal(m.value('room'), 'scalar');
  m.applyDelta(delta(51, 'room/child', 7));
  assert.deepEqual(m.value('room'), { child: 7 }, 'the server never emits a delta for the ancestor it replaced');
});

test('merge op: each key is a child put, and null children delete', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { a: 1, b: 2 }, 60);
  m.applyDelta(delta(61, 'room', { b: null, c: 3 }, 'merge'));
  assert.deepEqual(m.value('room'), { a: 1, c: 3 });
});

test('epoch change drops everything (§2 v1.5)', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { a: 1 }, 70);
  m.dropServerState();
  assert.equal(m.value('room'), null);
});

test('children() lists direct children only, leaves flagged', () => {
  const { Mirror } = loadMirror();
  const m = new Mirror();
  m.applySnapshot('room', { name: 'Ravi', stats: { wins: 3 } }, 80);
  const kids = m.children('room').map((k) => `${k.key}:${k.leaf}`);
  assert.deepEqual(kids, ['name:true', 'stats:false']);
});

/**
 * §5.9 Gate B REPEALED the read-only law this test used to assert, and replaced it with a narrower
 * one. The old form said "the console constructs no write frame, ever" and pinned the literal Set in
 * send(). Both halves are now false by design: an editor's console writes, and the allowlist moved
 * into the extracted `rtdb-wire` block so a test can reach the shipped copy of it.
 *
 * What survives is the part that was actually load-bearing, and it survives STRONGER: frame
 * construction happens in exactly ONE audited place, and it can only ever build a `put`. The UI
 * script — every line of it — still cannot name a write frame. So a future edit that quietly adds a
 * `merge` or a `cas` to a click handler fails here, which is what this test was always for.
 *
 * The role gate itself is tested where it lives: test/unit/console-wire.test.ts for what the page
 * offers, and test/integration/console-put.test.ts for what the GATEWAY does about it — the second
 * being the one that matters, because the wire is the law.
 */
test('write frames are built in ONE audited place, and only ever `put`', () => {
  const src = readFileSync(HTML, 'utf8');
  const wire = /<script id="rtdb-wire">([\s\S]*?)<\/script>/.exec(src)?.[1] ?? '';
  assert.ok(wire, 'the outbound rules must live in their own extractable block');

  // Everything after the wire block is UI. It may ask for a write; it may not construct one.
  const ui = src.split('<script id="rtdb-wire">')[1]?.split('</script>')[1] ?? '';
  for (const w of ['"put"', "'put'", '"merge"', "'merge'", '"cas"', "'cas'"]) {
    assert.ok(!ui.includes(`type: ${w}`), `only rtdb-wire may construct frames; the UI named ${w}`);
  }
  // And the one builder there is builds a put — never a merge, never a cas.
  for (const w of ['"merge"', "'merge'", '"cas"', "'cas'"]) {
    assert.ok(!wire.includes(`type: ${w}`), `the console must never construct a ${w} frame`);
  }
  assert.match(wire, /function outboundAllowed\(type, role\)/,
    'the allowlist is a function of the role, and it is the shipped one this test extracts');
  assert.doesNotMatch(src, /ALLOWED = new Set\(/,
    'send() must consult outboundAllowed, not carry a second copy of the rule');
});
