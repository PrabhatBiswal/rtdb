import assert from 'node:assert/strict';
import test from 'node:test';
import type { Ack, Err } from '../../src/protocol/frames.ts';
import { harness, waitUntil } from '../helpers.ts';
import { signDevToken } from '../../src/gateway/auth.ts';

/**
 * §5.9 Gate A — the console's write guard, exercised over a real socket, because the claim it makes
 * is about the WIRE. A console session that may not write is refused by the gateway; the button the
 * UI does or does not draw is convenience, and convenience is not a guard.
 *
 * Two things must hold for a console write: the `role` claim says editor or owner, AND the subject
 * is the deliberate-unlock `console-rw-…` that §5.9 Gate B mints only for those roles. Each half is
 * tested alone, because a guard that only checks one is a guard with a hole in the other.
 */
const token = (sub: string, role?: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, ...(role ? { role } : {}) });

const refused = async (p: Promise<unknown>): Promise<Err> => {
  try {
    await p;
  } catch (e) {
    return e as Err;
  }
  throw new Error('expected the write to be refused');
};

test('a viewer console session is refused at the wire, not at the button', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ token: token('console-asha', 'viewer') });
  const err = await refused(c.put('demo/roles', 'nope'));
  assert.equal(err.code, 'RULES');
  assert.match(err.msg, /may not write/);
  // And nothing landed.
  assert.equal((await gw.storage.readSnapshot('demo/roles')).value, null);
});

test('an editor on the unlock subject writes, and it lands', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ token: token('console-rw-asha', 'editor') });
  const ack = (await c.put('demo/roles', 'yes')) as Ack;
  assert.equal(ack.type, 'ack');
  assert.deepEqual((await gw.storage.readSnapshot('demo/roles')).value, 'yes');
});

test('an owner on the unlock subject writes too', async (t) => {
  const { connect } = await harness(t);
  const c = await connect({ token: token('console-rw-boss', 'owner') });
  assert.equal((await c.put('demo/roles', 1)).type, 'ack');
});

test('the role alone is not enough: editor on a plain console- subject is refused', async (t) => {
  // Our auth-server cannot mint this pairing, so it is a bug or a forgery. Either way, not a write.
  const { connect } = await harness(t);
  const c = await connect({ token: token('console-asha', 'editor') });
  assert.equal((await refused(c.put('demo/roles', 'nope'))).code, 'RULES');
});

test('the subject alone is not enough: viewer on an rw subject is refused', async (t) => {
  const { connect } = await harness(t);
  const c = await connect({ token: token('console-rw-asha', 'viewer') });
  assert.equal((await refused(c.put('demo/roles', 'nope'))).code, 'RULES');
});

test('a console session with no role claim at all is refused', async (t) => {
  const { connect } = await harness(t);
  const c = await connect({ token: token('console-legacy') });
  assert.equal((await refused(c.put('demo/roles', 'nope'))).code, 'RULES');
});

/**
 * BLAST RADIUS, which is the property this package must not get wrong. Removing the guard leaves
 * this test green — it was always allowed — so what it pins is not the guard's existence but its
 * REACH. The mutation that turns it red is the over-broad one: denying every write that carries no
 * write role, rather than only the console's own subjects. That mutation is the one that would take
 * a consuming app down, so it is the one with a test standing in front of it.
 */
test('app tokens are untouched: no role, not a console subject, writes normally', async (t) => {
  const { gw, connect } = await harness(t);
  const c = await connect({ token: token('u_1474396') });
  const ack = (await c.put('MPK_1010/1474396', { score: 42 })) as Ack;
  assert.equal(ack.type, 'ack');
  assert.deepEqual((await gw.storage.readSnapshot('MPK_1010/1474396')).value, { score: 42 });
});

test('app tokens keep writing even if one carries a role claim of its own', async (t) => {
  // `role` is not a reserved word in somebody else's IdP. A subject that is not ours is not ours.
  const { connect } = await harness(t);
  const c = await connect({ token: token('u_1474396', 'viewer') });
  assert.equal((await c.put('MPK_1010/1474396', 1)).type, 'ack');
});

test('reads are unchanged for every console session (§3 is not what this guards)', async (t) => {
  const { connect } = await harness(t);
  const writer = await connect({ token: token('console-rw-boss', 'owner') });
  await writer.put('demo/readable', { seen: true });

  const viewer = await connect({ token: token('console-asha', 'viewer') });
  const values: unknown[] = [];
  // listen() hands back an unsubscribe function; the snapshot arrives later, over the socket.
  viewer.listen('demo/readable', (v) => values.push(v));
  await waitUntil(() => values.length > 0);
  assert.deepEqual(values.at(-1), { seen: true });
});

test('one audit line per console write attempt, carrying the verdict and never the value', async (t) => {
  const lines: string[] = [];
  const { connect } = await harness(t, { log: (l) => lines.push(l) });

  const viewer = await connect({ token: token('console-asha', 'viewer') });
  await refused(viewer.put('demo/secret', { password: 'hunter2' }));
  const editor = await connect({ token: token('console-rw-boss', 'editor') });
  await editor.put('demo/secret', { password: 'hunter2' });

  const audit = lines
    .map((l) => JSON.parse(l) as Record<string, unknown>)
    .filter((e) => e['ev'] === 'console-write');
  assert.equal(audit.length, 2, 'one line per attempt, denied and allowed alike');

  const denied = audit.find((e) => e['allowed'] === false);
  assert.ok(denied, 'the denied attempt is the one worth having');
  assert.equal(denied['sub'], 'console-asha');
  assert.equal(denied['op'], 'put');
  assert.equal(denied['path'], 'demo/secret');
  assert.equal(denied['role'], 'viewer');

  // The value never appears — not in the fields, and not anywhere in the serialized line.
  for (const l of lines) assert.ok(!l.includes('hunter2'), `a written value reached the log: ${l}`);
  assert.ok(!('value' in (denied as object)));
});

test('an app write writes no audit line at all — the volume stays bounded by construction', async (t) => {
  const lines: string[] = [];
  const { connect } = await harness(t, { log: (l) => lines.push(l) });
  const c = await connect({ token: token('u_1474396') });
  await c.put('MPK_1010/1474396', 1);
  const audit = lines.map((l) => JSON.parse(l) as Record<string, unknown>).filter((e) => e['ev'] === 'console-write');
  assert.equal(audit.length, 0);
});
