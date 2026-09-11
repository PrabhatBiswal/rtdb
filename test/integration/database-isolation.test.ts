import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import type { Ack, Err } from '../../src/protocol/frames.ts';
import { harness, waitUntil } from '../helpers.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import type { GatewayOptions } from '../../src/gateway/server.ts';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';

/**
 * §5.20 Phase 1 — a token's database is a wall, exercised over a real socket.
 *
 * Over the WIRE for the same reason §5.9's tests are: the claim being made is about what the
 * gateway refuses, and a unit test of the predicate would pass just as happily if nobody had wired
 * it into `listen` or the write pipeline.
 *
 * TWO gateways for the whole file and BOTH ON MEMORY STORAGE, in `test:pg` as well as `npm test`.
 * Neither is a shortcut and neither is load-bearing any more — `7c3de46` bounded `test:pg`'s file
 * concurrency, which is the right lever for the suite as a whole. They stay because they are still
 * the cheaper way to assert what this file asserts.
 *
 * Memory, because every refusal here is decided in `write.ts`'s `#validate` and in `server.ts`'s
 * `listen` case — BEFORE any adapter is touched. A refused write reaches no storage at all, so a
 * Postgres-backed run of this file would assert exactly the same things while holding a pool and a
 * dedicated LISTEN connection to do it.
 *
 * Two gateways rather than fourteen, with the cases hanging off them as subtests: subtests still
 * pass and fail individually, so every refusal keeps its own teeth; what they no longer each get is
 * a gateway.
 *
 * Both choices came out of writing this the obvious way first — one `harness()` per case, thirteen
 * gateways — and watching `test:pg` fail with `53300 sorry, too many clients already` in files that
 * had nothing to do with this change. That is worth writing down where it will be read again,
 * because it is not really a test problem: the shape that blew the budget is one adapter = one pool
 * + one dedicated LISTEN connection, which is exactly what §5.20 Phase 3 proposes to multiply by
 * the number of databases.
 */
const appToken = (sub: string, ns?: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, ...(ns ? { ns } : {}) });

const consoleToken = (sub: string, role: string): string =>
  signDevToken({ sub, role, exp: Math.floor(Date.now() / 1000) + 3600 });

const refused = async (p: Promise<unknown>): Promise<Err> => {
  try {
    await p;
  } catch (e) {
    return e as Err;
  }
  throw new Error('expected the write to be refused');
};

/** `helpers.ts`'s `harness`, pinned to memory storage — see the note at the top of this file. */
const memHarness = (
  t: TestContext,
  opts: GatewayOptions = {},
): ReturnType<typeof harness> =>
  harness(t, { ...opts, storage: new MemoryStorage(opts.limits ?? DEFAULT_LIMITS) });

/** The first sub-scoped err this client sees, or a timeout. */
async function subErr(c: { on: (ev: string, fn: (e: Err) => void) => void }): Promise<Err> {
  const errs: Err[] = [];
  c.on('subError', (e: Err) => errs.push(e));
  await waitUntil(() => errs.length > 0, 'a sub-scoped err');
  return errs[0] as Err;
}

test('§5.20 Phase 1: the wall, with requireNs ON', async (t: TestContext) => {
  // `rules: () => true` is not decoration: it is an operator-written module that says YES to
  // everything, which is exactly what one is allowed to do. Every refusal below is therefore a
  // refusal the configured rules actively voted against and could not carry.
  const { gw, connect } = await memHarness(t, { requireNs: true, rules: () => true });
  const scoped = (): Promise<{
    put: (p: string, v: unknown) => Promise<unknown>;
    listen: (p: string, cb?: (v: unknown) => void) => unknown;
    on: (ev: string, fn: (e: Err) => void) => void;
  }> => connect({ token: appToken('app-car-web', 'car') }) as never;

  // The positive control comes FIRST. Without it every case below would still pass on a gateway
  // that refused absolutely everything, which is the failure mode a wall is most likely to have.
  await t.test('a scoped token works normally inside its own database', async () => {
    const c = await scoped();
    assert.equal(((await c.put('car/players/p1', { score: 1 })) as Ack).type, 'ack');
    assert.deepEqual((await gw.storage.readSnapshot('car/players/p1')).value, { score: 1 });
    const seen: unknown[] = [];
    c.listen('car/players', (v) => seen.push(v));
    await waitUntil(() => seen.length > 0, 'a snapshot inside the own database');
  });

  await t.test('the database node itself is inside the wall, not just what is under it', async () => {
    const c = await scoped();
    assert.equal(((await c.put('car/open', true)) as Ack).type, 'ack');
    const seen: unknown[] = [];
    c.listen('car', (v) => seen.push(v));
    await waitUntil(() => seen.length > 0, 'a snapshot of the database node');
  });

  await t.test('a write outside the token database is refused', async () => {
    const c = await scoped();
    const err = await refused(c.put('chat/rooms/r1', 'nope'));
    assert.equal(err.code, 'RULES');
    assert.match(err.msg, /outside this token's database/);
    assert.equal((await gw.storage.readSnapshot('chat/rooms/r1')).value, null, 'and nothing landed');
  });

  await t.test('a listen outside the token database is a sub-scoped RULES err', async () => {
    // §3 authorizes a subscription ONCE. A read that got through here would keep delivering another
    // database's deltas for the life of the connection, with nothing checking it again.
    const c = await scoped();
    c.listen('chat/rooms');
    const err = await subErr(c);
    assert.equal(err.code, 'RULES');
    assert.match(err.msg, /outside this token's database/);
  });

  await t.test('a scoped token cannot listen at the root, and not because of a size limit', async () => {
    // The default SNAPSHOT_MAX is 4 MiB and this tree holds a handful of bytes, so TOOBIG cannot be
    // what refuses this — which is the state a freshly reset shard is actually in.
    const c = await scoped();
    c.listen('');
    assert.equal((await subErr(c)).code, 'RULES', 'RULES, not TOOBIG');
  });

  await t.test('a `car` token cannot reach `car_race` — the prefix trap, on writes', async () => {
    // `path.startsWith(ns)` would let this through. Same trap `rules/own-subtree.ts:32` carries for
    // `u_1` and `u_12`, and it reads correct right up until two names share a prefix.
    const c = await scoped();
    assert.equal((await refused(c.put('car_race/track', 1))).code, 'RULES');
    assert.equal((await gw.storage.readSnapshot('car_race/track')).value, null);
  });

  await t.test('a `car` token cannot reach `car_race` — the prefix trap, on listen', async () => {
    const c = await scoped();
    c.listen('car_race');
    assert.equal((await subErr(c)).code, 'RULES');
  });

  await t.test('absence is not permission: an unscoped token is refused, read and write', async () => {
    // Every mint signs with the SAME shard secret — /login, /shadow-token, /app-token,
    // scripts/console-token.ts — so if "no ns" meant "every database", the weakest of those mints
    // would be an unconfined token. A roleless 24h device token already got through /stats and
    // /topnodes once on exactly this shape (console/auth-server.mjs).
    const c = (await connect({ token: appToken('device-pixel7') })) as never as Awaited<
      ReturnType<typeof scoped>
    >;
    assert.equal((await refused(c.put('car/x', 1))).code, 'RULES');
    c.listen('car');
    assert.equal((await subErr(c)).code, 'RULES');
  });

  // The console must not break. §5.9 already decides what a console session may write, and a
  // console has no ONE database — it administers all of them. If the wall applied to `console-…`
  // subjects the console would be locked out of the tree it exists to administer, which is the trap
  // `rules/own-subtree.ts` documents at its top.
  await t.test('a console editor still writes anywhere', async () => {
    const c = await connect({ token: consoleToken('console-rw-asha', 'editor') });
    assert.equal(((await c.put('chat/rooms/r2', 'yes')) as Ack).type, 'ack');
    assert.deepEqual((await gw.storage.readSnapshot('chat/rooms/r2')).value, 'yes');
  });

  await t.test('a console viewer still reads the whole tree, root included', async () => {
    const c = await connect({ token: consoleToken('console-asha', 'viewer') });
    const seen: unknown[] = [];
    c.listen('', (v) => seen.push(v));
    await waitUntil(() => seen.length > 0, 'the console sidebar still loads');
  });

  // R1 (checkpoint #1): the exemption costs a KNOWN role. A prefix is not a credential.
  await t.test('a roleless console- subject is refused, read and write', async () => {
    // Nothing mints this today — /login always sends a role, console-token.ts defaults to owner,
    // /shadow-token's subject is a deviceSlug and /app-token's is app-…. So it is either a bug or a
    // forgery, and neither buys a read of every database.
    const c = await connect({ token: appToken('console-ghost') });
    assert.equal((await refused(c.put('chat/x', 1))).code, 'RULES');
    c.listen('chat');
    assert.equal((await subErr(c)).code, 'RULES');
  });

  await t.test('an UNKNOWN role does not buy the exemption either', async () => {
    // `admin` is not one of owner/editor/viewer. A role our console cannot mint is worth exactly as
    // much as no role at all — the same rule auth-server applies to itself at `consoleUser`.
    const c = await connect({ token: consoleToken('console-asha', 'admin') });
    c.listen('chat');
    assert.equal((await subErr(c)).code, 'RULES');
  });

  await t.test('a console viewer is still refused a write, by §5.9 rather than by the wall', async () => {
    const c = await connect({ token: consoleToken('console-asha', 'viewer') });
    const err = await refused(c.put('chat/x', 1));
    assert.equal(err.code, 'RULES');
    assert.match(err.msg, /console session may not write/, 'the §5.9 message, not the database one');
  });
});

test('§5.20 Phase 1: with requireNs OFF, the single-database deployment is unchanged', async (t: TestContext) => {
  const { connect } = await memHarness(t);

  await t.test('an unscoped token still reads and writes the whole tree', async () => {
    // The entire existing suite and production `c0337c4` live in this branch. If this fails,
    // Phase 1 broke a deployment that has no databases in it.
    const c = await connect({ token: appToken('u_test') });
    assert.equal(((await c.put('anything/at/all', 1)) as Ack).type, 'ack');
    const seen: unknown[] = [];
    c.listen('', (v) => seen.push(v));
    await waitUntil(() => seen.length > 0, 'a root read still works');
  });

  await t.test('a roleless console- subject still reads, because OFF means unchanged', async () => {
    // R1's role requirement is deliberately scoped to `requireNs`. With the switch off an unscoped
    // token of ANY subject already reads the whole tree, so refusing this one would not close a
    // hole — it would invent a refusal no other subject faces.
    const c = await connect({ token: appToken('console-ghost') });
    const seen: unknown[] = [];
    c.listen('anything', (v) => seen.push(v));
    await waitUntil(() => seen.length > 0, 'a read that is still allowed');
  });

  await t.test('a scoped token is confined anyway — naming a database IS the constraint', async () => {
    // The switch only decides what SILENCE means. A token that names one is confined either way.
    const c = await connect({ token: appToken('app-car-web', 'car') });
    assert.equal((await refused(c.put('chat/x', 1))).code, 'RULES');
  });
});
