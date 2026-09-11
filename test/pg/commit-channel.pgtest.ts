import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import {
  CommitListener,
  commitChannel,
  createPool,
  MAX_SCHEMA_NAME,
  PostgresStorage,
} from '../../src/storage/postgres.ts';
import { createDatabase } from './helper.ts';
import { waitUntil } from '../../harness/scenario.ts';

/**
 * §5.22 Gate C — a commit wakes its OWN tenant's dispatcher and nobody else's.
 *
 * The measurement this gate exists for was taken before it: two adapters, two schemas, ONE Postgres
 * database, 5 commits in A produced 5 wake-ups in B while B's oplog was empty. LISTEN/NOTIFY is
 * scoped to the database, not the schema, so the literal `rtdb_commit` channel made every commit
 * O(N) on a shard already at 96% of its lock ceiling. These are that measurement, kept.
 *
 * `onCommit` fires for a notification from ANOTHER process; a gateway's own commits wake its local
 * listeners synchronously and are filtered by backend PID. So every test here needs a SECOND
 * adapter to play the other gateway — one adapter writing to itself proves nothing about the wire.
 */
const db = await createDatabase('gatec');
const open: PostgresStorage[] = [];
const listeners: CommitListener[] = [];
const pool = createPool(db.url, 8);

// ONE teardown hook, so the ORDER is explicit rather than left to the runner's. It has to be:
// adapters, then any listener they were sharing, then the pool, and only then the database. Getting
// it wrong is loud in a way worth naming — `db.drop()` uses `WITH (FORCE)`, so a listener still
// open when it runs is TERMINATED by the server, its reconnect loop fires against a database that
// no longer exists, and the retries surface as `uncaughtException` attributed to whichever test
// created it. The suite reports a file-level failure and names an innocent test.
after(async () => {
  await Promise.allSettled(open.map((s) => s.close()));
  await Promise.allSettled(listeners.map((l) => l.close()));
  await pool.end().catch(() => undefined);
  await db.drop();
});

/** A tenant on the shared pool, optionally on a shared listener. */
const tenant = (schema: string, listener?: CommitListener): PostgresStorage => {
  const s = new PostgresStorage({
    url: db.url,
    schema,
    controlSchema: 'c_ctl',
    pool,
    ...(listener ? { listener } : {}),
  });
  open.push(s);
  return s;
};

const sharedListener = (): CommitListener => {
  const l = new CommitListener(db.url);
  listeners.push(l);
  return l;
};

const put = (s: PostgresStorage, path: string): Promise<unknown> =>
  s.commitGroup([{ writeId: crypto.randomUUID(), path, op: 'put', value: 1 }]);

test("a commit does not wake another tenant's dispatcher", async () => {
  // THE measurement. Before this gate: 5 commits in A, 5 wake-ups in B, 0 rows in B's oplog.
  const writerA = tenant('gc_a');
  const readerB = tenant('gc_b');
  const readerBsPeer = tenant('gc_b'); // the "other gateway" holding B — the one that listens

  let wokeB = 0;
  readerBsPeer.onCommit(() => {
    wokeB++;
  });
  await readerB.head();
  await new Promise((r) => setTimeout(r, 300)); // let the LISTEN land before anything commits

  for (let i = 0; i < 5; i++) await put(writerA, `a/${i}`);
  await new Promise((r) => setTimeout(r, 600));

  assert.equal(wokeB, 0, "A's commits must not reach B");
  assert.equal((await readerB.readOplogSince(0, 100)).length, 0, "and B's oplog is empty, as it was");
  assert.equal(await writerA.head(), 5, "while A's own commits all landed");
});

test('a commit DOES wake another gateway on the same tenant', async () => {
  // The positive control, and without it the test above passes on a listener that never works.
  const gw1 = tenant('gc_same');
  const gw2 = tenant('gc_same');

  let woke = 0;
  gw2.onCommit(() => {
    woke++;
  });
  await gw2.head();
  await new Promise((r) => setTimeout(r, 300));

  await put(gw1, 'shared/x');
  await waitUntil(() => woke > 0, "the other gateway's dispatcher was poked");
});

test('ONE listener carries N tenants — the other half of the same edit', async () => {
  // Qualifying the channel is what lets one client hold every tenant's channel, which is why this
  // gate kills the amplification and the N-connections cost together. That single fact is why (f)
  // came out SCHEMA: it was the one place the database branch was winning.
  const listener = sharedListener();
  const writers = Array.from({ length: 6 }, (_, i) => tenant(`gc_m${i}`));
  const watchers = Array.from({ length: 6 }, (_, i) => tenant(`gc_m${i}`, listener));

  const woke = watchers.map(() => 0);
  watchers.forEach((w, i) => w.onCommit(() => { woke[i] = (woke[i] ?? 0) + 1; }));
  await Promise.all(watchers.map((w) => w.head()));
  await new Promise((r) => setTimeout(r, 400));

  // One tenant commits. Exactly one watcher may wake, on ONE shared connection.
  await put(writers[3] as PostgresStorage, 'only/here');
  await waitUntil(() => (woke[3] ?? 0) > 0, 'the right tenant woke');
  await new Promise((r) => setTimeout(r, 400));

  assert.deepEqual(
    woke.map((n) => (n > 0 ? 1 : 0)),
    [0, 0, 0, 1, 0, 0],
    'one commit, one tenant woken, six tenants on one listener',
  );
});

/**
 * Kill the listener's own backend from outside, the way an RDS failover or an idle-connection
 * reaper takes it — with no cooperation from us. Targeted by `application_name`, and asserted to be
 * exactly one: the first version of this killed every `LISTEN %` connection in the database, which
 * meant the test was terminating the OTHER tests' listeners.
 */
async function killListener(appName: string): Promise<void> {
  const c = await db.client();
  const { rowCount } = await c.query(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1`,
    [appName],
  );
  assert.equal(rowCount, 1, 'exactly one backend killed — this listener, nobody else');
}

test('a commit made INSIDE the gap still reaches the dispatcher', async () => {
  // Reconnection is part of THIS gate rather than a later one, and the reason changed at Gate B: a
  // dropped connection used to blind ONE dispatcher in a deployment where local listeners carried
  // every wake-up anyway. One client carrying N tenants blinds N, and past Gate D a second
  // gateway's commits arrive ONLY this way.
  //
  // This half is the POKE: the commit happens while there is no connection at all, so its NOTIFY is
  // raised into a void and no notification can ever carry it. Only the unconditional poke on
  // reconnect can get here.
  // `retryMs` is deliberately LONG here, and it is the test's own correctness rather than tuning:
  // the commit below must land while there is no connection, and at 100 ms a slow machine could
  // reconnect first — the NOTIFY would then arrive on a re-LISTENed channel, the poke would never
  // be needed, and this tooth would go quietly GREEN with the poke deleted. Not a false red, a
  // false green. One second makes "inside the gap" structural instead of a race the test wins.
  const listener = new CommitListener(db.url, 1000, 'gate-c-gap-probe');
  listeners.push(listener);
  const gw1 = tenant('gc_gap');
  const gw2 = tenant('gc_gap', listener);

  let woke = 0;
  gw2.onCommit(() => { woke++; });
  await gw2.head();
  await new Promise((r) => setTimeout(r, 300));

  await killListener('gate-c-gap-probe');
  await put(gw1, 'inside/the/gap');   // NOTIFY raised while nobody is listening
  await waitUntil(() => woke > 0, 'the reconnect poke carried what the gap swallowed');
});

test('and the reconnected listener is still LISTENING — a LATER commit wakes it too', async () => {
  // The witness for the OTHER mechanism, and the gate needs both. The test above is satisfied by a
  // listener that comes back DEAF: one poke, then silence forever. This one is not — the poke is
  // observed and then discounted, and a commit made AFTER the connection is back can only arrive as
  // a real notification on a re-`LISTEN`ed channel.
  const listener = new CommitListener(db.url, 100, 'gate-c-relisten-probe');
  listeners.push(listener);
  const gw1 = tenant('gc_relisten');
  const gw2 = tenant('gc_relisten', listener);

  let woke = 0;
  gw2.onCommit(() => { woke++; });
  await gw2.head();
  await new Promise((r) => setTimeout(r, 300));

  await killListener('gate-c-relisten-probe');
  await waitUntil(() => woke > 0, 'the reconnect happened at all');

  // Everything above this line is the previous test. Discount it and start again.
  woke = 0;
  await new Promise((r) => setTimeout(r, 200));
  await put(gw1, 'after/the/reconnect');
  await waitUntil(() => woke > 0, 'a commit made AFTER the reconnect still arrives');
});

test('a subscription that resolves after close() is not kept', async () => {
  // `onCommit` calls `#listen()` UN-AWAITED, so `close()` can land inside its await — and when it
  // does, the placeholder it stops is not the real subscription. Measured before the fix: the
  // closed adapter still fired on the next commit, and the shared listener held its callback (and
  // through it the adapter) for as long as the listener lived. That is `8d3be3a`'s pin again, one
  // `await` higher up.
  //
  // The live adapter is the control: without it, "closed fired 0" would also pass on a listener
  // that had stopped delivering to anyone.
  const listener = new CommitListener(db.url, 200, 'gate-c-race-probe');
  listeners.push(listener);
  const writer = tenant('gc_race');
  const doomed = tenant('gc_race', listener);
  const live = tenant('gc_race', listener);
  await Promise.all([writer.head(), doomed.head(), live.head()]);

  let doomedFired = 0;
  let liveFired = 0;
  live.onCommit(() => { liveFired++; });
  await new Promise((r) => setTimeout(r, 300));

  // The race, exactly: subscribe, then close without awaiting the subscription.
  doomed.onCommit(() => { doomedFired++; });
  await doomed.close();
  await new Promise((r) => setTimeout(r, 200));

  await put(writer, 'race/x');
  await waitUntil(() => liveFired > 0, 'the live adapter on the same listener still hears it');
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(doomedFired, 0, 'a closed adapter must hear nothing');
});

test('a schema name long enough to truncate its channel is refused', () => {
  // Postgres cuts an identifier at 63 BYTES with a NOTICE, not an error. Measured: `LISTEN` on a
  // 64-char channel then `NOTIFY` on another that differs only in its last character DELIVERS, and
  // the notification names the 63-byte form. Two tenants sharing a long enough prefix would
  // therefore share one channel and cross-wake — this gate's own defect, back and silent.
  //
  // Asserted through the CONSTRUCTOR, not against `MAX_SCHEMA_NAME`: the bound is only worth
  // anything where a name is actually refused. `51` is never written down — the limit derives from
  // the channel prefix, so the two cannot drift apart.
  const longest = 'a'.repeat(MAX_SCHEMA_NAME);
  const tooLong = 'a'.repeat(MAX_SCHEMA_NAME + 1);
  assert.equal(commitChannel(longest).length, 63, 'the longest legal name fills the identifier exactly');
  assert.doesNotThrow(() => new PostgresStorage({ url: db.url, schema: longest }));
  assert.throws(() => new PostgresStorage({ url: db.url, schema: tooLong }), /illegal schema name/);
  // The control schema becomes no channel, but it shares the guard: one rule, both names.
  assert.throws(
    () => new PostgresStorage({ url: db.url, schema: 'ok_name', controlSchema: tooLong }),
    /illegal control schema name/,
  );
});

test('the channel is derived from the schema, and the schema guard is what makes it safe', () => {
  // `NOTIFY` takes an identifier, not a parameter, so the constructor's guard is what stands
  // between a schema name and an interpolated statement. A second check beside the first is how two
  // rules drift.
  assert.notEqual(commitChannel('a'), commitChannel('b'));
  assert.throws(() => new PostgresStorage({ url: db.url, schema: 'a; NOTIFY evil' }), /illegal schema/);
});
