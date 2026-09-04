import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after } from 'node:test';
import pg from 'pg';
import { startGateway } from '../../src/gateway/server.ts';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { PostgresStorage } from '../../src/storage/postgres.ts';
import { RtdbClient } from '../../harness/client.ts';
import { goodToken, waitUntil, wsUrl } from '../helpers.ts';
import { createDatabase } from './helper.ts';

/**
 * WORKLOAD §5.11 Gate B: `connectionTimeoutMillis` on the pool.
 *
 * The starvation is produced the way production produces it — an external session holding
 * `rev_counter FOR UPDATE`, which is §4's own lock ordering. With `poolMax: 1` the storage's
 * transaction takes the only connection, blocks on that row lock, and every other acquisition
 * queues behind it. That is a genuinely exhausted pool, not a simulated one.
 */
const db = await createDatabase('starvation');
after(() => db.drop());

/** Take `rev_counter FOR UPDATE` on its own session and hold it until released. */
async function holdCounterLock(schema: string): Promise<() => Promise<void>> {
  const c = new pg.Client({ connectionString: db.url, options: `-c search_path=${schema}` });
  await c.connect();
  await c.query('BEGIN');
  await c.query('SELECT v FROM rev_counter WHERE shard = 0 FOR UPDATE');
  return async () => {
    await c.query('ROLLBACK').catch(() => undefined);
    await c.end().catch(() => undefined);
  };
}

test('§5.11 B2 (before/after): pg-pool queues FOREVER without the option, rejects with it', async () => {
  // The mechanism itself, straight from pg-pool: `index.js:206` skips the timeout branch when the
  // option is falsy, and only `:225` ever rejects. This is the "before" the gateway used to have.
  const raced = async (pool: pg.Pool): Promise<string> => {
    const hold = await pool.connect(); // max is 1, so the pool is now empty
    // The pending acquisition must ALWAYS be settled and released, even when the race is won by the
    // timer: otherwise it acquires the moment `hold` is released, nothing releases it, and
    // `pool.end()` waits for it forever — which hangs the test rather than failing it.
    const pending = pool.connect().then(
      (c) => {
        c.release();
        return 'acquired';
      },
      (e: Error) => `rejected: ${e.message}`,
    );
    const outcome = await Promise.race([
      pending,
      new Promise<string>((r) => setTimeout(() => r('STILL WAITING'), 1200)),
    ]);
    hold.release();
    await pending;
    return outcome;
  };

  const without = new pg.Pool({ connectionString: db.url, max: 1 });
  assert.equal(await raced(without), 'STILL WAITING', 'unset: the waiter never gives up — the hang');
  await without.end();

  const withIt = new pg.Pool({ connectionString: db.url, max: 1, connectionTimeoutMillis: 500 });
  assert.match(await raced(withIt), /^rejected: timeout exceeded/, 'set: it rejects, and can be caught');
  await withIt.end();
});

test('§5.11 B2: a starved pool rejects and the listen path repairs, instead of hanging', async (t) => {
  const schema = 's_starve';
  const storage = new PostgresStorage({ url: db.url, schema, poolMax: 1, limits: DEFAULT_LIMITS });
  // Forces schema creation before the external session tries to lock `rev_counter`.
  await storage.head();
  t.after(() => storage.close());

  const lines: string[] = [];
  const gw = await startGateway({ storage, log: (l) => lines.push(l) });
  t.after(() => gw.close());

  const mk = async (): Promise<RtdbClient> => {
    const c = new RtdbClient({ url: wsUrl(gw.port), token: goodToken(), pingIntervalMs: 60_000 });
    t.after(() => c.close());
    c.connect();
    await c.ready();
    return c;
  };
  const [a, b] = [await mk(), await mk()];

  const release = await holdCounterLock(schema);
  let released = false;
  t.after(async () => {
    if (!released) await release();
  });

  // This write takes the pool's only connection, BEGINs, and blocks on the row lock — holding the
  // connection for as long as the lock is held. That is exactly how production starves the pool:
  // stalled write transactions occupying every slot. It never settles, and see the note below.
  a.put('room/blocker', 1).catch(() => undefined);
  await new Promise((r) => setTimeout(r, 300)); // let it reach the lock and take the connection

  // With the pool now empty, a listen cannot acquire. WITHOUT connectionTimeoutMillis that wait was
  // unbounded and the subscription hung forever, silently. With it, the acquisition rejects at
  // 500ms, Gate A's containment takes over, and the sub is repaired via §3 resync and then, when
  // the repair budget runs out, abandoned WITH A LINE rather than left silent.
  a.listen('room/starved');
  await waitUntil(
    () => lines.some((l) => l.includes('listen-abandoned')),
    'the starved listen resolves and says so, instead of hanging',
    30_000,
  );

  // The gateway is up and BOTH connections are still live: one subscription's storage failure did
  // not close a socket or take the process down.
  //
  // Note what this deliberately does NOT assert. While the pool is fully held nothing can commit,
  // so "the neighbour keeps working" here means it stays connected and recovers — asserting that b
  // could write mid-starvation would be asserting a falsehood.
  assert.equal(b.state, 'connected', 'the neighbour was not dropped');
  assert.equal(a.state, 'connected', 'nor was the connection whose subscription was abandoned');

  // Recovery: release the lock and the neighbour commits normally again.
  released = true;
  await release();
  const ack = await b.put('room/after', 2);
  assert.equal(ack.type, 'ack', 'the gateway recovers the moment the pool frees up');
});

/**
 * FINDING, recorded not fixed (§5.11 Gate B report §Flagged). `connectionTimeoutMillis` bounds pool
 * ACQUISITION, not query execution — so it does not rescue the WRITE path from the fault above. The
 * blocked write is stuck inside its transaction on the row lock, holding its connection, and every
 * later write queues behind it in `#serialize`'s chain rather than at the pool, so it never
 * acquires, never times out, and never reaches Gate A's retry. Closing that needs a
 * `statement_timeout` or `lock_timeout`, which is out of this gate's scope.
 */
