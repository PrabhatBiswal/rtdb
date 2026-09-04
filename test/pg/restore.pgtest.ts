import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { type TestContext } from 'node:test';
import pg from 'pg';
import { RtdbClient } from '../../harness/client.ts';
import { PG_URL } from '../../harness/pg.ts';
import { GatewayProcess, waitUntil } from '../../harness/scenario.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import { goodToken } from '../helpers.ts';

/**
 * docs/wp4-restore-drill.md, automated. The hand-run drill proves the PROCEDURE works; this proves
 * the CONTRACT the procedure exists to satisfy — and the second test proves the contract is not
 * decoration by showing exactly how a client diverges when the epoch bump is skipped.
 */
const TOKEN = goodToken();
const LIMITS = makeLimits({ BACKOFF_CAP_MS: 40 });

/** A point-in-time restore, simulated where it counts: the head moves BACKWARDS. */
async function restoreTo(schema: string, rev: number, bumpEpoch: boolean): Promise<void> {
  const c = new pg.Client({ connectionString: PG_URL, options: `-c search_path=${schema}` });
  await c.connect();
  try {
    await c.query('BEGIN');
    await c.query('DELETE FROM oplog WHERE rev > $1', [rev]);
    await c.query('DELETE FROM nodes WHERE rev > $1', [rev]);
    await c.query(
      bumpEpoch
        ? `UPDATE rev_counter SET v = $1, epoch = (floor(random() * 2147483646) + 1)::bigint WHERE shard = 0`
        : `UPDATE rev_counter SET v = $1 WHERE shard = 0`,
      [rev],
    );
    await c.query('COMMIT');
  } finally {
    await c.end();
  }
}

async function shardWithTwoWrites(t: TestContext): Promise<{ gw: GatewayProcess; c: RtdbClient; epoch: number }> {
  const dir = mkdtempSync(join(tmpdir(), 'rtdb-restore-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const gw = await GatewayProcess.start({ BACKOFF_CAP_MS: 40 }, 0, join(dir, 'o.jsonl'));
  t.after(() => gw.stop());

  const c = new RtdbClient({ url: gw.url, token: TOKEN, limits: LIMITS, pingIntervalMs: 60_000 });
  t.after(() => c.close());
  c.connect();
  const hello = await c.ready();
  c.listen('room');
  await c.put('room/alpha', 1); // rev 1 — inside the backup
  await c.put('room/beta', 2); // rev 2 — written after it, and about to be lost
  await waitUntil(() => c.value('room/beta') === 2, 'both writes to land');
  return { gw, c, epoch: hello.epoch as number };
}

test('§2 restore + epoch bump: a live client drops the dead generation and resnapshots', async (t) => {
  const { gw, c, epoch } = await shardWithTwoWrites(t);
  const schema = gw.schema;
  assert.ok(schema, 'this suite runs on Postgres');

  await gw.kill();
  await restoreTo(schema, 1, true); // the drill: rewind to the backup, then BUMP
  await gw.restart();

  await waitUntil(() => c.epoch !== null && c.epoch !== epoch, 'a new epoch on helloAck');
  // Wait for the RE-SNAPSHOT, not for the drop: between the two the mirror is legitimately empty,
  // and asserting in that window tests nothing but my own timing.
  await waitUntil(() => c.value('room/alpha') === 1, 'a fresh snapshot of the restored shard');
  assert.deepEqual(
    c.value('room'),
    { alpha: 1 },
    'the client mirrors the RESTORED shard — beta was dropped and the snapshot did not bring it back',
  );

  await c.put('room/gamma', 3);
  await waitUntil(() => c.value('room/gamma') === 3, 'writes resume against the new generation');
  assert.deepEqual(c.value('room'), { alpha: 1, gamma: 3 });
});

test('§2 the same restore WITHOUT the bump diverges the client silently — this is why step 6 exists', async (t) => {
  const { gw, c, epoch } = await shardWithTwoWrites(t);
  const schema = gw.schema as string;

  await gw.kill();
  await restoreTo(schema, 1, false); // the mistake: same rewind, epoch left alone
  await gw.restart();
  await c.ready();
  await waitUntil(async () => (await serverRoom(schema)) !== null, 'the restored shard to be readable');

  assert.equal(c.epoch, epoch, 'the client is told nothing changed');
  // §3 v1.4 sends a fresh snapshot (lastRev is above the restored head), but §7's per-leaf LWW keeps
  // any leaf whose recorded rev is newer than the snapshot's — and rev 2 is "newer" than rev 1 in a
  // generation that no longer exists. The client keeps a value the server does not have.
  assert.equal(c.value('room/beta'), 2, 'the client still shows a write the restored shard never saw');
  assert.deepEqual(await serverRoom(schema), { alpha: 1 }, 'while the server has only the restored state');
});

/** The server's own view, read straight from `nodes` — no client, no mirror, no room for argument. */
async function serverRoom(schema: string): Promise<unknown> {
  const c = new pg.Client({ connectionString: PG_URL, options: `-c search_path=${schema}` });
  await c.connect();
  try {
    const { rows } = await c.query<{ path: string; value: unknown }>(
      `SELECT path, value FROM nodes WHERE path LIKE 'room/%' ORDER BY path`,
    );
    if (rows.length === 0) return null;
    return Object.fromEntries(rows.map((r) => [r.path.slice('room/'.length), r.value]));
  } finally {
    await c.end();
  }
}
