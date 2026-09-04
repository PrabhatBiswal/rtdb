import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { RtdbClient } from '../../harness/client.ts';
import { assertConverged, GatewayProcess, waitUntil } from '../../harness/scenario.ts';
import type { ServerFrame } from '../../src/protocol/frames.ts';
import { makeLimits } from '../../src/protocol/limits.ts';
import { goodToken } from '../helpers.ts';

/**
 * Gate D's durability proof. S10 already kills a gateway mid-traffic and shows the data comes back,
 * but it cannot tell "the shard survived" from "the shard was replaced and the client re-snapshotted
 * it" — both leave the same values on screen. That difference IS §2, so it gets asserted here:
 * the epoch must NOT move, and the client must resume from its lastRev without a second snapshot.
 */
const TOKEN = goodToken();
const LIMITS = makeLimits({ BACKOFF_CAP_MS: 40 });

test('§2 SIGKILL then restart: same epoch, data intact, resumed by lastRev', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'rtdb-durability-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  // Durable across restart: under Postgres that means the same schema, under memory the same file.
  const gw = await GatewayProcess.start({ BACKOFF_CAP_MS: 40 }, 0, join(dir, 'oplog.jsonl'));
  t.after(() => gw.stop());

  const c = new RtdbClient({ url: gw.url, token: TOKEN, limits: LIMITS, pingIntervalMs: 60_000 });
  t.after(() => c.close());
  c.connect();
  const before = await c.ready();

  let snapshots = 0;
  c.on('frame', (f: ServerFrame) => {
    if (f.type === 'snapshot') snapshots++;
  });

  c.listen('room');
  await c.put('room/a', 1);
  await waitUntil(() => c.value('room/a') === 1, 'the first write to land');
  assert.equal(snapshots, 1, 'the initial listen is served a snapshot');
  const headBefore = before.rev;

  await gw.kill(); // SIGKILL: no flush, no close frames, no chance to tidy up
  await gw.restart();
  const after = await c.ready();

  // §2: the epoch is the shard's promise that every rev it ever handed out still means what it did.
  assert.equal(after.epoch, before.epoch, 'a restart over surviving data is NOT a new generation');
  assert.ok(after.rev >= headBefore, `head went backwards: ${headBefore} -> ${after.rev}`);

  // ...and because the promise held, the client resumes where it was instead of re-reading the world.
  await waitUntil(() => c.value('room/a') === 1, 'the mirror still holds the pre-kill state');
  assert.equal(snapshots, 1, 'no second snapshot — the re-listen was served from its lastRev (§3)');

  await c.put('room/b', 2);
  await waitUntil(() => c.value('room/b') === 2, 'writes resume against the SAME generation');
  assert.deepEqual(c.value('room'), { a: 1, b: 2 }, 'nothing from before the kill was lost');
  await assertConverged([c], gw.url, TOKEN);
});

test('§9 the watermark survives the kill too, and still refuses a CAS below it', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'rtdb-durability-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  // A tiny retention so ordinary traffic drives the watermark up on its own.
  const gw = await GatewayProcess.start({ BACKOFF_CAP_MS: 40, OPLOG_RETENTION_REVS: 2 }, 0, join(dir, 'o.jsonl'));
  t.after(() => gw.stop());

  const c = new RtdbClient({ url: gw.url, token: TOKEN, limits: LIMITS, pingIntervalMs: 60_000 });
  t.after(() => c.close());
  c.connect();
  await c.ready();
  c.listen('room');
  for (const k of ['a', 'b', 'c', 'd']) await c.put(`room/${k}`, k);

  await gw.restart();
  await c.ready();

  // rev 1 is long pruned; a CAS against it cannot be proven safe and must fail conservatively (§4).
  const stale = await c.cas('room/a', 1, 'z');
  assert.equal(stale.type, 'casFail', 'a CAS below the restored watermark still fails conservatively');
  assert.deepEqual(c.value('room'), { a: 'a', b: 'b', c: 'c', d: 'd' }, 'and changed nothing');
});
