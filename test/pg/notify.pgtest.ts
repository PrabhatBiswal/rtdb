import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test, { after } from 'node:test';
import { sleep, waitUntil } from '../../harness/scenario.ts';
import type { GroupWrite } from '../../src/storage/adapter.ts';
import { createDatabase } from './helper.ts';

const db = await createDatabase('notify');
after(() => db.drop());

const put = (path: string, value: unknown): GroupWrite =>
  ({ writeId: randomUUID(), path, op: 'put', value: value as never });

test('§8: a commit on one connection wakes a LISTENer on another', async () => {
  const schema = 'crossconn';
  const writer = db.make(undefined, schema);
  const reader = db.make(undefined, schema);
  await reader.head(); // apply the schema before either side races

  let fired = 0;
  reader.onCommit(() => fired++);

  // LISTEN is established asynchronously, so the proof is "a commit eventually wakes it", not
  // "the first one does". Each round is a fresh writeId, so nothing dedups away.
  await waitUntil(async () => {
    await writer.commitGroup([put('p/score', Date.now())]);
    return fired > 0;
  }, 'a writer on another connection to wake the reader');

  // ...and it keeps waking: the poke is per-commit, not a one-shot.
  const before = fired;
  await waitUntil(async () => {
    await writer.commitGroup([put('p/score', Date.now())]);
    return fired > before;
  }, 'the second wake');
});

test('a commit does NOT wake its own adapter twice', async () => {
  const s = db.make(undefined, 'selffilter');
  let fired = 0;
  s.onCommit(() => fired++);
  await s.head(); // let LISTEN settle before the commit that must not double-fire
  await sleep(50);

  await s.commitGroup([put('a', 1)]);
  assert.equal(fired, 1, 'the local listener fires once, synchronously after COMMIT');

  // If our own NOTIFY were not filtered by sender PID, the second wake would land right about here.
  await sleep(150);
  assert.equal(fired, 1, 'and the NOTIFY we sent ourselves is not a second commit');
});
