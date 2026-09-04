import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { storageSemantics } from '../storage-suite.ts';

storageSemantics('MemoryStorage', (limits) => new MemoryStorage(limits));

test('§2 v1.5: the epoch survives a restart WITH persistence and changes without it', async (t) => {
  const dir = mkdtempSync(join(tmpdir(), 'rtdb-epoch-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = join(dir, 'oplog.jsonl');

  const first = new MemoryStorage(undefined, file);
  await first.commitGroup([{ writeId: '0d0e1f2a-3b4c-4d5e-8f90-a1b2c3d4e5f6', path: 'a', op: 'put', value: 1 }]);

  const restored = new MemoryStorage(undefined, file);
  assert.equal(await restored.epoch(), await first.epoch(), 'same data, same generation');
  assert.equal(await restored.head(), 1, 'and the oplog replayed as before');

  // No persisted state: the head is back to 0, so every rev the shard ever promised is dead.
  const reset = new MemoryStorage();
  assert.notEqual(await reset.epoch(), await first.epoch(), 'a broken rev promise means a new epoch');
});
