import assert from 'node:assert/strict';
import test, { after } from 'node:test';
import { PostgresStorage } from '../../src/storage/postgres.ts';
import { createDatabase } from './helper.ts';

// One database for this file, created before any test is declared and dropped after all of them.
const db = await createDatabase('gatea');
after(() => db.drop());

test('a fresh store is empty: head 0, nothing pruned, null everywhere', async () => {
  const s = db.make();
  assert.equal(await s.head(), 0);
  assert.equal(await s.prunedThroughRev(), 0);
  assert.deepEqual(await s.readSnapshot(''), { value: null, rev: 0 });
  assert.deepEqual(await s.readSnapshot('a/b'), { value: null, rev: 0 });
});

test('§2: the epoch is persisted WITH the data — one database, one generation', async () => {
  const schema = 'shared_epoch';
  const first = db.make(undefined, schema);
  const epoch = await first.epoch();
  assert.ok(Number.isInteger(epoch) && epoch >= 1, `epoch ${epoch} must be a positive integer`);

  const second = db.make(undefined, schema);
  assert.equal(await second.epoch(), epoch, 'a second adapter over the same data joins its generation');

  // ...and a store that comes up without that past is exactly the reset the epoch announces.
  const wiped = await createDatabase('gatea_wiped');
  try {
    assert.notEqual(await wiped.make().epoch(), epoch, 'no persisted state means a NEW generation');
  } finally {
    await wiped.drop();
  }
});

test('the schema applies idempotently, even from adapters racing on one schema', async () => {
  const schema = 'racy';
  const stores = [db.make(undefined, schema), db.make(undefined, schema), db.make(undefined, schema)];
  const epochs = await Promise.all(stores.map((s) => s.epoch()));
  assert.equal(new Set(epochs).size, 1, `three concurrent applies, one epoch — got ${epochs}`);
  assert.deepEqual(await stores[0]?.head(), 0);
});

test('§3: readSnapshot takes value and rev from ONE consistent read', async () => {
  const schema = 'snap';
  const s = db.make(undefined, schema);
  await s.epoch(); // applies the schema
  const c = await db.client(schema);
  // Gate A has no write path yet, so the leaves are arranged directly — this is testing the READ.
  await c.query(`INSERT INTO nodes (path, value, rev) VALUES
      ('MPK_1010/1474396/name', '"Ravi"', 7),
      ('MPK_1010/1474396/score', '42', 7),
      ('MPK_1010/1474396/stats/wins', '3', 7),
      ('MPK_1010/1474396/tags', '[1,{"x":2},3]', 7),
      ('other/leaf', 'true', 7)`);
  await c.query('UPDATE rev_counter SET v = 7 WHERE shard = 0');

  assert.deepEqual(await s.readSnapshot('MPK_1010/1474396'), {
    rev: 7,
    value: { name: 'Ravi', score: 42, stats: { wins: 3 }, tags: [1, { x: 2 }, 3] },
  });
  assert.equal((await s.readSnapshot('MPK_1010/1474396/score')).value, 42);
  assert.deepEqual((await s.readSnapshot('MPK_1010/1474396/stats')).value, { wins: 3 });
  assert.deepEqual((await s.readSnapshot('')).value, {
    MPK_1010: { 1474396: { name: 'Ravi', score: 42, stats: { wins: 3 }, tags: [1, { x: 2 }, 3] } },
    other: { leaf: true },
  });
  assert.equal((await s.readSnapshot('nothing/here')).value, null);
});

test('`_` and `%` in a path are data, not LIKE wildcards', async () => {
  const schema = 'wild';
  const s = db.make(undefined, schema);
  await s.epoch();
  const c = await db.client(schema);
  await c.query(`INSERT INTO nodes (path, value, rev) VALUES
      ('MPK_1010/mine', '1', 1),
      ('MPKX1010/theirs', '2', 1),
      ('100%/mine', '3', 1),
      ('100x/theirs', '4', 1)`);

  assert.deepEqual((await s.readSnapshot('MPK_1010')).value, { mine: 1 }, 'MPK_1010 must not match MPKX1010');
  assert.deepEqual((await s.readSnapshot('100%')).value, { mine: 3 }, '100% must not match 100x');
});

test('an illegal schema name never reaches the DDL string', () => {
  assert.throws(() => new PostgresStorage({ url: db.url, schema: 'public; DROP DATABASE x' }), /illegal schema/);
});
