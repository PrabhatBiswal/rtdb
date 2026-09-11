import assert from 'node:assert/strict';
import test from 'node:test';
import type { StorageAdapter } from '../src/storage/adapter.ts';
import { DEFAULT_LIMITS, makeLimits, type Limits } from '../src/protocol/limits.ts';

/**
 * StorageAdapter semantics, written against the INTERFACE so Phase 4's Postgres adapter reuses it
 * verbatim (WORKLOAD §6 Gate C). Nothing in here may touch an implementation detail.
 */
export function storageSemantics(name: string, make: (limits: Limits) => StorageAdapter): void {
  const fresh = (limits: Limits = DEFAULT_LIMITS): StorageAdapter => make(limits);
  let n = 0;
  const wid = (): string => `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`;
  const put = (path: string, value: unknown) =>
    ({ writeId: wid(), path, op: 'put' as const, value: value as never });

  test(`${name}: a DECLARED database survives having no data, and having its data deleted (§5.19)`, async () => {
    const s = fresh();
    // Declared first, written never. This is the state an owner hands to a team, and before the
    // registry it was not a state that could exist at all.
    await s.declareDatabase('car_race', 'console-rw-owner');
    assert.deepEqual(await s.topNodes(), ['car_race']);

    // Data under it does not duplicate the name, and a database that was never declared is still
    // listed - the registry is ADDITIVE, so a store that predates it does not lose its own contents.
    await s.commitGroup([put('car_race/round/1', { score: 3 }), put('legacy/x', 1)]);
    assert.deepEqual(await s.topNodes(), ['car_race', 'legacy']);

    // And the whole point: the team deletes everything, and the database is still theirs.
    await s.commitGroup([put('car_race', null), put('legacy', null)]);
    assert.deepEqual(await s.topNodes(), ['car_race']);
  });

  test(`${name}: declaring a database twice is a no-op, not an error (§5.19)`, async () => {
    const s = fresh();
    await s.declareDatabase('car_race', 'console-rw-owner');
    await s.declareDatabase('car_race', 'console-rw-someone-else');
    assert.deepEqual(await s.topNodes(), ['car_race']);
  });

  test(`${name}: a name beginning with _ is refused by the registry (§5.22 Gate E)`, async () => {
    // `_default` and `_other` are synthetic METRICS labels, and both are names `validatePath`
    // accepts — it forbids only `/ . # $ [ ] ` and control characters. A client who declared
    // `_default` would find their connections, lag and leadership merged into the bucket a gateway
    // uses for "no database named", silently, on the panel their bill is read from.
    //
    // Refused at the REGISTRY and not only at the admin route in front of it: the route is one door
    // and the adapter is the room. Both storages, from one rule, because two ideas of a legal name
    // is what this suite exists to prevent.
    const s = fresh();
    for (const bad of ['_default', '_other', '_root', '_anything']) {
      await assert.rejects(() => s.declareDatabase(bad, 'console-rw-owner'), /reserved/, bad);
    }
    // And a name that merely CONTAINS one is fine — the rule is about the first character only.
    await s.declareDatabase('car_race', 'console-rw-owner');
    const names = await s.topNodes();
    assert.ok(names.includes('car_race'), `the legal name was declared: ${names}`);
    assert.deepEqual(names.filter((n) => n.startsWith('_')), [], 'and no reserved name got in');

    // `includes`, not `deepEqual`, and the asymmetry is real rather than test hygiene: Gate A moved
    // the registry to a CONTROL schema shared by every adapter on the shard, so under Postgres this
    // suite's stores share one `databases` table and see each other's declarations — while
    // `MemoryStorage` keeps a per-instance Set and does not. `fresh()` gives a private tenant
    // schema, never a private registry, because a shard-wide registry is the whole point of Gate A.
  });

  test(`${name}: listDeclared is the REGISTRY, never the derived names (§5.22 Gate D shart B)`, async () => {
    // Two things size themselves off this list and neither may see a derived name: the shared pool
    // (`sharedPoolSize(N)`, where N is the number of serial `WritePipeline` chains, one per database
    // this gateway actually serves) and `tenantFor`'s refusal, which is the only thing standing
    // between a signed token and a `CREATE SCHEMA`. `topNodes` unions in every top-level key that
    // has DATA — so a shard with 30 raw namespaces under 3 declared databases sized the pool for 30,
    // and an undeclared namespace that once got written would vouch for itself at hello.
    const s = fresh();
    const raw = ['rawa', 'rawb', 'rawc', 'rawd', 'rawe'];
    await s.commitGroup(raw.map((r) => put(`${r}/x`, 1)));
    await s.declareDatabase('decl_one', 'console-rw-owner');
    await s.declareDatabase('decl_two', 'console-rw-owner');

    const declared = await s.listDeclared();
    const all = await s.topNodes();
    for (const r of raw) {
      assert.ok(all.includes(r), `${r} has data, so the sidebar lists it`);
      assert.ok(!declared.includes(r), `${r} was never declared, so the registry does not`);
    }
    for (const d of ['decl_one', 'decl_two']) {
      assert.ok(declared.includes(d), `${d} was declared`);
      assert.ok(all.includes(d), 'and declaring is still additive');
    }
    // 5 raw + 2 declared, and the gap between the two answers is exactly the 5.
    assert.equal(all.length - declared.length, 5, `topNodes ${all} vs listDeclared ${declared}`);

    // `includes` above and a computed gap here, rather than a `deepEqual` on either list, for the
    // reason the `_`-prefix test already gives: Gate A moved the registry to a CONTROL schema shared
    // by every adapter on the shard, so under Postgres this suite's stores see each other's
    // declarations. The gap is the claim that survives that — it is about THIS store's data.
  });

  test(`${name}: storageBytes grows with the data and answers per database (§5.24)`, async () => {
    // The usage panel's Storage line. What is asserted is the CONTRACT both adapters owe, not a
    // number: an empty store costs nothing, writing costs more than that, deleting gives it back,
    // and the answer is keyed by database. The exact bytes are Postgres' catalogue on one side and
    // an approximation on the other, and pinning either would be pinning the implementation.
    const s = fresh();
    const mine = async (): Promise<number> => {
      const all = await s.storageBytes();
      // Postgres answers for the WHOLE shard (one query, every tenant schema) and the in-memory
      // store for the one database it is — so take this store's own entry, however it is keyed.
      const own = Object.values(all);
      return own.length === 1 ? (own[0] as number) : own.reduce((a, b) => a + b, 0);
    };

    const empty = await mine();
    await s.commitGroup([put('sizing/a', 'x'.repeat(4000)), put('sizing/b', 'y'.repeat(4000))]);
    const filled = await mine();
    assert.ok(filled > empty, `writing 8KB must cost something (${empty} -> ${filled})`);

    await s.commitGroup([put('sizing', null)]);
    const emptied = await mine();
    assert.ok(emptied <= filled, `deleting must not cost MORE than holding (${filled} -> ${emptied})`);
  });

  test(`${name}: a database name is a SCHEMA name, and the registry refuses what storage would (§5.26)`, async () => {
    /**
     * These two rules used to live apart: `validateDatabaseName` accepted `LightingMacQueen` and
     * `PostgresStorage`'s constructor refused it. So a client could declare that name, mint a token
     * for it, and only then have hello fail — 1011 at the factory, three steps from the cause, with
     * a registry row that CANNOT be deleted because §5.19 gave declaring no inverse.
     *
     * Both storages assert it, because the whole point of one rule is that the two agree.
     */
    const s = fresh();
    const long = 'a'.repeat(52); // 52 > 51 = 63 - len('rtdb_commit_'), the NOTIFY channel's headroom

    // Postgres folds an unquoted identifier to lowercase, so `Car` would create `car` and the
    // registry would disagree with the catalogue about the name forever.
    await assert.rejects(() => s.declareDatabase('Car_Race', 'console-rw-owner'), /lowercase/, 'capitals');
    await assert.rejects(() => s.declareDatabase('LightingMacQueen', 'console-rw-owner'), /lowercase/, 'the real one');
    // A hyphen is not an identifier character: it would have to be quoted at every interpolation.
    await assert.rejects(() => s.declareDatabase('car-race', 'console-rw-owner'), /lowercase/, 'hyphen');
    await assert.rejects(() => s.declareDatabase('9lives', 'console-rw-owner'), /digit/, 'leading digit');
    // Silently truncated at 63 bytes as a channel name, which is how two databases share one.
    await assert.rejects(() => s.declareDatabase(long, 'console-rw-owner'), /at most 51/, '52 characters');

    // And the shape that is legal stays legal — the rule refuses characters, not names.
    await s.declareDatabase('car_race', 'console-rw-owner');
    await s.declareDatabase('lightingmacqueen', 'console-rw-owner');
    const declared = await s.listDeclared();
    for (const ok of ['car_race', 'lightingmacqueen']) assert.ok(declared.includes(ok), ok);
    for (const bad of ['Car_Race', 'LightingMacQueen', 'car-race', '9lives', long]) {
      assert.ok(!declared.includes(bad), `${bad} never reached the registry`);
    }
  });

  test(`${name}: an empty store has head 0 and reads null everywhere`, async () => {
    const s = fresh();
    assert.equal(await s.head(), 0);
    assert.deepEqual(await s.readSnapshot(''), { value: null, rev: 0 });
    assert.deepEqual(await s.readSnapshot('a/b'), { value: null, rev: 0 });
  });

  test(`${name}: the epoch is a positive integer and does not move while the store lives (§2)`, async () => {
    const s = fresh();
    const epoch = await s.epoch();
    assert.ok(Number.isInteger(epoch) && epoch >= 1);
    await s.commitGroup([put('a', 1)]);
    assert.equal(await s.epoch(), epoch, 'writes never bump the epoch — only a broken rev promise does');
  });

  test(`${name}: revs are gap-free, ascending, and assigned in arrival order`, async () => {
    const s = fresh();
    const first = await s.commitGroup([put('a', 1), put('b', 2), put('c', 3)]);
    assert.deepEqual(first.map((r) => r.rev), [1, 2, 3]);
    const second = await s.commitGroup([put('d', 4)]);
    assert.equal(second[0]?.rev, 4);
    assert.equal(await s.head(), 4);
  });

  test(`${name}: a put stores a subtree that reads back whole and by part`, async () => {
    const s = fresh();
    await s.commitGroup([put('MPK_1010/1474396', { name: 'Ravi', score: 42, stats: { wins: 3 } })]);
    assert.deepEqual((await s.readSnapshot('MPK_1010/1474396')).value, {
      name: 'Ravi',
      score: 42,
      stats: { wins: 3 },
    });
    assert.equal((await s.readSnapshot('MPK_1010/1474396/score')).value, 42);
    assert.deepEqual((await s.readSnapshot('MPK_1010/1474396/stats')).value, { wins: 3 });
    assert.deepEqual((await s.readSnapshot('')).value, {
      MPK_1010: { 1474396: { name: 'Ravi', score: 42, stats: { wins: 3 } } },
    });
  });

  test(`${name}: null deletes the whole subtree; empty objects are never stored (§1)`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', { b: 1, c: { d: 2 } })]);
    await s.commitGroup([put('a/c', null)]);
    assert.deepEqual((await s.readSnapshot('a')).value, { b: 1 });
    await s.commitGroup([put('a', {})]);
    assert.equal((await s.readSnapshot('a')).value, null);
  });

  test(`${name}: arrays are opaque leaf values, Firebase-style (§1)`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', { list: [1, { x: 2 }, 3] })]);
    assert.deepEqual((await s.readSnapshot('a/list')).value, [1, { x: 2 }, 3]);
  });

  test(`${name}: the leaf set stays prefix-free in both directions`, async () => {
    const s = fresh();
    await s.commitGroup([put('a/b', 5)]);          // scalar leaf
    await s.commitGroup([put('a/b/c', 6)]);        // ...becomes an object
    assert.deepEqual((await s.readSnapshot('a')).value, { b: { c: 6 } });
    await s.commitGroup([put('a/b', 7)]);          // ...and back to a scalar
    assert.deepEqual((await s.readSnapshot('a')).value, { b: 7 });
  });

  test(`${name}: merge writes deep keys atomically under ONE rev and leaves siblings alone`, async () => {
    const s = fresh();
    await s.commitGroup([put('p', { score: 1, tag: 'x', keep: true })]);
    const [ack] = await s.commitGroup([
      { writeId: wid(), path: 'p', op: 'merge', value: { score: 50, 'stats/wins': 3, tag: null } },
    ]);
    assert.equal(ack?.rev, 2, 'one merge is one rev however many keys it carries');
    assert.deepEqual((await s.readSnapshot('p')).value, { score: 50, keep: true, stats: { wins: 3 } });
  });

  /**
   * The ordering hazard the postgres batch has to respect. `{"a/b": ..., "a": ...}` is a LEGAL merge
   * — validate.ts allows deep relative keys — so a merge's child paths are NOT prefix-disjoint by
   * construction. Applied in order, the later `a` put replaces the whole `a` subtree and `a/b` is
   * gone; batched as one DELETE followed by one INSERT, `a/b` would survive. Both backends must
   * agree, and memory.ts applies strictly in order, so it is the reference.
   */
  test(`${name}: a merge with prefix-related keys applies in key order, not as a set`, async () => {
    const s = fresh();
    await s.commitGroup([
      { writeId: wid(), path: 'p', op: 'merge', value: { 'a/b': 1, a: { z: 2 } } },
    ]);
    assert.deepEqual((await s.readSnapshot('p')).value, { a: { z: 2 } }, 'the later `a` wins whole');

    // ...and the other order, where the deep key lands second and survives alongside nothing else.
    const t = fresh();
    await t.commitGroup([
      { writeId: wid(), path: 'p', op: 'merge', value: { a: { z: 2 }, 'a/b': 1 } },
    ]);
    assert.deepEqual((await t.readSnapshot('p')).value, { a: { z: 2, b: 1 } });
  });

  test(`${name}: a group writing prefix-related paths applies in arrival order`, async () => {
    const s = fresh();
    await s.commitGroup([put('a/b', 1), put('a', { z: 2 })]);
    assert.deepEqual((await s.readSnapshot('a')).value, { z: 2 }, 'the later, wider put wins');
  });

  test(`${name}: a group writing disjoint paths is unaffected by batching`, async () => {
    const s = fresh();
    await s.commitGroup([put('a/x', 1), put('b/y', 2), put('c', 3)]);
    assert.deepEqual((await s.readSnapshot('')).value, { a: { x: 1 }, b: { y: 2 }, c: 3 });
  });

  test(`${name}: a put at ROOT replaces the whole tree`, async () => {
    // Root is the one path the batched DELETE cannot express as a range — every path descends from
    // it, so `[p || '/', p || '0')` says the opposite of what root means. Nothing in this suite
    // wrote root before, only read it, so the branch that handles it had no tooth at all.
    const s = fresh();
    await s.commitGroup([put('a/x', 1), put('b', 2)]);
    await s.commitGroup([put('', { c: 3 })]);
    assert.deepEqual((await s.readSnapshot('')).value, { c: 3 }, 'a root put leaves nothing of the old tree');
    assert.equal((await s.readSnapshot('a/x')).value, null);
  });

  test(`${name}: a scalar at root is replaced by a deeper write, and vice versa`, async () => {
    // The ancestor half of the DELETE, at the one ancestor that is the empty string.
    const s = fresh();
    await s.commitGroup([put('', 7)]);
    assert.equal((await s.readSnapshot('')).value, 7);
    await s.commitGroup([put('a/b', 1)]);
    assert.deepEqual((await s.readSnapshot('')).value, { a: { b: 1 } }, 'the root scalar must be gone');
  });

  test(`${name}: a duplicate writeId returns the ORIGINAL rev and commits nothing (§4 step 4)`, async () => {
    const s = fresh();
    const w = put('a', 1);
    const [first] = await s.commitGroup([w]);
    const [again] = await s.commitGroup([{ ...w, value: 999 as never }]);
    assert.deepEqual(again, { writeId: w.writeId, rev: first?.rev as number, duplicate: true });
    assert.equal(await s.head(), 1, 'a replay must not burn a rev');
    assert.equal((await s.readSnapshot('a')).value, 1, 'a replay must not re-apply');
  });

  test(`${name}: duplicates inside one batch collapse to a single commit`, async () => {
    const s = fresh();
    const w = put('a', 1);
    const acks = await s.commitGroup([w, w, put('b', 2), w]);
    assert.deepEqual(acks.map((r) => r.rev), [1, 1, 2, 1]);
    assert.deepEqual(acks.map((r) => r.duplicate), [false, true, false, true]);
    assert.equal(await s.head(), 2, 'the counter is taken for new writes only — no gaps');
  });

  test(`${name}: CAS commits when nothing relevant landed after expectedRev (§4 step 3)`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 });
    assert.deepEqual(r, { ok: true, rev: 2, duplicate: false });
    assert.equal((await s.readSnapshot('p/score')).value, 51);
  });

  test(`${name}: CAS fails with fresh state when a relevant write landed`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    await s.commitGroup([put('p/score', 60)]);
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 });
    assert.deepEqual(r, { ok: false, rev: 2, value: 60 });
    assert.equal(await s.head(), 2, 'a failed CAS must not consume a rev — rev is gap-free (§1)');
  });

  test(`${name}: CAS relevance is ancestor-or-descendant, not exact path`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    await s.commitGroup([put('other', 1)]); // rev 2, irrelevant
    assert.equal((await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 })).ok, true);

    const s2 = fresh();
    await s2.commitGroup([put('p/score', 50)]);
    await s2.commitGroup([put('p', { score: 60 })]); // rev 2, an ANCESTOR write
    assert.equal((await s2.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 })).ok, false);
  });

  test(`${name}: CAS is defined on the oplog, so a delete still conflicts (§4)`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 42)]); // rev 1
    await s.commitGroup([put('p', null)]); // rev 2 — the leaf and its rev are gone from `nodes`
    const r = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 43 });
    assert.equal(r.ok, false, 'max(leaf revs) would have missed this; the oplog does not');
  });

  test(`${name}: a duplicate CAS writeId acks with its original rev`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/score', 50)]);
    const w = { writeId: wid(), path: 'p/score', expectedRev: 1, value: 51 as never };
    const first = await s.commitCas(w);
    assert.deepEqual(await s.commitCas(w), { ok: true, rev: (first as { rev: number }).rev, duplicate: true });
    assert.equal(await s.head(), 2);
  });

  test(`${name}: an expectedRev older than retention fails conservatively (§4)`, async () => {
    const s = fresh(makeLimits({ OPLOG_RETENTION_REVS: 2 }));
    await s.commitGroup([put('p/score', 1)]); // rev 1
    await s.commitGroup([put('other', 2)]); // rev 2
    await s.commitGroup([put('other', 3)]); // rev 3 -> rev 1 is pruned
    assert.equal(await s.prunedThroughRev(), 1);

    // expectedRev 0 would need the pruned rev 1 to decide -> we cannot prove it, so we must not commit.
    const stale = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 0, value: 9 });
    assert.equal(stale.ok, false, 'unprovable means casFail, never an optimistic commit');

    // expectedRev 1 is still decidable: everything after it (revs 2 and 3) is retained.
    const ok = await s.commitCas({ writeId: wid(), path: 'p/score', expectedRev: 1, value: 9 });
    assert.equal(ok.ok, true, 'the boundary is "can I see everything AFTER it", not "is it itself retained"');
  });

  test(`${name}: catch-up returns relevant entries only, ascending, capped at the limit`, async () => {
    const s = fresh();
    await s.commitGroup([put('p/a', 1), put('q/b', 2), put('p', { a: 3 }), put('p/a/deep', 4)]);
    const got = await s.readCatchup('p/a', 0, 100);
    assert.deepEqual(got.map((e) => e.rev), [1, 3, 4], 'self, ancestor and descendant are all relevant');
    assert.deepEqual((await s.readCatchup('p/a', 3, 100)).map((e) => e.rev), [4]);
    assert.equal((await s.readCatchup('', 0, 2)).length, 2, 'the limit is respected');
  });

  test(`${name}: the dispatcher tail returns every entry after a rev, ascending`, async () => {
    const s = fresh();
    await s.commitGroup([put('a', 1), put('b', 2)]);
    await s.commitCas({ writeId: wid(), path: 'c', expectedRev: 2, value: 3 });
    assert.deepEqual((await s.readOplogSince(0, 100)).map((e) => [e.rev, e.path]), [
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ]);
    assert.deepEqual((await s.readOplogSince(2, 100)).map((e) => e.rev), [3]);
  });

  test(`${name}: commits notify, and unsubscribing stops it`, async () => {
    const s = fresh();
    let fired = 0;
    const off = s.onCommit(() => fired++);
    await s.commitGroup([put('a', 1), put('b', 2)]);
    assert.equal(fired, 1, 'one notify per transaction, not per write');
    await s.commitCas({ writeId: wid(), path: 'c', expectedRev: 2, value: 1 });
    assert.equal(fired, 2);
    off();
    await s.commitGroup([put('d', 1)]);
    assert.equal(fired, 2);
  });
}
