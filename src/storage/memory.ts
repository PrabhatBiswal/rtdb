import { randomInt } from 'node:crypto';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import type { Json } from '../protocol/frames.ts';
import { DEFAULT_LIMITS, type Limits } from '../protocol/limits.ts';
import { ancestorsInclusive, isAncestorOrEqual, isRelevant, joinPath } from '../protocol/path.ts';
import type {
  AckResult,
  CasResult,
  CasWrite,
  GroupWrite,
  OplogEntry,
  SnapshotRead,
  StorageAdapter,
} from './adapter.ts';
import { flatten, type Leaf, unflatten } from './tree.ts';

interface Node {
  value: Json;
  rev: number;
}

/**
 * In-memory StorageAdapter with the same semantics as §8's SQL schema: a flattened `nodes` map, a
 * gap-free rev counter, an oplog, and a writeId unique index.
 *
 * Transaction model: every public method's body is SYNCHRONOUS. Node's event loop cannot interleave
 * two of them, so each is atomic by construction — that is what stands in for BEGIN/COMMIT here, and
 * it is why CAS's counter-lock-first ordering (§4 step 3) holds without a lock object.
 */
export class MemoryStorage implements StorageAdapter {
  /** Flattened leaf paths -> value + the rev that last wrote them. Kept prefix-free. */
  readonly #nodes = new Map<string, Node>();
  readonly #oplog: OplogEntry[] = [];
  readonly #writeIds = new Map<string, number>();
  readonly #listeners = new Set<() => void>();
  #rev = 0;
  #prunedThrough = 0;
  #replaying = false;
  readonly #epoch: number;

  /**
   * `persistPath` appends the oplog to a file and replays it on start, so a gateway that is
   * SIGKILLed comes back with its revs intact (chaos scenario 10 — a client's stored `lastRev` is
   * meaningless against a shard whose counter reset). Off by default.
   * ponytail: appendFileSync per commit, and the file is never compacted. It exists to survive a
   * kill in tests; Phase 4's Postgres is the real durability story.
   *
   * The epoch (§2, v1.5) lives in that same file, on its first line: a store that comes up WITHOUT
   * persisted state has a head of 0 and cannot honour any rev it previously handed out, which is
   * exactly the "reset that moves the head backwards" the epoch exists to announce.
   */
  constructor(
    private readonly limits: Limits = DEFAULT_LIMITS,
    private readonly persistPath?: string,
  ) {
    const persisted = persistPath && existsSync(persistPath) ? readFileSync(persistPath, 'utf8') : null;
    if (persisted !== null) {
      this.#epoch = this.#replay(persisted);
      return;
    }
    // Random, not `1`: a fresh generation must be distinguishable from every generation before it,
    // and with no persisted state there is nothing left to increment.
    this.#epoch = randomInt(1, 2 ** 31);
    if (persistPath) appendFileSync(persistPath, `${JSON.stringify({ epoch: this.#epoch })}\n`);
  }

  head(): Promise<number> {
    return Promise.resolve(this.#rev);
  }

  epoch(): Promise<number> {
    return Promise.resolve(this.#epoch);
  }

  /** §5.6's sidebar. One pass over the leaf map: in memory there is no index to skip along, and
   *  nothing here holds a shard big enough for that to matter. */
  topNodes(): Promise<string[]> {
    const names = new Set<string>();
    for (const path of this.#nodes.keys()) names.add(path.split('/', 1)[0] as string);
    return Promise.resolve([...names].sort());
  }

  prunedThroughRev(): Promise<number> {
    return Promise.resolve(this.#prunedThrough);
  }

  commitGroup(writes: GroupWrite[]): Promise<AckResult[]> {
    // Pass 1: resolve dedup BEFORE taking the counter, so N is exactly the number of new revs and
    // the sequence stays gap-free (§1). A duplicate inside the same batch resolves to its twin.
    const isNew: boolean[] = [];
    const inBatch = new Map<string, number>();
    let fresh = 0;
    for (const w of writes) {
      const prior = this.#writeIds.get(w.writeId) ?? inBatch.get(w.writeId);
      if (prior !== undefined) {
        isNew.push(false);
      } else {
        isNew.push(true);
        inBatch.set(w.writeId, -1);
        fresh++;
      }
    }

    // Pass 2: one counter take for the whole batch (§4 step 2: `v = v + N RETURNING v`), then revs
    // handed out in arrival order.
    let next = this.#take(fresh);
    const results: AckResult[] = [];
    for (let i = 0; i < writes.length; i++) {
      const w = writes[i] as GroupWrite;
      if (!isNew[i]) {
        const rev = (this.#writeIds.get(w.writeId) ?? inBatch.get(w.writeId)) as number;
        results.push({ writeId: w.writeId, rev, duplicate: true });
        continue;
      }
      const rev = next++;
      inBatch.set(w.writeId, rev);
      this.#apply(w.path, w.op, w.value, rev);
      this.#record({ rev, path: w.path, op: w.op, value: w.value, writeId: w.writeId, ts: Date.now() });
      results.push({ writeId: w.writeId, rev, duplicate: false });
    }

    this.#notify();
    return Promise.resolve(results);
  }

  commitCas(write: CasWrite): Promise<CasResult> {
    const prior = this.#writeIds.get(write.writeId);
    if (prior !== undefined) return Promise.resolve({ ok: true, rev: prior, duplicate: true });

    // §4: an expectedRev older than retention cannot be proven safe -> casFail with fresh state.
    if (write.expectedRev < this.#prunedThrough) return Promise.resolve(this.#casFail(write.path));

    // §4 step 3: the counter is held from here (the synchronous body) through the oplog check to the
    // commit — that ordering is what closes the check/commit race between concurrent CAS writes.
    const conflict = this.#oplog.some(
      (e) => e.rev > write.expectedRev && isRelevant(e.path, write.path),
    );
    if (conflict) return Promise.resolve(this.#casFail(write.path));

    const rev = this.#take(1);
    this.#apply(write.path, 'put', write.value, rev);
    this.#record({
      rev,
      path: write.path,
      op: 'put',
      value: write.value,
      writeId: write.writeId,
      ts: Date.now(),
    });
    this.#notify();
    return Promise.resolve({ ok: true, rev, duplicate: false });
  }

  readSnapshot(path: string): Promise<SnapshotRead> {
    // One consistent read (§3): the leaves and the rev come from the same synchronous instant, so
    // the snapshot fully includes the effects of the rev it reports.
    return Promise.resolve({ value: unflatten(path, this.#subtree(path)), rev: this.#rev });
  }

  readCatchup(path: string, sinceRev: number, limit: number): Promise<OplogEntry[]> {
    const out: OplogEntry[] = [];
    for (const e of this.#oplog) {
      if (e.rev <= sinceRev) continue;
      if (!isRelevant(e.path, path)) continue;
      out.push(e);
      if (out.length >= limit) break;
    }
    return Promise.resolve(out);
  }

  readOplogSince(afterRev: number, limit: number): Promise<OplogEntry[]> {
    const out: OplogEntry[] = [];
    for (const e of this.#oplog) {
      if (e.rev <= afterRev) continue;
      out.push(e);
      if (out.length >= limit) break;
    }
    return Promise.resolve(out);
  }

  onCommit(cb: () => void): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  // ------------------------------------------------------------------ internals

  #take(n: number): number {
    const base = this.#rev + 1;
    this.#rev += n;
    return base;
  }

  #notify(): void {
    for (const cb of this.#listeners) cb();
  }

  /** ponytail: O(nodes) prefix scan per write. Swap in a path trie if fanout/write CPU ever shows up. */
  #subtree(path: string): Leaf[] {
    const out: Leaf[] = [];
    for (const [p, node] of this.#nodes) if (isAncestorOrEqual(path, p)) out.push({ path: p, value: node.value });
    return out;
  }

  #apply(path: string, op: 'put' | 'merge', value: Json, rev: number): void {
    if (op === 'merge') {
      // §4: each key of the value is a child put at path/<key>; null children delete. All under
      // one rev, which is what makes a merge with deep keys atomic.
      for (const [key, child] of Object.entries(value as { [k: string]: Json })) {
        this.#put(joinPath(path, key), child, rev);
      }
      return;
    }
    this.#put(path, value, rev);
  }

  #put(path: string, value: Json, rev: number): void {
    // Replacing a subtree: every leaf at or under `path` goes.
    for (const p of [...this.#nodes.keys()]) if (isAncestorOrEqual(path, p)) this.#nodes.delete(p);
    // ...and a scalar sitting at an ancestor becomes an object, so that leaf goes too.
    for (const a of ancestorsInclusive(path)) if (a !== path) this.#nodes.delete(a);

    const flat = flatten(path, value, this.limits);
    // Validation (§4 step 1) already rejected anything unflattenable; reaching here with an error
    // would be a pipeline bug, and silently storing nothing would hide it.
    if (!flat.ok) throw new Error(`storage received an unvalidated write at "${path}": ${flat.msg}`);
    for (const leaf of flat.leaves) this.#nodes.set(leaf.path, { value: leaf.value, rev });
  }

  #casFail(path: string): CasResult {
    return { ok: false, rev: this.#rev, value: unflatten(path, this.#subtree(path)) };
  }

  /** Replays the persisted oplog and returns the epoch from its header line. */
  #replay(contents: string): number {
    this.#replaying = true;
    let epoch = 1;
    for (const line of contents.split('\n')) {
      if (line === '') continue;
      const parsed = JSON.parse(line) as OplogEntry | { epoch: number };
      if (!('rev' in parsed)) {
        epoch = parsed.epoch;
        continue;
      }
      this.#apply(parsed.path, parsed.op, parsed.value, parsed.rev);
      this.#record(parsed);
      this.#rev = Math.max(this.#rev, parsed.rev);
    }
    this.#replaying = false;
    return epoch;
  }

  #record(entry: OplogEntry): void {
    if (this.persistPath && !this.#replaying) appendFileSync(this.persistPath, `${JSON.stringify(entry)}\n`);
    this.#oplog.push(entry);
    this.#writeIds.set(entry.writeId, entry.rev);
    // §9 retention, count bound. ponytail: the 2h time bound and the Phase 4 pruning cron are not
    // here — a test-sized OPLOG_RETENTION_REVS is what exercises the "not retained" branch for now.
    while (this.#oplog.length > this.limits.OPLOG_RETENTION_REVS) {
      const dropped = this.#oplog.shift() as OplogEntry;
      this.#prunedThrough = dropped.rev;
      this.#writeIds.delete(dropped.writeId);
    }
  }
}
