import type { Json } from '../protocol/frames.ts';

export type WriteOp = 'put' | 'merge';

/** One row of §8's `oplog(rev, path, op, value, write_id, ts)`. */
export interface OplogEntry {
  rev: number;
  path: string;
  op: WriteOp;
  value: Json;
  writeId: string;
  ts: number;
}

/** A put/merge entering a group commit (§4 step 2). Already validated (§4 step 1). */
export interface GroupWrite {
  writeId: string;
  path: string;
  op: WriteOp;
  value: Json;
}

export interface AckResult {
  writeId: string;
  /** For a duplicate this is the ORIGINAL rev — the ack is identical either way (§4 step 4). */
  rev: number;
  duplicate: boolean;
}

export interface CasWrite {
  writeId: string;
  path: string;
  expectedRev: number;
  value: Json;
}

/** A mismatch is a normal outcome carrying fresh state, not an error (§4 step 3). */
export type CasResult =
  | { ok: true; rev: number; duplicate: boolean }
  | { ok: false; rev: number; value: Json };

export interface SnapshotRead {
  value: Json;
  rev: number;
}

/**
 * The in-memory ↔ Postgres seam (WORKLOAD §0.6). Every method is one transaction, because that is
 * the unit Postgres will need: no method may be composed of two calls that must be atomic together.
 */
export interface StorageAdapter {
  /** Head rev of shard 0. */
  head(): Promise<number>;

  /**
   * §2 (v1.5) the shard's generation, persisted with the data and bumped whenever the rev promise
   * breaks (restore, reset, or — here — a start with no persisted state at all).
   */
  epoch(): Promise<number>;

  /**
   * Every rev at or below this has been pruned from the oplog (§9 retention). A client's `lastRev`
   * or a CAS `expectedRev` is only usable when it is >= this; below it we cannot prove what changed.
   */
  prunedThroughRev(): Promise<number>;

  /** §4 step 2: ONE transaction, the counter taken once for N new revs, assigned in arrival order. */
  commitGroup(writes: GroupWrite[]): Promise<AckResult[]>;

  /** §4 step 3: SOLO, counter-lock-first, never batched with anything else. */
  commitCas(write: CasWrite): Promise<CasResult>;

  /** §3: `value` and `rev` from ONE consistent read; the snapshot fully includes rev's effects. */
  readSnapshot(path: string): Promise<SnapshotRead>;

  /**
   * §3 catch-up: entries relevant to `path` with rev > sinceRev, ascending, at most `limit`.
   * Callers pass CATCHUP_LIMIT + 1 so that "more than the limit" is distinguishable from "exactly".
   */
  readCatchup(path: string, sinceRev: number, limit: number): Promise<OplogEntry[]>;

  /** §8 dispatcher tail: ALL entries with rev > afterRev, ascending. */
  readOplogSince(afterRev: number, limit: number): Promise<OplogEntry[]>;

  /**
   * DISTINCT top-level path segments — the namespaces this shard holds.
   *
   * Exists because the console can never listen on root (§3's TOOBIG is the designed answer), which
   * leaves top-level names undiscoverable: an operator has to be TOLD that `userstatus` exists. This
   * answers that out of band of the wire, on the admin port, read-only, names only.
   *
   * Names, deliberately not counts: a per-namespace count is a scan over every leaf, and nobody has
   * asked for one.
   */
  topNodes(): Promise<string[]>;

  /** Commit notification — the in-memory stand-in for Postgres LISTEN/NOTIFY (§8). */
  onCommit(cb: () => void): () => void;

  /**
   * Release whatever the backend holds open (connections, listeners). Optional because the
   * in-memory adapter holds nothing; whoever CONSTRUCTED the storage calls it, since a gateway may
   * be handed a store it does not own (two gateways sharing one shard in the reconnect tests).
   */
  /**
   * How the nodes half of a commit was applied, cumulative — `groups` counts commits with more
   * than one target, `orderedFallbacks` the ones that could not batch and paid the old per-write
   * cost. Optional because only the Postgres adapter batches; `/metrics` reports it when present.
   */
  readonly applyStats?: { groups: number; orderedFallbacks: number };

  close?(): Promise<void>;
}
