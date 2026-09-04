import type { Delta, Json } from '../src/protocol/frames.ts';
import { makeLimits } from '../src/protocol/limits.ts';
import { ancestorsInclusive, isAncestorOrEqual, isRelevant, joinPath } from '../src/protocol/path.ts';
import { flatten, type Leaf, unflatten } from '../src/storage/tree.ts';

/** MAX_LEAVES_PER_WRITE bounds one WRITE (§9); a snapshot may legitimately be far bigger. */
const NO_CAP = makeLimits({ MAX_LEAVES_PER_WRITE: Number.MAX_SAFE_INTEGER });

/** §7: the overlay holds OPERATIONS, not merged values — value-only overlays break the moment a
 * non-replacement op exists (Extensions' `incr`), and op-typing costs nothing now. */
export interface OverlayOp {
  op: 'put' | 'merge';
  path: string;
  value: Json;
}

interface Cell {
  rev: number;
  /** A rev-stamped tombstone (§7). Without these, a late stale delta resurrects deleted data. */
  deleted: boolean;
  value: Json;
}

/**
 * §7 mirror: `view = serverState ⊕ pendingOverlay`.
 *  - serverState is mutated ONLY by server frames, in arrival order;
 *  - the overlay is this client's unacked writes, applied as functions over serverState in issue
 *    order — there is no rollback, correctness falls out of the layering.
 * Per-leaf rev LWW with tombstones sits underneath as DEFENSE: the dispatcher already guarantees
 * order (§8), this catches bugs, it does not license them.
 */
export class Mirror {
  readonly #cells = new Map<string, Cell>();
  readonly overlay: OverlayOp[] = [];

  /**
   * §3: the client replaces the sub's serverState with the snapshot value — but §7 (v1.3) applies
   * per-leaf LWW here too: a leaf or tombstone recorded ABOVE the snapshot's rev survives it. A
   * setup snapshot can legitimately read older than deltas this connection already applied
   * (`delta(N+1), snapshot(N), delta(N+1)` on an overlapping sub), and the client must not roll back
   * in between. The newer deltas restore full consistency either way.
   */
  applySnapshot(path: string, value: Json, rev: number): void {
    this.#write(path, value, rev, true);
  }

  applyDelta(d: Delta): void {
    if (d.op === 'merge') {
      // §3: each key of the value is a child put at path/<key>; null children delete.
      for (const [key, child] of Object.entries(d.value as { [k: string]: Json })) {
        this.#write(joinPath(d.path, key), child, d.rev, true);
      }
      return;
    }
    this.#write(d.path, d.value, d.rev, true);
  }

  /** serverState ⊕ overlay — what the app sees. */
  view(path: string): Json {
    return this.#render(path, true);
  }

  /** serverState alone, for convergence assertions. */
  serverValue(path: string): Json {
    return this.#render(path, false);
  }

  /**
   * §2 (v1.5) epoch change: every rev we hold is from a dead generation, so serverState, the
   * per-leaf revs and the tombstones all go. The overlay stays — unacked writes replay as-is and
   * commit as new writes against the restored shard.
   */
  dropServerState(): void {
    this.#cells.clear();
  }

  // ------------------------------------------------------------------ internals

  /** §7 LWW: a delta older than a leaf's recorded rev — or than a tombstone above it — is stale. */
  #stale(path: string, rev: number): boolean {
    for (const a of ancestorsInclusive(path)) {
      const cell = this.#cells.get(a);
      if (cell && cell.rev > rev) return true;
    }
    return false;
  }

  #write(path: string, value: Json, rev: number, lww: boolean): void {
    if (lww && this.#stale(path, rev)) return;

    for (const [p, c] of [...this.#cells]) {
      if (isAncestorOrEqual(path, p) && (!lww || c.rev <= rev)) this.#cells.delete(p);
    }
    // A scalar sitting at an ancestor has to give way, exactly as it does in storage — the server
    // never sends a delta for the ancestor it silently replaced, so the client must infer it.
    // Tombstones above us are LEFT in place: they still guard against older deltas.
    for (const a of ancestorsInclusive(path)) {
      if (a === path) continue;
      const c = this.#cells.get(a);
      if (c && !c.deleted && (!lww || c.rev <= rev)) this.#cells.delete(a);
    }
    // The tombstone covers the whole cleared subtree, not just leaves we happened to hold.
    this.#cells.set(path, { rev, deleted: true, value: null });

    const flat = flatten(path, value, NO_CAP);
    if (!flat.ok) return; // a server that sends an unflattenable value is a server bug
    for (const leaf of flat.leaves) {
      if (lww && this.#stale(leaf.path, rev)) continue;
      // §7: every EXTRACTED leaf is stamped with the delta's rev, not just the root.
      this.#cells.set(leaf.path, { rev, deleted: false, value: leaf.value });
    }
  }

  #render(root: string, withOverlay: boolean): Json {
    const leaves = new Map<string, Json>();
    for (const [p, c] of this.#cells) if (!c.deleted && isAncestorOrEqual(root, p)) leaves.set(p, c.value);
    if (withOverlay) for (const op of this.overlay) this.#overlay(leaves, op, root);
    const out: Leaf[] = [];
    for (const [path, value] of leaves) out.push({ path, value });
    return unflatten(root, out);
  }

  #overlay(leaves: Map<string, Json>, op: OverlayOp, root: string): void {
    const parts: [string, Json][] =
      op.op === 'merge'
        ? Object.entries(op.value as { [k: string]: Json }).map(([k, v]) => [joinPath(op.path, k), v])
        : [[op.path, op.value]];

    for (const [path, value] of parts) {
      if (!isRelevant(path, root)) continue;
      for (const p of [...leaves.keys()]) if (isAncestorOrEqual(path, p)) leaves.delete(p);
      for (const a of ancestorsInclusive(path)) if (a !== path) leaves.delete(a);
      const flat = flatten(path, value, NO_CAP);
      if (!flat.ok) continue;
      for (const leaf of flat.leaves) if (isAncestorOrEqual(root, leaf.path)) leaves.set(leaf.path, leaf.value);
    }
  }
}
