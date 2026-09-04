/**
 * All the dispatcher needs from a stream. The Redis body (`redis.ts`) answers the same call with an
 * XADD, which is why it may be async: `OrderedStream` satisfies it synchronously and nothing else
 * changes (WORKLOAD §0.7).
 */
export interface Appendable<T> {
  append(id: number, item: T): void | Promise<void>;
}

/**
 * The in-process stand-in for §8's per-shard Redis Stream: an append-only log in strict id order
 * with replay-from-id. Phase 5 swaps this for XADD/XRANGE; delivery code must not be able to tell
 * the difference (WORKLOAD §2), so the API is exactly what Redis Streams gives us — and nothing more.
 */
export class OrderedStream<T> {
  readonly #items: { id: number; item: T }[] = [];
  readonly #subs = new Set<(item: T, id: number) => void>();
  #trimmedThrough = 0;

  constructor(private readonly maxLen: number) {}

  get lastId(): number {
    return this.#items.at(-1)?.id ?? this.#trimmedThrough;
  }

  /**
   * Ids MUST ascend. Ordering is the whole point of the dispatcher (§8) — a violation here is a bug
   * that would silently corrupt every consumer, so it throws rather than reorders.
   */
  append(id: number, item: T): void {
    const last = this.#items.at(-1)?.id ?? this.#trimmedThrough;
    if (id <= last) throw new Error(`stream ids must ascend: got ${id} after ${last}`);
    this.#items.push({ id, item });
    while (this.#items.length > this.maxLen) {
      this.#trimmedThrough = (this.#items.shift() as { id: number }).id;
    }
    for (const cb of this.#subs) cb(item, id);
  }

  /** Replay after `afterId`, or null when the stream has trimmed past it (caller falls back to the oplog). */
  range(afterId: number): T[] | null {
    if (afterId < this.#trimmedThrough) return null;
    return this.#items.filter((e) => e.id > afterId).map((e) => e.item);
  }

  subscribe(cb: (item: T, id: number) => void): () => void {
    this.#subs.add(cb);
    return () => this.#subs.delete(cb);
  }
}
