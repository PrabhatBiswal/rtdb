import type { Delta } from '../protocol/frames.ts';
import type { OplogEntry, StorageAdapter } from '../storage/adapter.ts';
import type { Appendable } from './stream.ts';

export const toDelta = (e: OplogEntry): Delta => ({
  type: 'delta',
  rev: e.rev,
  path: e.path,
  op: e.op,
  value: e.value,
});

/**
 * §8 transactional outbox: ONE dispatcher per shard tails the oplog in rev order and publishes to
 * the shard's stream. Nothing else may publish. That is what makes stream order == commit order ==
 * rev order BY CONSTRUCTION — no reorder buffers, no publish races.
 *
 * Who holds the "only one dispatcher" invariant depends on the deployment: in single-process mode
 * it is held by there being exactly one instance; on the Redis bus it is held by leader election
 * (`redis.ts`), which starts and stops this class as leadership moves.
 */
export class Dispatcher {
  #last = 0;
  #again = false;
  #inflight: Promise<void> | null = null;
  #stop: (() => void) | null = null;

  constructor(
    private readonly storage: StorageAdapter,
    private readonly stream: Appendable<Delta>,
    private readonly batch = 500,
  ) {}

  get lastPublishedRev(): number {
    return this.#last;
  }

  /**
   * Leadership handoff (§8): a new leader picks up where the stream already is, so the tail neither
   * re-publishes what is on the bus nor skips what is not. Never called in single-process mode.
   */
  resume(rev: number): void {
    this.#last = rev;
  }

  start(): void {
    if (this.#stop) return;
    this.#stop = this.storage.onCommit(() => this.#kick());
    this.#kick();
  }

  /**
   * Fire-and-forget drain. A rejected pump — a pool shutting down under a closing gateway, a Redis
   * that just died — must not take the whole process down as an unhandled rejection; the next commit
   * pokes it again, and leadership hand-off re-reads the stream tail anyway.
   */
  #kick(): void {
    void this.pump().catch((e: unknown) => console.error('dispatcher pump failed:', e));
  }

  stop(): void {
    this.#stop?.();
    this.#stop = null;
  }

  /**
   * Drain the oplog into the stream. Re-entrant calls collapse into the running loop — two concurrent
   * drains would interleave revs, which is the one thing this class exists to prevent — and JOIN it:
   * the returned promise resolves once everything committed before the call has been published. The
   * flag is raised before joining and read at the top of the loop, both synchronously, so a caller
   * can never slip in behind the last iteration's decision to stop.
   */
  pump(): Promise<void> {
    this.#again = true;
    return (this.#inflight ??= this.#drain());
  }

  async #drain(): Promise<void> {
    try {
      while (this.#again) {
        this.#again = false;
        const entries = await this.storage.readOplogSince(this.#last, this.batch);
        for (const e of entries) {
          await this.stream.append(e.rev, toDelta(e));
          this.#last = e.rev;
        }
        if (entries.length === this.batch) this.#again = true; // more waiting
      }
    } finally {
      this.#inflight = null;
    }
  }
}
