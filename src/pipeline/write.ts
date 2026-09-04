import type { ErrCode, ServerFrame, WriteFrame } from '../protocol/frames.ts';
import type { Limits } from '../protocol/limits.ts';
import { joinPath } from '../protocol/path.ts';
import type { GroupWrite, StorageAdapter } from '../storage/adapter.ts';
import { flatten } from '../storage/tree.ts';
import { consoleWriteDenied, type Rules } from './rules.ts';

/**
 * §5.11: how many times a rejected commit is retried before the batch is abandoned. THREE, where
 * the listen repair gets eight, and the asymmetry is the point: `#serialize` is one serial chain, so
 * a retry here is head-of-line blocking on EVERY later write, while a listen repair blocks only its
 * own subscription. The real cost is worse than the spacing suggests — once Gate B sets
 * `connectionTimeoutMillis`, each attempt can itself sit for that timeout before rejecting, so the
 * stall is roughly attempts x (timeout + spacing). Gate B must pick its value against this product.
 */
const COMMIT_ATTEMPTS = 3;

/** Spacing base, doubling per attempt: 20ms, 40ms — 60ms of added stall across the three. */
const COMMIT_RETRY_BASE_MS = 20;

export interface Submission {
  frame: WriteFrame;
  userId: string;
  /** The connection's console role (§5.8). Null or absent for app tokens. */
  role?: string | null;
  reply: (frame: ServerFrame) => void;
}

/** §9: 100 writes/s sustained per connection, burst 500. Over that is `RATE`. */
export class RateLimiter {
  #tokens: number;
  #last: number;

  constructor(
    private readonly perSec: number,
    private readonly burst: number,
    now: number = Date.now(),
  ) {
    this.#tokens = burst;
    this.#last = now;
  }

  take(now: number = Date.now()): boolean {
    this.#tokens = Math.min(this.burst, this.#tokens + ((now - this.#last) / 1000) * this.perSec);
    this.#last = now;
    if (this.#tokens < 1) return false;
    this.#tokens -= 1;
    return true;
  }
}

/**
 * §4 write pipeline, in the order the spec states it:
 *  1. validate BEFORE the transaction (pure, no DB) — a bad write errs immediately and never enters
 *     a batch, which is exactly what makes batching safe;
 *  2. put/merge group-commit inside one transaction with revs from a single counter take;
 *  3. CAS commits SOLO, counter-lock-first;
 *  4. writeId dedup returns the original rev as a normal ack.
 */
export class WritePipeline {
  #pending: Submission[] = [];
  #timer: NodeJS.Timeout | null = null;
  #chain: Promise<unknown> = Promise.resolve();

  constructor(
    private readonly storage: StorageAdapter,
    private readonly rules: Rules,
    private readonly limits: Limits,
    /**
     * One line per write attempt from a console subject (§5.9). Bounded by nature — these are
     * human-driven, a person clicking — and bounded by construction too: the connection's rate
     * limiter is taken in server.ts BEFORE submit() is called, so a hostile session cannot turn the
     * deny path into a log flood at line rate. The denied lines are the valuable ones.
     */
    private readonly auditConsoleWrite?: (fields: Record<string, unknown>) => void,
    /** U3-shaped, bounded: one line when a batch is abandoned. See §5.11. */
    private readonly log?: (ev: string, fields: Record<string, unknown>) => void,
  ) {}

  submit(s: Submission): void {
    const bad = this.#validate(s);
    if (bad) {
      s.reply({ type: 'err', writeId: s.frame.writeId, code: bad.code, msg: bad.msg });
      return;
    }
    if (s.frame.type === 'cas') {
      // Solo, and behind whatever put/merge arrived earlier: §4 assigns revs in ARRIVAL order, so a
      // CAS must not overtake the batch it followed.
      void this.#serialize(async () => {
        await this.#drain();
        await this.#runCas(s);
      });
      return;
    }
    this.#pending.push(s);
    if (this.#timer === null) {
      this.#timer = setTimeout(() => void this.#serialize(() => this.#drain()), this.limits.GROUP_COMMIT_MS);
      this.#timer.unref();
    }
  }

  /** Commit whatever is pending now. For shutdown and for tests that do not want to wait 5ms. */
  flush(): Promise<unknown> {
    return this.#serialize(() => this.#drain());
  }

  // ------------------------------------------------------------------ internals

  /** §4 step 1: rules, path syntax and leaf count — all pure, all before any transaction. */
  #validate(s: Submission): { code: ErrCode; msg: string } | null {
    const f = s.frame;
    // BEFORE the configured rules, and not reachable by them: see rules.ts's note on why this is an
    // invariant rather than a policy. The audit line carries the verdict, never the value.
    const denied = consoleWriteDenied({ userId: s.userId, role: s.role ?? null, op: f.type });
    if (s.userId.startsWith('console-')) {
      // writeId is the correlation key: without it a logged attempt cannot be tied to the ack or err
      // the client saw, which is the exact gap U3 was created to close on the connection side.
      this.auditConsoleWrite?.({
        sub: s.userId, role: s.role ?? null, op: f.type, path: f.path,
        writeId: f.writeId, allowed: !denied,
      });
    }
    if (denied) return { code: 'RULES', msg: 'this console session may not write' };
    if (!this.rules({ userId: s.userId, role: s.role ?? null, op: f.type, path: f.path, value: f.value })) {
      return { code: 'RULES', msg: 'write denied' };
    }
    // Frame syntax and the 1 MiB frame cap were enforced at parse; what is left is the shape of the
    // value itself: key legality (§1) and the flattened leaf count (§9).
    const parts: { path: string; value: unknown }[] =
      f.type === 'merge'
        ? Object.entries(f.value).map(([k, v]) => ({ path: joinPath(f.path, k), value: v }))
        : [{ path: f.path, value: f.value }];

    let leaves = 0;
    for (const part of parts) {
      const r = flatten(part.path, part.value as never, this.limits);
      if (!r.ok) return { code: r.tooBig ? 'TOOBIG' : 'BADPATH', msg: r.msg };
      leaves += r.leaves.length;
      if (leaves > this.limits.MAX_LEAVES_PER_WRITE) {
        return { code: 'TOOBIG', msg: `write exceeds ${this.limits.MAX_LEAVES_PER_WRITE} leaves` };
      }
    }
    return null;
  }

  /**
   * §5.11: a commit that REJECTED — an RDS failover killing an in-flight query, or (once Gate B
   * lands) a pool acquisition timing out. Retried rather than reported, because a retry is safe by
   * construction and reporting is not:
   *
   *  - Safe: the whole batch is ONE transaction (`postgres.ts` `#tx`: BEGIN / fn / COMMIT, ROLLBACK
   *    on any throw), so there is no partial state to reconcile. The only indeterminate window is
   *    `COMMIT` itself rejecting, where the transaction may or may not have landed — and §4 step 4's
   *    writeId dedup resolves exactly that on replay, returning the original rev as a normal `ack`.
   *  - And reporting is not available: §4's err vocabulary is closed and every code in it blames the
   *    client, so an honest `err` cannot be written for our own database failing. A dishonest one is
   *    worse than none — the client settles the write `Failed` and never retries it, turning a
   *    transient server fault into permanent loss of that write.
   *
   * A successful retry acks normally and the client never learns anything happened, which is the
   * correct outcome for a transient fault.
   *
   * The retry waits INSIDE `#serialize`'s chain, deliberately. §4 assigns revs in arrival order and
   * the chain is what guarantees it; stepping out to avoid the stall would let a later batch commit
   * ahead of this one. Ordering is a correctness invariant, throughput during a storage fault is
   * not — and the chain is serial anyway, so waiting here costs no parallelism that existed.
   *
   * On exhaustion the batch is left UNSETTLED and one line is written. That is F5's shape and worth
   * naming as such: the client's overlay stays pinned and `onComplete` never fires. It is F5
   * narrowed to a gateway that by then is also failing its own `/healthz`, not F5 untouched.
   */
  async #commit<T>(fn: () => Promise<T>, what: Record<string, unknown>): Promise<T | null> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await fn();
      } catch (e: unknown) {
        if (attempt >= COMMIT_ATTEMPTS) {
          this.log?.('write-abandoned', {
            ...what,
            attempts: attempt,
            err: String(e instanceof Error ? e.message : e),
          });
          return null;
        }
        await new Promise((r) => setTimeout(r, COMMIT_RETRY_BASE_MS * 2 ** (attempt - 1)));
      }
    }
  }

  /** Serialize every transaction so commit order == arrival order, whatever mix of group and CAS. */
  #serialize<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.#chain.then(fn, fn);
    this.#chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async #drain(): Promise<void> {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#pending.length === 0) return;
    const batch = this.#pending;
    this.#pending = [];

    const writes: GroupWrite[] = batch.map((s) => ({
      writeId: s.frame.writeId,
      path: s.frame.path,
      op: s.frame.type as 'put' | 'merge',
      value: s.frame.value,
    }));
    const acks = await this.#commit(() => this.storage.commitGroup(writes), {
      writes: writes.length,
      paths: writes.length === 1 ? writes[0]?.path : undefined,
    });
    if (acks === null) return; // abandoned and logged; see #commit
    acks.forEach((ack, i) => {
      // §4: the ack is identical for a first commit and a duplicate replay. The client cannot and
      // need not distinguish — that is what makes a lost-ack retry safe.
      batch[i]?.reply({ type: 'ack', writeId: ack.writeId, rev: ack.rev });
    });
  }

  async #runCas(s: Submission): Promise<void> {
    const f = s.frame as Extract<WriteFrame, { type: 'cas' }>;
    const r = await this.#commit(
      () =>
        this.storage.commitCas({
          writeId: f.writeId,
          path: f.path,
          expectedRev: f.expectedRev,
          value: f.value,
        }),
      { writes: 1, paths: f.path },
    );
    if (r === null) return; // abandoned and logged; see #commit
    if (r.ok) {
      s.reply({ type: 'ack', writeId: f.writeId, rev: r.rev });
      return;
    }
    // A mismatch is a normal outcome carrying fresh state, not an error (§4 step 3).
    s.reply({ type: 'casFail', writeId: f.writeId, path: f.path, value: r.value, rev: r.rev });
  }
}
