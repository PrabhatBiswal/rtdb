import { createClient } from 'redis';
import type { Delta, Kick } from '../protocol/frames.ts';
import type { StorageAdapter } from '../storage/adapter.ts';
import { Dispatcher, toDelta } from './dispatcher.ts';

/** node-redis' concrete client type, without spelling out its four generics. */
export type Redis = ReturnType<typeof createClient>;

/** One XREAD/XRANGE entry: the stream id and our single-field payload. */
interface StreamMessage {
  id: string;
  message: Record<string, unknown>;
}

/** §8 is written per-shard; v1 runs shard 0, and the key names say so rather than assuming it. */
export const busKeys = (
  shard: string | number = 0,
): { stream: string; lock: string; fence: string; kick: string; epoch: string } => ({
  stream: `rtdb:${shard}:stream`,
  lock: `rtdb:${shard}:leader`,
  fence: `rtdb:${shard}:fence`,
  kick: `rtdb:${shard}:admin`,
  /** Which generation (§2) the stream's contents belong to. See `RedisBus#promote`. */
  epoch: `rtdb:${shard}:epoch`,
});

/**
 * A connected client, or a loud failure. WP4's no-silent-fallback rule: a gateway told to join a bus
 * it cannot reach must not quietly serve its own connections as an island.
 */
export async function connectRedis(url: string, bootAttempts = 3): Promise<Redis> {
  let up = false;
  const client = createClient({
    url,
    socket: {
      /**
       * Two different policies either side of the first successful connect, and the difference is
       * the WP4 rule: a URL that is wrong AT BOOT must crash the gateway, because a gateway serving
       * its own connections as an island is worse than one that does not start. After that a blip
       * is just a blip — node-redis reconnects forever and the consumer replays from its last id.
       */
      reconnectStrategy: (retries) =>
        up || retries < bootAttempts ? Math.min(50 * (retries + 1), 500) : new Error(`redis unreachable at ${url}`),
    },
  });
  client.on('error', () => undefined); // an unhandled 'error' event kills the process; the commands report
  await client.connect();
  up = true;
  return client;
}

// --------------------------------------------------------------------------- leader election

/** Renew/release only if the lock is still OURS — a plain PEXPIRE/DEL would steal the next leader's. */
const IF_MINE = (verb: string): string =>
  `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('${verb}', KEYS[1], unpack(ARGV, 2)) else return 0 end`;

export interface LeadershipOptions {
  ttlMs?: number;
  /** Called on every transition. `token` is the fencing token while leading, null otherwise. */
  onChange?: (leading: boolean, token: number | null) => void;
}

/**
 * §8's "Redis lock, TTL + fencing token". One shard, one publisher at a time:
 *
 * - the lock is `SET NX PX`, so exactly one candidate can hold it;
 * - the token is `INCR` on a separate counter, taken per acquisition attempt, so every successive
 *   holder's token is strictly greater than every token issued before it;
 * - the TTL is what makes a dead leader release it without cooperating.
 *
 * A holder that cannot prove it still holds the lock steps down immediately — "probably still the
 * leader" is the failure mode this class exists to remove.
 */
export class Leadership {
  #token: number | null = null;
  #timer: NodeJS.Timeout | null = null;
  #busy = false;
  #stopped = true;
  readonly #ttlMs: number;

  constructor(
    private readonly redis: Redis,
    private readonly keys: { lock: string; fence: string },
    private readonly opts: LeadershipOptions = {},
  ) {
    this.#ttlMs = opts.ttlMs ?? 3000;
  }

  get token(): number | null {
    return this.#token;
  }

  get isLeader(): boolean {
    return this.#token !== null;
  }

  /** Awaitable: the first campaign is complete when it resolves, so a lone gateway boots leading. */
  async start(): Promise<void> {
    if (!this.#stopped) return;
    this.#stopped = false;
    // A third of the TTL: two renewals may be lost before the lock expires under us.
    this.#timer = setInterval(() => void this.tick(), Math.max(1, Math.floor(this.#ttlMs / 3)));
    this.#timer.unref();
    await this.tick();
  }

  /** Campaign or renew, once. Public so tests can drive elections without sleeping on a timer. */
  async tick(): Promise<void> {
    if (this.#busy || this.#stopped) return;
    this.#busy = true;
    try {
      if (this.#token === null) await this.#acquire();
      else await this.#renew();
    } catch {
      // Cannot reach Redis => cannot prove we hold the lock => we do not hold the lock.
      this.#set(null);
    } finally {
      this.#busy = false;
    }
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    const token = this.#token;
    this.#set(null);
    // Hand over immediately instead of making the next candidate wait out the TTL.
    if (token !== null) {
      await this.redis
        .eval(IF_MINE('DEL'), { keys: [this.keys.lock], arguments: [String(token)] })
        .catch(() => undefined);
    }
  }

  async #acquire(): Promise<void> {
    // Taken before the SET, so a loser burns a token: tokens are monotone, never gap-free.
    const token = await this.redis.incr(this.keys.fence);
    const won = await this.redis.set(this.keys.lock, String(token), { NX: true, PX: this.#ttlMs });
    if (won) this.#set(token);
  }

  async #renew(): Promise<void> {
    const ok = await this.redis.eval(IF_MINE('PEXPIRE'), {
      keys: [this.keys.lock],
      arguments: [String(this.#token), String(this.#ttlMs)],
    });
    if (ok !== 1) this.#set(null); // someone else owns it now; we are a stale leader as of this moment
  }

  #set(token: number | null): void {
    if (this.#token === token) return;
    this.#token = token;
    this.opts.onChange?.(token !== null, token);
  }
}

// --------------------------------------------------------------------------- publisher

/** Redis' own wording when an explicit XADD id is not strictly greater than the stream's top item. */
const isStaleId = (e: unknown): boolean => /equal or smaller/i.test(String(e));

/**
 * One oplog entry onto the shard's stream, keyed by its own rev.
 *
 * The stream ID **is** the rev (`<rev>-0`), which buys three things at once: stream order == rev
 * order is enforced by Redis rather than promised by us; replay from a consumer's position is a
 * plain XRANGE; and a stale leader's late append is *refused by the server* — `'stale'` below is a
 * fenced-out publisher being harmless, not an error.
 */
export async function publishEntry(
  redis: Redis,
  stream: string,
  rev: number,
  delta: Delta,
  maxLen: number,
): Promise<'ok' | 'stale'> {
  try {
    await redis.xAdd(
      stream,
      `${rev}-0`,
      { d: JSON.stringify(delta) },
      { TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: maxLen } },
    );
    return 'ok';
  } catch (e) {
    if (isStaleId(e)) return 'stale';
    throw e;
  }
}

/** The rev the stream is already carrying, or null when it holds nothing (fresh, or Redis restarted). */
export async function streamTailRev(redis: Redis, stream: string): Promise<number | null> {
  const [last] = await redis.xRevRange(stream, '+', '-', { COUNT: 1 });
  return last ? Number(last.id.split('-')[0]) : null;
}

export interface BusOptions {
  shard?: string | number;
  ttlMs?: number;
  /** `MAXLEN ~` on the stream. Defaults to the oplog's rev retention, so both hold the same history. */
  maxLen?: number;
  /** §9 retention, run ONLY while holding leadership (WP4 Gate D ruling Q4). */
  prune?: { intervalMs: number; run: () => Promise<unknown> };
  /** Every delta this gateway must deliver, in ascending rev order. The consumer's whole output. */
  onDelta?: (delta: Delta) => void;
  /**
   * The bus lost history this gateway cannot reconstruct from the oplog either (§9 retention passed
   * it). Every live subscription is stale: §3's resync is the only honest repair.
   */
  onHistoryLost?: () => void;
  /** How long a blocking XREAD waits before the consumer reconciles against the oplog head, ms. */
  idleMs?: number;
  /**
   * §10 admin plane. Set, this gateway subscribes to the shard's out-of-band kick channel; unset, it
   * opens no connection for it at all. There is no admin plane without Redis.
   */
  onKick?: (kick: Kick) => void;
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * This gateway's half of §8's bus, both directions:
 *
 * - **publish** (leader only): one `Dispatcher` tailing the oplog, its append an XADD keyed by rev;
 * - **consume** (always): a DEDICATED blocking `XREAD` connection whose output is fed to the same
 *   delivery path the in-process `OrderedStream` feeds in single-process mode.
 *
 * Gateways NEVER publish their own writes — the dispatcher above is the only thing that appends,
 * and it only runs while this gateway holds the shard's lock.
 */
export class RedisBus {
  readonly keys: ReturnType<typeof busKeys>;
  readonly leadership: Leadership;
  readonly #dispatcher: Dispatcher;
  readonly #maxLen: number;
  readonly #idleMs: number;
  #prune: NodeJS.Timeout | null = null;
  /** Rises on every acquisition; a `#promote` from an older term must not start a newer term's work. */
  #term = 0;
  #promotion: Promise<void> = Promise.resolve();
  #publishing = false;
  /** The dedicated reader. Blocking reads may not share a connection with anything else. */
  #reader: Redis | null = null;
  /** The §10 admin subscriber. Pub/sub puts a connection in subscriber mode; it gets its own. */
  #admin: Redis | null = null;
  #stopped = false;
  /** Last stream id read, and the rev it carried. Revs are gap-free (§1), so a jump IS a gap. */
  #id = '$';
  /** The consumer's floor, seeded from the oplog head in `start()` — never left at zero. */
  #rev = 0;

  constructor(
    private readonly redis: Redis,
    private readonly storage: StorageAdapter,
    private readonly opts: BusOptions = {},
  ) {
    this.keys = busKeys(opts.shard ?? 0);
    this.#maxLen = opts.maxLen ?? 500_000;
    this.#idleMs = opts.idleMs ?? 1000;
    this.#dispatcher = new Dispatcher(storage, { append: (rev, delta) => this.#append(rev, delta) });
    this.leadership = new Leadership(redis, this.keys, {
      ...(opts.ttlMs !== undefined ? { ttlMs: opts.ttlMs } : {}),
      onChange: (leading) => {
        if (leading) this.#promotion = this.#promote();
        else this.#demote();
      },
    });
  }

  get isLeader(): boolean {
    return this.leadership.isLeader;
  }

  /** True while this gateway is the shard's publisher. False after a tail read it could not complete. */
  get publishing(): boolean {
    return this.#publishing;
  }

  /** The last rev this gateway delivered off the bus. Head minus this IS the consumer's lag. */
  get deliveredRev(): number {
    return this.#rev;
  }

  /**
   * Resolves once the consumer is attached and the first election has been decided, so a lone
   * gateway is already publishing when its socket opens. A Redis we cannot reach throws here, which
   * is the whole point: no silent island (WP4).
   */
  async start(): Promise<void> {
    this.#reader = this.redis.duplicate();
    this.#reader.on('error', () => undefined);
    await this.#reader.connect();
    /**
     * Two halves of one decision. We start reading the stream at `$`, because everything older is
     * already inside the snapshots this gateway will serve (§3's setup order discards anything at or
     * below a subscription's rev). But the floor is seeded from the OPLOG HEAD, not left at zero:
     * without it a gateway that has never delivered a delta accepts the first entry off the stream at
     * face value, whatever rev it carries — so a bus-level SKIP (Redis wiped, the new leader
     * correctly resuming at the head) is invisible to it, and its clients' snapshots strand below the
     * skipped range with nothing to notice it. The floor is what makes "$ is safe" enforceable.
     */
    this.#rev = await this.storage.head();
    void this.#consume();
    if (this.opts.onKick) {
      this.#admin = this.redis.duplicate();
      this.#admin.on('error', () => undefined);
      await this.#admin.connect();
      await this.#admin.subscribe(this.keys.kick, (msg) => this.#onAdmin(msg));
    }
    await this.leadership.start();
    await this.#promotion;
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    await this.leadership.stop();
    this.#demote();
    const sockets = [this.#reader, this.#admin];
    this.#reader = null;
    this.#admin = null;
    for (const c of sockets) {
      try {
        c?.destroy(); // breaks the in-flight blocking read
      } catch {
        /* already gone */
      }
    }
  }

  /**
   * §10, verbatim: `{"type":"kick","target":{"userId":...},"reason":...}`. The channel is an admin
   * plane, not a trusted one — anything that is not exactly this frame is ignored, the same
   * forward-compatibility rule the wire protocol follows for unknown types.
   */
  #onAdmin(message: string): void {
    let frame: unknown;
    try {
      frame = JSON.parse(message);
    } catch {
      return;
    }
    const f = frame as Partial<Kick>;
    if (f?.type !== 'kick' || typeof f.target?.userId !== 'string') return;
    this.opts.onKick?.(f as Kick);
  }

  // ------------------------------------------------------------------ publishing

  /**
   * §8's fencing guard sits on the TAIL-READ, not on the append: before publishing anything we
   * confirm we are still the leader of this term. The append itself needs no guard — Redis refuses a
   * non-ascending id — but reading the oplog as a leader we are not is the thing worth stopping.
   */
  async #append(rev: number, delta: Delta): Promise<void> {
    if (!this.leadership.isLeader) return;
    try {
      await publishEntry(this.redis, this.keys.stream, rev, delta, this.#maxLen);
    } catch {
      // We cannot prove the entry landed, so we cannot advance past it: drop the term and let the
      // next acquisition resume from the stream's real tail rather than from our own bookkeeping.
      this.#demote();
    }
  }

  async #promote(): Promise<void> {
    const term = ++this.#term;
    let from: number;
    try {
      // §2 first: an epoch bump means the head moved BACKWARDS (a PITR restore, a reset). The stream's
      // ids ARE revs, so a stream left over from the dead generation refuses every append the new one
      // makes — the very rule that makes a fenced-out leader harmless would wedge the bus until the
      // new head passed the old tail. Clear it, and record which generation the stream now belongs to.
      // A stream with no marker at all is treated as foreign, so the first promotion after this code
      // ships clears the bus once; consumers cover that with the oplog fallback like any other trim.
      const epoch = String(await this.storage.epoch());
      if ((await this.redis.get(this.keys.epoch)) !== epoch) {
        await this.redis.del(this.keys.stream);
        await this.redis.set(this.keys.epoch, epoch);
      }
      // No gap and no duplicate across a handoff: start where the stream actually is. Only a stream
      // that is genuinely EMPTY falls back to the head — Redis lost its history (or never had any),
      // the bus is live state, the oplog is the truth, and consumers take the readOplogSince path.
      const tail = await streamTailRev(this.redis, this.keys.stream);
      from = tail ?? (await this.storage.head());
    } catch {
      // A tail read we could not COMPLETE is not an empty stream. Resuming from the head here would
      // silently skip every rev the previous leader never got to publish — a bus-level gap every
      // consumer inherits. Publish nothing and try again; leadership is unaffected.
      if (term === this.#term && this.leadership.isLeader) {
        setTimeout(() => void (this.#promotion = this.#promote()), this.#idleMs).unref();
      }
      return;
    }
    if (term !== this.#term || !this.leadership.isLeader) return;
    this.#dispatcher.resume(from);
    this.#dispatcher.start();
    this.#publishing = true;
    const prune = this.opts.prune;
    if (prune && this.#prune === null) {
      this.#prune = setInterval(() => void prune.run().catch(() => undefined), prune.intervalMs);
      this.#prune.unref();
    }
  }

  #demote(): void {
    this.#term++;
    this.#publishing = false;
    this.#dispatcher.stop();
    if (this.#prune) clearInterval(this.#prune);
    this.#prune = null;
  }

  // ------------------------------------------------------------------ consuming

  /**
   * The blocking read loop. A dropped connection, a killed Redis, a slow shard — all end up back
   * here reading from `#id`, which is why replay after any interruption is free: XREAD from an
   * explicit id IS the XRANGE replay §8 asks for.
   */
  async #consume(): Promise<void> {
    while (!this.#stopped) {
      try {
        // node-redis types XREAD's reply as the whole RESP union; one shape is what it actually is.
        const res = (await (this.#reader as Redis).xRead(
          { key: this.keys.stream, id: this.#id },
          { BLOCK: this.#idleMs, COUNT: 500 },
        )) as { messages: StreamMessage[] }[] | null;
        if (this.#stopped) return;
        if (res?.[0]) await this.#deliver(res[0].messages);
        else await this.#reconcile();
      } catch {
        if (this.#stopped) return;
        // node-redis reconnects underneath us; come back to the same position and replay.
        await sleep(50);
      }
    }
  }

  async #deliver(messages: StreamMessage[]): Promise<void> {
    for (const m of messages) {
      const rev = Number(m.id.split('-')[0]);
      this.#id = m.id;
      if (rev <= this.#rev) continue; // already delivered: a replay overlap, not a duplicate
      // §1: revs are strictly increasing and GAP-FREE, so a jump is history the stream no longer
      // has. Fill it from the oplog before delivering what came after it — order is the contract.
      if (rev > this.#rev + 1) await this.#fillFromOplog(rev);
      this.opts.onDelta?.(JSON.parse(m.message['d'] as string) as Delta);
      this.#rev = rev;
    }
  }

  /**
   * Deliver `#rev+1 .. upto-1` from the oplog (§8's trim fallback). Beyond §9 retention there is
   * nothing left to read: those subscriptions are stale and only §3's resync can repair them.
   */
  async #fillFromOplog(upto: number): Promise<void> {
    const pruned = await this.storage.prunedThroughRev();
    if (pruned > this.#rev) {
      this.#rev = pruned;
      this.opts.onHistoryLost?.();
    }
    while (this.#rev + 1 < upto) {
      const entries = await this.storage.readOplogSince(this.#rev, 500);
      if (entries.length === 0) return;
      for (const e of entries) {
        if (e.rev >= upto) return;
        this.opts.onDelta?.(toDelta(e));
        this.#rev = e.rev;
      }
    }
  }

  /**
   * Nothing on the bus for a whole idle window. The oplog is the truth, so ask it: a leader that
   * died before publishing, or a Redis that came back empty, leaves revs that no XADD will ever
   * announce. Under traffic this never fires; on an idle shard it is what stops "the bus lost it"
   * from meaning "the client never hears about it".
   */
  async #reconcile(): Promise<void> {
    const head = await this.storage.head();
    if (head > this.#rev) await this.#fillFromOplog(head + 1);
  }
}
