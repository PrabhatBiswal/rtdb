import type { Delta, Resync, ServerFrame, Snapshot } from '../protocol/frames.ts';
import type { Limits } from '../protocol/limits.ts';
import { ancestorsInclusive, isAncestorOrEqual, isRelevant } from '../protocol/path.ts';
import type { StorageAdapter } from '../storage/adapter.ts';
import * as M from '../gateway/metrics.ts';
import { toDelta } from './dispatcher.ts';

/**
 * §5.11: how many times a listen whose STORAGE read failed is repaired before the subscription is
 * abandoned. Bounded because retrying into an exhausted pool is the feedback loop this package
 * exists to avoid — each attempt costs 3-4 pool acquisitions (prunedThroughRev, head, then catchup
 * or snapshot). Eight spaced attempts run ~3.3s, comfortably inside the window in which a genuinely
 * dead storage layer fails `/healthz` (2s race) and the gateway is pulled from service.
 */
const REPAIR_ATTEMPTS = 8;

/**
 * Cap on the spacing between those attempts. It is not about the retry rate: a sub being re-set-up
 * holds `buffer` non-null and `deliver()` pushes every relevant delta into it unbounded, so the gap
 * between attempts IS the window that buffer grows in. One second bounds it.
 */
const REPAIR_MAX_SPACING_MS = 1_000;

/** What the sink needs from a socket. uWS in production, a fake in tests. */
export interface Transport {
  /**
   * False when the socket REFUSED the frame — uWS drops what it cannot buffer, and a dropped frame
   * is a hole in the stream, not a slow one. Ignoring this is what lost a `resync` in WP2 Gate D.
   */
  send(text: string): boolean;
  bufferedAmount(): number;
  closed(): boolean;
}

interface SubState {
  subId: number;
  path: string;
  /** Non-null while the subscription is being set up: deltas land here instead of the wire (§3). */
  buffer: Delta[] | null;
  /**
   * The rev this subscription has been brought up to — its snapshot's rev, or the last delta sent
   * for it. §3's setup rule ("flush >N, discard the rest") is not a setup special case: the fanout
   * can hand us a rev the snapshot already contains at ANY time, because the bus lags the oplog it
   * is tailing. Re-sending it would put a lower rev on the wire after a higher one.
   */
  rev: number;
}

/**
 * Path -> the connections subscribed there. Joining a path IS the read authorization for the life of
 * the subscription (§3): deltas are never rules-checked per recipient, because a delta is encoded
 * once and broadcast.
 */
export class SubscriptionRegistry {
  readonly #byPath = new Map<string, Set<ConnectionSink>>();

  join(path: string, sink: ConnectionSink): void {
    let set = this.#byPath.get(path);
    if (!set) this.#byPath.set(path, (set = new Set()));
    set.add(sink);
  }

  leave(path: string, sink: ConnectionSink): void {
    const set = this.#byPath.get(path);
    if (!set) return;
    set.delete(sink);
    if (set.size === 0) this.#byPath.delete(path);
  }

  /** Deliver a delta to every connection with at least one relevant sub — ONCE per connection,
   * whatever the number of matching subs (deltas carry no subId; the client routes them itself). */
  route(delta: Delta): void {
    const targets = new Set<ConnectionSink>();
    // Subs at or above the delta: at most 33 exact lookups (§8's ancestor-query shape).
    for (const a of ancestorsInclusive(delta.path)) {
      for (const sink of this.#byPath.get(a) ?? []) targets.add(sink);
    }
    // Subs below the delta need a prefix scan.
    // ponytail: O(distinct sub paths) per delta. A path trie is the upgrade if fanout CPU shows up.
    for (const [path, sinks] of this.#byPath) {
      if (path !== delta.path && isAncestorOrEqual(delta.path, path)) for (const s of sinks) targets.add(s);
    }
    for (const sink of targets) sink.deliver(delta);
  }
}

/**
 * One connection's outbound half: subscriptions, the §3 listen setup order, micro-batching, and
 * the resync repair for a slow consumer.
 */
export class ConnectionSink {
  readonly subs = new Map<number, SubState>();
  #queue: ServerFrame[] = [];
  /** When the frame at the head of `#queue` started waiting — the only latency this sink adds. */
  #queuedAt = 0;
  #timer: NodeJS.Timeout | null = null;
  #resyncing = false;
  #closed = false;
  /** Subs told to resync, whose fresh snapshot is waiting for the socket to drain. */
  readonly #awaitingSnapshot = new Set<number>();
  #repair: NodeJS.Timeout | null = null;
  /**
   * §5.11 read-failure repairs: subId -> attempts so far and the earliest time to try again. Kept
   * here rather than on SubState because [listen] replaces the SubState wholesale on every retry,
   * which would reset the count and make the bound meaningless.
   */
  readonly #readRepair = new Map<number, { attempts: number; at: number }>();

  constructor(
    private readonly transport: Transport,
    private readonly registry: SubscriptionRegistry,
    private readonly storage: StorageAdapter,
    private readonly limits: Limits,
    /** U3-shaped, bounded: one line when a subscription is abandoned. See §5.11. */
    private readonly log?: (ev: string, fields: Record<string, unknown>) => void,
  ) {}

  /**
   * §3 listen setup order, and the order is the point: join the topic FIRST, buffer what arrives,
   * read at rev N, send, then flush only what is newer than N. Anything else reopens the
   * subscribe/snapshot race.
   */
  async listen(subId: number, path: string, lastRev?: number): Promise<void> {
    try {
      await this.#listen(subId, path, lastRev);
    } catch (e: unknown) {
      // §5.11: NEVER REJECTS. Every caller is `void this.listen(...)` — server.ts's listen frame,
      // and the two §3 repair sites in this file — so a rejection here was an unhandled rejection
      // that killed the process, and before that a client with no snapshot, no err and an open
      // socket: a permanent silent hang with not even a close to act on. Containing it inside the
      // shared method rather than at each call site is what makes all three safe at once.
      this.#repairRead(subId, path, e);
    }
  }

  async #listen(subId: number, path: string, lastRev?: number): Promise<void> {
    this.#drop(subId); // a re-listen on a live subId replaces it
    const sub: SubState = { subId, path, buffer: [], rev: 0 };
    this.subs.set(subId, sub); // 1. join the fanout topic
    this.registry.join(path, this);

    // 2. deltas buffer from here until step 5
    const pruned = await this.storage.prunedThroughRev();
    const head = await this.storage.head();
    if (this.#closed) return;

    // §3 serves catch-up only while the oplog "still retains R". A rev above the head was never
    // written here at all (a restored or reset shard), so it is not retained either -> snapshot.
    if (lastRev !== undefined && lastRev >= pruned && lastRev <= head) {
      // +1 so "more than CATCHUP_LIMIT" is distinguishable from "exactly CATCHUP_LIMIT".
      const entries = await this.storage.readCatchup(path, lastRev, this.limits.CATCHUP_LIMIT + 1);
      if (this.#closed) return;
      if (entries.length <= this.limits.CATCHUP_LIMIT) {
        M.listens.inc({ result: 'catchup' });
        this.#finishSetup(sub, entries.at(-1)?.rev ?? lastRev, entries.map(toDelta));
        return;
      }
      // too far behind to stream: fall through to a fresh snapshot
    }

    const snap = await this.storage.readSnapshot(path); // 3. one consistent read at rev N
    if (this.#closed) return;
    const frame: Snapshot = { type: 'snapshot', subId, path, value: snap.value, rev: snap.rev };
    if (Buffer.byteLength(JSON.stringify(frame), 'utf8') > this.limits.SNAPSHOT_MAX) {
      // §3: too big to complete on a mobile client -> sub-scoped err, no subscription.
      this.#drop(subId);
      M.listens.inc({ result: 'toobig' });
      this.enqueue({ type: 'err', subId, code: 'TOOBIG', msg: `snapshot exceeds ${this.limits.SNAPSHOT_MAX} bytes` });
      return;
    }
    M.listens.inc({ result: 'snapshot' });
    this.#finishSetup(sub, snap.rev, [frame]);
  }

  /** §3: no reply. In-flight deltas route to no sub and are dropped. */
  unlisten(subId: number): void {
    this.#drop(subId);
  }

  /** Called by the registry for every delta relevant to this connection. */
  deliver(delta: Delta): void {
    let live = false;
    for (const sub of this.subs.values()) {
      if (!isRelevant(sub.path, delta.path)) continue;
      if (sub.buffer) sub.buffer.push(delta);
      else if (delta.rev > sub.rev) {
        sub.rev = delta.rev;
        live = true;
      }
    }
    if (live) this.enqueue(delta);
  }

  /** Every server->client frame after helloAck goes through here, so per-connection order is one queue. */
  enqueue(frame: ServerFrame): void {
    if (this.#closed) return;
    // While a resync is outstanding the backlog is worthless by definition — pushing more deltas
    // into a socket that is already over its limit is what caused the overflow. Acks and errs still
    // go through: a write must always settle.
    if (this.#resyncing && (frame.type === 'delta' || frame.type === 'batch')) return;
    // §3/WORKLOAD §4: the 20ms batch window is opened by the FIRST frame and only affects frames
    // that arrive while it is open — an idle connection never pays batching latency.
    if (this.#timer === null) {
      this.#write([frame]);
      this.#armWindow();
      return;
    }
    if (this.#queue.length === 0) this.#queuedAt = performance.now();
    this.#queue.push(frame);
  }

  /**
   * Complete an outstanding repair once the socket can carry a snapshot again: §3 promises a fresh
   * snapshot after the resync frame, and the only moment we can deliver one is when the connection
   * has caught up. Polled rather than driven by uWS's `drain` event, which does not fire when the
   * backpressure sits in the kernel rather than in uWS's own buffer — measured, not assumed.
   */
  onDrain(): void {
    if (this.#closed) return;
    if (this.transport.bufferedAmount() > this.limits.SEND_QUEUE_MAX / 2) return;
    this.#resyncing = false;
    const now = Date.now();
    const waiting = [...this.#awaitingSnapshot];
    this.#awaitingSnapshot.clear();
    for (const subId of waiting) {
      const sub = this.subs.get(subId);
      if (!sub) continue;
      // A backpressure repair has no entry here and runs now; a §5.11 read repair is SPACED, and
      // firing it early is the feedback loop again at one subscription's granularity.
      const repair = this.#readRepair.get(subId);
      if (repair && repair.at > now) {
        this.#awaitingSnapshot.add(subId);
        continue;
      }
      // §3 promises the client "resync, then a fresh snapshot". Say it again now that the socket can
      // carry it: the copy sent under pressure may have been dropped, and a duplicate is idempotent
      // client-side (mark stale, keep serverState, wait for the snapshot) while a lost one leaves the
      // client trusting a mirror this server has already given up on.
      this.#sendResync(subId);
      void this.listen(subId, sub.path);
    }
    // Disarmed only once nothing is waiting: a spaced retry still due must outlive this tick.
    if (this.#awaitingSnapshot.size === 0 && this.#repair) {
      clearInterval(this.#repair);
      this.#repair = null;
    }
  }

  /**
   * §8 bus-loss recovery is the GATEWAY's job, never the client's (§3): when the fanout lost history
   * this gateway cannot reconstruct from the oplog either, every live subscription here is stale and
   * §3's resync — resync frame, then a fresh snapshot — is the only honest repair.
   */
  resyncAll(): void {
    if (this.#closed) return;
    for (const sub of [...this.subs.values()]) {
      if (sub.buffer) continue; // still setting up; its snapshot is newer than anything we lost
      this.#sendResync(sub.subId);
      void this.listen(sub.subId, sub.path);
    }
  }

  close(): void {
    this.#closed = true;
    for (const sub of this.subs.values()) this.registry.leave(sub.path, this);
    this.subs.clear();
    if (this.#timer) clearTimeout(this.#timer);
    if (this.#repair) clearInterval(this.#repair);
    this.#timer = null;
    this.#repair = null;
    this.#readRepair.clear();
    this.#queue = [];
  }

  // ------------------------------------------------------------------ internals

  #finishSetup(sub: SubState, n: number, initial: ServerFrame[]): void {
    this.#readRepair.delete(sub.subId); // the read succeeded; the bound starts over
    const buffered = sub.buffer ?? [];
    sub.buffer = null; // step 5 starts: this sub is live
    sub.rev = n;
    for (const f of initial) this.enqueue(f); // 4. send the snapshot / catch-up deltas
    for (const d of buffered) {
      if (d.rev <= sub.rev) continue; // 5. flush >N, discard the rest
      sub.rev = d.rev;
      this.enqueue(d);
    }
  }

  /**
   * §5.11: a listen whose storage read failed. `resync` rather than a sub-scoped `err`, because
   * PROTOCOL.md §3 names "internal error" among resync's own reasons, while a sub-scoped err is
   * spec'd to TERMINATE the subscription (codes RULES/BADPATH/TOOBIG only) — permanent, for what is
   * usually an RDS failover. And rather than closing the connection, which would re-listen every
   * subscription on it through the same starved pool: 3-4 pool acquisitions times N subs, feeding
   * demand back into the exhausted resource.
   *
   * At exhaustion the subscription is DROPPED, not left pending. Pending would keep `buffer`
   * non-null and `deliver()` pushes into it without bound, so "leave it and log" leaks memory for
   * the life of the connection. Dropping costs the client nothing it still had — it was never going
   * to receive a snapshot — and the log line is the record.
   */
  #repairRead(subId: number, path: string, cause: unknown): void {
    if (this.#closed) return;
    if (!this.subs.has(subId)) return; // unlistened while the read was in flight
    const state = this.#readRepair.get(subId) ?? { attempts: 0, at: 0 };
    state.attempts += 1;
    if (state.attempts > REPAIR_ATTEMPTS) {
      this.#readRepair.delete(subId);
      this.#awaitingSnapshot.delete(subId);
      this.#drop(subId);
      this.log?.('listen-abandoned', {
        subId,
        path,
        attempts: REPAIR_ATTEMPTS,
        err: String(cause instanceof Error ? cause.message : cause),
      });
      return;
    }
    state.at =
      Date.now() + Math.min(this.limits.DELTA_BATCH_MS * 2 ** (state.attempts - 1), REPAIR_MAX_SPACING_MS);
    this.#readRepair.set(subId, state);
    this.#sendResync(subId);
    this.#awaitingSnapshot.add(subId);
    this.#armRepair();
  }

  /** Sent directly, never queued: the backlog we just dropped must not swallow the repair. */
  #sendResync(subId: number): boolean {
    M.resyncs.inc();
    const frame: Resync = { type: 'resync', subId };
    return this.transport.send(JSON.stringify(frame));
  }

  #drop(subId: number): void {
    const sub = this.subs.get(subId);
    if (!sub) return;
    this.subs.delete(subId);
    for (const other of this.subs.values()) if (other.path === sub.path) return; // still in use
    this.registry.leave(sub.path, this);
  }

  #armWindow(): void {
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (this.#queue.length === 0) return; // idle again: the window closes
      const frames = this.#queue;
      M.fanoutSeconds.observe((performance.now() - this.#queuedAt) / 1000);
      this.#queue = [];
      this.#write(frames);
      this.#armWindow();
    }, this.limits.DELTA_BATCH_MS);
    this.#timer.unref();
  }

  #write(frames: ServerFrame[]): void {
    if (this.#closed || this.transport.closed()) return;
    // One JSON.stringify per FRAME rather than one per send. Byte-identical output — a JSON array
    // IS its elements joined with commas — and the same total work, but it is what makes the
    // per-prefix egress counter exact instead of an apportionment (WORKLOAD §2).
    // ponytail: §3's "encoded once and broadcast" still wants a cached encoding for the single-delta
    // case shared across connections; add it when fanout CPU actually shows up in a profile.
    const parts = frames.map((f) => JSON.stringify(f));
    for (let i = 0; i < frames.length; i++) {
      const frame = frames[i] as ServerFrame;
      if (frame.type === 'delta') M.deltasOut.inc();
      M.countBytesOut(frame, Buffer.byteLength(parts[i] as string, 'utf8'));
    }
    const delivered = this.transport.send(
      parts.length === 1 ? (parts[0] as string) : `{"type":"batch","frames":[${parts.join(',')}]}`,
    );
    // A frame the socket refused is a gap the client can never learn about on its own (§3 forbids it
    // from gap-detecting), so the subscription is stale whatever `bufferedAmount` happens to read.
    this.#checkPressure(!delivered);
  }

  /** §3: send-queue overflow after coalescing -> the subscription is stale server-side. Repair it. */
  #checkPressure(dropped = false): void {
    if (this.#resyncing) return this.onDrain();
    if (!dropped && this.transport.bufferedAmount() <= this.limits.SEND_QUEUE_MAX) return;

    this.#resyncing = true;
    this.#queue = []; // the backlog is worthless once we are re-snapshotting
    for (const sub of [...this.subs.values()]) {
      if (sub.buffer) continue; // still setting up; its snapshot is already on the way
      // ...and the snapshot goes out once we can send again. Registered BEFORE the send, because
      // the send is the part that can fail: the socket is over its limit by definition here, which
      // is exactly when uWS drops what it cannot buffer. onDrain() re-announces it from a socket
      // that has room.
      this.#awaitingSnapshot.add(sub.subId);
      this.#sendResync(sub.subId);
    }
    this.#armRepair();
  }

  /** The one spaced-repair timer, shared by the backpressure path and §5.11's read repair. */
  #armRepair(): void {
    if (this.#repair !== null) return;
    this.#repair = setInterval(() => this.onDrain(), this.limits.DELTA_BATCH_MS);
    this.#repair.unref();
  }
}
