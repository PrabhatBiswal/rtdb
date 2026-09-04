import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import type {
  Ack,
  CasFail,
  ClientFrame,
  Err,
  HelloAck,
  Json,
  ServerFrame,
  WriteFrame,
} from '../src/protocol/frames.ts';
import { CLOSE } from '../src/protocol/frames.ts';
import { DEFAULT_LIMITS, type Limits } from '../src/protocol/limits.ts';
import { isRelevant } from '../src/protocol/path.ts';
import { Mirror, type OverlayOp } from './mirror.ts';

/** §7 virtual path, served entirely client-side — it is not a legal wire path (§1 forbids `.`). */
export const INFO_CONNECTED = '.info/connected';

/**
 * §4 gives a write exactly three outcomes: `ack`, `casFail`, `err`. "Never settles" is not among
 * them, and the 2026-08-29 load test found 1,650 writes in exactly that state — issued on a client
 * whose FSM had stopped, so no socket would ever carry them and no reconnect would ever replay them.
 * A terminal client settles them with this instead. Named, and distinguishable from a server-sent
 * `Err` by `type`, so an app can tell a local abandonment from a rules/rate rejection.
 */
export class ClientClosedError extends Error {
  readonly type = 'closed';
  constructor(
    readonly writeId: string,
    message = 'the client is closed; this write will never be sent',
  ) {
    super(message);
    this.name = 'ClientClosedError';
  }
}

export type ValueListener = (value: Json) => void;

interface Sub {
  subId: number;
  path: string;
  /** Sent on re-listen after a reconnect (§6 step 2). */
  lastRev: number;
  stale: boolean;
  listeners: Set<ValueListener>;
}

interface Pending {
  frame: WriteFrame;
  overlay: OverlayOp | null;
  settle: (f: Ack | CasFail) => void;
  fail: (e: Err | ClientClosedError) => void;
}

/** §6: CONNECTED -> (drop) -> WAITING -> CONNECTING -> CONNECTED. */
export type ClientState = 'idle' | 'connecting' | 'connected' | 'waiting' | 'closed';

/** §6 full jitter: delay = random(0, min(30s, 1s * 2^attempt)). `attempt` is 0-based. */
export const backoffDelay = (
  attempt: number,
  limits: Limits = DEFAULT_LIMITS,
  random: () => number = Math.random,
): number => Math.floor(random() * Math.min(limits.BACKOFF_CAP_MS, 1000 * 2 ** attempt));

export interface ClientOptions {
  url: string;
  token: string;
  sdk?: string;
  limits?: Limits;
  /** §5: 25s foreground / 60s backgrounded. Tests shrink this. */
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
  /** Off for tests that assert a single connection attempt. Default true. */
  autoReconnect?: boolean;
}

/**
 * Harness client — a real protocol citizen (WORKLOAD §5), not a mock: same reconnect FSM,
 * same frames, real WebSockets. Gate B implements the connection half of the contract;
 * subscriptions, the pending queue and the mirror (§3/§7) arrive at Gate C.
 *
 * Events: `state` (ClientState), `frame` (every ServerFrame, batches already unwrapped),
 * `helloAck` (HelloAck), `close` ({code, reason}), `authFailure` ({code, reason}),
 * `epochChange` (§2 v1.5 wholesale drop).
 */
export class RtdbClient extends EventEmitter {
  readonly limits: Limits;
  #ws: WebSocket | null = null;
  #state: ClientState = 'idle';
  #attempt = 0;
  #closing = false;
  #session: string | null = null;
  #ping: NodeJS.Timeout | null = null;
  #pong: NodeJS.Timeout | null = null;
  #retry: NodeJS.Timeout | null = null;
  #stable: NodeJS.Timeout | null = null;
  #token: string;
  readonly mirror = new Mirror();
  readonly #subs = new Map<number, Sub>();
  /** Insertion-ordered, which IS the replay order §6 step 3 requires. */
  readonly #pending = new Map<string, Pending>();
  readonly #infoListeners = new Set<ValueListener>();
  #nextSubId = 1;

  constructor(readonly opts: ClientOptions) {
    super();
    this.limits = opts.limits ?? DEFAULT_LIMITS;
    this.#token = opts.token;
  }

  get state(): ClientState {
    return this.#state;
  }

  get session(): string | null {
    return this.#session;
  }

  /**
   * Start (or restart) the FSM. After an auth failure the only way back is a call with a FRESH
   * token — §6 (v1.2) forbids auto-retrying a 4401 with the same one.
   */
  connect(token?: string): void {
    if (token !== undefined) this.#token = token;
    if (this.#state === 'connecting' || this.#state === 'connected') return;
    this.#closing = false;
    this.#open();
  }

  /** Stop for good: no reconnect. */
  close(): void {
    this.#closing = true;
    this.#clearTimers();
    if (this.#stable) clearTimeout(this.#stable);
    this.#ws?.close();
    this.#ws = null;
    this.#setState('closed');
  }

  send(frame: ClientFrame): void {
    if (this.#ws?.readyState !== WebSocket.OPEN) return;
    this.#ws.send(JSON.stringify(frame));
  }

  /** Resolves on the next helloAck. */
  ready(): Promise<HelloAck> {
    if (this.#state === 'connected') return Promise.resolve(this.#lastAck as HelloAck);
    return new Promise((resolve) => this.once('helloAck', resolve));
  }

  #lastAck: HelloAck | null = null;
  /** §2 (v1.5): the shard generation our mirrors, revs and lastRevs belong to. */
  #epoch: number | null = null;

  get epoch(): number | null {
    return this.#epoch;
  }

  /** §2 (v1.5) wholesale drop: mirrors, per-leaf revs, tombstones and every stored lastRev. */
  #dropGeneration(): void {
    this.mirror.dropServerState();
    for (const sub of this.#subs.values()) {
      sub.lastRev = 0;
      sub.stale = false;
    }
    this.emit('epochChange');
  }

  // ------------------------------------------------------------------ §3 subscriptions

  /**
   * Subscribe to `path`. The listener fires with the full mirrored subtree after the initial
   * snapshot/catch-up and after every applied change — server delta OR local optimistic write —
   * always from the mirror, never a network round-trip (§7).
   */
  listen(path: string, onValue?: ValueListener): () => void {
    if (path === INFO_CONNECTED) return this.#listenInfo(onValue);

    const subId = this.#nextSubId++;
    const sub: Sub = { subId, path, lastRev: 0, stale: false, listeners: new Set() };
    if (onValue) sub.listeners.add(onValue);
    this.#subs.set(subId, sub);
    if (this.#state === 'connected') this.#sendListen(sub);
    return () => this.unlisten(subId);
  }

  unlisten(subId: number): void {
    if (!this.#subs.delete(subId)) return;
    this.send({ type: 'unlisten', subId });
  }

  /** The mirrored value at `path`: serverState ⊕ pendingOverlay (§7). */
  value(path: string): Json {
    return this.mirror.view(path);
  }

  // ------------------------------------------------------------------ §4 writes

  put(path: string, value: Json, writeId: string = randomUUID()): Promise<Ack | CasFail> {
    return this.#write({ type: 'put', writeId, path, value }, { op: 'put', path, value });
  }

  merge(path: string, value: { [k: string]: Json }, writeId: string = randomUUID()): Promise<Ack | CasFail> {
    return this.#write({ type: 'merge', writeId, path, value }, { op: 'merge', path, value });
  }

  /** A casFail is a normal outcome and RESOLVES; only an `err` rejects (§4). */
  cas(path: string, expectedRev: number, value: Json): Promise<Ack | CasFail> {
    return this.#write({ type: 'cas', writeId: randomUUID(), path, expectedRev, value }, { op: 'put', path, value });
  }

  /** Replay a write under a writeId we choose — the lost-ack case, driven explicitly (§6). */
  resend(writeId: string): void {
    const p = this.#pending.get(writeId);
    if (p) this.send(p.frame);
  }

  get pendingWriteIds(): string[] {
    return [...this.#pending.keys()];
  }

  /** Live subscriptions, for convergence checks. */
  get subscriptions(): { subId: number; path: string; lastRev: number }[] {
    return [...this.#subs.values()].map((s) => ({ subId: s.subId, path: s.path, lastRev: s.lastRev }));
  }

  // ------------------------------------------------------------------ internals

  #setState(s: ClientState): void {
    if (this.#state === s) return;
    const wasConnected = this.#state === 'connected';
    this.#state = s;
    this.emit('state', s);
    if (wasConnected !== (s === 'connected')) this.#fireInfo();
    // P1 guard 2 of 2: every terminal path — close(), a 4401 (§6 v1.2), autoReconnect off — funnels
    // through here. Past it there is no reconnect to run §6 step 3, so a queued write is abandoned,
    // not pending. Tell the app rather than leaving the promise dangling.
    if (s === 'closed') this.#failPending();
  }

  /** Settle every queued write as abandoned. Keyed off a snapshot: #settle mutates #pending. */
  #failPending(): void {
    for (const writeId of [...this.#pending.keys()]) {
      this.#settle(writeId, (p) => p.fail(new ClientClosedError(writeId)));
    }
  }

  #listenInfo(onValue?: ValueListener): () => void {
    if (onValue) {
      this.#infoListeners.add(onValue);
      onValue(this.#state === 'connected');
    }
    return () => {
      if (onValue) this.#infoListeners.delete(onValue);
    };
  }

  #sendListen(sub: Sub): void {
    this.send({
      type: 'listen',
      subId: sub.subId,
      path: sub.path,
      ...(sub.lastRev > 0 ? { lastRev: sub.lastRev } : {}),
    });
  }

  #write(frame: WriteFrame, overlay: OverlayOp): Promise<Ack | CasFail> {
    // P1 guard 1 of 2: `closed` is terminal — `send()` would drop the frame and nothing would ever
    // replay it, so queueing here is how a write hangs forever. Reject BEFORE the overlay entry and
    // the pending row exist, so an abandoned write leaves no trace in the mirror either.
    if (this.#state === 'closed') return Promise.reject(new ClientClosedError(frame.writeId));
    return new Promise((resolve, reject) => {
      // §7: the write joins the overlay as an OPERATION the moment it is issued, so the local view
      // is optimistic immediately; it leaves on ack/err/casFail. There is no rollback.
      this.mirror.overlay.push(overlay);
      this.#pending.set(frame.writeId, { frame, overlay, settle: resolve, fail: reject });
      this.send(frame);
      this.#fire(frame.path);
    });
  }

  /** Settle one pending write and drop its overlay entry (§7: ack/err/casFail all remove it). */
  #settle(writeId: string, done: (p: Pending) => void): void {
    const p = this.#pending.get(writeId);
    if (!p) return; // an ack for a write we already settled: harmless, and expected after a replay
    this.#pending.delete(writeId);
    if (p.overlay) {
      const i = this.mirror.overlay.indexOf(p.overlay);
      if (i >= 0) this.mirror.overlay.splice(i, 1);
    }
    done(p);
    this.#fire(p.frame.path);
  }

  /** Fire onValue for every sub the change could be visible in (§3 routing, §7 events). */
  #fire(changedPath: string): void {
    for (const sub of this.#subs.values()) {
      if (!isRelevant(sub.path, changedPath)) continue;
      const value = this.mirror.view(sub.path);
      for (const cb of sub.listeners) cb(value);
    }
  }

  #fireInfo(): void {
    const connected = this.#state === 'connected';
    for (const cb of this.#infoListeners) cb(connected);
  }

  #open(): void {
    this.#setState('connecting');
    const ws = new WebSocket(this.opts.url);
    this.#ws = ws;
    ws.addEventListener('open', () => {
      // §6 step 1 of the reconnect order: hello, always first.
      this.send({
        type: 'hello',
        proto: 1,
        token: this.#token,
        ...(this.opts.sdk !== undefined ? { sdk: this.opts.sdk } : {}),
      });
    });
    ws.addEventListener('message', (ev) => {
      if (typeof ev.data === 'string') this.#onText(ev.data);
    });
    // A failed CONNECT fires `error` and NEVER fires `close`, leaving readyState at CONNECTING
    // forever. Treating `close` as the only teardown signal strands the FSM on the first retry that
    // finds the server still down — which is every restart. Both events route to #down, which is
    // idempotent per socket.
    ws.addEventListener('error', () => this.#down(ws, 1006, 'connection failed'));
    ws.addEventListener('close', (ev) => this.#down(ws, ev.code, ev.reason));
  }

  #down(ws: WebSocket, code: number, reason: string): void {
    if (this.#ws !== ws) return; // a stale socket we already replaced, or already torn down
    this.#ws = null;
    if (ws.readyState !== WebSocket.CLOSED) {
      try {
        ws.close();
      } catch {
        /* already going away */
      }
    }
    this.emit('close', { code, reason });
    this.#clearTimers();
    // §6 (v1.2): a 4401 must NOT be auto-retried with the same token. Surface it and stop;
    // the app reconnects via connect(newToken) once its token source yields one.
    if (code === CLOSE.AUTH) {
      this.#setState('closed');
      this.emit('authFailure', { code, reason });
      return;
    }
    if (this.#closing || this.opts.autoReconnect === false) return this.#setState('closed');
    this.#setState('waiting');
    this.#scheduleRetry();
  }

  #onText(text: string): void {
    let frame: ServerFrame;
    try {
      frame = JSON.parse(text) as ServerFrame;
    } catch {
      return; // a server that sends us garbage is a server bug; nothing useful to do here
    }
    this.#apply(frame);
  }

  #apply(frame: ServerFrame): void {
    // §3: a batch's inner frames are processed in array order exactly as if received individually.
    if (frame.type === 'batch') {
      for (const inner of frame.frames) this.#apply(inner);
      return;
    }
    this.emit('frame', frame);
    switch (frame.type) {
      case 'helloAck':
        this.#lastAck = frame;
        this.#session = frame.session;
        // §2 (v1.5): a different epoch than the one we stored means the shard was restored or
        // reset — every rev we hold is from a dead generation. Drop the lot BEFORE the re-listens
        // below, so they go out with no lastRev and come back as fresh snapshots.
        if (this.#epoch !== null && this.#epoch !== frame.epoch) this.#dropGeneration();
        this.#epoch = frame.epoch;
        this.#setState('connected');
        // §6: the attempt counter resets only after the connection has been stable.
        this.#stable = setTimeout(() => (this.#attempt = 0), this.limits.BACKOFF_RESET_MS);
        this.#stable.unref();
        // §6, and the order is normative: (2) every listen with its stored lastRev, (3) every
        // pending write in its original order under its original writeId, (4) pings resume.
        for (const sub of this.#subs.values()) this.#sendListen(sub);
        for (const p of this.#pending.values()) this.send(p.frame);
        this.#startPings();
        this.emit('helloAck', frame);
        return;
      case 'pong':
        if (this.#pong) clearTimeout(this.#pong);
        this.#pong = null;
        return;

      case 'snapshot': {
        const sub = this.#subs.get(frame.subId);
        if (!sub) return; // unlistened while it was in flight
        this.mirror.applySnapshot(frame.path, frame.value, frame.rev);
        sub.lastRev = frame.rev;
        sub.stale = false;
        this.#fire(frame.path);
        return;
      }

      case 'delta': {
        this.mirror.applyDelta(frame);
        // Deltas carry no subId: every sub the path is relevant to advances (§3). Gaps in a sub's
        // rev sequence are NORMAL — the client never gap-detects.
        for (const sub of this.#subs.values()) {
          if (isRelevant(sub.path, frame.path)) sub.lastRev = Math.max(sub.lastRev, frame.rev);
        }
        this.#fire(frame.path);
        return;
      }

      case 'resync': {
        // §3: mark stale, do NOT clear serverState — the snapshot that follows replaces it, and
        // clearing now would only make the UI flicker.
        const sub = this.#subs.get(frame.subId);
        if (sub) sub.stale = true;
        this.emit('resync', frame);
        return;
      }

      case 'ack':
        this.#settle(frame.writeId, (p) => p.settle(frame));
        return;

      case 'casFail':
        this.#settle(frame.writeId, (p) => p.settle(frame));
        return;

      case 'err': {
        if (frame.writeId !== undefined) {
          // §4: an err-rejected write leaves the pending queue and surfaces; never auto-retried.
          this.#settle(frame.writeId, (p) => p.fail(frame));
          return;
        }
        if (frame.subId !== undefined) {
          this.#subs.delete(frame.subId);
          this.emit('subError', frame);
        }
        return;
      }

      default:
        return;
    }
  }

  /** §5: first ping right after helloAck, then on interval; no pong within 10s -> close + reconnect. */
  #startPings(): void {
    const every = this.opts.pingIntervalMs ?? this.limits.PING_FG_MS;
    const sendPing = (): void => {
      this.send({ type: 'ping', t: Date.now() });
      if (this.#pong) return; // one outstanding pong timer is enough
      this.#pong = setTimeout(() => {
        this.#pong = null;
        this.emit('pongTimeout');
        this.#ws?.close(); // -> close handler -> WAITING -> backoff -> CONNECTING
      }, this.opts.pongTimeoutMs ?? this.limits.PONG_TIMEOUT_MS);
      this.#pong.unref();
    };
    sendPing();
    this.#ping = setInterval(sendPing, every);
    this.#ping.unref();
  }

  #scheduleRetry(): void {
    const delay = backoffDelay(this.#attempt++, this.limits);
    this.emit('retry', { attempt: this.#attempt, delay });
    this.#retry = setTimeout(() => this.#open(), delay);
    this.#retry.unref();
  }

  #clearTimers(): void {
    if (this.#ping) clearInterval(this.#ping);
    if (this.#pong) clearTimeout(this.#pong);
    if (this.#retry) clearTimeout(this.#retry);
    if (this.#stable) clearTimeout(this.#stable);
    this.#ping = this.#pong = this.#retry = this.#stable = null;
  }
}
