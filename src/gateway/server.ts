import { randomBytes } from 'node:crypto';
import uWS from 'uWebSockets.js';
import type { ErrCode, ServerFrame } from '../protocol/frames.ts';
import { CLOSE } from '../protocol/frames.ts';
import { DEFAULT_LIMITS, type Limits } from '../protocol/limits.ts';
import { parseClientFrame } from '../protocol/validate.ts';
import { Dispatcher } from '../fanout/dispatcher.ts';
import { OrderedStream } from '../fanout/stream.ts';
import { RedisBus, type Redis } from '../fanout/redis.ts';
import { ConnectionSink, SubscriptionRegistry, type Transport } from '../fanout/subscriptions.ts';
import { allowAll, type Rules } from '../pipeline/rules.ts';
import { RateLimiter, WritePipeline } from '../pipeline/write.ts';
import type { Delta } from '../protocol/frames.ts';
import type { StorageAdapter } from '../storage/adapter.ts';
import { MemoryStorage } from '../storage/memory.ts';
import { DevHs256Validator, type AuthValidator } from './auth.ts';
import * as M from './metrics.ts';

/** Per-connection state. `userId === null` means hello has not been accepted yet (§2). */
interface ConnData {
  session: string;
  /** U3: `performance.now()` at open, so the close line can carry how long the socket held. */
  openedAt: number;
  userId: string | null;
  /** The token's console role (§5.8), or null: app tokens carry none and never will. */
  role: string | null;
  /** hello accepted, auth still in flight — a second hello here must not re-run validation. */
  authPending: boolean;
  /** uWS invalidates the socket after close; every send must check this first. */
  closed: boolean;
  sink: ConnectionSink;
  rate: RateLimiter;
}

export interface GatewayOptions {
  /** 0 (default) binds an ephemeral port — read the real one from `Gateway.port`. */
  port?: number;
  host?: string;
  limits?: Limits;
  auth?: AuthValidator;
  region?: string;
  storage?: StorageAdapter;
  rules?: Rules;
  /** Override the helloAck head rev (§2). Defaults to the storage head. */
  head?: () => number | Promise<number>;
  /**
   * A connected client joins this gateway to §8's Redis bus: candidate for the shard's one
   * dispatcher, and (Gate B) consumer of its stream. Absent, the gateway is the single-process
   * deployment it has always been (WORKLOAD §0.7) — Redis is additive, never a fallback.
   */
  redis?: Redis;
  /** §9 retention chore. On the bus it runs ONLY while leader (WP4 Gate D ruling Q4). */
  prune?: { intervalMs: number; run: () => Promise<unknown> };
  /**
   * Which shard's bus keys this gateway uses. v1 runs shard 0; tests give each harness its own so
   * many independent shards can share one Redis, exactly as they share one Postgres.
   */
  shard?: string | number;
  /**
   * §8's leader lock TTL, ms. WP5 Gate C established what this number actually budgets: the lock is
   * a PUBLISH budget, not a delivery budget — client-visible convergence does not wait for failover,
   * because the consumer's own reconcile carries delivery. Unset keeps `Leadership`'s 3000 ms.
   */
  lockTtlMs?: number;
  /**
   * U3 (WORKLOAD §2): where the connection-lifecycle lines go. The default is one JSON object per
   * line on stdout, which is what journald collects in production. Tests pass a sink so they can
   * assert on the lines instead of racing the test reporter for the same stream.
   */
  log?: (line: string) => void;
}

export interface Gateway {
  port: number;
  storage: StorageAdapter;
  close(): void;
}

const newSession = (): string => `s_${randomBytes(4).toString('hex')}`;

/** uWS `ws.send()` return value for "refused: over the backpressure limit". */
const UWS_DROPPED = 2;

export async function startGateway(opts: GatewayOptions = {}): Promise<Gateway> {
  const limits = opts.limits ?? DEFAULT_LIMITS;
  const auth = opts.auth ?? new DevHs256Validator();
  const region = opts.region ?? 'ap-south-1';
  const storage = opts.storage ?? new MemoryStorage(limits);
  const rules = opts.rules ?? allowAll;
  const head = opts.head ?? (() => storage.head());

  const writeLine = opts.log ?? ((line: string): void => void process.stdout.write(`${line}\n`));
  /**
   * U3: ONE structured line per connection-lifecycle event — open, close, subscribe, unsubscribe —
   * and nothing else. Bounded by construction: a gateway pushing 100k deltas/s writes nothing at all
   * while its connections simply hold, which is what makes always-on affordable (§2's "bounded
   * volume — no per-delta logging"). `connId` is the session the client is handed in helloAck, so a
   * client-side complaint and a server-side line name the same socket — which is the whole point:
   * the 2026-08-29 load test could not diagnose a stalled client because nothing was written down.
   */
  const logEvent = (ev: string, fields: Record<string, unknown>): void =>
    writeLine(JSON.stringify({ ts: new Date().toISOString(), ev, ...fields }));

  const registry = new SubscriptionRegistry();
  const pipeline = new WritePipeline(
    storage,
    rules,
    limits,
    (fields) => logEvent('console-write', fields),
    (ev, fields) => logEvent(ev, fields),
  );

  /** Open connections — needed for teardown, and for §10 `kick` when the admin plane lands. */
  const live = new Set<uWS.WebSocket<ConnData>>();

  /**
   * §8: oplog -> ONE dispatcher -> the shard's stream -> connections. Which stream is the only
   * difference between the two deployments, and delivery cannot tell them apart:
   *
   * - on the bus, the leader's dispatcher XADDs and EVERY gateway's consumer reads back;
   * - off it, one in-process dispatcher appends to an `OrderedStream` this process subscribes to.
   */
  const bus = opts.redis
    ? new RedisBus(opts.redis, storage, {
        maxLen: limits.OPLOG_RETENTION_REVS,
        ...(opts.shard !== undefined ? { shard: opts.shard } : {}),
        ...(opts.lockTtlMs !== undefined ? { ttlMs: opts.lockTtlMs } : {}),
        ...(opts.prune ? { prune: opts.prune } : {}),
        onDelta: (delta) => registry.route(delta),
        onHistoryLost: () => {
          for (const ws of [...live]) ws.getUserData().sink.resyncAll();
        },
        // §10: close every connection this user holds HERE — the publisher fanned the frame out to
        // every gateway, and §3's subscribe-time auth means the close plus re-auth on reconnect is
        // the enforcement point. Nothing client-visible beyond the code.
        onKick: (kick) => {
          for (const ws of [...live]) {
            if (ws.getUserData().userId === kick.target.userId) ws.end(CLOSE.KICK, 'kick');
          }
        },
      })
    : null;
  await bus?.start();

  const stream = bus ? null : new OrderedStream<Delta>(limits.OPLOG_RETENTION_REVS);
  stream?.subscribe((delta) => registry.route(delta));
  const dispatcher = stream ? new Dispatcher(storage, stream) : null;
  dispatcher?.start();

  // Off the bus there is exactly one gateway, so it is trivially the leader and the chore is its own.
  const chore = opts.prune;
  const prune =
    bus === null && chore
      ? setInterval(() => void chore.run().catch(() => undefined), chore.intervalMs)
      : null;
  prune?.unref();

  const send = (ws: uWS.WebSocket<ConnData>, frame: ServerFrame): void => {
    if (ws.getUserData().closed) return;
    ws.send(JSON.stringify(frame), false);
  };

  const sendErr = (
    ws: uWS.WebSocket<ConnData>,
    code: ErrCode,
    msg: string,
    scope: { subId?: number; writeId?: string } = {},
  ): void => send(ws, { type: 'err', code, msg, ...scope });

  /**
   * §2 hello is the only path that awaits I/O before a connection is usable, and BOTH awaits can
   * reject — an RDS failover kills in-flight queries and we run on RDS with PITR. Uncaught, that
   * rejection is not this connection's problem: Node 22 terminates the process on an unhandled
   * rejection and `deploy/compose.prod.yml:37` restarts it, so one failed query becomes a crash
   * LOOP — every restart drops every other connection on this gateway and the NLB moves them to its
   * twin, which then carries double load under the very condition that killed the first one.
   *
   * Containment is per connection: this socket dies, the process keeps serving everyone else.
   *
   * No `err` frame goes with it, deliberately. §4's err vocabulary is closed —
   * `AUTH|RULES|BADPATH|BADFRAME|TOOBIG|RATE` — and every one of those blames the CLIENT for
   * something it sent. This is our database failing, and PROTOCOL.md v1.5 is frozen, so inventing
   * an `INTERNAL` code is a protocol change this package is not scoped to make. The close carries
   * the whole message, which is what the client acts on anyway.
   */
  const failHello = (ws: uWS.WebSocket<ConnData>, where: string, cause: unknown): void => {
    const d = ws.getUserData();
    d.authPending = false;
    logEvent('hello-failed', {
      connId: d.session,
      where,
      err: String(cause instanceof Error ? cause.message : cause),
    });
    if (d.closed) return;
    // 1011 (RFC 6455 "internal error"), NOT one of §2's 44xx codes: 4400 and 4401 both accuse the
    // client, and 4401 is TERMINAL on ours (§6 v1.2 — only connect(newToken) leaves it). An RDS
    // failover is precisely the transient a client should ride out, so this must stay retryable.
    ws.end(1011, 'internal');
  };

  const app = uWS.App().ws<ConnData>('/*', {
    idleTimeout: limits.IDLE_TIMEOUT_SEC, // §5
    // §5 liveness is application-level ping/pong. WebSocket-protocol pings would be auto-answered
    // by the client's transport and would keep a dead app alive past idleTimeout.
    //
    // LOAD-BEARING, and for more than that: this line is also the only bound on a connection stuck
    // BEFORE helloAck. The client arms no timer until helloAck lands, so with automatic pings ON
    // the transport answers them, the socket never goes idle, and the hang is UNBOUNDED. Measured,
    // not argued — `scripts/uws-idle-probe.ts` config C: still open at 5.6x idleTimeout, never
    // fired; configs A and B close at 1.0x with this line as it stands. Do not delete it in a
    // refactor because §5 is "handled elsewhere" — §5 is not what it is holding up.
    sendPingsAutomatically: false,
    // §9 caps frames at 1 MiB; we allow twice that at the socket so an oversize frame arrives and
    // can be answered with an err instead of being dropped by the transport. The 2x is the backstop.
    maxPayloadLength: limits.FRAME_MAX * 2,

    open(ws) {
      const d = ws.getUserData();
      const transport: Transport = {
        // uWS: 1 = sent, 0 = buffered as backpressure (it will drain), 2 = DROPPED because the
        // backpressure limit is already exceeded. Only the last one is a lost frame.
        send: (text) => !d.closed && ws.send(text, false) !== UWS_DROPPED,
        bufferedAmount: () => (d.closed ? 0 : ws.getBufferedAmount()),
        closed: () => d.closed,
      };
      Object.assign(d, {
        session: newSession(),
        openedAt: performance.now(),
        userId: null,
        role: null,
        authPending: false,
        closed: false,
        // The log closure reads `d.session` lazily — it is being assigned in this same literal,
        // and by the time a subscription can be abandoned it is long set.
        sink: new ConnectionSink(transport, registry, storage, limits, (ev, fields) =>
          logEvent(ev, { connId: d.session, ...fields }),
        ),
        rate: new RateLimiter(limits.WRITE_RATE_PER_SEC, limits.WRITE_RATE_BURST),
      });
      live.add(ws);
      logEvent('open', { connId: d.session });
    },

    message(ws, message, isBinary) {
      const d = ws.getUserData();
      if (isBinary) {
        // §Transport: text frames. Binary is not a v1 encoding (MessagePack is v2, negotiated).
        if (d.userId === null) return void ws.end(CLOSE.PRE_HELLO, 'pre-hello');
        return sendErr(ws, 'BADFRAME', 'binary frames are not supported in v1');
      }
      if (message.byteLength > limits.FRAME_MAX) {
        if (d.userId === null) return void ws.end(CLOSE.PRE_HELLO, 'pre-hello');
        return sendErr(ws, 'TOOBIG', `frame exceeds ${limits.FRAME_MAX} bytes`);
      }

      const r = parseClientFrame(Buffer.from(message).toString('utf8'), limits);

      // §2: the client MUST NOT send anything before hello. Before hello, the forward-compat
      // "ignore unknown frames" rule does not apply — the lifecycle rule does, and it is a close.
      if (d.userId === null) {
        if (r.kind !== 'frame' || r.frame.type !== 'hello') {
          return void ws.end(CLOSE.PRE_HELLO, 'expected hello');
        }
        // A second hello while the first is still validating would double-send helloAck.
        if (d.authPending) return;
        const { proto, token } = r.frame;
        if (proto !== 1) {
          sendErr(ws, 'BADFRAME', `unsupported proto ${proto}`);
          return void ws.end(CLOSE.PRE_HELLO, 'unsupported proto');
        }
        d.authPending = true;
        void Promise.resolve(auth.validate(token))
          .then((res) => {
            d.authPending = false;
            if (d.closed) return;
            if (!res.ok) {
              sendErr(ws, 'AUTH', res.msg);
              return void ws.end(CLOSE.AUTH, 'AUTH');
            }
            d.userId = res.userId;
            d.role = res.role ?? null;
            // Its own catch, and its own `where`: a storage failure and an auth-backend failure are
            // different incidents and the log line has to say which. The inner chain is `void`-ed,
            // so its rejection never reaches the outer catch below.
            void Promise.all([head(), storage.epoch()])
              .then(([rev, epoch]) => {
                if (!d.closed) send(ws, { type: 'helloAck', rev, epoch, region, session: d.session });
              })
              .catch((e: unknown) => failHello(ws, 'head', e));
          })
          .catch((e: unknown) => failHello(ws, 'auth', e));
        return;
      }

      switch (r.kind) {
        case 'ignore':
          return; // §Transport: unknown frame types are ignored, never errors.
        case 'reject':
          return sendErr(ws, r.code, r.msg, {
            ...(r.subId !== undefined ? { subId: r.subId } : {}),
            ...(r.writeId !== undefined ? { writeId: r.writeId } : {}),
          });
        case 'frame':
          switch (r.frame.type) {
            case 'ping':
              // §5: `t` is echoed verbatim; clocks are never compared across machines.
              return send(ws, { type: 'pong', t: r.frame.t });
            case 'pong':
              return; // v1 servers never ping at the application level.
            case 'hello':
              return; // v1 validates at connect time only; a second hello is a no-op (§2).
            case 'listen': {
              const { subId, path } = r.frame;
              // §3: read authorization is evaluated ONCE, here. From now on topic membership IS the
              // authorization — deltas are never rules-checked per recipient.
              if (!rules({ userId: d.userId, role: d.role, op: 'read', path })) {
                return d.sink.enqueue({ type: 'err', subId, code: 'RULES', msg: 'read denied' });
              }
              logEvent('subscribe', { connId: d.session, subId, path });
              const { lastRev } = r.frame;
              // No catch: §5.11 made `listen` never reject. A storage failure is repaired inside
              // it — §3 resync plus a spaced, bounded retry — and abandoning writes a `listen-
              // abandoned` line. The old `.catch(console.error)` told the client nothing at all and
              // left the socket open, which on our own SDK is a permanent silent hang.
              void d.sink.listen(subId, path, lastRev);
              return;
            }
            case 'unlisten': {
              const { subId } = r.frame;
              // Read the path BEFORE the unlisten drops the sub — after it there is nothing to name.
              logEvent('unsubscribe', { connId: d.session, subId, path: d.sink.subs.get(subId)?.path ?? null });
              return d.sink.unlisten(subId);
            }
            case 'put':
            case 'merge':
            case 'cas': {
              const op = r.frame.type;
              if (!d.rate.take()) {
                M.acks.inc({ op, result: 'rate' });
                return sendErr(ws, 'RATE', 'write rate exceeded', { writeId: r.frame.writeId });
              }
              M.writes.inc({ op });
              const startedAt = performance.now();
              return pipeline.submit({
                frame: r.frame,
                userId: d.userId,
                role: d.role,
                reply: (f) => {
                  M.observeAck(op, f, startedAt);
                  d.sink.enqueue(f);
                },
              });
            }
          }
      }
    },

    close(ws, code) {
      const d = ws.getUserData();
      d.closed = true;
      // Before sink.close(), which clears the subs this line is meant to count.
      logEvent('close', {
        connId: d.session,
        code,
        ms: Math.round(performance.now() - d.openedAt),
        subs: d.sink.subs.size,
      });
      d.sink.close();
      live.delete(ws);
    },
  });

  // Off the bus there is one gateway, so it is trivially the leader, trivially publishing, and its
  // consumer cannot lag a stream it does not read from.
  M.bindSources({
    connections: () => live.size,
    leader: () => (bus === null || bus.isLeader ? 1 : 0),
    publishing: () => (bus === null || bus.publishing ? 1 : 0),
    lagRevs: async () => (bus === null ? 0 : (await storage.head()) - bus.deliveredRev),
  });

  const token = await new Promise<uWS.us_listen_socket>((resolve, reject) => {
    const cb = (t: uWS.us_listen_socket | false): void =>
      t ? resolve(t) : reject(new Error(`gateway failed to listen on port ${opts.port ?? 0}`));
    if (opts.host) app.listen(opts.host, opts.port ?? 0, cb);
    else app.listen(opts.port ?? 0, cb);
  });

  let stopped = false;
  return {
    port: uWS.us_socket_local_port(token),
    storage,
    // Idempotent on purpose: us_listen_socket_close on an already-freed socket segfaults the
    // process, and shutdown paths (test teardown, signal handlers) double-fire routinely.
    close() {
      if (stopped) return;
      stopped = true;
      dispatcher?.stop();
      void bus?.stop();
      if (prune) clearInterval(prune);
      for (const ws of [...live]) ws.end(1001, 'server shutting down');
      uWS.us_listen_socket_close(token);
    },
  };
}
