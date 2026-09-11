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
import { allowAll, outsideOwnDatabase, type Rules } from '../pipeline/rules.ts';
import { AcquisitionQuota, RateLimiter, WritePipeline } from '../pipeline/write.ts';
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
  /** The token's database claim (§5.20 Phase 1), or null for an unscoped token. */
  ns: string | null;
  /** hello accepted, auth still in flight — a second hello here must not re-run validation. */
  authPending: boolean;
  /** uWS invalidates the socket after close; every send must check this first. */
  closed: boolean;
  /**
   * §5.22 Gate D: created at HELLO, not at open — it needs the tenant's registry and store, and the
   * tenant is chosen from the token's `ns`, which does not exist until auth resolves. Null before
   * then, which costs nothing: §2 forbids a client from sending anything but hello first, so no
   * path can reach a sink that is not there yet.
   */
  sink: ConnectionSink | null;
  /** The database this connection is on. Null until hello picks it. */
  tenant: Tenant | null;
  /** Kept from `open` so the sink can be built later, at hello, once the tenant is known. */
  transport: Transport;
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
  /**
   * §5.22 Gate E: which database this gateway's metrics are labelled with. Unset is today's
   * single-tenant deployment and every test — `DEFAULT_DB_LABEL`, a real value rather than an
   * omitted label, because a series that sometimes carries `db` is two series to every query.
   *
   * §5.22 Gate D: this is the DEFAULT tenant's name. A connection whose token carries no `ns`
   * lands here, which is what keeps the single-database deployment — and all 8 files that call
   * `startGateway` — behaving exactly as before.
   */
  db?: string;
  /**
   * §5.22 Gate D: build the store for one database, on demand.
   *
   * Unset, this gateway is single-tenant: every connection uses `storage`, whatever its `ns` says,
   * which is today's deployment and every existing test. Set, the gateway becomes the `ns ->
   * {storage, pipeline, dispatcher|bus}` map §5.20 Phase 3 asks for — and `storage` is the DEFAULT
   * tenant's store, still returned as `Gateway.storage` because 34 assertions in the suite read it.
   *
   * LAZY by construction: a database that has been declared but never connected to costs nothing.
   * That is not an optimisation, it is what makes "the client hands an empty database to their
   * team" free — §5.19's whole reason for the registry.
   */
  tenantStorage?: (db: string) => StorageAdapter;
  /**
   * §5.20 Phase 1: refuse any token that carries no `ns` claim — the multi-tenant posture. Off by
   * default, which is the single-database deployment this gateway has always been: a token that
   * names a database is confined to it either way, and one that names none is unconstrained, exactly
   * as before. See `outsideOwnDatabase` for why absence is not permission once this is on.
   */
  requireNs?: boolean;
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
  /**
   * §5.23 faisla 5: how many gateways serve this shard, so each takes `1/G` of the database quota.
   * Unset is 1 — every test and every single-gateway deployment. Production sets 2.
   */
  gatewayCount?: number;
}

/**
 * A token naming a database that is not in the registry. Its own type because the hello chain has
 * to answer it differently from every other tenant-open failure: `AUTH`/4401 (terminal, the client
 * must not retry), not 1011 (transient, ride it out).
 */
class UnknownDatabase extends Error {}

/**
 * §5.22 Gate D: everything that is per-DATABASE, in one record.
 *
 * The list is not a design choice, it is what the code already coupled to one store: a
 * subscription registry (routing is per tree), a write pipeline (its `#chain` is what serialises
 * one database's commits), a dispatcher or a bus (one publisher per database's oplog), the §9
 * prune chore, the metrics binding, and the connections themselves — because §8's history-lost
 * resync and §10's kick must reach one database's sockets and no others.
 */
interface Tenant {
  readonly db: string;
  readonly storage: StorageAdapter;
  readonly registry: SubscriptionRegistry;
  readonly pipeline: WritePipeline;
  readonly bus: RedisBus | null;
  /** Open connections ON THIS DATABASE. Per tenant, or one tenant's incident wakes all of them. */
  readonly live: Set<uWS.WebSocket<ConnData>>;
  /** §5.23: this database's share of the shard's lock acquisitions. Charged at commit. */
  readonly quota: AcquisitionQuota;
  head(): number | Promise<number>;
  close(): void;
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
  const baseStorage = opts.storage ?? new MemoryStorage(limits);
  const rules = opts.rules ?? allowAll;
  const requireNs = opts.requireNs ?? false;

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

  const tenants = new Map<string, Tenant>();
  const defaultDb = opts.db ?? M.DEFAULT_DB_LABEL;

  /**
   * §5.22 Gate D: `head` overrides the rev `helloAck` carries, and every caller of it today is a
   * single-tenant test. What it should mean for N tenants — one head for all of them, or one per
   * tenant — has no caller to derive an answer from, so it is REFUSED rather than guessed. The day
   * a test needs a per-tenant head it will say which it meant, and that is when it gets designed.
   */
  if (opts.head && opts.tenantStorage) {
    throw new Error(
      'startGateway: `head` and `tenantStorage` are mutually exclusive — `head` overrides ONE ' +
        "tenant's rev, and which one it means for a multi-tenant gateway is not defined.",
    );
  }

  /**
   * Build one database's half of the gateway. Everything below is what `startGateway` used to do
   * exactly once, moved here verbatim — the single-tenant deployment is now the case where this
   * runs once, for `defaultDb`, which is why the 8 callers and 34 `gw.storage` assertions do not
   * move.
   */
  async function openTenant(db: string): Promise<Tenant> {
    const storage = db === defaultDb ? baseStorage : (opts.tenantStorage as (d: string) => StorageAdapter)(db);
    const registry = new SubscriptionRegistry();
    const live = new Set<uWS.WebSocket<ConnData>>();
    /**
     * §5.23 faisla 5: the bucket is PER GATEWAY, so a shard's quota is divided by how many
     * gateways serve it. `RTDB_GATEWAY_COUNT` is the deploy-review knob (production runs 2, giving
     * 32/64 per gateway); unset is 1, which is every test and every single-gateway deployment.
     *
     * ponytail: a fixed division, not a shared bucket. Redis would make it exact and would put a
     * round trip on the write path, which is the mistake §7 already paid for. The ceiling this
     * leaves is real and worth naming: an app whose connections all land on ONE gateway gets
     * `quota/G`, not `quota` — the NLB spreads connections, so this is a small app on a quiet
     * shard, not the normal case. `rtdb_quota_rejected_total{db}` is what makes it visible;
     * a per-database override in the registry is Phase 4.
     */
    const gateways = Math.max(1, opts.gatewayCount ?? 1);
    /**
     * §5.24 Gate C: the registry may override this database's rate. Read ONCE at open, beside the
     * storage the tenant is being built on — a bucket cannot be resized, and a quota that changed
     * under a running tenant would make the panel's number and the tenant's behaviour disagree
     * silently. `null`, or a name with no row at all (the DEFAULT tenant), is the shard's number.
     *
     * The BURST scales with it, so an override keeps the shape §5.23 chose rather than becoming a
     * different one: 2x the sustained rate, which is what makes one burst of any size affordable.
     */
    const row = await baseStorage.describeDatabase(db).catch(() => null);
    const perSec = row?.quotaAcqPerSec ?? limits.QUOTA_ACQ_PER_SEC;
    const burst = row?.quotaAcqPerSec === undefined || row?.quotaAcqPerSec === null
      ? limits.QUOTA_ACQ_BURST
      : row.quotaAcqPerSec * 2;
    const quota = new AcquisitionQuota(perSec / gateways, burst / gateways);
    // The SHARD's rate, not this gateway's share: the shard's rate is what a client was promised,
    // and the division by G is our deployment detail (§5.23 faisla 5).
    const unbindQuota = M.bindQuota(db, perSec, row?.quotaAcqPerSec != null);

    const pipeline = new WritePipeline(
      storage,
      rules,
      limits,
      (fields) => logEvent('console-write', { db, ...fields }),
      (ev, fields) => logEvent(ev, { db, ...fields }),
      // §5.23: one callback, two jobs, and both belong at COMMIT rather than at submit — at submit
      // nobody knows yet whether the write will ride a batch or go alone, which is the whole
      // 1.00-vs-500.00 spread the unit exists to price.
      () => {
        M.countAcquisition(db);
        quota.charge();
      },
      requireNs,
    );

    /**
     * §8: oplog -> ONE dispatcher -> the shard's stream -> connections. Which stream is the only
     * difference between the two deployments, and delivery cannot tell them apart:
     *
     * - on the bus, the leader's dispatcher XADDs and EVERY gateway's consumer reads back;
     * - off it, one in-process dispatcher appends to an `OrderedStream` this process subscribes to.
     *
     * Gate D: the bus keys are per SHARD (`redis.ts` `busKeys`), and a tenant is not a shard — so a
     * second tenant on one Redis needs its own key space or two databases share a stream. The shard
     * id therefore carries the database when there is more than one.
     */
    const bus = opts.redis
      ? new RedisBus(opts.redis, storage, {
          maxLen: limits.OPLOG_RETENTION_REVS,
          shard: busShard(db),
          ...(opts.lockTtlMs !== undefined ? { ttlMs: opts.lockTtlMs } : {}),
          ...(opts.prune ? { prune: opts.prune } : {}),
          onDelta: (delta) => registry.route(delta),
          onHistoryLost: () => {
            for (const ws of [...live]) ws.getUserData().sink?.resyncAll();
          },
          // §10: close every connection this user holds HERE — the publisher fanned the frame out
          // to every gateway, and §3's subscribe-time auth means the close plus re-auth on
          // reconnect is the enforcement point. Nothing client-visible beyond the code.
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

    // Off the bus there is one gateway, so it is trivially the leader, trivially publishing, and
    // its consumer cannot lag a stream it does not read from.
    // §5.23 Gate A: the rejection counter exists from the moment a database is served, at zero.
    // A series that appears only once it is non-zero reads as "no data" exactly when someone needs
    // to know that nothing was refused, and cannot be alerted on before the first incident.
    M.registerQuotaRejected(db);

    const unbindMetrics = M.bindSources(db, {
      connections: () => live.size,
      leader: () => (bus === null || bus.isLeader ? 1 : 0),
      publishing: () => (bus === null || bus.publishing ? 1 : 0),
      lagRevs: async () => (bus === null ? 0 : (await storage.head()) - bus.deliveredRev),
      // Only the Postgres adapter batches, so only it has these to report.
      applyStats: () =>
        ('applyStats' in storage ? storage.applyStats : undefined) ?? { groups: 0, orderedFallbacks: 0 },
    });

    return {
      db,
      storage,
      registry,
      pipeline,
      bus,
      live,
      quota,
      head: () => (db === defaultDb && opts.head ? opts.head() : storage.head()),
      close() {
        unbindMetrics();
        unbindQuota();
        dispatcher?.stop();
        void bus?.stop();
        if (prune) clearInterval(prune);
        for (const ws of [...live]) ws.end(1001, 'server shutting down');
      },
    };
  }

  /**
   * The shard id a database's bus keys hang off. Single-tenant keeps `opts.shard` untouched, which
   * is what every bus and cluster test depends on; multi-tenant appends the database, because
   * `busKeys(shard)` is the ONLY thing separating two streams on one Redis.
   */
  function busShard(db: string): string | number {
    const base = opts.shard ?? 0;
    // The DEFAULT tenant keeps `opts.shard` in BOTH modes, and that is not symmetry for its own
    // sake. `busKeys(shard)` names the stream, the leader lock, the fence and the epoch; moving the
    // default tenant to `0:_default` the moment a factory appears would change those keys on the
    // FIRST multi-tenant deploy. In the rolling window the old gateway publishes to shard `0` while
    // the new one consumes `0:_default` — and the bus IS cross-gateway delivery, so that is a
    // heavier version of Gate C's channel rename, on the path that carries every delta.
    return db === defaultDb ? base : `${base}:${db}`;
  }

  /**
   * §5.22 Gate F's precondition, and the hole it closes is Gate D's own.
   *
   * Opening a tenant runs the factory, which runs `PostgresStorage#init`, which is a
   * `CREATE SCHEMA IF NOT EXISTS` (`postgres.ts`). So until this check, any token signed with the
   * shard secret could name a database nobody declared and MAKE it, on hello, silently: §5.19's
   * "only an owner creates a database" was enforced at `/app-token`'s mint and nowhere on the
   * gateway. The refusal is `AUTH`/4401 rather than a storage error because that is what it is —
   * the token names a database it may not have, and §6 makes 4401 terminal, which is right for a
   * claim that will not become true by retrying.
   *
   * Asked of the REGISTRY on every open rather than from a boot-time snapshot: `tenants` already
   * caches an open tenant, so this is one indexed read per database per process, and a database
   * declared a minute ago has to be connectable without a restart. `listDeclared`, never
   * `topNodes` — the latter unions in every top-level key that has data, which would let an
   * undeclared namespace that once got written vouch for itself.
   *
   * The default tenant is exempt: it is the schema this gateway was CONFIGURED with, it exists
   * before any registry does, and it is never declared in one.
   */
  async function assertDeclared(db: string): Promise<void> {
    if (db === defaultDb) return;
    const declared = await baseStorage.listDeclared();
    if (!declared.includes(db)) throw new UnknownDatabase(`unknown database ${db}`);
  }

  /**
   * Lazily, and the laziness is load-bearing: a database that has been declared and never connected
   * to must cost nothing, or "hand your team an empty database" (§5.19) stops being free.
   *
   * Serialised through the map by storing the PROMISE, not the tenant — two connections arriving
   * together for a new database would otherwise each build a store, a bus and a metrics binding,
   * and the second would silently replace the first.
   */
  const opening = new Map<string, Promise<Tenant>>();
  function tenantFor(db: string): Promise<Tenant> {
    const live = tenants.get(db);
    if (live) return Promise.resolve(live);
    let p = opening.get(db);
    if (!p) {
      // `finally`, not `then`: deleted on REJECTION too. A first open that fails — the factory
      // throwing, or `bus.start()` losing a race with a Redis blip — would otherwise leave its
      // rejected promise in the map forever, and every later connection to that database would be
      // handed the same stale failure until the process restarted. A transient made permanent.
      p = assertDeclared(db)
        .then(() => openTenant(db))
        .then((t) => {
          tenants.set(db, t);
          return t;
        })
        .finally(() => void opening.delete(db));
      opening.set(db, p);
    }
    return p;
  }

  // The default tenant is opened EAGERLY, because `Gateway.storage`, `/healthz` and every existing
  // caller expect a working gateway the moment `startGateway` resolves.
  const defaultTenant = await tenantFor(defaultDb);

  /**
   * Connections that have opened but not yet said hello, so they belong to no database. Teardown
   * has to reach them too — otherwise a `close()` during a client's first 20 ms leaves a socket
   * nobody owns.
   */
  const pending = new Set<uWS.WebSocket<ConnData>>();
  // §5.22 Gate D (R7): pre-hello sockets are on no database, so they are in no `rtdb_connections`
  // label. Unregistered on close, like every other binding this gateway makes.
  const unbindPending = M.bindPending(() => pending.size);

  /**
   * §5.24 Gate A: through the connection's TRANSPORT, not straight to `ws.send`.
   *
   * The transport is where `rtdb_wire_bytes_out_total` is counted, and until this line every frame
   * that does not belong to a subscription — `helloAck`, `ack`, `casFail`, `err`, `pong` — reached
   * the socket by a road with no meter on it. That is not a rounding error on a write-heavy app:
   * an ack per write is the traffic.
   */
  const send = (ws: uWS.WebSocket<ConnData>, frame: ServerFrame): void => {
    const d = ws.getUserData();
    if (d.closed) return;
    d.transport.send(JSON.stringify(frame));
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
        send: (text) => {
          if (d.closed) return false;
          // §5.24 Gate A: counted BEFORE the send and whatever the socket then does with it. A
          // frame uWS buffers is still bytes this gateway owes the wire; a frame it DROPS is not,
          // but a dropped frame is already a §3 incident that ends in a resync, and counting it is
          // the smaller error than a meter that quietly disagrees with the socket under pressure.
          M.countWireBytes(d.tenant?.db ?? null, text);
          return ws.send(text, false) !== UWS_DROPPED;
        },
        bufferedAmount: () => (d.closed ? 0 : ws.getBufferedAmount()),
        closed: () => d.closed,
      };
      Object.assign(d, {
        session: newSession(),
        openedAt: performance.now(),
        userId: null,
        role: null,
        ns: null,
        authPending: false,
        closed: false,
        sink: null,
        tenant: null,
        rate: new RateLimiter(limits.WRITE_RATE_PER_SEC, limits.WRITE_RATE_BURST),
        transport,
      });
      // NOT added to a tenant's `live` set yet — this connection has no database until hello says
      // which one. It is counted from the moment it picks one, which is also the moment it can
      // cost that database anything.
      pending.add(ws);
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
            if (d.closed) return void (d.authPending = false);
            if (!res.ok) {
              d.authPending = false;
              sendErr(ws, 'AUTH', res.msg);
              return void ws.end(CLOSE.AUTH, 'AUTH');
            }
            /**
             * §5.22 Gate D — THE hello question, and it is why `helloAck` is where the tenant is
             * chosen rather than at the first `listen`.
             *
             * `helloAck` carries ONE rev and ONE epoch (§2), and both are facts about one
             * database's oplog: each tenant has its own `rev_counter` row and its own generation.
             * A connection cannot be handed a rev before its database is known, and it cannot
             * change database afterwards without lying about the epoch it was given — which is
             * exactly what §2's generation check exists to catch. So the token's `ns` selects the
             * tenant HERE, once, and the connection belongs to it for its whole life.
             *
             * A token with no `ns` lands on the default tenant — today's single-database
             * deployment, and the console, which has no one database (§5.9).
             */
            /**
             * SINGLE-TENANT IGNORES `ns` ENTIRELY, and that is the compatibility rule rather than a
             * shortcut: without a `tenantStorage` factory this gateway has exactly one store, so a
             * token naming a database it has never heard of must land on it like every other token
             * — which is what Phase 1 already promised, since `outsideOwnDatabase` confines that
             * token to its own subtree wherever it lands. Routing on `ns` here without a factory
             * would try to build a store that cannot be built and fail the connection instead.
             */
            void tenantFor(opts.tenantStorage ? (res.ns ?? defaultDb) : defaultDb)
              .then(async (tenant) => {
                if (d.closed) return;
                const [rev, epoch] = await Promise.all([tenant.head(), tenant.storage.epoch()]);
                if (d.closed) return;
                /**
                 * `userId` is what marks this connection as past hello, and it is set HERE — with
                 * the tenant and the sink, in one synchronous step — rather than when auth
                 * resolved. Set earlier it opens a window where the connection reads as
                 * authenticated while its database, and therefore its sink, do not exist yet; a
                 * frame arriving in it would reach a null sink. `authPending` stays true across
                 * the whole chain for the same reason, so a second hello cannot re-enter it.
                 */
                d.tenant = tenant;
                d.userId = res.userId;
                d.role = res.role ?? null;
                d.ns = res.ns ?? null;
                pending.delete(ws);
                tenant.live.add(ws);
                d.sink = new ConnectionSink(d.transport, tenant.registry, tenant.storage, limits,
                  (ev, fields) => logEvent(ev, { connId: d.session, db: tenant.db, ...fields }));
                d.authPending = false;
                send(ws, { type: 'helloAck', rev, epoch, region, session: d.session });
              })
              // Its own catch, and its own `where`: a storage failure and an auth-backend failure
              // are different incidents and the log line has to say which. This chain is `void`-ed,
              // so its rejection never reaches the outer catch below.
              .catch((e: unknown) => {
                // An undeclared database is the client's fault and permanent; everything else here
                // (an RDS failover, a Redis blip inside `bus.start()`) is ours and transient.
                if (!(e instanceof UnknownDatabase)) return failHello(ws, 'head', e);
                d.authPending = false;
                logEvent('hello-failed', { connId: d.session, where: 'database', err: e.message });
                if (d.closed) return;
                sendErr(ws, 'AUTH', e.message);
                ws.end(CLOSE.AUTH, 'AUTH');
              });
          })
          .catch((e: unknown) => failHello(ws, 'auth', e));
        return;
      }

      /**
       * Past hello, and `userId` is only set together with both of these (see the hello chain), so
       * this is an invariant restated rather than a case that happens. It is a check and not a `!`
       * because the invariant lives 60 lines away: if someone ever sets `userId` earlier again,
       * this drops the frame instead of reaching into null.
       */
      const { sink, tenant } = d;
      if (!sink || !tenant) return;

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
              // authorization — deltas are never rules-checked per recipient. Which is exactly why
              // §5.20's invariant has to be asked HERE and BEFORE the configured rules: this is the
              // only moment a read outside the token's database can still be refused.
              if (outsideOwnDatabase({ userId: d.userId, ns: d.ns, role: d.role, path, requireNs })) {
                return sink.enqueue({
                  type: 'err',
                  subId,
                  code: 'RULES',
                  msg: "read outside this token's database",
                });
              }
              if (!rules({ userId: d.userId, role: d.role, op: 'read', path })) {
                return sink.enqueue({ type: 'err', subId, code: 'RULES', msg: 'read denied' });
              }
              logEvent('subscribe', { connId: d.session, subId, path });
              const { lastRev } = r.frame;
              // No catch: §5.11 made `listen` never reject. A storage failure is repaired inside
              // it — §3 resync plus a spaced, bounded retry — and abandoning writes a `listen-
              // abandoned` line. The old `.catch(console.error)` told the client nothing at all and
              // left the socket open, which on our own SDK is a permanent silent hang.
              void sink.listen(subId, path, lastRev);
              return;
            }
            case 'unlisten': {
              const { subId } = r.frame;
              // Read the path BEFORE the unlisten drops the sub — after it there is nothing to name.
              logEvent('unsubscribe', { connId: d.session, subId, path: sink.subs.get(subId)?.path ?? null });
              return sink.unlisten(subId);
            }
            case 'put':
            case 'merge':
            case 'cas': {
              const op = r.frame.type;
              if (!d.rate.take()) {
                M.acks.inc({ op, result: 'rate' });
                return sendErr(ws, 'RATE', 'write rate exceeded', { writeId: r.frame.writeId });
              }
              /**
               * §5.23: the per-DATABASE quota, after the per-connection one and never instead of
               * it. They answer different questions — one client flooding is not the same incident
               * as one database eating the shard, and a tenant can exhaust the shard from a
               * hundred well-behaved connections without any of them tripping their own limit.
               *
               * Same `RATE` code, different message: §4 makes `msg` free text, so a client's
               * §6 handling is unchanged while an operator reading a log can tell the two apart.
               */
              if (!tenant.quota.allows()) {
                M.acks.inc({ op, result: 'rate' });
                M.countQuotaRejected(tenant.db);
                return sendErr(ws, 'RATE', 'database quota exceeded', { writeId: r.frame.writeId });
              }
              M.writes.inc({ op });
              const startedAt = performance.now();
              return tenant.pipeline.submit({
                frame: r.frame,
                userId: d.userId,
                role: d.role,
                ns: d.ns,
                reply: (f) => {
                  M.observeAck(op, f, startedAt);
                  sink.enqueue(f);
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
        // Null when the socket died before hello chose a database — a real case, not a defensive
        // one: §2's own pre-hello close path reaches here.
        subs: d.sink?.subs.size ?? 0,
        db: d.tenant?.db ?? null,
      });
      d.sink?.close();
      d.tenant?.live.delete(ws);
      pending.delete(ws);
    },
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
    // The DEFAULT tenant's store — what 34 assertions in the suite read, and what a single-tenant
    // gateway has always returned.
    storage: defaultTenant.storage,
    // Idempotent on purpose: us_listen_socket_close on an already-freed socket segfaults the
    // process, and shutdown paths (test teardown, signal handlers) double-fire routinely.
    close() {
      if (stopped) return;
      stopped = true;
      // Every tenant, not just the default: under Gate D one gateway holds N of them, and a
      // half-closed gateway leaves buses publishing and metrics bound for databases nobody serves.
      unbindPending();
      for (const t of tenants.values()) t.close();
      // AND the ones still opening. A tenant whose `openTenant` is mid-flight is not in `tenants`
      // yet, so it would finish after this and leave a live bus and a metrics binding behind a
      // gateway that is already closed.
      for (const p of [...opening.values()]) void p.then((t) => t.close()).catch(() => undefined);
      for (const ws of [...pending]) ws.end(1001, 'server shutting down');
      uWS.us_listen_socket_close(token);
    },
  };
}
