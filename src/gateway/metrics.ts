import { createServer, type Server } from 'node:http';
import { collectDefaultMetrics, Counter, Gauge, Histogram, register } from 'prom-client';
import type { ServerFrame } from '../protocol/frames.ts';
import type { StorageAdapter } from '../storage/adapter.ts';
import { validateDatabaseName } from '../protocol/path.ts';
import { DEFAULT_LIMITS } from '../protocol/limits.ts';

/**
 * WORKLOAD §2's `/metrics` + `/healthz` — the one change this package makes to `src/`.
 *
 * Deliberately a module-level singleton over prom-client's default registry rather than an injected
 * object: a gateway process runs exactly one gateway, and threading a metrics handle through
 * `SubscriptionRegistry` -> `ConnectionSink` -> `Transport` would be more plumbing than the numbers
 * are worth. Tests that start several gateways in ONE process therefore share these counters — they
 * never scrape, so nothing observes the mixing.
 */

// Ack latency is a ms-scale number (WP5: p50 ~2ms, p99 ~40ms under load), so the buckets are dense
// at the bottom and reach far enough up to show a stall rather than clipping it into +Inf.
//
// The top used to be 10s, and the 2026-08-29 load test paid for it: client-side ack p50 was 89.6s
// while `rtdb_ack_seconds` reported p50 AND p99 as exactly 10.000 — every observation past the top
// bucket collapses onto it, so the histogram reads "saturated" identically to "10 seconds", and we
// were blind server-side to the most important number of the test. The tail above is deliberately
// coarse: nobody tunes a 90-second ack, they only need to SEE it.
/**
 * The top bucket is a CEILING on what these histograms can ever report, and it has now been hit
 * twice. On 2026-08-29 the top was 30 s and Gate D's ack stall read `p50 = p99 = 30 s`; the top was
 * raised to 300 s, and §5.30's C1/C2' read `p50 = p99 = 300 s` for both `put` and `cas` while the
 * CLIENTS measured 385-750 s. A quantile pinned to the last bucket is not a measurement, it is the
 * histogram saying "at least this" — and both times it hid the size of the very thing under test.
 *
 * 900 and 1800 are added rather than the scale rewritten: existing recording rules and dashboards
 * keep every boundary they already query, and 30 minutes is past any latency this system could
 * report and still be called working.
 *
 * §5.35 adds 600 and 1200 for RESOLUTION, not reach — the ceiling has been 1800 in THIS TREE since
 * 1731986. Production is a different question: `5fa0069` (2026-09-09) predates that commit and
 * still tops at 300, so until it is rolled, `histogram_quantile` over a pegged ack returns exactly
 * 300 — the highest FINITE bound, which is what a quantile landing in +Inf reports. That is how
 * §5.30's C2' read "p50 = p99 = 300 s"; read it as right-censored, true value >= 300 s. The hole was between 300 and 900: §5.30 measured CLIENT-side
 * acks at 385-750 s, and every one of those reports as `le=900`, a quantile up to 1.8x the truth.
 * Two boundaries, so +2 series per label set on both histograms.
 */
const LATENCY_BUCKETS = [
  0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 10, 30, 60, 120, 300, 600, 900,
  1200, 1800,
];

export const connections = new Gauge({
  name: 'rtdb_connections',
  help: 'Open WebSocket connections on this gateway, by database (§8 sizes each gateway for 100% of them). Sum the label for the gateway total.',
  labelNames: ['db'],
  collect() {
    eachLabel(this, (db, n) => this.set({ db }, n), (s) => s.connections());
  },
});

/**
 * §5.22 Gate D (R7): connections that have opened but not yet said hello, so they belong to no
 * database yet and appear in NO `rtdb_connections` label.
 *
 * Its own series rather than a `db` label, because the whole point is that these have no database.
 * Without it a hello flood is invisible on every dashboard — sockets accumulating, `rtdb_connections`
 * flat — and §5.10's crash containment was built on exactly that path.
 */
export const connectionsPending = new Gauge({
  name: 'rtdb_connections_pending',
  help: 'Open sockets that have not completed hello, so they are on no database yet.',
  collect() {
    this.set(pendingSources.size === 0 ? 0 : [...pendingSources].reduce((n, f) => n + f(), 0));
  },
});

/**
 * §5.22 Gate D (R6): waiters on the shared pool, and it is the instrument §5.21's own sentence
 * asked for — "a pool wait looks like a slow commit, not a refusal". `P >= N + reads + 1` is a
 * claim about queueing that nothing could see; `pg.Pool` exposes `waitingCount`, so one line makes
 * the coupling Gate B removed from the lock, and left in the pool, visible.
 */
export const pgPoolWaiting = new Gauge({
  name: 'rtdb_pg_pool_waiting',
  help: 'Requests queued for a Postgres pool connection right now. Sustained non-zero means the pool floor is below the tenant count.',
  collect() {
    this.set(poolWaiting?.() ?? 0);
  },
});

export const writes = new Counter({
  name: 'rtdb_writes_total',
  help: 'Write frames accepted from clients (§4), by op.',
  labelNames: ['op'],
});

export const acks = new Counter({
  name: 'rtdb_acks_total',
  help: 'Settled writes by outcome. A casFail is a normal outcome (§4), not an error.',
  labelNames: ['op', 'result'],
});

/**
 * §5.23 Gate A: the QUOTA UNIT, metered before anything is enforced.
 *
 * One increment per lock acquisition — `commitGroup` or a solo `commitCas` — which is the resource
 * the shard actually runs out of. Writes are not: `scripts/quota-unit.ts` measures `writes/acq`
 * moving from 1.00 (a trickle, one write per batch window) to 500.00 (one burst), so a
 * writes/second number prices two identical bills 500× apart. Measured on `20a6ca2` with two
 * tenants: the trickling one spent 95 acq/s of a ~129 acq/s shard from 100 writes/s, while the
 * bursty one spent 19 acq/s from 2000 writes/s — 20× the writes for a fifth of the lock.
 *
 * Per DATABASE, because Phase 3 is what made that attributable: before per-database pipelines one
 * tenant's batch WAS the other's, so there was no honest way to say whose acquisition it had been.
 *
 * This is also Phase 4's Load numerator (ρ = acq/s × S) and the first half of v19's asked-for
 * instrument. It counts ATTEMPTS, including a retried transaction, because each attempt takes the
 * lock — the same thing `quota-unit.ts`'s stub counts, which is what makes that the tooth.
 */
export const lockAcquisitions = new Counter({
  name: 'rtdb_lock_acquisitions_total',
  help:
    'Lock acquisitions by database (§5.23): one per group commit, one per solo CAS. The quota ' +
    'unit, and the shard resource — NOT writes, which vary 500x per acquisition by shape alone.',
  labelNames: ['db'],
});

/**
 * Registered here and left at zero until §5.23 Gate B, deliberately.
 *
 * A counter that appears only once it is non-zero cannot be alerted on, cannot be graphed before
 * the incident, and reads as "no data" exactly when someone needs to know it is zero. Gate A is the
 * meter; this is the half of the meter that says nothing was refused.
 */
export const quotaRejected = new Counter({
  name: 'rtdb_quota_rejected_total',
  help:
    'Writes refused by the per-database acquisition quota (§5.23). Zero until Gate B turns ' +
    'enforcement on; non-zero means a database is being held to its share of the shard.',
  labelNames: ['db'],
});

/**
 * §5.24 Gate A: Downloads, metered where the bytes actually leave — one wrapper over `ws.send`.
 *
 * `rtdb_bytes_out_total{prefix}` stays exactly where it is and keeps answering a different
 * question (WHICH PATH the payload was for). It cannot answer this one, and measurement says so
 * rather than argument: it counts each INNER frame's JSON, so a batched send never counts its
 * `{"type":"batch","frames":[…]}` envelope (`subscriptions.ts`), and — the larger hole — it has ONE
 * caller, so every `ack`, `casFail`, `err`, `helloAck`, `pong` and `resync` was never counted at
 * all. For a write-heavy app the acks alone are bigger than the envelope.
 *
 * What this counts is BYTES HANDED TO THE SOCKET: the frame text plus RFC 6455's server-to-client
 * header (unmasked: 2 bytes, 4 once the payload reaches 126, 10 past 65535 — confirmed against the
 * wire in §5.22's measurement, not read off the spec and hoped for). It is exact for "what the
 * gateway sent the NLB", and it is deliberately NOT the customer's egress bill: TLS and TCP happen
 * on the NLB (`main.tf` TLS listener over a TCP target group) and are not observable from in here,
 * ever, in this shape. Measured, that gap is 1.00x for snapshot-shaped traffic and up to 1.24x for
 * a drip of small deltas, before packet headers. The panel says so in words; no factor is applied
 * to a number in code.
 */
export const wireBytesOut = new Counter({
  name: 'rtdb_wire_bytes_out_total',
  help:
    'Bytes handed to client sockets by database: frame text + WebSocket header (§5.24). BEFORE ' +
    'TLS and TCP, which happen on the NLB — the egress bill is 1.0-1.3x this by traffic shape.',
  labelNames: ['db'],
});

/**
 * RFC 6455 §5.2, server to client, so never masked: 2-byte header, plus 2 more once the payload
 * needs a 16-bit length, plus 8 instead past 65535. Payload length is in BYTES, not characters —
 * the caller passes what it measured.
 */
export const wsFrameBytes = (payloadBytes: number): number =>
  payloadBytes + (payloadBytes < 126 ? 2 : payloadBytes <= 65_535 ? 4 : 10);

/** One frame leaving a socket. `db` is null before hello, when the connection is on no database. */
export function countWireBytes(db: string | null, text: string): void {
  wireBytesOut.inc({ db: db === null ? '_none' : dbLabel(db) }, wsFrameBytes(Buffer.byteLength(text, 'utf8')));
}

/**
 * §5.24 Gate A: Storage, per database, on the §5.19 line an owner is billed for.
 *
 * A GAUGE and not a counter, because it is a level rather than a flow, and it is deliberately
 * `nodes` only: the oplog is retention (§9, two hours or 500k revs) and shrinks on its own, so
 * billing a client for it would charge them for our own durability window. The RUNBOOK says so
 * where an operator comparing this against `\dt+` will need it.
 */
let storageCollector: (() => Promise<void>) | null = null;
export const storageBytes = new Gauge({
  name: 'rtdb_storage_bytes',
  help:
    'Live data per database, the `nodes` relation only (§5.24). Excludes the oplog, which is ' +
    'retention rather than the client\'s data. A declared database with no data reads 0, not absent.',
  labelNames: ['db'],
  // Filled on SCRAPE, through `bindStorageBytes` below, so a gateway nobody scrapes runs no query.
  async collect(): Promise<void> {
    await storageCollector?.();
  },
});

/**
 * §5.24 Gate C: the rate this database is actually held to, and whether anybody chose it.
 *
 * A gauge for the same reason `rtdb_lock_hold_ms` is one: the usage panel's numbers should be
 * readable from the scrape rather than from a config file nobody has. `source` distinguishes the
 * two statements the panel has to make — "64 (default)" follows the shard when the shard's number
 * changes, "120 (set)" is a decision somebody made about this database and will not move.
 *
 * Per gateway, like the bucket itself: with `RTDB_GATEWAY_COUNT` set this reports the SHARD's rate,
 * not this gateway's share of it, because the shard's rate is what a client was promised.
 */
export const quotaAcqPerSec = new Gauge({
  name: 'rtdb_quota_acq_per_sec',
  help:
    'The lock-acquisition rate this database is held to (§5.24). source=default follows the ' +
    'shard\'s QUOTA_ACQ_PER_SEC; source=override is a per-database decision in the registry.',
  labelNames: ['db', 'source'],
});

/** One tenant's quota, as it was decided at open. Cleared with the tenant's other bindings. */
export function bindQuota(db: string, perSec: number, overridden: boolean): () => void {
  const labels = { db: dbLabel(db), source: overridden ? 'override' : 'default' };
  quotaAcqPerSec.set(labels, perSec);
  return () => quotaAcqPerSec.remove(labels);
}

/**
 * §5.24: the lock hold time `Load` is denominated in — rho = acquisitions/s x S.
 *
 * A gauge carrying a CONSTANT, and that is the point: 7.74 ms is P2's measurement of
 * `rev_counter … FOR UPDATE`, and putting it in the metric stream rather than in a dashboard
 * expression means the panel's arithmetic is auditable from the same scrape as its inputs, and
 * that changing it is a deploy rather than a dashboard edit nobody reviews. `RTDB_LOCK_HOLD_MS`
 * overrides it; §5.24 Gate D decides whether a live `pg_stat_statements` mean replaces it.
 */
export const lockHoldMs = new Gauge({
  name: 'rtdb_lock_hold_ms',
  help:
    'Measured lock hold per acquisition, ms — the S in Load = acquisitions/s x S (§5.24). A ' +
    'constant from P2 unless RTDB_LOCK_HOLD_MS says otherwise, exported so the panel can be read.',
});
lockHoldMs.set(Number(process.env['RTDB_LOCK_HOLD_MS'] ?? 0) > 0 ? Number(process.env['RTDB_LOCK_HOLD_MS']) : 7.74);

export const ackSeconds = new Histogram({
  name: 'rtdb_ack_seconds',
  help: 'Client write frame -> its ack/casFail/err leaving the pipeline, seconds.',
  labelNames: ['op'],
  buckets: LATENCY_BUCKETS,
});

export const deltasOut = new Counter({
  name: 'rtdb_deltas_out_total',
  help: 'Delta frames written to client sockets (once per connection, §3).',
});

export const fanoutSeconds = new Histogram({
  name: 'rtdb_fanout_seconds',
  help:
    'GATEWAY-SIDE only: time a server->client frame waited in the sink batch window before it was ' +
    'written (§3 micro-batching). It does NOT include the commit -> bus -> this gateway hop; ' +
    'rtdb_consumer_lag_revs is the signal for that half.',
  buckets: LATENCY_BUCKETS,
});

export const listens = new Counter({
  name: 'rtdb_listens_total',
  help: 'Completed listen setups by how §3 served them: oplog catch-up, fresh snapshot, or TOOBIG.',
  labelNames: ['result'],
});

export const resyncs = new Counter({
  name: 'rtdb_resyncs_total',
  help:
    'resync FRAMES sent (§3). A repair sends one under pressure and re-announces it once the socket ' +
    'drains, so this counts frames, not subscriptions — non-zero at all means clients cannot keep up.',
});

/**
 * The roadmap's per-path bandwidth. Labelled by TOP-LEVEL path prefix only — the ~14 dbName
 * namespaces — because egress is the app-dependent half of the cost model and a label per full path
 * would be unbounded cardinality. Counts frame payload bytes; a batch envelope's own ~27 bytes are
 * not attributed to anyone.
 */
export const bytesOut = new Counter({
  name: 'rtdb_bytes_out_total',
  help: 'Bytes of server->client frames, by top-level path prefix (bounded cardinality).',
  labelNames: ['prefix'],
});

export const leader = new Gauge({
  name: 'rtdb_leader',
  help: '1 while this gateway holds the lock for that database (§8). Per database, exactly one gateway may report 1.',
  labelNames: ['db'],
  collect() {
    eachLabel(this, (db, n) => this.set({ db }, n), (s) => s.leader());
  },
});

export const publishing = new Gauge({
  name: 'rtdb_publishing',
  help: "1 while this gateway is actually publishing to that database's stream. Leader without publishing = a promotion that could not read the stream tail.",
  labelNames: ['db'],
  collect() {
    eachLabel(this, (db, n) => this.set({ db }, n), (s) => s.publishing());
  },
});

export const consumerLag = new Gauge({
  name: 'rtdb_consumer_lag_revs',
  help: "Oplog head minus the last rev this gateway delivered from that database's bus. The ElastiCache-hop signal: it grows when the bus stalls, whatever the socket-side latency says.",
  labelNames: ['db'],
  async collect() {
    this.reset();
    // Sequential, not `Promise.all`: each `lagRevs()` is a `storage.head()` query, and a scrape
    // that fires N of them at once against a shared pool competes with the write path for the very
    // connections §5.22 Gate B was counting. A scrape is not worth a burst.
    //
    // Totalled by label like the others — the only one that cannot use `eachLabel`, because reading
    // it is asynchronous. Same rule, spelled out: tenants sharing a label are added, never
    // overwritten.
    const totals = new Map<string, number>();
    for (const [name, set] of sources) {
      const db = dbLabel(name);
      for (const s of set) totals.set(db, (totals.get(db) ?? 0) + (await s.lagRevs()));
    }
    for (const [db, n] of totals) this.set({ db }, n);
  },
});

export const applyGroups = new Counter({
  name: 'rtdb_storage_apply_groups_total',
  help: 'Commits whose nodes work had more than one target — the ones that COULD be applied as one batch.',
  labelNames: ['db'],
  collect() {
    eachLabel(this, (db, n) => this.inc({ db }, n), (s) => s.applyStats().groups);
  },
});

export const orderedFallbacks = new Counter({
  name: 'rtdb_storage_ordered_fallbacks_total',
  help: 'Commits that failed the prefix-disjointness guard and applied in order at the old per-write cost. Silent otherwise, and the cost is paid inside the rev_counter lock.',
  labelNames: ['db'],
  collect() {
    eachLabel(this, (db, n) => this.inc({ db }, n), (s) => s.applyStats().orderedFallbacks);
  },
});

// Process CPU, RSS, event-loop lag, handles. One line, and it is what tells a "the gateway is slow"
// page apart from a "the database is slow" one.
collectDefaultMetrics({ prefix: 'rtdb_proc_' });

// --------------------------------------------------------------------------- wiring

/** Live state the gauges read at scrape time. Bound by `startGateway`, which owns all of it. */
/**
 * What `/topnodes` answers: every top-level name (declared UNION derived, §5.6's sidebar) and the
 * DECLARED ones alone (§5.19's registry). Two lists because they are two different claims — see
 * `shard()` in `startAdminServer`.
 */
export interface Shard {
  names: string[];
  declared: string[];
  /**
   * §5.24 Gate C: the DEFAULT tenant's name — the schema this gateway was configured with, which is
   * never in the registry because nobody declared it. Today it is production's entire dataset, and
   * without it the usage panel has no line for the one database that currently has all the data.
   */
  defaultDb: string;
}

export interface MetricSources {
  connections: () => number;
  leader: () => 0 | 1;
  publishing: () => 0 | 1;
  lagRevs: () => Promise<number>;
  applyStats: () => { groups: number; orderedFallbacks: number };
}

/**
 * §5.22 Gate E: a REGISTRY, where this used to be `let sources: MetricSources | null`.
 *
 * The singleton is the reason this gate comes BEFORE Gate D rather than after it. `bindSources` is
 * called once per `startGateway`; the moment a process holds N tenants, every binding but the LAST
 * is overwritten and every gauge reports that one tenant's numbers under the whole gateway's name.
 * Nothing errors, no series disappears, and the dashboards keep drawing — `rtdb_connections` would
 * simply be one tenant's connection count labelled as the gateway's. Phase 3 does not leave the new
 * panel empty; it makes the EXISTING panels quietly wrong, which is why the order puts E first.
 *
 * The map is keyed by the client-visible database name, which is the label these series now carry.
 */
/**
 * Keyed by the RAW database name, never by the bounded label — and that distinction is the whole
 * of §5.22 Gate E's second condition.
 *
 * Keying by `dbLabel(db)` put the singleton straight back past the cap: the 65th and 66th tenants
 * both key on `_other`, the second overwrites the first, and one tenant's numbers are reported for
 * two. Measured on 66 tenants with `connections = i + 1`: `rtdb_connections{db="_other"}` read 66
 * where the truth is 131, `rtdb_leader{db="_other"}` read 1 where two tenants were leading, and
 * unbinding tenant 65 ALONE erased tenant 66's series completely — a live tenant with no numbers at
 * all, because the map entry it shared had been deleted out from under it.
 *
 * Reachable by design rather than by accident: §5.19 FAISLA #5's own words are that past 64 a
 * client's databases "fall into `_other`" — the 65th is not refused, it is merged. Nothing caps the
 * registry at 64.
 *
 * So the MAP is structure and holds one entry per tenant the gateway actually loaded; the LABEL is
 * cardinality and is applied at collect time; and tenants that land on one label are ADDED rather
 * than allowed to overwrite each other. A `Set` per name because binding the same database twice in
 * one process is a thing tests do, and an unbind must remove one binding rather than the name.
 */
const sources = new Map<string, Set<MetricSources>>();

/**
 * The label value for a gateway that has not been told which database it serves — today's
 * single-tenant deployment, and every test that starts a gateway without naming one. It is a real
 * value rather than an omitted label on purpose: a series that sometimes has `db` and sometimes
 * does not is two series to every query that touches it.
 */
export const DEFAULT_DB_LABEL = '_default';

/** Bind one database's live state. Returns the unbind, which removes THIS binding and no other. */
export function bindSources(db: string, s: MetricSources): () => void {
  let set = sources.get(db);
  if (!set) {
    set = new Set();
    sources.set(db, set);
  }
  const mine = set;
  mine.add(s);
  return () => {
    mine.delete(s);
    // Only if this is still the live entry AND nothing else is in it: a name that was unbound and
    // bound again holds a different set, and deleting on the stale one would drop the new binding.
    if (mine.size === 0 && sources.get(db) === mine) sources.delete(db);
  };
}

/** Test-only: forget every binding, so one test's gateway cannot be scraped by the next. */
export const resetSources = (): void => {
  sources.clear();
  pendingSources.clear();
  poolWaiting = null;
};

/**
 * Pre-hello socket counts, one per gateway in the process. A Set rather than a single source for
 * the same reason `sources` is a map: tests run several gateways at once, and the last one to
 * start must not be the only one that reports.
 */
const pendingSources = new Set<() => number>();

/** Register this gateway's pre-hello count. Returns the unregister. */
export function bindPending(count: () => number): () => void {
  pendingSources.add(count);
  return () => void pendingSources.delete(count);
}

/**
 * The shared pool's waiter count. ONE per process — the pool is shared across tenants by design
 * (Gate B), so a per-tenant reading would be the same number N times.
 */
let poolWaiting: (() => number) | null = null;

/** Bind the shared pool's `waitingCount`. Returns the unbind. */
export function bindPoolWaiting(count: () => number): () => void {
  poolWaiting = count;
  return () => {
    if (poolWaiting === count) poolWaiting = null;
  };
}

/**
 * Read one number from every bound tenant and TOTAL it per label.
 *
 * Summing is what makes the cap safe rather than lossy: past 64 databases the label stops
 * distinguishing them, and the honest reading of a bucket holding two leaders is 2. Overwriting
 * would report one of them and call it the truth.
 */
function totalsByLabel(read: (s: MetricSources) => number): Map<string, number> {
  const out = new Map<string, number>();
  for (const [name, set] of sources) {
    const label = dbLabel(name);
    for (const s of set) out.set(label, (out.get(label) ?? 0) + read(s));
  }
  return out;
}

/**
 * Reset first, so a tenant that has gone away takes its label with it. Without the reset a gauge
 * keeps the last value it was ever set to for a label nobody is feeding any more — a dead tenant
 * that reports 1 connection forever.
 */
function eachLabel(
  g: { reset: () => void },
  apply: (db: string, total: number) => void,
  read: (s: MetricSources) => number,
): void {
  g.reset();
  for (const [db, total] of totalsByLabel(read)) apply(db, total);
}

/** Observe a settled write. `startedAt` is a `performance.now()` reading from frame arrival. */
export function observeAck(op: string, frame: ServerFrame, startedAt: number): void {
  const result = frame.type === 'ack' ? 'ack' : frame.type === 'casFail' ? 'casFail' : 'err';
  acks.inc({ op, result });
  ackSeconds.observe({ op }, (performance.now() - startedAt) / 1000);
}

/**
 * Cardinality guard, not decoration: the prefix comes from client-supplied paths, so an app writing
 * under generated top-level keys would mint a label per key and take the scrape down with it. Past
 * the cap everything lands in `_other` and the series count stops growing.
 */
const MAX_PREFIXES = 64;

/**
 * One cap, applied per DIMENSION. `_other` past it, and the series count stops growing.
 *
 * §5.19 FAISLA #5 made this number a PRODUCT limit rather than an ops detail: a client with more
 * than 64 databases sees its metrics fall into `_other`. Gate E's `db` label had to agree with that
 * number rather than cut across it, so it reuses the same cap — and its own set, because a database
 * and a path prefix are different dimensions and sharing one set would let a busy tree's prefixes
 * evict a paying tenant's name.
 */
const bounded = (dimension: Set<string>) => (value: string): string => {
  if (dimension.has(value)) return value;
  if (dimension.size >= MAX_PREFIXES) return '_other';
  dimension.add(value);
  return value;
};

const boundPrefix = bounded(new Set());
const boundDb = bounded(new Set());

export function pathPrefix(path: string): string {
  if (path === '') return '_root';
  return boundPrefix(path.split('/', 1)[0] as string);
}

/**
 * The `db` label value for one database name. Bounded like every other client-supplied label —
 * under Gate D this string comes from a token claim, so it is exactly as untrusted as a path.
 */
export const dbLabel = (db: string): string => (db === '' ? DEFAULT_DB_LABEL : boundDb(db));

/**
 * §5.23 Gate A. Bounded through `dbLabel` like every other client-supplied label — a database name
 * comes from a token claim, so it is exactly as untrusted as a path, and Gate E's cap and `_other`
 * are the route it takes. No new road to the label space.
 */
export function countAcquisition(db: string): void {
  lockAcquisitions.inc({ db: dbLabel(db) }, 1);
}

/** Its zero-valued twin, so the series exists before there is anything to report. */
export function registerQuotaRejected(db: string): void {
  quotaRejected.inc({ db: dbLabel(db) }, 0);
}

/** §5.23 Gate B: one write refused because its DATABASE is over its share of the shard. */
export function countQuotaRejected(db: string): void {
  quotaRejected.inc({ db: dbLabel(db) }, 1);
}

/**
 * §5.24 Gate A: keep `rtdb_storage_bytes` fed, from ONE call, on a cache.
 *
 * Bound to a `collect` callback rather than a timer, so it costs nothing when nobody scrapes — and
 * cached for 30s for the reason `/topnodes` is cached: a scrape is not a reason to run a catalogue
 * query, and two Prometheus servers are not a reason to run two. Single-flighted for the other
 * half of that, exactly as `shard()` above is.
 *
 * A DECLARED database with no data reads 0 rather than being absent (§5.19 (c)): the whole point of
 * the registry is that an owner can hand over an empty database, and a usage line that vanishes
 * when a database is empty says "no data" where the truth is "no bytes".
 */
export function bindStorageBytes(storage: StorageAdapter, ttlMs = 30_000): () => void {
  let cached: { at: number; sizes: Record<string, number> } | null = null;
  let inflight: Promise<Record<string, number>> | null = null;

  const sizes = (): Promise<Record<string, number>> => {
    if (cached && Date.now() - cached.at < ttlMs) return Promise.resolve(cached.sizes);
    if (!inflight) {
      inflight = Promise.all([storage.storageBytes(), storage.listDeclared()])
        .then(([bytes, declared]) => {
          const answer: Record<string, number> = {};
          for (const db of declared) answer[db] = 0;
          for (const [db, n] of Object.entries(bytes)) if (db !== '') answer[db] = n;
          cached = { at: Date.now(), sizes: answer };
          return answer;
        })
        .finally(() => {
          inflight = null;
        });
    }
    return inflight;
  };

  storageCollector = async (): Promise<void> => {
    /**
     * A shard that cannot answer must not take the whole scrape down with it — every other metric
     * on this endpoint is still true, and `/metrics` returning 500 because ONE gauge's source is
     * unhappy is how a storage blip becomes "the gateway is blind". `try`, not `.catch()`: an
     * adapter that throws SYNCHRONOUSLY never returns a promise to attach a handler to, which is
     * exactly what a hand-shaped stub does and what a half-implemented adapter would do.
     */
    try {
      const answer = await sizes();
      for (const [db, bytes] of Object.entries(answer)) storageBytes.set({ db: dbLabel(db) }, bytes);
    } catch {
      /* the last good values stay; a gap is better than a failed scrape */
    }
  };
  const mine = storageCollector;
  return () => {
    // Only if it is still ours: two gateways in one test process bind in turn, and the second
    // closing must not silence the first (the `sources` map above exists for the same reason).
    if (storageCollector === mine) storageCollector = null;
    storageBytes.reset();
  };
}

/** Frame payload bytes, attributed to the frame's own path (frames without one are `_none`). */
export function countBytesOut(frame: ServerFrame, bytes: number): void {
  const path = 'path' in frame && typeof frame.path === 'string' ? pathPrefix(frame.path) : '_none';
  bytesOut.inc({ prefix: path }, bytes);
}

// --------------------------------------------------------------------------- the endpoints

/** A storage probe that cannot hang: a wedged database must FAIL the check, not stall the prober. */
async function storageReachable(storage: StorageAdapter, timeoutMs: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      storage.head(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('storage probe timed out')), timeoutMs);
      }),
    ]);
    return true;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * `/metrics` and `/healthz` on their OWN port, not on the WebSocket listener:
 *
 * - the WS port is what the NLB's TLS listener publishes to the internet, and neither the metrics
 *   nor the health of this box belongs there;
 * - an NLB target group health-checks a port of its own choosing, so this costs nothing;
 * - and uWS routes `ws('/*')` as a GET route itself, so co-hosting them means fighting the router.
 *
 * `node:http` rather than a second uWS App: this serves a scrape every 15s and a probe every 10s.
 */
export function startAdminServer(opts: {
  port: number;
  storage: StorageAdapter;
  healthTimeoutMs?: number;
  /** §5.6's cache window. Tests shrink it; nothing in production sets it. */
  topNodesTtlMs?: number;
  /**
   * §5.24 Gate C: the DEFAULT tenant's name, so `/topnodes` can say which database this gateway
   * serves when a token names none. `main.ts` passes what it passed `startGateway`; unset is
   * `public`, which is the same default both of those already carry.
   */
  db?: string;
}): Promise<Server> {
  /**
   * §5.6: the namespaces this shard holds, cached, and single-flighted.
   *
   * Cached because an operator leaning on a refresh button must not be able to turn a sidebar into
   * load on the shard. Single-flighted for the same reason from the other direction: N concurrent
   * requests arriving on a cold cache would otherwise become N queries, which is exactly the moment
   * the cache was supposed to cover.
   */
  // 10s, not 30 (ruling 2026-08-30): the skip scan measured 1.5ms on 200k rows, so even a leaning
  // operator is noise against the shard, and a namespace that has just appeared showing up within
  // ten seconds is meaningfully better to work with than within thirty.
  const ttlMs = opts.topNodesTtlMs ?? 10_000;
  let cached: { at: number; shard: Shard } | null = null;
  let inflight: Promise<Shard> | null = null;

  /**
   * §5.22 Gate F-3: BOTH lists, from one round trip, because they answer different questions and
   * the seam between them was being answered by whichever one was nearest.
   *
   * `names` is declared UNION derived and is what a SIDEBAR wants — every top-level name that
   * exists, so nothing the operator holds is invisible. `declared` is the registry alone and is
   * what a MINT wants: after Gate D the gateway refuses an undeclared `ns` at hello, so a token
   * minted against `names` for one of the default tenant's raw namespaces is a credential nobody
   * can connect with. One is a view, the other is an authority; sending only the union made every
   * caller guess which it had.
   */
  const shard = (): Promise<Shard> => {
    if (cached && Date.now() - cached.at < ttlMs) return Promise.resolve(cached.shard);
    if (!inflight) {
      inflight = Promise.all([opts.storage.topNodes(), opts.storage.listDeclared()])
        .then(([names, declared]) => {
          const answer = { names, declared, defaultDb: opts.db ?? 'public' };
          cached = { at: Date.now(), shard: answer };
          return answer;
        })
        .finally(() => {
          inflight = null;
        });
    }
    return inflight;
  };

  /** Who declared it, for the registry's own record. The console proxy sends the console subject. */
  const by = (req: { headers: Record<string, string | string[] | undefined> }): string => {
    const h = req.headers['x-rtdb-subject'];
    const v = Array.isArray(h) ? h[0] : h;
    return typeof v === 'string' && v.length > 0 && v.length <= 128 ? v : 'unknown';
  };

  // §5.24 Gate A: the Storage line, fed from one cached catalogue query for every tenant at once.
  const unbindStorage = bindStorageBytes(opts.storage, ttlMs);

  const server = createServer((req, res) => {
    const url = req.url ?? '/';
    if (url.startsWith('/healthz')) {
      void storageReachable(opts.storage, opts.healthTimeoutMs ?? 2000).then((ok) => {
        res.writeHead(ok ? 200 : 503, { 'content-type': 'text/plain' });
        res.end(ok ? 'ok\n' : 'storage unreachable\n');
      });
      return;
    }
    if (url.startsWith('/topnodes')) {
      void shard()
        .then((answer) => {
          res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
          // `names` keeps its old shape and meaning, so every existing reader is unchanged;
          // `declared` is the addition, and it is what the two mints must check against.
          res.end(JSON.stringify(answer));
        })
        .catch(() => {
          // The sidebar is a convenience; a shard that cannot answer must not take the page down.
          res.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"unavailable"}');
        });
      return;
    }
    /**
     * §5.19: declare a database. The console's `+` button ends here.
     *
     * A MUTATION on the admin port, which until now served only reads, so the boundary is worth
     * naming: this port is VPC-internal and never public (§5.6), and the real gate is the console's
     * auth-server, which checks a write role before proxying. What an unauthenticated caller inside
     * the VPC could do here is insert a name into a registry — it grants no access to anything,
     * because access comes from the token's claim and not from this table. That is the smallest
     * mutation the port could have grown, and it is why it was acceptable to grow one.
     */
    if (req.method === 'POST' && url.startsWith('/databases')) {
      let body = '';
      req.setEncoding('utf8');
      req.on('data', (c: string) => {
        body += c;
        // A registry name cannot be long, and an unauthenticated port must not buffer for a caller
        // that never stops sending.
        if (body.length > 1024) req.destroy();
      });
      req.on('end', () => {
        let name: unknown;
        let quota: unknown;
        try {
          ({ name, quotaAcqPerSec: quota } = JSON.parse(body) as { name?: unknown; quotaAcqPerSec?: unknown });
        } catch {
          res.writeHead(400, { 'content-type': 'application/json' }).end('{"error":"bad json"}');
          return;
        }
        /**
         * §5.24 Gate C: optional. Absent or null means the shard's default, which is a different
         * statement from a number — it follows `QUOTA_ACQ_PER_SEC` when the shard changes.
         *
         * A positive integer or nothing: a zero would be a database that can never write, and this
         * port has no way to say "I meant that", so it is refused rather than honoured. Bounded
         * because a number a caller invents becomes a token bucket's rate, and a huge one is an
         * override that quietly means "no quota".
         */
        if (quota !== undefined && quota !== null) {
          if (typeof quota !== 'number' || !Number.isInteger(quota) || quota < 1 || quota > 100_000) {
            res.writeHead(400, { 'content-type': 'application/json' }).end('{"error":"bad quota"}');
            return;
          }
        }
        // A database name is a path of exactly one segment, so §1's own validator decides it — the
        // alternative is a second, quietly different idea of what a legal name is. Gate E folded
        // the reserved `_` prefix into that same rule rather than checking it here as well.
        const why = validateDatabaseName(name, DEFAULT_LIMITS);
        if (why !== null) {
          // The REASON, word for word from `path.ts` — a caller that is told only "bad name" cannot
          // fix the name, and `path.ts:110` already writes its refusals for a client to read.
          res.writeHead(400, { 'content-type': 'application/json' }).end(JSON.stringify({ error: why }));
          return;
        }
        void opts.storage
          .declareDatabase(name as string, by(req), (quota as number | null | undefined) ?? null)
          .then(() => {
            // The sidebar caches for 10s. Without this the operator who just created a database is
            // told for ten seconds that it does not exist, and the obvious conclusion is that the
            // button is broken.
            cached = null;
            res.writeHead(201, { 'content-type': 'application/json' }).end(JSON.stringify({ name }));
          })
          .catch(() => {
            res.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"unavailable"}');
          });
      });
      return;
    }
    if (url.startsWith('/metrics')) {
      void register
        .metrics()
        .then((body) => {
          res.writeHead(200, { 'content-type': register.contentType });
          res.end(body);
        })
        .catch(() => {
          res.writeHead(500).end();
        });
      return;
    }
    res.writeHead(404).end();
  });
  // The gauge's collector outlives the HTTP server otherwise, and would keep a closed gateway's
  // storage alive to answer a scrape nobody is making.
  server.once('close', unbindStorage);
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, () => resolve(server));
  });
}
