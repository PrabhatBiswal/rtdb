import { createServer, type Server } from 'node:http';
import { collectDefaultMetrics, Counter, Gauge, Histogram, register } from 'prom-client';
import type { ServerFrame } from '../protocol/frames.ts';
import type { StorageAdapter } from '../storage/adapter.ts';

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
const LATENCY_BUCKETS = [
  0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 10, 30, 60, 120, 300,
];

export const connections = new Gauge({
  name: 'rtdb_connections',
  help: 'Open WebSocket connections on this gateway (§8 sizes each gateway for 100% of them).',
  collect() {
    this.set(sources?.connections() ?? 0);
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
  help: '1 while this gateway holds the shard lock (§8). Exactly one gateway may report 1.',
  collect() {
    this.set(sources?.leader() ?? 0);
  },
});

export const publishing = new Gauge({
  name: 'rtdb_publishing',
  help: '1 while this gateway is actually publishing to the stream. Leader without publishing = a promotion that could not read the stream tail.',
  collect() {
    this.set(sources?.publishing() ?? 0);
  },
});

export const consumerLag = new Gauge({
  name: 'rtdb_consumer_lag_revs',
  help: 'Oplog head minus the last rev this gateway delivered from the bus. The ElastiCache-hop signal: it grows when the bus stalls, whatever the socket-side latency says.',
  async collect() {
    this.set(sources ? await sources.lagRevs() : 0);
  },
});

// Process CPU, RSS, event-loop lag, handles. One line, and it is what tells a "the gateway is slow"
// page apart from a "the database is slow" one.
collectDefaultMetrics({ prefix: 'rtdb_proc_' });

// --------------------------------------------------------------------------- wiring

/** Live state the gauges read at scrape time. Bound by `startGateway`, which owns all of it. */
export interface MetricSources {
  connections: () => number;
  leader: () => 0 | 1;
  publishing: () => 0 | 1;
  lagRevs: () => Promise<number>;
}

let sources: MetricSources | null = null;

export const bindSources = (s: MetricSources): void => void (sources = s);

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
const seen = new Set<string>();

export function pathPrefix(path: string): string {
  if (path === '') return '_root';
  const first = path.split('/', 1)[0] as string;
  if (seen.has(first)) return first;
  if (seen.size >= MAX_PREFIXES) return '_other';
  seen.add(first);
  return first;
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
  let cached: { at: number; names: string[] } | null = null;
  let inflight: Promise<string[]> | null = null;

  const topNodes = (): Promise<string[]> => {
    if (cached && Date.now() - cached.at < ttlMs) return Promise.resolve(cached.names);
    if (!inflight) {
      inflight = opts.storage
        .topNodes()
        .then((names) => {
          cached = { at: Date.now(), names };
          return names;
        })
        .finally(() => {
          inflight = null;
        });
    }
    return inflight;
  };

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
      void topNodes()
        .then((names) => {
          res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
          res.end(JSON.stringify({ names }));
        })
        .catch(() => {
          // The sidebar is a convenience; a shard that cannot answer must not take the page down.
          res.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"unavailable"}');
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
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, () => resolve(server));
  });
}
