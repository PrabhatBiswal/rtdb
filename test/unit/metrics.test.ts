import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import type { ServerFrame } from '../../src/protocol/frames.ts';
import { startAdminServer } from '../../src/gateway/metrics.ts';
import type { StorageAdapter } from '../../src/storage/adapter.ts';

/** Only `head()` is ever called by the probe; the rest of the adapter would be dead weight here. */
const storageWhose = (head: () => Promise<number>): StorageAdapter =>
  ({ head }) as unknown as StorageAdapter;

/**
 * A storage that only answers topNodes, and counts how often it was actually asked.
 *
 * `delayMs` is load-bearing for the single-flight test, not decoration: an answer that resolves in a
 * microtask lets the FIRST request populate the cache before the others are even dispatched, so the
 * test passes whether or not single-flight exists. It did exactly that until the tooth was run —
 * a query the real thing takes milliseconds over a socket to answer has to take some time here too.
 */
const countingTopNodes = (
  names: string[],
  delayMs = 0,
): { storage: StorageAdapter; calls: () => number } => {
  let calls = 0;
  const storage = {
    head: async () => 0,
    topNodes: async () => {
      calls++;
      if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
      return names;
    },
  } as unknown as StorageAdapter;
  return { storage, calls: () => calls };
};

async function withServer<T>(
  storage: StorageAdapter,
  fn: (base: string) => Promise<T>,
  healthTimeoutMs = 2000,
  topNodesTtlMs?: number,
): Promise<T> {
  const server = await startAdminServer({
    port: 0,
    storage,
    healthTimeoutMs,
    ...(topNodesTtlMs !== undefined ? { topNodesTtlMs } : {}),
  });
  const { port } = server.address() as AddressInfo;
  try {
    return await fn(`http://127.0.0.1:${port}`);
  } finally {
    server.close();
  }
}

test('/healthz is 200 while storage answers', async () => {
  await withServer(storageWhose(async () => 42), async (base) => {
    const res = await fetch(`${base}/healthz`);
    assert.equal(res.status, 200);
    assert.equal(await res.text(), 'ok\n');
  });
});

// The tooth. A health check that cannot fail is not a health check (WORKLOAD §5), and these are the
// two ways a database goes away: it says no, or it says nothing at all.
test('/healthz is 503 when storage REJECTS', async () => {
  await withServer(
    storageWhose(() => Promise.reject(new Error('connection terminated unexpectedly'))),
    async (base) => assert.equal((await fetch(`${base}/healthz`)).status, 503),
  );
});

test('/healthz is 503 when storage HANGS, and answers within its own timeout', async () => {
  await withServer(
    storageWhose(() => new Promise<number>(() => undefined)), // never settles, like a wedged pool
    async (base) => {
      const started = Date.now();
      const res = await fetch(`${base}/healthz`);
      assert.equal(res.status, 503);
      assert.ok(Date.now() - started < 1000, 'the probe must fail fast, not hang with the database');
    },
    150,
  );
});

test('/metrics serves the exposition format with the §2 series', async () => {
  await withServer(storageWhose(async () => 0), async (base) => {
    const res = await fetch(`${base}/metrics`);
    assert.equal(res.status, 200);
    assert.match(res.headers.get('content-type') ?? '', /text\/plain/);
    const body = await res.text();
    for (const series of [
      'rtdb_connections',
      'rtdb_writes_total',
      'rtdb_acks_total',
      'rtdb_ack_seconds',
      'rtdb_deltas_out_total',
      'rtdb_fanout_seconds',
      'rtdb_listens_total',
      'rtdb_bytes_out_total',
      'rtdb_leader',
      'rtdb_publishing',
      'rtdb_consumer_lag_revs',
    ]) {
      assert.match(body, new RegExp(`^# HELP ${series} `, 'm'), `missing series ${series}`);
    }
  });
});

test('anything else is a 404 — this port serves two endpoints, not a filesystem', async () => {
  await withServer(storageWhose(async () => 0), async (base) => {
    assert.equal((await fetch(`${base}/`)).status, 404);
    assert.equal((await fetch(`${base}/../etc/passwd`)).status, 404);
  });
});

test('the bytes-out prefix label is bounded, whatever paths clients invent', async () => {
  const { pathPrefix } = await import('../../src/gateway/metrics.ts');
  // The label comes from client-supplied paths: an app writing under generated top-level keys must
  // not be able to mint a series per key and take the scrape down with it.
  assert.equal(pathPrefix('MPK_1010/1474396/score'), 'MPK_1010');
  assert.equal(pathPrefix(''), '_root');
  const labels = new Set<string>();
  for (let i = 0; i < 500; i++) labels.add(pathPrefix(`gen_${i}/x`));
  assert.ok(labels.size <= 65, `unbounded cardinality: ${labels.size} labels`);
  assert.ok(labels.has('_other'), 'past the cap everything must collapse into _other');
});

test('a batch assembled from per-frame encodings is byte-identical to encoding the batch', () => {
  // `ConnectionSink.#write` now encodes each frame separately so the egress counter can attribute
  // bytes per path prefix, and assembles the §3 batch envelope by hand. If that is ever not
  // byte-identical to `JSON.stringify({type:'batch',frames})`, the wire changed and this is the
  // only place that would say so.
  const frames: ServerFrame[] = [
    { type: 'delta', rev: 1, path: 'MPK_1010/1474396/score', op: 'put', value: 50 },
    { type: 'delta', rev: 2, path: 'MPK_1010/1474396', op: 'merge', value: { tag: null, 'stats/wins': 3 } },
    { type: 'snapshot', subId: 7, path: 'नाम/ünïcode', value: { name: 'Ravi ✓' }, rev: 3 },
    { type: 'ack', writeId: '0d0e1f20-0000-4000-8000-000000000000', rev: 4 },
    { type: 'err', subId: 7, code: 'TOOBIG', msg: 'snapshot exceeds 4194304 bytes' },
  ];
  const parts = frames.map((f) => JSON.stringify(f));
  assert.equal(
    `{"type":"batch","frames":[${parts.join(',')}]}`,
    JSON.stringify({ type: 'batch', frames }),
  );
  // ...and the per-frame byte counts must add up to the payload, envelope aside.
  const envelope = Buffer.byteLength('{"type":"batch","frames":[]}') + (frames.length - 1);
  assert.equal(
    parts.reduce((n, p) => n + Buffer.byteLength(p, 'utf8'), 0) + envelope,
    Buffer.byteLength(JSON.stringify({ type: 'batch', frames }), 'utf8'),
  );
});

test('a 90-second ack is measured, not clipped to the top bucket (load test 2026-08-29)', async () => {
  // The top bucket used to be 10s, so the load test's 89.6s ack p50 read as exactly 10.000 —
  // indistinguishable from a genuine ten seconds. The histogram did not report a stall; it hid one.
  const { ackSeconds, fanoutSeconds, observeAck } = await import('../../src/gateway/metrics.ts');
  ackSeconds.reset();
  fanoutSeconds.reset();
  observeAck('put', { type: 'ack', writeId: 'w', rev: 1 }, performance.now() - 90_000);
  fanoutSeconds.observe(90);

  for (const [name, h] of [['rtdb_ack_seconds', ackSeconds], ['rtdb_fanout_seconds', fanoutSeconds]] as const) {
    const counted = (await h.get()).values
      .filter((v) => v.metricName?.endsWith('_bucket') === true && v.value > 0)
      // `le` is prom-client's own bucket label; the typed labelNames never mention it.
      .map((v) => (v.labels as Record<string, string | number>)['le']);
    const finite = counted.filter((le): le is number => typeof le === 'number');
    assert.ok(finite.length > 0, `${name} saturated: a 90s observation reached nothing but +Inf`);
    assert.ok(Math.min(...finite) > 10, `${name} still tops out at 10s — the blindness is back`);
  }
});

test('the lag panels plot max_over_time, and the datasource floors $__interval to the scrape', async () => {
  // The 63,173-rev peak of 2026-08-29 was invisible because Grafana plots one SAMPLE per step, and
  // the sample it picked was 0. The repair has two halves that live in two files and are useless
  // apart: `max_over_time` in the panel, and a `timeInterval` floor on the datasource — without the
  // floor a narrow time range asks for a step shorter than the 15s scrape and the panel renders
  // gaps instead of peaks. This is the only place that says they must travel together.
  const read = (rel: string): Promise<string> => readFile(new URL(rel, import.meta.url), 'utf8');
  const dashboard = JSON.parse(await read('../../deploy/grafana/dashboards/rtdb.json')) as {
    panels: { title: string; targets: { expr: string }[] }[];
  };
  const lagPanels = dashboard.panels.filter((p) => p.targets.some((t) => /_lag_/.test(t.expr)));
  assert.ok(lagPanels.length >= 2, `expected the lag panels, found ${lagPanels.length}`);
  for (const panel of lagPanels) {
    for (const target of panel.targets) {
      assert.match(target.expr, /max_over_time\(/, `"${panel.title}" plots a raw sample of a gauge that spikes`);
    }
  }
  assert.match(
    await read('../../deploy/grafana/provisioning/datasources/prometheus.yml'),
    /timeInterval:\s*15s/,
    'the datasource must floor $__interval at the scrape interval, or max_over_time leaves gaps',
  );
});

test('/topnodes lists the shard\'s namespaces', async () => {
  const { storage } = countingTopNodes(['demo', 'userstatus']);
  await withServer(storage, async (base) => {
    const res = await fetch(`${base}/topnodes`);
    assert.equal(res.status, 200);
    assert.deepEqual(await res.json(), { names: ['demo', 'userstatus'] });
  });
});

test('/topnodes is CACHED — a leaning operator cannot turn a sidebar into load', async () => {
  const { storage, calls } = countingTopNodes(['demo']);
  await withServer(storage, async (base) => {
    for (let i = 0; i < 5; i++) assert.equal((await fetch(`${base}/topnodes`)).status, 200);
    assert.equal(calls(), 1, 'five requests inside the window must be ONE query');
  });
});

test('/topnodes single-flights a cold cache — N concurrent requests are still one query', async () => {
  // The moment the cache was meant to cover: nothing cached yet and several requests at once.
  const { storage, calls } = countingTopNodes(['demo'], 60);
  await withServer(storage, async (base) => {
    const all = await Promise.all([1, 2, 3, 4, 5].map(() => fetch(`${base}/topnodes`)));
    assert.deepEqual(all.map((r) => r.status), [200, 200, 200, 200, 200]);
    assert.equal(calls(), 1, 'a cold cache and five callers is still one query');
  });
});

test('/topnodes re-reads once the window expires', async () => {
  const { storage, calls } = countingTopNodes(['demo']);
  await withServer(
    storage,
    async (base) => {
      await fetch(`${base}/topnodes`);
      await new Promise((r) => setTimeout(r, 60));
      await fetch(`${base}/topnodes`);
      assert.equal(calls(), 2, 'a cache that never expires is a stale sidebar');
    },
    2000,
    30,
  );
});

test('/topnodes answers 503 rather than taking the page down when storage cannot', async () => {
  const storage = {
    head: async () => 0,
    topNodes: () => Promise.reject(new Error('shard unreachable')),
  } as unknown as StorageAdapter;
  await withServer(storage, async (base) => assert.equal((await fetch(`${base}/topnodes`)).status, 503));
});
