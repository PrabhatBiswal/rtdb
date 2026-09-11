import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import type { ServerFrame } from '../../src/protocol/frames.ts';
import { startAdminServer, type MetricSources } from '../../src/gateway/metrics.ts';
import type { StorageAdapter } from '../../src/storage/adapter.ts';

/** Only `head()` is ever called by the probe; the rest of the adapter would be dead weight here. */
const storageWhose = (head: () => Promise<number>): StorageAdapter =>
  ({ head, listDeclared: async () => [], storageBytes: async () => ({}) }) as unknown as StorageAdapter;

/**
 * A storage that only answers the two list queries, and counts how often it was actually asked.
 *
 * `delayMs` is load-bearing for the single-flight test, not decoration: an answer that resolves in a
 * microtask lets the FIRST request populate the cache before the others are even dispatched, so the
 * test passes whether or not single-flight exists. It did exactly that until the tooth was run —
 * a query the real thing takes milliseconds over a socket to answer has to take some time here too.
 */
const countingTopNodes = (
  names: string[],
  delayMs = 0,
  declared: string[] = [],
): { storage: StorageAdapter; calls: () => number } => {
  let calls = 0;
  const storage = {
    head: async () => 0,
    topNodes: async () => {
      calls++;
      if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
      return names;
    },
    // §5.22 Gate F-3: the route answers with both lists, from ONE cached round trip. Counting only
    // `topNodes` is still the right counter — the two go together or neither does.
    listDeclared: async () => declared,
    // §5.24: the scrape asks storage for its sizes now, and a stub that cannot answer would make
    // every /metrics assertion in this file a test of the collector's error handling instead.
    storageBytes: async () => ({}),
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

test('/topnodes lists the shard\'s namespaces AND which of them are declared', async () => {
  // §5.22 Gate F-3. Two lists because they are two claims: `names` is declared UNION derived and is
  // what a sidebar must show, `declared` is the registry alone and is what a MINT must check —
  // since Gate D the gateway refuses an undeclared `ns` at hello, so a token minted against the
  // union is one nobody can connect with. Sending only the union made every caller guess.
  const { storage } = countingTopNodes(['demo', 'userstatus'], 0, ['demo']);
  await withServer(storage, async (base) => {
    const res = await fetch(`${base}/topnodes`);
    assert.equal(res.status, 200);
    // §5.24 Gate C: `defaultDb` rides with them — the schema this gateway serves when a token names
    // none. It is never in the registry (nobody declared it) and today it holds production's whole
    // dataset, so a usage panel without it has no line for the only database that has data.
    assert.deepEqual(await res.json(), { names: ['demo', 'userstatus'], declared: ['demo'], defaultDb: 'public' });
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

test('a storage that cannot size itself does not take the whole scrape down', async () => {
  // §5.24: the Storage gauge asks the adapter on every scrape, so an unhappy shard now sits on the
  // path of every OTHER metric too. `/metrics` answering 500 because one gauge's source is down is
  // how a storage blip turns into "the gateway is blind" — and blind is when someone needs it most.
  const storage = {
    head: async () => 0,
    topNodes: async () => [],
    listDeclared: async () => [],
    // Throws SYNCHRONOUSLY, which is the shape a half-implemented adapter has: it never returns a
    // promise, so a `.catch()` would have had nothing to attach to.
    storageBytes: () => { throw new Error('shard unreachable'); },
  } as unknown as StorageAdapter;
  await withServer(storage, async (base) => {
    const res = await fetch(`${base}/metrics`);
    assert.equal(res.status, 200, 'every other series is still true');
    assert.match(await res.text(), /rtdb_connections_pending/, 'and still served');
  });
});

test('/topnodes answers 503 rather than taking the page down when storage cannot', async () => {
  const storage = {
    head: async () => 0,
    topNodes: () => Promise.reject(new Error('shard unreachable')),
    listDeclared: async () => [],
    storageBytes: async () => ({}),
  } as unknown as StorageAdapter;
  await withServer(storage, async (base) => assert.equal((await fetch(`${base}/topnodes`)).status, 503));
});

/**
 * §5.19: `POST /databases`. The registry is the whole reason "only the owner creates a database"
 * can be a rule, so what this pins is the refusals - a name that is not one segment must not reach
 * storage at all, because a name containing a slash would declare a registry entry that no path can
 * ever match, and one containing `.` or `#` names a path §1 refuses to store.
 */
const recordingRegistry = (): { storage: StorageAdapter; declared: string[][] } => {
  const declared: string[][] = [];
  const storage = {
    head: async () => 0,
    topNodes: async () => declared.map((d) => d[0] as string),
    listDeclared: async () => declared.map((d) => d[0] as string),
    storageBytes: async () => ({}),
    declareDatabase: async (name: string, by: string) => {
      declared.push([name, by]);
    },
  } as unknown as StorageAdapter;
  return { storage, declared };
};

const postDb = (base: string, body: unknown, subject?: string): Promise<Response> =>
  fetch(`${base}/databases`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(subject ? { 'x-rtdb-subject': subject } : {}) },
    body: JSON.stringify(body),
  });

test('POST /databases declares one, and records who asked', async () => {
  const { storage, declared } = recordingRegistry();
  await withServer(storage, async (base) => {
    const res = await postDb(base, { name: 'car_race' }, 'console-rw-prabhat');
    assert.equal(res.status, 201);
    assert.deepEqual(await res.json(), { name: 'car_race' });
    assert.deepEqual(declared, [['car_race', 'console-rw-prabhat']]);
  });
});

test('POST /databases refuses a name that is not ONE path segment', async () => {
  const bad: unknown[] = ['', 'a/b', 'a.b', 'a#b', 'a$b', 'a[b', '/', 42, null];
  for (const name of bad) {
    const { storage, declared } = recordingRegistry();
    await withServer(storage, async (base) => {
      const res = await postDb(base, { name });
      assert.equal(res.status, 400, `${JSON.stringify(name)} must be refused`);
      // The refusal has to happen BEFORE storage, not after - a rejected name that still reached
      // the registry would leave a row nothing can ever match.
      assert.deepEqual(declared, [], `${JSON.stringify(name)} must never reach storage`);
    });
  }
});


test('POST /databases refuses a reserved _ name, at the route as well as the registry', async () => {
  // The route answers 400 rather than letting the adapter throw a 500: a console clicking `+` on
  // `_default` should be told it is a bad name, not that the shard broke. Same rule object as the
  // registry's, so the two cannot drift.
  const declared: string[] = [];
  await withServer(
    { ...storageWhose(async () => 0), declareDatabase: async (n: string) => void declared.push(n) } as never,
    async (base) => {
      for (const bad of ['_default', '_other']) {
        const res = await fetch(`${base}/databases`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ name: bad }),
        });
        assert.equal(res.status, 400, bad);
        assert.match(String(((await res.json()) as { error?: unknown }).error), /reserved/, bad);
      }
      assert.deepEqual(declared, [], 'and nothing reached the registry');
    },
  );
});
test('POST /databases says WHICH rule the name broke, not just that it is bad (§5.27)', async () => {
  /**
   * 400 was never the missing half — the REASON was. Four rules, four different sentences, one
   * route: a console shows `error` in its alert verbatim, so a caller who cannot tell "too long"
   * from "wrong characters" has to guess at the fix.
   *
   * The sentences are `path.ts`'s own, matched loosely enough that rewording the human text does
   * not fail the test but swapping two reasons for each other does.
   */
  const cases: [unknown, RegExp][] = [
    ['LightingMacQueen', /lowercase letters/],
    ['a'.repeat(52), /at most 51/],
    ['a/b', /one path segment/],
    ['', /required/],
  ];
  for (const [name, reason] of cases) {
    const { storage, declared } = recordingRegistry();
    await withServer(storage, async (base) => {
      const res = await postDb(base, { name });
      assert.equal(res.status, 400, `${JSON.stringify(name)} must be refused`);
      assert.match(String(((await res.json()) as { error?: unknown }).error), reason, JSON.stringify(name));
      assert.deepEqual(declared, [], `${JSON.stringify(name)} must never reach storage`);
    });
  }
});

test('a declared database appears in the sidebar with no data under it', async () => {
  // The point of the registry, stated as a test: the sidebar must list it before anything is
  // written, because that is the state an owner hands to a team.
  const { storage } = recordingRegistry();
  await withServer(
    storage,
    async (base) => {
      assert.deepEqual(await (await fetch(`${base}/topnodes`)).json(), { names: [], declared: [], defaultDb: 'public' });
      assert.equal((await postDb(base, { name: 'car_race' })).status, 201);
      // And the 10s cache must not be what the operator sees next - creating one INVALIDATES it.
      // Both lists move together, which is the point of one cached round trip for the pair: a
      // database that showed in the sidebar but not in `declared` would be one the console offers
      // and neither mint will name.
      assert.deepEqual(await (await fetch(`${base}/topnodes`)).json(), {
        names: ['car_race'],
        declared: ['car_race'],
        defaultDb: 'public',
      });
    },
    2000,
    60_000,
  );
});

// --------------------------------------------------------------- §5.22 Gate E: the db label

/**
 * The gate's whole reason for coming BEFORE Gate D: `bindSources` was a module singleton, so a
 * process holding N tenants kept only the LAST binding and reported that one tenant's numbers under
 * the gateway's name. Nothing errors, no series vanishes, and every dashboard keeps drawing — which
 * is why a test is the only witness.
 *
 * These scrape the real endpoint and read the exposition text, because the claim is about what a
 * SCRAPE sees. Asserting on the registry object would pass on a metric nobody can collect.
 */
const sourcesFor = (n: number): MetricSources => ({
  connections: () => n,
  leader: () => (n === 1 ? 1 : 0),
  publishing: () => (n === 1 ? 1 : 0),
  lagRevs: () => Promise.resolve(n * 10),
  applyStats: () => ({ groups: n, orderedFallbacks: 0 }),
});

/** `rtdb_connections{db="car"} 3` -> 3. Null when the label is not in the scrape at all. */
const sampleFor = (body: string, series: string, db: string): number | null => {
  const m = new RegExp(`^${series}\\{db="${db}"\\} (\\S+)$`, 'm').exec(body);
  return m ? Number(m[1]) : null;
};

test('the db budget is its own — a prefix flood does not evict a database name', async () => {
  // R7, and the mentor was right that this is orderable rather than unobservable. My checkpoint
  // said both dimensions answer `_other` so the cases cannot be told apart — but the db dimension
  // only answers `_other` once ITS budget is spent, and the only thing that spends it is the
  // boundedness test far below.
  //
  // ORDER IS THE INSTRUMENT, so it is stated rather than assumed: `node:test` runs one file's tests
  // in source order in one process, module state and all. By the time this runs, "the bytes-out
  // prefix label is bounded" above has pushed 200 paths through and spent the PREFIX budget in
  // full. One database name bound here therefore answers the question directly — shared budget, it
  // comes back `_other`; separate budgets, it comes back as itself.
  //
  // It must stay ABOVE the boundedness test. Below it, this passes for the wrong reason and would
  // keep passing after the budgets were merged.
  const { dbLabel } = await import('../../src/gateway/metrics.ts');
  assert.equal(dbLabel('after_the_prefix_flood'), 'after_the_prefix_flood');
});

test('two databases in one process both reach the scrape', async (t) => {
  const { bindSources, resetSources } = await import('../../src/gateway/metrics.ts');
  resetSources();
  t.after(() => resetSources());

  bindSources('car', sourcesFor(1));
  bindSources('chat', sourcesFor(7));

  await withServer(storageWhose(async () => 0), async (base) => {
    const body = await (await fetch(`${base}/metrics`)).text();

    // The singleton's signature is that ONE of these is null and the other holds its own value.
    assert.equal(sampleFor(body, 'rtdb_connections', 'car'), 1);
    assert.equal(sampleFor(body, 'rtdb_connections', 'chat'), 7);
    // Every source-bound series, not just the first: each has its own `collect`, and the singleton
    // would have taken all six down together while each had to be fixed separately.
    assert.equal(sampleFor(body, 'rtdb_leader', 'car'), 1);
    assert.equal(sampleFor(body, 'rtdb_leader', 'chat'), 0);
    assert.equal(sampleFor(body, 'rtdb_publishing', 'car'), 1);
    assert.equal(sampleFor(body, 'rtdb_consumer_lag_revs', 'car'), 10);
    assert.equal(sampleFor(body, 'rtdb_consumer_lag_revs', 'chat'), 70);
    assert.equal(sampleFor(body, 'rtdb_storage_apply_groups_total', 'chat'), 7);
  });
});

test('a database that goes away takes its label with it', async (t) => {
  // The other half of a registry, and it fails the opposite way: a gauge keeps the last value it
  // was ever set for a label nobody feeds any more, so a dead tenant would report a live connection
  // count forever. Under Gate D tenants come and go; this is that, made visible.
  const { bindSources, resetSources } = await import('../../src/gateway/metrics.ts');
  resetSources();
  t.after(() => resetSources());

  const unbind = bindSources('ghost', sourcesFor(5));
  bindSources('stays', sourcesFor(2));

  await withServer(storageWhose(async () => 0), async (base) => {
    assert.equal(sampleFor(await (await fetch(`${base}/metrics`)).text(), 'rtdb_connections', 'ghost'), 5);
    unbind();
    const after = await (await fetch(`${base}/metrics`)).text();
    assert.equal(sampleFor(after, 'rtdb_connections', 'ghost'), null, 'the dead label is gone');
    assert.equal(sampleFor(after, 'rtdb_connections', 'stays'), 2, 'and the live one is untouched');
  });
});

test('past the cap, tenants sharing a label are ADDED and unbinding one keeps the rest', async (t) => {
  // §5.22 Gate E's second condition, and the singleton coming back through a side door. Keying the
  // registry by the BOUNDED label meant the 65th and 66th databases both keyed on `_other` and the
  // second overwrote the first — one tenant's numbers reported for two, which is the exact defect
  // this gate exists to remove.
  //
  // 66 tenants because the cap is 64: the first 64 get their own labels and the last two collide.
  // Reachable by design, not by accident — §5.19 FAISLA #5's own words are that past 64 a client's
  // databases fall into `_other`, and nothing caps the registry at 64.
  const { bindSources, resetSources } = await import('../../src/gateway/metrics.ts');
  resetSources();
  t.after(() => resetSources());

  // Every one of them a leader reporting one connection, so the claim does not depend on WHERE the
  // 64-name boundary happens to fall — the tests above have already spent part of the budget, and a
  // test that assumed a particular boundary would be asserting test order.
  const one: MetricSources = {
    connections: () => 1,
    leader: () => 1,
    publishing: () => 1,
    lagRevs: () => Promise.resolve(1),
    applyStats: () => ({ groups: 1, orderedFallbacks: 0 }),
  };
  const unbinds = Array.from({ length: 66 }, (_, i) => bindSources(`cap_${i}`, one));

  await withServer(storageWhose(async () => 0), async (base) => {
    const body = await (await fetch(`${base}/metrics`)).text();
    // Each tenant reports exactly 1, so `_other` must read the COUNT of the tenants that landed
    // there. More than one is the whole claim: keyed by the label, the bucket read 1 forever
    // however many collided.
    const other = sampleFor(body, 'rtdb_connections', '_other');
    assert.ok(other !== null && other > 1, `_other must total its tenants, got ${other}`);
    assert.equal(sampleFor(body, 'rtdb_leader', '_other'), other, 'and every leader in it is counted');
  });

  // And unbinding ONE tenant in the bucket must not erase the others. Before the fix this returned
  // null: the shared map entry was deleted out from under a tenant that was still bound.
  const before = unbinds.length - 1;
  unbinds[before]?.();
  await withServer(storageWhose(async () => 0), async (base) => {
    const body = await (await fetch(`${base}/metrics`)).text();
    assert.notEqual(sampleFor(body, 'rtdb_connections', '_other'), null, 'the survivors still report');
  });
});

test('the db label is bounded, whatever names arrive', async () => {
  // §5.19 FAISLA #5 made 64 a PRODUCT limit, not an ops detail. Under Gate D this value comes from
  // a token claim, so it is exactly as untrusted as a path.
  //
  // Asserted as BOUNDEDNESS rather than as a number, for two reasons. The cap is process-wide and
  // the tests above have already spent some of it, so any exact count here would be a statement
  // about test order. And a test comparing against the exported cap would move with any mutation of
  // it — Gate B's rule. What is true of the world is: names keep arriving, distinct labels stop
  // growing, and the overflow has a name.
  const { dbLabel, DEFAULT_DB_LABEL } = await import('../../src/gateway/metrics.ts');

  const distinct = (from: number, count: number): Set<string> =>
    new Set(Array.from({ length: count }, (_, i) => dbLabel(`db_${from + i}`)));

  const first = distinct(0, 200);
  assert.ok(first.has('_other'), 'past the cap, names land in _other');
  const afterMore = new Set([...first, ...distinct(1000, 200)]);
  assert.equal(afterMore.size, first.size, '200 more names add no new series');

  assert.equal(dbLabel(''), DEFAULT_DB_LABEL, 'an unnamed gateway is a real label, not a missing one');
  // "A name admitted early stays admitted" is NOT asserted here any more, and the reason is order
  // again: the cap test above binds 66 names deliberately and exhausts the budget, so by the time
  // this runs every new name is `_other` and there is nothing left to admit. That property is
  // asserted where it can be — "the db budget is its own" watches one name come back as itself, and
  // the two-database scrape reads `car` and `chat` by name.
});

// The separateness of the two budgets IS asserted, above and deliberately above: see "the db
// budget is its own". My checkpoint called it unobservable and the mentor cross-questioned that
// correctly — the db dimension only answers `_other` once ITS budget is spent, and this test is
// the only thing that spends it. Order was the instrument, not a fresh module.
