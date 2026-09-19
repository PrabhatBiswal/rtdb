/**
 * WORKLOAD §2 load rig. N connections across M client processes, W writes/sec, driving one or two
 * gateways, measuring what the system actually sustains: ack p50/p99, delta fan-out lag p50/p99, and
 * a convergence check against `nodes` at the end.
 *
 *   node --import tsx scripts/loadsim.ts [flags]
 *
 *   --conns N        total connections            (default 10000)
 *   --procs M        client processes             (default 8)
 *   --rate W         writes/sec, whole run        (default 2000)
 *   --seconds S      steady-state duration        (default 30)
 *   --spawn 1|2      gateways to start locally    (default 2; 2 implies Redis)
 *   --gateways URLs  comma-separated, instead of --spawn
 *   --bus 1          spawn Redis even for --spawn 1 (2 gateways always get one)
 *   --pg URL         Postgres for the `nodes` convergence check in --gateways mode
 *   --schema NAME    the shard's schema there (default public)
 *   --ns NAME        §5.20 database claim: token gets `ns`, and ROOT moves under it
 *   --leaves L       write to `<root>/own/<id>/<idx>/k<0..L>` instead of one `v` (default 1)
 *   --cleanup 0      keep the tree after the run; default is to `put null ROOT`
 *   --settle S       seconds to wait for the last deltas before counting what is behind (20)
 *   --hot F          fraction of CONNECTIONS also watching the shared hot path (default 0.1)
 *   --hotwrite F     fraction of WRITES aimed at that hot path            (default 0.05)
 *   --cas F          fraction of writes that are CAS                (default 0.05)
 *   --sample K       record every Kth delta's lag                   (default 20)
 *
 * Fan-out lag is measured by stamping `Date.now()` INTO the written value and subtracting it when
 * the delta lands. That is only legitimate because every process here is on one machine — §5's rule
 * that clocks are never compared across machines still stands for the protocol itself.
 *
 * Best-effort locally (user ruling): this records what THIS machine sustains. The formal 10k/2k pass
 * runs on AWS in Phase 6 with the same flags.
 */
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { RtdbClient } from '../harness/client.ts';
import { dropSchema, PG_URL, uniqueSchema } from '../harness/pg.ts';
import { RedisProcess } from '../harness/redis.ts';
import { GatewayProcess } from '../harness/scenario.ts';
import { signDevToken } from '../src/gateway/auth.ts';
import type { Ack, CasFail, Delta, ServerFrame } from '../src/protocol/frames.ts';
import { makeLimits } from '../src/protocol/limits.ts';

const flag = (name: string, fallback: string): string => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : (process.argv[i + 1] as string);
};
const num = (name: string, fallback: number): number => Number(flag(name, String(fallback)));

const CFG = {
  conns: num('conns', 10_000),
  procs: num('procs', 8),
  rate: num('rate', 2000),
  seconds: num('seconds', 30),
  hot: num('hot', 0.1),
  hotwrite: num('hotwrite', 0.05),
  cas: num('cas', 0.05),
  sample: num('sample', 20),
  leaves: num('leaves', 1),
};

/**
 * §5.20 Phase 1 confines an `ns` token to paths under that one segment (`rules.ts:142`), and the
 * claim ALSO picks the tenant at hello (`server.ts:588`). So naming a database moves the whole run
 * into it — schema and path both — and there is no second flag for the prefix: one of them would be
 * wrong eventually. Without `--ns` this is the old run, on the default tenant, at `sim`.
 */
const NS = flag('ns', '');
const ROOT = NS ? `${NS}/sim` : 'sim';
const HOT = `${ROOT}/hot`;
const TOKEN = signDevToken({
  sub: 'u_loadsim',
  ...(NS ? { ns: NS } : {}),
  exp: Math.floor(Date.now() / 1000) + 3600,
});
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

interface Report {
  conns: number;
  acks: number;
  casOk: number;
  casFail: number;
  errs: number;
  pending: number;
  mismatches: number;
  ackMs: number[];
  lagMs: number[];
  /** A few (path, value) pairs this worker wrote LAST, for the parent to check against `nodes`. */
  samples: [string, number][];
}

// --------------------------------------------------------------------------- worker

async function worker(): Promise<void> {
  const id = num('id', 0);
  const urls = flag('urls', '').split(',');
  const conns = num('conns', 1);
  const rate = num('rate', 1);
  const limits = makeLimits({ WRITE_RATE_PER_SEC: 1e9, WRITE_RATE_BURST: 1e9 });

  const r: Report = { conns, acks: 0, casOk: 0, casFail: 0, errs: 0, pending: 0, mismatches: 0, ackMs: [], lagMs: [], samples: [] };
  let seen = 0;

  const clients: RtdbClient[] = [];
  for (let i = 0; i < conns; i++) {
    const c = new RtdbClient({
      url: urls[i % urls.length] as string,
      token: TOKEN,
      limits,
      pingIntervalMs: 60_000,
    });
    c.on('frame', (f: ServerFrame) => {
      const deltas = f.type === 'delta' ? [f] : f.type === 'batch' ? f.frames.filter((x): x is Delta => x.type === 'delta') : [];
      for (const d of deltas) {
        if (++seen % CFG.sample !== 0) continue;
        const t = (d.value as { t?: number } | null)?.t;
        if (typeof t === 'number') r.lagMs.push(Date.now() - t);
      }
    });
    c.connect();
    await c.ready();
      // Mixed shapes (§2): everyone watches their own subtree, and a configurable fraction ALSO
    // watches the shared hot path — which is where the real fan-out cost lives.
    c.listen(`${ROOT}/own/${id}/${i}`);
    if (Math.random() < CFG.hot) c.listen(HOT);
    clients.push(c);
  }
  process.send?.({ ready: conns });
  await new Promise<void>((resolve) => process.once('message', () => resolve()));

  // 100 ticks a second, `rate/100` writes each: the writer tracks the target instead of sprinting
  // and idling. A tick that overruns its 10ms simply slips — the sustained rate is what we report.
  const perTick = Math.max(1, Math.round(rate / 100));
  const tickMs = perTick === 1 ? Math.max(1, Math.round(1000 / rate)) : 10;
  const deadline = Date.now() + CFG.seconds * 1000;
  /**
   * Keyed by PATH, not by connection: with `--leaves > 1` one connection writes many leaves, and a
   * rev remembered per connection would be a rev from a different node — every CAS would fail on
   * the first mismatch and report a fanout-shaped run as a CAS-shaped one.
   */
  const lastRev = new Map<string, number>();
  /** The last value each private path was written with — the ground truth for convergence. */
  const wrote = new Map<string, number>();
  let n = 0;

  const started0 = Date.now();
  let ticks = 0;
  const inflight: Promise<unknown>[] = [];
  while (Date.now() < deadline) {
    for (let k = 0; k < perTick; k++) {
      const idx = n % clients.length;
      const c = clients[idx] as RtdbClient;
      // §5.30: one leaf per connection keeps the subtree a fixed size forever. `--leaves L` spreads
      // the writes over L of them, so the tree GROWS while the run holds its rate — which is what
      // the storage line is supposed to climb on.
      const leaf = CFG.leaves > 1 ? `k${Math.floor(Math.random() * CFG.leaves)}` : 'v';
      const mine = `${ROOT}/own/${id}/${idx}/${leaf}`;
      const path = Math.random() < CFG.hotwrite ? `${HOT}/w${id}` : mine;
      const value = { t: Date.now(), n: ++n };
      const started = Date.now();
      const expected = lastRev.get(mine);
      const useCas = expected !== undefined && Math.random() < CFG.cas;
      inflight.push(
        (useCas ? c.cas(mine, expected, value) : c.put(path, value))
          .then((res: Ack | CasFail) => {
            if (res.type === 'ack') {
              r.acks++;
              if (useCas) r.casOk++;
              lastRev.set(mine, res.rev);
              if (useCas || path === mine) wrote.set(mine, value.n);
            } else {
              r.casFail++;
              lastRev.set(mine, res.rev);
            }
            r.ackMs.push(Date.now() - started);
          })
          .catch(() => r.errs++),
      );
    }
    // Pace against the wall clock, not against the acks: awaiting each tick's writes would fold ack
    // latency into the send rate and quietly report a slower run as the machine's ceiling.
    const wait = started0 + ++ticks * tickMs - Date.now();
    if (wait > 0) await sleep(wait);
  }
  await Promise.all(inflight);

  /**
   * Every connection watches its own subtree, so its mirror MUST eventually carry the last value it
   * wrote. Polled rather than slept on, and the deadline is what this number MEANS:
   *
   * **A non-zero count here is "deltas that had not arrived yet when the clock ran out", NOT a
   * divergence.** §5.30 C1 read 7,932 and 4,935 on two databases and 0 on the other two, with
   * `fanout_seconds` p50 at 4-7 MINUTES — every one of those paths was acked, committed, and
   * waiting in its own connection's sink queue. Against a 20 s deadline that is arithmetic, not a
   * fault. Divergence would have to survive an unbounded wait, and this loop cannot offer one: the
   * run has to end. So `--settle` is the knob, the printed line says how long it waited, and the
   * authoritative check for a real mismatch is the `nodes` convergence query below — which needs
   * `--pg` and is therefore SKIPPED on the production runs, where this number is all there is.
   */
  const behind = (): number => {
    let n = 0;
    for (const [path, value] of wrote) {
      // Second-to-last segment, not a fixed index: ROOT grows a database segment under `--ns` and
      // the leaf name varies under `--leaves`. The one thing that never moves is `.../<idx>/<leaf>`.
      const idx = Number(path.split('/').at(-2));
      const mirrored = (clients[idx] as RtdbClient).mirror.serverValue(path) as { n?: number } | null;
      if (mirrored?.n !== value) n++;
    }
    return n;
  };
  const settleBy = Date.now() + num('settle', 20) * 1000;
  while (behind() > 0 && Date.now() < settleBy) await sleep(250);
  r.mismatches = behind();
  for (const c of clients) r.pending += c.pendingWriteIds.length;
  r.samples = [...wrote].slice(-20);
  process.send?.({ report: { ...r, ackMs: r.ackMs, lagMs: r.lagMs } });
  for (const c of clients) c.close();
}

// --------------------------------------------------------------------------- parent

const pct = (xs: number[], p: number): number =>
  xs.length === 0 ? 0 : (xs.sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor((xs.length * p) / 100))] as number);

async function parent(): Promise<void> {
  const spawn = num('spawn', 2);
  const given = flag('gateways', '');
  let urls: string[];
  let stop = async (): Promise<void> => undefined;
  let pgUrl = flag('pg', PG_URL);
  let schema = flag('schema', 'public');

  if (given) {
    urls = given.split(',');
  } else {
    schema = uniqueSchema('loadsim');
    const redis = spawn > 1 || flag('bus', '') ? await RedisProcess.start() : null;
    const env = {
      RTDB_STORAGE: 'postgres',
      RTDB_PG_URL: pgUrl,
      RTDB_PG_SCHEMA: schema,
      ...(redis ? { RTDB_REDIS_URL: redis.url } : {}),
    };
    const gws: GatewayProcess[] = [];
    for (let i = 0; i < spawn; i++) gws.push(await GatewayProcess.start({}, 0, undefined, env));
    urls = gws.map((g) => g.url);
    stop = async () => {
      for (const g of gws) await g.stop();
      await redis?.stop();
      await dropSchema(schema).catch(() => undefined);
    };
  }

  const limit = (process.report?.getReport() as { userLimits?: { open_files?: { soft: number } } })?.userLimits;
  console.log(
    `loadsim: ${CFG.conns} conns / ${CFG.procs} procs / ${CFG.rate} w/s / ${CFG.seconds}s over ${urls.length} gateway(s)`,
  );
  console.log(`  gateways: ${urls.join(' ')}`);
  console.log(`  database: ${NS || '(default tenant, no --ns)'}    ROOT: ${ROOT}    leaves: ${CFG.leaves}`);
  console.log(`  open-file soft limit: ${limit?.open_files?.soft ?? 'unknown'} (each connection is one fd per side)`);
  console.log(
    `  shape: ${(CFG.hot * 100).toFixed(0)}% of connections watch the hot path, ${(CFG.hotwrite * 100).toFixed(0)}% of writes hit it` +
      ` -> ~${Math.round(CFG.rate * CFG.hotwrite * CFG.conns * CFG.hot)} hot deliveries/s, ${(CFG.cas * 100).toFixed(0)}% CAS`,
  );

  // An interrupted run must not orphan its gateways: a SIGKILLed gateway leaves a Postgres backend
  // holding the schema, and the next DROP blocks behind it. SIGKILL to us is still unrecoverable.
  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, () => void stop().finally(() => process.exit(130)));
  }

  const per = Math.ceil(CFG.conns / CFG.procs);
  const self = fileURLToPath(import.meta.url);
  const kids = Array.from({ length: CFG.procs }, (_, i) =>
    // The overrides go FIRST: `flag()` takes the first match, so these win over the inherited run
    // config while every other flag (--seconds, --hot, --cas, --sample) reaches the worker unchanged.
    fork(self, ['--id', String(i), '--urls', urls.join(','), '--conns', String(per), '--rate', String(Math.ceil(CFG.rate / CFG.procs)), ...process.argv.slice(2)], {
      execArgv: ['--import', 'tsx'],
      env: { ...process.env, LOADSIM_WORKER: '1' },
    }),
  );

  const reports: Report[] = [];
  const done = new Promise<void>((resolve) => {
    let ready = 0;
    let opened = 0;
    for (const k of kids) {
      k.on('message', (m: { ready?: number; report?: Report }) => {
        if (m.ready !== undefined) {
          opened += m.ready;
          if (++ready === kids.length) {
            console.log(`  ${opened} connections open; starting the clock`);
            for (const kid of kids) kid.send('go');
          }
        }
        if (m.report) {
          reports.push(m.report);
          if (reports.length === kids.length) resolve();
        }
      });
      k.on('exit', (code) => {
        if (code !== 0 && reports.length < kids.length) console.error(`  worker exited ${code}`);
      });
    }
  });
  await done;

  const all = <K extends keyof Report>(k: K): Report[K][] => reports.map((r) => r[k]);
  const sum = (k: 'acks' | 'casOk' | 'casFail' | 'errs' | 'pending' | 'mismatches' | 'conns'): number =>
    reports.reduce((a, r) => a + r[k], 0);
  const ackMs = (all('ackMs') as number[][]).flat();
  const lagMs = (all('lagMs') as number[][]).flat();

  // §2's convergence check, against the materialised `nodes` table itself.
  const client = new pg.Client({ connectionString: pgUrl, options: `-c search_path=${schema}` });
  let bad = 0;
  let checked = 0;
  // Skipped only when we were pointed at gateways AND not told where their shard lives.
  if (!given || flag('pg', '')) {
    await client.connect();
    for (const r of reports) {
      for (const [path, n] of r.samples) {
        // §1 flattens objects to leaves, so the materialised node for `{t, n}` at P is P/n and P/t
        // — there is no row at P itself. The leaf is what `nodes` actually holds (§8).
        const { rows } = await client.query<{ value: number }>('SELECT value FROM nodes WHERE path = $1', [`${path}/n`]);
        checked++;
        if (rows[0]?.value !== n) bad++;
      }
    }
    await client.end();
  }

  console.log('');
  console.log(`  connections     ${sum('conns')}`);
  console.log(`  acks            ${sum('acks')}  (${(sum('acks') / CFG.seconds).toFixed(0)}/s sustained, ${CFG.rate}/s asked)`);
  console.log(`  cas ok / fail   ${sum('casOk')} / ${sum('casFail')}   (a casFail is a normal outcome, §4)`);
  console.log(`  write errors    ${sum('errs')}`);
  console.log(`  unsettled       ${sum('pending')}`);
  console.log(`  ack p50 / p99   ${pct(ackMs, 50)}ms / ${pct(ackMs, 99)}ms`);
  console.log(`  fanout p50/p99  ${pct(lagMs, 50)}ms / ${pct(lagMs, 99)}ms   (${lagMs.length} samples, 1 in ${CFG.sample})`);
  console.log(`  convergence     ${checked - bad}/${checked} sampled paths match \`nodes\``);
  console.log(
    `  mirror behind    ${sum('mismatches')} paths still un-mirrored after ${num('settle', 20)}s` +
      ' (deltas not yet arrived, NOT a divergence — see the note in worker())',
  );

  /**
   * §5.30 Gate 0(a): the run puts its own tree back. `/topnodes` before must equal `/topnodes`
   * after — under `--ns` the database is DECLARED and stays either way, but on the default tenant
   * `sim` is a derived top-level name that would otherwise linger in the sidebar forever.
   *
   * `--cleanup 0` is for a LADDER: §5.30's steps are meant to grow one subtree across runs, so
   * every step but the last keeps what it wrote.
   */
  if (flag('cleanup', '1') !== '0') {
    const c = new RtdbClient({ url: urls[0] as string, token: TOKEN, pingIntervalMs: 60_000 });
    c.connect();
    await c.ready();
    await c.put(ROOT, null);
    c.close();
    console.log(`  cleanup         put null ${ROOT}`);
  }

  await stop();
  process.exit(sum('errs') === 0 && bad === 0 && sum('pending') === 0 ? 0 : 1);
}

if (process.env['LOADSIM_WORKER']) await worker();
else await parent();
