/**
 * How does a shard behave when N clients subscribe to the SAME node at the same instant?
 *
 *   node --import tsx scripts/listen-storm.ts --clients 1000 --path room/lobby
 *
 * The question this answers is not "is it fast". It is whether anyone is SKIPPED. §3 sets a
 * subscription up in five steps (join, buffer, read at rev N, send, flush >N), and steps 2-3 are an
 * `await` on storage — so every simultaneous listener is competing for the SAME pool, which is 10
 * connections wide by default and rejects an acquisition that waits longer than 500ms.
 *
 * THE GATEWAY RUNS IN ITS OWN PROCESS, and that is not a detail: N clients on the measuring
 * process's event loop would contend with the server's, and the numbers would describe this file
 * rather than the shard (the lesson `measure-gap.ts` carries at its head).
 *
 * What client contention DOES still distort is latency, not correctness: whether every client got
 * its snapshot is a fact about the server's answers, and it is read here from what arrived. The
 * per-client timings are indicative only, and the server-side counters are the finding.
 */
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { RtdbClient } from '../harness/client.ts';
import { signDevToken } from '../src/gateway/auth.ts';

const flag = (n: string, d: string): string => {
  const i = process.argv.indexOf(`--${n}`);
  return i === -1 ? d : (process.argv[i + 1] as string);
};
const N = Number(flag('clients', '1000'));
const PATH = flag('path', 'room/lobby');
const LEAVES = Number(flag('leaves', '200'));
const SECRET = 'storm-secret';
// A real port, not 0: main.ts reads 0 as "no admin server at all", and the server counters are the
// whole finding here.
const ADMIN = Number(flag('admin', '19191'));
const PG = process.env['RTDB_PG_URL'] ?? 'postgres://localhost/postgres';
const SCHEMA = `storm_${randomUUID().slice(0, 8).replace(/-/g, '')}`;
const CONTROL = `${SCHEMA}_ctl`;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- the gateway, in its own process
const gw = spawn(process.execPath, ['--import', 'tsx', 'src/gateway/main.ts'], {
  env: {
    ...process.env,
    RTDB_PORT: '0',
    RTDB_ADMIN_PORT: String(ADMIN),
    RTDB_STORAGE: 'postgres',
    RTDB_PG_URL: PG,
    RTDB_PG_SCHEMA: SCHEMA,
    RTDB_CONTROL_SCHEMA: CONTROL,
    RTDB_DEV_SECRET: SECRET,
    RTDB_RULES: 'harness/allow-all-rules.ts',
    // WITHOUT this the pool gauge is NEVER BOUND: `bindPoolWaiting` sits inside main.ts's
    // multi-tenant branch, so a single-tenant run reports 0 waiters whatever the pool is doing.
    // The first two runs of this probe reported exactly that, and it meant nothing.
    RTDB_MULTI_TENANT: '1',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});
const gwLines: string[] = [];
gw.stderr?.setEncoding('utf8');
gw.stderr?.on('data', (c: string) => gwLines.push(c));
gw.stdout?.setEncoding('utf8');

const firstLine: string = await new Promise((resolve, reject) => {
  let buf = '';
  gw.stdout?.on('data', (c: string) => {
    buf += c;
    for (const line of buf.split('\n')) {
      if (line.includes('rtdb listening')) resolve(line);
      // Every other stdout line is a U3 lifecycle event; keep them, they carry `listen-abandoned`.
      if (line.trim()) gwLines.push(line);
    }
    buf = buf.slice(buf.lastIndexOf('\n') + 1);
  });
  gw.once('exit', (code) => reject(new Error(`gateway exited before listening (${code})`)));
});
const port = Number(/rtdb listening (\d+)/.exec(firstLine)?.[1]);
const adminPort = ADMIN;
console.log(`gateway pid ${gw.pid}  ws :${port}  admin :${adminPort || '(none)'}  schema ${SCHEMA}`);

// ---------------------------------------------------------------- give the node something to read
const seed = new RtdbClient({ url: `ws://127.0.0.1:${port}`, token: signDevToken({ sub: 'seeder', exp: Math.floor(Date.now() / 1000) + 3600 }, SECRET) });
seed.connect();
await seed.ready();
const tree: Record<string, unknown> = {};
for (let i = 0; i < LEAVES; i++) tree[`k${i}`] = { id: i, name: `member-${i}`, at: Date.now() };
await seed.put(PATH, tree as never);
seed.close();
console.log(`seeded ${PATH} with ${LEAVES} leaves`);

// ---------------------------------------------------------------- N clients, all connected FIRST
const clients: RtdbClient[] = [];
for (let i = 0; i < N; i++) {
  const c = new RtdbClient({
    url: `ws://127.0.0.1:${port}`,
    token: signDevToken({ sub: `u_${i}`, exp: Math.floor(Date.now() / 1000) + 3600 }, SECRET),
    autoReconnect: false,
  });
  c.connect();
  clients.push(c);
}
await Promise.all(clients.map((c) => c.ready()));
console.log(`${N} clients connected and helloAck'd`);
await sleep(500); // let the connect burst drain, so the storm is only the listens

// ---------------------------------------------------------------- the storm: every listen, one tick
const got = new Array<number>(N).fill(0);
const errs: string[] = [];
let settled = 0;
const t0 = performance.now();
for (let i = 0; i < N; i++) {
  clients[i]?.on('frame', (f: { type: string; code?: string; msg?: string }) => {
    if (f.type === 'snapshot' && got[i] === 0) { got[i] = performance.now() - t0; settled++; }
    if (f.type === 'err') { errs.push(`${f.code}: ${f.msg}`); settled++; }
  });
  clients[i]?.listen(PATH);
}
console.log(`all ${N} listens issued in one tick; waiting…`);
/**
 * The pool-wait gauge has to be sampled DURING, and that is not a refinement — a gauge read after
 * the storm reports the calm, and the first run of this probe reported 0 for exactly that reason.
 */
let peakWaiting = 0;
const sampler = setInterval(() => {
  void fetch(`http://127.0.0.1:${adminPort}/metrics`)
    .then((r) => r.text())
    .then((t) => {
      const m = /^rtdb_pg_pool_waiting (\d+(?:\.\d+)?)$/m.exec(t);
      if (m) peakWaiting = Math.max(peakWaiting, Number(m[1]));
    })
    .catch(() => undefined);
}, 25);
for (let w = 0; w < 240 && settled < N; w++) await sleep(250);
clearInterval(sampler);
const elapsed = performance.now() - t0;

// ---------------------------------------------------------------- what the SERVER says
const metrics = adminPort ? await fetch(`http://127.0.0.1:${adminPort}/metrics`).then((r) => r.text()).catch(() => '') : '';
const grab = (re: RegExp): string[] => metrics.split('\n').filter((l) => re.test(l) && !l.startsWith('#'));

const times = got.filter((t) => t > 0).sort((a, b) => a - b);
const pct = (p: number): string => (times.length ? (times[Math.min(times.length - 1, Math.floor(times.length * p))] as number).toFixed(0) : '-');

console.log(`\n================ ${N} clients, one node, one tick ================`);
console.log(`snapshots received     ${times.length} / ${N}`);
console.log(`errs                   ${errs.length}${errs.length ? '  e.g. ' + errs[0] : ''}`);
console.log(`never settled          ${N - settled}`);
console.log(`first snapshot         ${pct(0)} ms`);
console.log(`p50 / p90 / p99        ${pct(0.5)} / ${pct(0.9)} / ${pct(0.99)} ms`);
console.log(`last snapshot          ${times.length ? (times.at(-1) as number).toFixed(0) : '-'} ms   (window ${elapsed.toFixed(0)} ms)`);
console.log(`pool waiters PEAK      ${peakWaiting}   (sampled every 25ms DURING the storm)`);
console.log('\n-- server counters --');
for (const l of grab(/^rtdb_listens_total|^rtdb_pg_pool_waiting|^rtdb_connections|^rtdb_subscriptions/)) console.log('  ' + l);
const abandoned = gwLines.join('\n').split('\n').filter((l) => l.includes('listen-abandoned'));
console.log(`\n-- listen-abandoned lines: ${abandoned.length}`);
for (const l of abandoned.slice(0, 3)) console.log('  ' + l.slice(0, 160));

// ---------------------------------------------------------------- leave nothing behind
for (const c of clients) c.close();
gw.kill('SIGKILL');
await sleep(300);
const admin = new pg.Client({ connectionString: PG });
await admin.connect();
await admin.query(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
await admin.query(`DROP SCHEMA IF EXISTS ${CONTROL} CASCADE`);
await admin.end();
console.log('\ncleaned up.');
process.exit(0);
