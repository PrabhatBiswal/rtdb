/**
 * §5.22 Gate D note (vi): what does an IDLE tenant cost?
 *
 *   OUT=/tmp/a1.json  WINDOW_MS=30000 node --import tsx scripts/idle-tenant-cost.ts 1
 *   OUT=/tmp/a14.json WINDOW_MS=30000 node --import tsx scripts/idle-tenant-cost.ts 14
 *
 * N buses = N Leadership renew timers (ttl/3) + N blocking XREAD loops whose idle path calls
 * `storage.head()` every window (`redis.ts` `#reconcile`). The order said measure rather than
 * assume, so: N tenants connect once and then do NOTHING, against a real Redis, for a window.
 *
 * One arm per PROCESS, and that is not tidiness: `process.cpuUsage()` is process-wide, so running
 * both arms in one process makes the second arm's CPU include whatever the first left running. It
 * also removes the cross-arm interference that made my first version stall partway through the
 * second arm — a script bug I could not reproduce when the arm ran alone.
 */
import { writeFileSync } from 'node:fs';
import { startGateway } from '../src/gateway/server.ts';
import { signDevToken } from '../src/gateway/auth.ts';
import { MemoryStorage } from '../src/storage/memory.ts';
import { DEFAULT_LIMITS } from '../src/protocol/limits.ts';
import { connectRedis } from '../src/fanout/redis.ts';
import { RedisProcess } from '../harness/redis.ts';
import { RtdbClient } from '../harness/client.ts';

const N = Number(process.argv[2] ?? 1);
const WINDOW_MS = Number(process.env.WINDOW_MS ?? 60_000);

const cmdstats = (info: string): Record<string, number> => {
  const out: Record<string, number> = {};
  for (const line of info.split('\n')) {
    const m = /^cmdstat_(\w+):calls=(\d+)/.exec(line.trim());
    if (m) out[m[1] as string] = Number(m[2]);
  }
  return out;
};

const proc = await RedisProcess.start();
/**
 * A killed measurement must not leave a redis-server behind. `RedisProcess` is a CHILD, so a
 * SIGTERM to this script does not reach it — three of them survived a `pkill` during development,
 * and the test suites' own `assertNoLeftovers` does not cover a script run by hand.
 */
for (const sig of ['SIGINT', 'SIGTERM'] as const) {
  process.on(sig, () => void proc.stop().finally(() => process.exit(1)));
}
const admin = await connectRedis(proc.url);

let heads = 0;
const gw = await startGateway({
  redis: await connectRedis(proc.url),
  storage: new MemoryStorage(DEFAULT_LIMITS),
  shard: 0,
  db: 'public',
  requireNs: true,
  tenantStorage: () => {
    const s = new MemoryStorage(DEFAULT_LIMITS);
    const real = s.head.bind(s);
    // The call the idle XREAD path makes every window (`redis.ts` `#reconcile`).
    (s as unknown as { head: () => Promise<number> }).head = () => { heads++; return real(); };
    return s;
  },
});

const clients: RtdbClient[] = [];
for (let i = 0; i < N; i++) {
  const c = new RtdbClient({
    url: `ws://127.0.0.1:${gw.port}`,
    token: signDevToken({ sub: `app-t${i}`, ns: `t${i}`, exp: Math.floor(Date.now() / 1000) + 3600 }),
    pingIntervalMs: 600_000,
  });
  c.connect();
  await c.ready();
  clients.push(c);
}

await new Promise((r) => setTimeout(r, 1500));   // settle startup OUT of the window
const before = cmdstats(await admin.info('commandstats'));
const cpu0 = process.cpuUsage();
const heads0 = heads;

await new Promise((r) => setTimeout(r, WINDOW_MS));

const after = cmdstats(await admin.info('commandstats'));
const cpu = process.cpuUsage(cpu0);
const clientList = String(await admin.sendCommand(['CLIENT', 'LIST'])).trim().split('\n').length;
const secs = WINDOW_MS / 1000;
const per = (k: string): number => Number((((after[k] ?? 0) - (before[k] ?? 0)) / secs).toFixed(2));

const result = JSON.stringify({
  tenants: N,
  xread_per_s: per('xread'),
  set_per_s: per('set'),
  eval_per_s: per('eval'),
  incr_per_s: per('incr'),
  redis_clients: clientList,
  head_calls_per_s: Number(((heads - heads0) / secs).toFixed(2)),
  cpu_ms_per_s: Number(((cpu.user + cpu.system) / 1000 / secs).toFixed(2)),
});
// Written to a FILE, not piped: this ends with `process.exit(0)` (the gateway and Redis leave
// handles that would otherwise hold the process), and `process.exit` does not flush a piped stdout
// — the first 60 s run produced an empty file and a zero exit code, which reads exactly like a
// measurement that ran and found nothing.
writeFileSync(process.env.OUT as string, result + '\n');
console.log(result);

for (const c of clients) c.close();
gw.close();
await admin.destroy();
await proc.stop();
process.exit(0);
