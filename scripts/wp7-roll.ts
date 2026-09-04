/**
 * WP7 Gate A: ride the rolling deploy. §8 sizes each gateway for 100% of the load, so restarting
 * one at a time must cost LATENCY and never a write (the WP6 drain property, re-proved against a
 * change that touches the write path's failure modes).
 *
 * The token comes from RTDB_TOKEN, minted by scripts/console-token.ts, which reads the secret from
 * SSM and prints only the token. The 2026-08-29 lesson stands: the secret never enters argv, an
 * environment that gets stored, or this file.
 *
 *   RTDB_TOKEN=$(node --import tsx scripts/console-token.ts --name wp7roll) \
 *     node --import tsx scripts/wp7-roll.ts [seconds]
 */
import { RtdbClient } from '../harness/client.ts';

const URL = process.env['RTDB_URL'] ?? 'ws://127.0.0.1:8080';
const token = process.env['RTDB_TOKEN'];
if (!token) throw new Error('RTDB_TOKEN is required (scripts/console-token.ts mints one)');
const seconds = Number(process.argv[2] ?? 420);
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const P = `MPK_1010/wp7roll-${Date.now()}`;

const clients = Array.from({ length: 8 }, () => new RtdbClient({ url: URL, token }));
for (const c of clients) c.connect();
await Promise.all(clients.map((c) => c.ready()));
for (const c of clients) c.listen(P);
await sleep(500);
console.log(`READY ${clients.length} connections on ${URL}, riding for ${seconds}s`);

let acks = 0;
let errs = 0;
let n = 0;
const lat: number[] = [];
const inflight: Promise<unknown>[] = [];
const drops: string[] = [];
clients.forEach((c, i) => c.on('close', (e: unknown) => {
  const { code } = e as { code: number };
  drops.push(`c${i}:${code}@${new Date().toISOString().slice(11, 19)}`);
}));

const until = Date.now() + seconds * 1000;
while (Date.now() < until) {
  const c = clients[n % clients.length] as RtdbClient;
  const t0 = Date.now();
  inflight.push(
    c
      .put(`${P}/w${n % clients.length}`, { n: ++n, t: t0 })
      .then((r) => {
        if (r.type === 'ack') {
          acks++;
          lat.push(Date.now() - t0);
        } else errs++;
      })
      .catch((e: unknown) => {
        errs++;
        console.log(`WRITE ERROR: ${String((e as { message?: string }).message ?? e)}`);
      }),
  );
  await sleep(100);
}
await Promise.all(inflight);
const pending = clients.reduce((a, c) => a + c.pendingWriteIds.length, 0);
lat.sort((a, b) => a - b);
const p = (q: number): number => lat[Math.min(lat.length - 1, Math.floor(lat.length * q))] ?? 0;
console.log(`sent ${n}  acks ${acks}  WRITE ERRORS ${errs}  unsettled ${pending}`);
console.log(`ack ms p50 ${p(0.5)}  p99 ${p(0.99)}  max ${lat.at(-1)}`);
console.log(`socket drops during the roll: ${drops.length ? drops.join(' ') : 'none'}`);
for (const c of clients) c.close();
const ok = errs === 0 && pending === 0 && acks === n;
console.log(ok ? 'ROLL OK: zero write errors' : 'ROLL FAILED');
process.exit(ok ? 0 : 1);
