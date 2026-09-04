/** Gate C failure tooth: stop one gateway under load. §8 sizes each gateway for 100%, and WP5's
 *  property is that a fault costs LATENCY, never a write. Zero write errors is the assertion. */
import { RtdbClient } from '../harness/client.ts';
import { signDevToken } from '../src/gateway/auth.ts';
const URL = process.env['RTDB_URL'] ?? 'ws://127.0.0.1:8080';
const token = signDevToken({ sub: 'u_drain', exp: Math.floor(Date.now() / 1000) + 3600 });
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
const P = `MPK_1010/drain-${Date.now()}`;

const clients = Array.from({ length: 8 }, () => new RtdbClient({ url: URL, token }));
for (const c of clients) c.connect();
await Promise.all(clients.map((c) => c.ready()));
for (const c of clients) c.listen(P);
await sleep(500);
console.log(`READY ${clients.length} connections`);

let acks = 0, errs = 0, n = 0;
const lat: number[] = [];
const inflight: Promise<unknown>[] = [];
const until = Date.now() + 100_000;
while (Date.now() < until) {
  const c = clients[n % clients.length] as RtdbClient;
  const t0 = Date.now();
  inflight.push(c.put(`${P}/w${n % clients.length}`, { n: ++n, t: t0 })
    .then((r) => { if (r.type === 'ack') { acks++; lat.push(Date.now() - t0); } else errs++; })
    .catch(() => { errs++; }));
  await sleep(100);
}
await Promise.all(inflight);
const pending = clients.reduce((a, c) => a + c.pendingWriteIds.length, 0);
lat.sort((a, b) => a - b);
const p = (q: number): number => lat[Math.min(lat.length - 1, Math.floor(lat.length * q))] ?? 0;
console.log(`sent ${n}  acks ${acks}  WRITE ERRORS ${errs}  unsettled ${pending}`);
console.log(`ack ms p50 ${p(0.5)}  p99 ${p(0.99)}  max ${lat.at(-1)}`);
for (const c of clients) c.close();
console.log(errs === 0 && pending === 0 && acks === n ? 'DRAIN OK: zero write errors' : 'DRAIN FAILED');
process.exit(errs === 0 && pending === 0 && acks === n ? 0 : 1);
