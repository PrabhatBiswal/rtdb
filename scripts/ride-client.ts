/**
 * The riding client §5.9-C Q1 has been asking for since 2026-08-31: a client WRITING through a
 * gateway roll, so "zero write errors" is measured on this code instead of inherited from an older
 * one. Writes to a scratch path; every write is counted and every outcome is named.
 *
 *   SK_FILE=/path/to/shadow-key node --import tsx scripts/ride-client.ts <seconds>
 */
import { readFileSync } from 'node:fs';
import { RtdbClient } from '../harness/client.ts';

const secs = Number(process.argv[2] ?? 90);
const key = readFileSync(process.env['SK_FILE'] as string, 'utf8').trim();

const tokenUrl = process.env['RTDB_SHADOW_TOKEN_URL'] ?? 'http://127.0.0.1:8788/shadow-token';

const r = await fetch(tokenUrl, {
  method: 'POST',
  headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
  body: JSON.stringify({ device: 'ride-client' }),
});
const { token } = (await r.json()) as { token: string };

const c = new RtdbClient({ url: process.env['RTDB_URL'] ?? 'ws://127.0.0.1:8080', token, sdk: 'ride/1' });
let acks = 0, errs = 0, issued = 0, closes = 0, opens = 0;
const errCodes: Record<string, number> = {};
const lags: number[] = [];

c.on('close', (e: { code: number; reason: string }) => {
  closes++;
  console.log(JSON.stringify({ t: new Date().toISOString(), ev: 'client.close', ...e, acks, errs, issued }));
});
c.connect();
const ack = await c.ready();
opens++;
console.log(JSON.stringify({ t: new Date().toISOString(), ev: 'connected', epoch: ack.epoch, head: ack.rev, session: ack.session }));
c.listen('drill/ride');

const t0 = Date.now();
const timer = setInterval(() => {
  issued++;
  const at = Date.now();
  c.put(`drill/ride/n`, issued).then(
    () => { acks++; lags.push(Date.now() - at); },
    (e: unknown) => {
      errs++;
      const code = String(e).match(/\b(AUTH|RULES|BADPATH|BADFRAME|TOOBIG|RATE|CLOSED)\b/)?.[1] ?? 'OTHER';
      errCodes[code] = (errCodes[code] ?? 0) + 1;
      console.log(JSON.stringify({ t: new Date().toISOString(), ev: 'write.err', code, msg: String(e).slice(0, 120) }));
    },
  );
}, 200); // 5 writes/sec — well under §9's 100/s

await new Promise((res) => setTimeout(res, secs * 1000));
clearInterval(timer);
await new Promise((res) => setTimeout(res, 3000)); // let the last writes settle
const sorted = [...lags].sort((a, b) => a - b);
console.log(JSON.stringify({
  t: new Date().toISOString(), ev: 'RESULT',
  durationSec: Math.round((Date.now() - t0) / 1000),
  issued, acks, errs, errCodes,
  unsettled: issued - acks - errs,
  reconnects: closes,
  ackLagMs: { p50: sorted[Math.floor(sorted.length / 2)] ?? 0, max: sorted.at(-1) ?? 0 },
}));
await c.put('drill/ride', null); // scratch cleanup
c.close();
