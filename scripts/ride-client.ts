/**
 * The riding client §5.9-C Q1 has been asking for since 2026-08-31: a client WRITING through a
 * gateway roll, so "zero write errors" is measured on this code instead of inherited from an older
 * one. Writes to a scratch path; every write is counted and every outcome is named.
 *
 *   SK_FILE=/path/to/shadow-key node --import tsx scripts/ride-client.ts <seconds>
 *
 * `RTDB_TOKEN` skips the mint — a local rig has no shadow-token server — and `RTDB_RIDE_PATH` moves
 * the scratch subtree, which §5.20 Phase 1 requires the moment the rider carries an `ns` claim: a
 * `b` token may not touch `drill/ride`, only `b/drill/ride` (`pipeline/rules.ts:142`).
 */
import { readFileSync } from 'node:fs';
import { RtdbClient } from '../harness/client.ts';

const secs = Number(process.argv[2] ?? 90);

const mint = async (): Promise<string> => {
  const key = readFileSync(process.env['SK_FILE'] as string, 'utf8').trim();
  const tokenUrl = process.env['RTDB_SHADOW_TOKEN_URL'] ?? 'http://127.0.0.1:8788/shadow-token';
  const r = await fetch(tokenUrl, {
    method: 'POST',
    headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' },
    body: JSON.stringify({ device: 'ride-client' }),
  });
  return ((await r.json()) as { token: string }).token;
};
const token = process.env['RTDB_TOKEN'] ?? (await mint());
const RIDE = process.env['RTDB_RIDE_PATH'] ?? 'drill/ride';

const c = new RtdbClient({ url: process.env['RTDB_URL'] ?? 'ws://127.0.0.1:8080', token, sdk: 'ride/1' });
let acks = 0, errs = 0, issued = 0, closes = 0, opens = 0;
const errCodes: Record<string, number> = {};
const lags: number[] = [];
let maxLag = -1, maxLagAt = 0;
/**
 * §5.28. `reconnects 1` says a reconnect happened; it does not say how long the client was gone,
 * and §5.27's two riders came back 2 s and 24 s after the SAME restart with no way to tell which
 * gateway either landed on. So: every (re)connect is a line with its session id, every backoff is
 * a line, and close→connected is measured rather than reconstructed from timestamps by hand.
 */
let closedAt: number | null = null;
const holes: number[] = [];

c.on('close', (e: { code: number; reason: string }) => {
  closes++;
  closedAt = Date.now();
  console.log(JSON.stringify({ t: new Date().toISOString(), ev: 'client.close', ...e, acks, errs, issued }));
});
c.on('state', (s: string) => {
  if (s === 'waiting') {
    // §6's backoff is FULL JITTER — `floor(random() * min(30_000, 1000 * 2**attempt))`
    // (`harness/client.ts:61`), so one sleep can be anything up to the 30 s cap. The gap between
    // this line and the next `connected` is what separates "the client slept" from "the target
    // would not answer", and without it a second 24 s reading proves nothing either way.
    console.log(JSON.stringify({ t: new Date().toISOString(), ev: 'waiting', n: opens }));
    return;
  }
  if (s !== 'connected') return;
  const now = Date.now();
  opens++;
  if (closedAt !== null) {
    holes.push(now - closedAt);
    closedAt = null;
  }
  // `ready()` resolves immediately in this state (the ack is already stored), so this costs one
  // microtask and keeps `epoch`/`head` on the same line as the session id.
  void c.ready().then((a: { epoch: number; rev: number; session: string }) => {
    console.log(JSON.stringify({
      t: new Date(now).toISOString(), ev: 'connected',
      session: a.session, epoch: a.epoch, head: a.rev, n: opens,
      ...(holes.length ? { reconnectMs: holes.at(-1) } : {}),
    }));
  });
});
c.connect();
await c.ready();
c.listen(RIDE);

const t0 = Date.now();
const timer = setInterval(() => {
  issued++;
  const at = Date.now();
  c.put(`${RIDE}/n`, issued).then(
    () => {
      acks++;
      const lag = Date.now() - at;
      lags.push(lag);
      // Which write took the longest matters as much as how long: if `maxAt` falls between a
      // `client.close` and the next `connected`, the lag IS the reconnect hole; outside it, it is
      // something else and the hole is not the explanation.
      if (lag > maxLag) { maxLag = lag; maxLagAt = at; }
    },
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
  ackLagMs: {
    p50: sorted[Math.floor(sorted.length / 2)] ?? 0,
    max: sorted.at(-1) ?? 0,
    maxAt: maxLag < 0 ? null : new Date(maxLagAt).toISOString(),
  },
  reconnectMs: {
    count: holes.length,
    max: Math.max(0, ...holes),
    p50: [...holes].sort((a, b) => a - b)[Math.floor(holes.length / 2)] ?? 0,
  },
}));
await c.put(RIDE, null); // scratch cleanup
c.close();
