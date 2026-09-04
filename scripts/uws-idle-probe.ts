/**
 * What does uWS's `idleTimeout` ACTUALLY do to a socket stuck before helloAck?
 *
 *   node --import tsx scripts/uws-idle-probe.ts
 *
 * Written 2026-09-01 to settle a hello-timeout severity dispute,
 * where the claim was that a malformed or missing `helloAck` hangs the client FOREVER. Against our
 * own gateway that is wrong — it is ~70s — and the difference matters, because it is the difference
 * between "production dies silently" and "production looks frozen and then recovers".
 *
 * Three configs, all mirroring `src/gateway/server.ts`'s ws options exactly except `idleTimeout`,
 * which is scaled 70 -> 8s so the whole thing runs in about a minute instead of six. It probes uWS
 * DIRECTLY rather than driving the SDK through it: driving the pair tells you the pair works, not
 * which half produced the bound.
 *
 *   A  server silent after upgrade                  -> expect close at ~1x idleTimeout
 *   B  server sends every 2s, never reads           -> does a SEND reset the timer?
 *   C  `sendPingsAutomatically` omitted (uWS default) -> the third-party / one-refactor-away case
 *
 * PINNED-VERSION CAVEAT: measured against uWebSockets.js v20.52.0 (package.json). B's result
 * CONTRADICTS that version's own typings, which say the timer resets on "sending or getting a
 * message" — measured, only receiving resets it. Re-run this after any uWS bump; the typings are
 * not a substitute for it. Nothing here touches the gateway or the SDK — it is a measurement, and
 * it stands up its own servers on loopback.
 */
import uWS from 'uWebSockets.js';

const IDLE = 8; // seconds; the gateway ships 70 (src/protocol/limits.ts:56)
const OBSERVE_MS = 45_000; // 5.6x IDLE — long enough to call "never fired"
const BASE_PORT = 39_517;

type Case = { label: string; chatty: boolean; autoPings: boolean };
const CASES: Case[] = [
  { label: 'A  silent server, sendPingsAutomatically:false', chatty: false, autoPings: false },
  { label: 'B  server SENDS every 2s, never reads         ', chatty: true, autoPings: false },
  { label: 'C  sendPingsAutomatically OMITTED (uWS default)', chatty: false, autoPings: true },
];

CASES.forEach((c, i) => {
  uWS
    .App()
    .ws<{ t?: NodeJS.Timeout }>('/*', {
      idleTimeout: IDLE,
      // Omitted entirely in case C — that is the whole point of case C.
      ...(c.autoPings ? {} : { sendPingsAutomatically: false }),
      open(ws) {
        if (!c.chatty) return;
        // 2s against an 8s timeout: three sends land inside one timeout window.
        ws.getUserData().t = setInterval(() => {
          try {
            ws.send('{"type":"noise"}', false);
          } catch {
            /* socket gone; the close handler clears the timer */
          }
        }, 2000);
      },
      message() {},
      close(ws) {
        const { t } = ws.getUserData();
        if (t) clearInterval(t);
      },
    })
    .listen(BASE_PORT + i, () => undefined);
});

/** One client: connect, send `hello`, then say nothing — exactly the pre-helloAck window. */
function probe(label: string, port: number): Promise<string> {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    let opened = 0;
    let got = 0;
    ws.onopen = () => {
      opened = Date.now();
      ws.send('{"type":"hello","token":"t","proto":1}');
    };
    ws.onmessage = () => void got++;
    ws.onclose = (e) => {
      const secs = (Date.now() - opened) / 1000;
      resolve(`${label}  CLOSED at ${secs.toFixed(1)}s (${(secs / IDLE).toFixed(1)}x idle) code=${e.code} rx=${got}`);
    };
    setTimeout(() => {
      try {
        ws.close();
      } catch {
        /* already gone */
      }
      resolve(`${label}  STILL OPEN at ${OBSERVE_MS / 1000}s (${OBSERVE_MS / 1000 / IDLE}x idle) rx=${got} — NEVER FIRED`);
    }, OBSERVE_MS);
  });
}

console.log(`uWebSockets.js v20.52.0, idleTimeout=${IDLE}s, client sends hello then nothing\n`);
for (const [i, c] of CASES.entries()) console.log(await probe(c.label, BASE_PORT + i));
process.exit(0);
