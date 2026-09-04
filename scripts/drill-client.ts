/**
 * The restore drill's hands: connect, optionally write, and print what the shard says about itself.
 * Used by docs/wp4-restore-drill.md — the whole point of that document is that it is re-runnable,
 * on this machine now and on AWS in Phase 6.
 *
 *   node --import tsx scripts/drill-client.ts <port> [write <key> <value>]
 */
import type { ServerFrame } from '../src/protocol/frames.ts';
import { RtdbClient } from '../harness/client.ts';
import { waitUntil } from '../harness/scenario.ts';
import { signDevToken } from '../src/gateway/auth.ts';

const [port, mode, key, value] = process.argv.slice(2);
if (!port) throw new Error('usage: drill-client.ts <port> [write <key> <value>]');

const token = signDevToken({ sub: 'u_drill', exp: Math.floor(Date.now() / 1000) + 3600 });
const c = new RtdbClient({ url: `ws://127.0.0.1:${port}`, token, pingIntervalMs: 60_000 });
c.connect();
const ack = await c.ready();
// Wait for the SNAPSHOT FRAME, not for a non-undefined value: an empty subtree reads as null, so
// "value is not undefined" is true before the server has said anything at all.
let snapshot = false;
c.on('frame', (f: ServerFrame) => {
  if (f.type === 'snapshot') snapshot = true;
});
c.listen('room');
await waitUntil(() => snapshot, 'the first snapshot');
if (mode === 'write' && key) await c.put(`room/${key}`, value ?? null);

console.log(`epoch=${ack.epoch} head=${ack.rev} room=${JSON.stringify(c.value('room'))}`);
c.close();
