/**
 * WP7 Gate C's peer: the SECOND client in "observe a peer update". It writes a member the phone has
 * never seen, then prints the server's own view of `demo/room` so the phone's screen can be compared
 * against it rather than against a hope.
 *
 *   RTDB_TOKEN=$(node --import tsx scripts/console-token.ts --name wp7phone) \
 *     node --import tsx scripts/wp7-gatec.ts <memberKey> <memberName>
 */
import type { ServerFrame } from '../src/protocol/frames.ts';
import { RtdbClient } from '../harness/client.ts';
import { waitUntil } from '../harness/scenario.ts';

const token = process.env['RTDB_TOKEN'];
if (!token) throw new Error('RTDB_TOKEN is required (scripts/console-token.ts mints one)');
const [key, name] = process.argv.slice(2);
if (!key || !name) throw new Error('usage: scripts/wp7-gatec.ts <memberKey> <memberName>');

const c = new RtdbClient({ url: process.env['RTDB_URL'] ?? 'ws://127.0.0.1:8080', token, pingIntervalMs: 60_000 });
c.connect();
const ack = await c.ready();

// Wait for the SNAPSHOT FRAME, not for a non-undefined value: an empty subtree reads as null, so
// "the value is not undefined" is true before the server has said anything at all (WP4's lesson).
let snapshot = false;
c.on('frame', (f: ServerFrame) => {
  if (f.type === 'snapshot') snapshot = true;
});
c.listen('demo/room');
await waitUntil(() => snapshot, 'the first snapshot');

const wrote = await c.put(`demo/room/members/${key}`, { name, status: 'online' });
console.log(`PEER WROTE demo/room/members/${key} -> ${JSON.stringify(wrote)}`);
// Our own echo is the LAST thing to converge under §7, so wait for serverState rather than the view.
await waitUntil(() => c.mirror.serverValue(`demo/room/members/${key}/name`) === name, 'the echo');

console.log(`EPOCH ${ack.epoch}`);
console.log(`SERVER_JSON ${JSON.stringify(c.value('demo/room'))}`);
c.close();
