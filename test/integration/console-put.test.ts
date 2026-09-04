import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { collect, harness, rawConnected, waitUntil } from '../helpers.ts';
import { signDevToken } from '../../src/gateway/auth.ts';

/**
 * §5.9 Gate B's local proof, and it is deliberately built the awkward way round.
 *
 * The frame is constructed by the CONSOLE'S OWN `buildPut`, extracted from the shipped HTML, and put
 * on a real socket to a real gateway — bypassing the page's allowlist entirely. That is the point:
 * a UI that declines to draw a button proves nothing about the wire, so the thing under test here is
 * what happens when someone gets a write frame onto the socket anyway.
 *
 * An editor's lands. A viewer's is refused by the GATEWAY with RULES, which is the half that matters.
 */
const HTML = fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url));

function consoleBuildPut(): (path: string, value: unknown) => Record<string, unknown> {
  const block = /<script id="rtdb-wire">([\s\S]*?)<\/script>/.exec(readFileSync(HTML, 'utf8'));
  assert.ok(block, 'the console must keep its outbound rules in a <script id="rtdb-wire"> block');
  const mod = { exports: {} as { buildPut: (p: string, v: unknown) => Record<string, unknown> } };
  new Function('module', block[1] as string)(mod);
  return mod.exports.buildPut;
}

const token = (sub: string, role: string): string =>
  signDevToken({ sub, exp: Math.floor(Date.now() / 1000) + 3600, role });

test("an editor's console put lands, built by the page's own code", async (t) => {
  const { gw } = await harness(t);
  const buildPut = consoleBuildPut();
  const ws = await rawConnected(gw.port, token('console-rw-asha', 'editor'));
  t.after(() => ws.close());
  const frames = collect(ws);

  const frame = buildPut('demo/console-edit', { edited: true });
  ws.send(JSON.stringify(frame));
  await waitUntil(() => frames.some((f) => f['writeId'] === frame['writeId']));

  const reply = frames.find((f) => f['writeId'] === frame['writeId']) as Record<string, unknown>;
  assert.equal(reply['type'], 'ack', 'an editor writes');
  assert.deepEqual((await gw.storage.readSnapshot('demo/console-edit')).value, { edited: true });
});

test("a viewer's hand-crafted put is refused BY THE GATEWAY, not by the page", async (t) => {
  const { gw } = await harness(t);
  const buildPut = consoleBuildPut();
  // A viewer's real session subject. The page would never send this frame; we send it anyway,
  // because "the page would never" is exactly the assumption an attacker does not share.
  const ws = await rawConnected(gw.port, token('console-asha', 'viewer'));
  t.after(() => ws.close());
  const frames = collect(ws);

  const frame = buildPut('demo/console-edit', 'nope');
  ws.send(JSON.stringify(frame));
  await waitUntil(() => frames.some((f) => f['writeId'] === frame['writeId']));

  const reply = frames.find((f) => f['writeId'] === frame['writeId']) as Record<string, unknown>;
  assert.equal(reply['type'], 'err');
  assert.equal(reply['code'], 'RULES');
  assert.equal((await gw.storage.readSnapshot('demo/console-edit')).value, null, 'nothing landed');
});

test('a delete from the console is a put of null, and leaves the tombstone §7 describes', async (t) => {
  const { gw } = await harness(t);
  const buildPut = consoleBuildPut();
  const ws = await rawConnected(gw.port, token('console-rw-asha', 'owner'));
  t.after(() => ws.close());
  const frames = collect(ws);

  const put = buildPut('demo/doomed', { a: 1, b: 2 });
  ws.send(JSON.stringify(put));
  await waitUntil(() => frames.some((f) => f['writeId'] === put['writeId']));

  const del = buildPut('demo/doomed', null);
  ws.send(JSON.stringify(del));
  await waitUntil(() => frames.some((f) => f['writeId'] === del['writeId']));

  const reply = frames.find((f) => f['writeId'] === del['writeId']) as Record<string, unknown>;
  assert.equal(reply['type'], 'ack', 'a delete is an ordinary write, and acks like one');
  assert.equal((await gw.storage.readSnapshot('demo/doomed')).value, null);
});
