import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test, { after } from 'node:test';
import { fileURLToPath } from 'node:url';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';
import { MemoryStorage } from '../../src/storage/memory.ts';
import { signDevToken } from '../../src/gateway/auth.ts';
import * as M from '../../src/gateway/metrics.ts';
import { startGateway, type Gateway } from '../../src/gateway/server.ts';
import { collect, nextFrame, openRaw, waitUntil, wsUrl } from '../helpers.ts';

/**
 * §5.22 Gate F-2 — a console database is a CONNECTION, and the switch happens in one place.
 *
 * Built the same awkward way round as `console-put.test.ts`: the decision under test is lifted out
 * of the SHIPPED page and run against a real gateway, rather than restated here. What the page
 * cannot lend is its socket code, which is DOM-bound — so this file performs the sequence
 * `switchDatabase` performs, and the parts that ARE the page's own (`switchTargetFor`,
 * `dropConnectionState`) are taken verbatim from the file that ships.
 */
const HTML = fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url));

function pageBlock<T>(id: string): T {
  const block = new RegExp(`<script id="${id}">([\\s\\S]*?)</script>`).exec(readFileSync(HTML, 'utf8'));
  assert.ok(block, `the console must keep this logic in a <script id="${id}"> block`);
  const mod = { exports: {} as T };
  new Function('module', block[1] as string)(mod);
  return mod.exports;
}

const wire = pageBlock<{
  buildPut: (p: string, v: unknown) => Record<string, unknown>;
  switchTargetFor: (path: string, currentDb: string | null, declared: Set<string>) => string | null | undefined;
  retireSocket: (ws: { onclose: unknown; onmessage: unknown; close: (code: number, reason: string) => void } | null) => void;
  dropConnectionState: (s: { subs: Map<number, unknown>; watched: Map<string, unknown>; mirror: { dropServerState(): void } }) => void;
  tokenDatabase: (t: string) => string | null;
}>('rtdb-wire');
interface PageMirror {
  applySnapshot(path: string, value: unknown, rev: number): void;
  value(path: string): unknown;
  dropServerState(): void;
}
const { Mirror } = pageBlock<{ Mirror: new () => PageMirror }>('rtdb-mirror');

/** A console session token: no `ns`, exactly what `/login` mints. */
const session = (role = 'owner'): string =>
  signDevToken({ sub: `console-${role === 'viewer' ? '' : 'rw-'}asha`, exp: Math.floor(Date.now() / 1000) + 3600, role });
/** What `POST /wire-token` returns: the SAME subject and role, plus `ns`. Its own teeth are in
 *  `test/unit/console-auth.test.ts`; here it stands for the credential, not for the endpoint. */
const wireToken = (db: string, role = 'owner'): string =>
  signDevToken({
    sub: `console-${role === 'viewer' ? '' : 'rw-'}asha`,
    exp: Math.floor(Date.now() / 1000) + 3600,
    role,
    ns: db,
  });

interface Rig {
  gw: Gateway;
  stores: Map<string, MemoryStorage>;
}

after(() => M.resetSources());

async function rig(): Promise<Rig> {
  const stores = new Map([
    ['car', new MemoryStorage(DEFAULT_LIMITS)],
    ['chat', new MemoryStorage(DEFAULT_LIMITS)],
  ]);
  const base = new MemoryStorage(DEFAULT_LIMITS);
  for (const db of stores.keys()) await base.declareDatabase(db, 'console-rw-owner');
  // A raw top-level namespace in the DEFAULT tenant, never declared — production's `userstatus`,
  // and everything else written before the registry existed. §5.22 Gate F-3's whole subject.
  await base.commitGroup([
    { writeId: crypto.randomUUID(), path: 'userstatus/u1', op: 'put', value: { online: true } },
  ]);
  await (stores.get('chat') as MemoryStorage).commitGroup([
    { writeId: crypto.randomUUID(), path: 'chat/rooms/r1', op: 'put', value: 'hi' },
  ]);
  await (stores.get('car') as MemoryStorage).commitGroup([
    { writeId: crypto.randomUUID(), path: 'car/players/p1', op: 'put', value: { score: 1 } },
  ]);
  const gw = await startGateway({
    storage: base,
    requireNs: true,
    db: 'public',
    tenantStorage: (db) => stores.get(db) as MemoryStorage,
  });
  return { gw, stores };
}

/** Say hello and return the ack, so a test can read the epoch it was given. */
async function hello(port: number, token: string): Promise<{ ws: WebSocket; ack: Record<string, unknown> }> {
  const ws = await openRaw(port);
  const ack = nextFrame<Record<string, unknown>>(ws);
  ws.send(JSON.stringify({ type: 'hello', proto: 1, token }));
  return { ws, ack: await ack };
}

/** Listen and return the snapshot frame for that path. */
async function snapshot(ws: WebSocket, frames: Record<string, unknown>[], path: string, subId: number): Promise<unknown> {
  ws.send(JSON.stringify({ type: 'listen', subId, path }));
  await waitUntil(() => frames.some((f) => f['type'] === 'snapshot' && f['subId'] === subId), `snapshot for ${path}`);
  return (frames.find((f) => f['type'] === 'snapshot' && f['subId'] === subId) as Record<string, unknown>)['value'];
}

const { register } = await import('prom-client');
const scrape = async (db: string): Promise<number | null> => {
  const m = new RegExp(`^rtdb_connections\\{db="${db}"\\} (\\S+)$`, 'm').exec(await register.metrics());
  return m ? Number(m[1]) : null;
};

test('a console database is a connection: the switch, and the bug it replaces', async (t) => {
  M.resetSources();
  const { gw, stores } = await rig();
  t.after(() => gw.close());

  // ---- (a) TODAY'S BUG, by name. The session token carries no `ns`, so this socket is on the
  // DEFAULT tenant — and the sidebar still lists `chat`, because `/topnodes` is the shard's list.
  // Drilling in on this socket reads `chat/rooms` inside the DEFAULT tenant's schema.
  const first = await hello(gw.port, session());
  const firstFrames = collect(first.ws);
  assert.equal(first.ack['type'], 'helloAck', 'a console subject is exempt from requireNs (§5.9)');
  assert.equal(
    await snapshot(first.ws, firstFrames, 'chat/rooms', 1),
    null,
    "the default tenant has nothing at chat/rooms — and chat's own store does",
  );
  assert.deepEqual(
    (await (stores.get('chat') as MemoryStorage).readSnapshot('chat/rooms')).value,
    { r1: 'hi' },
    'the data the console was NOT shown, and got no error about',
  );

  // ---- the page's own decision. `null` is the default tenant, which is not named `chat`.
  const declared = new Set(['car', 'chat']);
  assert.equal(wire.switchTargetFor('chat/rooms', null, declared), 'chat', 'drilling into chat means switching');
  assert.equal(wire.switchTargetFor('chat/rooms', 'chat', declared), undefined, 'and staying does not');

  // ---- (b) the switch: old socket CLOSED first, then a token for chat, then a fresh hello.
  const closed = new Promise<void>((r) => first.ws.addEventListener('close', () => r(), { once: true }));
  first.ws.close(1000, 'database switch');
  await closed;
  await waitUntil(async () => (await scrape('public')) === 0, 'the default tenant is down to zero');

  const second = await hello(gw.port, wireToken('chat'));
  const secondFrames = collect(second.ws);
  assert.equal(second.ack['type'], 'helloAck');
  assert.equal(
    second.ack['epoch'],
    await (stores.get('chat') as MemoryStorage).epoch(),
    "the epoch is CHAT's — one connection, one tenant, one generation (§2)",
  );
  assert.notEqual(
    second.ack['epoch'],
    await (stores.get('car') as MemoryStorage).epoch(),
    'and not the other database\'s, which is why one socket cannot serve two',
  );
  assert.deepEqual(await snapshot(second.ws, secondFrames, 'chat/rooms', 1), { r1: 'hi' }, 'the real chat data');

  // ---- (c) ONE console, ONE connection — and the claim is about the total, not about `chat`.
  // Counting only the new socket would pass just as happily with the old one still open, which is
  // the whole failure: a console left on two databases is counted on both, and both keep serving.
  assert.equal(await scrape('chat'), 1, 'the console is counted on chat');
  assert.equal(await scrape('public'), 0, 'and NOT still on the default tenant it came from');
  assert.equal(await scrape('car'), null, 'and never on a database it did not open');

  // ---- (d) §5.9 is untouched by any of this: the gateway still decides who may write, and it
  // decides it on the tenant the socket landed on. `rules.ts` is 0 lines in this commit.
  const put = wire.buildPut('chat/rooms/r2', 'from the console');
  second.ws.send(JSON.stringify(put));
  await waitUntil(() => secondFrames.some((f) => f['writeId'] === put['writeId']), 'the write was answered');
  assert.equal(
    (secondFrames.find((f) => f['writeId'] === put['writeId']) as Record<string, unknown>)['type'],
    'ack',
    'an editor-capable console session writes',
  );
  assert.equal((await (stores.get('chat') as MemoryStorage).readSnapshot('chat/rooms/r2')).value, 'from the console');
  assert.equal(
    (await (stores.get('car') as MemoryStorage).readSnapshot('chat/rooms/r2')).value,
    null,
    'and it landed in ONE database',
  );
  second.ws.close();
});

test("a viewer's console put on the switched database is refused BY THE GATEWAY", async (t) => {
  // The half of (d) that matters: the switch moved the socket, it did not move the wall. A viewer
  // holding a wire token for `chat` is still a viewer when the frame arrives.
  const { gw, stores } = await rig();
  t.after(() => gw.close());
  const { ws } = await hello(gw.port, wireToken('chat', 'viewer'));
  t.after(() => ws.close());
  const frames = collect(ws);

  const put = wire.buildPut('chat/rooms/r9', 'nope');
  ws.send(JSON.stringify(put));
  await waitUntil(() => frames.some((f) => f['writeId'] === put['writeId']));
  const reply = frames.find((f) => f['writeId'] === put['writeId']) as Record<string, unknown>;
  assert.equal(reply['type'], 'err');
  assert.equal(reply['code'], 'RULES', 'refused by the gateway, not by the page');
  assert.equal((await (stores.get('chat') as MemoryStorage).readSnapshot('chat/rooms/r9')).value, null);
});

test('a wire token stripped of its role is refused at the FIRST listen — F-1 tooth (c)', async (t) => {
  // Checkpoint R1's rule, and the reason `/wire-token` carries the session's role rather than only
  // its `ns`: `outsideOwnDatabase` gives `console-` subjects their exemption, and under
  // `requireNs` that exemption COSTS a known role — a subject prefix is not a credential. Every
  // mint on this shard signs with the same secret, so a token that names a database and no role is
  // exactly the shape that would otherwise walk in.
  //
  // Note where it is refused: not at hello — the token is well-formed and its database is real, so
  // the connection comes up — but at the first thing it asks for.
  const { gw } = await rig();
  t.after(() => gw.close());
  const roleless = signDevToken({
    sub: 'console-rw-asha',
    exp: Math.floor(Date.now() / 1000) + 3600,
    ns: 'chat',
  });
  const { ws, ack } = await hello(gw.port, roleless);
  t.after(() => ws.close());
  assert.equal(ack['type'], 'helloAck', 'the socket comes up; the refusal is not at the door');
  const frames = collect(ws);

  ws.send(JSON.stringify({ type: 'listen', subId: 1, path: 'chat/rooms' }));
  await waitUntil(() => frames.some((f) => f['subId'] === 1), 'the listen was answered');
  const reply = frames.find((f) => f['subId'] === 1) as Record<string, unknown>;
  assert.equal(reply['type'], 'err', 'no snapshot for a roleless console subject');
  assert.equal(reply['code'], 'RULES');

  // And the same token WITH the role reads it, so this is the role and not something else.
  const { ws: ok } = await hello(gw.port, wireToken('chat'));
  t.after(() => ok.close());
  const okFrames = collect(ok);
  assert.deepEqual(await snapshot(ok, okFrames, 'chat/rooms', 1), { r1: 'hi' });
});

test("switching drops the old database's tree, or the console draws it under the new one's name", () => {
  // Tooth (2)'s target, on the page's own function. Both databases are trees of the same shape, so
  // a mirror carried across a switch renders `car/players` while the socket is on `chat` — no
  // error, nothing on the wire, just the wrong data under the right name. And `watched` matters one
  // step later: `helloAck` re-listens everything still in it, which would send `listen car/players`
  // to the chat tenant.
  const mirror = new Mirror();
  mirror.applySnapshot('car/players', { p1: { score: 1 } }, 1);
  assert.deepEqual(mirror.value('car/players'), { p1: { score: 1 } }, 'the old database is on screen');

  const state = { subs: new Map([[1, {}]]), watched: new Map([['car/players', {}]]), mirror };
  wire.dropConnectionState(state);
  assert.equal(mirror.value('car/players'), null, 'and gone after the switch');
  assert.equal(state.subs.size, 0);
  assert.equal(state.watched.size, 0, 'so helloAck cannot re-listen it against the new tenant');
});

test('the console reads its own database out of the token it was given (file://)', async () => {
  // §5.22 Gate F-2 (ii): served, `currentDb` comes from `/wire-token`'s answer. Opened as a file
  // there is no auth-server to ask, and the only thing to read is the pasted token — decoded for
  // display, never verified, because nothing is trusted to it.
  assert.equal(wire.tokenDatabase(wireToken('chat')), 'chat');
  assert.equal(wire.tokenDatabase(session()), null, 'a session token names no database');
  assert.equal(wire.tokenDatabase('not-a-token'), null);
  // Which is exactly what makes the file:// refusal correct: a token for `chat` cannot drill to
  // `car`, and drawing car's paths from chat's store would be the silent version of that.
  assert.equal(wire.switchTargetFor('car/x', wire.tokenDatabase(wireToken('chat')), new Set(['car', 'chat'])), 'car');
});

// -------------------------------------------------------- §5.22 Gate F-3: raw names, and the way back

test('a RAW top-level name belongs to the default tenant, and drilling into it goes back there', async (t) => {
  // The seam Gate F-2 opened. `/topnodes` lists declared UNION derived, so the sidebar shows
  // `userstatus` — production's own namespace, written long before the registry — right beside a
  // real database. Gate F-2 treated every top-level name as a database, so from a connection on
  // `car` that click would have been "stay here and listen `userstatus/u1`" — read inside CAR's
  // schema. The same silent wrong data the gate was built to stop, one database over.
  const { gw, stores } = await rig();
  t.after(() => gw.close());
  const declared = new Set(['car', 'chat']);

  // The decision, on the page's own function. Three answers, and the third is the one that matters.
  assert.equal(wire.switchTargetFor('userstatus/x', 'car', declared), null, 'a raw name is the default tenant');
  assert.equal(wire.switchTargetFor('userstatus/x', null, declared), undefined, 'and from there, stay');
  assert.equal(wire.switchTargetFor('chat/r', null, declared), 'chat', 'a declared name is its own connection');
  assert.equal(wire.switchTargetFor('car/x', 'car', declared), undefined);

  // The list is EMPTY in two real moments — file://, where there is no auth-server to ask, and the
  // instant after a switch, when `helloAck` has fired `loadRoots` and it has not come back. The
  // connection's own database has to count as declared in both, or a drill inside the database we
  // just switched to bounces straight back to the default tenant.
  const unknown = new Set<string>();
  assert.equal(wire.switchTargetFor('chat/rooms', 'chat', unknown), undefined, 'stay inside our own');
  assert.equal(wire.switchTargetFor('car/x', 'chat', unknown), null, 'and anything else is default');
  assert.equal(wire.switchTargetFor('chat/x', null, unknown), undefined, 'from default, with no list, stay');

  // And on the wire: a console on `car` goes back to the default tenant with the SESSION token —
  // nothing is minted, because the default tenant has no registry name to ask for.
  const onCar = await hello(gw.port, wireToken('car'));
  const carFrames = collect(onCar.ws);
  assert.deepEqual(await snapshot(onCar.ws, carFrames, 'car/players', 1), { p1: { score: 1 } });
  assert.equal(
    await snapshot(onCar.ws, carFrames, 'userstatus/u1', 2),
    null,
    "car's schema has no userstatus — which is what the old code would have shown, with no error",
  );

  onCar.ws.close(1000, 'database switch');
  await waitUntil(async () => (await scrape('car')) === 0, 'the car connection is gone');

  const back = await hello(gw.port, session());
  t.after(() => back.ws.close());
  const backFrames = collect(back.ws);
  assert.deepEqual(
    await snapshot(back.ws, backFrames, 'userstatus/u1', 1),
    { online: true },
    'the session token lands on the default tenant, where the raw namespace actually lives',
  );
  assert.equal(await scrape('car'), 0, 'and the console is no longer counted on car');
  assert.equal(await scrape('public'), 1, 'it is counted on the default tenant, once');
});

test('a socket being replaced is retired in one order: onclose, onmessage, then close', () => {
  // Tooth (2)'s other half, and the smallest thing that can hold it. `switchDatabase` is DOM-bound
  // so no test in this repo can run it, but the ORDER is the whole property and it lives in the
  // wire block now: `onclose` is §6's reconnect backoff, so a socket closed with it still attached
  // schedules a re-open of the connection being left — and the console ends up on two databases.
  // `onmessage` next, so a frame already in flight from the old tenant cannot land in the new
  // tenant's mirror.
  const seen: string[] = [];
  const fake = {
    onclose: () => undefined as unknown,
    onmessage: () => undefined as unknown,
    close(code: number, reason: string) {
      // What the world sees at the moment of the close — not what was assigned before or after.
      seen.push(`close(${code},${reason})`);
      seen.push(`onclose=${fake.onclose === null ? 'null' : 'attached'}`);
      seen.push(`onmessage=${fake.onmessage === null ? 'null' : 'attached'}`);
    },
  };
  wire.retireSocket(fake);
  assert.deepEqual(seen, ['close(1000,database switch)', 'onclose=null', 'onmessage=null']);
  assert.equal(fake.onclose, null);
  assert.equal(fake.onmessage, null);

  // And a socket that is already gone is not an error: `switchDatabase` runs on a page whose
  // connection may have died a moment ago, and a throw there would strand the switch half-done.
  wire.retireSocket(null);
  const dead = {
    onclose: null as unknown,
    onmessage: null as unknown,
    close(): void { throw new Error('already closed'); },
  };
  wire.retireSocket(dead);
});
