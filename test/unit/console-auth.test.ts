import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { createServer as netServer } from 'node:net';
import { scryptSync, randomBytes, createHmac, timingSafeEqual } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateDatabaseName } from '../../src/protocol/path.ts';
import { DEFAULT_LIMITS } from '../../src/protocol/limits.ts';

/**
 * The console's front door, exercised as the thing it is: a process, over HTTP, with a fake `aws`
 * on its PATH.
 *
 * It reaches SSM by running the CLI (`execFile('aws', …)`), so a shim on PATH substitutes the whole
 * credential store without one line of the server changing shape for the test. What is exercised is
 * therefore the real routing, the real token, the real authorization — not an extracted copy of the
 * parts that were easy to import.
 *
 * The shim FAILS the way the CLI fails (rc 255, "ParameterNotFound" on stderr) for a parameter it
 * does not hold, because the migration-free fallback to the single-admin record hangs entirely on
 * telling that apart from a denial.
 */
const SERVER = fileURLToPath(new URL('../../console/auth-server.mjs', import.meta.url));
const HTML = fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url));

const SCRYPT = { N: 2 ** 15, r: 8, p: 1, keylen: 64 } as const;
const MAXMEM = 64 * 1024 * 1024;
const hashOf = (password: string, salt: Buffer): string =>
  scryptSync(password, salt, SCRYPT.keylen, { ...SCRYPT, maxmem: MAXMEM }).toString('base64');

const SHIM = `#!/usr/bin/env node
const fs = require('fs'), path = require('path');
const dir = process.env.FAKE_SSM_DIR;
const a = process.argv.slice(2);
const name = a[a.indexOf('--name') + 1];
const file = path.join(dir, encodeURIComponent(name));
if ((process.env.FAKE_SSM_DENY || '').split(',').includes(name)) {
  process.stderr.write('An error occurred (AccessDeniedException) when calling the GetParameter operation: User is not authorized.\\n');
  process.exit(255);
}
if (a[1] === 'get-parameter') {
  if (!fs.existsSync(file)) {
    process.stderr.write('An error occurred (ParameterNotFound) when calling the GetParameter operation: Parameter ' + name + ' not found.\\n');
    process.exit(255);
  }
  process.stdout.write(fs.readFileSync(file, 'utf8') + '\\n');
  process.exit(0);
}
if (a[1] === 'put-parameter') {
  const v = a[a.indexOf('--value') + 1];
  fs.writeFileSync(file, v.startsWith('file://') ? fs.readFileSync(v.slice(7), 'utf8') : v);
  process.stdout.write('1\\n');
  process.exit(0);
}
process.stderr.write('fake aws: unsupported call\\n');
process.exit(2);
`;

interface Rig {
  port: number;
  proc: ReturnType<typeof spawn>;
  log: () => string;
  ssmDir: string;
  ops: Server;
  /** What the stub shard reports as its declared databases. Mutable between calls. */
  shard: { names: string[]; declared: string[]; defaultDb: string };
  dir: string;
}

const freePort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const s = netServer();
    s.on('error', reject);
    s.listen(0, '127.0.0.1', () => {
      const p = (s.address() as { port: number }).port;
      s.close(() => resolve(p));
    });
  });

/**
 * The ops box, stubbed: a Prometheus for /stats AND the gateway admin port behind it.
 *
 * One server for both because the server under test does not take a gateway address — it DISCOVERS
 * one, by asking Prometheus for an active `rtdb-gateway` target and using that target's own
 * `__address__`. Pointing that address back at this same listener is the whole stub: it exercises
 * the real discovery path rather than skipping it, and a caller still cannot name a host, a port or
 * a path (which is the property `/topnodes` and `/app-token` are actually relying on).
 */
function stubOps(port: number, shard: { names: string[]; declared: string[]; defaultDb: string }): Server {
  const s = createServer((req, res) => {
    const url = (req.url ?? '').split('?')[0];
    /**
     * FIRST, because it is the only branch that answers a status of its own — everything below
     * writes a blanket 200 header before it looks at the path.
     *
     * §5.24 Gate D: the stub keeps a REAL registry, so a declare through this rig changes what
     * `/topnodes` answers next; without that, a broken cache invalidation looked exactly like a
     * working one. §5.26: and it REFUSES an illegal name with 400, the way the gateway's admin
     * route does with the shared `validateDatabaseName` — a stub that accepted everything is what
     * let the auth-server's "turn a 400 into a 503" go unnoticed.
     */
    if (req.method === 'POST' && url === '/databases') {
      let body = '';
      req.on('data', (c: Buffer) => { body += c.toString(); });
      req.on('end', () => {
        const name = (JSON.parse(body || '{}') as { name?: string }).name;
        // §5.27: the RULE, not a copy of it. A regex here was a second idea of a legal name living
        // in the rig that checks the first one — and now that the route answers WHICH rule broke,
        // a hand-written stub would also have to invent the sentences, which is the Gate E shape
        // again one layer out.
        const why = validateDatabaseName(name, DEFAULT_LIMITS);
        if (why !== null) {
          res.writeHead(400, { 'content-type': 'application/json' });
          return void res.end(JSON.stringify({ error: why }));
        }
        // `validateDatabaseName` returning null already means a non-empty string; the cast is the
        // gateway route's own (`metrics.ts:782`), not a second opinion about the type.
        const db = name as string;
        if (!shard.declared.includes(db)) {
          shard.declared.push(db);
          shard.names = [...shard.names, db].sort();
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{}');
      });
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    if (url === '/api/v1/targets') {
      return void res.end(JSON.stringify({
        status: 'success',
        data: {
          activeTargets: [{
            labels: { job: 'rtdb-gateway' },
            health: 'up',
            discoveredLabels: { __address__: `127.0.0.1:${port}` },
          }],
        },
      }));
    }
    // §5.22 Gate F-3: the gateway's admin route sends BOTH lists — every top-level name for the
    // sidebar, and the registry alone for the two mints. A stub that only sent `names` would be
    // testing a gateway that no longer exists.
    if (url === '/topnodes') return void res.end(JSON.stringify(shard));
    res.end(JSON.stringify({ status: 'success', data: { result: [{ metric: {}, value: [0, '7'] }] } }));
  });
  s.listen(port, '127.0.0.1');
  return s;
}

async function startServer(
  opts: {
    admin?: unknown; users?: unknown; shadowKey?: string; deny?: string;
    /** DECLARED databases — what the registry holds, and the only names a mint may name. */
    databases?: string[];
    /** Top-level names that have DATA but were never declared: the default tenant's own subtrees. */
    raw?: string[];
    /** §5.25 Gate 4: the ONE websocket origin the CSP allows and the page's box is filled with. */
    wss?: string;
  } = {},
): Promise<Rig> {
  const dir = mkdtempSync(join(tmpdir(), 'console-auth-'));
  const bin = join(dir, 'bin');
  const ssmDir = join(dir, 'ssm');
  mkdirSync(bin);
  mkdirSync(ssmDir);
  writeFileSync(join(bin, 'aws'), SHIM, { mode: 0o755 });

  const put = (name: string, value: unknown): void =>
    writeFileSync(join(ssmDir, encodeURIComponent(name)),
      typeof value === 'string' ? value : JSON.stringify(value));
  put('/rtdb/prod/jwt_secret', 'test-secret-for-console-auth');
  if (opts.admin) put('/rtdb/console/admin', opts.admin);
  if (opts.users) put('/rtdb/console/users', opts.users);
  if (opts.shadowKey) put('/rtdb/console/shadow_key', opts.shadowKey);

  const port = await freePort();
  const promPort = await freePort();
  const declaredNames = opts.databases ?? ['car_race'];
  const shard = {
    names: [...declaredNames, ...(opts.raw ?? [])].sort(),
    declared: declaredNames,
    // §5.24 Gate C: the schema this gateway serves when a token names none.
    defaultDb: 'public',
  };
  const ops = stubOps(promPort, shard);

  const proc = spawn(process.execPath, [SERVER], {
    env: {
      ...process.env,
      PATH: `${bin}:${process.env['PATH'] ?? ''}`,
      FAKE_SSM_DIR: ssmDir,
      FAKE_SSM_DENY: opts.deny ?? '',
      PORT: String(port),
      CONSOLE_HTML: HTML,
      PROM_URL: `http://127.0.0.1:${promPort}`,
      ...(opts.wss ? { CONSOLE_WSS: opts.wss } : {}),
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let out = '';
  proc.stdout?.on('data', (c: Buffer) => { out += c.toString(); });
  proc.stderr?.on('data', (c: Buffer) => { out += c.toString(); });
  const deadline = Date.now() + 10_000;
  while (!out.includes('"listening"')) {
    if (Date.now() > deadline) throw new Error(`server never listened:\n${out}`);
    await new Promise((r) => setTimeout(r, 25));
  }
  return { port, proc, log: () => out, ssmDir, ops, shard, dir };
}

function stop(rig: Rig): void {
  rig.proc.kill('SIGKILL');
  rig.ops.close();
  rmSync(rig.dir, { recursive: true, force: true });
}

const call = async (rig: Rig, path: string, init: RequestInit = {}): Promise<{ status: number; body: Record<string, unknown> }> => {
  const r = await fetch(`http://127.0.0.1:${rig.port}${path}`, init);
  const body = await r.json().catch(() => ({})) as Record<string, unknown>;
  return { status: r.status, body };
};

const login = (rig: Rig, email: string, password: string) =>
  call(rig, '/login', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });

const claims = (token: string): Record<string, unknown> =>
  JSON.parse(Buffer.from(token.split('.')[1] as string, 'base64url').toString('utf8')) as Record<string, unknown>;

const storeIn = (rig: Rig): Record<string, { role: string }> =>
  JSON.parse(readFileSync(join(rig.ssmDir, encodeURIComponent('/rtdb/console/users')), 'utf8')) as Record<string, { role: string }>;

interface Record_ { salt: string; hash: string; params: typeof SCRYPT; role: string }
/** The stored record, whole — a credential is proven by RECOMPUTING it, not by looking at its shape. */
const recordIn = (rig: Rig, email: string): Record_ =>
  (JSON.parse(readFileSync(join(rig.ssmDir, encodeURIComponent('/rtdb/console/users')), 'utf8')) as Record<string, Record_>)[email] as Record_;

const OWNER = { email: 'owner@example.com', password: 'correct horse battery' };
const adminRecord = (): unknown => {
  const salt = randomBytes(32);
  return { email: OWNER.email, salt: salt.toString('base64'), hash: hashOf(OWNER.password, salt), params: SCRYPT };
};

// ---------------------------------------------------------------------------------------------

test('the single-admin record still logs in, and comes back as the owner (no migration step)', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const bad = await login(rig, OWNER.email, 'wrong password entirely');
    assert.equal(bad.status, 401);

    const ok = await login(rig, OWNER.email, OWNER.password);
    assert.equal(ok.status, 200);
    assert.equal(ok.body['role'], 'owner');
    assert.equal(claims(ok.body['token'] as string)['role'], 'owner');
    // §5.9 Gate B: an owner may write, so their session is minted on the write-capable subject.
    // This expectation changed deliberately with the unlock — the one test edit in the package.
    assert.equal(claims(ok.body['token'] as string)['sub'], 'console-rw-owner');
    // The store parameter must NOT have been created by a mere login.
    assert.equal(existsSync(join(rig.ssmDir, encodeURIComponent('/rtdb/console/users'))), false);
  } finally { stop(rig); }
});

test('a token with no role claim is refused by /stats — a shadow token cannot read the fleet', async () => {
  const rig = await startServer({ admin: adminRecord(), shadowKey: 'a-shadow-key' });
  try {
    const shadow = await call(rig, '/shadow-token', {
      method: 'POST',
      headers: { authorization: 'Bearer a-shadow-key', 'content-type': 'application/json' },
      body: JSON.stringify({ device: 'testdevice' }),
    });
    assert.equal(shadow.status, 200);
    const token = shadow.body['token'] as string;
    assert.equal(claims(token)['role'], undefined, 'a device token must carry no console role');

    const denied = await call(rig, '/stats', { headers: { authorization: `Bearer ${token}` } });
    assert.equal(denied.status, 401, 'a roleless token must not reach /stats');

    const owner = await login(rig, OWNER.email, OWNER.password);
    const allowed = await call(rig, '/stats', { headers: { authorization: `Bearer ${owner.body['token'] as string}` } });
    assert.equal(allowed.status, 200);
  } finally { stop(rig); }
});

test('owner adds a viewer; the viewer signs in, reads stats, and is refused user management', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    const ownerToken = owner.body['token'] as string;

    const listed = await call(rig, '/users', { headers: { authorization: `Bearer ${ownerToken}` } });
    assert.equal(listed.status, 200);
    assert.deepEqual(listed.body['users'], [{ email: OWNER.email, role: 'owner' }]);

    const added = await call(rig, '/users', {
      method: 'POST',
      headers: { authorization: `Bearer ${ownerToken}`, 'content-type': 'application/json' },
      body: JSON.stringify({ email: 'asha@example.com', role: 'viewer' }),
    });
    assert.equal(added.status, 200);
    const password = added.body['password'] as string;
    assert.ok(password && password.length >= 20, 'a generated password comes back exactly once');

    // The owner was carried into the store it just created — not left behind in the old parameter.
    assert.deepEqual(Object.keys(storeIn(rig)).sort(), ['asha@example.com', OWNER.email]);

    const viewer = await login(rig, 'asha@example.com', password);
    assert.equal(viewer.status, 200);
    assert.equal(viewer.body['role'], 'viewer');
    const viewerToken = viewer.body['token'] as string;
    assert.equal(claims(viewerToken)['role'], 'viewer');

    // Sees the data...
    const stats = await call(rig, '/stats', { headers: { authorization: `Bearer ${viewerToken}` } });
    assert.equal(stats.status, 200);

    // ...and cannot manage users, by any method.
    for (const method of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']) {
      const init: RequestInit = { method, headers: { authorization: `Bearer ${viewerToken}`, 'content-type': 'application/json' } };
      if (method !== 'GET') init.body = JSON.stringify({ email: 'x@example.com', role: 'owner' });
      const r = await call(rig, '/users', init);
      assert.equal(r.status, 403, `${method} /users must be refused for a viewer`);
    }
    assert.deepEqual(Object.keys(storeIn(rig)).sort(), ['asha@example.com', OWNER.email], 'a refused call writes nothing');

    const anon = await call(rig, '/users');
    assert.equal(anon.status, 401);

    // The journal names the action and never the password.
    assert.match(rig.log(), /"event":"users.add"[^\n]*asha@example.com/);
    assert.ok(!rig.log().includes(password), 'a generated password must never reach the journal');
  } finally { stop(rig); }
});

test('the console cannot be left without an owner, and nobody can demote themselves', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const a = await login(rig, OWNER.email, OWNER.password);
    const aToken = a.body['token'] as string;
    const auth = (t: string) => ({ authorization: `Bearer ${t}`, 'content-type': 'application/json' });

    const added = await call(rig, '/users', {
      method: 'POST', headers: auth(aToken),
      body: JSON.stringify({ email: 'bee@example.com', role: 'owner' }),
    });
    assert.equal(added.status, 200);
    const b = await login(rig, 'bee@example.com', added.body['password'] as string);
    const bToken = b.body['token'] as string;

    const selfDemote = await call(rig, '/users', {
      method: 'PATCH', headers: auth(aToken),
      body: JSON.stringify({ email: OWNER.email, role: 'viewer' }),
    });
    assert.equal(selfDemote.status, 409, 'an owner may not demote themselves');

    const selfRemove = await call(rig, '/users', {
      method: 'DELETE', headers: auth(aToken), body: JSON.stringify({ email: OWNER.email }),
    });
    assert.equal(selfRemove.status, 409, 'an owner may not remove themselves');

    // A removed owner's token stays valid for its hour — that is what §10's kick is for. Which means
    // the last-owner guard is reachable: B, already removed, still holds an owner token.
    const removeB = await call(rig, '/users', {
      method: 'DELETE', headers: auth(aToken), body: JSON.stringify({ email: 'bee@example.com' }),
    });
    assert.equal(removeB.status, 200);

    const orphan = await call(rig, '/users', {
      method: 'PATCH', headers: auth(bToken),
      body: JSON.stringify({ email: OWNER.email, role: 'viewer' }),
    });
    assert.equal(orphan.status, 409, 'the last owner may not be demoted by a token that outlived its account');
    assert.match(String(orphan.body['error']), /no owner/);
    assert.equal(storeIn(rig)[OWNER.email]?.role, 'owner');
  } finally { stop(rig); }
});

test('two addresses cannot share the part before the @ — the token subject is named after it', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    const headers = { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' };

    const clash = await call(rig, '/users', {
      method: 'POST', headers, body: JSON.stringify({ email: 'owner@other.example', role: 'viewer' }),
    });
    assert.equal(clash.status, 409);
    assert.match(String(clash.body['error']), /before the @/);

    const fine = await call(rig, '/users', {
      method: 'POST', headers, body: JSON.stringify({ email: 'other@example.com', role: 'editor' }),
    });
    assert.equal(fine.status, 200);
  } finally { stop(rig); }
});

test('editor and owner are minted the write-capable subject; a viewer is not', async (t) => {
  // §5.9's deliberate unlock. The gateway demands this subject AND the role, so the mint is the
  // only place the two can be paired.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    assert.equal(claims(owner.body['token'] as string)['sub'], 'console-rw-owner');
    const headers = { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' };

    const added = await call(rig, '/users', {
      method: 'POST', headers, body: JSON.stringify({ email: 'seer@example.com', role: 'viewer' }),
    });
    const viewer = await login(rig, 'seer@example.com', added.body['password'] as string);
    assert.equal(claims(viewer.body['token'] as string)['sub'], 'console-seer', 'a viewer keeps the read-only subject');

    const ed = await call(rig, '/users', {
      method: 'POST', headers, body: JSON.stringify({ email: 'maker@example.com', role: 'editor' }),
    });
    const editor = await login(rig, 'maker@example.com', ed.body['password'] as string);
    assert.equal(claims(editor.body['token'] as string)['sub'], 'console-rw-maker');
    assert.equal(claims(editor.body['token'] as string)['role'], 'editor');
  } finally { stop(rig); }
});

test('the self-guards still know who you are once your subject says rw', async (t) => {
  // The regression the mint change causes if `isSelf` rebuilds the subject instead of stripping it:
  // an owner's own subject is console-rw-<localpart>, so a comparison against console-<localpart>
  // answers "not you" and the guards silently stop protecting the people who have them.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    assert.equal(claims(owner.body['token'] as string)['sub'], 'console-rw-owner');
    const headers = { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' };

    const demote = await call(rig, '/users', {
      method: 'PATCH', headers, body: JSON.stringify({ email: OWNER.email, role: 'viewer' }),
    });
    assert.equal(demote.status, 409, 'an owner on an rw subject still may not demote themselves');

    const remove = await call(rig, '/users', {
      method: 'DELETE', headers, body: JSON.stringify({ email: OWNER.email }),
    });
    assert.equal(remove.status, 409, 'nor remove themselves');
  } finally { stop(rig); }
});

test('a removal names BOTH subjects an operator might have to kick', async (t) => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    const headers = { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' };
    await call(rig, '/users', { method: 'POST', headers, body: JSON.stringify({ email: 'gone@example.com', role: 'editor' }) });
    await call(rig, '/users', { method: 'DELETE', headers, body: JSON.stringify({ email: 'gone@example.com' }) });

    const line = rig.log().split('\n').filter((l) => l.includes('users.remove')).pop() as string;
    const ev = JSON.parse(line) as { kickHints?: string[] };
    // A removed editor can hold a live rw session AND an older read-only one from before promotion.
    assert.deepEqual(ev.kickHints, ['console-gone', 'console-rw-gone']);
  } finally { stop(rig); }
});

test('the store has a stated ceiling, and says so by the numbers', async () => {
  // A 4 KB standard parameter holds about thirteen records. The one that would cross it must be
  // refused with a number the owner can act on, not by the CLI failing somewhere underneath.
  const salt = randomBytes(32);
  const store: Record<string, unknown> = {
    [OWNER.email]: { salt: salt.toString('base64'), hash: hashOf(OWNER.password, salt), params: SCRYPT, role: 'owner', setAt: new Date().toISOString(), setBy: 'console-owner' },
  };
  for (let i = 0; i < 12; i++) {
    store[`filler${i}@example.com`] = {
      salt: randomBytes(32).toString('base64'), hash: randomBytes(64).toString('base64'),
      params: SCRYPT, role: 'viewer', setAt: new Date().toISOString(), setBy: 'console-owner',
    };
  }
  assert.ok(JSON.stringify(store).length < 4096, 'the fixture must start UNDER the limit, or it proves nothing');

  const rig = await startServer({ admin: adminRecord(), users: store });
  try {
    const owner = await login(rig, OWNER.email, OWNER.password);
    assert.equal(owner.status, 200);
    const full = await call(rig, '/users', {
      method: 'POST',
      headers: { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' },
      body: JSON.stringify({ email: 'onetoomany@example.com', role: 'viewer' }),
    });
    assert.equal(full.status, 409);
    assert.match(String(full.body['error']), /full at 14 users/);
    assert.match(String(full.body['error']), /advanced tier/);
    // And nothing was written: the store is still the thirteen it was.
    assert.equal(Object.keys(storeIn(rig)).length, 13);
  } finally { stop(rig); }
});

test('a DENIED user store is not an absent one — the legacy record must not resurrect an account', async () => {
  // The store exists and no longer contains the legacy owner. Reading it is refused. Falling back to
  // `/rtdb/console/admin` here would sign in a user the store has removed, which is the whole reason
  // only ParameterNotFound is treated as "absent".
  const salt = randomBytes(32);
  const rig = await startServer({
    admin: adminRecord(),
    users: { 'bee@example.com': { salt: salt.toString('base64'), hash: hashOf('some other password', salt), params: SCRYPT, role: 'owner' } },
    deny: '/rtdb/console/users',
  });
  try {
    const r = await login(rig, OWNER.email, OWNER.password);
    assert.equal(r.status, 503, 'a denied read must fail, not fall back');
    assert.match(rig.log(), /login.ssm_error/);
  } finally { stop(rig); }
});

test('a credential store that is unreachable is not an empty one', async () => {
  // No admin record and no user store: every parameter read fails as ParameterNotFound, which is
  // "there is nobody", not "everybody is welcome".
  const rig = await startServer({});
  try {
    const r = await login(rig, OWNER.email, OWNER.password);
    assert.equal(r.status, 503);
  } finally { stop(rig); }
});

// --------------------------------------------------------------- v2: chosen passwords and resets

/** Distinctive on purpose: t4 greps the whole journal for this literal, and a common word would make
 *  its absence meaningless. */
const CHOSEN = 'chosen-tooth-passphrase-7719';
const RESET = 'reset-tooth-passphrase-4402';

const ownerHeaders = async (rig: Rig): Promise<Record<string, string>> => {
  const owner = await login(rig, OWNER.email, OWNER.password);
  assert.equal(owner.status, 200);
  return { authorization: `Bearer ${owner.body['token'] as string}`, 'content-type': 'application/json' };
};

test('the owner chooses the new user password, and what is STORED is scrypt of exactly that', async () => {
  // THE TOOTH for the add half. Not "a hash was written" — the hash is recomputed here from the
  // password the owner typed and the salt the server chose, and it must be the stored bytes. A
  // server that stored the plaintext, or hashed something else, is red on the same line.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    const added = await call(rig, '/users', {
      method: 'POST', headers,
      body: JSON.stringify({ email: 'chooser@example.com', role: 'editor', password: CHOSEN }),
    });
    assert.equal(added.status, 200);
    assert.equal(added.body['password'], undefined, 'a password the owner typed is not echoed back');

    const rec = recordIn(rig, 'chooser@example.com');
    assert.equal(rec.hash, hashOf(CHOSEN, Buffer.from(rec.salt, 'base64')),
      'the stored hash must BE scrypt(the chosen password, the stored salt)');
    assert.ok(!JSON.stringify(rec).includes(CHOSEN), 'the plaintext must not be in the record');

    // And the materialised store is a store you can sign in against.
    const back = await login(rig, 'chooser@example.com', CHOSEN);
    assert.equal(back.status, 200);
    assert.equal(back.body['role'], 'editor');
    assert.equal(claims(back.body['token'] as string)['sub'], 'console-rw-chooser');
  } finally { stop(rig); }
});

test('a reset replaces the password: the old one stops working, the new one signs in', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    await call(rig, '/users', {
      method: 'POST', headers,
      body: JSON.stringify({ email: 'forgot@example.com', role: 'viewer', password: CHOSEN }),
    });
    assert.equal((await login(rig, 'forgot@example.com', CHOSEN)).status, 200);

    const reset = await call(rig, '/users', {
      method: 'PUT', headers, body: JSON.stringify({ email: 'forgot@example.com', password: RESET }),
    });
    assert.equal(reset.status, 200);
    assert.equal(reset.body['passwordSet'], true);

    assert.equal((await login(rig, 'forgot@example.com', CHOSEN)).status, 401, 'the old password is gone');
    const fresh = await login(rig, 'forgot@example.com', RESET);
    assert.equal(fresh.status, 200);
    assert.equal(fresh.body['role'], 'viewer', 'a reset changes the password and nothing else');

    const rec = recordIn(rig, 'forgot@example.com');
    assert.equal(rec.hash, hashOf(RESET, Buffer.from(rec.salt, 'base64')));
    assert.equal(rec.role, 'viewer');

    // A reset of somebody who is not there is a 404, not a silent create.
    const ghost = await call(rig, '/users', {
      method: 'PUT', headers, body: JSON.stringify({ email: 'ghost@example.com', password: RESET }),
    });
    assert.equal(ghost.status, 404);
  } finally { stop(rig); }
});

test('a password under the floor is refused — at add and at reset — and nothing is written', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    await call(rig, '/users', {
      method: 'POST', headers,
      body: JSON.stringify({ email: 'floor@example.com', role: 'viewer', password: CHOSEN }),
    });
    const before = recordIn(rig, 'floor@example.com').hash;

    for (const password of ['', '  ', 'short12', '        ']) {
      const add = await call(rig, '/users', {
        method: 'POST', headers, body: JSON.stringify({ email: 'new@example.com', role: 'viewer', password }),
      });
      assert.equal(add.status, 400, `add must refuse ${JSON.stringify(password)}`);
      assert.match(String(add.body['error']), /password/);

      const reset = await call(rig, '/users', {
        method: 'PUT', headers, body: JSON.stringify({ email: 'floor@example.com', password }),
      });
      assert.equal(reset.status, 400, `reset must refuse ${JSON.stringify(password)}`);
    }
    // Eight characters is the floor, not the wall: the shortest acceptable one is accepted.
    const edge = await call(rig, '/users', {
      method: 'PUT', headers, body: JSON.stringify({ email: 'floor@example.com', password: 'exactly8' }),
    });
    assert.equal(edge.status, 200);

    assert.deepEqual(Object.keys(storeIn(rig)).sort(), ['floor@example.com', OWNER.email],
      'a refused password creates no user');
    assert.notEqual(recordIn(rig, 'floor@example.com').hash, before, 'and the accepted one did land');
  } finally { stop(rig); }
});

test('only an owner may reset a password — an editor holding a write-capable token cannot', async () => {
  // THE ROLE TOOTH. An editor gets `console-rw-<localpart>`, the same subject shape an owner gets, so
  // a gate that keyed off the SUBJECT rather than the role claim would let this through.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    const added = await call(rig, '/users', {
      method: 'POST', headers,
      body: JSON.stringify({ email: 'maker@example.com', role: 'editor', password: CHOSEN }),
    });
    assert.equal(added.status, 200);
    const editor = await login(rig, 'maker@example.com', CHOSEN);
    assert.equal(claims(editor.body['token'] as string)['sub'], 'console-rw-maker');

    const attempt = await call(rig, '/users', {
      method: 'PUT',
      headers: { authorization: `Bearer ${editor.body['token'] as string}`, 'content-type': 'application/json' },
      body: JSON.stringify({ email: OWNER.email, password: RESET }),
    });
    assert.equal(attempt.status, 403, 'an editor may not reset anyone, including themselves');

    // Their own password is untouched, which is what "writes nothing" has to mean here.
    assert.equal((await login(rig, 'maker@example.com', CHOSEN)).status, 200);
    const anon = await call(rig, '/users', {
      method: 'PUT', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email: OWNER.email, password: RESET }),
    });
    assert.equal(anon.status, 401);
  } finally { stop(rig); }
});

test('a reset is journalled by email, never by password, and names the sessions to kick', async () => {
  // THE JOURNAL TOOTH, with its own control: the email MUST be found, so "the password was not
  // found" cannot pass by the grep having looked at nothing.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    await call(rig, '/users', {
      method: 'POST', headers,
      body: JSON.stringify({ email: 'noted@example.com', role: 'editor', password: CHOSEN }),
    });
    await call(rig, '/users', {
      method: 'PUT', headers, body: JSON.stringify({ email: 'noted@example.com', password: RESET }),
    });

    const line = rig.log().split('\n').filter((l) => l.includes('users.password')).pop() as string;
    assert.ok(line, 'a reset must leave a journal line');
    const ev = JSON.parse(line) as { email?: string; actor?: string; kickHints?: string[]; sessionValidForUpToMinutes?: number };
    assert.equal(ev.email, 'noted@example.com');            // the control that must hit
    assert.equal(ev.actor, 'console-rw-owner');
    // §5.8's accepted residual: the member's token outlives the reset, so the operator is handed the
    // two subjects a kick would have to name — the same pair a removal reports.
    assert.deepEqual(ev.kickHints, ['console-noted', 'console-rw-noted']);
    assert.equal(ev.sessionValidForUpToMinutes, 60);

    assert.ok(!rig.log().includes(RESET), 'the new password must never reach the journal');
    assert.ok(!rig.log().includes(CHOSEN), 'nor the one it replaced');
  } finally { stop(rig); }
});

test('an owner may reset their OWN password — resetting is not demoting (§5.8 guards intact)', async () => {
  // The legacy single-admin record, with no user store yet: the reset is what materialises it.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const headers = await ownerHeaders(rig);
    const self = await call(rig, '/users', {
      method: 'PUT', headers, body: JSON.stringify({ email: OWNER.email, password: RESET }),
    });
    assert.equal(self.status, 200, 'an owner may set their own password');
    assert.equal((await login(rig, OWNER.email, OWNER.password)).status, 401);
    const again = await login(rig, OWNER.email, RESET);
    assert.equal(again.status, 200);
    assert.equal(again.body['role'], 'owner', 'still the owner');

    // And the guards the reset must NOT have loosened, on a fresh token.
    const fresh = { authorization: `Bearer ${again.body['token'] as string}`, 'content-type': 'application/json' };
    const demote = await call(rig, '/users', {
      method: 'PATCH', headers: fresh, body: JSON.stringify({ email: OWNER.email, role: 'viewer' }),
    });
    assert.equal(demote.status, 409, 'self-demote is still refused');
    const remove = await call(rig, '/users', {
      method: 'DELETE', headers: fresh, body: JSON.stringify({ email: OWNER.email }),
    });
    assert.equal(remove.status, 409, 'self-removal is still refused');
    assert.equal(storeIn(rig)[OWNER.email]?.role, 'owner');
  } finally { stop(rig); }
});

// ------------------------------------------------------------------- §5.20 Phase 1: /app-token

/**
 * The mint the client's own console uses to hand a database to their dev team. If this endpoint
 * does not exist, "the client gets a console" stops being true — they would need a backend of their
 * own to issue their team a credential.
 *
 * These run against the real process with the real discovery path, like everything else in this
 * file: the shard's declared databases come back through the stubbed Prometheus target, not through
 * a seam opened for the test.
 */
const appToken = (rig: Rig, token: string, body: unknown) =>
  call(rig, '/app-token', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
  });

/** The signature, recomputed. A token is proven by verifying it, not by reading its middle third. */
const signedWith = (token: string, secret: string): boolean => {
  const [head, body, mac] = token.split('.') as [string, string, string];
  const expected = createHmac('sha256', secret).update(`${head}.${body}`).digest();
  const got = Buffer.from(mac, 'base64url');
  return got.length === expected.length && timingSafeEqual(got, expected);
};

test('an owner mints an app token confined to one database, and the gateway can verify it', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['car_race', 'chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await appToken(rig, session['token'] as string, { database: 'car_race', app: 'android' });
    assert.equal(r.status, 201);
    assert.equal(r.body['sub'], 'app-car_race-android');
    assert.equal(r.body['database'], 'car_race');

    const c = claims(r.body['token'] as string);
    assert.equal(c['ns'], 'car_race', 'the claim the gateway keys the wall off');
    assert.equal(c['sub'], 'app-car_race-android');
    assert.equal('role' in c, false, 'an app token is not a console session and must not read like one');
    assert.equal(typeof c['exp'], 'number', 'expiry is half of revocation; the other half is §10 kick');

    // Signed with the SHARD secret, which is the point: the console signs what the gateway verifies.
    assert.equal(signedWith(r.body['token'] as string, 'test-secret-for-console-auth'), true);
  } finally {
    stop(rig);
  }
});

test('the app token subject is DERIVED, never taken from the caller', async () => {
  // This is the root of the whole wall. `outsideOwnDatabase` and `consoleWriteDenied` both exempt
  // `console-` subjects, so a caller who could choose its own `sub` would pick one and walk out.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await appToken(rig, session['token'] as string, {
      database: 'car_race',
      app: 'android',
      sub: 'console-rw-evil',
    });
    assert.equal(r.status, 201);
    assert.equal(r.body['sub'], 'app-car_race-android', 'the body sub is ignored, not honoured');
    assert.equal(claims(r.body['token'] as string)['sub'], 'app-car_race-android');
  } finally {
    stop(rig);
  }
});

test('a hostile app name cannot escape the subject shape', async () => {
  // The app name reaches a token subject, and §10's kick targets that string. `deviceSlug` is the
  // same squeeze /shadow-token already applies to a device id.
  const rig = await startServer({ admin: adminRecord() });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await appToken(rig, session['token'] as string, { database: 'car_race', app: 'console-rw-x/../y' });
    assert.equal(r.body['sub'], 'app-car_race-consolerwxy');
    // And an app name that squeezes down to nothing still yields a usable subject.
    const bare = await appToken(rig, session['token'] as string, { database: 'car_race', app: '///' });
    assert.equal(bare.body['sub'], 'app-car_race-app');
  } finally {
    stop(rig);
  }
});

test('minting for a database the shard has never seen is refused', async () => {
  // Where "only the owner creates databases" is actually enforced: RuleCtx is sync and has no
  // storage, so the rule cannot refuse an undeclared database. This can.
  const rig = await startServer({ admin: adminRecord(), databases: ['car_race'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await appToken(rig, session['token'] as string, { database: 'not_a_database' });
    assert.equal(r.status, 400);
    assert.match(String(r.body['error']), /no such database/);
  } finally {
    stop(rig);
  }
});

test('a viewer cannot mint an app token, and neither can a caller with no token', async () => {
  // Handing out a credential is the owner's act, exactly like POST /databases. A REAL viewer login,
  // not a hand-shaped token: the thing being tested is the role gate, and a forged token would be
  // refused by the signature check before ever reaching it.
  const VIEWER = { email: 'viewer@example.com', password: 'a completely different one' };
  const salt = randomBytes(32);
  const rig = await startServer({
    users: {
      [OWNER.email]: { salt: randomBytes(32).toString('base64'), hash: '', params: SCRYPT, role: 'owner' },
      [VIEWER.email]: { salt: salt.toString('base64'), hash: hashOf(VIEWER.password, salt), params: SCRYPT, role: 'viewer' },
    },
  });
  try {
    const { body: session } = await login(rig, VIEWER.email, VIEWER.password);
    assert.equal(session['role'], 'viewer', 'the viewer really did log in');
    assert.equal((await appToken(rig, session['token'] as string, { database: 'car_race' })).status, 401);
    assert.equal((await appToken(rig, 'not-a-token', { database: 'car_race' })).status, 401);
    assert.equal(
      (await call(rig, '/app-token', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ database: 'car_race' }),
      })).status,
      401,
      'no Authorization header at all',
    );
  } finally {
    stop(rig);
  }
});

test('a request with no database named is a 400, not a token for nothing', async () => {
  const rig = await startServer({ admin: adminRecord() });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    assert.equal((await appToken(rig, session['token'] as string, {})).status, 400);
    assert.equal((await appToken(rig, session['token'] as string, { database: '' })).status, 400);
  } finally {
    stop(rig);
  }
});

// ------------------------------------------------------------------ §5.22 Gate F-1: /wire-token

/**
 * The console's own wire credential, for ONE database.
 *
 * Unlike /app-token this is not the owner handing a database to somebody else — it is the console
 * saying which database the operator is looking at right now, so a VIEWER may mint one. What it may
 * not do is invent a database, or hand a credential to a caller with no console session.
 */
const wireToken = (rig: Rig, token: string | null, body: unknown) =>
  call(rig, '/wire-token', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token === null ? {} : { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(body),
  });

test('a console session mints a wire token for one database, keeping its own subject and role', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['car_race', 'chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await wireToken(rig, session['token'] as string, { database: 'chat' });
    assert.equal(r.status, 201);
    assert.equal(r.body['database'], 'chat');

    const c = claims(r.body['token'] as string);
    assert.equal(c['ns'], 'chat', 'the claim `server.ts` chooses the tenant from, at hello');
    // The subject and the role are the SESSION's, and that is the whole point: §10 kicks by subject
    // and every audit line names one, so a per-database subject would split both silently.
    assert.equal(c['sub'], session['sub'], 'same subject as the session that asked');
    assert.equal(c['role'], session['role'], 'and the same role — the console-exemption costs one');
    assert.match(String(c['sub']), /^console-/);
    assert.equal(signedWith(r.body['token'] as string, 'test-secret-for-console-auth'), true);
  } finally {
    stop(rig);
  }
});

test('a viewer MAY mint a wire token — reading one database is what a viewer does', async () => {
  // The difference from /app-token, stated as a test rather than left as a comment: that endpoint
  // hands a credential to a third party and is the owner's act; this one moves the operator's own
  // session onto one database. §5.9's `consoleWriteDenied` is what stops a viewer writing with it.
  const VIEWER = { email: 'viewer@example.com', password: 'a completely different one' };
  const salt = randomBytes(32);
  const rig = await startServer({
    databases: ['chat'],
    users: {
      [OWNER.email]: { salt: randomBytes(32).toString('base64'), hash: '', params: SCRYPT, role: 'owner' },
      [VIEWER.email]: { salt: salt.toString('base64'), hash: hashOf(VIEWER.password, salt), params: SCRYPT, role: 'viewer' },
    },
  });
  try {
    const { body: session } = await login(rig, VIEWER.email, VIEWER.password);
    assert.equal(session['role'], 'viewer', 'the viewer really did log in');
    const r = await wireToken(rig, session['token'] as string, { database: 'chat' });
    assert.equal(r.status, 201);
    assert.equal(claims(r.body['token'] as string)['role'], 'viewer', 'minted AS a viewer, not upgraded');
  } finally {
    stop(rig);
  }
});

test('no console session mints no wire token — tooth (a)', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'], shadowKey: 'a-shadow-key' });
  try {
    assert.equal((await wireToken(rig, null, { database: 'chat' })).status, 401, 'no Authorization header');
    assert.equal((await wireToken(rig, 'not-a-token', { database: 'chat' })).status, 401, 'a forged one');
    // And the shape that actually happened on this server once: a token signed with the SAME shard
    // secret that carries no role. A device's shadow token is exactly that, and a subject prefix is
    // not a credential — `consoleUser` demanding a KNOWN role is what refuses it.
    const shadow = await call(rig, '/shadow-token', {
      method: 'POST',
      headers: { authorization: 'Bearer a-shadow-key', 'content-type': 'application/json' },
      body: JSON.stringify({ device: 'testdevice' }),
    });
    assert.equal(claims(shadow.body['token'] as string)['role'], undefined, 'really roleless');
    assert.equal(
      (await wireToken(rig, shadow.body['token'] as string, { database: 'chat' })).status,
      401,
      'signed with the shard secret, but roleless',
    );
  } finally {
    stop(rig);
  }
});

test('minting a wire token for an undeclared database is refused, and nothing is minted — tooth (b)', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await wireToken(rig, session['token'] as string, { database: 'not_a_database' });
    assert.equal(r.status, 400);
    assert.match(String(r.body['error']), /no such database/);
    assert.equal('token' in r.body, false, 'and no credential came back with the refusal');
    // The same refusal for a missing body, so "no database" cannot fall through to the default one.
    assert.equal((await wireToken(rig, session['token'] as string, {})).status, 400);
  } finally {
    stop(rig);
  }
});

// ----------------------------------------------------- §5.22 Gate F-3: DECLARED, never the union

/**
 * The seam Gate F opened and Gate D made expensive.
 *
 * `/topnodes` answers with declared UNION derived, because a sidebar must not hide a namespace an
 * operator holds. Both mints were checking THAT list — so a name that exists only as DATA in the
 * default tenant's schema passed, and since Gate D the gateway refuses an undeclared `ns` at hello.
 * The token minted was one nobody could ever connect with: a 400 turned into a 4401 ten seconds
 * later, and for /app-token the dead credential had already been handed to somebody else.
 *
 * `userstatus` is not a hypothetical. It is production's own top-level namespace today, written
 * long before the registry existed, and it is exactly what the sidebar shows beside a real database.
 */
test('neither mint will name a database that only exists as DATA — tooth: check on declared', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'], raw: ['userstatus'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const token = session['token'] as string;

    // The sidebar sees both — that is its job, and it is why the mints cannot use its list.
    const sidebar = await call(rig, '/topnodes', { headers: { authorization: `Bearer ${token}` } });
    assert.deepEqual(sidebar.body['names'], ['chat', 'userstatus'], 'both are top-level names');
    assert.deepEqual(sidebar.body['declared'], ['chat'], 'only one is a database');

    for (const [what, r] of [
      ['wire', await wireToken(rig, token, { database: 'userstatus' })],
      ['app', await appToken(rig, token, { database: 'userstatus', app: 'android' })],
    ] as const) {
      assert.equal(r.status, 400, `${what}: a raw namespace is not a database`);
      assert.match(String(r.body['error']), /no such database/, what);
      assert.equal('token' in r.body, false, `${what}: and nothing was minted`);
    }

    // The declared one still mints from both, so this is the registry and not a blanket refusal.
    assert.equal((await wireToken(rig, token, { database: 'chat' })).status, 201);
    assert.equal((await appToken(rig, token, { database: 'chat', app: 'android' })).status, 201);
  } finally {
    stop(rig);
  }
});

test('a gateway that does not send `declared` mints nothing, rather than minting on the union', async () => {
  // The rolling-deploy direction. An older gateway answers `/topnodes` with `names` only, so
  // `declared` reads as empty and both mints refuse — a 400 the operator can read, rather than a
  // token that fails at hello ten seconds later on a shard they cannot see.
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'] });
  try {
    rig.shard.declared = [] as unknown as string[];
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const r = await wireToken(rig, session['token'] as string, { database: 'chat' });
    assert.equal(r.status, 400, 'fails closed, and says which way');
    assert.match(String(r.body['error']), /no such database/);
  } finally {
    stop(rig);
  }
});

// ------------------------------------------------------- §5.24 Gate B: GET /usage?db=

/**
 * One database's usage line. The shape is `/stats`': a FIXED list of queries, none of which a
 * caller can name, influence or add to. The one thing a caller supplies is the database — and it
 * is checked against the shard's REGISTRY before it is interpolated into anything, which is the
 * whole reason a caller-supplied value is safe on this endpoint.
 */
const usage = (rig: Rig, token: string | null, db: string) =>
  call(rig, `/usage?db=${encodeURIComponent(db)}`, {
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
  });

test('a console session reads one database\'s usage, and only declared names are answered', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'], raw: ['userstatus'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const token = session['token'] as string;

    const ok = await usage(rig, token, 'chat');
    assert.equal(ok.status, 200);
    assert.equal(ok.body['database'], 'chat');
    for (const line of ['connections', 'storageBytes', 'downloadsPerSec', 'load', 'quotaRejectedPerSec', 'shardLoad']) {
      assert.ok(line in ok.body, `the tile's ${line} line is answered`);
    }

    // REGISTRY, not the union — same rule as the two mints (§5.22 Gate F-3). `userstatus` is a raw
    // namespace of the default tenant: real data, never a database anyone was handed, and the panel
    // bills per database. It is also what keeps a second shard's schemas out of the answer when two
    // shards share one Postgres database, since `storageBytes` sizes every `nodes` relation it sees.
    const raw = await usage(rig, token, 'userstatus');
    assert.equal(raw.status, 400);
    assert.match(String(raw.body['error']), /no such database/);
    assert.equal('load' in raw.body, false, 'and no numbers came back with the refusal');

    // A name that is nothing at all, and the one shape a query-string endpoint must not pass on.
    assert.equal((await usage(rig, token, '')).status, 400);
    assert.equal((await usage(rig, token, 'chat"} or rtdb_connections{')).status, 400, 'not a selector');
  } finally {
    stop(rig);
  }
});

test('usage is not readable without a console session', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'], shadowKey: 'a-shadow-key' });
  try {
    assert.equal((await usage(rig, null, 'chat')).status, 401, 'no Authorization header');
    assert.equal((await usage(rig, 'not-a-token', 'chat')).status, 401, 'a forged one');
    // And the shape that has caught this server before: signed with the same shard secret, no role.
    const shadow = await call(rig, '/shadow-token', {
      method: 'POST',
      headers: { authorization: 'Bearer a-shadow-key', 'content-type': 'application/json' },
      body: JSON.stringify({ device: 'testdevice' }),
    });
    assert.equal((await usage(rig, shadow.body['token'] as string, 'chat')).status, 401, 'signed, but roleless');
  } finally {
    stop(rig);
  }
});

// -------------------------------------------------- §5.24 Gate C: the default tenant, and overrides

test('the DEFAULT tenant gets a tile, and it is the one holding all the data today', async () => {
  // `currentDb` is null on the default tenant — a connection there carries no `ns` to name it by —
  // and `/usage` refused it, so the ONE database with production's entire dataset was the only one
  // with no usage line. It is not in the registry and never will be: it is the schema the gateway
  // was configured with, not something anyone declared.
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'], raw: ['userstatus'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const token = session['token'] as string;

    const sidebar = await call(rig, '/topnodes', { headers: { authorization: `Bearer ${token}` } });
    assert.equal(sidebar.body['defaultDb'], 'public', 'the gateway says which database it serves');

    const ok = await usage(rig, token, 'public');
    assert.equal(ok.status, 200, 'the default tenant is readable');
    assert.equal(ok.body['database'], 'public');

    // And nothing else got in with it: a raw namespace is still not a database.
    assert.equal((await usage(rig, token, 'userstatus')).status, 400);
  } finally {
    stop(rig);
  }
});

test('a database can be declared WITH a quota, and the number is bounded at the door', async () => {
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const token = session['token'] as string;
    const declare = (body: unknown) =>
      call(rig, '/databases', {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
        body: JSON.stringify(body),
      });

    assert.equal((await declare({ name: 'with_quota', quotaAcqPerSec: 120 })).status, 201);
    assert.equal((await declare({ name: 'no_quota' })).status, 201, 'omitted stays optional');

    // A zero would be a database that can never write, and no door here can say "I meant that".
    for (const bad of [0, -5, 1.5, '64', 999_999]) {
      const r = await declare({ name: 'bad_quota', quotaAcqPerSec: bad });
      assert.equal(r.status, 400, `quota ${JSON.stringify(bad)} is refused`);
    }
  } finally {
    stop(rig);
  }
});

test('a database just created is in the sidebar AT ONCE, not after the cache window', async () => {
  /**
   * §5.24 Gate D, and this test exists because its absence was PROVEN: the mentor reverted the
   * cache-invalidation fix and the whole file stayed green. `declareDatabase` invalidates
   * `topCache` so the operator who just clicked `+` is not told for ten seconds that their database
   * does not exist — the obvious conclusion being that the button is broken.
   *
   * It went silently dead at §5.22 F-3, which renamed the cache from `{ names }` to `{ shard }`:
   * the invalidation kept writing a `names` key that nothing reads any more, and `topCache.shard`
   * stayed warm. Two lines, forty apart, with nothing tying them together — so this ties them.
   *
   * The window is REAL time, not mocked: `TOPNODES_TTL` is 10s and this asserts within
   * milliseconds, so a stale cache cannot pass by the test being slow.
   */
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const token = session['token'] as string;
    const list = async (): Promise<string[]> =>
      ((await call(rig, '/topnodes', { headers: { authorization: `Bearer ${token}` } })).body['declared'] as string[]);

    // WARM the cache first — an invalidation that is never needed cannot be shown to work.
    assert.deepEqual(await list(), ['chat'], 'the sidebar has been read, so the cache is populated');

    const created = await call(rig, '/databases', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ name: 'brand_new' }),
    });
    assert.equal(created.status, 201);

    assert.deepEqual(
      (await list()).sort(),
      ['brand_new', 'chat'],
      'the new database is there immediately — a cache that outlives its own creation reads as a broken + button',
    );
  } finally {
    stop(rig);
  }
});

test('the served page points at the SAME endpoint the CSP allows (§5.25 Gate 4)', async () => {
  /**
   * Two facts that had to agree and did not have to be written down together: the `connect-src`
   * origin in the CSP header, and the value in the page's endpoint box. When they differ the
   * browser refuses the socket BEFORE a byte leaves — the page says "connecting" and nothing else,
   * with no line in the auth-server's log or the gateway's. It cost a rehearsal at §5.24 Gate B and
   * the first riding-client run at §5.25 Gate 1.
   *
   * Asserted against a NON-DEFAULT origin, so a page that simply kept its own default would pass
   * the equality and fail this.
   */
  const rig = await startServer({ admin: adminRecord(), wss: 'wss://rtdb.example.test' });
  try {
    const r = await fetch(`http://127.0.0.1:${rig.port}/`);
    const html = await r.text();
    const csp = r.headers.get('content-security-policy') ?? '';
    assert.match(csp, /connect-src wss:\/\/rtdb\.example\.test 'self'/, 'the CSP allows the configured origin');
    assert.match(html, /<input name="url" value="wss:\/\/rtdb\.example\.test"/, 'and the box is filled with it');
    assert.doesNotMatch(html, /value="ws:\/\/127\.0\.0\.1:8080"/, 'the file:// default is gone from the SERVED page');

    // And the property behind both, stated once: whatever the box says must be inside the CSP.
    const boxed = /<input name="url" value="([^"]+)"/.exec(html)?.[1] as string;
    assert.ok(csp.includes(`connect-src ${boxed} `), `the box (${boxed}) must be an allowed origin`);
  } finally {
    stop(rig);
  }
});

test('opened as a FILE the page still carries the local default', () => {
  // The served page is rewritten; the file on disk is not. `file://` has no server to ask, and a
  // local gateway is the right guess for the one mode where somebody pastes their own token.
  const html = readFileSync(fileURLToPath(new URL('../../console/rtdb-console.html', import.meta.url)), 'utf8');
  assert.match(html, /<input name="url" value="ws:\/\/127\.0\.0\.1:8080"/);
});

test('a name that would not be a legal SCHEMA is refused at declare, not at hello (§5.26)', async () => {
  /**
   * The whole point of moving the rule: `TenantAlpha` used to pass `/databases` and fail three
   * steps later at the tenant factory — 1011 on a hello, with a registry row nobody can delete,
   * because §5.19 gave declaring no inverse on purpose.
   */
  const rig = await startServer({ admin: adminRecord(), databases: ['chat'] });
  try {
    const { body: session } = await login(rig, OWNER.email, OWNER.password);
    const declare = (name: string) =>
      call(rig, '/databases', {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${session['token'] as string}` },
        body: JSON.stringify({ name }),
      });

    // §5.27: and the reason survives the auth-server's pass-through — the console alerts `error`
    // verbatim, so "which rule" has to arrive here, not just 400.
    const bad: [string, RegExp][] = [
      ['TenantAlpha', /lowercase letters/],
      ['Car_Race', /lowercase letters/],
      ['car-race', /lowercase letters/],
      ['9lives', /lowercase letters/],
      ['a'.repeat(52), /at most 51/],
      // §5.29: the empty name too. It used to be answered by the auth-server's own
      // `{"error":"name required"}` (`auth-server.mjs:822`) — the one reason of the four that did
      // not come from `path.ts`, in the file whose own comment says the rule must not live there.
      //
      // The pattern needs BOTH words: a bare /required/ matches `name required` just as happily as
      // `database name is required`, so it would have passed against the very code this case
      // exists to keep deleted. `/database name.*required/` keys on the gateway's voice while
      // staying loose about how the sentence is worded.
      ['', /database name.*required/],
    ];
    for (const [name, reason] of bad) {
      const r = await declare(name);
      assert.equal(r.status, 400, `${name} is refused at the door`);
      assert.match(String(r.body['error']), reason, name);
    }
    // And the legal shape still declares — the rule refuses characters, not names.
    assert.equal((await declare('tenantalpha')).status, 201);
    assert.equal((await declare('car_race')).status, 201);
  } finally {
    stop(rig);
  }
});
