import assert from 'node:assert/strict';
import test from 'node:test';
import { spawn } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { createServer, type Server } from 'node:http';
import { createServer as netServer } from 'node:net';
import { scryptSync, randomBytes } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

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
const SERVER = fileURLToPath(new URL('../../tools/console/auth-server.mjs', import.meta.url));
const HTML = fileURLToPath(new URL('../../tools/console/rtdb-console.html', import.meta.url));

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
  prom: Server;
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

/** A stub Prometheus, so /stats answers 200 for an authorized caller instead of 502. */
function stubProm(port: number): Server {
  const s = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ status: 'success', data: { result: [{ metric: {}, value: [0, '7'] }] } }));
  });
  s.listen(port, '127.0.0.1');
  return s;
}

async function startServer(opts: { admin?: unknown; users?: unknown; shadowKey?: string; deny?: string } = {}): Promise<Rig> {
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
  const prom = stubProm(promPort);

  const proc = spawn(process.execPath, [SERVER], {
    env: {
      ...process.env,
      PATH: `${bin}:${process.env['PATH'] ?? ''}`,
      FAKE_SSM_DIR: ssmDir,
      FAKE_SSM_DENY: opts.deny ?? '',
      PORT: String(port),
      CONSOLE_HTML: HTML,
      PROM_URL: `http://127.0.0.1:${promPort}`,
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
  return { port, proc, log: () => out, ssmDir, prom, dir };
}

function stop(rig: Rig): void {
  rig.proc.kill('SIGKILL');
  rig.prom.close();
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
