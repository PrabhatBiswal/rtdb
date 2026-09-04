/**
 * The console's front door, on the ops box. Node stdlib only — no dependency, and none is wanted:
 * this process guards the one credential that can read the whole shard.
 *
 *   node auth-server.mjs            # PORT=8080 CONSOLE_HTML=./rtdb-console.html
 *
 * It does exactly two things:
 *   GET  /            -> the static console page (and nothing else from disk: no path is taken
 *                        from the request, so there is no traversal surface at all)
 *   POST /login       -> {email, password} -> scrypt-verify against SSM -> a 1h console token
 *   /users            -> the owner's user management (GET list, POST add, PATCH role, DELETE remove)
 *
 * The token it mints is the SAME shape scripts/console-token.ts mints: HS256 over the shard's
 * jwt_secret, sub=console-<localpart> for a viewer and console-rw-<localpart> for an editor or
 * owner (§5.9's deliberate unlock), exp +1h, plus a `role` claim. The page then dials
 * wss:// itself, so this service is never in the data path and never sees a delta.
 *
 * What it deliberately does NOT do: set a cookie (the token lives in page memory exactly as it did
 * when it was pasted by hand), serve anything but the one page, or write a password anywhere.
 */
import { createServer } from 'node:http';
import { execFile } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHmac, randomBytes, scrypt, timingSafeEqual } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';

const PORT = Number(process.env.PORT ?? 8080);
const REGION = process.env.AWS_REGION ?? 'ap-south-1';
const HTML_PATH = process.env.CONSOLE_HTML ?? new URL('./rtdb-console.html', import.meta.url).pathname;
const TOKEN_HOURS = Number(process.env.TOKEN_HOURS ?? 1);
// Which SSM parameter holds the credential. A knob, not a test hook: it is what lets this be
// exercised end-to-end against a throwaway credential without touching the real one.
const ADMIN_PARAM = process.env.ADMIN_PARAM ?? '/rtdb/console/admin';
/**
 * The multi-user store: {email -> {salt, hash, params, role}}. It does not exist until an owner adds
 * the second user, and it does not need a migration step to come into being — see loadUsers().
 */
const USERS_PARAM = process.env.USERS_PARAM ?? '/rtdb/console/users';
/**
 * owner: everything, including user management. editor / viewer: read the console.
 *
 * editor and viewer are the SAME thing on the wire today and that is deliberate, not an oversight:
 * this console is structurally read-only (send()'s allowlist, the 9 unmodified mirror tests), so
 * there is no write for an editor to be allowed. The distinction is carried in the TOKEN now so the
 * coming node-CRUD panel enforces it GATEWAY-side on the role claim, with no auth migration and no
 * second login. A role checked in the browser is decoration; the claim is the thing that travels.
 */
const ROLES = new Set(['owner', 'editor', 'viewer']);
/**
 * The roles that may write, and therefore the roles whose sessions get the DELIBERATE-UNLOCK subject
 * `console-rw-<localpart>` instead of `console-<localpart>` (§5.9 Gate B; the old console Gate A Q2
 * ruling asked for a distinct subject so §10's kick can name write-capable sessions as a class).
 *
 * The gateway demands BOTH this subject and the role claim before it will accept a write, so the two
 * are minted together here and nowhere else — a token with one and not the other cannot come from
 * this server, and the gateway refuses it on that basis.
 */
const WRITE_ROLES = new Set(['owner', 'editor']);
/** The localpart a subject was minted from, whichever of the two forms it takes. */
const subLocalpart = (s) => String(s ?? '').replace(/^console-(?:rw-)?/, '');
/**
 * SSM's standard parameter tier holds 4 KB, and one user record measures ~297 bytes (email, salt,
 * hash, params, role, who set it and when). That is about THIRTEEN users, not the "dozens" the work
 * order estimated — measured, not assumed. The guard exists so the thirteenth-plus owner gets a
 * number they can act on instead of an opaque CLI failure; the escape hatch, if it is ever wanted,
 * is the advanced tier (8 KB, ~$0.05/parameter/month) or a bucket, and neither is worth it yet.
 */
const STORE_MAX = 4096;

/** Distinguishable from any other write failure, because it is the owner's problem, not the box's. */
class StoreFull extends Error {}

/** The cost of a new user's password hash. Same numbers as scripts/console-admin-set.ts, on purpose:
 *  a record written here and a record written there must be verifiable by the same code path. */
const SCRYPT_PARAMS = { N: 2 ** 15, r: 8, p: 1, keylen: 64 };
/**
 * The store is HASH-ONLY and stays that way: a forgotten password is reset, never recovered, and no
 * route anywhere returns a stored one. The floor below is the only rule worth having, and it is
 * enforced HERE — the page checks it too, and a check in the page is decoration.
 *
 * Length, not composition: a run of spaces is refused because it is not a password, but a long
 * passphrase of ordinary words is a good one and no class rule is going to improve it.
 */
const MIN_PASSWORD = 8;
/** null when the plaintext is acceptable, else the reason to hand back verbatim. */
function passwordProblem(pw) {
  if (typeof pw !== 'string' || pw.trim() === '') return 'a password is required';
  if (pw.length < MIN_PASSWORD) return `a password must be at least ${MIN_PASSWORD} characters`;
  return null;
}
/**
 * The one place a plaintext becomes a stored record's credential half — used by the add route and by
 * the reset route, so the two cannot drift onto different costs or a different salt width. The
 * plaintext is an argument and nothing else: not returned, not logged, gone when this returns.
 */
async function credentialFor(password) {
  const salt = randomBytes(32);
  const hash = await scryptAsync(password, salt, SCRYPT_PARAMS.keylen,
    { N: SCRYPT_PARAMS.N, r: SCRYPT_PARAMS.r, p: SCRYPT_PARAMS.p, maxmem: SCRYPT_MAXMEM });
  return { salt: salt.toString('base64'), hash: hash.toString('base64'), params: SCRYPT_PARAMS };
}
/**
 * WP8 §2.3's shadow-token key. Under /rtdb/console/ and NOT /rtdb/shadow/ as §2.3 names it, for one
 * reason: the ops instance role is deliberately scoped to /rtdb/console/* plus the shard secret, not
 * to /rtdb/* at large (main.tf: "scoped to those two paths"). Reading /rtdb/shadow/key would need an
 * IAM change and therefore a terraform apply, which §0.5 forbids this package. Same secret, same
 * protection, zero infrastructure change.
 */
const SHADOW_PARAM = process.env.SHADOW_PARAM ?? '/rtdb/console/shadow_key';
/** Named rather than repeated as a literal, so the local store below can special-case exactly it. */
const JWT_PARAM = process.env.JWT_PARAM ?? '/rtdb/prod/jwt_secret';

/**
 * SELF-HOSTING WITHOUT AWS. Set CONSOLE_STORE_DIR and the user store becomes a file in that
 * directory instead of an SSM parameter, and the signing secret comes from RTDB_DEV_SECRET — the
 * same variable the gateway is given, which is the point: the console signs tokens the gateway must
 * verify, so one secret, one source.
 *
 * Unset, every path below is byte-identical to what it was: SSM reads, SSM writes, nothing new. The
 * AWS deployment is the reviewed path and this does not touch it.
 */
const STORE_DIR = process.env.CONSOLE_STORE_DIR;
const localFile = (name) => join(STORE_DIR, name.replace(/^\//, '').replace(/\//g, '_') + '.json');
/** §2.3: 24h. Longer than a console token because a device fetches one per app start, not per view. */
const SHADOW_HOURS = Number(process.env.SHADOW_HOURS ?? 24);
// The single origin the page is allowed to open a socket to. One value, one allowlist entry.
const WSS_ORIGIN = process.env.CONSOLE_WSS ?? 'ws://127.0.0.1:8080';
const MAX_FAILS = 5;
const LOCKOUT_MS = 15 * 60 * 1000;
/**
 * A GLOBAL backstop for /shadow-token, because per-IP limiting does not hold there.
 *
 * Found live on 2026-08-30: six bad keys through CloudFront produced six 401s and no lockout, because
 * a dual-stack client presents DIFFERENT source addresses per connection — the journal shows the same
 * curl alternating between 2407:8c0:… and 103.105.96.102, so each counter only ever reached three.
 * This is WP6 console Gate C's finding in a new costume: there it was one NLB node per subnet
 * diluting the counter three ways, and the fix (use the LAST X-Forwarded-For entry) was correct and
 * is still in place — the addresses below ARE the real client. The client simply has two of them.
 *
 * Per-IP stays (it is what stops one address hammering). This is the ceiling underneath it. The
 * legitimate population of this endpoint is a handful of debug devices fetching one token per app
 * start, so 20 failures in 15 minutes is far above real noise and far below a useful guessing rate.
 */
const GLOBAL_FAIL_MAX = Number(process.env.SHADOW_GLOBAL_MAX ?? 20);
const GLOBAL_WINDOW_MS = 15 * 60 * 1000;
const MAX_BODY = 4096;                       // a login body is ~100 bytes; anything larger is noise
// Node caps scrypt at 32 MiB by default and the chosen N/r needs exactly that; without an explicit
// ceiling verification fails with "memory limit exceeded" for every correct password.
const SCRYPT_MAXMEM = 64 * 1024 * 1024;
// Prometheus already scrapes the gateways every 15s. Reading ITS numbers is what makes the stats
// strip cost the gateways exactly nothing — no new endpoint, no new scrape, no extra connection.
const PROM = process.env.PROM_URL ?? 'http://127.0.0.1:9090';

/**
 * The ONLY queries this server will ever run. Not a default, not a starting point — a caller cannot
 * name a query, cannot influence one, and cannot add one. GET /stats runs exactly this list and
 * returns exactly these numbers, which is what keeps a read-only console from becoming a general
 * PromQL endpoint on the open internet.
 */
const QUERIES = {
  connections: 'rtdb_connections',
  connectionsTotal: 'sum(rtdb_connections)',
  writesPerSec: 'sum(rate(rtdb_writes_total[1m]))',
  consumerLag: 'max(rtdb_consumer_lag_revs)',
};

/**
 * §5.6's sidebar. The gateway answers `/topnodes` on its ADMIN port — never the public wire — and
 * this proxies it, token-gated, to a FIXED target kind: a gateway Prometheus already discovered.
 *
 * Discovered rather than configured, because a hardcoded private IP is wrong the first time an
 * instance is replaced, and Prometheus is already the thing on this box that knows where the
 * gateways are (ec2_sd on Project=rtdb + Role=gateway). Same reasoning as the dashboards.
 *
 * A caller cannot name a host, a port or a path here: it gets the namespaces of the shard this ops
 * box already watches, or it gets an error.
 */
const TOPNODES_TTL = 10_000;   // matches the gateway's window (ruling 2026-08-30)
let topCache = { names: null, at: 0 };

async function gatewayAdminBase() {
  const r = await fetch(`${PROM}/api/v1/targets?state=active`, { signal: AbortSignal.timeout(4000) });
  if (!r.ok) throw new Error(`prometheus targets ${r.status}`);
  const d = await r.json();
  const t = (d.data?.activeTargets ?? []).find(
    (x) => x.labels?.job === 'rtdb-gateway' && x.health === 'up' && x.discoveredLabels?.__address__,
  );
  if (!t) throw new Error('no healthy rtdb-gateway target');
  return `http://${t.discoveredLabels.__address__}`;
}

async function topNodes() {
  if (topCache.names && Date.now() - topCache.at < TOPNODES_TTL) return topCache.names;
  const base = await gatewayAdminBase();
  const r = await fetch(`${base}/topnodes`, { signal: AbortSignal.timeout(4000) });
  if (!r.ok) throw new Error(`gateway topnodes ${r.status}`);
  const d = await r.json();
  const names = Array.isArray(d?.names) ? d.names.filter((n) => typeof n === 'string') : [];
  topCache = { names, at: Date.now() };
  return names;
}

const scryptAsync = promisify(scrypt);
const execFileAsync = promisify(execFile);
const HTML = readFileSync(HTML_PATH, 'utf8');

/** One audit line per attempt. Never the password, never the token — outcome, who, and from where. */
const audit = (event, detail) =>
  console.log(JSON.stringify({ t: new Date().toISOString(), event, ...detail }));

// --------------------------------------------------------------------------- secrets
let cache = { users: null, secret: null, at: 0 };
const TTL = 5 * 60 * 1000;   // re-read every 5 min so a rotated password takes effect without a restart

async function ssm(name, decrypt = true) {
  if (STORE_DIR) {
    // The signing secret is not a stored document — it belongs to the gateway, and it arrives the
    // same way the gateway gets it. Absent is a configuration error worth failing on loudly, since
    // the alternative is signing with undefined and every token being rejected downstream.
    if (name === JWT_PARAM) {
      const secret = process.env.RTDB_DEV_SECRET;
      if (!secret) throw new Error('CONSOLE_STORE_DIR is set, so RTDB_DEV_SECRET must be too — the'
        + ' console signs tokens this gateway has to verify, so both need the same secret.');
      return secret;
    }
    try {
      return (await readFile(localFile(name), 'utf8')).trim();
    } catch (e) {
      // loadUsers() separates an ABSENT parameter from every other failure and falls back only on
      // the former, matching SSM's own wording. A local miss must carry that same signature or the
      // first-run fallback turns into a hard error instead.
      if (e.code !== 'ENOENT') throw e;
      const err = new Error(`ParameterNotFound: ${name}`);
      err.stderr = 'ParameterNotFound';
      throw err;
    }
  }
  const args = ['ssm', 'get-parameter', '--name', name, '--query', 'Parameter.Value',
                '--output', 'text', '--region', REGION];
  if (decrypt) args.push('--with-decryption');
  const { stdout } = await execFileAsync('aws', args, { maxBuffer: 1 << 20 });
  return stdout.trim();
}

/**
 * The user store — or the single-admin record it grew out of.
 *
 * There is no migration step and no migration script. Until an owner adds someone, USERS_PARAM does
 * not exist and this synthesises the store from the ORIGINAL `/rtdb/console/admin` record, whose
 * owner is the only user there has ever been. First login is therefore byte-identical to yesterday's,
 * the old parameter stays untouched as a rollback path, and the store materialises on the first
 * management write (which saves the owner into it along with the new user).
 *
 * Only a genuinely ABSENT parameter falls back. Any other failure — denied, throttled, unreachable —
 * propagates as an error, because quietly handing back the pre-multi-user store would log a removed
 * account back in.
 */
async function loadUsers() {
  let raw;
  try {
    raw = await ssm(USERS_PARAM);
  } catch (e) {
    const why = String(e.stderr ?? e.message ?? e);
    if (!/ParameterNotFound/i.test(why)) throw e;
    const admin = JSON.parse(await ssm(ADMIN_PARAM));
    return {
      [String(admin.email).toLowerCase()]:
        { salt: admin.salt, hash: admin.hash, params: admin.params, role: 'owner' },
    };
  }
  const parsed = JSON.parse(raw);
  // Addresses are compared lowercased everywhere (login lowercases what it is given), so normalise
  // once here rather than trusting every writer to have done it.
  const users = {};
  for (const [email, rec] of Object.entries(parsed)) users[String(email).toLowerCase()] = rec;
  return users;
}

async function credentials() {
  if (cache.users && Date.now() - cache.at < TTL) return cache;
  const [users, secret] = await Promise.all([loadUsers(), ssm(JWT_PARAM)]);
  cache = { users, secret, at: Date.now() };
  return cache;
}

/**
 * Write the store back, then make the cache tell the truth immediately: a five-minute wait before a
 * new user can sign in — or before a removed one cannot — would look exactly like a bug.
 *
 * `--value file://` rather than an argument, because argv is world-readable in `ps` and these are
 * password hashes; 0600, and gone in the `finally` whatever happens (RUNBOOK 7b's rule, same shape).
 */
async function putUsers(users) {
  const dir = await mkdtemp(join(tmpdir(), 'rtdb-users-'));
  const file = join(dir, 'users.json');
  const body = JSON.stringify(users);
  // Guarded HERE rather than at each route: every writer, now and later, goes through this function.
  if (Buffer.byteLength(body) > STORE_MAX) {
    throw new StoreFull(`the user store is full at ${Object.keys(users).length} users — SSM's standard`
      + ` parameter holds 4 KB, which is about thirteen. Remove someone, or move the parameter to the`
      + ` advanced tier.`);
  }
  try {
    if (STORE_DIR) {
      // 0700 on the directory, 0600 on the file: this holds password hashes and their salts. The
      // directory is created because SSM never needed one, so the local path is the first thing
      // here that can fail simply because a path does not exist yet.
      await mkdir(STORE_DIR, { recursive: true, mode: 0o700 });
      await writeFile(localFile(USERS_PARAM), body, { mode: 0o600 });
    } else {
      await writeFile(file, body, { mode: 0o600 });
      await execFileAsync('aws', ['ssm', 'put-parameter', '--name', USERS_PARAM, '--type', 'SecureString',
        '--overwrite', '--value', 'file://' + file, '--region', REGION], { maxBuffer: 1 << 20 });
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
  cache = { users, secret: cache.secret, at: Date.now() };
}

/**
 * Every management action is a read-modify-write of one parameter, so they run one at a time: two
 * owners adding a user in the same second must not each write a map missing the other's.
 * ponytail: in-process only. A second auth-server instance would need a conditional put, which SSM
 * does not have — there is one ops box, and this note is here for the day there is not.
 */
let writeChain = Promise.resolve();
function serialize(fn) {
  const run = writeChain.then(fn, fn);
  writeChain = run.then(() => undefined, () => undefined);
  return run;
}

/**
 * A stand-in record to hash against when the address is unknown, so a wrong address costs the same
 * ~100ms as a wrong password and this endpoint cannot be asked "does this account exist?".
 * Random at boot: nothing can match it.
 */
const DUMMY = {
  salt: randomBytes(32).toString('base64'),
  hash: randomBytes(SCRYPT_PARAMS.keylen).toString('base64'),
  params: SCRYPT_PARAMS,
};

/**
 * Cached SEPARATELY from `credentials()` on purpose: if the shadow key is missing or unreadable,
 * /login must keep working. A new endpoint may not be able to break the one people depend on.
 */
let shadowCache = { key: null, secret: null, at: 0 };

async function shadowCredentials() {
  if (shadowCache.key && Date.now() - shadowCache.at < TTL) return shadowCache;
  const [key, secret] = await Promise.all([ssm(SHADOW_PARAM), ssm(JWT_PARAM)]);
  shadowCache = { key, secret, at: Date.now() };
  return shadowCache;
}

/**
 * A device identifier is untrusted input that ends up inside a token's `sub`, and §10's kick targets
 * users by exactly that string. Restricting it to a short alphanumeric slug keeps a caller from
 * choosing a subject that collides with a console or app user.
 */
const deviceSlug = (raw) => String(raw ?? '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 32);

// --------------------------------------------------------------------------- token
const b64u = (b) => Buffer.from(b).toString('base64url');

/**
 * Byte-identical in shape to scripts/console-token.ts — one token format, two mints.
 *
 * `role` is omitted when there is none, which is exactly the shadow-token case: a device token is
 * not a console session and must not read like one.
 */
function mintToken(sub, secret, hours, role) {
  const head = b64u(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const claims = { sub, exp: Math.floor(Date.now() / 1000) + Math.round(hours * 3600) };
  if (role) claims.role = role;
  const body = b64u(JSON.stringify(claims));
  const mac = createHmac('sha256', secret).update(`${head}.${body}`).digest();
  return `${head}.${body}.${b64u(mac)}`;
}

// --------------------------------------------------------------------------- lockout
/**
 * In-process, per IP+email. A restart clears it — accepted for a single-admin console (a restart is
 * an operator action, not something an attacker can trigger from the outside), and documented as
 * such rather than left to be discovered.
 */
const fails = new Map();

/** Failure timestamps for the global backstop, pruned on read. Bounded by GLOBAL_FAIL_MAX. */
let globalFails = [];

function globalLocked() {
  const now = Date.now();
  globalFails = globalFails.filter((t) => now - t < GLOBAL_WINDOW_MS);
  if (globalFails.length < GLOBAL_FAIL_MAX) return 0;
  return GLOBAL_WINDOW_MS - (now - globalFails[0]);
}

function recordGlobalFail() {
  globalFails.push(Date.now());
  if (globalFails.length > GLOBAL_FAIL_MAX * 2) globalFails = globalFails.slice(-GLOBAL_FAIL_MAX);
}

/**
 * Drop entries whose lockout has expired. With one admin the map held one key; with a user list it
 * grows per distinct ip|email, and an attacker chooses both halves — so it is pruned on the read
 * path, the same shape the global backstop already uses.
 */
function pruneFails() {
  const now = Date.now();
  for (const [k, rec] of fails) if (rec.count >= MAX_FAILS && rec.until <= now) fails.delete(k);
}

function lockedFor(key) {
  pruneFails();
  const rec = fails.get(key);
  if (!rec) return 0;
  if (rec.count < MAX_FAILS) return 0;
  const left = rec.until - Date.now();
  if (left <= 0) { fails.delete(key); return 0; }
  return left;
}

function recordFail(key) {
  const rec = fails.get(key) ?? { count: 0, until: 0 };
  rec.count++;
  if (rec.count >= MAX_FAILS) rec.until = Date.now() + LOCKOUT_MS;
  fails.set(key, rec);
  return rec;
}

// --------------------------------------------------------------------------- http
const json = (res, code, obj) => {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  });
  res.end(body);
};

/**
 * The socket address is the LOAD BALANCER, not the caller — and the NLB has a node per subnet, so
 * consecutive requests from one attacker arrive from different addresses. Keying the lockout on it
 * silently divided the count by the number of NLB nodes and the threshold was never reached: five
 * wrong passwords in a row returned five 401s and no lockout at all.
 *
 * CloudFront APPENDS the viewer's address to X-Forwarded-For, so the trustworthy entry is the LAST
 * one — a caller can prepend whatever it likes, but it cannot append after CloudFront. The NLB is
 * layer 4 with preserve_client_ip off and adds nothing to the chain, so CloudFront's entry is the
 * end of it. Falling back to the socket keeps this working when opened directly on the box.
 */
function clientIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string' && xff.trim()) {
    const last = xff.split(',').pop().trim();
    if (last) return last;
  }
  return req.socket.remoteAddress ?? '?';
}

/**
 * Verify a token this server (or scripts/console-token.ts) minted, and return {sub, role}.
 * Constant-time on the signature, and the expiry is checked — an hour-old token is exactly the case
 * this exists for. Returns null on anything wrong; the caller turns that into one 401 with no detail.
 *
 * `role` is null for a token that carries no role claim. That is not a formality: every shadow-token
 * this same server mints is signed with the same shard secret, so before the role claim existed a
 * 24h device token was accepted by /stats and /topnodes. Requiring a KNOWN role is what closes it.
 */
function verifyToken(token, secret) {
  const parts = String(token).split('.');
  if (parts.length !== 3) return null;
  const [head, body, mac] = parts;
  const expected = createHmac('sha256', secret).update(`${head}.${body}`).digest();
  let given;
  try { given = Buffer.from(mac, 'base64url'); } catch { return null; }
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  let claims;
  try { claims = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')); } catch { return null; }
  if (typeof claims?.exp !== 'number' || claims.exp <= Math.floor(Date.now() / 1000)) return null;
  if (typeof claims.sub !== 'string') return null;
  return { sub: claims.sub, role: typeof claims.role === 'string' ? claims.role : null };
}

/**
 * The one place a request is turned into an identity. Returns {sub, role} for a token carrying a
 * KNOWN console role, and null otherwise — an unsigned, expired, or roleless token is the same
 * answer to a caller: 401, no detail.
 */
function consoleUser(req, secret) {
  const header = String(req.headers.authorization ?? '');
  const who = header.startsWith('Bearer ') ? verifyToken(header.slice(7).trim(), secret) : null;
  return who && ROLES.has(who.role) ? who : null;
}

/** One instant query against the local Prometheus. The query comes from QUERIES, never from a caller. */
async function promQuery(q) {
  const u = `${PROM}/api/v1/query?query=${encodeURIComponent(q)}`;
  const r = await fetch(u, { signal: AbortSignal.timeout(4000) });
  if (!r.ok) throw new Error(`prometheus ${r.status}`);
  const d = await r.json();
  if (d.status !== 'success') throw new Error('prometheus said ' + d.status);
  // Flatten to what the strip needs: a labelled number per series, nothing else from the payload.
  return (d.data?.result ?? []).map((s) => ({
    instance: s.metric?.instance ?? null,
    az: s.metric?.az ?? null,
    value: Number(s.value?.[1] ?? 0),
  }));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let n = 0;
    const chunks = [];
    req.on('data', (c) => {
      n += c.length;
      if (n > MAX_BODY) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = createServer(async (req, res) => {
  const ip = clientIp(req);
  const url = (req.url ?? '/').split('?')[0];

  if (req.method === 'GET' && (url === '/' || url === '/index.html')) {
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
      'referrer-policy': 'no-referrer',
      // The page talks to exactly ONE endpoint and loads nothing from anywhere. The endpoint is a
      // variable rather than a literal so a different shard (or a local one under test) does not
      // require editing the policy — the allowlist is still exactly one origin either way.
      'content-security-policy':
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
        `connect-src ${WSS_ORIGIN} 'self'; base-uri 'none'; form-action 'none'`,
    });
    res.end(HTML);
    return;
  }

  if (req.method === 'GET' && url === '/healthz') { res.writeHead(200).end('ok\n'); return; }

  // ------------------------------------------------------------------------- GET /stats
  // Signed-in operators only. Anonymous gets 401 and learns nothing about the fleet.
  if (req.method === 'GET' && url === '/stats') {
    let secret;
    try { ({ secret } = await credentials()); }
    catch (e) {
      audit('stats.ssm_error', { ip, msg: String(e).slice(0, 200) });
      return json(res, 503, { error: 'unavailable' });
    }
    const who = consoleUser(req, secret);
    if (!who) {
      audit('stats.denied', { ip });
      return json(res, 401, { error: 'sign in first' });
    }
    const sub = who.sub;
    try {
      const out = {};
      await Promise.all(Object.entries(QUERIES).map(async ([k, q]) => { out[k] = await promQuery(q); }));
      return json(res, 200, { at: Date.now(), ...out });
    } catch (e) {
      audit('stats.prom_error', { ip, sub, msg: String(e).slice(0, 200) });
      return json(res, 502, { error: 'metrics unavailable' });
    }
  }

  /** §5.6: the root namespaces, for the console's sidebar. Token-gated exactly like /stats. */
  if (req.method === 'GET' && url === '/topnodes') {
    let secret;
    try { ({ secret } = await credentials()); }
    catch { return json(res, 503, { error: 'credential store unavailable' }); }
    if (!consoleUser(req, secret)) {
      audit('topnodes.denied', { ip });
      return json(res, 401, { error: 'unauthorized' });
    }
    try {
      return json(res, 200, { names: await topNodes() });
    } catch (e) {
      // A sidebar that cannot load must not take the console down with it.
      audit('topnodes.error', { ip, msg: String(e).slice(0, 160) });
      return json(res, 503, { error: 'unavailable' });
    }
  }

  if (req.method === 'POST' && url === '/login') {
    let body;
    try { body = JSON.parse(await readBody(req)); }
    catch { audit('login.badrequest', { ip }); return json(res, 400, { error: 'bad request' }); }

    const email = String(body?.email ?? '').trim().toLowerCase();
    const password = String(body?.password ?? '');
    const key = `${ip}|${email}`;

    const left = lockedFor(key);
    if (left > 0) {
      audit('login.locked', { ip, email, secondsLeft: Math.ceil(left / 1000) });
      return json(res, 429, { error: 'too many attempts', retryInSeconds: Math.ceil(left / 1000) });
    }
    if (!email || !password) {
      recordFail(key);
      audit('login.missing', { ip, email });
      return json(res, 400, { error: 'email and password are required' });
    }

    let users, secret;
    try { ({ users, secret } = await credentials()); }
    catch (e) {
      audit('login.ssm_error', { ip, email, msg: String(e).slice(0, 200) });
      return json(res, 503, { error: 'credential store unavailable' });
    }

    // Hash EVEN IF the address is unknown — against DUMMY, which nothing can match — so a wrong
    // address and a wrong password take the same time. Otherwise this endpoint answers the question
    // "does this account exist?", which is exactly what a list of users must not leak.
    const account = users[email];
    const ref = account ?? DUMMY;
    const { N, r, p, keylen } = ref.params;
    const derived = await scryptAsync(password, Buffer.from(ref.salt, 'base64'), keylen, { N, r, p, maxmem: SCRYPT_MAXMEM });
    const expected = Buffer.from(ref.hash, 'base64');
    const emailOk = Boolean(account);
    const pwOk = derived.length === expected.length && timingSafeEqual(derived, expected);

    if (!emailOk || !pwOk) {
      const rec = recordFail(key);
      // emailOk/pwOk go to the LOG ONLY. The HTTP response below is identical either way, so this
      // tells an operator reading the journal which half was wrong without telling a caller
      // whether an address exists.
      audit('login.fail', { ip, email, emailOk, pwOk, attempt: rec.count, lockedOut: rec.count >= MAX_FAILS });
      return json(res, 401, { error: 'invalid credentials' });
    }

    // A record with no role is one written before roles existed, or by hand. Refusing it would lock
    // the owner out of their own console over a missing field; owner is what that record always was.
    const role = ROLES.has(account.role) ? account.role : 'owner';
    fails.delete(key);
    // editor/owner get the write-capable subject; a viewer keeps the read-only one. This is the
    // unlock: the gateway will not accept a write from any other subject, whatever the role says.
    const sub = `console-${WRITE_ROLES.has(role) ? 'rw-' : ''}${email.split('@')[0]}`;
    audit('login.ok', { ip, email, sub, role, expiresInHours: TOKEN_HOURS });
    return json(res, 200, { token: mintToken(sub, secret, TOKEN_HOURS, role), sub, role, expiresInHours: TOKEN_HOURS });
  }

  /**
   * User management. Owner only, ENFORCED HERE — the console hides the panel from everyone else, and
   * hiding a button is not an access control. One URL, four methods; the address is never in the
   * path, so nothing has to be encoded, parsed or guessed.
   */
  if (url === '/users') {
    let users, secret;
    try { ({ users, secret } = await credentials()); }
    catch (e) {
      audit('users.ssm_error', { ip, msg: String(e).slice(0, 200) });
      return json(res, 503, { error: 'credential store unavailable' });
    }

    const who = consoleUser(req, secret);
    if (!who) { audit('users.denied', { ip, reason: 'no token' }); return json(res, 401, { error: 'sign in first' }); }
    if (who.role !== 'owner') {
      audit('users.denied', { ip, sub: who.sub, role: who.role, method: req.method });
      return json(res, 403, { error: 'owner only' });
    }

    const list = () => Object.entries(users)
      .map(([email, u]) => ({ email, role: ROLES.has(u.role) ? u.role : 'owner' }))
      .sort((a, b) => a.email.localeCompare(b.email));

    if (req.method === 'GET') return json(res, 200, { users: list(), you: who.sub });

    let body;
    try { body = JSON.parse(await readBody(req) || '{}'); }
    catch { return json(res, 400, { error: 'bad request' }); }
    const email = String(body?.email ?? '').trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return json(res, 400, { error: 'a valid email is required' });

    // The token subject is `console-<localpart>`, and §10's kick targets that string. Two addresses
    // sharing a localpart would therefore share one identity: one kick would revoke both, and the
    // self-demote guard below could not tell them apart. Refuse the collision at the door.
    const localpart = email.split('@')[0];
    const collides = Object.keys(users).find((e) => e !== email && e.split('@')[0] === localpart);
    // Derived by STRIPPING the prefix, not by rebuilding it: an owner's own subject is now
    // `console-rw-<localpart>`, and a comparison against the read-only form would quietly answer
    // "not you" for exactly the people the self-guards below exist to protect.
    const isSelf = subLocalpart(who.sub) === localpart;
    const owners = (map) => Object.values(map).filter((u) => !ROLES.has(u.role) || u.role === 'owner').length;

    if (req.method === 'POST') {
      if (users[email]) return json(res, 409, { error: 'that user already exists' });
      if (collides) return json(res, 409, { error: `the part before the @ collides with ${collides}; tokens are named after it` });
      const role = String(body?.role ?? '');
      if (!ROLES.has(role)) return json(res, 400, { error: 'role must be owner, editor or viewer' });
      // The owner CHOOSES the password (v2) — the page always sends one. Omitting the field still
      // generates one, which is what the pre-v2 flow did and what a scripted caller may still want;
      // either way what is written is a scrypt hash and the plaintext is never stored.
      const chosen = body?.password !== undefined;
      if (chosen) {
        const bad = passwordProblem(body.password);
        if (bad) return json(res, 400, { error: bad });
      }
      const password = chosen ? String(body.password) : randomBytes(18).toString('base64url');
      const next = { ...users, [email]: {
        ...(await credentialFor(password)),
        role, setAt: new Date().toISOString(), setBy: who.sub,
      } };
      try { await serialize(() => putUsers(next)); }
      catch (e) {
        if (e instanceof StoreFull) { audit('users.store_full', { ip, actor: who.sub, users: Object.keys(users).length }); return json(res, 409, { error: e.message }); }
        audit('users.write_error', { ip, sub: who.sub, msg: String(e.stderr ?? e).slice(0, 200) });
        return json(res, 503, { error: 'could not write the user store' });
      }
      audit('users.add', { ip, actor: who.sub, email, role, chosen });   // never the password
      // A password the owner just typed is not echoed back: they have it, and a secret on the wire
      // for no reason is a secret in a proxy log for no reason. A GENERATED one is returned exactly
      // once, because this response is the only place it will ever exist.
      return json(res, 200, { email, role, ...(chosen ? {} : { password }) });
    }

    if (req.method === 'PATCH') {
      if (!users[email]) return json(res, 404, { error: 'no such user' });
      const role = String(body?.role ?? '');
      if (!ROLES.has(role)) return json(res, 400, { error: 'role must be owner, editor or viewer' });
      if (isSelf && role !== 'owner') return json(res, 409, { error: 'you cannot demote yourself' });
      const next = { ...users, [email]: { ...users[email], role, roleSetAt: new Date().toISOString(), setBy: who.sub } };
      if (owners(next) === 0) return json(res, 409, { error: 'that would leave the console with no owner' });
      try { await serialize(() => putUsers(next)); }
      catch (e) {
        if (e instanceof StoreFull) { audit('users.store_full', { ip, actor: who.sub, users: Object.keys(users).length }); return json(res, 409, { error: e.message }); }
        audit('users.write_error', { ip, sub: who.sub, msg: String(e.stderr ?? e).slice(0, 200) });
        return json(res, 503, { error: 'could not write the user store' });
      }
      audit('users.role', { ip, actor: who.sub, email, from: users[email].role ?? null, to: role });
      return json(res, 200, { email, role });
    }

    /**
     * Reset a password. PUT on this same URL rather than a second route, because the owner-only gate,
     * the body parse and the address validation above are EXACTLY the ones a reset needs — and a
     * second copy of an authorization gate is a gate that drifts out of step with the first.
     *
     * Self-reset is allowed, deliberately. The §5.8 guards below exist so the console cannot be left
     * unadministrable; changing your own password does not demote you and does not remove you, so
     * `isSelf` is not consulted here. Resetting is not demoting.
     */
    if (req.method === 'PUT') {
      if (!users[email]) return json(res, 404, { error: 'no such user' });
      const bad = passwordProblem(body?.password);
      if (bad) return json(res, 400, { error: bad });
      const credential = await credentialFor(String(body.password));
      const next = { ...users, [email]: {
        ...users[email], ...credential, pwSetAt: new Date().toISOString(), setBy: who.sub,
      } };
      try { await serialize(() => putUsers(next)); }
      catch (e) {
        if (e instanceof StoreFull) { audit('users.store_full', { ip, actor: who.sub, users: Object.keys(users).length }); return json(res, 409, { error: e.message }); }
        audit('users.write_error', { ip, sub: who.sub, msg: String(e.stderr ?? e).slice(0, 200) });
        return json(res, 503, { error: 'could not write the user store' });
      }
      // Same residual as a removal, and accepted on the same §5.8 grounds: the member's existing
      // token keeps working until it expires. An owner resetting a password BECAUSE it leaked needs
      // the kick, so both candidate subjects are named here rather than derived mid-incident.
      audit('users.password', { ip, actor: who.sub, email,   // never the password
        kickHints: [`console-${localpart}`, `console-rw-${localpart}`],
        sessionValidForUpToMinutes: Math.round(TOKEN_HOURS * 60) });
      return json(res, 200, { email, passwordSet: true });
    }

    if (req.method === 'DELETE') {
      if (!users[email]) return json(res, 404, { error: 'no such user' });
      if (isSelf) return json(res, 409, { error: 'you cannot remove yourself' });
      const next = { ...users };
      delete next[email];
      if (owners(next) === 0) return json(res, 409, { error: 'that would leave the console with no owner' });
      try { await serialize(() => putUsers(next)); }
      catch (e) {
        if (e instanceof StoreFull) { audit('users.store_full', { ip, actor: who.sub, users: Object.keys(users).length }); return json(res, 409, { error: e.message }); }
        audit('users.write_error', { ip, sub: who.sub, msg: String(e.stderr ?? e).slice(0, 200) });
        return json(res, 503, { error: 'could not write the user store' });
      }
      // Their token stays valid until it expires (one hour) — §10's kick is the immediate revocation,
      // and it is the same mechanism for a console subject as for an app user.
      // The residual window is stated in the journal, not left to be worked out later: their token
      // stays valid for up to its remaining hour, and `kick.ts` on kickHint is what ends it sooner.
      // BOTH candidate subjects: a removed editor or owner can hold a live session under the
      // write-capable subject AND, within the same hour, an older read-only one from before their
      // promotion. An operator mid-incident should not have to derive the second string.
      audit('users.remove', { ip, actor: who.sub, email,
        kickHints: [`console-${localpart}`, `console-rw-${localpart}`],
        sessionValidForUpToMinutes: Math.round(TOKEN_HOURS * 60) });
      return json(res, 200, { email, removed: true });
    }

    return json(res, 405, { error: 'method not allowed' });
  }

  /**
   * WP8 §2.3 — the INTERIM shadow token. A debug build of a consuming app presents a static bearer key and
   * gets a 24h token for `shadow-<device>`.
   *
   * Interim by design, and the replacement is already named: real integration means the app's own
   * backend mints these, the same way it mints the tokens it already uses, and this route is
   * DELETED. It exists so stage 1 can start without waiting on their backend.
   */
  if (req.method === 'POST' && url === '/shadow-token') {
    const key = `shadow|${ip}`;
    // Both limits, and the global one FIRST: an attacker who can rotate source addresses is exactly
    // the case per-IP cannot see, and the live test proved a plain dual-stack client already does.
    const globalLeft = globalLocked();
    if (globalLeft > 0) {
      audit('shadow.locked_global', { ip, secondsLeft: Math.ceil(globalLeft / 1000) });
      return json(res, 429, { error: 'too many attempts', retryInSeconds: Math.ceil(globalLeft / 1000) });
    }
    const left = lockedFor(key);
    if (left > 0) {
      audit('shadow.locked', { ip, secondsLeft: Math.ceil(left / 1000) });
      return json(res, 429, { error: 'too many attempts', retryInSeconds: Math.ceil(left / 1000) });
    }

    const header = String(req.headers.authorization ?? '');
    const presented = header.startsWith('Bearer ') ? header.slice(7).trim() : '';

    let body = {};
    try { body = JSON.parse(await readBody(req) || '{}'); }
    catch { recordFail(key); audit('shadow.badrequest', { ip }); return json(res, 400, { error: 'bad request' }); }

    const device = deviceSlug(body?.device);
    if (!device) {
      recordFail(key);
      audit('shadow.nodevice', { ip });
      return json(res, 400, { error: 'device is required' });
    }

    let expected, secret;
    try { ({ key: expected, secret } = await shadowCredentials()); }
    catch (e) {
      audit('shadow.ssm_error', { ip, msg: String(e).slice(0, 200) });
      return json(res, 503, { error: 'credential store unavailable' });
    }

    // Constant-time, and length-guarded because timingSafeEqual throws on a length mismatch — which
    // would itself be a timing signal, and a 500 instead of a 401.
    const a = Buffer.from(presented);
    const b = Buffer.from(expected);
    const ok = a.length === b.length && timingSafeEqual(a, b);
    if (!ok) {
      const rec = recordFail(key);
      recordGlobalFail();
      // The KEY is never logged, presented or expected — only that it did not match.
      audit('shadow.fail', { ip, device, attempt: rec.count, lockedOut: rec.count >= MAX_FAILS, globalFails: globalFails.length });
      return json(res, 401, { error: 'invalid key' });
    }

    fails.delete(key);
    const sub = `shadow-${device}`;
    audit('shadow.ok', { ip, sub, expiresInHours: SHADOW_HOURS });
    return json(res, 200, { token: mintToken(sub, secret, SHADOW_HOURS), sub, expiresInHours: SHADOW_HOURS });
  }

  res.writeHead(404, { 'content-type': 'text/plain' }).end('not found\n');
});

server.listen(PORT, '0.0.0.0', () => {
  audit('listening', { port: PORT, html: HTML_PATH, region: REGION, adminParam: ADMIN_PARAM });
});
