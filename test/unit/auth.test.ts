import assert from 'node:assert/strict';
import test from 'node:test';
import { DevHs256Validator, devSecret, signDevToken } from '../../src/gateway/auth.ts';

const SECRET = 'unit-secret';
const v = new DevHs256Validator(SECRET);
const future = Math.floor(Date.now() / 1000) + 3600;

const rejects = (token: string, re: RegExp) => {
  const r = v.validate(token);
  assert.equal(r.ok, false, `expected rejection for ${token.slice(0, 24)}...`);
  assert.match((r as { msg: string }).msg, re);
};

test('a well-formed token round-trips and yields sub as userId', () => {
  assert.deepEqual(v.validate(signDevToken({ sub: 'u_123', exp: future }, SECRET)), {
    ok: true,
    userId: 'u_123',
  });
  // exp is optional
  assert.deepEqual(v.validate(signDevToken({ sub: 'u_123' }, SECRET)), { ok: true, userId: 'u_123' });
});

test('signatures are checked against the configured secret', () => {
  rejects(signDevToken({ sub: 'u_1', exp: future }, 'other-secret'), /signature/);
});

test('a tampered payload invalidates the signature', () => {
  const [h, , s] = signDevToken({ sub: 'u_1', exp: future }, SECRET).split('.') as [string, string, string];
  const forged = Buffer.from(JSON.stringify({ sub: 'u_admin', exp: future })).toString('base64url');
  rejects(`${h}.${forged}.${s}`, /signature/);
});

test('a truncated signature is rejected, not a crash (timingSafeEqual throws on length mismatch)', () => {
  const t = signDevToken({ sub: 'u_1', exp: future }, SECRET);
  rejects(t.slice(0, -4), /signature/);
  rejects(`${t.split('.').slice(0, 2).join('.')}.`, /signature/);
});

test('only HS256 is accepted — alg:none and friends are refused', () => {
  for (const alg of ['none', 'HS512', 'RS256']) {
    const h = Buffer.from(JSON.stringify({ alg, typ: 'JWT' })).toString('base64url');
    const p = Buffer.from(JSON.stringify({ sub: 'u_1' })).toString('base64url');
    rejects(`${h}.${p}.`, /alg/);
  }
});

test('structurally broken tokens are rejected', () => {
  rejects('', /malformed token/);
  rejects('a.b', /malformed token/);
  rejects('a.b.c.d', /malformed token/);
  rejects('!!!.###.$$$', /malformed token header/);
});

test('expiry is enforced at connect time, in seconds (§2)', () => {
  rejects(signDevToken({ sub: 'u_1', exp: Math.floor(Date.now() / 1000) - 1 }, SECRET), /expired/);
  // a millisecond timestamp in `exp` would read as the year 58000 — still a number, still accepted;
  // what must not happen is seconds being treated as milliseconds and every token looking expired.
  assert.equal(v.validate(signDevToken({ sub: 'u_1', exp: future }, SECRET)).ok, true);
  rejects(signDevToken({ sub: 'u_1', exp: 'soon' }, SECRET), /malformed token exp/);
});

test('a token without a usable sub is rejected', () => {
  rejects(signDevToken({ exp: future }, SECRET), /no sub/);
  rejects(signDevToken({ sub: '', exp: future }, SECRET), /no sub/);
  rejects(signDevToken({ sub: 42, exp: future }, SECRET), /no sub/);
});

test('the dev secret comes from RTDB_DEV_SECRET and falls back to dev-secret', () => {
  const prev = process.env['RTDB_DEV_SECRET'];
  delete process.env['RTDB_DEV_SECRET'];
  assert.equal(devSecret(), 'dev-secret');
  process.env['RTDB_DEV_SECRET'] = 'from-env';
  assert.equal(devSecret(), 'from-env');
  assert.equal(new DevHs256Validator().validate(signDevToken({ sub: 'u_1' })).ok, true);
  if (prev === undefined) delete process.env['RTDB_DEV_SECRET'];
  else process.env['RTDB_DEV_SECRET'] = prev;
});

/**
 * §5.20 Phase 1 — the `ns` claim, at the only place it is read off the wire.
 *
 * The shape of these mirrors the `role` tests above on purpose: `ns` is the second claim this
 * validator carries, and the first one taught the lesson these pin. A malformed claim is REFUSED
 * rather than dropped, because `outsideOwnDatabase` reads absence as "names no database" — so
 * dropping a bad `ns` would silently widen a token instead of rejecting it.
 */
test('a valid ns claim is carried through, and a token without one carries no ns key', () => {
  const ok = v.validate(signDevToken({ sub: 'app-car-web', ns: 'car', exp: future }, SECRET));
  assert.deepEqual(ok, { ok: true, userId: 'app-car-web', ns: 'car' });
  // No `ns` key at all, not `ns: undefined` — same standing proof the role tests rely on.
  assert.deepEqual(v.validate(signDevToken({ sub: 'u_1', exp: future }, SECRET)), {
    ok: true,
    userId: 'u_1',
  });
});

test('a malformed ns is refused, never dropped', () => {
  // Empty is the dangerous one: `isAncestorOrEqual('', path)` is true for EVERY path, so an `ns: ''`
  // that survived would be a token scoped to the whole tree while looking scoped.
  rejects(signDevToken({ sub: 'u_1', ns: '', exp: future }, SECRET), /malformed token ns/);
  rejects(signDevToken({ sub: 'u_1', ns: 42, exp: future }, SECRET), /malformed token ns/);
  // A PATH is not a database. `ns: 'car/players'` would be a claim §1 accepts as a path and this
  // must not: the product sells one top-level segment, and two grammars is how they drift apart.
  rejects(signDevToken({ sub: 'u_1', ns: 'car/players', exp: future }, SECRET), /malformed token ns/);
  rejects(signDevToken({ sub: 'u_1', ns: 'car.race', exp: future }, SECRET), /malformed token ns/);
});
