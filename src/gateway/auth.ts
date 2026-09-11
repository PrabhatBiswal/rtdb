import { createHmac, timingSafeEqual } from 'node:crypto';
import { validateDatabaseName } from '../protocol/path.ts';
import { DEFAULT_LIMITS } from '../protocol/limits.ts';

/**
 * §2: the token is validated ONCE, at connect time. `reauth` is reserved for v2.
 *
 * This interface exists because a swap is planned (WORKLOAD §0.6): dev HS256 now,
 * production JWKS later. Nothing else about auth is pluggable.
 */
export type AuthResult =
  /**
   * `role` is the console's role claim (§5.8), carried through so the gateway can decide a write on
   * it (§5.9). It is optional and null for every app token, which have never had one and are not
   * meant to grow one: this is a claim OUR console mints for OUR console's sessions.
   *
   * `ns` is §5.20 Phase 1's database claim: the ONE top-level segment this token may touch. It is a
   * single path segment, never a path — a token confined to a subtree deeper than a database is not
   * a thing the product sells, and allowing one here would put a second path grammar next to §1's.
   * Absent means the token names no database, and what that MEANS is decided in `rules.ts`, not
   * here: this file reports the claim, the invariant decides.
   */
  | { ok: true; userId: string; role?: string | null; ns?: string | null }
  | { ok: false; msg: string };

export interface AuthValidator {
  validate(token: string): AuthResult | Promise<AuthResult>;
}

export const devSecret = (): string => process.env['RTDB_DEV_SECRET'] ?? 'dev-secret';

const b64u = (b: Buffer): string => b.toString('base64url');
const sig = (signingInput: string, secret: string): Buffer =>
  createHmac('sha256', secret).update(signingInput).digest();

/** Dev-only token minting, for the harness and tests. Production tokens come from the app's IdP. */
export function signDevToken(
  claims: Record<string, unknown>,
  secret: string = devSecret(),
): string {
  const head = b64u(Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })));
  const body = b64u(Buffer.from(JSON.stringify(claims)));
  return `${head}.${body}.${b64u(sig(`${head}.${body}`, secret))}`;
}

/** HS256 JWT verification with node:crypto only — no JWT library (WORKLOAD §3). */
export class DevHs256Validator implements AuthValidator {
  constructor(private readonly secret: string = devSecret()) {}

  validate(token: string): AuthResult {
    const parts = token.split('.');
    if (parts.length !== 3) return { ok: false, msg: 'malformed token' };
    const [head, body, mac] = parts as [string, string, string];

    let header: unknown;
    try {
      header = JSON.parse(Buffer.from(head, 'base64url').toString('utf8'));
    } catch {
      return { ok: false, msg: 'malformed token header' };
    }
    if (typeof header !== 'object' || header === null || (header as { alg?: unknown }).alg !== 'HS256') {
      return { ok: false, msg: 'unsupported token alg' };
    }

    const expected = sig(`${head}.${body}`, this.secret);
    const got = Buffer.from(mac, 'base64url');
    // timingSafeEqual throws on length mismatch, so the cheap length check has to come first.
    if (got.length !== expected.length || !timingSafeEqual(got, expected)) {
      return { ok: false, msg: 'bad signature' };
    }

    let claims: unknown;
    try {
      claims = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    } catch {
      return { ok: false, msg: 'malformed token claims' };
    }
    if (typeof claims !== 'object' || claims === null) return { ok: false, msg: 'malformed token claims' };
    const { sub, exp, role, ns } = claims as {
      sub?: unknown;
      exp?: unknown;
      role?: unknown;
      ns?: unknown;
    };

    // JWT `exp` is in SECONDS. Connect-time only: a token that expires mid-session stays valid
    // until the connection drops (§2, accepted).
    if (exp !== undefined) {
      if (typeof exp !== 'number') return { ok: false, msg: 'malformed token exp' };
      if (exp * 1000 <= Date.now()) return { ok: false, msg: 'token expired' };
    }
    if (typeof sub !== 'string' || sub === '') return { ok: false, msg: 'token has no sub' };

    // A malformed `ns` is REFUSED, not dropped. Dropping would be quietly fail-open under §5.20's
    // rollout: `outsideOwnDatabase` reads absence as "this token names no database", so a typo in a
    // minted claim would turn a confined token into an unconfined one.
    //
    // §5.22 Gate D: `validateDatabaseName`, the same rule the registry and the admin route use —
    // its fourth caller, and the reason is the token is the DOOR. `validateSegment` accepted
    // `_default`, so a token could name a synthetic metrics label and be handed to a tenant factory
    // as if it were a database. Checking it here means `tenantFor`'s input is already trusted and
    // the map needs no guard of its own.
    //
    // Whether the database EXISTS is deliberately not asked: that is the mint's job (`/app-token`
    // refuses an undeclared name), and hello is not a place to spend a registry round trip.
    if (ns !== undefined && ns !== null) {
      if (validateDatabaseName(ns, DEFAULT_LIMITS) !== null) {
        return { ok: false, msg: 'malformed token ns' };
      }
    }

    // A non-string role is not a role, and a token without one yields a result with NO `role` key at
    // all — byte-identical to what this returned before §5.9 existed. That is not tidiness: it is why
    // `test/unit/auth.test.ts`'s round-trip assertion still passes UNMODIFIED, which is a cheap
    // standing proof that app tokens are untouched by the console's role plumbing.
    return {
      ok: true,
      userId: sub,
      ...(typeof role === 'string' ? { role } : {}),
      ...(typeof ns === 'string' ? { ns } : {}),
    };
  }
}
