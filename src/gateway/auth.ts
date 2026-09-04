import { createHmac, timingSafeEqual } from 'node:crypto';

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
   */
  | { ok: true; userId: string; role?: string | null }
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
    const { sub, exp, role } = claims as { sub?: unknown; exp?: unknown; role?: unknown };

    // JWT `exp` is in SECONDS. Connect-time only: a token that expires mid-session stays valid
    // until the connection drops (§2, accepted).
    if (exp !== undefined) {
      if (typeof exp !== 'number') return { ok: false, msg: 'malformed token exp' };
      if (exp * 1000 <= Date.now()) return { ok: false, msg: 'token expired' };
    }
    if (typeof sub !== 'string' || sub === '') return { ok: false, msg: 'token has no sub' };

    // A non-string role is not a role, and a token without one yields a result with NO `role` key at
    // all — byte-identical to what this returned before §5.9 existed. That is not tidiness: it is why
    // `test/unit/auth.test.ts`'s round-trip assertion still passes UNMODIFIED, which is a cheap
    // standing proof that app tokens are untouched by the console's role plumbing.
    return typeof role === 'string' ? { ok: true, userId: sub, role } : { ok: true, userId: sub };
  }
}
