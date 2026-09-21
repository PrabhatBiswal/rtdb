import type { Limits } from './limits.ts';

/** §1 (v1.1): segments must not contain / . # $ [ ] nor control chars U+0000-U+001F, U+007F. */
const FORBIDDEN = /[./#$[\]\u0000-\u001f\u007f]/;

/**
 * §1 path validation. Returns null when valid, else a human reason (caller maps to BADPATH).
 * Root is "" (depth 0). No leading/trailing/double slash — those show up as empty segments.
 */
export function validatePath(path: unknown, limits: Limits): string | null {
  if (typeof path !== 'string') return 'path must be a string';
  if (path === '') return null;
  if (Buffer.byteLength(path, 'utf8') > limits.MAX_PATH_BYTES) {
    return `path longer than ${limits.MAX_PATH_BYTES} bytes`;
  }
  const segs = path.split('/');
  if (segs.length > limits.MAX_PATH_DEPTH) return `path deeper than ${limits.MAX_PATH_DEPTH} segments`;
  for (const s of segs) {
    if (s === '') return 'empty path segment';
    if (FORBIDDEN.test(s)) return 'path segment contains one of / . # $ [ ] or a control character';
  }
  return null;
}

/** Segments of a path; root ("") has none. */
export const segments = (path: string): string[] => (path === '' ? [] : path.split('/'));

/** Join an absolute base with a relative path (either may be ""). */
export const joinPath = (base: string, rel: string): string =>
  base === '' ? rel : rel === '' ? base : `${base}/${rel}`;

/** Is `a` the same path as `b`, or an ancestor of it? Root ("") is an ancestor of everything. */
export const isAncestorOrEqual = (a: string, b: string): boolean =>
  a === '' || a === b || b.startsWith(`${a}/`);

/**
 * §3 relevance: one path is at-or-under the other. Used identically by the oplog catch-up query,
 * the CAS conflict check, and delta routing — one predicate, three call sites.
 */
export const isRelevant = (a: string, b: string): boolean =>
  isAncestorOrEqual(a, b) || isAncestorOrEqual(b, a);

/** `path` and all its ancestors, root first (at most 33 entries — see §8's ancestor query). */
export function ancestorsInclusive(path: string): string[] {
  const out = [''];
  const segs = segments(path);
  for (let i = 0; i < segs.length; i++) out.push(segs.slice(0, i + 1).join('/'));
  return out;
}

/** `path` expressed relative to `ancestor` ("" when they are equal). Assumes isAncestorOrEqual. */
export const relativePath = (ancestor: string, path: string): string =>
  ancestor === '' ? path : path.slice(ancestor.length + 1);

/**
 * §5.22 Gate E: database names beginning with `_` are RESERVED, and the reason is not in this file.
 *
 * `metrics.ts` mints synthetic label values in the same namespace as real database names —
 * `_default` for a gateway that has not been told which database it serves, `_other` past the
 * 64-label cardinality cap, `_root` and `_none` on the path-prefix label. Every one of those is a
 * name `validatePath` accepts today: it forbids only `/ . # $ [ ]` and control characters. So a
 * client could declare a database called `_default` and find its connections, its lag and its
 * leadership silently merged into the synthetic bucket — and a usage panel is where a client's bill
 * is decided.
 *
 * Refusing the NAME rather than escaping the label is the cheaper half: `_other` already carries
 * this collision today for path prefixes, and one reserved character costs a client nothing.
 *
 * It lives here, beside §1's own validator, because it is a rule about NAMES and both the admin
 * route and the storage adapter have to reach it. One rule, one home, two callers — a second copy
 * next to the first is how two ideas of a legal name are born.
 */
export const RESERVED_NAME_PREFIX = '_';

/**
 * §5.26: a database name is also a POSTGRES SCHEMA NAME and a NOTIFY CHANNEL, and those are
 * stricter than a path segment. The rule lives HERE, with the name rule it belongs to, and
 * `postgres.ts` imports it — because the alternative is what this project already ran into.
 *
 * `TenantAlpha` passed `validateDatabaseName` and was refused by the storage adapter's own
 * guard, so a client could DECLARE it, MINT a token for it, and only then have hello fail with
 * 1011 at the factory — leaving a registry row that **cannot be deleted**, because §5.19 gave
 * declaring no inverse on purpose. Two validators, one name, and the disagreement surfaced three
 * steps later as an internal error.
 *
 * Lowercase because Postgres folds an unquoted identifier to lowercase: `CREATE SCHEMA Car` makes
 * `car`, so accepting `Car` would mean the registry and the catalogue disagree about a database's
 * name forever. No hyphens for the same family of reasons — `car-race` is not an identifier and
 * would have to be quoted at every interpolation site, which is a rule to remember rather than a
 * character to refuse.
 */
export const SCHEMA_NAME = /^[a-z_][a-z0-9_]*$/;

/**
 * 51 = 63 − `'rtdb_commit_'`. Postgres truncates identifiers at 63 bytes SILENTLY, and a database
 * name becomes `rtdb_commit_<name>` (§5.22 Gate C), so two long names sharing a prefix would land
 * on ONE channel and cross-wake each other's dispatchers. The number is derived, not chosen.
 */
export const MAX_DATABASE_NAME = 63 - 'rtdb_commit_'.length;

/** Null when `name` may be declared as a database, else a human reason. */
export function validateDatabaseName(name: unknown, limits: Limits): string | null {
  if (typeof name !== 'string' || name === '') return 'database name is required';
  if (name.includes('/')) return 'a database name is exactly one path segment';
  if (name.startsWith(RESERVED_NAME_PREFIX)) {
    return `names beginning with "${RESERVED_NAME_PREFIX}" are reserved`;
  }
  const bad = validatePath(name, limits);
  if (bad) return bad;
  // The two rules the STORAGE has always enforced, moved to where the name is decided. Said in
  // words rather than by showing the regex: a client reading this has to be able to fix the name.
  if (!SCHEMA_NAME.test(name)) {
    return 'a database name may use lowercase letters, digits and underscores, and may not start with a digit';
  }
  if (name.length > MAX_DATABASE_NAME) {
    return `a database name may be at most ${MAX_DATABASE_NAME} characters`;
  }
  return null;
}

/** A single path segment, for validating the keys inside a value object. */
export const validateSegment = (seg: string): string | null =>
  seg === ''
    ? 'empty key'
    : FORBIDDEN.test(seg)
      ? 'key contains one of / . # $ [ ] or a control character'
      : null;
