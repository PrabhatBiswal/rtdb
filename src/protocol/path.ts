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

/** A single path segment, for validating the keys inside a value object. */
export const validateSegment = (seg: string): string | null =>
  seg === ''
    ? 'empty key'
    : FORBIDDEN.test(seg)
      ? 'key contains one of / . # $ [ ] or a control character'
      : null;
