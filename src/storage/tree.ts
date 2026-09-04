import type { Json } from '../protocol/frames.ts';
import type { Limits } from '../protocol/limits.ts';
import { joinPath, relativePath, segments, validateSegment } from '../protocol/path.ts';

/** The stored unit: a flattened leaf path and its value (PROJECT_STATE: "flattened leaf paths"). */
export interface Leaf {
  path: string;
  value: Json;
}

export type FlattenResult = { ok: true; leaves: Leaf[] } | { ok: false; msg: string; tooBig?: true };

/**
 * Flatten a write's value into leaves (§1).
 *  - `null` and `{}` produce NO leaves — both mean "nothing is stored here" ("empty objects are
 *    never stored", and null deletes). The caller treats an empty result as a delete of the subtree.
 *  - arrays are opaque leaf values, Firebase-style.
 *  - object keys are validated as single path segments; a key with a `/` or a `.` is BADPATH, not
 *    silent nesting.
 * Stops as soon as MAX_LEAVES_PER_WRITE is exceeded, so an oversized write costs bounded work (§9).
 */
export function flatten(path: string, value: Json, limits: Limits): FlattenResult {
  const leaves: Leaf[] = [];

  const walk = (at: string, v: Json): string | null => {
    if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
      for (const [key, child] of Object.entries(v)) {
        const keyErr = validateSegment(key);
        if (keyErr) return `${keyErr} at "${at}"`;
        const err = walk(joinPath(at, key), child as Json);
        if (err) return err;
      }
      return null;
    }
    if (v === null) return null; // a null anywhere in the value means "no leaf here"
    if (leaves.length >= limits.MAX_LEAVES_PER_WRITE) return 'TOOBIG';
    leaves.push({ path: at, value: v });
    return null;
  };

  const err = walk(path, value);
  if (err === 'TOOBIG') {
    return { ok: false, msg: `write exceeds ${limits.MAX_LEAVES_PER_WRITE} leaves`, tooBig: true };
  }
  if (err) return { ok: false, msg: err };
  return { ok: true, leaves };
}

/** Rebuild the JSON value rooted at `root` from its leaves. No leaves -> null (§1). */
export function unflatten(root: string, leaves: Leaf[]): Json {
  if (leaves.length === 0) return null;
  // A leaf sitting exactly at the root means the root itself is a scalar or an array.
  const exact = leaves.find((l) => l.path === root);
  if (exact) return exact.value;

  const out: { [k: string]: Json } = {};
  for (const leaf of leaves) {
    const segs = segments(relativePath(root, leaf.path));
    let node = out;
    for (let i = 0; i < segs.length - 1; i++) {
      const key = segs[i] as string;
      const next = node[key];
      if (next === undefined || next === null || typeof next !== 'object' || Array.isArray(next)) {
        node = node[key] = {};
      } else {
        node = next as { [k: string]: Json };
      }
    }
    node[segs[segs.length - 1] as string] = leaf.value;
  }
  return out;
}
