import type { Json } from '../protocol/frames.ts';

export interface RuleCtx {
  userId: string;
  op: 'read' | 'put' | 'merge' | 'cas';
  path: string;
  value?: Json;
  /** The token's console role (§5.8), or null for the app tokens that carry none. */
  role?: string | null;
}

/** Called at §4 step 1 for writes and once per `listen` for reads (§3) — never per delta. */
export type Rules = (ctx: RuleCtx) => boolean;

/** Dev default. A real rules language is a later package (WORKLOAD §4). */
export const allowAll: Rules = () => true;

/**
 * §5.9's law, and the reason it is HERE and not in a `Rules` implementation.
 *
 * The console can edit the tree. What stops a viewer's session from writing is not the absence of a
 * button — it is this, on the wire, in the gateway. So it is an INVARIANT, not a policy: the write
 * pipeline consults it before the configured rules and no `Rules` function can vote it down.
 * Production runs `allowAll` (main.ts passes no rules at all), so a guard that lived inside a rules
 * implementation would have "somebody forgot to wire it" as its failure mode, and that failure mode
 * is silently-open console writes. main.ts needing no change is the proof it cannot be forgotten.
 *
 * TWO things must hold for a console session to write, not one:
 *
 *   - the token's `role` claim is `editor` or `owner`; and
 *   - the subject is `console-rw-…`, the deliberate-unlock subject minted only for those roles
 *     (§5.9 Gate B; the old console Gate A Q2 ruling asked for a distinct subject precisely so §10's
 *     kick can name write-capable sessions as a class).
 *
 * Demanding both costs nothing and buys two things: a kick that targets exactly the write-capable
 * sessions, and the refusal of a token claiming `role: editor` on a plain `console-` subject — which
 * our auth-server cannot mint, and which is therefore either a bug or a forgery. Neither deserves a
 * write.
 *
 * App tokens are untouched, and that is a statement about BLAST RADIUS: a subject that is not
 * `console-…` never reaches the role test at all. app traffic cannot be affected by this
 * function, because it returns false before looking at anything else.
 */
const CONSOLE_PREFIX = 'console-';
const CONSOLE_RW_PREFIX = 'console-rw-';
const WRITE_ROLES = new Set(['editor', 'owner']);

export interface ConsoleWriteCtx {
  userId: string;
  role?: string | null;
  op: RuleCtx['op'];
}

/** True when this write must be refused because it comes from a console session that may not write. */
export function consoleWriteDenied(ctx: ConsoleWriteCtx): boolean {
  if (ctx.op === 'read') return false;
  // The blast-radius line. Everything below it concerns our own console and nothing else.
  if (!ctx.userId.startsWith(CONSOLE_PREFIX)) return false;
  return !(ctx.userId.startsWith(CONSOLE_RW_PREFIX) && WRITE_ROLES.has(ctx.role ?? ''));
}
