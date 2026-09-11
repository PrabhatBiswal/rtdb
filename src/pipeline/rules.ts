import type { Json } from '../protocol/frames.ts';
import { isAncestorOrEqual } from '../protocol/path.ts';

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

/**
 * Every role the console mints, write-capable or not. `WRITE_ROLES` is the subset that may write;
 * this is the wider set that says "this token came from our console at all", and §5.20's read
 * exemption keys off it. Kept beside `WRITE_ROLES` rather than derived from it because they answer
 * different questions and the day a fourth role appears they will not move together.
 */
const CONSOLE_ROLES = new Set(['owner', 'editor', 'viewer']);

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

/**
 * §5.20 Phase 1: a token's database is a wall, and this is the wall.
 *
 * It sits HERE, beside `consoleWriteDenied`, for the reason that note gives and for a second one
 * that is stronger now than it was then. The first: a guard inside a `Rules` implementation has
 * "somebody forgot to wire it" as its failure mode. `RTDB_RULES` being fail-closed on Postgres
 * (`main.ts`) narrows that but does not close it — the guard proves a rules module was LOADED, never
 * that it enforces anything, and it is silent for memory storage. The second, and this is the one
 * that settles it: `RTDB_RULES` points at a module the OPERATOR wrote. Tenant isolation cannot be
 * delegated to the party it constrains. So the write pipeline and `listen` consult this BEFORE the
 * configured rules, and no `Rules` function can vote it down.
 *
 * READS ARE IN, not just writes. §3 authorizes a subscription once, at `listen`, and from then on
 * topic membership IS the authorization — so a token that may `listen` outside its database gets
 * every future delta of a database it does not own, forever, without another check. An isolation
 * promise that only covered writes would be false on the read side and unrecoverable on it.
 *
 * ROOT IS REFUSED, and by this predicate rather than by a limit. `listen ""` is refused today only
 * when the resulting snapshot exceeds `SNAPSHOT_MAX` — which is a CAPACITY answer, and on a small or
 * freshly reset shard it does not fire at all. `isAncestorOrEqual('car', '')` is false, so a
 * confined token is refused at the root because the root is not inside its database. Nothing about
 * that answer moves when a limit does.
 *
 * ABSENCE IS NOT PERMISSION. A token with no `ns` is refused under `requireNs`, and that is the
 * whole reason the option exists: every mint — /login, /shadow-token, /app-token,
 * `scripts/console-token.ts`, `signDevToken` — signs with the SAME shard secret, so if "no `ns`"
 * meant "every database", the weakest of those mints would be an unconfined token. That has already
 * happened once on this exact shape: a roleless 24h device token was accepted by /stats and
 * /topnodes until a KNOWN role was required (`console/auth-server.mjs`). Same trap, same answer.
 *
 * Console subjects are exempt and only they are: they administer every database, they have no one
 * database, and §5.9's invariant above already decides what they may write.
 *
 * BUT THE EXEMPTION COSTS A KNOWN ROLE (checkpoint #1, R1). `consoleWriteDenied` demands one; this
 * did not, and the gap was real: a `console-…` subject carrying NO role read every database under
 * `requireNs`, because the exemption keyed off the subject PREFIX alone and a prefix is not a
 * credential. Requiring a known role breaks nothing that exists — no mint produces a roleless
 * `console-` subject (`/login` always sends one, `scripts/console-token.ts` defaults to `owner` and
 * validates it, `/shadow-token`'s subject is a `deviceSlug`, `/app-token`'s is `app-…`) — and it is
 * the same medicine `console/auth-server.mjs` already applied to itself, where a roleless 24h
 * device token was reaching /stats and /topnodes until a KNOWN role was required.
 *
 * The role is demanded only under `requireNs`, and that is not a hedge. With the switch off an
 * unscoped token of ANY subject already reads the whole tree, so demanding a role there would not
 * close a hole — it would invent a refusal that no other subject faces. Off means unchanged.
 */
export interface DatabaseCtx {
  userId: string;
  /** The token's `ns` claim, or null/absent when it carries none. */
  ns?: string | null;
  /** The token's console role (§5.8). What buys a `console-…` subject its exemption. */
  role?: string | null;
  path: string;
  /**
   * Refuse a token that names no database. Off, an unscoped token behaves exactly as it did before
   * this function existed — which is what keeps the single-database deployment (and every test that
   * assumes it) unchanged. A token that DOES name a database is confined either way.
   */
  requireNs?: boolean;
}

/** True when this operation must be refused because it leaves the token's own database. */
export function outsideOwnDatabase(ctx: DatabaseCtx): boolean {
  if (ctx.userId.startsWith(CONSOLE_PREFIX)) {
    return ctx.requireNs === true && !CONSOLE_ROLES.has(ctx.role ?? '');
  }
  const ns = ctx.ns ?? '';
  if (ns === '') return ctx.requireNs === true;
  // `isAncestorOrEqual` and not `startsWith(ns)`: the bare prefix test lets a `car` token reach
  // `car-race`, which is the trap `rules/own-subtree.ts:32` already carries for `u_1` and `u_12`.
  // One predicate, and it is §3's own (`path.ts`), so relevance and authorization cannot drift.
  return !isAncestorOrEqual(ns, ctx.path);
}
