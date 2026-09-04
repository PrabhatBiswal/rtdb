/**
 * A watchable churn generator for `demo/room` — the half of a load pass a HUMAN looks at.
 *
 * The heavy rig (scripts/loadsim.ts) hammers `sim/*` at a rate no eye can follow. This does the
 * opposite on purpose: a few dozen operations a second against a SMALL, BOUNDED subtree, so the
 * console's tree visibly ticks — hp counting down, inventories reshuffling, players joining and
 * leaving — while the fleet is under real load elsewhere.
 *
 *   RTDB_DEV_SECRET=<jwt secret> node --import tsx scripts/demo-churn.ts [flags]
 *
 *   --url URL       gateway to write through   (default ws://127.0.0.1:8080)
 *   --rate N        operations per second      (default 40)
 *   --seconds S     run length, 0 = forever    (default 0)
 *   --slots N       player/member slots        (default 13, of which 12 are ever live)
 *   --seed          create the roster and exit
 *   --clean         delete demo/room entirely and exit
 *
 * SHAPE. Each player has eleven direct children and the tree goes five levels below `players`:
 *
 *   players/p03/inventory/bag2/item1/qty          <- depth 5, and every level churns
 *
 * BOUNDED, and this is the property that matters because a browser is subscribed here. The roster is
 * a fixed set of slot ids and churn REPLACES a slot's occupant; it never appends. Twelve live players
 * of ~34 nodes each is ~410 nodes under `players`, which is where it stays forever. A subtree that
 * grew without limit would eventually make every fresh listen a TOOBIG and the watcher would simply
 * see the tree stop.
 *
 * SMOOTH. ~40 ops/s is a rate a person can read. Faster is not more impressive, it is a blur.
 */
import { RtdbClient } from '../harness/client.ts';
import { signDevToken } from '../src/gateway/auth.ts';

const flag = (name: string, fallback: string): string => {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? fallback : (process.argv[i + 1] ?? fallback);
};
const has = (name: string): boolean => process.argv.includes(`--${name}`);

const URL_ = flag('url', 'ws://127.0.0.1:8080');
const RATE = Number(flag('rate', '40'));
const SECONDS = Number(flag('seconds', '0'));
/** One more slot than we ever fill, so a replacement can be ADDED before its predecessor is removed. */
const SLOTS = Number(flag('slots', '13'));
const LIVE_TARGET = SLOTS - 1;

const ROOT = 'demo/room';
const PLAYERS = `${ROOT}/players`;
const pad = (n: number): string => String(n).padStart(2, '0');
const playerSlots = Array.from({ length: SLOTS }, (_, i) => `p${pad(i + 1)}`);
const memberSlots = Array.from({ length: SLOTS }, (_, i) => `m${pad(i + 1)}`);

/** p01 is the user's stopwatch: it holds the banana score and is never evicted or reshuffled. */
const BANANA = 'p01';

const NAMES = ['ada', 'linus', 'grace', 'rob', 'ken', 'barbara', 'edsger', 'alan', 'margaret',
  'donald', 'niklaus', 'john', 'leslie', 'tony', 'guido', 'bjarne', 'anders', 'james'];
const WEAPONS = ['sword', 'bow', 'staff', 'axe', 'dagger', 'spear'];
const ARMOUR = ['leather', 'chain', 'plate', 'cloth', 'scale'];
const STATUS = ['online', 'idle', 'away', 'busy'];
const RARITY = ['common', 'rare', 'epic', 'legendary'];
const GUILDS = ['ravens', 'foxes', 'owls', 'wolves'];
const BAGS = ['bag1', 'bag2'];
const ITEMS = ['item1', 'item2', 'item3'];

const pick = <T,>(a: readonly T[]): T => a[Math.floor(Math.random() * a.length)]!;
const int = (lo: number, hi: number): number => lo + Math.floor(Math.random() * (hi - lo + 1));

const livePlayers = new Set<string>();
const liveMembers = new Set<string>();

if (!process.env['RTDB_DEV_SECRET']) {
  console.error("RTDB_DEV_SECRET must hold the shard's jwt secret — refusing to mint an unsigned token");
  process.exit(2);
}

const token = signDevToken({ sub: 'u_demo_churn', exp: Math.floor(Date.now() / 1000) + 6 * 3600 });
const c = new RtdbClient({ url: URL_, token, pingIntervalMs: 20_000 });

// ---------------------------------------------------------------- forensics
// Watch Test 1 ended with this writer frozen — ops AND errors both stopped, so writes were being
// issued that neither acked nor errored — and nothing here logged enough to say whether the socket
// had died or the writes had simply never settled. That gap is closed: connection state, close
// codes and pending depth are all recorded now, with a stamp, as they happen.
const stamp = (): string => new Date().toISOString().slice(11, 23);
let lastState = '';
c.on('state', (s: unknown) => {
  const v = String(s);
  if (v !== lastState) { console.log(`${stamp()}  STATE ${lastState || '-'} -> ${v}`); lastState = v; }
});
c.on('close', (d: unknown) => {
  const { code, reason } = (d ?? {}) as { code?: number; reason?: string };
  console.log(`${stamp()}  CLOSE code=${code} reason=${JSON.stringify(reason ?? '')} pending=${c.pendingWriteIds.length}`);
});
c.on('authFailure', (d: unknown) => console.log(`${stamp()}  AUTH FAILURE ${JSON.stringify(d)}`));
c.on('epochChange', () => console.log(`${stamp()}  EPOCH CHANGE — mirror dropped`));
c.on('resync', (f: unknown) => console.log(`${stamp()}  RESYNC ${JSON.stringify(f)}`));

c.connect();
const ack = await c.ready();
console.log(`churn -> ${URL_}  epoch=${ack.epoch} head=${ack.rev}`);

// ---------------------------------------------------------------- the shape
/** Eleven direct children, and an inventory that goes four levels deeper. */
function playerBody(): Record<string, unknown> {
  const inventory: Record<string, unknown> = {};
  for (const bag of BAGS) {
    const items: Record<string, unknown> = {};
    for (const it of ITEMS) items[it] = { qty: int(1, 99), rarity: pick(RARITY) };
    inventory[bag] = items;
  }
  return {
    name: pick(NAMES), hp: int(40, 100), score: int(0, 500), level: int(1, 60),
    mana: int(0, 200), stamina: int(0, 100), gold: int(0, 9999),
    status: pick(STATUS), guild: pick(GUILDS),
    gear: { weapon: pick(WEAPONS), armor: pick(ARMOUR) },
    inventory,
  };
}

const newPlayer = async (id: string): Promise<void> => {
  await c.put(`${PLAYERS}/${id}`, playerBody() as never);
  livePlayers.add(id);
};
const newMember = async (id: string): Promise<void> => {
  await c.put(`${ROOT}/members/${id}`, { name: pick(NAMES), status: pick(STATUS) });
  liveMembers.add(id);
};

if (has('clean')) {
  await c.put(ROOT, null);
  console.log('demo/room deleted');
  process.exit(0);
}

console.log(`seeding ${LIVE_TARGET} players + ${LIVE_TARGET} members under ${ROOT} ...`);
await c.put(ROOT, null);
await Promise.all(playerSlots.slice(0, LIVE_TARGET).map(newPlayer));
await Promise.all(memberSlots.slice(0, LIVE_TARGET).map(newMember));
let banana = 0;
await c.put(`${PLAYERS}/${BANANA}/banana_score`, banana);
console.log(`seeded. ~${LIVE_TARGET * 34} nodes under ${PLAYERS}, depth 5, `
  + `banana_score on ${BANANA}. SNAPSHOT_MAX is 4 MiB — this is kilobytes.`);
if (has('seed')) process.exit(0);

// ---------------------------------------------------------------- the churn
let ops = 0, errs = 0;
const started = Date.now();
const evictable = (): string[] => [...livePlayers].filter((s) => s !== BANANA);

async function oneOp(): Promise<void> {
  const roll = Math.random();

  if (roll < 0.22) {                                        // depth 2 — hp/mana/stamina tick
    const id = pick([...livePlayers]);
    if (id) await c.put(`${PLAYERS}/${id}/${pick(['hp', 'mana', 'stamina'])}`, int(1, 200));

  } else if (roll < 0.36) {                                 // depth 2 — score / gold / level
    const id = pick([...livePlayers]);
    if (id) await c.put(`${PLAYERS}/${id}/${pick(['score', 'gold', 'level'])}`, int(0, 9999));

  } else if (roll < 0.48) {                                 // depth 3 — gear
    const id = pick([...livePlayers]);
    if (id) {
      await (Math.random() < 0.5
        ? c.put(`${PLAYERS}/${id}/gear/weapon`, pick(WEAPONS))
        : c.put(`${PLAYERS}/${id}/gear/armor`, pick(ARMOUR)));
    }

  } else if (roll < 0.66) {                                 // depth 5 — the deepest leaves
    const id = pick([...livePlayers]);
    if (id) {
      const p = `${PLAYERS}/${id}/inventory/${pick(BAGS)}/${pick(ITEMS)}`;
      await (Math.random() < 0.6
        ? c.put(`${p}/qty`, int(1, 99))
        : c.put(`${p}/rarity`, pick(RARITY)));
    }

  } else if (roll < 0.76) {                                 // depth 4 — a whole item appears/vanishes
    const id = pick(evictable());
    if (id) {
      const p = `${PLAYERS}/${id}/inventory/${pick(BAGS)}/${pick(ITEMS)}`;
      await (Math.random() < 0.5
        ? c.put(p, null)                                    // item destroyed
        : c.put(p, { qty: int(1, 99), rarity: pick(RARITY) }));
    }

  } else if (roll < 0.84) {                                 // depth 3 — a whole bag is re-rolled
    const id = pick(evictable());
    if (id) {
      const items: Record<string, unknown> = {};
      for (const it of ITEMS) items[it] = { qty: int(1, 99), rarity: pick(RARITY) };
      await c.put(`${PLAYERS}/${id}/inventory/${pick(BAGS)}`, items as never);
    }

  } else if (roll < 0.92) {                                 // a player is replaced — ADD FIRST
    const free = playerSlots.filter((s) => !livePlayers.has(s));
    const gone = pick(evictable());
    // Add before remove, using the spare slot. Watch Test 1 did it the other way and a failed add
    // after a successful delete leaked the slot forever — the roster bled from 14 down to 4.
    if (free.length) await newPlayer(pick(free));
    if (gone && livePlayers.size > LIVE_TARGET) {
      await c.put(`${PLAYERS}/${gone}`, null);
      livePlayers.delete(gone);
    }

  } else {                                                  // same, for members
    const free = memberSlots.filter((s) => !liveMembers.has(s));
    const gone = pick([...liveMembers]);
    if (free.length) await newMember(pick(free));
    if (gone && liveMembers.size > LIVE_TARGET) {
      await c.put(`${ROOT}/members/${gone}`, null);
      liveMembers.delete(gone);
    }
  }
}

const tick = setInterval(() => {
  void oneOp().then(() => { ops++; }).catch(() => { errs++; });
}, Math.max(1, Math.round(1000 / RATE)));

// 🍌 — a number the user can watch climb. Read-modify-write on a counter we hold; there is no
// server-side increment and §11 is out of scope.
const bananaTimer = setInterval(() => {
  banana++;
  void c.put(`${PLAYERS}/${BANANA}/banana_score`, banana).catch(() => { errs++; });
}, 1000);

/** Refill anything that went missing, so a bad minute cannot permanently shrink the roster. */
const reconcile = setInterval(() => {
  void (async () => {
    for (const s of playerSlots) {
      if (livePlayers.size >= LIVE_TARGET) break;
      if (!livePlayers.has(s)) await newPlayer(s).catch(() => { errs++; });
    }
    for (const s of memberSlots) {
      if (liveMembers.size >= LIVE_TARGET) break;
      if (!liveMembers.has(s)) await newMember(s).catch(() => { errs++; });
    }
  })();
}, 5000);

const report = setInterval(() => {
  const s = (Date.now() - started) / 1000;
  console.log(`${stamp()}  ${ops} ops in ${s.toFixed(0)}s (${(ops / s).toFixed(1)}/s), ${errs} err, `
    + `${livePlayers.size}p+${liveMembers.size}m, banana=${banana}, `
    + `state=${c.state}, pending=${c.pendingWriteIds.length}`);
}, 10_000);

const stop = (why: string): void => {
  clearInterval(tick); clearInterval(report); clearInterval(bananaTimer); clearInterval(reconcile);
  const s = (Date.now() - started) / 1000;
  console.log(`${why}: ${ops} ops in ${s.toFixed(0)}s (${(ops / s).toFixed(1)}/s), ${errs} errors, `
    + `banana=${banana}, final state=${c.state}, pending=${c.pendingWriteIds.length}`);
  process.exit(0);
};
if (SECONDS > 0) setTimeout(() => stop('done'), SECONDS * 1000);
process.on('SIGINT', () => stop('interrupted'));
