# RTDB Wire Protocol — v1.5 (FROZEN)

Status: FROZEN (signed off 2026-08-27). Any change needs a Changelog entry + version bump.
Structure: §1–§10 = v1 CORE (normative, implement all). §11 = EXTENSIONS (design-ready; implement only when explicitly scheduled).

Transport: WebSocket (RFC 6455), text frames. Encoding: JSON in v1; v2 may add MessagePack, negotiated via `hello.proto`, never assumed.
Every frame is ONE JSON object with a `type` field. Unknown fields MUST be ignored. Unknown frame types MUST be ignored (not errors). This is the forward-compatibility rule that lets Extensions ship later without breaking old SDKs.

---

## 1. Core concepts

- **path** — slash-separated tree address, no leading/trailing slash. Segments non-empty, must not contain `/ . # $ [ ]` or control characters U+0000–U+001F, U+007F (Firebase-compatible). Root = `""`. Max depth 32, max length 768 bytes.
- **rev** — per-shard, strictly increasing, gap-free, COMMIT-ORDERED integer stamped on every committed write. v1 runs one shard; everything below is written per-shard so sharding later changes deployment, not design.
- **writeId** — client-generated UUIDv4 per write. The server deduplicates on it. Retrying with the same writeId is always safe *within oplog retention (§9)*. DOCUMENTED LIMITATION: beyond retention, dedup is not guaranteed; non-idempotent operations older than retention MUST NOT be blindly retried by SDKs — surface to the app instead.
- **subId** — small positive integer, unique per active subscription per connection. Correlates `listen`/`snapshot`/`resync`/`unlisten` and sub-scoped errors ONLY. Deltas never carry subId.
- **value** — JSON value. `null` deletes. Empty objects are never stored. Arrays are opaque leaf values, Firebase-style.

## 2. Connection lifecycle

```
open socket
  -> client: hello
  <- server: helloAck            (or err + close 4401)
  -> client: listen ... (all active subs)
  -> client: pending writes (original order, original writeIds)
  ... ping/pong, writes, deltas ...
```
Client MUST NOT send anything before `hello` (violation: server close 4400).

```json
{"type":"hello","proto":1,"token":"<JWT>","sdk":"kotlin/1.0.0"}
{"type":"helloAck","rev":184223,"epoch":1,"region":"ap-south-1","session":"s_8f2k"}
{"type":"err","code":"AUTH","msg":"token expired"}
```
**epoch** — the shard's generation number, per shard, persisted with the data. Starts at 1 and is bumped ONLY when the shard's rev promise breaks: a point-in-time restore, or any reset that can move the head backwards. The client stores the epoch alongside its data. On helloAck with a DIFFERENT epoch than stored, every rev the client holds is from a dead generation: it MUST wholesale-drop all serverState mirrors, per-leaf revs, tombstones, and stored lastRevs (re-listen with no lastRev -> fresh snapshots), then store the new epoch. Pending writes replay normally — their writeIds are unknown to the restored shard, so they simply commit as new writes. Without this, §7's per-leaf LWW correctly rejects the restored (lower-rev) snapshot and the client diverges silently until app restart.
v1 validates the token at connect time only. `reauth` frame reserved for v2 (v1 servers ignore it silently; v1 SDKs never send it).

## 3. Subscriptions

### Authorization model (normative — do not "improve" this)
Read authorization is evaluated ONCE, at `listen` time. If allowed, the connection joins the path's fanout topic and **topic membership IS the authorization** for the life of the subscription. Deltas are NEVER rules-checked per recipient — a delta is encoded once and broadcast; per-delta evaluation would force per-connection encoding and destroy the fanout architecture. Consequences (accepted, consistent with connect-time auth): a rules change or ban does not affect live subscriptions until reconnect; forced revocation is done via the admin `kick` (§10), which closes the connection and causes re-evaluation on reconnect.

### listen (client -> server)
```json
{"type":"listen","subId":7,"path":"MPK_1010/1474396","lastRev":184190}
```
- No/zero `lastRev` -> server replies `snapshot`. A `lastRev` above the shard head is NOT "retained" (it was never written here — a reset or restored shard) -> fresh `snapshot`. Otherwise, with `lastRev`=R, the SERVER decides:
  - oplog still retains R AND relevant entries with rev>R number <= CATCHUP_LIMIT -> send them as `delta` frames ascending;
  - otherwise -> fresh `snapshot`.
- "Relevant" entry: entry.path at/under listen path OR listen path at/under entry.path.
- Denied -> sub-scoped err (below), no subscription created.

### Server-side listen setup order (normative — closes the subscribe/snapshot race)
1. Join the fanout topic FIRST; 2. buffer arriving deltas; 3. read the snapshot (rev N); 4. send snapshot; 5. flush buffered deltas with rev>N, discard the rest.
Snapshot atomicity (normative): the snapshot's `value` and its `rev` MUST come from one consistent read (single MVCC snapshot / one transaction over nodes+oplog head). A snapshot at rev N MUST fully include the effects of rev N — otherwise step 5's ">N" rule drops a needed delta.

### snapshot (server -> client)
```json
{"type":"snapshot","subId":7,"path":"MPK_1010/1474396","value":{"name":"Ravi","score":42},"rev":184223}
```
Client replaces the sub's serverState with `value`, sets sub.lastRev=rev. If the subtree exceeds SNAPSHOT_MAX (§9) the server sends sub-scoped err `TOOBIG` instead of a snapshot (protects mobile clients from un-completable frames; chunked snapshots are v2).

### delta (server -> client) — no subId
```json
{"type":"delta","rev":184224,"path":"MPK_1010/1474396/score","op":"put","value":50}
```
- `path` ABSOLUTE. Client routes: apply to every local sub whose path is ancestor-or-equal of `path` or descendant of `path` (prefix checks).
- `op` = `put` (replace subtree; null deletes) | `merge` (each key of `value` = child put at `path/<key>`; null child deletes). Extensions may add ops; unknown op within a delta -> client MUST treat its sub as stale and re-listen (safe under forward compatibility).
- Ancestor-delta rule: if `path` is a strict ancestor of a sub's path, extract the portion of `value` at the sub's relative path (null if absent) and apply that.
- Ordering: deltas arrive in ascending rev per connection (guaranteed by the dispatcher, §8). Because rev is shard-global, per-subscription rev sequences naturally contain gaps; gaps are NORMAL. The client MUST NOT gap-detect or gap-resync; its contract is "apply what arrives, in order, idempotently."
- Bus-loss recovery is the GATEWAY's job (§8), never the client's.

### batch (server -> client)
```json
{"type":"batch","frames":[{...delta...},{...delta...}]}
```
Micro-batching wire form (default window 20–50ms under load). Contains server->client frames only; inner frames processed in array order exactly as if received individually.

### resync (server -> client)
```json
{"type":"resync","subId":7}
```
"Your subscription went stale server-side" (send-queue overflow after coalescing, internal error, etc.). A fresh `snapshot` for that subId follows. Client: mark sub stale, do NOT clear serverState until the snapshot arrives (avoid UI flicker), then replace. This frame is the missing half of the coalescing/overflow story: without it a server can never repair a slow consumer.

### Sub-scoped err (server -> client)
```json
{"type":"err","subId":7,"code":"RULES","msg":"read denied"}
```
Codes here: `RULES`, `BADPATH`, `TOOBIG`. Terminates that subscription only.

### unlisten (client -> server)
```json
{"type":"unlisten","subId":7}
```
No reply. In-flight deltas route to no sub and are dropped.

## 4. Writes

Common fields: `writeId` (UUIDv4), `path`, `value`.

```json
{"type":"put","writeId":"0d0e...","path":"MPK_1010/1474396","value":{"name":"Ravi","score":42}}
{"type":"merge","writeId":"7aa1...","path":"MPK_1010/1474396","value":{"score":50,"stats/wins":3,"tag":null}}
{"type":"cas","writeId":"91b2...","path":"MPK_1010/1474396/score","expectedRev":184224,"value":51}
```
- `put` = wire form of BOTH setValue() and removeValue() (`value:null`). `merge` keys may be deep relative paths; all children commit atomically under ONE rev.
- `cas`: commit IFF the oplog contains NO relevant entry with rev > expectedRev for that path. (Defined on the oplog, not max(leaf revs), which is wrong under deletes.) expectedRev older than retention -> server MUST reply `casFail` with fresh state (conservative).

### Write pipeline (normative for the backend)
1. **Validate BEFORE the transaction**: rules, path syntax, size, leaf-count — pure functions, no DB. A write failing validation gets its err immediately and never enters a batch. This is what makes batching safe: one bad write cannot roll back its batch-mates.
2. **Group commit**: put/merge writes arriving within the batch window (~5ms) commit in ONE transaction; the shard's rev counter row is taken once (`UPDATE rev_counter SET v=v+N RETURNING v`) and revs are assigned from the range in arrival order. Commit-ordering is preserved; single-row-lock throughput ceiling is lifted (counter+fsync amortized over N). Ack latency budget: +batch-window per write.
3. **CAS commits SOLO — never batched.** Its check-then-commit must be atomic with rev assignment: take the counter lock FIRST, then evaluate the oplog check, then commit — this lock ordering closes the check/commit race between concurrent CAS writes. A CAS mismatch is a normal outcome (casFail), not an error, and must never abort other writes.
4. writeId dedup enforced by unique index inside the same transaction; duplicate -> return original rev as a normal `ack`.

### Server replies
```json
{"type":"ack","writeId":"0d0e...","rev":184225}
{"type":"casFail","writeId":"91b2...","path":"...","value":50,"rev":184224}
{"type":"err","writeId":"0d0e...","code":"RULES","msg":"write denied"}
```
`ack` is identical for first-commit and duplicate-replay; the client cannot and need not distinguish. Write err codes: `AUTH`,`RULES`,`BADPATH`,`BADFRAME`,`TOOBIG`,`RATE`. An err-rejected write leaves the pending queue and surfaces to the app; it is never auto-retried.

## 5. Liveness

```json
{"type":"ping","t":1756280000000}
{"type":"pong","t":1756280000000}
```
Client pings 25s foreground / 60s backgrounded; first ping right after helloAck. No pong within 10s -> close + reconnect. Server idleTimeout 70s. `t` echoed verbatim; clocks are never compared across machines — only rev orders events.

## 6. Reconnect contract (normative)

`CONNECTED -> (drop) -> WAITING -> CONNECTING -> CONNECTED`
- Backoff: delay = random(0, min(30s, 1s*2^attempt)) — full jitter; counter resets after 30s stable. Platform network-available signals short-circuit the wait.
- Auth-rejected close (4401): the client MUST NOT auto-retry with the same token. Surface auth failure to the app; reconnect only when the token source yields a new token. (Other close codes follow normal backoff.)
- On CONNECTED strictly: (1) hello, (2) all listens with stored lastRev, (3) pending writes, original order, original writeIds, (4) pings resume.
- Lost-ack: committed write whose ack died -> resend -> dedup -> original-rev ack -> queue clears. No double apply. The single most important guarantee in this protocol.
- Half-received snapshot: mirrors advance only on complete frames; next listen carries the OLD lastRev; server simply serves again.
- Multiple devices/processes per account: independent; writeId uniqueness is global via UUIDv4.

## 7. SDK semantics (normative for EVERY SDK)

### Mirror: two layers, op-typed overlay, no rollback
```
view = serverState ⊕ pendingOverlay
```
- serverState: mutated ONLY by server frames (snapshot/delta), in arrival order.
- pendingOverlay: this client's unacked writes as an ordered list of OPERATIONS `{op: put|merge|..., path, value}` — not merged values. The view is rendered by applying overlay entries as functions over serverState in issue order. (Value-only overlays break the moment any non-replacement op — e.g. Extensions' `incr` — exists; op-typing costs nothing now and is mandatory then.)
- ack -> remove entry from overlay (its effect arrives via the server echo into serverState). err/final casFail -> remove entry; the view self-corrects. There is no rollback operation; correctness falls out of the layering, including under heavy concurrent writers.

### Per-leaf rev LWW + tombstones (defense-in-depth)
serverState tracks a rev per leaf. A delta older than a leaf's recorded rev is dropped for that leaf (stale). Applying a delta (or snapshot leaf) at `path` implicitly clears any non-tombstone scalar leaf at a strict ancestor of `path` — storage keeps the tree prefix-free and never emits a delta for the ancestor it silently replaced, so the client MUST infer that clear itself. Deletes MUST leave a rev-stamped tombstone for the leaf/subtree, kept for at least oplog retention — without tombstones the defense fails exactly in its target case: a late stale delta resurrecting deleted data. When applying an ancestor `put`, every extracted leaf is stamped with that delta's rev (not just the root). The same defense applies to SNAPSHOTS: when applying a snapshot at rev N, a leaf or tombstone whose recorded rev > N is kept in preference to the snapshot's content (deltas newer than N restore full consistency) — this closes the visible rollback when an overlapping sub's setup snapshot reads at an older rev than deltas the connection already applied. This layer is DEFENSE: the dispatcher (§8) already guarantees order; this catches bugs, it does not license them.

### Events
- `onValue` (= addValueEventListener): fire with the full mirrored subtree after initial snapshot/catch-up, then after every applied change (server delta OR local optimistic write). Served from the mirror; never a network round-trip.
- `child_added/changed/removed`: derived by diffing direct children of the listened node before/after a change.
- Virtual paths: `.info/connected` (boolean).
- Threading (Android): callbacks on main thread; I/O + mirror on a background dispatcher.

## 8. Backend architecture (normative where stated)

### Fanout: transactional outbox + per-shard dispatcher (normative)
Gateways NEVER publish their own writes to the bus. Per shard, ONE dispatcher (a leader-elected gateway role; Redis lock, TTL + fencing token) tails the oplog (`WHERE rev > last ORDER BY rev`, NOTIFY-triggered poll) and publishes each entry to that shard's **Redis Stream** in rev order. Therefore stream order == commit order == rev order BY CONSTRUCTION: no reorder buffers, no cross-gateway publish races, and replay after any consumer gap is a free XRANGE from the last-delivered stream ID (fall back to the oplog if the stream has trimmed). Consuming gateways deliver to their connections in stream order, which yields §3's ordering contract. Sharding later = per-shard counter + dispatcher + stream; nothing rewrites.

### Oplog schema + indexes (decide-now, not Phase 4)
```sql
oplog(rev BIGINT PK, path TEXT, op SMALLINT, value JSONB, write_id UUID UNIQUE, ts TIMESTAMPTZ)
-- descendant queries: index on (path text_pattern_ops, rev)   [path LIKE 'p/%' AND rev > R]
-- ancestor queries: the client/gateway expands the <=32 ancestor paths and uses path = ANY(...) with (path, rev)
nodes(path TEXT PK, value JSONB, rev BIGINT)  -- materialized cache of oplog; updated in the SAME txn as the oplog insert
rev_counter(shard INT PK, v BIGINT)
```
The relevance query (`descendant OR ancestor`) runs per-listen-resume and per-CAS; both shapes above are index-served.

### Operational notes (non-negotiable in deploy review)
- Each gateway must be sized to carry 100% of load: on a gateway death the NLB moves everyone to survivors immediately (client backoff applies only after a failed attempt — a restart IS a thundering herd). Snapshot read storms on mass-reconnect: bound with a small gateway-side snapshot cache or a read replica.
- Backup/restore: PITR on Postgres, restore drill before first production migration. (A database replacement without a rehearsed restore is a resume-generating event.)

## 9. Limits (server defaults, v1)

- Frame size client->server: 1 MiB. SNAPSHOT_MAX server->client: 4 MiB (beyond -> sub-scoped TOOBIG).
- MAX_LEAVES_PER_WRITE: 2,000 flattened leaves (bounds the delete+insert work and lock hold of one write; beyond -> TOOBIG).
- Path depth 32; path length 768 B. CATCHUP_LIMIT 500. Oplog retention: 2h or last 500k revs/shard, whichever smaller. Write rate per connection: 100/s sustained, burst 500 (RATE).

## 10. Admin plane (frame spec in v1; implementation may lag)

Out-of-band admin channel (Redis) -> gateways:
```json
{"type":"kick","target":{"userId":"u_123"},"reason":"ban"}
```
Gateways close all matching connections (WS code 4403). Combined with §3's subscribe-time auth, kick+reconnect is THE enforcement point for revocation. No client-visible frame beyond the close code.

## 11. EXTENSIONS (design-ready; scheduled separately, not part of v1 core sign-off)

- **E1. get** `{"type":"get","reqId":3,"path":"...","query?":{...}}` -> `{"type":"getAck","reqId":3,"value":...,"rev":184223}`. SDK rule: if an active sub covers the path, serve from the mirror with its lastRev — zero network. Returned `rev` makes read-then-write flows CAS-able.
- **E2. Windowed queries** — `listen`/`get` accept `query:{orderBy:"key", limitToLast|limitToFirst:N}` affecting the SNAPSHOT ONLY. Deltas stay dumb (broadcast preserved); the client filters against its window and derives child_removed itself. WINDOW REFILL (do not omit): on window underflow (a windowed child deleted), the client issues a windowed `get` to backfill — the (N+1)th child is not in its mirror. v1 scope if scheduled: orderByKey only; orderByChild demands server-side indexes and is explicitly out.
- **E3. push()** — client-side key generation using Firebase's exact push-id algorithm (48-bit ms timestamp + 72-bit random, lexicographically ordered, monotonic within one client-ms). Required verbatim so keys interleave correctly with existing Firebase keys during dual-write, and so orderByKey windows sort identically.
- **E4. Server time** — helloAck gains `serverTime` (ms); SDK maintains offset; virtual path `.info/serverTimeOffset`; server-resolved timestamp placeholder in writes (`{".sv":"timestamp"}`) resolved at commit.
- **E5. incr** — write op `{"type":"incr","writeId":"...","path":"...","delta":5}`: server-side atomic add under the write pipeline (no CAS round-trips). Semantics (Firebase-match): missing leaf -> delta from 0; non-numeric leaf -> result = delta. REQUIRES §7's op-typed overlay (already core). Idempotency limitation of §1 applies with real cost here — schedule together with a policy for which paths accept client incr at all.

## 12. Changelog

- v1.5 (2026-08-28) — §2: `epoch` added to helloAck (shard generation; bumped on PITR restore/reset). Client rule: epoch change -> wholesale drop of mirrors, per-leaf revs, tombstones and lastRevs, fresh snapshots, pending writes replay normally. Closes the Gate D finding: a restored (backwards) head made §7's LWW silently diverge a live client. Added pre-Kotlin-SDK deliberately, so no SDK generation ever ships without it.
- v1.4 (2026-08-28) — two clarifications codifying behavior found necessary in Gate D chaos testing: §3 a `lastRev` above the shard head is not retained -> snapshot; §7 a delta at `path` implicitly clears a non-tombstone scalar ancestor leaf (the server never emits a delta for it).
- v1.3 (2026-08-28) — §7: per-leaf rev LWW extended to snapshot application (leaves/tombstones newer than the snapshot's rev survive it). Found during Gate C review: with per-sub setup buffering, an overlapping live sub can wire-deliver `delta(N+1), snapshot(N), delta(N+1)` — the client must not visibly roll back in between.
- v1.2 (2026-08-28) — §6: close 4401 must not be auto-retried with the same token; surface to the app, reconnect only on a new token (gap found during Gate B review — a hot retry loop with a dead token was unspecified).
- v1.1 (2026-08-28) — §1 path segments additionally forbid control characters U+0000–U+001F and U+007F (Firebase rejects them; gap found during Gate A implementation review).
- v1 (2026-08-27) — FROZEN. User sign-off on draft-3, no content changes.
- v1-draft-3 (2026-08-27) — FINAL DRAFT. Core hardening from two review rounds: subscribe-time-only read auth (topic membership = authorization; per-delta eval forbidden); fanout rebuilt as transactional outbox + per-shard dispatcher over Redis Streams (stream order == rev order by construction; replaces draft-idea of reorder buffers; sharding-safe); group-commit write pipeline with pre-txn validation and CAS-solo + counter-lock-first ordering; writeId retention limitation documented; batch/resync/sub-scoped-err frames added; snapshot MVCC atomicity rule; SNAPSHOT_MAX + MAX_LEAVES_PER_WRITE limits; op-typed pendingOverlay; per-leaf rev defense with delete tombstones + full-extraction stamping; oplog schema/index plan; admin kick frame; ops notes (100%-sizing, restore drill). Features moved to design-ready Extensions (get, windowed queries incl. window-refill, push-id, server time, incr) — scheduled separately.
- v1-draft-2 (2026-08-27): 7 fixes from first self-review (commit-ordered rev; subId out of deltas; gap-normal rule; listen setup order; oplog-based CAS; layered mirror; reauth reserved).
- v1-draft-1 (2026-08-27): initial draft.
