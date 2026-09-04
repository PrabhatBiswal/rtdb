package com.hobostays.rtdb.core

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/**
 * §7: the overlay holds OPERATIONS, not merged values — a value-only overlay breaks the moment a
 * non-replacement op exists (§11's `incr`), and op-typing costs nothing now.
 */
data class OverlayOp(val op: String, val path: String, val value: JsonElement)

private data class Cell(
    val rev: Long,
    /** A rev-stamped tombstone (§7). Without these, a late stale delta resurrects deleted data. */
    val deleted: Boolean,
    val value: JsonElement,
)

/**
 * §7 mirror: `view = serverState ⊕ pendingOverlay`. A 1:1 port of WP1's `harness/mirror.ts`
 * (WORKLOAD §4: port the semantics, do not redesign them).
 *  - serverState is mutated ONLY by server frames, in arrival order;
 *  - the overlay is this client's unacked writes, applied as functions over serverState in issue
 *    order — there is no rollback, correctness falls out of the layering.
 * Per-leaf rev LWW with tombstones sits underneath as DEFENSE: the dispatcher already guarantees
 * order (§8), this catches bugs, it does not license them.
 */
class Mirror {
    private val cells = LinkedHashMap<String, Cell>()
    val overlay = mutableListOf<OverlayOp>()

    /**
     * §3: the client replaces the sub's serverState with the snapshot value — but §7 (v1.3) applies
     * per-leaf LWW here too: a leaf or tombstone recorded ABOVE the snapshot's rev survives it. A
     * setup snapshot can legitimately read older than deltas this connection already applied
     * (`delta(N+1), snapshot(N), delta(N+1)` on an overlapping sub), and the client must not roll
     * back in between. The newer deltas restore full consistency either way.
     */
    fun applySnapshot(path: String, value: JsonElement, rev: Long) = write(path, value, rev)

    /** §3: `put` replaces the subtree; `merge` writes each key as a child put. */
    fun applyDelta(delta: Delta) {
        when (delta.op) {
            "merge" -> {
                val value = delta.value
                if (value !is JsonObject) return // a merge whose value is not an object is a server bug
                for ((key, child) in value) write(joinPath(delta.path, key), child, delta.rev)
            }
            "put" -> write(delta.path, delta.value, delta.rev)
            // §3: an unknown op makes the SUBSCRIPTION stale (the client re-listens). The mirror
            // must not guess at it — RtdbClient gates this before we ever get here.
            else -> Unit
        }
    }

    /** serverState ⊕ overlay — what the app sees. */
    fun view(path: String): JsonElement = render(path, withOverlay = true)

    /** serverState alone, for convergence assertions. */
    fun serverValue(path: String): JsonElement = render(path, withOverlay = false)

    /**
     * §2 (v1.5) epoch change: every rev we hold is from a dead generation, so serverState, the
     * per-leaf revs and the tombstones all go. The overlay stays — unacked writes replay as-is and
     * commit as new writes against the restored shard.
     */
    fun dropServerState() = cells.clear()

    // ------------------------------------------------------------------ internals

    /** §7 LWW: a delta older than a leaf's recorded rev — or than a tombstone above it — is stale. */
    private fun isStale(path: String, rev: Long): Boolean =
        ancestorsInclusive(path).any { (cells[it]?.rev ?: -1) > rev }

    private fun write(path: String, value: JsonElement, rev: Long) {
        if (isStale(path, rev)) return

        for ((cellPath, cell) in cells.toList()) {
            if (isAncestorOrEqual(path, cellPath) && cell.rev <= rev) cells.remove(cellPath)
        }
        // A scalar sitting at an ancestor has to give way, exactly as it does in storage — the
        // server never sends a delta for the ancestor it silently replaced, so the client must infer
        // it (§7 v1.4). Tombstones above us are LEFT in place: they still guard against older deltas.
        for (ancestor in ancestorsInclusive(path)) {
            if (ancestor == path) continue
            val cell = cells[ancestor] ?: continue
            if (!cell.deleted && cell.rev <= rev) cells.remove(ancestor)
        }
        // The tombstone covers the whole cleared subtree, not just leaves we happened to hold.
        cells[path] = Cell(rev = rev, deleted = true, value = JsonNull)

        for (leaf in flatten(path, value)) {
            if (isStale(leaf.path, rev)) continue
            // §7: every EXTRACTED leaf is stamped with that delta's rev, not just the root.
            cells[leaf.path] = Cell(rev = rev, deleted = false, value = leaf.value)
        }
    }

    private fun render(root: String, withOverlay: Boolean): JsonElement {
        val leaves = LinkedHashMap<String, JsonElement>()
        for ((path, cell) in cells) {
            if (!cell.deleted && isAncestorOrEqual(root, path)) leaves[path] = cell.value
        }
        if (withOverlay) for (op in overlay) applyOverlay(leaves, op, root)
        return unflatten(root, leaves.map { Leaf(it.key, it.value) })
    }

    private fun applyOverlay(leaves: MutableMap<String, JsonElement>, op: OverlayOp, root: String) {
        val parts: List<Pair<String, JsonElement>> = if (op.op == "merge") {
            (op.value as? JsonObject)?.map { (key, value) -> joinPath(op.path, key) to value } ?: emptyList()
        } else {
            listOf(op.path to op.value)
        }

        for ((path, value) in parts) {
            if (!isRelevant(path, root)) continue
            for (leafPath in leaves.keys.toList()) if (isAncestorOrEqual(path, leafPath)) leaves.remove(leafPath)
            for (ancestor in ancestorsInclusive(path)) if (ancestor != path) leaves.remove(ancestor)
            for (leaf in flatten(path, value)) if (isAncestorOrEqual(root, leaf.path)) leaves[leaf.path] = leaf.value
        }
    }
}
