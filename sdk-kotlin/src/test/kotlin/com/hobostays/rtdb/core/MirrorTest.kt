package com.hobostays.rtdb.core

import com.hobostays.rtdb.json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

/**
 * §7 mirror semantics, ported case for case from WP1's `test/unit/mirror.test.ts` — the reference
 * implementation's own tests, which is what makes this a port and not a rewrite (WORKLOAD §4).
 */
class MirrorTest {

    private fun delta(rev: Long, path: String, value: String, op: String = "put") =
        Delta(rev = rev, path = path, op = op, value = json(value))

    @Test
    fun `a snapshot replaces the sub's serverState (§3)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"name":"Ravi","score":42}"""), 10)
        assertEquals(json("""{"name":"Ravi","score":42}"""), m.view("p"))
        m.applySnapshot("p", json("""{"score":7}"""), 11)
        assertEquals(json("""{"score":7}"""), m.view("p"), "replace, not merge")
    }

    @Test
    fun `deltas apply by op - put replaces a subtree, merge writes children`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"score":1,"tag":"x"}"""), 10)
        m.applyDelta(delta(11, "p/score", "50"))
        assertEquals(json("""{"score":50,"tag":"x"}"""), m.view("p"))
        m.applyDelta(delta(12, "p", """{"score":60,"stats/wins":3,"tag":null}""", op = "merge"))
        assertEquals(json("""{"score":60,"stats":{"wins":3}}"""), m.view("p"))
    }

    @Test
    fun `an ancestor delta is extracted at the sub's relative path, null when absent (§3)`() {
        val m = Mirror()
        m.applySnapshot("p/child", json("""{"a":1}"""), 10)
        m.applyDelta(delta(11, "p", """{"child":{"a":2,"b":3},"other":9}"""))
        assertEquals(json("""{"a":2,"b":3}"""), m.view("p/child"))
        m.applyDelta(delta(12, "p", """{"other":9}""")) // child is gone from the ancestor value
        assertEquals(JsonNull, m.view("p/child"))
    }

    @Test
    fun `per-leaf rev LWW drops a stale delta for that leaf only (§7)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1,"b":1}"""), 10)
        m.applyDelta(delta(12, "p/a", "\"new\""))
        m.applyDelta(delta(11, "p/a", "\"stale\"")) // arrives late, older rev
        assertEquals(json("""{"a":"new","b":1}"""), m.view("p"), "the stale delta is dropped, its sibling untouched")
        m.applyDelta(delta(13, "p/b", "2"))
        assertEquals(json("""{"a":"new","b":2}"""), m.view("p"))
    }

    @Test
    fun `a delete leaves a rev-stamped tombstone that blocks resurrection (§7)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":{"deep":1},"b":2}"""), 10)
        m.applyDelta(delta(12, "p/a", "null")) // delete the subtree
        assertEquals(json("""{"b":2}"""), m.view("p"))
        // A stale delta from before the delete must NOT bring the data back — the exact case the
        // tombstone exists for, including a leaf we no longer hold.
        m.applyDelta(delta(11, "p/a/deep", "99"))
        m.applyDelta(delta(11, "p/a/never-seen", "99"))
        assertEquals(json("""{"b":2}"""), m.view("p"))
        // ...but a NEWER write to the same place is applied normally.
        m.applyDelta(delta(13, "p/a/deep", "5"))
        assertEquals(json("""{"a":{"deep":5},"b":2}"""), m.view("p"))
    }

    @Test
    fun `an ancestor put stamps every extracted leaf, not just the root (§7)`() {
        val m = Mirror()
        m.applySnapshot("", json("""{"p":{"a":1,"b":1}}"""), 10)
        m.applyDelta(delta(20, "p", """{"a":2,"b":2}"""))
        m.applyDelta(delta(15, "p/a", "\"stale\"")) // older than the ancestor put that produced p/a
        assertEquals(json("""{"a":2,"b":2}"""), m.view("p"))
    }

    @Test
    fun `view = serverState + overlay, applied in issue order, with no rollback (§7)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"score":1,"tag":"x"}"""), 10)

        m.overlay += OverlayOp("put", "p/score", JsonPrimitive(2))
        assertEquals(json("""{"score":2,"tag":"x"}"""), m.view("p"), "optimistic")
        assertEquals(json("""{"score":1,"tag":"x"}"""), m.serverValue("p"), "serverState is untouched by the overlay")

        // A concurrent foreign delta lands while our write is still unacked: the view is still
        // serverState ⊕ overlay at every instant.
        m.applyDelta(delta(11, "p/tag", "\"y\""))
        assertEquals(json("""{"score":2,"tag":"y"}"""), m.view("p"))

        // ack: the entry leaves the overlay, its effect arrives via the server echo.
        m.applyDelta(delta(12, "p/score", "2"))
        m.overlay.clear()
        assertEquals(json("""{"score":2,"tag":"y"}"""), m.view("p"))
        assertEquals(m.view("p"), m.serverValue("p"), "converged")
    }

    @Test
    fun `overlay ops compose in order, including deletes and ancestor writes`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1,"b":2}"""), 10)
        m.overlay += OverlayOp("merge", "p", json("""{"a":null,"c/d":3}"""))
        assertEquals(json("""{"b":2,"c":{"d":3}}"""), m.view("p"))
        m.overlay += OverlayOp("put", "p", json("""{"z":1}""")) // replaces everything above it
        assertEquals(json("""{"z":1}"""), m.view("p"))
        assertEquals(JsonPrimitive(1), m.view("p/z"), "the overlay is visible from a descendant view too")
    }

    @Test
    fun `empty subtrees read as null, never as an empty object`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1}"""), 10)
        m.applyDelta(delta(11, "p/a", "null"))
        assertEquals(JsonNull, m.view("p"))
        assertEquals(JsonNull, m.view("p/nothing/here"))
    }

    @Test
    fun `a snapshot older than a leaf we already hold does not roll it back (§7 v1_3)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"score":1,"tag":"x"}"""), 10)

        // The wire sequence an overlapping sub's setup can produce: the live sub already applied rev
        // 11 when the new sub's snapshot, read at rev 10, arrives — and rev 11 is flushed after it.
        m.applyDelta(delta(11, "p/score", "99"))
        m.applySnapshot("p", json("""{"score":1,"tag":"x"}"""), 10)
        assertEquals(json("""{"score":99,"tag":"x"}"""), m.view("p"), "no visible rollback in between")

        m.applyDelta(delta(11, "p/score", "99"))
        assertEquals(json("""{"score":99,"tag":"x"}"""), m.view("p"), "and re-applying the delta is idempotent")
    }

    @Test
    fun `a snapshot does not resurrect what was deleted after its rev (§7 v1_3)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1,"b":2}"""), 10)
        m.applyDelta(delta(12, "p/a", "null")) // deleted at rev 12
        m.applySnapshot("p", json("""{"a":1,"b":2}"""), 11) // a stale snapshot still carrying `a`
        assertEquals(json("""{"b":2}"""), m.view("p"), "the tombstone outranks the older snapshot")
    }

    @Test
    fun `a newer snapshot replaces everything, including leaves we thought were current`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1}"""), 10)
        m.applyDelta(delta(11, "p/b", "2"))
        m.applySnapshot("p", json("""{"z":9}"""), 12)
        assertEquals(json("""{"z":9}"""), m.view("p"))
    }

    @Test
    fun `a delta below a scalar turns it into an object, as storage already did (§7 v1_4)`() {
        // The server deletes the ancestor leaf silently — it never sends a delta for it — so the
        // client has to infer it, or the mirror reads the stale scalar forever.
        val m = Mirror()
        m.applySnapshot("p", JsonPrimitive(5), 1)
        m.applyDelta(delta(2, "p/x", "1"))
        assertEquals(json("""{"x":1}"""), m.view("p"))
        m.applyDelta(delta(3, "p", "7")) // ...and back to a scalar
        assertEquals(JsonPrimitive(7), m.view("p"))
    }

    @Test
    fun `an unknown op is never guessed at (§3 - the subscription re-listens instead)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"n":1}"""), 10)
        m.applyDelta(delta(11, "p/n", "5", op = "incr"))
        assertEquals(json("""{"n":1}"""), m.view("p"), "an §11 op must not be applied as a put")
    }

    @Test
    fun `an epoch change drops serverState but keeps the pending overlay (§2 v1_5)`() {
        val m = Mirror()
        m.applySnapshot("p", json("""{"a":1}"""), 10)
        m.overlay += OverlayOp("put", "p/mine", JsonPrimitive("unacked"))
        m.dropServerState()

        assertEquals(json("""{"mine":"unacked"}"""), m.view("p"), "the unacked write survives the drop")
        assertEquals(JsonNull, m.serverValue("p"), "every rev from the dead generation is gone")
        // ...and the restored shard's low-rev snapshot now applies, where LWW would have refused it.
        m.applySnapshot("p", json("""{"b":2}"""), 1)
        assertEquals(json("""{"b":2}"""), m.serverValue("p"))
    }
}
