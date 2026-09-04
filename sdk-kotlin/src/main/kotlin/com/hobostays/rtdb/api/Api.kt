package com.hobostays.rtdb.api

import com.hobostays.rtdb.core.joinPath
import com.hobostays.rtdb.core.segments
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** §7 virtual path, served entirely client-side — it is not a legal wire path (§1 forbids `.`). */
const val INFO_CONNECTED: String = ".info/connected"

/**
 * The value at a path, as the mirror holds it. Values are `JsonElement`: the SDK does no reflection
 * (WORKLOAD §5), so mapping to app types is kotlinx-serialization's job at the call site.
 */
class DataSnapshot internal constructor(
    val path: String,
    val value: JsonElement,
) {
    /** The last segment of the path — Firebase's `getKey()`. Null at the root. */
    val key: String? = segments(path).lastOrNull()

    fun exists(): Boolean = value != JsonNull

    fun child(relative: String): DataSnapshot {
        var node: JsonElement = value
        for (segment in segments(relative)) {
            node = (node as? JsonObject)?.get(segment) ?: JsonNull
        }
        return DataSnapshot(joinPath(path, relative), node)
    }

    /** Direct children, in the order the mirror holds them. Empty when the value is not an object. */
    val children: List<DataSnapshot>
        get() = (value as? JsonObject)?.map { (key, child) -> DataSnapshot(joinPath(path, key), child) }
            ?: emptyList()

    override fun toString(): String = "DataSnapshot($path=$value)"
}

/** §3's sub-scoped err codes, and §4's write err codes, as they arrived. */
data class RtdbError(val code: String, val message: String)

class RtdbException(val error: RtdbError) : RuntimeException("${error.code}: ${error.message}")

/**
 * §4 gives a write exactly three outcomes: `ack`, `casFail`, `err`. "Never settles" is not among
 * them, and the 2026-08-29 load test found 1,650 writes in exactly that state. A client whose FSM
 * has STOPPED settles its writes with this instead of leaving them queued for a reconnect that is
 * never coming. `CLOSED` is deliberately not one of §4's wire codes: it is a LOCAL abandonment and
 * no server ever sends it.
 */
val CLOSED_ERROR: RtdbError = RtdbError("CLOSED", "the client is closed; this write will never be sent")

/** How a write settled (§4). Every write ends in exactly one of these, exactly once. */
sealed interface WriteResult {
    /** `ack` — identical for a first commit and a duplicate replay, by design (§4). */
    data class Committed(val rev: Long) : WriteResult

    /** `casFail` — a normal outcome carrying the state that beat us, not an error (§4). */
    data class Rejected(val value: JsonElement, val rev: Long) : WriteResult

    /** `err` — the write left the queue and is never auto-retried (§4). */
    data class Failed(val error: RtdbError) : WriteResult
}

/**
 * §7 `onValue`: fires with the full mirrored subtree after the initial snapshot, and after every
 * applied change — a server delta OR this client's own optimistic write. Always from the mirror,
 * never a network round trip.
 */
interface ValueEventListener {
    fun onDataChange(snapshot: DataSnapshot)

    /** §3: a sub-scoped err (RULES/BADPATH/TOOBIG) terminates this subscription. */
    fun onCancelled(error: RtdbError) {}
}

/**
 * §7 child events, derived by diffing the DIRECT children of the listened node before and after each
 * applied change.
 *
 * Firebase's `previousChildName` argument and `onChildMoved` are absent: both are ordering concepts,
 * and ordering arrives with §11's windowed queries, which are out of scope for v1.
 */
interface ChildEventListener {
    fun onChildAdded(snapshot: DataSnapshot) {}
    fun onChildChanged(snapshot: DataSnapshot) {}
    fun onChildRemoved(snapshot: DataSnapshot) {}
    fun onCancelled(error: RtdbError) {}
}

/** Direct children of a mirrored value; empty when it is not an object. */
internal fun directChildren(value: JsonElement): Map<String, JsonElement> =
    (value as? JsonObject) ?: emptyMap()
