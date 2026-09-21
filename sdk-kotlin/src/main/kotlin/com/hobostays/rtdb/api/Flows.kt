package com.hobostays.rtdb.api

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.conflate

/**
 * The `Flow` half of §7's `onValue` (WORKLOAD §5: callbacks are the primary surface, Flows where
 * they are natural — and a stream of values is where they are natural).
 *
 * Conflated on purpose: a value listener's contract is "the current subtree", so a slow collector
 * should skip to the latest rather than replay a backlog. A sub-scoped err (§3) ends the flow with
 * [RtdbException]; the subscription is unlistened when collection stops.
 */
fun RtdbRef.values(): Flow<DataSnapshot> = callbackFlow {
    val listener = object : ValueEventListener {
        override fun onDataChange(snapshot: DataSnapshot) {
            trySend(snapshot)
        }

        override fun onCancelled(error: RtdbError) {
            close(RtdbException(error))
        }
    }
    addValueEventListener(listener)
    awaitClose { removeEventListener(listener) }
}.conflate()

/**
 * One §7 child event. `Moved` is absent for the same reason `ChildEventListener` has no
 * `onChildMoved` (Api.kt:83) — ordering arrives with §11's windowed queries.
 */
sealed interface ChildEvent {
    val snapshot: DataSnapshot

    data class Added(override val snapshot: DataSnapshot) : ChildEvent
    data class Changed(override val snapshot: DataSnapshot) : ChildEvent
    data class Removed(override val snapshot: DataSnapshot) : ChildEvent
}

/**
 * The `Flow` half of §7's child events.
 *
 * NOT conflated, unlike [values]. A value listener's contract is "the current subtree", so dropping
 * a superseded one loses nothing; a child stream is a SEQUENCE, and a dropped Added or Removed is
 * silent divergence with no later event to repair it.
 *
 * A WRITER COLLECTING ITS OWN SUBTREE SEES ITS OPTIMISTIC VALUE FIRST, then the settled one.
 * §7's view is `serverState + overlay`, so writing to a key another client is also writing to
 * emits up to three events for two writes: your value the moment you write it, the other client's
 * the moment your ack settles and your overlay comes off, and yours again when your echo lands.
 * Measured, not reasoned: §5.36's soak counted it on writer clients only, and zero on clients that
 * only read. Render from this stream and a contended key will flicker; that is what optimistic
 * local write costs, and it is the same bargain Firebase makes.
 *
 * [Channel.UNLIMITED] rather than `callbackFlow`'s 64-slot default: a late collector is replayed
 * `child_added` for every existing child (RtdbClient.kt:183) in one tight loop on the client's
 * dispatcher, and `trySend` DROPS, silently, once the buffer is full. Measured on the default
 * buffer with 100 existing children: a collector that only appends keeps up and loses nothing, but
 * one that does ~2 ms of work per event receives exactly 65 of 100 — the 64 buffered plus the one
 * in flight. A collector doing real work per child is the normal case, so the default is unsafe.
 */
fun RtdbRef.childEvents(): Flow<ChildEvent> = callbackFlow {
    val listener = object : ChildEventListener {
        override fun onChildAdded(snapshot: DataSnapshot) {
            trySend(ChildEvent.Added(snapshot))
        }

        override fun onChildChanged(snapshot: DataSnapshot) {
            trySend(ChildEvent.Changed(snapshot))
        }

        override fun onChildRemoved(snapshot: DataSnapshot) {
            trySend(ChildEvent.Removed(snapshot))
        }

        override fun onCancelled(error: RtdbError) {
            close(RtdbException(error))
        }
    }
    addChildEventListener(listener)
    awaitClose { removeEventListener(listener) }
}.buffer(Channel.UNLIMITED)
