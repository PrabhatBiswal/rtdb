package com.hobostays.rtdb.api

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
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
