package com.hobostays.rtdb.api

import com.hobostays.rtdb.core.Ack
import com.hobostays.rtdb.core.CasFail
import com.hobostays.rtdb.core.Cas
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.Connection
import com.hobostays.rtdb.core.ConnectionListener
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Delta
import com.hobostays.rtdb.core.Err
import com.hobostays.rtdb.core.HelloAck
import com.hobostays.rtdb.core.Listen
import com.hobostays.rtdb.core.Merge
import com.hobostays.rtdb.core.Mirror
import com.hobostays.rtdb.core.OverlayOp
import com.hobostays.rtdb.core.Put
import com.hobostays.rtdb.core.Resync
import com.hobostays.rtdb.core.ServerFrame
import com.hobostays.rtdb.core.Snapshot
import com.hobostays.rtdb.core.TransportFactory
import com.hobostays.rtdb.core.Unlisten
import com.hobostays.rtdb.core.WriteFrame
import com.hobostays.rtdb.core.flatten
import com.hobostays.rtdb.core.isRelevant
import com.hobostays.rtdb.core.joinPath
import com.hobostays.rtdb.core.validatePath
import java.util.UUID
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.EmptyCoroutineContext

/**
 * The Firebase-shaped surface over PROTOCOL §3/§4/§7: `client.ref(path)` then `setValue`,
 * `updateChildren`, `removeValue`, `compareAndSet`, `addValueEventListener`, `addChildEventListener`.
 *
 * Threading (WORKLOAD §4): the mirror, the subscription table and the pending queue live on ONE
 * single-threaded dispatcher, shared with the [Connection] underneath, so nothing here locks.
 * App callbacks go through an injectable executor that defaults to that same dispatcher — Phase 3
 * passes Android's main thread and changes nothing else.
 */
class RtdbClient(
    options: ConnectionOptions,
    dispatcher: CoroutineDispatcher? = null,
    callbackExecutor: Executor? = null,
    transports: TransportFactory? = null,
) : AutoCloseable {

    private val ownedExecutor: ExecutorService? =
        if (dispatcher == null) {
            Executors.newSingleThreadExecutor { r -> Thread(r, "rtdb-client").apply { isDaemon = true } }
        } else {
            null
        }
    private val stateDispatcher: CoroutineDispatcher = dispatcher ?: ownedExecutor!!.asCoroutineDispatcher()
    private val scope = CoroutineScope(stateDispatcher + SupervisorJob())
    // Straight onto the dispatcher, not `scope.launch`: closing the client shuts this scope's
    // executor down, and the LAST thing a closing client owes the app is "your write was abandoned"
    // (P1). A scope-bound default cannot deliver that — the delivery mechanism dies with the thing
    // it has to report on. Ordering is unchanged: both forms enqueue on the same single thread.
    private val callbacks: Executor = callbackExecutor ?: Executor { task ->
        val owned = ownedExecutor
        // An injected dispatcher is not ours to shut down, so it is always there to dispatch on.
        if (owned == null) stateDispatcher.dispatch(EmptyCoroutineContext, task)
        // Ours, and close() shuts it down. Past that there is no order left to keep and exactly one
        // thing left to say, so a refused task runs on the caller's thread rather than being lost.
        // Re-entrant, and deliberately so: the alternative is silence, which §4 does not have.
        else try { owned.execute(task) } catch (refused: RejectedExecutionException) { task.run() }
    }

    /** §7's two layers. Exposed to tests for convergence assertions, not to apps. */
    internal val mirror = Mirror()

    private val connection = Connection(options, Bridge(), transports = transports, dispatcher = stateDispatcher)
    private val limits = options.limits

    // --- confined to the state dispatcher ---
    private val subs = LinkedHashMap<Int, Sub>()
    /** Insertion-ordered, which IS the replay order §6 step 3 requires. */
    private val pending = LinkedHashMap<String, PendingWrite>()
    private val infoListeners = mutableListOf<ValueEventListener>()
    private var nextSubId = 1
    private var connected = false

    /** §6 (v1.2): surfaced when a 4401 stops the FSM. Only `connect(freshToken)` restarts it. */
    @Volatile
    var onAuthFailure: ((RtdbError) -> Unit)? = null

    /** Read off the state dispatcher on purpose — see [write]. Set once, by [close]. */
    @Volatile
    private var closed = false

    val state: StateFlow<ClientState> get() = connection.state

    /** §2 (v1.5) the shard generation this client's mirror belongs to. */
    val epoch: Long? get() = connection.epoch

    fun connect(token: String? = null) = connection.connect(token)

    /**
     * §6: short-circuit a scheduled backoff wait — the platform says the network is back. A no-op
     * unless the connection is WAITING; in particular it never revives a 4401-CLOSED client
     * (§6 v1.2: only `connect(newToken)` does that). Android wires this to ConnectivityManager.
     */
    fun retryNow() = connection.retryNow()

    /** §5: ping every 25s in the foreground, 60s backgrounded. Takes effect from the next ping. */
    fun setBackgrounded(backgrounded: Boolean) = connection.setBackgrounded(backgrounded)

    /** The cadence currently in force (§5) — for tests, logs and the demo's status line. */
    val backgrounded: Boolean get() = connection.backgrounded

    /** Resolves when the connection is up; throws if the FSM has stopped (§6). */
    suspend fun ready(): HelloAck = connection.ready()

    override fun close() {
        // P1 guard 2 of 2, explicit half: queued FIRST, so it runs on the state dispatcher ahead of
        // everything the teardown puts behind it. The 4401 / autoReconnect-off half comes through
        // Bridge.onState(CLOSED) — which this path never sees, because Connection cancels its own
        // scope before that callback can be delivered.
        if (closed) return // idempotent: `use {}` closes a client a test already closed by hand
        closed = true
        post { failPending() }
        connection.close()
        // Both moved off the caller's thread and into the queue: run inline they overtook the
        // failPending() above, and a cancelled scope / shut executor is exactly what swallowed the
        // completions it queues. Same trick Connection.close() uses — the dispatcher is FIFO.
        scope.launch {
            scope.cancel()
            ownedExecutor?.shutdown()
        }
    }

    /** A handle on one path. Cheap: it holds no state of its own. */
    fun ref(path: String = ""): RtdbRef {
        if (path != INFO_CONNECTED) {
            validatePath(path, limits)?.let { throw IllegalArgumentException("invalid path \"$path\": $it") }
        }
        return RtdbRef(this, path)
    }

    // ------------------------------------------------------------------ §3 subscriptions

    internal fun addValueListener(path: String, listener: ValueEventListener) = post {
        if (path == INFO_CONNECTED) {
            infoListeners += listener
            emit { listener.onDataChange(DataSnapshot(path, JsonPrimitive(connected))) }
            return@post
        }
        val sub = subscribe(path)
        sub.valueListeners += listener
        // Firebase fires immediately from cache when there is any; a fresh sub waits for §3's snapshot.
        if (sub.hasData) {
            val value = mirror.view(path)
            sub.delivered = true
            sub.lastValue = value
            emit { listener.onDataChange(DataSnapshot(path, value)) }
        }
    }

    internal fun addChildListener(path: String, listener: ChildEventListener) = post {
        require(path != INFO_CONNECTED) { "$INFO_CONNECTED has no children" }
        val sub = subscribe(path)
        sub.childListeners += listener
        if (sub.hasData) {
            // Firebase replays child_added for everything already there, so a late listener sees the
            // same story as an early one.
            sub.children = directChildren(mirror.view(path))
            for ((key, value) in sub.children) {
                emit { listener.onChildAdded(DataSnapshot(joinPath(path, key), value)) }
            }
        }
    }

    internal fun removeListener(path: String, listener: Any) = post {
        if (path == INFO_CONNECTED) {
            infoListeners.remove(listener)
            return@post
        }
        val sub = subs.values.firstOrNull { it.path == path } ?: return@post
        sub.valueListeners.remove(listener)
        sub.childListeners.remove(listener)
        // §3: no reply to an unlisten; in-flight deltas route to no sub and are dropped.
        if (sub.valueListeners.isEmpty() && sub.childListeners.isEmpty()) {
            subs.remove(sub.subId)
            connection.send(Unlisten(subId = sub.subId))
        }
    }

    /**
     * Replay a pending write under its original writeId — §6's lost-ack case, driven explicitly.
     * The reconnect path does this for the whole queue; this is the single-write version tests use.
     */
    internal fun resend(writeId: String) = post {
        pending[writeId]?.let { connection.send(it.frame) }
    }

    /** The mirrored value at `path`: serverState ⊕ pendingOverlay (§7). For tests and probes. */
    internal fun value(path: String): JsonElement = mirror.view(path)

    internal val pendingWriteIds: List<String> get() = pending.keys.toList()

    internal val subscriptionPaths: List<String> get() = subs.values.map { it.path }

    /** The highest rev this subscription has applied (§3). Tests use it to prove nothing committed twice. */
    internal fun lastRev(path: String): Long = subs.values.firstOrNull { it.path == path }?.lastRev ?: 0

    // ------------------------------------------------------------------ §4 writes

    internal fun write(frame: WriteFrame, overlay: OverlayOp, onComplete: ((WriteResult) -> Unit)?) {
        val entered = AtomicBoolean(false)
        val job = scope.launch {
            entered.set(true)
            // P1 guard 1 of 2 — and it lives HERE, on the dispatcher, not on the caller's thread.
            // A caller-thread check is check-then-act across threads: the guard passes, close() or a
            // 4401 sweeps the queue, and only THEN does this block land, adding an entry to a queue
            // nobody will ever look at again. On the dispatcher there is no such window, because
            // both sweeps run here too: this block runs either before a sweep (and is swept by it)
            // or after one (and sees the terminal state below), never between the two.
            if (closed || connection.state.value == ClientState.CLOSED) {
                abandon(onComplete)
                return@launch
            }
            // A writeId already in flight is the SAME write (§1: the server deduplicates on it), so
            // it is resent, not overlaid twice — a second overlay entry for it would never settle.
            if (pending.containsKey(frame.writeId)) {
                connection.send(frame)
                return@launch
            }
            // §7: the write joins the overlay as an OPERATION the moment it is issued, so the local
            // view is optimistic immediately; it leaves on ack/casFail/err. There is no rollback.
            mirror.overlay += overlay
            pending[frame.writeId] = PendingWrite(frame, overlay, onComplete)
            connection.send(frame)
            fireChange(frame.path)
        }
        // The other half of the same race, and the reason the check above is not enough on its own:
        // close() cancels this scope, and a coroutine cancelled before it starts never runs its body
        // at all. The write would vanish without even a queue entry left behind to find it by. So
        // launch-OR-settle: if the body was never entered, this handler is the last word, and it
        // runs on whichever thread completed the job — the one thread guaranteed to still exist.
        job.invokeOnCompletion { if (!entered.get()) abandon(onComplete) }
    }

    /** One write, settled as abandoned. Never touches `pending`: this can run off the dispatcher. */
    private fun abandon(onComplete: ((WriteResult) -> Unit)?) {
        onComplete?.let { complete -> emit { complete(WriteResult.Failed(CLOSED_ERROR)) } }
    }

    // ------------------------------------------------------------------ internals

    private class Sub(val subId: Int, val path: String) {
        /** Sent on re-listen after a reconnect (§6 step 2). */
        var lastRev: Long = 0
        /** True once a snapshot or delta has landed — what makes a late listener fire immediately. */
        var hasData = false
        val valueListeners = mutableListOf<ValueEventListener>()
        val childListeners = mutableListOf<ChildEventListener>()
        /** The direct children the app was last told about; the baseline for §7's child diffing. */
        var children: Map<String, JsonElement> = emptyMap()
        /** The value the app was last told about — what makes an equal-value refire droppable. */
        var delivered = false
        var lastValue: JsonElement = JsonNull
    }

    private class PendingWrite(
        val frame: WriteFrame,
        val overlay: OverlayOp,
        val onComplete: ((WriteResult) -> Unit)?,
    )

    private fun post(block: () -> Unit) {
        scope.launch { block() }
    }

    private fun emit(block: () -> Unit) = callbacks.execute(block)

    private fun subscribe(path: String): Sub {
        subs.values.firstOrNull { it.path == path }?.let { return it }
        val sub = Sub(nextSubId++, path)
        subs[sub.subId] = sub
        if (connected) sendListen(sub)
        return sub
    }

    private fun sendListen(sub: Sub) {
        connection.send(
            Listen(subId = sub.subId, path = sub.path, lastRev = sub.lastRev.takeIf { it > 0 }),
        )
    }

    /** §6, and the order is normative: (2) listens with their stored lastRev, (3) pending writes. */
    private fun resume(epochChanged: Boolean) {
        if (epochChanged) {
            // §2 (v1.5): every rev we hold is from a dead generation. Drop the lot BEFORE the
            // re-listens below, so they go out with no lastRev and come back as fresh snapshots.
            mirror.dropServerState()
            for (sub in subs.values) {
                sub.lastRev = 0
                sub.hasData = false
            }
            // Not `delivered = false`: the app was told a value and it is still on screen. If the
            // restored shard happens to hold the same tree, that is genuinely no change to report.
        }
        for (sub in subs.values) sendListen(sub)
        for (write in pending.values) connection.send(write.frame)
    }

    private fun handle(frame: ServerFrame) {
        when (frame) {
            is Snapshot -> {
                val sub = subs[frame.subId] ?: return // unlistened while it was in flight
                mirror.applySnapshot(frame.path, frame.value, frame.rev)
                sub.lastRev = frame.rev
                sub.hasData = true
                fireChange(frame.path)
            }

            is Delta -> {
                // §3: an unknown op means this client cannot know what the subtree became — the
                // subscription is stale and must be re-listened, with no lastRev so the server
                // answers with a snapshot rather than replaying the op we could not read.
                if (frame.op != "put" && frame.op != "merge") return relistenAround(frame.path)
                mirror.applyDelta(frame)
                // Deltas carry no subId: every sub the path is relevant to advances (§3). Gaps in a
                // sub's rev sequence are NORMAL — the client never gap-detects.
                for (sub in subs.values) {
                    if (!isRelevant(sub.path, frame.path)) continue
                    sub.lastRev = maxOf(sub.lastRev, frame.rev)
                    sub.hasData = true
                }
                fireChange(frame.path)
            }

            // §3: deliberately nothing. Do NOT clear serverState — the fresh snapshot that
            // follows replaces it wholesale, and clearing now would only make the UI flicker. The
            // requirement is satisfied by inaction, which is why the old `stale` flag had no readers.
            is Resync -> Unit

            is Ack -> settle(frame.writeId) { WriteResult.Committed(frame.rev) }

            is CasFail -> settle(frame.writeId) { WriteResult.Rejected(frame.value, frame.rev) }

            is Err -> {
                val error = RtdbError(frame.code, frame.msg)
                when {
                    // §4: an err-rejected write leaves the queue and surfaces; never auto-retried.
                    frame.writeId != null -> settle(frame.writeId) { WriteResult.Failed(error) }
                    frame.subId != null -> cancelSub(frame.subId, error)
                    else -> Unit // connection-scoped: the FSM's business, not ours
                }
            }

            else -> Unit // helloAck and pong are the FSM's
        }
    }

    private fun relistenAround(path: String) {
        for (sub in subs.values) {
            if (!isRelevant(sub.path, path)) continue
            sub.lastRev = 0
            sendListen(sub)
        }
    }

    private fun cancelSub(subId: Int, error: RtdbError) {
        val sub = subs.remove(subId) ?: return
        for (listener in sub.valueListeners) emit { listener.onCancelled(error) }
        for (listener in sub.childListeners) emit { listener.onCancelled(error) }
    }

    /** Settle one pending write and drop its overlay entry (§7: ack/casFail/err all remove it). */
    private fun settle(writeId: String, result: () -> WriteResult) {
        val write = pending.remove(writeId) ?: return // already settled: expected after a replay
        // By identity, never by value: two identical unacked ops must not settle each other's entry.
        val index = mirror.overlay.indexOfFirst { it === write.overlay }
        if (index >= 0) mirror.overlay.removeAt(index)
        val outcome = result()
        write.onComplete?.let { complete -> emit { complete(outcome) } }
        fireChange(write.frame.path)
    }

    /**
     * Settle every queued write as abandoned. Idempotent — both terminal paths may reach it, and the
     * second finds an empty queue. Keyed off a snapshot of the keys: [settle] mutates `pending`.
     */
    private fun failPending() {
        for (writeId in pending.keys.toList()) settle(writeId) { WriteResult.Failed(CLOSED_ERROR) }
    }

    /** §7 events: fire onValue for every sub the change could be visible in, then diff its children. */
    private fun fireChange(changedPath: String) {
        for (sub in subs.values) {
            if (!isRelevant(sub.path, changedPath)) continue
            val value = mirror.view(sub.path)
            // Firebase fires onValue only when the value actually changed, and so do we: a delta
            // that touches a sibling, a re-applied idempotent delta, or an ack whose echo is already
            // in serverState all leave THIS subtree identical, and refiring it is pure waste on the
            // Android main thread (Gate C ruling, Q2). The children are unchanged with it, so the
            // whole sub is skipped, diff included.
            if (sub.delivered && sub.lastValue == value) continue
            sub.delivered = true
            sub.lastValue = value
            for (listener in sub.valueListeners) {
                emit { listener.onDataChange(DataSnapshot(sub.path, value)) }
            }
            if (sub.childListeners.isEmpty()) continue

            val before = sub.children
            val after = directChildren(value)
            sub.children = after
            for ((key, child) in after) {
                val old = before[key]
                val snapshot = DataSnapshot(joinPath(sub.path, key), child)
                when {
                    old == null -> for (l in sub.childListeners) emit { l.onChildAdded(snapshot) }
                    old != child -> for (l in sub.childListeners) emit { l.onChildChanged(snapshot) }
                }
            }
            for ((key, child) in before) {
                if (after.containsKey(key)) continue
                val snapshot = DataSnapshot(joinPath(sub.path, key), child)
                for (l in sub.childListeners) emit { l.onChildRemoved(snapshot) }
            }
        }
    }

    private fun fireInfo(nowConnected: Boolean) {
        if (connected == nowConnected) return
        connected = nowConnected
        for (listener in infoListeners) {
            emit { listener.onDataChange(DataSnapshot(INFO_CONNECTED, JsonPrimitive(nowConnected))) }
        }
    }

    private inner class Bridge : ConnectionListener {
        override fun onState(state: ClientState) {
            // P1 guard 2 of 2, FSM half: CLOSED is terminal — §6 (v1.2)'s 4401, or autoReconnect
            // off. Past it there is no reconnect left to run §6 step 3, so a queued write is
            // abandoned, not pending. Delivered on the state dispatcher, like everything here.
            if (state == ClientState.CLOSED) failPending()
            fireInfo(state == ClientState.CONNECTED)
        }

        override fun onHelloAck(ack: HelloAck, epochChanged: Boolean) = resume(epochChanged)

        override fun onFrame(frame: ServerFrame) = handle(frame)

        override fun onAuthFailure(code: Int, reason: String) {
            val handler = onAuthFailure ?: return
            emit { handler(RtdbError("AUTH", reason)) }
        }
    }
}

/** A handle on one path — Firebase's `DatabaseReference`. */
class RtdbRef internal constructor(
    private val client: RtdbClient,
    val path: String,
) {
    val key: String? get() = path.substringAfterLast('/').takeIf { it.isNotEmpty() && path.isNotEmpty() }

    fun child(relative: String): RtdbRef = client.ref(joinPath(path, relative))

    /** §4 `put` — the wire form of BOTH setValue and removeValue. */
    fun setValue(value: JsonElement, onComplete: ((WriteResult) -> Unit)? = null) {
        writable(value)
        val frame = Put(writeId = newWriteId(), path = path, value = value)
        client.write(frame, OverlayOp("put", path, value), onComplete)
    }

    /** §4 `put` with a null value. */
    fun removeValue(onComplete: ((WriteResult) -> Unit)? = null) = setValue(JsonNull, onComplete)

    /** §4 `merge` — keys may be deep relative paths; all children commit atomically under ONE rev. */
    fun updateChildren(children: Map<String, JsonElement>, onComplete: ((WriteResult) -> Unit)? = null) {
        val value = JsonObject(children)
        for ((key, child) in children) writableChild(key, child)
        val frame = Merge(writeId = newWriteId(), path = path, value = value)
        client.write(frame, OverlayOp("merge", path, value), onComplete)
    }

    /**
     * §4 `cas` — ours, not Firebase's: commits iff no relevant write landed after `expectedRev`.
     * A mismatch is a normal outcome ([WriteResult.Rejected] carrying fresh state), not an error.
     */
    fun compareAndSet(expectedRev: Long, value: JsonElement, onComplete: ((WriteResult) -> Unit)? = null) {
        writable(value)
        val frame = Cas(writeId = newWriteId(), path = path, expectedRev = expectedRev, value = value)
        client.write(frame, OverlayOp("put", path, value), onComplete)
    }

    fun addValueEventListener(listener: ValueEventListener): ValueEventListener {
        client.addValueListener(path, listener)
        return listener
    }

    fun addChildEventListener(listener: ChildEventListener): ChildEventListener {
        client.addChildListener(path, listener)
        return listener
    }

    fun removeEventListener(listener: ValueEventListener) = client.removeListener(path, listener)

    fun removeEventListener(listener: ChildEventListener) = client.removeListener(path, listener)

    override fun toString(): String = "RtdbRef(\"$path\")"

    /** §1 is validated locally so a bad write fails at the call site instead of costing a round trip. */
    private fun writable(value: JsonElement) {
        require(path != INFO_CONNECTED) { "$INFO_CONNECTED is read-only" }
        for (leaf in flatten(path, value)) {
            validatePath(leaf.path)?.let { throw IllegalArgumentException("invalid key in value: $it") }
        }
    }

    private fun writableChild(key: String, child: JsonElement) {
        require(path != INFO_CONNECTED) { "$INFO_CONNECTED is read-only" }
        val childPath = joinPath(path, key)
        validatePath(childPath)?.let { throw IllegalArgumentException("invalid merge key \"$key\": $it") }
        for (leaf in flatten(childPath, child)) {
            validatePath(leaf.path)?.let { throw IllegalArgumentException("invalid key in value: $it") }
        }
    }

    /** WORKLOAD §4: writeIds are UUIDv4, and the server deduplicates on them (§1). */
    private fun newWriteId(): String = UUID.randomUUID().toString()
}
