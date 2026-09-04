package com.hobostays.rtdb.core

import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

/** §6: CONNECTED -> (drop) -> WAITING -> CONNECTING -> CONNECTED. */
enum class ClientState { IDLE, CONNECTING, CONNECTED, WAITING, CLOSED }

/**
 * Everything above the wire hangs off these callbacks. They fire on the callback executor, in the
 * order the connection produced them.
 */
interface ConnectionListener {
    fun onState(state: ClientState) {}

    /**
     * §6 resume point: send listens, then pending writes, from here.
     * `epochChanged` is §2's (v1.5) wholesale-drop signal — when true the owner MUST drop its
     * mirrors, per-leaf revs, tombstones and stored lastRevs BEFORE it re-listens, so the listens
     * go out with no lastRev. One callback rather than two, so the two can never be misordered.
     */
    fun onHelloAck(ack: HelloAck, epochChanged: Boolean) {}

    /** Every server frame after helloAck, batches already unwrapped (§3). */
    fun onFrame(frame: ServerFrame) {}

    /** §6 (v1.2): a 4401 close. The FSM has stopped; only connect(newToken) restarts it. */
    fun onAuthFailure(code: Int, reason: String) {}

    fun onPongTimeout() {}

    /**
     * §2: the socket opened and hello went out, but no readable `helloAck` arrived within
     * [Limits.helloTimeoutMs]. The connection is dead and is retried like any other drop.
     */
    fun onHelloTimeout() {}

    fun onRetry(attempt: Int, delayMs: Long) {}
}

data class ConnectionOptions(
    val url: String,
    val token: String,
    val sdk: String? = null,
    val limits: Limits = Limits(),
    /** §5: 25s foreground / 60s backgrounded. Tests shrink it. */
    val pingIntervalMs: Long = limits.pingForegroundMs,
    val pongTimeoutMs: Long = limits.pongTimeoutMs,
    /** Off for tests that assert a single connection attempt. */
    val autoReconnect: Boolean = true,
)

/**
 * §6 full jitter: delay = random(0, min(30s, 1s * 2^attempt)). `attempt` is 0-based, `jitter` is the
 * roll in [0, 1) — passed in rather than drawn here, so the window is testable at its exact edges.
 * The ceiling is computed in floating point on purpose: 1000 * 2^attempt overflows a Long, where a
 * Double simply saturates and min() still returns the cap.
 */
fun backoffDelay(attempt: Int, limits: Limits, jitter: Double): Long =
    (jitter * min(limits.backoffCapMs.toDouble(), 1000.0 * 2.0.pow(attempt))).toLong()

/**
 * PROTOCOL §2/§5/§6: the connection state machine. It owns the socket, hello, liveness and
 * reconnect — and nothing else. Subscriptions, writes and the mirror sit ABOVE it (Gate C) and see
 * the wire only through [ConnectionListener] and [send].
 *
 * Threading (WORKLOAD §4): every field below is touched only on `state` — a single-threaded
 * dispatcher — so there are no locks and no `synchronized` anywhere. Transport callbacks and public
 * methods both hop onto it. Callbacks out to the app go through [callbacks], which must be
 * order-preserving (the default is the state dispatcher itself; Phase 3 swaps in Android's main
 * thread, which is also order-preserving).
 */
class Connection(
    private val options: ConnectionOptions,
    private val listener: ConnectionListener = object : ConnectionListener {},
    transports: TransportFactory? = null,
    dispatcher: CoroutineDispatcher? = null,
    callbacks: Executor? = null,
    private val random: Random = Random.Default,
) : AutoCloseable {

    private val ownedExecutor: ExecutorService? =
        if (dispatcher == null) Executors.newSingleThreadExecutor { r -> Thread(r, "rtdb-state").apply { isDaemon = true } } else null
    private val scope = CoroutineScope(
        (dispatcher ?: ownedExecutor!!.asCoroutineDispatcher()) + SupervisorJob(),
    )
    private val ownsTransports = transports == null
    private val transports: TransportFactory = transports ?: OkHttpTransportFactory()
    private val callbacks: Executor = callbacks ?: Executor { task -> scope.launch { task.run() } }

    private val _state = MutableStateFlow(ClientState.IDLE)
    val state: StateFlow<ClientState> = _state.asStateFlow()

    private val _lastAck = MutableStateFlow<HelloAck?>(null)

    /** §2 (v1.5): the shard generation this client's data belongs to. */
    @Volatile
    var epoch: Long? = null
        private set

    val session: String? get() = _lastAck.value?.session

    // --- confined to the state dispatcher from here down ---
    private var socket: Socket? = null
    private var token: String = options.token
    private var attempt = 0
    private var closing = false
    private var helloJob: Job? = null
    private var pingJob: Job? = null
    private var pongJob: Job? = null
    private var retryJob: Job? = null
    private var stableJob: Job? = null

    /** §5. Read from the ping loop on the state dispatcher; @Volatile only for the public getter. */
    @Volatile
    var backgrounded: Boolean = false
        private set

    /**
     * The foreground value comes from [ConnectionOptions] (tests shrink it); the background value
     * from [Limits], because nothing in the app configures a per-connection background cadence.
     */
    private val pingIntervalMs: Long
        get() = if (backgrounded) options.limits.pingBackgroundMs else options.pingIntervalMs

    /**
     * Start, or restart after an auth failure. §6 (v1.2) forbids retrying a 4401 with the same
     * token, so the only way back from one is this call with a FRESH token.
     */
    fun connect(token: String? = null) = post {
        if (token != null) this.token = token
        if (_state.value == ClientState.CONNECTING || _state.value == ClientState.CONNECTED) return@post
        closing = false
        open()
    }

    /** Stop for good: no reconnect. */
    override fun close() {
        post {
            closing = true
            clearTimers()
            socket?.transport?.close(Close.NORMAL, "client closing")
            socket = null
            setState(ClientState.CLOSED)
        }
        scope.launch {
            // Runs after the close above, because the dispatcher is single-threaded and FIFO.
            if (ownsTransports) transports.shutdown()
            scope.cancel()
            ownedExecutor?.shutdown()
        }
    }

    /**
     * Send one frame on a RESUMED connection; otherwise drop it (the queue is Gate C's).
     *
     * The state check is §2's "client MUST NOT send anything before hello" (WP3 finding F5): in
     * CONNECTING the socket already exists but hello has not gone out yet — it is sent from
     * [Socket.onOpen] — so an unguarded send puts a write on the wire FIRST and the server closes
     * the connection (4400 `expected hello`). With a real transport it is no better: OkHttp queues
     * sends made before the handshake and flushes them in queue order, ahead of the hello we only
     * enqueue from onOpen.
     *
     * Dropping here loses nothing: writes stay in the caller's pending queue and go out in §6's
     * resume order, which runs from onHelloAck — after setState(CONNECTED) — so it is not blocked
     * by this guard. hello and pings never come through here; they write to the transport directly.
     */
    fun send(frame: ClientFrame) = post {
        if (_state.value != ClientState.CONNECTED) return@post
        socket?.transport?.send(frame.encode())
    }

    /**
     * Resolves once the connection is up, with the helloAck that brought it up — and fails fast when
     * the FSM has STOPPED instead (close(), autoReconnect off, or §6 v1.2's 4401), because waiting
     * forever for a connection nobody is going to attempt is never the contract. An auth failure
     * still surfaces primarily through [ConnectionListener.onAuthFailure].
     */
    suspend fun ready(): HelloAck {
        val reached = state.first { it == ClientState.CONNECTED || it == ClientState.CLOSED }
        check(reached == ClientState.CONNECTED) { "the connection is closed and is not retrying" }
        return checkNotNull(_lastAck.value) { "CONNECTED without a helloAck" }
    }

    /**
     * §6: "Platform network-available signals short-circuit the wait." Skip the rest of a scheduled
     * backoff and open now. Only meaningful in WAITING — and the state check is what keeps it
     * honest in CLOSED: §6 (v1.2) says only `connect(newToken)` leaves a 4401, and a radio coming
     * back is not a new token. In CONNECTING/CONNECTED there is nothing to hurry.
     */
    fun retryNow() = post {
        if (_state.value != ClientState.WAITING) return@post
        open() // cancels retryJob itself
    }

    /**
     * §5: 25s foreground / 60s backgrounded. The ping loop reads the cadence at the top of every
     * iteration, so going TO the background costs nothing — the longer interval simply applies from
     * the next ping.
     *
     * Coming BACK, the ping loop is restarted so a ping goes out immediately (WP3 Gate B ruling Q3,
     * which revises WORKLOAD §4's "no reschedule"): a user looking at the screen should not wait up
     * to 60s+10s to find out the connection died while the app was away. One extra ping per
     * foreground, and only on the transition — a repeated setBackgrounded(false) probes nothing.
     */
    fun setBackgrounded(backgrounded: Boolean) = post {
        val returning = this.backgrounded && !backgrounded
        this.backgrounded = backgrounded
        // Only while CONNECTED: there is no socket to probe otherwise, and startPings() on a dead
        // connection would just spin a loop the reconnect is about to replace.
        if (returning && _state.value == ClientState.CONNECTED) startPings()
    }

    // ------------------------------------------------------------------ internals

    private fun post(block: () -> Unit) {
        scope.launch { block() }
    }

    private fun emit(block: ConnectionListener.() -> Unit) {
        callbacks.execute { listener.block() }
    }

    private fun setState(next: ClientState) {
        if (_state.value == next) return
        _state.value = next
        emit { onState(next) }
    }

    private fun open() {
        // A connect() while WAITING (a fresh token, an app foregrounding) races the retry we already
        // scheduled: without this cancel it fires later and opens a SECOND socket on top of this one.
        retryJob?.cancel()
        retryJob = null
        setState(ClientState.CONNECTING)
        val next = Socket()
        socket = next
        next.transport = transports.connect(options.url, next)
    }

    /**
     * One socket's callbacks, guarded by identity: a socket we have already replaced or torn down
     * can still deliver events, and they must be ignored rather than mutate the live FSM.
     */
    private inner class Socket : TransportListener {
        var transport: Transport? = null

        override fun onOpen() = post {
            if (socket !== this) return@post
            // §6 step 1 of the resume order: hello, always first, before anything else.
            transport?.send(Hello(token = token, sdk = options.sdk).encode())
            startHelloTimeout()
        }

        override fun onText(text: String) = post {
            if (socket !== this) return@post
            parseServerFrame(text)?.let { handle(it) }
        }

        override fun onClosed(code: Int, reason: String) = post {
            if (socket !== this) return@post
            socket = null
            down(code, reason)
        }
    }

    private fun handle(frame: ServerFrame) {
        // §3: a batch's inner frames are processed in array order exactly as if received
        // individually — including the rule that an unknown one among them is skipped alone.
        if (frame is Batch) {
            for (element in frame.frames) parseServerFrame(element)?.let { handle(it) }
            return
        }
        emit { onFrame(frame) }
        when (frame) {
            is HelloAck -> handleHelloAck(frame)
            is Pong -> {
                pongJob?.cancel()
                pongJob = null
            }
            else -> Unit // §3/§4 traffic belongs to the layer above (Gate C)
        }
    }

    private fun handleHelloAck(ack: HelloAck) {
        // The server answered. Disarmed ahead of the dedup guard below on purpose: "we heard a
        // readable helloAck" is the fact the timer is watching for, and it does not depend on
        // whether this is the first one.
        helloJob?.cancel()
        helloJob = null
        // §2 says one helloAck per connection. A second one would re-run the whole resume order —
        // re-listens and a pending-write replay — on a connection that is already resumed.
        if (_state.value == ClientState.CONNECTED) return
        // §2 (v1.5): a different epoch means the shard was restored or reset and every rev we hold
        // is from a dead generation. The owner is told before it re-listens, in the same callback.
        val changed = epoch != null && epoch != ack.epoch
        epoch = ack.epoch
        _lastAck.value = ack
        setState(ClientState.CONNECTED)
        // §6: the attempt counter resets only after the connection has held for a while.
        stableJob = scope.launch {
            delay(options.limits.backoffResetMs)
            attempt = 0
        }
        emit { onHelloAck(ack, changed) }
        startPings()
    }

    /**
     * §2 gives helloAck no deadline, and after the upgrade nothing else supplies one: OkHttp's read
     * timeout does not apply to an open web socket, and §5's pong timer is armed from
     * [handleHelloAck] — i.e. only once hello has already succeeded. So a server that accepts the
     * socket and then sends nothing, or sends a `helloAck` this SDK cannot parse (one missing field
     * is enough — [parseServerFrame] drops the frame silently), leaves the FSM in CONNECTING
     * FOREVER with no callback, no log and a [ready] that never returns. This timer is the whole of
     * what closes that window.
     *
     * It does NOT wait for the close handshake to bring the FSM down the way the pong timeout does.
     * A server that has just failed to answer hello is the last thing to trust with answering a
     * close frame, and OkHttp waits a further 60s for that reply before giving up — so [down] is
     * driven straight from here, and the socket is dropped and closed only as cleanup.
     */
    private fun startHelloTimeout() {
        helloJob?.cancel()
        helloJob = scope.launch {
            delay(options.limits.helloTimeoutMs)
            // Cleared BEFORE down(), whose clearTimers() would otherwise cancel the coroutine it is
            // running inside. Same idiom as the pong timer below.
            helloJob = null
            emit { onHelloTimeout() }
            val dead = socket ?: return@launch
            // Detached first, so the onClosed this close provokes fails its `socket !== this`
            // identity check and down() runs exactly once.
            socket = null
            dead.transport?.close(Close.NORMAL, "hello timeout")
            // ABNORMAL, never AUTH: §6 (v1.2) keeps 4401 terminal, and a timeout is not a rejection.
            down(Close.ABNORMAL, "hello timeout")
        }
    }

    /** §5: first ping right after helloAck, then on interval; no pong within the timeout -> close. */
    private fun startPings() {
        pingJob?.cancel()
        pingJob = scope.launch {
            while (true) {
                socket?.transport?.send(Ping(t = System.currentTimeMillis()).encode())
                if (pongJob == null) {
                    // One outstanding pong timer is enough, and it is armed by the ping that has no
                    // answer yet — not reset by every subsequent ping.
                    pongJob = scope.launch {
                        delay(options.pongTimeoutMs)
                        pongJob = null
                        emit { onPongTimeout() }
                        // -> onClosed -> WAITING -> backoff -> CONNECTING
                        socket?.transport?.close(Close.NORMAL, "pong timeout")
                    }
                }
                delay(pingIntervalMs)
            }
        }
    }

    private fun down(code: Int, reason: String) {
        clearTimers()
        // §6 (v1.2): a 4401 must NOT be auto-retried with the same token. Surface it and stop; the
        // app comes back through connect(newToken) once its token source yields one.
        if (code == Close.AUTH) {
            setState(ClientState.CLOSED)
            emit { onAuthFailure(code, reason) }
            return
        }
        if (closing || !options.autoReconnect) {
            setState(ClientState.CLOSED)
            return
        }
        setState(ClientState.WAITING)
        val delayMs = backoffDelay(attempt++, options.limits, random.nextDouble())
        emit { onRetry(attempt, delayMs) }
        retryJob = scope.launch {
            delay(delayMs)
            if (!closing) open()
        }
    }

    private fun clearTimers() {
        helloJob?.cancel()
        pingJob?.cancel()
        pongJob?.cancel()
        retryJob?.cancel()
        stableJob?.cancel()
        helloJob = null
        pingJob = null
        pongJob = null
        retryJob = null
        stableJob = null
    }
}
