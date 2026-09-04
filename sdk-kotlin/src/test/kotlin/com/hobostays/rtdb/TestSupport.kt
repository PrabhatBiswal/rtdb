package com.hobostays.rtdb

import com.hobostays.rtdb.api.ChildEventListener
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.DataSnapshot
import com.hobostays.rtdb.api.RtdbError
import com.hobostays.rtdb.api.ValueEventListener
import com.hobostays.rtdb.api.WriteResult
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Limits
import com.hobostays.rtdb.core.OkHttpTransportFactory
import com.hobostays.rtdb.core.Connection
import com.hobostays.rtdb.core.ConnectionListener
import com.hobostays.rtdb.core.HelloAck
import com.hobostays.rtdb.core.ServerFrame
import com.hobostays.rtdb.core.Transport
import com.hobostays.rtdb.core.TransportFactory
import com.hobostays.rtdb.core.TransportListener
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

/**
 * A state dispatcher the TEST drives: nothing runs until [drain] is called, and then everything
 * queued runs in order on the calling thread. It exists because the P1 races are about WHERE a
 * block lands relative to a teardown, and a test that tries to win that window with sleeps is a
 * test that passes for the wrong reason. With this, the interleaving is chosen, not raced for.
 */
class ManualDispatcher : CoroutineDispatcher() {
    private val queue = ConcurrentLinkedQueue<Runnable>()

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        queue += block
    }

    /** Run everything queued, including whatever those tasks queue behind them. */
    fun drain(): Int {
        var ran = 0
        while (true) {
            val next = queue.poll() ?: return ran
            next.run()
            ran++
        }
    }
}

/** JSON literals, so the ported tests read like WP1's. */
fun json(text: String): JsonElement = Json.parseToJsonElement(text)

/** Poll until `predicate` holds — the Kotlin twin of the harness's `waitUntil`. */
fun waitUntil(label: String, timeoutMs: Long = 5_000, predicate: () -> Boolean) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!predicate()) {
        check(System.currentTimeMillis() < deadline) { "timed out waiting for $label" }
        Thread.sleep(2)
    }
}

fun Connection.awaitState(state: ClientState, timeoutMs: Long = 5_000) = runBlocking {
    withTimeout(timeoutMs) { this@awaitState.state.first { it == state } }
}

/**
 * Wait for a state the FSM may pass THROUGH. A StateFlow conflates, so a 40ms WAITING between two
 * connects can be missed entirely by [awaitState]; the listener records every transition.
 */
fun RecordingListener.awaitState(state: ClientState, timeoutMs: Long = 5_000) =
    waitUntil("state $state", timeoutMs) { states.contains(state) }

fun RtdbClient.awaitState(state: ClientState, timeoutMs: Long = 5_000) = runBlocking {
    withTimeout(timeoutMs) { this@awaitState.state.first { it == state } }
}

/**
 * A socket the test drives by hand: no server, no threads, no timing. The FSM cannot tell the
 * difference, which is the point of [TransportFactory] existing at all (WORKLOAD §4).
 */
class FakeTransport : TransportFactory {
    val sent = CopyOnWriteArrayList<String>()
    val closes = CopyOnWriteArrayList<Int>()
    val connects = AtomicInteger()

    @Volatile
    private var listener: TransportListener? = null

    override fun connect(url: String, listener: TransportListener): Transport {
        this.listener = listener
        connects.incrementAndGet()
        return object : Transport {
            override fun send(text: String) {
                sent += text
            }

            override fun close(code: Int, reason: String) {
                closes += code
                listener.onClosed(code, reason)
            }
        }
    }

    /** The socket finished connecting. */
    fun open() = requireNotNull(listener).onOpen()

    /** One server -> client frame, as raw wire text. */
    fun deliver(text: String) = requireNotNull(listener).onText(text)

    /** The wire died: no close frame, just gone. */
    fun drop(code: Int = 1006, reason: String = "dropped") = requireNotNull(listener).onClosed(code, reason)

    fun waitForSent(count: Int, label: String = "$count frames sent") =
        waitUntil(label) { sent.size >= count }
}

/** Every callback the FSM makes, in order, for tests to assert on. */
class RecordingListener : ConnectionListener {
    val states = CopyOnWriteArrayList<ClientState>()
    val frames = CopyOnWriteArrayList<ServerFrame>()
    val acks = CopyOnWriteArrayList<Pair<HelloAck, Boolean>>()
    val authFailures = CopyOnWriteArrayList<Int>()
    val retries = CopyOnWriteArrayList<Long>()

    @Volatile
    var pongTimeouts = 0

    @Volatile
    var helloTimeouts = 0

    override fun onState(state: ClientState) { states += state }
    override fun onFrame(frame: ServerFrame) { frames += frame }
    override fun onHelloAck(ack: HelloAck, epochChanged: Boolean) { acks += ack to epochChanged }
    override fun onAuthFailure(code: Int, reason: String) { authFailures += code }
    override fun onRetry(attempt: Int, delayMs: Long) { retries += delayMs }
    override fun onPongTimeout() { pongTimeouts++ }
    override fun onHelloTimeout() { helloTimeouts++ }
}

/** A Random whose every roll is the same, so a full-jitter delay becomes an exact number. */
class FixedRandom(private val roll: Double) : Random() {
    override fun nextBits(bitCount: Int): Int = 0
    override fun nextDouble(): Double = roll
}

/** The `type` of every frame the client has sent, in order. */
fun FakeTransport.sentTypes(): List<String> =
    sent.map { (Json.parseToJsonElement(it) as JsonObject)["type"]!!.jsonPrimitive.content }

fun FakeTransport.sentOf(type: String): List<JsonObject> =
    sent.map { Json.parseToJsonElement(it) as JsonObject }.filter { it["type"]!!.jsonPrimitive.content == type }

/** Everything a ValueEventListener is told, in order. */
class ValueRecorder : ValueEventListener {
    val values = CopyOnWriteArrayList<JsonElement>()
    val cancels = CopyOnWriteArrayList<RtdbError>()

    override fun onDataChange(snapshot: DataSnapshot) {
        values += snapshot.value
    }

    override fun onCancelled(error: RtdbError) {
        cancels += error
    }

    val last: JsonElement? get() = values.lastOrNull()

    fun awaitValue(expected: JsonElement, timeoutMs: Long = 5_000) =
        waitUntil("value $expected (last was ${values.lastOrNull()})", timeoutMs) { values.lastOrNull() == expected }
}

/** Child events as "added:key=value" strings — order and content in one readable assertion. */
class ChildRecorder : ChildEventListener {
    val events = CopyOnWriteArrayList<String>()
    val cancels = CopyOnWriteArrayList<RtdbError>()

    override fun onChildAdded(snapshot: DataSnapshot) {
        events += "added:${snapshot.key}=${snapshot.value}"
    }

    override fun onChildChanged(snapshot: DataSnapshot) {
        events += "changed:${snapshot.key}=${snapshot.value}"
    }

    override fun onChildRemoved(snapshot: DataSnapshot) {
        events += "removed:${snapshot.key}=${snapshot.value}"
    }

    override fun onCancelled(error: RtdbError) {
        cancels += error
    }
}

/** Every settlement a write reported, in order (§4). */
class WriteRecorder {
    val results = CopyOnWriteArrayList<WriteResult>()
    val callback: (WriteResult) -> Unit = { results += it }

    fun awaitCount(n: Int, timeoutMs: Long = 5_000) = waitUntil("$n settled writes", timeoutMs) { results.size >= n }
}

// ---------------------------------------------------------------- real-gateway helpers

/** A client on a real gateway, connected and past hello. */
fun rtdbClient(
    url: String,
    token: String = devToken(),
    limits: Limits = Limits(backoffCapMs = 40),
    transports: TransportFactory? = null,
    pingIntervalMs: Long = 60_000,
): RtdbClient = RtdbClient(
    ConnectionOptions(
        url = url,
        token = token,
        sdk = "kotlin/0.1.0",
        limits = limits,
        pingIntervalMs = pingIntervalMs,
    ),
    transports = transports,
).also {
    it.connect()
    runBlocking { withTimeout(20_000) { it.ready() } }
}

/**
 * The server's own view of a path, read over the wire on a throwaway connection — the only honest
 * reference when the gateway is a separate process (WP1's `serverValue`).
 */
fun serverValue(url: String, path: String): JsonElement =
    rtdbClient(url).use { probe ->
        val values = ValueRecorder()
        probe.ref(path).addValueEventListener(values)
        waitUntil("the server's snapshot of \"$path\"", 20_000) { values.values.isNotEmpty() }
        values.last!!
    }

/**
 * The chaos suite's standing assertion (WP1's `assertConverged`): every client mirror equals server
 * state on every path it subscribes to, with nothing left pending. Compares serverState AND the
 * view — a converged client has an empty overlay, and asserting that separately is what catches a
 * write that never settled.
 */
fun assertConverged(url: String, vararg clients: RtdbClient) {
    for ((index, client) in clients.withIndex()) {
        // Convergence is eventual: a delta echo can beat its own ack across the wire, so the queue
        // drains a moment after the value is already right.
        waitUntil("client $index to settle every write", 20_000) { client.pendingWriteIds.isEmpty() }
        for (path in client.subscriptionPaths) {
            val expected = serverValue(url, path)
            waitUntil("client $index to converge at \"$path\"", 20_000) {
                client.mirror.serverValue(path) == expected
            }
            assertEquals(expected, client.mirror.serverValue(path), "client $index diverged at \"$path\"")
            assertEquals(expected, client.value(path), "client $index view != serverState at \"$path\"")
        }
    }
}

/**
 * The real transport with a tap on the inbound wire, so a scenario can assert what the SERVER chose
 * to send (a fresh snapshot vs catch-up deltas, a resync, how many acks) without a hook in the SDK.
 */
class SpyTransports(private val delegate: TransportFactory = OkHttpTransportFactory()) : TransportFactory {
    val received = CopyOnWriteArrayList<String>()

    override fun connect(url: String, listener: TransportListener): Transport =
        delegate.connect(
            url,
            object : TransportListener {
                override fun onOpen() = listener.onOpen()

                override fun onText(text: String) {
                    received += text
                    listener.onText(text)
                }

                override fun onClosed(code: Int, reason: String) = listener.onClosed(code, reason)
            },
        )

    override fun shutdown() = delegate.shutdown()

    /** How many frames of this type arrived, counting inside batches exactly as the client does (§3). */
    fun countOf(type: String): Int = received.sumOf { text ->
        val frame = Json.parseToJsonElement(text) as JsonObject
        when (frame["type"]?.jsonPrimitive?.content) {
            "batch" -> (frame["frames"] as JsonArray).count {
                (it as JsonObject)["type"]?.jsonPrimitive?.content == type
            }
            type -> 1
            else -> 0
        }
    }

    fun framesOf(type: String): List<JsonObject> = received.flatMap { text ->
        val frame = Json.parseToJsonElement(text) as JsonObject
        when (frame["type"]?.jsonPrimitive?.content) {
            "batch" -> (frame["frames"] as JsonArray).map { it as JsonObject }
            else -> listOf(frame)
        }
    }.filter { it["type"]?.jsonPrimitive?.content == type }
}
