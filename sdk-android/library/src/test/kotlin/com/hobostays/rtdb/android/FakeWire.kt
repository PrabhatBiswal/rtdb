package com.hobostays.rtdb.android

import android.os.Looper
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.ConnectionOptions
import com.hobostays.rtdb.core.Transport
import com.hobostays.rtdb.core.TransportFactory
import com.hobostays.rtdb.core.TransportListener
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.robolectric.Shadows.shadowOf

/**
 * A socket the test drives by hand — the Android twin of `sdk-kotlin`'s `FakeTransport` (which
 * lives in that build's test source set and so cannot be shared across the composite).
 */
class FakeWire : TransportFactory {
    val sent = CopyOnWriteArrayList<String>()
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

            override fun close(code: Int, reason: String) = listener.onClosed(code, reason)
        }
    }

    fun open() = requireNotNull(listener).onOpen()

    fun deliver(text: String) = requireNotNull(listener).onText(text)

    /** The wire died: no close frame, just gone. */
    fun drop(code: Int = 1006) = requireNotNull(listener).onClosed(code, "dropped")

    fun awaitConnect(count: Int, timeoutMs: Long = 5_000) =
        await("socket #$count", timeoutMs) { connects.get() >= count }

    // ------------------------------------------------- what the client sent, for the Java tests

    fun sentTypes(): List<String> = sent.map { field(it, "type") }

    fun sentValues(type: String): List<String> = sentOf(type).map { it["value"].toString() }

    fun sentPaths(type: String): List<String> = sentOf(type).map { field(it, "path") }

    fun awaitSent(type: String, count: Int) =
        await("$count $type frames (sent: ${sentTypes()})") { sentOf(type).size >= count }

    private fun sentOf(type: String): List<JsonObject> =
        sent.map { Json.parseToJsonElement(it) as JsonObject }.filter { field(it, "type") == type }

    private fun field(text: String, name: String) = field(Json.parseToJsonElement(text) as JsonObject, name)

    private fun field(frame: JsonObject, name: String) = frame[name]!!.jsonPrimitive.content

    companion object {
        /**
         * Drain the main looper until `until` holds. Robolectric pauses it, so SDK callbacks sit in
         * its queue until the test thread (which IS the main thread) lets them run; and the producer
         * is a background dispatcher, so no single `idle()` is guaranteed to be enough.
         */
        /** Let anything already queued on the main looper run — for "and nothing else happened". */
        @JvmStatic
        fun drainMainThread() {
            shadowOf(Looper.getMainLooper()).idle()
            Thread.sleep(30)
            shadowOf(Looper.getMainLooper()).idle()
        }

        @JvmStatic
        fun awaitMainThread(what: String, until: Until) {
            val deadline = System.currentTimeMillis() + 5_000
            while (!until.holds()) {
                shadowOf(Looper.getMainLooper()).idle()
                if (System.currentTimeMillis() > deadline) throw AssertionError("timed out waiting for $what")
                Thread.sleep(5)
            }
        }
    }
}

/** A Java-callable condition — `() -> Boolean` is not something Java writes comfortably. */
fun interface Until {
    fun holds(): Boolean
}

/** A connected client over [wire], with callbacks on the main thread, for the Java tests. */
object TestClients {
    @JvmStatic
    fun connected(wire: FakeWire): RtdbClient {
        val client = RtdbClient(
            ConnectionOptions(url = "ws://fake", token = "t", pingIntervalMs = 60_000),
            callbackExecutor = MainThreadExecutor,
            transports = wire,
        )
        client.connect()
        wire.awaitConnect(1)
        wire.open()
        wire.deliver(HELLO_ACK)
        await("CONNECTED") { client.state.value == ClientState.CONNECTED }
        return client
    }
}

/** Poll until `predicate` holds. The producer is a background dispatcher; there is no join point. */
fun await(what: String, timeoutMs: Long = 5_000, predicate: () -> Boolean) {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!predicate()) {
        if (System.currentTimeMillis() > deadline) throw AssertionError("timed out waiting for $what")
        Thread.sleep(2)
    }
}

const val HELLO_ACK: String = """{"type":"helloAck","rev":1,"epoch":1,"region":"test","session":"s_1"}"""
