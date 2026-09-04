package com.hobostays.rtdb.android

import android.os.Looper
import com.hobostays.rtdb.api.DataSnapshot
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.ValueEventListener
import com.hobostays.rtdb.core.ConnectionOptions
import java.util.concurrent.atomic.AtomicReference
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config

/**
 * The Gate A claim: with [MainThreadExecutor] in WP2's `callbackExecutor` seam, an app callback
 * lands on the MAIN looper even though the mirror and the socket live on a background dispatcher.
 *
 * Driven end to end through a real [RtdbClient] over a fake transport rather than by calling the
 * executor directly — an `Executor` that posts to a Handler is Android's behaviour, not ours; that
 * the SDK actually routes its callbacks through it is the part worth a test.
 *
 * API 23 (WORKLOAD §2's minSdk), so nothing here can quietly depend on a newer platform.
 */
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [23])
class MainThreadExecutorTest {

    @Test
    fun `value callbacks are delivered on the main looper`() {
        val wire = FakeWire()
        val client = RtdbClient(
            ConnectionOptions(url = "ws://fake", token = "t", autoReconnect = false),
            callbackExecutor = MainThreadExecutor,
            transports = wire,
        )
        client.use {
            client.connect()
            awaitDrained("the client to open a socket") { wire.connects.get() == 1 }
            wire.open()
            wire.deliver(HELLO_ACK)

            // AtomicReference, not a plain local: the callback runs on the main looper while this
            // test thread reads it, and Kotlin has no @Volatile for locals.
            val callbackThread = AtomicReference<Thread>()
            val delivered = AtomicReference<DataSnapshot>()
            client.ref("room").addValueEventListener(object : ValueEventListener {
                override fun onDataChange(snapshot: DataSnapshot) {
                    callbackThread.set(Thread.currentThread())
                    delivered.set(snapshot)
                }
            })
            wire.deliver("""{"type":"snapshot","subId":1,"path":"room","value":{"score":42},"rev":7}""")

            awaitDrained("onDataChange") { delivered.get() != null }
            val snapshot = delivered.get()

            assertEquals("room", snapshot.path)
            assertEquals("""{"score":42}""", snapshot.value.toString())
            assertEquals(Looper.getMainLooper().thread, callbackThread.get())
            // Cheap but load-bearing: if the SDK had simply run the callback inline on its own
            // dispatcher, the assert above could still pass by accident on a single-threaded test.
            assertNotEquals("rtdb-client", callbackThread.get().name)
        }
    }

    /**
     * Robolectric pauses the main looper, so the SDK's posts sit in its queue until the test thread
     * (which IS the main thread) drains them. Poll: the producer is a background dispatcher, so
     * there is no single point at which one `idle()` is guaranteed to be enough.
     */
    private fun awaitDrained(what: String, done: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 5_000
        while (!done()) {
            shadowOf(Looper.getMainLooper()).idle()
            if (System.currentTimeMillis() > deadline) throw AssertionError("timed out waiting for $what")
            Thread.sleep(5)
        }
    }
}
