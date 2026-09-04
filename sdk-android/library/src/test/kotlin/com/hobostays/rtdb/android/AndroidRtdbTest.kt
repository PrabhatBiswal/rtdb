package com.hobostays.rtdb.android

import android.content.Context
import android.net.ConnectivityManager
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.core.ConnectionOptions
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.RuntimeEnvironment
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.shadows.ShadowNetwork

/**
 * Gate B's Android half: the two platform signals actually reach WP2's core seams.
 *
 * API 23 — WORKLOAD §2's minSdk, and the reason §6's wiring uses `registerNetworkCallback(request,
 * cb)` rather than the API 24 `registerDefaultNetworkCallback`.
 */
@RunWith(RobolectricTestRunner::class)
@Config(sdk = [23])
class AndroidRtdbTest {

    private val app get() = RuntimeEnvironment.getApplication()

    private val connectivity
        get() = app.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

    private class FakeOwner : LifecycleOwner {
        override val lifecycle = LifecycleRegistry(this)
    }

    private fun client(wire: FakeWire) = RtdbClient(
        // pingIntervalMs long enough that §5 liveness never interferes with what these tests watch.
        ConnectionOptions(url = "ws://fake", token = "t", pingIntervalMs = 60_000),
        callbackExecutor = MainThreadExecutor,
        transports = wire,
    )

    @Test
    fun `a network-available signal short-circuits the backoff wait (§6)`() {
        val wire = FakeWire()
        val client = client(wire)
        client.use {
            AndroidRtdb.wire(app, client, FakeOwner().lifecycle).use {
                val callbacks = shadowOf(connectivity).networkCallbacks
                assertEquals("one NetworkCallback, registered by wire()", 1, callbacks.size)

                client.connect()
                wire.awaitConnect(1)
                wire.open()
                wire.deliver(HELLO_ACK)
                await("CONNECTED") { client.state.value == ClientState.CONNECTED }

                wire.drop() // the radio went away: §6 WAITING, with a backoff of up to 1s
                await("WAITING") { client.state.value == ClientState.WAITING }

                val started = System.currentTimeMillis()
                callbacks.single().onAvailable(ShadowNetwork.newInstance(1))

                wire.awaitConnect(2, timeoutMs = 2_000)
                val elapsed = System.currentTimeMillis() - started
                // §6's first backoff is random(0, 1s), so this bound is not a proof on its own — the
                // deterministic one is sdk-kotlin's ConnectionTest. What fails here without the
                // wiring is the assert above: no callback is registered at all.
                assertTrue("reconnected in ${elapsed}ms", elapsed < 100)
            }
        }
    }

    @Test
    fun `the process lifecycle switches the ping cadence (§5)`() {
        val wire = FakeWire()
        val owner = FakeOwner()
        val lifecycle = owner.lifecycle
        client(wire).use { client ->
            AndroidRtdb.wire(app, client, lifecycle).use {
                // Seeded from the current state: a process that has not started yet is backgrounded.
                await("the seeded cadence") { client.backgrounded }

                lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_CREATE)
                lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_START)
                await("foreground cadence") { !client.backgrounded }

                lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_STOP)
                await("background cadence") { client.backgrounded }

                lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_START)
                await("foreground cadence again") { !client.backgrounded }
            }
        }
    }

    @Test
    fun `closing the handle detaches both signals`() {
        val wire = FakeWire()
        val owner = FakeOwner()
        client(wire).use { client ->
            AndroidRtdb.wire(app, client, owner.lifecycle).close()

            assertEquals(0, shadowOf(connectivity).networkCallbacks.size)
            owner.lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_CREATE)
            owner.lifecycle.handleLifecycleEvent(Lifecycle.Event.ON_START)
            Thread.sleep(50)
            assertTrue("a removed observer must not still be flipping the cadence", client.backgrounded)
        }
    }

    @Test
    fun `create() returns one wired client (Gate A ruling Q6)`() {
        AndroidRtdb.create(app, ConnectionOptions(url = "ws://fake", token = "t")).use { client ->
            assertEquals(1, shadowOf(connectivity).networkCallbacks.size)
            // Not connected by create(): the token, and §6 v1.2's reconnect-with-a-new-token, are
            // the caller's business.
            assertEquals(ClientState.IDLE, client.state.value)
        }
    }
}
