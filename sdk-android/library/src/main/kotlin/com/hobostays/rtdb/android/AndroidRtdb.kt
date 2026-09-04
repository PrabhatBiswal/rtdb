package com.hobostays.rtdb.android

import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import androidx.lifecycle.ProcessLifecycleOwner
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.core.ConnectionOptions

/**
 * The Android entry point: one call that builds the client and hands the core the three things only
 * Android can tell it — the main thread (§7), the network coming back (§6), and the app going to
 * the background (§5).
 *
 * ONE client, ONE socket (WP3 Gate A ruling Q6). The app's ~14 Firebase instances become top-level
 * path namespaces (`dbName/table`), not 14 connections; the Java wrapper's `firebaseDatabaseReference`
 * helper is Gate C.
 */
object AndroidRtdb {

    /**
     * Build the client and wire it to the platform. Call from the main thread (the lifecycle
     * observer registration requires it), typically in `Application.onCreate`.
     *
     * It does NOT connect — `client.connect()` is the caller's call, because the token is, and §6
     * (v1.2) makes `connect(freshToken)` the only way back from an auth failure anyway.
     *
     * The wiring lives for the life of the process, which is the life of the one client. A caller
     * that needs to tear it down builds its own client and uses [wire], which hands back a handle.
     */
    @JvmStatic
    fun create(context: Context, options: ConnectionOptions): RtdbClient {
        // WP2's seam, and the whole of §7's Android threading requirement.
        val client = RtdbClient(options, callbackExecutor = MainThreadExecutor)
        wire(context, client)
        return client
    }

    /**
     * Attach the platform signals to an existing client. Main thread. Closing the returned handle
     * unregisters both — tests do; an app process rarely needs to.
     *
     * `lifecycle` is a parameter only so a test can drive its own [Lifecycle]; production always
     * takes the default, the process lifecycle.
     */
    @JvmStatic
    @JvmOverloads
    fun wire(
        context: Context,
        client: RtdbClient,
        lifecycle: Lifecycle = ProcessLifecycleOwner.get().lifecycle,
    ): AutoCloseable {
        val manager = context.applicationContext
            .getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager

        // §6: "Platform network-available signals short-circuit the wait." retryNow() is a no-op
        // unless the FSM is WAITING, so this cannot disturb a live connection — and it deliberately
        // cannot revive a 4401-CLOSED one (§6 v1.2).
        val onNetwork = object : ConnectivityManager.NetworkCallback() {
            override fun onAvailable(network: Network) = client.retryNow()
        }
        // The NetworkRequest form, not registerDefaultNetworkCallback(): that one is API 24 and
        // minSdk is 23 (WORKLOAD §2/§6 Gate B).
        val request = NetworkRequest.Builder()
            .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
            .build()
        manager.registerNetworkCallback(request, onNetwork)

        // §5: 25s foreground / 60s backgrounded. ON_START/ON_STOP on the PROCESS lifecycle are
        // "the app became visible / left the screen", already debounced across configuration
        // changes and activity handoffs — which is exactly why lifecycle-process exists.
        val onLifecycle = LifecycleEventObserver { _, event ->
            when (event) {
                Lifecycle.Event.ON_START -> client.setBackgrounded(false)
                Lifecycle.Event.ON_STOP -> client.setBackgrounded(true)
                else -> Unit
            }
        }
        // Seed from the current state before observing: an SDK created while the process is already
        // backgrounded would otherwise ping at the foreground cadence until the first ON_START.
        client.setBackgrounded(!lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED))
        lifecycle.addObserver(onLifecycle)

        return AutoCloseable {
            manager.unregisterNetworkCallback(onNetwork)
            lifecycle.removeObserver(onLifecycle)
        }
    }
}
