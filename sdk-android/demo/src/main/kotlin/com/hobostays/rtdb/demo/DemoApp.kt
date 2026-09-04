package com.hobostays.rtdb.demo

import android.app.Application
import android.content.Context
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.Handler
import android.os.Looper
import android.util.Base64
import android.util.Log
import androidx.lifecycle.ProcessLifecycleOwner
import com.hobostays.rtdb.android.AndroidRtdb
import com.hobostays.rtdb.android.compat.RtdbDatabase
import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.core.ConnectionOptions
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Where `AndroidRtdb.create()` belongs: once per process, on the main thread, in
 * `Application.onCreate`. One client, one socket (Gate A ruling Q6).
 */
class DemoApp : Application() {

    lateinit var client: RtdbClient
        private set

    /** The Firebase-shaped surface the app's 76 files will use. */
    lateinit var database: RtdbDatabase
        private set

    /** §5.7: the app's own token, fetched and kept fresh. Read by the screen for the pill. */
    lateinit var tokens: TokenSource
        private set

    override fun onCreate() {
        super.onCreate()
        // Created with NO token: the one the app connects with arrives from TokenSource, and
        // `connect(token)` is what carries it in. This is the ordering a migrating app copies —
        // build the client at process start, connect when the token lands, and never hold the
        // client's construction hostage to a network call.
        client = AndroidRtdb.create(
            this,
            ConnectionOptions(url = BuildConfig.RTDB_URL, token = "", sdk = "android-demo/0.1.0"),
        )
        database = RtdbDatabase(client)

        tokens = TokenSource(
            device = deviceSlug(),
            fetch = { device -> fetchShadowToken(BuildConfig.SHADOW_TOKEN_URL, BuildConfig.SHADOW_KEY, device) },
            // §6 v1.2: a 4401 stops the FSM and only connect(freshToken) restarts it. This one line
            // is the whole reconnect — the SDK re-sends the listens and the app sees a gap, not a
            // restart. It runs off the main thread; connect() posts to the state dispatcher.
            onToken = { fresh -> client.connect(fresh) },
            lifetimeCapMs = BuildConfig.TOKEN_LIFETIME_SECONDS * 1_000L,
        )
        // The reactive half of §5.7.3. The server is the authority on whether a token is good; our
        // reading of `exp` is a hint, and a revoked token (§10 kick) has a perfectly valid one.
        client.onAuthFailure = { error ->
            Log.i("RtdbDemo", "AUTH FAILURE ${error.code} — refetching")
            tokens.invalidate("${error.code}: ${error.message}")
        }

        // A token already in hand at launch: the pre-minted `rtdbToken` if the build has one, and
        // otherwise — only when there is no shadow key to fetch with — WP3's local dev token, so
        // the emulator flow keeps working byte for byte.
        val seed = BuildConfig.RTDB_TOKEN.ifEmpty { if (BuildConfig.SHADOW_KEY.isEmpty()) devToken() else "" }
        if (seed.isNotEmpty()) {
            tokens.seed(seed)
            client.connect(seed)
        }
        // Ordered after the seed on purpose: a build with no key still connects with whatever it
        // has, and the screen still says why it will never refresh.
        if (BuildConfig.SHADOW_KEY.isEmpty()) tokens.unconfigured()

        logCadence()
        logConnectivity()
    }

    /**
     * The server slugifies this to `[a-z0-9]{,32}` for the `sub` (`shadow-<slug>`), so it is
     * slugified here too — what the app sends and what the token says should not differ.
     */
    private fun deviceSlug(): String =
        android.os.Build.MODEL.lowercase().replace(Regex("[^a-z0-9]"), "").take(32).ifEmpty { "device" }

    /**
     * An INDEPENDENT observer of the same platform signal the SDK wires to `retryNow()` — the demo
     * watching the OS, not the SDK reporting on itself. Gate D lines this timestamp up against the
     * relay's connection log to attribute a reconnect to the network callback rather than to a
     * backoff that happened to expire.
     */
    private fun logConnectivity() {
        val manager = getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        manager.registerNetworkCallback(
            NetworkRequest.Builder().addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET).build(),
            object : ConnectivityManager.NetworkCallback() {
                override fun onAvailable(network: Network) {
                    Log.i("RtdbDemo", "NETWORK AVAILABLE")
                }

                override fun onLost(network: Network) {
                    Log.i("RtdbDemo", "NETWORK LOST")
                }
            },
        )
    }

    /**
     * The line Gate D reads off logcat: state + §5 cadence, every 5s, for the life of the process —
     * including while the app is in the background, which is the half the screen cannot show.
     * A test rig may poll; a real app must not.
     */
    private fun logCadence() {
        val handler = Handler(Looper.getMainLooper())
        handler.post(object : Runnable {
            override fun run() {
                // §5.7: the token schedule rides the cadence the demo already runs, which is why
                // TokenSource owns no timer of its own. Nothing here blocks — tick() decides on
                // this thread and fetches on another.
                tokens.tick()
                Log.i("RtdbDemo", "state=${client.state.value} token=${tokens.state} backgrounded=${client.backgrounded} process=${ProcessLifecycleOwner.get().lifecycle.currentState}")
                handler.postDelayed(this, 5_000)
            }
        })
    }
}

/**
 * A dev HS256 token, the same shape the harness mints (WP1 `signDevToken`). Dev gateway only —
 * production tokens come from the app's IdP and never from a client.
 */
private fun devToken(userId: String = "u_demo", secret: String = "dev-secret"): String {
    fun b64(bytes: ByteArray) = Base64.encodeToString(bytes, Base64.URL_SAFE or Base64.NO_PADDING or Base64.NO_WRAP)
    val head = b64("""{"alg":"HS256","typ":"JWT"}""".toByteArray())
    val claims = b64("""{"sub":"$userId","exp":${System.currentTimeMillis() / 1000 + 86_400}}""".toByteArray())
    val mac = Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(secret.toByteArray(), "HmacSHA256")) }
    return "$head.$claims.${b64(mac.doFinal("$head.$claims".toByteArray()))}"
}
