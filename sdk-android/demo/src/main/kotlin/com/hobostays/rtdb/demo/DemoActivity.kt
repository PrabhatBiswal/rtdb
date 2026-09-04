package com.hobostays.rtdb.demo

import android.graphics.Color
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.view.LayoutInflater
import android.view.ViewGroup
import android.widget.ArrayAdapter
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.hobostays.rtdb.android.compat.ChildEventListener
import com.hobostays.rtdb.android.compat.DataSnapshot
import com.hobostays.rtdb.android.compat.DatabaseReference
import com.hobostays.rtdb.android.compat.ValueEventListener
import com.hobostays.rtdb.api.RtdbError
import com.hobostays.rtdb.api.WriteResult
import com.hobostays.rtdb.core.ClientState
import com.hobostays.rtdb.demo.databinding.ActivityDemoBinding
import com.hobostays.rtdb.demo.databinding.ItemChildBinding

/**
 * The reference consumer (WORKLOAD §5.5). Everything here goes through the Firebase-shaped compat
 * surface, because that is what a migrating app calls — if the demo works, their call sites work.
 *
 * The screen's job is to TEACH the contract: §4 has exactly three outcomes for a write and this
 * shows which one happened, by name, every time.
 */
private const val DEFAULT_READ = "demo/room/members"
private const val SELF_CHECK = "selfcheck"
private val SELF_CHECK_VALUE = DemoMember(name = "r8", score = 8)

class DemoActivity : AppCompatActivity() {

    private lateinit var b: ActivityDemoBinding
    private val app get() = application as DemoApp
    private val ui = Handler(Looper.getMainLooper())

    private var listening: DatabaseReference? = null
    private var listeningPath = ""
    private val children = ChildAdapter()

    /**
     * The highest rev this client has seen ACKNOWLEDGED — not the subscription's lastRev, which the
     * published SDK keeps `internal`. The chip is labelled "ack rev" for that reason: a chip that
     * said "rev" would be lying by vocabulary about which number it is (ruling 2026-08-30). A public
     * lastRev/epoch surface is an 0.2 candidate, to be decided when shadow debugging actually needs
     * it rather than because a demo wanted a chip.
     */
    private var revSeen = 0L
    private var epoch: Long? = null
    private var serializerCheck = "serializer check: waiting"
    private val valueChecks = selfCheck()

    private val valueListener = object : ValueEventListener {
        override fun onDataChange(snapshot: DataSnapshot) {
            val v = snapshot.getValue()
            b.value.text = renderJson(v, palette())
            children.submit(snapshot.getChildren().map { it.getKey().orEmpty() to it.getValue() })
            render()
        }

        /** §3: a sub-scoped err (TOOBIG / RULES / BADPATH) ends this subscription. It renders HERE,
         *  inline on the panel — a TOOBIG on a fat path is a designed answer, not a crash. */
        override fun onCancelled(error: RtdbError) = showPanelError(
            error.code,
            "${error.message}\n\n(§3: the subscription ended. TOOBIG means the subtree exceeds " +
                "SNAPSHOT_MAX — listen lower down.)",
        )
    }

    /**
     * The R8 proof gets its OWN subscription. Hanging it off whatever path the operator happens to
     * be browsing meant the header read "waiting" forever unless they typed the right one — a
     * diagnostic that only appears if you already know where to look is not a diagnostic.
     */
    private val serializerProbe = object : ValueEventListener {
        override fun onDataChange(snapshot: DataSnapshot) {
            serializerCheck = try {
                when (val m = snapshot.getValue(DemoMember::class.java)) {
                    null -> "serializer check: waiting for $SELF_CHECK"
                    SELF_CHECK_VALUE -> "serializer check: OK ($m)"
                    else -> "serializer check: WRONG VALUE ($m)"
                }
            } catch (e: RuntimeException) {
                // What a stripped serializer looks like from the app's side: the lookup finds
                // nothing and the SDK refuses rather than handing back a half-filled object.
                "serializer check: FAILED ${e.javaClass.simpleName}: ${e.message}"
            }
            render()
        }

        override fun onCancelled(error: RtdbError) {
            serializerCheck = "serializer check: cancelled ${error.code}"
        }
    }

    private val childListener = object : ChildEventListener {
        override fun onChildAdded(snapshot: DataSnapshot) = children.flash(snapshot.getKey().orEmpty())
        override fun onChildChanged(snapshot: DataSnapshot) = children.flash(snapshot.getKey().orEmpty())
        override fun onChildRemoved(snapshot: DataSnapshot) = Unit
        override fun onCancelled(error: RtdbError) = Unit
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        b = ActivityDemoBinding.inflate(layoutInflater)
        setContentView(b.root)

        b.endpoint.text = BuildConfig.RTDB_URL
        b.readPath.setText(DEFAULT_READ)
        b.writePath.setText("$DEFAULT_READ/demo1")
        b.writeValue.setText("hello")
        b.children.layoutManager = LinearLayoutManager(this)
        b.children.adapter = children
        b.type.adapter = ArrayAdapter(
            this,
            android.R.layout.simple_spinner_dropdown_item,
            ValueType.entries.map { it.name.lowercase() },
        )

        b.listen.setOnClickListener { startListening(b.readPath.text.toString().trim()) }
        b.unlisten.setOnClickListener { stopListening() }
        b.set.setOnClickListener { write(merge = false) }
        b.merge.setOnClickListener { write(merge = true) }
        b.typed.setOnClickListener { writeTyped() }

        startListening(DEFAULT_READ)
        // Written through setValue(Any?), which routes a non-primitive through the GENERATED
        // serializer — the write-side twin of the getValue(Class) read the header reports on.
        val probe = app.database.getReference("demo/room").child(SELF_CHECK)
        probe.addValueEventListener(serializerProbe)
        probe.setValue(SELF_CHECK_VALUE) { r -> Log.i("RtdbDemo", "selfcheck -> $r") }

        ui.post(object : Runnable {
            override fun run() {
                render()
                ui.postDelayed(this, 1_000)
            }
        })
    }

    override fun onDestroy() {
        super.onDestroy()
        ui.removeCallbacksAndMessages(null)
        stopListening()
    }

    // ------------------------------------------------------------------ read

    private fun startListening(path: String) {
        stopListening()
        b.value.setTextColor(ContextCompat.getColor(this, R.color.text))
        b.value.text = "listening…"
        listeningPath = path
        b.childLabel.text = "children of $path"
        // §1 is validated LOCALLY, at the call site, so a bad path never costs a round trip — but it
        // throws, and an uncaught throw here killed the process. Same rule as a sub-scoped err: an
        // error about the data renders ON the panel. A demo that crashes teaches nothing.
        listening = try {
            app.database.getReference(path).also {
                it.addValueEventListener(valueListener)
                it.addChildEventListener(childListener)
            }
        } catch (e: IllegalArgumentException) {
            showPanelError("BADPATH (refused locally, §1)", e.message.orEmpty())
            null
        }
    }

    private fun showPanelError(title: String, detail: String) {
        b.value.text = "⚠ $title\n$detail"
        b.value.setTextColor(ContextCompat.getColor(this, R.color.bad))
        children.submit(emptyList())
    }

    private fun stopListening() {
        listening?.removeEventListener(valueListener)
        listening?.removeEventListener(childListener)
        listening = null
        children.submit(emptyList())
    }

    // ------------------------------------------------------------------ write

    private fun write(merge: Boolean) {
        val path = b.writePath.text.toString().trim()
        val type = ValueType.entries[b.type.selectedItemPosition]
        val text = b.writeValue.text.toString()
        val parsed = if (merge) parseMerge(type, text) else parseValue(type, text)
        if (parsed is Parsed.Bad) return result(parsed.why, R.color.warn)

        val value = (parsed as Parsed.Ok).value
        val ref = try {
            app.database.getReference(path)
        } catch (e: IllegalArgumentException) {
            return result("BADPATH (refused locally, §1): ${e.message}", R.color.warn)
        }
        val label = if (merge) "merge" else if (value == null) "put(null) = delete" else "put"
        result("$label $path …", R.color.muted)
        if (merge) {
            @Suppress("UNCHECKED_CAST")
            ref.updateChildren(value as Map<String, Any?>) { r -> settled(label, r) }
        } else {
            ref.setValue(value) { r -> settled(label, r) }
        }
    }

    /** The typed-object write: kept as its own control because it is the runtime R8 proof. */
    private fun writeTyped() {
        val path = b.writePath.text.toString().trim()
        val ref = try {
            app.database.getReference(path)
        } catch (e: IllegalArgumentException) {
            return result("BADPATH (refused locally, §1): ${e.message}", R.color.warn)
        }
        result("put(DemoMember) $path …", R.color.muted)
        ref.setValue(SELF_CHECK_VALUE) { r -> settled("put(DemoMember)", r) }
    }

    /**
     * §4 has exactly three outcomes and the demo names each one. `CLOSED` is the fourth thing that
     * used to happen instead — a write that never settled at all — and WP7's P1 fix turned it into
     * a real, named failure. It is on screen because it is the contract, not an edge case.
     */
    private fun settled(what: String, r: WriteResult) {
        Log.i("RtdbDemo", "$what -> $r")
        when (r) {
            is WriteResult.Committed -> {
                revSeen = maxOf(revSeen, r.rev)
                result("$what → ack rev ${r.rev}", R.color.ok)
            }
            is WriteResult.Rejected ->
                result("$what → casFail (server holds ${summarise(r.value)} @ rev ${r.rev})", R.color.warn)
            is WriteResult.Failed -> when (r.error.code) {
                "CLOSED" -> result("$what → ClientClosed: ${r.error.message}", R.color.bad)
                else -> result("$what → err ${r.error.code}: ${r.error.message}", R.color.bad)
            }
        }
    }

    private fun result(text: String, colour: Int) {
        b.result.text = text
        b.result.setTextColor(ContextCompat.getColor(this, colour))
    }

    // ------------------------------------------------------------------ chrome

    private fun render() {
        val state = app.client.state.value
        val token = app.tokens.state
        epoch = app.client.epoch
        val (label, colour) = when {
            // §5.7.3: the token dance gets the pill for as long as it lasts, because the connection
            // state underneath it (CLOSED, then CONNECTING) reads like a fault and is not one.
            //
            // Failed-with-a-retry-pending counts as the dance, and that is not a cosmetic choice:
            // in a real 4401 recovery the FETCHING slice is a few hundred milliseconds and the rest
            // of the visible window is a backoff wait. A pill that only lit up for Fetching showed
            // a red CLOSED for almost the whole thing — naming the symptom (the socket is down)
            // instead of the cause (we are between tokens). Seen on the phone, 2026-08-30.
            token is TokenSource.State.Fetching -> "refreshing token…" to R.color.warn
            token is TokenSource.State.Failed -> {
                val secs = (token.retryAtMs - System.currentTimeMillis() + 999) / 1000
                (if (secs > 0) "refreshing token… (retry ${secs}s)" else "refreshing token…") to R.color.warn
            }
            // The one state that is genuinely stuck: no key, and no connection to show for it.
            // Named rather than left as a mute CLOSED an operator would debug as a network problem.
            token is TokenSource.State.Unconfigured && state != ClientState.CONNECTED ->
                "NO SHADOW KEY" to R.color.bad
            state == ClientState.CONNECTED -> "CONNECTED" to R.color.ok
            state == ClientState.CONNECTING || state == ClientState.WAITING -> state.name to R.color.warn
            else -> state.name to R.color.bad
        }
        b.pill.text = label
        b.pill.background.setTint(ContextCompat.getColor(this, colour))
        val cadence = if (app.client.backgrounded) "60s bg" else "25s fg"
        b.chips.text =
            "epoch ${epoch ?: "—"}   ·   ack rev ${if (revSeen == 0L) "—" else revSeen}   ·   ping $cadence"
        b.checks.text = "$serializerCheck\n$valueChecks\n${tokenLine(token)}"
    }

    /**
     * What the app knows about its own token, in one line. It says WHEN, not what: a token is a
     * bearer credential and putting one on a screen — or in a screenshot for a gate report — is
     * handing it out.
     */
    private fun tokenLine(token: TokenSource.State): String = "token: " + when (token) {
        is TokenSource.State.Unconfigured ->
            "no shadow key configured (set rtdbShadowKey in local.properties) — this build cannot refresh"
        is TokenSource.State.Idle -> "fetching on the next tick"
        is TokenSource.State.Fetching -> "fetching…"
        is TokenSource.State.Held -> {
            val secs = (token.refreshAtMs - System.currentTimeMillis()) / 1000
            val exp = token.expSeconds?.let { "exp readable" } ?: "exp unreadable, assuming 5m"
            "held, refresh in ${if (secs > 0) "${secs}s" else "now"} ($exp)"
        }
        is TokenSource.State.Failed -> {
            val secs = (token.retryAtMs - System.currentTimeMillis()) / 1000
            "FAILED ${token.why} — retry in ${if (secs > 0) "${secs}s" else "now"}"
        }
    }

    private fun palette() = Palette(
        key = ContextCompat.getColor(this, R.color.json_key),
        string = ContextCompat.getColor(this, R.color.json_string),
        number = ContextCompat.getColor(this, R.color.json_number),
        bool = ContextCompat.getColor(this, R.color.json_bool),
        nul = ContextCompat.getColor(this, R.color.json_null),
        plain = ContextCompat.getColor(this, R.color.muted),
    )

    // ------------------------------------------------------------------ children

    private inner class ChildAdapter : RecyclerView.Adapter<ChildAdapter.Row>() {
        private var items: List<Pair<String, Any?>> = emptyList()
        private val flashing = mutableSetOf<String>()
        private var expanded: String? = null

        fun submit(next: List<Pair<String, Any?>>) {
            items = next
            notifyDataSetChanged()
        }

        /** A soft flash, faded rather than blinked (console Gate B). */
        fun flash(key: String) {
            flashing += key
            val i = items.indexOfFirst { it.first == key }
            if (i >= 0) notifyItemChanged(i)
            ui.postDelayed({
                flashing -= key
                val j = items.indexOfFirst { it.first == key }
                if (j >= 0) notifyItemChanged(j)
            }, 900)
        }

        inner class Row(val v: ItemChildBinding) : RecyclerView.ViewHolder(v.root)

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int) =
            Row(ItemChildBinding.inflate(LayoutInflater.from(parent.context), parent, false))

        override fun getItemCount() = items.size

        override fun onBindViewHolder(holder: Row, position: Int) {
            val (key, value) = items[position]
            val open = expanded == key
            holder.v.key.text = key
            holder.v.value.maxLines = if (open) 12 else 1
            holder.v.value.text = if (open) renderJson(value, palette()) else summarise(value)
            holder.v.root.setBackgroundColor(
                if (key in flashing) ContextCompat.getColor(this@DemoActivity, R.color.flash)
                else Color.TRANSPARENT,
            )
            holder.v.root.setOnClickListener {
                expanded = if (open) null else key
                notifyItemChanged(position)
            }
        }
    }
}
