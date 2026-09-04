package com.hobostays.rtdb.android.compat

import com.hobostays.rtdb.api.RtdbClient
import com.hobostays.rtdb.api.RtdbError
import com.hobostays.rtdb.api.RtdbRef
import com.hobostays.rtdb.api.WriteResult
import java.util.concurrent.ConcurrentHashMap
import com.hobostays.rtdb.api.ChildEventListener as CoreChildListener
import com.hobostays.rtdb.api.DataSnapshot as CoreSnapshot
import com.hobostays.rtdb.api.ValueEventListener as CoreValueListener

/**
 * The Firebase-shaped Java surface (WORKLOAD §2/§6 Gate C), shaped by the Gate A audit and its
 * rulings. Method names are Firebase's verbatim, so the app's ~180 reference sites and ~790
 * snapshot reads keep their shape and migrate by changing an import.
 *
 * What is deliberately NOT here, and why:
 * - `previousChildName` and `onChildMoved` — ruling Q2: 0 reads in 190 signatures, 64/64 empty bodies.
 * - `push()`, `runTransaction()`, `ServerValue.*`, queries — §11 Extensions (E2/E3/E4/E5), out of v1.
 * - `onDisconnect()`, `keepSynced()`, persistence — no protocol support / 0 call sites.
 * - a reflection POJO mapper — ruling Q4: `getValue(Class)` uses kotlinx's GENERATED serializers.
 */

/**
 * One database, one client, one socket (ruling Q6). The app's ~14 `FirebaseDatabase.getInstance(url)`
 * instances become top-level path namespaces under this single client.
 */
class RtdbDatabase(private val client: RtdbClient) {

    /** Firebase's `FirebaseDatabase.getReference(path)`. */
    @JvmOverloads
    fun getReference(path: String = ""): DatabaseReference = DatabaseReference(this, client.ref(path))

    /**
     * The namespace helper (ruling Q6), mirroring the app's own
     * `firebaseDatabaseReference(db_name, table)`: `(dbName, table)` -> the path `dbName/table`.
     *
     * At migration the app's URL constants (`liveChatDbUrl()`, `Constant.FLIP_COIN_GAME`, …) become
     * namespace names — the two helper bodies in `Utils`/`CommonMethods` are what change, not the
     * 180 call sites.
     */
    fun reference(dbName: String, table: String): DatabaseReference = getReference("$dbName/$table")

    // Firebase lets removeEventListener() be called on a DIFFERENT reference object than the one
    // that registered (223 sites do exactly that), so the app listener -> core adapter mapping has
    // to live here, on the database, keyed by the pair that identifies a subscription.
    private val adapters = ConcurrentHashMap<Pair<String, Any>, Any>()

    internal fun <T : Any> attach(path: String, listener: Any, adapter: T): T {
        adapters[path to listener] = adapter
        return adapter
    }

    internal fun detach(path: String, listener: Any): Any? = adapters.remove(path to listener)
}

/** Firebase's `DatabaseReference`. Cheap: it holds no state of its own. */
class DatabaseReference internal constructor(
    private val db: RtdbDatabase,
    private val ref: RtdbRef,
) {
    fun getKey(): String? = ref.key

    fun getPath(): String = ref.path

    fun child(relative: String): DatabaseReference = DatabaseReference(db, ref.child(relative))

    // ---------------------------------------------------------------- writes (§4)

    /** §4 `put`. Accepts what the audit found: Map, String, Number, Boolean, null, and @Serializable. */
    @JvmOverloads
    fun setValue(value: Any?, onComplete: CompletionListener? = null) =
        ref.setValue(toJson(value), onComplete?.let { it::onComplete })

    /**
     * §4 `merge` — the app's dominant write (60 sites, all flat `Map<String,Object>`; deep `"a/b"`
     * keys are legal on the wire but the audit found none).
     */
    @JvmOverloads
    fun updateChildren(children: Map<String, Any?>, onComplete: CompletionListener? = null) =
        ref.updateChildren(children.mapValues { (_, child) -> toJson(child) }, onComplete?.let { it::onComplete })

    /** §4 `put` with null. */
    @JvmOverloads
    fun removeValue(onComplete: CompletionListener? = null) = setValue(null, onComplete)

    // ---------------------------------------------------------------- listeners (§3, §7)

    fun addValueEventListener(listener: ValueEventListener): ValueEventListener {
        ref.addValueEventListener(db.attach(ref.path, listener, ValueAdapter(listener)))
        return listener
    }

    /**
     * Firebase's `addListenerForSingleValueEvent` (67 sites). Composed from what the core already
     * has — listen, take the first value, unlisten — NOT §11 E1's `get` frame: no new frame, no
     * protocol change. It costs a full subscription round trip, which E1 is what would fix.
     */
    fun addListenerForSingleValueEvent(listener: ValueEventListener) {
        var done = false
        val self = object : CoreValueListener {
            override fun onDataChange(snapshot: CoreSnapshot) {
                // The unlisten is a post; a second change could land before it takes effect.
                if (done) return
                done = true
                ref.removeEventListener(this)
                listener.onDataChange(DataSnapshot(snapshot))
            }

            override fun onCancelled(error: RtdbError) {
                if (done) return
                done = true
                ref.removeEventListener(this)
                listener.onCancelled(error)
            }
        }
        ref.addValueEventListener(self)
    }

    fun addChildEventListener(listener: ChildEventListener): ChildEventListener {
        ref.addChildEventListener(db.attach(ref.path, listener, ChildAdapter(listener)))
        return listener
    }

    fun removeEventListener(listener: ValueEventListener) {
        (db.detach(ref.path, listener) as? CoreValueListener)?.let { ref.removeEventListener(it) }
    }

    fun removeEventListener(listener: ChildEventListener) {
        (db.detach(ref.path, listener) as? CoreChildListener)?.let { ref.removeEventListener(it) }
    }

    override fun toString(): String = "DatabaseReference(\"${ref.path}\")"
}

/** Firebase's `DataSnapshot`, over the mirror (§7). */
class DataSnapshot internal constructor(private val snapshot: CoreSnapshot) {

    fun getKey(): String? = snapshot.key

    fun getPath(): String = snapshot.path

    fun exists(): Boolean = snapshot.exists()

    /** Plain Java types: String, Long, Double, Boolean, Map, List, or null (371 sites). */
    fun getValue(): Any? = toPlain(snapshot.value)

    /** The typed read (417 sites). `@Serializable` classes and the boxed primitives (ruling Q4). */
    fun <T : Any> getValue(type: Class<T>): T? = fromJson(snapshot.value, type)

    fun child(relative: String): DataSnapshot = DataSnapshot(snapshot.child(relative))

    fun hasChild(relative: String): Boolean = snapshot.child(relative).exists()

    /** Direct children, in mirror order; empty when the value is not an object (24 sites). */
    fun getChildren(): Iterable<DataSnapshot> = snapshot.children.map { DataSnapshot(it) }

    override fun toString(): String = "DataSnapshot(${snapshot.path}=${snapshot.value})"
}

/** Firebase's `ValueEventListener`. Both methods required, exactly as the app writes them today. */
interface ValueEventListener {
    fun onDataChange(snapshot: DataSnapshot)

    /** §3: a sub-scoped err (RULES/BADPATH/TOOBIG) terminates this subscription. */
    fun onCancelled(error: RtdbError)
}

/** Firebase's `ChildEventListener`, minus the two ordering methods (ruling Q2). */
interface ChildEventListener {
    fun onChildAdded(snapshot: DataSnapshot)
    fun onChildChanged(snapshot: DataSnapshot)
    fun onChildRemoved(snapshot: DataSnapshot)
    fun onCancelled(error: RtdbError)
}

/** Firebase's `DatabaseReference.CompletionListener`, carrying §4's three outcomes. */
fun interface CompletionListener {
    fun onComplete(result: WriteResult)
}

private class ValueAdapter(private val listener: ValueEventListener) : CoreValueListener {
    override fun onDataChange(snapshot: CoreSnapshot) = listener.onDataChange(DataSnapshot(snapshot))
    override fun onCancelled(error: RtdbError) = listener.onCancelled(error)
}

private class ChildAdapter(private val listener: ChildEventListener) : CoreChildListener {
    override fun onChildAdded(snapshot: CoreSnapshot) = listener.onChildAdded(DataSnapshot(snapshot))
    override fun onChildChanged(snapshot: CoreSnapshot) = listener.onChildChanged(DataSnapshot(snapshot))
    override fun onChildRemoved(snapshot: CoreSnapshot) = listener.onChildRemoved(DataSnapshot(snapshot))
    override fun onCancelled(error: RtdbError) = listener.onCancelled(error)
}
