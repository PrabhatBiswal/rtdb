package com.hobostays.rtdb.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement

/**
 * PROTOCOL §1–§10 frames (v1 CORE). §11 Extensions are deliberately absent.
 *
 * §Transport, which shapes everything here:
 *  - every frame is ONE JSON object with a `type` field — hence [classDiscriminator];
 *  - unknown FIELDS are ignored (and never echoed back: we re-encode from the parsed model);
 *  - unknown frame TYPES are ignored, never errors — see [parseServerFrame]. That is the rule that
 *    lets §11 ship later without breaking this SDK.
 */
val WireJson: Json = Json {
    classDiscriminator = "type"
    ignoreUnknownKeys = true
    encodeDefaults = true
    explicitNulls = false
}

// ---------------------------------------------------------------- client -> server

@Serializable
sealed interface ClientFrame

/** §2: nothing may precede it on a connection (violation: server close 4400). */
@Serializable
@SerialName("hello")
data class Hello(val token: String, val proto: Int = 1, val sdk: String? = null) : ClientFrame

/** §3: no/zero `lastRev` means "send me a snapshot", so a null is simply omitted on the wire. */
@Serializable
@SerialName("listen")
data class Listen(val subId: Int, val path: String, val lastRev: Long? = null) : ClientFrame

@Serializable
@SerialName("unlisten")
data class Unlisten(val subId: Int) : ClientFrame

@Serializable
sealed interface WriteFrame : ClientFrame {
    val writeId: String
    val path: String
}

/** §4: the wire form of BOTH setValue() and removeValue() (`value: null` deletes). */
@Serializable
@SerialName("put")
data class Put(
    override val writeId: String,
    override val path: String,
    val value: JsonElement,
) : WriteFrame

/** §4: keys may be deep relative paths; all children commit atomically under ONE rev. */
@Serializable
@SerialName("merge")
data class Merge(
    override val writeId: String,
    override val path: String,
    val value: JsonObject,
) : WriteFrame

@Serializable
@SerialName("cas")
data class Cas(
    override val writeId: String,
    override val path: String,
    val expectedRev: Long,
    val value: JsonElement,
) : WriteFrame

/** §5: `t` is echoed verbatim; clocks are never compared across machines. */
@Serializable
@SerialName("ping")
data class Ping(val t: Long) : ClientFrame

@Serializable
@SerialName("pong")
data class Pong(val t: Long) : ServerFrame

// ---------------------------------------------------------------- server -> client

@Serializable
sealed interface ServerFrame

@Serializable
@SerialName("helloAck")
data class HelloAck(
    val rev: Long,
    /** §2 (v1.5) the shard generation; a change means every rev the client holds is from a dead one. */
    val epoch: Long,
    val region: String,
    val session: String,
) : ServerFrame

@Serializable
@SerialName("snapshot")
data class Snapshot(
    val subId: Int,
    val path: String,
    val value: JsonElement,
    val rev: Long,
) : ServerFrame

/**
 * §3 — NO subId: a delta is encoded once and broadcast; the client routes it by path.
 * `op` stays a String because §3 requires an unknown op to make the SUBSCRIPTION stale (re-listen),
 * not to fail the frame — an enum would throw here and lose that distinction.
 */
@Serializable
@SerialName("delta")
data class Delta(
    val rev: Long,
    val path: String,
    val op: String,
    val value: JsonElement,
) : ServerFrame

/**
 * §3 micro-batching. The inner frames stay unparsed until [parseServerFrame] takes them one at a
 * time, so ONE unknown inner frame type is ignored on its own instead of discarding its batch-mates.
 */
@Serializable
@SerialName("batch")
data class Batch(val frames: List<JsonElement>) : ServerFrame

/** §3: "your subscription went stale server-side" — a fresh snapshot follows. */
@Serializable
@SerialName("resync")
data class Resync(val subId: Int) : ServerFrame

/** §4: identical for first commit and duplicate replay; the client cannot and need not tell. */
@Serializable
@SerialName("ack")
data class Ack(val writeId: String, val rev: Long) : ServerFrame

/** §4: a normal outcome carrying fresh state, not an error. */
@Serializable
@SerialName("casFail")
data class CasFail(
    val writeId: String,
    val path: String,
    val value: JsonElement,
    val rev: Long,
) : ServerFrame

/**
 * §2/§3/§4 — connection-scoped when bare, sub-scoped with `subId`, write-scoped with `writeId`.
 * `code` is a String for the same reason [Delta.op] is: a code we do not know yet must still parse.
 */
@Serializable
@SerialName("err")
data class Err(
    val code: String,
    val msg: String = "",
    val subId: Int? = null,
    val writeId: String? = null,
) : ServerFrame

/** §2/§10 WebSocket close codes. */
object Close {
    /** A frame arrived before hello (§2). */
    const val PRE_HELLO: Int = 4400

    /** Token rejected (§2); §6 v1.2 forbids auto-retrying it with the same token. */
    const val AUTH: Int = 4401

    /** Admin kick (§10). */
    const val KICK: Int = 4403

    const val NORMAL: Int = 1000

    /** No close frame ever arrived — the wire just stopped. */
    const val ABNORMAL: Int = 1006
}

// ---------------------------------------------------------------- codec

/** The one funnel for outbound frames: encoding through [ClientFrame] is what writes `type`. */
fun ClientFrame.encode(): String = WireJson.encodeToString(this)

/** §Transport: an unknown frame type — or anything else we cannot read — is ignored, never an error. */
fun parseServerFrame(text: String): ServerFrame? =
    try {
        WireJson.decodeFromString<ServerFrame>(text)
    } catch (e: SerializationException) {
        null
    }

/** The same rule for one frame inside a [Batch]. */
fun parseServerFrame(element: JsonElement): ServerFrame? =
    try {
        WireJson.decodeFromJsonElement<ServerFrame>(element)
    } catch (e: SerializationException) {
        null
    }
