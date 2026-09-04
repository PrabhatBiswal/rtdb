package com.hobostays.rtdb.android.compat

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializerOrNull

/**
 * The Java boundary: `Object` in, `JsonElement` out, and back again. Nothing above this file deals
 * in `Object`, and nothing below it deals in anything else (WORKLOAD §6 Gate C).
 *
 * The audit (Gate A §1 Q2) says what has to work: `Map<String,Object>` (60 updateChildren + 6
 * setValue sites), `String` (7), `null` (2), `boolean` (2), and exactly ONE POJO.
 */

/** `Object` -> `JsonElement`. Throws on anything the SDK cannot express, at the call site. */
fun toJson(value: Any?): JsonElement = when (value) {
    null -> JsonNull
    is JsonElement -> value
    is String -> JsonPrimitive(value)
    is Boolean -> JsonPrimitive(value)
    is Number -> JsonPrimitive(value)
    is Char -> JsonPrimitive(value.toString())
    is Map<*, *> -> JsonObject(
        value.entries.associate { (key, child) ->
            // Firebase keys are strings; a Map<Integer,…> silently became "1" there too.
            (key?.toString() ?: throw IllegalArgumentException("null key in $value")) to toJson(child)
        },
    )
    // §1: arrays are opaque leaf values, Firebase-style. They are stored and returned whole.
    is Iterable<*> -> JsonArray(value.map { toJson(it) })
    is Array<*> -> JsonArray(value.map { toJson(it) })
    // The one POJO write site (Gate A: FirebaseRandomCall.java:55), and the write-side twin of
    // getValue(Class): a generated serializer, looked up — never a reflection mapper (ruling Q4).
    else -> encodeSerializable(value)
}

/**
 * `JsonElement` -> plain Java types, for `getValue()` with no argument (371 sites).
 * Integers come back as `Long` and fractions as `Double`, which is what Firebase did.
 */
fun toPlain(value: JsonElement): Any? = when (value) {
    is JsonNull -> null
    is JsonPrimitive -> when {
        value.isString -> value.content
        else -> value.booleanOrNull ?: value.longOrNull ?: value.doubleOrNull ?: value.content
    }
    is JsonObject -> value.mapValues { (_, child) -> toPlain(child) }
    is JsonArray -> value.map { toPlain(it) }
}

private val json = Json { ignoreUnknownKeys = true }

private fun encodeSerializable(value: Any): JsonElement {
    val serializer = serializerOrNull(value.javaClass) ?: throw IllegalArgumentException(
        "cannot write ${value.javaClass.name}: the SDK does no reflection mapping — pass a Map, " +
            "a primitive, or annotate the class @Serializable (WP3 Gate A ruling Q4)",
    )
    return json.encodeToJsonElement(serializer, value)
}

/**
 * `JsonElement` -> `T`, for `getValue(Class<T>)` (251 POJO sites + 166 primitive ones).
 *
 * Ruling Q4: kotlinx-serialization's GENERATED serializer, found by lookup — the class carries its
 * own serializer because it is `@Serializable`. No reflection mapper, and no silent success on a
 * class nobody annotated: that throws here rather than handing back a half-filled object.
 */
@Suppress("UNCHECKED_CAST")
fun <T : Any> fromJson(value: JsonElement, type: Class<T>): T? {
    if (value is JsonNull) return null
    // The primitives first: 166 of the audit's getValue(Class) sites are String/Integer/Long/…,
    // and routing those through a serializer would be a Json round trip for nothing.
    primitive(value, type)?.let { return it as T }
    val serializer = serializerOrNull(type) ?: throw IllegalArgumentException(
        "cannot read ${type.name}: annotate it @Serializable (WP3 Gate A ruling Q4)",
    )
    return json.decodeFromJsonElement(serializer, value) as T
}

private fun primitive(value: JsonElement, type: Class<*>): Any? {
    val plain = if (value is JsonPrimitive) toPlain(value) else return null
    return when (type) {
        String::class.java -> plain?.toString()
        java.lang.Boolean::class.java, java.lang.Boolean.TYPE -> plain as? Boolean
        java.lang.Integer::class.java, Integer.TYPE -> (plain as? Number)?.toInt()
        java.lang.Long::class.java, java.lang.Long.TYPE -> (plain as? Number)?.toLong()
        java.lang.Double::class.java, java.lang.Double.TYPE -> (plain as? Number)?.toDouble()
        java.lang.Float::class.java, java.lang.Float.TYPE -> (plain as? Number)?.toFloat()
        java.lang.Short::class.java, java.lang.Short.TYPE -> (plain as? Number)?.toShort()
        Any::class.java -> plain
        else -> null
    }
}
