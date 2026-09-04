package com.hobostays.rtdb.demo

import android.text.SpannableStringBuilder
import android.text.Spanned
import android.text.style.ForegroundColorSpan
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * The two pure functions this app is really made of: how a value is rendered, and how the editor's
 * type selector turns text into something the SDK will accept. Both are here, away from the
 * Activity, because both have real branches and both are checked at startup by [selfCheck].
 */

/** The editor's type selector. `NULL` is a delete — §4's `put` with a null value. */
enum class ValueType { STRING, NUMBER, BOOLEAN, JSON, NULL }

/** What the parser produced, or why it could not. */
sealed interface Parsed {
    data class Ok(val value: Any?) : Parsed
    data class Bad(val why: String) : Parsed
}

private val json = Json { ignoreUnknownKeys = true }

/**
 * Text + a chosen type -> a value the compat layer accepts (`Values.toJson` takes primitives, Map,
 * List, or a `JsonElement`). Deliberately strict: a demo that silently coerces "yes" to a string
 * when the user asked for a boolean teaches the wrong thing about the wire.
 */
fun parseValue(type: ValueType, text: String): Parsed = when (type) {
    ValueType.STRING -> Parsed.Ok(text)
    ValueType.NULL -> Parsed.Ok(null)
    ValueType.NUMBER -> {
        val n: Any? = text.trim().toLongOrNull() ?: text.trim().toDoubleOrNull()
        if (n == null) Parsed.Bad("not a number: \"$text\"") else Parsed.Ok(n)
    }
    ValueType.BOOLEAN -> when (text.trim().lowercase()) {
        "true" -> Parsed.Ok(true)
        "false" -> Parsed.Ok(false)
        else -> Parsed.Bad("boolean must be true or false, got \"$text\"")
    }
    ValueType.JSON -> runCatching { json.parseToJsonElement(text) }
        .fold({ Parsed.Ok(it) }, { Parsed.Bad("invalid JSON: ${it.message?.take(80)}") })
}

/**
 * MERGE needs an OBJECT: §4's `merge` is "each key of value is a child put at path/<key>", so a
 * scalar has nothing to merge. Rejecting it here is the difference between the demo teaching that
 * rule and the server teaching it with a BADFRAME.
 */
fun parseMerge(type: ValueType, text: String): Parsed = when (val p = parseValue(type, text)) {
    is Parsed.Bad -> p
    is Parsed.Ok -> {
        val v = p.value
        if (v is JsonObject) Parsed.Ok(v.mapValues { (_, child) -> child as Any })
        else Parsed.Bad("merge needs a JSON object (§4: each key becomes a child put)")
    }
}

// ------------------------------------------------------------------ rendering

class Palette(
    val key: Int,
    val string: Int,
    val number: Int,
    val bool: Int,
    val nul: Int,
    val plain: Int,
)

/**
 * Pretty-print the mirror's plain-Java value with a colour per SCALAR TYPE. `getValue()` hands back
 * Long/Double/Boolean/String/Map/List/null (Firebase's own shapes), so the type is right there and
 * a reader never has to infer it from quoting.
 */
fun renderJson(value: Any?, p: Palette, indent: Int = 0, out: SpannableStringBuilder = SpannableStringBuilder()): SpannableStringBuilder {
    val pad = "  ".repeat(indent)
    when (value) {
        null -> out.color("null", p.nul)
        is String -> out.color("\"$value\"", p.string)
        is Boolean -> out.color(value.toString(), p.bool)
        is Number -> out.color(value.toString(), p.number)
        is Map<*, *> -> {
            if (value.isEmpty()) return out.color("{}", p.plain)
            out.color("{\n", p.plain)
            value.entries.forEachIndexed { i, (k, v) ->
                out.color("$pad  \"$k\"", p.key).color(": ", p.plain)
                renderJson(v, p, indent + 1, out)
                out.color(if (i == value.size - 1) "\n" else ",\n", p.plain)
            }
            out.color("$pad}", p.plain)
        }
        is List<*> -> {
            if (value.isEmpty()) return out.color("[]", p.plain)
            out.color("[\n", p.plain)
            value.forEachIndexed { i, v ->
                out.append(SpannableStringBuilder("$pad  "))
                renderJson(v, p, indent + 1, out)
                out.color(if (i == value.size - 1) "\n" else ",\n", p.plain)
            }
            out.color("$pad]", p.plain)
        }
        else -> out.color(value.toString(), p.plain)
    }
    return out
}

private fun SpannableStringBuilder.color(text: String, colour: Int): SpannableStringBuilder {
    val start = length
    append(text)
    setSpan(ForegroundColorSpan(colour), start, length, Spanned.SPAN_EXCLUSIVE_EXCLUSIVE)
    return this
}

/** One line for the child list: a scalar inline, a container summarised by size. */
fun summarise(value: Any?): String = when (value) {
    null -> "null"
    is String -> "\"$value\""
    is Map<*, *> -> "{${value.size} ${if (value.size == 1) "key" else "keys"}}"
    is List<*> -> "[${value.size}]"
    else -> value.toString()
}

// ------------------------------------------------------------------ teeth

/**
 * The teeth, and they run on the device at startup rather than in a test harness — the demo build
 * takes no test dependency (WORKLOAD §5.5 approves Material and "nothing else new"), and a parser
 * that silently mis-reads a type would have this app teaching the wrong contract on a screen someone
 * is using to learn it. Failures render in the status header, not just logcat.
 */
fun selfCheck(): String {
    val failures = mutableListOf<String>()
    fun expect(name: String, cond: Boolean) { if (!cond) failures += name }

    expect("string keeps its text", (parseValue(ValueType.STRING, "42") as Parsed.Ok).value == "42")
    expect("number 42 is a Long", (parseValue(ValueType.NUMBER, "42") as Parsed.Ok).value == 42L)
    expect("number 4.5 is a Double", (parseValue(ValueType.NUMBER, "4.5") as Parsed.Ok).value == 4.5)
    expect("number rejects words", parseValue(ValueType.NUMBER, "nope") is Parsed.Bad)
    expect("boolean true", (parseValue(ValueType.BOOLEAN, "TRUE") as Parsed.Ok).value == true)
    expect("boolean rejects yes", parseValue(ValueType.BOOLEAN, "yes") is Parsed.Bad)
    expect("null is a delete", (parseValue(ValueType.NULL, "ignored") as Parsed.Ok).value == null)
    expect("json object parses", parseValue(ValueType.JSON, """{"a":1}""") is Parsed.Ok)
    expect("json rejects garbage", parseValue(ValueType.JSON, "{oops") is Parsed.Bad)
    expect("merge takes an object", parseMerge(ValueType.JSON, """{"a":1}""") is Parsed.Ok)
    expect("merge REFUSES a scalar", parseMerge(ValueType.NUMBER, "1") is Parsed.Bad)
    expect("summarise counts keys", summarise(mapOf("a" to 1, "b" to 2)) == "{2 keys}")

    return if (failures.isEmpty()) "value checks: ${12} OK"
    else "value checks: FAILED — ${failures.joinToString()}"
}
