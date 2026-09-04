package com.hobostays.rtdb.core

import com.hobostays.rtdb.json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test

/** Flatten/unflatten, ported from WP1's `test/unit/tree.test.ts`. */
class TreeTest {

    @Test
    fun `flatten walks objects and stops at scalars and arrays (§1)`() {
        assertEquals(
            listOf(
                Leaf("p/name", JsonPrimitive("Ravi")),
                Leaf("p/stats/wins", JsonPrimitive(3)),
                Leaf("p/list", json("""[1,{"x":2}]""")),
            ),
            flatten("p", json("""{"name":"Ravi","stats":{"wins":3},"list":[1,{"x":2}]}""")),
        )
        assertEquals(listOf(Leaf("p", JsonPrimitive(5))), flatten("p", JsonPrimitive(5)))
    }

    @Test
    fun `null and empty objects produce no leaves — both mean nothing is stored here (§1)`() {
        assertEquals(emptyList(), flatten("p", JsonNull))
        assertEquals(emptyList(), flatten("p", json("{}")))
        assertEquals(listOf(Leaf("p/b", JsonPrimitive(1))), flatten("p", json("""{"a":null,"b":1,"c":{}}""")))
    }

    @Test
    fun `unflatten is flatten's inverse, including scalars sitting at the root`() {
        val value = json("""{"name":"Ravi","stats":{"wins":3}}""")
        assertEquals(value, unflatten("p", flatten("p", value)))
        assertEquals(JsonPrimitive(5), unflatten("p", flatten("p", JsonPrimitive(5))))
        assertEquals(JsonNull, unflatten("p", emptyList()), "no leaves reads as null, never as {}")
    }

    @Test
    fun `unflatten rebuilds a subtree relative to its root`() {
        val leaves = listOf(Leaf("a/b/c", JsonPrimitive(1)), Leaf("a/b/d", JsonPrimitive(2)))
        assertEquals(json("""{"c":1,"d":2}"""), unflatten("a/b", leaves))
        assertEquals(json("""{"b":{"c":1,"d":2}}"""), unflatten("a", leaves))
    }
}
