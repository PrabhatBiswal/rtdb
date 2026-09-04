package com.hobostays.rtdb.android.compat

import kotlinx.serialization.Serializable

/**
 * What a migrated app model looks like under ruling Q4: a Kotlin `@Serializable` data class whose
 * serializer the compiler GENERATES — nothing here is discovered by reflection over fields.
 * Shaped after the audit's real ones (FirebaseUserModel and friends), defaults included, because a
 * Firebase model is always older or newer than the data somewhere in a rollout.
 */
@Serializable
data class UserStatus(
    val name: String = "",
    val online: Boolean = false,
    val score: Long = 0,
)
