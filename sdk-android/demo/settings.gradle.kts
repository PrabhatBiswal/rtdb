// The demo is its OWN Gradle build from WP7 Gate C on, and that is the whole proof rather than a
// tidying: inside `sdk-android` it sat beside `includeBuild("../sdk-kotlin")`, and composite
// substitution would have quietly swapped `com.hobostays.rtdb:rtdb-kotlin-core:0.1.0` back for the
// source project next door. The build would have gone green while proving nothing about the
// published artifact. Out here there is no source to substitute — only ~/.m2.
//
// This file is also the answer to "what does an integrating engineer paste into their build": this,
// and the two `implementation` lines in build.gradle.kts.
pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}
dependencyResolutionManagement {
    repositories {
        // FIRST, and it is the only reason this build resolves at all: 0.1.0 exists nowhere else.
        // A remote repository is a later work package (user ruling 2026-08-30).
        mavenLocal()
        google()
        mavenCentral()
    }
}

rootProject.name = "rtdb-demo"
