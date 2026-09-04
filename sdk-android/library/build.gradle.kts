import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("com.android.library")
    kotlin("android")
    kotlin("plugin.serialization")
    // WORKLOAD §3/§4: Gradle's built-in publisher, and nothing else added.
    `maven-publish`
}

// WORKLOAD §2: the coordinate IS the deliverable — apps consume this as a versioned artifact.
group = "com.hobostays.rtdb"
version = "0.1.0"

android {
    namespace = "com.hobostays.rtdb.android"
    compileSdk = 35

    defaultConfig {
        // WORKLOAD §2. The core is plain JVM with no java.time/Java-8-API use, so no desugaring.
        minSdk = 23
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    // Robolectric (WORKLOAD §3) drives the lifecycle/connectivity wiring on the JVM — no emulator.
    testOptions.unitTests.isIncludeAndroidResources = true

    // An AAR has variants; a Maven coordinate does not. Publishing the RELEASE variant only is the
    // honest mapping — a consumer asking for `rtdb-android:0.1.0` gets the one build we test and
    // ship, not a debug build that happened to sort first.
    publishing {
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

kotlin {
    compilerOptions { jvmTarget = JvmTarget.JVM_17 }
}

dependencies {
    // Composite-substituted by ../sdk-kotlin. `api`, because RtdbClient/RtdbRef/DataSnapshot are
    // this library's public surface — Kotlin callers use them directly.
    api("com.hobostays.rtdb:rtdb-kotlin-core:0.1.0")
    // kotlinx-serialization-json and kotlinx-coroutines-core are NOT re-declared here any more:
    // Gate A ruling Q3 took fix (b) and the core now exposes them as `api`, so they arrive
    // transitively — which is what a consumer of :library gets too. (coroutines-CORE, not -android:
    // the main thread is reached through MainThreadExecutor's Handler, never Dispatchers.Main.)

    // §5's background cadence needs to know when the APP — not an Activity — leaves the screen.
    // The only new runtime dependency in this module, and the one WORKLOAD §3 names.
    api("androidx.lifecycle:lifecycle-process:2.6.2")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.robolectric:robolectric:4.14.1")
}

publishing {
    publications {
        // Registered late: the `release` component does not exist until AGP has created its
        // variants, and referencing it any earlier fails the configuration phase.
        afterEvaluate {
            create<MavenPublication>("release") {
                from(components["release"])
                artifactId = "rtdb-android"
                pom {
                    name = "RTDB Android"
                    description =
                        "Android bindings for the RTDB SDK: main-thread callbacks, the §5 " +
                            "foreground/background ping cadence, ConnectivityManager-driven reconnect, " +
                            "and the Firebase-shaped compat surface a migrating app calls."
                    licenses {
                        license {
                            name = "The Apache License, Version 2.0"
                            url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                        }
                    }
                }
            }
        }
    }
    // mavenLocal only (user ruling 2026-08-30). A remote repository is a later work package.
}
