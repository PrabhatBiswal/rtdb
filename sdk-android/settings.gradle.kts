pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "rtdb-android"

// WORKLOAD §4: a composite build, NOT a restructure of sdk-kotlin into a multi-project build.
// `com.hobostays.rtdb:rtdb-kotlin-core` in :library is substituted by the included build's output.
includeBuild("../sdk-kotlin")

// :demo is deliberately NOT here any more (WP7 Gate C). It has its own build in `demo/`, so that it
// resolves the SDK from ~/.m2 like any other consumer — inside this build, the substitution above
// would have handed it the source tree and the dependency proof would have proved nothing.
include(":library")
