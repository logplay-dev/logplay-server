plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:8.1.0")
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.3.0")
}
