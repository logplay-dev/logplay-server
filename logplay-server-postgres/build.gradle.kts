import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("kotlin-module-base")
    application
    id("com.gradleup.shadow") version "9.2.2"
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("dev.logplay.server.postgres.MainKt")
}

val vertxVersion = "5.0.6"

dependencies {
    implementation(project(":logplay-server-app"))
    implementation(platform("io.vertx:vertx-stack-depchain:$vertxVersion"))
    implementation("io.vertx:vertx-pg-client")
}

tasks.withType<ShadowJar> {
    archiveClassifier.set("fat")
    mergeServiceFiles()
}
