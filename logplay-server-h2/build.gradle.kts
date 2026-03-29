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
    mainClass.set("dev.logplay.server.h2.MainKt")
}

dependencies {
    implementation(project(":logplay-server-app"))
    implementation("com.h2database:h2:2.3.232")
}

tasks.withType<ShadowJar> {
    archiveClassifier.set("fat")
    mergeServiceFiles()
}
