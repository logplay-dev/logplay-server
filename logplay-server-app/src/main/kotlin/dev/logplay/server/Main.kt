package dev.logplay.server

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx

fun main() {
    val vertx = Vertx.vertx()
    vertx.deployVerticle(MainVerticle::class.java, DeploymentOptions())
}
