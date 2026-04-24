package org.zeplinko.logplay.server.web

import io.vertx.core.json.DecodeException
import io.vertx.core.json.Json
import io.vertx.ext.web.RoutingContext

data class ErrorResponse(val error: String?)

class InvalidRequestBodyException(cause: Throwable) :
    RuntimeException("Invalid request body", cause)

class InvalidQueryParameterException(name: String, message: String) :
    RuntimeException("Invalid query parameter '$name': $message")

inline fun <reified T> RoutingContext.parseBody(): T =
    try {
        Json.decodeValue(body().buffer(), T::class.java)
    } catch (e: DecodeException) {
        throw InvalidRequestBodyException(e)
    }
