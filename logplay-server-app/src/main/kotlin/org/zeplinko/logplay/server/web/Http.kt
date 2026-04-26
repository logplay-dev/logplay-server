package org.zeplinko.logplay.server.web

import io.vertx.core.json.DecodeException
import io.vertx.core.json.Json
import io.vertx.ext.web.RoutingContext

/**
 * Standard error envelope returned for every non-2xx response. Stays the same shape across all
 * status codes so clients can branch on `statusCode` first and pull the human-readable message from
 * `error` second.
 */
data class ErrorResponse(val error: String?)

/** Request body could not be parsed as JSON or did not match the expected DTO shape. → 400 */
class InvalidRequestBodyException(cause: Throwable) :
    RuntimeException("Invalid request body", cause)

/** A query parameter failed validation (e.g. `limit=abc` for an integer parameter). → 400 */
class InvalidQueryParameterException(name: String, message: String) :
    RuntimeException("Invalid query parameter '$name': $message")

/**
 * Decodes the request body as the reified type `T` using Vert.x's Jackson configuration.
 *
 * @throws InvalidRequestBodyException if the body is missing, malformed, or does not match `T`. The
 *   original [DecodeException] is preserved as the cause for diagnostics.
 */
inline fun <reified T> RoutingContext.parseBody(): T =
    try {
        Json.decodeValue(body().buffer(), T::class.java)
    } catch (e: DecodeException) {
        throw InvalidRequestBodyException(e)
    }
