package org.zeplinko.logplay.server.core.job

enum class JobStatus {
    PENDING,
    ACQUIRED,
    FINISHED,
    FAILED,
    ABORTED,
}

enum class JobEventType {
    CREATED,
    ACQUIRED,
    RELEASED,
    COMPLETED,
    ERROR_REPORTED,
    FAILED,
    ABORTED,
}

enum class ActorType {
    WORKER,
    SYSTEM,
}
