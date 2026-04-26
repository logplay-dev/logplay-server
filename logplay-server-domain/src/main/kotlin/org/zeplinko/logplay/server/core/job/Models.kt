package org.zeplinko.logplay.server.core.job

import java.time.Instant

/**
 * The central durable-execution aggregate. A job moves through the [JobStatus] lifecycle, with
 * state transitions stored as moves between three tables: `jobs` (authoritative metadata),
 * `job_queue` (PENDING references), and `job_acquired` (ACQUIRED references). The `Job` model is a
 * denormalised read view assembled by the gateway — `status`, `retries`, `acquiredByWorkerId`,
 * `lastAcquiredAt`, and `availableAt` are populated from the table where the job currently lives.
 *
 * @property id deterministic id derived from `(groupId, idempotencyKey)` via [JobIdGenerator]. The
 *   idempotency key itself is not stored on the server — uniqueness within `groupId` is enforced
 *   transitively by the primary key on this id.
 * @property groupId multi-tenant scope; workers acquire jobs by `(groupId, type)`.
 * @property retries number of times the job has been re-enqueued after a worker error. Sourced from
 *   `job_queue.retries` while PENDING and from `job_acquired.retries` while ACQUIRED. `null` for
 *   terminal jobs — the count isn't preserved on the `jobs` table once the job leaves the secondary
 *   tables, and the audit trail (`job_events`) is the source of truth for historical retry counts.
 * @property maxRetries `null` means unlimited retries; otherwise the job transitions to `FAILED`
 *   when `retries` reaches this bound.
 * @property inputData opaque, base64-transcoded on the wire; the server never inspects its shape.
 * @property lastAcquiredAt only populated while `status = ACQUIRED` (mirrors
 *   `job_acquired.acquired_at`); `null` for every other state. Historical acquisition timestamps
 *   are recoverable from job events.
 * @property acquiredByWorkerId set while `status = ACQUIRED`; `null` for every other state.
 * @property availableAt epoch-millis floor for re-acquisition; populated only while `status =
 *   PENDING`. `0` means "no deadline / always acquirable." Used by sleep-driven release.
 * @property terminalAt set when `status` is terminal (`FINISHED`, `FAILED`, `ABORTED`); the moment
 *   the terminal transition was recorded.
 * @property outputData populated only on `FINISHED`; opaque bytes.
 */
data class Job(
    val id: String,
    val groupId: String,
    val name: String,
    val type: String,
    val status: JobStatus,
    val retries: Int?,
    val maxRetries: Int? = null,
    val inputData: ByteArray? = null,
    val createdAt: Instant,
    val updatedAt: Instant,
    val lastAcquiredAt: Instant? = null,
    val acquiredByWorkerId: String? = null,
    val availableAt: Long? = null,
    val terminalAt: Instant? = null,
    val outputData: ByteArray? = null,
)

/**
 * A durable snapshot of execution progress within a job. Checkpoints form a singly-linked chain per
 * job — each entry references its predecessor by id, and a database constraint forbids two
 * checkpoints from claiming the same chain position.
 *
 * Equality is by [id] only because `data` is a `ByteArray` and structural equality on byte arrays
 * is content-equality, which is unnecessary and expensive for an entity already keyed by id.
 *
 * @property id deterministic id derived from `(jobId, previousCheckpointId)` via
 *   [CheckpointIdGenerator]; uniqueness of the chain position is enforced by the primary key.
 * @property previousCheckpointId `null` for the first checkpoint of a job.
 * @property orderKey monotonically increasing within a job (1-based, computed inside the
 *   server-side row lock); used as the cursor for pagination.
 * @property data opaque payload bytes. Nullable — workers may save chain markers without a payload.
 */
data class Checkpoint(
    val id: String,
    val jobId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val createdAt: Instant,
    val orderKey: Long,
    val data: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Checkpoint

        return id == other.id
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }
}

/**
 * Insertion-time projection of a [Job] — only the fields that are authoritative when a job is first
 * created. State (`status`, `retries`, `availableAt`, ...) is implicit (a freshly inserted job is
 * always PENDING with retries=0, availableAt=0); terminal fields and `outputData` don't exist yet.
 * The gateway returns a fully assembled [Job] so callers don't have to round-trip.
 */
data class NewJob(
    val id: String,
    val groupId: String,
    val name: String,
    val type: String,
    val maxRetries: Int? = null,
    val inputData: ByteArray? = null,
    val createdAt: Instant,
)

/** Input for [CreateJobUseCase]. See [Job] for field semantics. */
data class CreateJobCommand(
    val groupId: String,
    val name: String,
    val type: String,
    val maxRetries: Int? = null,
    val idempotencyKey: String,
    val inputData: ByteArray? = null,
)

/** Input for [AcquirePendingJobsUseCase]. */
data class AcquirePendingJobsCommand(
    val groupId: String,
    val type: String,
    val workerId: String,
    val limit: Int,
)

/**
 * Input for [SaveJobCheckpointUseCase]. `previousCheckpointId` must equal the job's current tail
 * checkpoint id (or `null` for the first checkpoint); otherwise the use case throws
 * [InvalidCheckpointOrderException].
 *
 * Custom equals/hashCode delegate to `ByteArray.contentEquals` / `contentHashCode` so two commands
 * carrying the same payload bytes compare equal in tests.
 */
data class SaveJobCheckpointCommand(
    val jobId: String,
    val workerId: String,
    val previousCheckpointId: String?,
    val name: String?,
    val data: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SaveJobCheckpointCommand

        if (jobId != other.jobId) return false
        if (workerId != other.workerId) return false
        if (previousCheckpointId != other.previousCheckpointId) return false
        if (name != other.name) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = jobId.hashCode()
        result = 31 * result + workerId.hashCode()
        result = 31 * result + (previousCheckpointId?.hashCode() ?: 0)
        result = 31 * result + (name?.hashCode() ?: 0)
        result = 31 * result + (data?.contentHashCode() ?: 0)
        return result
    }
}

/**
 * Input for [GetCheckpointsUseCase].
 *
 * @property after cursor — the id of the last checkpoint already seen by the caller. `null` for the
 *   first page.
 * @property limit page size; `null` falls back to the use-case default.
 */
data class GetCheckpointsCommand(val jobId: String, val after: String?, val limit: Int?)

/**
 * One page of checkpoints returned by [GetCheckpointsUseCase]. `hasMore` indicates whether the
 * caller should issue another request using the last item's id as the next `after` cursor.
 */
data class CheckpointPage(val checkpoints: List<Checkpoint>, val hasMore: Boolean)

/** Input for [CompleteJobUseCase]. */
data class CompleteJobCommand(
    val jobId: String,
    val workerId: String,
    val outputData: ByteArray? = null,
)

/**
 * Input for [ReleaseJobUseCase].
 *
 * @property availableAt optional epoch-millis floor for re-acquisition. `null` (or `0`) means the
 *   job becomes available immediately. Non-zero values are used by SDK sleep to defer
 *   re-acquisition until the wake-at deadline. The server treats this as a "don't pick this back up
 *   before T" hint and never compares the value against its own clock for any purpose other than
 *   the acquire gate.
 */
data class ReleaseJobCommand(val jobId: String, val workerId: String, val availableAt: Long? = null)

/**
 * Input for [ReportExecutionErrorUseCase]. The optional `error` is recorded on the corresponding
 * `ERROR_REPORTED` event's `eventDetail` for later forensic inspection.
 */
data class ReportExecutionErrorCommand(val jobId: String, val workerId: String, val error: String?)

/** Input for [AbortJobUseCase]. */
data class AbortJobCommand(val jobId: String)

/**
 * Result returned by the `compute` lambda passed to [JobGateway.reportExecutionError]. Carries the
 * resolved next state, the new retry count, and the events to persist atomically with the state
 * transition.
 */
data class ExecutionErrorResult(val status: JobStatus, val retries: Int, val events: List<JobEvent>)

/**
 * Immutable audit-log entry. Every state transition appends one (or, for `FAILED`, two) of these.
 *
 * @property actorType `WORKER` for transitions initiated by a worker call, `SYSTEM` for
 *   server-initiated transitions (failure after max retries, abort, dead-worker cleanup).
 * @property actorId worker id when [actorType] is `WORKER`, otherwise `null`.
 * @property eventDetail extended free-form info — typically a stack trace for `ERROR_REPORTED`, or
 *   a JSON blob like `{"availableAt": <ms>}` for `RELEASED` events that carry a sleep deadline.
 */
data class JobEvent(
    val id: String,
    val jobId: String,
    val eventType: JobEventType,
    val actorType: ActorType,
    val actorId: String?,
    val createdAt: Instant,
    val eventMessage: String?,
    val eventDetail: String?,
)
