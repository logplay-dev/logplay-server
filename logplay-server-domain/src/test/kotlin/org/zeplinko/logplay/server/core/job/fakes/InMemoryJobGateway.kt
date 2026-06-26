package org.zeplinko.logplay.server.core.job.fakes

import java.time.Instant
import org.zeplinko.logplay.server.core.job.*

/**
 * In-memory implementation of [JobGateway] mirroring the three-table state machine. Each job's
 * lifecycle state is recorded by its presence in exactly one of [queue] (PENDING), [acquired]
 * (ACQUIRED), or as a non-null [terminalStatuses] entry (FINISHED/FAILED/ABORTED). The [jobs] map
 * holds authoritative metadata regardless of state.
 */
class InMemoryJobGateway : JobGateway {

    private data class QueueRef(
        val groupId: String,
        val type: String,
        val enqueuedAt: Long,
        val retries: Int,
        val availableAt: Long,
    )

    private data class AcquiredRef(
        val groupId: String,
        val type: String,
        val workerId: String,
        val acquiredAt: Long,
        val retries: Int,
    )

    private data class TerminalRef(val status: JobStatus, val terminalAt: Instant)

    private val jobs = mutableMapOf<String, Job>()
    private val queue = mutableMapOf<String, QueueRef>()
    private val acquired = mutableMapOf<String, AcquiredRef>()
    private val terminalStatuses = mutableMapOf<String, TerminalRef>()
    private val checkpoints = mutableMapOf<String, Checkpoint>()
    private val events = mutableListOf<JobEvent>()

    private fun assemble(jobId: String): Job? {
        val base = jobs[jobId] ?: return null
        val terminal = terminalStatuses[jobId]
        if (terminal != null) {
            return base.copy(
                status = terminal.status,
                retries = null,
                terminalAt = terminal.terminalAt,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = null,
                updatedAt = terminal.terminalAt,
            )
        }
        val acq = acquired[jobId]
        if (acq != null) {
            val acquiredAt = Instant.ofEpochMilli(acq.acquiredAt)
            return base.copy(
                status = JobStatus.ACQUIRED,
                retries = acq.retries,
                acquiredByWorkerId = acq.workerId,
                lastAcquiredAt = acquiredAt,
                availableAt = null,
                updatedAt = acquiredAt,
            )
        }
        val q = queue[jobId]
        if (q != null) {
            return base.copy(
                status = JobStatus.PENDING,
                retries = q.retries,
                acquiredByWorkerId = null,
                lastAcquiredAt = null,
                availableAt = q.availableAt,
                updatedAt = Instant.ofEpochMilli(q.enqueuedAt),
            )
        }
        // Job exists in `jobs` but not in any state table — shouldn't happen; treat as PENDING with
        // missing queue entry surfaced as null status.
        return base.copy(status = JobStatus.PENDING, availableAt = 0L, updatedAt = base.createdAt)
    }

    override suspend fun acquirePendingJobs(
        groupId: String,
        type: String,
        workerId: String,
        limit: Int,
    ): List<Job> {
        val now = Instant.now().toEpochMilli()
        val readyIds =
            queue.entries
                .filter { (_, q) -> q.groupId == groupId && q.type == type && q.availableAt <= now }
                .sortedBy { it.value.enqueuedAt }
                .take(limit)
                .map { it.key }
        val acquiredJobs = mutableListOf<Job>()
        for (id in readyIds) {
            val q = queue.remove(id) ?: continue
            acquired[id] =
                AcquiredRef(
                    groupId = q.groupId,
                    type = q.type,
                    workerId = workerId,
                    acquiredAt = now,
                    retries = q.retries,
                )
            acquiredJobs.add(assemble(id)!!)
        }
        return acquiredJobs
    }

    override suspend fun findJobById(id: String): Job? = assemble(id)

    override suspend fun findAndLockJobById(id: String): Job? = assemble(id)

    private fun validateCheckpointConstraints(checkpoint: Checkpoint) {
        val hasDuplicateOrder =
            checkpoints.values.any {
                it.jobId == checkpoint.jobId && it.orderKey == checkpoint.orderKey
            }
        if (hasDuplicateOrder) throw InvalidCheckpointOrderException(checkpoint.jobId)
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? = checkpoints[id]

    override suspend fun latestCheckpoint(jobId: String): Checkpoint? =
        checkpoints.values.filter { it.jobId == jobId }.maxByOrNull { it.orderKey }

    override suspend fun findCheckpointsByJobId(
        jobId: String,
        afterOrderKey: Long?,
        limit: Int,
    ): List<Checkpoint> =
        checkpoints.values
            .filter { it.jobId == jobId }
            .let { list ->
                if (afterOrderKey != null) list.filter { it.orderKey > afterOrderKey } else list
            }
            .sortedBy { it.orderKey }
            .take(limit)

    override suspend fun insertJob(newJob: NewJob) {
        if (jobs.containsKey(newJob.id)) throw DuplicateJobIdException(newJob.id)
        jobs[newJob.id] =
            Job(
                id = newJob.id,
                groupId = newJob.groupId,
                name = newJob.name,
                type = newJob.type,
                status = JobStatus.PENDING,
                retries = 0,
                maxRetries = newJob.maxRetries,
                inputData = newJob.inputData,
                createdAt = newJob.createdAt,
                updatedAt = newJob.createdAt,
                availableAt = 0L,
            )
        queue[newJob.id] =
            QueueRef(
                groupId = newJob.groupId,
                type = newJob.type,
                enqueuedAt = newJob.createdAt.toEpochMilli(),
                retries = 0,
                availableAt = 0L,
            )
    }

    override suspend fun insertEvents(events: List<JobEvent>) {
        this.events.addAll(events)
    }

    override suspend fun removeFromQueue(jobId: String) {
        queue.remove(jobId)
    }

    override suspend fun removeFromAcquired(jobId: String) {
        acquired.remove(jobId)
    }

    override suspend fun enqueue(
        jobId: String,
        groupId: String,
        type: String,
        enqueuedAt: Instant,
        retries: Int,
        availableAt: Long,
    ) {
        queue[jobId] =
            QueueRef(
                groupId = groupId,
                type = type,
                enqueuedAt = enqueuedAt.toEpochMilli(),
                retries = retries,
                availableAt = availableAt,
            )
    }

    override suspend fun markTerminal(
        jobId: String,
        status: JobStatus,
        terminalAt: Instant,
        outputData: ByteArray?,
    ) {
        terminalStatuses[jobId] = TerminalRef(status, terminalAt)
        if (outputData != null) {
            jobs[jobId] = jobs.getValue(jobId).copy(outputData = outputData)
        }
    }

    override suspend fun insertCheckpoint(checkpoint: Checkpoint) {
        validateCheckpointConstraints(checkpoint)
        checkpoints[checkpoint.id] = checkpoint
    }

    override suspend fun resetAcquiredRetries(jobId: String) {
        val acq = acquired[jobId] ?: return
        acquired[jobId] = acq.copy(retries = 0)
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        events.filter { it.jobId == jobId }

    override suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
    ): List<String> {
        val ids = acquired.entries.filter { it.value.workerId in workerIds }.map { it.key }
        for (id in ids) {
            val acq = acquired.remove(id)!!
            queue[id] =
                QueueRef(
                    groupId = acq.groupId,
                    type = acq.type,
                    enqueuedAt = updatedAt.toEpochMilli(),
                    retries = acq.retries,
                    availableAt = 0L,
                )
        }
        return ids
    }

    // Test helpers
    fun save(job: Job) {
        jobs[job.id] = job
        when (job.status) {
            JobStatus.PENDING ->
                queue[job.id] =
                    QueueRef(
                        groupId = job.groupId,
                        type = job.type,
                        enqueuedAt = job.updatedAt.toEpochMilli(),
                        retries = job.retries ?: 0,
                        availableAt = job.availableAt ?: 0L,
                    )
            JobStatus.ACQUIRED ->
                acquired[job.id] =
                    AcquiredRef(
                        groupId = job.groupId,
                        type = job.type,
                        // Tests sometimes construct ACQUIRED jobs without binding a worker; fall
                        // back to a sentinel so the fake can still represent the shape.
                        workerId = job.acquiredByWorkerId ?: "test-worker",
                        acquiredAt = (job.lastAcquiredAt ?: job.updatedAt).toEpochMilli(),
                        retries = job.retries ?: 0,
                    )
            JobStatus.FINISHED,
            JobStatus.FAILED,
            JobStatus.ABORTED ->
                terminalStatuses[job.id] = TerminalRef(job.status, job.terminalAt ?: job.updatedAt)
        }
    }

    fun saveTestCheckpoint(checkpoint: Checkpoint) {
        checkpoints[checkpoint.id] = checkpoint
    }

    fun checkpointCount(): Int = checkpoints.size

    fun checkpointsForJob(jobId: String): List<Checkpoint> =
        checkpoints.values.filter { it.jobId == jobId }

    fun allEvents(): List<JobEvent> = events.toList()

    fun eventsForJob(jobId: String): List<JobEvent> = events.filter { it.jobId == jobId }
}
