package org.zeplinko.logplay.server.core.job.fakes

import java.time.Instant
import org.zeplinko.logplay.server.core.job.*
import org.zeplinko.logplay.server.core.worker.fakes.InMemoryWorkerGateway

/**
 * In-memory implementation of [JobGateway] mirroring the three-table state machine. Each job's
 * lifecycle state is recorded by its presence in exactly one of [queue] (PENDING), [acquired]
 * (ACQUIRED), or as a non-null [terminalStatuses] entry (FINISHED/FAILED/ABORTED). The [jobs] map
 * holds authoritative metadata regardless of state.
 */
class InMemoryJobGateway(private val workerGateway: InMemoryWorkerGateway? = null) : JobGateway {

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

    /**
     * Test hook fired inside [saveCheckpoint] after `compute()` returns and before the in-memory
     * mutations run. Tests use it to simulate a concurrent lease revocation landing in the gap
     * between the ownership check and the retries-reset write.
     */
    var onAfterCheckpointCompute: (() -> Unit)? = null

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
        eventFactory: ((Job) -> JobEvent)?,
    ): List<Job> {
        if (workerGateway != null) {
            val worker = workerGateway.findWorkerById(workerId)
            if (worker == null || worker.condemned) return emptyList()
        }
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
            val job = assemble(id)!!
            acquiredJobs.add(job)
        }
        if (eventFactory != null) {
            for (job in acquiredJobs) {
                events.add(eventFactory(job))
            }
        }
        return acquiredJobs
    }

    override suspend fun findJobById(id: String): Job? = assemble(id)

    private fun validateCheckpointConstraints(checkpoint: Checkpoint) {
        val hasDuplicateOrder =
            checkpoints.values.any {
                it.jobId == checkpoint.jobId && it.orderKey == checkpoint.orderKey
            }
        if (hasDuplicateOrder) throw InvalidCheckpointOrderException(checkpoint.jobId)
    }

    override suspend fun findCheckpointById(id: String): Checkpoint? = checkpoints[id]

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

    override suspend fun insertJobWithEvent(
        newJob: NewJob,
        event: JobEvent,
    ): InsertJobWithEventResult {
        if (jobs.containsKey(newJob.id)) return InsertJobWithEventResult.AlreadyExists
        // Stash the base in the `jobs` map as a Job view so assemble() can read its immutable
        // fields. The state-derived fields (status, retries, availableAt, ...) are recomputed by
        // assemble() from the queue/acquired/terminal refs we maintain alongside.
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
        events.add(event)
        return InsertJobWithEventResult.Success(assemble(newJob.id)!!)
    }

    override suspend fun findEventsByJobId(jobId: String): List<JobEvent> =
        events.filter { it.jobId == jobId }

    override suspend fun saveCheckpoint(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Checkpoint?) -> Checkpoint,
    ): SaveCheckpointResult {
        if (!jobs.containsKey(jobId)) return SaveCheckpointResult.NotFound
        terminalStatuses[jobId]?.let {
            return SaveCheckpointResult.WrongStatus(it.status)
        }
        val acq = acquired[jobId] ?: return SaveCheckpointResult.WrongStatus(JobStatus.PENDING)
        if (acq.workerId != workerId) return SaveCheckpointResult.WrongWorker(acq.workerId)
        val lastCheckpoint =
            checkpoints.values.filter { it.jobId == jobId }.maxByOrNull { it.orderKey }
        val checkpoint = compute(lastCheckpoint)
        validateCheckpointConstraints(checkpoint)
        onAfterCheckpointCompute?.invoke()
        // Re-classify after the test hook: if the lease was revoked, the job's state has
        // changed and we must surface the actual new state rather than the stale ownership.
        terminalStatuses[jobId]?.let {
            return SaveCheckpointResult.WrongStatus(it.status)
        }
        val current = acquired[jobId] ?: return SaveCheckpointResult.WrongStatus(JobStatus.PENDING)
        if (current.workerId != workerId) return SaveCheckpointResult.WrongWorker(current.workerId)
        checkpoints[checkpoint.id] = checkpoint
        acquired[jobId] = current.copy(retries = 0)
        return SaveCheckpointResult.Success(checkpoint)
    }

    override suspend fun reportExecutionError(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        compute: (Job) -> ExecutionErrorResult,
    ): ReportExecutionErrorResult {
        if (!jobs.containsKey(jobId)) return ReportExecutionErrorResult.NotFound
        terminalStatuses[jobId]?.let {
            return ReportExecutionErrorResult.WrongStatus(it.status)
        }
        val acq =
            acquired[jobId] ?: return ReportExecutionErrorResult.WrongStatus(JobStatus.PENDING)
        if (acq.workerId != workerId) return ReportExecutionErrorResult.WrongWorker(acq.workerId)
        val job = assemble(jobId)!!
        val result = compute(job)
        acquired.remove(jobId)
        when (result.status) {
            JobStatus.PENDING ->
                queue[jobId] =
                    QueueRef(
                        groupId = acq.groupId,
                        type = acq.type,
                        enqueuedAt = updatedAt.toEpochMilli(),
                        retries = result.retries,
                        availableAt = 0L,
                    )
            JobStatus.FAILED -> terminalStatuses[jobId] = TerminalRef(JobStatus.FAILED, updatedAt)
            else ->
                error(
                    "reportExecutionError can only resolve to PENDING or FAILED, got ${result.status}"
                )
        }
        events.addAll(result.events)
        return ReportExecutionErrorResult.Success(assemble(jobId)!!)
    }

    override suspend fun completeJob(
        jobId: String,
        workerId: String,
        outputData: ByteArray?,
        updatedAt: Instant,
        event: JobEvent,
    ): CompleteJobResult {
        if (!jobs.containsKey(jobId)) return CompleteJobResult.NotFound
        terminalStatuses[jobId]?.let {
            return CompleteJobResult.WrongStatus(it.status)
        }
        val acq = acquired[jobId] ?: return CompleteJobResult.WrongStatus(JobStatus.PENDING)
        if (acq.workerId != workerId) return CompleteJobResult.WrongWorker(acq.workerId)
        acquired.remove(jobId)
        terminalStatuses[jobId] = TerminalRef(JobStatus.FINISHED, updatedAt)
        jobs[jobId] = jobs[jobId]!!.copy(outputData = outputData)
        events.add(event)
        return CompleteJobResult.Success(assemble(jobId)!!)
    }

    override suspend fun abortJob(
        jobId: String,
        updatedAt: Instant,
        event: JobEvent,
    ): AbortJobResult {
        if (!jobs.containsKey(jobId)) return AbortJobResult.NotFound
        terminalStatuses[jobId]?.let {
            return AbortJobResult.AlreadyTerminal(it.status)
        }
        // Non-terminal jobs must live in exactly one of queue / acquired (three-table invariant).
        if (queue.remove(jobId) == null && acquired.remove(jobId) == null)
            error("three-table invariant violated: job $jobId has no state row")
        terminalStatuses[jobId] = TerminalRef(JobStatus.ABORTED, updatedAt)
        events.add(event)
        return AbortJobResult.Success(assemble(jobId)!!)
    }

    override suspend fun releaseJob(
        jobId: String,
        workerId: String,
        updatedAt: Instant,
        availableAt: Long?,
        event: JobEvent,
    ): ReleaseJobResult {
        if (!jobs.containsKey(jobId)) return ReleaseJobResult.NotFound
        terminalStatuses[jobId]?.let {
            return ReleaseJobResult.WrongStatus(it.status)
        }
        val acq = acquired[jobId] ?: return ReleaseJobResult.WrongStatus(JobStatus.PENDING)
        if (acq.workerId != workerId) return ReleaseJobResult.WrongWorker(acq.workerId)
        acquired.remove(jobId)
        queue[jobId] =
            QueueRef(
                groupId = acq.groupId,
                type = acq.type,
                enqueuedAt = updatedAt.toEpochMilli(),
                retries = acq.retries,
                availableAt = availableAt ?: 0L,
            )
        events.add(event)
        return ReleaseJobResult.Success(assemble(jobId)!!)
    }

    override suspend fun releaseJobsByWorkerId(
        workerId: String,
        updatedAt: Instant,
        eventFactory: (String) -> JobEvent,
    ): Int = releaseJobsByWorkerIds(listOf(workerId), updatedAt, eventFactory)

    override suspend fun releaseJobsByWorkerIds(
        workerIds: List<String>,
        updatedAt: Instant,
        eventFactory: (String) -> JobEvent,
    ): Int {
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
            events.add(eventFactory(id))
        }
        return ids.size
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
