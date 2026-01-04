package dev.logplay.server.core.job

data class CreateJobCommand(val name: String, val type: String)

data class GetPendingJobsCommand(val limit: Int)
