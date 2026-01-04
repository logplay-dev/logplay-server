package dev.logplay.server.job.web

import dev.logplay.server.job.use.case.JobUseCaseLookUp

class JobController(private val jobUseCaseLookUp: JobUseCaseLookUp) {}
