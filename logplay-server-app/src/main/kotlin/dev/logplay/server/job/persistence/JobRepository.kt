package dev.logplay.server.job.persistence

interface JobRepository {}

class InMemoryJobRepository : JobRepository {}
