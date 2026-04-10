# LogPlay — Stateful Server, Dumb Storage Architecture

> **Status**: Design exploration — not a migration plan.
> **Scope**: How a stateful, sharded LogPlay server cluster backed by a dumb storage layer would work end-to-end. What every component does, every flow looks like, every failure mode resolves to.
> **Out of scope**: Migration path, code changes, schema diff from today's design.

---

## Table of Contents

1. [Design Goals & Non-Goals](#1-design-goals--non-goals)
2. [Architectural Overview](#2-architectural-overview)
3. [Sharding Model](#3-sharding-model)
4. [Cluster Membership & Shard Ownership](#4-cluster-membership--shard-ownership)
5. [Server Process Internals](#5-server-process-internals)
6. [Storage Layer Contract](#6-storage-layer-contract)
7. [Worker Lifecycle](#7-worker-lifecycle)
8. [Job Lifecycle — End-to-End Flows](#8-job-lifecycle--end-to-end-flows)
9. [Failure Scenarios & Recovery](#9-failure-scenarios--recovery)
10. [Concurrency & Consistency Properties](#10-concurrency--consistency-properties)
11. [Open Questions & Trade-offs](#11-open-questions--trade-offs)
12. [Kubernetes Deployment](#12-kubernetes-deployment)

---

## 1. Design Goals & Non-Goals

### Goals

1. **Horizontal scalability of the control plane** — adding LogPlay server nodes linearly increases throughput. No central coordinator becomes a bottleneck.
2. **Storage backend agnosticism** — the same architecture must work over Postgres, Cassandra, Scylla, FoundationDB, MySQL. Storage must be reduced to "key-value-ish durable log per shard."
3. **No "queue on the database" pattern** — workers must never poll storage for work. Dispatch is push-based, in-process.
4. **At-least-once execution with idempotent state transitions** — every job is guaranteed to be attempted; correctness in the face of duplication is the SDK/checkpoint layer's job.
5. **Bounded recovery time after server failure** — when a server dies, its shards are reassigned and resume work within seconds, not minutes.
6. **No split-brain writes** — at any moment, exactly one server is the legal writer for any given shard. Writes by stale owners are rejected by storage.
7. **Cleanly observable state** — every state transition produces a durable event; the state machine is replay-friendly.

### Non-Goals

- **Strong consistency across shards.** A query that spans many jobs may see slightly stale data per shard. The unit of consistency is the job (and thus its shard).
- **Per-job exactly-once execution.** Workers may execute the same job multiple times due to retries; the SDK uses checkpoints to deduplicate effects.
- **Cross-region active-active.** Out of scope for this document. We assume a single-region cluster.
- **Workflow orchestration / saga primitives.** LogPlay remains a job runner. The SDK builds higher-level abstractions.

---

## 2. Architectural Overview

### Two planes — no external coordination service

```
   ┌────────────────────────────────────────────────────────────────┐
   │                       LogPlay Server Cluster                   │
   │                                                                │
   │   ┌───────────┐    ┌───────────┐    ┌───────────┐              │
   │   │ Server A  │◄──►│ Server B  │◄──►│ Server C  │   ...        │
   │   │           │    │           │    │           │              │
   │   │ Frontend  │    │ Frontend  │    │ Frontend  │  in-cluster  │
   │   │ History   │    │ History   │    │ History   │  gRPC peer   │
   │   │ Matching  │    │ Matching  │    │ Matching  │  forwarding  │
   │   │ Sweepers  │    │ Sweepers  │    │ Sweepers  │              │
   │   └─────┬─────┘    └─────┬─────┘    └─────┬─────┘              │
   │         │                │                │                    │
   └─────────┼────────────────┼────────────────┼────────────────────┘
             │                │                │
             │  range_id-fenced data writes + membership heartbeats
             ▼                ▼                ▼
   ┌────────────────────────────────────────────────────────────────┐
   │                       Dumb Storage Layer                       │
   │                                                                │
   │   Per-shard data:                                              │
   │     shard_meta, jobs, job_events, checkpoints,                 │
   │     transfer_tasks, timer_tasks                                │
   │     (all partitioned/keyed by shard_id; per-shard atomicity)   │
   │                                                                │
   │   Cluster membership:                                          │
   │     cluster_nodes  (one row per pod, heartbeat-refreshed)      │
   │                                                                │
   │   No external coordinator. No etcd. No Zookeeper. No gossip.   │
   └────────────────────────────────────────────────────────────────┘

                         ▲             ▲
                         │ HTTP/gRPC   │ long-poll
                         │             │
                  ┌──────┴────┐   ┌────┴────┐
                  │  Clients  │   │ Workers │
                  │ (creators)│   │         │
                  └───────────┘   └─────────┘
```

The deployment has exactly **two infrastructure components**: the LogPlay server cluster and the storage layer (Postgres or Cassandra/Scylla). There is no etcd, no Zookeeper, no Consul, no gossip protocol, no Kubernetes-specific coordinator. Cluster membership lives in a small table inside the same storage backend that holds job data — see [§4](#4-cluster-membership--shard-ownership) for the full mechanism.

### Three logical responsibilities inside each server

Every server process runs the same three logical components. They are co-resident but conceptually distinct:

- **Frontend** — stateless API surface. Accepts HTTP/gRPC requests from clients and workers, looks up shard ownership, and forwards the request to the right server. May be the same node or another node in the cluster.
- **History Engine** — owns a set of *History Shards*. For each owned shard, it runs the per-job state machine, owns the durable log of state transitions for that shard's jobs, and is the **only legal writer** to that shard.
- **Matching Engine** — owns a set of *Matching Shards*. For each owned shard, it maintains in-memory ready-queues for one or more job types, accepts long-poll requests from workers, and dispatches work.

History Shards and Matching Shards are **independently sharded**. A history shard is keyed by a hash of `job.id`; a matching shard is keyed by a hash of `job.type`. The same node may own some history shards, some matching shards, neither, or both — assignment is independent.

> **Terminology note.** This document uses the existing LogPlay domain model consistently: jobs have an `id`, a `name`, a `type`, and pass through `JobStatus` (`PENDING` → `ACQUIRED` → `FINISHED | FAILED | ABORTED`). The job's `type` — an opaque string label set by the client at creation time (e.g., `"send_email"`, `"resize_image"`) — is what workers declare when they register and what Matching uses for dispatch routing. Throughout this document, "job type" and `job.type` always refer to this field.

### Why this split?

History shards exist because **per-job state needs single-writer locality**. Matching shards exist because **workers want to subscribe to job TYPES, not job IDs**. If history and matching were the same sharding axis, a worker for `send_email` would have to long-poll every history shard. Splitting them lets one worker connect to one matching shard for its job type, regardless of which history shards the actual jobs live in.

### Data flow at 10,000 feet

1. Client `POST /jobs` → frontend → history shard owner for the new job → durably writes job + transfer task → returns to client.
2. History shard owner's transfer-queue-processor reads the transfer task → forwards to matching shard owner for the job type → matching adds to in-memory ready queue.
3. Worker is long-polling matching shard owner for its job type. Matching delivers the job descriptor inline. Worker starts execution.
4. Worker periodically heartbeats. Worker writes checkpoints. Eventually completes / errors / gets aborted.
5. Each transition is sent (via frontend) to the history shard owner, which is the only thing that may write that job's row.

Storage is a passive log. The intelligence is in the server.

### A Walkthrough: How It All Fits Together

The previous subsections described what each component is. This walkthrough shows them in motion, in a way that's meant to be read once for intuition. The detailed sections §3–§9 work through every flow precisely; this one is the mental model to carry into them.

#### The 30-second analogy

Think of the cluster as a restaurant:

- **History** is the kitchen's order book. Every order's full state is written there in durable ink. There is one book per group of orders, and only one cook is allowed to write in that book at a time.
- **Matching** is the server who shouts "order up!" and hands the plate to whichever waiter is standing at the pass. The server doesn't keep any notes of their own — the book already has everything. If the server walks off shift mid-service, a new server walks up, reads the pending orders from the book, and resumes shouting.

More concretely:

- **History** owns durable per-job state. Its data lives in the storage layer. Each history shard is owned by exactly one pod at a time, and that pod is the only one legally allowed to write to that shard. It handles all the heavy lifting: writing job state, recording events, managing checkpoints, handling heartbeats.
- **Matching** is a pure in-memory router. Its state lives nowhere but RAM on the pod that currently owns each matching shard. It holds two things per job type: a FIFO of "ready to run" offers, and a map of workers currently long-polling for that type. When an offer arrives, it hands it to a waiting worker; when a worker arrives first, it parks the worker until an offer shows up.

Everything that matters for correctness lives in History. Matching is a fast dispatcher that can crash at any moment without losing anything, because History will re-feed it from the durable `transfer_tasks` log on the next takeover.

#### What is `range_id`?

`range_id` is a version number stamped on each shard. It's stored on that shard's `shard_meta` row in storage. **Every write the owning pod makes is conditional on this number still matching.**

Think of it as a fence at the entrance to the shard's data: the pod holds a numbered ticket (cached in memory when it acquired the shard), and the storage layer only lets a write through if the ticket matches the number currently painted on the fence. When ownership transfers to a new pod, the new pod bumps the number. The old pod's cached ticket is now stale, and the next write it attempts will be rejected by the fence.

Here is the scenario the fence prevents. Suppose pod A is briefly out of sync (its membership view hasn't caught up yet) and still believes it owns shard 47. Meanwhile, pod B's view is fresher, and pod B takes over shard 47, bumping `range_id` from 100 to 101.

```
Time T0:  shard_meta.range_id = 100, owner hint = pod-A
          pod-A has cached "my range_id for shard 47 = 100"
Time T1:  pod-B's membership view updates. It decides pod-A is gone.
          pod-B runs CAS: "set range_id = 101 IF range_id = 100"
          Storage: success. range_id is now 101.
Time T2:  pod-A processes a request. Builds a fenced write
            "UPDATE ... IF range_id = 100"
          Storage: no, it's 101 now. Write is rejected.
          pod-A catches the exception, drops shard 47 from memory,
          returns a transient error to the caller.
Time T3:  Caller retries. Frontend re-resolves the owner. Routes to pod-B.
          pod-B handles it successfully.
```

**No data corruption.** Membership can be loose (it's eventually consistent from the `cluster_nodes` table); the fence in storage is strict. See §4.8 and §9.12 for the full treatment.

#### End-to-end flow, with each engine labeled

Here is the complete life of one job, annotated with which engine does which step. Follow the indentation: the cursor moves between pods as the request is forwarded.

```
1. Client: POST /jobs { type: "send_email", name: "...", input_data: ... }
     │
     ▼
   Any Frontend (picked by the Kubernetes LoadBalancer)
     │ computes shard_id = hash(job.id) mod N_H
     │ looks up the owning pod from its local MembershipSnapshot
     │ forwards via internal gRPC
     ▼
2. HISTORY ENGINE   [pod that owns this job's history shard]
     │
     │ Mailbox handler writes one fenced batch to storage:
     │   INSERT jobs (status='PENDING', type='send_email', ...)
     │   INSERT job_events (CREATED)
     │   INSERT transfer_tasks (job_id, job_type='send_email', ...)
     │   IF range_id = <current>
     │
     │ Returns 201 to the client.
     ▼
3. HISTORY ENGINE's TransferQueueProcessor   [background loop on same pod]
     │
     │ Reads its own shard's transfer_tasks table
     │   (cheap single-partition range scan)
     │
     │ Sends rpc.OfferJob to the Matching pod that owns
     │   hash("send_email") mod N_M
     ▼
4. MATCHING ENGINE   [pod that owns the matching shard for "send_email"]
     │
     │ Purely in-memory:
     │   if a worker is parked long-polling for "send_email":
     │     hand the offer directly to that parked gRPC stream
     │   else:
     │     append the offer to the in-memory ready queue
     │
     │ ACKs the OfferJob back to History.
     ▼
5. Worker (long-polling Matching for "send_email")
     │
     │ Receives the offer in the long-poll response.
     │ Sends AcceptJob to any Frontend.
     ▼
   Frontend forwards to the HISTORY engine for this job
   (it computes hash(job.id) mod N_H and routes accordingly)
     │
     ▼
6. HISTORY ENGINE
     │
     │ Mailbox handler writes one fenced batch:
     │   UPDATE jobs SET status='ACQUIRED',
     │                   acquired_by_worker_id=W,
     │                   lease_expires_at=now+60s
     │   INSERT job_events (ACQUIRED)
     │   DELETE transfer_tasks WHERE task_seq = <the row from step 3>
     │   IF range_id = <current>
     │
     │ Returns the lease deadline to the worker.
     ▼
7. Worker executes the job.
   Periodically:
     - sends Heartbeat       → routed to HISTORY → extends lease_expires_at
     - sends SaveJobCheckpoint → routed to HISTORY → writes checkpoints row
     ▼
8. Worker finishes.
   Sends CompleteJob → routed to HISTORY
     │
     │ Mailbox handler writes one fenced batch:
     │   UPDATE jobs SET status='FINISHED',
     │                   acquired_by_worker_id=NULL,
     │                   output_data=...
     │   INSERT job_events (COMPLETED)
     │   IF range_id = <current>
     ▼
   Done. Worker frees its slot and long-polls Matching for the next job.
```

Notice the pattern: **anything that touches durable state goes through History. Anything that routes work to a worker goes through Matching. Workers never touch storage directly. Frontends never touch storage. Only History does.**

#### What dies, what survives

The cleanest way to remember the split is to think about what each component is *recoverable from*:

| If this dies... | ...this is lost | ...this survives |
|---|---|---|
| **Frontend on pod X** | Nothing — frontend is stateless | Everything. Clients retry via the LoadBalancer and land on a different pod. |
| **History engine on pod X** | In-memory cache (acquired_jobs map, transfer queue cursor, mailbox backlog) | All durable state in storage. Another pod re-reads it after acquiring the shard (§5.2) and rebuilds the cache. |
| **Matching engine on pod X** | All in-memory ready queues and parked worker long-poll connections | Durable `transfer_tasks` rows in History. The new Matching owner gets re-fed by History's TransferQueueProcessor within seconds. Workers reconnect automatically. |
| **Worker process** | Whatever computation the worker was running in memory | Job state. The job stays ACQUIRED until its lease expires, then History's LeaseExpiryReaper releases it to PENDING (§9.5) and it gets redispatched to another worker. |
| **Storage layer** | Nothing — data is on disk, fully durable | Everything. When storage comes back, pods' mailboxes drain and workers' retries succeed (§9.8). |

The architecture is built around one invariant: **durable state lives in storage, addressed by shard. The server tier is a cache-and-router on top of storage. Any pod can take over for any other pod by reading the durable state.** Crashes in the server tier are always recoverable because the server tier holds no authoritative state of its own.

---

## 3. Sharding Model

### History Shards

A fixed number of history shards, `N_H`, decided at cluster creation time. **This number does not change without a re-sharding migration.** Typical values: 256, 1024, 4096. Pick large enough that hot shards are unlikely; small enough that membership/leasing overhead is manageable.

```
history_shard_id = hash(job_id) mod N_H
```

`N_H` should be much larger than the maximum number of server nodes you ever expect to run. A 1024-shard cluster running on 8 nodes assigns ~128 shards per node. If you grow to 64 nodes, ~16 shards per node. The grain is fine enough that load spreads but coarse enough that one node owning many shards is normal.

### Matching Shards

Independently, a fixed number of matching shards, `N_M`. Same considerations.

```
matching_shard_id = hash(job.type) mod N_M
```

Note: this means **all jobs of the same type land on the same matching shard owner**. If one job type has overwhelming traffic, that matching shard becomes hot. Mitigation: introduce sub-sharding for job types with prefix conventions like `send_email_0`, `send_email_1`, ..., handed out by the SDK — see §11 Q5 for the full discussion. Out of scope for v1.

### Job ID & Idempotency

Idempotency-key uniqueness is enforced **at the history shard layer**. To make this work without a separate global lookup table:

```
job_id = deterministic_hash(idempotency_key)        // e.g. UUIDv5 in a fixed namespace
history_shard_id = hash(job_id) mod N_H
```

Because `job_id` is a pure function of `idempotency_key`, two clients submitting the same idempotency key always produce the same `job_id` and route to the same history shard. The shard owner sees the second `INSERT` arrive for an existing `job_id` and returns the existing job (idempotent). No global uniqueness index needed.

Trade-off: clients can no longer pick their own job IDs. They get one back from the server. This is a feature: the server is the source of truth for identity.

### Carrying shard ID in identifiers

Workers and clients hold opaque `job_id` strings, but the shard ID is derivable from the job_id by anyone who knows `N_H` and the hash function. Frontends compute the shard locally on every request. There is no "where does this job live" lookup table — sharding is by computation.

If `N_H` ever needs to change (re-sharding), this becomes a migration. Plan to never re-shard.

---

## 4. Cluster Membership & Shard Ownership

This section is the heart of the design. It explains how a multi-node LogPlay cluster maintains agreement on which node owns which shard **without any external coordination service** — using only the dumb storage layer that already holds job data.

### 4.1 Design Principle: No External Coordinator

Most "stateful sharded service" designs reach for etcd, Zookeeper, or Consul. We deliberately don't. The reasoning:

- **Operational simplicity.** One stateful dependency (the storage layer) instead of two. Any operator who can run Postgres or Cassandra can run LogPlay.
- **No correlated failure modes.** A separate coordinator can fail in ways the storage layer can't, doubling the surface area for incidents.
- **Storage already provides what we need.** Both Postgres (transactions + conditional updates) and Cassandra (single-partition LWT) provide the linearizable single-row CAS that fencing requires. Adding etcd would be redundant.

The approach is: **put the membership table in the same dumb storage layer that already holds job data, and use periodic polling instead of gossip.** Every pod heartbeats its own row, every pod reads the table to discover peers, and every pod independently computes the same shard assignment from the same snapshot. The trade-off is convergence latency (described in [§4.7](#47-convergence-latency)) for the elimination of an entire moving part.

### 4.2 The `cluster_nodes` Table

A single small table in the storage layer holds the entire cluster's membership.

```
cluster_nodes (
  partition_id      INT,         -- always 0; one logical partition for the whole cluster
  node_id           UUID,         -- random per pod startup; new on every restart
  rpc_address       STRING,       -- "10.4.7.21:8081" — how peers reach this pod
  role              STRING,       -- 'logplay' (or split: 'frontend' / 'history' / 'matching')
  session_start_at  TIMESTAMP,
  last_heartbeat_at TIMESTAMP,    -- refreshed every ~2s
  PRIMARY KEY ((partition_id), role, node_id)
)
```

Notes on the schema:

- **`partition_id`** is a constant (0) so that on Cassandra/Scylla all rows live in a single partition and can be read with one efficient single-partition scan. Postgres ignores this column functionally — it's just there for schema parity and to make the same migration script work on both backends.
- **`node_id`** is a random UUID generated at process startup. It is *not* tied to a Kubernetes pod name. If a pod restarts (gets a new IP), it gets a fresh `node_id`. The old row is reaped (Postgres) or expires via TTL (Cassandra).
- **`rpc_address`** is the in-cluster gRPC endpoint. On Kubernetes this is the pod IP from the downward API. Other pods open direct gRPC connections to this address — see [§12](#12-kubernetes-deployment).
- **`role`** lets the design later split into separate frontend / history / matching processes if needed. For v1, all roles co-locate as `'logplay'`.

The table is **tiny**: one row per running pod. Even at 100 pods, it's 100 rows, 5 KB total. Reads and writes are negligible cost on either backend.

### 4.3 Three Loops on Every Pod

Every LogPlay pod runs three independent loops in the background. They are entirely driven by wall clock and only touch the storage layer.

#### Loop 1: Heartbeat (every 2 seconds)

The pod refreshes its own row in `cluster_nodes` to signal "still alive."

**Cassandra/Scylla:**
```cql
INSERT INTO cluster_nodes
  (partition_id, role, node_id, rpc_address, session_start_at, last_heartbeat_at)
VALUES
  (0, 'logplay', :node_id, :addr, :session_start, toTimestamp(now()))
USING TTL 30;
```
The `USING TTL 30` is the key trick: every heartbeat refreshes the row with a fresh 30-second time-to-live. If the pod stops heartbeating, the row vanishes automatically after 30 seconds. **No reaper needed**, and no tombstone backlog because TTL expiry cleans up as part of normal compaction.

**Postgres:**
```sql
INSERT INTO cluster_nodes
  (partition_id, role, node_id, rpc_address, session_start_at, last_heartbeat_at)
VALUES
  (0, 'logplay', :node_id, :addr, :session_start, now())
ON CONFLICT (partition_id, role, node_id) DO UPDATE
   SET last_heartbeat_at = EXCLUDED.last_heartbeat_at,
       rpc_address       = EXCLUDED.rpc_address;
```
Postgres has no TTL, so dead rows accumulate. They're cleaned up by Loop 4 below.

In both cases, the heartbeat is a single-row write. Pods never contend with each other on this table — each pod owns its own row.

#### Loop 2: Membership refresh (every 5 seconds)

The pod reads the live members from `cluster_nodes` and rebuilds its in-memory **membership view**.

**Cassandra/Scylla:**
```cql
SELECT node_id, rpc_address, last_heartbeat_at
  FROM cluster_nodes
 WHERE partition_id = 0
   AND role = 'logplay';
```
TTL has already removed dead rows; everything returned is alive. (A defensive client-side filter `last_heartbeat_at > now - 10s` guards against clock skew.)

**Postgres:**
```sql
SELECT node_id, rpc_address, last_heartbeat_at
  FROM cluster_nodes
 WHERE last_heartbeat_at > now() - interval '10 seconds';
```
The 10-second freshness window means a pod is considered dead 10 seconds after its last heartbeat.

The result is loaded into a **`MembershipSnapshot`** in memory, atomically replacing the previous one. Subsequent reads of cluster state during the next 5 seconds use this snapshot.

#### Loop 3: Shard reconciliation (every 1 second)

Using the latest `MembershipSnapshot`, the pod computes which shards it *should* own and reconciles its actual ownership against that desire.

```
for shard_id in 0..N_H:                                # history shards
  expected_owner = consistent_hash(shard_id, snapshot)
  if expected_owner == self:
    if shard_id not in owned_shards:
      try_acquire(shard_id)                            # see §4.5
  else:
    if shard_id in owned_shards:
      drop(shard_id)                                   # graceful in-memory release

# identical loop for matching_shard_id in 0..N_M
```

This loop is **purely in-process** — no storage IO unless an acquisition is attempted. With 1024 history shards × 5 matching shards = ~5000 hash computations, each taking nanoseconds, the loop is essentially free.

#### Loop 4: Reaper (every 30 seconds, Postgres only)

A simple `DELETE` removes dead rows from the membership table.

```sql
DELETE FROM cluster_nodes
 WHERE last_heartbeat_at < now() - interval '60 seconds';
```

The reaper runs on **all pods** without coordination — duplicate deletes are harmless. Cassandra doesn't need this loop; TTL handles it natively.

### 4.4 Consistent-Hash Shard Assignment

Every pod independently runs the same pure function:

```
expected_owner(shard_id, snapshot) -> node_id
```

The function is pure: same inputs, same output. As long as every pod sees the same snapshot, every pod arrives at the same answer. **There is no central authority.** Agreement emerges from determinism.

We use **consistent hashing with virtual nodes** (sometimes called "rendezvous hashing" or "highest random weight" — both work, both have the same desirable property):

> When the membership set changes by adding or removing one node, only `1/N` of the shards change their assigned owner.

This matters because shard acquisition is expensive (LWT on Cassandra ≈ 4 round trips). With consistent hashing, a node joining a 10-pod cluster moves ~10% of shards. With naive `shard_id mod live_count` assignment, a node joining moves ~90% of shards. The difference is huge under churn.

The implementation:

1. Each `node_id` (UUID) is hashed to ~128 virtual nodes on a 64-bit ring.
2. To find the owner of `shard_id`, compute `hash(shard_id)` and walk the ring clockwise to the next virtual node. The pod that virtual node belongs to is the owner.
3. Lookup is O(log V) where V is the total virtual node count.

The ring is **rebuilt on every membership snapshot refresh** (every 5s). It's a small data structure — a sorted array of 128 × num_pods entries — and rebuilding is microseconds.

### 4.5 Acquiring a Shard

The reconciliation loop in §4.3 detected "I should own shard 47, I don't yet, let me try." The acquisition flow:

```
try_acquire(shard_id):
  1. Read shard_meta row for shard_id from storage.
     observed_range_id = row.range_id   (or 0 if no row exists yet)
     observed_owner    = row.owner_hint

  2. Compute new_range_id = observed_range_id + 1

  3. Issue conditional CAS:
     - Cassandra: LWT with IF range_id = observed_range_id
     - Postgres:  UPDATE ... WHERE range_id = observed_range_id (check rowcount)

  4. If CAS succeeded:
     - rebuild in-memory shard state by reading all rows for this shard
       (jobs WHERE status=ACQUIRED, transfer_tasks, timer_tasks)
     - start the per-shard mailbox and background loops (§5)
     - mark shard live in owned_shards
     - return success

  5. If CAS failed:
     - someone else acquired it; backoff and try again on next reconciliation cycle
```

#### CAS in Cassandra/Scylla

```cql
UPDATE shard_meta
   SET range_id    = :new_range_id,
       owner_hint  = :node_id,
       acquired_at = toTimestamp(now())
 WHERE shard_id = :shard_id
    IF range_id = :observed_range_id;
```

This is a Paxos-backed lightweight transaction. It returns `[applied]=true` on success or `[applied]=false` with the actual current row on failure. Slow (~4 round trips) but bulletproof — no two pods can both succeed even if they race.

#### CAS in Postgres

```sql
UPDATE shard_meta
   SET range_id    = :new_range_id,
       owner_hint  = :node_id,
       acquired_at = now()
 WHERE shard_id = :shard_id
   AND range_id = :observed_range_id
RETURNING range_id;
```

If `RETURNING` yields a row, the acquisition succeeded. If it yields nothing, contention. This is a single-statement atomic update — Postgres handles row-level locking internally. Fast (~1 round trip).

In both backends, **at most one pod can win**. This is the only place in the entire system that requires linearizability, and both backends provide it for single-row CAS at acceptable cost.

### 4.6 Releasing a Shard

When the reconciliation loop says "I no longer should own shard 42" (because the membership snapshot changed and the consistent hash now points elsewhere), the pod gracefully releases:

```
drop(shard_id):
  1. Stop accepting new requests for this shard.
  2. Drain the mailbox: let in-flight commands complete.
  3. Stop the per-shard background loops.
  4. Drop in-memory state.
  5. Done. (No storage write.)
```

Notice that **the pod does NOT update the `shard_meta` row on release**. The `range_id` stays where it is. The next pod to acquire the shard will simply bump it from there.

This is a deliberate choice: explicit release writes are unnecessary because the next acquirer's CAS handles the transition. Skipping them avoids a class of bugs where "release succeeded but acquire failed" leaves a shard unowned with no one knowing.

### 4.7 Convergence Latency

The price we pay for skipping gossip: shard ownership transitions are slower than systems with active membership protocols.

**When a pod dies abruptly:**

| Phase | Time |
|---|---|
| Pod dies (T0) | 0s |
| Pod's row stops being refreshed | T0 + 0s |
| Other pods' freshness window expires the row in their next snapshot read (Postgres) / TTL expires the row (Cassandra) | T0 + ~10s |
| Other pods' next membership refresh sees the change | + up to 5s (poll interval) |
| Other pods' next reconciliation acts on it | + up to 1s |
| Acquisition CAS completes | + 10-20ms |
| **Worst case shard unavailability** | **~16 seconds** |

This is the time during which **clients hitting jobs in the dead pod's shards will see request errors and retry**. Workers heartbeating into those shards see heartbeat failures and back off. Once the new owner is up, retries succeed and the cluster catches up.

For LogPlay's expected scale (tens of pods, not hundreds; second-scale acceptable recovery), **16 seconds is fine**. Tightening intervals would reduce it but at increased storage chatter — and the chatter is wasted because the rare-event recovery path doesn't justify it.

For comparison: a gossip-based membership protocol typically converges in ~1-3 seconds, and an etcd-lease-based scheme converges in `lease_ttl` (≈9s). Our pure-polling model trades 5-10 seconds of convergence latency for the elimination of an entire moving part — no gossip library, no external coordination service, nothing beyond the storage layer LogPlay already depends on.

### 4.8 The Fencing Token (`range_id`)

The `range_id` from §4.5 is the only thing standing between the cluster and split-brain corruption. **Every storage write a shard owner makes carries the range_id and is rejected if storage's recorded value differs.** See [§6](#6-storage-layer-contract) for the exact write pattern.

This means **even if two pods both believe they own a shard** — and they will, briefly, during membership convergence — only one can successfully write. The pod whose `range_id` matches storage wins. The loser's writes fail with a `StaleRangeException`, and the loser must immediately drop in-memory state for that shard.

The two-layer safety property:

```
  Membership consistency  ─►  best-effort, eventually consistent (5-10s)
            │
            ▼
  Shard acquisition       ─►  linearizable (single-row LWT / conditional UPDATE)
            │
            ▼
  Shard data writes       ─►  fenced by range_id on every write
            │
            ▼
  Job state transitions   ─►  linearizable per job (mailbox + range_id)
```

Each layer relies on the layer below being strict. The top layer (membership) is allowed to be loose because the layer immediately below it (acquisition) is strict. This is the key insight that makes the whole architecture work without an external coordinator.

The pattern is identical to the well-known [fencing token pattern](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) used by distributed systems like GFS, HDFS, and Spanner to prevent stale leaders from corrupting shared state — we just store the token in the same storage layer that holds the data.

### 4.9 Why this is safe even when membership is wrong

Concretely, work through the worst case. Pods A and B both think they own shard 47 (because A's view has B as dead, and B's view has A as dead).

1. A reads `shard_meta` for shard 47, sees `range_id = 100`. A's CAS: `IF range_id = 100`. Suppose A wins. A's local cached `range_id = 101`.
2. B reads `shard_meta` for shard 47. By the time B reads, A's CAS has committed, so B sees `range_id = 101`. B's CAS: `IF range_id = 101`. B wins. B's local cached `range_id = 102`.
3. A processes a worker request for a job in shard 47. A's mailbox handler attempts a fenced write with `IF range_id = 101`. Storage sees the actual value is 102. A's write fails with `StaleRangeException`.
4. A drops shard 47 from in-memory state and returns the request with a transient error. Worker retries; frontend re-routes; B handles it.
5. **Net result: one wasted RPC + one retry. No corruption.**

The window in which both pods think they own the shard is bounded by however long it takes for one of them to notice the failed CAS or failed write. With a 1-second reconciliation loop and the storage write happening on the very next request, this window is typically sub-second.

### 4.10 Rebalancing on Cluster Size Changes

When a pod joins:
1. The new pod inserts its row into `cluster_nodes`.
2. Within 5 seconds, every other pod's next membership snapshot includes the new pod.
3. Every pod recomputes the consistent hash ring with the new member.
4. ~`1/N` of shards have a new expected owner (the new pod).
5. The previous owners drop those shards on their next reconciliation cycle.
6. The new pod acquires them via CAS on its next reconciliation cycle.

When a pod leaves gracefully:
1. The pod stops heartbeating before exiting.
2. (Optionally: the pod issues a final `DELETE FROM cluster_nodes WHERE node_id = self` for instant propagation.)
3. Other pods notice within 5-10 seconds and rebalance.

When a pod leaves abruptly: same as graceful but with ~16-second worst-case unavailability for affected shards (see §4.7).

There is **no separate "rebalance" operation** and **no admin command for moving shards** — the reconciliation loop handles both.

---

## 5. Server Process Internals

A server process is a single OS process running several cooperating components. They share an address space, so cross-component communication is in-process method calls / channels.

### 5.1 Frontend

Entirely stateless. Holds a cached view of the cluster membership — but the cache is just the **`MembershipSnapshot`** that the local Loop 2 (§4.3) refreshes every 5 seconds. There is no watch, no subscription, no event stream. The frontend reads the same in-process snapshot the reconciliation loop reads.

Responsibilities:
- Accept HTTP / gRPC requests from clients and workers.
- Compute the target shard for each request (`shard_id = hash(job_id) mod N_H`).
- Look up the expected owner via `consistent_hash(shard_id, MembershipSnapshot)`.
- Forward the request to the owning pod via in-cluster gRPC (using the `rpc_address` from the snapshot).
- If the owner is *this* pod, dispatch in-process to the History or Matching engine without serialization.
- Translate engine errors to HTTP responses.

#### Stale snapshot recovery

The frontend's snapshot may be up to 5 seconds behind ground truth. Three failure modes can occur:

1. **Forwarded to a pod that has since lost the shard**: the receiving pod's History engine returns `WrongShardOwnerError`. The frontend forces an immediate `MembershipSnapshot` refresh (out-of-band, not waiting for the next 5s tick), recomputes the owner, and retries against the new owner. Maximum one retry — if the second attempt also fails, the error bubbles to the caller.

2. **Forwarded to a pod that no longer exists**: the gRPC connection fails (DNS or TCP). Same recovery: refresh the snapshot, recompute, retry.

3. **Forwarded to a pod that does own the shard but its `range_id` is stale**: this is the genuine split-brain case. The receiving pod's mailbox handler attempts a fenced write, the storage CAS fails, the pod returns `StaleRangeException`, drops in-memory state, and the frontend retries (now the pod will return `WrongShardOwnerError` instead and route correctly).

In all three cases the **storage fence in §4.8 is the actual safety mechanism**; routing is just an optimization to avoid unnecessary retries.

### 5.2 History Engine

The most complex component. Owns a set of history shards. Each owned shard has its own per-shard state, isolated from the others. Concurrency *between* shards is unconstrained; concurrency *within* a shard is single-threaded (or, if multi-threaded for parallelism, mediated through a shard-local actor / mailbox).

#### Per-shard in-memory state

For each owned shard:

| Structure | Purpose |
|---|---|
| `range_id` | Current fencing token. |
| `acquired_jobs: Map<JobId, AcquiredJob>` | Jobs in ACQUIRED state, with `lease_expires_at`, `worker_id`, last heartbeat. |
| `pending_transfer_tasks: Queue<TransferTask>` | Read-ahead buffer of transfer tasks not yet ack'd by matching. |
| `timer_heap: MinHeap<TimerTask>` | Scheduled / delayed jobs ordered by fire time. |
| `mailbox: Channel<ShardCommand>` | Single point of entry for all writes. Serializes per-shard concurrency. |

The **mailbox** is the critical structural element. Every operation that mutates shard state is enqueued onto the mailbox; a single writer goroutine/coroutine processes it. This eliminates the need for in-memory locking and matches Cassandra's per-partition single-writer requirement perfectly.

#### Per-shard background loops

Each owned shard runs three background tasks:

1. **TransferQueueProcessor**: reads `transfer_tasks` rows from storage in order (clustered range scan, very cheap) and forwards each to the appropriate Matching shard owner via in-cluster gRPC. This is the bridge between durable dispatch state and the in-memory Matching engine. Its loop logic is detailed in §5.2.1 below.

2. **TimerProcessor**: peeks at the timer_heap. Sleeps until the next timer fires. When fired, generates a transfer task (so the job becomes ready to dispatch) and removes the timer.

3. **LeaseExpiryReaper**: periodically scans `acquired_jobs` for entries where `lease_expires_at < now`. For each, transitions the job back to PENDING and inserts a new `transfer_tasks` row, just like a worker had explicitly released it.

All three are subject to the **range_id fence**: every storage write they perform fails if the shard ownership has moved.

#### 5.2.1 TransferQueueProcessor — detailed loop

The TransferQueueProcessor is the most nuanced of the three loops because it is the one component that bridges History (durable) and Matching (in-memory), and it must handle Matching being temporarily unavailable or saturated.

**Constants (configurable per cluster):**

| Name | Default | Purpose |
|---|---|---|
| `LOOP_INTERVAL` | 500ms | How often the loop runs when idle |
| `BATCH_SIZE` | 100 | Max rows to read per loop iteration |
| `REOFFER_INTERVAL` | 5 minutes | How long before a previously-offered but unaccepted row is re-offered |
| `BACKOFF_INITIAL` | 1s | Starting backoff when Matching returns `QueueFull` |
| `BACKOFF_MAX` | 30s | Maximum backoff cap |

**Per-shard in-memory state:**

```
channel_pool: Map<NodeId, ManagedChannel>   -- pooled gRPC channels to peer pods
backoff_map:  Map<(NodeId, JobType), BackoffState>  -- per-destination, per-type pause state
```

**The loop:**

```
loop every LOOP_INTERVAL:
  rows = SELECT * FROM transfer_tasks
         WHERE shard_id = :my_shard_id
           AND visibility_time <= now
           AND (last_offered_at IS NULL OR last_offered_at < now - REOFFER_INTERVAL)
         ORDER BY task_seq
         LIMIT BATCH_SIZE

  if rows.empty:
    continue

  for row in rows:
    # 1. Resolve the target Matching pod
    matching_shard = hash(row.job_type) mod N_M
    target_node    = consistent_hash(matching_shard, MembershipSnapshot)
    target_address = MembershipSnapshot.nodes[target_node].rpc_address

    # 2. Check local backoff state for this destination + job type
    if backoff_map[(target_node, row.job_type)].paused_until > now:
      continue   # skip — target is temporarily saturated

    # 3. Get or create a pooled gRPC channel to the target pod
    channel = channel_pool.get_or_create(target_node, target_address)

    # 4. Send the offer
    result = channel.offerJob(OfferJobRequest {
      job_id           = row.job_id,
      history_shard_id = my_shard_id,
      job_type         = row.job_type,
      input_data       = read_from_jobs_cache_or_storage(row.job_id),
      attempt          = row.attempt,
    })

    # 5. Handle the response
    match result:
      case Accepted:
        # Matching has the offer (either queued or delivered to a parked worker).
        # Mark the row so we don't re-offer for REOFFER_INTERVAL.
        fenced_write:
          UPDATE transfer_tasks SET last_offered_at = now
          WHERE shard_id = :shard_id AND task_seq = row.task_seq
        # Reset backoff for this destination (it's healthy)
        backoff_map[(target_node, row.job_type)].reset()

      case QueueFull { current_depth, max_depth }:
        # Matching's in-memory queue for this job type is saturated.
        # Back off — do NOT update last_offered_at (we still owe this offer).
        backoff_map[(target_node, row.job_type)].record_rejection()
        # Backoff progression: 1s → 2s → 4s → 8s → ... → 30s (capped)
        # Skip remaining rows for this target+type in this loop iteration.

      case RpcError:
        # Target pod unreachable (crashed, network issue).
        # Back off for a short period, then retry.
        backoff_map[(target_node, row.job_type)].record_failure(short_duration = 2s)
        # The MembershipSnapshot will eventually reflect the dead pod;
        # the next loop iteration will resolve to the new owner.
```

**Key properties of this loop:**

- **Idle when there's no work.** If `transfer_tasks` is empty, the loop sleeps for 500ms and checks again. No wasted I/O.
- **Immediate when there IS work.** The mailbox handler for CreateJob / ReleaseJob / LeaseExpiryReaper can send an in-process notification to wake the loop immediately after inserting a row, bypassing the 500ms sleep.
- **Skips rows whose destinations are in backoff.** A saturated Matching pod for one job type doesn't slow down offers for other job types on different pods.
- **Re-offers are rare.** In the happy path, `last_offered_at` is set on first offer, the worker accepts within seconds, and the row is deleted long before `REOFFER_INTERVAL` elapses. Re-offers only fire when something went wrong (Matching crashed, worker died between offer and accept, etc.).
- **Channel pooling avoids per-offer connection overhead.** One persistent HTTP/2 channel per peer pod, reused across all offers.
- **All writes are fenced.** The `UPDATE transfer_tasks SET last_offered_at = now` goes through a fenced batch. If the shard was lost, the write fails and the loop stops.

#### State reconstruction on shard acquire

When a node takes over a shard (whether at startup or after a peer's death), it reconstructs the in-memory state by reading from storage:

```
1. Read shards row, capture range_id (already done during acquire).
2. Read all jobs in this shard with status = ACQUIRED → populate acquired_jobs.
3. Read all unprocessed transfer_tasks for this shard → populate pending_transfer_tasks.
4. Read all timer_tasks for this shard → populate timer_heap.
5. Start the three background loops.
6. Mark shard as serving.
```

The whole sequence is bounded by the size of in-flight state for the shard, not by total job history. Completed/failed jobs are not reloaded. This bounds startup cost.

### 5.3 Matching Engine

Owns matching shards. Each matching shard hosts ready-queues for one or more job types whose hash maps to that shard.

#### Per-shard in-memory state

| Structure | Purpose |
|---|---|
| `range_id` | Fencing token (separate from history shard range_ids). |
| `ready_queues: Map<JobType, ReadyQueue>` | One bounded FIFO of ready job offers per job type. |
| `pollers: Map<JobType, Queue<PollerSession>>` | Workers currently long-polling this matching shard, by job type. |

A `ReadyQueue` is a bounded in-memory FIFO of `JobOffer { job_id, history_shard_id, job_type, attempt }`, plus a dedup index of `Set<JobId>` to prevent the same job from appearing in the queue twice. Each queue has a configurable max size (`MAX_QUEUE_DEPTH`, default 10,000 per job type).

Matching has **no durable state of its own**. If a matching shard owner crashes, the new owner starts with empty queues. **History shards re-feed it** by replaying `transfer_tasks` (which are durable in history-shard storage). This is the essential design: matching is a real-time dispatcher, not a database.

#### OfferJob handler

When a `OfferJob` request arrives from a history shard:

```
handle_offer_job(req):
  queue = ready_queues.get_or_create(req.job_type, max_size = MAX_QUEUE_DEPTH)

  # Fast path: a worker is already parked for this job type
  if pollers[req.job_type].has_parked():
    poller = pollers[req.job_type].pop()
    deliver_to_poller(poller, req)
    return OfferJobAck { accepted: true }

  # Deduplicate: if this job_id is already queued, treat as idempotent
  if queue.contains(req.job_id):
    return OfferJobAck { accepted: true }  # already have it — no-op

  # Backpressure: if the queue is full, NACK
  if queue.size() >= queue.max_size:
    return OfferJobAck {
      accepted: false,
      reason: QUEUE_FULL,
      current_depth: queue.size(),
      max_depth: queue.max_size,
    }

  # Normal path: queue the offer
  queue.append(req)
  return OfferJobAck { accepted: true }
```

Three response types, and History handles each differently (see §5.2.1):

| Response | Meaning | History's reaction |
|---|---|---|
| `accepted: true` | Matching has the offer (either delivered to a worker immediately or queued in memory). | Mark `last_offered_at = now` on the `transfer_tasks` row. Reset backoff for this destination. |
| `accepted: false, reason: QUEUE_FULL` | Matching's in-memory queue for this job type is at capacity. | Do NOT update `last_offered_at`. Record backoff for this `(destination, job_type)`. Retry after backoff expires. |
| RPC error (connection failure, timeout) | Target pod unreachable. | Record short backoff. Re-resolve the destination on next membership refresh. |

Note the ordering in the handler: **parked workers are checked first, before the queue size limit.** This means a Matching pod with a full queue but also parked workers will still deliver offers immediately. The limit only applies when offers must wait in memory. In a healthy system with enough workers, the queue stays near zero and the limit is never hit.

#### PollJob handler (worker long-poll)

When a worker arrives via long-poll:

```
handle_poll_job(req):
  queue = ready_queues.get_or_create(req.job_type)

  # Fast path: work is already queued
  if queue.has_offers():
    offer = queue.pop()
    return PollJobResponse { offer }

  # No work — park the poller
  poller_session = create_poller_session(req.worker_id, req.job_type)
  pollers[req.job_type].append(poller_session)

  # Block until an offer arrives or timeout (e.g., 60s)
  await poller_session.signal or timeout(60s)

  if poller_session.has_offer():
    return PollJobResponse { poller_session.offer }
  else:
    return PollJobResponse { no_work: true }
    # Worker SDK retries immediately with a new long-poll
```

#### Backpressure: how overflow stays on disk, not in RAM

The queue depth limit on Matching creates a natural backpressure loop that keeps the architecture stable under overload:

```
 ┌─────────────┐  OfferJob  ┌──────────────┐  PollJob  ┌──────────┐
 │   History    │ ─────────►│   Matching    │ ────────► │  Worker   │
 │             │            │              │           │          │
 │ transfer_   │  QueueFull │ ready_queue  │           │          │
 │ tasks table │ ◄────NACK──│ (bounded,    │           │          │
 │ (unbounded  │            │  in-memory)  │           │          │
 │  on disk)   │            └──────────────┘           └──────────┘
 └─────────────┘
       ▲
       │ rows accumulate here when Matching is full
       │ (cheap disk space, not expensive RAM)
```

Under sustained overload:

1. History inserts `transfer_tasks` rows at the rate of incoming job creation.
2. TransferQueueProcessor forwards offers to Matching. Matching accepts them until its queue hits `MAX_QUEUE_DEPTH`.
3. The next offer is rejected with `QueueFull`. History backs off.
4. `transfer_tasks` rows accumulate in storage — they're just rows on disk, cheap and durable.
5. Workers drain Matching's queue at their natural rate. As the queue drops below the limit, History's backoff expires, and it resumes offering.
6. Eventually, the overflow in `transfer_tasks` drains and the system returns to steady state.

The architecture converts **in-memory pressure on Matching** into **disk-space lag in History's `transfer_tasks`**. Memory is expensive and finite; disk is cheap and practically unbounded. No pod OOMs, no offers are lost, dispatch just gets slower until workers catch up.

#### What Matching does NOT do

- It does **not** write to storage. Zero durable operations.
- It does **not** track which worker holds which job. That's History's `acquired_jobs` map.
- It does **not** verify worker identity. Any worker claiming to handle a job type is trusted to long-poll. Authorization is enforced at the Frontend or History layer.
- It does **not** retry failed dispatches. If an offer was delivered to a worker and the worker crashes before calling `AcceptJob`, History's `transfer_tasks` row still exists and will be re-offered (see §5.2.1, `REOFFER_INTERVAL`). Matching's role is fire-and-forget within a single offer.

### 5.4 Sweepers

Two cluster-level periodic sweepers. They need to run on **at least one** pod but should not run on all pods simultaneously (to avoid duplicated work). We elect a sweeper-runner using the same consistent-hash trick as shard ownership: each sweeper has a synthetic "sweeper key" (a fixed string like `"sweeper:dead-workers"`), and the pod that the consistent-hash function assigns to that key runs the sweeper. As cluster membership changes, the sweeper migrates naturally — same primitive, different inputs.

- **DeadWorkerSweeper** (assigned to `consistent_hash("sweeper:dead-workers")`): scans `workers` table for entries with stale heartbeats. Marks them condemned. After grace period, deletes them. Workers' acquired jobs are released by the history-shard-local LeaseExpiryReaper (which runs on the shard owner, not on this sweeper). The two are independent.
- **CompletedJobGCSweeper** (assigned to `consistent_hash("sweeper:gc")`): optional. Truncates very old completed jobs based on retention policy. Out of scope for this doc.
- **MembershipReaper** (Postgres only, assigned to `consistent_hash("sweeper:membership-reaper")`): runs the `DELETE FROM cluster_nodes WHERE last_heartbeat_at < now() - 60s` from §4.3 Loop 4. Cassandra doesn't need this because TTL handles it.

Even if two pods briefly disagree about who runs a sweeper (during membership convergence), the worst case is the sweeper running twice, which is harmless because all sweeper operations are idempotent (delete-if-stale, mark-if-not-marked).

### 5.5 Worker Connection Manager

A small per-server component holding open the long-poll gRPC streams to workers. When a poller's response is ready (offer matched or timeout), it writes to the stream. On stream close, it purges the poller from any matching shard's poller map.

---

## 6. Storage Layer Contract

LogPlay supports two storage backends as first-class targets: **Postgres** and **Cassandra/Scylla**. The same operations work on both, exposed via a single internal interface, with backend-specific implementations behind it. This section defines the contract.

### 6.1 The Operations the Storage Layer Must Provide

| # | Operation | Atomicity Required | Used by |
|---|---|---|---|
| 1 | **Membership heartbeat** — upsert one row in `cluster_nodes` | Single-row write | §4.3 Loop 1 |
| 2 | **Membership snapshot** — read all rows in `cluster_nodes` with fresh heartbeat | Single-partition / single-table read, eventually consistent OK | §4.3 Loop 2 |
| 3 | **Shard acquire CAS** — bump `range_id` on one `shard_meta` row, conditional on observed value | Single-row linearizable CAS | §4.5 |
| 4 | **Fenced atomic write** — write multiple rows for one shard, conditional on the shard's `range_id` | Atomic across the rows; rejected if `range_id` doesn't match | §8 (every state transition) |
| 5 | **Shard state load** — read all `jobs`, `transfer_tasks`, `timer_tasks` for one shard | Single-shard range scan, must observe the most recent `range_id` bump | §5.2 |
| 6 | **Per-shard range scan** — clustered scan within one shard ordered by clustering column | Single-shard range scan | per-shard background loops |
| 7 | **Membership reaper** (Postgres only) — delete `cluster_nodes` rows with stale heartbeats | Single-table delete, idempotent | §4.3 Loop 4 |

Every operation is either single-row, single-shard, or single-table. **Nothing in this contract requires cross-shard transactions, foreign keys, JOINs, unique constraints other than primary keys, or any kind of distributed consensus.** That's the entire point of "dumb storage."

### 6.2 Logical Schema

Every shard-data row carries `shard_id` as the leading column of its primary key. On Cassandra/Scylla this becomes the partition key (giving per-shard locality and single-partition atomicity). On Postgres it becomes the leading index column (giving locality of reference but no special atomicity — Postgres transactions cover everything).

```
shard_meta (
  shard_id      INT PRIMARY KEY,
  range_id      BIGINT NOT NULL,         -- monotonic fencing token
  owner_hint    TEXT,                    -- node_id of last known owner (informational)
  acquired_at   TIMESTAMP,
  last_write_at TIMESTAMP                -- updated by every fenced write
)

jobs (
  shard_id                  INT,
  id                        TEXT,        -- Job.id
  name                      TEXT,        -- Job.name
  type                      TEXT,        -- Job.type (job type label, e.g. "send_email")
  status                    TEXT,        -- PENDING|ACQUIRED|FINISHED|FAILED|ABORTED
  retries                   INT,
  max_retries               INT,
  idempotency_key           TEXT,
  acquired_by_worker_id     TEXT,        -- nullable
  last_acquired_at          TIMESTAMP,   -- nullable
  lease_expires_at          TIMESTAMP,   -- nullable; new in this design
  input_data                BYTEA,
  output_data               BYTEA,       -- nullable
  last_checkpoint_id        TEXT,        -- denormalized (see §8.4)
  last_checkpoint_order_key BIGINT,
  created_at                TIMESTAMP,
  updated_at                TIMESTAMP,
  PRIMARY KEY (shard_id, id)
)

job_events (
  shard_id      INT,
  job_id        TEXT,        -- references jobs.id
  event_seq     BIGINT,
  event_type    TEXT,        -- CREATED|ACQUIRED|RELEASED|COMPLETED|ERROR_REPORTED|FAILED|ABORTED
  actor_type    TEXT,        -- WORKER|SYSTEM
  actor_id      TEXT,
  event_message TEXT,
  event_detail  TEXT,
  created_at    TIMESTAMP,
  PRIMARY KEY (shard_id, job_id, event_seq)
)

checkpoints (
  shard_id               INT,
  job_id                 TEXT,        -- references jobs.id
  order_key              BIGINT,
  id                     TEXT,        -- Checkpoint.id
  previous_checkpoint_id TEXT,
  name                   TEXT,
  data                   BYTEA,
  created_at             TIMESTAMP,
  PRIMARY KEY (shard_id, job_id, order_key)
)

transfer_tasks (
  shard_id        INT,
  task_seq        BIGINT,
  job_id          TEXT,        -- references jobs.id
  job_type        TEXT,        -- denormalized copy of jobs.type, for dispatch routing
  visibility_time TIMESTAMP,
  attempt         INT,
  last_offered_at TIMESTAMP,   -- nullable; set when successfully forwarded to Matching
  PRIMARY KEY (shard_id, task_seq)
)

timer_tasks (
  shard_id  INT,
  fire_at   TIMESTAMP,
  task_seq  BIGINT,
  job_id    TEXT,        -- references jobs.id
  job_type  TEXT,        -- denormalized copy of jobs.type
  PRIMARY KEY (shard_id, fire_at, task_seq)
)

cluster_nodes (
  partition_id      INT,                 -- always 0
  role              TEXT,                -- 'logplay'
  node_id           UUID,
  rpc_address       TEXT,
  session_start_at  TIMESTAMP,
  last_heartbeat_at TIMESTAMP,
  PRIMARY KEY (partition_id, role, node_id)
)
```

The schema is **identical** on both backends, modulo the type names. Migrations are written once with backend-specific type substitutions (`BYTEA` ↔ `blob`, `TIMESTAMP` ↔ `timestamp`, `UUID` ↔ `uuid`).

### 6.3 Backend Mapping — Postgres

**Op 3 — Shard acquire CAS** (single-row conditional update):

```sql
UPDATE shard_meta
   SET range_id    = :new_range_id,
       owner_hint  = :node_id,
       acquired_at = now()
 WHERE shard_id = :shard_id
   AND range_id = :observed_range_id
RETURNING range_id;
```

If `RETURNING` yields a row, the acquisition succeeded. Otherwise contention.

**Op 4 — Fenced atomic write** (transaction with conditional fence):

```sql
BEGIN;

  -- The fence: succeed only if range_id is still what we expected
  UPDATE shard_meta
     SET last_write_at = now()
   WHERE shard_id = :shard_id
     AND range_id = :expected_range_id;
  -- if affected rows = 0: ROLLBACK and raise StaleRangeException

  -- The data writes: any combination of inserts/updates within this shard
  UPDATE jobs
     SET status = 'FINISHED', output_data = :out, updated_at = now()
   WHERE shard_id = :shard_id AND id = :job_id;

  INSERT INTO job_events (shard_id, job_id, event_seq, event_type, actor_type, actor_id, created_at)
       VALUES (:shard_id, :job_id, :seq, 'COMPLETED', 'WORKER', :worker_id, now());

COMMIT;
```

Postgres transactions give you cross-row atomicity for free. The conditional update on `shard_meta` is the fence; if it touches zero rows the transaction rolls back and the shard owner knows it lost ownership.

**Op 1 — Heartbeat** (upsert with `ON CONFLICT`):

```sql
INSERT INTO cluster_nodes (partition_id, role, node_id, rpc_address, session_start_at, last_heartbeat_at)
VALUES (0, 'logplay', :node_id, :addr, :session_start, now())
ON CONFLICT (partition_id, role, node_id) DO UPDATE
   SET last_heartbeat_at = EXCLUDED.last_heartbeat_at,
       rpc_address       = EXCLUDED.rpc_address;
```

**Op 2 — Membership snapshot** (filtered scan):

```sql
SELECT node_id, rpc_address, last_heartbeat_at
  FROM cluster_nodes
 WHERE last_heartbeat_at > now() - interval '10 seconds';
```

**Op 7 — Membership reaper**:

```sql
DELETE FROM cluster_nodes
 WHERE last_heartbeat_at < now() - interval '60 seconds';
```

### 6.4 Backend Mapping — Cassandra / Scylla

**Op 3 — Shard acquire CAS** (LWT with `IF`):

```cql
UPDATE shard_meta
   SET range_id    = :new_range_id,
       owner_hint  = :node_id,
       acquired_at = toTimestamp(now())
 WHERE shard_id = :shard_id
    IF range_id = :observed_range_id;
```

This is Paxos-backed and slow (~4 round trips) but bulletproof. Returns `[applied]=true` on success or `[applied]=false` with the actual current row on contention.

**Op 4 — Fenced atomic write** (single-partition logged BATCH with `IF`):

```cql
BEGIN BATCH

  -- The fence: conditional update on shard_meta, in the same partition (shard_id = :shard_id)
  UPDATE shard_meta
     SET last_write_at = toTimestamp(now())
   WHERE shard_id = :shard_id
      IF range_id = :expected_range_id;

  -- Data writes: all in the same shard_id partition
  UPDATE jobs
     SET status = 'FINISHED', output_data = :out, updated_at = toTimestamp(now())
   WHERE shard_id = :shard_id AND id = :job_id;

  INSERT INTO job_events (shard_id, job_id, event_seq, event_type, actor_type, actor_id, created_at)
       VALUES (:shard_id, :job_id, :seq, 'COMPLETED', 'WORKER', :worker_id, toTimestamp(now()));

APPLY BATCH;
```

Cassandra single-partition logged BATCHes with an `IF` clause are atomic and fenced via Paxos. The whole batch either applies or doesn't; the `IF` failure rolls everything back. **Critically: every table touched by the BATCH must be partitioned by `shard_id`** so they all live in the same partition. That's why the schema in §6.2 always leads with `shard_id`.

**Op 1 — Heartbeat** (insert with TTL):

```cql
INSERT INTO cluster_nodes (partition_id, role, node_id, rpc_address, session_start_at, last_heartbeat_at)
VALUES (0, 'logplay', :node_id, :addr, :session_start, toTimestamp(now()))
USING TTL 30;
```

The TTL refreshes every heartbeat. Dead pods' rows expire automatically — **no reaper needed**, no tombstone backlog (TTL expiry is part of normal compaction).

**Op 2 — Membership snapshot** (single-partition scan):

```cql
SELECT node_id, rpc_address, last_heartbeat_at
  FROM cluster_nodes
 WHERE partition_id = 0
   AND role = 'logplay';
```

Single-partition scan, very efficient. Defensive client-side filter on `last_heartbeat_at > now - 10s` handles clock skew.

**Op 7 — Membership reaper**: not needed. TTL handles it.

#### Cassandra-specific consistency tuning

Reads and writes that participate in fencing must use sufficiently strong consistency:

- **Membership reads**: `LOCAL_QUORUM` is sufficient. Stale reads here cost convergence latency, not correctness.
- **`shard_meta` LWT writes** (Op 3): `LOCAL_SERIAL` for the serial consistency, `LOCAL_QUORUM` for the commit consistency. This is the Cassandra default for LWT and is correct.
- **Fenced data writes** (Op 4): single-partition LWT BATCH at `LOCAL_SERIAL` / `LOCAL_QUORUM`.
- **Shard state load on takeover** (Op 5): **`LOCAL_SERIAL` reads** to ensure the new owner sees all writes the previous owner committed via LWT. This is the "read-your-own-writes after a fenced takeover" property and is the documented Cassandra pattern.
- **Background range scans** (Op 6): `LOCAL_QUORUM` is sufficient. Eventual consistency on these is fine because the writer is the same node doing the scan.

Postgres has no consistency knobs to tune; it's strongly consistent by default for everything.

### 6.5 The Storage Adapter Interface

The whole backend split lives behind one Kotlin interface that the server core depends on. The server has no idea which backend is in use.

```kotlin
interface ShardStorage {
    // Op 3
    suspend fun acquireShard(
        shardId: Int,
        expectedRangeId: Long,
        newRangeId: Long,
        nodeId: UUID,
    ): ShardAcquireResult     // Acquired(currentRow) | Lost(actualRow)

    // Op 4 — the hot path
    suspend fun applyShardWrites(
        shardId: Int,
        expectedRangeId: Long,
        writes: ShardWriteBatch,
    ): WriteResult            // Applied | StaleRange | StorageError

    // Op 5
    suspend fun loadShardState(shardId: Int): ShardState

    // Op 6
    suspend fun rangeScanTransferTasks(shardId: Int, fromSeq: Long, limit: Int): List<TransferTask>
    suspend fun rangeScanTimers(shardId: Int, beforeTime: Instant, limit: Int): List<TimerTask>
}

interface MembershipStorage {
    // Op 1
    suspend fun heartbeat(nodeId: UUID, address: String, role: String, sessionStart: Instant)

    // Op 2
    suspend fun listLiveNodes(role: String, freshnessWindow: Duration): List<NodeInfo>

    // Op 7 — Postgres-only; no-op on Cassandra
    suspend fun reapDeadNodes(olderThan: Instant)
}

data class ShardWriteBatch(
    val jobUpserts: List<JobRow>,
    val eventInserts: List<JobEventRow>,
    val checkpointInserts: List<CheckpointRow>,
    val transferTaskInserts: List<TransferTaskRow>,
    val transferTaskDeletes: List<Long>,         // task_seq values
    val timerTaskInserts: List<TimerTaskRow>,
    val timerTaskDeletes: List<TimerTaskKey>,
)
```

Two implementations live alongside today's `H2JobGateway` / `PostgresJobGateway`:

- `PostgresShardStorage` + `PostgresMembershipStorage` — uses Vert.x SQL Client transactions and conditional updates.
- `CassandraShardStorage` + `CassandraMembershipStorage` — uses the DataStax driver's logged BATCH and LWT support.

Each is a few hundred lines. Everything above this interface — the mailbox actor, the consistent hash ring, the reconciler, the frontend, the matching engine, the per-shard background loops — is identical across backends.

### 6.6 What the Storage Layer is NOT Asked to Do

This is the backstop against architectural drift. If anything in the design ever needs one of these, it's a bug.

- It is **not** asked to enforce uniqueness on idempotency keys (the deterministic job_id derivation in §3 handles that).
- It is **not** asked to enforce foreign keys (the shard owner is the only writer; consistency is application-managed within the shard).
- It is **not** asked to provide a queue. `transfer_tasks` is a simple ordered log; reading from it is a clustered range scan owned by the shard owner, never a "find me work across the cluster" query.
- It is **not** asked to do JOINs.
- It is **not** asked to do any cross-shard transaction.
- It is **not** asked to coordinate cluster membership (membership is just a small table that pods read/write independently).
- It is **not** asked to do leader election (the consistent hash function in §4.4 elects deterministically from the membership snapshot).

---

## 7. Worker Lifecycle

### 7.1 Registration

A worker process starts up and connects to any frontend.

```
Worker → Frontend: RegisterWorker {
  worker_id, job_types: ["send_email", "process_image"],
  heartbeat_timeout_ms, session_timeout_ms
}

Frontend → Worker: RegisterWorker.Response {
  accepted: true
}
```

Worker registration is **per-cluster**, not per-shard. The worker exists in a `workers` table (the same table the existing LogPlay `WorkerGateway` uses today). Registration is a simple insert; `worker_id` collisions return an error.

There is no immediate matching-shard assignment. The worker is just known to exist.

### 7.2 Polling for Work

The worker now begins long-polling for each job type it supports. **One long-poll stream per job type**, in parallel.

```
Worker → Frontend: PollJob { worker_id, job_type, max_concurrent_slots }

Frontend computes matching_shard_id = hash(job_type) mod N_M
Frontend looks up owner of that matching shard.
Frontend forwards (or in-process dispatches) to that node's Matching engine.

Matching parks the request in the poller queue for this job type.

[time passes, possibly seconds, possibly minutes]

A JobOffer arrives from a history shard for this job type.
Matching pops the parked poller and writes to its long-poll stream:

Matching → Worker: PollJob.Response {
  job_id, history_shard_id, job_type, input_data,
  attempt, lease_duration_ms, deadline
}
```

The worker now has a `JobOffer`. **The job is not yet ACQUIRED in storage** — that transition happens when the worker explicitly accepts the offer (see §8.2). The offer is just "here's work you can claim if you want it."

> **Important:** the transition to ACQUIRED happens **inside the history shard owner**, not in matching. Matching is purely a router — it hands offers to workers but never writes to storage. The actual transition from `PENDING` to `ACQUIRED` happens when the history shard receives the worker's `AcceptJob` RPC. (See [§8.2](#82-dispatch-and-acquisition-flow) for the precise sequence.)

### 7.3 Heartbeat

Once executing, the worker periodically sends heartbeats specific to the job:

```
Worker → Frontend: Heartbeat {
  worker_id, job_id, history_shard_id, range_token
}

Frontend → History Shard Owner: Heartbeat
Owner extends lease_expires_at in storage and in-memory state.
Owner returns the new lease deadline.
```

If a heartbeat fails (network blip, server moved), the worker retries. If retries exhaust before the lease expires, the worker gives up on the job and the history shard's LeaseExpiryReaper will reclaim it eventually.

The **range_token** is an opaque string the worker received in the original `PollJob.Response`, encoding the history shard's range_id at the time of dispatch. The worker echoes it on every subsequent request. If the history shard owner has changed (range_id incremented), the heartbeat is rejected with `StaleLeaseError` and the worker knows it must abandon the job — its lease no longer exists.

### 7.4 Worker Heartbeat (separate from job heartbeat)

The worker also sends a *worker-level* heartbeat to the cluster, used for global liveness tracking:

```
Worker → Frontend: WorkerHeartbeat { worker_id }

Frontend writes to `workers` table (or a dedicated heartbeat KV).
```

This heartbeat is what feeds the DeadWorkerSweeper. It's separate from the per-job heartbeat because:
- A worker might be holding zero jobs but is alive and ready.
- A worker might be holding many jobs across many history shards; per-job heartbeats are independent and can fail/succeed independently.

### 7.5 Deregistration

```
Worker → Frontend: DeregisterWorker { worker_id }

Frontend → all history shards holding jobs for this worker: ReleaseJob

(In practice: the frontend writes a "worker condemned" record;
sweepers and history shards observe it and release their jobs.)
```

Graceful deregistration is best-effort. The hard guarantee comes from the job lease mechanism: even if deregistration fails, jobs will be released when their leases expire.

---

## 8. Job Lifecycle — End-to-End Flows

This section walks through every state transition in detail. The job state machine is:

```
                  ┌───────┐
                  │ START │
                  └───┬───┘
                      │ create
                      ▼
                ┌───────────┐    abort     ┌─────────┐
        ┌─────► │  PENDING  │ ───────────► │ ABORTED │
        │       └─────┬─────┘              └─────────┘
        │             │ dispatch                ▲
        │             ▼                         │ abort
        │       ┌───────────┐                   │
        │       │ ACQUIRED  │ ──────────────────┤
        │       └─────┬─────┘                   │
        │             │                         │
        │      ┌──────┼──────┐                  │
        │      │      │      │                  │
        │ release   complete  error             │
        │      │      │      │                  │
        │      │      ▼      ▼                  │
        │      │ ┌────────┐ ┌──────┐            │
        └──────┘ │FINISHED│ │FAILED│            │
                 └────────┘ └──────┘            │
                                                │
                  (if retries exhausted, or worker dies repeatedly)
```

### 8.1 Job Creation Flow

```
Client                Frontend           History Shard          Storage
  │                       │                Owner (HSO)             │
  │ POST /jobs            │                   │                    │
  │   { idempotency_key,  │                   │                    │
  │     name, type,       │                   │                    │
  │     input_data, ...}  │                   │                    │
  │──────────────────────►│                   │                    │
  │                       │                   │                    │
  │                       │ job_id = UUIDv5(idempotency_key)       │
  │                       │ shard_id = hash(job_id) % N_H          │
  │                       │ owner = membership.lookup(shard_id)    │
  │                       │                   │                    │
  │                       │ rpc.CreateJob ───►│                    │
  │                       │                   │                    │
  │                       │                   │ Enqueue cmd to     │
  │                       │                   │  shard mailbox.    │
  │                       │                   │                    │
  │                       │                   │ === MAILBOX ===    │
  │                       │                   │                    │
  │                       │                   │ Read jobs row for  │
  │                       │                   │  job_id (within    │
  │                       │                   │  this shard).      │
  │                       │                   │──────────────────►│
  │                       │                   │                   │
  │                       │                   │   ◄────────────── │
  │                       │                   │   (not found, or  │
  │                       │                   │    found existing)│
  │                       │                   │                   │
  │                       │                   │ If existing: return│
  │                       │                   │  it. (idempotent).│
  │                       │                   │                   │
  │                       │                   │ Else: build BATCH │
  │                       │                   │   - INSERT jobs   │
  │                       │                   │     (status=PENDING)│
  │                       │                   │   - INSERT job_events│
  │                       │                   │     (CREATED)     │
  │                       │                   │   - INSERT transfer_tasks│
  │                       │                   │     (job_id, type)│
  │                       │                   │   IF range_id =   │
  │                       │                   │     <current>     │
  │                       │                   │──────────────────►│
  │                       │                   │                   │
  │                       │                   │   ◄────────────── │
  │                       │                   │     (committed)   │
  │                       │                   │                   │
  │                       │                   │ Notify Transfer-  │
  │                       │                   │  QueueProcessor   │
  │                       │                   │  (in-process).    │
  │                       │                   │                   │
  │                       │   ◄─── job ───────│                    │
  │   ◄─── 201 Created ───│                   │                    │
```

Key invariants:
- The transfer task is written **in the same batch** as the job state. Atomicity is at the shard level.
- The TransferQueueProcessor wakes up (notification or short polling interval) and forwards the task to matching. If the server crashes between commit and forwarding, the next owner of this shard will read the unprocessed transfer task on rebuild and forward it then. **No loss.**
- Idempotency: a duplicate POST with the same idempotency_key produces the same job_id, lands on the same shard, the shard owner sees the row already exists, and returns it without inserting again.

### 8.2 Dispatch and Acquisition Flow

```
HSO TransferQueue       Matching Engine        Frontend        Worker
  Processor            (target shard)             │              │
        │                     │                   │              │
        │ Read next transfer  │                   │              │
        │  task from storage  │                   │              │
        │ (cheap clustered    │                   │              │
        │  range scan)        │                   │              │
        │                     │                   │              │
        │ rpc.OfferJob ──────►│                   │              │
        │   { job_id,         │                   │              │
        │     history_shard,  │                   │              │
        │     job_type,       │                   │              │
        │     input_data,     │                   │              │
        │     attempt }       │                   │              │
        │                     │                   │              │
        │                     │ Check pollers map │              │
        │                     │  for this job     │              │
        │                     │  type.            │              │
        │                     │                   │              │
        │                     │ Case A: poller    │              │
        │                     │  parked. Pop it.  │              │
        │                     │                   │              │
        │                     │ Hand the offer to │              │
        │                     │  the parked stream│              │
        │                     │ ──── PollJob ────►│              │
        │                     │       Response   │              │
        │                     │                   │              │
        │                     │                   │ ──────────► │
        │                     │                   │ (gRPC reply)│
        │                     │                   │              │
        │   ◄── ACK ──────────│                   │              │
        │                     │                   │              │
        │  (transfer_task NOT │                   │              │
        │   deleted yet — see │                   │              │
        │   below)            │                   │              │
        │                     │                   │              │
        │                     │ Case B: no poller.│              │
        │                     │  Append to ready  │              │
        │                     │  queue. ACK now.  │              │
        │                     │  (Will dispatch   │              │
        │                     │  when a worker    │              │
        │                     │  arrives.)        │              │
```

> **State of `jobs.status` at this point:** still **PENDING**. The transition to ACQUIRED has not happened yet. The offer has been dispatched but the worker has not yet accepted the work.
>
> **Why not transition to ACQUIRED in the offer batch?** Because at the time the history shard wrote the transfer task, it didn't know which worker would receive it. We need the `acquired_by_worker_id` value to record the acquisition.

The actual ACQUIRED transition happens on the next round-trip, and this is also where the `transfer_tasks` row is finally deleted:

```
Worker received the offer (gRPC poll response).

Worker → Frontend: AcceptJob {
  worker_id, job_id, history_shard_id
}

Frontend → HSO (computed from history_shard_id):
   rpc.AcceptJob

HSO mailbox processes:
  Read jobs row.
  If status != PENDING: reject (race — someone else acquired it).
    [This can happen if the LeaseExpiryReaper or an abort beat us.]
  BATCH (fenced by range_id):
    UPDATE jobs SET
      status = 'ACQUIRED',
      acquired_by_worker_id = :worker_id,
      last_acquired_at = now,
      lease_expires_at = now + lease_duration
    INSERT job_events (ACQUIRED, actor_type=WORKER, actor_id=:worker_id)
    DELETE FROM transfer_tasks WHERE shard_id = :shard AND task_seq = :seq
    IF range_id = <current>
  Add to in-memory acquired_jobs map.
  Reply success with the lease deadline.

Frontend → Worker: AcceptJob.Response {
  lease_deadline, range_token
}

Worker begins executing.
```

The `transfer_tasks` DELETE is part of the same fenced batch as the state transition. This has an important property: **the transfer_task row survives as long as the job is still PENDING**. If the worker crashes between offer and accept, the transfer_task is still there and the TransferQueueProcessor will re-offer it on its next loop (once `last_offered_at` is older than the re-offer interval).

The alternative — deleting the transfer_task when Matching ACKs the offer — would be faster but would require a separate "stale offers" sweep to recover from worker crashes between offer and accept. Keeping the transfer_task alive until the actual accept makes the recovery path automatic.

Consequence: a transfer_task may be offered multiple times to different workers during unusual sequences (worker crashes, shard takeover, etc.). The `AcceptJob` handler is the source of truth for who got the job — only the first worker to successfully run the fenced batch wins; subsequent `AcceptJob` attempts hit `status != PENDING` and are rejected.

### 8.3 Heartbeat Flow

```
Worker → Frontend: Heartbeat { worker_id, job_id, range_token }
Frontend → HSO: rpc.Heartbeat

HSO mailbox:
  Look up job in acquired_jobs map.
  If not present: reject (StaleLeaseError) — job no longer ACQUIRED here.
  If range_token != current range_id encoded form: reject.
  BATCH (fenced):
    UPDATE jobs SET lease_expires_at = now + lease_duration
      WHERE shard_id = :shard AND id = :job_id
        AND acquired_by_worker_id = :worker_id
    IF range_id = <current>
  Update in-memory.
  Return new lease_deadline.

Worker schedules next heartbeat at lease_deadline - safety_margin.
```

### 8.4 Checkpoint Save Flow

This is the operation that today carries the `compute(job, lastCheckpoint) -> Checkpoint` lambda. In the stateful-server world, the lambda **moves into the history shard owner directly** — there is no longer a need for the gateway to invite domain code into a transaction, because the history shard *is* the writer and holds the relevant in-memory state.

```
Worker → Frontend: SaveJobCheckpoint {
  worker_id, job_id, previous_checkpoint_id, name, data
}
Frontend → HSO: rpc.SaveJobCheckpoint

HSO mailbox:
  Look up job in acquired_jobs.
  Validate acquired_by_worker_id matches.
  Validate previous_checkpoint_id == job.last_checkpoint_id.
    [Note: last_checkpoint_id is denormalized onto the jobs row,
     so no separate read of the checkpoints table is needed.]
  Compute:
    new_checkpoint = Checkpoint(
      id = uuid(),
      jobId = job_id,
      previousCheckpointId = previous_checkpoint_id,
      name = name,
      data = data,
      createdAt = now,
      orderKey = job.last_checkpoint_order_key + 1
    )
  BATCH (fenced):
    INSERT checkpoints (id, job_id, previous_checkpoint_id, name, data, order_key, created_at)
    UPDATE jobs SET
      last_checkpoint_id = new_checkpoint.id,
      last_checkpoint_order_key = new_checkpoint.orderKey,
      retries = 0,                            -- progress resets retry counter
      lease_expires_at = now + lease_duration,
      updated_at = now
    IF range_id = <current>
  Update in-memory.
  Return new_checkpoint to worker.
```

The key simplification: because the shard owner is the **only** writer for this shard, there's no LWT contention, no concurrent writer to lose to. The `range_id` fence is a defense-in-depth check against ownership transitions, not an arbitration mechanism between competing writers.

Domain note: the lambda pattern from today's code in `SaveJobCheckpointUseCaseImpl` (`compute: (Job, Checkpoint?) -> Checkpoint`) is *trivially* implementable here as a normal function call inside the shard owner's mailbox handler. The "transaction-scope inversion" trick (where the gateway invites domain code into an open transaction) is no longer needed because the shard owner controls both the read and the write directly.

### 8.5 Error Reporting & Retry Flow

```
Worker → Frontend: ReportExecutionError {
  worker_id, job_id, error_message
}
Frontend → HSO: rpc.ReportExecutionError

HSO mailbox:
  Look up job in acquired_jobs.
  Validate acquired_by_worker_id matches.
  new_retries = job.retries + 1
  if max_retries != null && new_retries >= max_retries:
    next_status = 'FAILED'
  else:
    next_status = 'PENDING'

  BATCH (fenced):
    UPDATE jobs SET
      status = next_status,
      retries = new_retries,
      acquired_by_worker_id = NULL,
      lease_expires_at = NULL,
      updated_at = now
    INSERT job_events (ERROR_REPORTED, actor_type=WORKER, actor_id=:worker_id, event_message=:error)
    if next_status == 'FAILED':
      INSERT job_events (FAILED, actor_type=SYSTEM)
    if next_status == 'PENDING':
      INSERT transfer_tasks (job_id, job_type, visibility_time=now, attempt=new_retries)
    IF range_id = <current>

  Remove from in-memory acquired_jobs.
  If next_status == 'PENDING':
    notify TransferQueueProcessor (so it forwards immediately).

  Return success.
```

The retry is implicit in the new transfer task: the next dispatch cycle will offer this job to a worker again. There is no explicit "schedule retry in N seconds" yet — that would use timer_tasks instead of transfer_tasks (insert into timer_tasks with `fire_at = now + backoff`, and the TimerProcessor will eventually convert it to a transfer task).

### 8.6 Job Completion Flow

```
Worker → Frontend: CompleteJob {
  worker_id, job_id, output_data
}
Frontend → HSO: rpc.CompleteJob

HSO mailbox:
  Look up job in acquired_jobs.
  Validate acquired_by_worker_id matches.

  BATCH (fenced):
    UPDATE jobs SET
      status = 'FINISHED',
      acquired_by_worker_id = NULL,
      lease_expires_at = NULL,
      output_data = :output_data,
      updated_at = now
    INSERT job_events (COMPLETED, actor_type=WORKER, actor_id=:worker_id)
    IF range_id = <current>

  Remove from in-memory acquired_jobs.
  Return success.

Worker on success: free its slot, long-poll matching for the next job.
```

### 8.7 Abort Flow

Abort is initiated by clients (admin operation), not by workers:

```
Client → Frontend: AbortJob { job_id }
Frontend → HSO (computed from job_id): rpc.AbortJob

HSO mailbox:
  Read jobs row.
  If status not in ['PENDING', 'ACQUIRED']: reject (not abortable).
  was_acquired = (status == 'ACQUIRED')

  BATCH (fenced):
    UPDATE jobs SET
      status = 'ABORTED',
      acquired_by_worker_id = NULL,
      lease_expires_at = NULL,
      updated_at = now
    INSERT job_events (ABORTED, actor_type=SYSTEM)
    DELETE FROM transfer_tasks WHERE shard_id = :shard AND job_id = :job_id
    IF range_id = <current>

  Remove from in-memory acquired_jobs (if was_acquired).
  Return success.
```

Note: aborting a currently-executing job does **not** notify the worker. The worker continues running its job handler until it tries to heartbeat or save a checkpoint, at which point its request is rejected with "job not in ACQUIRED state" and it knows to bail. There is no synchronous cancellation channel to the worker. Adding one (e.g., a server-pushed cancellation signal over the long-poll stream) is a v2 feature.

### 8.8 Delayed/Scheduled Job Flow

If a job is created with a `not_before` time, or if a retry uses backoff:

```
HSO mailbox (during create / error / explicit schedule):
  BATCH (fenced):
    INSERT/UPDATE jobs SET status = 'PENDING', not_before = T
    INSERT timer_tasks (fire_at=T, job_id, job_type)
    IF range_id = <current>

  Insert into in-memory timer_heap.

TimerProcessor loop:
  Sleep until next timer fires.
  When fired:
    BATCH (fenced):
      INSERT transfer_tasks (job_id, job_type, visibility_time=now)
      DELETE timer_tasks WHERE (shard_id, fire_at, task_seq) = ...
      IF range_id = <current>
    Notify TransferQueueProcessor.
```

The timer is a per-shard concern. If the shard moves, the new owner reloads timer_tasks from storage and rebuilds its timer_heap. No timers lost.

---

## 9. Failure Scenarios & Recovery

### 9.1 Server crash — graceful

The server receives SIGTERM. In order:

1. **Stop accepting new frontend requests** (close listening sockets after current requests drain or after a grace deadline).
2. **For each owned shard**: drain the mailbox (let queued commands complete), stop background loops, drop in-memory state.
3. **Optionally**: issue `DELETE FROM cluster_nodes WHERE node_id = self` to immediately propagate the departure (instead of waiting for the heartbeat to age out).
4. **Close in-cluster RPC connections.**
5. Process exits.

Within ~5 seconds (the membership refresh interval), every other pod's `MembershipSnapshot` reflects the departure. The consistent hash ring is recomputed; ~`1/N` of shards now have a new expected owner. Each new owner attempts acquisition via §4.5 on its next reconciliation cycle (within 1 second of the snapshot refresh). The whole transition completes in **~6-7 seconds for graceful shutdown**.

If step 3 is performed, propagation drops to whatever the next refresh cycle is on the other pods.

**Job impact:** any in-flight request for a job whose shard is mid-transition sees a transient `WrongShardOwnerError` or `StaleRangeException`. The frontend re-resolves and retries against the new owner. End-user latency increases by a retry round-trip during the transition window.

### 9.2 Server crash — abrupt

Power loss, kernel panic, OOM kill. The pod stops heartbeating immediately and never gets a chance to release shards or delete its `cluster_nodes` row.

1. The pod's row in `cluster_nodes` stops being refreshed (T0).
2. **Cassandra**: TTL (30s) on the row expires. After expiry, queries no longer return it. The defensive freshness filter (`last_heartbeat_at > now - 10s`) catches it sooner — within ~10s the row is "dead" from a query perspective even though physically still present.
   **Postgres**: queries already filter on `last_heartbeat_at > now - 10s`, so the row drops out within 10s. Physical deletion happens later via the membership reaper (§4.3 Loop 4).
3. Within the next 5 seconds (membership refresh), every other pod's `MembershipSnapshot` no longer includes the dead pod.
4. Within the next 1 second (reconciliation), the new expected owners attempt to acquire the orphaned shards.
5. Acquisition CAS completes in ~10ms (Postgres) or ~40ms (Cassandra LWT).

**Worst case shard unavailability: ~16 seconds** (10s freshness window + 5s refresh + 1s reconciliation + CAS). During this window, requests for affected jobs fail; workers' heartbeats fail; the frontend retries with backoff but eventually surfaces errors to clients.

**Edge case: the dead node had ACQUIRED jobs whose `lease_expires_at` in storage is now in the past.** The new owner's state reconstruction (§5.2) reads the jobs row by row and populates its in-memory `acquired_jobs` map with whatever it finds — including the expired lease. Recovery rules:

- **The new owner trusts what's in storage.** If storage says a job is ACQUIRED by worker W with lease until T, then it's still ACQUIRED. The new owner respects it.
- **The LeaseExpiryReaper** (one of the per-shard background loops) sweeps `acquired_jobs` against wall-clock time. If `lease_expires_at < now`, it releases the job back to PENDING. This catches jobs whose workers also died (or whose workers were heartbeating into the dead pod and never reached the new owner before the lease expired).
- **If the original worker is healthy** and just couldn't reach the previous server, its next heartbeat goes to the new owner (via frontend re-resolution). The new owner finds the job in `acquired_jobs`, validates worker_id matches, extends the lease. **No work lost.**

So a shard transition during normal operation is invisible to healthy workers; only workers that were *also* down or partitioned during the window lose their jobs (and those would have lost them anyway via lease expiry).

### 9.3 Server isolated from storage but still reachable by clients

A LogPlay pod can still accept requests from clients and workers but has lost its connection to the storage layer.

1. **Heartbeat loop** (§4.3 Loop 1) fails. The pod's `cluster_nodes` row stops being refreshed.
2. **Membership refresh loop** fails. The pod's `MembershipSnapshot` becomes stale.
3. **Reconciliation loop** still runs but acts on the stale snapshot — it may continue to believe it owns shards it has actually lost.
4. Within ~10s, other pods (which can reach storage) see this pod's row as expired. They recompute the hashring without it. They acquire the orphaned shards via CAS, bumping the `range_id` on each.
5. Meanwhile, the isolated pod processes a request. Its mailbox handler attempts a fenced write. Storage is unreachable → write fails immediately with a connection error → the pod returns an error to the caller.
6. When the isolated pod's storage connection comes back, its first attempted write hits the fence: `range_id` mismatch (because step 4 bumped it). The pod drops in-memory state for that shard and re-enters reconciliation.

**The pod cannot corrupt anything during isolation** because every write attempt either fails (storage unreachable) or is fenced out (storage reachable but `range_id` is stale). The blast radius is bounded to "transient errors returned to in-flight requests" — never silent data loss.

A defensive optimization: if the pod detects more than N consecutive failed writes or membership refreshes, it can preemptively go into "stand-down" mode: drop all in-memory shard state, stop accepting requests, and only resume when it can re-register cleanly. This minimizes the window of returning errors.

### 9.4 Network partition splitting the cluster

A partition splits LogPlay pods into two groups: A, B on one side; C, D on the other. Both sides can reach the storage layer (which is its own dependency, often more highly available than the LogPlay cluster).

- **Both sides see the full membership** (both can read `cluster_nodes`), so both compute the same hashring and converge on the same shard ownership. No split-brain at the membership layer.
- **In-cluster gRPC forwarding fails across the partition.** A request landing on pod A for a shard owned by pod C cannot be forwarded — the gRPC connection fails. Frontend returns an error.
- Clients/workers retry. If they happen to retry to a pod on the side that owns the target shard, the request succeeds. If not, they get errors until they discover a healthy route.
- A reasonable client policy is **load-balance retries across multiple cluster endpoints**, which is what a Kubernetes Service does naturally — successive requests round-robin and eventually land on a pod that can serve them.

If **the storage layer itself partitions** (one side loses quorum or becomes read-only), the side that lost its storage access enters the §9.3 isolation scenario; the other side functions normally (with some shards possibly stuck in transition until the dead side's membership rows expire).

If **the storage layer goes fully down**, see §9.8.

There is no scenario where two pods can both successfully write to the same shard. The fencing token is global to all replicas of the storage layer, and only one writer can have the matching `range_id` at any moment.

### 9.5 Worker crash mid-execution

```
Time T0: Worker accepts job J. lease_expires_at = T0 + 60s.
Time T0+5s: Worker heartbeats. Lease extended to T0+65s.
Time T0+10s: Worker process killed.
Time T0+65s: Lease expires.
Time T0+65s..T0+90s: HSO LeaseExpiryReaper notices.
  BATCH:
    UPDATE jobs SET status=PENDING, retries=retries+1,
      worker_id=NULL, lease_expires_at=NULL, updated_at=now
    INSERT job_events (RELEASED, actor=system)
    INSERT transfer_tasks (job_id, ...)
    IF range_id = <current>
  Remove from in-memory acquired_jobs.

Job is PENDING again, and a new dispatch cycle picks it up.
```

The retries counter increments: from the system's perspective, a worker death looks like a transient failure. The job's `max_retries` will eventually be reached if the same job keeps killing workers, and it transitions to FAILED.

> **Should worker death increment retries?** Open question. Today's code does NOT — only explicit `reportExecutionError` does. In a stateful design, you can choose either policy. If worker deaths shouldn't count, the LeaseExpiryReaper just resets to PENDING without bumping retries. The trade-off: poison-pill jobs that crash workers will retry forever unless retries is bumped.

### 9.6 Worker hangs (alive but not making progress)

The worker is alive enough to send heartbeats but stuck inside the job handler (deadlock, infinite loop, slow external call). There is no way for the server to know the difference between "doing slow legitimate work" and "stuck."

**Mitigation:** the server enforces a per-job maximum execution time (`max_execution_ms`). If `now - last_acquired_at > max_execution_ms`, the LeaseExpiryReaper releases the job regardless of heartbeat freshness. This is opt-in per job type or per job.

Alternative: the SDK requires periodic "I'm still progressing" calls (different from heartbeats — these advance an explicit progress marker). The server treats absence of progress for too long as a stuck worker. Out of scope for v1.

### 9.7 Worker crashes between offer and accept

```
T0:      HSO TransferQueueProcessor offers job J to Matching.
T0+1ms:  Matching delivers the offer to worker W via long-poll.
T0+2ms:  Matching returns OfferJobAck { accepted: true }.
T0+3ms:  HSO sets last_offered_at = now on the transfer_tasks row.
T0+4ms:  Worker W crashes before sending AcceptJob.
```

The job is PENDING in storage. The `transfer_tasks` row **still exists** (it was not deleted — deletion only happens on `AcceptJob`, per §8.2). `last_offered_at` is set to T0+3ms.

**Recovery is automatic via the `REOFFER_INTERVAL` mechanism in §5.2.1:**

1. 5 minutes later (`REOFFER_INTERVAL`), the TransferQueueProcessor's query filter `last_offered_at < now - REOFFER_INTERVAL` matches the row again.
2. The loop re-reads it and sends a fresh `OfferJob` to the Matching pod.
3. If the original Matching pod is still alive, its dedup logic (§5.3) may detect the duplicate `job_id` already in its queue, or may not (if the queue was drained or the pod restarted). Either way, the offer enters the dispatch loop and a worker eventually receives it.
4. The worker calls `AcceptJob`, which succeeds (the job is still PENDING), and the `transfer_tasks` row is deleted in the same fenced batch.

Alternatively, if the **Matching pod** itself crashed between T0+2ms and when a new worker was supposed to pick up the offer, the re-offer naturally lands on the new Matching pod (since the TransferQueueProcessor resolves the target pod fresh every loop iteration from the `MembershipSnapshot`).

**Key property:** the `transfer_tasks` row's existence guarantees the job will eventually be dispatched. No sweeper, no orphan-detection, no manual intervention required. The re-offer mechanism in §5.2.1 handles this case uniformly with all other "Matching lost the offer" scenarios.

The trade-off: a single job can be offered to multiple workers during unusual sequences (re-offer while the old offer is still in Matching's queue). The `AcceptJob` handler on History is the single source of truth — only the first worker whose fenced batch commits wins. The second gets `JobNotPendingError` and quietly goes back to long-polling.

### 9.8 Storage outage (full)

The dumb storage layer is completely down or unreachable from all LogPlay pods.

- Every write in every shard fails. Mailboxes block on storage IO and back up.
- Heartbeat loops fail. **Every pod's `cluster_nodes` row eventually ages out** from every other pod's perspective.
- Membership refresh loops fail. Snapshots become stale but are not invalidated (a pod uses the last successful snapshot it has).
- Reconciliation loops continue but no acquisitions can succeed (storage is unreachable for the CAS).
- Workers' heartbeats fail. Workers retry with backoff. After their lease deadlines, workers give up on jobs.
- New job creation fails at the frontend.

Because LogPlay has no separate coordinator, **a storage outage is a total cluster outage**. There is no degraded mode that lets in-flight work continue. This is the cost of having one infrastructure dependency instead of two: when it fails, the whole system fails.

When storage recovers:
- Heartbeats resume; membership rebuilds within 5-10 seconds.
- Reconciliation acquires shards; the cluster catches up to the new view.
- Mailboxes drain. In-flight commands either commit (if the cached `range_id` is still valid — usually it is, because no one acquired the shard during the outage) or fail with `StaleRangeException` (if another pod did acquire it, in which case the original pod drops it cleanly).
- Workers retry; their leases either still exist in storage (good — pick up where they left off) or have expired (the LeaseExpiryReaper releases them and they get redispatched).

**Recovery time after outage end: ~16-20 seconds** for the cluster to be fully serving again.

**Important:** during the outage, the in-memory state on each server is still consistent with what storage *will* see when it comes back, because the server hasn't acknowledged writes that didn't actually commit. No state divergence; just unavailability.

### 9.9 Storage partial failure (one shard's partition unavailable)

Cassandra/Scylla can lose availability for individual partitions during node failures. In LogPlay terms: one specific shard's partition is unwritable for a while.

- Only the affected shard is degraded. Other shards continue to operate normally.
- The owning pod's mailbox for that shard backs up. Requests for jobs in that shard time out.
- The pod's heartbeat loop is unaffected (it writes to a different partition — `cluster_nodes`, partition_id=0).
- The pod's reconciliation loop is unaffected (it doesn't touch the affected shard's partition unless it's trying to acquire/release).
- When the partition recovers, the mailbox drains.

The shard is **not** transferred to another pod during the outage, because the membership view hasn't changed — the owning pod is still alive and still believes it owns the shard. This is correct: a different pod taking over wouldn't help because the new owner would also be unable to write to the same partition.

This is one of the advantages of Cassandra/Scylla over Postgres: a single-partition failure is contained, whereas a Postgres tablespace failure typically affects all tables.

### 9.10 Storage replica lag (Cassandra-only)

Cassandra at `LOCAL_QUORUM` can briefly serve stale data if a replica is lagging behind its peers.

- A pod takes over a shard. Its CAS bumps `range_id` from 100 to 101 at `LOCAL_QUORUM`.
- The pod immediately reads the shard's data to rebuild in-memory state. The reads go to the same replicas at `LOCAL_QUORUM`.
- Because the CAS used Paxos and the read is `LOCAL_QUORUM`, R + W > N is satisfied and the new owner sees the previous owner's last committed writes. **Correct.**

But for `loadShardState` reads, we strengthen to `LOCAL_SERIAL`:

- `LOCAL_SERIAL` reads go through Paxos and observe any in-flight LWT writes that have been proposed but not yet committed at all replicas.
- This guarantees the new owner sees a fully consistent snapshot at the moment of takeover.
- The cost is one extra round trip per read on the rebuild path, which only happens once per shard acquisition. Negligible.

Postgres has no equivalent concern; reads are strongly consistent by default.

### 9.11 The new shard owner reads stale data after takeover

After a takeover, the new owner reads in-flight state from storage. With Cassandra's eventual consistency, a read might miss writes that the old owner committed just before crashing.

**Mitigation 1**: use `LOCAL_QUORUM` or `QUORUM` reads on all rebuild reads. With `QUORUM` writes from the old owner and `QUORUM` reads from the new owner, R+W > N is satisfied and the new owner sees all of the old owner's committed writes.

**Mitigation 2**: the bump of `range_id` in storage acts as a synchronization point. The new owner does a `QUORUM` write of the new range_id. By Cassandra's semantics, any subsequent `QUORUM` read on the same partition will see this write **and** any writes that linearize before it (i.e., the old owner's committed writes). This is the standard Cassandra "lock" pattern via LWT — and the documented way to fence reads after a leader change.

This guarantees the new owner sees a consistent snapshot at the moment of takeover.

### 9.12 Two servers temporarily both believe they own a shard

This is the canonical "split-brain" case the design must handle. Suppose pod A's `MembershipSnapshot` is briefly stale (its membership refresh hasn't run yet). A's snapshot still includes a now-dead pod, and the consistent hash gives A continued ownership of shard 47. Meanwhile pod B's snapshot has refreshed, the dead pod is gone, and B's hash gives it ownership of shard 47. B successfully runs the §4.5 acquisition CAS and bumps `range_id` from 100 to 101. Both A and B now believe they own shard 47.

- A serves a worker request for a job in shard 47. It reads its in-memory state, computes a result, attempts a fenced data write (Op 4 from §6.1).
- A's write includes the conditional fence `IF range_id = 100` (A's cached value).
- Storage's recorded `range_id` is now 101 (because B bumped it). The fence fails. A's write is rejected with `StaleRangeException`.
- A's mailbox handler catches the exception, drops in-memory state for shard 47 immediately, and returns the request with a transient error.
- The frontend forces an out-of-band membership refresh (§9.13), recomputes ownership (sees B is the owner now), retries to B.
- B accepts the request and processes it correctly.

**Worst case impact:** the worker received a transient error. The frontend automatically retries. The retry is routed (via re-resolution) to B and succeeds. End-to-end latency increases by a retry cycle for a small window of requests during the transition.

**No data corruption.** This is the entire point of the fencing token. Even though membership consistency is loose (the input layer in the §4.8 stack), the storage fence (the bottom layer) is strict, and the strict layer is the one that decides who can write.

### 9.13 Frontend membership snapshot staleness

Frontends look up shard ownership via the in-process `MembershipSnapshot`, which is refreshed every 5 seconds by Loop 2 (§4.3). Between refreshes, the snapshot can be up to 5 seconds out of date.

- Frontend receives a request. Snapshot says shard 17 → pod X.
- Sends gRPC to X. X has already lost shard 17 (its own reconciliation loop dropped it on its more recent snapshot). X returns `WrongShardOwnerError`.
- Frontend triggers an **out-of-band snapshot refresh** (a single immediate `SELECT` from `cluster_nodes`, not waiting for the 5s tick).
- Re-routes to the new owner (pod Y).
- Total latency cost: one extra in-cluster RPC + one storage read.

The out-of-band refresh is rate-limited (e.g., max once per second per pod) so a flood of `WrongShardOwnerError`s during a transition doesn't hammer the storage layer. Even at 10K req/sec hitting one pod during a transition, the storage gets at most 1 extra read per second from that pod.

None of this is observable to clients beyond a single-retry latency bump during the brief transition window.

### 9.14 Postgres-only: Reaper falls behind

The membership reaper (§4.3 Loop 4) deletes `cluster_nodes` rows where `last_heartbeat_at < now() - 60s`. If this loop hasn't run in a while (the elected reaper-runner pod is itself slow or has failed), dead rows accumulate.

- **Correctness impact: zero.** Membership reads filter on `last_heartbeat_at > now - 10s`, so dead rows that the reaper hasn't gotten around to deleting are still ignored.
- **Performance impact: minimal.** The `cluster_nodes` table is tiny; even with hundreds of stale rows it's a few KB. The membership read scans only the index range matching `last_heartbeat_at > now - 10s`.
- **Recovery**: as soon as another pod becomes the elected reaper-runner (the consistent hash function reassigns it because the old runner is now considered dead), it runs the DELETE and cleans up.

This is why the reaper is "best effort" with no urgency requirements.

### 9.15 Cassandra-only: Clock skew larger than freshness window

Cassandra's TTL is computed against the coordinator node's wall clock, and freshness filters are computed against the reading pod's wall clock. If wall clocks across LogPlay pods or Cassandra nodes drift by more than the freshness window (10s), pods can either:

- **See "live" rows that are actually dead** (the writer's clock was ahead): a pod thinks a peer is alive when it isn't. Result: forwarding to a dead peer, getting a connection error, retrying. Self-heals on next refresh once enough wall-clock time passes.
- **See dead rows when they're actually live** (the writer's clock was behind): a pod thinks a live peer is dead. Result: the consistent hash drops them, ownership reshuffles unnecessarily, then reshuffles back when the reader's clock catches up. Annoying but not corrupting.

**Mitigation**: NTP (or chrony) is required across all LogPlay pods and Cassandra nodes. Operational requirement: clock skew < 1 second cluster-wide. With NTP this is easily achievable; without it, the design has issues regardless of which membership scheme it uses.

Postgres is unaffected because all clocks involved are the Postgres server's clock (`now()` on the server side).

---

## 10. Concurrency & Consistency Properties

### Per-job

- **Linearizable.** All writes to a job go through the mailbox of its history shard owner, which is single-threaded per shard. Combined with the range_id fencing, every successful write is observable on subsequent reads. No anomalies.

### Per-shard

- **Linearizable** for the same reason. The shard owner is the single writer; the storage layer enforces fencing.

### Across shards

- **Eventually consistent.** Two clients reading two different jobs (different shards) may see inconsistent timelines (e.g., job A in shard 5 finished after job B in shard 17, but a reader sees B finished before A even though wall-clock says otherwise). For LogPlay's job-runner semantics, this doesn't matter — there's no operation that requires cross-job atomicity.

### Worker observation lag

- A worker that completes job J observes its own write (linearizable from its perspective). Other workers reading J may have a brief lag depending on the storage backend's read consistency.

### Idempotency

- The same idempotency key always produces the same job_id, lands on the same shard, and the shard owner returns the existing job for any duplicate. This is **strict idempotency** for job creation.
- For state transitions (complete, error, abort): the gateway returns success if the transition has already happened (e.g., a duplicate `CompleteJob` for an already-COMPLETED job is a no-op success). This is **commutative** rather than strictly idempotent — but it's safe.

### Lease semantics

- Leases are **server-clock based**. There is no shared clock across servers. A new shard owner uses its own clock for lease checks. Time skew between old and new owners can mean a job whose lease "expired" on the old owner is still considered valid by the new owner (or vice versa).
- **Mitigation**: leases use generous safety margins. NTP is assumed within ~100ms. The lease window is much larger than expected clock skew (60s lease vs. 100ms skew).

---

## 11. Open Questions & Trade-offs

### Q1: Heartbeat / refresh / reconciliation interval tuning

The defaults documented in §4.3 — heartbeat 2s, membership refresh 5s, reconciliation 1s, freshness window 10s — give ~16s worst-case shard unavailability after an abrupt pod death. The intervals are tuneable:

| Profile | Heartbeat | Refresh | Freshness | Worst-case unavailability | Storage chatter (per pod) |
|---|---|---|---|---|---|
| **Relaxed (recommended)** | 2s | 5s | 10s | ~16s | 0.5 writes/s + 0.2 reads/s |
| **Tight** | 1s | 2s | 5s | ~7s | 1 writes/s + 0.5 reads/s |
| **Aggressive** | 500ms | 1s | 3s | ~4s | 2 writes/s + 1 reads/s |

Even at "aggressive" the storage cost is trivial (~3 ops/sec per pod, all on a single small table). The relaxed profile is the recommended default because:

- 16-second recovery is acceptable for LogPlay's expected workloads.
- Looser intervals tolerate larger NTP clock skew, longer GC pauses, and brief network blips without false-positive shard transfers.
- The relaxed window also gives more time for the in-flight worker heartbeats to reach the new owner, smoothing the worker-side experience during transitions.

### Q1b: One-phase vs two-phase dispatch

We chose two-phase (offer → accept) for ease of failure analysis. One-phase (matching writes the ACQUIRED state on history's behalf) is faster and avoids the [§9.7](#97-worker-crashes-between-offer-and-accept) edge case but couples matching to history's write path.

**Lean toward two-phase.** The simpler failure model is worth one extra RPC per dispatch.

### Q2: Should worker death count as a retry?

Today: no. A new design might want to count it (so poison-pill jobs eventually die). There's no right answer; document the policy.

### Q3: Co-located vs separated history and matching engines

Co-located (one process runs both): simpler ops, less network overhead, harder to scale dimensions independently.

Separated (history servers and matching servers as separate processes): more flexible scaling, more moving parts. Each tier can scale independently based on its own bottleneck — useful at very large deployments where the write load on History and the dispatch load on Matching have wildly different profiles.

**Lean toward co-located for v1.** Easy to split later if dispatch becomes a bottleneck.

### Q4: How is `N_H` (history shard count) chosen?

It is fixed at cluster creation. Common choice: `N_H = 4 × max_expected_nodes`. For a max-32-node deployment, `N_H = 128`. For a 1000-node deployment, `N_H = 4096`.

Re-sharding requires migration: read all jobs, recompute shards, rewrite into new shard slots. This is expensive but rare.

### Q5: What if a single job type has more throughput than one matching shard can handle?

One matching shard owns all jobs of one type, by design. If `send_email` saturates a single matching shard, the cluster has a hot shard.

**Mitigation**: the SDK can introduce sub-task-types: `send_email_0`, `send_email_1`, ..., picked by hash of recipient. Each sub-type lands on a different matching shard, splitting load. The application has to opt into this.

### Q6: How are admin operations (list jobs by status, search by name, etc.) handled?

The history shard layout is **not designed for ad-hoc queries**. Searching for "all PENDING jobs across the cluster" requires fan-out reads to every shard. This is fine for periodic operations but bad for interactive admin UI.

**Solution**: a separate **read model** populated asynchronously. Each history shard owner publishes job_events to a downstream system (Elasticsearch, OpenSearch, ClickHouse, Postgres replica). The admin UI queries the read model. The read model is eventually consistent with the canonical state in the dumb storage layer.

This is the **CQRS** pattern. Out of scope for this document, but it is the answer.

### Q7: What is the SDK's surface for the new design?

Clients see no change: `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/abort`. Workers see a new long-poll API (`PollJob` instead of `acquirePendingJobs`). Worker SDKs need to be rewritten around the long-poll model. The application code inside the worker's task handlers need not change.

### Q8: Backpressure

Workers offer a `max_concurrent_slots` count when polling. Matching only delivers up to that count of in-flight tasks per worker. If the worker is overloaded, it stops polling — matching parks the offer for another worker.

History shards can fill their mailbox if matching is slow. There's no built-in backpressure between history and matching today; if matching's mailbox fills, the history shard's TransferQueueProcessor blocks. This is acceptable as long as matching is generally faster than history (which it should be — it's purely in-memory).

### Q9: How does the lambda compute pattern from today's domain layer translate?

It collapses. Today's lambdas exist because the use case can't pull a row out of the gateway and call back into the gateway under the same lock. In the stateful-server model, the **shard owner is the writer** — it can read, compute domain logic, and write, all in the same mailbox handler, no callbacks needed.

A clean restructuring would eliminate `JobGateway` entirely and replace it with:
- A **storage interface** (`ShardStorage`) — dumb CRUD against one shard, range_id-fenced.
- A **shard runtime** (`HistoryShardEngine`) — owns the in-memory state, runs the mailbox loop, calls into pure domain functions for state transitions.

Domain code becomes pure functions: `(currentJob, command) -> (newJob, newEvents, newTransferTasks)`. The shard engine applies the result to in-memory state and persists via the storage interface in one batch. **No more callbacks. No more transactional inversion. The clean architecture is actually cleaner under this model than under the current one.**

---

## Appendix A — Glossary

| Term | Meaning |
|---|---|
| **History Shard** | A unit of partitioning for job state. Owned by exactly one pod at a time. |
| **Matching Shard** | A unit of partitioning for job dispatch. Owns ready queues for one or more job types. |
| **Shard Owner** | The single pod currently holding the latest `range_id` for a shard, recognized by the storage layer. |
| **range_id** | Monotonically increasing fencing token bumped on every ownership transition; stored on the `shard_meta` row. |
| **Mailbox** | Per-shard single-writer command queue inside the owning pod. |
| **Transfer Task** | A durable record indicating a job is ready to dispatch. Stored in the history shard. |
| **Timer Task** | A durable record for a delayed transition (scheduled job, retry backoff). |
| **Lease** | The time-bounded right of a worker to hold a job in ACQUIRED state. |
| **Long-poll** | The dispatch mechanism: workers hold an open RPC stream and receive offers as they arrive. |
| **Frontend** | Stateless API surface that routes requests to the right shard owner. |
| **History Engine** | The component on each pod that owns history shards. |
| **Matching Engine** | The component on each pod that owns matching shards. |
| **`cluster_nodes`** | The membership table in the storage layer. One row per live pod, refreshed by heartbeat. |
| **`shard_meta`** | The shard metadata table in the storage layer. One row per shard, holds the authoritative `range_id`. |
| **MembershipSnapshot** | The in-memory view of live cluster pods that each pod maintains, refreshed every 5s. |
| **Consistent hash ring** | The deterministic function each pod runs over its `MembershipSnapshot` to compute shard ownership. |
| **ShardStorage / MembershipStorage** | The internal interfaces hiding the Postgres-vs-Cassandra differences from the rest of the codebase. |

## Appendix B — Why this is "dumb storage"

A storage backend qualifies for this architecture if it provides the seven operations from §6.1:

1. **Single-row write** (heartbeat upsert).
2. **Filtered table read** (membership snapshot).
3. **Single-row linearizable CAS** (shard acquire fencing).
4. **Atomic write across multiple rows for one shard, conditional on a fence value** (the hot path).
5. **Range scan within one shard** (state load on takeover).
6. **Range scan within one shard ordered by clustering column** (background loops).
7. **Filtered delete** (Postgres reaper; Cassandra uses TTL).

That's it. **No transactions across shards. No locks. No foreign keys. No unique constraints other than primary keys. No JOINs. No leader election. No coordination service.**

The same operations are implemented on each supported backend in §6.3 / §6.4. **Postgres** and **Cassandra/Scylla** are the two first-class targets, but the contract is also satisfied by:

- **MySQL 8** — same as Postgres (transactions + conditional updates).
- **CockroachDB / Spanner** — same as Postgres at the API level; benefits from the per-shard locality automatically.
- **FoundationDB** — directories per shard; per-shard transactions.
- **DynamoDB** — partition key per shard; `TransactWriteItems` for atomic writes; conditional `UpdateItem` for the fence.

The key design property: **the operations are simple enough that any backend with single-row CAS and per-key locality can implement them**. The complex coordination logic (consistent hashing, mailbox actor, fenced writes, lease management) lives in the LogPlay server tier where it can be tested in one place. The backends just store and return rows.

This is the entire payoff for moving the smarts out of storage and into the server: **one set of correctness reasoning, many possible storage implementations**.

---

## 12. Kubernetes Deployment

The architecture has two infrastructure components — the LogPlay server pods and the storage layer — and that's all. There is no etcd, no Zookeeper, no gossip layer, no external coordination cluster. Kubernetes manifests are correspondingly small.

### 12.1 Pod identity: Deployment vs StatefulSet

Because cluster membership lives in the `cluster_nodes` table and is keyed by random UUIDs (not pod names or DNS), **stable pod identities are not required**. We use a `Deployment`, not a `StatefulSet`:

- **Deployment** gives pods ephemeral names (`logplay-7d4f9c8b6-x7k2p`) and ephemeral IPs.
- On startup, each pod generates a fresh UUID for its `node_id`, reads its own pod IP from the Kubernetes downward API, and inserts itself into `cluster_nodes` with that IP as `rpc_address`.
- On restart, the pod gets a new UUID and a new row. The old row ages out (Cassandra TTL or Postgres reaper).
- Scaling up: `kubectl scale deployment logplay --replicas=N`. New pods join the membership table immediately and the cluster rebalances within ~6s.
- Scaling down: pods receive SIGTERM, run their graceful shutdown (§9.1), and exit. Other pods see the change within ~6s.

This is operationally simpler than StatefulSet: no PVCs per pod, no sequential rollouts, no sticky ordinals, no headless service requirement for stable DNS.

### 12.2 Services

Two Kubernetes Services:

```
                      ┌─────────────────────┐
   external traffic ──┤ Service: logplay    │  type: LoadBalancer (or ClusterIP+Ingress)
   (clients,          │     (api: 8080)     │  round-robins across all pods
    workers)          └──────────┬──────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
       ┌──────────┐       ┌──────────┐       ┌──────────┐
       │ logplay  │◄─────►│ logplay  │◄─────►│ logplay  │
       │  -x7k2p  │       │  -j3m9p  │       │  -p2k4n  │
       │          │       │          │       │          │
       │ :8080    │       │ :8080    │       │ :8080    │  external API
       │ :8081    │       │ :8081    │       │ :8081    │  internal gRPC
       └────┬─────┘       └────┬─────┘       └────┬─────┘
            │                  │                  │
            │  direct pod-to-pod gRPC via         │
            │  rpc_address from cluster_nodes     │
            ▼                  ▼                  ▼
                  ┌──────────────────────────────┐
                  │      Storage Layer           │
                  │  (Postgres or Cassandra)     │
                  └──────────────────────────────┘
```

1. **External Service** — `type: LoadBalancer` or `ClusterIP` behind an Ingress. Round-robins client and worker requests across all pods. This is the only endpoint visible from outside the cluster.

2. **No headless service is needed.** Peer-to-peer gRPC forwarding does not go through any Kubernetes service. Each pod publishes its own pod IP into `cluster_nodes.rpc_address`, and other pods open gRPC connections directly to those IPs. Kubernetes pod IPs are routable within the cluster — you don't need DNS, you don't need a Service object, you just connect to the IP.

### 12.3 The forwarding flow

A worker sends a `Heartbeat` for job `abc123`. The request lands on a random pod via the LoadBalancer:

```
1. External Service (LoadBalancer) round-robins → pod logplay-x7k2p

2. logplay-x7k2p Frontend:
   - shard_id = hash("abc123") mod N_H = 47
   - look up consistent_hash(47, MembershipSnapshot) → node_id = <UUID-A>
   - look up rpc_address for <UUID-A> in MembershipSnapshot → "10.0.4.7:8081"
   - get pooled gRPC connection to "10.0.4.7:8081"
   - forward via internal gRPC

3. Pod at 10.0.4.7 (logplay-j3m9p) receives Heartbeat on internal port 8081:
   - dispatch to History Engine
   - mailbox handler for shard 47 processes the heartbeat
   - return result

4. logplay-x7k2p forwards reply to worker
```

The worker has no idea two pods were involved. Total in-cluster latency: well under a millisecond.

### 12.4 Manifest skeleton

```yaml
# Public Service for external traffic
apiVersion: v1
kind: Service
metadata:
  name: logplay
spec:
  type: LoadBalancer    # or ClusterIP if behind Ingress
  selector: { app: logplay }
  ports:
    - name: api
      port: 8080
      targetPort: 8080

---
# Deployment — ephemeral pods, no stable identity required
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logplay
spec:
  replicas: 8
  selector:
    matchLabels: { app: logplay }
  template:
    metadata:
      labels: { app: logplay }
    spec:
      containers:
        - name: server
          image: logplay-server:1.0
          ports:
            - { name: api,      containerPort: 8080 }
            - { name: internal, containerPort: 8081 }
          env:
            # Pod IP for cluster_nodes.rpc_address
            - name: POD_IP
              valueFrom: { fieldRef: { fieldPath: status.podIP } }
            - name: INTERNAL_PORT
              value: "8081"
            # Storage backend selection
            - name: LOGPLAY_STORAGE_BACKEND
              value: "postgres"           # or "cassandra"
            - name: LOGPLAY_STORAGE_URL
              valueFrom: { secretKeyRef: { name: logplay-db, key: url } }
          readinessProbe:
            httpGet: { path: /healthz, port: 8080 }
            periodSeconds: 5
          lifecycle:
            preStop:
              exec:
                command: ["/bin/logplay-graceful-shutdown"]
```

The pod's startup sequence:
1. Read `POD_IP` from the downward API.
2. Generate a fresh `node_id` UUID.
3. Connect to storage. Run schema migrations if needed.
4. Insert self into `cluster_nodes` with `rpc_address = $POD_IP:$INTERNAL_PORT`.
5. Start the heartbeat / refresh / reconciliation loops (§4.3).
6. Mark `/healthz` as ready.
7. Begin accepting requests on port 8080 and internal RPCs on port 8081.

The pod's shutdown sequence (`preStop` + SIGTERM handler):
1. Mark `/healthz` as unready (Kubernetes Service stops sending new traffic).
2. Drain in-flight requests with a deadline.
3. For each owned shard: drain mailbox, stop background loops, drop in-memory state.
4. Issue `DELETE FROM cluster_nodes WHERE node_id = self` (best-effort, for fast propagation).
5. Close storage connection.
6. Process exits.

### 12.5 What's NOT in the manifest

No StatefulSet. No headless Service. No PVCs. No init containers for cluster bootstrap. No sidecars. No external coordinator. No CRDs. No operator. **Just one Service and one Deployment.**

Adding LogPlay to an existing Kubernetes cluster requires applying ~50 lines of YAML and pointing it at a database. Scaling is a single `kubectl scale` command. The whole operational story is "it's a stateless web service that happens to coordinate amongst its replicas through a database table."

That's the payoff for the design.