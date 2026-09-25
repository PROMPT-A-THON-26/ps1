# Vault

> Fault-Tolerant Distributed Object Storage System

**Repository:** `PROMPT-A-THON-26/ps1`  
**Status:** Part B control plane implemented and integrated  
**Problem Domain:** Distributed systems / object storage / fault tolerance

---

## 1. Overview

Vault is a fault-tolerant distributed object storage system designed to store, replicate, retrieve, verify, repair, and rebalance large volumes of data across multiple storage nodes.

The central idea is simple:

> A file should remain available and correct even when individual storage machines fail, become temporarily unreachable, contain corrupted data, or disagree about the state of a replica.

Vault is being designed as a real distributed system rather than a simple file-upload application. The system will deliberately simulate failures and demonstrate automatic recovery.

---

## 2. Problem Statement

Vault must be capable of:

- Storing large objects across unreliable storage nodes.
- Replicating objects according to configurable durability policies.
- Serving concurrent reads and writes.
- Handling individual node failures.
- Handling partial network partitions.
- Detecting data corruption.
- Detecting stale or inconsistent replicas.
- Performing background integrity verification.
- Automatically repairing missing, unavailable, stale, or corrupted replicas.
- Rebalancing data when storage nodes are added or removed.
- Maintaining consistent metadata.
- Providing predictable availability according to the configured durability policy.
- Minimizing recovery time and unnecessary storage/network overhead.

---

## 3. Project Goal

The final system should be able to demonstrate a complete lifecycle such as:

```text
Client uploads object
        |
        v
Vault selects storage nodes
        |
        v
Object is replicated
        |
        v
Replicas are verified
        |
        v
Metadata is committed
        |
        v
Client can retrieve the object
        |
        v
A storage node fails
        |
        v
Vault detects the failure
        |
        v
Affected replicas are identified
        |
        v
A healthy replica is used as the source
        |
        v
A replacement replica is created
        |
        v
Checksum is verified
        |
        v
Metadata is updated
        |
        v
Configured durability is restored
```

The same architecture will handle corruption, temporary node unreachability, and rebalancing.

---

# 4. High-Level Architecture

```text
                           +----------------------+
                           |        CLIENT        |
                           | CLI / Web / API      |
                           +----------+-----------+
                                      |
                                HTTP / REST
                                      |
                                      v
                    +-----------------------------+
                    |        VAULT GATEWAY        |
                    |                             |
                    | Upload / Download           |
                    | Delete / List               |
                    | Metadata / Version APIs     |
                    | Request validation           |
                    +--------------+--------------+
                                   |
                 +-----------------+-----------------+
                 |                 |                 |
                 v                 v                 v
          +-------------+   +-------------+   +-------------+
          |  Metadata   |   |  Placement  |   |   Policy    |
          |   Manager   |   |   Manager   |   |   Manager   |
          +------+------+   +------+------+   +-------------+
                 |                 |
                 +--------+--------+
                          |
                          v
                 +-------------------+
                 |     PostgreSQL    |
                 |     Metadata      |
                 +---------+---------+
                           |
                           v
                  +-----------------+
                  | Storage Control |
                  |      Plane      |
                  +--------+--------+
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
        +---------+   +---------+   +---------+
        | Node 01 |   | Node 02 |   | Node 03 |
        | Storage |   | Storage |   | Storage |
        +---------+   +---------+   +---------+
             |             |             |
             +-------------+-------------+
                           |
                           v
                 +----------------------+
                 |  Background Workers  |
                 |                      |
                 | Health Monitor       |
                 | Integrity Scanner    |
                 | Repair Manager       |
                 | Rebalancer           |
                 +----------------------+
```

---

# 5. Architecture Principles

### 5.1 Separate data plane and control plane

**Data plane** handles the actual object bytes.

**Control plane** decides:

- where objects should be stored;
- how many replicas are required;
- which nodes are healthy;
- which replicas are valid;
- which objects require repair;
- how data should be rebalanced.

### 5.2 PostgreSQL stores metadata, not large object payloads

PostgreSQL tracks objects, versions, replicas, nodes, states, checksums, and timestamps.

The actual object data remains on storage nodes.

### 5.3 Never claim a replica is healthy before verification

A replica becomes `HEALTHY` only after the storage operation has completed and the stored data has been verified.

### 5.4 Copy before delete

During repair or rebalancing:

```text
COPY -> VERIFY -> METADATA UPDATE -> DELETE OLD COPY
```

Never:

```text
DELETE -> COPY
```

### 5.5 Unreachable does not automatically mean dead

A node that cannot be contacted may be experiencing a temporary network partition.

The node lifecycle therefore includes:

```text
HEALTHY -> SUSPECT -> UNAVAILABLE -> RECOVERING -> HEALTHY
```

### 5.6 Distributed behavior must be testable

Every important fault-tolerance feature should have a reproducible failure test.

---

# 6. Technology Stack

| Component | Initial Technology |
|---|---|
| Language | Python |
| Public API | FastAPI |
| Metadata database | PostgreSQL |
| Background jobs | Celery |
| Broker | Redis |
| Storage | Local filesystem per storage-node container |
| Node communication | HTTP |
| Integrity | SHA-256 |
| Deployment | Docker Compose |
| API style | REST |
| Testing | Pytest + integration/failure tests |

These choices are the initial implementation baseline. Individual technologies can be replaced later if a real technical requirement justifies it, but the component contracts should remain stable.

---

# 7. Repository Structure

The planned repository structure is:

```text
ps1/
|
+-- gateway/
|   +-- api.py
|   +-- upload.py
|   +-- download.py
|   +-- routing.py
|
+-- metadata/
|   +-- models.py
|   +-- database.py
|   +-- manager.py
|
+-- storage/
|   +-- node_server.py
|   +-- storage_engine.py
|   +-- checksum.py
|
+-- replication/
|   +-- replica_manager.py
|   +-- placement.py
|
+-- health/
|   +-- heartbeat.py
|   +-- failure_detector.py
|
+-- repair/
|   +-- repair_manager.py
|   +-- integrity_checker.py
|
+-- rebalance/
|   +-- rebalancer.py
|
+-- worker/
|
+-- common/
|
+-- tests/
|
+-- docker/
|
+-- docker-compose.yml
+-- requirements.txt
+-- .env.example
+-- README.md
```

The exact file structure may evolve during implementation, but ownership boundaries should remain clear.

---

# 8. Two-Team Development Split

Vault is divided into two major independently developed areas.

## Part A — Data Plane

Part A owns how data is physically stored and transferred.

Responsibilities:

- Storage node service.
- Local object/chunk storage.
- Streaming uploads.
- Streaming downloads.
- Chunking.
- SHA-256 verification.
- Replica copy operations.
- Replica deletion.
- Storage-node health and statistics endpoints.

Primary directories:

```text
storage/
replication/
common/
tests/storage/
```

---

## Part B — Control Plane

Part B owns the system's decisions and reliability mechanisms.

Responsibilities:

- Public API gateway.
- PostgreSQL metadata.
- Object/version management.
- Node registry.
- Placement.
- Replication policies.
- Heartbeats and failure detection.
- Repair scheduling.
- Network partition handling.
- Rebalancing.
- Background workers.

Primary directories:

```text
gateway/
metadata/
health/
repair/
rebalance/
worker/
tests/control/
```

---

# 9. Shared Contracts

The two parts must not independently redefine the following concepts.

## Object

```text
object_id
name
current_version_id
state
created_at
updated_at
```

## Version

```text
version_id
object_id
version_number
size_bytes
checksum
state
created_at
committed_at
```

## Replica

```text
replica_id
version_id
node_id
status
checksum
size_bytes
last_verified_at
created_at
updated_at
```

## Storage Node

```text
node_id
address
status
capacity_bytes
used_bytes
last_heartbeat_at
created_at
updated_at
```

---

# 10. Object Identification

Filenames are not primary identifiers.

For example:

```text
name:
movie.mp4

object_id:
<generated stable ID>

version:
5
```

This prevents collisions and allows an object to have multiple versions.

Recommended ID strategy:

- UUIDv7, ULID, or equivalent sortable unique IDs for objects/versions.
- Stable generated IDs for storage nodes.

The exact ID library will be selected during implementation.

---

# 11. Metadata Model

Initial PostgreSQL schema:

```text
objects
------------------------------------------------
object_id              PK
name
current_version_id
state
created_at
updated_at

versions
------------------------------------------------
version_id             PK
object_id              FK -> objects
version_number
size_bytes
checksum
state
created_at
committed_at

replicas
------------------------------------------------
replica_id             PK
version_id             FK -> versions
node_id                FK -> storage_nodes
status
checksum
size_bytes
last_verified_at
created_at
updated_at

storage_nodes
------------------------------------------------
node_id                PK
address
status
capacity_bytes
used_bytes
last_heartbeat_at
created_at
updated_at
```

Important constraints will include:

- unique object identity;
- unique version number within an object;
- unique replica for a given version/node pair;
- foreign-key integrity;
- transactional metadata updates.

---

# 12. Object Versioning

Objects are versioned rather than blindly overwritten.

Example:

```text
movie.mp4

v1
v2
v3
v4 <- current
```

A conditional update can use:

```http
X-Expected-Version: 4
```

If the current version is already 5, the operation returns a version conflict instead of silently overwriting another client's update.

This is the basis for safe concurrent writes.

---

# 13. Replica State Machine

Initial replica states:

```text
PENDING
   |
   v
COPYING
   |
   v
HEALTHY
  / |  \
 v  v   v
STALE
CORRUPTED
UNAVAILABLE
  \  |  /
   \ | /
    v
REPAIRING
    |
    v
COPYING
```

Additional terminal/error state:

```text
FAILED
```

Meaning:

| State | Meaning |
|---|---|
| PENDING | Replica has been scheduled but storage has not started |
| COPYING | Data is being written |
| HEALTHY | Stored data is verified and matches expected metadata |
| STALE | Replica contains an older valid version |
| CORRUPTED | Checksum does not match |
| UNAVAILABLE | Replica's node cannot currently be reached |
| REPAIRING | Replica is being rebuilt |
| FAILED | Operation failed and requires retry or further handling |

---

# 14. Storage Node State Machine

```text
JOINING
   |
   v
HEALTHY
   |
   v
SUSPECT
   |
   v
UNAVAILABLE
   |
   v
RECOVERING
   |
   v
HEALTHY
```

A node can also enter:

```text
DRAINING -> REMOVED
```

for controlled removal.

---

# 15. Public REST API

Base path:

```text
/api/v1
```

## Object APIs

```http
PUT    /api/v1/objects/{name}
GET    /api/v1/objects/{name}
HEAD   /api/v1/objects/{name}
DELETE /api/v1/objects/{name}

GET    /api/v1/objects
GET    /api/v1/objects/{name}/metadata
GET    /api/v1/objects/{name}/versions
```

## Node APIs

```http
GET /api/v1/nodes
GET /api/v1/nodes/{node_id}
```

## Health

```http
GET /api/v1/health
```

## Administrative APIs

```http
POST /api/v1/admin/repair
GET  /api/v1/admin/repair/{repair_id}

POST /api/v1/admin/integrity/check
GET  /api/v1/admin/integrity/check/{job_id}

POST /api/v1/admin/rebalance
GET  /api/v1/admin/rebalance/{job_id}
```

Administrative endpoints will be protected separately from normal object operations.

---

# 16. Internal Storage Node API

The control plane communicates with storage nodes through an internal API.

```http
PUT    /internal/v1/objects/{object_id}/{version_id}
GET    /internal/v1/objects/{object_id}/{version_id}
HEAD   /internal/v1/objects/{object_id}/{version_id}
DELETE /internal/v1/objects/{object_id}/{version_id}

GET    /internal/v1/objects/{object_id}/{version_id}/verify

GET    /internal/v1/health
GET    /internal/v1/stats
```

The internal contract allows Part A and Part B to be developed independently.

---

# 17. Upload Workflow

```text
Client
  |
  | PUT object
  v
Gateway
  |
  | create/validate version
  v
Metadata Manager
  |
  | placement request
  v
Placement Manager
  |
  +--------+--------+
  |        |        |
  v        v        v
Node 1   Node 2   Node 3
  |        |        |
  +--------+--------+
           |
           v
       Verification
           |
           v
     Write policy met?
        /       \
      YES       NO
       |         |
       v         v
   COMMIT      FAIL/RETRY
```

Critical rule:

> Metadata must not claim a replica is healthy before the storage operation and required verification have succeeded.

---

# 18. Download Workflow

```text
Client
  |
  | GET object
  v
Gateway
  |
  v
Metadata
  |
  v
Current version
  |
  v
Healthy replicas
  |
  v
Select suitable replica
  |
  v
Stream object
  |
  v
Return to client
```

If a selected replica fails, another eligible healthy replica can be attempted.

---

# 19. Replication

Example configuration:

```yaml
replication:
  factor: 3
  write_quorum: 2
  read_quorum: 1
```

With RF=3:

```text
             Object
            /  |  \
           /   |   \
          v    v    v
        Node1 Node2 Node3
```

The exact consistency and availability behavior of each policy must be documented and tested rather than inferred from the configuration names alone.

---

# 20. Large Object Handling

Vault must not load a 10 GB or 50 GB object entirely into RAM.

The storage path will use streaming.

Example:

```text
10 GB object
     |
     +-- chunk 0
     +-- chunk 1
     +-- chunk 2
     +-- ...
     +-- chunk N
```

Initial configurable chunk size:

```text
16 MB
```

The implementation can support different sizes later.

Chunking enables:

- lower memory usage;
- streaming transfer;
- partial repair;
- better large-object handling.

---

# 21. Integrity Verification

Vault uses SHA-256 initially.

Example:

```text
Expected checksum:
ABC123

Node 1 -> ABC123  OK
Node 2 -> XYZ999  CORRUPTED
Node 3 -> ABC123  OK
```

Node 2 becomes `CORRUPTED`.

A verified replica is used to rebuild it.

The rebuilt data must be verified before it becomes `HEALTHY`.

---

# 22. Failure Detection

Storage nodes periodically report health.

Example configuration:

```yaml
health:
  heartbeat_interval_seconds: 5
  suspect_after_seconds: 15
  unavailable_after_seconds: 30
```

Initial state progression:

```text
HEALTHY
   |
   | missed heartbeats
   v
SUSPECT
   |
   | timeout threshold
   v
UNAVAILABLE
```

These values are configuration examples and will be tuned through testing.

---

# 23. Automatic Repair

If:

```text
healthy replicas < configured replication factor
```

the repair system queues the object.

Repair workflow:

```text
Detect under-replication
        |
        v
Find verified source replica
        |
        v
Choose target node
        |
        v
Create REPAIRING replica
        |
        v
Copy object/chunks
        |
        v
Verify checksum
        |
        v
Mark HEALTHY
        |
        v
Update metadata
```

Repair should have:

- retry;
- backoff;
- concurrency limits;
- prioritization;
- observable status.

---

# 24. Corruption Handling

Integrity scanner:

```text
Replica
  |
  v
Calculate checksum
  |
  v
Compare with expected checksum
  |
  +---- match ----> HEALTHY
  |
  +---- mismatch -> CORRUPTED
                         |
                         v
                       REPAIR
```

Corruption should be recorded as an observable event.

---

# 25. Network Partition Handling

A node that cannot be reached is not automatically considered permanently dead.

Example:

```text
Node 1 -------- X -------- Node 2
               partition
```

Vault uses:

- heartbeats;
- timeouts;
- SUSPECT state;
- UNAVAILABLE state;
- version information;
- configured write/read policies;
- retries and backoff;
- reconciliation when connectivity returns.

The objective is to avoid unsafe replica deletion or conflicting metadata decisions during temporary partitions.

---

# 26. Rebalancing

When a new node joins:

```text
Node 1
Node 2
Node 3

       +

Node 4
```

Vault calculates desired placement and migrates selected data.

Safe sequence:

```text
SELECT
  |
  v
COPY
  |
  v
VERIFY
  |
  v
UPDATE METADATA
  |
  v
REMOVE OLD REPLICA
```

The old replica is not removed before the new copy is verified.

---

# 27. Concurrency

Vault must safely support:

- concurrent reads;
- concurrent writes;
- reads during repair;
- multiple repair jobs;
- metadata updates occurring simultaneously.

The initial strategy includes:

- asynchronous I/O;
- PostgreSQL transactions;
- object/version conditional writes;
- explicit version conflicts;
- atomic replica-state transitions.

---

# 28. Error Model

Errors use a consistent format:

```json
{
  "error": {
    "code": "OBJECT_NOT_FOUND",
    "message": "Object was not found.",
    "request_id": "req_8f72a1"
  }
}
```

Initial error codes:

```text
OBJECT_NOT_FOUND
OBJECT_ALREADY_EXISTS
VERSION_CONFLICT
NODE_UNAVAILABLE
INSUFFICIENT_REPLICAS
CHECKSUM_MISMATCH
REPAIR_IN_PROGRESS
STORAGE_FULL
INVALID_REQUEST
INTERNAL_ERROR
```

---

# 29. Request Tracing

Every request should receive a request ID.

```text
Client
  |
  | req_123
  v
Gateway
  |
  | req_123
  v
Metadata
  |
  | req_123
  v
Storage Node
```

This makes distributed debugging much easier.

---

# 30. Observability

Important events:

```text
upload_started
upload_committed
upload_failed

replica_created
replica_verified
replica_failed

node_suspected
node_unavailable
node_recovered

checksum_mismatch

repair_started
repair_completed
repair_failed

rebalance_started
rebalance_completed
```

Metrics should include:

```text
objects_total
bytes_stored
replicas_total

healthy_nodes
unavailable_nodes

uploads_total
downloads_total

repairs_total
repairs_successful
repairs_failed

corruption_events

repair_duration
upload_duration
download_duration

rebalance_objects_moved
rebalance_bytes_moved
```

Performance numbers will be measured during testing rather than claimed beforehand.

---

# 31. Docker Development Cluster

The local distributed environment will contain:

```text
Docker Network
|
+-- vault-gateway
+-- postgres
+-- redis
+-- vault-worker
|
+-- vault-node-01
+-- vault-node-02
+-- vault-node-03
+-- vault-node-04
```

The extra node is useful for repair and rebalancing demonstrations.

Nodes can be deliberately stopped or isolated to reproduce failures.

---

# 32. Development Roadmap

## Phase 0 — Foundation

- Repository structure.
- Python configuration.
- Docker Compose.
- Environment configuration.
- Logging.
- Shared models/constants.
- Test skeleton.
- API contract documentation.

## Phase 1 — Storage Node

- Node server.
- Local storage.
- PUT.
- GET.
- HEAD.
- DELETE.
- Health.
- Statistics.

## Phase 2 — Large Objects

- Streaming upload.
- Streaming download.
- Chunking.
- Large-file tests.
- Disk-space checks.

## Phase 3 — Metadata

- PostgreSQL.
- Models.
- Migrations.
- Constraints.
- Transactions.
- Metadata manager.

## Phase 4 — Gateway

- Public API.
- Upload.
- Download.
- Delete.
- List.
- Metadata.
- Versions.
- Errors.

## Phase 5 — Node Management and Placement

- Registration.
- Discovery.
- Capacity tracking.
- Node states.
- Placement algorithm.

## Phase 6 — Replication

- Replica creation.
- RF.
- Write policy.
- Read selection.
- Replica state tracking.

## Phase 7 — Concurrency and Versioning

- Object versions.
- Expected-version writes.
- Transactions.
- Conflict handling.
- Concurrent read/write tests.

## Phase 8 — Integrity

- SHA-256.
- Chunk checksums.
- Verification.
- Corruption detection.
- Integrity scans.

## Phase 9 — Health

- Heartbeats.
- Failure detector.
- SUSPECT.
- UNAVAILABLE.
- Recovery detection.

## Phase 10 — Automatic Repair

- Under-replication detection.
- Repair queue.
- Source selection.
- Target selection.
- Copy.
- Verification.
- Retry.
- Repair prioritization.

## Phase 11 — Network Partitions

- Timeout behavior.
- Partition behavior.
- Read/write policy.
- Reconciliation.

## Phase 12 — Rebalancing

- Node join.
- Node drain.
- Placement recalculation.
- Migration.
- Verification.
- Cleanup.
- Bandwidth limits.

## Phase 13 — Failure Testing

- Node crash.
- Node restart.
- Corruption.
- Network isolation.
- Concurrent writes.
- Large objects.
- Multiple node failures.
- Repair stress.
- Rebalancing.

## Phase 14 — Performance

Measure:

- upload throughput;
- download throughput;
- repair duration;
- storage overhead;
- network overhead;
- concurrency;
- rebalancing cost.

## Phase 15 — Finalization

- Documentation.
- API documentation.
- Logging/metrics.
- Demo scripts.
- Failure simulation.
- Final README.
- Architecture diagrams.
- Final demonstration.

---

# 33. Recommended Git Workflow

Do not develop directly on `main`.

Recommended structure:

```text
main
 |
 +-- develop
      |
      +-- feature/storage-node
      +-- feature/metadata
      +-- feature/gateway
      +-- feature/replication
      +-- feature/health
      +-- feature/repair
      +-- feature/rebalance
```

Workflow:

```text
Feature branch
      |
      v
Implementation
      |
      v
Tests
      |
      v
Pull Request
      |
      v
Review
      |
      v
develop
      |
      v
Integration tests
      |
      v
main
```

No feature should be merged without its relevant tests.

---

# 34. Testing Strategy

Testing is divided into four levels.

### Unit tests

Test individual functions:

- checksum;
- placement;
- state transitions;
- metadata operations;
- chunk handling.

### Integration tests

Test components together:

```text
Gateway -> PostgreSQL
Gateway -> Storage Node
Repair -> Storage Node
```

### Failure tests

Intentionally break things:

```text
stop node
corrupt data
block network
restart node
fill storage
```

### End-to-end tests

Test complete workflows:

```text
upload
  -> replicate
  -> download
  -> fail node
  -> repair
  -> verify
```

---

# 35. Critical Demonstration Scenarios

The final demonstration should visibly show at least these scenarios.

## Scenario 1 — Replicated upload

```text
Upload large object
       |
       v
3 replicas created
       |
       v
All checksums match
```

## Scenario 2 — Node failure

```text
3 healthy replicas
       |
       v
Stop Node 2
       |
       v
Node 2 -> UNAVAILABLE
       |
       v
Repair starts
       |
       v
Replacement replica verified
       |
       v
3 healthy replicas restored
```

## Scenario 3 — Corruption

```text
Corrupt Node 2 data
       |
       v
Integrity scan
       |
       v
Checksum mismatch
       |
       v
Repair
       |
       v
Checksum valid
```

## Scenario 4 — Network partition

```text
Isolate Node 2
       |
       v
Node becomes SUSPECT/UNAVAILABLE
       |
       v
Cluster follows configured policy
       |
       v
Restore network
       |
       v
Node reconciles
```

## Scenario 5 — Rebalancing

```text
3-node cluster
       |
       v
Node 4 joins
       |
       v
Placement recalculation
       |
       v
Data migration
       |
       v
Verification
       |
       v
Balanced cluster
```

---

# 36. Definition of Done

Vault's core implementation is considered successful when it can demonstrate:

- large object upload;
- large object download;
- configurable replication;
- multiple storage nodes;
- metadata consistency;
- concurrent operations;
- checksum verification;
- corruption detection;
- node failure detection;
- automatic replica repair;
- temporary node/network unreachability handling;
- stale/inconsistent replica handling;
- node recovery;
- background rebalancing;
- measurable recovery time;
- measurable storage overhead;
- reproducible automated failure tests;
- complete Docker-based local deployment.

---

# 37. What Parquet Means Here

Parquet is **not a required internal storage mechanism** for Vault.

A Parquet file can simply be an object stored by Vault:

```text
Pandas
   |
   v
dataset.parquet
   |
   v
Vault
   |
   +-- Node 1
   +-- Node 2
   +-- Node 3
```

Vault's job is to store and protect the object. It does not need Pandas to operate.

---

# 38. Development Rule

The project will be implemented incrementally.

For every step:

1. Define the interface.
2. Implement the component.
3. Add unit tests.
4. Add integration tests where required.
5. Verify the component against the shared contract.
6. Integrate through a pull request.
7. Run failure tests before moving to the next major stage.

No team member should silently change shared schemas, API contracts, state names, or replication semantics without updating the relevant documentation and tests.

---

# 39. Current Project Status

The repository is currently being prepared for implementation.

The planned sequence is:

```text
Architecture
    |
    v
Contracts
    |
    v
Foundation
    |
    v
Storage Node
    |
    v
Metadata + Gateway
    |
    v
Replication
    |
    v
Integrity
    |
    v
Failure Detection
    |
    v
Repair
    |
    v
Network Partitions
    |
    v
Rebalancing
    |
    v
Failure Testing
    |
    v
Performance + Demo
```

The next implementation task is **Step 0 — Project Foundation**, followed by the detailed Step 1 storage-node work.

---

## 40. Project Philosophy

Vault is being built around one central principle:

> **Data should survive failures without the system losing track of what is valid.**

The system should not merely store copies. It should know:

- what data exists;
- which version is current;
- where replicas are;
- which replicas are healthy;
- which nodes are available;
- what has failed;
- what needs repair;
- when repair is complete;
- and whether the repaired data is actually correct.

That is the core of Vault.


# 41. Current Implemented Status

The integrated control-plane branch now contains the implemented Part B reliability path through final validation:

```text
B1  Database foundation
B2  Metadata manager
B3  Storage-node client
B4  Node registry / health
B5  Deterministic placement
B6  Gateway object I/O
B7  Replication and quorum policy
B8  Heartbeats / failure detector
B9  Durable repair
B10 Integrity scanning / corruption repair
B11 Network-partition recovery
B12 Safe rebalancing / node drain
B13 Failure / chaos regression tests
B14 Part A storage-node integration tests
```

Part A's storage-node implementation is integrated under `storage/` and is exercised by the B14 suite through the exact internal HTTP contract. The integration tests cover health, statistics, PUT, duplicate protection, HEAD, streaming GET, VERIFY, DELETE, and a gateway-to-storage-node object lifecycle.

B13 covers node failure and repair, checksum corruption, network isolation/recovery, conditional concurrent writes, and persistence of job failure state across process restart.

The final integration branch is intended as the handoff candidate; no feature work is considered complete until its GitHub Actions matrix passes on Python 3.11 and 3.12.


---

# 42. Final Part B Reliability Completion

The control plane now covers the full B1-B14 milestone sequence plus the production-facing reliability pieces defined by the implementation specification:

- durable PostgreSQL metadata and Alembic migration foundation;
- object/version/replica/node lifecycle enforcement;
- deterministic placement, replication, quorum-aware gateway I/O, and conditional writes;
- heartbeat processing and failure detection;
- automatic repair-job creation when a committed replica becomes unavailable because of node failure;
- verified checksum/size integrity scanning and corruption-triggered repair;
- partition recovery and recovered-node reconciliation;
- copy/verify-before-delete rebalancing and node draining;
- failure/chaos validation and Part A storage-node integration;
- Celery + Redis background workers with retry/backoff, late acknowledgements, bounded worker concurrency, and periodic scheduling;
- administrative repair, integrity, and rebalancing APIs;
- central environment-backed configuration for durability, health, storage timeout, worker retry, and scan policies.

## Background worker

The Celery application is:

`worker.celery_app:celery_app`

Run a worker with:

`celery -A worker.celery_app:celery_app worker --loglevel=INFO`

Run the periodic scheduler with:

`celery -A worker.celery_app:celery_app beat --loglevel=INFO`

Canonical tasks:

`repair_version`, `verify_replica`, `scan_node`, `check_under_replicated_objects`, `rebalance_node`, `migrate_replica`, `process_node_health`

Additional orchestration tasks exist for durable job execution and fan-out:

`run_repair_job`, `run_rebalance_job`, `scan_version`, `scan_all_nodes`, `rebalance_draining_nodes`

Redis is the broker/result backend by default. Retryable storage/control-plane failures use exponential backoff and durable repair/rebalance state remains in PostgreSQL.

## Administrative API

Repair:

`POST /api/v1/admin/repair`

`GET /api/v1/admin/repair/{repair_id}`

Integrity:

`POST /api/v1/admin/integrity/check`

`GET /api/v1/admin/integrity/check/{job_id}`

Rebalance:

`POST /api/v1/admin/rebalance`

`GET /api/v1/admin/rebalance/{job_id}`

The integrity request accepts exactly one target: `replica_id`, `node_id`, or `version_id`.

The rebalancing request accepts either a `node_id` to drain or a `replica_id` plus `target_node_id` to migrate a specific replica.

## Configuration

The repository's `.env.example` documents the centralized configuration surface. The `VAULT_*` variables are the preferred names; legacy durability/health names remain accepted for compatibility.

Database migrations:

`alembic upgrade head`

The migration environment reads `DATABASE_URL` so development can use SQLite while deployment can use PostgreSQL.

## Reliability invariants

The completed control plane maintains the following invariants:

1. A replica reaches `HEALTHY` only after storage success and verification.
2. Repair and rebalancing verify a trusted healthy source before copying.
3. Replacement data is verified before the old copy is deleted.
4. Node unreachability is not treated as data corruption.
5. A recovered node remains `RECOVERING` until reconciliation succeeds.
6. Failed repair/rebalance work is persisted and retriable.
7. Concurrent version updates are protected by `X-Expected-Version`.
8. Large object payloads remain on storage nodes rather than in PostgreSQL metadata.

## Final validation

CI compiles all control-plane packages, worker code, migrations, tests, and the integrated Part A storage package, then runs the full pytest suite on Python 3.11 and 3.12.
