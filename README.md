# Omnifleet Data Platform — Infrastructure Documentation

| | |
|---|---|
| **Project** | Omnifleet — Single-Node Lakehouse / Streaming Analytics Platform |
| **Orchestration** | Docker Compose (single host) |
| **Access** | Private Tailscale network — `https://ubuntu-server.tail712639.ts.net` |

This document describes the platform exactly as configured. No configuration was modified during this review — it is a read-only architectural reference.

---

## 1. Executive Summary

Omnifleet is a self-hosted **data lakehouse and streaming analytics platform** running entirely on a single Docker host. It combines:

- A **streaming ingestion path**: Kafka (KRaft mode) + Schema Registry, consumed by Spark Structured Streaming.
- A **lakehouse storage layer**: MinIO (S3-compatible object storage) organized into a medallion architecture (raw → bronze → silver → gold), with a dedicated checkpoint bucket.
- A **processing layer**: a single-master/single-worker Apache Spark cluster, plus a Jupyter notebook environment acting as an interactive Spark driver.
- A **warehouse layer**: PostgreSQL 16, holding `stream`, `warehouse`, and `metadata` schemas, modeled with dbt.
- A **BI/analytics layer**: Apache Superset and Grafana, both backed by Redis for caching/rate-limiting.
- A unified **edge/reverse proxy**: Caddy, exposing every UI under a single TLS-secured hostname reachable over Tailscale.

The entire stack is designed to fit a constrained host (**4 vCPU / ~9.6 GB RAM**), using Docker Compose **profiles** to run only the subset of services needed for a given workload (streaming, batch, or analytics).

---

## 2. Host Environment & Capacity

As documented at the top of `docker-compose.yml`:

| Resource | Capacity |
|---|---|
| CPU | 4 cores |
| RAM | 9.6 GB (~9 GB usable) |
| Fast storage | `/data/ssd` |
| Bulk/cold storage | `/data/hdd` |
| Swap | 4 GB |

This single-host budget directly drives every `mem_limit` / `mem_reservation` / `cpus` value in the compose file, the choice of profiles (so unrelated workloads don't run simultaneously), and the SSD vs. HDD volume placement strategy (Section 13).

---

## 3. Architecture Diagrams

### 3.1 Service & Network Topology

```mermaid
graph TB
    subgraph Internet["Tailscale Private Network"]
        User[("User Browser")]
    end

    User -->|"HTTPS :443 / :80"| Caddy["Caddy<br/>Reverse Proxy + TLS"]

    subgraph Frontend["frontend network (172.21.0.0/16)"]
        Caddy
    end

    subgraph Backend["backend network (172.20.0.0/16)"]
        Postgres[("PostgreSQL 16")]
        MinIO["MinIO<br/>(S3 Object Storage)"]
        Kafka["Kafka (KRaft)"]
        SchemaReg["Schema Registry"]
        SparkMaster["Spark Master"]
        SparkWorker["Spark Worker"]
        Redis[("Redis 7")]
        dbt["dbt (interactive)"]
    end

    Caddy --> Postgres
    Caddy -->|"/pgadmin"| pgAdmin["pgAdmin"]
    Caddy -->|"/minio, /s3"| MinIO
    Caddy -->|"/kafka"| KafkaUI["Kafka UI"]
    Caddy -->|"/schema-registry"| SchemaReg
    Caddy -->|"/spark-master"| SparkMaster
    Caddy -->|"/spark-worker"| SparkWorker
    Caddy -->|"/jupyter"| Jupyter["Jupyter Lab"]
    Caddy -->|"/grafana"| Grafana["Grafana"]
    Caddy -->|"/superset, /"| Superset["Superset"]

    SparkMaster --- SparkWorker
    SparkWorker <-->|reads/writes| MinIO
    Jupyter <-->|driver| SparkMaster
    Jupyter <-->|reads/writes| MinIO
    Jupyter <--> Kafka
    Kafka <--> SchemaReg
    SparkWorker <--> Kafka
    Postgres --- dbt
    Superset --> Postgres
    Grafana --> Postgres
    Superset -.cache/rate-limit.-> Redis
    Grafana -.remote cache.-> Redis
    SparkWorker -.lookup cache.-> Redis

    pgAdmin -.-> Postgres
    KafkaUI -.-> Kafka
```

### 3.2 Data Flow (Medallion Lakehouse)

```mermaid
graph LR
    Source[("Fleet/Telemetry<br/>Producers")] --> Kafka["Kafka Topics<br/>(12 partitions)"]
    Kafka <-.schemas.-> SchemaReg["Schema Registry"]
    Kafka --> SparkStreaming["Spark Structured<br/>Streaming"]

    SparkStreaming --> Raw["MinIO: raw bucket<br/>(unversioned landing)"]
    Raw --> Bronze["MinIO: bronze bucket<br/>(versioned)"]
    Bronze --> Silver["MinIO: silver bucket<br/>(versioned, conformed)"]
    Silver --> Gold["MinIO: gold bucket<br/>(versioned, business-level)"]

    SparkStreaming -->|checkpoints| LocalCkpt["/opt/spark/checkpoints<br/>(SSD, streaming WAL)"]
    SparkStreaming -.optional state.-> MinioCkpt["MinIO: checkpoint bucket<br/>(versioned)"]

    Gold --> SparkBatch["Spark Batch Jobs"]
    SparkBatch --> PgStream["Postgres: stream schema"]
    PgStream --> dbtT["dbt models"]
    dbtT --> PgWarehouse["Postgres: warehouse schema"]
    dbtT --> PgMeta["Postgres: metadata schema"]

    PgWarehouse --> Superset["Superset Dashboards"]
    PgWarehouse --> Grafana["Grafana Dashboards"]

    SparkStreaming -.vehicle/route lookups.-> Redis[("Redis<br/>ephemeral cache")]
```

---

## 4. Docker Networks

Two user-defined bridge networks segment the platform:

| Network | Subnet | Purpose |
|---|---|---|
| `omnifleet-backend` | `172.20.0.0/16` | Internal data-plane traffic — databases, message bus, Spark cluster, cache. All services attach here. |
| `omnifleet-frontend` | `172.21.0.0/16` | Edge-facing traffic. Caddy and any service it reverse-proxies to (pgAdmin, MinIO, Kafka UI, Jupyter, Grafana, Superset) attach here in addition to `backend`. |

`schema-registry`, `spark-master`, and `spark-worker` are **backend-only**, yet are still reverse-proxied by Caddy — this works because **Caddy itself is attached to both networks**, bridging frontend traffic into the backend segment for those specific routes.

---

## 5. Deployment Profiles

Compose profiles let the operator start only the services relevant to the current workload, keeping the host within its ~9 GB RAM budget.

| Profile | Services included |
|---|---|
| *(none / core — always runs)* | `postgres`, `pgadmin`, `minio`, `mc-init`, `caddy` |
| `streaming` | `kafka`, `schema-registry`, `kafka-ui`, `spark-master`, `spark-worker`, `jupyter`, `grafana`, `redis` |
| `Processing` | `spark-master`, `spark-worker`, `jupyter` |
| `modeling` | `dbt` |
| `analytics` | `grafana`, `superset`, `redis` |

Services belonging to multiple profiles (`spark-master`, `spark-worker`, `jupyter`, `redis`, `grafana`, `superset`) are shared infrastructure reused across workload types rather than duplicated.

### 5.1 Indicative Memory Footprint per Profile

Approximate sums of `mem_limit` (see Section 14 for full table), illustrating why profiles exist on a ~9.6 GB host:

| Combination | Approx. total `mem_limit` |
|---|---|
| Core only | ~1.6 GB |
| Core + `streaming` | ~9.0 GB |
| Core + `Processing` | ~5.3 GB |
| Core + `analytics` | ~2.0 GB |

Running `streaming` and `Processing` simultaneously is possible but approaches the host's usable RAM — note that `streaming` already includes Spark/Jupyter, so adding `Processing` separately is only needed for standalone batch workloads outside of streaming mode.

---

## 6. Reverse Proxy & External Access — Caddy

**Container:** `omnifleet-caddy` · **Image:** `caddy:2-alpine` · **Networks:** `frontend`, `backend`

### 6.1 Global Configuration

- Admin API bound to `0.0.0.0:2019` (used internally for the container healthcheck via `wget` against `/config/`; not published to the host).
- HTTP/1.1 and HTTP/2 (`h1 h2`) enabled.
- Structured JSON access logs written to `/data/caddy/logs/access.log`, rotated at 10 MB with 3 files retained.

### 6.2 Site Block — `ubuntu-server.tail712639.ts.net`

- **TLS**: manually provided certificate/key pair (`/certs/ubuntu-server.tail712639.ts.net.crt` / `.key`), mounted read-only from `/opt/omnifleet/caddy/certs` — i.e., a Tailscale-issued certificate for the tailnet's MagicDNS hostname, not a public CA cert.
- **Compression**: `gzip` and `zstd` enabled for all responses.
- **Security headers** applied globally:
  - `X-Content-Type-Options: nosniff`
  - `X-Frame-Options: SAMEORIGIN`
  - `Referrer-Policy: strict-origin-when-cross-origin`

### 6.3 Routing Table

| Path | Redirect | Proxied To | Notes |
|---|---|---|---|
| `/health` | — | (static `"ok"` response) | Simple liveness probe endpoint |
| `/portainer`, `/portainer/` | → `https://...:9443` (302) | *(external — Portainer is not part of this compose file)* | "Smart shortcut" to Portainer's native HTTPS port |
| `/grafana`, `/grafana*` | `/grafana` → `/grafana/` | `grafana:3000` | |
| `/superset`, `/superset/*` | `/superset(/)` → `/superset/welcome/` | `superset:8088` | |
| `/jupyter`, `/jupyter*` | `/jupyter` → `/jupyter/` | `jupyter:8888` | Adds `Host`, `X-Real-IP`, `X-Forwarded-For`, `X-Forwarded-Proto`; `flush_interval -1` (unbuffered) for WebSocket/kernel streaming |
| `/minio/*` | `/minio` → `/minio/` | `minio:9001` | `handle_path` strips the `/minio` prefix (MinIO Console) |
| `/s3*` | `/s3` → `/s3/` | `minio:9000` | `handle_path` strips the `/s3` prefix (S3 API endpoint) |
| `/pgadmin*` | `/pgadmin` → `/pgadmin/` | `pgadmin:80` | |
| `/kafka*` | `/kafka` → `/kafka/` | `kafka-ui:8080` | |
| `/schema-registry/*` | `/schema-registry` → `/schema-registry/` | `schema-registry:8181` | `handle_path` strips the prefix |
| `/spark-worker/*` | `/spark-worker` → `/spark-worker/` | `spark-worker:8081` | `handle_path` strips the prefix |
| `/spark-master/*` | `/spark-master` → `/spark-master/` | `spark-master:8080` | `handle_path` strips the prefix |
| *(everything else)* | — | `superset:8088` | Fallback / default landing application |

---

## 7. Core Layer (Always Running)

These services run regardless of which optional profile is active.

### 7.1 PostgreSQL

**Container:** `omnifleet-postgres` · **Image:** `postgres:16`

The central relational database for the platform — backs Superset, Grafana, dbt, and the `stream` / `warehouse` / `metadata` schemas.

| Setting | Value |
|---|---|
| `POSTGRES_INITDB_ARGS` | `--encoding=UTF8 --locale=C` |
| Init scripts | `/opt/omnifleet/init/postgres` → `/docker-entrypoint-initdb.d` (read-only) — runs `001-schemas.sql` on first boot |
| Data volume | `/data/ssd/postgres` (SSD — performance-critical) |
| Backups volume | `/data/hdd/backup/postgres` (HDD — cold storage) |
| Host port | `5432:5432` |
| Healthcheck | `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB` (20s interval, 5 retries, 40s start period) |
| Resources | `mem_limit: 768m`, `mem_reservation: 384m`, `cpus: 0.8` |
| Networks | `backend` |
| Hardening | `no-new-privileges:true`, `cap_drop: [SYS_ADMIN, SYS_PTRACE]` |

**Tuned `postgresql.conf` parameters** (passed via `command`), reflecting the constrained-memory host:

| Parameter | Value | Purpose |
|---|---|---|
| `max_connections` | 40 | Limits connection-related memory overhead |
| `shared_buffers` | 192MB | ~25% of the container's memory limit |
| `effective_cache_size` | 576MB | Planner hint for available OS cache |
| `maintenance_work_mem` | 64MB | Memory for VACUUM/CREATE INDEX |
| `wal_buffers` | 8MB | WAL write buffer |
| `checkpoint_completion_target` | 0.9 | Spreads checkpoint I/O |
| `random_page_cost` | 1.1 | Tuned for SSD storage |
| `effective_io_concurrency` | 200 | Tuned for SSD storage |
| `work_mem` | 6MB | Per-operation sort/hash memory (kept low given `max_connections`) |
| `log_min_duration_statement` | 500 | Logs queries slower than 500ms |

### 7.2 pgAdmin

**Container:** `omnifleet-pgadmin` · **Image:** `dpage/pgadmin4:9`

Web UI for PostgreSQL administration, served under `/pgadmin`.

| Setting | Value |
|---|---|
| `PGADMIN_CONFIG_SERVER_MODE` | `False` (desktop/single-user mode) |
| `PGADMIN_CONFIG_MASTER_PASSWORD_REQUIRED` | `False` |
| `PGADMIN_DISABLE_POSTFIX` | `True` (no outbound mail) |
| `SCRIPT_NAME` | `/pgadmin` (so generated URLs match the Caddy subpath) |
| Depends on | `postgres` (`service_healthy`) |
| Data volume | `/data/hdd/pgadmin` → `/var/lib/pgadmin`; logs → `/var/log/pgadmin` |
| Scratch | `/tmp` as `tmpfs` (mode `1777`) |
| Host port | `5050:80` |
| Resources | `mem_limit: 256m`, `mem_reservation: 128m`, `cpus: 0.2` |
| Networks | `backend`, `frontend` |

### 7.3 MinIO

**Container:** `omnifleet-minio` · **Image:** `quay.io/minio/minio:latest` · **Hostname:** `minio`

S3-compatible object storage — the lakehouse's storage layer.

| Setting | Value |
|---|---|
| User | `1000:1000` (non-root) |
| `MINIO_DISK_USAGE_CRAWL_DELAY` | `72h` (reduces background scan overhead on a small host) |
| `MINIO_BROWSER` | `on` |
| `MINIO_BROWSER_REDIRECT_URL` | `https://ubuntu-server.tail712639.ts.net/minio/` (correct redirect after login when accessed via Caddy) |
| Command | `server /data --console-address ":9001"` |
| Data volume | `/data/hdd/minio` → `/data` (HDD — bulk object storage) |
| Host ports | `9000` (S3 API), `9001` (Console) |
| Healthcheck | `mc ready local` (20s interval, 5 retries, 40s start period) |
| Resources | `mem_limit: 384m`, `mem_reservation: 128m`, `cpus: 0.4` |
| Networks | `backend`, `frontend` |
| Hardening | `no-new-privileges:true`, **`cap_drop: ALL`** (most restrictive of any service) |

### 7.4 mc-init (Bucket Initialization)

**Container:** `omnifleet-mc-init` · **Image:** `quay.io/minio/mc:latest`

A one-shot initialization job (`restart: on-failure`) that provisions the data lake's bucket layout on startup.

| Setting | Value |
|---|---|
| User | `1000:1000` |
| Depends on | `minio` (`service_healthy`) |
| `MC_HOST_local` | `http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000` |
| `MC_CONFIG_DIR` | `/tmp/.mc` |
| Script volume | `/opt/omnifleet/init/minio` → `/scripts` (read-only) |
| Entrypoint / command | `/bin/sh /scripts/init-buckets.sh` |
| Resources | `mem_limit: 128m`, `mem_reservation: 32m`, `cpus: 0.1` |
| Networks | `backend` |

**`init-buckets.sh` behavior:**

1. Creates five buckets (idempotently, `mc mb --ignore-existing`), named via environment variables:
   - `${RAW_BUCKET}`, `${BRONZE_BUCKET}`, `${SILVER_BUCKET}`, `${GOLD_BUCKET}`, `${CHECKPOINT_BUCKET}`
2. Enables **versioning** on `bronze`, `silver`, `gold`, and `checkpoint` buckets — explicitly noted in the script as **mandatory for the Spark S3A "magic committer"** (see Section 11.3). The `raw` landing bucket is left unversioned.

### 7.5 Caddy

See Section 6 for full detail.

| Setting | Value |
|---|---|
| Host ports | `80`, `443` |
| Volumes | Caddyfile (ro), TLS certs (ro), Caddy data/config on HDD |
| Healthcheck | `wget` against the local admin API `/config/` endpoint (30s interval, 5 retries) |
| Resources | `mem_limit: 64m`, `mem_reservation: 32m`, `cpus: 0.1` |
| Networks | `frontend`, `backend` |

---

## 8. Data Lake Layout (MinIO Buckets)

The platform implements a **medallion architecture** on top of MinIO:

| Bucket (env var) | Versioning | Role |
|---|---|---|
| `${RAW_BUCKET}` | No | Landing zone for raw ingested data |
| `${BRONZE_BUCKET}` | Yes | Cleaned / typed data, written via Spark's S3A magic committer |
| `${SILVER_BUCKET}` | Yes | Conformed / joined datasets |
| `${GOLD_BUCKET}` | Yes | Business-level, aggregate-ready datasets |
| `${CHECKPOINT_BUCKET}` | Yes | Reserved checkpoint/state storage in object storage |

> **Note on streaming checkpoints:** `spark-defaults.conf` explicitly configures Structured Streaming checkpoints to live on the **local SSD** (`/opt/spark/checkpoints`), with an inline comment warning *"Do NOT point this at Redis — Redis is ephemeral"*. The `${CHECKPOINT_BUCKET}` in MinIO is provisioned (with versioning) but the active streaming checkpoint path is local-disk based for performance and durability.

---

## 9. Streaming Platform

### 9.1 Kafka

**Container:** `omnifleet-kafka` · **Image:** `confluentinc/cp-kafka:8.1.0` · **Hostname:** `kafka` · **Profile:** `streaming`

Runs in **KRaft mode** (no ZooKeeper) as a single combined broker+controller node.

| Setting | Value |
|---|---|
| `CLUSTER_ID` | `${KAFKA_CLUSTER_ID}` |
| `KAFKA_NODE_ID` | `1` |
| `KAFKA_PROCESS_ROLES` | `broker,controller` |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka:9093` |
| Listeners | `INTERNAL://0.0.0.0:29092`, `EXTERNAL://0.0.0.0:9092`, `CONTROLLER://0.0.0.0:9093` |
| Advertised listeners | `INTERNAL://kafka:29092`, `EXTERNAL://localhost:9092` |
| Security protocol map | All listeners `PLAINTEXT` |
| Inter-broker listener | `INTERNAL` |
| Replication factors | Offsets topic = 1, transaction state log = 1, min ISR = 1 (single-node, no HA) |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | `0` |
| `KAFKA_HEAP_OPTS` | `-Xms256m -Xmx768m` |
| Message size limits | `KAFKA_MESSAGE_MAX_BYTES` = `KAFKA_REPLICA_FETCH_MAX_BYTES` = `2097152` (2 MB) |
| Socket buffer | `KAFKA_SOCKET_RECEIVE_BUFFER_BYTES` = `2097152` |
| Log retention | `168` hours (7 days) **or** `1073741824` bytes (1 GB), whichever comes first |
| Log segment size | `268435456` bytes (256 MB) |
| Default partitions | `12` |
| Default replication factor | `1` |
| Data volume | `/data/ssd/kafka` (SSD — low-latency log writes) |
| Host port | `9092` |
| Healthcheck | `kafka-broker-api-versions --bootstrap-server kafka:29092` (30s interval, 10 retries, 60s start period) |
| Resources | `mem_limit: 900m`, `mem_reservation: 512m`, `cpus: 0.8` |
| Networks | `backend` |

### 9.2 Schema Registry

**Container:** `omnifleet-schema-registry` · **Image:** `confluentinc/cp-schema-registry:8.1.0` · **Hostname:** `schema-registry` · **Profile:** `streaming`

| Setting | Value |
|---|---|
| Depends on | `kafka` (`service_healthy`) |
| Listener | `http://0.0.0.0:8181` |
| Kafka bootstrap | `PLAINTEXT://kafka:29092` |
| `SCHEMA_REGISTRY_HEAP_OPTS` | `-Xms128m -Xmx384m` |
| Topic replication factor | `1` |
| Compatibility level | `BACKWARD` (new schema versions must remain readable by consumers using the previous schema) |
| Host port | `8181` |
| Healthcheck | `curl -fsS http://localhost:8181/subjects` (30s interval, 10 retries, 60s start period) |
| Resources | `mem_limit: 450m`, `mem_reservation: 256m`, `cpus: 0.3` |
| Networks | `backend` |

### 9.3 Kafka UI

**Container:** `omnifleet-kafka-ui` · **Image:** `provectuslabs/kafka-ui:latest` · **Profile:** `streaming`

| Setting | Value |
|---|---|
| Depends on | `kafka` (`service_healthy`) |
| `KAFKA_CLUSTERS_0_NAME` | `${KAFKA_CLUSTER_NAME}` |
| `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` | `kafka:29092` |
| `KAFKA_CLUSTERS_0_METRICS_PORT` | *(empty — JMX metrics collection disabled)* |
| `SERVER_SERVLET_CONTEXT_PATH` | `/kafka` (matches the Caddy subpath) |
| Host port | `8090:8080` |
| Healthcheck | `wget` against `/kafka/actuator/health` (30s interval, 5 retries, 60s start period) |
| Resources | `mem_limit: 256m`, `mem_reservation: 128m`, `cpus: 0.2` |
| Networks | `backend`, `frontend` |

---

## 10. Processing Platform — Apache Spark

A single-master, single-worker Spark 3.5.2 cluster (Bitnami images), used by both `streaming` and `batch` profiles.

### 10.1 Spark Master

**Container:** `omnifleet-spark-master` · **Image:** `bitnami/spark:3.5.2` · **Hostname:** `spark-master` · **Profile:** `Processing`, `streaming`

| Setting | Value |
|---|---|
| User | `1001:0` |
| `init` | `true` (PID 1 is tini — ensures proper signal handling and zombie reaping) |
| Depends on | `postgres`, `minio` (`service_healthy`) |
| `SPARK_MODE` | `master` |
| `SPARK_MASTER_HOST` | `spark-master` |
| `SPARK_LOCAL_DIRS` | `/opt/bitnami/spark/tmp` |
| `SPARK_LOG_LEVEL` | `ERROR` |
| `SPARK_NO_DAEMONIZE` | `true` |
| `SPARK_PUBLIC_DNS` | `https://ubuntu-server.tail712639.ts.net/` |
| Host ports | `7077` (cluster RPC), `8080` (Web UI) |
| Healthcheck | Python IPv6 socket connect to `::1:8080` (30s interval, 5 retries, 60s start period) |
| Resources | `mem_limit: 512m`, `mem_reservation: 256m`, `cpus: 0.3` |
| Networks | `backend` |
| Key volumes | checkpoints (SSD), tmp (SSD), `/opt/omnifleet/jobs` (ro), events (SSD), `spark-defaults.conf` (ro), `log4j2.properties` (ro) |

### 10.2 Spark Worker

**Container:** `omnifleet-spark-worker` · **Image:** `bitnami/spark:3.5.2` · **Hostname:** `spark-worker` · **Profile:** `Processing`, `streaming`

The cluster's only execution node — sized to be the largest container on the host.

| Setting | Value |
|---|---|
| User | `1001:0` |
| `init` | `true` (PID 1 is tini — ensures proper signal handling and zombie reaping) |
| Depends on | `spark-master` (`service_healthy`) |
| `SPARK_MODE` | `worker` |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` |
| `SPARK_WORKER_MEMORY` | `2g` |
| `SPARK_WORKER_CORES` | `2` |
| `SPARK_LOCAL_DIRS` | `/opt/bitnami/spark/tmp` |
| `SPARK_WORKER_DIR` | `/tmp/spark-work` |
| `SPARK_LOG_LEVEL` | `ERROR` |
| `SPARK_NO_DAEMONIZE` | `true` |
| `SPARK_PUBLIC_DNS` | `https://ubuntu-server.tail712639.ts.net/` |
| Host port | `8081` (Worker UI) |
| Healthcheck | Python IPv6 socket connect to `::1:8081` (30s interval, 5 retries, 60s start period) |
| Resources | `mem_limit: 2300m`, `mem_reservation: 1300m`, `cpus: 2.0` |
| Networks | `backend` |
| Key volumes | checkpoints (SSD), `/opt/omnifleet/jobs` (ro), tmp (SSD), work dir (SSD), `/opt/omnifleet/jars` → `jars-extra` (ro), events (SSD), `spark-defaults.conf` (ro), `log4j2.properties` (ro) |

### 10.3 Spark Configuration Deep-Dive (`spark-defaults.conf`)

The configuration file is dated **2026-06** and explicitly documents the cluster shape: *1 Master (512m/0.3cpu) · 1 Worker (2300m/2cpu) · Jupyter Driver (2500m/1.5cpu)*, storage via *MinIO S3A (magic committer) · PostgreSQL · Redis 7 (ephemeral cache)*, and streams via *Kafka + Structured Streaming · Schema Registry*.

#### a) Cluster & Networking
- `spark.master = spark://spark-master:7077`
- `spark.driver.host = jupyter`, `spark.driver.bindAddress = 0.0.0.0` — the Jupyter container acts as the Spark driver.
- Spark UI: enabled, behind a reverse proxy (`spark.ui.reverseProxy = true`, `spark.ui.reverseProxyUrl = https://ubuntu-server.tail712639.ts.net/spark-master/`), port `4040`, max port retries `10`.
- Retention: `100` jobs, `100` stages, `500` tasks retained in the UI.
- `spark.master.ui.decommission.enabled = false`

#### b) Resource Management
A documented memory budget for the three roles:

```
JUPYTER (2500m limit): driver 800m + overhead 256m + Jupyter kernel ~400m + OS ~1044m
WORKER  (2300m limit): executor 1000m + overhead 256m + worker daemon ~200m + OS ~844m
MASTER  (512m  limit): daemon only ~150-200m
```

Concrete settings:
- `spark.driver.memory = 800m`, `spark.driver.memoryOverhead = 256m`
- `spark.executor.memory = 1000m`, `spark.executor.memoryOverhead = 256m`
- `spark.executor.cores = 2`, `spark.executor.instances = 1`
- `spark.memory.fraction = 0.75`, `spark.memory.storageFraction = 0.30`

#### c) Storage & Filesystem — MinIO / S3A
- Endpoint `http://minio:9000`, path-style access enabled, SSL disabled (internal network).
- Implementation: `org.apache.hadoop.fs.s3a.S3AFileSystem`.
- **Fast upload** enabled (`bytebuffer` buffer, 4 active blocks) — streams writes directly to S3 without local temp files.
- Connection pool sized for a single executor: max `30` connections, `5s` establish timeout, `200s` connection timeout, `3` attempts max, retry limit `7` at `500ms` intervals.
- **Multipart**: `64M` size/threshold (reduced from the typical `128M` default to match the 1 GB executor heap), purge enabled after `86400`s (24h), block size `64M`.
- `spark.hadoop.parquet.enable.summary-metadata = false`

#### d) S3A Magic Committer
Configured for Spark 3.5.2 + Hadoop 3.3.4:
- `spark.sql.sources.commitProtocolClass = org.apache.spark.internal.io.cloud.PathOutputCommitProtocol`
- `spark.sql.parquet.output.committer.class = org.apache.hadoop.mapreduce.lib.output.BindingPathOutputCommitter`
- `spark.hadoop.fs.s3a.committer.name = magic`, `committer.magic.enabled = true`
- `committer.staging.conflict-mode = replace`
- `mapreduce.fileoutputcommitter.algorithm.version = 2`

> A code comment notes this **requires versioned MinIO buckets** (`mc versioning enable`) — satisfied by `init-buckets.sh` for `bronze`/`silver`/`gold`/`checkpoint`. If versioning were unavailable, the comment notes the committer would need to fall back to `staging`.

#### e) Extra Classpaths
Confirmed against the compose volume mounts:
- Driver (Jupyter): `/opt/spark/jars-extra/*` ← host `/opt/omnifleet/jars`
- Executor (Worker): `/opt/bitnami/spark/jars-extra/*` ← host `/opt/omnifleet/jars`
- Master: no `jars-extra` mount (runs no user code)

#### f) Serialization & Compression
- `spark.serializer = org.apache.spark.serializer.KryoSerializer`, buffer max `128m`, registration not required.
- Shuffle and spill compression enabled.
- Codec: `lz4` — chosen over `snappy` for lower CPU overhead on constrained hardware.

#### g) Adaptive Query Execution (AQE)
- Enabled, with coalesce-partitions (min size `1MB`, advisory partition size `32MB`).
- Skew-join handling enabled (skew factor `5`, threshold `64MB`).
- `spark.sql.shuffle.partitions = 8` — documented as `executor_cores(2) × 4`, appropriate for this single-worker cluster.
- `spark.shuffle.service.enabled = false` (no external shuffle service — single worker).

#### h) SQL & Parquet
- Parquet compression: `snappy`; `mergeSchema = false`; filter pushdown enabled.
- `spark.sql.sources.partitionOverwriteMode = dynamic`
- Arrow-based PySpark execution enabled (`spark.sql.execution.arrow.pyspark.enabled = true`).
- `spark.sql.legacy.timeParserPolicy = CORRECTED`

#### i) Kafka & Structured Streaming
- Consumer cache: capacity `32`, timeout `5m`.
- `spark.kafka.consumer.fetchedData.maxBytes = 26214400` (25 MB — halved from the 50 MB default to protect the 1 GB executor heap).
- **Checkpoints**: `/opt/spark/checkpoints` (local SSD volume) — explicitly chosen over MinIO for streaming WAL durability/performance, and explicitly **not** Redis.
- `spark.sql.streaming.minBatchesToRetain = 10`
- `spark.streaming.stopGracefullyOnShutdown = true`

#### j) History Server & Event Logging
- Event logging enabled to `file:///tmp/spark-events` (backed by `/data/ssd/spark/events`, shared between worker and Jupyter).
- Rolling event logs, max file size `64m`.
- `spark.history.retainedApplications = 20`

#### k) Redis — Ephemeral Lookup Cache
A clearly documented usage policy is embedded in the config:

```
Config: redis:7-alpine · --maxmemory 200mb · allkeys-lru · no AOF/RDB

✅ Safe uses : vehicle/route lookup cache, rate-limit counters, pub/sub
❌ Never use : streaming checkpoints, dedup store, aggregation state

allkeys-lru will silently evict ANY key under memory pressure.
Always guard reads: if conn.get(key) is None → recompute from Postgres.
```

Settings: `spark.redis.host = redis`, `port = 6379`, no auth (commented out intentionally), timeout `10000`ms, max pipeline size `50`, TTL `3600`s (1 hour).

#### l) Logging & JVM Options
Both driver and executor JVMs are configured with:
- `-Dlog4j2.configurationFile=...` (path differs per role — see Section 10.4)
- `-XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:InitiatingHeapOccupancyPercent=35`
- `-XX:+ExitOnOutOfMemoryError` (fail fast on OOM rather than degrade silently)
- `spark.log.level = ERROR`

#### m) Streaming State Store
- `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` — uses the high-performance RocksDB-based state store instead of the default in-memory one.
- `spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled = true` — incremental changelog updates instead of full state snapshots.

### 10.4 Logging Configuration (`log4j2.properties`)

Shared by the Spark driver (Jupyter) and executor (Spark Worker):

| Setting | Value | Effect |
|---|---|---|
| `status` | `error` | Suppresses Log4j2's own internal status messages |
| `appender.console` | `Console` → `SYSTEM_ERR`, `PatternLayout` (`%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex`) | Standard timestamped console logging |
| `rootLogger.level` | `error` | Only errors are logged — keeps log volume minimal on a constrained host |
| `logger.nativecodeloader` (`org.apache.hadoop.util.NativeCodeLoader`) | `error` | Silences the common (harmless) "could not load native Hadoop library" warning |

Mount paths:
- Driver (Jupyter): `/opt/spark/conf/log4j2.properties`
- Executor (Worker): `/opt/bitnami/spark/conf/log4j2.properties`

---

## 11. Notebook / Development Environment — Jupyter

**Container:** `omnifleet-jupyter` · **Image:** `omnifleet-jupyter:3.11` (custom build) · **Hostname:** `jupyter` · **Profile:** `Processing`, `streaming`

Acts as both the interactive data-science workbench **and** the Spark driver for ad-hoc/notebook-based jobs.

| Setting | Value |
|---|---|
| User | `1001:0` |
| `init` | `true` (PID 1 is tini — ensures proper signal handling and zombie reaping) |
| Depends on | `postgres`, `minio`, `spark-master` (`service_healthy`) |
| `BASE_URL` | `/jupyter/` |
| `JUPYTER_TOKEN` | `${JUPYTER_TOKEN}` (token-based auth, no password) |
| Spark driver settings | `SPARK_DRIVER_MEMORY=800m`, `SPARK_EXECUTOR_INSTANCES=1`, `SPARK_EXECUTOR_CORES=2`, `SPARK_EXECUTOR_MEMORY=1000m`, `SPARK_LOCAL_IP=jupyter`, `SPARK_MASTER=spark://spark-master:7077`, `SPARK_LOCAL_DIRS=/tmp/spark-local` (matches `spark-defaults.conf`) |
| MinIO access | `MINIO_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| Postgres access | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| Kafka access | `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` |
| Schema Registry | `SCHEMA_REGISTRY_URL=https://ubuntu-server.tail712639.ts.net/schema-registry/` (external Caddy URL, rather than the internal `schema-registry:8181` hostname) |
| Host ports | `8888` (Jupyter Lab), `4040-4050` (Spark application UI range — one port per active SparkSession) |
| Healthcheck | `curl -sf http://localhost:8888/jupyter/api` (30s interval, 5 retries, 60s start period) |
| Resources | `mem_limit: 2500m`, `mem_reservation: 1200m`, `cpus: 1.5` |
| Networks | `backend`, `frontend` |
| Key volumes | custom `start-jupyter.sh` (ro), `/opt/omnifleet/notebooks` → `/home/jovyan/work` (persistent workspace), `/opt/omnifleet/jobs` → `/home/jovyan/jobs` (ro, shared with Spark workers), `/opt/omnifleet/jars` → `jars-extra` (ro), checkpoints (SSD), `spark-defaults.conf` / `log4j2.properties` (ro), events (SSD) |

### 11.1 Startup Script (`start-jupyter.sh`)

Launches **JupyterLab** (not the classic notebook UI) with:

```bash
jupyter lab \
    --ip=0.0.0.0 --port=8888 --no-browser \
    --ServerApp.allow_root=false \
    --ServerApp.base_url="${BASE_URL:-/jupyter/}" \
    --ServerApp.allow_origin='*' \
    --ServerApp.allow_remote_access=true \
    --ServerApp.disable_check_xsrf=false \
    --ServerApp.websocket_compression_options='{}' \
    --ServerApp.tornado_settings='{"websocket_ping_interval":30000,"websocket_ping_timeout":120000}' \
    --IdentityProvider.token="${JUPYTER_TOKEN}" \
    --LabApp.default_url=/lab \
    --LabApp.extension_manager=readonly \
    ${JUPYTER_ARGS:-}
```

Key points:
- Runs as non-root (`allow_root=false`).
- `allow_origin='*'` + `allow_remote_access=true` are required for the service to be reachable through the Caddy reverse proxy (different origin from the upstream's perspective).
- **XSRF protection remains enabled** (`disable_check_xsrf=false`) — a deliberate security choice despite the relaxed CORS settings.
- WebSocket compression is disabled (`{}`), and ping interval/timeout (30s / 120s) are tuned to keep long-lived kernel WebSocket connections alive through the proxy.
- `extension_manager=readonly` prevents installing/removing JupyterLab extensions from the UI.
- `JUPYTER_ARGS` allows additional runtime flags to be injected via environment without editing the script.

### 11.2 Python Environment (`requirements_jupyter.txt`)

Installed with `--prefer-binary` (faster installs, avoids source builds):

| Category | Packages |
|---|---|
| Notebook UI | `jupyterlab==4.2.5`, `notebook==7.2.2`, `httpx==0.27.2` |
| Core data tools | `numpy==2.1.1`, `pandas==2.2.3`, `pyarrow==17.0.0` |
| Spark | `pyspark==3.5.2` (version-matched to the cluster's Spark 3.5.2) |
| Plotting | `matplotlib==3.9.2` |
| Database connectivity | `sqlalchemy==2.0.36`, `psycopg[binary]==3.2.3` |
| Parquet/ORC | `fastparquet==2024.11.0` |
| S3/MinIO | `boto3==1.35.63` |
| Kafka & serialization | `confluent-kafka==2.5.0`, `fastavro==1.9.7` |
| Env/secrets | `python-dotenv==1.0.1` |
| Validation | `pydantic==2.9.1` |
| Utilities | `tqdm==4.66.5`, `requests==2.32.3` |

---

## 12. Data Modeling — dbt

**Container:** `omnifleet-dbt` · **Image:** `ghcr.io/dbt-labs/dbt-postgres:1.9.0` · **Profile:** `modeling`

| Setting | Value |
|---|---|
| `working_dir` | `/usr/app` |
| Depends on | `postgres` (`service_healthy`) |
| Environment | `DBT_USER`, `DBT_PASSWORD`, `DBT_HOST=postgres`, `DBT_SCHEMA=analytics` |
| Entrypoint/command | `sleep infinity` — the container stays alive without running anything by default |
| Volumes | `/opt/omnifleet/dbt` → `/usr/app` (project files), `/data/hdd/backup/dbt` → `/usr/app/logs`, `/data/hdd/dbt/target` → `/usr/app/target`, `/data/hdd/dbt/packages` → `/usr/app/dbt_packages` |
| Resources | `mem_limit: 256m`, `mem_reservation: 64m`, `cpus: 0.3` |
| Networks | `backend` |

This container is designed for **interactive use** — an operator runs `docker exec` into it to execute `dbt run`, `dbt test`, `dbt build`, etc. Persisting `target` and `dbt_packages` to HDD avoids re-downloading packages and retains compiled artifacts/run results across restarts.

---

## 13. Analytics & BI Layer

### 13.1 Grafana

**Container:** `omnifleet-grafana` · **Image:** `grafana/grafana:11.1.0` · **Profiles:** `analytics`, `streaming`

| Setting | Value |
|---|---|
| User | `472` (Grafana's standard non-root UID) |
| Depends on | `postgres`, `redis` (`service_healthy`) |
| `GF_SECURITY_ADMIN_USER` / `PASSWORD` | from `.env` |
| `GF_SERVER_ROOT_URL` | from `.env` (expected: `https://ubuntu-server.tail712639.ts.net/grafana/`) |
| `GF_SERVER_SERVE_FROM_SUB_PATH` | `true` (works with the Caddy `/grafana/` prefix) |
| `GF_PANELS_DISABLE_SANITIZE_HTML` | `false` (HTML sanitization in panels remains **enabled** — XSS mitigation) |
| `GF_USERS_ALLOW_SIGN_UP` | `false` |
| `GF_REMOTE_CACHE_TYPE` | `redis` |
| `GF_REMOTE_CACHE_CONNSTR` | `addr=redis:6379,pool_size=100,db=0,ssl=false` |
| Data volume | `/data/hdd/grafana` → `/var/lib/grafana` |
| Provisioning volume | `/opt/omnifleet/grafana/provisioning` → `/etc/grafana/provisioning` (ro) — contains `datasources.yml` (see below) |
| Host port | `3000` |
| Healthcheck | `wget --spider` against `/api/health` (30s interval, 5 retries, 40s start period) |
| Resources | `mem_limit: 256m`, `mem_reservation: 128m`, `cpus: 0.3` |
| Networks | `backend`, `frontend` |

#### Provisioned Datasource (`datasources.yml`)

| Field | Value |
|---|---|
| `apiVersion` | `1` |
| Name | `PostgreSQL (Omnifleet)` |
| Type | `postgres` |
| URL | `postgres:5432` |
| User | `omnifleet` |
| Database | `omnifleet_db` |
| `sslmode` | `disable` (acceptable on the isolated internal `backend` network) |
| `postgresVersion` | `1000` (Grafana's internal version code, meaning "PostgreSQL ≥ 10") |
| `isDefault` | `true` |

> **Observation:** the datasource password is stored as a literal value under `secureJsonData` in this provisioning file (rather than templated from an environment variable as is done elsewhere, e.g. `${PGADMIN_DEFAULT_PASSWORD}`). This is noted here purely as a documentation observation — no changes have been made.

### 13.2 Apache Superset

**Container:** `omnifleet-superset` · **Image:** `omnifleet-superset:4.1.1` (custom build) · **Profile:** `analytics`

The fallback/default application served by Caddy at `/`.

| Setting | Value |
|---|---|
| User | `10001:0` (Superset's standard non-root UID) |
| `init` | `true` (PID 1 is tini — ensures proper signal handling and zombie reaping) |
| Depends on | `postgres` (`service_healthy`) |
| `SUPERSET_WEBSERVER_WORKERS` | `2` |
| Admin account env vars | `SUPERSET_ADMIN_USERNAME`, `PASSWORD`, `FIRSTNAME`, `LASTNAME`, `EMAIL` |
| `SUPERSET_SECRET_KEY` | from `.env` (Flask session/encryption key) |
| `SUPERSET_CONFIG_PATH` | `/app/pythonpath/superset_config.py` |
| `SQLALCHEMY_DATABASE_URI` | `postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}` — Superset's metadata DB lives in the same Postgres instance as the rest of the platform |
| Entrypoint/command | `/bin/sh /app/init-superset.sh` |
| Data volume | `/data/hdd/superset` → `/app/superset_home` |
| Script/config volumes | `init-superset.sh` (ro), `superset_config.py` → `/app/pythonpath/superset_config.py` (ro) |
| Host port | `8088` |
| Healthcheck | `curl -f http://localhost:8088/health` (30s interval, 5 retries, 90s start period) |
| Resources | `mem_limit: 1200m`, `mem_reservation: 512m`, `cpus: 0.8` |
| Networks | `backend`, `frontend` |

#### Startup Script (`init-superset.sh`)

```bash
superset db upgrade                     # Alembic migrations against the metadata DB (idempotent)

superset fab create-admin \
    --username "$SUPERSET_ADMIN_USERNAME" \
    --firstname "$SUPERSET_ADMIN_FIRSTNAME" \
    --lastname "$SUPERSET_ADMIN_LASTNAME" \
    --email "$SUPERSET_ADMIN_EMAIL" \
    --password "$SUPERSET_ADMIN_PASSWORD" \
    || true                             # idempotent: ignores "user already exists"

superset init                           # sets up default roles/permissions

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 2 --threads 2 \
    --timeout 90 \
    --forwarded-allow-ips='*' \
    "superset.app:create_app()"
```

`exec` replaces the shell process so `gunicorn` becomes PID 1. `--forwarded-allow-ips='*'` trusts `X-Forwarded-*` headers from Caddy for correct HTTPS/URL detection behind the proxy.

#### Application Configuration (`superset_config.py`)

| Setting | Value | Purpose |
|---|---|---|
| `SESSION_COOKIE_PATH` | `"/"` | Cookie valid across the whole domain, since Caddy proxies `/superset/*` without stripping the prefix |
| `SESSION_COOKIE_SAMESITE` | `"Lax"` | CSRF mitigation while allowing normal top-level navigation |
| `SESSION_COOKIE_HTTPONLY` | `True` | Session cookie not accessible to JavaScript (XSS mitigation) |
| `WTF_CSRF_SSL_STRICT` | `False` | Relaxes CSRF Referer/origin strictness, paired with `forwarded-allow-ips` |
| `TALISMAN_ENABLED` | `False` | Disables Superset's built-in Flask-Talisman security headers — Caddy applies its own edge security headers instead (Section 6.2), avoiding duplication/conflicts |
| `CONTENT_SECURITY_POLICY_WARNING` | `False` | Suppresses the startup warning tied to Talisman being disabled |
| `ROW_LIMIT` | `5000` | Caps rows returned to charts/SQL Lab — protects memory on this host |
| `SQLLAB_ASYNC_TIME_LIMIT_SEC` | `300` | 5-minute cap on async SQL Lab queries |
| `RATELIMIT_STORAGE_URI` | `redis://redis:6379/0` | Flask-Limiter backend — same Redis instance/DB as Grafana's remote cache, distinct key namespace |

### 13.3 Redis

**Container:** `omnifleet-redis` · **Image:** `redis:7-alpine` · **Profiles:** `streaming`, `analytics`

A shared, **fully ephemeral** in-memory cache used by Spark (lookup cache), Grafana (remote cache), and Superset (rate limiting).

| Setting | Value |
|---|---|
| `restart` | `unless-stopped` |
| Command flags | `--save ""` and `--appendonly no` (both persistence mechanisms disabled by design) |
| `--maxmemory` | `200mb` |
| `--maxmemory-policy` | `allkeys-lru` |
| `--tcp-keepalive` | `60` |
| `--timeout` | `300` (idle connections closed after 5 minutes) |
| Healthcheck | `redis-cli ping` (30s interval, 5s timeout, 3 retries) |
| Resources | `mem_limit: 256m`, `mem_reservation: 128m`, `cpus: 0.2` |
| Networks | `backend` (no host port published — internal only) |

---

## 14. Resource Allocation Summary

| Service | Image | Profile(s) | `mem_limit` | `mem_reservation` | `cpus` | Networks |
|---|---|---|---|---|---|---|
| postgres | postgres:16 | *(core)* | 768m | 384m | 0.8 | backend |
| pgadmin | dpage/pgadmin4:9 | *(core)* | 256m | 128m | 0.2 | backend, frontend |
| minio | quay.io/minio/minio:latest | *(core)* | 384m | 128m | 0.4 | backend, frontend |
| mc-init | quay.io/minio/mc:latest | *(core, one-shot)* | 128m | 32m | 0.1 | backend |
| caddy | caddy:2-alpine | *(core)* | 64m | 32m | 0.1 | frontend, backend |
| kafka | confluentinc/cp-kafka:8.1.0 | streaming | 900m | 512m | 0.8 | backend |
| schema-registry | confluentinc/cp-schema-registry:8.1.0 | streaming | 450m | 256m | 0.3 | backend |
| kafka-ui | provectuslabs/kafka-ui:latest | streaming | 256m | 128m | 0.2 | backend, frontend |
| spark-master | bitnami/spark:3.5.2 | Processing, streaming | 512m | 256m | 0.3 | backend |
| spark-worker | bitnami/spark:3.5.2 | Processing, streaming | 2300m | 1300m | 2.0 | backend |
| jupyter | omnifleet-jupyter:3.11 | Processing, streaming | 2500m | 1200m | 1.5 | backend, frontend |
| dbt | ghcr.io/dbt-labs/dbt-postgres:1.9.0 | modeling | 256m | 64m | 0.3 | backend |
| grafana | grafana/grafana:11.1.0 | analytics, streaming | 256m | 128m | 0.3 | backend, frontend |
| superset | omnifleet-superset:4.1.1 | analytics | 1200m | 512m | 0.8 | backend, frontend |
| redis | redis:7-alpine | streaming, analytics | 256m | 128m | 0.2 | backend |

**Total `mem_limit` if every service ran simultaneously:** ~10.2 GB — confirming that **profiles are essential** to operating within the ~9.6 GB host budget (see Section 5.1 for realistic combinations).

---

## 15. Volume & Storage Layout

| Path | Tier | Used by |
|---|---|---|
| `/data/ssd/postgres` | SSD | PostgreSQL data directory |
| `/data/ssd/kafka` | SSD | Kafka log segments |
| `/data/ssd/spark/checkpoints` | SSD | Spark Structured Streaming checkpoints (master, worker, jupyter) |
| `/data/ssd/spark/tmp` | SSD | Spark master/worker scratch (`SPARK_LOCAL_DIRS`) |
| `/data/ssd/spark/work` | SSD | Spark worker work directory |
| `/data/ssd/spark/events` | SSD | Spark event logs (master, worker, jupyter) |
| `/data/hdd/minio` | HDD | MinIO object storage (data lake) |
| `/data/hdd/backup/postgres` | HDD | PostgreSQL backups |
| `/data/hdd/backup/dbt` | HDD | dbt run logs |
| `/data/hdd/pgadmin` | HDD | pgAdmin data + logs |
| `/data/hdd/caddy/data`, `/data/hdd/caddy/config` | HDD | Caddy runtime state |
| `/data/hdd/grafana` | HDD | Grafana data (dashboards, DB if applicable) |
| `/data/hdd/superset` | HDD | Superset home (uploads, cache) |
| `/data/hdd/dbt/target`, `/data/hdd/dbt/packages` | HDD | dbt compiled artifacts and downloaded packages |
| `/opt/omnifleet/...` | *(config tree)* | All read-only configuration, init scripts, job code, JARs, notebooks |

**Pattern:** latency-sensitive, write-heavy, or streaming-state data goes on **SSD**; bulk object storage, backups, and application metadata go on **HDD**. All operational configuration lives under `/opt/omnifleet/` and is mounted read-only (`:ro`) into containers wherever it is not meant to be modified at runtime.

---

## 16. Environment Variables Reference

All services use `env_file: .env` (a root `.env` file, not included in this review). Variables referenced across the stack:

| Variable | Used by |
|---|---|
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | postgres, jupyter, dbt (via host), superset |
| `POSTGRES_HOST`, `POSTGRES_PORT` | jupyter |
| `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` | pgadmin |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | minio, mc-init, jupyter |
| `MINIO_ENDPOINT` | jupyter |
| `RAW_BUCKET`, `BRONZE_BUCKET`, `SILVER_BUCKET`, `GOLD_BUCKET`, `CHECKPOINT_BUCKET` | mc-init |
| `KAFKA_CLUSTER_ID` | kafka |
| `KAFKA_CLUSTER_NAME` | kafka-ui |
| `JUPYTER_TOKEN` | jupyter |
| `DBT_USER`, `DBT_PASSWORD` | dbt |
| `GF_SECURITY_ADMIN_USER`, `GF_SECURITY_ADMIN_PASSWORD`, `GF_SERVER_ROOT_URL` | grafana |
| `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`, `SUPERSET_ADMIN_FIRSTNAME`, `SUPERSET_ADMIN_LASTNAME`, `SUPERSET_ADMIN_EMAIL` | superset |
| `SUPERSET_SECRET_KEY` | superset |

---

## 17. Database Schema Initialization (`001-schemas.sql`)

Executed automatically on first PostgreSQL container start via `docker-entrypoint-initdb.d` (the `001-` numeric prefix indicates this is the first of a potential ordered series of init scripts):

```sql
CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS metadata;
```

| Schema | Likely role in the architecture |
|---|---|
| `stream` | Landing tables for data written by Spark Structured Streaming jobs |
| `warehouse` | Modeled/star-schema tables built by dbt (consumed by Superset & Grafana) |
| `metadata` | Pipeline/catalog/audit metadata |

(dbt's container additionally targets a `DBT_SCHEMA=analytics`, which is configured at the dbt-profile level rather than created by this init script.)

---

## 18. Security Hardening Summary

| Mechanism | Applied to |
|---|---|
| `security_opt: no-new-privileges:true` | All hardened services (everything except `mc-init` and `redis`, which inherit defaults) |
| `cap_drop: ALL` | `minio` (most restrictive — also runs as non-root `1000:1000`) |
| `cap_drop: [SYS_ADMIN, SYS_PTRACE]` | postgres, kafka, schema-registry, kafka-ui, spark-master, spark-worker, jupyter, dbt, grafana, superset |
| Non-root execution | minio (`1000:1000`), spark-master/worker/jupyter (`1001:0`), superset (`10001:0`), grafana (`472`) |
| Read-only mounts (`:ro`) | All configuration files, init scripts, job code, extra JARs, Caddy certs/Caddyfile |
| Per-service `mem_limit` / `cpus` | Every service — prevents any single container from exhausting host resources |
| Healthchecks + `condition: service_healthy` | Used throughout `depends_on` chains to enforce correct startup ordering (e.g., `mc-init` waits for `minio`; `superset`/`jupyter`/`spark-master` wait for `postgres`) |
| Bounded logging (`json-file`, `max-size: 10m`, `max-file: 3`) | All services using the shared `default-logging` template |
| TLS termination at the edge | Caddy, using a certificate/key issued for the Tailscale tailnet hostname — the platform is reachable only over the private Tailscale network, not the public internet |
| XSRF/CSRF protections retained | Jupyter (`disable_check_xsrf=false`), Superset (`SESSION_COOKIE_SAMESITE=Lax`, `HTTPONLY=True`) |

### Observations (informational only — no changes made)

- `datasources.yml` (Grafana's Postgres datasource provisioning) and `spark-defaults.conf` (S3A access/secret key) both contain **literal credential values** rather than environment-variable placeholders, unlike most other services which consume `${...}` variables from `.env`. This is noted purely for awareness during future credential rotation.
- A large number of services publish ports directly to the host (e.g., `5432`, `9092`, `7077`/`8080`/`8081`, `9000`/`9001`, `8181`, `8090`, `8888` + `4040-4050`, `3000`, `8088`, `5050`) in addition to being reverse-proxied by Caddy. Since the host is only reachable via Tailscale, this appears to be an intentional design choice to allow both direct tool access (e.g., `psql`, Spark clients, S3 SDKs) and browser access via the unified hostname.

---

## 19. Service Access Map

All paths below are relative to `https://ubuntu-server.tail712639.ts.net/`.

| Service | Path (via Caddy) | Direct host port |
|---|---|---|
| Superset (default landing page) | `/` and `/superset/` | `8088` |
| Health check | `/health` | — |
| Grafana | `/grafana/` | `3000` |
| Jupyter Lab | `/jupyter/` | `8888` (+ `4040-4050` for Spark UIs) |
| MinIO Console | `/minio/` | `9001` |
| MinIO S3 API | `/s3` | `9000` |
| pgAdmin | `/pgadmin/` | `5050` |
| Kafka UI | `/kafka/` | `8090` |
| Schema Registry | `/schema-registry/` | `8181` |
| Spark Master UI | `/spark-master/` | `8080` |
| Spark Worker UI | `/spark-worker/` | `8081` |
| Portainer *(external service)* | `/portainer` → `:9443` | `9443` |
| PostgreSQL | — | `5432` |
| Kafka (external listener) | — | `9092` |
| Redis | — | *(internal only — no host port)* |

---

## 20. Configuration File Index

| File | Mounted to | Role |
|---|---|---|
| `Caddyfile` | `caddy:/etc/caddy/Caddyfile` | Reverse proxy routing, TLS, security headers |
| `datasources.yml` | `grafana:/etc/grafana/provisioning/...` | Auto-provisions the Postgres datasource in Grafana |
| `init-buckets.sh` | `mc-init:/scripts/init-buckets.sh` | Creates and versions the MinIO data-lake buckets |
| `001-schemas.sql` | `postgres:/docker-entrypoint-initdb.d/001-schemas.sql` | Creates `stream`, `warehouse`, `metadata` schemas on first boot |
| `init-superset.sh` | `superset:/app/init-superset.sh` | Superset migration, admin creation, and gunicorn launch |
| `superset_config.py` | `superset:/app/pythonpath/superset_config.py` | Superset application configuration overrides |
| `requirements_jupyter.txt` | *(build-time, custom `omnifleet-jupyter` image)* | Python dependency manifest for the notebook environment |
| `start-jupyter.sh` | `jupyter:/usr/local/bin/start-jupyter.sh` | JupyterLab launch command and proxy-friendly settings |
| `spark-defaults.conf` | `spark-master` / `spark-worker` / `jupyter` (role-specific paths) | Cluster-wide Spark configuration (S3A, AQE, Kafka, Redis, RocksDB state store, etc.) |
| `log4j2.properties` | `spark-master` / `spark-worker` / `jupyter` (role-specific paths) | Spark driver/executor logging configuration |
| `docker-compose.yml` | *(orchestration root)* | Defines all services, networks, volumes, resource limits, and profiles |

---

*End of document. This documentation reflects the configuration as updated per the revised `docker-compose.yml`. No infrastructure files were modified — this is a documentation-only update.*
