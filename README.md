# Real-Time CDC Data Lakehouse

A production-grade streaming data pipeline running entirely on a local Kubernetes cluster. Captures change events from PostgreSQL and MongoDB in real time via Debezium, routes them through Kafka into a ClickHouse analytics store (silver layer), and orchestrates a daily aggregation job in Airflow (gold layer).

```
PostgreSQL ──┐
              ├──► Debezium ──► Kafka ──► ClickHouse silver ──► Airflow ──► gold_user_activity
MongoDB    ──┘
```

**Zero prior setup required.** Run `./startup.sh` on a bare machine — it installs Docker, kubectl, Helm, and Kind automatically, then deploys the full pipeline end-to-end.

---

## Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [startup.sh — Intelligent Deployment](#startupsh--intelligent-deployment)
- [cleanup.sh — Smart Teardown](#cleanupsh--smart-teardown)
- [Step Reference](#step-reference)
- [Accessing Services](#accessing-services)
- [Data Model](#data-model)
- [Gold Layer DAG](#gold-layer-dag)
- [Verifying the Pipeline](#verifying-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)
- [Resource Requirements](#resource-requirements)

---

## Architecture

| Layer | Technology | Namespace | Purpose |
|-------|-----------|-----------|---------|
| Host tools | Docker, kubectl, Helm, Kind | host OS | Installed by step 1 |
| Cluster | Kind 1.29 | — | Local K8s (1 control-plane + 1 worker) |
| Source 1 | PostgreSQL 15 | `default` | Users table — system of record |
| Source 2 | MongoDB 6.0 | `default` | Events collection (CDC via change streams) |
| CDC | Debezium 2.7 | `kafka` | Captures INSERT / UPDATE / DELETE |
| Streaming | Kafka (Strimzi KRaft) | `kafka` | Central event bus |
| Coordination | ClickHouseKeeper (CHK) | `clickhouse-operator` | ZooKeeper-compatible coordination |
| Analytics DB | ClickHouse 24.3 (Altinity CHI) | `clickhouse-operator` | Silver + Gold layers |
| Orchestration | Airflow 2.9 | `airflow` | Daily gold aggregation DAG |

### Data Flow

```
[PostgreSQL users]
        │ logical replication (pgoutput)
        ▼
[Debezium postgres-connector]  ──errors──► postgres.public.users.dlq
        │ ExtractNewRecordState (unwrapped CDC)
        ▼
[Kafka: postgres.public.users]
        │ Kafka engine consumer
        ▼
[ClickHouse: silver_users]   ← ReplacingMergeTree(updated_at), UTC
                                soft-deletes: is_deleted=1

[MongoDB commerce.events]
        │ change streams (replica set rs0)
        ▼
[Debezium mongodb-connector]  ──errors──► mongo.commerce.events.dlq
        ▼
[ClickHouse: silver_events]  ← ReplacingMergeTree(created_at), UTC

        │ Airflow @daily · catchup=True · max_active_runs=1
        ▼
[ClickHouse: gold_user_activity]
  check_silver → create_table → delete → insert (LEFT JOIN, UTC) → verify → optimize
```

---

## Quick Start

```bash
git clone <your-repo-url> cdc-lakehouse
cd cdc-lakehouse
chmod +x startup.sh cleanup.sh

./startup.sh          # Full deploy: installs tools + deploys pipeline (step 1 → 15)
```

That's it. The script handles everything — tool installation, cluster creation, all 15 deployment steps — detecting and skipping any step that is already complete.

**Estimated time:**
- First run on a bare machine: **50–70 minutes** (tool install + image pulls)
- Subsequent runs (tools + cluster already present): **5–15 minutes**
- After a light cleanup: **5–10 minutes** (images cached, PVCs intact)

---

## startup.sh — Intelligent Deployment

`startup.sh` is the single entry point for the entire pipeline. Before executing each step, it queries the live system state (tool presence, Kubernetes pod status, API responses) to determine whether that step is already complete. Completed steps are silently skipped — running `./startup.sh` twice on a fully-deployed system is a no-op.

### Usage

```bash
./startup.sh [OPTIONS]
```

| Option | Description |
|--------|-------------|
| *(none)* | Full deploy from step 1 — installs tools, runs pre-flight, deploys pipeline |
| `--from STEP` | Resume from a specific step, skipping everything before it |
| `--only STEP` | Run exactly one step and exit |
| `--dry-run` | Show what would run without executing anything |
| `--yes` | Non-interactive mode — no confirmation prompts (CI/CD) |
| `--retries N` | Max automatic retries per step (default: 3) |
| `--help` | Print usage |

### Examples

```bash
# Complete deploy from scratch — installs tools, checks system, deploys all 15 steps
./startup.sh

# If tools are already installed, skip steps 1–2 and start from the Kind cluster
./startup.sh --from 3

# Resume from Kafka after a partial failure (skips steps 1–8 automatically)
./startup.sh --from 9

# Re-run only the Debezium connector configuration
./startup.sh --only 11

# Preview what would run without making any changes
./startup.sh --dry-run

# Fully automated, no prompts — ideal for CI/CD pipelines
./startup.sh --yes

# Automated full deploy from a bare machine
./startup.sh --yes --from 1
```

### Step numbers

| Step | Component | What it does |
|------|-----------|-------------|
| **1** | **Install Prerequisites** | Installs Docker, kubectl, Helm, Kind, curl, python3 on macOS and Linux |
| **2** | **System Pre-flight Checks** | Verifies RAM, disk, ports, Docker memory, internet connectivity |
| 3 | Create Kind Cluster | 2-node cluster with NodePort mappings |
| 4 | Install Strimzi Operator | KRaft-mode Kafka operator + Lease RBAC fix |
| 5 | Install ClickHouse Operator | Altinity operator |
| 6 | Deploy PostgreSQL | Logical replication + WAL bloat protection |
| 7 | Deploy MongoDB | Replica set for CDC change streams |
| 8 | Populate Sample Data | 10 users + 26 events with all CDC operation types |
| 9 | Deploy Kafka (KRaft) | Single-node broker + CDC topics + DLQ topics |
| 10 | Deploy Kafka Connect | Debezium Connect as a plain Deployment |
| 11 | Configure Debezium | PG + Mongo connectors with DLQ routing |
| 12 | Deploy ClickHouseKeeper + ClickHouse | CHK + CHI |
| 13 | Create Silver Layer | Kafka engine tables + ReplacingMergeTree + MVs |
| 14 | Deploy Airflow + Gold DAG | Helm install + 5-task gold aggregation DAG |
| 15 | End-to-End Validation | Silver health, lag check, DAG trigger, CDC smoke test |

### How step detection works

| Step | Live check performed |
|------|---------------------|
| 1 | All 6 CLI tools exist AND Docker daemon responds |
| 2 | Docker running + available RAM ≥ 6 GB + disk ≥ 8 GB |
| 3 | `kind get clusters` contains `cdc-lakehouse`; `kubectl cluster-info` responds |
| 4 | `strimzi-cluster-operator` has 1 ready replica; leader lease exists |
| 5 | `clickhouse-operator` has 1 ready replica |
| 6 | PostgreSQL pod Running; `pg_isready` responds |
| 7 | MongoDB pod Running; `rs.status().myState == 1` (PRIMARY) |
| 8 | `SELECT count(*) FROM users` returns > 0 |
| 9 | Kafka CR condition `Ready=True` |
| 10 | Debezium Connect pod Running; REST API responds on port 8083 |
| 11 | Both connector tasks report `RUNNING` via Connect REST API |
| 12 | CHI status=`Completed`; CHK responds `imok` |
| 13 | All 6 commerce tables exist; `silver_users` has rows |
| 14 | Airflow webserver Running; `gold_user_activity` table exists |
| 15 | `gold_user_activity` has at least one row |

### Failure handling

When a step fails after all automatic retries, an interactive menu appears:

```
╔══ Step 9 FAILED — Deploy Kafka (KRaft) ══╗
  Failed after 3 automatic retry attempts.

  1) Retry this step (fresh attempts)
  2) Skip and continue  (not recommended — may break later steps)
  3) Abort and clean up all resources deployed so far
  4) Abort and keep current state (for manual debugging)
```

In `--yes` mode, the deployment aborts immediately on unrecoverable failure.

---

## cleanup.sh — Smart Teardown

`cleanup.sh` tears down the pipeline with two modes designed for different workflows. Every deletion is guarded — the script checks whether each resource exists before deleting it, so running cleanup on a partial environment produces no spurious errors.

### Modes

**`light`** — Removes all Kubernetes workloads while preserving:
- The Kind cluster (avoids cluster creation time on next deploy)
- All Persistent Volume Claims (data survives — no re-population needed)
- CRDs and ClusterRoles (operator reinstall is instant)

Best for: iterating on configuration, fixing a broken service, fast iteration.  
**Redeploy after light cleanup: ~5–10 minutes.**

**`full`** — Destroys everything: all workloads, namespaces, PVCs, CRDs, ClusterRoles, and the Kind cluster. Returns machine to a completely clean state.

Best for: finishing a demo, resolving deep state inconsistency, switching branches.  
**Redeploy after full cleanup: ~50–70 minutes (fresh image pull + cluster init).**

> **Note on steps 1–2:** Installed system tools (Docker, kubectl, Helm, Kind) are **never removed automatically**. Removing system-level tools without explicit user intent is dangerous. To uninstall tools, use your package manager manually.

### Usage

```bash
./cleanup.sh [OPTIONS]
```

| Option | Description |
|--------|-------------|
| *(none)* | Interactive menu — choose mode with confirmation |
| `--mode light` | Remove services, keep cluster and PVCs |
| `--mode full` | Destroy everything |
| `--up-to-step N` | Only clean components from step N downward |
| `--yes` | Skip confirmation prompts |
| `--help` | Print usage |

### Examples

```bash
# Interactive menu (recommended for first use)
./cleanup.sh

# Remove all K8s services, keep the Kind cluster for fast redeploy
./cleanup.sh --mode light

# Full teardown, no prompts
./cleanup.sh --mode full --yes

# Remove only Airflow and ClickHouse — keep Kafka, PostgreSQL, MongoDB
./cleanup.sh --mode light --up-to-step 12 --yes

# Tear down from Kafka downward, deleting PVCs too
./cleanup.sh --mode full --up-to-step 9

# Targeted: remove only Airflow, keep everything else
./cleanup.sh --mode light --up-to-step 14 --yes
```

### Light vs Full comparison

| Resource | `--mode light` | `--mode full` |
|----------|---------------|--------------|
| Kind cluster | ✔ Preserved | ✗ Deleted |
| Docker image cache (in Kind node) | ✔ Preserved | ✗ Deleted with cluster |
| PostgreSQL PVC | ✔ Preserved | ✗ Deleted |
| MongoDB PVC | ✔ Preserved | ✗ Deleted |
| Kafka PVCs | ✔ Preserved | ✗ Deleted |
| ClickHouse PVCs | ✔ Preserved | ✗ Deleted |
| CRDs + ClusterRoles | ✔ Preserved | ✗ Deleted |
| Namespaces (kafka, airflow, clickhouse-operator) | ✗ Deleted | ✗ Deleted |
| System tools (Docker, kubectl, helm, kind) | ✔ Never removed | ✔ Never removed |
| **Redeploy time** | ~5–10 min | ~50–70 min |

---

## Step Reference

### Step 1 — Install Prerequisites (`step-01-install-prerequisites.sh`)

Installs all required CLI tools on a fresh machine. Safe to re-run — each tool is checked before attempting installation.

**Supported platforms:**

| OS | Docker | kubectl | Helm | Kind |
|----|--------|---------|------|------|
| macOS | Docker Desktop (Homebrew Cask) | Homebrew | Homebrew | Homebrew |
| Ubuntu / Debian | Docker Engine (official APT repo) | Kubernetes APT repo | Official installer | Binary download |
| RHEL / Fedora / CentOS | Docker Engine (official DNF repo) | Kubernetes YUM repo | Official installer | Binary download |
| Arch Linux | Community packages | Community packages | Official installer | Binary download |

Also installs: `curl`, `python3`, `nc` (netcat, used for ClickHouseKeeper health checks).  
Sets up shell completions for kubectl, helm, and kind on macOS.

**Expected output (clean machine):**
```
[OK]    Docker Engine installed and started.
[OK]    kubectl installed: v1.29.0
[OK]    Helm installed: v3.14.0
[OK]    Kind installed: v0.22.0
✔ Step 1 Complete — All prerequisites installed
```

**Expected output (tools already present):**
```
[SKIP]  Docker (26.1) — already installed
[SKIP]  kubectl (v1.29.0) — already installed
[SKIP]  Helm (v3.14.0) — already installed
[SKIP]  Kind (v0.22.0) — already installed
✔ Step 1 Complete — All prerequisites installed
```

---

### Step 2 — System Pre-flight Checks (`step-02-preflight-checks.sh`)

Verifies the machine is ready before any Kubernetes resources are created. Catches problems early rather than mid-deploy.

**Checks performed:**

| # | Check | Pass condition | Warning condition | Fail condition |
|---|-------|---------------|------------------|----------------|
| 1 | Required tools | All 6 present | — | Any missing |
| 2 | Docker daemon | Running and responding | — | Not running |
| 3 | Available RAM | ≥ 10 GB | 8–10 GB | < 8 GB |
| 4 | Available disk | ≥ 20 GB | 10–20 GB | < 10 GB |
| 5 | Docker memory allocation | ≥ 10 GB (macOS: Docker Desktop settings) | < 10 GB | — |
| 6 | Required ports free | All 8 ports available | Any port in use | — |
| 7 | Kind cluster state | No cluster, or existing + reachable | Existing but unreachable | — |
| 8 | Internet connectivity | All 4 container registries reachable | Any registry unreachable | — |

**Ports checked:** 5432 (PG), 27017 (Mongo), 9092 (Kafka), 8083 (Connect), 8123 (CH HTTP), 9000 (CH Native), 8080 (Airflow), 9181 (CHK).

**Step 2 is non-blocking for warnings** — warnings indicate degraded performance risk but do not stop deployment. Only hard errors (insufficient RAM/disk, Docker not running, missing tools) will cause step 2 to fail.

---

### Step 3 — Create Kind Cluster
Creates a 2-node Kind cluster (`cdc-lakehouse`) with NodePort mappings for all services.

---

### Step 4 — Install Strimzi Operator
Installs Strimzi 0.41.0. Applies a Lease RBAC fix not present in the default manifest (resolves a CrashLoopBackOff on leader election).

---

### Step 5 — ClickHouse Operator
Installs the Altinity ClickHouse operator into `clickhouse-operator` namespace.

---

### Step 6 — Deploy PostgreSQL
PostgreSQL 15-alpine with `max_slot_wal_keep_size=1073741824` (1 GB WAL cap — prevents disk exhaustion when Debezium is offline) and `wal_sender_timeout=0`. Credentials in Kubernetes Secret `pg-credentials`.

---

### Step 7 — Deploy MongoDB
MongoDB 6.0 as a single-node replica set (`rs0`). Uses TCP socket readiness probes (not mongosh exec) to avoid Node.js startup timeouts.

---

### Step 8 — Populate Sample Data
10 PostgreSQL users with INSERT / UPDATE / DELETE operations. 26 MongoDB events including events for the deleted user (user_id=10) to test orphan handling.

---

### Step 9 — Deploy Kafka (KRaft)
Single-node Strimzi KRaft broker. Creates CDC topics and DLQ topics (`*.dlq`) for malformed message routing.

---

### Step 10 — Deploy Kafka Connect
Debezium Connect 2.7 as a plain Kubernetes `Deployment` (not a Strimzi `KafkaConnect` CR).

**Why not use the Strimzi `KafkaConnect` CR?** Strimzi injects an init container that copies scripts to `/opt/kafka/` but the Debezium image stores them at `/kafka/` — a path mismatch that causes an immediate crash. A plain `Deployment` avoids this while retaining the full Connect REST API on port 8083.

---

### Step 11 — Configure Debezium Connectors
Registers PostgreSQL and MongoDB connectors with Dead-Letter Queue routing:

| Setting | Value | Purpose |
|---------|-------|---------|
| `errors.tolerance` | `all` | Process all messages, route errors to DLQ |
| `errors.deadletterqueue.topic.name` | `*.dlq` | Persistent audit trail |
| `errors.log.include.messages` | `true` | Full message context with each error |
| `slot.drop.on.stop` | `false` | Preserve PG replication slot on restart |
| `heartbeat.interval.ms` | `10000` | Keep WAL active during quiet periods |

---

### Step 12 — Deploy ClickHouseKeeper + ClickHouse
1. **ClickHouseKeeper (CHK)** — single-replica ZooKeeper-compatible coordination using the embedded keeper mode in the ClickHouse binary.
2. **ClickHouseInstallation (CHI)** — single-shard, single-replica ClickHouse cluster pointed at CHK. Credentials in Secret `ch-credentials`.

---

### Step 13 — Create Silver Layer

| Table | Engine | Purpose |
|-------|--------|---------|
| `kafka_users_queue` | Kafka | Consumes `postgres.public.users` |
| `silver_users` | ReplacingMergeTree(updated_at) | Latest user state; soft-deletes |
| `mv_users_from_kafka` | Materialized View | Routes Kafka → silver_users |
| `kafka_events_queue` | Kafka | Consumes `mongo.commerce.events` |
| `silver_events` | ReplacingMergeTree(created_at) | Event history; soft-deletes |
| `mv_events_from_kafka` | Materialized View | Routes Kafka → silver_events |

All datetime handling uses explicit `'UTC'` timezone. Kafka engine tables have no DEFAULT expressions (ClickHouse Code 36 restriction) — defaults live in the Materialized Views.

---

### Step 14 — Deploy Airflow + Gold DAG
Airflow 2.9.3 via Helm (LocalExecutor). Creates the canonical `gold_user_activity` table and packages the DAG into a Kubernetes ConfigMap by reading `dags/gold_user_activity_dag.py` directly — no heredoc embedded in the shell script. If the DAG file is missing, step 14 exits with a clear error pointing to the expected path.

---

### Step 15 — End-to-End Validation
Full pipeline health check: silver row counts, consumer group lag, DLQ inspection, Airflow DAG trigger, gold layer validation, live CDC smoke test (insert PG row → verify propagation to ClickHouse within 30s → clean up).

---

## Accessing Services

| Service | URL / Host | Credentials |
|---------|-----------|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | admin / admin |
| ClickHouse Native | localhost:9000 | admin / admin |
| Kafka | localhost:9092 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| PostgreSQL | localhost:5432 | postgres / postgres |
| MongoDB | localhost:27017 | — |

```bash
# ClickHouse CLI
clickhouse-client --host localhost --port 9000 --user admin --password admin

# PostgreSQL
psql -h localhost -U postgres -d commerce

# MongoDB
mongosh "mongodb://localhost:27017/commerce?replicaSet=rs0"

# Debezium connector status
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

---

## Data Model

### Source: PostgreSQL `users`

| Column | Type |
|--------|------|
| user_id | SERIAL PK |
| full_name | VARCHAR(255) |
| email | VARCHAR(255) UNIQUE |
| created_at | TIMESTAMPTZ |
| updated_at | TIMESTAMPTZ |

### Source: MongoDB `commerce.events`

| Field | Type |
|-------|------|
| _id | ObjectId |
| user_id | Int |
| event_type | String (`login` `page_view` `add_to_cart` `purchase` `remove_from_cart`) |
| page | String (nullable) |
| metadata | Object |
| created_at | ISODate (UTC) |

### ClickHouse Silver: `silver_users`

| Column | Type | Notes |
|--------|------|-------|
| user_id | Int32 | Ordering key |
| full_name | String | — |
| email | String | — |
| created_at | DateTime64(3, 'UTC') | — |
| updated_at | DateTime64(3, 'UTC') | ReplacingMergeTree version |
| is_deleted | UInt8 | 1 = soft-deleted |
| _ingested_at | DateTime | Kafka consumption time |

### ClickHouse Silver: `silver_events`

| Column | Type | Notes |
|--------|------|-------|
| _id | String | MongoDB document ID |
| user_id | Int32 | — |
| event_type | String | — |
| page | Nullable(String) | — |
| metadata | String | JSON |
| created_at | DateTime64(3, 'UTC') | ReplacingMergeTree version |
| is_deleted | UInt8 | 1 = soft-deleted |
| _ingested_at | DateTime | — |

### ClickHouse Gold: `gold_user_activity`

| Column | Type | Notes |
|--------|------|-------|
| activity_date | Date | Calendar day processed |
| user_id | Int32 | — |
| full_name | String | Empty for orphan events |
| email | String | Empty for orphan events |
| total_events | UInt64 | Non-deleted events on activity_date |
| last_event_time | Nullable(DateTime64(3, 'UTC')) | NULL when total_events = 0 |
| _processed_at | DateTime | Wall-clock time of DAG run |

---

## Gold Layer DAG

**ID:** `gold_user_activity` | **Schedule:** `@daily` (midnight UTC) | **Catchup:** `True` | **Max active runs:** `1`

### DAG source file

The DAG lives as a standalone Python file at `dags/gold_user_activity_dag.py`. It is completely decoupled from the deployment shell scripts — edit it directly whenever you need to change query logic, the schedule, retry settings, or task structure:

```
dags/
└── gold_user_activity_dag.py    ← edit this file directly
```

Step 14 (`scripts/step-14-deploy-airflow.sh`) reads this file at deploy time and packages it into a Kubernetes ConfigMap that is mounted into Airflow's webserver and scheduler pods. No heredoc, no copy to `/tmp` — the script simply does:

```bash
kubectl create configmap airflow-dags -n airflow \
    --from-file=gold_user_activity_dag.py=dags/gold_user_activity_dag.py
```

**To update the DAG after deployment:**

```bash
# 1. Edit the DAG file
vim dags/gold_user_activity_dag.py

# 2. Re-package it into the ConfigMap (Airflow picks it up within ~30s)
kubectl delete configmap airflow-dags -n airflow
kubectl create configmap airflow-dags -n airflow \
    --from-file=gold_user_activity_dag.py=dags/gold_user_activity_dag.py

# 3. Or simply re-run step 14 (idempotent — skips Helm if Airflow is already running)
./startup.sh --only 14
```

### Task pipeline

```
check_silver_data           ← fail-fast if silver tables empty (CDC stall detection)
        │
create_gold_table           ← idempotent DDL (IF NOT EXISTS)
        │
delete_existing_partition   ← DELETE WHERE activity_date=ds  (primary idempotency)
        │
insert_gold_activity        ← LEFT JOIN silver FINAL, UTC-safe, NULL last_event_time
        │
verify_gold_output          ← pretty-print per-user results to task log
        │
optimize_silver_tables      ← OPTIMIZE FINAL (prevents FINAL query degradation)
```

---

## Verifying the Pipeline

```bash
CH_POD=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
    -n clickhouse-operator -o jsonpath='{.items[0].metadata.name}')

# Silver layer counts
kubectl exec ${CH_POD} -n clickhouse-operator -- \
  clickhouse-client --user admin --password admin --query "
  SELECT 'silver_users' AS tbl, count() AS total, countIf(is_deleted=1) AS deleted
  FROM commerce.silver_users
  UNION ALL
  SELECT 'silver_events', count(), countIf(is_deleted=1)
  FROM commerce.silver_events;"

# Specific CDC operations (alice update, bob rename, jack delete)
kubectl exec ${CH_POD} -n clickhouse-operator -- \
  clickhouse-client --user admin --password admin --query "
  SELECT user_id, full_name, email, is_deleted
  FROM commerce.silver_users FINAL WHERE user_id IN (1,2,10) ORDER BY user_id;"

# Consumer group lag
kubectl exec kafka-cluster-combined-0 -n kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group clickhouse_users_consumer --describe
```

---

## Troubleshooting

### Tools not installing (step 1)

**macOS — Docker Desktop requires manual permission grants:**  
After `brew install --cask docker`, Docker Desktop opens and shows a permissions dialog. Grant the requested permissions, then re-run: `./startup.sh --from 1`

**Linux — no sudo access:**  
Contact your system administrator or run as root: `sudo ./scripts/step-01-install-prerequisites.sh`

**Behind a corporate proxy:**  
Export proxy variables before running: `export HTTP_PROXY=http://proxy:port HTTPS_PROXY=http://proxy:port NO_PROXY=localhost,127.0.0.1`

---

### Step 2 fails with "insufficient RAM"

The minimum is 8 GB free RAM. Options:
- Close other applications to free RAM.
- On macOS: increase Docker Desktop memory allocation (Settings → Resources → Memory ≥ 10 GB).
- Reduce Airflow or ClickHouse memory limits in the step scripts and re-run.

---

### Silver tables empty after step 13

The Kafka consumer committed offsets before the Materialized View was attached:

```bash
./scripts/step-15-e2e-validate.sh
# Part 2 auto-resets consumer offsets when empty tables are detected.
```

---

### Debezium connector FAILED state

```bash
CONNECT_POD=$(kubectl get pod -l app=debezium-connect -n kafka \
    -o jsonpath='{.items[0].metadata.name}')
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
# Restart:
curl -s -X POST http://localhost:8083/connectors/postgres-connector/tasks/0/restart
```

---

### Strimzi operator CrashLoopBackOff

Step 4 applies the Lease RBAC fix, but if you see it crashing:
```bash
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=30 | grep -i leader
./startup.sh --only 4   # re-run just the operator install
```

---

### Out of memory / pods evicted

```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Light cleanup, then redeploy from operators (skips image pull, cluster init)
./cleanup.sh --mode light --yes
./startup.sh --from 4 --yes
```

---

## Security Notes

Configured for **local development only**. Do not use in production without addressing:

| Area | Local | Production |
|------|-------|-----------|
| Passwords | Hardcoded | Vault / AWS Secrets Manager |
| ClickHouse network | Open to all | Restrict to pod CIDRs |
| Airflow `EXPOSE_CONFIG` | True | Disabled |
| Debezium credentials | Connector JSON | Kafka Connect Secret Provider |
| TLS | None | Enable on Kafka + ClickHouse HTTP |

Passwords are stored in Kubernetes Secrets (`pg-credentials`, `ch-credentials`) from step 6 and 12 onwards — pod env vars reference these Secrets.

---

## Resource Requirements

| Service | Memory request | Memory limit |
|---------|---------------|-------------|
| PostgreSQL | 256 Mi | 512 Mi |
| MongoDB | 256 Mi | 512 Mi |
| Kafka | 768 Mi | 1.5 Gi |
| Debezium Connect | 512 Mi | 768 Mi |
| ClickHouseKeeper | 256 Mi | 512 Mi |
| ClickHouse | 1 Gi | 2 Gi |
| Airflow webserver | 768 Mi | 1.5 Gi |
| Airflow scheduler | 512 Mi | 1 Gi |
| Strimzi operator | 128 Mi | 384 Mi |
| ClickHouse operator | 128 Mi | 256 Mi |
| **Total** | **~4.6 Gi** | **~9.4 Gi** |

Recommended: **12 GB free RAM**, **30 GB free disk**.

---

## Repository Structure

```
.
├── README.md
├── startup.sh                              ← Smart deploy (step 1 → 15, auto-skip)
├── cleanup.sh                              ← Teardown: light (keep cluster) or full
├── kind-cluster-config-light.yaml
│
├── dags/                                   ← Airflow DAG files (edit these directly)
│   └── gold_user_activity_dag.py           ← Daily gold aggregation DAG
│                                              Change queries, schedule, or tasks here.
│                                              Step 14 reads this file at deploy time.
│
└── scripts/                                ← Infrastructure deployment steps
    ├── step-01-install-prerequisites.sh    ← Installs Docker, kubectl, Helm, Kind
    ├── step-02-preflight-checks.sh         ← Verifies RAM, disk, ports, Docker health
    ├── step-03-create-cluster.sh
    ├── step-04-install-strimzi.sh
    ├── step-05-install-clickhouse-operator.sh
    ├── step-06-deploy-postgres.sh
    ├── step-07-deploy-mongodb.sh
    ├── step-08-populate-data.sh
    ├── step-09-deploy-kafka.sh
    ├── step-10-deploy-kafka-connect.sh
    ├── step-11-configure-debezium.sh
    ├── step-12-deploy-clickhouse.sh
    ├── step-13-create-silver-layer.sh
    ├── step-14-deploy-airflow.sh           ← Reads dags/gold_user_activity_dag.py
    └── step-15-e2e-validate.sh
```

### Why DAGs live separately from scripts

Deployment scripts (`scripts/`) and DAG logic (`dags/`) change for completely different reasons and at different frequencies:

| What changes | Why | Who changes it |
|---|---|---|
| `scripts/step-*.sh` | Infrastructure changes (K8s versions, Helm chart upgrades, new services) | DevOps / platform engineer |
| `dags/gold_user_activity_dag.py` | Business logic (new metrics, schedule changes, extra tasks, ClickHouse query tuning) | Data engineer |

Keeping them separate means a data engineer can change the aggregation SQL, add a new task, or adjust the retry policy by editing a single Python file — without ever opening a shell script. Step 14 always reads the current file contents at deploy time, so changes are picked up automatically on the next `./startup.sh --only 14` run.