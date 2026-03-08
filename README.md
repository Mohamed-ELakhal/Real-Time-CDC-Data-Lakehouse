# Real-Time CDC Data Lakehouse

A production-grade streaming data pipeline running on Kubernetes. Captures change events from PostgreSQL and MongoDB in real time via Debezium, routes them through Kafka into a ClickHouse analytics store (silver layer), and orchestrates a daily aggregation job in Airflow (gold layer).

```
PostgreSQL ──┐
              ├──► Debezium ──► Kafka ──► ClickHouse silver ──► Airflow ──► gold_user_activity
MongoDB    ──┘
```

**Two deployment options — same pipeline, same result:**

| | Local | AWS (Terraform) |
|---|---|---|
| **Where it runs** | Your laptop / desktop | EC2 t3.xlarge (4 vCPU / 16 GB) |
| **Prerequisites** | 12 GB free RAM, Docker | AWS account, Terraform, SSH key |
| **Setup time** | 50–70 min (first run) | ~5 min Terraform + 50–70 min pipeline |
| **Cost** | Electricity only | ~$0.166 / hr (~$4 / day) |
| **Best for** | Development, demos | Sharing, CI, low-RAM machines |

---

## Contents

- [Architecture](#architecture)
- [Deployment Options](#deployment-options)
  - [Option A — Local](#option-a--local)
  - [Option B — AWS with Terraform](#option-b--aws-with-terraform)
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
- [Repository Structure](#repository-structure)

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
  check_silver → check_consumer_lag → create_table → drop_partition → insert → verify → optimize
```

---

## Deployment Options

### Option A — Local

Run the entire pipeline on your own machine. Docker, kubectl, Helm, and Kind are installed automatically by step 1.

**Requirements:** 12 GB free RAM · 30 GB free disk · macOS or Linux

```bash
git clone <your-repo-url> cdc-lakehouse
cd cdc-lakehouse
chmod +x startup.sh cleanup.sh

./startup.sh          # installs tools + deploys all 15 steps
```

**Estimated time:**
- First run on a bare machine: **50–70 minutes** (tool install + image pulls)
- Subsequent runs (tools already present): **5–15 minutes**
- After a light cleanup: **5–10 minutes** (images cached, PVCs intact)

**Access services at** `localhost` — see [Accessing Services](#accessing-services).

---

### Option B — AWS with Terraform

Provision a `t3.xlarge` EC2 instance (4 vCPU / 16 GB RAM) via Terraform. The instance bootstraps itself with all required tools on first boot. You SSH in and run `./startup.sh` exactly as in the local path.

**When to use this option:**
- Your laptop has less than 12 GB free RAM
- You want to share the running pipeline with teammates via public IP
- You want a persistent environment that survives laptop restarts

#### Prerequisites

| Tool | Install |
|------|---------|
| Terraform ≥ 1.3 | https://developer.hashicorp.com/terraform/downloads |
| AWS CLI v2 | https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html |
| SSH key pair | `ssh-keygen -t ed25519 -C "cdc-lakehouse" -f ~/.ssh/cdc_lakehouse` |

#### Step 1 — Configure AWS credentials

Choose **one** method:

**Method A — Environment variables (recommended):**
```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="wJalr..."

# Verify it works:
aws sts get-caller-identity
```

**Method B — Named AWS CLI profile:**
```bash
aws configure --profile cdc-lakehouse
# Prompts for: Access Key ID, Secret Access Key, region, output format

# Verify:
aws sts get-caller-identity --profile cdc-lakehouse
```

**Method C — Default profile:**
```bash
aws configure
aws sts get-caller-identity
```

#### Step 2 — Configure Terraform variables

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` — the only required changes are the SSH key paths:

```hcl
# Required: point to your SSH key pair
ssh_public_key_path  = "~/.ssh/cdc_lakehouse.pub"
ssh_private_key_path = "~/.ssh/cdc_lakehouse"

# Credentials: match the method you chose above
aws_profile = ""              # Method A (env vars)
# aws_profile = "cdc-lakehouse"  # Method B (named profile)
# aws_profile = "default"        # Method C (default profile)

# Region: change to your nearest region
aws_region = "eu-west-1"

# Optional: restrict SSH to your IP for security
# allowed_ssh_cidr = "203.0.113.10/32"  # find your IP: curl checkip.amazonaws.com
```

#### Step 3 — Provision the instance

```bash
cd terraform/

terraform init      # download AWS provider (~30 seconds)
terraform plan      # preview what will be created (no changes made)
terraform apply     # create VPC, subnet, security group, EC2 instance
```

`terraform apply` takes about **2–3 minutes**. At the end it prints:

```
Outputs:

instance_public_ip  = "54.171.x.x"
ssh_command         = "ssh -i ~/.ssh/cdc_lakehouse ubuntu@54.171.x.x"
bootstrap_log       = "tail -f /var/log/cdc-lakehouse-init.log"
service_urls = {
  airflow_ui        = "http://54.171.x.x:8080"
  clickhouse_http   = "http://54.171.x.x:8123"
  kafka_connect_api = "http://54.171.x.x:8083"
  ...
}
stop_instance_command  = "aws ec2 stop-instances --instance-ids i-0abc... --region eu-west-1"
start_instance_command = "aws ec2 start-instances --instance-ids i-0abc... --region eu-west-1"
```

#### Step 4 — Wait for bootstrap, then SSH in

The EC2 instance installs Docker, kubectl, kind, and Helm automatically on first boot (~3–5 minutes). Wait for bootstrap to complete before starting the pipeline:

```bash
# SSH in (~90 seconds after apply)
ssh -i ~/.ssh/cdc_lakehouse ubuntu@<PUBLIC_IP>

# Watch bootstrap progress (wait until you see "Bootstrap complete")
tail -f /var/log/cdc-lakehouse-init.log
```

#### Step 5 — Deploy the pipeline

Once bootstrap is complete, deploy exactly the same way as local:

```bash
# On the EC2 instance:
git clone <your-repo-url> cdc-lakehouse
cd cdc-lakehouse
chmod +x startup.sh cleanup.sh

./startup.sh --yes    # --yes skips confirmation prompts (non-interactive)
```

**Estimated time:** 50–70 minutes on first run (container image pulls).

#### Step 6 — Access services

Replace `localhost` with the EC2 public IP in all service URLs:

| Service | Local URL | AWS URL |
|---------|-----------|---------|
| Airflow UI | http://localhost:8080 | http://\<PUBLIC_IP\>:8080 |
| ClickHouse HTTP | http://localhost:8123 | http://\<PUBLIC_IP\>:8123 |
| ClickHouse Native | localhost:9000 | \<PUBLIC_IP\>:9000 |
| Kafka Connect REST | http://localhost:8083 | http://\<PUBLIC_IP\>:8083 |
| Kafka | localhost:9092 | \<PUBLIC_IP\>:9092 |
| PostgreSQL | localhost:5432 | \<PUBLIC_IP\>:5432 |
| MongoDB | localhost:27017 | \<PUBLIC_IP\>:27017 |

#### Cost management

The instance costs **~$0.166/hr** while running. Stop it when not in use — stopping halts compute billing while keeping the EBS disk (all data and Docker image caches are preserved).

```bash
# Stop instance (keeps disk, halts compute billing)
aws ec2 stop-instances --instance-ids <INSTANCE_ID> --region eu-west-1

# Start again (public IP will change — retrieve new IP after starting)
aws ec2 start-instances --instance-ids <INSTANCE_ID> --region eu-west-1
aws ec2 describe-instances --instance-ids <INSTANCE_ID> \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text

# Or use the ready-made commands from terraform output:
terraform output stop_instance_command
terraform output start_instance_command
terraform output get_new_ip_after_start
```

After restarting, SSH in and run `./startup.sh` — it detects completed steps and skips them, so only the steps that need recovery actually run.

#### Teardown

```bash
# From your local machine (in the terraform/ directory):
terraform destroy -auto-approve
```

This removes the EC2 instance, EBS disk, security group, subnet, VPC, and key pair. **All billing stops immediately.** The destroy takes about 2 minutes.

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
| `--yes` | Non-interactive mode — no confirmation prompts (CI/CD, AWS) |
| `--retries N` | Max automatic retries per step (default: 3) |
| `--help` | Print usage |

### Examples

```bash
# Complete deploy from scratch — installs tools, checks system, deploys all 15 steps
./startup.sh

# On AWS: fully non-interactive (no prompts)
./startup.sh --yes

# If tools are already installed, skip steps 1–2 and start from the Kind cluster
./startup.sh --from 3

# Resume from Kafka after a partial failure (skips steps 1–8 automatically)
./startup.sh --from 9

# Re-run only the Debezium connector configuration
./startup.sh --only 11

# Preview what would run without making any changes
./startup.sh --dry-run
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
| 11 | Configure Debezium | PG + Mongo connectors with DLQ routing + slot health check |
| 12 | Deploy ClickHouseKeeper + ClickHouse | CHK + CHI |
| 13 | Create Silver Layer | Kafka engine tables + ReplacingMergeTree + MVs |
| 14 | Deploy Airflow + Gold DAG | Helm install + 7-task gold aggregation DAG |
| 15 | End-to-End Validation | Silver health, slot check, schema drift, DAG trigger, CDC smoke test |

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

> **AWS note:** `cleanup.sh` manages the Kubernetes workloads inside the EC2 instance. To destroy the EC2 instance itself (and stop AWS billing), run `terraform destroy -auto-approve` from the `terraform/` directory on your local machine.

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

> **AWS note:** On EC2, the cloud-init bootstrap installs all four tools automatically before you SSH in. Step 1 detects them and prints `[SKIP]` for each. No manual intervention needed.

**Supported platforms:**

| OS | Docker | kubectl | Helm | Kind |
|----|--------|---------|------|------|
| macOS | Docker Desktop (Homebrew Cask) | Homebrew | Homebrew | Homebrew |
| Ubuntu / Debian | Docker Engine (official APT repo) | Kubernetes APT repo | Official installer | Binary download |
| RHEL / Fedora / CentOS | Docker Engine (official DNF repo) | Kubernetes YUM repo | Official installer | Binary download |
| Arch Linux | Community packages | Community packages | Official installer | Binary download |

Also installs: `curl`, `python3`, `nc` (netcat, used for ClickHouseKeeper health checks).

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
| 5 | Docker memory allocation | ≥ 10 GB (macOS only) | < 10 GB | — |
| 6 | Required ports free | All 8 ports available | Any port in use | — |
| 7 | Kind cluster state | No cluster, or existing + reachable | Existing but unreachable | — |
| 8 | Internet connectivity | All 4 container registries reachable | Any registry unreachable | — |

> **AWS note:** A `t3.xlarge` has 16 GB RAM and a clean Ubuntu install — step 2 passes all checks automatically.

---

### Step 3 — Create Kind Cluster
Creates a 2-node Kind cluster (`cdc-lakehouse`) with NodePort mappings for all 7 services.

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
Registers PostgreSQL and MongoDB connectors with Dead-Letter Queue routing and WAL slot health monitoring:

| Setting | Value | Purpose |
|---------|-------|---------|
| `errors.tolerance` | `all` | Process all messages, route errors to DLQ |
| `errors.deadletterqueue.topic.name` | `*.dlq` | Persistent audit trail |
| `errors.log.include.messages` | `true` | Full message context with each error |
| `slot.drop.on.stop` | `false` | Preserve PG replication slot on restart |
| `snapshot.isolation.mode` | `repeatable_read` | Consistent snapshot on crash-restart |
| `heartbeat.interval.ms` | `10000` | Keep WAL active during quiet periods |

Part 5 of step 11 checks the `debezium_slot` WAL lag — warns at 500 MB (half the 1 GB invalidation cap) and prints exact recovery commands if the slot has been invalidated.

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
Airflow 2.9.3 via Helm (LocalExecutor). Creates the canonical `gold_user_activity` table (daily partition key) and packages the DAG into a Kubernetes ConfigMap by reading `dags/gold_user_activity_dag.py` directly.

---

### Step 15 — End-to-End Validation
Full pipeline health check across 8 parts: resource & eviction check, silver row counts, replication slot health (WAL lag), consumer lag, DLQ inspection, Airflow DAG trigger, gold layer validation, schema drift detection (PostgreSQL vs ClickHouse column comparison), and a live CDC smoke test (insert PG row → verify propagation to ClickHouse within 30s → clean up).

---

## Accessing Services

### Local

| Service | URL / Host | Credentials |
|---------|-----------|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | admin / admin |
| ClickHouse Native | localhost:9000 | admin / admin |
| Kafka | localhost:9092 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| PostgreSQL | localhost:5432 | postgres / postgres |
| MongoDB | localhost:27017 | — |

### AWS

Replace `localhost` with your EC2 public IP (shown in `terraform output instance_public_ip`):

| Service | URL / Host | Credentials |
|---------|-----------|-------------|
| Airflow UI | http://\<PUBLIC_IP\>:8080 | admin / admin |
| ClickHouse HTTP | http://\<PUBLIC_IP\>:8123 | admin / admin |
| ClickHouse Native | \<PUBLIC_IP\>:9000 | admin / admin |
| Kafka | \<PUBLIC_IP\>:9092 | — |
| Kafka Connect REST | http://\<PUBLIC_IP\>:8083 | — |
| PostgreSQL | \<PUBLIC_IP\>:5432 | postgres / postgres |
| MongoDB | \<PUBLIC_IP\>:27017 | — |

```bash
# ClickHouse CLI (local)
clickhouse-client --host localhost --port 9000 --user admin --password admin

# ClickHouse CLI (AWS)
clickhouse-client --host <PUBLIC_IP> --port 9000 --user admin --password admin

# PostgreSQL (local)
psql -h localhost -U postgres -d commerce

# PostgreSQL (AWS)
psql -h <PUBLIC_IP> -U postgres -d commerce

# MongoDB (local)
mongosh "mongodb://localhost:27017/commerce?replicaSet=rs0"

# MongoDB (AWS)
mongosh "mongodb://<PUBLIC_IP>:27017/commerce?replicaSet=rs0"

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

Partition key: `toYYYYMMDD(activity_date)` — one partition per calendar day, enabling atomic `DROP PARTITION` for idempotent backfill runs.

---

## Gold Layer DAG

**ID:** `gold_user_activity` | **Schedule:** `@daily` (midnight UTC) | **Catchup:** `True` | **Max active runs:** `1`

### DAG source file

The DAG lives as a standalone Python file at `dags/gold_user_activity_dag.py`. Edit it directly to change query logic, schedule, retry settings, or task structure. Step 14 reads this file at deploy time and packages it into a Kubernetes ConfigMap mounted into Airflow's webserver and scheduler pods.

**To update the DAG after deployment:**

```bash
# 1. Edit the DAG file
vim dags/gold_user_activity_dag.py

# 2. Re-package into the ConfigMap (Airflow picks it up within ~30s)
kubectl delete configmap airflow-dags -n airflow
kubectl create configmap airflow-dags -n airflow \
    --from-file=gold_user_activity_dag.py=dags/gold_user_activity_dag.py

# 3. Or simply re-run step 14 (idempotent)
./startup.sh --only 14
```

### Task pipeline

```
check_silver_data       ← fail-fast if silver FINAL count = 0 (CDC stall)
        │
check_consumer_lag      ← verify Kafka consumer threads alive; warn on errors
        │
create_gold_table       ← idempotent DDL (IF NOT EXISTS, daily partitions)
        │
delete_existing_partition ← DROP PARTITION '<YYYYMMDD>' (synchronous, atomic)
        │
insert_gold_activity    ← LEFT JOIN silver FINAL, UTC-safe, NULL last_event_time
        │
verify_gold_output      ← pretty-print per-user results to task log
        │
optimize_silver_tables  ← OPTIMIZE FINAL (prevents FINAL query degradation)
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
  FROM commerce.silver_users FINAL
  UNION ALL
  SELECT 'silver_events', count(), countIf(is_deleted=1)
  FROM commerce.silver_events FINAL;"

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

# Replication slot WAL lag
kubectl exec $(kubectl get pod -l app=postgres -n default \
    -o jsonpath='{.items[0].metadata.name}') -n default -- \
  psql -U postgres -c "
  SELECT slot_name, active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_behind,
    COALESCE(invalidation_reason, 'none') AS invalidation_reason
  FROM pg_replication_slots WHERE slot_name = 'debezium_slot';"
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
- **Use the AWS option** — a `t3.xlarge` has 16 GB RAM and this issue does not occur.

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
kubectl exec "${CONNECT_POD}" -n kafka -- \
  curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
# Restart the failed task:
kubectl exec "${CONNECT_POD}" -n kafka -- \
  curl -s -X POST http://localhost:8083/connectors/postgres-connector/tasks/0/restart
```

---

### Replication slot invalidated (WAL cap exceeded)

If Debezium was offline long enough for the 1 GB WAL cap (set in step 6) to be hit, the slot is invalidated and CDC changes during the offline window are lost. Step 15 Part 2.5 detects this and prints the exact recovery steps, or run manually:

```bash
# Check slot status
kubectl exec $(kubectl get pod -l app=postgres -n default \
    -o jsonpath='{.items[0].metadata.name}') -n default -- \
  psql -U postgres -c "SELECT slot_name, invalidation_reason FROM pg_replication_slots;"

# If invalidated — re-run step 11 which handles recovery automatically
./scripts/step-11-configure-debezium.sh
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
# Check for evicted pods (step 15 Part 0 does this automatically)
kubectl get pods -A --field-selector=status.phase=Failed | grep Evicted

# Light cleanup restarts workloads while preserving PVC data
./cleanup.sh --mode light --yes
./startup.sh --from 4 --yes
```

---

### AWS: public IP changed after instance restart

The public IP changes each time you start the instance. Retrieve the new IP:

```bash
terraform output get_new_ip_after_start
# or:
aws ec2 describe-instances --instance-ids <INSTANCE_ID> \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
```

---

### AWS: Terraform credentials error

```
Error: No valid credential sources found
```

Verify your credentials:
```bash
aws sts get-caller-identity

# If using env vars, check they are exported in the current shell:
echo $AWS_ACCESS_KEY_ID

# If using a profile, pass it explicitly to verify:
aws sts get-caller-identity --profile cdc-lakehouse
```

---

## Security Notes

Configured for **development use**. Do not expose to production traffic without addressing:

| Area | Current | Production recommendation |
|------|---------|--------------------------|
| Passwords | Hardcoded | Vault / AWS Secrets Manager |
| ClickHouse network | Open to 0.0.0.0/0 | Restrict to pod CIDRs |
| Airflow `EXPOSE_CONFIG` | True | Disabled |
| Debezium credentials | Connector JSON | Kafka Connect Secret Provider |
| TLS | None | Enable on Kafka + ClickHouse HTTP |
| SSH access (AWS) | `allowed_ssh_cidr = "0.0.0.0/0"` | Set to your IP: `"x.x.x.x/32"` |
| Service ports (AWS) | Open to internet | Restrict to known CIDR ranges |

Passwords are stored in Kubernetes Secrets (`pg-credentials`, `ch-credentials`) from step 6 and 12 onwards — pod env vars reference these Secrets rather than embedding credentials in manifests.

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

**Minimum free RAM: 8 GB** (degraded). **Recommended: 12 GB** (stable).  
**Disk:** 30 GB recommended (Docker image caches).

**AWS:** `t3.xlarge` (4 vCPU / 16 GB) exceeds all requirements with headroom to spare.

---

## Repository Structure

```
.
├── README.md
├── startup.sh                              ← Smart deploy (step 1 → 15, auto-skip)
├── cleanup.sh                              ← Teardown: light (keep cluster) or full
├── kind-cluster-config-light.yaml
│
├── terraform/                              ← AWS provisioning (Option B)
│   ├── providers.tf                        ← AWS provider, credential config
│   ├── variables.tf                        ← All input variables with defaults
│   ├── main.tf                             ← VPC, subnet, SG, EC2, cloud-init
│   ├── outputs.tf                          ← IP, SSH command, service URLs
│   └── terraform.tfvars.example            ← Copy → terraform.tfvars, fill in keys
│
├── dags/                                   ← Airflow DAG files (edit directly)
│   └── gold_user_activity_dag.py           ← Daily gold aggregation DAG
│
└── scripts/                                ← Infrastructure deployment steps
    ├── step-01-install-prerequisites.sh
    ├── step-02-preflight-checks.sh
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
    ├── step-14-deploy-airflow.sh
    └── step-15-e2e-validate.sh
```

### Why DAGs live separately from scripts

| What changes | Why | Who changes it |
|---|---|---|
| `scripts/step-*.sh` | Infrastructure changes (K8s versions, Helm chart upgrades, new services) | DevOps / platform engineer |
| `dags/gold_user_activity_dag.py` | Business logic (new metrics, schedule changes, extra tasks, ClickHouse query tuning) | Data engineer |

Keeping them separate means a data engineer can change the aggregation SQL, add a new task, or adjust the retry policy by editing a single Python file — without ever opening a shell script. Step 14 always reads the current file contents at deploy time, so changes are picked up automatically on the next `./startup.sh --only 14` run.

### Why Terraform lives separately from scripts

The `terraform/` directory manages AWS infrastructure (what machine runs the pipeline). The `scripts/` directory manages the Kubernetes workloads (what runs on that machine). They are independent layers:

- `terraform apply` provisions and destroys the EC2 host
- `./startup.sh` and `./cleanup.sh` manage the pipeline on whatever host you're running on (local or AWS)

You never need to re-apply Terraform to redeploy the pipeline, and you never need to touch the scripts to change AWS region or instance size.
