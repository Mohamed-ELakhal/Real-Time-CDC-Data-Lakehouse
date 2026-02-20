#!/bin/bash
# step-14-deploy-airflow.sh  ── v2 (hardened)
# Changes vs v1:
#   • Gold table DDL is now defined HERE only (single source of truth).
#     The DAG no longer recreates the table — it uses IF NOT EXISTS from step-14.
#     Schema: (activity_date, user_id, full_name, email,
#              total_events, last_event_time, _processed_at)
#     This resolves the schema mismatch between step-14 DDL and step-15 fallback.
#   • DAG loaded from the v2 gold_user_activity_dag.py (5-task pipeline with
#     silver check, UTC timezone, OPTIMIZE task, max_active_runs=1).
#   • last_event_time declared Nullable(DateTime64(3,'UTC')) — NULL for 0-event days.
#   • Credentials note: production should use Airflow Connections via K8s Secrets.

set -e

echo "============================================"
echo "STEP 14: Deploy Airflow with Gold Layer"
echo "============================================"
echo ""

NAMESPACE="airflow"
CH_NAMESPACE="clickhouse-operator"

CH_POD=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
    -n ${CH_NAMESPACE} -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -z "$CH_POD" ]; then
    echo "❌ ClickHouse pod not found — run step-12 first"
    exit 1
fi
echo "Using ClickHouse pod: ${CH_POD}"
echo ""

# ============================================================
# PART 1: Create Gold Layer Table in ClickHouse
# ============================================================
echo "============================================"
echo "PART 1: Create Gold Layer in ClickHouse"
echo "============================================"
echo ""

# ── CANONICAL SCHEMA ──────────────────────────────────────────────────────
# This is the single source of truth for gold_user_activity.
# The DAG uses the same column names. step-15 fallback also uses these.
#
# Columns:
#   activity_date   – the calendar day being summarised (previous day in ds)
#   user_id         – from silver_users
#   full_name       – from silver_users (empty string for orphan events)
#   email           – from silver_users (empty string for orphan events)
#   total_events    – count of non-deleted events for user on activity_date
#   last_event_time – max(created_at) of events; NULL if total_events=0
#   _processed_at   – wall-clock timestamp of the DAG run that produced the row
#
# Engine: ReplacingMergeTree(_processed_at) as secondary deduplication
# safety net. Primary idempotency is DELETE+INSERT per activity_date.
# ─────────────────────────────────────────────────────────────────────────
echo "Creating gold_user_activity table (canonical schema v2)..."
kubectl exec ${CH_POD} -n ${CH_NAMESPACE} -- \
    clickhouse-client --user admin --password admin --query "
DROP TABLE IF EXISTS commerce.gold_user_activity;
CREATE TABLE commerce.gold_user_activity
(
    activity_date   Date,
    user_id         Int32,
    full_name       String,
    email           String,
    total_events    UInt64,
    last_event_time Nullable(DateTime64(3, 'UTC')),
    _processed_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_processed_at)
PARTITION BY toYYYYMM(activity_date)
ORDER BY (activity_date, user_id)
SETTINGS index_granularity = 8192;
"
echo "✓ gold_user_activity table created (canonical schema v2)"
echo ""

# ============================================================
# PART 2: Prepare Airflow metadata DB
# ============================================================
echo "============================================"
echo "PART 2: Prepare Airflow Metadata DB"
echo "============================================"
echo ""

PG_POD=$(kubectl get pod -l app=postgres -n default \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -z "$PG_POD" ]; then
    echo "❌ postgres-0 pod not found — run step-06 first"
    exit 1
fi
echo "Creating 'airflow' metadata database in postgres-0..."
kubectl exec "${PG_POD}" -n default -- \
    psql -U postgres -c "CREATE DATABASE airflow;" 2>/dev/null \
    || echo "  (airflow database already exists, continuing)"
echo "✓ Airflow metadata DB ready at postgres.default.svc.cluster.local:5432/airflow"
echo ""

# ============================================================
# PART 3: Create DAG ConfigMap
# ============================================================
echo "============================================"
echo "PART 3: Create DAG ConfigMap"
echo "============================================"
echo ""

# ── Locate the DAG source file ────────────────────────────────
# The DAG lives as a standalone Python file at dags/gold_user_activity_dag.py
# (one level above the scripts/ directory, at the project root).
# This keeps DAG logic independent of deployment scripts — edit the .py
# file directly without touching any shell code.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAG_FILE="${SCRIPT_DIR}/../dags/gold_user_activity_dag.py"

if [[ ! -f "${DAG_FILE}" ]]; then
    echo "❌ DAG file not found at: ${DAG_FILE}"
    echo "   Expected layout:"
    echo "     <project-root>/"
    echo "       dags/"
    echo "         gold_user_activity_dag.py   ← edit this file to change DAG logic"
    echo "       scripts/"
    echo "         step-14-deploy-airflow.sh   ← this script"
    exit 1
fi

# Resolve to absolute path for clarity in logs
DAG_FILE="$(cd "$(dirname "${DAG_FILE}")" && pwd)/$(basename "${DAG_FILE}")"
echo "Using DAG file: ${DAG_FILE}"
echo ""

echo "Creating/updating DAG ConfigMap..."
kubectl delete configmap airflow-dags -n ${NAMESPACE} --ignore-not-found=true
kubectl create configmap airflow-dags -n ${NAMESPACE} \
    --from-file=gold_user_activity_dag.py="${DAG_FILE}"
echo "✓ DAG ConfigMap created from dags/gold_user_activity_dag.py"
echo ""

# ============================================================
# PART 4: Deploy Airflow via Helm
# ============================================================
echo "============================================"
echo "PART 4: Deploy Airflow"
echo "============================================"
echo ""

echo "Cleaning up any existing Airflow installation..."
helm uninstall airflow -n ${NAMESPACE} 2>/dev/null || true
kubectl delete namespace ${NAMESPACE} --ignore-not-found=true
kubectl wait --for=delete namespace/${NAMESPACE} --timeout=120s 2>/dev/null || true
echo "Creating namespace..."
kubectl create namespace ${NAMESPACE}
echo ""

CH_HOST="chi-analytics-main-0-0.${CH_NAMESPACE}.svc.cluster.local"
echo "ClickHouse connection: ${CH_HOST}:8123"
echo ""

echo "Writing Helm values file..."
cat > /tmp/airflow-values.yaml << YAML
images:
  airflow:
    repository: apache/airflow
    tag: "2.9.3"
    pullPolicy: IfNotPresent

defaultAirflowTag: "2.9.3"
executor: "LocalExecutor"

env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
  - name: AIRFLOW__WEBSERVER__EXPOSE_CONFIG
    value: "True"
  - name: AIRFLOW__CORE__DEFAULT_TIMEZONE
    value: "UTC"

# Use existing postgres-0 as the Airflow metadata DB.
postgresql:
  enabled: false

data:
  metadataConnection:
    user: postgres
    pass: postgres
    host: postgres.default.svc.cluster.local
    port: 5432
    db: airflow
    protocol: postgresql+psycopg2
    sslmode: disable

webserver:
  replicas: 1
  resources:
    requests:
      memory: 768Mi
      cpu: 200m
    limits:
      memory: 1.5Gi
      cpu: 1000m
  startupProbe:
    periodSeconds: 15
    failureThreshold: 20
    timeoutSeconds: 10
  service:
    type: NodePort
    ports:
      - name: airflow-ui
        port: 8080
        targetPort: 8080
        nodePort: 30080

scheduler:
  replicas: 1
  resources:
    requests:
      memory: 512Mi
      cpu: 200m
    limits:
      memory: 1Gi
      cpu: 500m

triggerer:
  enabled: false
flower:
  enabled: false
statsd:
  enabled: false
pgbouncer:
  enabled: false

dags:
  persistence:
    enabled: false
  gitSync:
    enabled: false

logs:
  persistence:
    enabled: false
YAML

echo "Installing Airflow (chart 1.15.0 = Airflow 2.9.x)..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update 2>/dev/null || true
helm install airflow apache-airflow/airflow \
  --version 1.15.0 \
  --namespace ${NAMESPACE} \
  --values /tmp/airflow-values.yaml \
  --timeout 15m \
  --wait \
  --wait-for-jobs

echo "✓ Airflow base installation complete"
echo ""

# ============================================================
# PART 5: Mount DAG ConfigMap into Airflow pods
# ============================================================
echo "============================================"
echo "PART 5: Mount DAG ConfigMap"
echo "============================================"
echo ""

echo "Patching Airflow webserver Deployment..."
kubectl patch deployment airflow-webserver -n ${NAMESPACE} --type=json -p='[
  {"op":"add","path":"/spec/template/spec/volumes/-",
   "value":{"name":"dag-cm","configMap":{"name":"airflow-dags"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-",
   "value":{"name":"dag-cm",
            "mountPath":"/opt/airflow/dags/gold_user_activity_dag.py",
            "subPath":"gold_user_activity_dag.py","readOnly":true}}
]'

echo "Patching Airflow scheduler..."
kubectl patch statefulset airflow-scheduler -n ${NAMESPACE} --type=json -p='[
  {"op":"add","path":"/spec/template/spec/volumes/-",
   "value":{"name":"dag-cm","configMap":{"name":"airflow-dags"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-",
   "value":{"name":"dag-cm",
            "mountPath":"/opt/airflow/dags/gold_user_activity_dag.py",
            "subPath":"gold_user_activity_dag.py","readOnly":true}}
]' 2>/dev/null || \
kubectl patch deployment airflow-scheduler -n ${NAMESPACE} --type=json -p='[
  {"op":"add","path":"/spec/template/spec/volumes/-",
   "value":{"name":"dag-cm","configMap":{"name":"airflow-dags"}}},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts/-",
   "value":{"name":"dag-cm",
            "mountPath":"/opt/airflow/dags/gold_user_activity_dag.py",
            "subPath":"gold_user_activity_dag.py","readOnly":true}}
]'

echo "✓ DAG mounted in webserver and scheduler"
echo ""

echo "Waiting for rollout to complete..."
sleep 10
kubectl rollout status deployment/airflow-webserver -n ${NAMESPACE} --timeout=300s
kubectl rollout status statefulset/airflow-scheduler -n ${NAMESPACE} --timeout=300s 2>/dev/null \
  || kubectl rollout status deployment/airflow-scheduler -n ${NAMESPACE} --timeout=300s 2>/dev/null \
  || echo "  (scheduler rollout check skipped)"

echo ""
echo "============================================"
echo "✓ STEP 14 COMPLETE"
echo "============================================"
echo ""

echo "Airflow Status:"
kubectl get pods -n ${NAMESPACE}
echo ""
echo "Services:"
kubectl get svc -n ${NAMESPACE}
echo ""
echo "Airflow Web UI:  http://localhost:8080  (admin / admin)"
echo ""
echo "Gold Layer:"
echo "  Table:    commerce.gold_user_activity"
echo "  Schema:   activity_date, user_id, full_name, email,"
echo "            total_events, last_event_time, _processed_at"
echo "  Engine:   ReplacingMergeTree(_processed_at), partitioned by month"
echo ""
echo "DAG:"
echo "  Name:           gold_user_activity"
echo "  Schedule:       @daily (01:00 UTC → processes previous calendar day)"
echo "  catchup:        True  → backfills from start_date=2026-02-01"
echo "  max_active_runs:1     → no DELETE/INSERT race conditions"
echo "  Tasks:          check_silver → create_table → delete → insert → verify → optimize"
echo ""
echo "Next: STEP 15 - End-to-End Validation"
echo "Run: ./scripts/step-15-e2e-validate.sh"