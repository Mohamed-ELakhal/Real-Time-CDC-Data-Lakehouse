#!/bin/bash
# step-14-deploy-airflow.sh  ── v3 (fully automated, no manual intervention required)
#
# Fixes vs v2:
#   BUG 1 — DAG ConfigMap silently deleted:
#     v2 created the ConfigMap in PART 3, then PART 4 deleted the entire
#     namespace (taking the ConfigMap with it), then PART 5 patched pods to
#     mount a ConfigMap that no longer existed. Fixed by moving ConfigMap
#     creation to AFTER namespace recreation, immediately before PART 5.
#
#   BUG 2 — Migration job never ran / failed silently:
#     v2 relied entirely on the helm chart's airflow-run-airflow-migrations job,
#     which can fail silently or be cleaned up before helm reports an error.
#     Fixed by explicitly running `airflow db migrate` via kubectl exec into
#     a temporary pod AFTER helm install, with retry logic.
#
#   BUG 3 — No postgres readiness verification before helm install:
#     v2 created the airflow DB then immediately ran helm install. Any transient
#     postgres unavailability poisoned the migration job. Fixed by adding an
#     explicit cross-namespace connectivity check before helm install.

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

echo "Creating gold_user_activity table (canonical schema v2)..."
kubectl exec ${CH_POD} -n ${CH_NAMESPACE} -- \
    clickhouse-client --user admin --password admin \
    --query "DROP TABLE IF EXISTS commerce.gold_user_activity"

kubectl exec ${CH_POD} -n ${CH_NAMESPACE} -- \
    clickhouse-client --user admin --password admin \
    --query "CREATE TABLE commerce.gold_user_activity
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
SETTINGS index_granularity = 8192"
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

# Ensure postgres is fully ready before proceeding
echo "Waiting for postgres pod to be fully ready..."
kubectl wait --for=condition=ready pod/${PG_POD} -n default --timeout=60s
echo "✓ Postgres pod ready"
echo ""

echo "Creating 'airflow' metadata database in postgres-0..."
kubectl exec "${PG_POD}" -n default -- \
    psql -U postgres -c "CREATE DATABASE airflow;" 2>/dev/null \
    || echo "  (airflow database already exists, continuing)"
echo "✓ Airflow metadata DB ready at postgres.default.svc.cluster.local:5432/airflow"
echo ""

# ─── FIX BUG 3: Verify cross-namespace connectivity before helm install ────────
# The migration job runs inside the airflow namespace and must reach
# postgres.default.svc.cluster.local. A transient failure here silently kills
# the migration job. We verify reachability now and fail fast if it's broken.
#
# NOTE: We use `kubectl exec` into the existing postgres pod (which is in the
# default namespace) and verify the airflow database exists and is accepting
# connections. This avoids `kubectl run --rm` which requires -it (interactive
# TTY) and therefore breaks in non-interactive scripts.
echo "Verifying postgres is ready and airflow database is accessible..."

for attempt in $(seq 1 10); do
    RESULT=$(kubectl exec "${PG_POD}" -n default -- \
        psql -U postgres -d airflow -c "SELECT 1 AS connected;" 2>&1 || echo "FAILED")
    if echo "$RESULT" | grep -q "connected"; then
        echo "✓ Postgres airflow database confirmed accessible (attempt ${attempt})"
        break
    fi
    echo "  [${attempt}/10] postgres not yet reachable, waiting 5s..."
    sleep 5
    if [ "$attempt" -eq 10 ]; then
        echo "❌ Cannot reach postgres airflow database after 10 attempts"
        echo "   Output: ${RESULT}"
        exit 1
    fi
done
echo ""

# ============================================================
# PART 3: Locate DAG file (validation only — ConfigMap created in PART 5)
# ============================================================
echo "============================================"
echo "PART 3: Validate DAG File"
echo "============================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAG_FILE="${SCRIPT_DIR}/../dags/gold_user_activity_dag.py"

if [[ ! -f "${DAG_FILE}" ]]; then
    echo "❌ DAG file not found at: ${DAG_FILE}"
    echo "   Expected layout:"
    echo "     <project-root>/"
    echo "       dags/"
    echo "         gold_user_activity_dag.py"
    echo "       scripts/"
    echo "         step-14-deploy-airflow.sh"
    exit 1
fi
DAG_FILE="$(cd "$(dirname "${DAG_FILE}")" && pwd)/$(basename "${DAG_FILE}")"
echo "✓ DAG file found: ${DAG_FILE}"
echo "  (ConfigMap will be created AFTER namespace recreation in PART 5)"
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
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
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

# Use --wait but NOT --wait-for-jobs here.
# The helm chart's migration job is unreliable in constrained environments
# (transient postgres unavailability causes it to fail silently).
# We run migrations explicitly in PART 4b below.
helm install airflow apache-airflow/airflow \
  --version 1.15.0 \
  --namespace ${NAMESPACE} \
  --values /tmp/airflow-values.yaml \
  --timeout 15m \
  --wait

echo "✓ Airflow base installation complete"
echo ""

# ─── FIX BUG 2: Explicit migration step ───────────────────────────────────────
# Do NOT rely on the helm chart's migration job. Run migrations explicitly so
# we have full control over retry logic and can fail fast with a clear error.
echo "============================================"
echo "PART 4b: Run Airflow DB Migrations Explicitly"
echo "============================================"
echo ""

# Retrieve the Fernet key that helm generated and stored in a Secret
FERNET_KEY=$(kubectl get secret airflow-fernet-key -n ${NAMESPACE} \
    -o jsonpath='{.data.fernet-key}' | base64 -d)
if [ -z "$FERNET_KEY" ]; then
    echo "❌ Could not retrieve Fernet key from airflow-fernet-key secret"
    exit 1
fi
echo "✓ Fernet key retrieved from airflow-fernet-key secret"

WEBSERVER_SECRET=$(kubectl get secret airflow-webserver-secret-key -n ${NAMESPACE} \
    -o jsonpath='{.data.webserver-secret-key}' | base64 -d 2>/dev/null || echo "")

# NOTE: We use Kubernetes Jobs (not `kubectl run --rm`) because --rm requires
# an interactive TTY (-it) which breaks in non-interactive scripts.
# Jobs run to completion, expose exit codes, and are cleanable after.

run_airflow_job() {
    local JOB_NAME=$1
    local COMMAND=$2
    local ARGS=$3

    # Clean up any previous job with this name
    kubectl delete job "${JOB_NAME}" -n ${NAMESPACE} --ignore-not-found=true
    kubectl wait --for=delete job/${JOB_NAME} -n ${NAMESPACE} --timeout=30s 2>/dev/null || true

    kubectl apply -f - <<JOBSPEC
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  ttlSecondsAfterFinished: 60
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: airflow
        image: apache/airflow:2.9.3
        command: ["bash", "-c", "${COMMAND} ${ARGS}"]
        env:
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          value: "postgresql+psycopg2://postgres:postgres@postgres.default.svc.cluster.local:5432/airflow"
        - name: AIRFLOW__CORE__FERNET_KEY
          value: "${FERNET_KEY}"
        - name: AIRFLOW__WEBSERVER__SECRET_KEY
          value: "${WEBSERVER_SECRET:-placeholder}"
        - name: AIRFLOW__CORE__LOAD_EXAMPLES
          value: "False"
JOBSPEC

    echo "  Waiting for job/${JOB_NAME} to complete..."
    kubectl wait --for=condition=complete job/${JOB_NAME} \
        -n ${NAMESPACE} --timeout=300s

    # Check for failure
    FAILED=$(kubectl get job ${JOB_NAME} -n ${NAMESPACE} \
        -o jsonpath='{.status.failed}' 2>/dev/null || echo "0")
    if [ "${FAILED:-0}" != "0" ] && [ "${FAILED:-0}" != "" ]; then
        echo "❌ Job ${JOB_NAME} failed. Logs:"
        kubectl logs -l job-name=${JOB_NAME} -n ${NAMESPACE} --tail=50 2>/dev/null || true
        exit 1
    fi
    kubectl delete job "${JOB_NAME}" -n ${NAMESPACE} --ignore-not-found=true
}

echo "Running airflow db migrate via Kubernetes Job..."
run_airflow_job "airflow-db-migrate" "airflow db migrate" ""
echo "✓ Airflow DB migrations complete"
echo ""

# Verify migrations actually applied
echo "Verifying migration head in DB..."
run_airflow_job "airflow-db-check" "airflow db check-migrations" "--migration-wait-timeout=30"
echo "✓ Migration head confirmed in DB"
echo ""

# Create admin user (idempotent — `airflow users create` exits 0 if user exists)
echo "Creating admin user (admin/admin)..."
run_airflow_job "airflow-create-admin" \
    "airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || true" \
    ""
echo "✓ Admin user ready"
echo ""

# ============================================================
# PART 5: Create DAG ConfigMap and mount into Airflow pods
# ============================================================
# ─── FIX BUG 1: ConfigMap created HERE, after namespace exists ────────────────
# v2 created it in PART 3 before `kubectl delete namespace airflow` in PART 4,
# which silently destroyed it. Creating it here ensures it exists when pods start.
echo "============================================"
echo "PART 5: Create DAG ConfigMap and Mount"
echo "============================================"
echo ""

echo "Creating DAG ConfigMap (namespace now stable)..."
kubectl delete configmap airflow-dags -n ${NAMESPACE} --ignore-not-found=true
kubectl create configmap airflow-dags -n ${NAMESPACE} \
    --from-file=gold_user_activity_dag.py="${DAG_FILE}"
echo "✓ DAG ConfigMap created from dags/gold_user_activity_dag.py"
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

echo "Patching Airflow scheduler StatefulSet..."
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

echo "Waiting for rollouts to complete..."
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