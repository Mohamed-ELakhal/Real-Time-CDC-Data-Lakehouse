#!/bin/bash
# step-06-deploy-postgres.sh  ── v2 (hardened)
# Changes vs v1:
#   • max_slot_wal_keep_size = 1 GB  → prevents WAL disk exhaustion when
#     Debezium is offline (replication slot WAL bloat — highest-severity risk)
#   • wal_sender_timeout = 0         → disables idle sender timeout to avoid
#     Debezium slot drop on quiet periods
#   • Kubernetes Secret for PG credentials → no plaintext passwords in pod env

set -e

echo "============================================"
echo "STEP 6: Deploy PostgreSQL with CDC Enabled"
echo "============================================"
echo ""

echo "Verifying cluster access..."
if ! kubectl cluster-info &>/dev/null; then echo "❌ Cannot access cluster"; exit 1; fi
echo "✓ Cluster accessible"
echo ""

NAMESPACE="default"

echo "Cleaning up any previous PostgreSQL deployment..."
kubectl delete statefulset postgres -n ${NAMESPACE} --ignore-not-found=true
kubectl delete service postgres   -n ${NAMESPACE} --ignore-not-found=true
kubectl delete configmap postgres-config -n ${NAMESPACE} --ignore-not-found=true
kubectl delete secret   pg-credentials   -n ${NAMESPACE} --ignore-not-found=true
echo "✓ Cleanup done"
echo ""

# ----------------------------------------------------------------
# Step 1 — Kubernetes Secret for PostgreSQL credentials
# Using a Secret means passwords never appear in pod YAML or logs.
# ----------------------------------------------------------------
echo "Step 1/5: Creating PostgreSQL credentials Secret..."
kubectl create secret generic pg-credentials \
  --namespace ${NAMESPACE} \
  --from-literal=POSTGRES_DB=commerce \
  --from-literal=POSTGRES_USER=postgres \
  --from-literal=POSTGRES_PASSWORD=postgres
echo "✓ Secret 'pg-credentials' created"
echo ""

echo "Step 2/5: Creating PostgreSQL ConfigMap (non-secret config)..."
kubectl apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-config
  namespace: ${NAMESPACE}
data:
  POSTGRES_DB: commerce
  POSTGRES_USER: postgres
YAML
echo "✓ ConfigMap created"
echo ""

echo "Step 3/5: Creating PostgreSQL StatefulSet..."
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: ${NAMESPACE}
  labels:
    app: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      securityContext:
        fsGroup: 70
      containers:
      - name: postgres
        image: postgres:15-alpine
        ports:
        - containerPort: 5432
          name: postgres
        # Reference Secret for password — not shown in 'kubectl describe pod'
        env:
        - name: POSTGRES_DB
          valueFrom:
            secretKeyRef:
              name: pg-credentials
              key: POSTGRES_DB
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: pg-credentials
              key: POSTGRES_USER
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: pg-credentials
              key: POSTGRES_PASSWORD
        # FIXED: No 'command' override — entrypoint handles privilege drop.
        # Only 'args' appended to docker-entrypoint.sh.
        args:
          - "postgres"
          - "-c"
          - "wal_level=logical"
          - "-c"
          - "max_wal_senders=5"
          - "-c"
          - "max_replication_slots=5"
          - "-c"
          - "shared_preload_libraries=pg_stat_statements"
          # ── WAL BLOAT PROTECTION ─────────────────────────────────
          # Caps WAL retained per slot to 1 GB.  Without this cap,
          # a stalled Debezium connector causes PG to accumulate WAL
          # indefinitely → disk exhaustion → ALL writes fail.
          - "-c"
          - "max_slot_wal_keep_size=1073741824"
          # Disable idle sender timeout so Debezium's replication
          # slot is not dropped during quiet periods.
          - "-c"
          - "wal_sender_timeout=0"
        resources:
          requests:
            memory: 256Mi
            cpu: 100m
          limits:
            memory: 512Mi
            cpu: 500m
        volumeMounts:
        - name: postgres-storage
          mountPath: /var/lib/postgresql/data
          subPath: postgres
        livenessProbe:
          exec:
            command: [pg_isready, -U, postgres]
          initialDelaySeconds: 30
          periodSeconds: 10
          failureThreshold: 5
        readinessProbe:
          exec:
            command: [pg_isready, -U, postgres]
          initialDelaySeconds: 10
          periodSeconds: 5
          failureThreshold: 3
  volumeClaimTemplates:
  - metadata:
      name: postgres-storage
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: standard
      resources:
        requests:
          storage: 2Gi
YAML
echo "✓ StatefulSet created"
echo ""

echo "Step 4/5: Creating PostgreSQL Service (NodePort 30432)..."
kubectl apply -f - <<YAML
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: ${NAMESPACE}
  labels:
    app: postgres
spec:
  type: NodePort
  ports:
  - port: 5432
    targetPort: 5432
    nodePort: 30432
    name: postgres
  selector:
    app: postgres
YAML
echo "✓ Service created"
echo ""

echo "Step 5/5: Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres -n ${NAMESPACE} --timeout=300s

echo ""
echo "============================================"
echo "✓ STEP 6 COMPLETE"
echo "============================================"
echo ""
echo "PostgreSQL: postgres.default.svc.cluster.local:5432  (external: localhost:5432)"
echo "Credentials stored in Kubernetes Secret 'pg-credentials'"
echo ""
echo "CDC Settings:"
echo "  ✓ wal_level=logical"
echo "  ✓ max_wal_senders=5"
echo "  ✓ max_replication_slots=5"
echo "  ✓ max_slot_wal_keep_size=1GB  ← WAL bloat protection"
echo "  ✓ wal_sender_timeout=0        ← keeps slot alive during quiet periods"

echo ""
echo "Verifying CDC configuration..."
POD=$(kubectl get pod -l app=postgres -n ${NAMESPACE} -o jsonpath='{.items[0].metadata.name}')
kubectl exec "$POD" -n ${NAMESPACE} -- psql -U postgres -c \
  "SELECT name, setting FROM pg_settings WHERE name IN
   ('wal_level','max_wal_senders','max_replication_slots',
    'max_slot_wal_keep_size','wal_sender_timeout')
   ORDER BY name;" | grep -v "^$"

echo ""
echo "Replication slot health (run after Debezium is connected):"
echo "  kubectl exec $POD -n ${NAMESPACE} -- psql -U postgres -c"
echo "    \"SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff("
echo "      pg_current_wal_lsn(), restart_lsn)) AS wal_behind"
echo "      FROM pg_replication_slots;\""

echo ""
echo "============================================"
echo "Next: STEP 7 - Deploy MongoDB"
echo "Run: ./scripts/step-07-deploy-mongodb.sh"
echo "============================================"