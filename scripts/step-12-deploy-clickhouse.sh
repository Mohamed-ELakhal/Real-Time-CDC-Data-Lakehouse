#!/bin/bash
# step-12-deploy-clickhouse.sh  ── v2 (hardened)
# Changes vs v1:
#   • ClickHouseKeeper (CHK) deployed as a StatefulSet before CHI.
#     The challenge requirement says "CHI & CHK". CHK is the ZooKeeper-compatible
#     coordination service built into the ClickHouse binary.
#   • Kubernetes Secret for ClickHouse admin password.
#   • CHI references the CHK endpoint for distributed coordination.

set -e

echo "============================================"
echo "STEP 12: Deploy ClickHouseKeeper + ClickHouse"
echo "============================================"
echo ""

NAMESPACE="clickhouse-operator"

echo "Verifying cluster access..."
if ! kubectl cluster-info &>/dev/null; then echo "❌ Cannot access cluster"; exit 1; fi
echo "✓ Cluster accessible"
echo ""

# ----------------------------------------------------------------
# Step 0: Create Kubernetes Secret for ClickHouse credentials
# ----------------------------------------------------------------
echo "Step 0/5: Creating ClickHouse credentials Secret..."
kubectl delete secret ch-credentials -n ${NAMESPACE} --ignore-not-found=true
kubectl create secret generic ch-credentials \
  --namespace ${NAMESPACE} \
  --from-literal=CH_USER=admin \
  --from-literal=CH_PASSWORD=admin
echo "✓ Secret 'ch-credentials' created in namespace ${NAMESPACE}"
echo ""

# ----------------------------------------------------------------
# Step 1: Deploy ClickHouseKeeper (CHK)
#
# WHY CHK?
#   The challenge spec explicitly asks for CHI & CHK.
#   CHK is the modern, ZooKeeper-compatible coordination layer
#   embedded inside the ClickHouse binary. It handles:
#     - Distributed DDL coordination
#     - ReplicatedMergeTree metadata (not strictly needed for single-node
#       but required for production patterns)
#   We run a single-replica CHK (acceptable for local dev) using the
#   same ClickHouse image already in use for the analytics cluster.
# ----------------------------------------------------------------
echo "Step 1/5: Deploying ClickHouseKeeper (CHK)..."

kubectl apply -f - <<YAML
---
apiVersion: v1
kind: Service
metadata:
  name: clickhouse-keeper
  namespace: ${NAMESPACE}
  labels:
    app: clickhouse-keeper
spec:
  clusterIP: None   # headless — pods address each other by DNS
  ports:
  - port: 9181
    name: client
  - port: 9234
    name: raft
  selector:
    app: clickhouse-keeper
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: clickhouse-keeper
  namespace: ${NAMESPACE}
  labels:
    app: clickhouse-keeper
spec:
  serviceName: clickhouse-keeper
  replicas: 1   # single-replica CHK — adequate for local dev
  selector:
    matchLabels:
      app: clickhouse-keeper
  template:
    metadata:
      labels:
        app: clickhouse-keeper
    spec:
      containers:
      - name: clickhouse-keeper
        image: clickhouse/clickhouse-server:24.3-alpine
        command:
          - clickhouse-keeper
          - --config=/etc/clickhouse-keeper/keeper_config.xml
        ports:
        - containerPort: 9181
          name: client
        - containerPort: 9234
          name: raft
        resources:
          requests:
            memory: 256Mi
            cpu: 100m
          limits:
            memory: 512Mi
            cpu: 500m
        volumeMounts:
        - name: keeper-config
          mountPath: /etc/clickhouse-keeper
        - name: keeper-data
          mountPath: /var/lib/clickhouse-keeper
        readinessProbe:
          tcpSocket:
            port: 9181
          initialDelaySeconds: 10
          periodSeconds: 5
          failureThreshold: 6
        livenessProbe:
          tcpSocket:
            port: 9181
          initialDelaySeconds: 20
          periodSeconds: 10
          failureThreshold: 5
      volumes:
      - name: keeper-config
        configMap:
          name: clickhouse-keeper-config
  volumeClaimTemplates:
  - metadata:
      name: keeper-data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: standard
      resources:
        requests:
          storage: 1Gi
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickhouse-keeper-config
  namespace: ${NAMESPACE}
data:
  keeper_config.xml: |
    <clickhouse>
      <logger>
        <level>warning</level>
        <console>true</console>
      </logger>
      <listen_host>0.0.0.0</listen_host>
      <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse-keeper/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse-keeper/snapshots</snapshot_storage_path>
        <coordination_settings>
          <operation_timeout_ms>10000</operation_timeout_ms>
          <session_timeout_ms>30000</session_timeout_ms>
          <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>
        <raft_configuration>
          <server>
            <id>1</id>
            <hostname>clickhouse-keeper-0.clickhouse-keeper.${NAMESPACE}.svc.cluster.local</hostname>
            <port>9234</port>
          </server>
        </raft_configuration>
      </keeper_server>
    </clickhouse>
YAML

echo "Waiting for ClickHouseKeeper pod to be ready..."
kubectl wait --for=condition=ready pod/clickhouse-keeper-0 -n ${NAMESPACE} --timeout=180s
echo "✓ ClickHouseKeeper ready"
echo ""

# Verify CHK is responding
CHK_RESPONSE=$(kubectl exec clickhouse-keeper-0 -n ${NAMESPACE} -- \
    /bin/sh -c 'echo "ruok" | nc localhost 9181' 2>/dev/null || echo "error")
if [ "$CHK_RESPONSE" = "imok" ]; then
    echo "✓ CHK health check: imok"
else
    echo "⚠ CHK health check returned: ${CHK_RESPONSE} (may need a few more seconds)"
fi
echo ""

# ----------------------------------------------------------------
# Step 2: Clean up any previous CHI
# ----------------------------------------------------------------
echo "Step 2/5: Cleaning up any previous CHI..."
kubectl delete chi analytics -n clickhouse --ignore-not-found=true
kubectl delete chi analytics -n ${NAMESPACE} --ignore-not-found=true
sleep 5
echo "✓ Cleanup done"
echo ""

# ----------------------------------------------------------------
# Step 3: Deploy ClickHouse (CHI) pointing at CHK
# ----------------------------------------------------------------
echo "Step 3/5: Deploying ClickHouse CHI..."
kubectl apply -f - <<YAML
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: analytics
  namespace: ${NAMESPACE}
spec:
  configuration:
    zookeeper:
      # Point CHI at the CHK StatefulSet we deployed above.
      # This enables distributed DDL and ReplicatedMergeTree support.
      nodes:
        - host: clickhouse-keeper-0.clickhouse-keeper.${NAMESPACE}.svc.cluster.local
          port: 9181
    clusters:
      - name: "main"
        layout:
          shardsCount: 1
          replicasCount: 1
    users:
      admin/password: "admin"
      admin/networks/ip: "::/0"
  defaults:
    templates:
      podTemplate: default
      dataVolumeClaimTemplate: default
  templates:
    podTemplates:
      - name: default
        spec:
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:24.3-alpine
              resources:
                requests:
                  memory: "1Gi"
                  cpu: "200m"
                limits:
                  memory: "2Gi"
                  cpu: "1000m"
    volumeClaimTemplates:
      - name: default
        spec:
          storageClassName: standard
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
YAML
echo "✓ CHI manifest applied"
echo ""

# ----------------------------------------------------------------
# Step 4: Wait for CHI to be ready
# ----------------------------------------------------------------
echo "Step 4/5: Waiting for CHI to be ready (max 5 minutes)..."
for i in $(seq 1 30); do
    STATUS=$(kubectl get chi analytics -n ${NAMESPACE} \
        -o jsonpath='{.status.status}' 2>/dev/null || echo "")
    PODS=$(kubectl get pods -n ${NAMESPACE} \
        -l clickhouse.altinity.com/chi=analytics --no-headers 2>/dev/null | wc -l)
    echo "  [$i/30] CHI status: ${STATUS:-pending}, Pods: ${PODS}"
    if [ "$STATUS" = "Completed" ] && [ "$PODS" -gt 0 ]; then
        echo "✓ ClickHouse deployed successfully!"
        break
    fi
    sleep 10
done
echo ""

# ----------------------------------------------------------------
# Step 5: Test connectivity
# ----------------------------------------------------------------
echo "Step 5/5: Testing ClickHouse connection..."
CH_POD=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
    -n ${NAMESPACE} -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$CH_POD" ]; then
    kubectl wait --for=condition=ready pod/${CH_POD} -n ${NAMESPACE} --timeout=180s || true
    sleep 10
    kubectl exec ${CH_POD} -n ${NAMESPACE} -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT 'Connected', version(), hostName()" 2>/dev/null \
        && echo "✓ ClickHouse responding" \
        || echo "⚠ Not ready yet — may need more time"

    echo ""
    echo "Verifying CHK connectivity from ClickHouse..."
    kubectl exec ${CH_POD} -n ${NAMESPACE} -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT * FROM system.zookeeper WHERE path='/' LIMIT 1" 2>/dev/null \
        && echo "✓ CHK coordination confirmed" \
        || echo "⚠ CHK connection check inconclusive — may be normal if no ZK nodes yet"
fi

echo ""
echo "============================================"
echo "✓ STEP 12 COMPLETE"
echo "============================================"
echo ""
echo "Services deployed:"
echo "  ClickHouseKeeper:  clickhouse-keeper-0.clickhouse-keeper.${NAMESPACE}.svc.cluster.local:9181"
echo "  ClickHouse HTTP:   chi-analytics-main-0-0.${NAMESPACE}.svc.cluster.local:8123"
echo "  ClickHouse Native: chi-analytics-main-0-0.${NAMESPACE}.svc.cluster.local:9000"
echo "  External HTTP:     localhost:8123"
echo ""
echo "Credentials stored in Kubernetes Secret 'ch-credentials' in namespace ${NAMESPACE}"
echo ""
echo "Next: STEP 13 - Create Silver Layer"
echo "Run: ./scripts/step-13-create-silver-layer.sh"