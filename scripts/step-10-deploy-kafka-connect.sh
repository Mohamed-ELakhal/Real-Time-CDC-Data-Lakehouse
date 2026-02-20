#!/bin/bash

set -e

echo "============================================"
echo "STEP 10: Deploy Kafka Connect + Debezium"
echo "============================================"
echo ""

NAMESPACE="kafka"

echo "Verifying prerequisites..."
KAFKA_READY=$(kubectl get kafka kafka-cluster -n ${NAMESPACE} \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "False")
if [ "$KAFKA_READY" != "True" ]; then
    echo "❌ Kafka cluster not ready. Run step-09 first."
    exit 1
fi
echo "✓ Kafka cluster ready"
echo ""

# ----------------------------------------------------------------
# WHY PLAIN DEPLOYMENT INSTEAD OF STRIMZI KafkaConnect CR?
#
# Strimzi's KafkaConnect CR works by:
#   1. Injecting an init container that copies Strimzi's own scripts
#      into /opt/kafka/ inside the container
#   2. Overriding the entrypoint to run /opt/kafka/kafka_connect_run.sh
#
# The Debezium Connect image (quay.io/debezium/connect) uses a
# completely different layout — scripts live at /kafka/, not /opt/kafka/.
# Result: "No such file or directory" crash on every start.
#
# The only correct ways to use Strimzi KafkaConnect are:
#   a) Use Strimzi's own base image + add connector JARs via build spec
#      (requires a push-accessible container registry — not available in Kind)
#   b) Use a plain Kubernetes Deployment with the Debezium image directly
#
# We choose (b): plain Deployment. Connectors are still managed via
# the Connect REST API on port 8083 — same as production setups.
# ----------------------------------------------------------------

echo "Cleaning up any previous KafkaConnect CR..."
kubectl delete kafkaconnect debezium-connect -n ${NAMESPACE} --ignore-not-found=true
kubectl wait --for=delete pod -l strimzi.io/name=debezium-connect-connect \
    -n ${NAMESPACE} --timeout=60s 2>/dev/null || true
echo "✓ Cleanup done"
echo ""

# Get the bootstrap server ClusterIP (more reliable than DNS in some Kind setups)
KAFKA_BOOTSTRAP="kafka-cluster-kafka-bootstrap.${NAMESPACE}.svc.cluster.local:9092"
echo "Kafka bootstrap: ${KAFKA_BOOTSTRAP}"
echo ""

echo "Step 1/3: Deploying Kafka Connect (plain Deployment)..."
kubectl apply -f - <<YAML
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: debezium-connect
  namespace: ${NAMESPACE}
  labels:
    app: debezium-connect
spec:
  replicas: 1
  selector:
    matchLabels:
      app: debezium-connect
  template:
    metadata:
      labels:
        app: debezium-connect
    spec:
      containers:
      - name: connect
        image: quay.io/debezium/connect:2.7
        ports:
        - containerPort: 8083
          name: rest-api
        env:
        # Core Connect config — must match the topics we pre-created in step 9
        - name: BOOTSTRAP_SERVERS
          value: "${KAFKA_BOOTSTRAP}"
        - name: GROUP_ID
          value: "debezium-connect-cluster"
        - name: OFFSET_STORAGE_TOPIC
          value: "debezium-offsets"
        - name: CONFIG_STORAGE_TOPIC
          value: "debezium-configs"
        - name: STATUS_STORAGE_TOPIC
          value: "debezium-status"
        - name: OFFSET_STORAGE_REPLICATION_FACTOR
          value: "1"
        - name: CONFIG_STORAGE_REPLICATION_FACTOR
          value: "1"
        - name: STATUS_STORAGE_REPLICATION_FACTOR
          value: "1"
        # JSON converters without schema — ClickHouse Kafka engine parses raw JSON
        - name: KEY_CONVERTER
          value: "org.apache.kafka.connect.json.JsonConverter"
        - name: VALUE_CONVERTER
          value: "org.apache.kafka.connect.json.JsonConverter"
        - name: KEY_CONVERTER_SCHEMAS_ENABLE
          value: "false"
        - name: VALUE_CONVERTER_SCHEMAS_ENABLE
          value: "false"
        # Heap tuned for 768Mi limit
        - name: KAFKA_HEAP_OPTS
          value: "-Xms256m -Xmx512m"
        resources:
          requests:
            memory: 512Mi
            cpu: 200m
          limits:
            memory: 768Mi
            cpu: 500m
        # TCP probe — Connect REST API on 8083
        readinessProbe:
          tcpSocket:
            port: 8083
          initialDelaySeconds: 45
          periodSeconds: 10
          failureThreshold: 6
        livenessProbe:
          tcpSocket:
            port: 8083
          initialDelaySeconds: 60
          periodSeconds: 30
          failureThreshold: 5
---
apiVersion: v1
kind: Service
metadata:
  name: debezium-connect
  namespace: ${NAMESPACE}
  labels:
    app: debezium-connect
spec:
  ports:
  - port: 8083
    targetPort: 8083
    name: rest-api
  selector:
    app: debezium-connect
YAML
echo "✓ Deployment and Service created"
echo ""

echo "Step 2/3: Waiting for Connect pod to be ready..."
echo "  This may take 4-6 minutes (Debezium image pull ~500MB)..."
kubectl wait --for=condition=ready pod \
    -l app=debezium-connect \
    -n ${NAMESPACE} \
    --timeout=480s
echo "✓ Connect pod ready"
echo ""

echo "Step 3/3: Verifying Connect REST API and plugins..."
CONNECT_POD=$(kubectl get pod -l app=debezium-connect -n ${NAMESPACE} \
    -o jsonpath='{.items[0].metadata.name}')

echo "  Pod: ${CONNECT_POD}"
echo ""

# Give REST API a moment to fully initialize after pod is ready
sleep 10

echo "Connect REST API response:"
kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s http://localhost:8083/ | python3 -m json.tool 2>/dev/null || \
    kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- curl -s http://localhost:8083/

echo ""
echo "Installed Debezium connector plugins:"
kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s http://localhost:8083/connector-plugins | \
    python3 -c "
import sys, json
try:
    plugins = json.load(sys.stdin)
    for p in plugins:
        if 'debezium' in p['class'].lower():
            print('  ✓', p['class'])
except:
    print('  (could not parse plugin list)')
" 2>/dev/null

echo ""
echo "============================================"
echo "✓ STEP 10 COMPLETE"
echo "============================================"
echo ""
echo "Kafka Connect endpoint:"
echo "  In-cluster: http://debezium-connect.${NAMESPACE}.svc.cluster.local:8083"
echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo ""
echo "============================================"
echo "Next: STEP 11 - Configure Debezium Connectors"
echo "Run: ./scripts/step-11-configure-debezium.sh"
echo "============================================"