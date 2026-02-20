#!/bin/bash

set -e

echo "============================================"
echo "STEP 9: Deploy Kafka Cluster (KRaft, Strimzi)"
echo "============================================"
echo ""

NAMESPACE="kafka"

echo "Verifying Strimzi operator is ready..."
if ! kubectl get deployment strimzi-cluster-operator -n ${NAMESPACE} &>/dev/null; then
    echo "❌ Strimzi operator not found in namespace '${NAMESPACE}'"
    echo "   Run: ./scripts/step-04-install-strimzi.sh"
    exit 1
fi
OPERATOR_READY=$(kubectl get deployment strimzi-cluster-operator -n ${NAMESPACE} \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [ "$OPERATOR_READY" != "1" ]; then
    echo "❌ Strimzi operator not ready (readyReplicas=${OPERATOR_READY})"
    exit 1
fi
echo "✓ Strimzi operator ready"
echo ""

# ----------------------------------------------------------------
# KRaft mode in Strimzi 0.41 requires:
#   1. A Kafka resource with .spec.kafka.metadataVersion set and
#      useKRaft feature gate enabled via annotations
#   2. A KafkaNodePool resource — KRaft uses node pools to
#      assign roles (controller, broker) to nodes
#
# In KRaft a node can be:
#   - controller only (manages metadata, like ZK did)
#   - broker only (handles client data)
#   - combined (both roles) ← we use this for single-node simplicity
# ----------------------------------------------------------------

echo "Step 1/3: Deploying Kafka cluster in KRaft mode..."
kubectl apply -f - <<YAML
---
# KafkaNodePool defines the set of nodes and their roles.
# We use a single combined node (controller + broker) to minimise
# memory usage while still being a valid KRaft deployment.
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: combined
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: persistent-claim
    size: 5Gi
    class: standard
    deleteClaim: false
  resources:
    requests:
      memory: 768Mi
      cpu: 200m
    limits:
      memory: 1.5Gi
      cpu: 1000m
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: kafka-cluster
  namespace: ${NAMESPACE}
  annotations:
    # Enable KRaft mode — no ZooKeeper needed
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 3.7.0
    # metadataVersion must match the Kafka version
    metadataVersion: "3.7-IV4"
    listeners:
      # Internal listener for in-cluster clients (Debezium, ClickHouse)
      - name: plain
        port: 9092
        type: internal
        tls: false
      # External listener via NodePort for debugging from localhost
      - name: external
        port: 9094
        type: nodeport
        tls: false
        configuration:
          bootstrap:
            nodePort: 30092
    config:
      # Replication factors set to 1 — single broker, no redundancy needed for local dev
      offsets.topic.replication.factor: "1"
      transaction.state.log.replication.factor: "1"
      transaction.state.log.min.isr: "1"
      default.replication.factor: "1"
      min.insync.replicas: "1"
      # Log retention: 7 days or 2GB per partition — enough for CDC replay
      log.retention.hours: "168"
      log.retention.bytes: "2147483648"
      # Allow Debezium to use large messages (e.g. initial snapshots)
      message.max.bytes: "10485760"
    resources:
      requests:
        memory: 768Mi
        cpu: 200m
      limits:
        memory: 1.5Gi
        cpu: 1000m
  # No ZooKeeper section needed in KRaft mode
  entityOperator:
    topicOperator:
      resources:
        requests:
          memory: 128Mi
          cpu: 50m
        limits:
          memory: 256Mi
          cpu: 200m
    userOperator:
      resources:
        requests:
          memory: 128Mi
          cpu: 50m
        limits:
          memory: 256Mi
          cpu: 200m
YAML
echo "✓ Kafka cluster manifest applied"
echo ""

echo "Step 2/3: Waiting for Kafka cluster to be ready..."
echo "  This may take 3-5 minutes (image pull + KRaft init)..."
echo ""

# Wait for the Kafka resource to reach Ready condition
kubectl wait kafka/kafka-cluster \
  --for=condition=Ready \
  --timeout=480s \
  -n ${NAMESPACE}

echo ""
echo "Step 3/3: Creating CDC topics..."
# Pre-creating topics gives us explicit control over partitions and
# retention. Debezium would auto-create them, but explicit creation
# is better practice and avoids race conditions on first connect.
kubectl apply -f - <<YAML
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: postgres-users
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: postgres.public.users
  config:
    retention.ms: "604800000"     # 7 days
    cleanup.policy: delete
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: mongo-events
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: mongo.commerce.events
  config:
    retention.ms: "604800000"
    cleanup.policy: delete
---
# Debezium internal topics
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: debezium-offsets
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: debezium-offsets
  config:
    cleanup.policy: compact
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: debezium-configs
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: debezium-configs
  config:
    cleanup.policy: compact
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: debezium-status
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: debezium-status
  config:
    cleanup.policy: compact
YAML
echo "✓ Topics created"
echo ""

# Wait a moment for topic operator to reconcile
sleep 10

echo "============================================"
echo "✓ STEP 9 COMPLETE"
echo "============================================"
echo ""

echo "Kafka Cluster Status:"
kubectl get kafka kafka-cluster -n ${NAMESPACE} \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")]}{"\n"}' 2>/dev/null || true
echo ""

kubectl get pods -n ${NAMESPACE}
echo ""

echo "Topics:"
kubectl get kafkatopics -n ${NAMESPACE}
echo ""

echo "Kafka Bootstrap Addresses:"
echo "  Internal (in-cluster): kafka-cluster-kafka-bootstrap.${NAMESPACE}.svc.cluster.local:9092"
echo "  External (localhost):  localhost:30092"

echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo ""
echo "============================================"
echo "Next: STEP 10 - Deploy Kafka Connect"
echo "Run: ./scripts/step-10-deploy-kafka-connect.sh"
echo "============================================"