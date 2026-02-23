#!/bin/bash
# step-11-configure-debezium.sh  ── v2 (hardened)
# Changes vs v1:
#   • Dead-Letter Queue (DLQ) configured for both connectors:
#     malformed messages route to *-dlq topics instead of being silently skipped.
#   • errors.tolerance=all + errors.log.enable=true for full error visibility.
#   • slot.drop.on.stop=false explicitly set to protect the replication slot.
#   • Consistent heartbeat interval to keep WAL alive during quiet periods.

set -e

echo "============================================"
echo "STEP 11: Configure Debezium Connectors"
echo "============================================"
echo ""

NAMESPACE="kafka"

echo "Verifying Kafka Connect is ready..."
CONNECT_POD=$(kubectl get pod -l app=debezium-connect -n ${NAMESPACE} \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -z "$CONNECT_POD" ]; then
    echo "❌ Kafka Connect pod not found. Run step-10 first."
    exit 1
fi
kubectl wait --for=condition=ready pod/${CONNECT_POD} -n ${NAMESPACE} --timeout=60s
echo "✓ Kafka Connect pod ready: ${CONNECT_POD}"
echo ""

# ================================================================
# Helper: wait for connector to reach RUNNING state
# ================================================================
wait_running() {
    local name=$1
    echo "  Waiting for ${name} to reach RUNNING state..."
    for i in $(seq 1 18); do
        STATE=$(kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
            curl -s "http://localhost:8083/connectors/${name}/status" 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); \
              tasks=d.get('tasks',[]); \
              print(tasks[0]['state'] if tasks else 'STARTING')" 2>/dev/null || echo "STARTING")
        if [ "$STATE" = "RUNNING" ]; then
            echo "  ✓ ${name} task state: RUNNING"
            return 0
        fi
        echo "    [$i/18] state=${STATE}, waiting 5s..."
        sleep 5
    done
    echo "  ⚠ ${name} did not reach RUNNING within 90s — check logs"
    return 1
}

# ================================================================
# PRE-STEP: Create DLQ topics for malformed messages
# ================================================================
echo "Creating Dead-Letter Queue topics..."
kubectl apply -f - <<YAML
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: postgres-users-dlq
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: postgres.public.users.dlq
  config:
    retention.ms: "2592000000"    # 30 days — time to investigate errors
    cleanup.policy: delete
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: mongo-events-dlq
  namespace: ${NAMESPACE}
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  partitions: 1
  replicas: 1
  topicName: mongo.commerce.events.dlq
  config:
    retention.ms: "2592000000"
    cleanup.policy: delete
YAML
echo "✓ DLQ topics created"
echo ""

# ================================================================
# PART 1: Register PostgreSQL Connector
# ================================================================
echo "============================================"
echo "PART 1: PostgreSQL Connector"
echo "============================================"
echo ""

echo "Removing any existing postgres-connector..."
kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s -X DELETE http://localhost:8083/connectors/postgres-connector 2>/dev/null || true
sleep 3

echo "Registering postgres-connector..."
PG_RESULT=$(kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    --data '{
  "name": "postgres-connector",
  "config": {
    "connector.class":                          "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname":                        "postgres.default.svc.cluster.local",
    "database.port":                            "5432",
    "database.user":                            "postgres",
    "database.password":                        "postgres",
    "database.dbname":                          "commerce",
    "topic.prefix":                             "postgres",
    "table.include.list":                       "public.users",
    "plugin.name":                              "pgoutput",
    "publication.name":                         "dbz_publication",
    "publication.autocreate.mode":              "filtered",
    "slot.name":                                "debezium_slot",
    "slot.drop.on.stop":                        "false",
    "heartbeat.interval.ms":                    "10000",
    "snapshot.mode":                            "initial",
    "transforms":                               "unwrap",
    "transforms.unwrap.type":                   "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones":        "false",
    "transforms.unwrap.delete.handling.mode":   "rewrite",
    "transforms.unwrap.add.fields":             "op,db,table,ts_ms",
    "topic.creation.enable":                    "true",
    "topic.creation.default.replication.factor":"1",
    "topic.creation.default.partitions":        "1",
    "key.converter":                            "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":             "false",
    "value.converter":                          "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable":           "false",
    "errors.tolerance":                         "all",
    "errors.log.enable":                        "true",
    "errors.log.include.messages":              "true",
    "errors.deadletterqueue.topic.name":        "postgres.public.users.dlq",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}' \
    http://localhost:8083/connectors)

echo "Response: $PG_RESULT"
echo ""

if echo "$PG_RESULT" | grep -q '"error_code"'; then
    echo "❌ PostgreSQL connector registration failed"
    echo "$PG_RESULT" | python3 -m json.tool 2>/dev/null || echo "$PG_RESULT"
    exit 1
fi
echo "✓ postgres-connector registered"
echo ""
wait_running "postgres-connector"
echo ""

# ================================================================
# PART 2: Register MongoDB Connector
# ================================================================
echo "============================================"
echo "PART 2: MongoDB Connector"
echo "============================================"
echo ""

echo "Removing any existing mongodb-connector..."
kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s -X DELETE http://localhost:8083/connectors/mongodb-connector 2>/dev/null || true
sleep 3

echo "Registering mongodb-connector..."
MG_RESULT=$(kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
    curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Accept: application/json" \
    --data '{
  "name": "mongodb-connector",
  "config": {
    "connector.class":                          "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string":                "mongodb://mongodb-0.mongodb.default.svc.cluster.local:27017/?replicaSet=rs0",
    "topic.prefix":                             "mongo",
    "database.include.list":                    "commerce",
    "collection.include.list":                  "commerce.events",
    "snapshot.mode":                            "initial",
    "capture.mode":                             "change_streams_update_full",
    "tombstones.on.delete":                     "false",
    "heartbeat.interval.ms":                    "10000",
    "topic.creation.enable":                    "true",
    "topic.creation.default.replication.factor":"1",
    "topic.creation.default.partitions":        "1",
    "transforms":                               "unwrap",
    "transforms.unwrap.type":                   "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
    "transforms.unwrap.drop.tombstones":        "true",
    "transforms.unwrap.delete.handling.mode":   "rewrite",
    "transforms.unwrap.add.fields":             "op,ts_ms",
    "key.converter":                            "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":             "false",
    "value.converter":                          "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable":           "false",
    "errors.tolerance":                         "all",
    "errors.log.enable":                        "true",
    "errors.log.include.messages":              "true",
    "errors.deadletterqueue.topic.name":        "mongo.commerce.events.dlq",
    "errors.deadletterqueue.topic.replication.factor": "1",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}' \
    http://localhost:8083/connectors)

echo "Response: $MG_RESULT"
echo ""

if echo "$MG_RESULT" | grep -q '"error_code"'; then
    echo "❌ MongoDB connector registration failed"
    echo "$MG_RESULT" | python3 -m json.tool 2>/dev/null || echo "$MG_RESULT"
    exit 1
fi
echo "✓ mongodb-connector registered"
echo ""
wait_running "mongodb-connector"
echo ""

# ================================================================
# PART 3: Wait for initial snapshots
# ================================================================
echo "Waiting 30s for initial snapshots to complete..."
sleep 30

# ================================================================
# PART 4: Verify connector status and DLQ health
# ================================================================
echo "============================================"
echo "PART 4: Final Status"
echo "============================================"
echo ""

for connector in postgres-connector mongodb-connector; do
    echo "--- ${connector} ---"
    kubectl exec "${CONNECT_POD}" -n ${NAMESPACE} -- \
        curl -s http://localhost:8083/connectors/${connector}/status 2>/dev/null \
        | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  Connector: {d[\"connector\"][\"state\"]}')
    for i, t in enumerate(d.get('tasks', [])):
        state = t['state']
        icon = '✓' if state == 'RUNNING' else '✗'
        print(f'  {icon} Task[{i}]: {state}')
        if t.get('trace'):
            print(f'    Error: {t[\"trace\"][:200]}')
except Exception as e:
    print(f'  Parse error: {e}')
" 2>&1
    echo ""
done

echo "DLQ topic message counts (should be 0 on clean run):"
for dlq_topic in postgres.public.users.dlq mongo.commerce.events.dlq; do
    COUNT=$(kubectl exec kafka-cluster-combined-0 -n ${NAMESPACE} -- \
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:9092 \
        --topic "${dlq_topic}" --time -1 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}' || echo "0")
    ICON="✓"
    [ "$COUNT" != "0" ] && ICON="⚠"
    echo "  ${ICON} ${dlq_topic}: ${COUNT} messages"
done
echo ""

echo "Message counts in main topics:"
for topic in postgres.public.users mongo.commerce.events; do
    COUNT=$(kubectl exec kafka-cluster-combined-0 -n ${NAMESPACE} -- \
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:9092 \
        --topic "${topic}" --time -1 2>/dev/null \
        | awk -F: '{sum += $3} END {print sum+0}' || echo "0")
    echo "  ${topic}: ${COUNT} messages"
done

echo ""
echo "Replication slot health:"
PG_POD=$(kubectl get pod -l app=postgres -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "$PG_POD" ]; then
    kubectl exec "$PG_POD" -n default -- psql -U postgres -c \
      "SELECT slot_name, active,
              pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_behind
       FROM pg_replication_slots;" 2>/dev/null || echo "  (slot not yet created)"
fi

echo ""
echo "============================================"
echo "✓ STEP 11 COMPLETE"
echo "============================================"
echo ""
echo "Next: STEP 12 - Deploy ClickHouse"
echo "Run: ./scripts/step-12-deploy-clickhouse.sh"
echo ""
echo "DLQ monitoring: check DLQ topics periodically with:"
echo "  kubectl exec kafka-cluster-combined-0 -n kafka -- \\"
echo "    /opt/kafka/bin/kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 \\"
echo "    --topic postgres.public.users.dlq --from-beginning --max-messages 5"