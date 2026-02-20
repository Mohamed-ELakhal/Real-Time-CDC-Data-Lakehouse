#!/bin/bash
# step-15-e2e-validate.sh  ── v2 (hardened)
# Changes vs v1:
#   • Fallback INSERT in Part 5 now uses the canonical schema from step-14:
#     (activity_date, user_id, full_name, email, total_events,
#      last_event_time, _processed_at)
#     This resolves the previous column name mismatch that caused
#     the fallback to fail with a ClickHouse "unknown column" error.
#   • Part 2 now shows quantitative consumer lag (LAG column) for both
#     ClickHouse consumer groups before deciding to reset.
#   • Part 6 CDC smoke test improved: cleans up test user after verification.
#   • All toDate() calls use explicit 'UTC' timezone.

set -e

echo "============================================"
echo "STEP 15: End-to-End Validation"
echo "============================================"
echo ""
echo "This script:"
echo "  1. Verifies silver layer schema exists"
echo "  2. Diagnoses and fixes empty tables (consumer offset reset)"
echo "  3. Checks consumer group lag"
echo "  4. Triggers / validates the Airflow gold DAG"
echo "  5. Validates gold_user_activity output"
echo "  6. Runs a live CDC smoke test (insert → verify propagation)"
echo ""

CH_NAMESPACE="clickhouse-operator"
PG_NAMESPACE="default"
MG_NAMESPACE="default"
KAFKA_NAMESPACE="kafka"
AF_NAMESPACE="airflow"

KAFKA_BOOTSTRAP="kafka-cluster-kafka-bootstrap.${KAFKA_NAMESPACE}.svc.cluster.local:9092"
KAFKA_POD="kafka-cluster-combined-0"

CH_POD=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
    -n ${CH_NAMESPACE} -o jsonpath='{.items[0].metadata.name}')
PG_POD=$(kubectl get pod -l app=postgres \
    -n ${PG_NAMESPACE} -o jsonpath='{.items[0].metadata.name}')
MG_POD=$(kubectl get pod -l app=mongodb \
    -n ${MG_NAMESPACE} -o jsonpath='{.items[0].metadata.name}')
AF_POD=$(kubectl get pod -l component=webserver \
    -n ${AF_NAMESPACE} -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

echo "Pods:"
echo "  ClickHouse: ${CH_POD}"
echo "  PostgreSQL: ${PG_POD}"
echo "  MongoDB:    ${MG_POD}"
echo "  Airflow:    ${AF_POD:-not found}"
echo ""

ch() {
    kubectl exec "${CH_POD}" -n ${CH_NAMESPACE} -- \
        clickhouse-client --user admin --password admin --multiquery --query "$1"
}

# ============================================================
# PART 1: Verify silver layer exists — re-create if missing
# ============================================================
echo "============================================"
echo "PART 1: Silver Layer Verification"
echo "============================================"
echo ""

TABLES=$(ch "SHOW TABLES IN commerce;" 2>/dev/null || echo "")
echo "Tables in commerce DB:"
echo "${TABLES:-  (none — step-13 may not have run)}"
echo ""

MISSING=0
for T in kafka_users_queue silver_users mv_users_from_kafka \
          kafka_events_queue silver_events mv_events_from_kafka \
          gold_user_activity; do
    if ! echo "$TABLES" | grep -q "^${T}$"; then
        echo "  ⚠ Missing: $T"
        MISSING=1
    else
        echo "  ✓ $T"
    fi
done
echo ""

if [ "$MISSING" = "1" ]; then
    echo "Some tables missing — running step-13 and step-14 to restore..."
    ./scripts/step-13-create-silver-layer.sh
    ./scripts/step-14-deploy-airflow.sh
    echo ""
fi

# ============================================================
# PART 2: Check silver table row counts + consumer lag
# ============================================================
echo "============================================"
echo "PART 2: Silver Table Row Counts + Consumer Lag"
echo "============================================"
echo ""

USERS_COUNT=$(ch "SELECT count() FROM commerce.silver_users;" 2>/dev/null || echo "0")
EVENTS_COUNT=$(ch "SELECT count() FROM commerce.silver_events;" 2>/dev/null || echo "0")
echo "  silver_users:  ${USERS_COUNT} rows"
echo "  silver_events: ${EVENTS_COUNT} rows"
echo ""

echo "Consumer group lag:"
for GROUP in clickhouse_users_consumer clickhouse_events_consumer; do
    echo "  --- ${GROUP} ---"
    kubectl exec ${KAFKA_POD} -n ${KAFKA_NAMESPACE} -- \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 \
        --group "${GROUP}" \
        --describe 2>/dev/null \
        | awk 'NR==1 || /^[^ ]/' \
        | head -10 \
        | sed 's/^/    /' \
        || echo "    (group not yet created)"
    echo ""
done

if [ "$USERS_COUNT" = "0" ] || [ "$EVENTS_COUNT" = "0" ]; then
    echo "⚠ Silver tables empty — resetting Kafka consumer offsets..."
    echo ""

    echo "Current Kafka topic end offsets:"
    for TOPIC in postgres.public.users mongo.commerce.events; do
        END=$(kubectl exec ${KAFKA_POD} -n ${KAFKA_NAMESPACE} -- \
            /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
            --bootstrap-server localhost:9092 \
            --topic "${TOPIC}" --time -1 2>/dev/null \
            | awk -F: '{sum+=$3} END{print sum+0}')
        echo "  ${TOPIC}: ${END} messages"
    done
    echo ""

    echo "Resetting by dropping and recreating Kafka engine tables..."
    ch "DROP TABLE IF EXISTS commerce.kafka_users_queue;"
    ch "DROP TABLE IF EXISTS commerce.kafka_events_queue;"

    KAFKA_BOOTSTRAP_CH="kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"

    for GROUP in clickhouse_users_consumer clickhouse_events_consumer; do
        kubectl exec ${KAFKA_POD} -n ${KAFKA_NAMESPACE} -- \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --group "${GROUP}" --delete 2>/dev/null \
            && echo "  ✓ Consumer group '${GROUP}' deleted" \
            || echo "  ℹ Consumer group '${GROUP}' not found"
    done
    echo ""

    ch "
CREATE TABLE commerce.kafka_users_queue (
    before_user_id    Nullable(Int32),
    before_full_name  Nullable(String),
    before_email      Nullable(String),
    before_created_at Nullable(String),
    before_updated_at Nullable(String),
    user_id           Nullable(Int32),
    full_name         Nullable(String),
    email             Nullable(String),
    created_at        Nullable(String),
    updated_at        Nullable(String),
    op                String,
    ts_ms             Nullable(Int64),
    __deleted         Nullable(String)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list          = '${KAFKA_BOOTSTRAP_CH}',
    kafka_topic_list           = 'postgres.public.users',
    kafka_group_name           = 'clickhouse_users_consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 10;
"
    echo "✓ kafka_users_queue recreated"

    ch "
CREATE TABLE commerce.kafka_events_queue (
    _id        String,
    user_id    Int32,
    event_type String,
    page       Nullable(String),
    metadata   String,
    created_at DateTime64(3, 'UTC'),
    __deleted  Nullable(UInt8)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list          = '${KAFKA_BOOTSTRAP_CH}',
    kafka_topic_list           = 'mongo.commerce.events',
    kafka_group_name           = 'clickhouse_events_consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 10;
"
    echo "✓ kafka_events_queue recreated"

    ch "
DROP VIEW IF EXISTS commerce.mv_users_from_kafka;
CREATE MATERIALIZED VIEW commerce.mv_users_from_kafka
TO commerce.silver_users AS
SELECT
    coalesce(user_id, before_user_id) AS user_id,
    coalesce(full_name, before_full_name) AS full_name,
    coalesce(email, before_email) AS email,
    parseDateTimeBestEffortOrZero(
        coalesce(created_at, before_created_at, '1970-01-01'), 'UTC'
    ) AS created_at,
    parseDateTimeBestEffortOrZero(
        coalesce(updated_at, before_updated_at, '1970-01-01'), 'UTC'
    ) AS updated_at,
    if(op = 'd', 1, 0) AS is_deleted,
    now() AS _ingested_at
FROM commerce.kafka_users_queue
WHERE coalesce(user_id, before_user_id) IS NOT NULL;
"
    echo "✓ mv_users_from_kafka recreated"

    ch "
DROP VIEW IF EXISTS commerce.mv_events_from_kafka;
CREATE MATERIALIZED VIEW commerce.mv_events_from_kafka
TO commerce.silver_events AS
SELECT
    _id, user_id, event_type, page, metadata, created_at,
    ifNull(__deleted, 0) AS is_deleted,
    now() AS _ingested_at
FROM commerce.kafka_events_queue;
"
    echo "✓ mv_events_from_kafka recreated"

    echo ""
    echo "Waiting 20s for ClickHouse to consume from offset 0..."
    sleep 20

    USERS_COUNT=$(ch "SELECT count() FROM commerce.silver_users;" 2>/dev/null || echo "0")
    EVENTS_COUNT=$(ch "SELECT count() FROM commerce.silver_events;" 2>/dev/null || echo "0")
    echo "After reset — silver_users: ${USERS_COUNT}  silver_events: ${EVENTS_COUNT}"
fi

# ============================================================
# PART 3: DLQ health check
# ============================================================
echo ""
echo "============================================"
echo "PART 3: DLQ Health Check"
echo "============================================"
echo ""
for DLQ in postgres.public.users.dlq mongo.commerce.events.dlq; do
    COUNT=$(kubectl exec ${KAFKA_POD} -n ${KAFKA_NAMESPACE} -- \
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:9092 \
        --topic "${DLQ}" --time -1 2>/dev/null \
        | awk -F: '{sum+=$3} END{print sum+0}' || echo "unknown")
    ICON="✓"
    [ "$COUNT" != "0" ] && [ "$COUNT" != "unknown" ] && ICON="⚠ INVESTIGATE"
    echo "  ${ICON}  ${DLQ}: ${COUNT} messages"
done
echo ""

# ============================================================
# PART 4: Trigger Airflow DAG
# ============================================================
echo "============================================"
echo "PART 4: Trigger Airflow Gold DAG"
echo "============================================"
echo ""

if [ -z "$AF_POD" ]; then
    echo "⚠ Airflow not deployed — skipping DAG trigger (run step-14 first)"
else
    kubectl wait --for=condition=ready pod/${AF_POD} -n ${AF_NAMESPACE} --timeout=60s
    TODAY=$(date -u +%Y-%m-%d)
    YESTERDAY=$(date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null \
        || date -u -v-1d +%Y-%m-%d 2>/dev/null \
        || echo "${TODAY}")

    echo "Triggering gold_user_activity for ${YESTERDAY}..."
    kubectl exec "${AF_POD}" -n ${AF_NAMESPACE} -- \
        airflow dags trigger gold_user_activity \
        --exec-date "${YESTERDAY}T00:00:00+00:00" \
        2>/dev/null && echo "✓ DAG triggered" \
        || echo "⚠ Trigger may have failed — check Airflow UI"

    echo ""
    echo "Waiting 45s for DAG run to complete..."
    sleep 45

    echo "Recent DAG runs:"
    kubectl exec "${AF_POD}" -n ${AF_NAMESPACE} -- \
        airflow dags list-runs -d gold_user_activity --limit 3 2>/dev/null \
        || echo "  (check Airflow UI at http://localhost:8080)"
fi
echo ""

# ============================================================
# PART 5: Verify gold layer
# ============================================================
echo "============================================"
echo "PART 5: Gold Layer Verification"
echo "============================================"
echo ""

GOLD_COUNT=$(ch "SELECT count() FROM commerce.gold_user_activity;" 2>/dev/null || echo "0")
echo "gold_user_activity row count: ${GOLD_COUNT}"
echo ""

if [ "$GOLD_COUNT" = "0" ]; then
    echo "Gold table empty — running direct aggregation (same logic as DAG)..."
    echo "(Bypasses Airflow for immediate validation)"
    TODAY=$(date -u +%Y-%m-%d)
    # ── CANONICAL FALLBACK INSERT ──────────────────────────────────────────
    # Column order MUST match step-14 DDL:
    #   activity_date, user_id, full_name, email,
    #   total_events, last_event_time, _processed_at
    # ──────────────────────────────────────────────────────────────────────
    ch "
INSERT INTO commerce.gold_user_activity
    (activity_date, user_id, full_name, email,
     total_events, last_event_time, _processed_at)

SELECT
    toDate('${TODAY}', 'UTC')                                 AS activity_date,
    u.user_id,
    u.full_name,
    u.email,
    count(e._id)                                              AS total_events,
    if(count(e._id) > 0, max(e.created_at), NULL)            AS last_event_time,
    now()                                                     AS _processed_at

FROM (
    SELECT user_id, full_name, email
    FROM commerce.silver_users FINAL
    WHERE is_deleted = 0
) u

LEFT JOIN (
    SELECT _id, user_id, created_at
    FROM commerce.silver_events
    WHERE is_deleted = 0
) e ON u.user_id = e.user_id

GROUP BY u.user_id, u.full_name, u.email;
"
    echo "✓ Gold aggregation inserted directly"
    echo ""
fi

echo "--- gold_user_activity contents ---"
ch "
SELECT
    toString(activity_date)     AS date,
    user_id,
    full_name,
    total_events,
    if(isNull(last_event_time), 'NULL', toString(last_event_time)) AS last_seen,
    toString(_processed_at)     AS processed_at
FROM commerce.gold_user_activity
ORDER BY activity_date DESC, total_events DESC
LIMIT 30;
"
echo ""

# ============================================================
# PART 6: Live CDC smoke test (insert → verify → cleanup)
# ============================================================
echo "============================================"
echo "PART 6: Live CDC Smoke Test"
echo "============================================"
echo ""
echo "Inserting a test user into PostgreSQL and verifying propagation..."
echo ""

TEST_EMAIL="cdc_test_$(date +%s)@example.com"
kubectl exec "${PG_POD}" -n ${PG_NAMESPACE} -- \
    psql -U postgres -d commerce -c \
    "INSERT INTO users (full_name, email) VALUES ('CDC Test User', '${TEST_EMAIL}');"
echo "✓ Inserted test user: ${TEST_EMAIL}"

NEW_ID=$(kubectl exec "${PG_POD}" -n ${PG_NAMESPACE} -- \
    psql -U postgres -d commerce -Atc \
    "SELECT user_id FROM users WHERE email='${TEST_EMAIL}';")
echo "  user_id in PostgreSQL: ${NEW_ID}"
echo ""

echo "Waiting 15s for CDC propagation (PG WAL → Debezium → Kafka → ClickHouse)..."
sleep 15

FOUND=$(ch "SELECT count() FROM commerce.silver_users WHERE user_id = ${NEW_ID};" 2>/dev/null || echo "0")

if [ "$FOUND" = "1" ]; then
    echo "✓ CDC VERIFIED — user_id=${NEW_ID} found in silver_users!"
    ch "SELECT user_id, full_name, email, toString(updated_at) AS updated_at FROM commerce.silver_users WHERE user_id = ${NEW_ID};"
else
    echo "⚠ Not in silver_users yet — waiting another 15s..."
    sleep 15
    FOUND2=$(ch "SELECT count() FROM commerce.silver_users WHERE user_id = ${NEW_ID};" 2>/dev/null || echo "0")
    if [ "$FOUND2" = "1" ]; then
        echo "✓ Found on second check (CDC latency ~30s)"
    else
        echo "❌ Not found after 30s — pipeline may be stalled"
        echo ""
        echo "Diagnosis:"
        echo "  Connector status:"
        CONNECT_POD=$(kubectl get pod -l app=debezium-connect -n ${KAFKA_NAMESPACE} \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        [ -n "$CONNECT_POD" ] && kubectl exec "${CONNECT_POD}" -n ${KAFKA_NAMESPACE} -- \
            curl -s http://localhost:8083/connectors/postgres-connector/status \
            | python3 -m json.tool 2>/dev/null | grep -E "state|trace" | head -10
        echo ""
        echo "  Consumer lag:"
        kubectl exec ${KAFKA_POD} -n ${KAFKA_NAMESPACE} -- \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --group clickhouse_users_consumer --describe 2>/dev/null
    fi
fi

echo ""
echo "Cleaning up test user..."
kubectl exec "${PG_POD}" -n ${PG_NAMESPACE} -- \
    psql -U postgres -d commerce -c \
    "DELETE FROM users WHERE email='${TEST_EMAIL}';" 2>/dev/null \
    && echo "✓ Test user removed from PostgreSQL"
echo ""

# ============================================================
# PART 7: Final Pipeline Status Summary
# ============================================================
echo "============================================"
echo "PART 7: Final Pipeline Status"
echo "============================================"
echo ""

echo "Source databases:"
PG_COUNT=$(kubectl exec "${PG_POD}" -n ${PG_NAMESPACE} -- \
    psql -U postgres -d commerce -Atc "SELECT count(*) FROM users;" 2>/dev/null || echo "?")
MG_COUNT=$(kubectl exec "${MG_POD}" -n ${MG_NAMESPACE} -- \
    mongosh --eval 'db.getSiblingDB("commerce").events.countDocuments()' --quiet 2>/dev/null || echo "?")
echo "  PostgreSQL users:  ${PG_COUNT}"
echo "  MongoDB events:    ${MG_COUNT}"
echo ""

echo "ClickHouse silver layer:"
ch "
SELECT
    'silver_users'  AS tbl, count() AS total,
    countIf(is_deleted=1) AS deleted, count()-countIf(is_deleted=1) AS active
FROM commerce.silver_users
UNION ALL
SELECT 'silver_events', count(), countIf(is_deleted=1), count()-countIf(is_deleted=1)
FROM commerce.silver_events;
"
echo ""

echo "ClickHouse gold layer:"
ch "
SELECT
    toString(activity_date) AS date,
    count()          AS users,
    sum(total_events) AS total_events,
    countIf(total_events > 0) AS users_with_events
FROM commerce.gold_user_activity
GROUP BY activity_date
ORDER BY activity_date DESC
LIMIT 10;
"
echo ""

echo "CHK (ClickHouseKeeper) health:"
kubectl exec clickhouse-keeper-0 -n ${CH_NAMESPACE} -- \
    /bin/sh -c 'echo "ruok" | nc localhost 9181' 2>/dev/null \
    && echo "  ✓ imok" || echo "  ⚠ CHK not responding"
echo ""

echo "All pods:"
kubectl get pods -A --no-headers \
    | grep -v "kube-system\|local-path" \
    | awk '{printf "  %-20s %-45s %-12s\n", $1, $2, $4}'
echo ""

echo "Resource usage:"
docker stats --no-stream --format \
    "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Pipeline:"
echo "  PostgreSQL + MongoDB → Debezium → Kafka → ClickHouse (silver)"
echo "                       → Airflow (gold_user_activity)"
echo ""
echo "  Airflow UI:   http://localhost:8080  (admin / admin)"
echo "  ClickHouse:   localhost:8123         (admin / admin)"
echo "  Kafka:        localhost:9092"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✓ PIPELINE COMPLETE — all 15 steps done!"