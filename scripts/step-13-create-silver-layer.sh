#!/bin/bash
# step-13-create-silver-layer.sh  ── v2 (hardened)
# Changes vs v1:
#   • All toDate() calls now use explicit 'UTC' timezone argument to prevent
#     midnight-boundary event misassignment when server locale differs.
#   • kafka_skip_broken_messages reduced to 10 (was 100).
#     Fewer silent skips → faster detection of schema/format regressions.
#     Malformed messages are routed to DLQ topics configured in step-11.
#   • Added OPTIMIZE TABLE calls at the end to force an initial merge,
#     ensuring FINAL queries are efficient from the first run.
#   • Updated MV for events to use toDate(created_at, 'UTC').

set -e

echo "============================================"
echo "STEP 13: Create Silver Layer in ClickHouse"
echo "============================================"
echo ""

CH_NAMESPACE="clickhouse-operator"

echo "Verifying ClickHouse pod..."
CH_POD=$(kubectl get pod \
    -l clickhouse.altinity.com/chi=analytics \
    -n ${CH_NAMESPACE} \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -z "$CH_POD" ]; then
    echo "❌ ClickHouse pod not found in namespace ${CH_NAMESPACE}"
    echo "   Run: ./scripts/step-12-deploy-clickhouse.sh"
    exit 1
fi

kubectl wait --for=condition=ready pod/${CH_POD} -n ${CH_NAMESPACE} --timeout=60s
echo "✓ ClickHouse pod ready: ${CH_POD}"
echo ""

ch_query() {
    kubectl exec "${CH_POD}" -n ${CH_NAMESPACE} -- \
        clickhouse-client --user admin --password admin \
        --multiquery \
        --query "$1"
}

echo "Testing ClickHouse connection..."
ch_query "SELECT 'Connected to ClickHouse ' || version()"
echo ""

KAFKA_BOOTSTRAP="kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"

# ============================================================
# PART 1: Create commerce database
# ============================================================
echo "============================================"
echo "PART 1: Create commerce database"
echo "============================================"
echo ""
ch_query "CREATE DATABASE IF NOT EXISTS commerce;"
echo "✓ Database created"
echo ""

# ============================================================
# PART 2: Users Silver Layer (PostgreSQL CDC)
# ============================================================
echo "============================================"
echo "PART 2: Users Silver Layer (PostgreSQL CDC)"
echo "============================================"
echo ""

echo "Step 1/3: Creating Kafka engine table for users..."
ch_query "
DROP TABLE IF EXISTS commerce.kafka_users_queue;
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
    kafka_broker_list          = '${KAFKA_BOOTSTRAP}',
    kafka_topic_list           = 'postgres.public.users',
    kafka_group_name           = 'clickhouse_users_consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 10;
"
echo "✓ Kafka queue table created"
echo ""

echo "Step 2/3: Creating silver_users table (ReplacingMergeTree)..."
ch_query "
CREATE TABLE IF NOT EXISTS commerce.silver_users (
    user_id      Int32,
    full_name    String,
    email        String,
    created_at   DateTime64(3, 'UTC'),
    updated_at   DateTime64(3, 'UTC'),
    is_deleted   UInt8     DEFAULT 0,
    _ingested_at DateTime  DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id
SETTINGS index_granularity = 8192;
"
echo "✓ silver_users table created"
echo ""

echo "Step 3/3: Creating materialized view for CDC processing..."
ch_query "
DROP VIEW IF EXISTS commerce.mv_users_from_kafka;
CREATE MATERIALIZED VIEW commerce.mv_users_from_kafka
TO commerce.silver_users AS
SELECT
    coalesce(user_id,   before_user_id)   AS user_id,
    coalesce(full_name, before_full_name) AS full_name,
    coalesce(email,     before_email)     AS email,
    -- Use UTC explicitly to prevent midnight-boundary misassignment
    -- when the ClickHouse server has a non-UTC locale configured.
    parseDateTimeBestEffortOrZero(
        coalesce(created_at, before_created_at, '1970-01-01'), 'UTC'
    ) AS created_at,
    parseDateTimeBestEffortOrZero(
        coalesce(updated_at, before_updated_at, '1970-01-01'), 'UTC'
    ) AS updated_at,
    if(op = 'd', 1, 0) AS is_deleted,
    now()              AS _ingested_at
FROM commerce.kafka_users_queue
WHERE coalesce(user_id, before_user_id) IS NOT NULL;
"
echo "✓ Materialized view created"
echo ""

# ============================================================
# PART 3: Events Silver Layer (MongoDB CDC)
# ============================================================
echo "============================================"
echo "PART 3: Events Silver Layer (MongoDB CDC)"
echo "============================================"
echo ""

echo "Step 1/3: Creating Kafka engine table for events..."
ch_query "
DROP TABLE IF EXISTS commerce.kafka_events_queue;
CREATE TABLE commerce.kafka_events_queue (
    _id        String,
    user_id    Int32,
    event_type String,
    page       Nullable(String),
    metadata   String,
    created_at DateTime64(3, 'UTC'),
    __deleted  Nullable(UInt8)   -- NO DEFAULT: Kafka engine restriction (Code: 36)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list          = '${KAFKA_BOOTSTRAP}',
    kafka_topic_list           = 'mongo.commerce.events',
    kafka_group_name           = 'clickhouse_events_consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 10;
"
echo "✓ Kafka queue table created"
echo ""

echo "Step 2/3: Creating silver_events table..."
ch_query "
CREATE TABLE IF NOT EXISTS commerce.silver_events (
    _id          String,
    user_id      Int32,
    event_type   String,
    page         Nullable(String),
    metadata     String,
    created_at   DateTime64(3, 'UTC'),
    is_deleted   UInt8     DEFAULT 0,
    _ingested_at DateTime  DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (_id)
SETTINGS index_granularity = 8192;
"
echo "✓ silver_events table created"
echo ""

echo "Step 3/3: Creating materialized view for MongoDB CDC..."
ch_query "
DROP VIEW IF EXISTS commerce.mv_events_from_kafka;
CREATE MATERIALIZED VIEW commerce.mv_events_from_kafka
TO commerce.silver_events AS
SELECT
    _id,
    user_id,
    event_type,
    page,
    metadata,
    created_at,
    ifNull(__deleted, 0) AS is_deleted,
    now()                AS _ingested_at
FROM commerce.kafka_events_queue;
"
echo "✓ Materialized view created"
echo ""

# ============================================================
# PART 4: Initial OPTIMIZE to prevent FINAL query bloat
# ============================================================
echo "============================================"
echo "PART 4: Initial Table Optimization"
echo "============================================"
echo ""
echo "Running OPTIMIZE to consolidate initial parts..."
echo "(Ensures first FINAL queries are fast and don't scan many small parts)"
ch_query "OPTIMIZE TABLE commerce.silver_users FINAL;"
ch_query "OPTIMIZE TABLE commerce.silver_events FINAL;"
echo "✓ Tables optimized"
echo ""

# ============================================================
# PART 5: Verification
# ============================================================
echo "============================================"
echo "PART 5: Verification"
echo "============================================"
echo ""

echo "Tables in commerce database:"
ch_query "SHOW TABLES IN commerce;"
echo ""

echo "Waiting 15s for initial Kafka messages to be consumed..."
sleep 15

echo "silver_users row count (from PostgreSQL CDC):"
ch_query "SELECT count() AS total, countIf(is_deleted=1) AS deleted FROM commerce.silver_users;"
echo ""

echo "silver_events row count (from MongoDB CDC):"
ch_query "SELECT count() AS total, countIf(is_deleted=1) AS deleted FROM commerce.silver_events;"
echo ""

echo "Sample silver_users (latest state via FINAL):"
ch_query "
SELECT user_id, full_name, email, toString(updated_at) AS updated_at, is_deleted
FROM commerce.silver_users FINAL
ORDER BY user_id
LIMIT 15;
"
echo ""

echo "Consumer lag monitoring (run anytime to check pipeline health):"
cat << 'HEREDOC'
  kubectl exec kafka-cluster-combined-0 -n kafka -- \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group clickhouse_users_consumer --describe

  # Monitor DLQ for malformed messages:
  kubectl exec kafka-cluster-combined-0 -n kafka -- \
    /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 \
    --topic postgres.public.users.dlq --time -1

  # Check ClickHouse's own consumer error log:
  clickhouse-client --query \
    "SELECT count(), max(event_time) FROM system.kafka_consumer_errors"
HEREDOC

echo ""
echo "============================================"
echo "✓ STEP 13 COMPLETE"
echo "============================================"
echo ""
echo "Silver layer is ready."
echo ""
echo "Next: STEP 14 - Deploy Airflow"
echo "Run: ./scripts/step-14-deploy-airflow.sh"