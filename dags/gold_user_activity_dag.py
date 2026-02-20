"""
gold_user_activity_dag.py
=========================
Daily gold layer aggregation for the Real-Time CDC Data Lakehouse.

Pipeline:
    silver_users FINAL  ⨝  silver_events FINAL  →  gold_user_activity

Schedule:   @daily (midnight UTC) — processes the *previous* calendar day
Catchup:    True  — backfills from start_date automatically
Idempotent: DELETE WHERE activity_date=ds, then INSERT (max_active_runs=1
            serialises concurrent backfill runs to avoid race conditions)

Tasks
-----
  0. check_silver_data         Assert silver_users and silver_events are non-empty.
                                Fails loudly if the CDC pipeline has stalled rather
                                than silently producing an empty gold table.

  1. create_gold_table          Idempotent DDL (IF NOT EXISTS). Safe to re-run.

  2. delete_existing_partition  DELETE WHERE activity_date = ds. Primary idempotency
                                mechanism — reruns are always clean.

  3. insert_gold_activity       LEFT JOIN aggregate. UTC-safe date filtering.
                                last_event_time = NULL (not epoch 1970) for users
                                with zero events on that day.

  4. verify_gold_output         Pretty-print per-user results to the Airflow task log.

  5. optimize_silver_tables     OPTIMIZE TABLE FINAL on both silver tables to prevent
                                ReplacingMergeTree FINAL query degradation over time.

Schema (gold_user_activity)
---------------------------
    activity_date   Date
    user_id         Int32
    full_name       String          -- empty string for orphan events (deleted users)
    email           String          -- empty string for orphan events
    total_events    UInt64
    last_event_time Nullable(DateTime64(3, 'UTC'))   -- NULL when total_events = 0
    _processed_at   DateTime        -- wall-clock time of this DAG run

Engine: ReplacingMergeTree(_processed_at)
         Secondary deduplication safety net. Primary idempotency is DELETE+INSERT.

Edit this file directly to change the DAG logic, schedule, or ClickHouse query.
Step 14 (step-14-deploy-airflow.sh) reads this file and packages it into a
Kubernetes ConfigMap that is mounted into Airflow's webserver and scheduler pods.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── ClickHouse connection ─────────────────────────────────────────────────────
CH_HOST     = "chi-analytics-main-0-0.clickhouse-operator.svc.cluster.local"
CH_PORT     = 8123
CH_USER     = "admin"
CH_PASSWORD = "admin"
CH_DATABASE = "commerce"

# ── DAG defaults ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


# ── ClickHouse helpers ────────────────────────────────────────────────────────

def _ch_post(sql: str) -> str:
    """Execute a write query against ClickHouse via HTTP POST."""
    params = urllib.parse.urlencode({
        "query":    sql,
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    req = urllib.request.Request(
        f"http://{CH_HOST}:{CH_PORT}/?{params}", method="POST"
    )
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read().decode().strip()
        if body:
            print(f"  CH: {body[:400]}")
        return body


def _ch_select(sql: str) -> list:
    """Execute a SELECT and return rows as a list of lists (JSONCompact)."""
    params = urllib.parse.urlencode({
        "query":    sql + " FORMAT JSONCompact",
        "user":     CH_USER,
        "password": CH_PASSWORD,
        "database": CH_DATABASE,
    })
    with urllib.request.urlopen(
        f"http://{CH_HOST}:{CH_PORT}/?{params}", timeout=30
    ) as r:
        return json.loads(r.read())["data"]


# ── Task implementations ──────────────────────────────────────────────────────

def check_silver_data(**ctx):
    """
    Task 0 — fail-fast silver layer health check.

    Asserts both silver tables are non-empty before the aggregation runs.
    Without this guard, a stalled CDC pipeline produces a silently empty
    gold table that looks identical to 'no users had events today'.
    """
    print("=== [Task 0] Silver layer health check ===")
    u = int(_ch_select("SELECT count() FROM commerce.silver_users")[0][0])
    e = int(_ch_select("SELECT count() FROM commerce.silver_events")[0][0])
    print(f"  silver_users:  {u} rows")
    print(f"  silver_events: {e} rows")

    if u == 0:
        raise ValueError(
            "silver_users is EMPTY — CDC pipeline has stalled.\n"
            "Remediation: run step-15-e2e-validate.sh Part 2 to auto-reset "
            "consumer offsets, or check the postgres-connector status in Debezium."
        )
    if e == 0:
        raise ValueError(
            "silver_events is EMPTY — MongoDB CDC pipeline has stalled.\n"
            "Remediation: check mongodb-connector task state and "
            "clickhouse_events_consumer lag."
        )

    print("  ✓ Silver tables are populated — safe to aggregate")
    return {"silver_users": u, "silver_events": e}


def create_gold_table(**ctx):
    """
    Task 1 — idempotent DDL.

    Uses IF NOT EXISTS so reruns and backfills never drop existing data.
    The canonical schema is also created by step-14 before Airflow starts —
    this task is a safety net for cold-start scenarios.
    """
    print("=== [Task 1] Ensure gold_user_activity table exists ===")
    _ch_post("""
        CREATE TABLE IF NOT EXISTS commerce.gold_user_activity
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
        ORDER BY (activity_date, user_id)
        PARTITION BY toYYYYMM(activity_date)
        SETTINGS index_granularity = 8192
    """)
    print("  ✓ gold_user_activity table ensured")


def delete_existing_partition(**ctx):
    """
    Task 2 — primary idempotency mechanism.

    Deletes all rows for activity_date = ds before the INSERT.
    Combined with max_active_runs=1, this guarantees exactly-once
    semantics per calendar day even during backfill runs.
    """
    ds = ctx["ds"]
    print(f"=== [Task 2] Delete existing rows for activity_date={ds} ===")
    _ch_post(
        f"ALTER TABLE commerce.gold_user_activity "
        f"DELETE WHERE activity_date = toDate('{ds}', 'UTC')"
    )
    # ClickHouse mutations are asynchronous — wait for the delete to settle
    # before the INSERT begins to avoid a transient double-count window.
    time.sleep(5)
    print(f"  ✓ Cleared existing rows for {ds}")


def insert_gold_activity(**ctx):
    """
    Task 3 — core aggregation query.

    LEFT JOIN ensures users with zero events on ds still appear in the output
    (total_events=0, last_event_time=NULL). This matters for SLA monitoring
    and completeness checks downstream.

    UTC-safety: toDate(created_at, 'UTC') prevents midnight-boundary
    misassignment when the ClickHouse server locale is not UTC.
    """
    ds = ctx["ds"]
    print(f"=== [Task 3] Insert gold aggregation for {ds} ===")
    _ch_post(f"""
        INSERT INTO commerce.gold_user_activity
            (activity_date, user_id, full_name, email,
             total_events, last_event_time, _processed_at)

        SELECT
            toDate('{ds}', 'UTC')                                  AS activity_date,
            u.user_id,
            u.full_name,
            u.email,
            count(e._id)                                           AS total_events,
            if(count(e._id) > 0, max(e.created_at), NULL)         AS last_event_time,
            now()                                                  AS _processed_at

        FROM (
            SELECT user_id, full_name, email
            FROM   commerce.silver_users FINAL
            WHERE  is_deleted = 0
        ) AS u

        LEFT JOIN (
            SELECT _id, user_id, created_at
            FROM   commerce.silver_events FINAL
            WHERE  toDate(created_at, 'UTC') = toDate('{ds}', 'UTC')
              AND  is_deleted = 0
        ) AS e ON u.user_id = e.user_id

        GROUP BY u.user_id, u.full_name, u.email
    """)

    rows = _ch_select(
        f"SELECT count(), sum(total_events) FROM commerce.gold_user_activity "
        f"WHERE activity_date = toDate('{ds}', 'UTC')"
    )
    user_rows  = int(rows[0][0])
    total_evts = int(rows[0][1])
    print(f"  ✓ Inserted {user_rows} user rows with {total_evts} total events for {ds}")

    if user_rows == 0:
        raise ValueError(
            f"Gold INSERT produced 0 rows for {ds}. "
            "silver_users may have become empty mid-run."
        )


def verify_gold_output(**ctx):
    """
    Task 4 — result verification.

    Prints a human-readable table of per-user activity to the Airflow task log.
    Users with events are marked ● ; users with no events that day are marked ○.
    """
    ds = ctx["ds"]
    print(f"=== [Task 4] Verify gold_user_activity for {ds} ===")
    rows = _ch_select(f"""
        SELECT
            activity_date,
            user_id,
            full_name,
            email,
            total_events,
            if(isNull(last_event_time), 'NULL', toString(last_event_time)) AS last_seen
        FROM  commerce.gold_user_activity FINAL
        WHERE activity_date = toDate('{ds}', 'UTC')
        ORDER BY total_events DESC, user_id
        LIMIT 50
    """)

    print(f"\n{'=' * 72}")
    print(f"  gold_user_activity — {ds}  ({len(rows)} users)")
    print(f"{'=' * 72}")
    print(f"  {'user_id':>7}  {'full_name':<22}  {'events':>6}  last_event")
    print(f"  {'-' * 68}")
    for r in rows:
        _, uid, name, email, total, last_seen = r
        marker = "●" if int(total) > 0 else "○"
        print(f"  {marker} {uid:>6}  {(name or '(deleted)'):<22}  {total:>6}  {last_seen}")

    with_events = sum(1 for r in rows if int(r[4]) > 0)
    print(f"\n  Summary: {len(rows)} users total, {with_events} had events on {ds}")


def optimize_silver_tables(**ctx):
    """
    Task 5 — periodic silver layer maintenance.

    Forces a FINAL merge on both ReplacingMergeTree silver tables.
    Without this, parts accumulate faster than background merges process them
    and FINAL queries (used in the INSERT above) slow down linearly with part count.
    Running OPTIMIZE after each gold run keeps part counts bounded.
    """
    print("=== [Task 5] Optimize silver tables ===")
    _ch_post("OPTIMIZE TABLE commerce.silver_users FINAL")
    print("  ✓ silver_users optimized")
    _ch_post("OPTIMIZE TABLE commerce.silver_events FINAL")
    print("  ✓ silver_events optimized")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="gold_user_activity",
    description=(
        "Daily gold layer: silver_users ⨝ silver_events → gold_user_activity. "
        "Idempotent (DELETE+INSERT per day). catchup=True for historical backfill."
    ),
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 1),
    catchup=True,
    max_active_runs=1,          # serialise backfill runs — no DELETE/INSERT races
    default_args=DEFAULT_ARGS,
    tags=["gold", "daily", "clickhouse", "idempotent"],
    doc_md=__doc__,
) as dag:

    t0 = PythonOperator(task_id="check_silver_data",         python_callable=check_silver_data)
    t1 = PythonOperator(task_id="create_gold_table",         python_callable=create_gold_table)
    t2 = PythonOperator(task_id="delete_existing_partition", python_callable=delete_existing_partition)
    t3 = PythonOperator(task_id="insert_gold_activity",      python_callable=insert_gold_activity)
    t4 = PythonOperator(task_id="verify_gold_output",        python_callable=verify_gold_output)
    t5 = PythonOperator(task_id="optimize_silver_tables",    python_callable=optimize_silver_tables)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5