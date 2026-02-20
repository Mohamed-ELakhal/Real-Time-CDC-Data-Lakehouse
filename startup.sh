#!/bin/bash
# ============================================================
#  Real-Time CDC Data Lakehouse — startup.sh
#  One-command deployment with intelligent step detection.
#
#  Usage:
#    ./startup.sh                   # Full deploy from step 1 (install tools → pipeline)
#    ./startup.sh --from 3          # Resume from Kind cluster (tools already installed)
#    ./startup.sh --from 9          # Resume from Kafka
#    ./startup.sh --only 13         # Run a single step
#    ./startup.sh --dry-run         # Show what would run, touch nothing
#    ./startup.sh --yes             # Non-interactive (CI/CD mode)
#    ./startup.sh --help
# ============================================================

set +e

# ── ANSI colours ─────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'
B='\033[0;34m'; C='\033[0;36m'; M='\033[0;35m'
W='\033[1;37m'; DIM='\033[2m'; NC='\033[0m'
TICK="${G}✔${NC}"; CROSS="${R}✘${NC}"; SKIP="${Y}⊘${NC}"

# ── Project identity ─────────────────────────────────────────
PROJECT_NAME="Real-Time CDC Data Lakehouse"
CLUSTER_NAME="cdc-lakehouse"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/scripts" && pwd)"
STATE_FILE="/tmp/.cdc_lakehouse_state"

# ── Defaults ─────────────────────────────────────────────────
FROM_STEP=1          # default: start from the very beginning
ONLY_STEP=""
DRY_RUN=false
AUTO_YES=false
MAX_RETRIES=3
RETRY_DELAY=12       # seconds between automatic retries

# ── Step registry ─────────────────────────────────────────────
declare -A STEP_LABEL STEP_SCRIPT

STEP_LABEL[1]="Install Prerequisites"
STEP_LABEL[2]="System Pre-flight Checks"
STEP_LABEL[3]="Create Kind Cluster"
STEP_LABEL[4]="Install Strimzi Operator"
STEP_LABEL[5]="Install ClickHouse Operator"
STEP_LABEL[6]="Deploy PostgreSQL"
STEP_LABEL[7]="Deploy MongoDB"
STEP_LABEL[8]="Populate Sample Data"
STEP_LABEL[9]="Deploy Kafka (KRaft)"
STEP_LABEL[10]="Deploy Kafka Connect"
STEP_LABEL[11]="Configure Debezium Connectors"
STEP_LABEL[12]="Deploy ClickHouseKeeper + ClickHouse"
STEP_LABEL[13]="Create Silver Layer"
STEP_LABEL[14]="Deploy Airflow + Gold DAG"
STEP_LABEL[15]="End-to-End Validation"

STEP_SCRIPT[1]="step-01-install-prerequisites.sh"
STEP_SCRIPT[2]="step-02-preflight-checks.sh"
STEP_SCRIPT[3]="step-03-create-cluster.sh"
STEP_SCRIPT[4]="step-04-install-strimzi.sh"
STEP_SCRIPT[5]="step-05-install-clickhouse-operator.sh"
STEP_SCRIPT[6]="step-06-deploy-postgres.sh"
STEP_SCRIPT[7]="step-07-deploy-mongodb.sh"
STEP_SCRIPT[8]="step-08-populate-data.sh"
STEP_SCRIPT[9]="step-09-deploy-kafka.sh"
STEP_SCRIPT[10]="step-10-deploy-kafka-connect.sh"
STEP_SCRIPT[11]="step-11-configure-debezium.sh"
STEP_SCRIPT[12]="step-12-deploy-clickhouse.sh"
STEP_SCRIPT[13]="step-13-create-silver-layer.sh"
STEP_SCRIPT[14]="step-14-deploy-airflow.sh"
STEP_SCRIPT[15]="step-15-e2e-validate.sh"

STEPS=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15)

# ── Logging ──────────────────────────────────────────────────
log_info()  { echo -e "${B}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${G}[OK]${NC}    $*"; }
log_warn()  { echo -e "${Y}[WARN]${NC}  $*"; }
log_err()   { echo -e "${R}[ERROR]${NC} $*"; }
log_step()  { echo -e "\n${W}╔══ Step $1 · ${STEP_LABEL[$1]} ══╗${NC}"; }
log_skip()  { echo -e "  ${SKIP} ${DIM}Step $1 — ${STEP_LABEL[$1]} — already complete, skipping${NC}"; }
separator() { echo -e "${DIM}──────────────────────────────────────────────────────────────${NC}"; }
big_sep()   { echo -e "\n${C}══════════════════════════════════════════════════════════════${NC}\n"; }

# ── Argument parsing ──────────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --from|-f)
                FROM_STEP="$2"; shift 2 ;;
            --only|-o)
                ONLY_STEP="$2"; FROM_STEP="$2"; shift 2 ;;
            --dry-run|-n)
                DRY_RUN=true; shift ;;
            --yes|-y)
                AUTO_YES=true; shift ;;
            --retries)
                MAX_RETRIES="$2"; shift 2 ;;
            --help|-h)
                print_help; exit 0 ;;
            *)
                log_err "Unknown argument: $1"
                echo "Run with --help for usage."
                exit 1 ;;
        esac
    done
}

print_help() {
    cat <<EOF

${W}${PROJECT_NAME} — startup.sh${NC}

${W}USAGE${NC}
  ./startup.sh [OPTIONS]

${W}OPTIONS${NC}
  --from STEP         Resume from this step (skips all steps before it)
  --only STEP         Run exactly one step and exit
  --dry-run           Show which steps would run without executing anything
  --yes               Non-interactive mode — no confirmation prompts (CI/CD)
  --retries N         Max automatic retries per step (default: 3)
  --help              Show this message

${W}STEP NUMBERS${NC}
$(for s in "${STEPS[@]}"; do printf "  %2d  %s\n" "$s" "${STEP_LABEL[$s]}"; done)

${W}EXAMPLES${NC}
  ./startup.sh                    # Full deploy from scratch (step 1 → 15)
  ./startup.sh --from 3           # Skip tool install, resume from Kind cluster
  ./startup.sh --from 9           # Resume from Kafka after a partial failure
  ./startup.sh --only 11          # Re-run Debezium connector configuration only
  ./startup.sh --dry-run          # Preview what would run without touching anything
  ./startup.sh --yes              # Fully automated, no prompts (CI/CD pipeline)
  ./startup.sh --yes --from 1     # Automated full deploy from scratch

${W}NOTES${NC}
  • Step detection is live: each step is skipped automatically if its
    resources are already running. Running ./startup.sh twice is safe.
  • If a step fails after ${MAX_RETRIES} retries, an interactive menu offers:
    retry / skip / abort-with-cleanup / abort-keep-state.
  • Steps 1–2 are idempotent: already-installed tools are detected and skipped.

EOF
}

# ── Script file verification ──────────────────────────────────
# Called once at startup. Only checks that script files exist on disk.
# Does NOT check whether tools are installed (that is step 1's job).
check_scripts() {
    log_info "Verifying deployment scripts in: ${SCRIPTS_DIR}"
    local missing=()
    local steps_to_check=("${STEPS[@]}")
    [[ -n "$ONLY_STEP" ]] && steps_to_check=("$ONLY_STEP")

    for s in "${steps_to_check[@]}"; do
        [[ $s -lt $FROM_STEP ]] && continue
        local path="${SCRIPTS_DIR}/${STEP_SCRIPT[$s]}"
        if [[ ! -f "$path" ]]; then
            missing+=("${STEP_SCRIPT[$s]}")
        else
            chmod +x "$path"
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_err "Missing script files:"
        printf "  %s\n" "${missing[@]}"
        log_err "Ensure all scripts are present in: ${SCRIPTS_DIR}"
        return 1
    fi
    log_ok "All scripts present and executable."
    return 0
}

# ── Per-step readiness checks ─────────────────────────────────
# Returns 0 = already done (skip), non-zero = needs to run.

is_done_1() {
    # All required CLI tools must be present and Docker daemon running
    for cmd in docker kubectl helm kind curl python3; do
        command -v "$cmd" &>/dev/null || return 1
    done
    docker info &>/dev/null 2>&1 || return 1
}

is_done_2() {
    # Minimum RAM and disk checks pass, and Docker daemon is live
    docker info &>/dev/null 2>&1 || return 1

    local ram_gb=0
    if [[ "$(uname -s)" == "Darwin" ]]; then
        ram_gb=$(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%d", $1/1024/1024/1024}')
    else
        ram_gb=$(awk '/MemAvailable/ {printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null)
    fi
    [[ "${ram_gb:-0}" -ge 6 ]] || return 1   # 6 GB is the absolute floor

    local disk_gb
    disk_gb=$(df -k "$HOME" 2>/dev/null | awk 'NR==2 {printf "%d", $4/1024/1024}')
    [[ "${disk_gb:-0}" -ge 8 ]] || return 1  # 8 GB minimum disk
}

is_done_3() {
    kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$" || return 1
    kubectl --context "kind-${CLUSTER_NAME}" cluster-info &>/dev/null || return 1
}

is_done_4() {
    kubectl get deployment strimzi-cluster-operator -n kafka \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -q "^1$" || return 1
    kubectl get lease strimzi-cluster-operator -n kafka &>/dev/null || return 1
}

is_done_5() {
    kubectl get deployment clickhouse-operator -n clickhouse-operator \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -q "^1$" || return 1
}

is_done_6() {
    local phase
    phase=$(kubectl get pod -l app=postgres -n default \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] || return 1
    kubectl exec -n default "$(kubectl get pod -l app=postgres -n default \
        -o jsonpath='{.items[0].metadata.name}')" \
        -- pg_isready -U postgres &>/dev/null || return 1
}

is_done_7() {
    local phase
    phase=$(kubectl get pod -l app=mongodb -n default \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] || return 1
    local state
    state=$(kubectl exec mongodb-0 -n default -- \
        mongosh --eval "rs.status().myState" --quiet 2>/dev/null)
    [[ "$state" == "1" ]] || return 1
}

is_done_8() {
    local pg_pod
    pg_pod=$(kubectl get pod -l app=postgres -n default \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -z "$pg_pod" ]] && return 1
    local count
    count=$(kubectl exec "$pg_pod" -n default -- \
        psql -U postgres -d commerce -Atc "SELECT count(*) FROM users;" 2>/dev/null)
    [[ "${count:-0}" -gt 0 ]] 2>/dev/null || return 1
}

is_done_9() {
    local ready
    ready=$(kubectl get kafka kafka-cluster -n kafka \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
    [[ "$ready" == "True" ]] || return 1
}

is_done_10() {
    local phase
    phase=$(kubectl get pod -l app=debezium-connect -n kafka \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    [[ "$phase" == "Running" ]] || return 1
    kubectl exec -n kafka \
        "$(kubectl get pod -l app=debezium-connect -n kafka \
            -o jsonpath='{.items[0].metadata.name}')" \
        -- curl -sf http://localhost:8083/ &>/dev/null || return 1
}

is_done_11() {
    local pod
    pod=$(kubectl get pod -l app=debezium-connect -n kafka \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -z "$pod" ]] && return 1
    local pg_state mg_state
    pg_state=$(kubectl exec "$pod" -n kafka -- \
        curl -sf http://localhost:8083/connectors/postgres-connector/status 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); \
          tasks=d.get('tasks',[]); print(tasks[0]['state'] if tasks else 'NONE')" 2>/dev/null)
    mg_state=$(kubectl exec "$pod" -n kafka -- \
        curl -sf http://localhost:8083/connectors/mongodb-connector/status 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); \
          tasks=d.get('tasks',[]); print(tasks[0]['state'] if tasks else 'NONE')" 2>/dev/null)
    [[ "$pg_state" == "RUNNING" && "$mg_state" == "RUNNING" ]] || return 1
}

is_done_12() {
    local status
    status=$(kubectl get chi analytics -n clickhouse-operator \
        -o jsonpath='{.status.status}' 2>/dev/null)
    [[ "$status" == "Completed" ]] || return 1
    kubectl exec clickhouse-keeper-0 -n clickhouse-operator -- \
        /bin/sh -c 'echo "ruok" | nc localhost 9181' 2>/dev/null \
        | grep -q "imok" || return 1
}

is_done_13() {
    local ch_pod
    ch_pod=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
        -n clickhouse-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -z "$ch_pod" ]] && return 1
    local tables
    tables=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT count() FROM system.tables WHERE database='commerce'
                 AND name IN ('silver_users','silver_events',
                              'kafka_users_queue','kafka_events_queue',
                              'mv_users_from_kafka','mv_events_from_kafka')" 2>/dev/null)
    [[ "$tables" == "6" ]] || return 1
    local ucount
    ucount=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT count() FROM commerce.silver_users" 2>/dev/null)
    [[ "${ucount:-0}" -gt 0 ]] 2>/dev/null || return 1
}

is_done_14() {
    local ws_phase
    ws_phase=$(kubectl get pod -l component=webserver -n airflow \
        -o jsonpath='{.items[0].status.phase}' 2>/dev/null)
    [[ "$ws_phase" == "Running" ]] || return 1
    local ch_pod
    ch_pod=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
        -n clickhouse-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -z "$ch_pod" ]] && return 1
    kubectl exec "$ch_pod" -n clickhouse-operator -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT 1 FROM system.tables
                 WHERE database='commerce' AND name='gold_user_activity'" \
        2>/dev/null | grep -q "1" || return 1
}

is_done_15() {
    local ch_pod
    ch_pod=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
        -n clickhouse-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -z "$ch_pod" ]] && return 1
    local gcount
    gcount=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
        clickhouse-client --user admin --password admin \
        --query "SELECT count() FROM commerce.gold_user_activity" 2>/dev/null)
    [[ "${gcount:-0}" -gt 0 ]] 2>/dev/null || return 1
}

step_is_already_done() {
    "is_done_${1}" 2>/dev/null
}

# ── Run a single step with retry + interactive failure handler ─
run_step() {
    local s="$1"
    local script="${SCRIPTS_DIR}/${STEP_SCRIPT[$s]}"
    local attempt=1

    log_step "$s"

    while [[ $attempt -le $MAX_RETRIES ]]; do
        if [[ $attempt -gt 1 ]]; then
            log_warn "Retry ${attempt}/${MAX_RETRIES} in ${RETRY_DELAY}s..."
            sleep "$RETRY_DELAY"
        fi

        if $DRY_RUN; then
            log_ok "[DRY-RUN] Would execute: $script"
            return 0
        fi

        bash "$script"
        local rc=$?

        if [[ $rc -eq 0 ]]; then
            echo "$s" >> "$STATE_FILE"
            log_ok "Step $s complete."
            return 0
        fi

        log_err "Step $s exited with code $rc (attempt ${attempt}/${MAX_RETRIES})"
        attempt=$((attempt + 1))
    done

    # All automatic retries exhausted
    if $AUTO_YES; then
        log_err "Step $s failed after ${MAX_RETRIES} attempts. Aborting (--yes mode)."
        return 1
    fi

    # Interactive recovery menu
    while true; do
        echo ""
        echo -e "${R}╔══ Step $s FAILED — ${STEP_LABEL[$s]} ══╗${NC}"
        echo -e "  Failed after ${MAX_RETRIES} automatic retry attempts."
        echo ""
        echo -e "  ${W}1)${NC} Retry this step (fresh attempts)"
        echo -e "  ${W}2)${NC} Skip and continue ${R}(not recommended — may break later steps)${NC}"
        echo -e "  ${W}3)${NC} Abort and clean up all resources deployed so far"
        echo -e "  ${W}4)${NC} Abort and keep current state (for manual debugging)"
        echo ""
        read -rp "  Choice [1-4]: " choice
        echo ""

        case "$choice" in
            1)
                attempt=1
                while [[ $attempt -le $MAX_RETRIES ]]; do
                    bash "$script" && {
                        echo "$s" >> "$STATE_FILE"
                        log_ok "Step $s complete."
                        return 0
                    }
                    attempt=$((attempt + 1))
                    [[ $attempt -le $MAX_RETRIES ]] && sleep "$RETRY_DELAY"
                done
                ;;
            2)
                log_warn "Skipping step $s. Downstream steps may fail."
                read -rp "  Confirm skip? (yes/no): " confirm
                [[ "$confirm" =~ ^[Yy][Ee][Ss]$ ]] && {
                    log_warn "Step $s skipped."
                    return 0
                }
                ;;
            3)
                log_info "Triggering cleanup up to step $s..."
                local cleanup_script
                cleanup_script="$(dirname "${BASH_SOURCE[0]}")/cleanup.sh"
                if [[ -f "$cleanup_script" ]]; then
                    bash "$cleanup_script" --mode full --up-to-step "$s" --yes
                else
                    log_warn "Cleanup script not found. Clean up manually."
                fi
                exit 1
                ;;
            4)
                log_warn "Aborting without cleanup. State preserved for debugging."
                exit 1
                ;;
            *)
                log_err "Invalid choice. Enter 1, 2, 3, or 4."
                ;;
        esac
    done
}

# ── Progress bar ──────────────────────────────────────────────
draw_progress() {
    local done_count="$1" total="$2"
    local pct=$(( done_count * 100 / total ))
    local filled=$(( done_count * 30 / total ))
    local bar=""
    for ((i=0; i<filled; i++));  do bar+="█"; done
    for ((i=filled; i<30; i++)); do bar+="░"; done
    echo -e "  ${G}[${bar}]${NC} ${pct}% (${done_count}/${total} steps)"
}

# ── Main deploy loop ──────────────────────────────────────────
deploy() {
    local start_ts; start_ts=$(date +%s)
    local steps_run=0 steps_skipped=0

    big_sep
    echo -e "${W}  ${PROJECT_NAME}${NC}"
    echo -e "${DIM}  Intelligent deployment — auto-detects completed steps${NC}"
    big_sep

    log_info "Scripts:    ${SCRIPTS_DIR}"
    log_info "Cluster:    ${CLUSTER_NAME}"
    [[ -n "$ONLY_STEP" ]] && log_info "Only step:  ${ONLY_STEP}" \
                          || log_info "From step:  ${FROM_STEP}"
    $DRY_RUN && log_warn "DRY-RUN mode — no changes will be made."
    echo ""

    if ! $AUTO_YES && ! $DRY_RUN; then
        if [[ $FROM_STEP -le 1 ]]; then
            echo -e "  This will install required tools (if missing) and deploy the full pipeline."
        fi
        read -rp "  Start deployment? (yes/no): " ans
        [[ "$ans" =~ ^[Yy][Ee][Ss]$ ]] || { log_info "Deployment cancelled."; exit 0; }
    fi

    > "$STATE_FILE"

    local effective_steps=("${STEPS[@]}")
    [[ -n "$ONLY_STEP" ]] && effective_steps=("$ONLY_STEP")

    # Build the filtered list of steps to evaluate
    local steps_to_run=()
    for s in "${effective_steps[@]}"; do
        [[ $s -lt $FROM_STEP ]] && continue
        steps_to_run+=("$s")
    done
    local total_to_run=${#steps_to_run[@]}

    echo ""
    log_info "Steps to evaluate: ${steps_to_run[*]}"
    separator

    local completed_so_far=0
    for s in "${steps_to_run[@]}"; do
        if step_is_already_done "$s"; then
            log_skip "$s"
            steps_skipped=$((steps_skipped + 1))
        else
            run_step "$s" || exit 1
            steps_run=$((steps_run + 1))
        fi
        completed_so_far=$((completed_so_far + 1))
        draw_progress "$completed_so_far" "$total_to_run"
    done

    local end_ts; end_ts=$(date +%s)
    local elapsed=$(( end_ts - start_ts ))
    local mins=$(( elapsed / 60 ))
    local secs=$(( elapsed % 60 ))

    big_sep
    echo -e "${G}  ✔ ${W}${PROJECT_NAME} — Deployment Complete${NC}"
    echo ""
    log_ok "Duration:                  ${mins}m ${secs}s"
    log_ok "Steps executed:            ${steps_run}"
    log_ok "Steps skipped (done):      ${steps_skipped}"
    big_sep
}

# ── Post-deploy access summary ────────────────────────────────
show_summary() {
    $DRY_RUN && return
    [[ $FROM_STEP -gt 14 ]] && return  # skip if only validation ran

    local ch_pod pg_pod mg_pod
    ch_pod=$(kubectl get pod -l clickhouse.altinity.com/chi=analytics \
        -n clickhouse-operator -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    pg_pod=$(kubectl get pod -l app=postgres \
        -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    mg_pod=$(kubectl get pod -l app=mongodb \
        -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

    echo ""
    echo -e "${W}╔══ Pipeline Health ══╗${NC}"
    echo ""

    local pg_count="?"
    [[ -n "$pg_pod" ]] && pg_count=$(kubectl exec "$pg_pod" -n default -- \
        psql -U postgres -d commerce -Atc "SELECT count(*) FROM users;" 2>/dev/null || echo "?")
    echo -e "  ${TICK} PostgreSQL users:       ${pg_count} rows"

    local mg_count="?"
    [[ -n "$mg_pod" ]] && mg_count=$(kubectl exec "$mg_pod" -n default -- \
        mongosh --quiet --eval 'db.getSiblingDB("commerce").events.countDocuments()' \
        2>/dev/null || echo "?")
    echo -e "  ${TICK} MongoDB events:         ${mg_count} documents"

    if [[ -n "$ch_pod" ]]; then
        local su_count se_count gold_count
        su_count=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
            clickhouse-client --user admin --password admin \
            --query "SELECT count() FROM commerce.silver_users" 2>/dev/null || echo "?")
        se_count=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
            clickhouse-client --user admin --password admin \
            --query "SELECT count() FROM commerce.silver_events" 2>/dev/null || echo "?")
        gold_count=$(kubectl exec "$ch_pod" -n clickhouse-operator -- \
            clickhouse-client --user admin --password admin \
            --query "SELECT count() FROM commerce.gold_user_activity" 2>/dev/null || echo "?")
        echo -e "  ${TICK} silver_users:           ${su_count} rows"
        echo -e "  ${TICK} silver_events:          ${se_count} rows"
        echo -e "  ${TICK} gold_user_activity:     ${gold_count} rows"
    fi

    local connect_pod
    connect_pod=$(kubectl get pod -l app=debezium-connect -n kafka \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$connect_pod" ]]; then
        for conn in postgres-connector mongodb-connector; do
            local state
            state=$(kubectl exec "$connect_pod" -n kafka -- \
                curl -sf "http://localhost:8083/connectors/${conn}/status" 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); \
                  tasks=d.get('tasks',[]); \
                  print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null \
                || echo "UNKNOWN")
            local icon="${TICK}"; [[ "$state" != "RUNNING" ]] && icon="${CROSS}"
            echo -e "  ${icon} ${conn}: ${state}"
        done
    fi

    echo ""
    echo -e "${W}╔══ Access Points ══╗${NC}"
    echo ""
    echo -e "  ${C}Airflow UI${NC}       →  http://localhost:8080   (admin / admin)"
    echo -e "  ${C}ClickHouse HTTP${NC}  →  http://localhost:8123   (admin / admin)"
    echo -e "  ${C}Kafka${NC}            →  localhost:9092"
    echo -e "  ${C}PostgreSQL${NC}       →  localhost:5432          (postgres / postgres)"
    echo -e "  ${C}MongoDB${NC}          →  localhost:27017"
    echo ""
    echo -e "${W}╔══ Useful Commands ══╗${NC}"
    echo ""
    echo -e "  ${DIM}# ClickHouse CLI${NC}"
    echo -e "  clickhouse-client --host localhost --port 9000 --user admin --password admin"
    echo ""
    echo -e "  ${DIM}# Trigger gold DAG for yesterday${NC}"
    echo -e "  kubectl exec -n airflow \$(kubectl get pod -l component=webserver \\"
    echo -e "    -n airflow -o jsonpath='{.items[0].metadata.name}') -- \\"
    echo -e "    airflow dags trigger gold_user_activity"
    echo ""
    echo -e "  ${DIM}# Run end-to-end validation${NC}"
    echo -e "  ./scripts/step-15-e2e-validate.sh"
    echo ""
    echo -e "  ${DIM}# Light cleanup (keeps cluster + data)${NC}"
    echo -e "  ./cleanup.sh --mode light"
    echo ""
    echo -e "  ${DIM}# Full cleanup (destroy everything)${NC}"
    echo -e "  ./cleanup.sh --mode full"
    echo ""
    separator
}

# ── Entry point ───────────────────────────────────────────────
main() {
    parse_args "$@"

    # For steps 1 and 2, we can't rely on kubectl/kind being present yet,
    # so skip the global prerequisite guard if starting from step 1 or 2.
    if [[ $FROM_STEP -ge 3 ]]; then
        # Verify tools are present before trying to run K8s steps
        local missing=()
        for cmd in docker kubectl helm kind curl python3; do
            command -v "$cmd" &>/dev/null || missing+=("$cmd")
        done
        if [[ ${#missing[@]} -gt 0 ]]; then
            log_err "Required tools missing: ${missing[*]}"
            log_err "Run from step 1 to install them: ./startup.sh --from 1"
            exit 1
        fi
        if ! docker info &>/dev/null 2>&1; then
            log_err "Docker daemon is not running."
            log_err "Start Docker, then re-run: ./startup.sh --from 2"
            exit 1
        fi
    fi

    check_scripts || exit 1
    deploy
    show_summary
}

main "$@"