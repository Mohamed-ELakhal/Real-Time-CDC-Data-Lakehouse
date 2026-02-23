#!/bin/bash
# ============================================================
#  Real-Time CDC Data Lakehouse — cleanup.sh
#  Smart teardown with light and full modes.
#
#  Usage:
#    ./cleanup.sh                         # Interactive menu
#    ./cleanup.sh --mode light            # Remove services, keep cluster + PVCs
#    ./cleanup.sh --mode full             # Destroy everything including Kind cluster
#    ./cleanup.sh --mode full --yes       # Non-interactive full teardown (CI mode)
#    ./cleanup.sh --up-to-step 12        # Tear down from step 12 downward
#    ./cleanup.sh --help
#
#  Note: Steps 1 (prerequisites) and 2 (pre-flight) have no
#  Kubernetes resources to clean. Installed system tools (Docker,
#  kubectl, helm, kind) are NEVER removed automatically — removing
#  system-level tools without explicit user intent is dangerous.
# ============================================================

set +e

R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'
B='\033[0;34m'; C='\033[0;36m'; W='\033[1;37m'; DIM='\033[2m'; NC='\033[0m'
TICK="${G}✔${NC}"; SKIP="${Y}⊘${NC}"; CROSS="${R}✘${NC}"

PROJECT_NAME="Real-Time CDC Data Lakehouse"
CLUSTER_NAME="cdc-lakehouse"

MODE=""
AUTO_YES=false
UP_TO_STEP=15
TIMEOUT_NS=120

log_info() { echo -e "${B}[INFO]${NC}  $*"; }
log_ok()   { echo -e "${G}[OK]${NC}    $*"; }
log_warn() { echo -e "${Y}[SKIP]${NC}  $*"; }
log_err()  { echo -e "${R}[ERR]${NC}   $*"; }
log_sec()  { echo -e "\n${W}── $* ──${NC}"; }
separator(){ echo -e "${DIM}──────────────────────────────────────────────────────────────${NC}"; }
big_sep()  { echo -e "\n${C}══════════════════════════════════════════════════════════════${NC}\n"; }

# ── Argument parsing ──────────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --mode|-m)       MODE="$2";         shift 2 ;;
            --up-to-step)    UP_TO_STEP="$2";   shift 2 ;;
            --yes|-y)        AUTO_YES=true;      shift   ;;
            --help|-h)       print_help; exit 0          ;;
            *)               log_err "Unknown argument: $1"; echo "Run --help for usage."; exit 1 ;;
        esac
    done
}

print_help() {
    cat <<EOF

${W}${PROJECT_NAME} — cleanup.sh${NC}

${W}MODES${NC}
  light   Remove all Kubernetes workloads (Helm releases, Deployments, StatefulSets,
          Services, ConfigMaps, Secrets) while preserving the Kind cluster, all
          Persistent Volume Claims, and cluster-scoped CRDs/ClusterRoles.

          Best for: iterating on config, fixing a broken service, fast redeploy.
          Redeploy after light cleanup: ~5–10 min (images cached, no cluster init).

  full    Destroy everything: all workloads, namespaces, PVCs, CRDs, ClusterRoles,
          and the Kind cluster itself. Returns machine to a completely clean state.

          Best for: finishing a demo, switching branches, resolving deep state issues.
          Redeploy after full cleanup: ~40–60 min (fresh image pull + cluster init).

${W}NOTE ON STEPS 1–2${NC}
  Steps 1 (Install Prerequisites) and 2 (Pre-flight Checks) install system-level
  tools (Docker, kubectl, helm, kind). These are NEVER removed automatically.
  To uninstall tools, use your package manager (brew, apt, dnf) manually.

${W}USAGE${NC}
  ./cleanup.sh                          Interactive menu
  ./cleanup.sh --mode light             Remove services, keep cluster and PVCs
  ./cleanup.sh --mode full              Full teardown (destructive)
  ./cleanup.sh --mode full --yes        Non-interactive full teardown
  ./cleanup.sh --up-to-step 12         Tear down from Airflow → ClickHouse only
  ./cleanup.sh --help

${W}STEP MAP${NC} (cleanup runs in reverse — highest first)
  15  End-to-End Validation    (no K8s resources to clean)
  14  Airflow
  13  Silver Layer              (no separate resources — part of ClickHouse)
  12  ClickHouse + Keeper
  10  Kafka Connect / Debezium
  11  Debezium Connectors       (managed by step 10)
   9  Kafka Cluster
   7  MongoDB
   6  PostgreSQL
   5  ClickHouse Operator
   4  Strimzi Operator
   3  Kind Cluster
   2  Pre-flight Checks         (no K8s resources)
   1  Prerequisites             (tools NOT removed — see note above)

EOF
}

# ── Interactive mode selector ─────────────────────────────────
interactive_menu() {
    big_sep
    echo -e "${W}  ${PROJECT_NAME} — Cleanup${NC}"
    big_sep

    echo -e "  Choose a cleanup mode:\n"
    echo -e "  ${W}1)${NC} ${C}Light${NC} — remove all services, keep Kind cluster and PVCs"
    echo -e "       ${DIM}Faster redeploy — images are cached, storage survives${NC}"
    echo ""
    echo -e "  ${W}2)${NC} ${R}Full${NC}  — destroy everything including the Kind cluster"
    echo -e "       ${DIM}Complete reset to pristine state${NC}"
    echo ""
    echo -e "  ${W}3)${NC} Custom — tear down from a specific step downward"
    echo ""
    echo -e "  ${W}4)${NC} Exit without cleaning"
    echo ""

    read -rp "  Choice [1-4]: " choice
    echo ""

    case "$choice" in
        1) MODE="light" ;;
        2) MODE="full"
           echo -e "${R}  ⚠ This will permanently destroy the Kind cluster and all data.${NC}"
           read -rp "  Type 'destroy' to confirm: " confirm
           [[ "$confirm" == "destroy" ]] || { log_info "Aborted."; exit 0; }
           ;;
        3)
            echo -e "\n  Step numbers:"
            printf "    %2d  %s\n" \
                14 "Airflow" \
                12 "ClickHouse + Keeper" \
                10 "Kafka Connect" \
                9  "Kafka Cluster" \
                7  "MongoDB" \
                6  "PostgreSQL" \
                5  "ClickHouse Operator" \
                4  "Strimzi Operator" \
                3  "Kind Cluster"
            echo ""
            read -rp "  Clean up from step [3-15]: " UP_TO_STEP
            [[ "$UP_TO_STEP" =~ ^[0-9]+$ ]] && \
            [[ $UP_TO_STEP -ge 3 ]] && \
            [[ $UP_TO_STEP -le 15 ]] || { log_err "Invalid step."; exit 1; }
            echo ""
            echo -e "  Mode for partial cleanup:"
            echo -e "  ${W}1)${NC} Light (keep cluster + PVCs)"
            echo -e "  ${W}2)${NC} Full  (delete PVCs in scope too)"
            read -rp "  Choice [1/2]: " mmode
            [[ "$mmode" == "2" ]] && MODE="full" || MODE="light"
            ;;
        4) log_info "Cleanup cancelled."; exit 0 ;;
        *) log_err "Invalid choice."; exit 1 ;;
    esac
}

# ── Existence guards (skip if already gone) ───────────────────
ns_exists()          { kubectl get namespace "$1" &>/dev/null; }
resource_exists()    { kubectl get "$1" "$2" ${3:+-n "$3"} &>/dev/null; }
helm_release_exists(){ helm list -n "$2" 2>/dev/null | grep -q "^$1"; }
cluster_exists()     { kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; }

# ── Safe delete helpers ───────────────────────────────────────
safe_delete() {
    local kind="$1" name="$2" ns="$3"
    local ns_flag=(); [[ -n "$ns" ]] && ns_flag=(-n "$ns")
    if resource_exists "$kind" "$name" "$ns"; then
        log_info "Deleting ${kind}/${name}${ns:+ in $ns}..."
        kubectl delete "$kind" "$name" "${ns_flag[@]}" \
            --ignore-not-found=true --timeout=90s 2>&1 | grep -v "NotFound" || true
        log_ok "Deleted ${kind}/${name}."
    else
        log_warn "${kind}/${name}${ns:+ in $ns} — not found, skipped."
    fi
}

safe_delete_namespace() {
    local ns="$1"
    if ns_exists "$ns"; then
        log_info "Deleting namespace '${ns}'..."
        kubectl delete namespace "$ns" --ignore-not-found=true 2>&1 | grep -v "NotFound" || true
        local t=0
        while ns_exists "$ns" && [[ $t -lt $TIMEOUT_NS ]]; do sleep 2; t=$((t+2)); done
        if ns_exists "$ns"; then
            log_warn "Namespace '${ns}' still exists after ${TIMEOUT_NS}s — may need manual removal."
        else
            log_ok "Namespace '${ns}' deleted."
        fi
    else
        log_warn "Namespace '${ns}' — not found, skipped."
    fi
}

safe_delete_pvcs() {
    local ns="$1" selector="$2"
    ns_exists "$ns" || { log_warn "Namespace '${ns}' not found — skipping PVC cleanup."; return; }
    local sel_flag=(); [[ -n "$selector" ]] && sel_flag=(-l "$selector")
    local pvcs
    pvcs=$(kubectl get pvc -n "$ns" "${sel_flag[@]}" \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    if [[ -z "$pvcs" ]]; then
        log_warn "No PVCs found in '${ns}'${selector:+ matching $selector} — skipped."
        return
    fi
    log_info "Deleting PVCs in '${ns}': ${pvcs}"
    for pvc in $pvcs; do
        kubectl delete pvc "$pvc" -n "$ns" \
            --ignore-not-found=true --timeout=60s 2>&1 | grep -v "NotFound" || true
    done
    log_ok "PVCs deleted in '${ns}'."
}

safe_delete_cluster_resources() {
    local rtype="$1" pattern="$2"
    local resources
    resources=$(kubectl get "$rtype" -o name 2>/dev/null | grep "$pattern" || echo "")
    if [[ -z "$resources" ]]; then
        log_warn "No ${rtype} matching '${pattern}' — skipped."
        return
    fi
    log_info "Deleting cluster-scoped ${rtype} matching '${pattern}'..."
    echo "$resources" | xargs -r kubectl delete \
        --ignore-not-found=true --timeout=60s 2>&1 | grep -v "NotFound" || true
    log_ok "Cluster-scoped ${rtype} cleaned."
}

# ── Per-component cleanup functions ──────────────────────────

clean_steps_1_2() {
    # Steps 1 and 2 install system tools and run checks.
    # There are no Kubernetes resources or cluster objects to remove.
    # System tools (Docker, kubectl, helm, kind) are intentionally NOT removed.
    log_sec "Steps 1–2: Prerequisites & Pre-flight"
    echo -e "  ${SKIP} ${DIM}Steps 1–2 install system tools — not removed by this script.${NC}"
    echo -e "  ${DIM}To uninstall tools manually:${NC}"
    if [[ "$(uname -s)" == "Darwin" ]]; then
        echo -e "  ${DIM}  brew uninstall kind helm kubectl && brew uninstall --cask docker${NC}"
    else
        echo -e "  ${DIM}  sudo apt-get remove docker-ce kubectl helm  (Ubuntu/Debian)${NC}"
        echo -e "  ${DIM}  sudo rm /usr/local/bin/kind${NC}"
    fi
    echo ""
}

clean_airflow() {
    log_sec "Step 14 — Airflow"
    if helm_release_exists airflow airflow; then
        log_info "Uninstalling Airflow Helm release..."
        helm uninstall airflow -n airflow --wait --timeout=3m 2>&1 | grep -v "not found" || true
        log_ok "Airflow Helm release uninstalled."
    else
        log_warn "Airflow Helm release not found — skipped."
    fi
    safe_delete_namespace "airflow"
    echo ""
}

clean_clickhouse() {
    log_sec "Step 12 — ClickHouse (CHI)"
    if resource_exists "chi" "analytics" "clickhouse-operator"; then
        log_info "Deleting ClickHouseInstallation 'analytics'..."
        kubectl delete chi analytics -n clickhouse-operator \
            --ignore-not-found=true --timeout=120s 2>&1 | grep -v "NotFound" || true
        log_info "Waiting for ClickHouse pods to terminate..."
        kubectl wait --for=delete pod -l clickhouse.altinity.com/chi=analytics \
            -n clickhouse-operator --timeout=120s 2>&1 | grep -v "no matching" || true
        log_ok "ClickHouse pods terminated."
    else
        log_warn "CHI 'analytics' not found — skipped."
    fi
    if [[ "$MODE" == "full" ]]; then
        safe_delete_pvcs "clickhouse-operator" "clickhouse.altinity.com/chi=analytics"
    else
        log_warn "[light] Keeping ClickHouse PVCs (data preserved for fast redeploy)."
    fi
    echo ""
}

clean_clickhouse_keeper() {
    log_sec "Step 12 — ClickHouseKeeper (CHK)"
    safe_delete "statefulset" "clickhouse-keeper" "clickhouse-operator"
    safe_delete "service"     "clickhouse-keeper" "clickhouse-operator"
    safe_delete "configmap"   "clickhouse-keeper-config" "clickhouse-operator"
    if [[ "$MODE" == "full" ]]; then
        safe_delete_pvcs "clickhouse-operator" "app=clickhouse-keeper"
    else
        log_warn "[light] Keeping CHK PVCs."
    fi
    echo ""
}

clean_kafka_connect() {
    log_sec "Steps 10–11 — Kafka Connect + Debezium"
    safe_delete "deployment" "debezium-connect" "kafka"
    safe_delete "service"    "debezium-connect" "kafka"
    if ns_exists "kafka"; then
        kubectl wait --for=delete pod -l app=debezium-connect \
            -n kafka --timeout=60s 2>&1 | grep -v "no matching" || true
        log_ok "Kafka Connect pods gone."
    fi
    echo ""
}

clean_kafka() {
    log_sec "Step 9 — Kafka Cluster"
    if ! ns_exists "kafka"; then
        log_warn "Namespace 'kafka' not found — skipped."; echo ""; return
    fi
    local topics
    topics=$(kubectl get kafkatopic -n kafka -o name 2>/dev/null || echo "")
    if [[ -n "$topics" ]]; then
        log_info "Deleting Kafka topics..."
        echo "$topics" | xargs -r kubectl delete -n kafka \
            --ignore-not-found=true --timeout=60s 2>&1 | grep -v "NotFound" || true
        log_ok "Kafka topics deleted."
    else
        log_warn "No Kafka topics found — skipped."
    fi
    safe_delete "kafkanodepool" "combined"      "kafka"
    safe_delete "kafka"         "kafka-cluster" "kafka"
    log_info "Waiting for Kafka pods to terminate..."
    kubectl wait --for=delete pod -l strimzi.io/cluster=kafka-cluster \
        -n kafka --timeout=120s 2>&1 | grep -v "no matching" || true
    if [[ "$MODE" == "full" ]]; then
        safe_delete_pvcs "kafka" "strimzi.io/cluster=kafka-cluster"
    else
        log_warn "[light] Keeping Kafka PVCs."
    fi
    echo ""
}

clean_mongodb() {
    log_sec "Step 7 — MongoDB"
    safe_delete "statefulset" "mongodb"          "default"
    safe_delete "service"     "mongodb"          "default"
    safe_delete "service"     "mongodb-external" "default"
    ns_exists "default" && \
        kubectl wait --for=delete pod/mongodb-0 \
            -n default --timeout=60s 2>&1 | grep -v "not found" || true
    if [[ "$MODE" == "full" ]]; then
        safe_delete "pvc" "mongodb-storage-mongodb-0" "default"
    else
        log_warn "[light] Keeping MongoDB PVC."
    fi
    echo ""
}

clean_postgres() {
    log_sec "Step 6 — PostgreSQL"
    safe_delete "statefulset" "postgres"        "default"
    safe_delete "service"     "postgres"        "default"
    safe_delete "configmap"   "postgres-config" "default"
    safe_delete "secret"      "pg-credentials"  "default"
    ns_exists "default" && \
        kubectl wait --for=delete pod/postgres-0 \
            -n default --timeout=60s 2>&1 | grep -v "not found" || true
    if [[ "$MODE" == "full" ]]; then
        safe_delete "pvc" "postgres-storage-postgres-0" "default"
    else
        log_warn "[light] Keeping PostgreSQL PVC."
    fi
    echo ""
}

clean_operators() {
    log_sec "Steps 4–5 — Operators (Strimzi + Altinity)"
    safe_delete_namespace "clickhouse-operator"
    safe_delete_namespace "kafka"
    if [[ "$MODE" == "full" ]]; then
        log_info "Removing cluster-scoped Strimzi resources..."
        safe_delete_cluster_resources "clusterrolebinding" "strimzi"
        safe_delete_cluster_resources "clusterrole"        "strimzi"
        safe_delete_cluster_resources "crd"                "strimzi.io"
        log_info "Removing cluster-scoped ClickHouse resources..."
        safe_delete_cluster_resources "clusterrolebinding" "clickhouse"
        safe_delete_cluster_resources "clusterrole"        "clickhouse"
        safe_delete_cluster_resources "crd"                "clickhouse.altinity.com"
    else
        log_warn "[light] Keeping CRDs and ClusterRoles (needed for fast redeploy)."
    fi
    echo ""
}

clean_kind_cluster() {
    log_sec "Step 3 — Kind Cluster"

    # Delete the current cluster name
    if cluster_exists; then
        log_info "Deleting Kind cluster '${CLUSTER_NAME}'..."
        kind delete cluster --name "${CLUSTER_NAME}" 2>&1 || true
        log_ok "Kind cluster '${CLUSTER_NAME}' deleted."
    else
        log_warn "Kind cluster '${CLUSTER_NAME}' not found — skipped."
    fi

    # Also catch the legacy cluster name from before the project rename.
    # If the user ran the old scripts, this cluster may still be alive
    # and invisible to the guards above (which only check for cdc-lakehouse).
    local LEGACY_NAME="data-engineering-challenge"
    if kind get clusters 2>/dev/null | grep -q "^${LEGACY_NAME}$"; then
        log_warn "Found legacy cluster '${LEGACY_NAME}' (old project name) — deleting..."
        kind delete cluster --name "${LEGACY_NAME}" 2>&1 || true
        log_ok "Legacy cluster '${LEGACY_NAME}' deleted."
    fi

    echo ""
}

# ── Orchestrated teardown ─────────────────────────────────────
run_cleanup() {
    local mode_label
    [[ "$MODE" == "full" ]] && mode_label="${R}FULL${NC}" \
                             || mode_label="${Y}LIGHT${NC}"

    big_sep
    echo -e "${W}  ${PROJECT_NAME} — Cleanup (${mode_label}${W})${NC}"
    [[ "$MODE" == "light" ]] && \
        echo -e "  ${DIM}Removing services, keeping Kind cluster and PVCs${NC}"
    [[ "$MODE" == "full" ]] && \
        echo -e "  ${DIM}Removing all resources including cluster and data volumes${NC}"
    big_sep

    local start_ts; start_ts=$(date +%s)

    # Steps 1–2 note (always shown if up-to-step covers them)
    [[ $UP_TO_STEP -ge 1 ]] && clean_steps_1_2

    # Step 14 — Airflow
    [[ $UP_TO_STEP -ge 14 ]] && clean_airflow

    # Step 13 — Silver Layer (no separate K8s resources; part of CHI)

    # Step 12 — ClickHouse (CHI + CHK)
    [[ $UP_TO_STEP -ge 12 ]] && {
        clean_clickhouse
        clean_clickhouse_keeper
    }

    # Steps 10–11 — Kafka Connect + Debezium
    [[ $UP_TO_STEP -ge 10 ]] && clean_kafka_connect

    # Step 9 — Kafka cluster
    [[ $UP_TO_STEP -ge 9 ]] && clean_kafka

    # Step 7 — MongoDB
    [[ $UP_TO_STEP -ge 7 ]] && clean_mongodb

    # Step 6 — PostgreSQL
    [[ $UP_TO_STEP -ge 6 ]] && clean_postgres

    # Steps 4–5 — Operators
    [[ $UP_TO_STEP -ge 4 ]] && clean_operators

    # Step 3 — Kind cluster (full mode only)
    if [[ $UP_TO_STEP -ge 3 ]]; then
        if [[ "$MODE" == "full" ]]; then
            clean_kind_cluster
        else
            log_sec "Step 3 — Kind Cluster"
            log_warn "[light] Kind cluster '${CLUSTER_NAME}' preserved."
            echo -e "  ${DIM}To redeploy services: ./startup.sh --from 4${NC}"
            echo ""
        fi
    fi

    local end_ts; end_ts=$(date +%s)
    local elapsed=$(( end_ts - start_ts ))
    local mins=$(( elapsed / 60 )) secs=$(( elapsed % 60 ))

    big_sep
    if [[ "$MODE" == "full" ]]; then
        echo -e "${G}  ✔ Full cleanup complete — ${mins}m ${secs}s${NC}"
        echo ""
        echo -e "  ${DIM}Environment is clean. Full redeploy from scratch:${NC}"
        echo -e "  ${W}./startup.sh${NC}          ${DIM}# starts from step 1 (tool check + pipeline)${NC}"
    else
        echo -e "${G}  ✔ Light cleanup complete — ${mins}m ${secs}s${NC}"
        echo ""
        echo -e "  ${Y}Kind cluster and PVCs are preserved.${NC}"
        echo -e "  ${DIM}Fast redeploy (skip cluster + tool install):${NC}"
        echo -e "  ${W}./startup.sh --from 4${NC}  ${DIM}# reinstall operators onward${NC}"
        echo -e "  ${W}./startup.sh --from 6${NC}  ${DIM}# redeploy databases onward (operators kept)${NC}"
    fi
    big_sep
}

# ── Entry point ───────────────────────────────────────────────
main() {
    parse_args "$@"

    [[ -z "$MODE" ]] && interactive_menu

    [[ "$MODE" == "light" || "$MODE" == "full" ]] || {
        log_err "Invalid mode: '${MODE}'. Use 'light' or 'full'."; exit 1
    }

    if ! $AUTO_YES; then
        echo ""
        echo -e "  Mode: ${W}${MODE^^}${NC}"
        [[ "$MODE" == "full" ]] && \
            echo -e "  ${R}⚠ This will permanently delete the Kind cluster and all data volumes.${NC}"
        echo ""
        read -rp "  Proceed? (yes/no): " confirm
        [[ "$confirm" =~ ^[Yy][Ee][Ss]$ ]] || { log_info "Aborted."; exit 0; }
    fi

    run_cleanup
}

main "$@"