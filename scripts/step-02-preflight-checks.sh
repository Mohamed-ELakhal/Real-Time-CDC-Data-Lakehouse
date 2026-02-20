#!/bin/bash
# ============================================================
#  Real-Time CDC Data Lakehouse
#  Step 2 — System Pre-flight Checks
#
#  Verifies the machine is ready to run the pipeline before
#  any Kubernetes resources are created. Catches problems
#  early so failures don't happen mid-deploy.
#
#  Checks:
#    1. All required tools are present and functional
#    2. Docker daemon is running
#    3. Available RAM    (warn < 10 GB, error < 8 GB)
#    4. Available disk   (warn < 20 GB, error < 10 GB)
#    5. Docker daemon memory (macOS Docker Desktop)
#    6. Required ports are free (8080, 8123, 9092, 5432, 27017)
#    7. No conflicting Kind cluster named cdc-lakehouse already running
#    8. Internet connectivity (pulls images from registries)
# ============================================================

set +e

R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'
B='\033[0;34m'; W='\033[1;37m'; DIM='\033[2m'; NC='\033[0m'

log_info()  { echo -e "${B}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${G}[OK]${NC}    $*"; }
log_warn()  { echo -e "${Y}[WARN]${NC}  $*"; }
log_err()   { echo -e "${R}[ERROR]${NC} $*"; }
separator() { echo -e "${DIM}──────────────────────────────────────────────────────────────${NC}"; }

CLUSTER_NAME="cdc-lakehouse"
MIN_RAM_GB=8
REC_RAM_GB=10
MIN_DISK_GB=10
REC_DISK_GB=20
MIN_DOCKER_MEM_GB=8

WARNINGS=0
ERRORS=0

warn() { log_warn "$*"; WARNINGS=$((WARNINGS + 1)); }
fail() { log_err  "$*"; ERRORS=$((ERRORS + 1)); }
pass() { log_ok   "$*"; }

echo ""
echo -e "${W}════════════════════════════════════════════════════════════${NC}"
echo -e "${W}  Step 2 — System Pre-flight Checks${NC}"
echo -e "${W}════════════════════════════════════════════════════════════${NC}"
echo ""

OS_TYPE="linux"
[[ "$(uname -s)" == "Darwin" ]] && OS_TYPE="macos"

# ── Check 1: Required tools ───────────────────────────────────
separator
echo -e "${W}[1/8] Required Tools${NC}"
echo ""

TOOLS_OK=true
for cmd in docker kubectl helm kind curl python3; do
    if command -v "$cmd" &>/dev/null; then
        local_ver=""
        case "$cmd" in
            docker)  local_ver=$(docker --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1) ;;
            kubectl) local_ver=$(kubectl version --client --short 2>/dev/null \
                                | grep -oE 'v[0-9.]+' | head -1 \
                                || kubectl version --client 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
            helm)    local_ver=$(helm version --short 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
            kind)    local_ver=$(kind version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
            curl)    local_ver=$(curl --version 2>/dev/null | head -1 | awk '{print $2}') ;;
            python3) local_ver=$(python3 --version 2>&1 | awk '{print $2}') ;;
        esac
        echo -e "  ${G}✔${NC}  ${cmd}  ${DIM}${local_ver}${NC}"
    else
        echo -e "  ${R}✘${NC}  ${cmd}  ${R}NOT FOUND${NC}"
        TOOLS_OK=false
        ERRORS=$((ERRORS + 1))
    fi
done

if ! $TOOLS_OK; then
    echo ""
    fail "Some required tools are missing. Run: ./startup.sh --from 1"
fi
echo ""

# ── Check 2: Docker daemon ────────────────────────────────────
separator
echo -e "${W}[2/8] Docker Daemon${NC}"
echo ""

if ! command -v docker &>/dev/null; then
    fail "Docker is not installed."
elif docker info &>/dev/null 2>&1; then
    local_docker_ver=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "unknown")
    pass "Docker daemon is running (server: ${local_docker_ver})"
else
    if [[ "$OS_TYPE" == "macos" ]]; then
        log_info "Docker Desktop not running — attempting to start..."
        open -a Docker 2>/dev/null || true
        log_info "Waiting up to 90s for Docker daemon..."
        for i in $(seq 1 45); do
            docker info &>/dev/null 2>&1 && break
            sleep 2
            [[ $((i % 10)) -eq 0 ]] && log_info "Still waiting... (${i}/45)"
        done
        if docker info &>/dev/null 2>&1; then
            pass "Docker Desktop started successfully."
        else
            fail "Docker Desktop did not start. Please open Docker Desktop manually."
        fi
    else
        log_info "Attempting to start Docker daemon..."
        sudo systemctl start docker 2>/dev/null || true
        sleep 3
        if docker info &>/dev/null 2>&1; then
            pass "Docker daemon started."
        else
            fail "Docker daemon is not running. Run: sudo systemctl start docker"
        fi
    fi
fi
echo ""

# ── Check 3: Available RAM ────────────────────────────────────
separator
echo -e "${W}[3/8] Available RAM${NC}"
echo ""

RAM_GB=0
if [[ "$OS_TYPE" == "macos" ]]; then
    TOTAL_RAM_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
    RAM_GB=$(( TOTAL_RAM_BYTES / 1024 / 1024 / 1024 ))
    # On macOS, "available" RAM is tricky; report total
    log_info "Total system RAM: ${RAM_GB} GB"
    log_info "(macOS manages memory dynamically — total RAM is what matters)"
else
    RAM_KB=$(awk '/MemAvailable/ {print $2}' /proc/meminfo 2>/dev/null || echo "0")
    RAM_GB=$(( RAM_KB / 1024 / 1024 ))
    TOTAL_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo 2>/dev/null || echo "0")
    TOTAL_GB=$(( TOTAL_KB / 1024 / 1024 ))
    log_info "Available RAM: ${RAM_GB} GB / Total: ${TOTAL_GB} GB"
fi

if [[ $RAM_GB -ge $REC_RAM_GB ]]; then
    pass "RAM is sufficient (${RAM_GB} GB available, ${REC_RAM_GB} GB recommended)."
elif [[ $RAM_GB -ge $MIN_RAM_GB ]]; then
    warn "RAM is below recommended (${RAM_GB} GB available, ${REC_RAM_GB} GB recommended)."
    log_warn "  Pipeline will run but may experience memory pressure on heavier loads."
    log_warn "  Reduce limits in step-12 (ClickHouse) or step-14 (Airflow) if pods are evicted."
else
    fail "Insufficient RAM (${RAM_GB} GB available, ${MIN_RAM_GB} GB minimum required)."
    log_err "  The pipeline will very likely OOMKill. Provision more RAM before proceeding."
fi
echo ""

# ── Check 4: Available disk space ────────────────────────────
separator
echo -e "${W}[4/8] Available Disk Space${NC}"
echo ""

if [[ "$OS_TYPE" == "macos" ]]; then
    DISK_AVAIL_KB=$(df -k "$HOME" 2>/dev/null | awk 'NR==2 {print $4}')
else
    DISK_AVAIL_KB=$(df -k "$HOME" 2>/dev/null | awk 'NR==2 {print $4}')
fi
DISK_GB=$(( ${DISK_AVAIL_KB:-0} / 1024 / 1024 ))

if [[ $DISK_GB -ge $REC_DISK_GB ]]; then
    pass "Disk space is sufficient (${DISK_GB} GB available, ${REC_DISK_GB} GB recommended)."
elif [[ $DISK_GB -ge $MIN_DISK_GB ]]; then
    warn "Disk space is below recommended (${DISK_GB} GB available, ${REC_DISK_GB} GB recommended)."
    log_warn "  Docker image cache and PVCs may run out of space mid-deploy."
    log_warn "  Free up disk space or reduce PVC sizes in the step scripts."
else
    fail "Insufficient disk space (${DISK_GB} GB available, ${MIN_DISK_GB} GB minimum)."
fi
echo ""

# ── Check 5: Docker Desktop memory allocation (macOS) ─────────
separator
echo -e "${W}[5/8] Docker Engine Memory Allocation${NC}"
echo ""

if [[ "$OS_TYPE" == "macos" ]]; then
    # Docker Desktop stores its config in ~/Library/Group Containers/.../settings.json
    DOCKER_SETTINGS=$(find ~/Library/Group\ Containers -name settings.json \
        -path "*/Docker*" 2>/dev/null | head -1)
    if [[ -n "$DOCKER_SETTINGS" ]]; then
        DOCKER_MEM_MB=$(python3 -c "
import json, sys
try:
    d = json.load(open('${DOCKER_SETTINGS}'))
    print(d.get('memoryMiB', d.get('memory', 0)))
except:
    print(0)
" 2>/dev/null || echo "0")
        DOCKER_MEM_GB=$(( ${DOCKER_MEM_MB:-0} / 1024 ))
        if [[ $DOCKER_MEM_GB -ge $MIN_DOCKER_MEM_GB ]]; then
            pass "Docker Desktop memory: ${DOCKER_MEM_GB} GB (≥ ${MIN_DOCKER_MEM_GB} GB required)."
        else
            warn "Docker Desktop memory allocation is low (${DOCKER_MEM_GB} GB)."
            log_warn "  Increase to ≥ 10 GB in: Docker Desktop → Settings → Resources → Memory"
            log_warn "  Current value: ${DOCKER_MEM_MB} MB"
        fi
    else
        log_info "Docker Desktop settings file not found — skipping memory allocation check."
        log_info "Verify manually: Docker Desktop → Settings → Resources → Memory ≥ 10 GB"
    fi
elif command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
    # Linux Docker Engine: use cgroup limits
    DOCKER_MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
    DOCKER_MEM_GB=$(( ${DOCKER_MEM_BYTES:-0} / 1024 / 1024 / 1024 ))
    if [[ $DOCKER_MEM_GB -ge $MIN_DOCKER_MEM_GB ]]; then
        pass "Docker Engine can access ${DOCKER_MEM_GB} GB of memory."
    else
        warn "Docker reports only ${DOCKER_MEM_GB} GB accessible. ${MIN_DOCKER_MEM_GB} GB+ recommended."
    fi
else
    log_info "Docker not running — skipping memory allocation check."
fi
echo ""

# ── Check 6: Required ports ───────────────────────────────────
separator
echo -e "${W}[6/8] Required Port Availability${NC}"
echo ""

declare -A PORT_DESC
PORT_DESC[5432]="PostgreSQL"
PORT_DESC[27017]="MongoDB"
PORT_DESC[9092]="Kafka"
PORT_DESC[8083]="Kafka Connect"
PORT_DESC[8123]="ClickHouse HTTP"
PORT_DESC[9000]="ClickHouse Native"
PORT_DESC[8080]="Airflow UI"
PORT_DESC[9181]="ClickHouseKeeper"

PORTS_OK=true
for port in 5432 27017 9092 8083 8123 9000 8080 9181; do
    desc="${PORT_DESC[$port]}"
    IN_USE=false

    if [[ "$OS_TYPE" == "macos" ]]; then
        lsof -nP -iTCP:${port} -sTCP:LISTEN &>/dev/null && IN_USE=true
    else
        # Try ss first (modern), fall back to netstat, then lsof
        if command -v ss &>/dev/null; then
            ss -lnp "sport = :${port}" 2>/dev/null | grep -q "LISTEN" && IN_USE=true
        elif command -v netstat &>/dev/null; then
            netstat -lnp 2>/dev/null | grep ":${port} " | grep -q LISTEN && IN_USE=true
        elif command -v lsof &>/dev/null; then
            lsof -nP -iTCP:${port} -sTCP:LISTEN &>/dev/null && IN_USE=true
        fi
    fi

    if $IN_USE; then
        # Identify the process occupying the port
        PROC=""
        if command -v lsof &>/dev/null; then
            PROC=$(lsof -nP -iTCP:${port} -sTCP:LISTEN 2>/dev/null \
                | awk 'NR==2 {print $1 " (PID " $2 ")"}')
        fi
        warn "Port ${port} (${desc}) is in use${PROC:+ by ${PROC}}."
        warn "  Kind will map this port via NodePort — if Kind is already running, this may be fine."
        PORTS_OK=false
    else
        echo -e "  ${G}✔${NC}  Port ${port}  ${DIM}(${desc})${NC}"
    fi
done

$PORTS_OK && pass "All required ports are available." || \
    log_warn "  Ports in use may conflict with Kind NodePort mappings. Stop conflicting services if deploying fresh."
echo ""

# ── Check 7: Conflicting Kind cluster ────────────────────────
separator
echo -e "${W}[7/8] Kind Cluster State${NC}"
echo ""

if ! command -v kind &>/dev/null; then
    log_info "Kind not installed yet — skipping cluster state check."
elif kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    CLUSTER_STATUS=$(kubectl --context "kind-${CLUSTER_NAME}" cluster-info 2>/dev/null \
        && echo "reachable" || echo "unreachable")
    if [[ "$CLUSTER_STATUS" == "reachable" ]]; then
        pass "Kind cluster '${CLUSTER_NAME}' already exists and is reachable."
        log_info "  Startup will skip cluster creation (step 3 auto-detects this)."
    else
        warn "Kind cluster '${CLUSTER_NAME}' exists but is not reachable."
        warn "  It may be in a broken state. Consider: kind delete cluster --name ${CLUSTER_NAME}"
    fi
else
    pass "No existing '${CLUSTER_NAME}' cluster — will be created fresh in step 3."
fi
echo ""

# ── Check 8: Internet connectivity ───────────────────────────
separator
echo -e "${W}[8/8] Internet Connectivity${NC}"
echo ""

REGISTRIES=(
    "registry.k8s.io:Container images (Kubernetes)"
    "quay.io:Debezium / Strimzi images"
    "docker.io:Docker Hub images"
    "ghcr.io:GitHub Container Registry"
    "dl.k8s.io:kubectl binaries"
)

CONN_OK=true
for entry in "${REGISTRIES[@]}"; do
    host="${entry%%:*}"
    desc="${entry#*:}"
    if curl -fsSL --connect-timeout 5 --max-time 8 \
            "https://${host}" -o /dev/null 2>/dev/null; then
        echo -e "  ${G}✔${NC}  ${host}  ${DIM}(${desc})${NC}"
    else
        # Some registries reject root-path requests — check TCP connectivity instead
        if curl -fsSL --connect-timeout 5 "https://${host}/v2/" -o /dev/null 2>/dev/null \
           || nc -zw 5 "${host}" 443 2>/dev/null; then
            echo -e "  ${G}✔${NC}  ${host}  ${DIM}(${desc})${NC}"
        else
            warn "Cannot reach ${host} (${desc})."
            CONN_OK=false
        fi
    fi
done

$CONN_OK && pass "Internet connectivity: all registries reachable." || \
    log_warn "  Some registries unreachable. Image pulls may fail mid-deploy."
echo ""

# ── Summary ───────────────────────────────────────────────────
separator
echo ""
echo -e "${W}Pre-flight Summary${NC}"
echo ""
echo -e "  Warnings:  ${Y}${WARNINGS}${NC}"
echo -e "  Errors:    ${R}${ERRORS}${NC}"
echo ""

if [[ $ERRORS -eq 0 ]]; then
    if [[ $WARNINGS -gt 0 ]]; then
        echo -e "${Y}════════════════════════════════════════════════════════════${NC}"
        echo -e "${Y}  ⚠  Step 2 Complete with ${WARNINGS} warning(s)${NC}"
        echo -e "${Y}     Review warnings above before proceeding.${NC}"
        echo -e "${Y}════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "  Deployment can continue — some warnings are non-blocking."
        echo "  Run: ./startup.sh --from 3   (or ./startup.sh to auto-continue)"
    else
        echo -e "${G}════════════════════════════════════════════════════════════${NC}"
        echo -e "${G}  ✔  Step 2 Complete — System is ready for deployment${NC}"
        echo -e "${G}════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "  Your machine meets all requirements. Proceeding to step 3..."
    fi
    exit 0
else
    echo -e "${R}════════════════════════════════════════════════════════════${NC}"
    echo -e "${R}  ✘  Step 2 Failed — ${ERRORS} blocking issue(s) found${NC}"
    echo -e "${R}     Resolve the errors above before proceeding.${NC}"
    echo -e "${R}════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  After fixing the issues, re-run: ./startup.sh --from 2"
    exit 1
fi