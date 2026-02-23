#!/bin/bash
# ============================================================
#  Real-Time CDC Data Lakehouse
#  Step 1 — Install Prerequisites
#
#  Installs: Docker, kubectl, Helm, Kind, curl, python3
#  Supports: macOS (Homebrew), Ubuntu/Debian, RHEL/Fedora/CentOS
#
#  Safe to re-run — each tool is checked before installing.
# ============================================================

set -e

R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'
B='\033[0;34m'; W='\033[1;37m'; DIM='\033[2m'; NC='\033[0m'

log_info()  { echo -e "${B}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${G}[OK]${NC}    $*"; }
log_warn()  { echo -e "${Y}[WARN]${NC}  $*"; }
log_err()   { echo -e "${R}[ERROR]${NC} $*"; }
log_skip()  { echo -e "${DIM}[SKIP]  $* — already installed${NC}"; }
separator() { echo -e "${DIM}──────────────────────────────────────────────────────────────${NC}"; }

INSTALLED=()
SKIPPED=()
FAILED=()

echo ""
echo -e "${W}════════════════════════════════════════════════════════════${NC}"
echo -e "${W}  Step 1 — Install Prerequisites${NC}"
echo -e "${W}════════════════════════════════════════════════════════════${NC}"
echo ""

# ── OS Detection ─────────────────────────────────────────────
detect_os() {
    unset OS_TYPE PKG_MGR
    case "$(uname -s)" in
        Darwin)
            OS_TYPE="macos"
            PKG_MGR="brew"
            ;;
        Linux)
            if   [[ -f /etc/debian_version ]]; then
                OS_TYPE="debian"
                PKG_MGR="apt"
            elif [[ -f /etc/redhat-release ]]; then
                # Fedora 22+ uses dnf; older RHEL/CentOS uses yum
                if command -v dnf &>/dev/null; then
                    OS_TYPE="rhel"
                    PKG_MGR="dnf"
                else
                    OS_TYPE="rhel"
                    PKG_MGR="yum"
                fi
            elif [[ -f /etc/arch-release ]]; then
                OS_TYPE="arch"
                PKG_MGR="pacman"
            else
                OS_TYPE="linux-unknown"
                PKG_MGR=""
            fi
            ;;
        *)
            OS_TYPE="unsupported"
            PKG_MGR=""
            ;;
    esac

    log_info "Detected OS:  ${OS_TYPE}"
    log_info "Package mgr:  ${PKG_MGR:-unknown}"
    echo ""
}

# ── Sudo helper ───────────────────────────────────────────────
HAS_SUDO=false
check_sudo() {
    if [[ $EUID -eq 0 ]]; then
        HAS_SUDO=true   # already root
        return
    fi
    if sudo -n true 2>/dev/null; then
        HAS_SUDO=true
    else
        log_warn "sudo not available without password. Some installs may require your password."
        # Try a real sudo prompt
        if sudo true 2>/dev/null; then
            HAS_SUDO=true
        fi
    fi
}

run_sudo() {
    if [[ $EUID -eq 0 ]]; then
        "$@"
    else
        sudo "$@"
    fi
}

# ── WSL detection ─────────────────────────────────────────────
# WSL does not run systemd by default, so 'systemctl' will fail.
# Use 'service docker start' instead, or launch dockerd directly.
IS_WSL=false
detect_wsl() {
    if grep -qi microsoft /proc/version 2>/dev/null \
       || grep -qi wsl /proc/version 2>/dev/null \
       || [[ -n "${WSL_DISTRO_NAME:-}" ]]; then
        IS_WSL=true
        log_info "WSL environment detected — will use 'service' instead of 'systemctl'."
    fi
}

# Start and enable Docker in a way appropriate for the environment
start_docker_daemon() {
    if $IS_WSL; then
        if command -v service &>/dev/null; then
            run_sudo service docker start 2>/dev/null || true
        else
            # Last resort: launch dockerd directly in background
            log_info "Starting dockerd directly (no service manager in WSL)..."
            run_sudo dockerd > /tmp/dockerd.log 2>&1 &
        fi
    else
        # Native Linux with systemd
        run_sudo systemctl enable docker --quiet 2>/dev/null || true
        run_sudo systemctl start docker 2>/dev/null || true
    fi
    # Wait up to 15s for daemon to respond
    local waited=0
    while ! docker info &>/dev/null 2>&1 && [[ $waited -lt 15 ]]; do
        sleep 1; waited=$((waited + 1))
    done
}

# ── Homebrew (macOS) ──────────────────────────────────────────
ensure_homebrew() {
    if [[ "$OS_TYPE" != "macos" ]]; then return; fi
    separator
    log_info "Checking Homebrew..."
    if command -v brew &>/dev/null; then
        log_skip "Homebrew"
        brew update --quiet 2>/dev/null || true
    else
        log_info "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL \
            https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        # Add brew to PATH for Apple Silicon
        if [[ -f /opt/homebrew/bin/brew ]]; then
            eval "$(/opt/homebrew/bin/brew shellenv)"
            echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> "${HOME}/.zprofile"
        fi
        log_ok "Homebrew installed."
        INSTALLED+=("homebrew")
    fi
    echo ""
}

# ── Docker ───────────────────────────────────────────────────
install_docker() {
    separator
    log_info "Checking Docker..."

    if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
        local ver; ver=$(docker --version | grep -oE '[0-9]+\.[0-9]+' | head -1)
        log_skip "Docker (${ver})"
        SKIPPED+=("docker")
        return
    fi

    case "$OS_TYPE" in
        macos)
            if [[ -d "/Applications/Docker.app" ]]; then
                log_info "Docker Desktop found but daemon not running — starting it..."
                open -a Docker
                log_info "Waiting up to 60s for Docker daemon..."
                for i in $(seq 1 30); do
                    docker info &>/dev/null && break
                    sleep 2
                done
                docker info &>/dev/null && { log_ok "Docker daemon started."; SKIPPED+=("docker"); return; }
                log_warn "Docker Desktop installed but daemon did not start in time."
                log_warn "Please open Docker Desktop manually and re-run this step."
                FAILED+=("docker-start")
                return 1
            fi
            log_info "Installing Docker Desktop via Homebrew Cask..."
            log_warn "This will download ~600 MB. Docker Desktop will open after install."
            brew install --cask docker
            log_info "Launching Docker Desktop for the first time..."
            open -a Docker
            log_info "Waiting up to 120s for Docker daemon to start..."
            for i in $(seq 1 60); do
                docker info &>/dev/null && break
                sleep 2
                [[ $((i % 10)) -eq 0 ]] && log_info "Still waiting... (${i}/60)"
            done
            if ! docker info &>/dev/null; then
                log_warn "Docker Desktop installed but not yet responding."
                log_warn "Please grant required permissions in the Docker Desktop UI,"
                log_warn "then re-run: ./startup.sh --from 1"
                FAILED+=("docker-daemon")
                return 1
            fi
            log_ok "Docker Desktop installed and running."
            INSTALLED+=("docker")
            ;;

        debian)
            log_info "Installing Docker Engine (official script)..."
            # Remove any conflicting packages first
            for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do
                run_sudo apt-get remove -y "$pkg" 2>/dev/null || true
            done
            run_sudo apt-get update -qq
            run_sudo apt-get install -y -qq ca-certificates curl gnupg lsb-release
            run_sudo install -m 0755 -d /etc/apt/keyrings
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
                | run_sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg 2>/dev/null
            run_sudo chmod a+r /etc/apt/keyrings/docker.gpg
            echo "deb [arch=$(dpkg --print-architecture) \
                  signed-by=/etc/apt/keyrings/docker.gpg] \
                  https://download.docker.com/linux/$(. /etc/os-release && echo "$ID") \
                  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
                | run_sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            run_sudo apt-get update -qq
            run_sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io \
                docker-buildx-plugin docker-compose-plugin
            # Add current user to docker group (avoids sudo for docker commands)
            run_sudo usermod -aG docker "${USER}" 2>/dev/null || true
            start_docker_daemon
            log_ok "Docker Engine installed and started."
            log_warn "You may need to log out and back in for group membership to take effect."
            log_warn "Or run: newgrp docker"
            INSTALLED+=("docker")
            ;;

        rhel)
            log_info "Installing Docker Engine for RHEL/Fedora/CentOS..."
            run_sudo "$PKG_MGR" remove -y docker docker-client docker-client-latest \
                docker-common docker-latest docker-latest-logrotate docker-logrotate \
                docker-engine podman runc 2>/dev/null || true
            run_sudo "$PKG_MGR" install -y yum-utils 2>/dev/null || true
            run_sudo yum-config-manager --add-repo \
                https://download.docker.com/linux/centos/docker-ce.repo 2>/dev/null || \
            run_sudo "$PKG_MGR" config-manager --add-repo \
                https://download.docker.com/linux/fedora/docker-ce.repo 2>/dev/null || true
            run_sudo "$PKG_MGR" install -y docker-ce docker-ce-cli containerd.io \
                docker-buildx-plugin docker-compose-plugin
            run_sudo usermod -aG docker "${USER}" 2>/dev/null || true
            start_docker_daemon
            log_ok "Docker Engine installed and started."
            INSTALLED+=("docker")
            ;;

        arch)
            log_info "Installing Docker via pacman..."
            run_sudo pacman -Sy --noconfirm docker docker-compose
            run_sudo usermod -aG docker "${USER}" 2>/dev/null || true
            start_docker_daemon
            log_ok "Docker installed."
            INSTALLED+=("docker")
            ;;

        *)
            log_warn "Cannot auto-install Docker on this OS (${OS_TYPE})."
            log_warn "Please install Docker manually: https://docs.docker.com/get-docker/"
            FAILED+=("docker")
            ;;
    esac
    echo ""
}

# ── kubectl ──────────────────────────────────────────────────
install_kubectl() {
    separator
    log_info "Checking kubectl..."

    if command -v kubectl &>/dev/null; then
        local ver; ver=$(kubectl version --client --short 2>/dev/null | grep -oE 'v[0-9.]+' | head -1)
        log_skip "kubectl (${ver})"
        SKIPPED+=("kubectl")
        return
    fi

    case "$OS_TYPE" in
        macos)
            brew install kubectl
            ;;
        debian)
            run_sudo apt-get install -y -qq apt-transport-https
            curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key \
                | run_sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg 2>/dev/null
            echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] \
                  https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' \
                | run_sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null
            run_sudo apt-get update -qq
            run_sudo apt-get install -y -qq kubectl
            ;;
        rhel)
            cat <<EOF | run_sudo tee /etc/yum.repos.d/kubernetes.repo > /dev/null
[kubernetes]
name=Kubernetes
baseurl=https://pkgs.k8s.io/core:/stable:/v1.29/rpm/
enabled=1
gpgcheck=1
gpgkey=https://pkgs.k8s.io/core:/stable:/v1.29/rpm/repodata/repomd.xml.key
EOF
            run_sudo "$PKG_MGR" install -y kubectl
            ;;
        arch)
            run_sudo pacman -Sy --noconfirm kubectl
            ;;
        *)
            # Universal fallback: download binary directly
            log_info "Installing kubectl via direct binary download..."
            local KUBE_VERSION
            KUBE_VERSION=$(curl -fsSL https://dl.k8s.io/release/stable.txt 2>/dev/null || echo "v1.29.0")
            local ARCH
            ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
            curl -fsSL "https://dl.k8s.io/release/${KUBE_VERSION}/bin/linux/${ARCH}/kubectl" \
                -o /tmp/kubectl
            run_sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
            rm -f /tmp/kubectl
            ;;
    esac

    command -v kubectl &>/dev/null || { log_err "kubectl installation failed."; FAILED+=("kubectl"); return 1; }
    log_ok "kubectl installed: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"
    INSTALLED+=("kubectl")
    echo ""
}

# ── Helm ──────────────────────────────────────────────────────
install_helm() {
    separator
    log_info "Checking Helm..."

    if command -v helm &>/dev/null; then
        local ver; ver=$(helm version --short 2>/dev/null | grep -oE 'v[0-9.]+' | head -1)
        log_skip "Helm (${ver})"
        SKIPPED+=("helm")
        return
    fi

    case "$OS_TYPE" in
        macos)
            brew install helm
            ;;
        *)
            log_info "Installing Helm via official installer script..."
            curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \
                | bash
            ;;
    esac

    command -v helm &>/dev/null || { log_err "Helm installation failed."; FAILED+=("helm"); return 1; }
    log_ok "Helm installed: $(helm version --short 2>/dev/null)"
    INSTALLED+=("helm")
    echo ""
}

# ── Kind ──────────────────────────────────────────────────────
install_kind() {
    separator
    log_info "Checking Kind..."

    if command -v kind &>/dev/null; then
        local ver; ver=$(kind version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1)
        log_skip "Kind (${ver})"
        SKIPPED+=("kind")
        return
    fi

    case "$OS_TYPE" in
        macos)
            brew install kind
            ;;
        *)
            local ARCH
            ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
            local KIND_VERSION="v0.22.0"
            log_info "Installing Kind ${KIND_VERSION} (${ARCH})..."
            curl -fsSL "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}" \
                -o /tmp/kind
            run_sudo install -m 0755 /tmp/kind /usr/local/bin/kind
            rm -f /tmp/kind
            ;;
    esac

    command -v kind &>/dev/null || { log_err "Kind installation failed."; FAILED+=("kind"); return 1; }
    log_ok "Kind installed: $(kind version 2>/dev/null)"
    INSTALLED+=("kind")
    echo ""
}

# ── curl ──────────────────────────────────────────────────────
install_curl() {
    separator
    log_info "Checking curl..."
    if command -v curl &>/dev/null; then
        log_skip "curl ($(curl --version | head -1 | awk '{print $2}'))"
        SKIPPED+=("curl")
        return
    fi
    case "$OS_TYPE" in
        macos)   brew install curl ;;
        debian)  run_sudo apt-get install -y -qq curl ;;
        rhel)    run_sudo "$PKG_MGR" install -y curl ;;
        arch)    run_sudo pacman -Sy --noconfirm curl ;;
        *)       log_warn "Please install curl manually."; FAILED+=("curl"); return ;;
    esac
    log_ok "curl installed."
    INSTALLED+=("curl")
    echo ""
}

# ── python3 ──────────────────────────────────────────────────
install_python3() {
    separator
    log_info "Checking python3..."
    if command -v python3 &>/dev/null; then
        log_skip "python3 ($(python3 --version 2>&1 | awk '{print $2}'))"
        SKIPPED+=("python3")
        return
    fi
    case "$OS_TYPE" in
        macos)   brew install python3 ;;
        debian)  run_sudo apt-get install -y -qq python3 ;;
        rhel)    run_sudo "$PKG_MGR" install -y python3 ;;
        arch)    run_sudo pacman -Sy --noconfirm python ;;
        *)       log_warn "Please install python3 manually."; FAILED+=("python3"); return ;;
    esac
    log_ok "python3 installed."
    INSTALLED+=("python3")
    echo ""
}

# ── nc (netcat) — used by CHK health check ───────────────────
install_nc() {
    separator
    log_info "Checking netcat (nc)..."
    if command -v nc &>/dev/null; then
        log_skip "nc (netcat)"
        SKIPPED+=("nc")
        return
    fi
    case "$OS_TYPE" in
        macos)   brew install netcat 2>/dev/null || true ;;
        debian)  run_sudo apt-get install -y -qq netcat-openbsd 2>/dev/null || true ;;
        rhel)    run_sudo "$PKG_MGR" install -y nmap-ncat 2>/dev/null || true ;;
        arch)    run_sudo pacman -Sy --noconfirm openbsd-netcat 2>/dev/null || true ;;
        *)       log_warn "netcat not found — CHK health checks may be limited." ;;
    esac
    command -v nc &>/dev/null && { log_ok "nc installed."; INSTALLED+=("nc"); } \
        || log_warn "nc not available; CHK health checks will skip."
    echo ""
}

# ── Shell completion (optional quality-of-life) ──────────────
setup_completions() {
    [[ "$OS_TYPE" == "macos" ]] || return   # Linux distros usually handle this via packages

    log_info "Setting up shell completions..."
    local shell_rc="${HOME}/.zshrc"
    [[ "$SHELL" == *"bash"* ]] && shell_rc="${HOME}/.bashrc"

    for tool in kubectl helm kind; do
        if command -v "$tool" &>/dev/null; then
            local comp_line
            case "$tool" in
                kubectl) comp_line='source <(kubectl completion '"$(basename "$SHELL")"')' ;;
                helm)    comp_line='source <(helm completion '"$(basename "$SHELL")"')' ;;
                kind)    comp_line='source <(kind completion '"$(basename "$SHELL")"')' ;;
            esac
            grep -qF "$comp_line" "$shell_rc" 2>/dev/null \
                || echo "$comp_line" >> "$shell_rc"
        fi
    done
    log_ok "Shell completions configured in ${shell_rc}"
    echo ""
}

# ── Final verification ────────────────────────────────────────
verify_all() {
    separator
    echo ""
    echo -e "${W}Installation Summary${NC}"
    echo ""

    local all_ok=true
    for cmd in docker kubectl helm kind curl python3; do
        if command -v "$cmd" &>/dev/null; then
            local ver=""
            case "$cmd" in
                docker)  ver=$(docker --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1) ;;
                kubectl) ver=$(kubectl version --client --short 2>/dev/null | grep -oE 'v[0-9.]+' | head -1 \
                               || kubectl version --client 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
                helm)    ver=$(helm version --short 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
                kind)    ver=$(kind version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1) ;;
                curl)    ver=$(curl --version 2>/dev/null | head -1 | awk '{print $2}') ;;
                python3) ver=$(python3 --version 2>&1 | awk '{print $2}') ;;
            esac
            echo -e "  ${G}✔${NC}  ${cmd}   ${DIM}${ver}${NC}"
        else
            echo -e "  ${R}✘${NC}  ${cmd}   ${R}NOT FOUND${NC}"
            all_ok=false
        fi
    done
    echo ""

    if [[ ${#INSTALLED[@]} -gt 0 ]]; then
        echo -e "  Newly installed: ${G}${INSTALLED[*]}${NC}"
    fi
    if [[ ${#SKIPPED[@]} -gt 0 ]]; then
        echo -e "  Already present: ${DIM}${SKIPPED[*]}${NC}"
    fi
    if [[ ${#FAILED[@]} -gt 0 ]]; then
        echo -e "  ${R}Failed:${NC} ${FAILED[*]}"
    fi
    echo ""

    if ! docker info &>/dev/null 2>&1; then
        log_warn "Docker is installed but the daemon is not running."
        if [[ "$OS_TYPE" == "macos" ]]; then
            log_warn "Open Docker Desktop from Applications, then re-run step 2."
        else
            log_warn "Start it with: sudo systemctl start docker"
        fi
        all_ok=false
    fi

    if $all_ok; then
        echo -e "${G}════════════════════════════════════════════════════════════${NC}"
        echo -e "${G}  ✔  Step 1 Complete — All prerequisites installed${NC}"
        echo -e "${G}════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "Next: ./startup.sh --from 2   or   ./startup.sh (auto-continues)"
        return 0
    else
        echo -e "${R}  ✘  Some tools are missing or Docker is not running.${NC}"
        echo -e "  Please resolve the issues above and re-run: ${W}./startup.sh --from 1${NC}"
        return 1
    fi
}

# ── Main ──────────────────────────────────────────────────────
main() {
    detect_os
    detect_wsl
    check_sudo

    ensure_homebrew
    install_docker   || true    # warn but don't abort — user may fix manually
    install_kubectl  || true
    install_helm     || true
    install_kind     || true
    install_curl     || true
    install_python3  || true
    install_nc       || true
    setup_completions

    verify_all
}

main "$@"